"""
Monarch Money ↔ Google Sheets Sync
====================================
Phase 1: Fetch Monarch data + read sheet → report discrepancies
Phase 2: On approval, update only manual (non-formula) cells

Never modifies formula cells.
Run: python3 sync.py
"""

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from monarchmoney import MonarchMoney

CONFIG_FILE = ".mm/config.json"
SHEET_RANGE = "Fact!A2:L44"
SESSION_FILE = ".mm/mm_session.pickle"

# Load sensitive config from .mm/config.json (not committed to git)
_config = json.loads(Path(CONFIG_FILE).read_text()) if Path(CONFIG_FILE).exists() else {}
SPREADSHEET_ID = _config.get("spreadsheet_id") or os.environ.get("MONARCH_SPREADSHEET_ID")
if not SPREADSHEET_ID:
    raise SystemExit(f"❌ spreadsheet_id missing. Add it to {CONFIG_FILE} or set MONARCH_SPREADSHEET_ID env var.")
SHEET_RANGE    = _config.get("sheet_range", SHEET_RANGE)
MONARCH_TO_SHEET = _config.get("account_mapping", {})

# Account mapping loaded from .mm/config.json — see config for details
MONARCH_TO_SHEET = {}  # populated after config is loaded below

# Monarch ticker normalization (BTC-USD → BTC)
def normalize_ticker(sym):
    if not sym or sym in ("USD-USD", "CASH"): return None
    return sym.replace("-USD", "")


def gog_read(range_str, render="FORMATTED_VALUE"):
    """Read sheet range via gog CLI."""
    result = subprocess.run(
        ["gog", "sheets", "read", SPREADSHEET_ID, range_str,
         "--json", f"--render={render}"],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"gog read failed: {result.stderr}")
    return json.loads(result.stdout).get("values", [])


def gog_write(range_str, values):
    """Write values to sheet via gog CLI."""
    data = json.dumps({"range": range_str, "values": values})
    result = subprocess.run(
        ["gog", "sheets", "write", SPREADSHEET_ID, range_str,
         "--json", "--values", data],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        raise RuntimeError(f"gog write failed: {result.stderr}")


def parse_dollar(s):
    """Parse '$1,234.56' or '$ (1,234.56)' → float."""
    if not s or not str(s).strip(): return None
    s = str(s).strip().replace("$", "").replace(",", "").replace(" ", "")
    s = s.replace("(", "-").replace(")", "")
    try: return float(s)
    except: return None


async def fetch_monarch():
    """Fetch accounts + holdings from Monarch."""
    mm = MonarchMoney(session_file=SESSION_FILE)
    mm.load_session()

    print("📡 Fetching Monarch accounts...")
    acct_data = await mm.get_accounts()
    accounts = {a["displayName"]: a for a in acct_data["accounts"]}

    print("📡 Fetching Monarch holdings...")
    holdings = {}
    for a in acct_data["accounts"]:
        if (a.get("holdingsCount") or 0) > 0:
            h = await mm.get_account_holdings(a["id"])
            edges = h.get("portfolio", {}).get("aggregateHoldings", {}).get("edges", [])
            qty_map = {}
            for e in edges:
                n = e["node"]
                sec = n.get("security") or {}
                sym = normalize_ticker(sec.get("ticker") or sec.get("name"))
                if sym:
                    qty_map[sym] = round(n.get("quantity") or 0, 6)
            holdings[a["displayName"]] = qty_map

    return accounts, holdings


def read_sheet():
    """Read sheet: formatted values + formulas, row-indexed."""
    print("📊 Reading Google Sheet...")
    formatted = gog_read(SHEET_RANGE, render="FORMATTED_VALUE")
    formulas   = gog_read(SHEET_RANGE, render="FORMULA")

    rows = []
    for i, row in enumerate(formatted):
        formula_row = formulas[i] if i < len(formulas) else []
        # Pad to 12 columns
        row = list(row) + [""] * (12 - len(row))
        formula_row = list(formula_row) + [""] * (12 - len(formula_row))
        rows.append({
            "row_index": i + 2,  # 1-indexed, +1 for header = +2
            "account":   row[0],
            "desc":      row[1],
            "symbol":    row[2],
            "type":      row[3],
            "units":     row[5],
            "per_unit":  row[6],
            "value":     row[11],
            # Formula flags
            "units_is_formula":    str(formula_row[5]).startswith("="),
            "per_unit_is_formula": str(formula_row[6]).startswith("="),
            "value_is_formula":    str(formula_row[11]).startswith("="),
        })
    return rows


def build_discrepancies(sheet_rows, monarch_accounts, monarch_holdings):
    """Compare Monarch vs sheet, return list of proposed updates."""
    discrepancies = []

    # Build sheet lookup: {account_name: [rows]}
    sheet_by_acct = {}
    for r in sheet_rows:
        sheet_by_acct.setdefault(r["account"], []).append(r)

    for monarch_name, monarch_acct in monarch_accounts.items():
        sheet_names = MONARCH_TO_SHEET.get(monarch_name)
        if not sheet_names:
            continue

        m_balance = monarch_acct.get("currentBalance") or 0
        m_holdings = monarch_holdings.get(monarch_name, {})
        acct_type = (monarch_acct.get("type") or {}).get("name", "")

        for sheet_name in sheet_names:
            rows = sheet_by_acct.get(sheet_name, [])
            if not rows:
                continue

            for r in rows:
                sym = r["symbol"].strip()
                row_idx = r["row_index"]

                # ── Quantity check (holdings accounts) ──────────────────
                if sym and m_holdings:
                    m_qty = m_holdings.get(sym)
                    s_qty_raw = r["units"]
                    s_qty = None
                    try: s_qty = float(str(s_qty_raw).replace(",", "")) if s_qty_raw else None
                    except: pass

                    if m_qty is not None and s_qty is not None and not r["units_is_formula"]:
                        diff = abs(m_qty - s_qty)
                        if diff > 0.01:
                            discrepancies.append({
                                "type": "quantity",
                                "row": row_idx,
                                "col": "F",
                                "account": sheet_name,
                                "symbol": sym,
                                "current": s_qty,
                                "proposed": m_qty,
                                "diff": m_qty - s_qty,
                            })

                # ── Value check (cash/manual accounts) ──────────────────
                if not sym and not r["value_is_formula"]:
                    s_val = parse_dollar(r["value"])
                    if s_val is None:
                        continue

                    # For multi-row accounts match by description
                    if monarch_name == "2026 Tesla Model Y Performance" and "Tesla" not in r["desc"]:
                        continue
                    if monarch_name == "2024 Honda Odyssey" and "Honda" not in r["desc"]:
                        continue
                    if monarch_name == "Gold":
                        continue  # Gold rows use units, handled above

                    # For investment accounts, cash row = USD-USD holding, not full balance
                    if m_holdings:
                        proposed = m_holdings.get("USD", m_holdings.get("CASH", None))
                        if proposed is None:
                            continue  # no cash holding to compare
                    else:
                        proposed = m_balance

                    diff = abs(proposed - s_val)
                    if diff > 1.0:
                        discrepancies.append({
                            "type": "value",
                            "row": row_idx,
                            "col": "L",
                            "account": sheet_name,
                            "symbol": r["desc"] or sheet_name,
                            "current": s_val,
                            "proposed": proposed,
                            "diff": proposed - s_val,
                        })

    return discrepancies


def print_report(discrepancies):
    if not discrepancies:
        print("\n✅ Everything is in sync! No updates needed.")
        return

    qty_diffs  = [d for d in discrepancies if d["type"] == "quantity"]
    val_diffs  = [d for d in discrepancies if d["type"] == "value"]

    if qty_diffs:
        print(f"\n{'═'*70}")
        print("📦 QUANTITY DISCREPANCIES")
        print(f"{'═'*70}")
        print(f"  {'Account':<25} {'Symbol':<8} {'Sheet':>12} {'Monarch':>12} {'Diff':>10}")
        print(f"  {'-'*25} {'-'*8} {'-'*12} {'-'*12} {'-'*10}")
        for d in qty_diffs:
            print(f"  {d['account']:<25} {d['symbol']:<8} {d['current']:>12.4f} {d['proposed']:>12.4f} {d['diff']:>+10.4f}")

    if val_diffs:
        print(f"\n{'═'*70}")
        print("💰 VALUE DISCREPANCIES")
        print(f"{'═'*70}")
        print(f"  {'Account':<30} {'Sheet':>14} {'Monarch':>14} {'Diff':>12}")
        print(f"  {'-'*30} {'-'*14} {'-'*14} {'-'*12}")
        for d in val_diffs:
            print(f"  {d['symbol']:<30} ${d['current']:>13,.2f} ${d['proposed']:>13,.2f} ${d['diff']:>+11,.2f}")


def apply_updates(discrepancies, sheet_rows):
    """Write proposed values to sheet, skipping formula cells."""
    row_map = {r["row_index"]: r for r in sheet_rows}

    for d in discrepancies:
        cell = f"Fact!{d['col']}{d['row']}"
        val = round(d["proposed"], 4) if d["type"] == "quantity" else round(d["proposed"], 2)
        print(f"  ✏️  Updating {cell}: {d['current']} → {val}")
        gog_write(cell, [[val]])

    print(f"\n✅ Updated {len(discrepancies)} cell(s).")


async def main():
    monarch_accounts, monarch_holdings = await fetch_monarch()
    sheet_rows = read_sheet()
    discrepancies = build_discrepancies(sheet_rows, monarch_accounts, monarch_holdings)

    print_report(discrepancies)

    if not discrepancies:
        return

    print(f"\n{'─'*70}")
    print(f"Found {len(discrepancies)} discrepancy(s).")
    answer = input("\nApply updates to Google Sheet? [y/N] ").strip().lower()
    if answer == "y":
        apply_updates(discrepancies, sheet_rows)
    else:
        print("No changes made.")


asyncio.run(main())
