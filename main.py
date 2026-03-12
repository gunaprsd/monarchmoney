import asyncio
import json
from pathlib import Path

from monarchmoney import MonarchMoney

_SESSION_FILE_ = ".mm/mm_session.pickle"


async def get_mm() -> MonarchMoney:
    """Load saved session, or use token from env/file, or interactive login."""
    Path(".mm").mkdir(exist_ok=True)
    mm = MonarchMoney(session_file=_SESSION_FILE_)

    # Option 1: token stored in .mm/token.txt (for Google SSO users)
    token_file = Path(".mm/token.txt")
    if token_file.exists():
        token = token_file.read_text().strip()
        print("Loading token from .mm/token.txt...")
        mm.set_token(token)
        mm._headers["Authorization"] = f"Token {token}"
        mm.save_session()
        print(f"Session saved to {_SESSION_FILE_}")
        return mm

    # Option 2: existing saved session
    if Path(_SESSION_FILE_).exists():
        print(f"Loading saved session from {_SESSION_FILE_}...")
        mm.load_session()
        return mm

    # Option 3: interactive login (email/password accounts only)
    print("No saved session found. Starting interactive login...")
    await mm.interactive_login()
    mm.save_session()
    print(f"Session saved to {_SESSION_FILE_}")
    return mm


async def run() -> None:
    mm = await get_mm()

    # Subscription details
    subs = await mm.get_subscription_details()
    print("Subscription:", subs)

    # Accounts
    accounts = await mm.get_accounts()
    with open("data.json", "w") as f:
        json.dump(accounts, f, indent=2)
    print(f"Accounts saved to data.json")

    # Institutions
    institutions = await mm.get_institutions()
    with open("institutions.json", "w") as f:
        json.dump(institutions, f, indent=2)

    # Budgets
    budgets = await mm.get_budgets()
    with open("budgets.json", "w") as f:
        json.dump(budgets, f, indent=4)
    print(f"Budgets saved to budgets.json")

    # Transactions summary
    transactions_summary = await mm.get_transactions_summary()
    with open("transactions_summary.json", "w") as f:
        json.dump(transactions_summary, f, indent=2)

    # Transaction categories
    categories = await mm.get_transaction_categories()
    with open("categories.json", "w") as f:
        json.dump(categories, f, indent=2)

    income_categories = {}
    expense_category_groups = {}

    for c in categories.get("categories", []):
        group_type = c.get("group", {}).get("type")
        group_name = c.get("group", {}).get("name")
        cat_name = c.get("name")
        if group_type == "income":
            print(f'income - {group_name} - {cat_name}')
            income_categories[cat_name] = 0
        elif group_type == "expense":
            print(f'expense - {group_name} - {cat_name}')
            expense_category_groups[group_name] = 0

    # Transactions
    transactions = await mm.get_transactions(limit=10)
    with open("transactions.json", "w") as f:
        json.dump(transactions, f, indent=2)
    print(f"Last 10 transactions saved to transactions.json")

    # Cashflow (current month)
    from datetime import date
    today = date.today()
    start = today.replace(day=1).isoformat()
    end = today.isoformat()
    cashflow = await mm.get_cashflow(start_date=start, end_date=end)
    with open("cashflow.json", "w") as f:
        json.dump(cashflow, f, indent=2)

    for c in cashflow.get("summary", []):
        s = c.get("summary", {})
        savings_rate = s.get("savingsRate") or 0
        print(
            f'Income: {s.get("sumIncome")}  '
            f'Expense: {s.get("sumExpense")}  '
            f'Savings: {s.get("savings")}  '
            f'({savings_rate:.0%})'
        )

    for c in cashflow.get("byCategory", []):
        cat = c.get("groupBy", {}).get("category", {})
        if cat.get("group", {}).get("type") == "income":
            name = cat.get("name")
            if name in income_categories:
                income_categories[name] += c.get("summary", {}).get("sum", 0)

    for c in cashflow.get("byCategoryGroup", []):
        cg = c.get("groupBy", {}).get("categoryGroup", {})
        if cg.get("type") == "expense":
            name = cg.get("name")
            if name in expense_category_groups:
                expense_category_groups[name] += c.get("summary", {}).get("sum", 0)

    print("\nIncome by category:", income_categories)
    print("\nExpense by group:", expense_category_groups)


asyncio.run(run())
