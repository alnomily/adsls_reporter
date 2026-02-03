import sys
import os
# Ensure project root is on sys.path so 'bot' package can be imported when run as a script
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from bot.table_report import TableReportGenerator

g = TableReportGenerator()
# Synthetic data
data = [
    ("12345", {
        "plan": "Basic",
        "account_status": "active",
        "yesterday_balance": "10",
        "today_balance": "5",
        "usage": "5",
        "expiry_date": "01/01/2030",
        "remaining_days": "100",
        "balance_value": "10.00",
        "consumption_value": "5.00",
        "notes": "ok"
    })
]
paths = g.generate_financial_table_report(data)
print('Generated:', paths)
for p in paths:
    try:
        print(p, 'size=', os.path.getsize(p))
    except Exception as e:
        print('Error getting size for', p, e)
