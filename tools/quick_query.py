from __future__ import annotations

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from bot.local_postgres import fetch_one


if __name__ == "__main__":
    user_id = sys.argv[1] if len(sys.argv) > 1 else None
    if not user_id:
        raise SystemExit("Usage: python tools/quick_query.py <user_id>")

    row = fetch_one(
        "SELECT user_id, report_date, is_active FROM adsl_daily_reports WHERE user_id = %s ORDER BY report_date DESC LIMIT 1",
        [user_id],
    )
    print(row)
