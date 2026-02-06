import os
import sys

# Ensure repo root is on sys.path when executed as a script from tools/
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from bot.local_postgres import fetch_all


def main() -> None:
    tables = [
        "networks",
        "chats_networks",
        "chats_users",
        "users_accounts",
        "pending_requests",
        "account_data",
        "adsl_daily_reports",
        "logs",
    ]

    for t in tables:
        try:
            obj = fetch_all(
                """
                SELECT table_schema, table_name, table_type
                FROM information_schema.tables
                WHERE table_name = %s
                ORDER BY table_schema
                """.strip(),
                [t],
            )
            schemas = [f"{r['table_schema']}({r['table_type']})" for r in obj]

            rows = fetch_all(
                """
                SELECT table_schema, column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY table_schema, ordinal_position
                """.strip(),
                [t],
            )
            cols = [f"{r['table_schema']}.{r['column_name']}" for r in rows]
            print(t, {"schemas": schemas, "columns": cols})
        except Exception as e:
            print("ERR", t, e)


if __name__ == "__main__":
    main()
