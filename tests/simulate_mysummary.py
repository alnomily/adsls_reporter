import asyncio
import os
import sys
from datetime import datetime

# Ensure project root is on sys.path so `bot` package imports resolve when running this script
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Mocks and test harness for /mysummary flow
from bot.table_report import TableReportGenerator
from bot.app import EXEC, SCRAPE_SEMAPHORE

# We'll monkeypatch functions in bot.user_manager and bot.utils_shared
import bot.user_manager as user_manager
import bot.utils_shared as utils_shared


async def fake_save_scraped_account(username: str, token_id: str) -> bool:
    # Simulate a successful scrape (fast)
    await asyncio.sleep(0.01)
    return True


async def fake_get_users_by_token(token_id: str):
    # Return a set of fake users
    return [
        {"id": "1", "username": "111"},
        {"id": "2", "username": "222"},
        {"id": "3", "username": "333"},
    ]


async def fake_get_latest_account_data(user_id: str):
    # Return a minimal account data dictionary expected by the TableReportGenerator
    return {
        "usage": "5.0",
        "yesterday_balance": "10",
        "today_balance": "15",
        "plan": "Basic",
        "account_status": "Active",
        "expiry_date": "01/12/2025",
        "remaining_days": "10",
        "balance_value": "10.00",
        "consumption_value": "5.00",
        "notes": "ok",
    }


async def run_sim(token_id: str = "12345"):
    # Patch the functions
    user_manager.UserManager.get_users_by_network = staticmethod(fake_get_users_by_token)
    utils_shared.save_scraped_account = fake_save_scraped_account
    user_manager.UserManager.get_latest_account_data = staticmethod(fake_get_latest_account_data)

    users = await user_manager.UserManager.get_users_by_network(token_id)
    print("fetched users:", users)

    reports = []

    async def fetch_and_collect(u: dict) -> None:
        try:
            async with SCRAPE_SEMAPHORE:
                try:
                    await utils_shared.save_scraped_account(u["username"], token_id)
                except Exception as e:
                    print("save_scraped_account failed for", u.get('username'), e)

                latest = await user_manager.UserManager.get_latest_account_data(u["id"])
                if latest:
                    reports.append((u["username"], latest))
        except Exception as e:
            print("fetch_and_collect exception for", u.get('username'), e)

    tasks = [asyncio.create_task(fetch_and_collect(u)) for u in users]
    await asyncio.gather(*tasks, return_exceptions=True)

    print("collected reports:", reports)

    # Generate images in executor (match handler behavior)
    loop = asyncio.get_running_loop()
    image_paths = await loop.run_in_executor(EXEC, lambda: TableReportGenerator().generate_financial_table_report(reports, save_path='reports/sim_mysummary.png'))

    print("generated images:")
    for p in image_paths:
        try:
            size = os.path.getsize(p)
        except Exception:
            size = 0
        print(p, size)

    # Clean up generated files
    for p in image_paths:
        try:
            os.remove(p)
        except Exception:
            pass


if __name__ == '__main__':
    asyncio.run(run_sim())
