import asyncio
import logging
import os
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict

from zoneinfo import ZoneInfo

from bot.chat_user_manager import chat_user_manager
from bot.app import SCRAPE_SEMAPHORE
from bot.cache import CacheManager
# run_blocking and save_scraped_account imported lazily inside functions to avoid circular imports
from bot.user_manager import UserManager
from bot.report_sender import collect_saved_user_reports, generate_images, send_images
from bot.app import bot
from bot.utils_shared import run_blocking, save_scraped_account, get_all_users
from bot.selected_network_manager import SelectedNetwork, selected_network_manager

# from config import SECONDARY_ADMIN
logger = logging.getLogger(__name__)

_all_users_refresh_lock = asyncio.Lock()


async def _retry_async(op, *, attempts: int = 3, base_delay: float = 2.0, max_delay: float = 20.0, task_name: str = "async operation"):
    """Simple exponential backoff helper for async callables."""
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return await op()
        except Exception as e:  # noqa: PERF203 we want to log and retry
            last_exc = e
            if attempt >= attempts:
                logger.warning("%s failed after %d attempts: %s", task_name, attempt, e)
                break
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            delay += random.uniform(0, base_delay)
            logger.warning(
                "%s attempt %d/%d failed: %s. Retrying in %.1fs",
                task_name,
                attempt,
                attempts,
                e,
                delay,
            )
            await asyncio.sleep(delay)
    if last_exc:
        raise last_exc
    return None


async def periodic_sync() -> None:
    await asyncio.sleep(5)
    while True:
        try:
            logger.info("Running auto sync...")
            await _retry_async(
                lambda: run_blocking(lambda: __import__("scraper.runner").fetch_users()),
                attempts=3,
                base_delay=2.0,
                max_delay=20.0,
                task_name="auto sync fetch_users",
            )
            CacheManager.clear()
            logger.info("Auto sync done.")
        except Exception as e:
            logger.exception("Error in auto sync: %s", e)
        await asyncio.sleep(12 * 3600)


async def periodic_all_users_refresh() -> None:
    await asyncio.sleep(5)
    interval = max(10, int(os.getenv("ALL_USERS_REFRESH_INTERVAL", "3600")))
    sem_users = asyncio.Semaphore(24)

    async def _run_refresh() -> None:
        try:
            logger.info("Starting periodic refresh of all users...")
            resp = await _retry_async(
                lambda: get_all_users(),
                attempts=3,
                base_delay=2.0,
                max_delay=20.0,
                task_name="get_all_users",
            )
            all_users = getattr(resp, "data", None) or []
            if not all_users:
                logger.warning("No users found for periodic refresh")
                return

            async def fetch_and_save_user(user: Dict[str, Any]) -> bool:
                async with sem_users:
                    username = user.get("username")
                    network_id = user.get("network_id", "")
                    try:
                        await _retry_async(
                            lambda: asyncio.wait_for(save_scraped_account(username, network_id), timeout=30),
                            attempts=3,
                            base_delay=2.0,
                            max_delay=15.0,
                            task_name=f"save_scraped_account {username}",
                        )
                        return True
                    except asyncio.TimeoutError:
                        logger.warning("⏰ Timeout fetching data for %s", username)
                        return False
                    except Exception as e:
                        logger.warning("❌ Failed to fetch data for %s: %s", username, e)
                        return False

            tasks = [asyncio.create_task(fetch_and_save_user(user)) for user in all_users]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for result in results if result is True)
            logger.info("Periodic refresh done: %d/%d users updated", success_count, len(all_users))
        except Exception as e:
            logger.exception("Error in periodic_all_users_refresh: %s", e)

    while True:
        start = asyncio.get_running_loop().time()
        if _all_users_refresh_lock.locked():
            logger.info("Periodic refresh already running; skipping this tick")
        else:
            async with _all_users_refresh_lock:
                logger.info("Acquired lock for periodic refresh")
                await _run_refresh()
        elapsed = asyncio.get_running_loop().time() - start
        await asyncio.sleep(max(0.0, interval - elapsed))


async def periodic_daily_report() -> None:
    await asyncio.sleep(5)
    try:
        tz = ZoneInfo("Asia/Aden")
    except Exception:
        tz = timezone.utc

    sem_tokens = asyncio.Semaphore(6)
    sem_users = asyncio.Semaphore(24)

    async def fetch_all_users_data() -> bool:
        try:
            logger.info("Starting Phase 1: Fetching all user data from YemenNet...")
            resp = await _retry_async(
                lambda: get_all_users(),
                attempts=3,
                base_delay=2.0,
                max_delay=20.0,
                task_name="get_all_users",
            )
            all_users = getattr(resp, "data", None) or []
            if not all_users:
                logger.warning("No users found for data fetching")
                return False

            async def fetch_and_save_user(user: Dict[str, Any]) -> bool:
                async with sem_users:
                    username = user.get("username")
                    network_id = user.get("network_id", "")
                    try:
                        # async with SCRAPE_SEMAPHORE:
                        try:
                            await _retry_async(
                                lambda: asyncio.wait_for(save_scraped_account(username, network_id), timeout=30),
                                attempts=3,
                                base_delay=2.0,
                                max_delay=15.0,
                                task_name=f"save_scraped_account {username}",
                            )
                            logger.debug("✅ Successfully fetched data for %s", username)
                            return True
                        except asyncio.TimeoutError:
                            logger.warning("⏰ Timeout fetching data for %s", username)
                            return False
                        except Exception as e:
                            logger.warning("❌ Failed to fetch data for %s: %s", username, e)
                            return False
                    except Exception as e:
                        logger.warning("❌ Error in fetch_and_save_user for %s: %s", username, e)
                        return False

            tasks = [asyncio.create_task(fetch_and_save_user(user)) for user in all_users]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(1 for result in results if result is True)
            total_count = len(all_users)
            logger.info("Phase 1 completed: %d/%d users successfully fetched", success_count, total_count)
            return success_count > 0
        except Exception as e:
            logger.exception("Error in fetch_all_users_data: %s", e)
            return False

    def _time_allowed_for_network(times_field: Any, scheduled_hour: int, scheduled_minute: int, scheduled_second: int) -> bool:
        """
        Windowed schedule check: allow when an entry is within ±60 seconds of the scheduled time.
        If times_field is falsy (None/empty), treat as "no restriction" -> allow.
        Accepted input formats are normalized to HH:MM:SS:
            - list/tuple/set of strings (e.g., "06", "06:00", "06:00:00")
            - comma-separated string (e.g., "06:00,18:00:00")
            - integers representing hours (e.g., 6)
            - list/tuple entries like (6,), (6, 0), (6, 0, 0)
        """
        if not times_field:
            return True

        def _normalize_item_to_full(item: Any) -> str | None:
            try:
                # tuple/list like (h,), (h,m), (h,m,s)
                if isinstance(item, (list, tuple)):
                    if len(item) == 0:
                        return None
                    h = int(item[0])
                    m = int(item[1]) if len(item) >= 2 else 0
                    s = int(item[2]) if len(item) >= 3 else 0
                    return f"{h:02d}:{m:02d}:{s:02d}"
                # integer hour
                if isinstance(item, int):
                    return f"{item:02d}:00:00"
                # string: may be comma-separated or single time
                s = str(item).strip()
                if not s:
                    return None
                parts = s.split(":")
                if len(parts) == 1:  # HH
                    h = int(parts[0])
                    return f"{h:02d}:00:00"
                if len(parts) == 2:  # HH:MM
                    h = int(parts[0])
                    m = int(parts[1])
                    return f"{h:02d}:{m:02d}:00"
                # HH:MM:SS (or longer, ignore extras)
                h = int(parts[0])
                m = int(parts[1])
                ssec = int(parts[2])
                return f"{h:02d}:{m:02d}:{ssec:02d}"
            except Exception:
                return None

        # Build normalized set of allowed times (HH:MM:SS)
        normalized: set[str] = set()
        if isinstance(times_field, str):
            raw_items = [i.strip() for i in times_field.split(",") if i.strip()]
        elif isinstance(times_field, (list, tuple, set)):
            raw_items = list(times_field)
        else:
            raw_items = [times_field]

        for it in raw_items:
            norm = _normalize_item_to_full(it)
            if norm:
                normalized.add(norm)

        # Convert times to seconds-of-day and allow a small tolerance window
        scheduled_sod = scheduled_hour * 3600 + scheduled_minute * 60 + scheduled_second
        logger.debug("Normalized schedule entries: %s", sorted(normalized))
        for t in normalized:
            try:
                h, m, s = [int(x) for x in t.split(":", 2)]
                target_sod = h * 3600 + m * 60 + s
                if abs(target_sod - scheduled_sod) <= 60:
                    return True
            except Exception:
                continue
        return False

    async def process_token(token: str, scheduled_hour: int, scheduled_minute: int, scheduled_second: int) -> None:
        # Get all networks for this user/token
        chat_user = await chat_user_manager.get(token)
        if not chat_user:
            logger.warning("No chat user found for token %s", token)
            return
        user_networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
        if not user_networks:
            logger.warning("No networks found for user %s", token)
            return
        def fix_network_dict(network: Dict[str, Any]) -> SelectedNetwork:
            return SelectedNetwork(
                id = network.get("id"),
                network_id=network.get("network_id"),
                network_name=network.get("network_name"),
                user_name=network.get("user_name"),
                # Use the correct DB field name (times_to_send_reports)
                times_to_send_reports=network.get("times_to_send_reports", 15),
                warning_count_remaining_days=network.get("warning_count_remaining_days", 7),
                danger_count_remaining_days=network.get("danger_count_remaining_days", 3),
                warning_percentage_remaining_balance=network.get("warning_percentage_remaining_balance", 30),
                danger_percentage_remaining_balance=network.get("danger_percentage_remaining_balance", 10),
                is_active=network.get("is_network_active", False),
                expiration_date=network.get("expiration_date", None),
                telegram_id=network.get("telegram_id"),
                chat_user_id=network.get("chat_user_id"),
                permissions=network.get("permissions", ""),
                network_type=network.get("network_type", ""),
            )
        async def process_network(network):
            network = fix_network_dict(network)
            # Skip expired networks based on expiration_date (compare dates in Asia/Aden)
            try:
                exp_raw = network.expiration_date
                if exp_raw:
                    exp_date = None
                    if isinstance(exp_raw, str):
                        try:
                            exp_date = datetime.fromisoformat(exp_raw).date()
                        except Exception:
                            try:
                                d, m, y = [int(x) for x in exp_raw.replace("/", "-").split("-")]
                                exp_date = datetime(y, m, d).date()
                            except Exception:
                                exp_date = None

                    if exp_date is not None:
                        today_local = datetime.now(tz).date()
                        if exp_date < today_local:
                            logger.info("⏭ Skipping network %s (%s) - expired on %s", network.network_name, token, exp_date.isoformat())
                            return
            except Exception:
                # Do not block processing on parse errors; proceed as if not expired
                pass
            if not network.is_active:
                logger.info("⏭ Skipping network %s (%s) - marked inactive", network.network_name, token)
                return
            times_field = SelectedNetwork.from_bitmask_to_times_list(network.times_to_send_reports)
            logger.info("Network %s times_to_send_reports: %s", network.network_name, times_field)
            if times_field:
                allowed = _time_allowed_for_network(times_field, scheduled_hour, scheduled_minute, scheduled_second)
                logger.info("Network %s (%s) allowed to send at %02d:%02d:%02d: %s", network.network_name, token, scheduled_hour, scheduled_minute, scheduled_second, allowed)
                if not allowed:
                    logger.info("⏭ Skipping network %s (%s) for scheduled time %02d:%02d:%02d due to times_to_send setting",
                                network.network_name, token, scheduled_hour, scheduled_minute, scheduled_second)
                    return

            async with sem_tokens:
                try:
                    users = await UserManager.get_users_by_network(network.network_id)
                    if not users:
                        return

                    user_reports = await collect_saved_user_reports(users, sem_users, UserManager,chat_user.order_by)
                    if not user_reports:
                        logger.info("📭 No data available for network %s", network.network_name)
                        return

                    logger.info("📄 Generating report for network %s with %d users", network.network_name, len(user_reports))
                    try:
                        images, cleanup_dir = await run_blocking(lambda: generate_images(user_reports, network, chat_user))
                    except Exception:
                        logger.exception("❌ Report generation failed for network %s", network.network_name)
                        return

                    try:
                        result = await send_images(
                            bot,
                            network,
                            token,
                            images,
                            user_reports,
                            tz,
                            cleanup_dir,
                            True,
                            True,
                            (scheduled_hour, scheduled_minute, scheduled_second),
                        )
                        logger.info("Report send result for network %s: %s", network.network_name, result)
                    except Exception:
                        logger.exception("❌ Failed to send images for network %s", network.network_name)

                except Exception:
                    logger.exception("❌ process_network error for network %s", network.network_name)

        # Run all networks for this user concurrently
        await asyncio.gather(*(process_network(network) for network in user_networks), return_exceptions=True)

    # Times to send reports each day (hour, minute, second)
    report_times = [
        (6, 0, 0),
        (12, 0, 0),
        (18, 0, 0),
        (23, 50, 0),
    ]

    while True:
        now = datetime.now(tz)
        # Find the next scheduled time
        next_times = []
        for hour, minute, second in report_times:
            target = now.replace(hour=hour, minute=minute, second=second, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            next_times.append(target)
        next_target = min(next_times)
        wait_seconds = (next_target - now).total_seconds()
        logger.info("🕒 Next daily report scheduled at %s (in %.0f seconds)", next_target.strftime("%Y-%m-%d %H:%M:%S"), wait_seconds)
        await asyncio.sleep(wait_seconds)

        try:
            logger.info("🚀 Starting daily report process...")
            fetch_success = await fetch_all_users_data()
            if not fetch_success:
                logger.warning("⚠️ Data fetching phase failed, but continuing with available data")

            await asyncio.sleep(5)
            logger.info("📖 Starting Phase 2: Reading saved data and generating reports...")

            # determine scheduled time that just fired
            scheduled_hour = next_target.hour
            scheduled_minute = next_target.minute
            scheduled_second = next_target.second

            tokens = await UserManager.get_all_tokens()
            logger.info("Fetched %d networks for daily report processing", len(tokens))
            logger.info("Networks: %s", tokens)

            if not tokens:
                logger.info("📭 No networks found for daily report")
                continue

            logger.info("👥 Processing %d networks for report generation", len(tokens))
            tasks = [asyncio.create_task(process_token(net, scheduled_hour, scheduled_minute, scheduled_second)) for net in tokens]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("✅ Daily report process completed")

        except Exception as e:
            logger.exception("❌ Error in periodic_daily_report: %s", e)

async def cache_cleaner() -> None:
    """Periodically clear in-memory cache to avoid stale data accumulation."""
    # initial short delay so startup can finish
    await asyncio.sleep(5)
    while True:
        try:
            logger.info("🧹 Running cache cleaner: clearing in-memory cache")
            CacheManager.clear()
        except Exception as e:
            logger.exception("Error in cache_cleaner: %s", e)
        # run every hour
        await asyncio.sleep(60 * 60)
