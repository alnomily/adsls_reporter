# Add more admin commands as needed...
# File: bot.py
# Refactored YemenNet bot — improved performance, reliability and compatibility
# with the updated DB schema (users_accounts has created_at, updated_at,
# confiscation_date, adsl_number, subscription_date, plan, expiry_date).
import asyncio
import logging
import re
import os
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

from zoneinfo import ZoneInfo

import psycopg2

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    BotCommand,
    BotCommandScopeChat,
    ReplyKeyboardMarkup,
    KeyboardButton,
)
from aiogram.utils.markdown import hcode, hbold
from aiogram.exceptions import TelegramBadRequest

from bot.report_image import ReportImageGenerator
from bot.table_report import TableReportGenerator

# Core imports and handler registration
from config import BOT_TOKEN, ADMIN_ID, ADMIN_IDS
from scraper.runner import fetch_users, fetch_single_user, save_account_data
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import httpx
from bot.app import bot, dp, EXEC, SCRAPE_SEMAPHORE, shutdown_executor
from bot.utils import set_freshness, is_stale, clean_text, utcnow, BotUtils
from bot.user_manager import UserManager
from bot.report_sender import collect_saved_user_reports, generate_images, send_images

# Register handler modules (they register on import) without rebinding the name `bot`
from bot.handlers import (
    main_menu,
    help_menu,
    admin_handlers,
    user_handlers,
    callbacks_handlers,
    reports_handlers,
    background_tasks as background_tasks_module,
    partners_handlers,      # ✅ أضف هذا
)
from bot.handlers.background_tasks import periodic_daily_report, cache_cleaner, periodic_all_users_refresh

from bot.cache import CacheManager, set_freshness
from bot.utils_shared import (
    run_blocking,
    save_scraped_account,
    user_exists,
    insert_user_account,
    delete_user_account,
    update_user_status,
    get_all_users_by_network_id,
    count_table,
    get_networks,
    insert_pending_request,
    get_pending_request,
    update_pending_status,
    get_active_users,
)

# Logging
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("YemenNetBot")

# Admin commands are in bot/handlers/admin_handlers.py (imported above)

# Callbacks and approval handlers are registered in `bot/handlers/callbacks_handlers.py`.
# Approval and refresh/live callbacks live there to avoid circular imports.


# Background tasks are implemented in `bot/handlers/background_tasks.py`.
# The module is imported above; schedule its tasks from main().

 
# Error handler

@dp.errors()
async def errors_handler(update: types.Update, exception: Exception | None = None, *args, **kwargs) -> bool:
    # Be permissive about the signature and accept extra args/kwargs that
    # different aiogram middleware versions may pass. Log full exception info
    # for debugging.
    try:
        logger.exception("Update %s caused error: %s", update, exception)
    except Exception:
        # Fallback logging if formatting fails for any reason
        logger.exception("Error in error handler", exc_info=True)
    return True

# Simple file log
def add_log(message: str, tag: str = None, path: str = None) -> None:
    log_path = path or os.path.join(os.path.dirname(__file__), "filelog.txt")
    try:
        try:
            tz = ZoneInfo("Asia/Aden")
        except Exception:
            tz = timezone.utc
        dt = datetime.now(tz)
        ts = dt.strftime("%Y/%m/%d %H:%M:%S.") + f"{dt.microsecond//1000:03d}"
        entry = f"{ts} - {tag} - {message}\n" if tag else f"{ts} - {message}\n"
        with open(log_path, "a", encoding="utf-8") as lf:
            lf.write(entry)
    except Exception:
        logger.debug("Failed to write file log", exc_info=True)

async def main() -> None:
    logger.info("Starting YemenNet Bot...")

    # test local postgres connectivity
    try:
        # quick connectivity check via count_table
        await count_table("users_accounts")
        logger.info("PostgreSQL connected ✅")
    except psycopg2.errors.UndefinedTable:
        logger.warning("PostgreSQL connected ✅ (schema not initialized yet: users_accounts missing)")
    except Exception as e:
        logger.exception("PostgreSQL connection failed ❌: %s", e)
        return

    # nicer command menu (use emojis for visual appeal) and optional startup message to admin
    async def _set_commands_with_retry():
        cmds = [
            BotCommand(command="start", description="🚀 بدء البوت"),
            BotCommand(command="networks", description="📡 الشبكات"),
            BotCommand(command="adsls", description="👥 خطوط النت"),
            BotCommand(command="reports", description="📄 التقارير"),
            BotCommand(command="account", description="🧾 ملخص حسابك"),
            BotCommand(command="settings", description="⚙️ إعدادات"),
            BotCommand(command="about", description="ℹ️ حول البوت"),
            BotCommand(command="help", description="❓ المساعدة"),
        ]

        # Shared persistent reply keyboard (big inner buttons like the screenshot)
        # Import and reuse MAIN_MENU_KB in your /start handler (or any entry point):
        # await message.answer("اختر خياراً:", reply_markup=MAIN_MENU_KB)
        globals()["MAIN_MENU_KB"] = ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text="🚀 بدء البوت")],
                [KeyboardButton(text="🧾 ملخص حسابك")],
                [KeyboardButton(text="📡 الشبكات"),KeyboardButton(text="👥 خطوط النت")],
                [KeyboardButton(text="📄 التقارير")],
                [KeyboardButton(text="⚙️ إعدادات")],
                [KeyboardButton(text="ℹ️ حول البوت"),KeyboardButton(text="❓ المساعدة")],
            ],
        )

        # Retry a few times with exponential backoff to tolerate transient Telegram outages
        attempt = 0
        delay = 2
        while attempt < 5:
            try:
                await bot.set_my_commands(cmds, request_timeout=60)
                # Set admin-only commands appended to normal menu for admin chats
                admin_cmds_extra = [
                    BotCommand(command="admin", description="🛠️ لوحة الإدارة"),
                ]
                admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
                for admin_id in admin_targets:
                    try:
                        # Admin sees normal + admin commands
                        await bot.set_my_commands(
                            cmds + admin_cmds_extra,
                            scope=BotCommandScopeChat(chat_id=admin_id),
                            request_timeout=60,
                        )
                        logger.info("Admin command menu set (normal + admin) for admin_id=%s ✔️", admin_id)
                    except Exception:
                        logger.debug("Failed to set admin commands for admin_id=%s", admin_id, exc_info=True)
                logger.info("Bot commands set ✔️")
                return True
            except Exception as e:
                attempt += 1
                if attempt >= 5:
                    logger.warning("Giving up setting bot commands after %d attempts: %s", attempt, e)
                    return False
                logger.warning("Failed to set bot commands (attempt %d): %s. Retrying in %ds...", attempt, e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 20)

    try:
        await _set_commands_with_retry()
    except Exception:
        logger.exception("Failed to set bot commands after retries")

    # send a formatted, more attractive command list to the admin on startup
    try:
        cmds = [
            ("/start", "🚀 بدء البوت"),
            ("/networks", "📡 إدارة الشبكات"),
            ("/adsls", "👥 إدارة خطوط النت"),
            ("/reports", "📄 التقارير"),
            ("/account", "🧾 ملخص حسابك"),
            ("/settings", "⚙️ إعدادات"),
            ("/about", "ℹ️ حول البوت"),
            ("/help", "❓ المساعدة"),
            ("/admin", "🛠️ لوحة الإدارة"),
        ]
        lines = [f"<b>{k}</b> — {v}" for k, v in cmds]
        admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
        for admin_id in admin_targets:
            try:
                await bot.send_message(admin_id, "<b>قائمة الأوامر</b>\n\n" + "\n".join(lines), parse_mode="HTML")
            except Exception:
                logger.debug("Couldn't send startup command list to admin_id=%s", admin_id, exc_info=True)
    except Exception:
        logger.debug("Couldn't send startup command list to admins", exc_info=True)

    # asyncio.create_task(periodic_sync())
    asyncio.create_task(periodic_daily_report())
    asyncio.create_task(cache_cleaner())
    asyncio.create_task(periodic_all_users_refresh())
    # asyncio.create_task(periodic_send_image())

    try:
        bot_info = await bot.get_me()
        logger.info("Bot online: @%s", bot_info.username)
    except Exception:
        logger.warning("Bot get_me failed (network unstable). Continuing to polling...")

    try:
        add_log('start')
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        shutdown_executor(wait=False)

if __name__ == "__main__":
    add_log('start')
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    except Exception as fatal:
        logger.exception("Fatal error: %s", fatal)
