import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

from aiogram import types
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

from bot.app import dp, bot, EXEC, SCRAPE_SEMAPHORE
from bot.cache import CacheManager
from bot.user_manager import UserManager
from bot.utils_shared import save_scraped_account, run_blocking
from bot.report_sender import collect_saved_user_reports, generate_images, send_images
from bot.table_report import TableReportGenerator
from bot.report_image import ReportImageGenerator
from bot.user_report import AccountData, UserReport
from bot.utils import BotUtils
from bot.utils import block_if_active_flow

logger = logging.getLogger(__name__)


# @dp.message(Command("sendreport"))
# async def send_report_command(message: types.Message):
#     if not BotUtils.is_admin(message.from_user.id):
#         await message.answer("⛔ هذا الأمر متاح للمشرف فقط.")
#         return

#     await message.answer("⏳ يتم الآن إنشاء التقرير...")

#     try:
#         from bot.report_generator import ReportGenerator
#         generator = ReportGenerator()
#         report_path = generator.build_and_export()

#         if report_path:
#             await bot.send_photo(
#                 chat_id=message.chat.id,
#                 photo=types.FSInputFile(report_path),
#                 caption="✅ تقرير الاستهلاك اليومي"
#             )
#         else:
#             await message.answer("⚠️ لا توجد بيانات لإعداد التقرير.")
#     except Exception as e:
#         await message.answer("❌ فشل إنشاء التقرير")
#         logger.exception("sendreport error: %s", e)


# @dp.message(Command("allsummary"))
# async def allsummary_command(message: types.Message):
#     processing_msg = await message.answer("📊 جاري تجهيز التقرير الشامل...")
#     try:
#         # Get all users data (may be a heavy operation)
#         all_users_data = await UserManager.get_all_users_reports_concurrent(64)
#         if not all_users_data:
#             await processing_msg.edit_text("📭 لا توجد بيانات للمستخدمين.")
#             return

#         table_generator = TableReportGenerator()
#         image_paths = table_generator.generate_financial_table_report(all_users_data)

#         if not image_paths:
#             await processing_msg.edit_text("❌ فشل في إنشاء التقرير.")
#             return

#         if len(image_paths) == 1:
#             photo = types.FSInputFile(image_paths[0])
#             await message.answer_photo(photo, caption=f"📋 التقرير الشامل للمستخدمين\n👥 عدد المستخدمين: {len(all_users_data)}\n📄 الصفحة: 1/1\n🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#         else:
#             for i, image_path in enumerate(image_paths, 1):
#                 photo = types.FSInputFile(image_path)
#                 await message.answer_photo(photo, caption=f"📋 التقرير الشامل للمستخدمين\n👥 عدد المستخدمين: {len(all_users_data)}\n📄 الصفحة: {i}/{len(image_paths)}\n🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
#                 if i < len(image_paths):
#                     await asyncio.sleep(1)

#         await processing_msg.delete()
#         for image_path in image_paths:
#             try:
#                 os.remove(image_path)
#             except Exception:
#                 pass

#     except Exception as e:
#         logger.exception("Error generating all users summary: %s", e)
#         await processing_msg.edit_text("❌ حدث خطأ في إنشاء التقرير الشامل.\n🔧 يرجى المحاولة مرة أخرى لاحقاً.")


@dp.message(Command("image"))
async def image_command(message: types.Message, command: CommandObject, state: FSMContext):
    if await block_if_active_flow(message, state):
        return
    if not command.args:
        await message.answer("❗ Please provide a username: /image <username>")
        return

    username = command.args.strip()
    processing_msg = await message.answer(f"🖼 Generating image report for {username}...")

    try:
        user = await UserManager.get_user_data(username)
        if not user:
            await processing_msg.edit_text(f"❌ User {username} not found in database.")
            return

        account_data = await UserManager.get_latest_account_data(user['id'])
        if not account_data:
            await processing_msg.edit_text(f"ℹ️ No data found for {username}.")
            return

        account = AccountData(
            username=username,
            account_type=account_data.get('account_type'),
            status=account_data.get('status'),
            expiry_date=account_data.get('expiry_date'),
            remaining_days=account_data.get('remaining_days'),
            package=account_data.get('package'),
            balance=account_data.get('balance'),
            available_balance=account_data.get('available_balance'),
            subscription_date=account_data.get('subscription_date'),
            plan=account_data.get('plan'),
            scraped_at=account_data.get('scraped_at')
        )

        report = UserReport(
            account=account,
            requested_by=message.from_user.full_name,
            fetched_at=BotUtils.utcnow(),
            is_fresh=not BotUtils.is_stale(account_data.get('scraped_at', ''))
        )

        generator = ReportImageGenerator()
        image_path = generator.generate_user_report_image(report)

        photo = types.FSInputFile(image_path)
        await message.answer_photo(photo, caption=f"📊 Image Report for {username}\n🕒 Generated at {report.fetched_at.strftime('%Y-%m-%d %H:%M:%S UTC')}")

        await processing_msg.delete()
        try:
            os.remove(image_path)
        except Exception:
            pass

    except Exception as e:
        logger.exception("Error generating image report for %s: %s", username, e)
        await processing_msg.edit_text(f"❌ Error generating image report for {username}.\n🔧 Please try again later.")


# @dp.message(Command("mysummary"))
# async def mysummary_command(message: types.Message, command: Optional[CommandObject] = None):
#     token_id = str(message.chat.id)
#     logger.info("mysummary requested by token=%s user=%s", token_id, message.from_user.id)

#     waiting = await message.answer("📊 Generating your summary report...")

#     try:
#         users = await UserManager.get_users_by_token(token_id)
#         if not users:
#             await waiting.edit_text("⚠️ You don't have any accounts yet. Add with /addusers")
#             return

#         reports = []

#         async def fetch_and_collect(u: dict) -> None:
#             try:
#                 async with SCRAPE_SEMAPHORE:
#                     try:
#                         await asyncio.wait_for(save_scraped_account(u["username"], token_id), timeout=30)
#                     except asyncio.TimeoutError:
#                         logger.warning("Timeout while saving scraped account %s", u.get("username"))
#                     except Exception:
#                         logger.debug("Failed to fetch/save live for %s", u.get("username"), exc_info=True)

#                     latest = await UserManager.get_latest_account_data(u["id"])
#                     if latest:
#                         reports.append((u["username"], latest))
#             except Exception:
#                 logger.exception("Error in fetch_and_collect for %s", u.get("username"))

#         tasks = [asyncio.create_task(fetch_and_collect(u)) for u in users]
#         await asyncio.gather(*tasks, return_exceptions=True)

#         if not reports:
#             await waiting.edit_text("⚠️ No data available right now.")
#             return

#         # Use the current running loop to run blocking image generation in the executor
#         loop = __import__('asyncio').get_running_loop()
#         image_paths = await loop.run_in_executor(EXEC, lambda: TableReportGenerator().generate_financial_table_report(reports))

#         await waiting.delete()
#         for i, img in enumerate(image_paths, 1):
#             try:
#                 await bot.send_photo(chat_id=int(token_id), photo=types.FSInputFile(img), caption=f"📄 Page {i}/{len(image_paths)}")
#             except Exception:
#                 logger.exception("Failed to send page %d for mysummary to %s", i, token_id)
#             finally:
#                 try:
#                     os.remove(img)
#                 except Exception:
#                     pass

#     except Exception as e:
#         logger.exception("mysummary command error: %s", e)
#         try:
#             await waiting.edit_text("❌ Error generating your summary. Please try again later.")
#         except Exception:
#             pass
