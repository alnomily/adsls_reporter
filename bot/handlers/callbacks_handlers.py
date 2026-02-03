import logging

from aiogram import types
from aiogram.filters import CommandObject

from bot.app import dp, bot
from bot.user_manager import UserManager
from bot.utils_shared import run_blocking, get_pending_request, update_pending_status

logger = logging.getLogger(__name__)


@dp.callback_query(lambda c: c.data and c.data.startswith("approve_"))
async def approve_request_callback(callback: types.CallbackQuery):
    req_id = callback.data.replace("approve_", "")

    try:
        # Use the normalized helper which returns a dict with chat_id/text
        from bot.utils_shared import get_request_by_id, update_pending_status

        request = await get_request_by_id(req_id)
        if not request:
            await callback.answer("❌ الطلب غير موجود أو تمت معالجته بالفعل.")
            return

        token_id = request.get("chat_id") or request.get("token_id")
        request_text = request.get("text") or request.get("request_text") or ""

        pairs = request_text.split()
        successes = []
        skipped = []
        failures = []

        for pair in pairs:
            parts = pair.split(":")
            if len(parts) < 2:
                continue
            adsl_number, username = parts[0], parts[1]
            password = '123456'
            try:
                await UserManager.insert_user(username, password, token_id, adsl_number)
                successes.append(username)
            except Exception as exc:
                msg = str(exc)
                # Treat duplicate-username DB errors as "already exists" (skip)
                if 'duplicate key value' in msg or '23505' in msg or 'already exists' in msg:
                    skipped.append((username, msg))
                    logger.info("Skipping existing user %s: %s", username, msg)
                    continue
                failures.append((username, msg))
                logger.exception("Failed inserting user %s from request %s", username, req_id)

        # mark pending as approved
        try:
            await update_pending_status(req_id, "approved")
        except Exception:
            logger.exception("Failed to update pending request %s status", req_id)

        # Build a human-readable summary for the admin message
        summary_lines = ["✅ Request processed."]
        if successes:
            summary_lines.append(f"تمت الإضافة: {', '.join(successes)}")
        if skipped:
            summary_lines.append(f"تخطّي (موجود مسبقاً): {', '.join(s for s, _ in skipped)}")
        if failures:
            summary_lines.append(f"فشل: {', '.join(s for s, _ in failures)}")

        summary_text = "\n".join(summary_lines)

        try:
            if callback.message:
                try:
                    await callback.message.edit_text((callback.message.text or "") + "\n\n" + summary_text, reply_markup=None)
                except Exception:
                    await callback.message.edit_reply_markup(None)
        except Exception:
            logger.debug("Failed to update admin message after processing request", exc_info=True)

        try:
            await bot.send_message(token_id, "✅ Your add users request was approved.")
        except Exception:
            logger.debug("Failed to notify token %s after approval", token_id, exc_info=True)

    except Exception as e:
        logger.exception(f"Approve error: {e}")
        await callback.answer("❌ Error approving request")


@dp.callback_query(lambda c: c.data and c.data.startswith("reject_"))
async def reject_request_callback(callback: types.CallbackQuery):
    req_id = callback.data.replace("reject_", "")

    try:
        await update_pending_status(req_id, "rejected")
        await callback.message.edit_text("❌ Request rejected")
        await callback.answer("Rejected")
    except Exception as e:
        logger.error(f"Reject error: {e}")
        await callback.answer("❌ Error rejecting")


@dp.callback_query(lambda c: c.data and c.data.startswith("refresh_"))
async def refresh_callback(callback: types.CallbackQuery) -> None:
    username = callback.data.replace("refresh_", "")
    await callback.answer(f"🔄 تحديث {username}...")
    fake_msg = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=callback.message.date,
        text=f"/checklive {username}"
    )
    # Call the checklive handler by dispatching a fake message through the same handler
    # to avoid import-time circular dependencies.
    await dp.feed_update(fake_msg)


@dp.callback_query(lambda c: c.data and c.data.startswith("live_"))
async def live_callback(callback: types.CallbackQuery) -> None:
    username = callback.data.replace("live_", "")
    await callback.answer(f"📡 فحص مباشر لـ {username}")
    fake_msg = types.Message(
        message_id=callback.message.message_id,
        from_user=callback.from_user,
        chat=callback.message.chat,
        date=callback.message.date,
        text=f"/checklive {username}"
    )
    await dp.feed_update(fake_msg)
