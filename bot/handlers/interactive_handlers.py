import logging

from aiogram import types
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

from bot.app import dp, bot
from bot.utils import block_if_active_flow
from bot.utils_shared import insert_pending_request
from bot.selected_network_manager import selected_network_manager
from config import ADMIN_ID, ADMIN_IDS


logger = logging.getLogger(__name__)

ADDUSERS_SESSIONS: dict[int, dict] = {}

def get_action_keyboard(chat_id, step="confirm"):
    if step == "confirm":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="➕ إضافة آخر", callback_data=f"addusers_action:{chat_id}:add"),
                InlineKeyboardButton(text="✅ تم / إرسال", callback_data=f"addusers_action:{chat_id}:done"),
                InlineKeyboardButton(text="❌ إلغاء", callback_data=f"addusers_action:{chat_id}:cancel")
            ]
        ])
    elif step == "cancel":
        return InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="❌ إلغاء", callback_data=f"addusers_action:{chat_id}:cancel")
            ]
        ])
    return None

@dp.message(Command("addusers"))
async def add_users_request_command(message: types.Message, command: CommandObject, state: FSMContext):
    if await block_if_active_flow(message, state):
        return
    token_id = str(message.chat.id)
    network = await selected_network_manager.get(token_id)
    if not network:
        await message.answer(" لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
        return  
    # If user provided args inline, keep the original quick request behavior
    if command.args:
        request_text = command.args.strip()
        try:
            result = await insert_pending_request(network.network_id, request_text)
            data = getattr(result, "data", None)
            await message.answer("✅ تم إرسال طلبك وهو قيد الموافقة.")

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ موافقة", callback_data=f"approve_{data[0].get('id') if data else ''}"),
                    InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{data[0].get('id') if data else ''}")
                ]
            ])
            admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
            for admin_id in admin_targets:
                try:
                    await bot.send_message(
                        admin_id,
                        f"📩 طلب إضافة خط نت جديد:\n👤 من المستخدم: {token_id}\nللشبكة: {network.network_name}\n📝 الطلب: {request_text}",
                        reply_markup=keyboard
                    )
                except Exception:
                    logger.exception("Failed to notify admin %s about addusers request", admin_id)
        except Exception as e:
            logger.error(f"Error saving addusers request: {e}")
            await message.answer("❌ فشل إرسال الطلب. يرجى المحاولة لاحقاً.")
        return

    # Start interactive flow (no args)
    ADDUSERS_SESSIONS[message.chat.id] = {
        "step": "username",
        "entries": [],
        "current": {}
    }

    await message.answer(
        "📝 إضافة خطوط نت\n"
        "يرجى إرسال اسم المستخدم للحساب الأول.",
        reply_markup=get_action_keyboard(message.chat.id, step="cancel")
    )

@dp.callback_query(lambda c: c.data and c.data.startswith("addusers_action:"))
async def addusers_action_callback(callback: types.CallbackQuery):
    try:
        _, chat_id_str, action = callback.data.split(":", 2)
        chat_id = int(chat_id_str)
    except Exception:
        await callback.answer("إجراء غير صالح.")
        return

    if callback.from_user.id != chat_id:
        await callback.answer("هذا الزر مخصص لصاحب الجلسة فقط.", show_alert=True)
        return

    state = ADDUSERS_SESSIONS.get(chat_id)
    if not state:
        await callback.answer("لا توجد عملية نشطة.", show_alert=True)
        return

    if action == "add":
        state["step"] = "username"
        state["current"] = {}
        await bot.send_message(chat_id, "يرجى إرسال اسم المستخدم التالي.", reply_markup=get_action_keyboard(chat_id, step="cancel"))
        await callback.answer()
        return

    if action == "cancel":
        ADDUSERS_SESSIONS.pop(chat_id, None)
        try:
            await callback.message.edit_text(callback.message.text + "\n\n❌ تم إلغاء العملية.", reply_markup=None)
        except Exception:
            pass
        await callback.answer("تم إلغاء العملية.")
        return

    if action == "done":
        entries = state.get("entries", [])
        if not entries:
            await callback.answer("⚠️ لا توجد مدخلات لتقديمها.", show_alert=True)
            return

        request_text = " ".join(entries)
        try:
            result = await insert_pending_request(str(chat_id), request_text)
            data = getattr(result, "data", None)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ موافقة", callback_data=f"approve_{data[0].get('id') if data else ''}"),
                    InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{data[0].get('id') if data else ''}")
                ]
            ])
            admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
            for admin_id in admin_targets:
                try:
                    await bot.send_message(
                        admin_id,
                        f"📩 طلب إضافة مستخدمين جديد:\n👤 من التوكن: {chat_id}\n📝 الطلب: {request_text}",
                        reply_markup=keyboard
                    )
                except Exception:
                    logger.exception("Failed to notify admin %s about interactive addusers request", admin_id)
            try:
                await callback.message.edit_text(callback.message.text + "\n\n✅ تم إرسال الطلب وهو قيد الموافقة.", reply_markup=None)
            except Exception:
                pass
            await bot.send_message(chat_id, "✅ تم إرسال طلبك وهو قيد الموافقة.")
        except Exception as e:
            logger.exception("Error saving addusers request (interactive): %s", e)
            await bot.send_message(chat_id, "❌ فشل إرسال الطلب. يرجى المحاولة لاحقاً.")
        finally:
            ADDUSERS_SESSIONS.pop(chat_id, None)
            await callback.answer("تم الإرسال.")
        return

@dp.message()
async def interactive_addusers_handler(message: types.Message):
    if not message.text or message.text.startswith("/"):
        return

    state = ADDUSERS_SESSIONS.get(message.chat.id)
    if not state:
        return

    text = message.text.strip()
    chat_id = message.chat.id

    try:
        if state["step"] == "username":
            state["current"]["username"] = text
            state["step"] = "adsl"
            await message.answer(
                f"🔢 الآن أرسل رقم الـ ADSL للمستخدم `{text}`.",
                reply_markup=get_action_keyboard(chat_id, step="cancel")
            )
            return

        if state["step"] == "adsl":
            username = state["current"].get("username")
            adsl = text
            state["entries"].append(f"{adsl}:{username}")
            state["current"] = {}
            state["step"] = "confirm"
            entries_list = "\n".join([f"{i+1}. {e}" for i, e in enumerate(state["entries"])])
            await message.answer(
                f"✅ تم تسجيل الطلبات:\n{entries_list}\n\n"
                "اختر إجراء:",
                reply_markup=get_action_keyboard(chat_id, step="confirm")
            )
            return

    except Exception as e:
        logger.exception("interactive addusers handler error: %s", e)
        ADDUSERS_SESSIONS.pop(message.chat.id, None)
        await message.answer("❌ حدث خطأ. تم إلغاء العملية.")

@dp.callback_query(lambda c: c.data and (c.data.startswith("approve_") or c.data.startswith("reject_")))
async def handle_approve_reject(callback: types.CallbackQuery):
    action, req_id = callback.data.split("_", 1)
    # TODO: Replace this with your actual function to fetch request info by req_id
    from bot.utils_shared import get_request_by_id  # Make sure this function exists and works
    request = await get_request_by_id(req_id)
    if not request:
        # If the DB row is gone (already processed/removed), clear the admin buttons
        msg = "⚠️ تعذر العثور على الطلب. ربما تمت المعالجة أو الحذف بالفعل."
        try:
            if callback.message:
                # Try to append a notice and remove the inline buttons so admin cannot act
                try:
                    await callback.message.edit_text((callback.message.text or "") + "\n\n" + msg, reply_markup=None)
                except Exception:
                    # As a fallback, just remove the markup
                    await callback.message.edit_reply_markup(None)
        except Exception:
            logger.debug("Failed to update admin message for missing request", exc_info=True)

        await callback.answer(msg, show_alert=True)
        return

    chat_id = request.get('chat_id')
    request_text = request.get('text')

    if action == "approve":
        # Insert users into the database based on request_text (format: "adsl:username adsl2:username2 ...")
        try:
            from bot.user_manager import UserManager
            from bot.utils_shared import update_pending_status

            token_id = request.get('chat_id') or request.get('token_id')
            pairs = (request_text or "").split()
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
                    if 'duplicate key value' in msg or '23505' in msg or 'already exists' in msg:
                        skipped.append((username, msg))
                        logger.info("Skipping existing user %s: %s", username, msg)
                        continue
                    failures.append((username, msg))
                    logger.exception("Failed to insert user %s from approval %s", username, req_id)

            # Mark pending request as approved in DB
            try:
                await update_pending_status(req_id, "approved")
            except Exception:
                logger.exception("Failed to update pending status for %s", req_id)

        except Exception:
            logger.exception("Error while processing approved request %s", req_id)

        # Notify requester
        try:
            await bot.send_message(chat_id, f"✅ تم قبول طلبك:\n{request_text}")
        except Exception:
            logger.debug("Failed to notify requester after approval", exc_info=True)

        # Update the admin message: mark as approved and remove buttons, include summary
        approver = callback.from_user.username or str(callback.from_user.id)
        admin_note = f"\n\n✅ تمت الموافقة بواسطة @{approver}"
        summary_lines = [admin_note]
        if successes:
            summary_lines.append(f"تمت الإضافة: {', '.join(successes)}")
        if skipped:
            summary_lines.append(f"تخطّي (موجود مسبقاً): {', '.join(s for s, _ in skipped)}")
        if failures:
            summary_lines.append(f"فشل: {', '.join(s for s, _ in failures)}")

        try:
            if callback.message:
                try:
                    await callback.message.edit_text((callback.message.text or "") + "\n\n" + "\n".join(summary_lines), reply_markup=None)
                except Exception:
                    await callback.message.edit_reply_markup(None)
        except Exception:
            logger.debug("Failed to update admin message after approve", exc_info=True)

        await callback.answer("تمت الموافقة على الطلب.")
    elif action == "reject":
        # Notify requester
        await bot.send_message(chat_id, f"❌ تم رفض طلبك:\n{request_text}")

        # Update the admin message: mark as rejected and remove buttons
        rejector = callback.from_user.username or str(callback.from_user.id)
        admin_note = f"\n\n❌ تم الرفض بواسطة @{rejector}"
        try:
            if callback.message:
                try:
                    await callback.message.edit_text((callback.message.text or "") + admin_note, reply_markup=None)
                except Exception:
                    await callback.message.edit_reply_markup(None)
        except Exception:
            logger.debug("Failed to edit admin message after reject", exc_info=True)

        await callback.answer("تم رفض الطلب.")
