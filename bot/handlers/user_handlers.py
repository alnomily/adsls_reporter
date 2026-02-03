import asyncio
import os
import logging
from PIL import Image
from datetime import datetime, timezone, timedelta
import calendar
from typing import Any, Dict, List, Optional

from aiogram import F, types
from aiogram.filters import Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
import calendar
from bot.selected_network_manager import selected_network_manager,SelectedNetwork
from bot.chat_user_manager import chat_user_manager
from bot.app import dp, bot, EXEC, SCRAPE_SEMAPHORE
from bot.state import PENDING_ADD_USERS
PENDING_ENABLE_REQUESTS: dict = {}
from bot.utils_shared import create_chat_user, create_network, save_scraped_account, get_all_users_by_network_id
from config import ADMIN_ID, ADMIN_IDS
from scraper.runner import process_all_adsls, process_all_adsls_with_usernames
from scraper.utils import add_log

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)
from bot.utils import BotUtils
# avoid circular imports: import run_blocking/save_scraped_account lazily inside functions
from bot.user_manager import UserManager
from bot.utils_shared import save_scraped_account
from bot.table_report import TableReportGenerator
from bot.report_sender import collect_saved_user_reports, generate_images, send_images
from zoneinfo import ZoneInfo
from html import escape
import pytz
from aiogram.fsm.state import StatesGroup, State
from bot.handlers.partners_handlers import partners_command
from aiogram.fsm.state import StatesGroup, State
import re

from bot.handlers import main_menu
from bot.handlers.main_menu import build_command_menu_reply
# Payment method choices for admin approval
PAYMENT_METHOD_OPTIONS = ["جيب", "كريمي", "حوالة محلية", "نقدي"]

# class RegisterState(StatesGroup):
#     name = State()
#     network = State()
#     adsl = State()

class RegisterState(StatesGroup):
    name = State()
    network = State()
    adsl = State()
    adsl_with_name = State()

    choose_adsl_source = State()
    choose_old_network = State()
    choose_adsls_to_move = State()


class AdminApproveState(StatesGroup):
    choose_expiration_date = State()
    enter_amount = State()
    choose_payment_method = State()

@dp.message(Command("start"))
async def start_handler(message: types.Message, state: FSMContext):
    telegram_id = str(message.chat.id)

    user = await chat_user_manager.get(telegram_id)
    logger.info("Start command from telegram_id=%s user=%s", telegram_id, user.__repr__())
    if user and user.is_active:
        # Show the reply main menu keyboard (KeyboardButtons near input)
        try:
            kb = build_command_menu_reply()
        except Exception:
            kb = None
        await message.answer(f"👋 مرحباً {user.user_name}\n✔️ حسابك نشط\n", reply_markup=kb)
        return
    elif user and not user.is_active:
        await message.answer(f"👋 مرحباً {user.user_name}\n❌ حسابك غير نشط. يرجى التواصل مع الإدارة لتفعيله.\n")
        return

    await state.set_state(RegisterState.name)
    await state.update_data(registration_mode=True)
    await message.answer("📝 أدخل اسمك:")

@dp.message(RegisterState.name)
async def register_name(message: types.Message, state: FSMContext):
    # Mark that we are in the new-network registration flow so the next handler accepts the network name
    await state.update_data(user_name=message.text, expecting_new_network=True)
    await state.set_state(RegisterState.network)
    await message.answer("🌐 أدخل اسم الشبكة:")

@dp.message(RegisterState.network)
async def register_network_add(message: types.Message, state: FSMContext):
    data = await state.get_data()
    # Only handle if we are in the "network_add" flow; otherwise let the original handler run
    if not data.get("expecting_new_network"):
        logger.info("Not expecting new network, skipping register_network_add")
        return

    name = (message.text or "").strip()
    logger.info("Adding new network with name=%s", name)
    if name.startswith("/"):
        await message.answer("⚠️ لا يمكن أن يبدأ اسم الشبكة بـ '/'. الرجاء إدخال اسم صحيح:")
        await state.set_state(RegisterState.network)
        return

    await state.update_data(network_name=name)
    await state.set_state(RegisterState.adsl)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ نعم، إضافة ADSL الآن", callback_data="confirm_add_adsls_yes")],
        [InlineKeyboardButton(text="❌ لا", callback_data="skip_adsls")]
    ])
    await message.answer("📡 هل تريد إضافة خطوط الإنترنت الآن؟", reply_markup=kb)

    # clear the temporary marker
    await state.update_data(expecting_new_network=False)

@dp.message(RegisterState.network)
async def register_network(message: types.Message, state: FSMContext):
    data = await state.get_data()
    # Only handle if we are in the "network_add" flow; otherwise let the original handler run
    if data.get("expecting_new_network"):
        name = (message.text or "").strip()
        logger.info("Adding new network with name=%s", name)
        if name.startswith("/"):
            await message.answer("⚠️ لا يمكن أن يبدأ اسم الشبكة بـ '/'. الرجاء إدخال اسم صحيح:")
            await state.set_state(RegisterState.network)
            return

        await state.update_data(network_name=message.text)
        await state.set_state(RegisterState.adsl)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ نعم، إضافة ADSL الآن", callback_data="confirm_add_adsls_yes")],
        [InlineKeyboardButton(text="❌ لا", callback_data="skip_adsls")]
    ])
    await message.answer("📡 هل تريد إضافة خطوط الإنترنت الآن؟", reply_markup=kb)

@dp.callback_query(F.data == "confirm_add_adsls_yes")
async def confirm_add_adsls_yes(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    registration_mode = bool(data.get("registration_mode"))
    if registration_mode:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL (كل رقم في سطر)", callback_data="adsl_manual")],
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL مع أسماء المستخدمين", callback_data="adsl_manual_with_names")],
            [InlineKeyboardButton(text="📡 رفع ملف نصي بأرقام ADSL", callback_data="adsl_file")],
            [InlineKeyboardButton(text="⬅️ تسجيل بدون خطوط ADSL", callback_data="skip_adsls")]
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL (كل رقم في سطر)", callback_data="adsl_manual")],
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL مع أسماء المستخدمين", callback_data="adsl_manual_with_names")],
            [InlineKeyboardButton(text="📡 رفع ملف نصي بأرقام ADSL", callback_data="adsl_file")],
            [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")],
            [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
        ])
    try:
        await call.message.edit_text("📌 اختر طريقة إضافة خطوط الـ ADSL:", reply_markup=kb)
    except Exception:
        await call.message.answer("📌 اختر طريقة إضافة خطوط الـ ADSL:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "skip_adsls")
async def skip_adsls(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    telegram_id = str(call.from_user.id)

    # Block duplicate pending requests for this chat
    if _has_pending_request(call.message.chat.id):
        await call.answer("⚠️ لديك طلب قيد المراجعة حالياً. انتظر قرار الإدارة قبل إرسال طلب جديد.", show_alert=True)
        await state.clear()
        await call.message.delete()
        return

    # ensure chat_user exists or create
    chat_user = await chat_user_manager.get(telegram_id)
    isSignup = bool(data.get("expecting_new_network"))
    if not chat_user:
        user = await create_chat_user(telegram_id, data.get("user_name", ""))
        chat_user_id = user.data[0]["id"] if user and getattr(user, "data", None) else None
        chat_user_name = data.get("user_name", "")
    else:
        chat_user_id = chat_user.chat_user_id
        chat_user_name = chat_user.user_name

    network_id = None
    network_name = ""
    if "network_name" in (data or {}):
        network = await create_network(chat_user_id, escape_markdown(data.get("network_name", "")))
        network_name = escape_markdown(data.get("network_name", ""))
        try:
            network_id = _extract_network_id(network)
        except Exception as e:
            logger.exception("Error processing network creation response: %s", e)
            network_id = None

    PENDING_ADD_USERS[call.message.chat.id] = {
        "user_ids": [],
        "network_id": network_id,
        "adsl_numbers": [],
        "user_name": chat_user_name,
        "network_name": network_name,
        "admin_msgs": {}
    }

    # Ask admins to approve the newly added network
    # await _notify_admins_network_request(telegram_id, chat_user_name, network_name, network_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ قبول", callback_data=f"approve_{telegram_id}"),
            InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{telegram_id}")
        ]
    ])
    admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
    admin_msgs = {}
    for admin_id in admin_targets:
        try:
            sent_msg = await bot.send_message(
                admin_id,
                (
                    f"طلب تسجيل جديد:\n" if isSignup else  "طلب اضافة شبكة:\n"
                    f"اسم المشترك: {chat_user_name}\n"
                    f"الشبكة: {network_name}\n"
                    f"معرف الشبكة: {network_id}\n"
                    f"خطوط الإنترنت: لا توجد خطوط\n\nهل تقبل الطلب؟"
                ),
                reply_markup=kb
            )
            admin_msgs[admin_id] = getattr(sent_msg, "message_id", None)
        except Exception:
            logger.exception("Failed to notify admin about signup")

    if admin_msgs:
        try:
            PENDING_ADD_USERS[call.message.chat.id]["admin_msgs"] = admin_msgs
        except Exception:
            pass

    # Inform the user (ensure non-empty message)
    user_added_text = f"✅ تم تسجيلك باسم {chat_user_name}." if isSignup else ""
    network_added_text = f"🌐 تم اضافة الشبكة: {network_name}." if network_name else ""
    adsls_added_text = "📡 تم إضافة خطوط الإنترنت:\n" + "\n".join(data.get("adsl_numbers", [])) if data.get("adsl_numbers") else ""
    waiting_approve_text = "\n⏳ في انتظار موافقة الادارة لإضافة الشبكة وخطوط الإنترنت." if network_id else ""

    message_text_parts = [p for p in [user_added_text, network_added_text, adsls_added_text, waiting_approve_text] if p]
    message_text = "\n".join(message_text_parts) if message_text_parts else "⏳ تم استلام طلبك، بانتظار موافقة الإدارة."

    try:
        await call.message.edit_text(message_text)
    except Exception:
        await call.message.answer(message_text)

    await state.clear()
    await call.answer()

@dp.callback_query(F.data == "adsl_manual")
async def adsl_manual(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegisterState.adsl)
    data = await state.get_data()
    registration_mode = bool(data.get("registration_mode"))
    if registration_mode:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 أرسل ارقام ADSL مع اسماء المستخدمين", callback_data="adsl_manual_with_names")],
            [InlineKeyboardButton(text="⬅️ تسجيل بدون خطوط ADSL", callback_data="skip_adsls")],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")],
            [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
        ])
    try:
        await call.message.edit_text("📡كتب أرقام ADSL (كل رقم في سطر):\nمثال:\n01087890\n01098099\n01836382", reply_markup=kb)
    except Exception:
        await call.message.answer("📡 كتب أرقام ADSL (كل رقم في سطر):\nمثال:\n01087890\n01098099\n01836382", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "adsl_manual_with_names")
async def adsl_manual_with_names(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(RegisterState.adsl_with_name)
    data = await state.get_data()
    registration_mode = bool(data.get("registration_mode"))
    if registration_mode:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL (كل رقم في سطر)", callback_data="adsl_manual")],
            [InlineKeyboardButton(text="⬅️ تسجيل بدون خطوط ADSL", callback_data="skip_adsls")],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")],
            [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
        ])
    try:
        await call.message.edit_text("📡 اكتب أرقام ADSL مع أسماء المستخدمين (كل زوج في سطر، مفصول بمسافة أو فاصلة) الخانة الأولى هي رقم ADSL واسم المستخدم بعدها:\nمثال:\n01087890 087890\n01098099,11098099\n01836382 1836382", reply_markup=kb)
    except Exception:
        await call.message.answer("📡 اكتب أرقام ADSL مع أسماء المستخدمين (كل زوج في سطر، مفصول بمسافة أو فاصلة) الخانة الأولى هي رقم ADSL واسم المستخدم بعدها:\nمثال:\n01087890 087890\n01098099,11098099\n01836382 1836382", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "adsl_move")
async def adsl_move(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    current_state = await state.get_state()
    data = await state.get_data()

    # Detect if we are inside the initial /start registration flow (skip active check there)
    in_registration_flow = current_state in {
        RegisterState.name.state,
        RegisterState.network.state,
        RegisterState.adsl.state,
        RegisterState.choose_adsl_source.state,
    } or bool(data.get("expecting_new_network"))

    if not in_registration_flow:
        if not user:
            await call.answer("❌ لم يتم تسجيلك بعد. استخدم /start للتسجيل أولاً.", show_alert=True)
            return
        if not getattr(user, "is_active", False):
            await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة لتفعيله.", show_alert=True)
            return

    if not user:
        await call.answer("❌ لم يتم العثور على حساب. أكمل التسجيل أولاً.", show_alert=True)
        return

    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    logger.info("Fetched %d active networks for user_id=%s", len(active_networks), user.chat_user_id)
    logger.info("Active networks: %s", active_networks)
    if not active_networks :
        await call.answer("❌ لا توجد شبكات مفعلة لنقل خطوط ADSL منها.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {escape_markdown(n['network_name'])} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})",
            callback_data=f"move_from_network_{n['network_id']}|{escape_markdown(n['network_name'])}"
        )] for n in active_networks if _is_owner_or_full_perm(n) and n.get("adsls_count", 0) > 0
    ])

    await state.set_state(RegisterState.choose_old_network)
    await call.message.edit_text("🌐 اختر الشبكة التي تريد نقل الـ ADSL منها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("move_from_network_"))
async def choose_old_network(call: types.CallbackQuery, state: FSMContext):
    # Remove prefix
    data_part = call.data[len("move_from_network_"):]
    logger.info("Choosing old network data_part=%s", data_part)
    # Try to split by '|'
    if "|" in data_part:
        old_network_id, old_network_name = data_part.split("|", 1)
    else:
        old_network_id, old_network_name = data_part, ""
    old_network_id = int(old_network_id)
    logger.info("Chosen old network id=%s name=%s", old_network_id, old_network_name)
    await state.update_data(old_network_id=old_network_id, old_network_name=old_network_name)

    adsls = await UserManager.get_users_by_network(old_network_id)
    logger.info("Fetched %d ADSLs for old network id=%s", len(adsls), old_network_id)
    if not adsls:
        await call.answer("❌ لا يوجد ADSLs في هذه الشبكة", show_alert=True)
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📡 {a.get('adsl_number')}", callback_data=f"toggle_adsl_{a.get('id')}|{a.get('adsl_number')}")]
        for a in adsls
    ] + [
        [InlineKeyboardButton(text="✅ تأكيد النقل", callback_data="confirm_move_adsls")],
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_move_adsls")]
    ])

    await state.update_data(selected_adsls=[])
    await state.set_state(RegisterState.choose_adsls_to_move)

    await call.message.edit_text("📡 اختر خطوط ADSL المراد نقلها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("toggle_adsl_"))
async def toggle_adsl(call: types.CallbackQuery, state: FSMContext):
    adsl_data = call.data[len("toggle_adsl_"):]
    if "|" in adsl_data:
        adsl_id_str, adsl_number = adsl_data.split("|", 1)
    else:
        adsl_id_str = adsl_data
        adsl_number = adsl_id_str

    adsl_id = str(adsl_id_str)
    adsl_number = str(adsl_number)

    data = await state.get_data()
    selected_numbers = set(data.get("selected_adsls", []))
    selected_ids = set(str(x) for x in data.get("selected_ids", []))

    # toggle using id for ids set and number for numbers set
    if adsl_id in selected_ids:
        selected_ids.remove(adsl_id)
        selected_numbers.discard(adsl_number)
    else:
        selected_ids.add(adsl_id)
        selected_numbers.add(adsl_number)

    await state.update_data(selected_adsls=list(selected_numbers), selected_ids=list(selected_ids))

    # rebuild inline keyboard to reflect current selection
    old_network_id = data.get("old_network_id")
    adsls = data.get("adsls", None)
    try:
        if adsls is None and old_network_id:
            adsls = await UserManager.get_users_by_network(old_network_id)
            await state.update_data(adsls=adsls)
    except Exception:
        adsls = []

    rows = []
    for a in adsls:
        aid = str(a.get("id"))
        label = a.get("adsl_number") or a.get("username") or aid
        text = f"✅ {label}" if aid in selected_ids else f"📡 {label}"
        callback_val = f"toggle_adsl_{aid}|{a.get('adsl_number')}" if a.get('adsl_number') else f"toggle_adsl_{aid}"
        rows.append([InlineKeyboardButton(text=text, callback_data=callback_val)])
    rows.append([InlineKeyboardButton(text="✅ تأكيد النقل", callback_data="confirm_move_adsls")])
    rows.append([InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_move_adsls")])

    try:
        await call.message.edit_text("📡 اختر خطوط ADSL المراد نقلها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception:
        pass

    await call.answer()

@dp.callback_query(F.data == "confirm_move_adsls")
async def confirm_move_adsls(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    adsls_ids = data.get("selected_ids", [])
    adsls_numbers = data.get("selected_adsls", [])
    
    old_network_id = int(data.get("old_network_id", 0))
    old_network_name = escape_markdown(data.get("old_network_name", ""))

    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    user = await create_chat_user(telegram_id, data["user_name"]) if not chat_user else chat_user
    chat_user_id = user.data[0]["id"] if not chat_user else chat_user.chat_user_id

    if not adsls_ids:
        await call.answer("⚠️ لم يتم اختيار أي ADSL", show_alert=True)
        return
    
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    network_id = None
    network_name = ""
    if "network_name" in (data or {}):
        network = await create_network(chat_user_id, escape_markdown(data["network_name"]))
        network_name = escape_markdown(data["network_name"])

        try:
            network_id = getattr(network, "data", None) or network
        except Exception as e:
            logger.exception("Error processing network creation response: %s", e)
            network_id = None
    else:
        # Ask user to choose destination network (exclude the source network)
        networks = await UserManager.get_networks_for_user(chat_user_id)
        if not networks:
            await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
            return
        dest_networks = [n for n in networks if n.get("id") != old_network_id and n.get("is_network_active", False)]
        if not dest_networks:
            await call.answer("❌ لا توجد شبكات أخرى نشطة لنقل الخطوط إليها.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
            return
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {escape_markdown(n['network_name'])} {f"({n['adsls_count']})" if n.get('adsls_count') is not None else "(0)"}", callback_data=f"move_to_network_{n['network_id']}|{escape_markdown(n['network_name'])}")]
            for n in dest_networks if _is_owner_or_full_perm(n) and n.get("network_id") != old_network_id
            ] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_move_adsls")]]
        )

        await call.message.edit_text("🌐 اختر الشبكة التي تريد نقل خطوط الـ ADSL إليها:", reply_markup=kb)
        await call.answer()

        # store pending move info so move_to_network handler can complete the operation
        await state.update_data(pending_move_adsls=adsls_ids, pending_move_adsls_numbers=adsls_numbers, old_network_id=old_network_id)
        return

    is_changed = await UserManager.change_users_network(
        users_ids=adsls_ids,
        old_network_id=old_network_id,
        new_network_id=network_id,
    )
    def _rtl_wrap(text: str,bold = False) -> str:
            """Force right-to-left display even if text includes LTR parts."""
            RLI = "\u2067"  # Right-to-Left isolate
            PDI = "\u2069"  # Pop directional isolate
            RLM = "\u200F"  # Right-to-left mark
            return f"{RLM}{RLI}{text}{PDI}"

    def _format_adsl_number_line(adsl_number) -> str:
        bold_name = f"<b>{escape(str(adsl_number))}</b>"
        raw_line = f"🔹 {bold_name}"
        return _rtl_wrap(raw_line)

    def _format_block(adsl_numbers: list) -> str:
        if not adsl_numbers:
            return "لا توجد"
        return "\n".join(_format_adsl_number_line(n) for n in adsl_numbers)
    
    frame_top = "╔═══════════⋆⋆⋆═══════════╗"
    frame_mid = "╚═══════════⋆⋆⋆═══════════╝"
    box_top = "╭───────────❖────────────╮"
    box_bottom = "╰───────────❖────────────╯"

    lines = [
        "🔁 <b>نقل خطوط ADSL</b>",
        frame_top,
        _rtl_wrap(f"🔹 اسم المشترك: <b>{chat_user.user_name if chat_user else 'غير معروف'}</b>"),
        _rtl_wrap(f"🔹 من شبكة: <b>{old_network_name}</b>"),
        _rtl_wrap(f"🔹 الى شبكة: <b>{network_name}</b>"),
        _rtl_wrap(f"🔹 عدد الخطوط: <b>{len(adsls_numbers)}</b>"),
        frame_mid,
        "",
    ]
    if is_changed:
        lines += [
        "✔️ <b>تم نقل خطوط الـ ADSL</b>",
        box_top,
        _format_block(adsls_numbers),
        box_bottom,
        "",
        "💡 للاستفسار، يرجى التواصل مع الإدارة @mig0_0",
        ]
    else:
        lines += [
        "❌ <b>فشل في نقل خطوط الـ ADSL</b>",
        box_top,
        _format_block(adsls_numbers),
        box_bottom,
        "",
        "💡 للاستفسار، يرجى التواصل مع الإدارة @mig0_0",
        ]
        
    await state.clear()
    await call.message.edit_text("\n".join(lines), parse_mode="HTML")
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("move_to_network_"))
async def move_to_network(call: types.CallbackQuery, state: FSMContext):
    new_network_id = int(call.data.split("_")[-1].split("|")[0])
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    data = await state.get_data()
    adsls_ids = data.get("pending_move_adsls", [])
    adsls_numbers = data.get("pending_move_adsls_numbers", [])
    old_network_id = int(data.get("old_network_id", 0))
    old_network_name = escape_markdown(data.get("old_network_name", ""))    
    new_network_name = escape_markdown(call.data.split("_")[-1].split("|")[1])

    if not adsls_ids:
        await call.answer("❌ لم يتم العثور على خطوط ADSL للنقل.", show_alert=True)
        return

    is_changed = await UserManager.change_users_network(
        users_ids=adsls_ids,
        old_network_id=old_network_id,
        new_network_id=new_network_id
    )

    def _rtl_wrap(text: str,bold = False) -> str:
            """Force right-to-left display even if text includes LTR parts."""
            RLI = "\u2067"  # Right-to-Left isolate
            PDI = "\u2069"  # Pop directional isolate
            RLM = "\u200F"  # Right-to-left mark
            return f"{RLM}{RLI}{text}{PDI}"

    def _format_adsl_number_line(adsl_number) -> str:
        bold_name = f"<b>{escape(str(adsl_number))}</b>"
        raw_line = f"🔹 {bold_name}"
        return _rtl_wrap(raw_line)

    def _format_block(adsl_numbers: list) -> str:
        if not adsl_numbers:
            return "لا توجد"
        return "\n".join(_format_adsl_number_line(n) for n in adsl_numbers)
    
    frame_top = "╔═══════════⋆⋆⋆═══════════╗"
    frame_mid = "╚═══════════⋆⋆⋆═══════════╝"
    box_top = "╭───────────❖────────────╮"
    box_bottom = "╰───────────❖────────────╯"

    lines = [
        "🔁 <b>نقل خطوط ADSL</b>",
        frame_top,
        _rtl_wrap(f"🔹 اسم المشترك: <b>{chat_user.user_name if chat_user else 'غير معروف'}</b>"),
        _rtl_wrap(f"🔹 من شبكة: <b>{old_network_name}</b>"),
        _rtl_wrap(f"🔹 الى شبكة: <b>{new_network_name}</b>"),
        _rtl_wrap(f"🔹 عدد الخطوط: <b>{len(adsls_numbers)}</b>"),
        frame_mid,
        "",
    ]
    if is_changed:
        lines += [
        "✔️ <b>تم نقل خطوط الـ ADSL</b>",
        box_top,
        _format_block(adsls_numbers),
        box_bottom,
        "",
        "💡 للاستفسار، يرجى التواصل مع الإدارة @mig0_0",
        ]
    else:
        lines += [
        "❌ <b>فشل في نقل خطوط الـ ADSL</b>",
        box_top,
        _format_block(adsls_numbers),
        box_bottom,
        "",
        "💡 للاستفسار، يرجى التواصل مع الإدارة @mig0_0",
        ]
        
    await state.clear()
    await call.message.edit_text("\n".join(lines), parse_mode="HTML")
    await call.answer()

@dp.callback_query(F.data == "cancel_move_adsls")
async def cancel_move_adsls_callback(call: types.CallbackQuery, state: FSMContext):
    await call.message.edit_text("⬅️ تم إلغاء عملية النقل.")
    await state.clear()
    await call.answer()

@dp.message(RegisterState.adsl)
async def register_finish(message: types.Message, state: FSMContext):
    data = await state.get_data()
    telegram_id = str(message.chat.id)
    registration_mode = bool(data.get("registration_mode"))

    # Capture chosen network context up front to avoid undefined variables later
    chosen_net_id = data.get("selected_network_id")
    chosen_net_name = data.get("selected_network_name")
    is_add_network_request = bool(data.get("expecting_new_network"))

    # Block duplicate pending requests for this chat
    if not registration_mode and _has_pending_request(message.chat.id):
        await message.answer("⚠️ لديك طلب قيد المراجعة حالياً. انتظر قرار الإدارة قبل إرسال طلب جديد.")
        await state.clear()
        await message.delete()
        return

    adsl_numbers = [x for x in message.text.splitlines() if x.strip()]

    # ensure chat_user exists or create
    chat_user = await chat_user_manager.get(telegram_id)
    user = await create_chat_user(telegram_id, data["user_name"]) if not chat_user else chat_user
    chat_user_id = user.data[0]["id"] if not chat_user else chat_user.chat_user_id

    network_id = None
    network_name = ""
    if "network_name" in (data or {}):
        # Create a new network and extract its id from the RPC response
        resp_net = await create_network(chat_user_id, data["network_name"])
        network_name = data["network_name"]
        try:
            network_id = _extract_network_id(resp_net)
        except Exception as e:
            logger.exception("Error processing network creation response: %s", e)
            network_id = None

        if not network_id:
            try:
                # Fallback: fetch networks list and pick the most recent by name
                nets = await UserManager.get_networks_for_user(chat_user_id)
                match = next((n for n in nets if (n.get("network_name") if isinstance(n, dict) else getattr(n, "network_name", "")) == network_name), None)
                network_id = (match.get("id") if isinstance(match, dict) else getattr(match, "id", None)) if match else None
            except Exception as e:
                logger.exception("Error fetching networks for user: %s", e)
                network_id = None
        if not network_id:
            await message.answer("❌ فشل في إنشاء الشبكة الجديدة.")
            await state.clear()
            return

        # Send approval request to admins for the new network
        # await _notify_admins_network_request(telegram_id, username, network_name, network_id)
    else:
        # Prefer the network chosen during ADSL add flow stored in FSM data
        if chosen_net_id:
            network_id = int(chosen_net_id)
            network_name = chosen_net_name or ""
            if not network_name:
                try:
                    net_obj = await UserManager.get_network_by_id(network_id)
                    network_name = (net_obj.get('network_name') if isinstance(net_obj, dict) else getattr(net_obj, 'network_name', ''))
                except Exception:
                    network_name = ""
        else:
            selected_network = await selected_network_manager.get(telegram_id)
            if not selected_network:
                await message.answer(" لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
                await state.clear()
                return
            network_id = selected_network.network_id
            network_name = selected_network.network_name

    username = chat_user.user_name if chat_user else data["user_name"]

    logger.info("Registering user_id=%s network_id=%s with %d ADSL numbers", chat_user_id, network_id, len(adsl_numbers))
    logger.info("ADSL Numbers: %s", adsl_numbers)

    summary = process_all_adsls(
        adsl_numbers=adsl_numbers,
        network_id=network_id,
        max_workers=6
    )

    failure_reasons_text = None
    if (summary.get("failure_reasons") and
        any(reason == "المستخدم موجود مسبقاً" for reason in summary["failure_reasons"].values())):
        logger.info("Some ADSLs already exist, notifying user")
        failure_reasons_text = ("⚠️ بعض أرقام ADSL التي حاولت إضافتها موجودة مسبقاً في النظام. يرجى مراجعة الأرقام التالية:\n" +
            "\n".join(
                f"❌ {adsl}: {reason}"
                for adsl, reason in summary["failure_reasons"].items()
                if reason == "المستخدم موجود مسبقاً"
            )) if summary.get("failure_reasons") and len(summary.get("failure_reasons")) > 1 else f"⚠️ رقم ADSL الذي حاولت إضافته موجود مسبقاً في النظام. يرجى مراجعة الرقم {list(summary['failure_reasons'].keys())[0]}."    

    successful_adsl_users = summary.get('success', '').split(",") if summary.get('success') else []

    PENDING_ADD_USERS[message.chat.id] = {
        "user_ids": successful_adsl_users,
        "network_id": network_id,
        "adsl_numbers": summary.get('success_adsl', '').split(",") if summary.get('success_adsl') else [],
        "user_name": username,
        "network_name": network_name,
        "admin_msgs": {}
    }

    # Build user-facing message with fallback to avoid empty text
    failed_adsl_list = summary.get('failed_adsl', '').split(',') if summary.get('failed_adsl') else []
    parts = [
        f"✅ تم ارسال طلب تسجيل {'خط واحد' if len(successful_adsl_users) == 1 else f'{len(successful_adsl_users)} خطوط'}" if successful_adsl_users else "",
        f"{"فشل في تسجيل الخطوط التالية:\n" if len(failed_adsl_list) > 0 else "فشل في تسجيل الخط: "}" + "\n".join(failed_adsl_list) + f"\n جرب اضافة {"الخطوط" if len(failed_adsl_list) > 1 else "الخط"} عن طريق رقم ADSL و اسم المستخدم من قائمة خطوط النت في البوت, او تواصل مع الادارة {"@mig0_0" if successful_adsl_users else ""}" if summary.get('failed_adsl') and successful_adsl_users else "",
        f"{failure_reasons_text or ''}",
        "⏳ في انتظار موافقة الإدارة..." if successful_adsl_users else "❌ فشل في تسجيل أي حساب. يرجى التواصل مع الإدارة @mig0_0 لاضافة الخطوط التي فشلت."
    ]
    msg_text = "\n".join([p for p in parts if p]) or "⏳ تم استلام طلبك، بانتظار موافقة الإدارة."

    await message.answer(msg_text)

    if summary.get("failure_reasons"):
        failure_msgs = []
        for adsl, reason in summary["failure_reasons"].items():
            failure_msgs.append(f"❌ {adsl}: {reason}")
        await message.answer(
            "⚠️ بعض الأرقام لم تتم إضافتها بنجاح:\n" +
            "\n".join(failure_msgs)
        )
    
    if not successful_adsl_users:
        await state.clear()
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ قبول", callback_data=f"approve_{telegram_id}"),
            InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{telegram_id}")
        ]
    ])

    admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
    admin_msgs = {}

    logger.info("Notifying admins about ADSL add request, registration_mode=%s", registration_mode)
    for admin_id in admin_targets:
        try:
            sent_msg = await bot.send_message(
                admin_id,
                (
                    f"{"طلب تسجيل مشترك جديد:\n" if registration_mode else "طلب تفعيل شبكة جديدة:\n" if is_add_network_request else "طلب تفعيل خطوط إنترنت:\n"}"
                    f"اسم المشترك: {username}\n"
                    f"الشبكة: {network_name}\n"
                    f"معرف الشبكة: {network_id}\n"
                    f"خطوط الإنترنت:\n" + "\n".join(summary.get("success_adsl", "").split(",")) +
                    "\n\nهل تقبل الطلب؟"
                ),
                reply_markup=kb
            )
            admin_msgs[admin_id] = getattr(sent_msg, "message_id", None)
        except Exception:
            logger.exception("Failed to notify admin about signup")
    if admin_msgs:
        try:
            PENDING_ADD_USERS[message.chat.id]["admin_msgs"] = admin_msgs
        except Exception as e:
            logger.exception("Error storing admin messages: %s", e)

    if registration_mode:
        add_more_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ إضافة خطوط إنترنت أخرى", callback_data=f"select_network_to_add_adsls_{network_id}")],
            [InlineKeyboardButton(text="❌ لاحقاً", callback_data="registration_add_more_no")]
        ])
        try:
            await message.answer("هل تريد إضافة خطوط إنترنت أخرى الآن؟", reply_markup=add_more_kb)
        except Exception:
            try:
                await message.answer("هل تريد إضافة خطوط إنترنت أخرى الآن؟")
            except Exception as e:
                logger.exception("Error sending add more ADSL prompt: %s", e)
        return
    await state.clear()
    
    

@dp.message(RegisterState.adsl_with_name)
async def register_finish_with_names(message: types.Message, state: FSMContext):
    data = await state.get_data()
    telegram_id = str(message.chat.id)
    registration_mode = bool(data.get("registration_mode"))
    
    # Block duplicate pending requests for this chat
    if not registration_mode and _has_pending_request(message.chat.id):
        await message.answer("⚠️ لديك طلب قيد المراجعة حالياً. انتظر قرار الإدارة قبل إرسال طلب جديد.")
        await state.clear()
        await message.delete()
        return
    
    chosen_net_id = data.get("selected_network_id")
    chosen_net_name = data.get("selected_network_name")
    is_add_network_request = bool(data.get("expecting_new_network"))
    logger.info("Registering ADSLs with usernames for telegram_id=%s", telegram_id)
    logger.info(f"chosen_net_id={chosen_net_id}, chosen_net_name={chosen_net_name}, is_add_network_request={is_add_network_request}")

    adsl_entries = [x for x in message.text.splitlines() if x.strip()]
    adsl_numbers = []
    user_names = []
    for entry in adsl_entries:
        parts = entry.replace(",", " ").split()
        if parts:
            adsl_numbers.append(parts[0].strip())
            if len(parts) > 1:
                user_names.append(parts[1].strip())
            else:
                await message.answer(f"❌ لم يتم توفير اسم مستخدم لرقم ADSL {parts[0].strip()}. يرجى المحاولة مرة أخرى مع توفير أسماء المستخدمين.")
                await state.clear()
                return
    # ensure chat_user exists or create
    chat_user = await chat_user_manager.get(telegram_id)
    user = await create_chat_user(telegram_id, data["user_name"]) if not chat_user else chat_user
    chat_user_id = user.data[0]["id"] if not chat_user else chat_user.chat_user_id

    network_id = None
    network_name = ""
    if "network_name" in (data or {}):
        # Create a new network and extract its id from the RPC response
        resp_net = await create_network(chat_user_id, data["network_name"])
        network_name = data["network_name"]
        try:
            network_id = _extract_network_id(resp_net)
        except Exception as e:
            logger.exception("Error processing network creation response: %s", e)
            network_id = None
        if not network_id:
            try:
                # Fallback: fetch networks list and pick the most recent by name
                nets = await UserManager.get_networks_for_user(chat_user_id)
                match = next((n for n in nets if (n.get("network_name") if isinstance(n, dict) else getattr(n, "network_name", "")) == network_name), None)
                network_id = (match.get("id") if isinstance(match, dict) else getattr(match, "id", None)) if match else None
            except Exception as e:
                logger.exception("Error fetching networks for user: %s", e)
                network_id = None
        if not network_id:
            await message.answer("❌ فشل في إنشاء الشبكة الجديدة.")
            await state.clear()
            return
    else:
        if chosen_net_id:
            network_id = int(chosen_net_id)
            network_name = chosen_net_name or ""
            if not network_name:
                try:
                    net_obj = await UserManager.get_network_by_id(network_id)
                    network_name = (net_obj.get('network_name') if isinstance(net_obj, dict) else getattr(net_obj, 'network_name', ''))
                except Exception:
                    network_name = ""
        else:
            selected_network = await selected_network_manager.get(telegram_id)
            if not selected_network:
                await message.answer(" لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
                await state.clear()
                return
            network_id = selected_network.network_id
            network_name = selected_network.network_name
            
    username = chat_user.user_name if chat_user else data["user_name"]

    logger.info("Registering user_id=%s network_id=%s with %d ADSL numbers", chat_user_id, network_id, len(adsl_numbers))
    logger.info("ADSL Numbers: %s", adsl_numbers)

    summary = process_all_adsls_with_usernames(
        adsl_user_map=dict(zip(adsl_numbers, user_names)),
        network_id=network_id,
        max_workers=6
    )
    failure_reasons_text = None
    if (summary.get("failure_reasons") and
        any(reason == "المستخدم موجود مسبقاً" for reason in summary["failure_reasons"].values())):
        logger.info("Some ADSLs already exist, notifying user")
        failure_reasons_text = ("⚠️ بعض أرقام ADSL التي حاولت إضافتها موجودة مسبقاً في النظام. يرجى مراجعة الأرقام التالية:\n" +
            "\n".join(
                f"❌ {adsl}: {reason}"
                for adsl, reason in summary["failure_reasons"].items()
                if reason == "المستخدم موجود مسبقاً"
            )) if summary.get("failure_reasons") and len(summary.get("failure_reasons")) > 1 else f"⚠️ رقم ADSL الذي حاولت إضافته موجود مسبقاً في النظام. يرجى مراجعة الرقم {list(summary['failure_reasons'].keys())[0]}."

    successful_adsl_users = summary.get('success', '').split(",") if summary.get('success') else []

    PENDING_ADD_USERS[message.chat.id] = {
        "user_ids": successful_adsl_users,
        "network_id": network_id,
        "adsl_numbers": summary.get('success_adsl', '').split(",") if summary.get('success_adsl') else [],
        "user_name": chat_user.user_name if chat_user else data["user_name"],
        "network_name": network_name,
        "admin_msgs": {}
    }
    # Build user-facing message with fallback to avoid empty text
    failed_adsl_list = summary.get('failed_adsl', '').split(',') if summary.get('failed_adsl') else []
    parts = [
        f"✅ تم ارسال طلب تسجيل {'خط واحد' if len(successful_adsl_users) == 1 else f'{len(successful_adsl_users)} خطوط'}" if successful_adsl_users else "",
        f"{"فشل في تسجيل الخطوط التالية:\n" if len(failed_adsl_list) > 0 else "فشل في تسجيل الخط: "}" + "\n".join(failed_adsl_list) + f"\n جرب اضافة {"الخطوط" if len(failed_adsl_list) > 1 else "الخط"} عن طريق رقم ADSL و اسم المستخدم من قائمة خطوط النت في البوت, او تواصل مع الادارة {"@mig0_0" if successful_adsl_users else ""}" if summary.get('failed_adsl') and successful_adsl_users else "",
        f"{failure_reasons_text or ''}",
        "⏳ في انتظار موافقة الإدارة..." if successful_adsl_users else "❌ فشل في تسجيل أي حساب. يرجى التواصل مع الإدارة @mig0_0 لاضافة الخطوط التي فشلت."
    ]
    msg_text = "\n".join([p for p in parts if p]) or "⏳ تم استلام طلبك، بانتظار موافقة الإدارة."

    await message.answer(msg_text)
    if not successful_adsl_users:
        await state.clear()
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ قبول", callback_data=f"approve_{telegram_id}"),
            InlineKeyboardButton(text="❌ رفض", callback_data=f"reject_{telegram_id}")
        ]
    ])
    admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
    admin_msgs = {}

    logger.info("Notifying admins about ADSL add request with usernames, registration_mode=%s", registration_mode)
    logger.info("registration_mode=%s", registration_mode)
    for admin_id in admin_targets:
        try:
            sent_msg = await bot.send_message(
                admin_id,
                (
                    f"{"طلب تسجيل مشترك جديد:\n" if not registration_mode else "طلب تفعيل شبكة جديدة:\n" if is_add_network_request else "طلب تفعيل خطوط إنترنت:\n"}"
                    f"اسم المشترك: {chat_user.user_name if chat_user else data['user_name']}\n"
                    f"الشبكة: {network_name}\n"
                    f"معرف الشبكة: {network_id}\n"
                    f"خطوط الإنترنت:\n" + "\n".join(summary.get("success_adsl", "").split(",")) +
                    "\n\nهل تقبل الطلب؟"
                ),
                reply_markup=kb
            )
            admin_msgs[admin_id] = getattr(sent_msg, "message_id", None)
        except Exception:
            logger.exception("Failed to notify admin about signup")
    if admin_msgs:
        try:
            PENDING_ADD_USERS[message.chat.id]["admin_msgs"] = admin_msgs
        except Exception as e:
            logger.exception("Error storing admin messages: %s", e)

    # Offer to add more ADSL users right away (registration mode)
    if registration_mode:
        add_more_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ إضافة خطوط إنترنت أخرى", callback_data=f"select_network_to_add_adsls_{network_id}")],
            [InlineKeyboardButton(text="❌ لاحقاً", callback_data="registration_add_more_no")]
        ])
        try:
            await message.answer("هل تريد إضافة خطوط إنترنت أخرى الآن؟", reply_markup=add_more_kb)
        except Exception:
            try:
                await message.answer("هل تريد إضافة خطوط إنترنت أخرى الآن؟")
            except Exception as e:
                logger.exception("Error sending add more ADSL prompt: %s", e)
        return
    await state.clear()

@dp.callback_query(lambda c: c.data.startswith("approve_") and c.data.replace("approve_", "", 1).isdigit())
async def approve_application(call: types.CallbackQuery, state: FSMContext):
    telegram_id = int(call.data.split("_", 1)[1])
    data = PENDING_ADD_USERS.get(telegram_id)
    logger.info("Approving application for telegram_id=%s data=%s", telegram_id, data)
    if not data:
        await call.answer("❌ الطلب غير موجود أو انتهت صلاحيته.", show_alert=True)
        return

    # Remove approve/reject buttons once an admin starts the approval flow
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        logger.exception("Failed to clear approval buttons")

    await state.set_state(AdminApproveState.choose_expiration_date)
    await state.update_data(
        approval_target_telegram_id=telegram_id,
        approval_payload=data,
        approval_message_text=call.message.text,
        approval_message_chat_id=call.message.chat.id,
        approval_message_id=getattr(call.message, "message_id", None),
    )

    try:
        await call.message.answer("📅 اختر مدة التفعيل (1-6 أشهر):", reply_markup=_build_expiration_keyboard())
    except Exception:
        logger.exception("Failed to prompt admin for expiration date")
    await call.answer("📅 اختر مدة التفعيل")


def _build_expiration_keyboard(days_ahead: int = 60) -> InlineKeyboardMarkup:
    """Build a month-based picker (1-6 months ahead) for admin approval."""
    today = datetime.now(timezone.utc).date()
    buttons = []
    for months in range(1, 7):
        target_date = _add_months(today, months)
        label = f"{months} شهر ({target_date.strftime('%Y-%m-%d')})"
        buttons.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"approve_expiry_months_{months}"
            )
        )

    rows = []
    for idx in range(0, len(buttons), 3):
        rows.append(buttons[idx: idx + 3])

    rows.append([InlineKeyboardButton(text="❌ إلغاء", callback_data="approve_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(lambda c: c.data.startswith("approve_expiry_months_"))
async def handle_admin_choose_expiry(call: types.CallbackQuery, state: FSMContext):
    state_data = await state.get_data()
    payload = state_data.get("approval_payload") or {}
    if not payload:
        await call.answer("❌ لا يوجد طلب معلق.", show_alert=True)
        await state.clear()
        return

    months_str = call.data.replace("approve_expiry_months_", "", 1)
    try:
        months = int(months_str)
    except Exception:
        await call.answer("⚠️ خيار غير صالح.", show_alert=True)
        return

    if months <= 0:
        await call.answer("⚠️ اختر مدة صالحة.", show_alert=True)
        return

    today = datetime.now(timezone.utc).date()
    exp_date = _add_months(today, months)

    lines_count = len(payload.get("adsl_numbers", [])) or len(payload.get("user_ids", [])) or 0
    suggested_amount = lines_count * 200

    await state.update_data(
        approval_expiration_date=exp_date.isoformat(),
        approval_suggested_amount=suggested_amount,
        approval_duration_months=months,
    )
    await state.set_state(AdminApproveState.enter_amount)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"استخدام المبلغ المقترح ({suggested_amount})", callback_data=f"approve_use_amount_{suggested_amount}")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="approve_change_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="approve_cancel")],
        ]
    )

    prompt = (
        f"⏳ مدة التفعيل: {months} شهر\n"
        f"📅 تاريخ الانتهاء: {exp_date.isoformat()}\n"
        f"📡 عدد الخطوط: {lines_count}\n"
        f"💵 المبلغ المقترح (200 لكل خط): {suggested_amount}\n"
        "✏️ أرسل مبلغاً مختلفاً إذا لزم، أو اضغط استخدام المبلغ المقترح."
    )

    try:
        await call.message.edit_text(prompt, reply_markup=kb)
    except Exception:
        await call.message.answer(prompt, reply_markup=kb)
    await call.answer()


@dp.message(AdminApproveState.enter_amount)
async def handle_admin_amount(message: types.Message, state: FSMContext):
    state_data = await state.get_data()
    target_telegram_id = int(state_data.get("approval_target_telegram_id", 0) or 0)
    payload = state_data.get("approval_payload") or {}
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)

    if not target_telegram_id or not payload or not exp_date:
        await message.answer("❌ لا يوجد طلب معلق، ابدأ من جديد بالضغط على زر القبول.")
        await state.clear()
        return

    try:
        amount = int((message.text or "").strip())
    except Exception:
        await message.answer("⚠️ أدخل المبلغ كرقم صحيح.")
        return

    if amount < 0:
        await message.answer("⚠️ يجب ألا يكون المبلغ سالباً.")
        return

    await state.update_data(approval_amount=amount)
    await state.set_state(AdminApproveState.choose_payment_method)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 جيب", callback_data="approve_paymethod_جيب"), InlineKeyboardButton(text="🏦 كريمي", callback_data="approve_paymethod_كريمي")],
            [InlineKeyboardButton(text="💸 حوالة محلية", callback_data="approve_paymethod_حوالة محلية"), InlineKeyboardButton(text="💵 نقدي", callback_data="approve_paymethod_نقدي")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="approve_change_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="approve_cancel")],
        ]
    )

    await message.answer(
        f"⏳ المدة: {months} شهر\n📅 تاريخ الانتهاء: {exp_date}\n💵 المبلغ: {amount}\nاختر طريقة الدفع:",
        reply_markup=kb,
    )


@dp.callback_query(lambda c: c.data.startswith("approve_use_amount_"))
async def handle_use_suggested_amount(call: types.CallbackQuery, state: FSMContext):
    state_data = await state.get_data()
    target_telegram_id = int(state_data.get("approval_target_telegram_id", 0) or 0)
    payload = state_data.get("approval_payload") or {}
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)

    if not target_telegram_id or not payload or not exp_date:
        await call.answer("❌ لا يوجد طلب معلق.", show_alert=True)
        await state.clear()
        return

    try:
        amount = int(call.data.replace("approve_use_amount_", "", 1))
    except Exception:
        await call.answer("⚠️ مبلغ غير صالح.", show_alert=True)
        return

    await state.update_data(approval_amount=amount)
    await state.set_state(AdminApproveState.choose_payment_method)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 جيب", callback_data="approve_paymethod_جيب"), InlineKeyboardButton(text="🏦 كريمي", callback_data="approve_paymethod_كريمي")],
            [InlineKeyboardButton(text="💸 حوالة محلية", callback_data="approve_paymethod_حوالة محلية"), InlineKeyboardButton(text="💵 نقدي", callback_data="approve_paymethod_نقدي")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="approve_change_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="approve_cancel")],
        ]
    )

    try:
        await call.message.edit_text(
            f"⏳ المدة: {months} شهر\n📅 تاريخ الانتهاء: {exp_date}\n💵 المبلغ: {amount}\nاختر طريقة الدفع:",
            reply_markup=kb,
        )
    except Exception:
        await call.message.answer(
            f"⏳ المدة: {months} شهر\n📅 تاريخ الانتهاء: {exp_date}\n💵 المبلغ: {amount}\nاختر طريقة الدفع:",
            reply_markup=kb,
        )
    await call.answer()


@dp.callback_query(F.data == "approve_change_expiry")
async def handle_change_expiry(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminApproveState.choose_expiration_date)
    await call.message.edit_text(
        "📅 اختر مدة التفعيل (1-6 أشهر):",
        reply_markup=_build_expiration_keyboard(),
    )
    await call.answer()


@dp.callback_query(F.data == "approve_cancel")
async def handle_approve_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ تم إلغاء عملية التفعيل.")
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("approve_paymethod_"))
async def handle_admin_payment_method(call: types.CallbackQuery, state: FSMContext):
    state_data = await state.get_data()
    target_telegram_id = int(state_data.get("approval_target_telegram_id", 0) or 0)
    payload = state_data.get("approval_payload") or {}
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)
    amount = state_data.get("approval_amount")

    if not target_telegram_id or not payload or not exp_date or not amount:
        await call.answer("❌ البيانات غير مكتملة.", show_alert=True)
        await state.clear()
        return

    payment_method = call.data.replace("approve_paymethod_", "", 1)
    if not payment_method:
        await call.answer("⚠️ اختر طريقة دفع صالحة.", show_alert=True)
        return

    admin_tid = str(call.from_user.id)
    payer = await chat_user_manager.get(admin_tid)
    if not payer:
        payer_resp = await create_chat_user(admin_tid, call.from_user.full_name or admin_tid)
        payer_chat_user_id = payer_resp.data[0]["id"] if getattr(payer_resp, "data", None) else 0
    else:
        payer_chat_user_id = getattr(payer, "chat_user_id", 0)

    if not payer_chat_user_id:
        await call.answer("❌ تعذر تحديد حساب الدافع. حاول مرة أخرى.", show_alert=True)
        await state.clear()
        return

    await UserManager.activate_users(payload.get("user_ids", []))
    is_activated = await UserManager.approve_registration(
        users_ids=payload.get("user_ids", []),
        telegram_id=target_telegram_id,
        network_id=payload.get("network_id"),
        payer_chat_user_id=payer_chat_user_id,
        expiration_date=exp_date,
        amount=int(amount),
        payment_method=payment_method,
    )

    if is_activated:
        PENDING_ADD_USERS.pop(target_telegram_id, None)
        await chat_user_manager.activate_chat_user_in_cache(str(target_telegram_id))
        await bot.send_message(
            target_telegram_id,
            "✅ تم قبول طلبك من قبل الإدارة.\n"
            f"⏳ المدة: {months} شهر\n"
            f"📅 تاريخ الانتهاء: {exp_date}\n"
            f"💳 المبلغ: {amount}\n"
            f"💰 طريقة الدفع: {payment_method}",
        )

        base_text = state_data.get("approval_message_text", "")
        updated_text = (
            f"{base_text.replace('هل تقبل الطلب؟', '').strip()}\n"
            f"✅ تم قبول الطلب.\n"
            f"⏳ المدة: {months} شهر\n"
            f"📅 تاريخ الانتهاء: {exp_date}\n"
            f"💳 المبلغ: {amount}\n"
            f"💰 طريقة الدفع: {payment_method}"
        )

        msg_chat_id = state_data.get("approval_message_chat_id")
        msg_id = state_data.get("approval_message_id")
        try:
            if msg_chat_id and msg_id:
                await bot.edit_message_text(updated_text, chat_id=msg_chat_id, message_id=msg_id)
        except Exception:
            logger.exception("Failed to edit admin approval message")

        await _broadcast_admin_decision(payload.get("admin_msgs", {}), updated_text, exclude_admin_id=call.from_user.id)
        await call.message.edit_text(
            f"✅ تم التفعيل وتسجيل الدفع.\n⏳ {months} شهر\n📅 {exp_date}\n💵 {amount}\n💰 {payment_method}"
        )
    else:
        await call.message.edit_text("❌ حدث خطأ أثناء قبول الطلب. حاول مرة أخرى.")

    await state.clear()
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("reject_"))
async def reject_application(call: types.CallbackQuery):
    telegram_id = int(call.data.split("_", 1)[1])
    data = PENDING_ADD_USERS.pop(telegram_id, None)
    logger.info("Rejecting application for telegram_id=%s data=%s", telegram_id, data)
    if not data:
        await call.answer("❌ الطلب غير موجود أو انتهت صلاحيته.", show_alert=True)
        return
    await bot.send_message(telegram_id, "❌ تم رفض طلبك من قبل الإدارة.")
    updated_text = f"{call.message.text.replace('هل تقبل الطلب؟', '')}\n❌ تم رفض الطلب."
    await call.message.edit_text(updated_text)
    await _broadcast_admin_decision(data.get("admin_msgs", {}) if data else {}, updated_text, exclude_admin_id=call.from_user.id)
    await call.answer()

@dp.message(Command("help"))
async def help_command(message: types.Message) -> None:
    lines = [
        "📖 <b>دليل الاستخدام المفصل</b>",
        "",
        "🚀 <b>البدء والتسجيل</b>",
        "• /start — تسجيل جديد أو فحص حالة حسابك (نشط/غير نشط).",
        "• أثناء التسجيل: أدخل اسمك ثم اسم الشبكة ثم أضف خطوط ADSL أو تخطَّ هذه الخطوة.",
        "• /help — عرض هذا الدليل في أي وقت.",
        "",
        "🌐 <b>إدارة الشبكات</b>",
        "• /networks — القائمة الكاملة: إضافة، تعديل الاسم/المواعيد، حذف، تبديل الشبكة النشطة، إدارة الشركاء.",
        "• /addnetwork — إنشاء شبكة جديدة سريعًا (ينشئ شبكة ثم يمكنك إضافة خطوط).",
        "• ملاحظات الصلاحيات: المالك فقط يمكنه التعديل/الحذف/إدارة الشركاء، الشريك (قراءة/قراءة+كتابة) يقتصر على ما تسمح به الصلاحية.",
        "",
        "📡 <b>إدارة خطوط الإنترنت (ADSL)</b>",
        "• /addusers — إضافة خطوط نت للشبكات التي تملك صلاحية الكتابة عليها.",
        "• من /networks ثم إدارة الشبكات: نقل خطوط بين شبكاتك، أو حذف خطوط من شبكة مملوكة.",
        "• النقل يتطلب شبكتين قابلتين للكتابة على الأقل، وإحدى الشبكات تحتوي خطوطًا موجودة.",
        "",
        "📊 <b>التقارير والمتابعة</b>",
        "• /account — ملخص حسابك: الشبكة النشطة، معرف المشترك، شبكات المالك والشريك (مع المعرف والصلاحية والحالة).",
        "• /reports — تقرير فوري للشبكة الحالية أو لكل الشبكات النشطة.",
        "• /reportdate — اختيار تاريخ محدد للحصول على تقارير تاريخية لكل أو بعض الشبكات.",
        "• التنبيهات: يمكنك ضبط أوقات إرسال التقارير والتنبيهات من الإعدادات.",
        "",
        "🤝 <b>الشركاء</b>",
        "• من /networks ➜ إدارة الشركاء: دعوة/إدارة الشركاء للشبكات التي تملكها فقط.",
        "• الصلاحيات التي تمنحها للشريك تحدد ما يمكنه فعله (قراءة فقط أو كتابة).",
        "",
        "⚙️ <b>الإعدادات</b>",
        "• /settings — تغيير الشبكة النشطة، تعديل اسم المشترك، مواعيد التقارير، مستويات التحذير والخطر للرصيد/الأيام، وتفعيل/إيقاف تقارير الشركاء.",
        "• يمكنك أيضًا تبديل الشبكة النشطة مباشرة من نفس القائمة.",
        "",
        "ℹ️ <b>الدعم والمعلومات</b>",
        "• /about — معلومات عن البوت وكيفية التواصل مع الإدارة للدعم.",
        "",
        "💡 <b>نصائح سريعة</b>",
        "• حدّث الشبكة النشطة قبل تشغيل الأوامر التي تعتمد عليها (مثل /reports أو /addusers).",
        "• إذا لم ترَ شبكات نشطة، تواصل مع الإدارة لتفعيلها.",
        "• تأكد من صلاحياتك (مالك/شريك قراءة أو كتابة) قبل محاولة نقل أو حذف الخطوط.",
    ]
    await message.answer("\n".join(lines), parse_mode="HTML")

@dp.message(Command("networks"))
async def networks_menu(message: types.Message, state: FSMContext):
    telegram_id = str(message.chat.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    netowrks = await UserManager.get_networks_for_user(user.chat_user_id)
    inactive_networks = [n for n in netowrks if not n.get("is_network_active", False)]
    active_networks = [n for n in netowrks if n.get("is_network_active", False)]
    if len(netowrks) > 0 and not active_networks:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔓 طلب تفعيل شبكة", callback_data="enable_network_request_list")],
            [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]
        ])
        await message.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.", reply_markup=kb)
        return
    if not active_networks or len(active_networks) == 0:
        await state.clear()
        kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ إضافة شبكة", callback_data="network_add")],
        [InlineKeyboardButton(text="⬅️ إغلاق", callback_data="close_settings")]
        ])
        await message.answer("🌐 اضافة شبكة جديدة:", reply_markup=kb)
        return
    
    await state.clear()
    rows = [
        [InlineKeyboardButton(text="🔄 تغيير الشبكة النشطة", callback_data="change_active_network")],
        [InlineKeyboardButton(text="➕ إضافة شبكة", callback_data="network_add")],
        [InlineKeyboardButton(text="✏️ تعديل شبكة", callback_data="network_edit")],
        [InlineKeyboardButton(text="🗑️ حذف شبكة", callback_data="network_delete")],
        [InlineKeyboardButton(text="🤝 إدارة الشركاء", callback_data="partners")]
    ]
    if inactive_networks:
        rows.append([InlineKeyboardButton(text="🔓 طلب تفعيل شبكة", callback_data="enable_network_request_list")])
    rows.append([InlineKeyboardButton(text="⬅️ إغلاق", callback_data="close_settings")])

    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await message.answer("🌐 اختر عملية على الشبكات:", reply_markup=kb)

@dp.callback_query(F.data == "show_networks")
async def networks_back_callback(call: types.CallbackQuery, state: FSMContext):
    # Remove current menu/message, then show the ADSL menu
    try:
        await call.message.delete()
    except Exception:
        pass
    try:
        await networks_menu(call.message, state)
    except Exception:
        pass
    await call.answer()

@dp.callback_query(F.data == "enable_network_request_list")
async def enable_network_request_list(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return

    netowrks = await UserManager.get_networks_for_user(user.chat_user_id)
    inactive = [n for n in netowrks if not n.get("is_network_active", False)] if netowrks else []
    if not inactive:
        await call.answer("لا توجد شبكات موقوفة لطلب تفعيلها.", show_alert=True)
        return

    pending = set(PENDING_ENABLE_REQUESTS.get(telegram_id, set()))
    rows = []
    for n in inactive:
        nid = n.get("network_id") or n.get("id")
        if nid is None:
            continue
        if nid in pending:
            label = f"⏳ {escape_markdown(n.get('network_name', 'شبكة'))} (طلب قيد المراجعة)"
            rows.append([InlineKeyboardButton(text=label, callback_data="noop")])
        else:
            label = f"🔓 {escape_markdown(n.get('network_name', 'شبكة'))}"
            rows.append([InlineKeyboardButton(text=label, callback_data=f"enable_network_request_{nid}")])

    rows.append([InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    try:
        await call.message.edit_text("اختر الشبكة الموقوفة لإرسال طلب تفعيل:", reply_markup=kb)
    except Exception:
        await call.message.answer("اختر الشبكة الموقوفة لإرسال طلب تفعيل:", reply_markup=kb)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("enable_network_request_"))
async def enable_network_request(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return

    try:
        network_id = int(call.data.rsplit("_", 1)[-1])
    except Exception:
        await call.answer("خطأ في اختيار الشبكة.", show_alert=True)
        return

    pending = set(PENDING_ENABLE_REQUESTS.get(telegram_id, set()))
    if network_id in pending:
        await call.answer("لديك طلب تفعيل قيد المراجعة لهذه الشبكة.", show_alert=True)
        return

    # record pending
    pending.add(network_id)
    PENDING_ENABLE_REQUESTS[telegram_id] = pending

    # notify admins
    admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
    net_obj = await UserManager.get_network_by_network_id(network_id)
    net_name = escape_markdown(net_obj.get("network_name") if isinstance(net_obj, dict) else getattr(net_obj, "network_name", "")) if net_obj else ""
    for admin_id in admin_targets:
        try:
            await bot.send_message(
                admin_id,
                (
                    "🔓 طلب تفعيل شبكة موقوفة:\n"
                    f"👤 المستخدم: {user.user_name}\n"
                    f"🆔 معرف الشبكة: {network_id}\n"
                    f"🌐 اسم الشبكة: {net_name}\n"
                    "يرجى تفعيل الشبكة أو التواصل مع المستخدم."
                )
            )
        except Exception:
            logger.exception("Failed to notify admin about enable request")

    try:
        await call.message.edit_text("✅ تم إرسال طلب التفعيل. سيتم التواصل معك بعد المراجعة.")
    except Exception:
        await call.answer("✅ تم إرسال طلب التفعيل. سيتم التواصل معك بعد المراجعة.", show_alert=True)
        return
    await call.answer()


@dp.callback_query(F.data == "noop")
async def noop_callback(call: types.CallbackQuery):
    await call.answer()

@dp.callback_query(F.data == "network_add")
async def network_add_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)

    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return

    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    # mark that we're specifically adding a new network so we can validate input later
    await state.update_data(user_name=user.user_name, expecting_new_network=True)
    await state.set_state(RegisterState.network)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_networks")],
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_add_network")]
    ])
    await call.message.edit_text("🌐 أدخل اسم الشبكة الجديدة (لا يبدأ بـ '/'):",
                                 reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "cancel_add_network")
async def cancel_add_network(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await call.message.edit_text("⬅️ تم إلغاء عملية إضافة الشبكة.")
    except Exception:
        try:
            await call.message.delete()
        except Exception:
            pass
    await call.answer()

def _is_read_perm(obj):
        if obj is None:
            return False
        if isinstance(obj, dict):
            p = obj.get("permissions")
        else:
            p = getattr(obj, "permissions", None)
        return isinstance(p, str) and p.strip().lower() == "read"

def _is_owner_perm(obj):
        if obj is None:
            return False
        if isinstance(obj, dict):
            p = obj.get("permissions")
        else:
            p = getattr(obj, "permissions", None)
        return isinstance(p, str) and p.strip().lower() == "owner"
    
def _is_owner_or_full_perm(obj):
        if obj is None:
            return False
        if isinstance(obj, dict):
            p = obj.get("permissions")
        else:
            p = getattr(obj, "permissions", None)
        return isinstance(p, str) and p.strip().lower() in ("owner", "full")

def _is_active_network(obj):
        if obj is None:
            return False
        if isinstance(obj, dict):
            active = obj.get("is_network_active")
        else:
            active = getattr(obj, "is_network_active", None)
        return bool(active)


def _add_months(base_date, months: int):
    """Add months to a date, clamping the day to the target month's last day."""
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_date.day, calendar.monthrange(year, month)[1])
    return base_date.replace(year=year, month=month, day=day)

# Helper: safely convert a value to int with default fallback
def _safe_int(val, default: int = 0) -> int:
    try:
        if val is None:
            return default
        return int(val)
    except Exception:
        return default


def _has_pending_request(chat_id) -> bool:
    try:
        return chat_id in PENDING_ADD_USERS
    except Exception:
        return False


def _extract_network_id(network_resp: Any) -> Optional[int]:
    """Best-effort extractor for network id from various supabase responses."""
    data = getattr(network_resp, "data", None)
    if isinstance(data, list) and data and isinstance(data[0], dict):
        return data[0].get("id") or data[0].get("network_id")
    if isinstance(data, dict):
        return data.get("id") or data.get("network_id")
    if isinstance(network_resp, dict):
        return network_resp.get("id") or network_resp.get("network_id")
    try:
        return int(data)
    except Exception:
        return None

async def _broadcast_admin_decision(admin_msgs: dict, updated_text: str, exclude_admin_id: Optional[int] = None):
    """Edit pending approval messages for other admins to reflect the decision."""
    if not admin_msgs:
        return
    for admin_id, msg_id in admin_msgs.items():
        if not admin_id or not msg_id:
            continue
        try:
            if exclude_admin_id and str(admin_id) == str(exclude_admin_id):
                continue
            await bot.edit_message_text(updated_text, chat_id=admin_id, message_id=msg_id)
        except Exception:
            logger.debug("Failed to broadcast admin decision to %s", admin_id, exc_info=True)

@dp.callback_query(F.data == "network_edit")
async def network_edit_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} ✏️ {n['network_name']} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})", callback_data=f"edit_network_{n['id']}")]
            for n in networks if _is_owner_perm(n)
        ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_networks")]]
        +[[InlineKeyboardButton(text="⬅️ إغلاق", callback_data="close_settings")]]
    )
    await call.message.edit_text("✏️ اختر الشبكة لتعديلها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("edit_network_") and not c.data.startswith("edit_network_action_"))
async def edit_network_selected(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    uid = call.from_user.id
    telegram_id = str(uid)

    try:
        network = await UserManager.get_network_by_id(network_id)
    except Exception:
        network = None

    if not _is_active_network(network):
        await call.answer("❌ لا يمكنك تعديل هذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return

    if not _is_owner_perm(network):
        await call.answer("⚠️ لا يمكنك تعديل هذه الشبكة. فقط مالك الشبكة يمكنه التعديل عليها.", show_alert=True)
        return
    
    try:
        chat_user = await chat_user_manager.get(telegram_id)
        if chat_user:
            await selected_network_manager.set(network_id, chat_user.chat_user_id, telegram_id=telegram_id)
    except Exception:
        logger.debug("Could not set selected network when initiating edit_network", exc_info=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ تعديل اسم الشبكة", callback_data=f"edit_network_action_change_name_{network_id}")],
        [InlineKeyboardButton(text="🕒 تعديل مواعيد التقارير", callback_data=f"edit_network_action_change_times_{network_id}")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="network_edit")]
    ])

    try:
        await call.message.edit_text("✏️ اختر الإجراء الذي تريد تطبيقه على هذه الشبكة:", reply_markup=kb)
    except Exception:
        await call.message.answer("✏️ اختر الإجراء الذي تريد تطبيقه على هذه الشبكة:", reply_markup=kb)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("edit_network_action_change_name_"))
async def edit_network_change_name(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    uid = call.from_user.id
    telegram_id = str(uid)

    try:
        network = await UserManager.get_network_by_id(network_id)
    except Exception:
        network = None

    if not network:
        await call.answer("❌ حدث خطأ غير متوقع. الشبكة غير موجودة.", show_alert=True)
        return

    if not _is_active_network(network):
        await call.answer("❌ لا يمكنك تعديل هذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return

    if not _is_owner_perm(network):
        await call.answer("⚠️ لا يمكنك تعديل اسم هذه الشبكة.", show_alert=True)
        return

    # Mark awaiting name in our simple state map and FSM data; request return to networks menu after save
    user_settings_state[uid] = "awaiting_network_name"
    await state.update_data(edit_network_id=network_id, return_to_networks_after_name=True)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ إلغاء", callback_data=f"edit_network_action_cancel_{network_id}")],
        [InlineKeyboardButton(text="⬅️ رجوع إلى الشبكات", callback_data="network_edit")]
    ])

    try:
        await call.message.edit_text("📝 أرسل اسم الشبكة الجديد:", reply_markup=kb)
    except Exception:
        await call.message.answer("📝 أرسل اسم الشبكة الجديد:", reply_markup=kb)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("edit_network_action_change_times_"))
async def edit_network_change_times(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    uid = call.from_user.id

    try:
        network = await UserManager.get_network_by_id(network_id)
    except Exception:
        network = None

    if not network:
        await call.answer("❌ حدث خطأ غير متوقع. الشبكة غير موجودة.", show_alert=True)
        return
    
    if not _is_active_network(network):
        await call.answer("❌ لا يمكنك تعديل هذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return

    if not _is_owner_perm(network):
        await call.answer("⚠️ لا يمكنك تعديل مواعيد هذه الشبكة.", show_alert=True)
        return

    # store edit id and set awaiting_report_times
    await state.update_data(edit_network_id=network_id,return_to_networks_after_times=True)
    user_settings_state[uid] = "awaiting_report_times"

    # prepare current selection from network.times_to_send_reports
    try:
        selected_times = set(SelectedNetwork.from_bitmask_to_times_list(network.get("times_to_send_reports", 15)))
        logger.debug("Current selected times for network_id=%s: %s", network_id, selected_times)
    except Exception:
        selected_times = set()

    user_report_selections[uid] = selected_times

    kb = _make_times_keyboard(uid, True)
    try:
        await call.message.edit_text("🕒 اختر مواعيد إرسال التقرير (يمكن اختيار أكثر من موعد):", reply_markup=kb)
    except Exception:
        await call.message.answer("🕒 اختر مواعيد إرسال التقرير (يمكن اختيار أكثر من موعد):", reply_markup=kb)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("edit_network_action_cancel_"))
async def edit_network_change_cancel(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    uid = call.from_user.id

    # clear any temporary state
    user_settings_state.pop(uid, None)
    user_report_selections.pop(uid, None)
    try:
        await state.clear()
    except Exception:
        pass

    try:
        await call.message.edit_text("⬅️ تم إلغاء تعديل الشبكة.")
    except Exception:
        try:
            await call.message.delete()
        except Exception:
            pass
    await call.answer("✅ تم الإلغاء")

@dp.callback_query(F.data == "network_delete")
async def network_delete_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🗑️ {n['network_name']} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})", callback_data=f"delete_network_{n['network_id']}")]
            for n in active_networks if _is_owner_perm(n)
        ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_networks")]] + [[InlineKeyboardButton(text="⬅️ إغلاق", callback_data="close_settings")]]
    )
    await call.message.edit_text("🗑️ اختر الشبكة لحذفها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("delete_network_"))
async def delete_network_selected(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])

    # Confirm deletion
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ تأكيد الحذف", callback_data=f"confirm_delete_network_{network_id}")],
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]
    ])
    await call.message.edit_text("⚠️ هل أنت متأكد من حذف هذه الشبكة؟", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("confirm_delete_network_"))
async def confirm_delete_network(call: types.CallbackQuery):
    network_id = int(call.data.split("_")[-1])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑️ نعم، احذف الشبكة نهائيًا", callback_data=f"perform_delete_network_{network_id}")],
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text(
            "⚠️ هل أنت متأكد تمامًا من حذف هذه الشبكة سيتم حذف جميع الخطوط المرتبطة بها؟ هذا الإجراء لا يمكن التراجع عنه.",
            reply_markup=kb
        )
    except Exception:
        await call.message.answer(
            "⚠️ هل أنت متأكد تمامًا من حذف هذه الشبكة سيتم حذف جميع الخطوط المرتبطة بها؟ هذا الإجراء لا يمكن التراجع عنه.",
            reply_markup=kb
        )

    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("perform_delete_network_"))
async def perform_delete_network(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    success = await UserManager.remove_network(network_id)
    if success:
        await call.message.edit_text("✅ تم حذف الشبكة بنجاح.")
    else:
        await call.message.edit_text("❌ فشل في حذف الشبكة.")
        
    try:
        await network_delete_cb(call, state)
    except Exception:
        pass
    await call.answer()

@dp.callback_query(F.data == "partners")
async def partners_menu_cb(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return

    # If only one network, set it and forward directly
    if len(active_networks) == 1:
        try:
            await selected_network_manager.set(active_networks[0]["id"], user.chat_user_id, telegram_id=telegram_id)
        except Exception:
            pass
        await partners_command(call.message)
        await call.answer()
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {escape_markdown(n['network_name'])} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})",
                    callback_data=f"partners_select_{n['id']}|{escape_markdown(n['network_name'])}"
                )
            ] for n in active_networks if _is_owner_perm(n)
        ] +  [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_networks")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]]
    )

    try:
        await call.message.edit_text("🌐 اختر الشبكة التي تريد إدارة الشركاء لها:", reply_markup=kb)
    except Exception:
        await call.message.answer("🌐 اختر الشبكة التي تريد إدارة الشركاء لها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("partners_select_"))
async def partners_select_cb(call: types.CallbackQuery):
    payload = call.data[len("partners_select_"):]
    if "|" in payload:
        network_id_str, network_name = payload.split("|", 1)
    else:
        network_id_str, network_name = payload, ""
    try:
        network_id = int(network_id_str)
    except Exception:
        await call.answer("❌ خطأ في اختيار الشبكة.", show_alert=True)
        return

    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ خطأ في المستخدم", show_alert=True)
        return

    # Check read permissions
    network = await UserManager.get_network_by_id(network_id)

    if not network:
        await call.answer("❌ حدث خطأ غير متوقع. الشبكة غير موجودة.", show_alert=True)
        return

    if not _is_active_network(network):
        await call.answer("❌ لا يمكنك تعديل هذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return
    
    if not _is_owner_perm(network):
        await call.answer("⚠️ ليس لديك صلاحية إدارة الشركاء لهذه الشبكة.", show_alert=True)
        return

    try:
        await selected_network_manager.set(network_id, chat_user.chat_user_id, telegram_id=telegram_id)
    except Exception:
        logger.debug("Could not set selected network before opening partners view", exc_info=True)

    try:
        await call.message.edit_text(f"🌐 جاري فتح إدارة الشركاء للشبكة {escape_markdown(network_name)}...")
    except Exception:
        pass

    await partners_command(call.message)
    await call.answer()


@dp.message(Command("adsls"))
async def adsls_menu(message: types.Message, state: FSMContext):
    telegram_id = str(message.chat.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ إضافة ADSL", callback_data="adsls_add")],
        [InlineKeyboardButton(text="🔁 نقل ADSL", callback_data="adsls_move")],
        [InlineKeyboardButton(text="📑 ترتيب ADSL في الشبكة", callback_data="order_index_networks")],
        [InlineKeyboardButton(text="✂️ حذف ADSL", callback_data="adsls_delete")],
        [InlineKeyboardButton(text="⬅️ إغلاق", callback_data="close_settings")],

    ])
    await message.answer("📡 اختر عملية على خطوط الـ ADSL:", reply_markup=kb)

@dp.callback_query(F.data == "show_adsls")
async def adsls_back_callback(call: types.CallbackQuery, state: FSMContext):
    # Remove current menu/message, then show the ADSL menu
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        await call.message.delete()
    except Exception:
        pass
    try:
        await adsls_menu(call.message, state)
    except Exception:
        pass
    await call.answer()

@dp.callback_query(F.data == "adsls_add")
async def adsls_add_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    # Always show available writable networks for adding ADSLs
    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬 يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    writable_networks = [n for n in active_networks if _is_owner_or_full_perm(n) ]
    if not writable_networks:
        await call.answer("❌ لا توجد شبكات تملك صلاحية الكتابة عليها. صلاحياتك قراءة فقط.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {n['network_name']} ({f"{n['adsls_count']}" if n.get('adsls_count') is not None else '0'})", callback_data=f"select_network_to_add_adsls_{n['id']}")]
            for n in writable_networks
        ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]]
    )
    await call.message.edit_text("🌐 الرجاء اختيار الشبكة التي تريد إضافة المستخدمين إليها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "adsl_file")
async def adsl_file_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    # await state.set_state(RegisterState.adsl_file)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]
    ])
    await call.message.edit_text("سيتم اضافة هذه الخطوة قريباً", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("select_network_to_add_adsls_"))
async def select_network_for_adsls(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # registration_mode is set only during /start registration flow
    registration_mode = bool(data.get("registration_mode"))
    logger.info("select_network_for_adsls: registration_mode=%s", registration_mode)
    try:
        network_id = int(call.data.split("_")[-1])
    except Exception:
        await call.answer("❌ خطأ في اختيار الشبكة.", show_alert=True)
        return

    try:
        network = await (UserManager.get_network_by_network_id(network_id) if registration_mode else UserManager.get_network_by_id(network_id))
    except Exception:
        network = None
        
    if not network:
        await call.answer("❌ الشبكة غير موجودة.", show_alert=True)
        return

    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user and not registration_mode:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if user and not user.is_active and not registration_mode:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    if not registration_mode and not _is_active_network(network):
        await call.answer("❌ لا يمكنك اضافة خطوط ADSL لهذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return

    if _is_read_perm(network):
        await call.answer("⚠️ ليس لديك صلاحية إضافة خطوط لهذه الشبكة. صلاحياتك قراءة فقط.", show_alert=True)
        return

    # Save chosen network in FSM data for subsequent steps
    name_val = (network.get('network_name') if isinstance(network, dict) else getattr(network, 'network_name', ''))
    await state.update_data(selected_network_id=(network.get('network_id') if isinstance(network, dict) else getattr(network, 'network_id', network_id)),
                           selected_network_name=name_val)

    if registration_mode:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📡 أرسل أرقام ADSL (كل رقم في سطر)", callback_data="adsl_manual")],
            [InlineKeyboardButton(text="📡 أرسل ارقام ADSL مع اسماء المستخدمين", callback_data="adsl_manual_with_names")],
            [InlineKeyboardButton(text="📡 رفع ملف نصي بأرقام ADSL", callback_data="adsl_file")],
            [InlineKeyboardButton(text="❌ لا أريد إضافة المزيد", callback_data="registration_add_more_no")],
        ])
    else:
         kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 أرسل أرقام ADSL (كل رقم في سطر)", callback_data="adsl_manual")],
        [InlineKeyboardButton(text="📡 أرسل ارقام ADSL مع اسماء المستخدمين", callback_data="adsl_manual_with_names")],
        [InlineKeyboardButton(text="📡 رفع ملف نصي بأرقام ADSL", callback_data="adsl_file")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")],
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]
        ])

    try:
        await call.message.edit_text(f"📡 إضافة خطوط لشبكة {escape_markdown(name_val)}...\n📡 اختر الإجراء:", reply_markup=kb)
    except Exception:
        await call.message.answer(f"📡 إضافة خطوط لشبكة {escape_markdown(name_val)}...\n📡 اختر الإجراء:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "registration_add_more_no")
async def registration_add_more_no(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    try:
        await call.message.edit_text("✅ تم استلام طلبك. يمكنك إضافة خطوط لاحقاً بعد قبول طلبك من /adsls.")
    except Exception:
        await call.answer("✅ تم استلام طلبك. يمكنك إضافة خطوط لاحقاً بعد قبول طلبك من /adsls.", show_alert=True)
        return
    await call.answer()

@dp.callback_query(F.data == "adsls_move")
async def adsls_move_cb(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active or not networks:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    active_networks = [n for n in networks if n.get('is_network_active', True)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return

    # Require at least two writable networks and at least one with ADSLs to move
    writable_networks = [n for n in networks if not _is_read_perm(n)]
    has_adsls_source = any(_safe_int(n.get('adsls_count'), 0) > 0 for n in writable_networks)
    if len(writable_networks) < 2 or not has_adsls_source:
        await call.answer("يجب أن يكون لديك أكثر من شبكة واحدة لنقل خطوط الإنترنت بينها. ويجب أن تحتوي على الاقل واحدة من هذه الشبكات على خطوط ADSL.", show_alert=True)
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {n['network_name']} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})", callback_data=f"move_from_network_{n['network_id']}|{escape_markdown(n['network_name'])}")]
            for n in networks if _is_owner_or_full_perm(n) and _safe_int(n.get("adsls_count"), 0) > 0  # Filter out read-only networks
        ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_move_adsls")]]
    )
    
    if not kb.inline_keyboard:
        await call.answer("❌ لا توجد شبكات قابلة للنقل.", show_alert=True)
        return

    await state.set_state(RegisterState.choose_old_network)
    await call.message.edit_text("🌐 اختر الشبكة التي تريد نقل الـ ADSL منها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("toggle_delete_adsl_"))
async def toggle_delete_adsl(call: types.CallbackQuery, state: FSMContext):
    adsl_data = call.data[len("toggle_delete_adsl_"):]
    if "|" in adsl_data:
        adsl_id_str, adsl_number = adsl_data.split("|", 1)
    else:
        adsl_id_str = adsl_data
        adsl_number = adsl_id_str

    adsl_id = str(adsl_id_str)
    adsl_number = str(adsl_number)

    data = await state.get_data()
    selected_to_delete = set(data.get("selected_adsls_to_delete", []))

    if adsl_id in selected_to_delete:
        selected_to_delete.remove(adsl_id)
    else:
        selected_to_delete.add(adsl_id)

    await state.update_data(selected_adsls_to_delete=list(selected_to_delete))

    # Rebuild the inline keyboard to reflect current selection
    telegram_id = str(call.from_user.id)
    data2 = await state.get_data()
    chosen_delete_network_id = data2.get("delete_network_id")
    if chosen_delete_network_id:
        net_obj = await UserManager.get_network_by_id(int(chosen_delete_network_id))
        net_id_for_users = (net_obj.get('network_id') if isinstance(net_obj, dict) else getattr(net_obj, 'network_id', None))
        adsls = await UserManager.get_users_by_network(net_id_for_users) if net_id_for_users else []
    else:
        network = await selected_network_manager.get(telegram_id)
        adsls = await UserManager.get_users_by_network(network.network_id) if network else []

    rows = []
    for a in adsls:
        aid = str(a.get("id"))
        label = a.get("adsl_number") or a.get("username") or aid
        text = f"✅ {label}" if aid in selected_to_delete else f"✂️ {label}"
        callback_val = f"toggle_delete_adsl_{aid}|{a.get('adsl_number')}" if a.get('adsl_number') else f"toggle_delete_adsl_{aid}"
        rows.append([InlineKeyboardButton(text=text, callback_data=callback_val)])
    rows.append([InlineKeyboardButton(text="🗑️ تأكيد الحذف", callback_data="confirm_delete_adsls")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")])
    rows.append([InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")])

    try:
        await call.message.edit_text("✂️ اختر خطوط ADSL المراد حذفها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception:
        pass

    await call.answer()

@dp.callback_query(F.data == "adsls_delete")
async def adsls_delete_cb(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    user = await chat_user_manager.get(telegram_id)
    if not user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    networks = await UserManager.get_networks_for_user(user.chat_user_id)
    if not networks:
        await call.answer("❌ لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return

    # If multiple networks, let user choose the network to delete from
    writable_networks = [n for n in active_networks if _is_owner_or_full_perm(n)]
    if len(writable_networks) > 1:
        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(
                    text=f"{'🌟' if _is_owner_perm(n) else '🤝'} 🌐 {n['network_name']} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})",
                    callback_data=f"delete_from_network_{n['id']}|{escape_markdown(n['network_name'])}"
                )]
                for n in writable_networks if n['adsls_count']
            ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")]]
        )
        await call.message.edit_text("🌐 اختر الشبكة التي تريد حذف الـ ADSL منها:", reply_markup=kb)
        await call.answer()
        return

    # Otherwise, use the active or only network
    network = await selected_network_manager.get(telegram_id) or writable_networks[0]
    if _is_read_perm(network):
        await call.answer("⚠️ ليس لديك صلاحية لحذف خطوط هذه الشبكة. صلاحياتك قراءة فقط.", show_alert=True)
        return

    # Remember chosen network id for subsequent delete operations
    try:
        await (await call.bot.fsm.get_context(call.from_user.id, call.message.chat.id)).update_data(delete_network_id=(network.id if hasattr(network, 'id') else (network.get('id') if isinstance(network, dict) else None)))
    except Exception:
        pass

    adsls = await UserManager.get_users_by_network(network.network_id if hasattr(network, 'network_id') else network.get('network_id'))
    if not adsls:
        await call.answer("📭 لا توجد خطوط ADSL للحذف.", show_alert=True)
        return

    kb_rows = [
        [InlineKeyboardButton(text=f"✂️ {a.get('adsl_number') or a.get('username')}", callback_data=f"toggle_delete_adsl_{a.get('id')}|{a.get('adsl_number')}")]
        for a in adsls
    ]  + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")],
        [InlineKeyboardButton(text="🗑️ تأكيد الحذف", callback_data="confirm_delete_adsls")]
        ]

    await call.message.edit_text("✂️ اختر ADSL للحذف:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("delete_from_network_"))
async def delete_from_network_selected(call: types.CallbackQuery, state: FSMContext):
    payload = call.data[len("delete_from_network_"):]
    if "|" in payload:
        network_id_str, network_name = payload.split("|", 1)
    else:
        network_id_str, network_name = payload, ""
    try:
        network_id = int(network_id_str)
    except Exception:
        await call.answer("❌ خطأ في اختيار الشبكة.", show_alert=True)
        return

    network = await UserManager.get_network_by_id(network_id)

    if not network:
        await call.answer("❌ حدث خطأ غير متوقع. الشبكة غير موجودة.", show_alert=True)
        return

    if not _is_active_network(network):
        await call.answer("❌ لا يمكنك تعديل هذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك الموقوفة", show_alert=True)
        return

    if _is_read_perm(network):
        await call.answer("⚠️ ليس لديك صلاحية لحذف خطوط هذه الشبكة. صلاحياتك قراءة فقط.", show_alert=True)
        return
    
    if _safe_int(network.get('adsls_count'), 0) == 0:
        await call.answer("📭 لا توجد خطوط ADSL للحذف.", show_alert=True)
        return

    # Save chosen network id in FSM data for subsequent steps
    await state.update_data(delete_network_id=network_id)

    # Fetch ADSLs for the chosen network
    net_id_for_users = (network.get('network_id') if isinstance(network, dict) else getattr(network, 'network_id', None))
    adsls = await UserManager.get_users_by_network(net_id_for_users) if net_id_for_users else []
    if not adsls:
        await call.answer("📭 لا توجد خطوط ADSL للحذف.", show_alert=True)
        return

    kb_rows = [
        [InlineKeyboardButton(text=f"✂️ {a.get('adsl_number') or a.get('user_name')}", callback_data=f"toggle_delete_adsl_{a.get('id')}|{a.get('adsl_number')}")]
        for a in adsls
    ]  + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="show_adsls")]] + [[InlineKeyboardButton(text="⬅️ إلغاء", callback_data="close_settings")],
        [InlineKeyboardButton(text="🗑️ تأكيد الحذف", callback_data="confirm_delete_adsls")]
        ]

    try:
        await call.message.edit_text("✂️ اختر ADSL للحذف:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    except Exception:
        await call.message.answer("✂️ اختر ADSL للحذف:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()

@dp.callback_query(F.data == "confirm_delete_adsls")
async def confirm_delete_adsls(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    adsls_ids = data.get("selected_adsls_to_delete", [])
    if not adsls_ids:
        await call.answer("⚠️ لم يتم اختيار أي ADSL للحذف.", show_alert=True)
        return

    await UserManager.delete_users_by_ids(adsls_ids)

    try:
        await call.message.edit_text("✅ تم حذف خطوط ADSL المحددة.")
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        try:
            await adsls_delete_cb(call, state)
        except Exception:
            pass

    await state.clear()
    await call.answer()

def _get_network_permisssions_str(obj: Optional[SelectedNetwork]) -> str:
    if obj is None:
        return "غير معروف"
    if isinstance(obj, dict):
        p = obj.get("permissions")
    else:
        p = getattr(obj, "permissions", None)
    if isinstance(p, str):
        if p.strip().lower() == "read":
            return "⚠️ قراءة فقط"
        elif p.strip().lower() == "read_write":
            return "قراءة وكتابة ✍️"
        elif p.strip().lower() == "full":
            return "كامل 🔒"
        elif p.strip().lower() == "owner":
            return "مالك 👑"
    return "غير معروف"

@dp.message(Command("account"))
async def status_command(message: types.Message) -> None:
    try:
        token_id = str(message.chat.id)
        chat_user = await chat_user_manager.get(token_id)
        if not chat_user:
            await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
            return
        networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
        owner_networks = [n for n in networks if _is_owner_perm(n)]
        owner_active_networks = [n for n in owner_networks if _is_active_network(n)]
        owner_deactive_networks = [n for n in owner_networks if not _is_active_network(n)]
        partnered_networks = [n for n in networks if not _is_owner_perm(n)]
        partnered_active_networks = [n for n in partnered_networks if _is_active_network(n)]
        partnered_deactive_networks = [n for n in partnered_networks if not _is_active_network(n)]
        selected_network = await selected_network_manager.get(token_id)

        def _rtl_wrap(text: str,bold = False) -> str:
            """Force right-to-left display even if text includes LTR parts."""
            RLI = "\u2067"  # Right-to-Left isolate
            PDI = "\u2069"  # Pop directional isolate
            RLM = "\u200F"  # Right-to-left mark
            return f"{RLM}{RLI}{text}{PDI}"

        def _format_network_line(net) -> str:
            net_id = net.get("network_id") if isinstance(net, dict) else getattr(net, "network_id", None)
            
            perm = _get_network_permisssions_str(net)
            name = (net.get("network_name") if isinstance(net, dict) else getattr(net, "network_name", "")) or ""
            bold_name = f"<b>{escape(str(name))}</b>"
            status_icon = "🟢" if _is_active_network(net) else "🔴"
            raw_line = f"🔹 {status_icon} 🆔 {net_id} • {bold_name} • {perm}"
            return _rtl_wrap(raw_line)

        def _format_block(nets: list) -> str:
            if not nets:
                return "لا توجد"
            return "\n".join(_format_network_line(n) for n in nets)
        
        
        # # Count users associated with this token
        # resp_users = await get_all_users_by_network_id(selected_network.network_id)
        # users_list = getattr(resp_users, "data", []) or []
        # logger.debug(f"[status] found {len(users_list)} users for network {selected_network.network_id}")
        # logger.debug(f"[status] users data: {users_list}")
        # user_count = len(users_list)

        # # Active users are those with status 'active' in the token's users
        # active_count = sum(1 for u in users_list if str(u.get('status', '')) == 'حساب نشط')
        # no_balance_count = sum(1 for u in users_list if str(u.get('status', '')) == 'بلا رصيد')
        # inactive_count = sum(1 for u in users_list if str(u.get('status', '')) == 'فصلت الخدمة')
        

        frame_top = "╔═══════════⋆⋆⋆═══════════╗"
        frame_mid = "╚═══════════⋆⋆⋆═══════════╝"
        box_top = "╭───────────❖────────────╮"
        box_bottom = "╰───────────❖────────────╯"

        lines = [
            "📜 <b>تقرير حسابك وشبكاتك</b>",
            frame_top,
            _rtl_wrap(f"🔹 الشبكة النشطة: <b>{selected_network.network_name if selected_network else 'لا توجد شبكة نشطة'}</b>"),
            _rtl_wrap(f"🔹 اسم المشترك: <b>{chat_user.user_name if chat_user else 'غير معروف'}</b>"),
            _rtl_wrap(f"🔹 معرف المشترك: <b>{chat_user.chat_user_id}</b>"),
            frame_mid,
            "",
            "🌟 <b>شبكاتي الأساسية</b>",
            box_top,
            _format_block(owner_networks),
            box_bottom,
            "",
            "🤝 <b>شبكات الشركاء</b>",
            box_top,
            _format_block(partnered_networks),
            box_bottom,
            "",
            "💡 لتحديث الشبكة النشطة استخدم /networks",
        ]

        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.error(f"[status] error for token {token_id}: {e}")
        await message.answer("⚠️ لا يمكن قراءة حالة النظام حالياً.")


@dp.message(Command("allusers"))
async def allusers_command(message: types.Message, command: CommandObject) -> None:
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    network= await selected_network_manager.get(token_id)
    network_id = network.network_id if network else None
    if not network_id:
        await message.answer(" لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
        return

    try:
        users = await UserManager.get_all_users_data_by_network_id(network_id)
        if not users:
            await message.answer("📭 No users found under your account.")
            return

        # users is a list of dicts from the DB. format_all_users_summary expects
        # a list of (username, account_dict) tuples where account_dict contains
        # latest account fields (available_balance, expiry_date, status, ...).
        # Fetch latest account data for each user concurrently.
        tasks = []
        for u in users:
            tasks.append(UserManager.get_latest_account_data(u.get('id')))

        latest_results = await asyncio.gather(*tasks, return_exceptions=True)

        users_data = []
        for u, latest in zip(users, latest_results):
            acc = {}
            if isinstance(latest, Exception) or latest is None:
                # fall back to the user row data
                acc = {**u}
            else:
                # merge user row and latest account data
                acc = {**u, **latest}
            users_data.append((u.get('username', ''), acc))

        reply = BotUtils.format_all_users_summary(users_data)
        max_len = 4000

        if len(reply) <= max_len:
            await message.answer(reply)
        else:
            for i in range(0, len(reply), max_len):
                await message.answer(reply[i:i+max_len])

    except Exception as e:
        logger.error(f"[allusers] error for network {network_id}: {e}")
        await message.answer("❌ Failed to fetch your users.")


@dp.message(Command("about"))
async def about_command(message: types.Message) -> None:
    await message.answer(
        "🤖 <b>بوت استعلامات يمن نت | إدارة ومراقبة ADSL باحترافية</b>\n\n"
        "منصة موثوقة لملاك ومديري شبكات الانترنت: متابعة لحظية، صلاحيات دقيقة، وتنبيهات مبكرة لحماية الخدمة واستمراريتها.\n\n"
        "<b>مميزات البوت</b>\n"
        "• مراقبة فورية للرصيد والأيام المتبقية مع تحديث آمن للبيانات.\n"
        "• تقارير مصوّرة وجداول إرسال تلقائية بملخصات مختصرة.\n"
        "• إدارة شبكات متعددة وخطوط ADSL مع صلاحيات مالك/شريك واضحة (قراءة، كتابة، كامل، مالك).\n"
        "• تنبيهات مبكرة قابلة للضبط عند حدود التحذير والخطر للرصيد أو الأيام.\n\n"
        "<b>كيفية الاستخدام السريع</b>\n"
        "• /start للتسجيل والتأكد من حالة الحساب وتعيين شبكة نشطة.\n"
        "• /help لقراءة دليل الاستخدام التفصيلي: خطوات، صلاحيات، تقارير...\n"
        "• /networks لإدارة الشبكات والشركاء وتبديل الشبكة النشطة.\n"
        "• /reports للتقارير الفورية أو التاريخية لجميع الشبكات أو شبكة محددة.\n\n"
        "<b>الدعم والتواصل</b>\n"
        "للاستفسارات أو الدعم الفني، راسل فريق الإدارة عبر <b>@mig0_0</b>.\n",
        parse_mode="HTML"
    )

def _build_mysummary_now_keyboard(network):
    if network:
        rows = [
            [InlineKeyboardButton(text=f"📡 {network.network_name}", callback_data="mysummary_selected_network")],
            [InlineKeyboardButton(text="🌐 كل الشبكات", callback_data="mysummary_all_networks")],
            [InlineKeyboardButton(text="🔄 اختر شبكة", callback_data="mysummary_choose_network")]
        ]
    else:
        rows = [
            [InlineKeyboardButton(text="🌐 كل الشبكات", callback_data="mysummary_all_networks")],
            [InlineKeyboardButton(text="🔄 اختر شبكة", callback_data="mysummary_choose_network")]
        ]
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="reports")])
    rows.append([InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(Command("reports"))
async def mysummary_command(message: types.Message, command: Optional[CommandObject] = None):
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await message.answer("لاتوجد شبكات مرتبطة بحسابك. الرجاء إضافة شبكة أولاً.")
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await message.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة")
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚡ تقارير فورية", callback_data="mysummary_now")],
        [InlineKeyboardButton(text="🗓️ تقارير قديمة", callback_data="mysummary_reportdate")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")],
    ])
    await message.answer("📊 اختر نوع التقرير:", reply_markup=kb)


@dp.callback_query(F.data == "mysummary_now")
async def mysummary_now_cb(call: types.CallbackQuery):
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await call.answer("لاتوجد شبكات مرتبطة بحسابك. الرجاء إضافة شبكة أولاً.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    network = await selected_network_manager.get(token_id)
    kb = _build_mysummary_now_keyboard(network)
    try:
        try:
            await call.message.delete()
        except Exception:
            pass
        await call.message.answer("📊 اختر نوع التقرير الذي تريده:", reply_markup=kb)
    except Exception:
        await call.message.answer("📊 اختر نوع التقرير الذي تريده:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "mysummary_reportdate")
async def mysummary_reportdate_cb(call: types.CallbackQuery):
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer()
    # Reuse the interactive reportdate flow for historical reports
    await reportdate_command(call.message, command=None)


@dp.message(Command("reportdate"))
async def reportdate_command(message: types.Message, command: Optional[CommandObject] = None) -> None:
    """Start an interactive date picker to fetch historical reports from adsl_daily_reports."""
    uid = message.from_user.id
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    selected_network = await selected_network_manager.get(token_id)
    
    
    reportdate_sessions[uid] = {"scope": None, "network_id": None}

    if selected_network:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(selected_network) else "🤝"} 🌐 {selected_network.network_name if selected_network else ''}", callback_data="reportdate_scope_current")],
            [InlineKeyboardButton(text="🌐 اختيار شبكة", callback_data="reportdate_scope_choose")],
            [InlineKeyboardButton(text="🌀 كل شبكاتي", callback_data="reportdate_scope_all")],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="close_settings")],
        ])
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🌐 اختيار شبكة", callback_data="reportdate_scope_choose")],
            [InlineKeyboardButton(text="🌀 كل شبكاتي", callback_data="reportdate_scope_all")],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="close_settings")],
        ])
    await message.answer("📅 اختر نطاق الشبكات لتقرير التاريخ:", reply_markup=kb)
    
@dp.callback_query(F.data == "mysummary_selected_network")
async def mysummary_selected_network_cb(call: types.CallbackQuery):
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        await call.answer()
    except Exception:
        pass
    network = await selected_network_manager.get(str(call.from_user.id))
    if not network:
        await call.message.edit_text("❌ لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
        return
    await call.message.edit_text(f"📡 جاري تجهيز تقرير شبكة {network.network_name} ...")
    # Auto-delete the callback message after 2 seconds
    try:
        import asyncio as _asyncio
        _asyncio.create_task(_delete_message_after(call.message, 2))
    except Exception:
        pass
    await _send_mysummary_for_selected_network(call.message, network)
    # Avoid answering the same callback twice; late answers can throw TelegramBadRequest

@dp.callback_query(F.data == "mysummary_all_networks")
async def mysummary_all_networks_cb(call: types.CallbackQuery):
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    await call.answer() 
    await call.message.edit_text("🌐 جاري تجهيز تقرير كل الشبكات...")
    # Auto-delete the callback message after 2 seconds
    try:
        import asyncio as _asyncio
        _asyncio.create_task(_delete_message_after(call.message, 2))
    except Exception:
        pass
    await _send_mysummary_for_all_networks(call.message)

@dp.callback_query(F.data == "mysummary_choose_network")
async def mysummary_choose_network_cb(call: types.CallbackQuery):
    await call.answer()
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await call.answer("لاتوجد شبكات مرتبطة بحسابك. الرجاء إضافة شبكة أولاً.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {n['network_name']} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})", callback_data=f"mysummary_network_{n['id']}")]
            for n in active_networks
        ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="reports")]] + [[InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]]
    )
    await call.message.edit_text("🔄 اختر الشبكة التي تريد تقريرها:", reply_markup=kb)


# -------- reportdate helpers --------
async def _collect_available_dates_for_networks(networks: list) -> set[str]:
    user_ids = []
    for net in networks:
        net_obj = _ensure_selected_network(net)
        if not net_obj.network_id:
            continue
        try:
            users = await UserManager.get_users_by_network(net_obj.network_id)
        except Exception:
            users = []
        user_ids.extend([u.get("id") for u in users if u.get("id")])
    if not user_ids:
        return set()
    dates = await UserManager.get_available_report_dates(user_ids, limit=180)
    return set(dates)


def _build_calendar(year: int, month: int, available_dates: Optional[set[str]] = None) -> InlineKeyboardMarkup:
    cal = calendar.Calendar(firstweekday=0)
    month_days = list(cal.itermonthdays(year, month))
    header = f"📅 {year}-{month:02d}"
    rows = [[InlineKeyboardButton(text=header, callback_data="noop")]]
    week_names = ["ن", "ا", "ث", "ر", "خ", "ج", "س"]
    rows.append([InlineKeyboardButton(text=d, callback_data="noop") for d in week_names])
    week = []
    for i, day in enumerate(month_days, start=1):
        if day == 0:
            week.append(InlineKeyboardButton(text=" ", callback_data="noop"))
        else:
            date_str = f"{year}-{month:02d}-{day:02d}"
            is_available = (available_dates is None) or (date_str in available_dates)
            label = f"●{day}" if available_dates is not None and is_available else str(day)
            cb = f"reportdate_day_{date_str}" if is_available else "noop"
            week.append(InlineKeyboardButton(text=label, callback_data=cb))
        if i % 7 == 0:
            rows.append(week)
            week = []
    if week:
        rows.append(week)
    # navigation
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year -= 1
    next_month = month + 1
    next_year = year
    if next_month == 13:
        next_month = 1
        next_year += 1
    rows.append([
        InlineKeyboardButton(text="⬅️ السابق", callback_data=f"reportdate_nav_{prev_year}-{prev_month:02d}-01"),
        InlineKeyboardButton(text="➡️ التالي", callback_data=f"reportdate_nav_{next_year}-{next_month:02d}-01"),
    ])
    rows.append([InlineKeyboardButton(text="❌ إلغاء", callback_data="close_settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _ensure_selected_network(net) -> SelectedNetwork:
    if isinstance(net, SelectedNetwork):
        return net
    if isinstance(net, dict):
        return SelectedNetwork(
            net.get("id") or net.get("network_id") or 0,
            net.get("network_id") or net.get("id") or 0,
            net.get("network_name") or "",
            net.get("user_name") or net.get("username") or "",
            net.get("times_to_send_reports") or 0,
            net.get("warning_count_remaining_days") or 0,
            net.get("danger_count_remaining_days") or 0,
            net.get("warning_percentage_remaining_balance") or 0,
            net.get("danger_percentage_remaining_balance") or 0,
            net.get("is_network_active") or False,
            net.get("expiration_date") or None,
            net.get("telegram_id") or "",
            net.get("chat_user_id") or 0,
            net.get("network_type") or "",
            net.get("permissions") or "",
        )
    # fallback empty shell to avoid attribute errors; better than crashing
    return SelectedNetwork(0, 0, "", "", 0, 0, 0, 0, 0,False,"", "", 0, "", "")


async def _render_datepicker(target, year: int, month: int, available_dates: Optional[set[str]] = None):
    kb = _build_calendar(year, month, available_dates)
    text = "📅 اختر التاريخ للتقرير:" 
    try:
        await target.edit_text(text, reply_markup=kb)
    except Exception:
        try:
            await target.answer(text, reply_markup=kb)
        except Exception:
            pass


async def _run_reportdate_for_scope(message: types.Message, scope: str, report_date: str, selected_network_id: Optional[int] = None):
    picker_message = message
    try:
        token_id = str(message.chat.id)
        chat_user = await chat_user_manager.get(token_id)
        if not chat_user:
            await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
            return
        if not chat_user.is_active:
            await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
            return

        networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
        if not networks:
            await message.answer("📭 لا توجد شبكات مرتبطة بحسابك.")
            return
        active_networks = [n for n in networks if n.get("is_network_active", False)]
        if not active_networks:
            await message.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة")
            return

        target_networks = []
        if scope == "current":
            net = await selected_network_manager.get(token_id)
            if not net:
                await message.answer("❌ لا يوجد شبكة محددة.")
                return
            target_networks = [net]
        elif scope == "choose":
            net = next((n for n in active_networks if (n.get("id") if isinstance(n, dict) else getattr(n, "id", None)) == selected_network_id), None)
            if not net:
                await message.answer("❌ الشبكة غير موجودة.")
                return
            target_networks = [net]
        else:  # all
            target_networks = active_networks

        for net in target_networks:
            net_obj = _ensure_selected_network(net)
            net_id = net_obj.network_id
            net_name = net_obj.network_name
            users = await UserManager.get_users_by_network(net_id)
            if not users:
                await message.answer(f"📭 لا توجد خطوط للشبكة {net_name}.")
                continue
            user_ids = [u.get("id") for u in users if u.get("id")]
            if not user_ids:
                await message.answer(f"📭 لا توجد معرفات خطوط صالحة للشبكة {net_name}.")
                continue
            rows = await UserManager.get_daily_reports_for_users(user_ids, report_date)
            if not rows:
                await message.answer(f"📭 لا توجد بيانات للتاريخ {report_date} للشبكة {net_name}.")
                continue
            reports = []
            for row in rows:
                adsl = row.get("adsl_number") or row.get("username") or row.get("user_id")
                payload = {
                    "order_index": row.get("order_index"),
                    "plan_limit": row.get("plan_limit") or row.get("data_limit"),
                    "plan_price": row.get("plan_price"),
                    "gb_price": row.get("gb_price"),
                    "account_status": row.get("account_status"),
                    "yesterday_balance": row.get("yesterday_balance"),
                    "today_balance": row.get("today_balance"),
                    "usage": row.get("usage"),
                    "usage_value": row.get("usage_value"),
                    "finishing_balance_estimate": row.get("finishing_balance_estimate"),
                    "remaining_days": row.get("remaining_days"),
                    "balance_value": row.get("balance_value"),
                    "notes": row.get("notes"),
                }
                reports.append((adsl, payload))

            # Sort reports based on chat_user.order_by before generating images
            try:
                order_by_opt = (getattr(chat_user, "order_by", "usage") or "usage").strip()
            except Exception:
                order_by_opt = "usage"

            # Helper functions for robust numeric and ADSL sorting
            import re as _re
            def _number(val, default: float) -> float:
                if val is None:
                    return default
                if isinstance(val, (int, float)):
                    return float(val)
                if isinstance(val, str):
                    match = _re.search(r"-?\d+(?:\.\d+)?", val.replace(",", ""))
                    if match:
                        try:
                            return float(match.group(0))
                        except Exception:
                            pass
                return default

            def _adsl_sort_key(item):
                uname, data = item
                adsl_val = data.get("adsl_number") or uname or ""
                if isinstance(adsl_val, str) and adsl_val.strip().isdigit():
                    try:
                        return int(adsl_val.strip())
                    except Exception:
                        return adsl_val
                return adsl_val

            if order_by_opt == "usage":
                reports = sorted(reports, key=lambda item: _number(item[1].get("usage"), float("-inf")), reverse=True)
            elif order_by_opt == "remaining_days":
                reports = sorted(reports, key=lambda item: _number(item[1].get("remaining_days"), float("inf")))
            elif order_by_opt == "balance":
                reports = sorted(reports, key=lambda item: _number(item[1].get("balance_value", item[1].get("balance")), float("-inf")), reverse=True)
            elif order_by_opt == "adsl_order_index":
                reports = sorted(reports, key=lambda item: _number(item[1].get("order_index"), float("inf")))
            else:  # adsl_number
                reports = sorted(reports, key=_adsl_sort_key)

            waiting = await message.answer(f"📊 جاري تجهيز تقرير {report_date} للشبكة {net_name}...")
            try:
                loop = __import__('asyncio').get_running_loop()
                image_paths, out_dir = await loop.run_in_executor(EXEC, lambda: generate_images(reports, net_obj, chat_user,report_date))
            except Exception as e:
                logger.exception("Failed to generate historical report images: %s", e)
                try:
                    await waiting.edit_text("❌ فشل إنشاء التقرير للتاريخ المحدد.")
                except Exception:
                    pass
                continue

            try:
                try:
                    tz = ZoneInfo("Asia/Aden")
                except Exception:
                    tz = pytz.timezone("Asia/Aden")
                result = await send_images(bot, net_obj, token_id, image_paths, reports, tz, cleanup_dir=out_dir, sendToAdmin=False, isDailyReport=False, report_date=report_date)
                if result.get("sent", 0) == 0 and not result.get("chat_not_found"):
                    await bot.send_message(chat_id=int(token_id), text="⚠️ لم يتم إرسال أي صفحات من التقرير.")
            finally:
                try:
                    await waiting.delete()
                except Exception:
                    pass
    finally:
        try:
            await picker_message.delete()
        except Exception:
            pass

@dp.callback_query(F.data == "reports")
async def mysummary_back_callback(call: types.CallbackQuery):
    # Remove current menu/message, then show the reports menu
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        await call.message.delete()
    except Exception:
        pass
    try:
        await mysummary_command(call.message)
    except Exception:
        pass
    await call.answer()


@dp.callback_query(F.data == "reportdate_scope_current")
async def reportdate_scope_current(call: types.CallbackQuery):
    uid = call.from_user.id
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    net = await selected_network_manager.get(token_id)
    if not net:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        return
    available_dates = await _collect_available_dates_for_networks([net])
    if not available_dates:
        await call.answer(f"📭 لا توجد تقارير متاحة لــ{net.network_name if hasattr(net, 'network_name') else ''}.", show_alert=True)
    reportdate_sessions[uid] = {"scope": "current", "network_id": net.id if hasattr(net, "id") else None, "available_dates": available_dates}
    today = datetime.now().date()
    await _render_datepicker(call.message, today.year, today.month, available_dates if available_dates else None)
    await call.answer()


@dp.callback_query(F.data == "reportdate_scope_all")
async def reportdate_scope_all(call: types.CallbackQuery):
    uid = call.from_user.id
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    nets = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not nets:
        await call.answer("📭 لا توجد شبكات مرتبطة بحسابك.", show_alert=True)
        return
    active_networks = [n for n in nets if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    available_dates = await _collect_available_dates_for_networks(active_networks)
    if not available_dates:
        await call.answer("📭 لا توجد تواريخ متاحة في التقارير لهذا النطاق.", show_alert=True)
    reportdate_sessions[uid] = {"scope": "all", "network_id": None, "available_dates": available_dates}
    today = datetime.now().date()
    await _render_datepicker(call.message, today.year, today.month, available_dates if available_dates else None)
    await call.answer()


@dp.callback_query(F.data == "reportdate_scope_choose")
async def reportdate_scope_choose(call: types.CallbackQuery):
    uid = call.from_user.id
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    nets = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not nets:
        await call.answer("📭 لا توجد شبكات.", show_alert=True)
        return
    active_networks = [n for n in nets if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'🌟' if _is_owner_perm(n) else '🤝'} 🌐 {n['network_name']}", callback_data=f"reportdate_choose_{n['id']}")]
        for n in active_networks
    ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="close_settings")]])
    await call.message.edit_text("🌐 اختر الشبكة للتقرير التاريخي:", reply_markup=kb)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("reportdate_choose_"))
async def reportdate_choose_network(call: types.CallbackQuery):
    uid = call.from_user.id
    try:
        nid = int(call.data.split("_")[-1])
    except Exception:
        await call.answer("❌ خطأ في اختيار الشبكة.", show_alert=True)
        return
    token_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    nets = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    active_networks = [n for n in nets if n.get("is_network_active", False)]
    net_obj = next((n for n in active_networks if (n.get("id") if isinstance(n, dict) else getattr(n, "id", None)) == nid), None)
    if not net_obj:
        await call.answer("❌ الشبكة غير موجودة.", show_alert=True)
        return
    available_dates = await _collect_available_dates_for_networks([net_obj] if net_obj else [])
    if not available_dates:
        await call.answer("📭 لا توجد تواريخ متاحة في التقارير لهذا النطاق.", show_alert=True)
    reportdate_sessions[uid] = {"scope": "choose", "network_id": nid, "available_dates": available_dates}
    today = datetime.now().date()
    await _render_datepicker(call.message, today.year, today.month, available_dates if available_dates else None)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("reportdate_nav_"))
async def reportdate_nav(call: types.CallbackQuery):
    try:
        _, _, date_str = call.data.split("_", 2)
        y, m, _ = date_str.split("-")
        year = int(y); month = int(m)
    except Exception:
        await call.answer()
        return
    sess = reportdate_sessions.get(call.from_user.id) or {}
    available_dates = sess.get("available_dates")
    await _render_datepicker(call.message, year, month, available_dates)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("reportdate_day_"))
async def reportdate_pick_day(call: types.CallbackQuery):
    uid = call.from_user.id
    sess = reportdate_sessions.get(uid) or {}
    scope = sess.get("scope")
    network_id = sess.get("network_id")
    if not scope:
        await call.answer("⚠️ اختر الشبكة أولاً.", show_alert=True)
        return
    try:
        date_str = call.data[len("reportdate_day_"):]
        from datetime import datetime as _dt
        _dt.strptime(date_str, "%Y-%m-%d")
    except Exception:
        await call.answer("⚠️ تاريخ غير صالح.", show_alert=True)
        return
    await call.answer()
    await _run_reportdate_for_scope(call.message, scope, date_str, network_id)

@dp.callback_query(lambda c: c.data.startswith("mysummary_network_"))
async def mysummary_network_cb(call: types.CallbackQuery):
    network_id = int(call.data.split("_")[-1])
    try:
        await call.answer()
    except Exception:
        pass
    logger.info("reports invoked for specific network id=%d", network_id)
    await call.message.edit_text("📡 جاري تجهيز تقرير الشبكة المختارة...")
    # Auto-delete the callback message after 2 seconds
    try:
        import asyncio as _asyncio
        _asyncio.create_task(_delete_message_after(call.message, 2))
    except Exception:
        pass
    await _send_mysummary_for_network(call.message, network_id)
    # Avoid duplicate answers to prevent 'query is too old' errors

async def _send_mysummary_for_selected_network(message: types.Message, network):
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    await mysummary_command_core(message, network, chat_user, token_id)

async def _send_mysummary_for_all_networks(message: types.Message):
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await message.answer("لاتوجد شبكات مرتبطة بحسابك. الرجاء إضافة شبكة أولاً.")
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await message.answer("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة")
        return
    # Run all summaries concurrently
    tasks = [
        mysummary_command_core(message, network, chat_user, token_id)
        for network in active_networks
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

async def _send_mysummary_for_network(message: types.Message, network_id: int):
    token_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(token_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    network = await UserManager.get_network_by_id(network_id)
    if not network:
        await message.answer("❌ الشبكة غير موجودة.")
        return
    if not _is_active_network(network):
        await message.answer("❌ لا يمكنك الحصول على تقارير لهذه الشبكة لأنها غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة")
        return
    await mysummary_command_core(message, network, chat_user, token_id)
# You can refactor your existing reports logic into this core function:
async def mysummary_command_core(message: types.Message, network, chat_user,token_id: str) -> None:
    def ensure_selected_network(network):
        if isinstance(network, SelectedNetwork):
            return network
        # Try to convert dict to SelectedNetwork
        logger.info("Ensuring selected network from dict: %s", network)
        return SelectedNetwork(
            id = network.get("id"),
            network_id=_safe_int(network.get("network_id"), 0),
            network_name=network.get("network_name"),
            user_name=network.get("user_name", ""),
            # Ensure numeric types for scheduling and thresholds
            times_to_send_reports=_safe_int(network.get("times_to_send_reports", 15), 15),
            danger_percentage_remaining_balance=_safe_int(network.get("danger_percentage_remaining_balance", DEFAULT_DANGER_BALANCE), DEFAULT_DANGER_BALANCE),
            warning_percentage_remaining_balance=_safe_int(network.get("warning_percentage_remaining_balance", DEFAULT_WARNING_BALANCE), DEFAULT_WARNING_BALANCE),
            danger_count_remaining_days=_safe_int(network.get("danger_count_remaining_days", DEFAULT_DANGER_DAYS), DEFAULT_DANGER_DAYS),
            warning_count_remaining_days=_safe_int(network.get("warning_count_remaining_days", DEFAULT_WARNING_DAYS), DEFAULT_WARNING_DAYS),
            is_active=network.get("is_network_active", True),
            expiration_date=network.get("expiration_date", None),
            telegram_id=str(network.get("telegram_id", "")),
            chat_user_id=_safe_int(network.get("chat_user_id", 0), 0),
            network_type=network.get("network_type", ""),
            permissions=network.get("permissions", "")
        )
    try:
        network = ensure_selected_network(network)
        network_id = network.network_id
        network_name = network.network_name
        source_users = await UserManager.get_users_by_network(network_id)
        waiting = await message.answer(f"📊 جاري اعداد تقرير شبكة {network_name}")
        logger.info("reports fetched %d users for network=%s", len(source_users or []), network_name)
        if not source_users:
            await waiting.edit_text(f"لا توجد اي خطوط مضافة للشبكة '{network_name}'.\n الرجاء اضافة خطوط اولاً.")
            return

        collected_users: List[Dict[str, Any]] = []
        add_log(f"mysummary_start")
        async def fetch_and_collect(u: dict) -> None:
            try:
                # async with SCRAPE_SEMAPHORE:
                try:
                    await asyncio.wait_for(save_scraped_account(u["username"], network_id), timeout=30)
                except asyncio.TimeoutError:
                    logger.warning("Timeout while saving scraped account %s", u.get("username"))
                except Exception:
                    logger.debug("Failed to fetch/save live for %s", u.get("username"), exc_info=True)

                latest = await UserManager.get_latest_account_data(u["id"])
                if latest:
                    collected_users.append({
                        "id": u["id"],
                        "adsl_number": u["adsl_number"],
                        "username": u["username"],
                        "status": u.get("status", ""),
                        "order_index" : u.get("order_index", 0),
                    })
            except Exception:
                logger.exception("Error in fetch_and_collect for %s %s",escape_markdown(network_name), u.get("username"))

        tasks = [asyncio.create_task(fetch_and_collect(u)) for u in source_users]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        sem_users = asyncio.Semaphore(24)
        reports = await collect_saved_user_reports(collected_users, sem_users, UserManager, chat_user.order_by)
        if not reports:
            await waiting.edit_text(f"لا توجد بيانات متاحة للتقارير في الشبكة '{network_name}'.")
            return
        # Use the current running loop to run blocking image generation in the executor
        loop = __import__('asyncio').get_running_loop()
        # generate_images returns (images, out_dir) and writes into a unique invocation directory
        try:
            image_paths, out_dir = await loop.run_in_executor(EXEC, lambda: generate_images(reports,network,chat_user))
        except Exception as e:
            logger.exception("Failed to generate report images for reports: %s", e)
            try:
                await waiting.edit_text("❌ Error generating your summary. Please try again later.")
            except Exception:
                pass
            return

        # delegate sending and cleanup to report_sender.send_images which handles retries and atomic cleanup
        try:
            await waiting.delete()
            try:
                tz = ZoneInfo("Asia/Aden")
            except Exception:
                tz = pytz.timezone("Asia/Aden")
            result = await send_images(bot,network, token_id, image_paths, reports, tz, cleanup_dir=out_dir, sendToAdmin=False,isDailyReport=False)
            # Optionally inform the user about the result
            try:
                if result.get('sent', 0) == 0 and not result.get('chat_not_found'):
                    await bot.send_message(chat_id=int(token_id), text="⚠️ لم يتم إرسال أي صفحات من التقرير.")
            except Exception:
                pass
        except Exception as e:
            logger.exception("reports send_images failed: %s", e)
            try:
                await waiting.edit_text("❌ Error sending your summary. Please try again later.")
            except Exception:
                pass
        add_log(f"mysummary_end")
    except Exception as e:
        logger.exception("reports command error: %s", e)
        try:
            await waiting.edit_text("❌ Error generating your summary. Please try again later.")
        except Exception:
            pass

# @dp.message(Command("allsummary"))
# async def allsummary_command(message: types.Message):
#     netowrk = await selected_network_manager.get(str(message.chat.id))
#     if not netowrk:
#         await message.answer(" لا يوجد شبكة محددة. الرجاء تحديد شبكة أولاً.")
#         return
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
def escape_markdown(text: str) -> str:
    for ch in ('_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!'):
        text = text.replace(ch, f'\\{ch}')
    return text

# Helper to delete a message after a delay
async def _delete_message_after(message: types.Message, seconds: float = 2.0):
    try:
        import asyncio as _asyncio
        await _asyncio.sleep(seconds)
        try:
            await message.delete()
        except Exception:
            pass
    except Exception:
        pass

@dp.message(Command("settings"))
async def settings_handler(message: types.Message):
    telegram_id = str(message.chat.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await message.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
        return
    if not chat_user.is_active:
        await message.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
        return
    
    network = await selected_network_manager.get(telegram_id)

    receive_partnered = chat_user.receive_partnered_reports if chat_user else False

    if not chat_user or not chat_user.user_name:
        await message.answer("⚠️ لم يتم تسجيلك بعد. استخدم /start للتسجيل.")
        return

    order_by_labels = {
        "usage": "الاستخدام (الأعلى أولاً)",
        "remaining_days": "الأيام المتبقية (الأقل أولاً)",
        "balance": "الرصيد (الأعلى أولاً)",
        "adsl_number": "رقم ADSL (تصاعدي)",
        "adsl_order_index": "ترتيب مخصص لكل ADSL",
    }
    current_order_by = chat_user.order_by if chat_user else "usage"
    order_label = order_by_labels.get(current_order_by, order_by_labels["usage"])

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ تعديل اسم المشترك", callback_data="set_name")],
        [InlineKeyboardButton(text="🕒 تعديل مواعيد التقارير", callback_data="set_report_times")],
        [InlineKeyboardButton(text=("📥 تقارير الشركاء: مفعّل ✅" if receive_partnered else "📥 تقارير الشركاء: متوقف ⛔"), callback_data="toggle_partner_reports")],
        [InlineKeyboardButton(text=f"📑 ترتيب الخطوط في التقارير: {order_label}", callback_data="set_order_by")],
        [InlineKeyboardButton(text="⚠️ تعديل إعدادات التحذير والخطر", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])

    user_name = escape_markdown(chat_user.user_name)
    network_name = escape_markdown(network.network_name if network else "غير محددة")
    await message.answer(
        f"المستخدم ({user_name}) | الشبكة النشطة: {network_name}\n⚙️ *إعدادات المستخدم*\nاختر العملية المطلوبة:",
        reply_markup=kb,
        parse_mode="Markdown"
)


def _order_by_options():
    return {
        "usage": "الاستخدام (الأعلى أولاً)",
        "remaining_days": "الأيام المتبقية (الأقل أولاً)",
        "balance": "الرصيد (الأعلى أولاً)",
        "adsl_number": "رقم ADSL (تصاعدي)",
        "adsl_order_index": "ترتيب مخصص لكل ADSL",
    }


def _order_by_keyboard(current: str) -> InlineKeyboardMarkup:
    labels = _order_by_options()
    rows = []
    for key, label in labels.items():
        prefix = "✅ " if key == current else ""
        rows.append([InlineKeyboardButton(text=f"{prefix}{label}", callback_data=f"order_by_{key}")])
    # New: manage per-ADSL order index within a selected owner's network
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")])
    rows.append([InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_order_by_menu(target_message: types.Message, current: str, note: Optional[str] = None):
    labels = _order_by_options()
    current_label = labels.get(current, labels.get("usage"))
    header = f"اختر ترتيب الخطوط في التقارير:\nالحالي: {current_label}"
    text = f"{note}\n\n{header}" if note else header
    try:
        await target_message.edit_text(text, reply_markup=_order_by_keyboard(current))
    except Exception:
        # If edit fails (e.g., message deleted), try sending a new one
        try:
            await target_message.answer(text, reply_markup=_order_by_keyboard(current))
        except Exception:
            pass


@dp.callback_query(F.data == "set_order_by")
async def set_order_by_callback(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط.", show_alert=True)
        return

    current = chat_user.order_by if chat_user else "usage"
    await _render_order_by_menu(call.message, current)
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("order_by_"))
async def order_by_choice_callback(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط.", show_alert=True)
        return

    choice = call.data.replace("order_by_", "", 1)
    labels = _order_by_options()
    if choice not in labels:
        await call.answer("❌ اختيار غير صالح", show_alert=True)
        return

    ok = await chat_user_manager.change_order_by(telegram_id, choice)
    note = "✅ تم تحديث الترتيب" if ok else "⚠️ تعذر حفظ الترتيب الآن"
    current = choice if ok else (chat_user.order_by if chat_user else "usage")
    await _render_order_by_menu(call.message, current, note=note)
    await call.answer("تم الحفظ" if ok else "لم يتم التحديث", show_alert=not ok)

# ----- ADSL order index management -----
@dp.callback_query(F.data == "order_index_networks")
async def order_index_networks_cb(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط.", show_alert=True)
        return

    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await call.message.edit_text("📭 لا توجد شبكات مرتبطة بحسابك.")
        await call.answer()
        return
    active_owner_networks = [n for n in networks if n.get("is_network_active", False) and _is_owner_perm(n)]
    if not active_owner_networks:
        await call.message.edit_text("❌ لا توجد شبكات مملوكة ومفعلة لتعديل ترتيب ADSL.")
        await call.answer()
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🌟 🌐 {escape_markdown(n['network_name'])}", callback_data=f"order_index_network_{n['network_id']}")]
        for n in active_owner_networks
    ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_order_by")],
         [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]])

    await call.message.edit_text("🌐 اختر الشبكة لتعديل ترتيب خطوط ADSL:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("order_index_network_"))
async def order_index_pick_network(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط.", show_alert=True)
        return

    try:
        network_id = int(call.data.split("_")[-1])
    except Exception:
        await call.answer("❌ اختيار شبكة غير صالح.", show_alert=True)
        return

    await state.update_data(order_index_network_id=network_id)

    rows = await UserManager.get_adsls_order_indexed(network_id)
    if not rows:
        await call.message.edit_text("📭 لا توجد خطوط ADSL لهذه الشبكة أو لا توجد بيانات ترتيب.")
        await call.answer()
        return

    kb_rows = [
        [InlineKeyboardButton(text=f"🔢 {adsl} — ترتيب: {idx if idx > -1 else 'غير محدد'}", callback_data=f"order_index_select_{aid}")]
        for (aid, adsl, idx) in rows
    ]
    kb_rows += [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="order_index_networks")],
                [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]]
    await call.message.edit_text("📑 اختر ADSL لتعديل رقم ترتيبه:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("order_index_select_"))
async def order_index_select_adsl(call: types.CallbackQuery, state: FSMContext):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط.", show_alert=True)
        return

    adsl_id = call.data.replace("order_index_select_", "", 1)
    data = await state.get_data()
    network_id = data.get("order_index_network_id")
    current_idx = await UserManager.get_adsl_order_index(adsl_id)

    # Set state to accept next numeric input as new order index
    user_settings_state[call.from_user.id] = f"awaiting_adsl_order_index_{adsl_id}"
    prompt = (f"🔢 أدخل رقم الترتيب الجديد لـ ADSL (الحالي: {current_idx if current_idx > -1 else 'غير محدد'})\n"
              f"📡 الشبكة: #{network_id}" if current_idx is not None else
              f"🔢 أدخل رقم الترتيب الجديد لـ ADSL\n📡 الشبكة: #{network_id}")
    try:
        await call.message.edit_text(prompt)
    except Exception:
        try:
            await call.message.answer(prompt)
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "change_active_network")
async def change_active_network(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)

    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
    if not networks:
        await call.message.edit_text("❌ لا توجد شبكات مرتبطة بحسابك.")
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.message.edit_text("❌ لا توجد شبكات مفعلة مرتبطة بحسابك.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة")
        return
    
    network = await selected_network_manager.get(telegram_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"{"🌟" if _is_owner_perm(n) else "🤝"} 🌐 {escape_markdown(n['network_name'])} ({f'{n['adsls_count']}' if n.get('adsls_count') is not None else '0'})",
            callback_data=f"select_network_{n['id']}"
        )] for n in active_networks
    ] + [
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")]
    ])

    network_name = escape_markdown(network.network_name if network else "غير محددة")
    await call.message.edit_text(
        "الشبكة النشطة ({})\n🌐 *اختر الشبكة النشطة:*".format(network_name),
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await call.answer()


@dp.callback_query(lambda c: c.data.startswith("select_network_"))
async def select_network_callback(call: types.CallbackQuery, state: FSMContext):
    network_id = int(call.data.split("_")[-1])
    telegram_id = str(call.from_user.id)

    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ المستخدم غير موجود", show_alert=True)
        return

    success = await selected_network_manager.set(
        network_id,
        chat_user.chat_user_id,
        telegram_id=telegram_id
    )
    network = await selected_network_manager.get(telegram_id)
    network_name = escape_markdown(network.network_name if network else "غير محددة")
    if success:
        await call.message.edit_text(f"✅ تم تنشيط الشبكة {network_name} بنجاح.")
    else:
        await call.message.edit_text(f"❌ فشل في تنشيط الشبكة {network_name}.")

    # بعد تغيير الشبكة، اعرض قائمة الشبكات (show_networks)
    try:
        await asyncio.sleep(2)
        try:
            await call.message.delete()
        except Exception:
            pass
        await networks_menu(call.message, state)
    except Exception:
        # كإجراء احتياطي، أجب على الاستدعاء دون إعادة التوجيه
        pass
    await call.answer()


@dp.callback_query(F.data == "toggle_partner_reports")
async def toggle_partner_reports(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("⚠️ لم يتم العثور على المستخدم.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    current = chat_user.receive_partnered_reports if chat_user else False
    new_pref = not current
    try:
        ok = await chat_user_manager.change_receive_partnered_reports(telegram_id, new_pref)
    except Exception as e:
        logger.exception("Error updating partnered reports preference: %s", e)
        ok = False

    if ok:
        # Close current menu then show refreshed settings
        try:
            await call.message.delete()
            await settings_handler(call.message)
        except Exception:
            try:
                # If delete fails (e.g., message too old), attempt inline update
                await settings_handler(call.message)
            except Exception:
                pass
        await call.answer("✅ تم تحديث استقبال تقارير الشركاء." if new_pref else "✅ تم إيقاف استقبال تقارير الشركاء.")
    else:
        await call.answer("❌ تعذر تحديث الإعداد.", show_alert=True)


user_settings_state = {}
user_report_selections = {}
user_warning_danger_prefs: dict[int, dict[str, int]] = {}
# reportdate session cache per user id
reportdate_sessions: dict[int, dict[str, Any]] = {}


REPORT_TIMES = ["06:00:00", "12:00:00", "18:00:00", "23:50:00"]

DEFAULT_WARNING_DAYS = 7
DEFAULT_DANGER_DAYS = 3
DEFAULT_WARNING_BALANCE = 30
DEFAULT_DANGER_BALANCE = 10

def _get_warning_danger_prefs(uid: int) -> dict[str, int]:
    base = {
        "warning_days": DEFAULT_WARNING_DAYS,
        "danger_days": DEFAULT_DANGER_DAYS,
        "warning_balance": DEFAULT_WARNING_BALANCE,
        "danger_balance": DEFAULT_DANGER_BALANCE,
    }
    user_vals = user_warning_danger_prefs.get(uid, {})
    base.update({k: v for k, v in user_vals.items() if isinstance(v, int)})
    return base

def _format_prefs_text(prefs: dict[str, int]) -> str:
    return (
        "⚙️ الإعدادات الحالية:\n"
        f"🟡 أيام التحذير: {prefs['warning_days']}\n"
        f"🔴 أيام الخطر: {prefs['danger_days']}\n"
        f"🟡 رصيد التحذير: {prefs['warning_balance']} %\n"
        f"🔴 رصيد الخطر: {prefs['danger_balance']} %"
    )

async def _persist_warning_danger_settings(user_id: int) -> bool:
    network = await selected_network_manager.get(str(user_id))
    if not network:
        return False
    prefs = _get_warning_danger_prefs(user_id)
    return await selected_network_manager.change_warning_and_danger_settings(
        network,
        prefs["warning_days"],
        prefs["danger_days"],
        prefs["warning_balance"],
        prefs["danger_balance"],
    )

async def _gather_with_concurrency(limit: int, coros: list):
    sem = asyncio.Semaphore(limit)
    async def run(c):
        async with sem:
            return await c
    results = await asyncio.gather(*(run(c) for c in coros), return_exceptions=True)
    # Normalize exceptions to False/None for boolean aggregations
    return [False if isinstance(r, Exception) else r for r in results]

def _format_bulk_change_summary(results: list[dict], title: str) -> str:
    ok = [r for r in results if r.get("ok")]
    fail = [r for r in results if not r.get("ok")]
    lines = [f"📌 {title} — النتائج:"]
    if ok:
        ok_names = ", ".join(r.get("name") or f"#{r.get('id')}" for r in ok)
        lines.append(f"✅ تم التحديث: {ok_names}")
    if fail:
        fail_names = ", ".join(r.get("name") or f"#{r.get('id')}" for r in fail)
        lines.append(f"❌ فشل التحديث: {fail_names}")
    return "\n".join(lines)

async def _persist_warning_danger_settings_to_targets(user_id: int, target_ids: list[int]) -> list[dict]:
    if not target_ids:
        return []
    prefs = _get_warning_danger_prefs(user_id)
    async def update_one(nid: int) -> dict:
        try:
            net_obj = await UserManager.get_network_by_id(int(nid))
            if not net_obj:
                return {"id": nid, "name": f"#{nid}", "ok": False}
            sel_net = SelectedNetwork(
                id=net_obj.get("id"),
                network_id=net_obj.get("network_id"),
                network_name=net_obj.get("network_name"),
                user_name=net_obj.get("user_name", ""),
                times_to_send_reports=net_obj.get("times_to_send_reports", 15),
                danger_percentage_remaining_balance=net_obj.get("danger_percentage_remaining_balance", 10),
                warning_percentage_remaining_balance=net_obj.get("warning_percentage_remaining_balance", 30),
                danger_count_remaining_days=net_obj.get("danger_count_remaining_days", 3),
                warning_count_remaining_days=net_obj.get("warning_count_remaining_days", 7),
                is_active=net_obj.get("is_network_active", False),
                expiration_date=net_obj.get("expiration_date", None),
                telegram_id=str(user_id),
                chat_user_id=net_obj.get("chat_user_id", 0),
                network_type=net_obj.get("network_type", ""),
                permissions=net_obj.get("permissions", "")
            )
            if not _is_active_network(net_obj):
                return {"id": nid, "name": net_obj.get("network_name") or f"#{nid}", "ok": False}
            ok = await selected_network_manager.change_warning_and_danger_settings(
                sel_net,
                prefs["warning_days"],
                prefs["danger_days"],
                prefs["warning_balance"],
                prefs["danger_balance"],
            )
            return {"id": nid, "name": net_obj.get("network_name") or f"#{nid}", "ok": bool(ok)}
        except Exception:
            return {"id": nid, "name": f"#{nid}", "ok": False}

    results = await _gather_with_concurrency(6, [update_one(int(n)) for n in target_ids])
    # results already contain dicts; filter out any non-dict entries defensively
    return [r for r in results if isinstance(r, dict)]

def _make_times_keyboard(user_id: int,return_to_networks_after_times: bool = False) -> InlineKeyboardMarkup:
    sel = user_report_selections.get(user_id, set())
    
    rows = []
    for t in REPORT_TIMES:
        short = t[:5]
        key = t.replace(":", "")
        text = f"✅ {short}" if t in sel else short
        rows.append([InlineKeyboardButton(text=text, callback_data=f"toggle_time_{key}")])
    rows.append([InlineKeyboardButton(text="💾 حفظ", callback_data="save_report_times"),
                 InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_report_times")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data=("settings_back" if not return_to_networks_after_times else "networks_menu"))])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message()
async def catch_settings_input(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    state_name = user_settings_state.get(user_id)
    chat_user = await chat_user_manager.get(str(user_id))
    if not state_name or not chat_user:
        return
    
    def _save_pref(uid: int, key: str, val: int):
        prefs = user_warning_danger_prefs.setdefault(uid, {})
        prefs[key] = val

    if state_name in ("awaiting_warning_days", "awaiting_danger_days",
                      "awaiting_warning_balance", "awaiting_danger_balance"):
        val_txt = (message.text or "").strip()
        if not val_txt.isdigit():
            await message.reply("⚠️ أدخل رقماً صحيحاً.")
            return
        val_int = int(val_txt)
        if val_int < 0:
            await message.reply("⚠️ لا يمكن أن يكون الرقم سالباً.")
            return

        prefs = _get_warning_danger_prefs(user_id)
        if state_name == "awaiting_danger_days" and val_int > prefs["warning_days"]:
            await message.reply(f"⚠️ يجب أن يكون حد الخطر ({val_int}) ≤ حد التحذير ({prefs['warning_days']}).")
            return
        if state_name == "awaiting_danger_days" and  (val_int > 30 or val_int <= 0):
            await message.reply(f"⚠️ يجب أن يكون حد الخطر للأيام بين 1 و 30 يوماً.")
            return
        if state_name == "awaiting_warning_days" and val_int < prefs["danger_days"]:
            await message.reply(f"⚠️ يجب أن يكون حد التحذير ({val_int}) ≥ حد الخطر ({prefs['danger_days']}).")
        if state_name == "awaiting_warning_days" and (val_int > 30 or val_int <= 0):
            await message.reply(f"⚠️ يجب أن يكون حد التحذير للأيام بين 1 و 30 يوماً.")
            return
        if state_name == "awaiting_danger_balance" and val_int > prefs["warning_balance"]:
            await message.reply(f"⚠️ يجب أن يكون رصيد الخطر ({val_int}) ≤ رصيد التحذير ({prefs['warning_balance']}).")
            return
        if state_name == "awaiting_danger_balance" and (val_int >= 100 or val_int <= 0):
            await message.reply(f"⚠️ يجب أن يكون رصيد الخطر بين 1 و 99%.")
            return
        if state_name == "awaiting_warning_balance" and val_int < prefs["danger_balance"]:
            await message.reply(f"⚠️ يجب أن يكون رصيد التحذير ({val_int}) ≥ رصيد الخطر ({prefs['danger_balance']}).")
            return
        if state_name == "awaiting_warning_balance" and (val_int >= 100 or val_int <= 0):
            await message.reply(f"⚠️ يجب أن يكون رصيد التحذير بين 1 و 99%.")
            return

        if state_name == "awaiting_warning_days":
            _save_pref(user_id, "warning_days", val_int)
        elif state_name == "awaiting_danger_days":
            _save_pref(user_id, "danger_days", val_int)
        elif state_name == "awaiting_warning_balance":
            _save_pref(user_id, "warning_balance", val_int)
        else:
            _save_pref(user_id, "danger_balance", val_int)

        # Persist to DB for selected targets if any, otherwise active network
        try:
            data = await state.get_data()
        except Exception:
            data = {}
        target_ids = data.get("wd_target_network_ids", [])
        bulk_details = []
        if target_ids:
            bulk_details = await _persist_warning_danger_settings_to_targets(user_id, target_ids)
            saved = all(d.get("ok") for d in bulk_details) if bulk_details else False
        else:
            saved = await _persist_warning_danger_settings(user_id)
        status = ("💾 تم الحفظ لكل الشبكات." if target_ids and saved else
                  "⚠️ تم الحفظ لبعض الشبكات فقط." if target_ids and not saved else
                  ("💾 تم الحفظ." if saved else "⚠️ لم يتم الحفظ."))
        user_settings_state.pop(user_id, None)
        await state.clear()

        prefs_after = _get_warning_danger_prefs(user_id)
        if target_ids:
            summary = _format_bulk_change_summary(bulk_details, "إعدادات التحذير والخطر")
            await message.reply(status + "\n\n" + summary + "\n\n" + _format_prefs_text(prefs_after))
        else:
            await message.reply(status + "\n\n" + _format_prefs_text(prefs_after))
        try:
            await asyncio.sleep(1)
            await settings_handler(message)
        except Exception:
            pass
        return

    # New: handle numeric input for ADSL order index update
    if state_name.startswith("awaiting_adsl_order_index_"):
        val_txt = (message.text or "").strip()
        if not val_txt.isdigit():
            await message.reply("⚠️ أدخل رقماً صحيحاً للترتيب.")
            return
        val_int = int(val_txt)
        if val_int < 0:
            await message.reply("⚠️ لا يمكن أن يكون الرقم سالباً.")
            return

        adsl_id = state_name.split("_")[-1]
        ok = await UserManager.update_adsl_order_index(adsl_id, val_int)
        user_settings_state.pop(user_id, None)
        # Try to refresh the ADSL list for the selected network
        try:
            data = await state.get_data()
        except Exception:
            data = {}
        network_id = data.get("order_index_network_id")
        if ok:
            await message.reply("✅ تم تحديث ترتيب ADSL.")
        else:
            await message.reply("❌ فشل في تحديث ترتيب ADSL.")

        if network_id:
            rows = await UserManager.get_adsls_order_indexed(int(network_id))
            if rows:
                kb_rows = [
                    [InlineKeyboardButton(text=f"🔢 {adsl} — ترتيب: {idx if idx > -1 else 'غير محدد'}", callback_data=f"order_index_select_{aid}")]
                    for (aid, adsl, idx) in rows
                ]
                kb_rows += [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="order_index_networks")],
                            [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]]
                try:
                    await message.answer("📑 قائمة ترتيب خطوط الشبكة (انقر للتعديل):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb_rows))
                except Exception:
                    pass
        return

    if state_name == "awaiting_network_name":
        data = await state.get_data()
        edit_network_id = data.get("edit_network_id")
        if not edit_network_id:
            await message.reply("❌ لم يتم تحديد شبكة للتعديل.")
            user_settings_state.pop(user_id, None)
            await state.clear()
            return

        networks = await UserManager.get_networks_for_user(chat_user.chat_user_id)
        logger.info("User %s networks for name change: %s", user_id, networks)
        active_networks = [n for n in networks if n.get("is_network_active", False)]
        logger.info("User %s active networks for name change: %s", user_id, active_networks)
        network = next((n for n in active_networks if n.get("id") == edit_network_id), None)
        if not network and networks:
            await message.reply("❌ الشبكة غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك.")
            user_settings_state.pop(user_id, None)
            await state.clear()
            return

        isSaved = await UserManager.update_network(edit_network_id, message.text, network.get("times_to_send_reports", 15))
        if isSaved:
            await selected_network_manager.update(str(user_id), message.text, chat_user.user_name)
            await message.reply(f"✅ تم حفظ اسم الشبكة: {message.text}")
        else:
            await message.reply("❌ فشل في حفظ اسم الشبكة.")
        
        user_settings_state.pop(user_id, None)
        await state.clear()

        return_to_networks = bool(data.get("return_to_networks_after_name"))
        if return_to_networks:
            try:
                await asyncio.sleep(2)
                await networks_menu(message, state)
            except Exception:
                pass
        return

    if state_name == "awaiting_name":
        if not chat_user:
            await message.reply("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.")
            user_settings_state.pop(user_id, None)
            return
        if not chat_user.is_active:
            await message.reply("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.")
            user_settings_state.pop(user_id, None)
            return
        if message.text.startswith("/") or message.text.strip() == "":
            await message.reply("⚠️ لا يمكن أن يبدأ اسم المستخدم بـ '/' أو يكون فارغاً. الرجاء إدخال اسم صحيح:")
            return
        
        try:
            logger.info("User %s setting new chat user name: %s", user_id, message.text.strip())
            new_chat_user = await chat_user_manager.set(str(user_id), message.text.strip())
            logger.info("User %s set new chat user name result: %s", user_id, new_chat_user)

            if new_chat_user:
                await message.reply(f"✅ تم حفظ اسم المستخدم: {new_chat_user.user_name}")
            else:
                logger.error("Failed to set new chat user name for user %s", user_id)
                await message.reply("❌ فشل في حفظ اسم المستخدم.")
        except Exception:
            logger.exception("Exception while setting new chat user name for user %s", user_id)
            await message.reply("❌ فشل في حفظ اسم المستخدم.")
        
        user_settings_state.pop(user_id, None)
        await state.clear()

        try:
            await asyncio.sleep(2)
            await settings_handler(message)
        except Exception:
            pass


@dp.callback_query(F.data == "set_name")
async def set_name_callback(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    user_settings_state[call.from_user.id] = "awaiting_name"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("📝 اكتب الاسم الجديد:", reply_markup=kb)
    except Exception:
        # Fallback to sending a new message if edit fails
        await call.message.answer("📝 اكتب الاسم الجديد:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "set_report_times")
async def set_report_times_callback(call: types.CallbackQuery):
    # Show mode selection for changing report times
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🕒 تغيير مواعيد شبكة واحدة", callback_data="report_times_one")],
        [InlineKeyboardButton(text="🕒 تغيير مواعيد كل الشبكات", callback_data="report_times_all")],
        [InlineKeyboardButton(text="🕒 تغيير مواعيد عدة شبكات", callback_data="report_times_multi")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    await call.message.edit_text("🕒 اختر الطريقة لتعديل مواعيد التقارير:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "report_times_one")
async def report_times_one_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات .", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{'🌟' if _is_owner_perm(n) else '🤝'} 🌐 {escape_markdown(n['network_name'])}", callback_data=f"choose_times_network_{n['id']}")]
        for n in active_networks
    ] + [[InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_report_times")], [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]])
    await call.message.edit_text("🌐 اختر الشبكة لتعديل مواعيد تقاريرها:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("choose_times_network_"))
async def choose_times_network_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        network_id = int(call.data.split("_")[-1])
    except Exception:
        await call.answer("❌ خطأ في اختيار الشبكة.", show_alert=True)
        return
    # Load current times from the chosen network
    net_obj = await UserManager.get_network_by_id(network_id)
    if not net_obj:
        await call.answer("❌ الشبكة غير موجودة.", show_alert=True)
        return
    if not _is_active_network(net_obj):
        await call.answer("❌ الشبكة غير مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكتك.", show_alert=True)
        return
    selected_times = set(SelectedNetwork.from_bitmask_to_times_list(net_obj.get("times_to_send_reports", 15)))
    user_report_selections[uid] = selected_times
    # Persist the DB row id for consistent lookups later
    await state.update_data(times_target_network_ids=[network_id])
    user_settings_state[uid] = "awaiting_report_times"
    kb = _make_times_keyboard(uid)
    await call.message.edit_text("🕒 اختر مواعيد التقارير للشبكة المختارة:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "report_times_all")
async def report_times_all_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    # Use DB row ids to align with get_network_by_id lookups
    if not networks:
        await call.answer("❌ لا توجد شبكات .", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    # Start with empty selection; user will choose desired times
    user_report_selections[uid] = set()
    await state.update_data(times_target_network_ids=[n.get("id") for n in active_networks])
    user_settings_state[uid] = "awaiting_report_times"
    kb = _make_times_keyboard(uid)
    await call.message.edit_text("🕒 اختر مواعيد التقارير لتطبيقها على كل شبكاتك:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "report_times_multi")
async def report_times_multi_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات .", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    data = await state.get_data()
    selected_ids = set(data.get("times_target_network_ids", []))
    rows = []
    for n in active_networks:
        nid = n.get("id")
        text = f"✅ {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}" if nid in selected_ids else f"🌐 {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"toggle_times_network_{nid}")])
    rows.append([InlineKeyboardButton(text="💾 متابعة لاختيار المواعيد", callback_data="proceed_report_times_multi")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_report_times"), InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    await state.update_data(times_target_network_ids=list(selected_ids))
    await call.message.edit_text("🌐 اختر الشبكات التي تريد تعديل مواعيد تقاريرها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("toggle_times_network_"))
async def toggle_times_network_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    nid_str = call.data.split("_")[-1]
    try:
        nid = int(nid_str)
    except Exception:
        await call.answer()
        return
    data = await state.get_data()
    selected = set(int(x) for x in data.get("times_target_network_ids", []))
    if nid in selected:
        selected.remove(nid)
    else:
        selected.add(nid)
    await state.update_data(times_target_network_ids=list(selected))
    # Re-render the multi-selection keyboard
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات .", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    rows = []
    for n in active_networks:
        nid2 = n.get("id")
        text = f"✅ {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}" if nid2 in selected else f"🌐 {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"toggle_times_network_{nid2}")])
    rows.append([InlineKeyboardButton(text="💾 متابعة لاختيار المواعيد", callback_data="proceed_report_times_multi")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_report_times"), InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    try:
        await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception:
        try:
            await call.message.edit_text("🌐 اختر الشبكات التي تريد تعديل مواعيد تقاريرها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "proceed_report_times_multi")
async def proceed_report_times_multi_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    data = await state.get_data()
    target_ids = data.get("times_target_network_ids", [])
    if not target_ids:
        await call.answer("⚠️ الرجاء اختيار شبكة واحدة على الأقل.", show_alert=True)
        return
    user_report_selections[uid] = set()
    logger.info("User %s proceeding to choose report times for multiple networks: %s", uid, target_ids)
    user_settings_state[uid] = "awaiting_report_times"
    kb = _make_times_keyboard(uid)
    await call.message.edit_text("🕒 اختر مواعيد التقارير لتطبيقها على الشبكات المحددة:", reply_markup=kb)
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("toggle_time_"))
async def toggle_time_callback(call: types.CallbackQuery):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    if user_settings_state.get(uid) != "awaiting_report_times":
        await call.answer()
        return
    # Read flag from FSM data set by edit_network_change_times
    try:
        data = await call.bot.fsm.get_context(call.from_user.id, call.message.chat.id).get_data()  # fallback if state not passed
    except Exception:
        data = {}
    return_to_networks_after_times = bool(data.get("return_to_networks_after_times"))
    key = call.data[len("toggle_time_"):]
    # reconstruct time like "060000" -> "06:00:00"
    if len(key) == 6:
        t = f"{key[0:2]}:{key[2:4]}:{key[4:6]}"
    else:
        await call.answer()
        return
    sel = user_report_selections.setdefault(uid, set())
    if t in sel:
        sel.remove(t)
    else:
        sel.add(t)
    # update keyboard
    try:
        await call.message.edit_reply_markup(reply_markup=_make_times_keyboard(uid, return_to_networks_after_times))
    except Exception:
        pass
    await call.answer()

@dp.callback_query(F.data == "save_report_times")
async def save_report_times_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    if user_settings_state.get(uid) != "awaiting_report_times":
        await call.answer()
        return
    sel = user_report_selections.get(uid, set())
    if not sel:
        await call.answer("⚠️ الرجاء اختيار وقت واحد على الأقل.", show_alert=True)
        return
    # sort times in chronological order
    times_sorted = sorted(list(sel), key=lambda x: tuple(map(int, x.split(":"))))
    times_str = ",".join(times_sorted)
    logger.info("User %s saving report times: %s", uid, times_str)
    logger.info("User %s saving report times (sorted): %s", uid, times_sorted)
    # Determine targets from FSM data; if not set, fallback to active network
    # Read selected target network IDs from FSMContext (consistent across flow)
    try:
        data = await state.get_data()
    except Exception:
        data = {}
    target_ids = data.get("times_target_network_ids", [])
    logger.info("User %s saving report times for target network IDs: %s", uid, target_ids)
    success_all = True
    if target_ids:
        async def update_one(nid: int) -> dict:
            try:
                net_obj = await UserManager.get_network_by_id(int(nid))
                if not net_obj:
                    return {"id": nid, "name": f"#{nid}", "ok": False}
                sel_net = SelectedNetwork(
                    id=net_obj.get("id"),
                    network_id=net_obj.get("network_id"),
                    network_name=net_obj.get("network_name"),
                    user_name=net_obj.get("user_name", ""),
                    times_to_send_reports=net_obj.get("times_to_send_reports", 15),
                    danger_percentage_remaining_balance=net_obj.get("danger_percentage_remaining_balance", 10),
                    warning_percentage_remaining_balance=net_obj.get("warning_percentage_remaining_balance", 30),
                    danger_count_remaining_days=net_obj.get("danger_count_remaining_days", 3),
                    warning_count_remaining_days=net_obj.get("warning_count_remaining_days", 7),
                    is_active=net_obj.get("is_network_active", False),
                    expiration_date=net_obj.get("expiration_date", None),
                    telegram_id=str(uid),
                    chat_user_id=net_obj.get("chat_user_id", 0),
                    network_type=net_obj.get("network_type", ""),
                    permissions=net_obj.get("permissions", "")
                )
                if not _is_active_network(net_obj):
                    return {"id": nid, "name": net_obj.get("network_name") or f"#{nid}", "ok": False}
                ok = await selected_network_manager.change_times_to_send_report(sel_net, times_sorted)
                return {"id": nid, "name": net_obj.get("network_name") or f"#{nid}", "ok": bool(ok)}
            except Exception:
                return {"id": nid, "name": f"#{nid}", "ok": False}

        results = await _gather_with_concurrency(6, [update_one(int(n)) for n in target_ids])
        # Ensure list of dicts
        details = [r for r in results if isinstance(r, dict)]
        success_all = all(d.get("ok") for d in details) if details else False
    else:
        network = await selected_network_manager.get(str(uid))
        chat_user = await chat_user_manager.get(str(uid))
        if not network or not chat_user:
            await call.answer("❌ خطأ في البيانات.", show_alert=True)
            return
        success_all = await selected_network_manager.change_times_to_send_report(network, times_sorted)

    if target_ids:
        # When bulk-saving, show a per-network summary
        summary = _format_bulk_change_summary(details, "مواعيد التقارير")
        if success_all:
            await call.message.edit_text(f"✅ تم حفظ مواعيد التقارير: {', '.join(times_sorted)}\n\n{summary}")
            # Clear selection states after successful bulk save
            user_settings_state.pop(uid, None)
            user_report_selections.pop(uid, None)
        else:
            await call.message.edit_text(f"❌ تم حفظ بعض الشبكات فقط لمواعيد التقارير: {', '.join(times_sorted)}\n\n{summary}")
    elif success_all:
        await call.message.edit_text(f"✅ تم حفظ مواعيد التقارير: {', '.join(times_sorted)}")
        # Only clear local selection state after successful save
        user_settings_state.pop(uid, None)
        user_report_selections.pop(uid, None)
    else:
        # Preserve selections so the user can retry without losing targets
        await call.message.edit_text("❌ فشل في حفظ مواعيد التقارير لبعض الشبكات. يمكنك إعادة المحاولة دون فقدان الاختيارات.")
    await call.answer()

@dp.callback_query(F.data == "cancel_report_times")
async def cancel_report_times_callback(call: types.CallbackQuery):
    uid = call.from_user.id
    user_settings_state.pop(uid, None)
    user_report_selections.pop(uid, None)
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer("✅ تم إلغاء تعديل مواعيد التقارير")

@dp.callback_query(F.data == "settings_back")
async def settings_back_callback(call: types.CallbackQuery):
    await call.message.delete()
    await settings_handler(call.message)
    await call.answer()

@dp.callback_query(F.data == "networks_menu")
async def networks_menu_callback(call: types.CallbackQuery, state: FSMContext):
    # Route back to networks menu when invoked from report times editing
    await networks_menu(call.message, state)
    await call.answer()

@dp.callback_query(F.data == "close_settings")
async def close_settings_callback(call: types.CallbackQuery):
    await call.message.delete()
    await call.answer("✅ تم إغلاق الإعدادات")

class WarningDangerState(StatesGroup):
    waiting_for_danger_days = State()
    waiting_for_warning_days = State()
    waiting_for_danger_balance = State()
    waiting_for_warning_balance = State()

@dp.callback_query(F.data == "set_warning_danger_settings")
async def set_warning_danger_settings_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    # Force scope selection first
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ الشبكة النشطة الحالية", callback_data="wd_scope_selected")],
        [InlineKeyboardButton(text="🌐 اختيار شبكة واحدة", callback_data="wd_scope_one")],
        [InlineKeyboardButton(text="🌐 عدة شبكات", callback_data="wd_scope_multi")],
        [InlineKeyboardButton(text="🌐 كل الشبكات التي أملكها", callback_data="wd_scope_all")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")],
    ])

    await call.message.edit_text(
        "⚠️❗ *إعدادات التحذير والخطر*\nاختر نطاق التطبيق أولاً:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await call.answer()

@dp.callback_query(F.data == "wd_scope")
async def wd_scope_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ الشبكة النشطة الحالية", callback_data="wd_scope_selected")],
        [InlineKeyboardButton(text="🌐 اختيار شبكة واحدة", callback_data="wd_scope_one")],
        [InlineKeyboardButton(text="🌐 عدة شبكات", callback_data="wd_scope_multi")],
        [InlineKeyboardButton(text="🌐 كل الشبكات التي أملكها", callback_data="wd_scope_all")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")],
    ])
    await call.message.edit_text("اختر نطاق تطبيق إعدادات التحذير والخطر:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "wd_scope_selected")
async def wd_scope_selected_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة نشطة.", show_alert=True)
        return
    await state.update_data(wd_target_network_ids=[network.id])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 عرض الإعدادات الحالية", callback_data="view_warning_danger_settings")],
        [InlineKeyboardButton(text="⚠️ تعديل أيام التحذير", callback_data="edit_warning_days")],
        [InlineKeyboardButton(text="❗ تعديل أيام الخطر", callback_data="edit_danger_days")],
        [InlineKeyboardButton(text="⚠️ تعديل رصيد التحذير", callback_data="edit_warning_balance")],
        [InlineKeyboardButton(text="❗ تعديل رصيد الخطر", callback_data="edit_danger_balance")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("✅ سيتم تطبيق التغييرات على الشبكة النشطة فقط.\nاختر الإعداد الذي ترغب في تعديله:", reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "wd_scope_one")
async def wd_scope_one_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات .", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    rows = [
        [InlineKeyboardButton(text=f"{'🌟' if _is_owner_perm(n) else '🤝'} 🌐 {escape_markdown(n['network_name'])}", callback_data=f"wd_choose_network_{n['id']}")]
        for n in active_networks
    ]
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="wd_scope"), InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    await call.message.edit_text("🌐 اختر شبكة واحدة لتطبيق الإعدادات عليها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("wd_choose_network_"))
async def wd_choose_network_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    try:
        network_id = int(call.data.split("_")[-1])
    except Exception:
        await call.answer()
        return
    await state.update_data(wd_target_network_ids=[network_id])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 عرض الإعدادات الحالية", callback_data="view_warning_danger_settings")],
        [InlineKeyboardButton(text="⚠️ تعديل أيام التحذير", callback_data="edit_warning_days")],
        [InlineKeyboardButton(text="❗ تعديل أيام الخطر", callback_data="edit_danger_days")],
        [InlineKeyboardButton(text="⚠️ تعديل رصيد التحذير", callback_data="edit_warning_balance")],
        [InlineKeyboardButton(text="❗ تعديل رصيد الخطر", callback_data="edit_danger_balance")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("✅ سيتم تطبيق التغييرات على الشبكة المختارة فقط.\nاختر الإعداد الذي ترغب في تعديله:", reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "wd_scope_all")
async def wd_scope_all_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    ids = [n.get("id") for n in active_networks]
    if not ids:
        await call.answer("❌ لا توجد شبكات.", show_alert=True)
        return
    await state.update_data(wd_target_network_ids=ids)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 عرض الإعدادات الحالية", callback_data="view_warning_danger_settings")],
        [InlineKeyboardButton(text="⚠️ تعديل أيام التحذير", callback_data="edit_warning_days")],
        [InlineKeyboardButton(text="❗ تعديل أيام الخطر", callback_data="edit_danger_days")],
        [InlineKeyboardButton(text="⚠️ تعديل رصيد التحذير", callback_data="edit_warning_balance")],
        [InlineKeyboardButton(text="❗ تعديل رصيد الخطر", callback_data="edit_danger_balance")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("✅ سيتم تطبيق التغييرات على كل الشبكات المحددة.\nاختر الإعداد الذي ترغب في تعديله:", reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "wd_scope_multi")
async def wd_scope_multi_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    data = await state.get_data()
    selected_ids = set(data.get("wd_target_network_ids", []))
    rows = []
    for n in networks:
        nid = n.get("id")
        text = f"✅ {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}" if nid in selected_ids else f"🌐 {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"wd_toggle_network_{nid}")])
    rows.append([InlineKeyboardButton(text="💾 متابعة", callback_data="wd_proceed")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="wd_scope"), InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    await state.update_data(wd_target_network_ids=list(selected_ids))
    await call.message.edit_text("🌐 اختر الشبكات التي تريد تطبيق الإعدادات عليها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await call.answer()

@dp.callback_query(lambda c: c.data.startswith("wd_toggle_network_"))
async def wd_toggle_network_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    nid_str = call.data.split("_")[-1]
    try:
        nid = int(nid_str)
    except Exception:
        await call.answer()
        return
    data = await state.get_data()
    selected = set(int(x) for x in data.get("wd_target_network_ids", []))
    if nid in selected:
        selected.remove(nid)
    else:
        selected.add(nid)
    await state.update_data(wd_target_network_ids=list(selected))

    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) if chat_user else []
    if not networks:
        await call.answer("❌ لا توجد شبكات.", show_alert=True)
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        await call.answer("❌ لا توجد شبكات مفعلة.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", show_alert=True)
        return
    rows = []
    for n in active_networks:
        nid2 = n.get("id")
        text = f"✅ {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}" if nid2 in selected else f"🌐 {'🌟' if _is_owner_perm(n) else '🤝'} {escape_markdown(n['network_name'])}"
        rows.append([InlineKeyboardButton(text=text, callback_data=f"wd_toggle_network_{nid2}")])
    rows.append([InlineKeyboardButton(text="💾 متابعة", callback_data="wd_proceed")])
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="wd_scope"), InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")])
    try:
        await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    except Exception:
        try:
            await call.message.edit_text("🌐 اختر الشبكات التي تريد تطبيق الإعدادات عليها:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "wd_proceed")
async def wd_proceed_callback(call: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    target_ids = data.get("wd_target_network_ids", [])
    if not target_ids:
        await call.answer("⚠️ الرجاء اختيار شبكة واحدة على الأقل.", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 عرض الإعدادات الحالية", callback_data="view_warning_danger_settings")],
        [InlineKeyboardButton(text="⚠️ تعديل أيام التحذير", callback_data="edit_warning_days")],
        [InlineKeyboardButton(text="❗ تعديل أيام الخطر", callback_data="edit_danger_days")],
        [InlineKeyboardButton(text="⚠️ تعديل رصيد التحذير", callback_data="edit_warning_balance")],
        [InlineKeyboardButton(text="❗ تعديل رصيد الخطر", callback_data="edit_danger_balance")],
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="settings_back")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    await call.message.edit_text("✅ تم تحديد الشبكات المستهدفة.\nاختر الإعداد الذي ترغب في تعديله:", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "view_warning_danger_settings")
async def view_warning_danger_settings_callback(call: types.CallbackQuery):
    uid = call.from_user.id
    telegram_id = str(uid)
    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    if not chat_user:
        try:
            await call.message.edit_text("❌ لم يتم تسجيلك بعد.", reply_markup=kb)
        except Exception:
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
        await call.answer()
        return

    networks = await UserManager.get_networks_for_user(chat_user.chat_user_id) or []
    if not networks:
        try:
            await call.message.edit_text("❌ لا توجد شبكات لعرض إعداداتها.", reply_markup=kb)
        except Exception:
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
        await call.answer()
        return
    active_networks = [n for n in networks if n.get("is_network_active", False)]
    if not active_networks:
        try:
            await call.message.edit_text("❌ لا توجد شبكات مفعلة لعرض إعداداتها.\n💬يرجى التواصل مع الإدارة لتفعيل شبكاتك الموقوفة", reply_markup=kb)
        except Exception:
            try:
                await call.message.edit_reply_markup(reply_markup=kb)
            except Exception:
                pass
        await call.answer()
        return

    # Build a readable, chunked summary for all networks' warning/danger settings
    header = f"⚙️ الإعدادات الحالية لكل الشبكات ({len(networks)}):\n\n"
    chunks = []
    current = header
    def push_chunk():
        nonlocal current
        if current.strip():
            chunks.append(current)
        current = ""

    for n in networks:
        name = n.get("network_name", f"#{n.get('id')}")
        warn_days = n.get("warning_count_remaining_days", 7)
        dang_days = n.get("danger_count_remaining_days", 3)
        warn_pct = n.get("warning_percentage_remaining_balance", 30)
        dang_pct = n.get("danger_percentage_remaining_balance", 10)
        block = (
            f"🌐 {name}\n"
            f"  • 🟡 أيام التحذير: {warn_days}\n"
            f"  • 🔴 أيام الخطر: {dang_days}\n"
            f"  • 🟡 رصيد التحذير: {warn_pct} %\n"
            f"  • 🔴 رصيد الخطر: {dang_pct} %\n\n"
        )
        # If adding this block would exceed a safe limit, push current and start new
        if len(current) + len(block) > 3500:
            push_chunk()
            current = block
        else:
            current += block
    push_chunk()

    # Send the chunks: first via edit_text, rest via answer
    try:
        await call.message.edit_text(chunks[0], reply_markup=kb)
    except Exception:
        try:
            await call.message.edit_reply_markup(reply_markup=kb)
        except Exception:
            pass
    for extra in chunks[1:]:
        try:
            await call.message.answer(extra)
        except Exception:
            pass
    await call.answer()

@dp.callback_query(F.data == "edit_warning_days")
async def edit_warning_days_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    chat_user = await chat_user_manager.get(str(uid))
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return

    user_settings_state[uid] = "awaiting_warning_days"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("📝 اكتب عدد أيام التحذير الجديدة\n اذا تجاوزت الايام المتبقية عن الرقم هذا سوف يكون باللون الأصفر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 30 :", reply_markup=kb)
    except Exception:
        # Fallback to sending a new message if edit fails
        await call.message.answer("📝 اكتب عدد أيام التحذير الجديدة\n اذا تجاوزت الايام المتبقية عن الرقم هذا سوف يكون باللون الأصفر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 30 :", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "edit_danger_days")
async def edit_danger_days_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    chat_user = await chat_user_manager.get(str(uid))
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    user_settings_state[uid] = "awaiting_danger_days"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("📝 اكتب عدد أيام الخطر الجديدة\n اذا تجاوزت الايام المتبقية عن الرقم هذا سوف يكون باللون الأحمر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 30 :", reply_markup=kb)
    except Exception:
        # Fallback to sending a new message if edit fails
        await call.message.answer("📝 اكتب عدد أيام الخطر الجديدة\n اذا تجاوزت الايام المتبقية عن الرقم هذا سوف يكون باللون الأحمر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 30 :", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "edit_warning_balance")
async def edit_warning_balance_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    chat_user = await chat_user_manager.get(str(uid))
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    user_settings_state[uid] = "awaiting_warning_balance"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.answer("📝 اكتب عدد أيام التحذير الجديدة\n  اذا تجاوزت الرصيد المتبقي هذه النسبة سوف يكون الرصيد المتبقي باللون الأصفر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 99 % :", reply_markup=kb)
    except Exception:
        # Fallback to sending a new message if edit fails
        await call.message.answer("📝 اكتب عدد أيام التحذير الجديدة\n  اذا تجاوزت الرصيد المتبقي هذه النسبة سوف يكون الرصيد المتبقي باللون الأصفر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 99 % :", reply_markup=kb)
    await call.answer()

@dp.callback_query(F.data == "edit_danger_balance")
async def edit_danger_balance_callback(call: types.CallbackQuery, state: FSMContext):
    uid = call.from_user.id
    chat_user = await chat_user_manager.get(str(uid))
    if not chat_user:
        await call.answer("❌ لم يتم تسجيلك بعد.\n استخدم /start للتسجيل أولاً.", show_alert=True)
        return
    if not chat_user.is_active:
        await call.answer("❌ حسابك غير نشط. يرجى التواصل مع الإدارة.", show_alert=True)
        return
    
    user_settings_state[uid] = "awaiting_danger_balance"
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ رجوع", callback_data="set_warning_danger_settings")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="close_settings")]
    ])
    try:
        await call.message.edit_text("📝 اكتب رصيد الخطر الجديد\n  اذا تجاوزت الرصيد المتبقي هذه النسبة سوف يكون الرصيد المتبقي باللون الأحمر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 99 % :", reply_markup=kb)
    except Exception:
        # Fallback to sending a new message if edit fails
        await call.message.answer("📝 اكتب رصيد الخطر الجديد\n  اذا تجاوزت الرصيد المتبقي هذه النسبة سوف يكون الرصيد المتبقي باللون الأحمر في جدول التقارير\nيجب ان يكون العدد ما بين 1 و 99 % :", reply_markup=kb)
    await call.answer()

# You need to handle the next message from the user to actually save the name/networkName.

# Report-related handlers (image, reports, allsummary, sendreport) moved to
# bot/handlers/reports_handlers.py to avoid duplication and circular imports.
