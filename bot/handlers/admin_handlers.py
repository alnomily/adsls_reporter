import logging
import calendar
from datetime import datetime, timezone
from functools import partial
from typing import Optional
import json
import asyncio

from aiogram import types, F
from aiogram.filters import Command, CommandObject
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

from bot.app import dp, bot, EXEC, SCRAPE_SEMAPHORE
from bot.utils import BotUtils
from bot.cache import CacheManager
from bot.user_manager import UserManager
from scraper.runner import fetch_users
from bot.chat_user_manager import chat_user_manager
from bot.utils_shared import (
    run_blocking,
    count_table,
    user_exists,
    insert_user_account,
    delete_user_account,
    update_user_status,
    get_active_users,
    get_networks,
    get_pending_requests,
    get_pending_request,
    update_pending_status,
    create_chat_user,
    count_pending_requests,
)
from config import ADMIN_ID, ADMIN_IDS

logger = logging.getLogger(__name__)
logger.info("admin_handlers module loaded and handlers registered")

PAGE_SIZE_CHATS = 20
PAGE_SIZE_NETWORKS = 20
PAGE_SIZE_REQUESTS = 20
PAYMENT_METHOD_OPTIONS = ["جيب", "كريمي", "حوالة محلية", "نقدي", "بدون دفع"]

# Global cache for chat users to avoid repeated fetches during pagination flows
_CACHED_CHATS_USERS: Optional[list] = None
_CACHED_CHATS_LOCK = asyncio.Lock()

# Track current pagination page for chat/network pickers so we can refresh without jumping back to page 0
_CHAT_PAGE_STATE = {"activate": 0, "deactivate": 0}
_NETWORK_PAGE_STATE = {"activate": 0, "deactivate": 0}
_REQUEST_PAGE_STATE = {"pending": 0}
_REQUEST_FILTER_STATE = {"status": "pending", "type": "all"}

# Global cache for networks
_CACHED_NETWORKS: Optional[list] = None
_CACHED_NETWORKS_LOCK = asyncio.Lock()


async def _get_cached_chats_users() -> list:
    global _CACHED_CHATS_USERS
    if _CACHED_CHATS_USERS is not None:
        return _CACHED_CHATS_USERS
    async with _CACHED_CHATS_LOCK:
        # Double-check inside lock
        if _CACHED_CHATS_USERS is not None:
            return _CACHED_CHATS_USERS
        resp = await UserManager.get_chats_users()
        _CACHED_CHATS_USERS = resp or []
        return _CACHED_CHATS_USERS


def _clear_cached_chats_users() -> None:
    global _CACHED_CHATS_USERS
    _CACHED_CHATS_USERS = None


def _set_chat_page(action: str, page: int) -> None:
    _CHAT_PAGE_STATE[action] = max(0, page)


def _get_chat_page(action: str) -> int:
    return max(0, _CHAT_PAGE_STATE.get(action, 0))


async def _get_cached_networks() -> list:
    global _CACHED_NETWORKS
    if _CACHED_NETWORKS is not None:
        return _CACHED_NETWORKS
    async with _CACHED_NETWORKS_LOCK:
        if _CACHED_NETWORKS is not None:
            return _CACHED_NETWORKS
        resp = await get_networks()
        _CACHED_NETWORKS = getattr(resp, "data", []) or []
        return _CACHED_NETWORKS


def _clear_cached_networks() -> None:
    global _CACHED_NETWORKS
    _CACHED_NETWORKS = None


def _set_network_page(action: str, page: int) -> None:
    _NETWORK_PAGE_STATE[action] = max(0, page)


def _get_network_page(action: str) -> int:
    return max(0, _NETWORK_PAGE_STATE.get(action, 0))


def _reset_page_state() -> None:
    _CHAT_PAGE_STATE.update({"activate": 0, "deactivate": 0})
    _NETWORK_PAGE_STATE.update({"activate": 0, "deactivate": 0})
    _REQUEST_PAGE_STATE.update({"pending": 0})
    _REQUEST_FILTER_STATE.update({"status": "pending", "type": "all"})


# =========================
# FSM
# =========================
class AdminState(StatesGroup):
    add_user_username = State()
    add_user_password = State()
    add_user_adsl = State()


class AdminRequestState(StatesGroup):
    choose_expiration_date = State()
    enter_amount = State()
    choose_payment_method = State()


# =========================
# Helpers
# =========================
async def safe_edit_text(msg: types.Message, text: str, kb: InlineKeyboardMarkup, markdown: bool = True):
    try:
        if msg.text == text and msg.reply_markup == kb:
            return
        await msg.edit_text(text, reply_markup=kb, parse_mode=("Markdown" if markdown else None))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


def _build_admin_menu_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="🔔 تفعيل حساب", callback_data="admin:chat:activate"),
         InlineKeyboardButton(text="🔕 إيقاف حساب", callback_data="admin:chat:deactivate")],
        [InlineKeyboardButton(text="📡 تفعيل شبكة", callback_data="admin:network:activate"),
         InlineKeyboardButton(text="📴 إيقاف شبكة", callback_data="admin:network:deactivate")],
        [InlineKeyboardButton(text="🧾 الطلبات", callback_data="admin:requests")],
        [InlineKeyboardButton(text="📊 إحصائيات", callback_data="admin:stats"),
         InlineKeyboardButton(text="🔄 مزامنة", callback_data="admin:sync")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="admin:close")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _set_request_page(page: int) -> None:
    _REQUEST_PAGE_STATE["pending"] = max(0, page)


def _get_request_page() -> int:
    return max(0, _REQUEST_PAGE_STATE.get("pending", 0))


def _set_request_filter(status: Optional[str] = None, req_type: Optional[str] = None) -> None:
    if status:
        _REQUEST_FILTER_STATE["status"] = status
    if req_type:
        _REQUEST_FILTER_STATE["type"] = req_type


def _get_request_filters() -> dict:
    return {
        "status": _REQUEST_FILTER_STATE.get("status", "pending"),
        "type": _REQUEST_FILTER_STATE.get("type", "all"),
    }


def _normalize_request_payload(request_row: dict) -> dict:
    payload = request_row.get("request_payload") if isinstance(request_row, dict) else None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    if not isinstance(payload, dict):
        payload = {}
    return payload


def _format_request_label(request_row: dict) -> str:
    payload = _normalize_request_payload(request_row)
    req_type = request_row.get("request_type") or "legacy"
    network_name = payload.get("network_name") or request_row.get("network_id") or "-"
    telegram_id = payload.get("telegram_id") or request_row.get("requester_telegram_id") or "-"
    return f"{req_type} | {network_name} | {telegram_id}"


def _build_request_details_text(request_row: dict) -> str:
    payload = _normalize_request_payload(request_row)
    req_type = request_row.get("request_type") or "legacy"
    network_name = payload.get("network_name") or "-"
    network_id = payload.get("network_id") or request_row.get("network_id") or "-"
    telegram_id = payload.get("telegram_id") or request_row.get("requester_telegram_id") or "-"
    user_name = payload.get("user_name") or "-"
    adsl_numbers = payload.get("adsl_numbers") or []
    lines_count = len(adsl_numbers) if isinstance(adsl_numbers, list) else 0
    return (
        "🧾 تفاصيل الطلب\n\n"
        f"📌 النوع: {req_type}\n"
        f"👤 المشترك: {user_name}\n"
        f"📱 التليجرام: {telegram_id}\n"
        f"🌐 الشبكة: {network_name} (ID: {network_id})\n"
        f"📡 عدد الخطوط: {lines_count}"
    )


def _build_expiration_keyboard() -> InlineKeyboardMarkup:
    today = datetime.now(timezone.utc).date()
    buttons = []
    for months in range(1, 7):
        target_date = _add_months(today, months)
        label = f"{months} شهر ({target_date.strftime('%Y-%m-%d')})"
        buttons.append(
            InlineKeyboardButton(
                text=label,
                callback_data=f"admin:requests:expiry:{months}"
            )
        )

    rows = []
    for idx in range(0, len(buttons), 3):
        rows.append(buttons[idx: idx + 3])

    rows.append([InlineKeyboardButton(text="❌ إلغاء", callback_data="admin:requests:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _add_months(base_date, months: int):
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    day = min(base_date.day, calendar.monthrange(year, month)[1])
    return base_date.replace(year=year, month=month, day=day)


def _safe_int(val, default: int = 0) -> int:
    try:
        if val is None:
            return default
        return int(val)
    except Exception:
        return default


def _build_paged_rows(items, start, end, label_fn, cb_fn):
    rows = [
        [InlineKeyboardButton(text=label_fn(item), callback_data=cb_fn(item))]
        for item in items[start:end]
    ]
    return rows


async def _show_chat_picker(message: types.Message, chats: list, action: str, page: int) -> None:

    def _is_active_flag(v) -> bool:
        if isinstance(v, bool):
            return v
        try:
            # Handle int-like values and common string representations
            if isinstance(v, (int, float)):
                return int(v) == 1
            s = str(v).strip().lower()
            return s in {"1", "true", "yes", "active"}
        except Exception:
            return False
        
    if action == "activate":
        chats = [c for c in chats if not _is_active_flag(c.get("is_active"))]
    else:
        chats = [c for c in chats if _is_active_flag(c.get("is_active"))]

    total = len(chats)
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")]])
        await safe_edit_text(message, f"❌ لا توجد حسابات {'نشطة' if action == 'deactivate' else 'غير نشطة'}", kb, markdown=False)
        return
    page = max(page, 0)
    start = page * PAGE_SIZE_CHATS
    end = min(start + PAGE_SIZE_CHATS, total)
    if start >= total:
        page = max((total - 1) // PAGE_SIZE_CHATS, 0)
        start = page * PAGE_SIZE_CHATS
        end = min(start + PAGE_SIZE_CHATS, total)

    _set_chat_page(action, page)

    total_pages = max((total + PAGE_SIZE_CHATS - 1) // PAGE_SIZE_CHATS, 1)
    current_page_display = page + 1

    rows = _build_paged_rows(
        chats,
        start,
        end,
        lambda c: f"{c.get('user_name')} ({c.get('telegram_id')})",
        lambda c: f"admin:chat:{action}:{c.get('telegram_id')}"
    )

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text=f"⬅️ السابق ({page})", callback_data=f"admin:chat:{action}:page:{page-1}"))
    if end < total:
        nav_row.append(InlineKeyboardButton(text=f"التالي ({page+2}) ➡️", callback_data=f"admin:chat:{action}:page:{page+1}"))
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    header = f"قائمة الحسابات {'النشطة' if action == 'deactivate' else 'غير النشطة'} — العدد: {total}{f' • الصفحة {current_page_display}/{total_pages}' if total_pages > 1 else ''}\nاختر حساب {'للتعطيل' if action == 'deactivate' else 'للتفعيل'}:\n〰️"
    await safe_edit_text(message, header, kb, markdown=False)


async def _show_network_picker(message: types.Message, nets: list, action: str, page: int) -> None:
    # Filter networks by active flag depending on action
    def _is_active_flag(v) -> bool:
        if isinstance(v, bool):
            return v
        try:
            # Handle int-like values and common string representations
            if isinstance(v, (int, float)):
                return int(v) == 1
            s = str(v).strip().lower()
            return s in {"1", "true", "yes", "active"}
        except Exception:
            return False

    if action == "activate":
        nets = [n for n in nets if not _is_active_flag(n.get("is_active"))]
    else:
        nets = [n for n in nets if _is_active_flag(n.get("is_active"))]

    total = len(nets)
    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")]])
        await safe_edit_text(message, f"❌ لا توجد شبكات {'نشطة' if action == 'deactivate' else 'غير نشطة'}", kb, markdown=False)
        return
    page = max(page, 0)
    start = page * PAGE_SIZE_NETWORKS
    end = min(start + PAGE_SIZE_NETWORKS, total)
    if start >= total:
        page = max((total - 1) // PAGE_SIZE_NETWORKS, 0)
        start = page * PAGE_SIZE_NETWORKS
        end = min(start + PAGE_SIZE_NETWORKS, total)

    _set_network_page(action, page)

    total_pages = max((total + PAGE_SIZE_NETWORKS - 1) // PAGE_SIZE_NETWORKS, 1)
    current_page_display = page + 1

    rows = _build_paged_rows(
        nets,
        start,
        end,
        lambda n: f"{n.get('network_name')}",
        lambda n: f"admin:network:{action}:{n.get('id')}"
    )

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text=f"⬅️ السابق ({page})", callback_data=f"admin:network:{action}:page:{page-1}"))
    if end < total:
        nav_row.append(InlineKeyboardButton(text=f"التالي ({page+2}) ➡️", callback_data=f"admin:network:{action}:page:{page+1}"))
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    header = f"قائمة الشبكات {'النشطة' if action == 'deactivate' else 'غير النشطة'} — العدد: {total}{f' • الصفحة {current_page_display}/{total_pages}' if total_pages > 1 else ''}\nاختر شبكة {'للتعطيل' if action == 'deactivate' else 'للتفعيل'}:\n〰️"
    await safe_edit_text(message, header, kb, markdown=False)


def _admin_menu_text() -> str:
    return (
        "🛠️ لوحة الإدارة\n\n"
        "اختر عملية من الأزرار التالية لإدارة النظام."
    )


# =========================
# /admin menu
# =========================
@dp.message(Command("admin"))
async def admin_command(message: types.Message):
    if not BotUtils.is_admin(message.from_user.id):
        await message.answer("⛔ هذا الأمر خاص بالمشرف فقط.")
        return
    kb = _build_admin_menu_kb()
    await message.answer(_admin_menu_text(), reply_markup=kb, parse_mode="Markdown")


# =========================
# Pending requests management
# =========================
async def _show_requests_picker(message: types.Message, page: int) -> None:
    filters = _get_request_filters()
    resp = await get_pending_requests(
        filters.get("status"),
        filters.get("type"),
        limit=PAGE_SIZE_REQUESTS,
        offset=page * PAGE_SIZE_REQUESTS,
    )
    rows_data = getattr(resp, "data", []) or []
    total = await count_pending_requests(filters.get("status"), filters.get("type"))

    if total == 0:
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")]])
        await safe_edit_text(message, "❌ لا توجد طلبات معلقة.", kb, markdown=False)
        return

    page = max(page, 0)
    if page * PAGE_SIZE_REQUESTS >= total:
        page = max((total - 1) // PAGE_SIZE_REQUESTS, 0)

    _set_request_page(page)
    total_pages = max((total + PAGE_SIZE_REQUESTS - 1) // PAGE_SIZE_REQUESTS, 1)
    current_page_display = page + 1

    rows = []
    status_filter = filters.get("status") or "pending"
    type_filter = filters.get("type") or "all"

    status_row = [
        InlineKeyboardButton(text=("✅ معلقة" if status_filter == "pending" else "معلقة"), callback_data="admin:requests:filter:status:pending"),
        InlineKeyboardButton(text=("✅ مقبولة" if status_filter == "approved" else "مقبولة"), callback_data="admin:requests:filter:status:approved"),
        InlineKeyboardButton(text=("✅ مرفوضة" if status_filter == "rejected" else "مرفوضة"), callback_data="admin:requests:filter:status:rejected"),
        InlineKeyboardButton(text=("✅ الكل" if status_filter == "all" else "الكل"), callback_data="admin:requests:filter:status:all"),
    ]
    rows.append(status_row)

    type_row = [
        InlineKeyboardButton(text=("✅ كل الأنواع" if type_filter == "all" else "كل الأنواع"), callback_data="admin:requests:filter:type:all"),
        InlineKeyboardButton(text=("✅ الشبكات" if type_filter == "network" else "الشبكات"), callback_data="admin:requests:filter:type:network"),
        InlineKeyboardButton(text=("✅ الخطوط" if type_filter == "adsl" else "الخطوط"), callback_data="admin:requests:filter:type:adsl"),
    ]
    rows.append(type_row)

    rows.extend([
        [InlineKeyboardButton(text=_format_request_label(r), callback_data=f"admin:requests:view:{r.get('id')}")]
        for r in rows_data
    ])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton(text=f"⬅️ السابق ({page})", callback_data=f"admin:requests:page:{page-1}"))
    if (page + 1) * PAGE_SIZE_REQUESTS < total:
        nav_row.append(InlineKeyboardButton(text=f"التالي ({page+2}) ➡️", callback_data=f"admin:requests:page:{page+1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:menu")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    header = (
        f"🧾 الطلبات — العدد: {total}{f' • الصفحة {current_page_display}/{total_pages}' if total_pages > 1 else ''}\n"
        f"الحالة: {status_filter} | النوع: {type_filter}\n"
        "اختر طلباً للعرض:\n〰️"
    )
    await safe_edit_text(message, header, kb, markdown=False)


@dp.callback_query(F.data == "admin:requests")
async def admin_requests_menu(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    _set_request_page(0)
    await _show_requests_picker(call.message, _get_request_page())
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:filter:status:"))
async def admin_requests_filter_status(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    status = call.data.split(":", 4)[4]
    _set_request_filter(status=status)
    _set_request_page(0)
    await _show_requests_picker(call.message, _get_request_page())
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:filter:type:"))
async def admin_requests_filter_type(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    req_type = call.data.split(":", 4)[4]
    _set_request_filter(req_type=req_type)
    _set_request_page(0)
    await _show_requests_picker(call.message, _get_request_page())
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:page:"))
async def admin_requests_page(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    page = int(call.data.split(":", 3)[3])
    _set_request_page(page)
    await _show_requests_picker(call.message, _get_request_page())
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:view:"))
async def admin_requests_view(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    req_id = call.data.split(":", 3)[3]
    resp = await get_pending_request(req_id)
    data = getattr(resp, "data", None) or resp
    request_row = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else None)
    if not request_row:
        await call.answer("❌ الطلب غير موجود.", show_alert=True)
        await _show_requests_picker(call.message, _get_request_page())
        return

    await state.update_data(request_row=request_row, request_id=req_id)
    text = _build_request_details_text(request_row)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ قبول", callback_data=f"admin:requests:approve:{req_id}"),
             InlineKeyboardButton(text="⚡ قبول سريع", callback_data=f"admin:requests:approve_quick:{req_id}")],
            [InlineKeyboardButton(text="❌ رفض", callback_data=f"admin:requests:reject:{req_id}")],
            [InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:requests")],
        ]
    )
    await safe_edit_text(call.message, text, kb, markdown=False)
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:approve:"))
async def admin_requests_approve(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    req_id = call.data.split(":", 3)[3]
    resp = await get_pending_request(req_id)
    data = getattr(resp, "data", None) or resp
    request_row = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else None)
    if not request_row:
        await call.answer("❌ الطلب غير موجود.", show_alert=True)
        await _show_requests_picker(call.message, _get_request_page())
        return

    await state.update_data(request_row=request_row, request_id=req_id)
    await state.set_state(AdminRequestState.choose_expiration_date)
    await call.message.edit_text(
        f"{_build_request_details_text(request_row)}\n\n📅 اختر مدة التفعيل (1-6 أشهر):",
        reply_markup=_build_expiration_keyboard(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:approve_quick:"))
async def admin_requests_approve_quick(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    req_id = call.data.split(":", 3)[3]
    resp = await get_pending_request(req_id)
    data = getattr(resp, "data", None) or resp
    request_row = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else None)
    if not request_row:
        await call.answer("❌ الطلب غير موجود.", show_alert=True)
        await _show_requests_picker(call.message, _get_request_page())
        return

    await state.update_data(request_row=request_row, request_id=req_id, approval_quick=True)
    await state.set_state(AdminRequestState.choose_expiration_date)
    await call.message.edit_text(
        f"{_build_request_details_text(request_row)}\n\n⚡ قبول سريع: اختر مدة التفعيل فقط.",
        reply_markup=_build_expiration_keyboard(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:expiry:"))
async def admin_requests_choose_expiry(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    state_data = await state.get_data()
    request_row = state_data.get("request_row") or {}
    payload = _normalize_request_payload(request_row)

    months_str = call.data.split(":", 3)[3]
    months = _safe_int(months_str, 0)
    if months <= 0:
        await call.answer("⚠️ مدة غير صالحة.", show_alert=True)
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

    if state_data.get("approval_quick"):
        req_id = state_data.get("request_id")
        target_telegram_id = payload.get("telegram_id") or request_row.get("requester_telegram_id")
        network_id = payload.get("network_id") or request_row.get("network_id")
        user_ids = payload.get("user_ids") or []

        if not req_id or not target_telegram_id or not network_id:
            await call.answer("❌ بيانات الطلب غير مكتملة.", show_alert=True)
            await state.clear()
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

        await UserManager.activate_users(user_ids)
        is_activated = await UserManager.approve_registration(
            users_ids=user_ids,
            telegram_id=str(target_telegram_id),
            payer_chat_user_id=payer_chat_user_id,
            network_id=int(network_id),
            expiration_date=exp_date.isoformat(),
            amount=None,
            payment_method=None,
        )

        if is_activated:
            try:
                await update_pending_status(req_id, "approved")
            except Exception:
                logger.exception("Failed to update pending request status to approved")
            try:
                await chat_user_manager.refresh(str(target_telegram_id))
            except Exception:
                logger.exception("Failed to refresh chat user cache")
            try:
                await bot.send_message(
                    str(target_telegram_id),
                    "✅ تم قبول طلبك من قبل الإدارة.\n"
                    f"⏳ المدة: {months} شهر\n"
                    f"📅 تاريخ الانتهاء: {exp_date.isoformat()}\n"
                    "💳 المبلغ: بدون مبلغ\n"
                    "💰 طريقة الدفع: بدون دفع",
                )
            except Exception:
                logger.exception("Failed to notify requester about approval")

            await call.message.edit_text(
                f"✅ تم التفعيل (بدون مبلغ).\n⏳ {months} شهر\n📅 {exp_date.isoformat()}",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:requests")]]
                ),
            )
        else:
            await call.message.edit_text("❌ حدث خطأ أثناء قبول الطلب. حاول مرة أخرى.")

        await state.clear()
        await call.answer()
        return

    await state.set_state(AdminRequestState.enter_amount)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"استخدام المبلغ المقترح ({suggested_amount})", callback_data=f"admin:requests:use_amount:{suggested_amount}")],
            [InlineKeyboardButton(text="بدون مبلغ", callback_data="admin:requests:use_amount:0")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="admin:requests:retry_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="admin:requests:cancel")],
        ]
    )

    prompt = (
        "🧾 إعدادات الدفع\n"
        f"⏳ مدة التفعيل: {months} شهر\n"
        f"📅 تاريخ الانتهاء: {exp_date.isoformat()}\n"
        f"📡 عدد الخطوط: {lines_count}\n"
        f"💵 المبلغ المقترح (200 لكل خط): {suggested_amount}\n"
        "✏️ أرسل مبلغاً مختلفاً إذا لزم، أو اختر بدون مبلغ."
    )

    await call.message.edit_text(prompt, reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "admin:requests:retry_expiry")
async def admin_requests_retry_expiry(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    await state.set_state(AdminRequestState.choose_expiration_date)
    await call.message.edit_text(
        "📅 اختر مدة التفعيل (1-6 أشهر):",
        reply_markup=_build_expiration_keyboard(),
    )
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:use_amount:"))
async def admin_requests_use_amount(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    amount = _safe_int(call.data.split(":", 3)[3], 0)
    state_data = await state.get_data()
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)
    if amount < 0 or not exp_date:
        await call.answer("⚠️ مبلغ غير صالح.", show_alert=True)
        return

    await state.update_data(approval_amount=amount)
    await state.set_state(AdminRequestState.choose_payment_method)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 جيب", callback_data="admin:requests:pay:جيب"), InlineKeyboardButton(text="🏦 كريمي", callback_data="admin:requests:pay:كريمي")],
            [InlineKeyboardButton(text="💸 حوالة محلية", callback_data="admin:requests:pay:حوالة محلية"), InlineKeyboardButton(text="💵 نقدي", callback_data="admin:requests:pay:نقدي")],
            [InlineKeyboardButton(text="🚫 بدون دفع", callback_data="admin:requests:pay:بدون دفع")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="admin:requests:retry_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="admin:requests:cancel")],
        ]
    )

    await call.message.edit_text(
        f"🧾 تأكيد الدفع\n⏳ المدة: {months} شهر\n📅 تاريخ الانتهاء: {exp_date}\n💵 المبلغ: {amount}\nاختر طريقة الدفع:\n(يمكن اختيار بدون دفع)",
        reply_markup=kb,
    )
    await call.answer()


@dp.message(AdminRequestState.enter_amount)
async def admin_requests_amount(message: types.Message, state: FSMContext):
    if not BotUtils.is_admin(message.from_user.id):
        await message.answer("⛔ غير مسموح")
        return
    state_data = await state.get_data()
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)
    if not exp_date:
        await message.answer("❌ لا يوجد طلب معلق.")
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
    await state.set_state(AdminRequestState.choose_payment_method)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 جيب", callback_data="admin:requests:pay:جيب"), InlineKeyboardButton(text="🏦 كريمي", callback_data="admin:requests:pay:كريمي")],
            [InlineKeyboardButton(text="💸 حوالة محلية", callback_data="admin:requests:pay:حوالة محلية"), InlineKeyboardButton(text="💵 نقدي", callback_data="admin:requests:pay:نقدي")],
            [InlineKeyboardButton(text="🚫 بدون دفع", callback_data="admin:requests:pay:بدون دفع")],
            [InlineKeyboardButton(text="⬅️ تعديل التاريخ", callback_data="admin:requests:retry_expiry"), InlineKeyboardButton(text="❌ إلغاء", callback_data="admin:requests:cancel")],
        ]
    )

    await message.answer(
        f"🧾 تأكيد الدفع\n⏳ المدة: {months} شهر\n📅 تاريخ الانتهاء: {exp_date}\n💵 المبلغ: {amount}\nاختر طريقة الدفع:\n(يمكن اختيار بدون دفع)",
        reply_markup=kb,
    )


@dp.callback_query(F.data.startswith("admin:requests:pay:"))
async def admin_requests_payment(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    state_data = await state.get_data()
    request_row = state_data.get("request_row") or {}
    payload = _normalize_request_payload(request_row)
    req_id = state_data.get("request_id")
    exp_date = state_data.get("approval_expiration_date")
    months = _safe_int(state_data.get("approval_duration_months"), 0)
    amount = state_data.get("approval_amount")

    if not req_id or not exp_date or amount is None:
        await call.answer("❌ البيانات غير مكتملة.", show_alert=True)
        await state.clear()
        return

    payment_method = call.data.split(":", 3)[3]
    if payment_method not in PAYMENT_METHOD_OPTIONS:
        await call.answer("⚠️ اختر طريقة دفع صالحة.", show_alert=True)
        return

    target_telegram_id = payload.get("telegram_id") or request_row.get("requester_telegram_id")
    network_id = payload.get("network_id") or request_row.get("network_id")
    user_ids = payload.get("user_ids") or []

    if not target_telegram_id or not network_id:
        await call.answer("❌ بيانات الطلب غير مكتملة.", show_alert=True)
        await state.clear()
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

    amount_value = _safe_int(amount, 0)
    if amount_value < 0:
        await call.answer("⚠️ مبلغ غير صالح.", show_alert=True)
        return

    await UserManager.activate_users(user_ids)
    is_activated = await UserManager.approve_registration(
        users_ids=user_ids,
        telegram_id=str(target_telegram_id),
        payer_chat_user_id=payer_chat_user_id,
        network_id=int(network_id),
        expiration_date=exp_date,
        amount=amount_value,
        payment_method=payment_method,
    )

    if is_activated:
        try:
            await update_pending_status(req_id, "approved")
        except Exception:
            logger.exception("Failed to update pending request status to approved")
        try:
            await chat_user_manager.refresh(str(target_telegram_id))
        except Exception:
            logger.exception("Failed to refresh chat user cache")
        try:
            await bot.send_message(
                str(target_telegram_id),
                "✅ تم قبول طلبك من قبل الإدارة.\n"
                f"⏳ المدة: {months} شهر\n"
                f"📅 تاريخ الانتهاء: {exp_date}\n"
                f"💳 المبلغ: {amount_value}\n"
                f"💰 طريقة الدفع: {payment_method}",
            )
        except Exception:
            logger.exception("Failed to notify requester about approval")

        status_line = "✅ تم التفعيل وتسجيل الدفع."
        await call.message.edit_text(
            f"{status_line}\n⏳ {months} شهر\n📅 {exp_date}\n💵 {amount_value}\n💰 {payment_method}",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:requests")]]
            ),
        )
    else:
        await call.message.edit_text("❌ حدث خطأ أثناء قبول الطلب. حاول مرة أخرى.")

    await state.clear()
    await call.answer()


@dp.callback_query(F.data.startswith("admin:requests:reject:"))
async def admin_requests_reject(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    req_id = call.data.split(":", 3)[3]
    resp = await get_pending_request(req_id)
    data = getattr(resp, "data", None) or resp
    request_row = data if isinstance(data, dict) else (data[0] if isinstance(data, list) and data else None)
    if not request_row:
        await call.answer("❌ الطلب غير موجود.", show_alert=True)
        await _show_requests_picker(call.message, _get_request_page())
        return

    try:
        await update_pending_status(req_id, "rejected")
    except Exception:
        logger.exception("Failed to update pending request status to rejected")

    payload = _normalize_request_payload(request_row)
    target_telegram_id = payload.get("telegram_id") or request_row.get("requester_telegram_id")
    try:
        if target_telegram_id:
            await chat_user_manager.refresh(str(target_telegram_id))
    except Exception:
        logger.exception("Failed to refresh chat user cache")
    try:
        if target_telegram_id:
            await bot.send_message(str(target_telegram_id), "❌ تم رفض طلبك من قبل الإدارة.")
    except Exception:
        logger.exception("Failed to notify requester about rejection")

    await call.message.edit_text(
        "❌ تم رفض الطلب.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ رجوع", callback_data="admin:requests")]]
        ),
    )
    await state.clear()
    await call.answer()


@dp.callback_query(F.data == "admin:requests:cancel")
async def admin_requests_cancel(call: types.CallbackQuery, state: FSMContext):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    await state.clear()
    await _show_requests_picker(call.message, _get_request_page())
    await call.answer()


# =========================
# Chat activate/deactivate
# =========================
@dp.callback_query(F.data == "admin:chat:activate")
async def admin_chat_activate(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    chats = await _get_cached_chats_users()
    if not chats:
        await call.answer("❌ لا توجد دردشات", show_alert=True)
        return
    _set_chat_page("activate", 0)
    await _show_chat_picker(call.message, chats, action="activate", page=_get_chat_page("activate"))
    await call.answer()

@dp.callback_query(F.data == "admin:chat:deactivate")
async def admin_chat_deactivate(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    chats = await _get_cached_chats_users()
    if not chats:
        await call.answer("❌ لا توجد دردشات", show_alert=True)
        return
    _set_chat_page("deactivate", 0)
    await _show_chat_picker(call.message, chats, action="deactivate", page=_get_chat_page("deactivate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:chat:activate:page:"))
async def admin_chat_activate_page(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    page = int(call.data.split(":", 4)[4])
    chats = await _get_cached_chats_users()
    _set_chat_page("activate", page)
    await _show_chat_picker(call.message, chats, action="activate", page=_get_chat_page("activate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:chat:deactivate:page:"))
async def admin_chat_deactivate_page(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    page = int(call.data.split(":", 4)[4])
    chats = await _get_cached_chats_users()
    _set_chat_page("deactivate", page)
    await _show_chat_picker(call.message, chats, action="deactivate", page=_get_chat_page("deactivate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:chat:activate:"))
async def admin_chat_activate_target(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    telegram_id = call.data.split(":", 3)[3]
    is_activated = await chat_user_manager.activate_chat_user(str(telegram_id))
    if is_activated:
        await call.answer(f"✅ تم تفعيل الدردشة {telegram_id}")
        try:
            await _notify_other_admins(
                actor_id=call.from_user.id,
                text=await _format_admin_event_chat("تفعيل", telegram_id)
            )
        except Exception:
            logger.exception("Failed to notify admins about chat activation %s", telegram_id)
    else:
        await call.answer(f"❌ فشل تفعيل الدردشة {telegram_id}", show_alert=True)
    _clear_cached_chats_users()
    chats = await _get_cached_chats_users()
    await _show_chat_picker(call.message, chats, action="activate", page=_get_chat_page("activate"))


@dp.callback_query(F.data.startswith("admin:chat:deactivate:"))
async def admin_chat_deactivate_target(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    chat_id = call.data.split(":", 3)[3]
    is_deactivated = await chat_user_manager.deactivate_chat_user(str(chat_id))
    if is_deactivated:
        await call.answer(f"✅ تم إيقاف الدردشة {chat_id}")
        try:
            await _notify_other_admins(
                actor_id=call.from_user.id,
                text=await _format_admin_event_chat("إيقاف", chat_id)
            )
        except Exception:
            logger.exception("Failed to notify admins about chat deactivation %s", chat_id)
    else:
        await call.answer(f"❌ فشل إيقاف الدردشة {chat_id}", show_alert=True)
    _clear_cached_chats_users()
    chats = await _get_cached_chats_users()
    await _show_chat_picker(call.message, chats, action="deactivate", page=_get_chat_page("deactivate"))


# =========================
# Network activate/deactivate
# =========================
@dp.callback_query(F.data == "admin:network:activate")
async def admin_network_activate(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    nets = await _get_cached_networks()
    if not nets:
        await call.answer("❌ لا توجد شبكات", show_alert=True)
        return
    _set_network_page("activate", 0)
    await _show_network_picker(call.message, nets, action="activate", page=_get_network_page("activate"))
    await call.answer()

@dp.callback_query(F.data == "admin:network:deactivate")
async def admin_network_deactivate(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    nets = await _get_cached_networks()
    if not nets:
        await call.answer("❌ لا توجد شبكات", show_alert=True)
        return
    _set_network_page("deactivate", 0)
    await _show_network_picker(call.message, nets, action="deactivate", page=_get_network_page("deactivate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:network:activate:page:"))
async def admin_network_activate_page(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    page = int(call.data.split(":", 4)[4])
    nets = await _get_cached_networks()
    _set_network_page("activate", page)
    await _show_network_picker(call.message, nets, action="activate", page=_get_network_page("activate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:network:deactivate:page:"))
async def admin_network_deactivate_page(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    page = int(call.data.split(":", 4)[4])
    nets = await _get_cached_networks()
    _set_network_page("deactivate", page)
    await _show_network_picker(call.message, nets, action="deactivate", page=_get_network_page("deactivate"))
    await call.answer()


@dp.callback_query(F.data.startswith("admin:network:activate:"))
async def admin_network_activate_target(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    net_id = call.data.split(":", 3)[3]
    is_activated = await UserManager.activate_network(int(net_id))
    if is_activated:
        await call.answer(f"✅ تم تفعيل الشبكة {net_id}")
        try:
            await _notify_other_admins(
                actor_id=call.from_user.id,
                text=await _format_admin_event_network("تفعيل", int(net_id))
            )
        except Exception:
            logger.exception("Failed to notify admins about network activation %s", net_id)
    else:
        await call.answer(f"❌ فشل تفعيل الشبكة {net_id}", show_alert=True)
    _clear_cached_networks()
    nets = await _get_cached_networks()
    await _show_network_picker(call.message, nets, action="activate", page=_get_network_page("activate"))


@dp.callback_query(F.data.startswith("admin:network:deactivate:"))
async def admin_network_deactivate_target(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    net_id = call.data.split(":", 3)[3]
    is_deactivated = await UserManager.deactivate_network(int(net_id))
    if is_deactivated:
        await call.answer(f"✅ تم إيقاف الشبكة {net_id}")
        # Notify all partners of this network about the deactivation
        try:
            await _notify_partners_network_deactivated(int(net_id))
        except Exception:
            logger.exception("Failed to notify partners about network deactivation for %s", net_id)
        try:
            await _notify_other_admins(
                actor_id=call.from_user.id,
                text=await _format_admin_event_network("إيقاف", int(net_id))
            )
        except Exception:
            logger.exception("Failed to notify admins about network deactivation %s", net_id)
    else:   
        await call.answer(f"❌ فشل إيقاف الشبكة {net_id}", show_alert=True)
    _clear_cached_networks()
    nets = await _get_cached_networks()
    await _show_network_picker(call.message, nets, action="deactivate", page=_get_network_page("deactivate"))


async def _notify_partners_network_deactivated(network_id: int) -> None:
    """Broadcast a warning to all partners of the given network that it has been deactivated."""
    try:
        network = await UserManager.get_network_by_network_id(network_id)
    except Exception:
        network = None
        logger.exception("Could not fetch network by network_id=%s for partner notification", network_id)

    network_name = (network.get("network_name") if isinstance(network, dict) else None) or "شبكة"

    try:
        partners = await UserManager.get_network_partners(network_id, True)
    except Exception:
        partners = []
        logger.exception("Could not fetch partners for network_id=%s", network_id)

    if not partners:
        return

    # Notify all partners who have a telegram_id; do not depend on receive_partnered_report flag
    for p in partners:
        try:
            telegram_id = p.get("telegram_id")
            is_partner_active = p.get("is_partner_active", True)
            if not telegram_id:
                continue
            # Optional: only notify active partners
            if not bool(is_partner_active):
                continue
            await bot.send_message(
                str(telegram_id),
                (
                    "⚠️ تم إيقاف الشبكة\n"
                    f"🌐 الاسم: {network_name}\n"
                    f"🆔 المعرف: {network_id}\n"
                    "لن تصلك التقارير ولن تتمكن من إدارة هذه الشبكة حتى يتم إعادة تفعيلها."
                )
            )
        except Exception:
            logger.exception("Failed to send deactivation warning to partner %s for network %s", p, network_id)


async def _notify_other_admins(actor_id: int, text: str) -> None:
    """Send a notification message to all other admins about an admin action."""
    admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
    if not admin_targets:
        return
    for aid in admin_targets:
        try:
            # Skip notifying the actor
            if str(aid) == str(actor_id):
                continue
            await bot.send_message(aid, text)
        except Exception:
            logger.exception("Failed to notify admin %s about action", aid)


async def _format_admin_event_chat(action_word: str, telegram_id: str) -> str:
    """Format an admin event message for chat activation/deactivation."""
    try:
        cu = await chat_user_manager.get(str(telegram_id))
        target_name = cu.user_name if cu else None
    except Exception:
        target_name = None
    display = f"{target_name} ({telegram_id})" if target_name else f"{telegram_id}"
    return f"ℹ️ إشعار إداري:\nقام مشرف بـ {action_word} الحساب: {display}"


async def _format_admin_event_network(action_word: str, network_id: int) -> str:
    """Format an admin event message for network activation/deactivation."""
    try:
        net = await UserManager.get_network_by_network_id(network_id)
        if not net:
            net = await UserManager.get_network_by_id(network_id)
    except Exception:
        net = None
    name = (net.get("network_name") if isinstance(net, dict) else None) or "شبكة"
    return (
        "ℹ️ إشعار إداري:\n"
        f"قام مشرف بـ {action_word} الشبكة: {name} (ID: {network_id})"
    )


# =========================
# Stats / Sync
# =========================
@dp.callback_query(F.data == "admin:stats")
async def admin_stats(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    try:
        # Run counts in parallel and compute disabled via difference for fewer queries
        tasks = [
            count_table("users_accounts"),
            count_table("users_accounts", filter_column="is_active", filter_value=True),
            count_table("chats_users"),
            count_table("chats_users", filter_column="is_active", filter_value=True),
            count_table("networks"),
            count_table("networks", filter_column="is_active", filter_value=True),
        ]
        (
            resp_adsls,
            resp_active_adsls,
            resp_users,
            resp_active_users,
            resp_networks,
            resp_active_networks,
        ) = await asyncio.gather(*tasks, return_exceptions=False)

        adsls_count = getattr(resp_adsls, "count", 0) or 0
        active_adsls_count = getattr(resp_active_adsls, "count", 0) or 0
        disabled_adsls_count = max(0, adsls_count - active_adsls_count)

        users_count = getattr(resp_users, "count", 0) or 0
        active_users_count = getattr(resp_active_users, "count", 0) or 0
        disabled_users_count = max(0, users_count - active_users_count)

        networks_count = getattr(resp_networks, "count", 0) or 0
        active_networks_count = getattr(resp_active_networks, "count", 0) or 0
        disabled_networks_count = max(0, networks_count - active_networks_count)

        text = (
            f"📊 إحصائيات النظام:\n\n"
            f"👥 عدد المستخدمين: {users_count}\n"
            f"🟢 المستخدمين النشطين: {active_users_count}\n"
            f"🔴 المستخدمين المعطلين: {disabled_users_count}\n\n"
            f"📡 عدد خطوط الـ ADSL: {adsls_count}\n"
            f"🟢 خطوط الـ ADSL النشطة: {active_adsls_count}\n"
            f"🔴 خطوط الـ ADSL المعطلة: {disabled_adsls_count}\n\n"
            f"🌐 عدد الشبكات: {networks_count}\n"
            f"🟢 الشبكات النشطة: {active_networks_count}\n"
            f"🔴 الشبكات المعطلة: {disabled_networks_count}\n\n"
        )
        await safe_edit_text(call.message, text, _build_admin_menu_kb())
        await call.answer()
    except Exception as e:
        logger.exception("/admin stats error: %s", e)
        await call.answer("❌ حدث خطأ أثناء قراءة الإحصائيات.", show_alert=True)


@dp.callback_query(F.data == "admin:sync")
async def admin_sync(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    # Answer immediately to avoid callback timeout while long sync runs
    try:
        await call.answer("⏳ جارٍ المزامنة...", show_alert=False)
    except TelegramBadRequest:
        pass
    status_msg = call.message
    try:
        loop = __import__('asyncio').get_running_loop()
        result = await loop.run_in_executor(EXEC, fetch_users)
        CacheManager.clear()
        success = sum(1 for v in result.values() if v)
        fail = len(result) - success
        await status_msg.edit_text(
            f"✅ تم المزامنة بنجاح\n"
            f"🟢 ناجح: {success}\n"
            f"🔴 فشل: {fail}\n",
            reply_markup=_build_admin_menu_kb()
        )
    except Exception as e:
        logger.exception("/admin sync error: %s", e)
        try:
            await call.answer("❌ حدث خطأ أثناء المزامنة.", show_alert=True)
        except TelegramBadRequest:
            pass


@dp.callback_query(F.data == "admin:menu")
async def admin_menu_back(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    await safe_edit_text(call.message, _admin_menu_text(), _build_admin_menu_kb())
    await call.answer()
    _clear_cached_chats_users()
    _clear_cached_networks()
    _reset_page_state()

# =========================
# Close admin menu
# =========================
@dp.callback_query(F.data == "admin:close")
async def admin_close(call: types.CallbackQuery):
    if not BotUtils.is_admin(call.from_user.id):
        await call.answer("⛔ غير مسموح", show_alert=True)
        return
    try:
        await call.message.delete()
    except TelegramBadRequest:
        pass
    await call.answer("تم الإغلاق.")
    _clear_cached_chats_users()
    _clear_cached_networks()
    _reset_page_state()
