import logging
from functools import partial
from typing import Optional
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
)
from config import ADMIN_ID, ADMIN_IDS

logger = logging.getLogger(__name__)
logger.info("admin_handlers module loaded and handlers registered")

PAGE_SIZE_CHATS = 20
PAGE_SIZE_NETWORKS = 20

# Global cache for chat users to avoid repeated fetches during pagination flows
_CACHED_CHATS_USERS: Optional[list] = None
_CACHED_CHATS_LOCK = asyncio.Lock()

# Track current pagination page for chat/network pickers so we can refresh without jumping back to page 0
_CHAT_PAGE_STATE = {"activate": 0, "deactivate": 0}
_NETWORK_PAGE_STATE = {"activate": 0, "deactivate": 0}

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


# =========================
# FSM
# =========================
class AdminState(StatesGroup):
    add_user_username = State()
    add_user_password = State()
    add_user_adsl = State()


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
        [InlineKeyboardButton(text="📊 إحصائيات", callback_data="admin:stats"),
         InlineKeyboardButton(text="🔄 مزامنة", callback_data="admin:sync")],
        [InlineKeyboardButton(text="❌ إغلاق", callback_data="admin:close")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


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
