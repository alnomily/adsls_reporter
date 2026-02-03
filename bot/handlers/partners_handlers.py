import logging
from typing import Optional
from aiogram import types
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram import F

from bot.app import dp
from bot.user_manager import UserManager
from bot.selected_network_manager import SelectedNetwork, selected_network_manager
from bot.chat_user_manager import chat_user_manager

logger = logging.getLogger(__name__)


# =========================
# FSM
# =========================
class PartnerState(StatesGroup):
    add_id = State()
    add_permissions = State()


# =========================
# UI builder
# =========================
async def build_partners_view(network, partners):
    lines = [f"🌐 **شركاء الشبكة:** {network.network_name}\n"]
    rows = []
    
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

    if not partners:
        lines.append("— لا يوجد شركاء بعد —")
        lines.append("🔍 يمكنك إضافة شريك جديد بالضغط على زر '➕ إضافة شريك'.")
    else:
        for p in partners:
            pid = p.get("id")
            name = p.get("name") or p.get("user_name") or str(pid)
            tg = p.get("chat_user_id") or "غير متوفر"
            active = bool(p.get("is_partner_active") or p.get("active") or p.get("status") == "active")
            status = "✅" if active else "🔴"
            permissions = _get_network_permisssions_str(p)
            lines.append(f"{tg}- **{name}** {status} | صلاحيات: {permissions}")
        rows.append([InlineKeyboardButton(text="✏️ تعديل صلاحيات", callback_data="partners:choose_edit_perm")])
        rows.append([
        InlineKeyboardButton(text="🗑️ حذف", callback_data="partners:choose_delete"),
        InlineKeyboardButton(text="🔄 تفعيل/تعطيل", callback_data="partners:choose_toggle")])

    rows.append([InlineKeyboardButton(text="➕ إضافة شريك", callback_data="partners:add")])
    rows.append([
        InlineKeyboardButton(text="🔄 تحديث", callback_data="partners:refresh"),
        InlineKeyboardButton(text="❌ إغلاق", callback_data="partners:close")
    ])

    return "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# /partners
# =========================
@dp.message(Command("partners"))
async def partners_command(message: types.Message):
    telegram_id = str(message.chat.id)

    chat_user = await chat_user_manager.get(telegram_id)
    if not chat_user:
        await message.answer("⚠️ لم يتم تسجيلك بعد. استخدم /start أولاً.")
        return

    network = await selected_network_manager.get(telegram_id)
    if not network:
        await message.answer("⚠️ لا يوجد شبكة محددة.")
        return
    if network.permissions == "read":
        await message.answer("⚠️ لا يمكنك إدارة الشركاء على شبكة بصلاحية قراءة فقط.")
        return
    partners = await UserManager.get_network_partners(network.network_id)
    text, kb = await build_partners_view(network, partners)
    await safe_edit_text(message, text, kb)


# =========================
# Add partner flow (ask for partnerId then permissions)
# =========================
@dp.callback_query(F.data == "partners:add")
async def partners_add_start(call: types.CallbackQuery, state: FSMContext):
    await state.set_state(PartnerState.add_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ إلغاء", callback_data="cancel_add_partner")]
    ])

    await call.message.edit_text(
        "🆔 أرسل **معرف المشترك** للشريك : ستجده من خلال استعلام عن حالة النظام من قائمة الأوامر في واجهة البوت",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await call.answer()

@dp.callback_query(F.data == "cancel_add_partner")
async def partners_add_cancel(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ تم إلغاء إضافة الشريك.")
    await call.answer()


@dp.message(PartnerState.add_id)
async def partners_add_id(message: types.Message, state: FSMContext):
    await state.update_data(partner_telegram_id=message.text.strip())
    await state.set_state(PartnerState.add_permissions)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="قراءة فقط", callback_data="partners:set_perm:0")],
        [InlineKeyboardButton(text="قراءة وكتابة", callback_data="partners:set_perm:1")],
        [InlineKeyboardButton(text="كامل", callback_data="partners:set_perm:2")]
    ])

    await message.answer("🔒 اختر صلاحية الشريك:", reply_markup=kb)


@dp.callback_query(F.data.startswith("partners:set_perm:"))
async def partners_add_set_permission(call: types.CallbackQuery, state: FSMContext):
    current = await state.get_state()
    if current != PartnerState.add_permissions.state:
        await call.answer("❌ العملية منتهية أو غير صالحة.", show_alert=True)
        return

    key = call.data.split(":")[2]
    perm = key
    if perm not in ["0", "1", "2"]:
        await call.answer("❌ صلاحية غير صحيحة.", show_alert=True)
        return

    data = await state.get_data()
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        await state.clear()
        return

    ok = await UserManager.add_network_partner(
        network.network_id,
        data["partner_telegram_id"],
        int(perm)
    )

    await call.answer("✅ تم إضافة الشريك." if ok else "❌ فشل في إضافة الشريك.")
    try:
        await call.message.edit_text("✅ تم إضافة الشريك." if ok else "❌ فشل في إضافة الشريك.")
    except TelegramBadRequest as e:
        # Ignore "message is not modified" which happens if the same button is tapped repeatedly
        if "message is not modified" not in str(e):
            raise
    await state.clear()


# =========================
# Helpers
# =========================
async def safe_edit_text(msg: types.Message, text: str, kb: InlineKeyboardMarkup):
    try:
        # Avoid redundant edit if content is unchanged
        if msg.text == text and msg.reply_markup == kb:
            return
        await msg.edit_text(text, reply_markup=kb, parse_mode="Markdown")
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise


async def build_partner_choice_kb(network, partners, action_prefix):
    rows = []
    for p in partners:
        pid = p.get("id")
        name = p.get("name") or p.get("user_name") or str(pid)
        active = bool(p.get("is_partner_active") or p.get("active") or p.get("status") == "active")
        status = "✅" if active else "🔴"
        rows.append([InlineKeyboardButton(text=f"{status} {name}", callback_data=f"{action_prefix}:{pid}")])
    rows.append([InlineKeyboardButton(text="إلغاء", callback_data="partners:refresh")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# =========================
# Choose operation -> choose partner flows
# =========================
@dp.callback_query(F.data == "partners:choose_edit_perm")
async def partners_choose_edit_perm(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        return
    partners = await UserManager.get_network_partners(network.network_id)
    kb = await build_partner_choice_kb(network, partners, "partners:edit_perm_select")
    await call.message.edit_text("✏️ اختر الشريك لتعديل الصلاحيات:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("partners:edit_perm_select:"))
async def partners_edit_perm_select(call: types.CallbackQuery):
    pid = int(call.data.split(":")[2])
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="قراءة فقط", callback_data=f"partners:update_perm:{pid}:0")],
        [InlineKeyboardButton(text="قراءة وكتابة", callback_data=f"partners:update_perm:{pid}:1")],
        [InlineKeyboardButton(text="كامل", callback_data=f"partners:update_perm:{pid}:2")],
        [InlineKeyboardButton(text="إلغاء", callback_data="partners:refresh")]
    ])
    await call.message.edit_text("🔒 اختر الصلاحية الجديدة للشريك:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data == "partners:choose_delete")
async def partners_choose_delete(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        return
    partners = await UserManager.get_network_partners(network.network_id)
    kb = await build_partner_choice_kb(network, partners, "partners:delete_select")
    await call.message.edit_text("🗑️ اختر الشريك للحذف:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("partners:delete_select:"))
async def partners_delete_select(call: types.CallbackQuery):
    pid = int(call.data.split(":")[2])
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    ok = await UserManager.remove_network_partner(pid)
    await call.answer("✅ تم الحذف." if ok else "❌ فشل الحذف.", show_alert=False)
    partners = await UserManager.get_network_partners(network.network_id)
    text, kb = await build_partners_view(network, partners)
    await safe_edit_text(call.message, text, kb)


@dp.callback_query(F.data == "partners:choose_toggle")
async def partners_choose_toggle(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        return
    partners = await UserManager.get_network_partners(network.network_id)
    kb = await build_partner_choice_kb(network, partners, "partners:toggle_select")
    await call.message.edit_text("🔄 اختر الشريك للتبديل بين تفعيل/تعطيل:", reply_markup=kb)
    await call.answer()


@dp.callback_query(F.data.startswith("partners:toggle_select:"))
async def partners_toggle_select(call: types.CallbackQuery):
    pid = int(call.data.split(":")[2])
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    # Determine current state of partner
    partners = await UserManager.get_network_partners(network.network_id)
    target = next((p for p in partners if p.get("id") == pid), None)
    if not target:
        await call.answer("❌ الشريك غير موجود.", show_alert=True)
        return
    active = bool(target.get("is_partner_active") or target.get("active") or target.get("status") == "active")
    if active:
        ok = await UserManager.deactivate_network_partner(pid)
        await call.answer("✅ تم الإيقاف." if ok else "❌ فشل الإيقاف.", show_alert=False)
    else:
        ok = await UserManager.activate_network_partner(pid)
        await call.answer("✅ تم التفعيل." if ok else "❌ فشل التفعيل.", show_alert=False)

    partners = await UserManager.get_network_partners(network.network_id)
    text, kb = await build_partners_view(network, partners)
    await safe_edit_text(call.message, text, kb)


@dp.callback_query(F.data.startswith("partners:update_perm:"))
async def partner_update_permissions(call: types.CallbackQuery):
    parts = call.data.split(":")
    pid = int(parts[2])
    perm = parts[3]

    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)
    if not network:
        await call.answer("❌ لا يوجد شبكة محددة.", show_alert=True)
        return

    ok = await UserManager.update_network_partner_permissions(pid, int(perm))

    await call.answer("✅ تم تحديث الصلاحية." if ok else "❌ فشل في تحديث الصلاحية.")
    partners = await UserManager.get_network_partners(network.network_id)
    text, kb = await build_partners_view(network, partners)
    await safe_edit_text(call.message, text, kb)


# =========================
# Refresh / Close
# =========================
@dp.callback_query(F.data == "partners:refresh")
async def partners_refresh(call: types.CallbackQuery):
    telegram_id = str(call.from_user.id)
    network = await selected_network_manager.get(telegram_id)

    partners = await UserManager.get_network_partners(network.network_id)
    text, kb = await build_partners_view(network, partners)
    await safe_edit_text(call.message, text, kb)
    await call.answer()


@dp.callback_query(F.data == "partners:close")
async def partners_close(call: types.CallbackQuery):
    await call.message.delete()
    await call.answer("تم الإغلاق.")
