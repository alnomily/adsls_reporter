import re
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List, Tuple

from aiogram import types
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from zoneinfo import ZoneInfo


FRESHNESS = None  # to be set by bot main module if needed


def set_freshness(delta):
    global FRESHNESS
    FRESHNESS = delta


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def is_stale(timestamp: Optional[str]) -> bool:
    if not timestamp:
        return True
    try:
        ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return utcnow() - ts > FRESHNESS
    except Exception:
        return True


def clean_text(text: Any) -> str:
    if text is None:
        return "N/A"
    cleaned = re.sub(r'[*_`\[\]\(\)]', '', str(text))
    return cleaned.strip() or "N/A"


def _escape_html(s: Optional[str]) -> str:
    if s is None:
        return ""
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _short_timestamp(ts: Optional[str]) -> str:
    if not ts:
        return "N/A"
    try:
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        try:
            tz = ZoneInfo("Asia/Aden")
        except Exception:
            tz = timezone.utc
        return t.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ts)[:19]


def _format_balance(balance: Any) -> str:
    b = clean_text(balance)
    m = re.search(r"[\d\.,]+", b)
    if not m:
        return "0.00 جيجابايت" if b == "N/A" else f"{b} جيجابايت"
    try:
        num = float(m.group().replace(',', ''))
        return f"{num:,.2f} جيجابايت"
    except Exception:
        return f"{b} جيجابايت"


def format_account_data(username: str, acc: Dict[str, Any], is_fresh: bool = False) -> str:
    status = clean_text(acc.get("status", "N/A"))
    adsl_number = clean_text(acc.get("adsl_number", "N/A"))
    plan = clean_text(acc.get("plan", "N/A"))
    subscription_date = clean_text(acc.get("subscription_date", "N/A")).split(" ")[0]
    expiry_date = clean_text(acc.get("expiry_date", "N/A")).split(" ")[0]
    confiscation_date = clean_text(acc.get("confiscation_date", "N/A"))
    scraped_at = _short_timestamp(acc.get("scraped_at") or acc.get("updated_at") or acc.get("created_at"))
    balance = _format_balance(acc.get("available_balance", "N/A"))

    status_emoji = "🟢" if "نشط" in status or status == "active" else "🔴"

    uname_esc = _escape_html(username)
    adsl_esc = _escape_html(adsl_number)
    status_esc = _escape_html(status)
    plan_esc = _escape_html(plan)
    subscription_esc = _escape_html(subscription_date)
    expiry_esc = _escape_html(expiry_date)
    confiscation_esc = _escape_html(confiscation_date)
    scraped_esc = _escape_html(scraped_at)
    balance_esc = _escape_html(balance)

    lines = [
        f"<b>📊 معلومات حساب يمن نت</b>",
        "",
        f"<b>👤 المستخدم:</b> <code>{uname_esc}</code>",
        f"<b>📡 ADSL:</b> {adsl_esc}",
        f"<b>🔄 الحالة:</b> {status_emoji} {status_esc}",
        f"<b>📦 الباقة:</b> {plan_esc}",
        f"<b>💰 الرصيد المتاح:</b> {balance_esc}",
        f"<b>📅 تاريخ الاشتراك:</b> {subscription_esc}",
        f"<b>⏳ تاريخ الانتهاء:</b> {expiry_esc}",
        f"<b>🛑 تاريخ الحجز/المصادرة:</b> {confiscation_esc}",
        f"<b>🕒 آخر تحديث:</b> {scraped_esc}",
        "",
        f"<b>💡 مصدر البيانات:</b> {'🆕 مباشر' if is_fresh else '📦 من التخزين'}"
    ]
    return "\n".join(lines)


def format_users_list(users: List[Dict[str, Any]]) -> str:
    if not users:
        return "📭 لا يوجد مستخدمين مسجلين."
    header = [f"👥 المستخدمون ({len(users)})", ""]
    lines = []
    for user in users:
        uname = clean_text(user.get("username", "N/A"))
        status = clean_text(user.get("account_status", "N/A"))
        status_emoji = "🟢" if status in ("active", "حساب نشط") else "🔴"
        bal = _format_balance(user.get("today_balance", "N/A"))
        rem = clean_text(user.get("remaining_days", "N/A"))
        adsl = clean_text(user.get("adsl_number", "N/A"))

        uname_esc = _escape_html(uname)
        bal_esc = _escape_html(bal)
        adsl_esc = _escape_html(adsl)
        rem_esc = _escape_html(rem)

        lines.append(f"📡 {adsl_esc} 📡\n💰 الرصيد المتاح: {bal_esc}\n⏳ الأيام المتبقية: {rem_esc}\n{status_emoji} الحالة: {status}\n")
    return "\n".join(header + lines)


def format_multi_user_summary(users_data: List[Tuple[str, Dict[str, Any]]]) -> str:
    if not users_data:
        return "📭 لا توجد بيانات."
    lines = [f"<b>📊 ملخص عدة حسابات</b>", ""]
    for username, acc in users_data:
        balance = _format_balance(acc.get("available_balance", "N/A"))
        expiry = clean_text(acc.get("expiry_date", "N/A")).split(" ")[0]
        status = clean_text(acc.get("status", "N/A"))
        status_emoji = "🟢" if "نشط" in status or status == "active" else "🔴"

        uname_esc = _escape_html(username)
        balance_esc = _escape_html(balance)
        expiry_esc = _escape_html(expiry)

        lines.append(f"{status_emoji} <code>{uname_esc}</code> | 💰 {balance_esc} | ⏳ {expiry_esc}")
    return "\n".join(lines)


def format_all_users_summary(users_data: List[Tuple[str, Dict[str, Any]]]) -> str:
    if not users_data:
        return "📭 لا توجد بيانات متاحة."
    total_balance = 0.0
    formatted_lines = []
    for username, acc in users_data:
        balance_str = _format_balance(acc.get("available_balance", "N/A"))
        m = re.search(r"[\d\.,]+", balance_str)
        if m:
            try:
                total_balance += float(m.group().replace(',', ''))
            except Exception:
                pass
        expiry = clean_text(acc.get("expiry_date", "N/A")).split(" ")[0]
        status = clean_text(acc.get("status", "N/A"))
        status_emoji = "🟢" if "نشط" in status or status == "active" else "🔴"

        uname_esc = _escape_html(username)
        balance_esc = _escape_html(balance_str)
        expiry_esc = _escape_html(expiry)

        formatted_lines.append(f"{status_emoji} <code>{uname_esc}</code> | 💰 {balance_esc} | ⏳ {expiry_esc}")

    active_count = sum(1 for _, d in users_data if "نشط" in d.get("status", "") or d.get("status") == "active")
    header = [
        f"<b>🌐 ملخص جميع المستخدمين</b>",
        f"📊 الإجمالي: {len(users_data)} | 🟢 نشط: {active_count} | 🔴 متوقف: {len(users_data) - active_count}",
        ""
    ]
    footer = ["", f"💰 إجمالي الرصيد المتاح: {total_balance:,.2f} جيجابايت"]
    return "\n".join(header + formatted_lines + footer)


def _describe_active_flow(user_id: Optional[int], state_name: Optional[str]) -> str:
    if state_name:
        if "RegisterState:name" in state_name:
            return "تسجيل الحساب: إدخال الاسم"
        if "RegisterState:network" in state_name:
            return "إضافة شبكة: إدخال اسم الشبكة"
        if "RegisterState:adsl" in state_name:
            return "إضافة ADSL: إدخال الأرقام"
        if "RegisterState:adsl_with_name" in state_name:
            return "إضافة ADSL: إدخال الأرقام مع الأسماء"
        if "RegisterState:choose_old_network" in state_name:
            return "نقل ADSL: اختيار شبكة المصدر"
        if "RegisterState:choose_adsls_to_move" in state_name:
            return "نقل ADSL: اختيار الخطوط"
        if "AdminApproveState" in state_name or "AdminRequestState" in state_name:
            return "اعتماد طلب (لوحة الإدارة)"

    if user_id is not None:
        try:
            from bot.handlers import user_handlers
            state_hint = user_handlers.user_settings_state.get(user_id)
            if state_hint:
                if str(state_hint).startswith("awaiting_adsl_order_index_"):
                    return "إعدادات: تعديل ترتيب ADSL"
                if state_hint == "awaiting_network_name":
                    return "إعدادات: تعديل اسم الشبكة"
                if state_hint == "awaiting_report_times":
                    return "إعدادات: تعديل مواعيد التقارير"
                if state_hint in (
                    "awaiting_warning_days",
                    "awaiting_danger_days",
                    "awaiting_warning_balance",
                    "awaiting_danger_balance",
                ):
                    return "إعدادات: التحذير والخطر"
                return "إعدادات المستخدم"
            if user_handlers.reportdate_sessions.get(user_id):
                return "التقارير التاريخية: اختيار التاريخ"
        except Exception:
            pass

        try:
            from bot.handlers import interactive_handlers
            if interactive_handlers.ADDUSERS_SESSIONS.get(user_id):
                return "إضافة خطوط النت (جلسة /addusers)"
        except Exception:
            pass

    return "عملية غير مكتملة"


async def block_if_active_flow(target: types.Message | types.CallbackQuery, state: FSMContext) -> bool:
    current_state = await state.get_state()
    if not current_state:
        current_state = None

    user_id = None
    try:
        if isinstance(target, types.CallbackQuery):
            user_id = target.from_user.id
        else:
            user_id = target.from_user.id if target.from_user else target.chat.id
    except Exception:
        user_id = None

    has_non_fsm_flow = False
    if user_id is not None:
        try:
            from bot.handlers import user_handlers
            if user_handlers.user_settings_state.get(user_id):
                has_non_fsm_flow = True
            elif user_handlers.reportdate_sessions.get(user_id):
                has_non_fsm_flow = True
        except Exception:
            pass

        try:
            from bot.handlers import interactive_handlers
            if interactive_handlers.ADDUSERS_SESSIONS.get(user_id):
                has_non_fsm_flow = True
        except Exception:
            pass

    if not current_state and not has_non_fsm_flow:
        return False

    flow_label = _describe_active_flow(user_id, current_state)
    text = (
        "⚠️ لديك عملية قيد التنفيذ.\n"
        f"🔎 العملية الحالية: {flow_label}\n"
        "يمكنك إكمالها أو إلغائها من الزر أدناه."
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ إلغاء العملية", callback_data="cancel_active_flow")]]
    )
    if isinstance(target, types.CallbackQuery):
        try:
            await target.answer()
        except Exception:
            pass
        try:
            await target.message.answer(text, reply_markup=kb)
        except Exception:
            pass
    else:
        await target.answer(text, reply_markup=kb)
    return True


class BotUtils:
    @staticmethod
    def is_admin(user_id: int) -> bool:
        try:
            from config import ADMIN_ID, ADMIN_IDS
            admin_targets = ADMIN_IDS or ([ADMIN_ID] if ADMIN_ID else [])
            return user_id in admin_targets
        except Exception:
            return False

    @staticmethod
    def utcnow() -> datetime:
        return utcnow()

    @staticmethod
    def is_stale(timestamp: Optional[str]) -> bool:
        return is_stale(timestamp)

    @staticmethod
    def clean_text(text: Any) -> str:
        return clean_text(text)

    @staticmethod
    def _escape_html(s: Optional[str]) -> str:
        return _escape_html(s)

    @staticmethod
    def _short_timestamp(ts: Optional[str]) -> str:
        return _short_timestamp(ts)

    @staticmethod
    def _format_balance(balance: Any) -> str:
        return _format_balance(balance)

    @staticmethod
    def format_account_data(username: str, acc: Dict[str, Any], is_fresh: bool = False) -> str:
        return format_account_data(username, acc, is_fresh=is_fresh)

    @staticmethod
    def format_users_list(users: List[Dict[str, Any]]) -> str:
        return format_users_list(users)

    @staticmethod
    def format_multi_user_summary(users_data: List[Tuple[str, Dict[str, Any]]]) -> str:
        return format_multi_user_summary(users_data)

    @staticmethod
    def format_all_users_summary(users_data: List[Tuple[str, Dict[str, Any]]]) -> str:
        return format_all_users_summary(users_data)

