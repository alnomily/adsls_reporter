import re
from datetime import datetime, timezone
from typing import Any, Optional, Dict, List, Tuple
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

