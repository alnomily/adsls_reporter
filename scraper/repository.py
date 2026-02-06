import logging
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from scraper.yemen_net_plan_manage import yemen_net

from bot.local_postgres import call_function, execute, fetch_all, fetch_one

logger = logging.getLogger("yemen_scraper.repo")


def _parse_expiry_date(value: Any) -> Optional[date]:
    """Best-effort parsing for scraped expiry date strings.

    Examples seen:
      - "Tuesday 17/02/2026 06:00 PM"
      - "17/02/2026 06:00 PM"
      - "17/02/2026"
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    s = str(value).strip()
    if not s or s in {"-", "none", "null"}:
        return None

    for fmt in (
        "%A %d/%m/%Y %I:%M %p",
        "%a %d/%m/%Y %I:%M %p",
        "%d/%m/%Y %I:%M %p",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass

    # Fallback: pick the dd/mm/yyyy part out of a longer localized string.
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            day = int(m.group(1))
            month = int(m.group(2))
            year = int(m.group(3))
            return date(year, month, day)
        except Exception:
            return None

    return None


def fetch_active_users() -> List[Dict[str, Any]]:
    try:
        return fetch_all(
            "SELECT id, username, password FROM users_accounts WHERE is_active = TRUE"
        )
    except Exception:
        logger.exception("Failed to fetch users from local postgres")
        return []


def fetch_user_by_username(username: str, is_admin: bool = False) -> Optional[Dict[str, Any]]:
    try:
        if is_admin:
            return fetch_one(
                "SELECT id, username, password FROM users_accounts WHERE username = %s LIMIT 1",
                [username],
            )
        return fetch_one(
            "SELECT id, username, password FROM users_accounts WHERE username = %s AND is_active = TRUE LIMIT 1",
            [username],
        )
    except Exception:
        logger.exception("Failed to fetch user %s", username)
        return None


def save_account_data_rpc(user_id: int, account_data: Dict[str, Any]) -> bool:
    plan_text = yemen_net.parse_plan_text(account_data.get("plan") or "").get_details().get("plan_id") or ""
    logger.info(
        "Saving account data for user_id=%s with plan=%s", user_id, plan_text
    )

    # Use the local PostgreSQL function to handle inserts/updates consistently.
    try:
        resp = call_function(
            "insert_account_data_and_update_user_account",
            {
                "p_account_name": account_data.get("account_name"),
                "p_user_id": user_id,
                "p_available_balance": account_data.get("available_balance"),
                "p_plan": plan_text,
                "p_status": account_data.get("status"),
                "p_expiry_date": account_data.get("expiry_date"),
            },
        )

        logger.info("Saving account data: %s, %s, %s, %s, %s, %s", account_data.get("account_name"), user_id, account_data.get("available_balance"), plan_text, account_data.get("status"), account_data.get("expiry_date"))
        data = getattr(resp, "data", None) or []
        if data and isinstance(data[0], dict):
            return bool(data[0].get("success"))
        return False
    except Exception:
        logger.exception("Failed to save account data for user_id=%s", user_id)
        return False


def insert_log(user_id: int, result: str, details: str = None) -> None:
    try:
        execute(
            "INSERT INTO logs (user_id, result, details, created_at) VALUES (%s, %s, %s, NOW())",
            [user_id, result, details],
        )
    except Exception:
        logger.debug("Unable to write log to local postgres", exc_info=True)
