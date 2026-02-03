import logging
from typing import Any, Dict, List, Optional

from scraper.yemen_net_plan_manage import yemen_net

from .session import get_supabase

logger = logging.getLogger("yemen_scraper.repo")


def fetch_active_users() -> List[Dict[str, Any]]:
    try:
        sb = get_supabase()
        res = sb.table("users_accounts").select("id,username,password").execute()
        return res.data or []
    except Exception:
        logger.exception("Failed to fetch users from supabase")
        return []


def fetch_user_by_username(username: str, is_admin: bool = False) -> Optional[Dict[str, Any]]:
    try:
        sb = get_supabase()
        query = sb.table("users_accounts").select("id,username,password").eq("username", username)
        if not is_admin:
            query = query.eq("is_active", True)
        res = query.single().execute()
        return getattr(res, "data", None)
    except Exception:
        logger.exception("Failed to fetch user %s", username)
        return None


def save_account_data_rpc(user_id: int, account_data: Dict[str, Any]) -> bool:
    sb = get_supabase()
    if not sb:
        logger.debug("No supabase client available to save account data for %s", user_id)
        return False

    try:
        plan_obj = yemen_net.parse_plan_text(account_data.get("plan") or "")
    except Exception:
        plan_obj = None
    plan_id = plan_obj.get_details().get("plan_id") if plan_obj else None
    
    logger.info("Saving account data for user_id=%s with plan_id=%s", user_id, plan_id)

    payload = {
        "p_account_name": account_data.get("account_name"),
        "p_user_id": user_id,
        "p_available_balance": account_data.get("available_balance"),
        "p_plan": str(plan_id),
        "p_status": account_data.get("status"),
        "p_expiry_date": account_data.get("expiry_date"),
    }

    try:
        result = sb.rpc("insert_account_data_and_update_user_account", payload).execute()
        data = getattr(result, "data", None)
        if data and isinstance(data, list) and len(data) > 0:
            success = data[0].get("success", False)
            message = data[0].get("message", "")
            logger.info(f"RPC message for user_id={user_id}: {message}")
            return bool(success)
        logger.debug("Unexpected RPC response format for user_id=%s: %s", user_id, data)
        return False
    except Exception:
        logger.exception("Failed to call RPC for user_id=%s", user_id)
        return False


def insert_log(user_id: int, result: str, details: str = None) -> None:
    try:
        sb = get_supabase()
        if not sb:
            return
        sb.table("logs").insert({"user_id": user_id, "result": result, "details": details}).execute()
    except Exception:
        logger.debug("Unable to write log to supabase", exc_info=True)
