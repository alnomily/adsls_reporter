import asyncio
import logging
from functools import partial
from typing import Any, Callable, Dict, Optional

from postgrest import APIError

from bot.app import EXEC
from bot.cache import CacheManager
from bot.lazy_supabase import supabase
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type


def _sync_count_table(tbl: str,filter_column: Optional[str] = None, filter_value: Optional[Any] = None):
    query = supabase.table(tbl).select("id", count="exact")
    if filter_column and filter_value is not None:
        query = query.eq(filter_column, filter_value)
    return query.execute()


def _sync_get_networks():
    return supabase.table("networks").select("*").execute()


def _sync_get_all_users():
    return supabase.table("users_accounts").select("id, username, password, network_id").eq("is_active", True).execute()


def _sync_insert_pending(network_id: str, request_text: str):
    return supabase.table("pending_requests").insert({"network_id": network_id, "request_text": request_text, "status": "pending"}).execute()


def _sync_user_exists(username: str):
    return supabase.table("users_accounts").select("id").eq("username", username).limit(1).execute()

def _sync_users_exists(adsls: list):
    return supabase.table("users_accounts").select("adsl_number").in_("adsl_number", adsls).execute()

def _unwrap_network_id(network_id: Any):
    """Ensure network_id is a primitive (int/str)."""
    try:
        # Direct ints/strs pass through
        if isinstance(network_id, (int, str)):
            return network_id
        # SingleAPIResponse or similar with .data
        data = getattr(network_id, "data", None)
        if data:
            if isinstance(data, list) and data:
                cand = data[0]
                if isinstance(cand, dict):
                    return cand.get("network_id") or cand.get("id")
            if isinstance(data, dict):
                return data.get("network_id") or data.get("id")
        # Dict object
        if isinstance(network_id, dict):
            return network_id.get("network_id") or network_id.get("id")
    except Exception:
        pass
    return network_id


def _sync_insert_user_account(username: str, password: str, network_id: Any, adsl: Optional[str] = None):
    network_id = _unwrap_network_id(network_id)
    payload = {"username": str(username), "password": str(password), "network_id": network_id}
    try:
        if adsl:
            payload["adsl_number"] = str(adsl)
        resp = supabase.table("users_accounts").insert(payload).execute()
        data = getattr(resp, "data", None) or resp
        if isinstance(data, list) and data:
            return data[0].get("id")
        if isinstance(data, dict):
            return data.get("id")
        return None
    except APIError as e:
        if "duplicate key value violates unique constraint" in str(e):
            return "DUPLICATE"
        if "already exists" in str(e):
            return "DUPLICATE"
        raise

def _sync_insert_users_accounts(usersnames: list, network_id: str, adsl: Optional[str] = None):
    payloads = []
    for username in usersnames:
        payload = {"username": username, "network_id": network_id}
        if adsl:
            payload["adsl_number"] = adsl
        payloads.append(payload)
    return supabase.table("users_accounts").insert(payloads).execute()

def _sync_delete_user(username: str):
    return supabase.table("users_accounts").delete().eq("username", username).execute()


def _sync_update_user_status(username: str, status: str):
    return supabase.table("users_accounts").update({"status": status, "updated_at": "now()"}).eq("username", username).execute()


def _sync_get_all_users_by_network(network_id: str):
    return supabase.table("users_accounts").select("id, username, adsl_number, status, order_index").eq("network_id", network_id).eq("is_active", True).execute()


def _sync_active_users():
    return supabase.table("users_accounts").select("id").eq("status", "حساب نشط").execute()


def _sync_get_user_data(username: str, network_id: str, is_admin: bool = False):
    query = (
        supabase.table("users_accounts")
        .select("id, username, adsl_number, plan, subscription_date, status, created_at, updated_at, confiscation_date, order_index")
        .eq("username", username)
    )
    if not is_admin:
        query = query.eq("is_active", True).eq("network_id", network_id)

    return query.limit(1).execute()

def _sync_get_users_by_network(network_id: str):
    return (
        supabase.table("users_accounts")
        .select("id, username, adsl_number, status, order_index")
        .eq("network_id", network_id)
        .eq("is_active", True)
        .order("id", desc=True)
        .execute()
    )

def _sync_get_all_users_for_admin():
    return (
        supabase.table("users_accounts")
        .select("id, username, adsl_number, status, network_id, order_index")
        .order("username", desc=False)
        .execute()
    )

def _sync_set_users_active(users_ids: list):
    return (
        supabase.table("users_accounts")
        .update({"is_active": True})
        .in_("id", users_ids)
        .execute()
    )

def _sync_change_users_network(users_ids: list, old_network_id: int, new_network_id: int):
    return (
        supabase.table("users_accounts")
        .update({"network_id": new_network_id})
        .in_("id", users_ids)
        .eq("network_id", old_network_id)
        .execute()
    )

def _sync_delete_users_by_ids(users_ids: list):
    return (
        supabase.table("users_accounts")
        .delete()
        .in_("id", users_ids)
        .execute()
    )

def _sync_get_adsls_order_indexed(network_id: int):
    return (
        supabase.table("users_accounts")
        .select("id,adsl_number,order_index")
        .eq("network_id", network_id)
        .eq("is_active", True)
        .order("adsl_number", desc=False)
        .execute()
    )

def _sync_get_adsl_order_index(id: str):
    return (
        supabase.table("users_accounts")
        .select("order_index")
        .eq("id", id)
        .eq("is_active", True)
        .limit(1)
        .execute()
    )

def _sync_update_adsl_order_index(id: str, order_index: int):
    return supabase.rpc("change_user_account_order_index", {"p_id": id, "p_order_index": order_index}).execute()

def _sync_add_network_partner(network_id: int, chat_user_id: int, permissions: int | str | None = 1):
    """
    Insert a network partner. `permissions` can be:
      - int: mapped as {3: 'owner', 2: 'full', 1: 'read_write', 0: 'read'}
      - str: one of 'owner','full','read','read_write'
      - None: omit the permissions column
    """
    allowed = {"owner", "full", "read", "read_write"}
    # map ints to enum names
    int_map = {3: "owner", 2: "full", 1: "read_write", 0: "read"}

    if isinstance(permissions, int):
        perm_val = int_map.get(permissions, None)
    elif isinstance(permissions, str):
        perm_val = permissions
    else:
        perm_val = None

    if perm_val is not None and perm_val not in allowed:
        raise ValueError(f"Invalid permission value: {permissions!r}")

    payload = {"network_id": network_id, "chat_user_id": chat_user_id,"network_type": "partner","permissions": perm_val}

    return supabase.table("chats_networks").insert(payload).execute()

def _sync_activate_partnered_networks(chat_network_id: int):
    return (
        supabase.table("chats_networks")
        .update({"is_active": True})
        .eq("id", chat_network_id)
        .eq("network_type", "partner")
        .execute()
    )

def _sync_get_all_partnered_networks(network_id: int, with_owner: bool = False):
    query = (
        supabase.table("networks_details")
        .select("*")
        .eq("network_id", network_id)
    )
    if not with_owner:
        query = query.eq("network_type", "partner")

    return query.execute()


def _sync_deactivate_partnered_networks(chat_network_id: int):
    return (
        supabase.table("chats_networks")
        .update({"is_active": False})
        .eq("id", chat_network_id)
        .eq("network_type", "partner")
        .execute()
    )

def _sync_change_partner_permissions(chat_network_id: int, permissions: int):
    
    allowed = {"owner", "full", "read", "read_write"}
    # map ints to enum names
    int_map = {3: "owner", 2: "full", 1: "read_write", 0: "read"}

    if isinstance(permissions, int):
        perm_val = int_map.get(permissions, None)
    elif isinstance(permissions, str):
        perm_val = permissions
    else:
        perm_val = None

    if perm_val is not None and perm_val not in allowed:
        raise ValueError(f"Invalid permission value: {permissions!r}")
    
    return (
        supabase.table("chats_networks")
        .update({"permissions": perm_val})
        .eq("id", chat_network_id)
        .eq("network_type", "partner")
        .execute()
    )
    

def _sync_delete_partnered_networks(chat_network_id: int):
    return (
        supabase.table("chats_networks")
        .delete()
        .eq("id", chat_network_id)
        .eq("network_type", "partner")
        .execute()
    )

def _sync_get_latest_account_data(user_id: str, is_admin: bool = False):
    query = (
        supabase.table("adsl_daily_report")
        .select("*")
        .eq("user_id", user_id)
    )
    if not is_admin:
        query = query.eq("is_active", True)
    return query.limit(1).execute()
def _sync_get_user_logs(user_id: str, limit: int = 5):
    return (
        supabase.table("logs")
        .select("id, user_id, result, created_at")
        .eq("user_id", user_id)
        .order("id", desc=True)
        .limit(limit)
        .execute()
    )


def _sync_get_users_ordered():
    return (
        supabase.table("users_accounts")
        .select("id, username, adsl_number, updated_at")
        .eq("is_active", True)
        .order("username", desc=False)
        .execute()
    )


def _sync_get_daily_reports_for_users(user_ids: list, report_date: str):
    if not user_ids:
        return {"data": []}
    return (
        supabase.table("adsl_daily_reports")
        .select("*")
        .in_("user_id", user_ids)
        .eq("report_date", report_date)
        .order("username", desc=False)
        .execute()
    )


async def get_daily_reports_for_users(user_ids: list, report_date: str):
    return await run_blocking(partial(_sync_get_daily_reports_for_users, user_ids, report_date))


def _sync_get_available_report_dates(user_ids: list, limit: int = 120):
    if not user_ids:
        return {"data": []}
    return (
        supabase.table("adsl_daily_reports")
        .select("report_date")
        .in_("user_id", user_ids)
        .order("report_date", desc=True)
        .limit(limit)
        .execute()
    )


async def get_available_report_dates(user_ids: list, limit: int = 120):
    return await run_blocking(partial(_sync_get_available_report_dates, user_ids, limit))


def _sync_get_account_available_balance(user_id: str, offset: int = 0):
    # offset allows getting the previous-day value when offset=1
    return (
        supabase.table("account_data")
        .select("available_balance")
        .eq("user_id", user_id)
        .order("scraped_at", desc=True)
        .offset(offset)
        .limit(1)
        .execute()
    )

def _sync_get_chat_user(telegram_id: str):
    return (
        supabase.table("chats_users")
        .select("*")
        .eq("telegram_id", telegram_id)
        .maybe_single()
        .execute()
    )

def _sync_get_chats_users():
    return (
        supabase.table("chats_users")
        .select("*")
        .execute()
    )

def _sync_active_chat_user(telegram_id: str):
    return (
        supabase.table("chats_users")
        .update({"is_active": True})
        .eq("telegram_id", telegram_id)
        .execute()
    )

def _sync_deactivate_chat_user(telegram_id: str):
    return (
        supabase.table("chats_users")
        .update({"is_active": False})
        .eq("telegram_id", telegram_id)
        .execute()
    )

def _sync_get_chat_users_tokens(chats_users_ids: list):
    return (
        supabase.table("chats_users")
        .select("telegram_id")
        .in_("id", chats_users_ids)
        .execute()
    )

def _sync_change_receive_partnered_reports(chat_user_id: int, receive_partnered_report: bool):
    return (
        supabase.table("chats_users")
        .update({"receive_partnered_report": receive_partnered_report})
        .eq("id", chat_user_id)
        .execute()
    )

def _sync_create_chat_user(telegram_id: str, user_name: str):
    return (
        supabase.table("chats_users")
        .upsert({
            "telegram_id": telegram_id,
            "user_name": user_name
        })
        .execute()
    )

def _sync_create_network(chat_user_id: int, network_name: str):
    return supabase.rpc(
        "create_network_for_chat_user",
        {
            "p_chat_user_id": chat_user_id,
            "p_network_name": network_name,
        },
    ).execute()

def _sync_remove_network(network_id: int):
    return (
        supabase.table("networks")
        .delete()
        .eq("id", network_id)
        .execute()
    )

def _sync_active_network(network_id: int):
    return supabase.rpc(
        "activate_network",
        {
            "p_network_id": network_id
        }
    ).execute()

def _sync_deactivate_network(network_id: int):
    return supabase.rpc(
        "deactive_network",
        {
            "p_network_id": network_id
        }
    ).execute()

def _sync_get_network_by_id(chat_network_id: int):
    return (
        supabase.table("networks_details")
        .select("*")
        .eq("id", chat_network_id)
        .maybe_single()
        .execute()
    )

def _sync_get_network_by_network_id(network_id: int):
    return (
        supabase.table("networks_details")
        .select("*")
        .eq("network_id", network_id)
        .eq("network_type", "owner")
        .maybe_single()
        .execute()
    )

def _sync_get_networks_for_user(chat_user_id: int):
    return (
        supabase.table("networks_details")
        .select("*")
        .eq("chat_user_id", chat_user_id)
        .execute()
    )

def _sync_update_chat_user(telegram_id: str, user_name: str):
    return (
        supabase.table("chats_users")
        .update({"user_name": user_name})
        .eq("telegram_id", telegram_id)
        .execute()
    )

def _sync_update_network(chat_network_id: int, network_name: str, times_to_send_reports: int):
    return supabase.rpc(
        "update_chat_network",
        {
            "p_chat_network_id": chat_network_id,
            "p_network_name": network_name,
            "p_times_to_send_reports": times_to_send_reports
        }
    ).execute()

def _sync_change_chat_networks_times_to_send_reports(chat_network_id: int, times_to_send_reports: int):
    return supabase.rpc(
        "update_chat_network_times_to_send_reports",
        {
            "p_chat_network_id": chat_network_id,
            "p_times_to_send_reports": times_to_send_reports
        }
    ).execute()
def _sync_change_warning_and_danger_settings(chat_network_id: int, warning_count_remaining_days: int, danger_count_remaining_days: int, warning_percentage_remaining_balance: int, danger_percentage_remaining_balance: int):
    return supabase.rpc(
        "update_chat_network_warning_and_danger_settings",
        {
            "p_chat_network_id": chat_network_id,
            "p_warning_count_remaining_days": warning_count_remaining_days,
            "p_danger_count_remaining_days": danger_count_remaining_days,
            "p_warning_percentage_remaining_balance": warning_percentage_remaining_balance,
            "p_danger_percentage_remaining_balance": danger_percentage_remaining_balance
        }
    ).execute()

def _sync_set_selected_network(chat_network_id: int, chat_user_id: int):
    return supabase.rpc(
        "set_selected_network",
        {
            "p_network_id": chat_network_id,
            "p_chat_user_id": chat_user_id
        }
    ).execute()


def _sync_get_selected_network(telegram_id: str):
    return (
        supabase.table("networks_details")
        .select("*")
        .eq("telegram_id", telegram_id)
        .eq("is_selected_network", True)
        .eq("is_network_active", True)
        .limit(1)
        .execute()
    )

def _sync_get_token_by_network_id(network_id: str):
    return (
        supabase.table("networks")
        .select("chats_users(telegram_id)")
        .eq("id", network_id)
        .maybe_single()
        .execute()
    )

def _sync_get_all_tokens():
    return (
        supabase.table("networks_details")
        .select("telegram_id")
        .eq("is_selected_network", True)
        .execute()
    )

def _sync_approve_registration(
    users_ids: list,
    telegram_id: str,
    network_id: int,
    payer_chat_user_id: int,
    expiration_date: str,
    amount: int,
    payment_method: str,
):
    return supabase.rpc(
        "approve_registration",
        {
            "p_users_ids": users_ids,
            "p_telegram_id": telegram_id,
            "p_network_id": network_id,
            "p_payer_chat_user_id": payer_chat_user_id,
            "p_expiration_date": expiration_date,
            "p_amount": amount,
            "p_payment_method": payment_method,
        },
    ).execute()

def _sync_change_order_by(telegram_id: str, order_by: str):
    return supabase.rpc(
        "change_chat_user_order_by",
        {"p_telegram_id": telegram_id,
         "p_order_by": order_by,}
    ).execute()

async def count_table(tbl: str, filter_column: Optional[str] = None, filter_value: Optional[Any] = None):
    return await run_blocking(partial(_sync_count_table, tbl, filter_column, filter_value))


async def get_networks():
    return await run_blocking(_sync_get_networks)


async def get_all_users():
    return await run_blocking(_sync_get_all_users)


async def insert_pending_request(network_id: str, request_text: str):
    return await run_blocking(partial(_sync_insert_pending, network_id, request_text))


def _sync_get_pending(req_id: str):
    return supabase.table("pending_requests").select("*").eq("id", req_id).single().execute()


def _sync_update_pending(req_id: str, status: str):
    return supabase.table("pending_requests").update({"status": status}).eq("id", req_id).execute()


async def get_pending_request(req_id: str):
    return await run_blocking(partial(_sync_get_pending, req_id))


async def get_request_by_id(req_id: str):
    """Compatibility wrapper used by interactive handlers.

    Returns a normalized dict with at least the keys: id, chat_id, text.
    It wraps `get_pending_request` and maps DB column names (token_id/request_text)
    to the expected `chat_id`/`text` field names.
    """
    resp = await get_pending_request(req_id)
    # supabase responses may expose .data or return a raw dict
    data = getattr(resp, "data", None) or resp

    # Normalize shape: data may be a dict, a list with one dict, or None
    record = None
    if isinstance(data, dict):
        # supabase single() sometimes returns the record directly
        record = data
    elif isinstance(data, list) and data:
        # some callers return list of rows
        record = data[0]

    if not record:
        return None

    chat_id = record.get("token_id") or record.get("chat_id") or record.get("token")
    text = record.get("request_text") or record.get("text") or record.get("request")

    normalized = {**record}
    normalized.update({"chat_id": chat_id, "text": text, "id": record.get("id")})
    return normalized


async def update_pending_status(req_id: str, status: str):
    return await run_blocking(partial(_sync_update_pending, req_id, status))


async def user_exists(username: str):
    return await run_blocking(partial(_sync_user_exists, username))


async def insert_user_account(username: str, password: str, network_id: str, adsl: Optional[str] = None):
    return await run_blocking(partial(_sync_insert_user_account, username, password, network_id, adsl))

async def insert_users_accounts(usernames: list, network_id: str, adsl: Optional[str] = None):
    return await run_blocking(partial(_sync_insert_users_accounts, usernames, network_id, adsl))


async def delete_user_account(username: str):
    return await run_blocking(partial(_sync_delete_user, username))


async def update_user_status(username: str, status: str):
    return await run_blocking(partial(_sync_update_user_status, username, status))


async def get_all_users_by_network_id(network_id: str):
    return await run_blocking(partial(_sync_get_all_users_by_network, network_id))


async def get_active_users():
    return await run_blocking(_sync_active_users)


async def get_user_data_db(username: str, network_id: str, is_admin: bool = False):
    return await run_blocking(partial(_sync_get_user_data, username, network_id, is_admin))


async def get_users_by_network_db(network_id: str):
    return await run_blocking(partial(_sync_get_users_by_network, network_id))

async def get_all_users_for_admin():
    return await run_blocking(_sync_get_all_users_for_admin)

async def activate_users(users_ids: list):
    return await run_blocking(partial(_sync_set_users_active, users_ids))

async def get_latest_account_data_db(user_id: str, retries: int = 4, initial_backoff: float = 0.5,is_admin: bool = False):
    """Attempt to read the latest account data from Supabase with retries and exponential backoff.

    This wraps the blocking `_sync_get_latest_account_data` call (executed via `run_blocking`) and
    retries on transient failures to reduce the number of placeholder rows caused by intermittent
    HTTP/stream/socket errors.
    """
    last_exc = None
    # allow a slightly longer initial backoff and more retries in production
    backoff = initial_backoff
    for attempt in range(1, retries + 1):
        try:
            return await run_blocking(partial(_sync_get_latest_account_data, user_id, is_admin))
        except asyncio.TimeoutError:
            logger.warning("get_latest_account_data_db timeout for user %s (attempt %s/%s)", user_id, attempt, retries)
            last_exc = asyncio.TimeoutError()
        except Exception as e:
            # Log and retry transient errors
            logger.warning("get_latest_account_data_db attempt %s/%s failed for user %s: %s", attempt, retries, user_id, e)
            last_exc = e

        if attempt < retries:
            await asyncio.sleep(backoff)
            backoff *= 2

    # After exhausting retries, re-raise the last exception so callers can handle it
    if last_exc:
        raise last_exc
    return None


async def get_user_logs_db(user_id: str, limit: int = 5):
    return await run_blocking(partial(_sync_get_user_logs, user_id, limit))

async def get_chat_user(telegram_id: str):
    return await run_blocking(partial(_sync_get_chat_user, telegram_id))

async def get_chats_users():
    return await run_blocking(_sync_get_chats_users)

async def get_chat_users_tokens(chats_users_ids: list):
    return await run_blocking(partial(_sync_get_chat_users_tokens, chats_users_ids))

async def create_chat_user(telegram_id: str, user_name: str):
    return await run_blocking(partial(_sync_create_chat_user, telegram_id, user_name))

async def create_network(chat_user_id: int, network_name: str):
    return await run_blocking(partial(_sync_create_network, chat_user_id, network_name))

async def get_networks_for_user(chat_user_id: int):
    return await run_blocking(partial(_sync_get_networks_for_user, chat_user_id))

async def set_selected_network(chat_network_id: int, chat_user_id: int):
    return await run_blocking(partial(_sync_set_selected_network, chat_network_id, chat_user_id))

async def get_selected_network(telegram_id: str):
    return await run_blocking(partial(_sync_get_selected_network, telegram_id))

async def get_token_by_network_id(network_id: str):
    return await run_blocking(partial(_sync_get_token_by_network_id, network_id))

async def get_all_tokens():
    return await run_blocking(_sync_get_all_tokens)

async def update_chat_user(telegram_id: str, user_name: str):
    return await run_blocking(partial(_sync_update_chat_user, telegram_id, user_name))

async def update_network(chat_network_id: int, network_name: str, times_to_send_reports: int):
    return await run_blocking(partial(_sync_update_network, chat_network_id, network_name, times_to_send_reports))

async def change_users_network(users_ids: list, old_network_id: int, new_network_id: int):
    return await run_blocking(partial(_sync_change_users_network, users_ids, old_network_id, new_network_id))

async def add_network_partner(network_id: int, chat_user_id: int, permissions: int = 1):
    return await run_blocking(partial(_sync_add_network_partner, network_id, chat_user_id, permissions))

async def activate_partnered_networks(chat_network_id: int):
    return await run_blocking(partial(_sync_activate_partnered_networks, chat_network_id))

async def get_all_partnered_networks(network_id: int,with_owner: bool=False):
    return await run_blocking(partial(_sync_get_all_partnered_networks, network_id, with_owner))
async def deactivate_partnered_networks(chat_network_id: int):
    return await run_blocking(partial(_sync_deactivate_partnered_networks, chat_network_id))

async def change_partner_permissions(chat_network_id: int, permissions: int):
    return await run_blocking(partial(_sync_change_partner_permissions, chat_network_id, permissions))

async def delete_partnered_networks(chat_network_id: int):
    return await run_blocking(partial(_sync_delete_partnered_networks, chat_network_id))

async def delete_users_by_ids(users_ids: list):
    return await run_blocking(partial(_sync_delete_users_by_ids, users_ids))

async def remove_network(network_id: int):
    return await run_blocking(partial(_sync_remove_network, network_id))

async def get_network_by_id(network_id: int):
    return await run_blocking(partial(_sync_get_network_by_id, network_id))

async def users_exists(adsls: list):
    return await run_blocking(partial(_sync_users_exists, adsls))

async def change_chat_networks_times_to_send_reports(chat_network_id: int, times_to_send_reports: int):
    return await run_blocking(partial(_sync_change_chat_networks_times_to_send_reports, chat_network_id, times_to_send_reports))

async def change_warning_and_danger_settings(chat_network_id: int, warning_count_remaining_days: int, danger_count_remaining_days: int, warning_percentage_remaining_balance: int, danger_percentage_remaining_balance: int):
    return await run_blocking(partial(_sync_change_warning_and_danger_settings, chat_network_id, warning_count_remaining_days, danger_count_remaining_days, warning_percentage_remaining_balance, danger_percentage_remaining_balance))

async def change_receive_partnered_reports(chat_user_id: int, receive_partnered_report: bool):
    return await run_blocking(partial(_sync_change_receive_partnered_reports, chat_user_id, receive_partnered_report))

async def activate_chat_user(telegram_id: str):
    return await run_blocking(partial(_sync_active_chat_user, telegram_id))

async def deactivate_chat_user(telegram_id: str):
    return await run_blocking(partial(_sync_deactivate_chat_user, telegram_id))

async def activate_network(network_id: int):
    return await run_blocking(partial(_sync_active_network, network_id))

async def deactivate_network(network_id: int):
    return await run_blocking(partial(_sync_deactivate_network, network_id))

async def approve_registration(
    users_ids: list,
    telegram_id: str,
    network_id: int,
    payer_chat_user_id: int,
    expiration_date: str,
    amount: int,
    payment_method: str,
):
    return await run_blocking(
        partial(
            _sync_approve_registration,
            users_ids,
            telegram_id,
            network_id,
            payer_chat_user_id,
            expiration_date,
            amount,
            payment_method,
        )
    )

async def get_network_by_network_id(network_id: int):
    return await run_blocking(partial(_sync_get_network_by_network_id, network_id))

async def change_order_by(telegram_id: str, order_by: str):
    return await run_blocking(partial(_sync_change_order_by, telegram_id, order_by))

async def get_adsls_order_indexed(network_id: int):
    return await run_blocking(partial(_sync_get_adsls_order_indexed, network_id))

async def get_adsl_order_index(id: str):
    return await run_blocking(partial(_sync_get_adsl_order_index, id))

async def update_adsl_order_index(id: str, order_index: int):
    return await run_blocking(partial(_sync_update_adsl_order_index, id, order_index))

def sync_get_users_ordered():
    """Synchronous helper for reporting code that expects sync calls."""
    return _sync_get_users_ordered()


def sync_get_account_available_balance(user_id: str, offset: int = 0):
    """Synchronous helper returning the raw supabase response for available_balance."""
    return _sync_get_account_available_balance(user_id, offset)

def sync_insert_user_account(username: str, password: str, network_id: str, adsl: Optional[str] = None):
    """Synchronous helper for inserting a user account."""
    return _sync_insert_user_account(username, password, network_id, adsl)

def sync_users_exists(adsls: list):
    """Synchronous helper for checking if multiple users exist."""
    return _sync_users_exists(adsls)

logger = logging.getLogger("YemenNetBot.utils_shared")


async def run_blocking(func: Callable, /, *args, **kwargs):
    """Run a blocking function in the shared thread executor with simple retry/backoff for transient socket issues."""
    loop = asyncio.get_running_loop()
    # Increase retries to handle transient socket errors seen on some Windows hosts
    retries = 5
    backoff = 0.2
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(EXEC, partial(func, *args, **kwargs)),
                timeout=30.0,
            )
        except asyncio.TimeoutError:
            logger.error("Blocking operation timed out: %s", getattr(func, "__name__", str(func)))
            raise
        except Exception as e:
            last_exc = e
            msg = str(e).lower()
            if (
                "10035" in msg
                or "would block" in msg
                or "non-blocking" in msg
                or "connectionterminated" in msg
            ):
                logger.warning(
                    "Transient socket error in blocking op %s (attempt %s/%s): %s",
                    getattr(func, "__name__", str(func)),
                    attempt,
                    retries,
                    e,
                )
                if attempt < retries:
                    await asyncio.sleep(backoff)
                    backoff *= 2
                    continue
            # Reset Supabase client on HTTP/2 stream/connection state errors and retry
            reset_triggers = (
                "streaminputs.send_headers",
                "recv_headers",
                "recv_data",
                "connectionstate.closed",
                "h2",
                "invalid input",
            )
            if any(t in msg for t in reset_triggers):
                try:
                    from bot.lazy_supabase import supabase
                    supabase.reset()
                    logger.warning("Reset Supabase client due to connection state error: %s", e)
                except Exception:
                    pass
                if attempt < retries:
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 2.0)
                    continue
            logger.error("Error in blocking operation %s: %s", getattr(func, "__name__", str(func)), e)
            raise
    if last_exc:
        raise last_exc


async def save_scraped_account(username: str, network_id: int,is_admin: bool = False) -> bool:
    """Fetch live data for `username` using the scraper and only save it if the account belongs to network.

    This mirrors the previous logic in `bot.bot` but lives in a shared module so handlers
    can import it without creating circular imports.
    """
    # import UserManager lazily to avoid circular imports
    from bot.user_manager import UserManager
    from scraper.runner import fetch_single_user
    if not network_id:
        logger.warning("No network provided for scrape attempt: %s", username)
        return False
    user = await UserManager.get_user_data(username, network_id,is_admin)
    if not user:
        logger.warning("Unauthorized scrape attempt: %s by network %s", username, network_id)
        return False

    loop = asyncio.get_running_loop()

    @retry(reraise=True, stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
           retry=retry_if_exception_type(Exception))
    def _fetch_blocking(u):
        return fetch_single_user(u, is_admin)

    try:
        result = await asyncio.wait_for(
            loop.run_in_executor(EXEC, partial(_fetch_blocking, username)),
            timeout=60,
        )
    except asyncio.TimeoutError:
        logger.error("Timeout fetching data for %s", username)
        return False
    except Exception as e:
        logger.exception("fetch_single_user failed for %s: %s", username, e)
        return False

    if not isinstance(result, dict) or username not in result:
        logger.warning("Unexpected fetch result for %s: %r", username, result)
        return False

    success = result.get(username, False)
    if success:
        logger.info("✅ Saved scraped data for %s under network %s", username, network_id)
        try:
            CacheManager.clear(f"user_{network_id}_{username}")
        except Exception:
            pass
        return True
    else:
        logger.error("❌ Failed to fetch or save data for %s under network %s", username, network_id)
        return False