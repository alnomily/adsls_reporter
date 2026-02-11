import asyncio
import json
import logging
import os
import re
from datetime import date, datetime
from functools import partial
from typing import Any, Callable, Dict, Optional

import psycopg2
from psycopg2 import errors as pg_errors

from bot.app import EXEC
from bot.cache import CacheManager
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from bot.local_postgres import (
    DBResponse,
    call_function,
    count_table as pg_count_table,
    execute,
    fetch_all,
    fetch_one,
    fetch_value,
    insert_returning_one,
)


def _sync_count_table(tbl: str,filter_column: Optional[str] = None, filter_value: Optional[Any] = None):
    cnt = pg_count_table(tbl, filter_column, filter_value)
    return DBResponse(data=[], count=cnt)


def _sync_get_networks():
    return DBResponse(data=fetch_all('SELECT * FROM networks'))


def _sync_get_all_users():
    return DBResponse(
        data=fetch_all(
            'SELECT id, username, password, network_id FROM users_accounts WHERE is_active = TRUE'
        )
    )


def _sync_insert_pending(network_id: str, request_text: str):
    row = insert_returning_one(
        'INSERT INTO pending_requests (token_id, request_text, status) VALUES (%s, %s, %s) RETURNING *',
        [network_id, request_text, 'pending'],
    )
    return DBResponse(data=[row] if row else [])


def _sync_insert_pending_v2(
    request_type: str,
    payload: dict,
    requester_telegram_id: Optional[str] = None,
    network_id: Optional[int] = None,
):
    token_id = requester_telegram_id or (payload or {}).get("telegram_id") or ""
    request_text = (payload or {}).get("request_text") or ""
    row = insert_returning_one(
        """
        INSERT INTO pending_requests
            (token_id, request_text, request_type, request_payload, status, requester_telegram_id, network_id)
        VALUES
            (%s, %s, %s, %s::jsonb, %s, %s, %s)
        RETURNING *
        """.strip(),
        [
            str(token_id),
            request_text,
            request_type,
            json.dumps(payload or {}),
            "pending",
            requester_telegram_id,
            network_id,
        ],
    )
    return DBResponse(data=[row] if row else [])


def _sync_insert_payment(
    payer_chat_user_id: int,
    network_id: int,
    amount: int,
    period_months: int,
    payment_method: Optional[str] = None,
):
    row = insert_returning_one(
        """
        INSERT INTO payments
            (payer_chat_user_id, network_id, amount, period_months, payment_method)
        VALUES
            (%s, %s, %s, %s, %s)
        RETURNING *
        """.strip(),
        [payer_chat_user_id, network_id, amount, period_months, payment_method],
    )
    return DBResponse(data=[row] if row else [])


def _sync_user_exists(username: str):
    rows = fetch_all('SELECT id FROM users_accounts WHERE username = %s LIMIT 1', [username])
    return DBResponse(data=rows)

def _sync_users_exists(adsls: list):
    if not adsls:
        return DBResponse(data=[])
    rows = fetch_all('SELECT adsl_number FROM users_accounts WHERE adsl_number = ANY(%s::text[])', [adsls])
    return DBResponse(data=rows)

def _sync_users_exists_accounts2(adsls: list):
    if not adsls:
        return DBResponse(data=[])
    try:
        rows = fetch_all('SELECT adsl_number FROM users_accounts2 WHERE adsl_number = ANY(%s::text[])', [adsls])
        return DBResponse(data=rows)
    except psycopg2.errors.UndefinedTable:
        logger.error("users_accounts2 table is missing. Create it before running range processing.")
        raise RuntimeError("users_accounts2 table missing")

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


def _parse_expiry_date_value(value: Any) -> Optional[date]:
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


def _sync_insert_user_account(username: str, password: str, network_id: Any, adsl: Optional[str] = None):
    network_id = _unwrap_network_id(network_id)
    payload_username = str(username)
    payload_password = str(password)
    payload_adsl = str(adsl) if adsl else None

    cols = ["username", "password", "network_id", "is_active"] + (["adsl_number"] if payload_adsl else [])
    vals = [payload_username, payload_password, network_id, True] + ([payload_adsl] if payload_adsl else [])
    placeholders = ", ".join(["%s"] * len(vals))

    try:
        row = insert_returning_one(
            f"INSERT INTO users_accounts ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id",
            vals,
        )
        return (row or {}).get("id")
    except psycopg2.IntegrityError as e:
        # 23505 = unique_violation
        if getattr(e, "pgcode", None) == "23505" or isinstance(getattr(e, "__cause__", None), pg_errors.UniqueViolation):
            return "DUPLICATE"
        raise

def _sync_insert_user_account2(
    username: str,
    password: str,
    network_id: Any,
    adsl: Optional[str] = None,
    account_data: Optional[Dict[str, Any]] = None,
):
    network_id = _unwrap_network_id(network_id)
    payload_username = str(username)
    payload_password = str(password)
    payload_adsl = str(adsl) if adsl else None
    account_data = account_data or {}

    account_name = account_data.get("account_name")
    plan = account_data.get("plan")
    status = account_data.get("status")
    expiry_date = _parse_expiry_date_value(account_data.get("expiry_date"))
    balance_value = account_data.get("available_balance")

    cols = ["username", "password", "network_id", "is_active"] + (["adsl_number"] if payload_adsl else [])
    vals = [payload_username, payload_password, network_id, True] + ([payload_adsl] if payload_adsl else [])

    if account_name:
        cols.append("account_name")
        vals.append(str(account_name))
    if plan:
        cols.append("plan")
        vals.append(str(plan))
    if status:
        cols.append("status")
        vals.append(str(status))
    if expiry_date:
        cols.append("expiry_date")
        vals.append(expiry_date)
    if balance_value is not None:
        cols.append("balance_value")
        vals.append(str(balance_value))
    placeholders = ", ".join(["%s"] * len(vals))

    try:
        row = insert_returning_one(
            f"INSERT INTO users_accounts2 ({', '.join(cols)}) VALUES ({placeholders}) RETURNING id",
            vals,
        )
        return (row or {}).get("id")
    except psycopg2.IntegrityError as e:
        if getattr(e, "pgcode", None) == "23505" or isinstance(getattr(e, "__cause__", None), pg_errors.UniqueViolation):
            return "DUPLICATE"
        raise
    except psycopg2.errors.UndefinedTable:
        logger.error("users_accounts2 table is missing. Create it before inserting.")
        raise RuntimeError("users_accounts2 table missing")


def _sync_get_users_accounts2(limit: int = 20, offset: int = 0):
    try:
        rows = fetch_all(
            """
            SELECT id, username, account_name, adsl_number, plan, status, expiry_date, balance_value, network_id
            FROM users_accounts2
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """.strip(),
            [limit, offset],
        )
        return DBResponse(data=rows)
    except psycopg2.errors.UndefinedTable:
        logger.error("users_accounts2 table is missing. Create it before listing.")
        return DBResponse(data=[], count=0)


def _sync_search_users_accounts2_by_account_name(query: str, limit: int = 20, offset: int = 0):
    try:
        rows = fetch_all(
            """
            SELECT id, username, account_name, adsl_number, plan, status, expiry_date, balance_value, network_id
            FROM users_accounts2
            WHERE account_name ILIKE %s
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
            """.strip(),
            [f"%{query}%", limit, offset],
        )
        return DBResponse(data=rows)
    except psycopg2.errors.UndefinedTable:
        logger.error("users_accounts2 table is missing. Create it before searching.")
        return DBResponse(data=[], count=0)


def _sync_count_users_accounts2_by_account_name(query: Optional[str] = None):
    try:
        if query:
            count = fetch_value(
                "SELECT COUNT(*) FROM users_accounts2 WHERE account_name ILIKE %s",
                [f"%{query}%"],
            )
        else:
            count = fetch_value("SELECT COUNT(*) FROM users_accounts2", [])
        return DBResponse(data=[], count=int(count or 0))
    except psycopg2.errors.UndefinedTable:
        logger.error("users_accounts2 table is missing. Create it before counting.")
        return DBResponse(data=[], count=0)

def _sync_insert_users_accounts(usersnames: list, network_id: str, adsl: Optional[str] = None):
    if not usersnames:
        return DBResponse(data=[])
    rows = []
    for uname in usersnames:
        row = {
            "username": str(uname),
            "password": "123456",
            "network_id": network_id,
            "is_active": True,
        }
        if adsl:
            row["adsl_number"] = str(adsl)
        rows.append(row)

    inserted: list[dict[str, Any]] = []
    for r in rows:
        try:
            row_db = insert_returning_one(
                """INSERT INTO users_accounts (username, password, network_id, is_active, adsl_number)
                   VALUES (%s, %s, %s, %s, %s)
                   RETURNING id, username""",
                [r.get("username"), r.get("password"), r.get("network_id"), r.get("is_active"), r.get("adsl_number")],
            )
            if row_db:
                inserted.append(row_db)
        except psycopg2.IntegrityError:
            # keep supabase-like behavior: surface the error to callers if they care
            raise

    return DBResponse(data=inserted)

def _sync_delete_user(username: str):
    execute('DELETE FROM users_accounts WHERE username = %s', [username])
    return DBResponse(data=[])


def _sync_update_user_status(username: str, status: str):
    execute('UPDATE users_accounts SET status = %s, updated_at = NOW() WHERE username = %s', [status, username])
    return DBResponse(data=[])


def _sync_get_all_users_by_network(network_id: str):
    return DBResponse(
        data=fetch_all(
            'SELECT id, username, adsl_number, status, order_index FROM users_accounts WHERE network_id = %s AND is_active = TRUE',
            [network_id],
        )
    )


def _sync_active_users():
    return DBResponse(data=fetch_all('SELECT id FROM users_accounts WHERE status = %s', ['حساب نشط']))


def _sync_get_user_data(username: str, network_id: str, is_admin: bool = False):
    base = (
        'SELECT id, username, adsl_number, plan, subscription_date, status, created_at, updated_at, confiscation_date, order_index '
        'FROM users_accounts WHERE username = %s'
    )
    params: list[Any] = [username]
    if not is_admin:
        base += ' AND is_active = TRUE AND network_id = %s'
        params.append(network_id)
    base += ' LIMIT 1'
    rows = fetch_all(base, params)
    return DBResponse(data=rows)

def _sync_get_users_by_network(network_id: str):
    rows = fetch_all(
        'SELECT id, username, adsl_number, status, order_index FROM users_accounts WHERE network_id = %s AND is_active = TRUE ORDER BY id DESC',
        [network_id],
    )
    return DBResponse(data=rows)

def _sync_get_all_users_for_admin():
    return DBResponse(
        data=fetch_all(
            'SELECT id, username, adsl_number, status, network_id, order_index FROM users_accounts ORDER BY username ASC'
        )
    )

def _sync_set_users_active(users_ids: list):
    if not users_ids:
        return DBResponse(data=[])
    execute('UPDATE users_accounts SET is_active = TRUE WHERE id = ANY(%s::uuid[])', [users_ids])
    return DBResponse(data=[])

def _sync_change_users_network(users_ids: list, old_network_id: int, new_network_id: int):
    if not users_ids:
        return DBResponse(data=[])
    execute(
        'UPDATE users_accounts SET network_id = %s WHERE id = ANY(%s::uuid[]) AND network_id = %s',
        [new_network_id, users_ids, old_network_id],
    )
    return DBResponse(data=[])

def _sync_delete_users_by_ids(users_ids: list):
    if not users_ids:
        return DBResponse(data=[])
    execute('DELETE FROM users_accounts WHERE id = ANY(%s::uuid[])', [users_ids])
    return DBResponse(data=[])

def _sync_get_adsls_order_indexed(network_id: int):
    return DBResponse(
        data=fetch_all(
            'SELECT id, adsl_number, order_index FROM users_accounts WHERE network_id = %s AND is_active = TRUE ORDER BY adsl_number ASC',
            [network_id],
        )
    )

def _sync_get_adsl_order_index(id: str):
    row = fetch_one('SELECT order_index FROM users_accounts WHERE id = %s AND is_active = TRUE LIMIT 1', [id])
    return DBResponse(data=row or {})

def _sync_update_adsl_order_index(id: str, order_index: int):
    try:
        return call_function(
            "change_user_account_order_index",
            {"p_id": id, "p_order_index": order_index},
            param_types={"p_id": "uuid", "p_order_index": "int4"},
        )
    except Exception:
        # Fallback if the RPC isn't present in this DB.
        execute(
            "UPDATE users_accounts SET order_index = %s WHERE id = %s",
            [order_index, id],
        )
        return DBResponse(
            data=[{"success": True, "message": "order_index updated"}]
        )

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

    row = insert_returning_one(
        'INSERT INTO chats_networks (network_id, chat_user_id, network_type, permissions) VALUES (%s, %s, %s, %s) RETURNING *',
        [network_id, chat_user_id, 'partner', perm_val],
    )
    return DBResponse(data=[row] if row else [])

def _sync_activate_partnered_networks(chat_network_id: int):
    execute(
        "UPDATE chats_networks SET is_active = TRUE WHERE id = %s AND network_type = 'partner'",
        [chat_network_id],
    )
    return DBResponse(data=[])

def _sync_get_all_partnered_networks(network_id: int, with_owner: bool = False):
    if with_owner:
        rows = fetch_all('SELECT * FROM networks_details WHERE network_id = %s', [network_id])
    else:
        rows = fetch_all(
            'SELECT * FROM networks_details WHERE network_id = %s AND network_type = %s',
            [network_id, 'partner'],
        )
    return DBResponse(data=rows)


def _sync_deactivate_partnered_networks(chat_network_id: int):
    execute(
        "UPDATE chats_networks SET is_active = FALSE WHERE id = %s AND network_type = 'partner'",
        [chat_network_id],
    )
    return DBResponse(data=[])

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
    
    execute(
        "UPDATE chats_networks SET permissions = %s WHERE id = %s AND network_type = 'partner'",
        [perm_val, chat_network_id],
    )
    return DBResponse(data=[])
    

def _sync_delete_partnered_networks(chat_network_id: int):
    execute("DELETE FROM chats_networks WHERE id = %s AND network_type = 'partner'", [chat_network_id])
    return DBResponse(data=[])

def _sync_get_latest_account_data(user_id: str, is_admin: bool = False):
    def _has_col(table_name: str, column_name: str) -> bool:
        try:
            return bool(
                fetch_value(
                    """
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = %s
                      AND column_name = %s
                    LIMIT 1
                    """.strip(),
                    [table_name, column_name],
                )
            )
        except Exception:
            return False

    def _fetch_latest(table: str) -> DBResponse:
        base = f"SELECT * FROM {table} WHERE user_id = %s"
        params: list[Any] = [user_id]
        if not is_admin:
            base += " AND is_active = TRUE"

        # Prefer report_date when present; otherwise fall back to LIMIT 1 without ordering.
        order_col = "report_date" if _has_col(table, "report_date") else None
        if order_col:
            base += f" ORDER BY {order_col} DESC LIMIT 1"
        else:
            base += " LIMIT 1"

        row = fetch_one(base, params)
        return DBResponse(data=[row] if row else [])

    try:
        return _fetch_latest("adsl_daily_report")
    except psycopg2.errors.UndefinedTable:
        # Back-compat if the object name differs in some deployments
        return _fetch_latest("adsl_daily_report")
def _sync_get_user_logs(user_id: str, limit: int = 5):
    return DBResponse(
        data=fetch_all(
            'SELECT id, user_id, result, created_at FROM logs WHERE user_id = %s ORDER BY id DESC LIMIT %s',
            [user_id, limit],
        )
    )


def _sync_get_users_ordered():
    return DBResponse(
        data=fetch_all(
            'SELECT id, username, adsl_number, updated_at FROM users_accounts WHERE is_active = TRUE ORDER BY username ASC'
        )
    )


def _sync_get_daily_reports_for_users(user_ids: list, report_date: str):
    if not user_ids:
        return {"data": []}
    try:
        rows = fetch_all(
            'SELECT * FROM adsl_daily_reports WHERE user_id = ANY(%s::uuid[]) AND report_date = %s ORDER BY username ASC',
            [user_ids, report_date],
        )
    except psycopg2.errors.UndefinedColumn:
        rows = fetch_all(
            'SELECT * FROM adsl_daily_reports WHERE user_id = ANY(%s::uuid[]) AND report_date = %s ORDER BY order_index ASC, username ASC',
            [user_ids, report_date],
        )
    return DBResponse(data=rows)


async def get_daily_reports_for_users(user_ids: list, report_date: str):
    return await run_blocking(partial(_sync_get_daily_reports_for_users, user_ids, report_date))


def _sync_get_available_report_dates(user_ids: list, limit: int = 120):
    if not user_ids:
        return {"data": []}
    rows = fetch_all(
        'SELECT report_date::text FROM adsl_daily_reports WHERE user_id = ANY(%s::uuid[]) ORDER BY report_date DESC LIMIT %s',
        [user_ids, limit],
    )
    return DBResponse(data=rows)


async def get_available_report_dates(user_ids: list, limit: int = 120):
    return await run_blocking(partial(_sync_get_available_report_dates, user_ids, limit))


def _sync_get_account_available_balance(user_id: str, offset: int = 0):
    # offset allows getting the previous-day value when offset=1
    rows = fetch_all(
        'SELECT available_balance FROM account_data WHERE user_id = %s ORDER BY scraped_at DESC OFFSET %s LIMIT 1',
        [user_id, offset],
    )
    return DBResponse(data=rows)

def _sync_get_chat_user(telegram_id: str):
    row = fetch_one('SELECT * FROM chats_users WHERE telegram_id = %s LIMIT 1', [telegram_id])
    return DBResponse(data=row or {})

def _sync_get_chats_users():
    return DBResponse(data=fetch_all('SELECT * FROM chats_users'))

def _sync_active_chat_user(telegram_id: str):
    execute('UPDATE chats_users SET is_active = TRUE WHERE telegram_id = %s', [telegram_id])
    return DBResponse(data=[])

def _sync_deactivate_chat_user(telegram_id: str):
    execute('UPDATE chats_users SET is_active = FALSE WHERE telegram_id = %s', [telegram_id])
    return DBResponse(data=[])

def _sync_get_chat_users_tokens(chats_users_ids: list):
    if not chats_users_ids:
        return DBResponse(data=[])
    return DBResponse(
        data=fetch_all('SELECT telegram_id FROM chats_users WHERE id = ANY(%s::int[])', [chats_users_ids])
    )

def _sync_change_receive_partnered_reports(chat_user_id: int, receive_partnered_report: bool):
    execute(
        'UPDATE chats_users SET receive_partnered_report = %s WHERE id = %s',
        [receive_partnered_report, chat_user_id],
    )
    return DBResponse(data=[])

def _sync_create_chat_user(telegram_id: str, user_name: str):
    row = insert_returning_one(
        """INSERT INTO chats_users (telegram_id, user_name)
           VALUES (%s, %s)
           ON CONFLICT (telegram_id) DO UPDATE SET user_name = EXCLUDED.user_name
           RETURNING *""",
        [telegram_id, user_name],
    )
    return DBResponse(data=[row] if row else [])

def _sync_create_network(chat_user_id: int, network_name: str):
    return call_function(
        "create_network_for_chat_user",
        {
            "p_chat_user_id": chat_user_id,
            "p_network_name": network_name,
        },
    )

def _sync_remove_network(network_id: int):
    execute('DELETE FROM networks WHERE id = %s', [network_id])
    return DBResponse(data=[])

def _sync_active_network(network_id: int):
    return call_function("activate_network", {"p_network_id": network_id})

def _sync_deactivate_network(network_id: int):
    return call_function("deactive_network", {"p_network_id": network_id})

def _sync_get_network_by_id(chat_network_id: int):
    row = fetch_one('SELECT * FROM networks_details WHERE id = %s LIMIT 1', [chat_network_id])
    return DBResponse(data=row or {})

def _sync_get_network_by_network_id(network_id: int):
    row = fetch_one(
        'SELECT * FROM networks_details WHERE network_id = %s AND network_type = %s LIMIT 1',
        [network_id, 'owner'],
    )
    return DBResponse(data=row or {})

def _sync_get_networks_for_user(chat_user_id: int):
    rows = fetch_all('SELECT * FROM networks_details WHERE chat_user_id = %s', [chat_user_id])
    return DBResponse(data=rows)

def _sync_update_chat_user(telegram_id: str, user_name: str):
    execute('UPDATE chats_users SET user_name = %s WHERE telegram_id = %s', [user_name, telegram_id])
    return DBResponse(data=[])

def _sync_update_network(chat_network_id: int, network_name: str, times_to_send_reports: int):
    try:
        return call_function(
            "update_chat_network",
            {
                "p_chat_network_id": chat_network_id,
                "p_network_name": network_name,
                "p_times_to_send_reports": times_to_send_reports,
            },
            param_types={
                "p_chat_network_id": "int4",
                "p_network_name": "text",
                "p_times_to_send_reports": "int4",
            },
        )
    except Exception:
        execute(
            "UPDATE chats_networks SET network_name = %s, times_to_send_reports = %s WHERE id = %s",
            [network_name, times_to_send_reports, chat_network_id],
        )
        return DBResponse(data=[{"success": True, "message": "chat_network updated"}])

def _sync_change_chat_networks_times_to_send_reports(chat_network_id: int, times_to_send_reports: int):
    return call_function(
        "update_chat_network_times_to_send_reports",
        {
            "p_chat_network_id": chat_network_id,
            "p_times_to_send_reports": times_to_send_reports,
        },
    )
def _sync_change_warning_and_danger_settings(chat_network_id: int, warning_count_remaining_days: int, danger_count_remaining_days: int, warning_percentage_remaining_balance: int, danger_percentage_remaining_balance: int):
    return call_function(
        "update_chat_network_warning_and_danger_settings",
        {
            "p_chat_network_id": chat_network_id,
            "p_warning_count_remaining_days": warning_count_remaining_days,
            "p_danger_count_remaining_days": danger_count_remaining_days,
            "p_warning_percentage_remaining_balance": warning_percentage_remaining_balance,
            "p_danger_percentage_remaining_balance": danger_percentage_remaining_balance,
        },
    )

def _sync_set_selected_network(chat_network_id: int, chat_user_id: int):
    return call_function(
        "set_selected_network",
        {
            "p_network_id": chat_network_id,
            "p_chat_user_id": chat_user_id,
        },
    )


def _sync_get_selected_network(telegram_id: str):
    row = fetch_one(
        'SELECT * FROM networks_details WHERE telegram_id = %s AND is_selected_network = TRUE AND is_network_active = TRUE LIMIT 1',
        [telegram_id],
    )
    return DBResponse(data=row or {})

def _sync_get_token_by_network_id(network_id: str):
    token = fetch_value(
        "SELECT cu.telegram_id FROM chats_networks cn JOIN chats_users cu ON cu.id = cn.chat_user_id WHERE cn.network_id = %s AND cn.network_type = 'owner' LIMIT 1",
        [network_id],
    )
    if token is None:
        return DBResponse(data={})
    return DBResponse(data={"chats_users": {"telegram_id": str(token)}})

def _sync_get_all_tokens():
    return DBResponse(data=fetch_all('SELECT telegram_id FROM networks_details WHERE is_selected_network = TRUE'))

def _sync_approve_registration(
    users_ids: list,
    telegram_id: str,
    payer_chat_user_id: int,
    network_id: int,
    expiration_date: str,
    amount: int,
    payment_method: str,
):
    return call_function(
        "approve_registration",
        {
            "p_users_ids": users_ids,
            "p_telegram_id": telegram_id,
            "p_payer_chat_user_id": payer_chat_user_id,
            "p_network_id": network_id,
            "p_expiration_date": expiration_date,
            "p_amount": amount,
            "p_payment_method": payment_method,
        },
        param_types={
            "p_users_ids": "uuid[]",
            "p_telegram_id": "text",
            "p_expiration_date": "date",
            "p_payment_method": "text",
        },
    )

def _sync_change_order_by(telegram_id: str, order_by: str):
    return call_function(
        "change_chat_user_order_by",
        {"p_telegram_id": telegram_id, "p_order_by": order_by},
    )

async def count_table(tbl: str, filter_column: Optional[str] = None, filter_value: Optional[Any] = None):
    return await run_blocking(partial(_sync_count_table, tbl, filter_column, filter_value))


async def get_networks():
    return await run_blocking(_sync_get_networks)


async def get_all_users():
    return await run_blocking(_sync_get_all_users)


async def insert_pending_request(network_id: str, request_text: str):
    return await run_blocking(partial(_sync_insert_pending, network_id, request_text))


async def insert_pending_request_v2(
    request_type: str,
    payload: dict,
    requester_telegram_id: Optional[str] = None,
    network_id: Optional[int] = None,
):
    return await run_blocking(
        partial(
            _sync_insert_pending_v2,
            request_type,
            payload,
            requester_telegram_id,
            network_id,
        )
    )


async def insert_payment(
    payer_chat_user_id: int,
    network_id: int,
    amount: int,
    period_months: int,
    payment_method: Optional[str] = None,
):
    return await run_blocking(
        partial(
            _sync_insert_payment,
            payer_chat_user_id,
            network_id,
            amount,
            period_months,
            payment_method,
        )
    )


def _sync_get_pending(req_id: str):
    row = fetch_one('SELECT * FROM pending_requests WHERE id = %s LIMIT 1', [req_id])
    return DBResponse(data=row or {})


def _sync_get_pending_requests(
    status: Optional[str] = "pending",
    request_type: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    clauses = []
    params: list[Any] = []

    if status and status != "all":
        clauses.append("status = %s")
        params.append(status)

    if request_type and request_type != "all":
        if request_type == "network":
            clauses.append("request_type = %s")
            params.append("network_add")
        elif request_type == "adsl":
            clauses.append("request_type = ANY(%s::text[])")
            params.append(["adsl_add", "adsl_add_with_names"])
        else:
            clauses.append("request_type = %s")
            params.append(request_type)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"SELECT * FROM pending_requests {where_sql} ORDER BY created_at DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])
    rows = fetch_all(query, params)
    return DBResponse(data=rows)


def _sync_count_pending_requests(status: Optional[str] = "pending", request_type: Optional[str] = None) -> int:
    clauses = []
    params: list[Any] = []

    if status and status != "all":
        clauses.append("status = %s")
        params.append(status)

    if request_type and request_type != "all":
        if request_type == "network":
            clauses.append("request_type = %s")
            params.append("network_add")
        elif request_type == "adsl":
            clauses.append("request_type = ANY(%s::text[])")
            params.append(["adsl_add", "adsl_add_with_names"])
        else:
            clauses.append("request_type = %s")
            params.append(request_type)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    query = f"SELECT COUNT(*) FROM pending_requests {where_sql}"
    return int(fetch_value(query, params) or 0)


def _sync_update_pending(req_id: str, status: str):
    row = fetch_one(
        'UPDATE pending_requests SET status = %s, updated_at = NOW() WHERE id = %s RETURNING *',
        [status, req_id],
    )
    return DBResponse(data=[row] if row else [])


def _sync_update_pending_admin_msgs(req_id: str, admin_msgs: dict) -> DBResponse:
    row = fetch_one(
        """
        UPDATE pending_requests
           SET request_payload = jsonb_set(
               COALESCE(request_payload, '{}'::jsonb),
               '{admin_msgs}',
               %s::jsonb,
               true
           )
         WHERE id = %s
        RETURNING *
        """.strip(),
        [json.dumps(admin_msgs or {}), req_id],
    )
    return DBResponse(data=[row] if row else [])


def _sync_update_pending_latest_for_requester(telegram_id: str, status: str):
    row = fetch_one(
        """
        UPDATE pending_requests
           SET status = %s, updated_at = NOW()
         WHERE id = (
               SELECT id
                 FROM pending_requests
                WHERE requester_telegram_id = %s
                  AND status = 'pending'
                ORDER BY created_at DESC
                LIMIT 1
         )
        RETURNING *
        """.strip(),
        [status, telegram_id],
    )
    return DBResponse(data=[row] if row else [])


def _sync_has_pending_request(requester_telegram_id: str, request_types: Optional[list[str]] = None) -> bool:
    clauses = ["requester_telegram_id = %s", "status = 'pending'"]
    params: list[Any] = [requester_telegram_id]

    if request_types:
        clauses.append("request_type = ANY(%s::text[])")
        params.append(request_types)

    where_sql = " AND ".join(clauses)
    query = f"SELECT 1 FROM pending_requests WHERE {where_sql} LIMIT 1"
    return bool(fetch_value(query, params))


def _sync_get_pending_requests_for_requester(
    requester_telegram_id: str,
    request_types: Optional[list[str]] = None,
    network_id: Optional[int] = None,
):
    clauses = ["requester_telegram_id = %s", "status = 'pending'"]
    params: list[Any] = [requester_telegram_id]

    if request_types:
        clauses.append("request_type = ANY(%s::text[])")
        params.append(request_types)

    if network_id is not None:
        clauses.append("network_id = %s")
        params.append(network_id)

    where_sql = " AND ".join(clauses)
    query = f"SELECT * FROM pending_requests WHERE {where_sql} ORDER BY created_at DESC"
    rows = fetch_all(query, params)
    return DBResponse(data=rows)


async def get_pending_request(req_id: str):
    return await run_blocking(partial(_sync_get_pending, req_id))


async def get_pending_requests(
    status: Optional[str] = "pending",
    request_type: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
):
    return await run_blocking(partial(_sync_get_pending_requests, status, request_type, limit, offset))


async def count_pending_requests(status: Optional[str] = "pending", request_type: Optional[str] = None):
    return await run_blocking(partial(_sync_count_pending_requests, status, request_type))


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


async def update_pending_admin_msgs(req_id: str, admin_msgs: dict):
    return await run_blocking(partial(_sync_update_pending_admin_msgs, req_id, admin_msgs))


async def update_pending_status_latest_for_requester(telegram_id: str, status: str):
    return await run_blocking(partial(_sync_update_pending_latest_for_requester, telegram_id, status))


async def has_pending_request(requester_telegram_id: str, request_types: Optional[list[str]] = None) -> bool:
    return await run_blocking(partial(_sync_has_pending_request, requester_telegram_id, request_types))


async def get_pending_requests_for_requester(
    requester_telegram_id: str,
    request_types: Optional[list[str]] = None,
    network_id: Optional[int] = None,
):
    return await run_blocking(
        partial(_sync_get_pending_requests_for_requester, requester_telegram_id, request_types, network_id)
    )


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
    """Attempt to read the latest account data from the database with retries and exponential backoff.

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


async def get_users_accounts2(limit: int = 20, offset: int = 0):
    return await run_blocking(partial(_sync_get_users_accounts2, limit, offset))


async def search_users_accounts2_by_account_name(query: str, limit: int = 20, offset: int = 0):
    return await run_blocking(partial(_sync_search_users_accounts2_by_account_name, query, limit, offset))


async def count_users_accounts2_by_account_name(query: Optional[str] = None):
    return await run_blocking(partial(_sync_count_users_accounts2_by_account_name, query))
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

async def users_exists_accounts2(adsls: list):
    return await run_blocking(partial(_sync_users_exists_accounts2, adsls))

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
    payer_chat_user_id: int,
    network_id: int,
    expiration_date: str,
    amount: int,
    payment_method: str,
):
    return await run_blocking(
        partial(
            _sync_approve_registration,
            users_ids,
            telegram_id,
            payer_chat_user_id,
            network_id,
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
    """Synchronous helper returning a DBResponse for available_balance."""
    return _sync_get_account_available_balance(user_id, offset)

def sync_insert_user_account(username: str, password: str, network_id: str, adsl: Optional[str] = None):
    """Synchronous helper for inserting a user account."""
    return _sync_insert_user_account(username, password, network_id, adsl)

def sync_insert_user_account2(
    username: str,
    password: str,
    network_id: str,
    adsl: Optional[str] = None,
    account_data: Optional[Dict[str, Any]] = None,
):
    """Synchronous helper for inserting a user account into users_accounts2."""
    return _sync_insert_user_account2(username, password, network_id, adsl, account_data)

def sync_users_exists(adsls: list):
    """Synchronous helper for checking if multiple users exist."""
    return _sync_users_exists(adsls)

def sync_users_exists_accounts2(adsls: list):
    """Synchronous helper for checking if multiple users exist in users_accounts2."""
    return _sync_users_exists_accounts2(adsls)

logger = logging.getLogger("YemenNetBot.utils_shared")
SCRAPE_LOCK_TIMEOUT_SECONDS = int(os.getenv("SCRAPE_LOCK_TIMEOUT_SECONDS", "90"))
_scrape_locks: Dict[str, asyncio.Lock] = {}
_scrape_locks_guard = asyncio.Lock()


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
            logger.error("Error in blocking operation %s: %s", getattr(func, "__name__", str(func)), e)
            raise
    if last_exc:
        raise last_exc


async def _get_scrape_lock(key: str) -> asyncio.Lock:
    async with _scrape_locks_guard:
        lock = _scrape_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _scrape_locks[key] = lock
        return lock


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

    lock_key = f"{network_id}:{username}"
    lock = await _get_scrape_lock(lock_key)
    try:
        await asyncio.wait_for(lock.acquire(), timeout=SCRAPE_LOCK_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning("Scrape lock timeout for %s", lock_key)
        return False

    try:
        user = await UserManager.get_user_data(username, network_id, is_admin)
        if not user:
            logger.warning("Unauthorized scrape attempt: %s by network %s", username, network_id)
            return False

        loop = asyncio.get_running_loop()

        @retry(
            reraise=True,
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
            retry=retry_if_exception_type(Exception),
        )
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

        logger.error("❌ Failed to fetch or save data for %s under network %s", username, network_id)
        return False
    finally:
        if lock.locked():
            lock.release()