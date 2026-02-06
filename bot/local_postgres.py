import os
import re
import threading
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor


@dataclass
class DBResponse:
    """Minimal supabase-like response wrapper.

    Many callers in this codebase expect an object with a `.data` attribute
    (list/dict) and sometimes `.count`.
    """

    data: Any = None
    count: Optional[int] = None


_VALID_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)).strip())
    except Exception:
        return default


def _db_config() -> dict[str, Any]:
    # Defaults match the local setup described in the migration conversation.
    return {
        "host": os.getenv("LOCAL_PG_HOST", "localhost"),
        "port": _env_int("LOCAL_PG_PORT", 5432),
        "dbname": os.getenv("LOCAL_PG_DB", "yemen_net_postgres"),
        "user": os.getenv("LOCAL_PG_USER", "postgres"),
        "password": os.getenv("LOCAL_PG_PASSWORD", "alnomily_2024"),
        # Keep connections responsive.
        "connect_timeout": _env_int("LOCAL_PG_CONNECT_TIMEOUT", 10),
    }


_thread = threading.local()


def get_conn():
    conn = getattr(_thread, "conn", None)
    if conn is None or getattr(conn, "closed", 1):
        conn = psycopg2.connect(**_db_config())
        # Autocommit reduces the chance of leaked open transactions when running
        # many short queries in threadpool workers.
        conn.autocommit = True
        _thread.conn = conn
    return conn


def _validate_ident(name: str) -> None:
    if not _VALID_IDENT.match(name or ""):
        raise ValueError(f"Invalid SQL identifier: {name!r}")


def fetch_all(query: str, params: Optional[Iterable[Any]] = None) -> list[dict[str, Any]]:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        rows = cur.fetchall() or []
        return [dict(r) for r in rows]


def fetch_one(query: str, params: Optional[Iterable[Any]] = None) -> Optional[dict[str, Any]]:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


def execute(query: str, params: Optional[Iterable[Any]] = None) -> int:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        return cur.rowcount


def fetch_value(query: str, params: Optional[Iterable[Any]] = None) -> Any:
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return row[0] if row else None


def insert_returning_one(query: str, params: Optional[Iterable[Any]] = None) -> Optional[dict[str, Any]]:
    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
        return dict(row) if row else None


def call_function(
    function_name: str,
    params: Optional[Mapping[str, Any]] = None,
    *,
    schema: str = "public",
) -> DBResponse:
    """Call a SQL function by name using named parameters.

    Example:
      call_function('activate_network', {'p_network_id': 1})
    """

    _validate_ident(schema)
    _validate_ident(function_name)

    params = params or {}
    for k in params.keys():
        _validate_ident(k)

    keys = list(params.keys())
    values = [params[k] for k in keys]

    if keys:
        args_sql = sql.SQL(", ").join([sql.SQL("{} => %s").format(sql.Identifier(k)) for k in keys])
    else:
        args_sql = sql.SQL("")

    q = sql.SQL("SELECT * FROM {}.{}({})").format(
        sql.Identifier(schema),
        sql.Identifier(function_name),
        args_sql,
    )

    conn = get_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, values)
        try:
            rows = cur.fetchall()
            return DBResponse(data=[dict(r) for r in (rows or [])])
        except psycopg2.ProgrammingError:
            # No results (e.g. VOID function)
            return DBResponse(data=[])


def count_table(table: str, filter_column: Optional[str] = None, filter_value: Any = None) -> int:
    _validate_ident(table)
    if filter_column:
        _validate_ident(filter_column)

    if filter_column and filter_value is not None:
        q = sql.SQL("SELECT COUNT(*) FROM {} WHERE {} = %s").format(
            sql.Identifier(table),
            sql.Identifier(filter_column),
        )
        return int(fetch_value(q.as_string(get_conn()), [filter_value]) or 0)

    q = sql.SQL("SELECT COUNT(*) FROM {}" ).format(sql.Identifier(table))
    return int(fetch_value(q.as_string(get_conn())) or 0)
