from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool


_DB_POOL: Optional[ThreadedConnectionPool] = None


def _db_cfg() -> Dict[str, Any]:
    """Build DB config from environment variables.

    与旧后端保持同一套 TDX_DB_* 环境变量约定，避免重复配置。
    """

    return {
        "host": os.getenv("TDX_DB_HOST", "localhost"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
    }


def init_db_pool(minconn: int = 1, maxconn: int = 10) -> None:
    """Initialize global psycopg2 connection pool for this backend process.

    - 仅在 next_app FastAPI 进程中使用连接池；
    - 若初始化失败，则退回到按需直连模式，保持兼容性。
    """

    global _DB_POOL
    if _DB_POOL is not None:
        return

    cfg = _db_cfg()
    try:
        _DB_POOL = ThreadedConnectionPool(minconn, maxconn, **cfg)
    except Exception:
        # Fallback: keep _DB_POOL as None so that get_conn() uses direct connections.
        _DB_POOL = None


def close_db_pool() -> None:
    """Close all connections in the global pool (if any)."""

    global _DB_POOL
    if _DB_POOL is not None:
        try:
            _DB_POOL.closeall()
        except Exception:
            pass
        _DB_POOL = None


@contextmanager
def get_conn():
    """Yield a DB connection, using pool when available.

    - 优先使用本进程内的连接池，减少建连开销；
    - 若池未初始化或初始化失败，则退回到临时直连模式。
    """

    global _DB_POOL

    if _DB_POOL is None:
        conn = psycopg2.connect(**_db_cfg())
        conn.autocommit = True
        try:
            yield conn
        finally:
            conn.close()
        return

    conn = _DB_POOL.getconn()
    try:
        conn.autocommit = True
        yield conn
    finally:
        try:
            _DB_POOL.putconn(conn)
        except Exception:
            pass
