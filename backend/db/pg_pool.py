from __future__ import annotations

import os
import time
from contextlib import contextmanager
import threading
import traceback
import inspect
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool


_DB_POOL: Optional[ThreadedConnectionPool] = None

# Track checked-out connections to identify long holders / pool starvation.
_CHECKED_OUT: Dict[int, Dict[str, Any]] = {}
_CHECKED_OUT_LOCK = threading.RLock()


def _env_truthy(key: str) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _checked_out_snapshot(limit: int = 5) -> str:
    """Render a short snapshot of currently checked-out connections."""

    now = time.time()
    with _CHECKED_OUT_LOCK:
        items = []
        for _, info in _CHECKED_OUT.items():
            try:
                held = now - float(info.get("checkout_ts", now))
            except Exception:
                held = 0.0
            items.append((held, info))
        items.sort(key=lambda x: x[0], reverse=True)

    lines = [f"checked_out={len(items)} top={min(len(items), limit)}"]
    for held, info in items[:limit]:
        lines.append(
            "held=%.3fs thread=%s\n%s"
            % (held, info.get("thread"), info.get("stack"))
        )
    return "\n".join(lines)


def _caller_hint() -> str:
    """Best-effort caller hint for debugging pool starvation."""

    try:
        # Walk up the stack and find the first *application* frame.
        # Prefer paths under the repo workspace, skip stdlib/site-packages wrappers.
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
        repo_root_norm = repo_root.replace("\\", "/").lower()
        for frame_info in inspect.stack()[2:40]:
            fn = frame_info.filename.replace("\\", "/")
            fn_l = fn.lower()
            if fn_l.endswith("/backend/db/pg_pool.py"):
                continue
            # Skip common wrappers
            if "/lib/" in fn_l or "/site-packages/" in fn_l:
                continue
            if fn_l.endswith("/contextlib.py") or fn_l.endswith("/threading.py"):
                continue
            if repo_root_norm and fn_l.startswith(repo_root_norm):
                return f"{fn}:{frame_info.lineno} in {frame_info.function}"
        # Fallback: return first frame outside this file.
        for frame_info in inspect.stack()[2:40]:
            fn = frame_info.filename.replace("\\", "/")
            if not fn.lower().endswith("/backend/db/pg_pool.py"):
                return f"{fn}:{frame_info.lineno} in {frame_info.function}"
    except Exception:
        return "<caller unavailable>"
    return "<caller not found>"


def _db_cfg() -> Dict[str, Any]:
    """Build DB config from environment variables.

    与旧后端保持同一套 TDX_DB_* 环境变量约定，避免重复配置。
    """

    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "application_name": "AIstock-backend",
        "options": "-c client_encoding=utf8",
    }


def _apply_statement_timeout(conn: psycopg2.extensions.connection) -> None:
    v = (os.getenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS") or "").strip()
    if not v:
        return
    try:
        ms = int(v)
        if ms < 0:
            return
    except Exception:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout TO {ms}")
    except Exception:
        return


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
        class _LoggedThreadedConnectionPool(ThreadedConnectionPool):
            def _connect(self, key=None):  # type: ignore[override]
                t0 = time.time()
                conn = super()._connect(key)
                dt = time.time() - t0
                if dt > 0.2 or _env_truthy("DB_POOL_DEBUG"):
                    print(
                        "DEBUG: Pool _connect took %.4fs pid=%s thread=%s"
                        % (dt, os.getpid(), threading.current_thread().name)
                    )
                return conn

        _DB_POOL = _LoggedThreadedConnectionPool(minconn, maxconn, **cfg)
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
    start_time = time.time()
    if _DB_POOL is None:
        try:
            if _env_truthy("DB_POOL_DEBUG"):
                print(
                    "DEBUG: DB checkout (direct). pid=%s thread=%s caller=%s"
                    % (os.getpid(), threading.current_thread().name, _caller_hint())
                )
            conn = psycopg2.connect(**_db_cfg())
            conn.autocommit = True
            _apply_statement_timeout(conn)
            duration = time.time() - start_time
            if duration > 0.1:
                print(f"DEBUG: Direct DB connection took {duration:.4f}s")
            try:
                yield conn
            finally:
                conn.close()
        except Exception as e:
            print(f"DEBUG: DB connection failed: {e}")
            raise
        return

    try:
        if _env_truthy("DB_POOL_DEBUG"):
            print(
                "DEBUG: DB checkout (pool). pid=%s thread=%s caller=%s"
                % (os.getpid(), threading.current_thread().name, _caller_hint())
            )
        conn = _DB_POOL.getconn()
        duration = time.time() - start_time
        if duration > 0.1:
            print(f"DEBUG: Pool getconn took {duration:.4f}s")
        if duration > 0.5:
            stack = "".join(traceback.format_stack(limit=12))
            print(
                "DEBUG: Pool getconn slow (>0.5s). thread=%s\n%s"
                % (threading.current_thread().name, stack)
            )
            try:
                pool = _DB_POOL
                lock_state = None
                if pool is not None and hasattr(pool, "_lock") and hasattr(pool._lock, "locked"):
                    lock_state = pool._lock.locked()  # type: ignore[attr-defined]
                used_n = None
                free_n = None
                max_n = None
                if pool is not None:
                    try:
                        used_n = len(getattr(pool, "_used", {}) or {})
                    except Exception:
                        used_n = None
                    try:
                        free_n = len(getattr(pool, "_pool", []) or [])
                    except Exception:
                        free_n = None
                    max_n = getattr(pool, "maxconn", None)
                print(
                    "DEBUG: Pool state after slow getconn: free=%s used=%s max=%s lock_locked=%s"
                    % (free_n, used_n, max_n, lock_state)
                )
            except Exception:
                pass
            if _env_truthy("DB_POOL_DEBUG") or _env_truthy("DB_POOL_DEBUG_SNAPSHOT"):
                try:
                    print(
                        "DEBUG: Pool starvation snapshot\n%s"
                        % _checked_out_snapshot(limit=5)
                    )
                except Exception:
                    pass
        try:
            conn.autocommit = True
            _apply_statement_timeout(conn)
            with _CHECKED_OUT_LOCK:
                _CHECKED_OUT[id(conn)] = {
                    "checkout_ts": time.time(),
                    "thread": threading.current_thread().name,
                    "stack": "".join(traceback.format_stack(limit=12)),
                }
            yield conn
        finally:
            checkout_info: Optional[Dict[str, Any]] = None
            with _CHECKED_OUT_LOCK:
                checkout_info = _CHECKED_OUT.pop(id(conn), None)
            if checkout_info is not None:
                held = time.time() - float(checkout_info.get("checkout_ts", time.time()))
                if held > 1.0:
                    print(
                        "DEBUG: DB conn held %.3fs (>1s). thread=%s\n%s"
                        % (held, checkout_info.get("thread"), checkout_info.get("stack"))
                    )
            pool = _DB_POOL
            if pool is None:
                # Pool may have been closed during shutdown; do a best-effort close.
                try:
                    conn.close()
                except Exception:
                    pass
            else:
                pool.putconn(conn)
    except Exception as e:
        print(f"DEBUG: Pool operation failed: {e}")
        raise
