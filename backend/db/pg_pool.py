from __future__ import annotations

import os
import time
from contextlib import contextmanager
import threading
import traceback
import inspect
import logging
import json
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

# pandas 2.x 对 psycopg2 原生连接发出 UserWarning（非 SQLAlchemy），
# 这是已知兼容用法，全局抑制此警告避免日志噪音
import warnings
warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy connectable",
    category=UserWarning,
    module="pandas",
)


_DB_POOL: Optional[ThreadedConnectionPool] = None
logger = logging.getLogger("aistock.db.pg_pool")

DEFAULT_STATEMENT_TIMEOUT_MS = 60_000

# Track checked-out connections to identify long holders / pool starvation.
_CHECKED_OUT: Dict[int, Dict[str, Any]] = {}
_CHECKED_OUT_LOCK = threading.RLock()


def _env_truthy(key: str) -> bool:
    v = (os.getenv(key) or "").strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _conn_audit_enabled() -> bool:
    return _env_truthy("AISTOCK_DB_CONN_AUDIT") or _env_truthy("DB_POOL_DEBUG")


def _pool_state(pool: Optional[ThreadedConnectionPool]) -> Dict[str, Any]:
    if pool is None:
        return {"pool_free": None, "pool_used": None, "pool_max": None}
    state: Dict[str, Any] = {"pool_free": None, "pool_used": None, "pool_max": getattr(pool, "maxconn", None)}
    try:
        state["pool_used"] = len(getattr(pool, "_used", {}) or {})
    except Exception as exc:
        state["pool_used_error"] = f"{type(exc).__name__}: {exc}"
    try:
        state["pool_free"] = len(getattr(pool, "_pool", []) or [])
    except Exception as exc:
        state["pool_free_error"] = f"{type(exc).__name__}: {exc}"
    return state


def _emit_conn_audit_metric(event: str, **fields: Any) -> None:
    """Emit structured DB connection audit data for production incident triage."""

    if not (_conn_audit_enabled() or event in {"checkout_slow", "held_slow", "connect_slow"}):
        return
    payload: Dict[str, Any] = {
        "metric": "db_connection_audit",
        "event": event,
        "pid": os.getpid(),
        "thread": threading.current_thread().name,
    }
    payload.update(fields)
    logger.info(
        "db_connection_audit %s",
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        extra={"aistock_metric": payload},
    )


def _emit_checkout_audit(
    *,
    mode: str,
    duration: float,
    caller: str,
    statement_timeout_ms: Optional[int],
    pool: Optional[ThreadedConnectionPool] = None,
) -> None:
    event = "checkout_slow" if duration > 0.1 else "checkout"
    _emit_conn_audit_metric(
        event,
        mode=mode,
        duration_ms=round(duration * 1000, 3),
        caller=caller,
        statement_timeout_ms=statement_timeout_ms,
        **_pool_state(pool),
    )


def _checked_out_snapshot(limit: int = 5) -> str:
    """Render a short snapshot of currently checked-out connections."""

    now = time.time()
    with _CHECKED_OUT_LOCK:
        items = []
        for _, info in _CHECKED_OUT.items():
            try:
                held = now - float(info.get("checkout_ts", now))
            except Exception as exc:
                logger.warning(
                    "DB connection checkout timestamp parse failed; reason_code=AISTOCK_DB_CHECKOUT_TS_PARSE_FAILED error=%s",
                    f"{type(exc).__name__}: {exc}",
                    exc_info=True,
                )
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
    except Exception as exc:
        logger.warning(
            "DB caller hint collection failed; reason_code=AISTOCK_DB_CALLER_HINT_FAILED error=%s",
            f"{type(exc).__name__}: {exc}",
            exc_info=True,
        )
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


def _statement_timeout_ms() -> int:
    v = (os.getenv("AISTOCK_PG_STATEMENT_TIMEOUT_MS") or str(DEFAULT_STATEMENT_TIMEOUT_MS)).strip()
    try:
        ms = int(v)
        if ms < 0:
            return DEFAULT_STATEMENT_TIMEOUT_MS
    except Exception as exc:
        logger.warning(
            "Invalid AISTOCK_PG_STATEMENT_TIMEOUT_MS; using default. reason_code=AISTOCK_DB_STATEMENT_TIMEOUT_INVALID value=%r error=%s",
            v,
            f"{type(exc).__name__}: {exc}",
            exc_info=True,
        )
        return DEFAULT_STATEMENT_TIMEOUT_MS
    return ms


def _apply_statement_timeout(conn: psycopg2.extensions.connection) -> Optional[int]:
    ms = _statement_timeout_ms()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout TO {ms}")
        return ms
    except Exception as exc:
        logger.warning(
            "Failed to apply DB statement_timeout; reason_code=AISTOCK_DB_STATEMENT_TIMEOUT_APPLY_FAILED timeout_ms=%s error=%s",
            ms,
            f"{type(exc).__name__}: {exc}",
            exc_info=True,
        )
        return None


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
    except Exception as exc:
        # Fallback: keep _DB_POOL as None so that get_conn() uses direct connections.
        logger.warning(
            "DB pool initialization failed; falling back to direct connections. reason_code=AISTOCK_DB_POOL_INIT_FAILED error=%s",
            f"{type(exc).__name__}: {exc}",
            exc_info=True,
        )
        _DB_POOL = None


def close_db_pool() -> None:
    """Close all connections in the global pool (if any)."""

    global _DB_POOL
    if _DB_POOL is not None:
        try:
            _DB_POOL.closeall()
        except Exception as exc:
            logger.warning(
                "DB pool closeall failed; reason_code=AISTOCK_DB_POOL_CLOSE_FAILED error=%s",
                f"{type(exc).__name__}: {exc}",
                exc_info=True,
            )
        _DB_POOL = None


def _prepare_connection(conn: psycopg2.extensions.connection, *, autocommit: bool) -> Optional[int]:
    """Apply session options outside the caller transaction, then enter the requested mode."""

    original_autocommit = bool(conn.autocommit)
    original_status = conn.get_transaction_status()
    if original_status != psycopg2.extensions.TRANSACTION_STATUS_IDLE:
        try:
            conn.rollback()
        except Exception:
            conn.autocommit = original_autocommit
            raise
    conn.autocommit = True
    try:
        statement_timeout_ms = _apply_statement_timeout(conn)
    except Exception:
        conn.autocommit = original_autocommit
        raise
    conn.autocommit = autocommit
    return statement_timeout_ms


def _commit_connection(conn: psycopg2.extensions.connection, *, mode: str, manage_transaction: bool) -> None:
    if not manage_transaction:
        return
    if conn.autocommit:
        return
    try:
        conn.commit()
    except Exception as exc:
        _emit_conn_audit_metric(
            "transaction_commit_failed",
            mode=mode,
            error=f"{type(exc).__name__}: {exc}",
            reason_code="AISTOCK_DB_TRANSACTION_COMMIT_FAILED",
        )
        raise


def _rollback_connection(
    conn: psycopg2.extensions.connection,
    *,
    mode: str,
    original_error: BaseException,
    manage_transaction: bool,
) -> None:
    if not manage_transaction:
        return
    if conn.autocommit:
        return
    try:
        conn.rollback()
    except Exception as exc:
        _emit_conn_audit_metric(
            "transaction_rollback_failed",
            mode=mode,
            error=f"{type(exc).__name__}: {exc}",
            original_error=f"{type(original_error).__name__}: {original_error}",
            reason_code="AISTOCK_DB_TRANSACTION_ROLLBACK_FAILED",
        )
        raise RuntimeError(
            "DB transaction rollback failed; reason_code=AISTOCK_DB_TRANSACTION_ROLLBACK_FAILED"
        ) from exc


@contextmanager
def get_conn(*, autocommit: bool = False, manage_transaction: bool = True):
    """Yield a DB connection, using pool when available.

    - 优先使用本进程内的连接池，减少建连开销；
    - 若池未初始化或初始化失败，则退回到临时直连模式。
    """

    global _DB_POOL
    start_time = time.time()
    if _DB_POOL is None:
        caller = _caller_hint() if _conn_audit_enabled() else "<audit disabled>"
        try:
            if _env_truthy("DB_POOL_DEBUG"):
                print(
                    "DEBUG: DB checkout (direct). pid=%s thread=%s caller=%s"
                    % (os.getpid(), threading.current_thread().name, caller)
                )
            conn = psycopg2.connect(**_db_cfg())
            statement_timeout_ms = _prepare_connection(conn, autocommit=autocommit)
            duration = time.time() - start_time
            if duration > 0.1:
                print(f"DEBUG: Direct DB connection took {duration:.4f}s")
            if duration > 0.1 and caller == "<audit disabled>":
                caller = _caller_hint()
            _emit_checkout_audit(
                mode="direct",
                duration=duration,
                caller=caller,
                statement_timeout_ms=statement_timeout_ms,
            )
            try:
                try:
                    yield conn
                    _commit_connection(conn, mode="direct", manage_transaction=manage_transaction)
                except Exception as exc:
                    _rollback_connection(
                        conn,
                        mode="direct",
                        original_error=exc,
                        manage_transaction=manage_transaction,
                    )
                    raise
            finally:
                conn.close()
        except Exception as e:
            _emit_conn_audit_metric("connection_failed", mode="direct", error=str(e), caller=caller)
            print(f"DEBUG: DB connection failed: {e}")
            raise
        return

    try:
        caller = _caller_hint() if _conn_audit_enabled() else "<audit disabled>"
        if _env_truthy("DB_POOL_DEBUG"):
            print(
                "DEBUG: DB checkout (pool). pid=%s thread=%s caller=%s"
                % (os.getpid(), threading.current_thread().name, caller)
            )
        conn = _DB_POOL.getconn()
        duration = time.time() - start_time
        if duration > 0.1:
            print(f"DEBUG: Pool getconn took {duration:.4f}s")
        if duration > 0.1 and caller == "<audit disabled>":
            caller = _caller_hint()
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
                    except Exception as exc:
                        logger.warning(
                            "DB pool used-count snapshot failed; reason_code=AISTOCK_DB_POOL_USED_SNAPSHOT_FAILED error=%s",
                            f"{type(exc).__name__}: {exc}",
                            exc_info=True,
                        )
                        used_n = None
                    try:
                        free_n = len(getattr(pool, "_pool", []) or [])
                    except Exception as exc:
                        logger.warning(
                            "DB pool free-count snapshot failed; reason_code=AISTOCK_DB_POOL_FREE_SNAPSHOT_FAILED error=%s",
                            f"{type(exc).__name__}: {exc}",
                            exc_info=True,
                        )
                        free_n = None
                    max_n = getattr(pool, "maxconn", None)
                print(
                    "DEBUG: Pool state after slow getconn: free=%s used=%s max=%s lock_locked=%s"
                    % (free_n, used_n, max_n, lock_state)
                )
            except Exception as exc:
                logger.warning(
                    "DB pool slow-checkout diagnostic snapshot failed; reason_code=AISTOCK_DB_POOL_SLOW_DIAGNOSTIC_FAILED error=%s",
                    f"{type(exc).__name__}: {exc}",
                    exc_info=True,
                )
            if _env_truthy("DB_POOL_DEBUG") or _env_truthy("DB_POOL_DEBUG_SNAPSHOT"):
                try:
                    print(
                        "DEBUG: Pool starvation snapshot\n%s"
                        % _checked_out_snapshot(limit=5)
                    )
                except Exception as exc:
                    logger.warning(
                        "DB pool starvation snapshot failed; reason_code=AISTOCK_DB_POOL_STARVATION_SNAPSHOT_FAILED error=%s",
                        f"{type(exc).__name__}: {exc}",
                        exc_info=True,
                    )
        try:
            statement_timeout_ms = _prepare_connection(conn, autocommit=autocommit)
            _emit_checkout_audit(
                mode="pool",
                duration=duration,
                caller=caller,
                statement_timeout_ms=statement_timeout_ms,
                pool=_DB_POOL,
            )
            with _CHECKED_OUT_LOCK:
                _CHECKED_OUT[id(conn)] = {
                    "checkout_ts": time.time(),
                    "thread": threading.current_thread().name,
                    "stack": "".join(traceback.format_stack(limit=12)),
                }
            try:
                yield conn
                _commit_connection(conn, mode="pool", manage_transaction=manage_transaction)
            except Exception as exc:
                _rollback_connection(
                    conn,
                    mode="pool",
                    original_error=exc,
                    manage_transaction=manage_transaction,
                )
                raise
        finally:
            checkout_info: Optional[Dict[str, Any]] = None
            with _CHECKED_OUT_LOCK:
                checkout_info = _CHECKED_OUT.pop(id(conn), None)
            if checkout_info is not None:
                held = time.time() - float(checkout_info.get("checkout_ts", time.time()))
                if held > 1.0:
                    _emit_conn_audit_metric(
                        "held_slow",
                        mode="pool",
                        held_ms=round(held * 1000, 3),
                        **_pool_state(_DB_POOL),
                    )
                    print(
                        "DEBUG: DB conn held %.3fs (>1s). thread=%s\n%s"
                        % (held, checkout_info.get("thread"), checkout_info.get("stack"))
                    )
            pool = _DB_POOL
            if pool is None:
                # Pool may have been closed during shutdown; do a best-effort close.
                try:
                    conn.close()
                except Exception as exc:
                    logger.warning(
                        "DB connection close after pool shutdown failed; reason_code=AISTOCK_DB_CONN_CLOSE_FAILED error=%s",
                        f"{type(exc).__name__}: {exc}",
                        exc_info=True,
                    )
            else:
                pool.putconn(conn)
    except Exception as e:
        _emit_conn_audit_metric("connection_failed", mode="pool", error=str(e), **_pool_state(_DB_POOL))
        print(f"DEBUG: Pool operation failed: {e}")
        raise
