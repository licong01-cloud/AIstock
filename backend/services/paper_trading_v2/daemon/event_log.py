"""Daemon event log — PG outbox (primary) + SQLite (fallback).

T6.2 (D2 path A) refactor (2026-05-10):

The daemon now writes events directly to ``qe_archive.outbox_event`` so the
DW ETL can ingest them without a sqlite replay sidecar. The SQLite store
(``var/paper_v2_sim/daemon_events.db``) remains as a *fallback*: if PG is
unreachable / mis-configured at emit time, the row lands in SQLite with
``unsynced=1`` and is later pushed by ``replay_unsynced_on_startup``.

Failure semantics (per ``feedback_no_silent_errors``):

* PG write succeeds -> row not duplicated to SQLite.
* PG write fails, SQLite succeeds -> emit() returns the record, logs a
  WARNING with the original PG error; row carries ``unsynced=1`` for replay.
* PG write fails AND SQLite write fails -> the original SQLite exception
  propagates (no silent swallow). PG error is logged before re-raise.

The 9 canonical event-type names follow ``paper.daemon.<event>`` and are
mapped onto the existing ``DaemonEventType`` enum members so existing call
sites (sim_runner.py) stay unchanged.

Outbox row shape (mirroring ``backend/db/init_qe_archive_schema.py``):

* event_id      = ``qear_evt_<sha256-of-fingerprint>[:24]`` (idempotent)
* event_type    = ``paper.daemon.<name>``
* source_system = ``paper_v2.daemon``
* source_id     = ``run_id``
* source_sub_id = ``f"{event_seq:06d}"`` (unique per (run_id, event_seq))
* payload       = JSONB containing portfolio_id, package_id, event_seq,
                  event_ts, handle_id, intent_id, symbol, payload_json
* status        = 'pending' (the qe_archive worker will claim & process it)

We do NOT modify ``qe_archive`` source code; we only INSERT rows into the
existing table via the canonical ``backend/db/pg_pool.get_conn`` pattern.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Canonical event-type strings (T6.2)
#
# These are the strings written to ``qe_archive.outbox_event.event_type``.
# The DaemonEventType enum below stores the SQLite-side internal name; the
# PG-side canonical name is looked up via PAPER_DAEMON_EVENT_TYPE_NAMES.
# ---------------------------------------------------------------------------

PAPER_DAEMON_EVENT_TYPE_NAMES: dict[str, str] = {
    "RUN_STARTED": "paper.daemon.run_started",
    "INTENT_CREATED": "paper.daemon.intent_created",
    "ORDER_SUBMITTED": "paper.daemon.order_submitted",
    "FILL_RECEIVED": "paper.daemon.fill_received",
    "ORDER_REJECTED": "paper.daemon.order_rejected",
    "ORDER_CANCELLED": "paper.daemon.order_cancelled",
    "POSITION_UPDATED": "paper.daemon.position_updated",
    "RUN_COMPLETED": "paper.daemon.run_completed",
    "RUN_FAILED": "paper.daemon.run_failed",
}


PAPER_DAEMON_SOURCE_SYSTEM = "paper_v2.daemon"


class DaemonEventType(str, Enum):
    """Types written by ``PaperV2SimRunner``.

    Names mirror Engine §3.6.1 OrderHandle states + lifecycle markers added
    by the daemon for run-level audit (RUN_STARTED / RUN_COMPLETED).
    """

    RUN_STARTED = "RUN_STARTED"
    INTENT_CREATED = "INTENT_CREATED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    FILL_RECEIVED = "FILL_RECEIVED"
    ORDER_REJECTED = "ORDER_REJECTED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    POSITION_UPDATED = "POSITION_UPDATED"
    RUN_COMPLETED = "RUN_COMPLETED"
    RUN_FAILED = "RUN_FAILED"

    @property
    def canonical_name(self) -> str:
        """Return the canonical ``paper.daemon.<x>`` string for PG outbox."""
        return PAPER_DAEMON_EVENT_TYPE_NAMES[self.value]


@dataclass(frozen=True)
class DaemonEventRecord:
    run_id: str
    portfolio_id: str
    package_id: str
    event_type: DaemonEventType
    event_seq: int
    event_ts: datetime
    payload: dict[str, Any]
    handle_id: str | None = None
    intent_id: str | None = None
    symbol: str | None = None


_DDL = """
CREATE TABLE IF NOT EXISTS daemon_event_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id       TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    package_id   TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    event_seq    INTEGER NOT NULL,
    event_ts     TEXT NOT NULL,
    handle_id    TEXT,
    intent_id    TEXT,
    symbol       TEXT,
    payload_json TEXT NOT NULL,
    unsynced     INTEGER NOT NULL DEFAULT 1,
    synced_at    TEXT,
    UNIQUE(run_id, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_daemon_event_log_run ON daemon_event_log(run_id, event_seq);
CREATE INDEX IF NOT EXISTS idx_daemon_event_log_portfolio ON daemon_event_log(portfolio_id);
CREATE INDEX IF NOT EXISTS idx_daemon_event_log_unsynced ON daemon_event_log(unsynced);
"""


# Sentinel used to indicate the PG connection factory has been disabled for
# this writer (e.g. SQLite-only test mode).
class _PGDisabled:
    pass


_PG_DISABLED_SENTINEL = _PGDisabled()


def _default_pg_conn_provider():
    """Return a context manager yielding a psycopg2 connection.

    Lazily imports ``backend.db.pg_pool`` so that import of this module does
    NOT pull in psycopg2 if the daemon is being used in pure-SQLite mode
    (e.g. CLI demo_run.py with no DB available).
    """
    from backend.db.pg_pool import get_conn

    return get_conn()


def _build_outbox_event_id(run_id: str, event_seq: int) -> str:
    """Stable, idempotent event_id matching qe_archive convention.

    Mirrors ``OutboxEventRecord.__post_init__``: ``qear_evt_<sha256[:24]>``
    over a fingerprint of (event_type, source_system, source_id, source_sub_id).
    Replays must produce the same event_id to hit the ON CONFLICT DO NOTHING.
    """
    fingerprint = json.dumps(
        {
            "source_system": PAPER_DAEMON_SOURCE_SYSTEM,
            "source_id": run_id,
            "source_sub_id": f"{event_seq:06d}",
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()
    return f"qear_evt_{digest[:24]}"


class DaemonEventLog:
    """Append-only writer for paper-v2 sim daemon events.

    One writer instance pins one ``run_id``. Caller is responsible for
    creating a new instance per simulated run.

    Thread-safe: ``record()`` and ``read_all()`` acquire an internal lock, so
    multi-callback fan-out (LocalSim subscribe_fill_callback) can safely write
    from background threads.

    Persistence:
      * primary: ``qe_archive.outbox_event`` (path A, T6.2)
      * fallback: SQLite at ``db_path``; rows mark ``unsynced=1`` and are
        retried by ``replay_unsynced_on_startup``.
    """

    def __init__(
        self,
        *,
        db_path: Path | str,
        run_id: str | None = None,
        portfolio_id: str,
        package_id: str,
        pg_conn_provider: Any = None,
    ) -> None:
        if not portfolio_id:
            raise ValueError("portfolio_id is required")
        if not package_id:
            raise ValueError("package_id is required")
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._run_id = run_id or f"run_{uuid4().hex}"
        self._portfolio_id = portfolio_id
        self._package_id = package_id
        self._lock = threading.Lock()
        self._seq = 0
        # pg_conn_provider is a zero-arg callable returning a context manager
        # that yields a psycopg2 connection. ``None`` means "use the canonical
        # backend.db.pg_pool.get_conn at first emit". The sentinel
        # _PG_DISABLED_SENTINEL means "skip PG entirely" (tests / SQLite-only).
        self._pg_conn_provider = pg_conn_provider
        self._pg_disabled_reason: str | None = None
        self._init_schema()

    @property
    def run_id(self) -> str:
        return self._run_id

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def portfolio_id(self) -> str:
        return self._portfolio_id

    @property
    def package_id(self) -> str:
        return self._package_id

    @property
    def pg_disabled_reason(self) -> str | None:
        return self._pg_disabled_reason

    def disable_pg(self, reason: str) -> None:
        """Disable PG writes for the lifetime of this writer (SQLite only)."""
        self._pg_conn_provider = _PG_DISABLED_SENTINEL
        self._pg_disabled_reason = reason

    # ------------------------------------------------------------------
    # SQLite schema
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        # WAL mode lets reads and the single writer coexist without contention.
        conn = sqlite3.connect(self._db_path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(_DDL)
            # In-place migration for legacy DB files that pre-date T6.2.
            # SQLite doesn't support ADD COLUMN IF NOT EXISTS, so we probe.
            existing_cols = {
                row[1]
                for row in conn.execute("PRAGMA table_info(daemon_event_log)").fetchall()
            }
            if "unsynced" not in existing_cols:
                conn.execute(
                    "ALTER TABLE daemon_event_log "
                    "ADD COLUMN unsynced INTEGER NOT NULL DEFAULT 1"
                )
            if "synced_at" not in existing_cols:
                conn.execute(
                    "ALTER TABLE daemon_event_log ADD COLUMN synced_at TEXT"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_daemon_event_log_unsynced "
                "ON daemon_event_log(unsynced)"
            )

    # ------------------------------------------------------------------
    # Emit (PG primary, SQLite fallback)
    # ------------------------------------------------------------------

    def record(
        self,
        event_type: DaemonEventType,
        payload: dict[str, Any],
        *,
        handle_id: str | None = None,
        intent_id: str | None = None,
        symbol: str | None = None,
        event_ts: datetime | None = None,
    ) -> DaemonEventRecord:
        """Append a single event. Returns the persisted record.

        Primary path: PG outbox. On failure, falls back to SQLite with
        ``unsynced=1`` and logs a warning. If SQLite ALSO fails, the original
        SQLite exception propagates (no silent swallow).
        """
        if not isinstance(event_type, DaemonEventType):
            raise TypeError(
                f"event_type must be DaemonEventType, got {type(event_type)!r}"
            )
        ts = event_ts or datetime.now(UTC)
        payload_json = json.dumps(payload, default=_json_default, ensure_ascii=False)

        with self._lock:
            self._seq += 1
            seq = self._seq

            pg_synced = False
            pg_error: Exception | None = None
            try:
                self._write_pg(
                    event_type=event_type,
                    event_seq=seq,
                    event_ts=ts,
                    payload_json=payload_json,
                    handle_id=handle_id,
                    intent_id=intent_id,
                    symbol=symbol,
                )
                pg_synced = True
            except Exception as exc:  # noqa: BLE001 — fallback is intentional
                pg_error = exc
                logger.warning(
                    "paper-v2 daemon PG outbox write failed (run_id=%s seq=%s); "
                    "falling back to SQLite. err=%s",
                    self._run_id,
                    seq,
                    exc,
                )

            try:
                self._write_sqlite(
                    event_type=event_type,
                    event_seq=seq,
                    event_ts=ts,
                    payload_json=payload_json,
                    handle_id=handle_id,
                    intent_id=intent_id,
                    symbol=symbol,
                    unsynced=0 if pg_synced else 1,
                    synced_at=ts.isoformat() if pg_synced else None,
                )
            except Exception as sqlite_exc:
                # Both paths failed — propagate. Log the PG error context
                # (if any) so debugging has both signals.
                if pg_error is not None:
                    logger.error(
                        "paper-v2 daemon: BOTH PG and SQLite writes failed "
                        "(run_id=%s seq=%s). pg_error=%s sqlite_error=%s",
                        self._run_id,
                        seq,
                        pg_error,
                        sqlite_exc,
                    )
                raise

            return DaemonEventRecord(
                run_id=self._run_id,
                portfolio_id=self._portfolio_id,
                package_id=self._package_id,
                event_type=event_type,
                event_seq=seq,
                event_ts=ts,
                payload=payload,
                handle_id=handle_id,
                intent_id=intent_id,
                symbol=symbol,
            )

    def _resolve_pg_provider(self):
        """Resolve / lazily initialise the PG connection provider.

        Returns either a callable yielding a context manager, or the
        ``_PG_DISABLED_SENTINEL`` singleton (in which case PG must be skipped).
        """
        if self._pg_conn_provider is _PG_DISABLED_SENTINEL:
            return _PG_DISABLED_SENTINEL
        if self._pg_conn_provider is None:
            # Lazy: probe for canonical TDX_DB_HOST. If absent, demote to
            # SQLite-only quietly (warns once via pg_disabled_reason).
            if not os.getenv("TDX_DB_HOST"):
                self.disable_pg("TDX_DB_HOST not set; SQLite-only mode")
                return _PG_DISABLED_SENTINEL
            self._pg_conn_provider = _default_pg_conn_provider
        return self._pg_conn_provider

    def _write_pg(
        self,
        *,
        event_type: DaemonEventType,
        event_seq: int,
        event_ts: datetime,
        payload_json: str,
        handle_id: str | None,
        intent_id: str | None,
        symbol: str | None,
    ) -> None:
        provider = self._resolve_pg_provider()
        if provider is _PG_DISABLED_SENTINEL:
            raise RuntimeError(
                f"PG outbox disabled: {self._pg_disabled_reason or 'unknown'}"
            )

        from psycopg2.extras import Json  # lazy import — avoids hard dep

        outbox_payload: dict[str, Any] = {
            "portfolio_id": self._portfolio_id,
            "package_id": self._package_id,
            "event_seq": event_seq,
            "event_ts": event_ts.isoformat(),
            "handle_id": handle_id,
            "intent_id": intent_id,
            "symbol": symbol,
            "payload": json.loads(payload_json),
        }
        event_id = _build_outbox_event_id(self._run_id, event_seq)
        sql = """
            INSERT INTO qe_archive.outbox_event (
                event_id, event_type, source_system, source_id,
                source_sub_id, payload, status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
        """
        params = (
            event_id,
            event_type.canonical_name,
            PAPER_DAEMON_SOURCE_SYSTEM,
            self._run_id,
            f"{event_seq:06d}",
            Json(outbox_payload),
            "pending",
        )
        with provider() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)

    def _write_sqlite(
        self,
        *,
        event_type: DaemonEventType,
        event_seq: int,
        event_ts: datetime,
        payload_json: str,
        handle_id: str | None,
        intent_id: str | None,
        symbol: str | None,
        unsynced: int,
        synced_at: str | None,
    ) -> None:
        with closing(self._connect()) as conn:
            conn.execute(
                """
                INSERT INTO daemon_event_log (
                    run_id, portfolio_id, package_id, event_type,
                    event_seq, event_ts, handle_id, intent_id, symbol,
                    payload_json, unsynced, synced_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    self._run_id,
                    self._portfolio_id,
                    self._package_id,
                    event_type.value,
                    event_seq,
                    event_ts.isoformat(),
                    handle_id,
                    intent_id,
                    symbol,
                    payload_json,
                    unsynced,
                    synced_at,
                ),
            )

    # ------------------------------------------------------------------
    # Replay (boot-time recovery)
    # ------------------------------------------------------------------

    def replay_unsynced_on_startup(self) -> dict[str, int]:
        """Push any unsynced SQLite rows to the PG outbox.

        Idempotent — on PG side, ``ON CONFLICT (event_id) DO NOTHING`` makes
        replays safe even if a previous attempt partially succeeded. On SQLite
        side, only rows still flagged ``unsynced=1`` are picked up.

        Per-row failures are logged + skipped (the row stays unsynced and is
        retried on the next call). Method-level failures (e.g. PG totally
        down) propagate to the caller.

        Returns a counters dict: {"pushed": N, "skipped": M, "scanned": K}.
        """
        scanned = 0
        pushed = 0
        skipped = 0

        # Read pending rows out first; close the SQLite read connection before
        # any per-row PG write to avoid holding two connections per attempt.
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT id, event_type, event_seq, event_ts, handle_id,
                       intent_id, symbol, payload_json
                FROM daemon_event_log
                WHERE run_id = ? AND unsynced = 1
                ORDER BY event_seq
                """,
                (self._run_id,),
            ).fetchall()

        for row in rows:
            scanned += 1
            (
                row_id,
                event_type_str,
                event_seq,
                event_ts_str,
                handle_id,
                intent_id,
                symbol,
                payload_json,
            ) = row
            try:
                event_type = DaemonEventType(event_type_str)
                event_ts = datetime.fromisoformat(event_ts_str)
                self._write_pg(
                    event_type=event_type,
                    event_seq=event_seq,
                    event_ts=event_ts,
                    payload_json=payload_json,
                    handle_id=handle_id,
                    intent_id=intent_id,
                    symbol=symbol,
                )
            except Exception as exc:  # noqa: BLE001 — per-row tolerant
                skipped += 1
                logger.warning(
                    "paper-v2 daemon replay: row id=%s seq=%s failed; "
                    "leaving unsynced for next attempt. err=%s",
                    row_id,
                    event_seq,
                    exc,
                )
                continue

            # Mark synced. Each mark-synced is its own SQLite tx — keeps
            # retry idempotent on partial replay.
            try:
                with closing(self._connect()) as conn:
                    conn.execute(
                        "UPDATE daemon_event_log SET unsynced = 0, "
                        "synced_at = ? WHERE id = ?",
                        (datetime.now(UTC).isoformat(), row_id),
                    )
                pushed += 1
            except Exception as exc:  # noqa: BLE001
                # PG insert succeeded but the local mark failed — log and
                # continue. Next replay will re-INSERT (ON CONFLICT NO-OP)
                # then try the mark again.
                skipped += 1
                logger.warning(
                    "paper-v2 daemon replay: PG insert OK but SQLite mark-"
                    "synced failed for id=%s seq=%s. err=%s",
                    row_id,
                    event_seq,
                    exc,
                )

        return {"pushed": pushed, "skipped": skipped, "scanned": scanned}

    # ------------------------------------------------------------------
    # Read helpers (unchanged)
    # ------------------------------------------------------------------

    def read_all(self) -> list[DaemonEventRecord]:
        """Return all events recorded under this run_id, ordered by event_seq."""
        with closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT run_id, portfolio_id, package_id, event_type, event_seq,
                       event_ts, handle_id, intent_id, symbol, payload_json
                FROM daemon_event_log
                WHERE run_id = ?
                ORDER BY event_seq
                """,
                (self._run_id,),
            ).fetchall()
        return [
            DaemonEventRecord(
                run_id=row[0],
                portfolio_id=row[1],
                package_id=row[2],
                event_type=DaemonEventType(row[3]),
                event_seq=row[4],
                event_ts=datetime.fromisoformat(row[5]),
                handle_id=row[6],
                intent_id=row[7],
                symbol=row[8],
                payload=json.loads(row[9]),
            )
            for row in rows
        ]

    def count(self, event_type: DaemonEventType | None = None) -> int:
        with closing(self._connect()) as conn:
            if event_type is None:
                row = conn.execute(
                    "SELECT COUNT(*) FROM daemon_event_log WHERE run_id = ?",
                    (self._run_id,),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM daemon_event_log WHERE run_id = ? AND event_type = ?",
                    (self._run_id, event_type.value),
                ).fetchone()
        return int(row[0])

    def count_unsynced(self) -> int:
        """Count SQLite rows still flagged unsynced (debug / testing)."""
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM daemon_event_log "
                "WHERE run_id = ? AND unsynced = 1",
                (self._run_id,),
            ).fetchone()
        return int(row[0])


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__str__"):
        return str(obj)
    raise TypeError(f"unserialisable type: {type(obj)!r}")
