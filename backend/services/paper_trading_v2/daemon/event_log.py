"""Daemon event log — SQLite-backed append-only event store.

Phase 2 T5: Lead's task #35 specifies "daemon_event_log table writes". To stay
inside the worktree's authorized scope (DB migration on the production Postgres
``paper_v2`` requires user approval per agent_teams_session_handoff_20260509
§6 P0), this module persists into a worktree-local SQLite file
(default ``var/paper_v2_sim/daemon_events.db``, gitignored).

The schema mirrors what a production Postgres ``daemon_event_log`` table would
look like, so the same writer can later be retargeted at PG by swapping
the connection string -- callers stay unchanged.

Schema:

    CREATE TABLE daemon_event_log (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id       TEXT NOT NULL,
      portfolio_id TEXT NOT NULL,
      package_id   TEXT NOT NULL,
      event_type   TEXT NOT NULL,
      event_seq    INTEGER NOT NULL,
      event_ts     TEXT NOT NULL,    -- ISO8601 with timezone
      handle_id    TEXT,             -- nullable for non-order events
      intent_id    TEXT,
      symbol       TEXT,
      payload_json TEXT NOT NULL,    -- structured event-specific fields
      UNIQUE(run_id, event_seq)
    );

Append semantics: each writer instance owns its own ``run_id`` and monotonic
``event_seq``; cross-process concurrency on the same DB file is supported by
SQLite's WAL mode but Phase 2 T5 only exercises single-process writers.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import closing
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4


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
    UNIQUE(run_id, event_seq)
);
CREATE INDEX IF NOT EXISTS idx_daemon_event_log_run ON daemon_event_log(run_id, event_seq);
CREATE INDEX IF NOT EXISTS idx_daemon_event_log_portfolio ON daemon_event_log(portfolio_id);
"""


class DaemonEventLog:
    """Append-only writer for paper-v2 sim daemon events.

    One writer instance pins one ``run_id``. Caller is responsible for
    creating a new instance per simulated run.

    Thread-safe: ``record()`` and ``read_all()`` acquire an internal lock, so
    multi-callback fan-out (LocalSim subscribe_fill_callback) can safely write
    from background threads.
    """

    def __init__(
        self,
        *,
        db_path: Path | str,
        run_id: str | None = None,
        portfolio_id: str,
        package_id: str,
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

    def _connect(self) -> sqlite3.Connection:
        # WAL mode lets reads and the single writer coexist without contention,
        # which matters when an integration test reads while LocalSim's fill
        # callback is still firing.
        conn = sqlite3.connect(self._db_path, isolation_level=None)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(_DDL)

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
        """Append a single event. Returns the persisted record."""
        if not isinstance(event_type, DaemonEventType):
            raise TypeError(f"event_type must be DaemonEventType, got {type(event_type)!r}")
        ts = event_ts or datetime.now(UTC)
        # serialise payload defensively so callers can pass datetimes / Decimals
        payload_json = json.dumps(payload, default=_json_default, ensure_ascii=False)
        with self._lock:
            self._seq += 1
            seq = self._seq
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO daemon_event_log (
                        run_id, portfolio_id, package_id, event_type,
                        event_seq, event_ts, handle_id, intent_id, symbol,
                        payload_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        self._run_id,
                        self._portfolio_id,
                        self._package_id,
                        event_type.value,
                        seq,
                        ts.isoformat(),
                        handle_id,
                        intent_id,
                        symbol,
                        payload_json,
                    ),
                )
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


def _json_default(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if hasattr(obj, "__str__"):
        return str(obj)
    raise TypeError(f"unserialisable type: {type(obj)!r}")
