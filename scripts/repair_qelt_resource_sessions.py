from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import psycopg2

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.db.pg_pool import get_conn  # noqa: E402


RECONCILE_REASON = "QE_RESOURCE_RECONCILED_FROM_LOOP_TERMINAL"
QELT_SOURCE_PREFIX = "qelt:"
TERMINAL_SESSION_STATES = ("completed", "failed", "cancelled")
REPAIR_SCHEMA_VERSION = "qelt_resource_session_repair_v1"
OUTBOX_PATTERN = "*/Loop*/long_trend_evaluations/qelt_*/outbox/*.json"

CANDIDATE_SQL = """
SELECT
    s.session_id,
    s.source_run_key,
    s.attempt_no,
    s.task_id,
    s.loop_id,
    s.loop_index,
    s.node_id,
    s.status AS before_status,
    s.current_phase AS before_current_phase,
    s.last_sequence_no,
    s.terminal_reason_code AS before_terminal_reason_code,
    s.completed_at AS before_completed_at,
    e.evaluation_id,
    e.resource_session_id AS control_resource_session_id,
    e.parent_task_id,
    e.parent_loop_index,
    e.node_id AS control_node_id,
    e.request_sha,
    e.request_json,
    p.phase AS sequence_one_phase,
    p.event_sha256 AS sequence_one_event_sha256
FROM qe_archive.run_resource_session s
JOIN qe_archive.run_evaluation e
  ON e.resource_session_id = s.session_id
LEFT JOIN qe_archive.run_resource_phase p
  ON p.session_id = s.session_id
 AND p.sequence_no = 1
WHERE s.source_run_key LIKE 'qelt:%'
  AND s.source_run_key = 'qelt:' || e.evaluation_id
  AND s.attempt_no = 1
  AND s.task_id = e.parent_task_id
  AND s.loop_index = e.parent_loop_index
  AND s.node_id = e.node_id
  AND s.terminal_reason_code = %s
  AND s.status IN ('completed', 'failed', 'cancelled')
  AND s.current_phase = s.status
  AND s.completed_at IS NOT NULL
  AND s.last_sequence_no IN (0, 1)
  AND (
      SELECT COUNT(*)
      FROM qe_archive.run_resource_phase phase_count
      WHERE phase_count.session_id = s.session_id
  ) = s.last_sequence_no
ORDER BY e.evaluation_id
"""

PROTECTED_QELT_SQL = """
SELECT COALESCE(terminal_reason_code, '<null>') AS terminal_reason_code, status, COUNT(*) AS row_count
FROM qe_archive.run_resource_session
WHERE source_run_key LIKE 'qelt:%'
  AND terminal_reason_code IS DISTINCT FROM %s
GROUP BY COALESCE(terminal_reason_code, '<null>'), status
ORDER BY terminal_reason_code, status
"""

APPLY_SQL = """
UPDATE qe_archive.run_resource_session
SET status = 'running',
    current_phase = CASE WHEN last_sequence_no = 0 THEN 'created' ELSE 'long_trend_eval' END,
    terminal_reason_code = NULL,
    completed_at = NULL,
    updated_at = clock_timestamp()
WHERE session_id = %s
  AND source_run_key = %s
  AND attempt_no = 1
  AND status = %s
  AND current_phase = %s
  AND last_sequence_no = %s
  AND terminal_reason_code = %s
  AND completed_at IS NOT DISTINCT FROM %s
RETURNING session_id, source_run_key, status, current_phase, last_sequence_no,
          terminal_reason_code, completed_at, updated_at
"""

READBACK_SQL = """
SELECT session_id, source_run_key, status, current_phase, last_sequence_no,
       terminal_reason_code, completed_at, updated_at
FROM qe_archive.run_resource_session
WHERE session_id = ANY(%s)
ORDER BY session_id
"""

ROLLBACK_SQL = """
UPDATE qe_archive.run_resource_session
SET status = %s,
    current_phase = %s,
    terminal_reason_code = %s,
    completed_at = %s,
    updated_at = clock_timestamp()
WHERE session_id = %s
  AND source_run_key = %s
  AND status = 'running'
  AND current_phase = %s
  AND last_sequence_no = %s
  AND terminal_reason_code IS NULL
  AND completed_at IS NULL
RETURNING session_id, source_run_key, status, current_phase, last_sequence_no,
          terminal_reason_code, completed_at, updated_at
"""


class QeltRepairError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class OutboxEvent:
    evaluation_id: str
    sequence_no: int
    session_id: str
    source_run_key: str
    task_id: str
    loop_id: str
    loop_index: int
    node_id: str
    phase: str
    delivered: bool
    event_sha256: str
    relative_path: str
    payload: Mapping[str, Any]

    def identity(self) -> dict[str, Any]:
        return {
            "evaluation_id": self.evaluation_id,
            "sequence_no": self.sequence_no,
            "session_id": self.session_id,
            "source_run_key": self.source_run_key,
            "task_id": self.task_id,
            "loop_id": self.loop_id,
            "loop_index": self.loop_index,
            "node_id": self.node_id,
            "phase": self.phase,
            "delivered": self.delivered,
            "event_sha256": self.event_sha256,
            "relative_path": self.relative_path,
        }


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _timestamp_parameter(value: Any) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise QeltRepairError(
            "QELT_REPAIR_RECEIPT_INVALID",
            f"receipt timestamp is invalid: {value!r}",
        ) from exc
    if parsed.tzinfo is None:
        raise QeltRepairError("QELT_REPAIR_RECEIPT_INVALID", "receipt timestamp must include a timezone")
    return parsed


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise QeltRepairError(
            "QELT_REPAIR_OUTBOX_INVALID",
            f"cannot read durable outbox {path}: {type(exc).__name__}: {exc}",
        ) from exc
    if not isinstance(value, dict):
        raise QeltRepairError("QELT_REPAIR_OUTBOX_INVALID", f"durable outbox is not a JSON object: {path}")
    return value


def scan_outboxes(root: Path) -> dict[tuple[str, int], OutboxEvent]:
    root = root.resolve()
    if not root.is_dir():
        raise QeltRepairError("QELT_REPAIR_OUTBOX_ROOT_INVALID", f"outbox root is not a directory: {root}")
    events: dict[tuple[str, int], OutboxEvent] = {}
    for path in sorted(root.glob(OUTBOX_PATTERN)):
        row = _read_json_object(path)
        payload = row.get("payload")
        if not isinstance(payload, Mapping):
            raise QeltRepairError("QELT_REPAIR_OUTBOX_INVALID", f"outbox payload is missing: {path}")
        metadata = payload.get("metadata")
        if not isinstance(metadata, Mapping):
            raise QeltRepairError("QELT_REPAIR_OUTBOX_INVALID", f"outbox metadata is missing: {path}")
        try:
            evaluation_id = str(metadata["evaluation_id"])
            sequence_no = int(payload["sequence_no"])
            loop_index = int(payload["loop_index"])
            event = OutboxEvent(
                evaluation_id=evaluation_id,
                sequence_no=sequence_no,
                session_id=str(payload["session_id"]),
                source_run_key=str(payload["source_run_key"]),
                task_id=str(payload["task_id"]),
                loop_id=str(payload["loop_id"]),
                loop_index=loop_index,
                node_id=str(payload["node_id"]),
                phase=str(payload["phase"]),
                delivered=row.get("delivered") is True,
                event_sha256=_canonical_sha256(payload),
                relative_path=path.relative_to(root).as_posix(),
                payload=dict(payload),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_INVALID",
                f"outbox identity is incomplete: {path}: {type(exc).__name__}: {exc}",
            ) from exc
        expected_source = f"{QELT_SOURCE_PREFIX}{evaluation_id}"
        if (
            not evaluation_id.startswith("qelt_")
            or event.source_run_key != expected_source
            or path.parents[1].name != evaluation_id
            or event.loop_id != f"Loop{event.loop_index}"
            or path.parents[3].name != event.loop_id
        ):
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_IDENTITY_CONFLICT",
                f"outbox path and payload identity differ: {path}",
                context=event.identity(),
            )
        key = (evaluation_id, sequence_no)
        prior = events.get(key)
        if prior is not None:
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_SEQUENCE_CONFLICT",
                f"multiple durable outboxes exist for evaluation/sequence {key}",
                context={"prior": prior.identity(), "current": event.identity()},
            )
        events[key] = event
    return events


def summarize_outboxes(outboxes: Mapping[tuple[str, int], OutboxEvent]) -> dict[str, Any]:
    phase_counts: dict[str, int] = {}
    delivered_count = 0
    for event in outboxes.values():
        phase_counts[event.phase] = phase_counts.get(event.phase, 0) + 1
        delivered_count += int(event.delivered)
    return {
        "total_event_count": len(outboxes),
        "delivered_count": delivered_count,
        "pending_count": len(outboxes) - delivered_count,
        "phase_counts": dict(sorted(phase_counts.items())),
    }


def _mapping(value: Any, *, field_name: str) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise QeltRepairError("QELT_REPAIR_CONTROL_IDENTITY_CONFLICT", f"{field_name} is invalid JSON") from exc
        if isinstance(parsed, Mapping):
            return parsed
    raise QeltRepairError("QELT_REPAIR_CONTROL_IDENTITY_CONFLICT", f"{field_name} is not an object")


def _require_outbox_identity(event: OutboxEvent, row: Mapping[str, Any]) -> None:
    expected = {
        "evaluation_id": str(row["evaluation_id"]),
        "session_id": str(row["session_id"]),
        "source_run_key": str(row["source_run_key"]),
        "task_id": str(row["task_id"]),
        "loop_id": str(row["loop_id"]),
        "loop_index": int(row["loop_index"]),
        "node_id": str(row["node_id"]),
    }
    actual = {key: event.identity()[key] for key in expected}
    if actual != expected:
        raise QeltRepairError(
            "QELT_REPAIR_OUTBOX_IDENTITY_CONFLICT",
            f"outbox identity differs from DB control identity for {row['evaluation_id']}",
            context={"expected": expected, "actual": actual},
        )


def build_repair_plan(
    rows: Sequence[Mapping[str, Any]],
    outboxes: Mapping[tuple[str, int], OutboxEvent],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        evaluation_id = str(row["evaluation_id"])
        source_run_key = str(row["source_run_key"])
        if (
            source_run_key != f"{QELT_SOURCE_PREFIX}{evaluation_id}"
            or str(row["control_resource_session_id"]) != str(row["session_id"])
            or str(row["parent_task_id"]) != str(row["task_id"])
            or int(row["parent_loop_index"]) != int(row["loop_index"])
            or str(row["control_node_id"]) != str(row["node_id"])
        ):
            raise QeltRepairError(
                "QELT_REPAIR_CONTROL_IDENTITY_CONFLICT",
                f"DB session/control identity differs for {evaluation_id}",
            )
        request = _mapping(row["request_json"], field_name="request_json")
        request_resource = _mapping(request.get("resource_session"), field_name="request_json.resource_session")
        if request_resource != {"session_id": str(row["session_id"]), "source_run_key": source_run_key}:
            raise QeltRepairError(
                "QELT_REPAIR_CONTROL_IDENTITY_CONFLICT",
                f"secret-free request resource identity differs for {evaluation_id}",
            )

        last_sequence_no = int(row["last_sequence_no"])
        sequence_one = outboxes.get((evaluation_id, 1))
        if sequence_one is None:
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_MISSING",
                f"sequence 1 durable outbox is missing for {evaluation_id}",
            )
        _require_outbox_identity(sequence_one, row)
        if sequence_one.phase != "long_trend_eval":
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_PHASE_INVALID",
                f"sequence 1 is not long_trend_eval for {evaluation_id}",
            )
        terminal_event = outboxes.get((evaluation_id, 2))
        if terminal_event is None:
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_MISSING",
                f"sequence 2 durable terminal outbox is missing for {evaluation_id}",
            )
        _require_outbox_identity(terminal_event, row)
        if terminal_event.delivered or terminal_event.phase not in {"completed", "failed"}:
            raise QeltRepairError(
                "QELT_REPAIR_OUTBOX_PHASE_INVALID",
                f"sequence 2 must be a pending completed/failed event for {evaluation_id}",
            )

        if last_sequence_no == 0:
            if sequence_one.delivered or row.get("sequence_one_phase") is not None:
                raise QeltRepairError(
                    "QELT_REPAIR_SEQUENCE_EVIDENCE_CONFLICT",
                    f"sequence 1 evidence conflicts with last_sequence_no=0 for {evaluation_id}",
                )
            next_event = sequence_one
            repaired_phase = "created"
        elif last_sequence_no == 1:
            if (
                not sequence_one.delivered
                or str(row.get("sequence_one_phase") or "") != "long_trend_eval"
                or str(row.get("sequence_one_event_sha256") or "") != sequence_one.event_sha256
            ):
                raise QeltRepairError(
                    "QELT_REPAIR_SEQUENCE_EVIDENCE_CONFLICT",
                    f"accepted sequence 1 does not match durable outbox for {evaluation_id}",
                )
            next_event = terminal_event
            repaired_phase = "long_trend_eval"
        else:
            raise QeltRepairError(
                "QELT_REPAIR_SEQUENCE_UNSUPPORTED",
                f"last_sequence_no is not 0 or 1 for {evaluation_id}",
            )

        candidate = {
            "session_id": str(row["session_id"]),
            "evaluation_id": evaluation_id,
            "source_run_key": source_run_key,
            "task_id": str(row["task_id"]),
            "loop_id": str(row["loop_id"]),
            "loop_index": int(row["loop_index"]),
            "node_id": str(row["node_id"]),
            "last_sequence_no": last_sequence_no,
            "before_status": str(row["before_status"]),
            "before_current_phase": str(row["before_current_phase"]),
            "before_terminal_reason_code": str(row["before_terminal_reason_code"]),
            "before_completed_at": _json_value(row.get("before_completed_at")),
            "after_status": "running",
            "after_current_phase": repaired_phase,
            "next_sequence_no": next_event.sequence_no,
            "next_phase": next_event.phase,
            "next_event_sha256": next_event.event_sha256,
            "next_outbox_path": next_event.relative_path,
            "sequence_one_event_sha256": sequence_one.event_sha256,
            "terminal_event_sha256": terminal_event.event_sha256,
            "terminal_outbox_path": terminal_event.relative_path,
            "request_sha": str(row["request_sha"]),
        }
        candidates.append(candidate)

    candidates.sort(key=lambda item: (item["evaluation_id"], item["session_id"]))
    digest_payload = [
        {
            key: candidate[key]
            for key in (
                "session_id",
                "evaluation_id",
                "source_run_key",
                "task_id",
                "loop_index",
                "node_id",
                "last_sequence_no",
                "before_status",
                "before_current_phase",
                "before_terminal_reason_code",
                "before_completed_at",
                "after_status",
                "after_current_phase",
                "next_sequence_no",
                "next_phase",
                "next_event_sha256",
                "sequence_one_event_sha256",
                "terminal_event_sha256",
                "request_sha",
            )
        }
        for candidate in candidates
    ]
    return {
        "schema_version": REPAIR_SCHEMA_VERSION,
        "candidate_count": len(candidates),
        "candidate_digest": _canonical_sha256(digest_payload),
        "candidates": candidates,
    }


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(_json_value(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


class QeltResourceSessionRepair:
    def __init__(self, connection_provider: Callable[..., Any] | None = None) -> None:
        self._connection_provider = connection_provider or get_conn

    def _connection(self, *, transactional: bool) -> Any:
        return self._connection_provider(autocommit=not transactional, manage_transaction=transactional)

    @staticmethod
    def _fetch_rows(conn: Any, *, for_update: bool = False) -> list[dict[str, Any]]:
        sql = CANDIDATE_SQL + (" FOR UPDATE OF s" if for_update else "")
        with conn.cursor() as cur:
            cur.execute(sql, (RECONCILE_REASON,))
            columns = [item[0] for item in cur.description]
            return [dict(zip(columns, row)) if not isinstance(row, Mapping) else dict(row) for row in cur.fetchall()]

    @staticmethod
    def _protected_rows(conn: Any) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(PROTECTED_QELT_SQL, (RECONCILE_REASON,))
            columns = [item[0] for item in cur.description]
            return [dict(zip(columns, row)) if not isinstance(row, Mapping) else dict(row) for row in cur.fetchall()]

    def preflight(self, outbox_root: Path) -> dict[str, Any]:
        outboxes = scan_outboxes(outbox_root)
        outbox_summary = summarize_outboxes(outboxes)
        try:
            with self._connection(transactional=False) as conn:
                plan = build_repair_plan(self._fetch_rows(conn), outboxes)
                protected = self._protected_rows(conn)
        except psycopg2.Error as exc:
            raise QeltRepairError(
                "QELT_REPAIR_DATABASE_UNAVAILABLE",
                f"database preflight failed: {type(exc).__name__}: {exc}",
                context={
                    "outbox_root": str(outbox_root.resolve()),
                    "outbox_summary": outbox_summary,
                },
            ) from exc
        return {
            **plan,
            "mode": "preflight",
            "outbox_root": str(outbox_root.resolve()),
            "outbox_summary": outbox_summary,
            "protected_qelt_rows": _json_value(protected),
            "apply_sql_sha256": hashlib.sha256(APPLY_SQL.encode("utf-8")).hexdigest(),
        }

    def apply(
        self,
        outbox_root: Path,
        *,
        expected_candidate_digest: str,
        receipt_path: Path,
    ) -> dict[str, Any]:
        outbox_root = outbox_root.resolve()
        receipt_path = receipt_path.resolve()
        if receipt_path.is_relative_to(outbox_root):
            raise QeltRepairError(
                "QELT_REPAIR_RECEIPT_PATH_INVALID",
                "repair receipt must not be written inside the RD outbox/workspace root",
            )
        outboxes = scan_outboxes(outbox_root)
        with self._connection(transactional=True) as conn:
            plan = build_repair_plan(self._fetch_rows(conn, for_update=True), outboxes)
            if plan["candidate_digest"] != expected_candidate_digest:
                raise QeltRepairError(
                    "QELT_REPAIR_CANDIDATE_DIGEST_CHANGED",
                    "candidate digest changed between preflight and apply",
                    context={"expected": expected_candidate_digest, "actual": plan["candidate_digest"]},
                )
            if not plan["candidates"]:
                raise QeltRepairError("QELT_REPAIR_NO_CANDIDATES", "apply requires at least one exact candidate")
            applied: list[dict[str, Any]] = []
            with conn.cursor() as cur:
                for candidate in plan["candidates"]:
                    cur.execute(
                        APPLY_SQL,
                        (
                            candidate["session_id"],
                            candidate["source_run_key"],
                            candidate["before_status"],
                            candidate["before_current_phase"],
                            candidate["last_sequence_no"],
                            candidate["before_terminal_reason_code"],
                            _timestamp_parameter(candidate["before_completed_at"]),
                        ),
                    )
                    row = cur.fetchone()
                    if cur.rowcount != 1 or row is None:
                        raise QeltRepairError(
                            "QELT_REPAIR_APPLY_GUARD_FAILED",
                            f"guarded update refused session {candidate['session_id']}",
                        )
                    columns = [item[0] for item in cur.description]
                    applied.append(dict(zip(columns, row)) if not isinstance(row, Mapping) else dict(row))
            receipt = {
                **plan,
                "mode": "apply",
                "transaction_status": "prepared",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "outbox_root": str(outbox_root),
                "receipt_path": str(receipt_path),
                "applied": _json_value(applied),
            }
            _atomic_json(receipt_path, receipt)
            conn.commit()
        receipt["transaction_status"] = "applied"
        _atomic_json(receipt_path, receipt)
        return receipt

    @staticmethod
    def _load_receipt(receipt_path: Path) -> dict[str, Any]:
        receipt = _read_json_object(receipt_path.resolve())
        if receipt.get("schema_version") != REPAIR_SCHEMA_VERSION or not isinstance(receipt.get("candidates"), list):
            raise QeltRepairError("QELT_REPAIR_RECEIPT_INVALID", "repair receipt schema is invalid")
        return receipt

    @staticmethod
    def _readback_rows(conn: Any, session_ids: Sequence[str]) -> list[dict[str, Any]]:
        with conn.cursor() as cur:
            cur.execute(READBACK_SQL, (list(session_ids),))
            columns = [item[0] for item in cur.description]
            return [dict(zip(columns, row)) if not isinstance(row, Mapping) else dict(row) for row in cur.fetchall()]

    def readback(self, receipt_path: Path) -> dict[str, Any]:
        receipt = self._load_receipt(receipt_path)
        candidates = receipt["candidates"]
        session_ids = [str(item["session_id"]) for item in candidates]
        with self._connection(transactional=False) as conn:
            rows = self._readback_rows(conn, session_ids)
        by_id = {str(row["session_id"]): row for row in rows}
        mismatches: list[dict[str, Any]] = []
        for candidate in candidates:
            row = by_id.get(str(candidate["session_id"]))
            expected = {
                "source_run_key": candidate["source_run_key"],
                "status": candidate["after_status"],
                "current_phase": candidate["after_current_phase"],
                "last_sequence_no": candidate["last_sequence_no"],
                "terminal_reason_code": None,
                "completed_at": None,
            }
            actual = {key: _json_value(row.get(key)) if row else None for key in expected}
            if actual != expected:
                mismatches.append({"session_id": candidate["session_id"], "expected": expected, "actual": actual})
        if mismatches:
            raise QeltRepairError(
                "QELT_REPAIR_READBACK_MISMATCH",
                "repair readback differs from expected resumable session state",
                context={"mismatches": mismatches},
            )
        return {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "mode": "readback",
            "candidate_digest": receipt["candidate_digest"],
            "candidate_count": len(candidates),
            "rows": _json_value(rows),
        }

    def verify_idempotency(self, outbox_root: Path, receipt_path: Path) -> dict[str, Any]:
        readback = self.readback(receipt_path)
        preflight = self.preflight(outbox_root)
        repaired_ids = {str(item["session_id"]) for item in self._load_receipt(receipt_path)["candidates"]}
        repeated_ids = {str(item["session_id"]) for item in preflight["candidates"]} & repaired_ids
        if repeated_ids:
            raise QeltRepairError(
                "QELT_REPAIR_NOT_IDEMPOTENT",
                "repaired sessions remain eligible for a repeated apply",
                context={"session_ids": sorted(repeated_ids)},
            )
        return {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "mode": "verify-idempotency",
            "candidate_digest": readback["candidate_digest"],
            "repeated_candidate_count": 0,
            "readback": readback,
        }

    def rollback(self, receipt_path: Path) -> dict[str, Any]:
        receipt = self._load_receipt(receipt_path)
        candidates = receipt["candidates"]
        restored: list[dict[str, Any]] = []
        with self._connection(transactional=True) as conn:
            with conn.cursor() as cur:
                for candidate in candidates:
                    cur.execute(
                        ROLLBACK_SQL,
                        (
                            candidate["before_status"],
                            candidate["before_current_phase"],
                            candidate["before_terminal_reason_code"],
                            _timestamp_parameter(candidate["before_completed_at"]),
                            candidate["session_id"],
                            candidate["source_run_key"],
                            candidate["after_current_phase"],
                            candidate["last_sequence_no"],
                        ),
                    )
                    row = cur.fetchone()
                    if cur.rowcount != 1 or row is None:
                        raise QeltRepairError(
                            "QELT_REPAIR_ROLLBACK_GUARD_FAILED",
                            "guarded rollback refused because the session advanced or changed",
                            context={"session_id": candidate["session_id"]},
                        )
                    columns = [item[0] for item in cur.description]
                    restored.append(dict(zip(columns, row)) if not isinstance(row, Mapping) else dict(row))
            conn.commit()
        return {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "mode": "rollback",
            "candidate_digest": receipt["candidate_digest"],
            "restored_count": len(restored),
            "rows": _json_value(restored),
        }


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Preflight and guardedly repair only qelt resource sessions terminalized by the generic Loop reconciler. "
            "Use the configured DEV database first; production DML requires separate authorization."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("preflight", "apply", "readback", "verify-idempotency", "rollback"),
        required=True,
    )
    parser.add_argument("--outbox-root", type=Path, help="Read-only RD-Agent QE workspace root.")
    parser.add_argument("--expected-candidate-digest")
    parser.add_argument("--receipt-path", type=Path)
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args(argv)
    if args.mode in {"preflight", "apply", "verify-idempotency"} and args.outbox_root is None:
        parser.error(f"--outbox-root is required for mode={args.mode}")
    if args.mode in {"apply", "readback", "verify-idempotency", "rollback"} and args.receipt_path is None:
        parser.error(f"--receipt-path is required for mode={args.mode}")
    if args.mode == "apply" and not args.expected_candidate_digest:
        parser.error("--expected-candidate-digest is required for mode=apply")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    service = QeltResourceSessionRepair()
    try:
        if args.mode == "preflight":
            result = service.preflight(args.outbox_root)
        elif args.mode == "apply":
            result = service.apply(
                args.outbox_root,
                expected_candidate_digest=args.expected_candidate_digest,
                receipt_path=args.receipt_path,
            )
        elif args.mode == "readback":
            result = service.readback(args.receipt_path)
        elif args.mode == "verify-idempotency":
            result = service.verify_idempotency(args.outbox_root, args.receipt_path)
        else:
            result = service.rollback(args.receipt_path)
    except QeltRepairError as exc:
        payload = {
            "success": False,
            "mode": args.mode,
            "reason_code": exc.reason_code,
            "message": str(exc),
            "context": _json_value(exc.context),
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2 if args.pretty else None))
        return 1
    except psycopg2.Error as exc:
        payload = {
            "success": False,
            "mode": args.mode,
            "reason_code": "QELT_REPAIR_DATABASE_FAILED",
            "message": f"database operation failed: {type(exc).__name__}: {exc}",
            "context": {},
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2 if args.pretty else None))
        return 1
    print(
        json.dumps(
            {"success": True, **_json_value(result)},
            ensure_ascii=False,
            sort_keys=True,
            indent=2 if args.pretty else None,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
