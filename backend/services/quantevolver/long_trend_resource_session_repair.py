"""Guarded BUG-847 repair for qelt resource sessions terminalized by the generic reconciler.

Preflight and readback are read-only. Apply and rollback only update the resource-session
state columns named in this module; they never mutate phase rows, control rows, Archive
runs, CAS, metrics, parent loops, or experiment results.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn

RECONCILER_REASON = "QE_RESOURCE_RECONCILED_FROM_LOOP_TERMINAL"
PLAN_SCHEMA = "qelt_resource_session_bug847_repair_plan_v1"
RECEIPT_SCHEMA = "qelt_resource_session_bug847_repair_receipt_v1"
TERMINAL_STATES = frozenset({"completed", "failed", "cancelled"})
LEGACY_API_OPTIONAL_FIELDS = (
    "started_at",
    "ended_at",
    "duration_seconds",
    "sample_count",
    "process_rss_peak_bytes",
    "process_vm_hwm_peak_bytes",
    "gpu_device_index",
    "gpu_name",
    "gpu_memory_used_peak_bytes",
    "gpu_process_memory_peak_bytes",
    "gpu_utilization_avg_pct",
    "gpu_utilization_peak_pct",
    "cuda_allocated_peak_bytes",
    "cuda_reserved_peak_bytes",
    "cuda_allocated_end_bytes",
    "cuda_reserved_end_bytes",
    "resident_requested",
    "resident_active",
    "resident_fallback",
    "fallback_reason_code",
    "release_check_passed",
    "reason_code",
)

PREFLIGHT_SQL = """
SELECT s.session_id, s.source_run_key, s.task_id, s.loop_id, s.loop_index, s.node_id,
       s.status, s.current_phase, s.last_sequence_no, s.terminal_reason_code,
       s.completed_at, e.evaluation_id, e.request_sha, e.resource_session_id,
       e.parent_task_id, e.parent_loop_index, e.node_id AS control_node_id,
       (SELECT COUNT(*) FROM qe_archive.run_resource_phase p
        WHERE p.session_id = s.session_id) AS phase_row_count,
       (SELECT MAX(p.sequence_no) FROM qe_archive.run_resource_phase p
        WHERE p.session_id = s.session_id) AS max_phase_sequence,
       (SELECT COALESCE(jsonb_object_agg(p.sequence_no::text, p.event_sha256), '{}'::jsonb)
        FROM qe_archive.run_resource_phase p
        WHERE p.session_id = s.session_id) AS phase_event_hashes
FROM qe_archive.run_resource_session s
JOIN qe_archive.run_evaluation e
  ON e.resource_session_id = s.session_id
 AND ('qelt:' || e.evaluation_id) = s.source_run_key
WHERE s.source_run_key LIKE 'qelt:%%'
  AND s.terminal_reason_code = %s
  AND s.status IN ('completed', 'failed', 'cancelled')
  AND s.current_phase IN ('completed', 'failed', 'cancelled')
  AND s.last_sequence_no IN (0, 1)
ORDER BY s.source_run_key
"""

APPLY_SQL = """
UPDATE qe_archive.run_resource_session s
SET status = %s,
    current_phase = %s,
    terminal_reason_code = NULL,
    completed_at = NULL,
    updated_at = NOW()
WHERE s.session_id = %s
  AND s.source_run_key = %s
  AND s.status = %s
  AND s.current_phase = %s
  AND s.last_sequence_no = %s
  AND s.terminal_reason_code = %s
  AND s.completed_at IS NOT DISTINCT FROM %s
  AND EXISTS (
      SELECT 1 FROM qe_archive.run_evaluation e
      WHERE e.resource_session_id = s.session_id
        AND e.evaluation_id = %s
        AND e.parent_task_id = s.task_id
        AND e.parent_loop_index = s.loop_index
        AND e.node_id = s.node_id
        AND e.request_sha = %s
        AND ('qelt:' || e.evaluation_id) = s.source_run_key
  )
  AND NOT EXISTS (
      SELECT 1 FROM qe_archive.run_resource_phase p
      WHERE p.session_id = s.session_id AND p.sequence_no > %s
  )
"""

READBACK_SQL = """
SELECT session_id, source_run_key, status, current_phase, last_sequence_no,
       terminal_reason_code, completed_at
FROM qe_archive.run_resource_session
WHERE session_id = ANY(%s)
ORDER BY source_run_key
"""

CURRENT_STATE_SQL = """
SELECT s.status, s.current_phase, s.last_sequence_no, s.terminal_reason_code, s.completed_at,
       EXISTS (
           SELECT 1 FROM qe_archive.run_resource_phase p
           WHERE p.session_id = s.session_id AND p.sequence_no > %s
       ) AS has_later_phase,
       EXISTS (
           SELECT 1 FROM qe_archive.run_evaluation e
           WHERE e.resource_session_id = s.session_id
             AND e.evaluation_id = %s
             AND e.request_sha = %s
             AND ('qelt:' || e.evaluation_id) = s.source_run_key
       ) AS control_matches
FROM qe_archive.run_resource_session s
WHERE s.session_id = %s AND s.source_run_key = %s
"""

ROLLBACK_SQL = """
UPDATE qe_archive.run_resource_session s
SET status = %s,
    current_phase = %s,
    terminal_reason_code = %s,
    completed_at = %s,
    updated_at = NOW()
WHERE s.session_id = %s
  AND s.source_run_key = %s
  AND s.status = %s
  AND s.current_phase = %s
  AND s.last_sequence_no = %s
  AND s.terminal_reason_code IS NULL
  AND s.completed_at IS NULL
  AND EXISTS (
      SELECT 1 FROM qe_archive.run_evaluation e
      WHERE e.resource_session_id = s.session_id
        AND e.evaluation_id = %s
        AND e.request_sha = %s
        AND ('qelt:' || e.evaluation_id) = s.source_run_key
  )
  AND NOT EXISTS (
      SELECT 1 FROM qe_archive.run_resource_phase p
      WHERE p.session_id = s.session_id AND p.sequence_no > %s
  )
"""


class QELTResourceRepairError(RuntimeError):
    pass


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False, default=str
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_scalar(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else str(value)


def _legacy_api_event_sha256(payload: Mapping[str, Any]) -> str:
    """Reproduce the pre-BUG-847 Pydantic model_dump hash for historical readback only."""

    normalized = dict(payload)
    for field in LEGACY_API_OPTIONAL_FIELDS:
        normalized.setdefault(field, None)
    normalized.setdefault("metadata", {})
    return _canonical_sha256(normalized)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise QELTResourceRepairError(f"cannot read durable JSON {path}: {type(exc).__name__}: {exc}") from exc
    if not isinstance(value, dict):
        raise QELTResourceRepairError(f"durable JSON is not an object: {path}")
    return value


def _expected_state(last_sequence_no: int) -> tuple[str, str, int]:
    if last_sequence_no == 0:
        return "reserved", "created", 1
    if last_sequence_no == 1:
        return "running", "long_trend_eval", 2
    raise QELTResourceRepairError(f"unsupported last_sequence_no={last_sequence_no}")


def _outbox_evidence(workspace: Path, row: Mapping[str, Any]) -> dict[str, Any]:
    evaluation_id = str(row["evaluation_id"])
    matches = list(workspace.glob(f"*/Loop*/long_trend_evaluations/{evaluation_id}"))
    if len(matches) != 1:
        raise QELTResourceRepairError(
            f"evaluation {evaluation_id} must map to exactly one RD workspace job; found {len(matches)}"
        )
    job_dir = matches[0]
    job = _load_json(job_dir / "job.json")
    request = _load_json(job_dir / "request.json")
    resource = request.get("resource_session")
    if not isinstance(resource, Mapping):
        raise QELTResourceRepairError(f"evaluation {evaluation_id} request lacks resource_session identity")
    expected_identity = {
        "evaluation_id": evaluation_id,
        "task_id": str(row["task_id"]),
        "loop_id": str(row["loop_id"]),
        "session_id": str(row["session_id"]),
        "source_run_key": str(row["source_run_key"]),
        "node_id": str(row["node_id"]),
    }
    actual_identity = {
        "evaluation_id": str(job.get("evaluation_id") or job_dir.name),
        "task_id": str(job.get("task_id") or job_dir.parents[2].name),
        "loop_id": str(job.get("loop_id") or job_dir.parents[1].name),
        "session_id": str(resource.get("session_id") or ""),
        "source_run_key": str(resource.get("source_run_key") or ""),
        "node_id": str(request.get("node_id") or ""),
    }
    if actual_identity != expected_identity:
        raise QELTResourceRepairError(
            f"evaluation {evaluation_id} RD job identity mismatch: expected={expected_identity} actual={actual_identity}"
        )
    _after_status, _after_phase, next_sequence = _expected_state(int(row["last_sequence_no"]))
    phase_hashes = {str(key): str(value) for key, value in dict(row.get("phase_event_hashes") or {}).items()}
    outbox_paths = sorted((job_dir / "outbox").glob("*.json"))
    if [path.name for path in outbox_paths] != ["000001.json", "000002.json"]:
        raise QELTResourceRepairError(
            f"evaluation {evaluation_id} must have the exact durable sequence 1/2 outbox chain"
        )
    outboxes: list[dict[str, Any]] = []
    for outbox_path in outbox_paths:
        outbox = _load_json(outbox_path)
        payload = outbox.get("payload")
        if not isinstance(payload, Mapping):
            raise QELTResourceRepairError(f"outbox has no payload object: {outbox_path}")
        sequence_no = int(payload.get("sequence_no") or -1)
        payload_identity = {
            "session_id": str(payload.get("session_id") or ""),
            "source_run_key": str(payload.get("source_run_key") or ""),
            "task_id": str(payload.get("task_id") or ""),
            "loop_id": str(payload.get("loop_id") or ""),
            "loop_index": int(payload.get("loop_index") or -1),
            "node_id": str(payload.get("node_id") or ""),
            "sequence_no": sequence_no,
            "evaluation_id": str((payload.get("metadata") or {}).get("evaluation_id") or ""),
        }
        expected_payload_identity = {
            "session_id": expected_identity["session_id"],
            "source_run_key": expected_identity["source_run_key"],
            "task_id": expected_identity["task_id"],
            "loop_id": expected_identity["loop_id"],
            "loop_index": int(row["loop_index"]),
            "node_id": expected_identity["node_id"],
            "sequence_no": int(outbox_path.stem),
            "evaluation_id": evaluation_id,
        }
        if payload_identity != expected_payload_identity:
            raise QELTResourceRepairError(
                f"evaluation {evaluation_id} outbox identity mismatch: "
                f"expected={expected_payload_identity} actual={payload_identity}"
            )
        if sequence_no == 1 and str(payload.get("phase")) != "long_trend_eval":
            raise QELTResourceRepairError(f"evaluation {evaluation_id} sequence 1 is not long_trend_eval")
        if sequence_no == 2 and str(payload.get("phase")) not in TERMINAL_STATES:
            raise QELTResourceRepairError(f"evaluation {evaluation_id} sequence 2 is not terminal")
        event_sha256 = _canonical_sha256(payload)
        legacy_api_event_sha256 = _legacy_api_event_sha256(payload)
        stored_hash = outbox.get("event_sha256")
        if stored_hash not in (None, event_sha256):
            raise QELTResourceRepairError(f"evaluation {evaluation_id} outbox event hash is inconsistent")
        delivered = outbox.get("delivered") is True
        last_sequence_no = int(row["last_sequence_no"])
        if sequence_no <= last_sequence_no:
            durable_phase_hash = phase_hashes.get(str(sequence_no))
            accepted_hashes = {event_sha256, legacy_api_event_sha256}
            if not delivered or durable_phase_hash not in accepted_hashes:
                raise QELTResourceRepairError(
                    f"evaluation {evaluation_id} delivered outbox does not match its durable phase hash"
                )
        elif delivered:
            raise QELTResourceRepairError(
                f"evaluation {evaluation_id} outbox sequence {sequence_no} claims delivery beyond DB state"
            )
        outboxes.append(
            {
                "outbox_relative_path": outbox_path.relative_to(workspace).as_posix(),
                "payload_identity": payload_identity,
                "phase": str(payload.get("phase") or ""),
                "phase_status": str(payload.get("phase_status") or ""),
                "reason_code": payload.get("reason_code"),
                "event_sha256": event_sha256,
                "legacy_api_event_sha256": legacy_api_event_sha256,
                "durable_phase_event_sha256": phase_hashes.get(str(sequence_no)),
                "durable_phase_hash_authority": (
                    "legacy_api_model_dump_v1"
                    if phase_hashes.get(str(sequence_no)) == legacy_api_event_sha256
                    else "raw_payload_canonical_v1"
                    if phase_hashes.get(str(sequence_no)) == event_sha256
                    else None
                ),
                "delivered": delivered,
                "delivery_state": outbox.get("delivery_state") or ("delivered" if delivered else "pending"),
            }
        )
    return {
        "job_relative_path": job_dir.relative_to(workspace).as_posix(),
        "next_sequence_no": next_sequence,
        "outboxes": outboxes,
    }


def collect_preflight(
    connection: Any,
    workspace: Path,
    *,
    expected_count: int,
    expected_outbox_count: int,
    expected_pending_outbox_count: int,
) -> dict[str, Any]:
    if expected_count <= 0:
        raise QELTResourceRepairError("expected_count must be positive")
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(PREFLIGHT_SQL, (RECONCILER_REASON,))
        rows = [dict(item) for item in cur.fetchall()]
    if len(rows) != expected_count:
        raise QELTResourceRepairError(f"expected {expected_count} repair candidates, found {len(rows)}")
    candidates: list[dict[str, Any]] = []
    for row in rows:
        last_sequence_no = int(row["last_sequence_no"])
        if str(row["source_run_key"]) != f"qelt:{row['evaluation_id']}":
            raise QELTResourceRepairError(f"session {row['session_id']} source/evaluation identity differs")
        if str(row["resource_session_id"]) != str(row["session_id"]):
            raise QELTResourceRepairError(f"session {row['session_id']} control binding differs")
        if (
            str(row["parent_task_id"]) != str(row["task_id"])
            or int(row["parent_loop_index"]) != int(row["loop_index"])
            or str(row["control_node_id"]) != str(row["node_id"])
        ):
            raise QELTResourceRepairError(f"session {row['session_id']} control parent identity differs")
        phase_row_count = int(row.get("phase_row_count") or 0)
        max_phase_sequence = row.get("max_phase_sequence")
        if phase_row_count != last_sequence_no or (
            last_sequence_no > 0 and int(max_phase_sequence or -1) != last_sequence_no
        ):
            raise QELTResourceRepairError(f"session {row['session_id']} phase sequence evidence is inconsistent")
        after_status, after_phase, _next_sequence = _expected_state(last_sequence_no)
        evidence = _outbox_evidence(workspace.resolve(), row)
        before = {
            "status": str(row["status"]),
            "current_phase": str(row["current_phase"]),
            "last_sequence_no": last_sequence_no,
            "terminal_reason_code": str(row["terminal_reason_code"]),
            "completed_at": _stable_scalar(row.get("completed_at")),
        }
        candidates.append(
            {
                "session_id": str(row["session_id"]),
                "source_run_key": str(row["source_run_key"]),
                "evaluation_id": str(row["evaluation_id"]),
                "request_sha": str(row["request_sha"]),
                "before": before,
                "after": {
                    "status": after_status,
                    "current_phase": after_phase,
                    "last_sequence_no": last_sequence_no,
                    "terminal_reason_code": None,
                    "completed_at": None,
                },
                "outbox_evidence": evidence,
            }
        )
    outbox_count = sum(len(item["outbox_evidence"]["outboxes"]) for item in candidates)
    pending_outbox_count = sum(
        1
        for item in candidates
        for outbox in item["outbox_evidence"]["outboxes"]
        if not outbox["delivered"]
    )
    if outbox_count != expected_outbox_count or pending_outbox_count != expected_pending_outbox_count:
        raise QELTResourceRepairError(
            "durable outbox counts differ from the approved preflight contract: "
            f"outboxes={outbox_count}/{expected_outbox_count} "
            f"pending={pending_outbox_count}/{expected_pending_outbox_count}"
        )
    plan = {
        "schema_version": PLAN_SCHEMA,
        "reason_code": "BUG_847_QELT_RESOURCE_SESSION_REPAIR",
        "expected_count": expected_count,
        "candidate_count": len(candidates),
        "outbox_count": outbox_count,
        "pending_outbox_count": pending_outbox_count,
        "workspace": str(workspace.resolve()),
        "candidates": candidates,
    }
    plan["plan_sha256"] = _canonical_sha256(plan)
    return plan


def validate_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    materialized = dict(plan)
    declared_hash = materialized.pop("plan_sha256", None)
    if materialized.get("schema_version") != PLAN_SCHEMA:
        raise QELTResourceRepairError("repair plan schema is unsupported")
    actual_hash = _canonical_sha256(materialized)
    if declared_hash != actual_hash:
        raise QELTResourceRepairError("repair plan canonical hash mismatch")
    candidates = materialized.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != int(materialized.get("expected_count") or -1):
        raise QELTResourceRepairError("repair plan candidate count differs from expected_count")
    return {**materialized, "plan_sha256": actual_hash}


def apply_plan(connection: Any, plan: Mapping[str, Any], *, commit: bool = True) -> dict[str, Any]:
    checked = validate_plan(plan)
    applied: list[str] = []
    already_applied: list[str] = []
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cur:
            for item in checked["candidates"]:
                before = item["before"]
                after = item["after"]
                cur.execute(
                    APPLY_SQL,
                    (
                        after["status"], after["current_phase"], item["session_id"], item["source_run_key"],
                        before["status"], before["current_phase"], before["last_sequence_no"], RECONCILER_REASON,
                        before["completed_at"], item["evaluation_id"], item["request_sha"],
                        before["last_sequence_no"],
                    ),
                )
                if cur.rowcount == 1:
                    applied.append(item["session_id"])
                    continue
                cur.execute(
                    CURRENT_STATE_SQL,
                    (
                        after["last_sequence_no"], item["evaluation_id"], item["request_sha"],
                        item["session_id"], item["source_run_key"],
                    ),
                )
                current = cur.fetchone()
                if current and all(
                    current[field] == after[field]
                    for field in ("status", "current_phase", "last_sequence_no", "terminal_reason_code", "completed_at")
                ) and not current["has_later_phase"] and current["control_matches"]:
                    already_applied.append(item["session_id"])
                    continue
                raise QELTResourceRepairError(
                    f"guarded apply expected original or already-repaired state for {item['session_id']}"
                )
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "operation": "apply",
        "plan_sha256": checked["plan_sha256"],
        "updated_count": len(applied),
        "already_applied_count": len(already_applied),
        "session_ids": applied,
        "already_applied_session_ids": already_applied,
        "candidates": checked["candidates"],
    }
    receipt["receipt_sha256"] = _canonical_sha256(receipt)
    return receipt


def readback(connection: Any, plan: Mapping[str, Any], *, expect_repaired: bool) -> dict[str, Any]:
    checked = validate_plan(plan)
    session_ids = [item["session_id"] for item in checked["candidates"]]
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(READBACK_SQL, (session_ids,))
        rows = [dict(item) for item in cur.fetchall()]
    by_id = {str(item["session_id"]): item for item in rows}
    mismatches: list[dict[str, Any]] = []
    for item in checked["candidates"]:
        actual = by_id.get(item["session_id"])
        expected = item["after"] if expect_repaired else item["before"]
        fields = ("status", "current_phase", "last_sequence_no", "terminal_reason_code", "completed_at")
        if actual is None or any(_stable_scalar(actual.get(field)) != expected.get(field) for field in fields):
            mismatches.append({"session_id": item["session_id"], "expected": expected, "actual": actual})
    return {
        "status": "passed" if not mismatches and len(rows) == len(session_ids) else "failed",
        "expected_count": len(session_ids),
        "read_count": len(rows),
        "mismatches": mismatches,
        "rows": rows,
    }


def rollback_plan(connection: Any, receipt: Mapping[str, Any], *, commit: bool = True) -> dict[str, Any]:
    if receipt.get("schema_version") != RECEIPT_SCHEMA or receipt.get("operation") != "apply":
        raise QELTResourceRepairError("rollback requires an apply receipt")
    materialized = dict(receipt)
    declared_hash = materialized.pop("receipt_sha256", None)
    if declared_hash != _canonical_sha256(materialized):
        raise QELTResourceRepairError("apply receipt canonical hash mismatch")
    rolled_back: list[str] = []
    try:
        with connection.cursor() as cur:
            for item in receipt["candidates"]:
                before = item["before"]
                after = item["after"]
                cur.execute(
                    ROLLBACK_SQL,
                    (
                        before["status"], before["current_phase"], before["terminal_reason_code"],
                        before["completed_at"], item["session_id"], item["source_run_key"],
                        after["status"], after["current_phase"], after["last_sequence_no"],
                        item["evaluation_id"], item["request_sha"], after["last_sequence_no"],
                    ),
                )
                if cur.rowcount != 1:
                    raise QELTResourceRepairError(
                        f"guarded rollback expected one untouched repaired row for {item['session_id']}, "
                        f"updated {cur.rowcount}"
                    )
                rolled_back.append(item["session_id"])
        if commit:
            connection.commit()
    except Exception:
        connection.rollback()
        raise
    return {"status": "rolled_back", "updated_count": len(rolled_back), "session_ids": rolled_back}


def _emit(value: Mapping[str, Any], output: Path | None) -> None:
    encoded = json.dumps(dict(value), ensure_ascii=False, sort_keys=True, indent=2, default=str) + "\n"
    if output is None:
        print(encoded, end="")
    else:
        output.write_text(encoded, encoding="utf-8")


def _load_plan(path: Path) -> dict[str, Any]:
    return validate_plan(_load_json(path))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", required=True, choices=("preflight", "apply", "readback", "rollback"))
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--expected-count", type=int, default=15)
    parser.add_argument("--expected-outbox-count", type=int, default=30)
    parser.add_argument("--expected-pending-outbox-count", type=int, default=18)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    if args.mode == "preflight":
        if args.workspace is None:
            parser.error("--workspace is required for preflight")
        with get_conn(autocommit=False, manage_transaction=True) as connection:
            connection.set_session(readonly=True)
            result = collect_preflight(
                connection,
                args.workspace,
                expected_count=args.expected_count,
                expected_outbox_count=args.expected_outbox_count,
                expected_pending_outbox_count=args.expected_pending_outbox_count,
            )
    elif args.mode == "apply":
        if args.plan is None:
            parser.error("--plan is required for apply")
        if args.output is None:
            parser.error("--output is required for apply so guarded rollback retains a durable receipt")
        with get_conn(autocommit=False, manage_transaction=True) as connection:
            result = apply_plan(connection, _load_plan(args.plan))
    elif args.mode == "readback":
        if args.plan is None:
            parser.error("--plan is required for readback")
        with get_conn(autocommit=True) as connection:
            result = readback(connection, _load_plan(args.plan), expect_repaired=True)
    else:
        if args.receipt is None:
            parser.error("--receipt is required for rollback")
        with get_conn(autocommit=False, manage_transaction=True) as connection:
            result = rollback_plan(connection, _load_json(args.receipt))
    _emit(result, args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
