"""Authenticated QE runtime phase sessions and warehouse persistence.

This module owns the structured contract used by runner-side telemetry and the
custom-evo GPU phase lease.  It never creates schema at runtime.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Mapping

from psycopg2.extras import Json, RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.quantevolver.qe_gpu_training_policy import (
    GPU_TRAINING_POLICIES,
    GPU_TRAINING_POLICY_EXCLUSIVE,
)


RESOURCE_SCHEMA_REASON = "QE_RESOURCE_SESSION_SCHEMA_NOT_READY"
AUTH_FAILED_REASON = "QE_RESOURCE_EVENT_AUTH_FAILED"
SEQUENCE_CONFLICT_REASON = "QE_RESOURCE_EVENT_SEQUENCE_CONFLICT"
PHASE_INVALID_REASON = "QE_RESOURCE_EVENT_PHASE_INVALID"

TERMINAL_PHASES = {"completed", "failed", "cancelled"}
GPU_RELEASE_PHASES = {"gpu_phase_released", "release_rejected"}
_PHASE_TRANSITIONS: dict[str, set[str]] = {
    "created": {"bootstrap", "train", "backtest", "failed"},
    "bootstrap": {"train", "backtest", "finalize", "completed", "failed"},
    "train": {"predict", "release_rejected", "finalize", "completed", "failed"},
    "predict": {"gpu_phase_released", "release_rejected", "finalize", "completed", "failed"},
    "gpu_phase_released": {"backtest", "finalize", "completed", "failed"},
    "release_rejected": {"backtest", "finalize", "completed", "failed"},
    "backtest": {"finalize", "completed", "failed"},
    "finalize": {"completed", "failed"},
    "completed": set(),
    "failed": set(),
    "cancelled": set(),
}


class QEResourcePhaseError(RuntimeError):
    def __init__(self, reason_code: str, message: str):
        super().__init__(f"{reason_code}: {message}")
        self.reason_code = reason_code
        self.message = message


@dataclass(frozen=True)
class ResourceSessionSecret:
    session_id: str
    source_run_key: str
    attempt_no: int
    token: str
    gpu_training_policy: str


def _token_sha256(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _canonical_sha256(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def validate_phase_transition(current_phase: str, event: Mapping[str, Any]) -> None:
    phase = str(event.get("phase") or "")
    if phase not in _PHASE_TRANSITIONS:
        raise QEResourcePhaseError(PHASE_INVALID_REASON, f"unknown phase {phase!r}")
    if phase not in _PHASE_TRANSITIONS.get(current_phase, set()):
        raise QEResourcePhaseError(
            PHASE_INVALID_REASON,
            f"transition {current_phase!r} -> {phase!r} is not allowed",
        )
    if phase == "gpu_phase_released":
        if event.get("release_check_passed") is not True:
            raise QEResourcePhaseError(
                PHASE_INVALID_REASON,
                "gpu_phase_released requires release_check_passed=true",
            )
        if event.get("reason_code") != "QE_GPU_PHASE_RELEASE_CONFIRMED":
            raise QEResourcePhaseError(
                PHASE_INVALID_REASON,
                "gpu_phase_released requires QE_GPU_PHASE_RELEASE_CONFIRMED",
            )


class QEResourcePhaseService:
    SESSION_TABLE = "qe_archive.run_resource_session"
    PHASE_TABLE = "qe_archive.run_resource_phase"

    def __init__(self, connection_provider: Callable[[], Any] | None = None) -> None:
        self._connection_provider = connection_provider or get_conn

    def ensure_schema_ready(self) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass(%s), to_regclass(%s)", (self.SESSION_TABLE, self.PHASE_TABLE))
                row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            raise QEResourcePhaseError(
                RESOURCE_SCHEMA_REASON,
                "qe_archive resource session/phase tables are missing; apply the versioned migration before enabling telemetry",
            )

    def create_session(
        self,
        *,
        task_id: str,
        loop_index: int,
        node_id: str,
        phase_pipeline_enabled: bool,
        gpu_training_policy: str = GPU_TRAINING_POLICY_EXCLUSIVE,
    ) -> ResourceSessionSecret:
        self.ensure_schema_ready()
        normalized_policy = str(gpu_training_policy or "").strip().lower()
        if normalized_policy not in GPU_TRAINING_POLICIES:
            raise ValueError(f"invalid gpu_training_policy: {gpu_training_policy!r}")
        source_run_key = f"{task_id}_L{int(loop_index)}"
        session_id = f"qers_{uuid.uuid4().hex}"
        token = secrets.token_urlsafe(32)
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (source_run_key,),
                )
                cur.execute(
                    """
                    SELECT COALESCE(MAX(attempt_no), 0) + 1
                    FROM qe_archive.run_resource_session
                    WHERE source_run_key = %s
                    """,
                    (source_run_key,),
                )
                attempt_no = int(cur.fetchone()[0])
                cur.execute(
                    """
                    INSERT INTO qe_archive.run_resource_session (
                        session_id, source_run_key, attempt_no, task_id, loop_id,
                        loop_index, node_id, token_sha256, phase_pipeline_enabled,
                        gpu_training_policy, current_phase, last_sequence_no, status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'created', 0, 'reserved')
                    """,
                    (
                        session_id,
                        source_run_key,
                        attempt_no,
                        task_id,
                        f"Loop{int(loop_index)}",
                        int(loop_index),
                        node_id,
                        _token_sha256(token),
                        bool(phase_pipeline_enabled),
                        normalized_policy,
                    ),
                )
            conn.commit()
        return ResourceSessionSecret(
            session_id=session_id,
            source_run_key=source_run_key,
            attempt_no=attempt_no,
            token=token,
            gpu_training_policy=normalized_policy,
        )

    def mark_session_submitted(self, session_id: str) -> None:
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.run_resource_session
                    SET status = 'running', updated_at = NOW()
                    WHERE session_id = %s AND status = 'reserved'
                    """,
                    (session_id,),
                )
                if cur.rowcount != 1:
                    cur.execute(
                        "SELECT status FROM qe_archive.run_resource_session WHERE session_id = %s",
                        (session_id,),
                    )
                    row = cur.fetchone()
                    if not row or row[0] not in {"running", "completed", "failed", "cancelled"}:
                        raise QEResourcePhaseError(PHASE_INVALID_REASON, f"session {session_id} is not reserved")
            conn.commit()

    def mark_session_terminal(self, session_id: str, *, status: str, reason_code: str | None = None) -> None:
        normalized = "cancelled" if status in {"cancelled", "canceled"} else status
        if normalized not in TERMINAL_PHASES:
            raise ValueError(f"invalid terminal resource session status: {status}")
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE qe_archive.run_resource_session
                    SET status = %s,
                        current_phase = CASE
                            WHEN current_phase IN ('completed', 'failed', 'cancelled') THEN current_phase
                            ELSE %s
                        END,
                        terminal_reason_code = COALESCE(%s, terminal_reason_code),
                        completed_at = COALESCE(completed_at, NOW()),
                        updated_at = NOW()
                    WHERE session_id = %s
                    """,
                    (normalized, normalized, reason_code, session_id),
                )
            conn.commit()

    def has_unreleased_gpu_session(
        self,
        *,
        node_id: str,
        requested_policy: str = GPU_TRAINING_POLICY_EXCLUSIVE,
        exclude_session_id: str | None = None,
    ) -> bool:
        self.ensure_schema_ready()
        normalized_policy = str(requested_policy or "").strip().lower()
        if normalized_policy not in GPU_TRAINING_POLICIES:
            raise ValueError(f"invalid requested_policy: {requested_policy!r}")
        params: list[Any] = [node_id]
        exclusion = ""
        if exclude_session_id:
            exclusion = "AND session_id <> %s"
            params.append(exclude_session_id)
        policy_conflict = ""
        if normalized_policy != GPU_TRAINING_POLICY_EXCLUSIVE:
            policy_conflict = "AND gpu_training_policy = 'exclusive'"
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT 1
                    FROM qe_archive.run_resource_session
                    WHERE node_id = %s
                      AND phase_pipeline_enabled = TRUE
                      {policy_conflict}
                      AND status IN ('reserved', 'running')
                      AND current_phase NOT IN ('gpu_phase_released', 'release_rejected', 'backtest', 'finalize', 'completed', 'failed', 'cancelled')
                      AND EXISTS (
                          SELECT 1
                          FROM qe_evolution_loops l
                          WHERE l.task_id = qe_archive.run_resource_session.task_id
                            AND l.loop_index = qe_archive.run_resource_session.loop_index
                            AND l.status IN ('pending', 'running', 'processing')
                      )
                      {exclusion}
                    LIMIT 1
                    """,
                    params,
                )
                return cur.fetchone() is not None

    def get_session_state(self, session_id: str) -> dict[str, Any] | None:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT session_id, source_run_key, attempt_no, task_id, loop_id,
                           loop_index, node_id, phase_pipeline_enabled, gpu_training_policy, current_phase,
                           last_sequence_no, status, gpu_phase_released_at,
                           terminal_reason_code, created_at, updated_at, completed_at
                    FROM qe_archive.run_resource_session
                    WHERE session_id = %s
                    """,
                    (session_id,),
                )
                row = cur.fetchone()
        return dict(row) if row else None

    def ingest_event(self, *, token: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        self.ensure_schema_ready()
        required = {
            "session_id",
            "source_run_key",
            "task_id",
            "loop_id",
            "loop_index",
            "node_id",
            "sequence_no",
            "phase",
            "phase_status",
        }
        missing = sorted(key for key in required if payload.get(key) in (None, ""))
        if missing:
            raise QEResourcePhaseError(PHASE_INVALID_REASON, f"event is missing required fields: {missing}")

        event = dict(payload)
        encoded_event = json.dumps(event, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        if len(encoded_event) > 256 * 1024:
            raise QEResourcePhaseError(
                PHASE_INVALID_REASON,
                f"resource event exceeds 262144-byte limit: {len(encoded_event)}",
            )
        event_sha256 = _canonical_sha256(event)
        session_id = str(event["session_id"])
        sequence_no = int(event["sequence_no"])
        phase = str(event["phase"])
        if phase not in _PHASE_TRANSITIONS:
            raise QEResourcePhaseError(PHASE_INVALID_REASON, f"unknown phase {phase!r}")

        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qe_archive.run_resource_session WHERE session_id = %s FOR UPDATE",
                    (session_id,),
                )
                session = cur.fetchone()
                if not session or not secrets.compare_digest(str(session["token_sha256"]), _token_sha256(token)):
                    raise QEResourcePhaseError(AUTH_FAILED_REASON, "resource session token or session_id is invalid")
                identity_pairs = {
                    "source_run_key": str(event["source_run_key"]),
                    "task_id": str(event["task_id"]),
                    "loop_id": str(event["loop_id"]),
                    "loop_index": int(event["loop_index"]),
                    "node_id": str(event["node_id"]),
                }
                for key, actual in identity_pairs.items():
                    if session[key] != actual:
                        raise QEResourcePhaseError(
                            AUTH_FAILED_REASON,
                            f"resource event {key} does not match session binding",
                        )

                last_sequence = int(session["last_sequence_no"] or 0)
                if sequence_no <= last_sequence:
                    cur.execute(
                        """
                        SELECT id, event_sha256 FROM qe_archive.run_resource_phase
                        WHERE session_id = %s AND sequence_no = %s
                        """,
                        (session_id, sequence_no),
                    )
                    prior = cur.fetchone()
                    if prior and prior["event_sha256"] == event_sha256:
                        return {
                            "status": "idempotent",
                            "session_id": session_id,
                            "sequence_no": sequence_no,
                            "phase": phase,
                        }
                    raise QEResourcePhaseError(
                        SEQUENCE_CONFLICT_REASON,
                        f"sequence {sequence_no} conflicts with last_sequence_no={last_sequence}",
                    )
                if sequence_no != last_sequence + 1:
                    raise QEResourcePhaseError(
                        SEQUENCE_CONFLICT_REASON,
                        f"expected sequence {last_sequence + 1}, got {sequence_no}",
                    )

                current_phase = str(session["current_phase"] or "created")
                validate_phase_transition(current_phase, event)

                cur.execute(
                    """
                    INSERT INTO qe_archive.run_resource_phase (
                        session_id, source_run_key, sequence_no, phase, phase_status,
                        started_at, ended_at, duration_seconds, sample_count,
                        process_rss_peak_bytes, process_vm_hwm_peak_bytes,
                        gpu_device_index, gpu_name, gpu_memory_used_peak_bytes,
                        gpu_process_memory_peak_bytes, gpu_utilization_avg_pct,
                        gpu_utilization_peak_pct, cuda_allocated_peak_bytes,
                        cuda_reserved_peak_bytes, cuda_allocated_end_bytes,
                        cuda_reserved_end_bytes, resident_requested, resident_active,
                        resident_fallback, fallback_reason_code, release_check_passed,
                        reason_code, metadata, event_sha256
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        session_id,
                        event["source_run_key"],
                        sequence_no,
                        phase,
                        event["phase_status"],
                        event.get("started_at"),
                        event.get("ended_at"),
                        event.get("duration_seconds"),
                        event.get("sample_count"),
                        event.get("process_rss_peak_bytes"),
                        event.get("process_vm_hwm_peak_bytes"),
                        event.get("gpu_device_index"),
                        event.get("gpu_name"),
                        event.get("gpu_memory_used_peak_bytes"),
                        event.get("gpu_process_memory_peak_bytes"),
                        event.get("gpu_utilization_avg_pct"),
                        event.get("gpu_utilization_peak_pct"),
                        event.get("cuda_allocated_peak_bytes"),
                        event.get("cuda_reserved_peak_bytes"),
                        event.get("cuda_allocated_end_bytes"),
                        event.get("cuda_reserved_end_bytes"),
                        event.get("resident_requested"),
                        event.get("resident_active"),
                        event.get("resident_fallback"),
                        event.get("fallback_reason_code"),
                        event.get("release_check_passed"),
                        event.get("reason_code"),
                        Json(event.get("metadata") or {}),
                        event_sha256,
                    ),
                )
                terminal_status = phase if phase in TERMINAL_PHASES else None
                cur.execute(
                    """
                    UPDATE qe_archive.run_resource_session
                    SET current_phase = %s,
                        last_sequence_no = %s,
                        status = COALESCE(%s, status),
                        gpu_phase_released_at = CASE
                            WHEN %s = 'gpu_phase_released' THEN COALESCE(gpu_phase_released_at, NOW())
                            ELSE gpu_phase_released_at
                        END,
                        completed_at = CASE
                            WHEN %s IS NOT NULL THEN COALESCE(completed_at, NOW())
                            ELSE completed_at
                        END,
                        terminal_reason_code = CASE
                            WHEN %s IS NOT NULL THEN COALESCE(%s, terminal_reason_code)
                            ELSE terminal_reason_code
                        END,
                        updated_at = NOW()
                    WHERE session_id = %s
                    """,
                    (
                        phase,
                        sequence_no,
                        terminal_status,
                        phase,
                        terminal_status,
                        terminal_status,
                        event.get("reason_code"),
                        session_id,
                    ),
                )
            conn.commit()
        return {
            "status": "accepted",
            "session_id": session_id,
            "sequence_no": sequence_no,
            "phase": phase,
        }

    def list_resource_phases(
        self,
        *,
        run_id: str | None = None,
        task_id: str | None = None,
        loop_index: int | None = None,
        source_run_key: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        self.ensure_schema_ready()
        limit = max(1, min(int(limit), 200))
        clauses: list[str] = []
        params: list[Any] = []
        if run_id:
            clauses.append("s.archive_run_id = %s")
            params.append(run_id)
        if task_id:
            clauses.append("s.task_id = %s")
            params.append(task_id)
        if loop_index is not None:
            clauses.append("s.loop_index = %s")
            params.append(int(loop_index))
        if source_run_key:
            clauses.append("s.source_run_key = %s")
            params.append(source_run_key)
        where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(limit)
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    f"""
                    SELECT s.session_id, s.source_run_key, s.attempt_no, s.task_id,
                           s.loop_id, s.loop_index, s.node_id, s.archive_run_id,
                           s.phase_pipeline_enabled, s.gpu_training_policy, s.current_phase,
                           s.last_sequence_no, s.status, s.gpu_phase_released_at,
                           s.terminal_reason_code, s.created_at, s.updated_at,
                           s.completed_at,
                           COALESCE(
                               jsonb_agg(
                                   jsonb_build_object(
                                       'sequence_no', p.sequence_no,
                                       'phase', p.phase,
                                       'phase_status', p.phase_status,
                                       'started_at', p.started_at,
                                       'ended_at', p.ended_at,
                                       'duration_seconds', p.duration_seconds,
                                       'sample_count', p.sample_count,
                                       'process_rss_peak_bytes', p.process_rss_peak_bytes,
                                       'process_vm_hwm_peak_bytes', p.process_vm_hwm_peak_bytes,
                                       'gpu_memory_used_peak_bytes', p.gpu_memory_used_peak_bytes,
                                       'gpu_process_memory_peak_bytes', p.gpu_process_memory_peak_bytes,
                                       'gpu_utilization_avg_pct', p.gpu_utilization_avg_pct,
                                       'gpu_utilization_peak_pct', p.gpu_utilization_peak_pct,
                                       'cuda_allocated_peak_bytes', p.cuda_allocated_peak_bytes,
                                       'cuda_reserved_peak_bytes', p.cuda_reserved_peak_bytes,
                                       'cuda_allocated_end_bytes', p.cuda_allocated_end_bytes,
                                       'cuda_reserved_end_bytes', p.cuda_reserved_end_bytes,
                                       'resident_requested', p.resident_requested,
                                       'resident_active', p.resident_active,
                                       'resident_fallback', p.resident_fallback,
                                       'fallback_reason_code', p.fallback_reason_code,
                                       'release_check_passed', p.release_check_passed,
                                       'reason_code', p.reason_code,
                                       'metadata', p.metadata
                                   ) ORDER BY p.sequence_no
                               ) FILTER (WHERE p.id IS NOT NULL),
                               '[]'::jsonb
                           ) AS phases
                    FROM qe_archive.run_resource_session s
                    LEFT JOIN qe_archive.run_resource_phase p ON p.session_id = s.session_id
                    {where_sql}
                    GROUP BY s.session_id
                    ORDER BY s.created_at DESC
                    LIMIT %s
                    """,
                    params,
                )
                return [dict(row) for row in cur.fetchall()]

    def bind_archive_run(
        self,
        *,
        task_id: str,
        loop_index: int,
        archive_run_id: str,
        attempt_no: int | None = None,
    ) -> int:
        self.ensure_schema_ready()
        with self._connection_provider() as conn:
            with conn.cursor() as cur:
                if attempt_no is not None:
                    cur.execute(
                        """
                        UPDATE qe_archive.run_resource_session
                        SET archive_run_id = %s, updated_at = NOW()
                        WHERE task_id = %s AND loop_index = %s AND attempt_no = %s
                          AND archive_run_id IS NULL
                        """,
                        (archive_run_id, task_id, int(loop_index), int(attempt_no)),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE qe_archive.run_resource_session
                        SET archive_run_id = %s, updated_at = NOW()
                        WHERE session_id = (
                            SELECT session_id
                            FROM qe_archive.run_resource_session
                            WHERE task_id = %s AND loop_index = %s AND archive_run_id IS NULL
                            ORDER BY attempt_no DESC
                            LIMIT 1
                        )
                        """,
                        (archive_run_id, task_id, int(loop_index)),
                    )
                count = int(cur.rowcount)
            conn.commit()
        return count
