from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import socket
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping

import httpx
from psycopg2.extras import RealDictCursor

from backend.db.pg_pool import get_conn
from backend.services.qe_archive.models import normalize_json

from .qe_execution_reservation import (
    ACTIVE_RESERVATION_STATUSES,
    CapacityWaitRecorder,
    QEExecutionReservationAcquireResult,
    QEExecutionReservationError,
    QEExecutionReservationRepository,
    QEExecutionReservationSpec,
    QEExecutionReservationToken,
    SourceClaim,
)
from .qe_workspace_client import (
    QEWorkspaceSubmissionContractError,
    QEWorkspaceSubmissionInspection,
    QEWorkspaceSubmissionReceipt,
    QEWorkspaceSubmissionRejected,
    QEWorkspaceSubmissionTransportError,
    QEWorkspaceClient,
)


DEFAULT_WSL_NODE_ID = "wsl2-5080"
WSL_HARD_CAPACITY = 2
REMOTE_HARD_CAPACITY = 4
DEFAULT_RESERVATION_LEASE_SECONDS = 120
_PROCESS_SUBMISSION_OWNER_ID = (
    f"qe_submit_{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:12]}"
)
_QUEUE_ONLY_LOCK = threading.RLock()
_QUEUE_ONLY_NODE_DIAGNOSTICS: dict[str, tuple[Mapping[str, Any], ...]] = {}
logger = logging.getLogger("aistock.qe_execution_capacity")


class QEWorkspaceSubmissionCoordinatorError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        reason_code: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


@dataclass(frozen=True)
class QEWorkspaceSubmissionPayload:
    task_id: str
    loop_index: int
    config: Mapping[str, Any]
    experiment_files: Mapping[str, str]
    wsl_command: str
    model_source: Mapping[str, Any] | None = None
    callback_url: str | None = None

    @property
    def loop_id(self) -> str:
        return f"Loop{self.loop_index}"


@dataclass(frozen=True)
class QEWorkspaceSubmissionSource:
    source_kind: str
    source_execution_id: str
    node_id: str
    submission_intent_hash: str
    owner_id: str
    claim_source: SourceClaim
    record_waiting_capacity: CapacityWaitRecorder
    requested_node_capacity: int | None = None
    lease_seconds: int = DEFAULT_RESERVATION_LEASE_SECONDS


@dataclass(frozen=True)
class QEWorkspaceSubmissionOutcome:
    state: str
    task_id: str
    loop_id: str
    reservation_id: str | None
    reservation_status: str | None
    remote_status: str | None
    active_count: int
    node_capacity: int
    duplicate_replay: bool
    remote_acceptance_unknown: bool
    source_claim: Mapping[str, Any] | None = None
    receipt: QEWorkspaceSubmissionReceipt | QEWorkspaceSubmissionInspection | None = None
    detail: Mapping[str, Any] | None = None

    @property
    def submitted(self) -> bool:
        return self.state in {"submitted", "duplicate_replay", "receipt_recovered"}

    @property
    def waiting_capacity(self) -> bool:
        return self.state == "waiting_capacity"


class QEActiveExecutionCapacityService:
    """Resolve the operational cap without GPU or desktop resource telemetry."""

    def __init__(
        self,
        *,
        wsl_node_id: str = DEFAULT_WSL_NODE_ID,
        wsl_hard_capacity: int = WSL_HARD_CAPACITY,
        remote_hard_capacity: int = REMOTE_HARD_CAPACITY,
    ) -> None:
        if not str(wsl_node_id or "").strip():
            raise QEWorkspaceSubmissionCoordinatorError(
                "WSL node identity must not be empty",
                reason_code="qe_execution_capacity_contract_invalid",
            )
        if wsl_hard_capacity < 1 or remote_hard_capacity < 1:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE node hard capacities must be positive",
                reason_code="qe_execution_capacity_contract_invalid",
            )
        self._wsl_node_id = str(wsl_node_id).strip()
        self._wsl_hard_capacity = int(wsl_hard_capacity)
        self._remote_hard_capacity = int(remote_hard_capacity)

    def resolve_node_capacity(self, node_id: str, requested_limit: int | None = None) -> int:
        normalized_node_id = str(node_id or "").strip()
        if not normalized_node_id:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE node identity must not be empty",
                reason_code="qe_execution_capacity_node_invalid",
            )
        hard_cap = (
            self._wsl_hard_capacity
            if normalized_node_id == self._wsl_node_id
            else self._remote_hard_capacity
        )
        if requested_limit is None:
            return hard_cap
        if isinstance(requested_limit, bool):
            normalized_limit = 0
        else:
            try:
                normalized_limit = int(requested_limit)
            except (TypeError, ValueError):
                normalized_limit = 0
        if normalized_limit < 1:
            raise QEWorkspaceSubmissionCoordinatorError(
                "requested QE node capacity must be a positive integer",
                reason_code="qe_execution_capacity_request_invalid",
                context={"node_id": normalized_node_id, "requested_limit": requested_limit},
            )
        return min(normalized_limit, hard_cap)

    @staticmethod
    def queue_only_diagnostics(node_id: str) -> tuple[Mapping[str, Any], ...]:
        with _QUEUE_ONLY_LOCK:
            return tuple(
                dict(item)
                for item in _QUEUE_ONLY_NODE_DIAGNOSTICS.get(str(node_id), ())
            )


def set_qe_capacity_queue_only_nodes(
    diagnostics_by_node: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> None:
    normalized: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for node_id, diagnostics in diagnostics_by_node.items():
        normalized_node_id = str(node_id or "").strip()
        if not normalized_node_id:
            continue
        normalized[normalized_node_id] = tuple(dict(item) for item in diagnostics)
    with _QUEUE_ONLY_LOCK:
        _QUEUE_ONLY_NODE_DIAGNOSTICS.clear()
        _QUEUE_ONLY_NODE_DIAGNOSTICS.update(normalized)


@dataclass(frozen=True)
class QEActiveExecutionImportResult:
    discovered_count: int
    imported_count: int
    deduplicated_count: int
    reservations: tuple[Mapping[str, Any], ...]
    unresolved: tuple[Mapping[str, Any], ...]
    aliases_by_remote_identity: Mapping[str, tuple[Mapping[str, Any], ...]]
    queue_only_nodes: Mapping[str, tuple[Mapping[str, Any], ...]]


class QEActiveExecutionImportService:
    """One-time activation import for QE executions already active before P0-1B."""

    ACTIVE_SOURCE_STATUSES = ("running", "processing", "submitting", "reconciling")

    def __init__(
        self,
        *,
        reservation_repository: QEExecutionReservationRepository | None = None,
        connection_provider: Any = get_conn,
        workspace_client_factory: Any = QEWorkspaceClient.for_node,
    ) -> None:
        self._repository = reservation_repository or QEExecutionReservationRepository()
        self._connection_provider = connection_provider
        self._workspace_client_factory = workspace_client_factory

    def import_current_active_sources(self) -> QEActiveExecutionImportResult:
        self._repository.preflight_schema(raise_on_error=True)
        candidates = self._discover_candidates()
        unresolved: list[Mapping[str, Any]] = []
        grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
        for candidate in candidates:
            node_id = str(candidate.get("node_id") or "").strip()
            task_id = str(candidate.get("qe_task_id") or "").strip()
            loop_id = self._canonical_loop_id(task_id, candidate.get("qe_loop_id"))
            if not node_id or not task_id or loop_id is None:
                unresolved.append(
                    {
                        "reason_code": "qe_capacity_identity_unresolved",
                        **dict(candidate),
                    }
                )
                continue
            grouped.setdefault((node_id, task_id, loop_id), []).append(candidate)

        reservations: list[Mapping[str, Any]] = []
        aliases: dict[str, tuple[Mapping[str, Any], ...]] = {}
        for node_id, task_id, loop_id in sorted(grouped):
            remote_key = f"{node_id}:{task_id}:{loop_id}"
            source_execution_id = f"legacy:{remote_key}"
            spec = QEExecutionReservationSpec(
                node_id=node_id,
                source_kind="legacy_active_import",
                source_execution_id=source_execution_id,
                qe_task_id=task_id,
                qe_loop_id=loop_id,
                submission_intent_hash=submission_intent_hash_for_source(
                    source_kind="legacy_active_import",
                    source_execution_id=source_execution_id,
                    node_id=node_id,
                    task_id=task_id,
                    loop_id=loop_id,
                ),
            )
            reservation = self._repository.import_legacy_active_execution(
                spec,
                remote_status="legacy_active_import",
            )
            reservations.append(reservation)
            aliases[remote_key] = tuple(dict(item) for item in grouped[(node_id, task_id, loop_id)])
        return QEActiveExecutionImportResult(
            discovered_count=len(candidates),
            imported_count=len(reservations),
            deduplicated_count=sum(max(0, len(items) - 1) for items in grouped.values()),
            reservations=tuple(reservations),
            unresolved=tuple(unresolved),
            aliases_by_remote_identity=aliases,
            queue_only_nodes={},
        )

    async def import_current_active_sources_verified(self) -> QEActiveExecutionImportResult:
        """Cross-check active DB sources against QE Workspace before activation."""
        self._repository.preflight_schema(raise_on_error=True)
        candidates = self._discover_candidates()
        node_ids = self._list_compute_node_ids()
        resolved: list[Mapping[str, Any]] = []
        unresolved: list[Mapping[str, Any]] = []
        queue_only: dict[str, list[Mapping[str, Any]]] = {}

        for candidate in candidates:
            task_id = str(candidate.get("qe_task_id") or "").strip()
            loop_id = self._canonical_loop_id(task_id, candidate.get("qe_loop_id"))
            configured_node = str(candidate.get("node_id") or "").strip()
            if not task_id or loop_id is None:
                diagnostic = {
                    "reason_code": "qe_capacity_identity_unresolved",
                    "candidate_nodes": [configured_node] if configured_node else [],
                    **dict(candidate),
                }
                unresolved.append(diagnostic)
                if configured_node:
                    queue_only.setdefault(configured_node, []).append(diagnostic)
                continue

            nodes_to_check = [configured_node] if configured_node else node_ids
            checks = await asyncio.gather(
                *(
                    self._inspect_candidate_on_node(
                        node_id=node_id,
                        task_id=task_id,
                        loop_id=loop_id,
                    )
                    for node_id in nodes_to_check
                )
            )
            active_checks = [check for check in checks if check["state"] == "active"]
            unavailable_checks = [
                check for check in checks if check["state"] == "unavailable"
            ]
            terminal_checks = [check for check in checks if check["state"] == "terminal"]

            if configured_node:
                if active_checks:
                    resolved.append({**dict(candidate), "node_id": configured_node})
                    continue
                if unavailable_checks:
                    diagnostic = {
                        "reason_code": "qe_capacity_remote_verification_unavailable",
                        "verification": checks,
                        **dict(candidate),
                    }
                    unresolved.append(diagnostic)
                    resolved.append({**dict(candidate), "node_id": configured_node})
                    continue
                if terminal_checks:
                    unresolved.append(
                        {
                            "reason_code": "qe_capacity_source_remote_terminal",
                            "verification": checks,
                            **dict(candidate),
                        }
                    )
                    continue
                diagnostic = {
                    "reason_code": "qe_capacity_configured_node_execution_missing",
                    "candidate_nodes": [configured_node],
                    "verification": checks,
                    **dict(candidate),
                }
                unresolved.append(diagnostic)
                queue_only.setdefault(configured_node, []).append(diagnostic)
                continue

            if len(active_checks) == 1:
                resolved.append(
                    {
                        **dict(candidate),
                        "node_id": active_checks[0]["node_id"],
                    }
                )
                continue
            related_nodes = sorted(
                {
                    str(check["node_id"])
                    for check in [*active_checks, *unavailable_checks]
                }
            )
            diagnostic = {
                "reason_code": "qe_capacity_identity_unresolved",
                "candidate_nodes": related_nodes,
                "verification": checks,
                **dict(candidate),
            }
            unresolved.append(diagnostic)
            for node_id in related_nodes:
                queue_only.setdefault(node_id, []).append(diagnostic)

        result = self._import_resolved_candidates(
            candidates=resolved,
            discovered_count=len(candidates),
            unresolved=unresolved,
            queue_only=queue_only,
        )
        set_qe_capacity_queue_only_nodes(result.queue_only_nodes)
        return result

    def _import_resolved_candidates(
        self,
        *,
        candidates: list[Mapping[str, Any]],
        discovered_count: int,
        unresolved: list[Mapping[str, Any]],
        queue_only: Mapping[str, list[Mapping[str, Any]]],
    ) -> QEActiveExecutionImportResult:
        grouped: dict[tuple[str, str, str], list[Mapping[str, Any]]] = {}
        for candidate in candidates:
            node_id = str(candidate.get("node_id") or "").strip()
            task_id = str(candidate.get("qe_task_id") or "").strip()
            loop_id = self._canonical_loop_id(task_id, candidate.get("qe_loop_id"))
            if not node_id or not task_id or loop_id is None:
                raise QEWorkspaceSubmissionCoordinatorError(
                    "verified active execution lost its canonical identity before import",
                    reason_code="qe_capacity_identity_unresolved",
                    context=dict(candidate),
                )
            grouped.setdefault((node_id, task_id, loop_id), []).append(candidate)

        reservations: list[Mapping[str, Any]] = []
        aliases: dict[str, tuple[Mapping[str, Any], ...]] = {}
        for node_id, task_id, loop_id in sorted(grouped):
            remote_key = f"{node_id}:{task_id}:{loop_id}"
            source_execution_id = f"legacy:{remote_key}"
            spec = QEExecutionReservationSpec(
                node_id=node_id,
                source_kind="legacy_active_import",
                source_execution_id=source_execution_id,
                qe_task_id=task_id,
                qe_loop_id=loop_id,
                submission_intent_hash=submission_intent_hash_for_source(
                    source_kind="legacy_active_import",
                    source_execution_id=source_execution_id,
                    node_id=node_id,
                    task_id=task_id,
                    loop_id=loop_id,
                ),
            )
            reservations.append(
                self._repository.import_legacy_active_execution(
                    spec,
                    remote_status="legacy_active_import_verified",
                )
            )
            aliases[remote_key] = tuple(dict(item) for item in grouped[(node_id, task_id, loop_id)])
        return QEActiveExecutionImportResult(
            discovered_count=discovered_count,
            imported_count=len(reservations),
            deduplicated_count=sum(max(0, len(items) - 1) for items in grouped.values()),
            reservations=tuple(reservations),
            unresolved=tuple(dict(item) for item in unresolved),
            aliases_by_remote_identity=aliases,
            queue_only_nodes={
                node_id: tuple(dict(item) for item in diagnostics)
                for node_id, diagnostics in queue_only.items()
            },
        )

    def _list_compute_node_ids(self) -> list[str]:
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT node_id FROM infra.compute_nodes ORDER BY node_id")
                return [str(row["node_id"]) for row in cur.fetchall()]

    async def _inspect_candidate_on_node(
        self,
        *,
        node_id: str,
        task_id: str,
        loop_id: str,
    ) -> Mapping[str, Any]:
        client = self._workspace_client_factory(node_id)
        try:
            async with client:
                receipt = await client.inspect_loop_submission(task_id, loop_id)
                try:
                    status_payload = await client.get_loop_status(task_id, loop_id)
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 404:
                        raise
                    status_payload = {"status": "not_found"}
        except (
            httpx.HTTPError,
            QEWorkspaceSubmissionRejected,
            QEWorkspaceSubmissionTransportError,
            QEWorkspaceSubmissionContractError,
        ) as exc:
            return {
                "node_id": node_id,
                "state": "unavailable",
                "reason_code": getattr(
                    exc,
                    "reason_code",
                    "qe_capacity_remote_verification_unavailable",
                ),
                "message": str(exc),
            }
        status = str(status_payload.get("status") or "").strip().lower()
        receipt_status = str(receipt.status or "").strip().lower()
        if status in {"completed", "failed", "cancelled", "canceled", "interrupted"}:
            state = "terminal"
        elif status in {"running", "processing", "queued", "pending", "submitted"}:
            state = "active"
        elif receipt_status in {"reserved", "started", "running"}:
            state = "active"
        elif receipt_status in {"completed", "failed", "cancelled"}:
            state = "terminal"
        else:
            state = "absent"
        return {
            "node_id": node_id,
            "state": state,
            "receipt_status": receipt_status,
            "remote_status": status,
        }

    def _discover_candidates(self) -> list[Mapping[str, Any]]:
        candidates: list[Mapping[str, Any]] = []
        with self._connection_provider() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT 'qe_evolution_loop' AS source_kind,
                           l.loop_id AS source_execution_id,
                           COALESCE(NULLIF(l.node_id, ''), NULLIF(t.node_id, '')) AS node_id,
                           l.task_id AS qe_task_id,
                           'Loop' || l.loop_index::text AS qe_loop_id,
                           l.status AS source_status
                    FROM qe_evolution_loops AS l
                    JOIN qe_evolution_tasks AS t ON t.task_id = l.task_id
                    WHERE l.status = ANY(%s)
                    """,
                    (list(self.ACTIVE_SOURCE_STATUSES),),
                )
                candidates.extend(dict(row) for row in cur.fetchall())
                cur.execute(
                    """
                    SELECT 'qe_experiment' AS source_kind,
                           e.experiment_id AS source_execution_id,
                           NULLIF(e.custom_params ->> 'execution_node_id', '') AS node_id,
                           e.qe_task_id,
                           e.qe_loop_id,
                           e.status AS source_status
                    FROM qe_experiments AS e
                    WHERE e.status = ANY(%s)
                    """,
                    (list(self.ACTIVE_SOURCE_STATUSES),),
                )
                candidates.extend(dict(row) for row in cur.fetchall())
                cur.execute(
                    """
                    SELECT 'qe_multi_alpha_group' AS source_kind,
                           g.parent_experiment_id || ':' || g.group_name AS source_execution_id,
                           NULLIF(g.assigned_node_id, '') AS node_id,
                           e.qe_task_id,
                           g.qe_loop_id,
                           g.status AS source_status
                    FROM qe_multi_alpha_groups AS g
                    JOIN qe_experiments AS e
                      ON e.experiment_id = g.parent_experiment_id
                    WHERE g.status = ANY(%s)
                    """,
                    (list(self.ACTIVE_SOURCE_STATUSES),),
                )
                candidates.extend(dict(row) for row in cur.fetchall())
        return candidates

    @staticmethod
    def _canonical_loop_id(task_id: str, value: Any) -> str | None:
        raw = str(value or "").strip()
        if raw.startswith(f"{task_id}_"):
            raw = raw[len(task_id) + 1 :]
        if raw.startswith("Loop") and raw[4:].isdigit() and int(raw[4:]) >= 1:
            return raw
        return None


class QEExecutionSourceClaimFactory:
    """Build source-specific SQL callbacks used inside the reservation transaction."""

    @staticmethod
    def evolution_loop(
        *,
        loop_id: str,
        node_id: str,
    ) -> tuple[SourceClaim, CapacityWaitRecorder]:
        normalized_loop_id = str(loop_id or "").strip()
        normalized_node_id = str(node_id or "").strip()

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE qe_evolution_loops
                SET status = 'running',
                    node_id = %s,
                    agent_analysis = CASE
                        WHEN agent_analysis LIKE '{"_qe_execution_capacity"%%'
                        THEN NULL
                        ELSE agent_analysis
                    END,
                    updated_at = NOW()
                WHERE loop_id = %s
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'canceled')
                RETURNING loop_id, task_id, status, node_id
                """,
                (normalized_node_id, normalized_loop_id),
            )
            return cur.fetchone()

        def record_waiting(
            cur: Any,
            active_count: int,
            node_capacity: int,
        ) -> Mapping[str, Any] | None:
            evidence = json.dumps(
                {
                    "_qe_execution_capacity": {
                        "state": "waiting_capacity",
                        "node_id": normalized_node_id,
                        "active_count": int(active_count),
                        "node_capacity": int(node_capacity),
                    }
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            cur.execute(
                """
                UPDATE qe_evolution_loops
                SET status = 'pending',
                    node_id = %s,
                    agent_analysis = %s,
                    updated_at = NOW()
                WHERE loop_id = %s
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'canceled')
                RETURNING loop_id, task_id, status, node_id, agent_analysis
                """,
                (normalized_node_id, evidence, normalized_loop_id),
            )
            return cur.fetchone()

        return claim_source, record_waiting

    @staticmethod
    def experiment(
        *,
        experiment_id: str,
        node_id: str,
        qe_task_id: str,
        qe_loop_id: str,
    ) -> tuple[SourceClaim, CapacityWaitRecorder]:
        normalized_experiment_id = str(experiment_id or "").strip()
        normalized_node_id = str(node_id or "").strip()
        normalized_task_id = str(qe_task_id or "").strip()
        normalized_loop_id = str(qe_loop_id or "").strip()

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE qe_experiments
                SET status = 'running',
                    qe_task_id = %s,
                    qe_loop_id = %s,
                    custom_params = COALESCE(custom_params, '{}'::jsonb)
                        || jsonb_build_object('execution_node_id', %s::text),
                    started_at = COALESCE(started_at, NOW()),
                    updated_at = NOW()
                WHERE experiment_id = %s
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'canceled')
                RETURNING experiment_id, status, qe_task_id, qe_loop_id
                """,
                (
                    normalized_task_id,
                    normalized_loop_id,
                    normalized_node_id,
                    normalized_experiment_id,
                ),
            )
            return cur.fetchone()

        def record_waiting(
            cur: Any,
            _active_count: int,
            _node_capacity: int,
        ) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE qe_experiments
                SET status = 'pending',
                    qe_task_id = %s,
                    qe_loop_id = %s,
                    custom_params = COALESCE(custom_params, '{}'::jsonb)
                        || jsonb_build_object('execution_node_id', %s::text),
                    updated_at = NOW()
                WHERE experiment_id = %s
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'canceled')
                RETURNING experiment_id, status, qe_task_id, qe_loop_id
                """,
                (
                    normalized_task_id,
                    normalized_loop_id,
                    normalized_node_id,
                    normalized_experiment_id,
                ),
            )
            return cur.fetchone()

        return claim_source, record_waiting

    @staticmethod
    def multi_alpha_node(
        *,
        experiment_id: str,
        node_id: str,
        qe_loop_id: str,
        group_names: tuple[str, ...],
    ) -> tuple[SourceClaim, CapacityWaitRecorder]:
        normalized_experiment_id = str(experiment_id or "").strip()
        normalized_node_id = str(node_id or "").strip()
        normalized_loop_id = str(qe_loop_id or "").strip()
        normalized_group_names = tuple(
            dict.fromkeys(str(name or "").strip() for name in group_names if str(name or "").strip())
        )
        if not normalized_group_names:
            raise QEWorkspaceSubmissionCoordinatorError(
                "multi-alpha node submission requires at least one group name",
                reason_code="qe_execution_source_identity_invalid",
            )

        def _update(cur: Any, *, status: str) -> Mapping[str, Any] | None:
            cur.execute(
                """
                UPDATE qe_multi_alpha_groups
                SET assigned_node_id = %s,
                    qe_loop_id = %s,
                    status = %s,
                    error_message = NULL
                WHERE parent_experiment_id = %s
                  AND group_name = ANY(%s)
                  AND status NOT IN ('completed', 'failed', 'cancelled', 'canceled')
                RETURNING parent_experiment_id, group_name, status,
                          assigned_node_id, qe_loop_id
                """,
                (
                    normalized_node_id,
                    normalized_loop_id,
                    status,
                    normalized_experiment_id,
                    list(normalized_group_names),
                ),
            )
            rows = cur.fetchall()
            if len(rows) != len(normalized_group_names):
                raise QEWorkspaceSubmissionCoordinatorError(
                    "multi-alpha source claim did not update the complete node group set",
                    reason_code="qe_execution_source_not_claimable",
                    context={
                        "experiment_id": normalized_experiment_id,
                        "node_id": normalized_node_id,
                        "expected_group_names": list(normalized_group_names),
                        "updated_group_count": len(rows),
                    },
                )
            return {
                "parent_experiment_id": normalized_experiment_id,
                "status": status,
                "node_id": normalized_node_id,
                "qe_loop_id": normalized_loop_id,
                "group_names": list(normalized_group_names),
            }

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            return _update(cur, status="running")

        def record_waiting(
            cur: Any,
            _active_count: int,
            _node_capacity: int,
        ) -> Mapping[str, Any] | None:
            return _update(cur, status="pending")

        return claim_source, record_waiting

    @staticmethod
    def pred_backtest_run(
        *,
        run_id: str,
        backtest_name: str,
        node_id: str,
    ) -> tuple[SourceClaim, CapacityWaitRecorder]:
        normalized_run_id = str(run_id or "").strip()
        normalized_backtest_name = str(backtest_name or "").strip()
        normalized_node_id = str(node_id or "").strip()

        def _update(
            cur: Any,
            *,
            phase: str,
            active_count: int | None = None,
            node_capacity: int | None = None,
        ) -> Mapping[str, Any] | None:
            progress = {
                "qe_submission": {
                    "phase": phase,
                    "backtest_name": normalized_backtest_name,
                    "node_id": normalized_node_id,
                    "active_count": active_count,
                    "node_capacity": node_capacity,
                }
            }
            cur.execute(
                """
                UPDATE strategy_pkg.multi_alpha_combine_backtest_run
                SET phase = %s,
                    progress_json = COALESCE(progress_json, '{}'::jsonb) || %s::jsonb,
                    updated_at = NOW()
                WHERE id = %s
                  AND status NOT IN ('succeeded', 'partial_failed', 'failed', 'cancelled')
                RETURNING id, status, phase, progress_json
                """,
                (
                    phase,
                    json.dumps(progress, ensure_ascii=False, sort_keys=True),
                    normalized_run_id,
                ),
            )
            return cur.fetchone()

        def claim_source(cur: Any) -> Mapping[str, Any] | None:
            return _update(cur, phase="submitting")

        def record_waiting(
            cur: Any,
            active_count: int,
            node_capacity: int,
        ) -> Mapping[str, Any] | None:
            return _update(
                cur,
                phase="waiting_capacity",
                active_count=int(active_count),
                node_capacity=int(node_capacity),
            )

        return claim_source, record_waiting


def qe_submission_owner_id() -> str:
    return _PROCESS_SUBMISSION_OWNER_ID


class QEWorkspaceSubmissionCoordinator:
    """Reserve one canonical slot before any production QE Workspace POST."""

    def __init__(
        self,
        *,
        reservation_repository: QEExecutionReservationRepository | None = None,
        capacity_service: QEActiveExecutionCapacityService | None = None,
    ) -> None:
        self._repository = reservation_repository or QEExecutionReservationRepository()
        self._capacity_service = capacity_service or QEActiveExecutionCapacityService()

    async def submit(
        self,
        *,
        client: Any,
        source: QEWorkspaceSubmissionSource,
        payload: QEWorkspaceSubmissionPayload,
    ) -> QEWorkspaceSubmissionOutcome:
        self._validate_payload(payload)
        self._repository.preflight_schema(raise_on_error=True)
        capacity = self._capacity_service.resolve_node_capacity(
            source.node_id,
            source.requested_node_capacity,
        )
        spec = QEExecutionReservationSpec(
            node_id=source.node_id,
            source_kind=source.source_kind,
            source_execution_id=source.source_execution_id,
            qe_task_id=payload.task_id,
            qe_loop_id=payload.loop_id,
            submission_intent_hash=source.submission_intent_hash,
        )
        queue_only_diagnostics = self._capacity_service.queue_only_diagnostics(
            source.node_id
        )
        if queue_only_diagnostics:
            queued = self._repository.record_queue_only_wait_if_unreserved(
                spec,
                node_capacity=capacity,
                record_waiting_capacity=source.record_waiting_capacity,
            )
            if queued is not None:
                return self._capacity_wait_outcome(
                    payload,
                    spec,
                    queued,
                    detail={
                        "reason_code": "qe_capacity_node_queue_only",
                        "diagnostics": [
                            dict(item) for item in queue_only_diagnostics
                        ],
                    },
                )
        acquired = self._repository.reserve_execution_and_claim_source(
            spec,
            node_capacity=capacity,
            owner_id=source.owner_id,
            lease_seconds=source.lease_seconds,
            claim_source=source.claim_source,
            record_waiting_capacity=source.record_waiting_capacity,
        )
        if not acquired.acquired:
            return self._capacity_wait_outcome(payload, spec, acquired)

        reservation = dict(acquired.reservation or {})
        if not reservation:
            raise QEWorkspaceSubmissionCoordinatorError(
                "capacity repository reported an acquired slot without a reservation row",
                reason_code="qe_execution_reservation_missing_after_acquire",
                context={"reservation_id": spec.reservation_id},
            )
        owned_by_caller = str(reservation.get("owner_id") or "") == source.owner_id
        if acquired.duplicate_replay:
            if not owned_by_caller:
                claimed = self._repository.claim_reservation_for_source(
                    source_kind=source.source_kind,
                    source_execution_id=source.source_execution_id,
                    owner_id=source.owner_id,
                    lease_seconds=source.lease_seconds,
                )
                if claimed is not None:
                    reservation = dict(claimed)
                    owned_by_caller = (
                        str(reservation.get("owner_id") or "") == source.owner_id
                    )
            inspection = await self._inspect_or_unknown(
                client=client,
                payload=payload,
                spec=spec,
                acquired=acquired,
                reservation=reservation,
                token=self._token_for(reservation) if owned_by_caller else None,
                trigger="existing_reservation",
            )
            if inspection is not None:
                return inspection
            if not owned_by_caller:
                return self._unknown_outcome(
                    payload=payload,
                    spec=spec,
                    acquired=acquired,
                    reservation=reservation,
                    detail={
                        "reason_code": "qe_execution_reservation_owned_elsewhere",
                        "owner_id": reservation.get("owner_id"),
                    },
                )

        token = self._token_for(reservation)
        reservation = self._repository.transition_execution_reservation(
            spec.reservation_id,
            token=token,
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status="submitting",
            remote_status="post_pending",
        )
        token = self._token_for(reservation)
        return await self._post_with_reconciliation(
            client=client,
            source=source,
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=reservation,
            token=token,
        )

    def record_authoritative_remote_status(
        self,
        *,
        source: QEWorkspaceSubmissionSource,
        outcome: QEWorkspaceSubmissionOutcome,
        remote_status: str,
    ) -> Mapping[str, Any]:
        if not outcome.reservation_id:
            raise QEWorkspaceSubmissionCoordinatorError(
                "cannot record a remote status without an acquired reservation",
                reason_code="qe_execution_reservation_missing",
            )
        return self.record_authoritative_remote_status_for_source(
            source_kind=source.source_kind,
            source_execution_id=source.source_execution_id,
            remote_status=remote_status,
            owner_id=source.owner_id,
            lease_seconds=source.lease_seconds,
            expected_reservation_id=outcome.reservation_id,
        )

    def record_authoritative_remote_status_for_source(
        self,
        *,
        source_kind: str,
        source_execution_id: str,
        remote_status: str,
        owner_id: str | None = None,
        lease_seconds: int = DEFAULT_RESERVATION_LEASE_SECONDS,
        expected_reservation_id: str | None = None,
    ) -> Mapping[str, Any]:
        normalized_owner_id = str(owner_id or qe_submission_owner_id()).strip()
        reservation = self._repository.get_reservation_for_source(
            source_kind=source_kind,
            source_execution_id=source_execution_id,
        )
        if reservation is None:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE execution reservation disappeared before terminal reconciliation",
                reason_code="qe_execution_reservation_missing",
                context={
                    "source_kind": source_kind,
                    "source_execution_id": source_execution_id,
                },
            )
        reservation_id = str(reservation.get("reservation_id") or "")
        if expected_reservation_id and reservation_id != expected_reservation_id:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE execution reservation identity changed before terminal reconciliation",
                reason_code="qe_execution_reservation_identity_conflict",
                context={
                    "expected_reservation_id": expected_reservation_id,
                    "actual_reservation_id": reservation_id,
                },
            )
        next_status, release_reason = self._reservation_state_for_remote(remote_status)
        if str(reservation.get("status") or "") in {"released", "failed", "cancelled"}:
            return reservation
        if str(reservation.get("owner_id") or "") != normalized_owner_id:
            claimed = self._repository.claim_reservation_for_source(
                source_kind=source_kind,
                source_execution_id=source_execution_id,
                owner_id=normalized_owner_id,
                lease_seconds=lease_seconds,
            )
            if claimed is None or str(claimed.get("owner_id") or "") != normalized_owner_id:
                raise QEWorkspaceSubmissionCoordinatorError(
                    "current reconciler could not claim the QE execution reservation",
                    reason_code="qe_execution_reservation_owner_mismatch",
                    context={
                        "reservation_id": reservation_id,
                        "expected_owner_id": normalized_owner_id,
                        "actual_owner_id": reservation.get("owner_id"),
                    },
                )
            reservation = claimed
        return self._repository.transition_execution_reservation(
            reservation_id,
            token=self._token_for(reservation),
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status=next_status,
            remote_status=remote_status,
            release_reason_code=release_reason,
        )

    async def _post_with_reconciliation(
        self,
        *,
        client: Any,
        source: QEWorkspaceSubmissionSource,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        token: QEExecutionReservationToken,
    ) -> QEWorkspaceSubmissionOutcome:
        try:
            receipt = await client.submit_loop(
                payload.task_id,
                payload.loop_index,
                dict(payload.config),
                dict(payload.experiment_files),
                payload.wsl_command,
                model_source=dict(payload.model_source) if payload.model_source else None,
                callback_url=payload.callback_url,
                submission_intent_hash=source.submission_intent_hash,
            )
        except QEWorkspaceSubmissionRejected as exc:
            if exc.status_code < 500:
                self._repository.transition_execution_reservation(
                    spec.reservation_id,
                    token=token,
                    expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
                    next_status="failed",
                    remote_status=f"rejected_http_{exc.status_code}",
                    release_reason_code=exc.reason_code,
                )
                raise
            failure: Exception = exc
        except (QEWorkspaceSubmissionTransportError, QEWorkspaceSubmissionContractError) as exc:
            failure = exc
        else:
            try:
                self._validate_receipt_identity(receipt, spec=spec, payload=payload)
                return self._persist_receipt_outcome(
                    payload=payload,
                    spec=spec,
                    acquired=acquired,
                    reservation=reservation,
                    token=token,
                    receipt=receipt,
                    state="duplicate_replay" if receipt.duplicate_replay else "submitted",
                )
            except Exception as exc:
                logger.error(
                    "QE Workspace accepted task=%s loop=%s but local receipt persistence "
                    "did not complete; preserving the source as reconciling: %s",
                    payload.task_id,
                    payload.loop_id,
                    exc,
                    exc_info=True,
                )
                return self._unknown_outcome(
                    payload=payload,
                    spec=spec,
                    acquired=acquired,
                    reservation=reservation,
                    receipt=receipt,
                    detail={
                        "reason_code": "qe_workspace_accepted_local_persistence_unknown",
                        "persistence_error_code": getattr(
                            exc,
                            "reason_code",
                            type(exc).__name__,
                        ),
                        "message": str(exc),
                    },
                )

        reconciled = await self._inspect_or_unknown(
            client=client,
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=reservation,
            token=token,
            trigger=getattr(failure, "reason_code", type(failure).__name__),
        )
        if reconciled is not None:
            return reconciled
        return self._mark_unknown(
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=reservation,
            token=token,
            detail={
                "reason_code": getattr(
                    failure,
                    "reason_code",
                    "qe_workspace_submission_response_unknown",
                ),
                "message": str(failure),
            },
        )

    async def _inspect_or_unknown(
        self,
        *,
        client: Any,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        token: QEExecutionReservationToken | None,
        trigger: str,
    ) -> QEWorkspaceSubmissionOutcome | None:
        try:
            inspection = await client.inspect_loop_submission(
                payload.task_id,
                payload.loop_id,
                submission_intent_hash=spec.submission_intent_hash,
            )
        except (QEWorkspaceSubmissionTransportError, QEWorkspaceSubmissionRejected):
            return None
        if inspection.status == "not_reserved":
            return None
        self._validate_inspection_identity(inspection, spec=spec, payload=payload)
        if token is None:
            return self._unknown_outcome(
                payload=payload,
                spec=spec,
                acquired=acquired,
                reservation=reservation,
                receipt=inspection,
                detail={
                    "reason_code": "qe_execution_reservation_owned_elsewhere",
                    "inspection_trigger": trigger,
                },
            )
        return self._persist_inspection_outcome(
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=reservation,
            token=token,
            inspection=inspection,
            trigger=trigger,
        )

    def _persist_receipt_outcome(
        self,
        *,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        token: QEExecutionReservationToken,
        receipt: QEWorkspaceSubmissionReceipt,
        state: str,
    ) -> QEWorkspaceSubmissionOutcome:
        next_status, release_reason = self._reservation_state_for_remote(receipt.receipt_status)
        updated = self._repository.transition_execution_reservation(
            spec.reservation_id,
            token=token,
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status=next_status,
            remote_status=receipt.receipt_status,
            release_reason_code=release_reason,
        )
        return self._outcome(
            state=state,
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=updated,
            remote_status=receipt.receipt_status,
            duplicate_replay=receipt.duplicate_replay or acquired.duplicate_replay,
            remote_acceptance_unknown=False,
            receipt=receipt,
        )

    def _persist_inspection_outcome(
        self,
        *,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        token: QEExecutionReservationToken,
        inspection: QEWorkspaceSubmissionInspection,
        trigger: str,
    ) -> QEWorkspaceSubmissionOutcome:
        next_status, release_reason = self._reservation_state_for_remote(inspection.status)
        updated = self._repository.transition_execution_reservation(
            spec.reservation_id,
            token=token,
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status=next_status,
            remote_status=inspection.status,
            release_reason_code=release_reason,
        )
        return self._outcome(
            state="receipt_recovered",
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=updated,
            remote_status=inspection.status,
            duplicate_replay=True,
            remote_acceptance_unknown=False,
            receipt=inspection,
            detail={"inspection_trigger": trigger},
        )

    def _mark_unknown(
        self,
        *,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        token: QEExecutionReservationToken,
        detail: Mapping[str, Any],
    ) -> QEWorkspaceSubmissionOutcome:
        updated = self._repository.transition_execution_reservation(
            spec.reservation_id,
            token=token,
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status="reconciling",
            remote_status="submission_unknown",
        )
        return self._unknown_outcome(
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=updated,
            detail=detail,
        )

    def _capacity_wait_outcome(
        self,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        detail: Mapping[str, Any] | None = None,
    ) -> QEWorkspaceSubmissionOutcome:
        return QEWorkspaceSubmissionOutcome(
            state="waiting_capacity",
            task_id=payload.task_id,
            loop_id=payload.loop_id,
            reservation_id=None,
            reservation_status=None,
            remote_status=None,
            active_count=acquired.active_count,
            node_capacity=acquired.node_capacity,
            duplicate_replay=False,
            remote_acceptance_unknown=False,
            source_claim=None,
            detail={
                "source_kind": spec.source_kind,
                "source_execution_id": spec.source_execution_id,
                "node_id": spec.node_id,
                **dict(detail or {}),
            },
        )

    def _unknown_outcome(
        self,
        *,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        receipt: QEWorkspaceSubmissionReceipt | QEWorkspaceSubmissionInspection | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> QEWorkspaceSubmissionOutcome:
        return self._outcome(
            state="reconciling",
            payload=payload,
            spec=spec,
            acquired=acquired,
            reservation=reservation,
            remote_status=str(reservation.get("remote_status") or "submission_unknown"),
            duplicate_replay=acquired.duplicate_replay,
            remote_acceptance_unknown=True,
            receipt=receipt,
            detail=detail,
        )

    @staticmethod
    def _outcome(
        *,
        state: str,
        payload: QEWorkspaceSubmissionPayload,
        spec: QEExecutionReservationSpec,
        acquired: QEExecutionReservationAcquireResult,
        reservation: Mapping[str, Any],
        remote_status: str | None,
        duplicate_replay: bool,
        remote_acceptance_unknown: bool,
        receipt: QEWorkspaceSubmissionReceipt | QEWorkspaceSubmissionInspection | None = None,
        detail: Mapping[str, Any] | None = None,
    ) -> QEWorkspaceSubmissionOutcome:
        return QEWorkspaceSubmissionOutcome(
            state=state,
            task_id=payload.task_id,
            loop_id=payload.loop_id,
            reservation_id=spec.reservation_id,
            reservation_status=str(reservation.get("status") or "") or None,
            remote_status=remote_status,
            active_count=acquired.active_count,
            node_capacity=acquired.node_capacity,
            duplicate_replay=duplicate_replay,
            remote_acceptance_unknown=remote_acceptance_unknown,
            source_claim=(
                dict(acquired.source_claim) if acquired.source_claim is not None else None
            ),
            receipt=receipt,
            detail=dict(detail or {}),
        )

    @staticmethod
    def _reservation_state_for_remote(remote_status: str) -> tuple[str, str | None]:
        normalized = str(remote_status or "").strip().lower()
        if normalized in {"reserved", "started", "queued", "pending", "submitted"}:
            return "submitting", None
        if normalized in {"running", "processing"}:
            return "running", None
        if normalized == "completed":
            return "released", "qe_workspace_remote_completed"
        if normalized == "failed":
            return "failed", "qe_workspace_remote_failed"
        if normalized in {"cancelled", "canceled", "interrupted"}:
            return "cancelled", "qe_workspace_remote_cancelled"
        raise QEWorkspaceSubmissionCoordinatorError(
            "QE Workspace returned an unmapped submission status",
            reason_code="qe_workspace_submission_status_unmapped",
            context={"remote_status": remote_status},
        )

    @staticmethod
    def _token_for(reservation: Mapping[str, Any]) -> QEExecutionReservationToken:
        return QEExecutionReservationToken(
            owner_id=str(reservation.get("owner_id") or ""),
            fencing_token=int(reservation.get("fencing_token") or 0),
            row_version=int(reservation.get("row_version") or 0),
        )

    @staticmethod
    def _validate_payload(payload: QEWorkspaceSubmissionPayload) -> None:
        if not str(payload.task_id or "").strip():
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace task identity must not be empty",
                reason_code="qe_workspace_submission_identity_invalid",
            )
        if isinstance(payload.loop_index, bool) or int(payload.loop_index) < 1:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace loop index must be positive",
                reason_code="qe_workspace_submission_identity_invalid",
            )

    @classmethod
    def _validate_receipt_identity(
        cls,
        receipt: QEWorkspaceSubmissionReceipt,
        *,
        spec: QEExecutionReservationSpec,
        payload: QEWorkspaceSubmissionPayload,
    ) -> None:
        if (
            receipt.task_id != payload.task_id
            or receipt.loop_id != payload.loop_id
            or receipt.submission_intent_hash != spec.submission_intent_hash
        ):
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace receipt does not match the reserved execution identity",
                reason_code="qe_workspace_submission_identity_mismatch",
            )
        expected_digest = canonical_qe_workspace_request_digest(payload)
        if receipt.request_digest != expected_digest:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace receipt request digest does not match the submitted payload",
                reason_code="qe_workspace_submission_request_digest_mismatch",
                context={
                    "expected_request_digest": expected_digest,
                    "actual_request_digest": receipt.request_digest,
                },
            )

    @classmethod
    def _validate_inspection_identity(
        cls,
        inspection: QEWorkspaceSubmissionInspection,
        *,
        spec: QEExecutionReservationSpec,
        payload: QEWorkspaceSubmissionPayload,
    ) -> None:
        if (
            inspection.task_id != payload.task_id
            or inspection.loop_id != payload.loop_id
            or inspection.submission_intent_hash != spec.submission_intent_hash
        ):
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace inspected receipt does not match the reservation identity",
                reason_code="qe_workspace_submission_identity_mismatch",
            )
        expected_digest = canonical_qe_workspace_request_digest(payload)
        if inspection.request_digest != expected_digest:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE Workspace inspected receipt request digest does not match the payload",
                reason_code="qe_workspace_submission_request_digest_mismatch",
                context={
                    "expected_request_digest": expected_digest,
                    "actual_request_digest": inspection.request_digest,
                },
            )


@dataclass(frozen=True)
class QEExecutionReservationReconcileResult:
    checked: int = 0
    transitioned: int = 0
    terminal_released: int = 0
    owned_elsewhere: int = 0
    not_reserved: int = 0
    errors: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "checked": self.checked,
            "transitioned": self.transitioned,
            "terminal_released": self.terminal_released,
            "owned_elsewhere": self.owned_elsewhere,
            "not_reserved": self.not_reserved,
            "errors": self.errors,
        }


class QEExecutionReservationReconciler:
    """Converge active reservations from the authoritative QE receipt ledger."""

    def __init__(
        self,
        *,
        repository: QEExecutionReservationRepository | None = None,
        owner_id: str | None = None,
        lease_seconds: int = DEFAULT_RESERVATION_LEASE_SECONDS,
        post_grace_seconds: int = 15,
    ) -> None:
        self._repository = repository or QEExecutionReservationRepository()
        self._owner_id = str(owner_id or qe_submission_owner_id()).strip()
        self._lease_seconds = max(1, int(lease_seconds))
        self._post_grace_seconds = max(1, int(post_grace_seconds))

    async def scan_once(self) -> dict[str, int]:
        self._repository.preflight_schema(raise_on_error=True)
        counters = {
            "checked": 0,
            "transitioned": 0,
            "terminal_released": 0,
            "owned_elsewhere": 0,
            "not_reserved": 0,
            "errors": 0,
        }
        for reservation in self._repository.list_active_reservations():
            counters["checked"] += 1
            try:
                state = await self._reconcile_one(reservation)
                if state in counters:
                    counters[state] += 1
            except Exception as exc:
                counters["errors"] += 1
                logger.warning(
                    "QE reservation reconciliation failed: reservation=%s source=%s/%s "
                    "remote=%s/%s error=%s",
                    reservation.get("reservation_id"),
                    reservation.get("source_kind"),
                    reservation.get("source_execution_id"),
                    reservation.get("qe_task_id"),
                    reservation.get("qe_loop_id"),
                    exc,
                    exc_info=True,
                )
        return counters

    async def _reconcile_one(self, reservation: Mapping[str, Any]) -> str | None:
        owned = self._claim_or_reuse(reservation)
        if owned is None:
            return "owned_elsewhere"

        node_id = str(owned.get("node_id") or "").strip()
        task_id = str(owned.get("qe_task_id") or "").strip()
        loop_id = str(owned.get("qe_loop_id") or "").strip()
        client = QEWorkspaceClient.for_node(node_id)
        async with client:
            inspection = await client.inspect_loop_submission(
                task_id,
                loop_id,
                submission_intent_hash=str(owned.get("submission_intent_hash") or ""),
            )

        if inspection.status == "not_reserved":
            updated = self._transition_if_needed(
                owned,
                next_status="reconciling",
                remote_status="not_reserved",
                release_reason_code=None,
            )
            return "transitioned" if updated else "not_reserved"

        expected_intent = str(owned.get("submission_intent_hash") or "")
        if inspection.submission_intent_hash != expected_intent:
            raise QEWorkspaceSubmissionCoordinatorError(
                "QE receipt intent does not match the active reservation",
                reason_code="qe_workspace_submission_identity_mismatch",
                context={
                    "reservation_id": owned.get("reservation_id"),
                    "expected_submission_intent_hash": expected_intent,
                    "actual_submission_intent_hash": inspection.submission_intent_hash,
                },
            )

        next_status, release_reason = QEWorkspaceSubmissionCoordinator._reservation_state_for_remote(
            inspection.status
        )
        updated = self._transition_if_needed(
            owned,
            next_status=next_status,
            remote_status=inspection.status,
            release_reason_code=release_reason,
        )
        if not updated:
            return None
        if next_status in {"released", "failed", "cancelled"}:
            return "terminal_released"
        return "transitioned"

    def _claim_or_reuse(
        self,
        reservation: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        current_owner = str(reservation.get("owner_id") or "")
        if current_owner == self._owner_id:
            if self._post_is_inside_grace_period(reservation):
                return reservation
            try:
                return self._repository.heartbeat_execution_reservation(
                    str(reservation["reservation_id"]),
                    token=QEWorkspaceSubmissionCoordinator._token_for(reservation),
                    lease_seconds=self._lease_seconds,
                )
            except QEExecutionReservationError as exc:
                if exc.reason_code not in {
                    "qe_execution_reservation_stale_owner",
                    "qe_execution_reservation_stale_row_version",
                    "qe_execution_reservation_lease_expired",
                    "qe_execution_reservation_cas_failed",
                }:
                    raise
        return self._repository.claim_reservation_for_source(
            source_kind=str(reservation.get("source_kind") or ""),
            source_execution_id=str(reservation.get("source_execution_id") or ""),
            owner_id=self._owner_id,
            lease_seconds=self._lease_seconds,
        )

    def _transition_if_needed(
        self,
        reservation: Mapping[str, Any],
        *,
        next_status: str,
        remote_status: str,
        release_reason_code: str | None,
    ) -> bool:
        if (
            str(reservation.get("status") or "") == next_status
            and str(reservation.get("remote_status") or "") == remote_status
        ):
            return False
        self._repository.transition_execution_reservation(
            str(reservation["reservation_id"]),
            token=QEWorkspaceSubmissionCoordinator._token_for(reservation),
            expected_statuses=tuple(ACTIVE_RESERVATION_STATUSES),
            next_status=next_status,
            remote_status=remote_status,
            release_reason_code=release_reason_code,
        )
        return True

    def _post_is_inside_grace_period(self, reservation: Mapping[str, Any]) -> bool:
        if str(reservation.get("remote_status") or "") != "post_pending":
            return False
        updated_at = reservation.get("updated_at")
        if not isinstance(updated_at, datetime):
            return False
        normalized = updated_at if updated_at.tzinfo else updated_at.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - normalized).total_seconds() < self._post_grace_seconds


def canonical_qe_workspace_request_digest(payload: QEWorkspaceSubmissionPayload) -> str:
    file_hashes = {
        str(path): hashlib.sha256(str(content).encode("utf-8")).hexdigest()
        for path, content in sorted(payload.experiment_files.items())
    }
    identity = normalize_json(
        {
            "loop_index": int(payload.loop_index),
            "config": dict(payload.config),
            "experiment_files_sha256": file_hashes,
            "wsl_command": payload.wsl_command or "",
            "model_source": dict(payload.model_source or {}),
        }
    )
    encoded = json.dumps(
        identity,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def submission_intent_hash_for_source(
    *,
    source_kind: str,
    source_execution_id: str,
    node_id: str,
    task_id: str,
    loop_id: str,
) -> str:
    identity = {
        "source_kind": str(source_kind),
        "source_execution_id": str(source_execution_id),
        "node_id": str(node_id),
        "task_id": str(task_id),
        "loop_id": str(loop_id),
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
