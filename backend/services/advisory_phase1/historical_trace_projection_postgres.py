"""One-snapshot fixed-SQL PostgreSQL projector for Phase 1G G2."""

from __future__ import annotations

from contextlib import contextmanager
import logging
from typing import Any, Callable, Iterator

import psycopg2.extras

from backend.services.advisory_phase0a.policy import canonical_json_sha256
from backend.services.advisory_phase1.phase1g_contract import (
    Phase1GTargetExecutionRequest,
)
from backend.services.advisory_phase1.phase1g_historical_trace_contract import (
    Phase1GHistoricalTraceError,
    Phase1GTargetProjectionSnapshot,
    REASON_ARTIFACT_NOT_FOUND,
    REASON_DSE_NOT_FOUND,
    REASON_PACKAGE_NOT_FOUND,
    build_phase1g_historical_trace_projection,
    build_phase1g_target_projection_snapshot,
    project_phase1g_artifact,
    project_phase1g_dse,
    project_phase1g_manifest,
)
from backend.services.advisory_phase1.phase1g_phase1e_projection import (
    Phase1EExecutionPlanProjection,
)
from backend.services.advisory_phase1.phase1g_source_replay import (
    Phase1GSourceReplayError,
    REASON_G2_UNEXPECTED_ERROR,
    parse_phase1g_source_operation,
    phase1e_plan_program_id,
    replay_phase1g_source_operation,
)
from backend.services.advisory_phase1.phase1g_source_replay_postgres import (
    Phase1GSourceReplayPostgresReader,
)


logger = logging.getLogger(__name__)
ConnFactory = Callable[[], Iterator[Any]]


TRANSACTION_STATE_SELECT_SQL = """
    SELECT current_setting('transaction_read_only') AS transaction_read_only,
           current_setting('transaction_isolation') AS transaction_isolation
"""
DSE_SELECT_EXACT_SQL = """
    SELECT evidence_id, target_trade_date, cutoff_date, package_id, manifest_sha256,
           runtime_profile_version_id, runtime_profile_hash, source_type, data_source,
           candidate_count, excluded_count, artifact_hash, evidence_payload_json, created_at
    FROM selection.daily_selection_evidence
    WHERE evidence_id = %s
"""
ARTIFACT_SELECT_EXACT_SQL = """
    SELECT artifact_id, package_id, manifest_sha256, trade_date, data_source,
           runtime_config_hash, scores_json, artifact_sha256, score_count,
           universe_count, top_score_symbol, status, metadata,
           artifact_contract_version, artifact_payload_sha256,
           artifact_input_context_hash, source_revision_set_hash,
           asset_closure_hash, created_at
    FROM strategy_pkg.selection_score_artifact
    WHERE artifact_id = %s
"""
PACKAGE_SELECT_EXACT_SQL = """
    SELECT package_id, manifest_json, manifest_sha256, alpha_mode
    FROM strategy_pkg.package
    WHERE package_id = %s AND manifest_sha256 = %s
"""
BINDING_SELECT_EXACT_SQL = """
    SELECT binding_version_id, program_id, package_mode, package_ids,
           runtime_config_json, effective_from_trade_date, effective_to_trade_date,
           activation_status, binding_payload_json
    FROM app.advisory_strategy_binding_version
    WHERE binding_version_id = %s AND program_id = %s
"""

PHASE1G_G2_HISTORICAL_SQL_REGISTRY = {
    "transaction_state": TRANSACTION_STATE_SELECT_SQL,
    "dse_exact": DSE_SELECT_EXACT_SQL,
    "artifact_exact": ARTIFACT_SELECT_EXACT_SQL,
    "package_exact": PACKAGE_SELECT_EXACT_SQL,
    "binding_exact": BINDING_SELECT_EXACT_SQL,
}
PHASE1G_G2_HISTORICAL_SQL_REGISTRY_HASH = canonical_json_sha256(
    PHASE1G_G2_HISTORICAL_SQL_REGISTRY
)


class Phase1GPostgresReadOnlyError(Phase1GSourceReplayError):
    """The injected PostgreSQL transaction is not a safe G2 snapshot."""


class Phase1GPostgresHistoricalProjection:
    """Open one caller-selected database snapshot; never resolves a connection itself."""

    def __init__(self, conn_factory: ConnFactory) -> None:
        self._conn_factory = conn_factory

    @contextmanager
    def snapshot(self) -> Iterator["Phase1GPostgresHistoricalSnapshot"]:
        with self._conn_factory() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                cur.execute(TRANSACTION_STATE_SELECT_SQL)
                state = cur.fetchone()
                if state is None or str(state["transaction_read_only"]).lower() not in {
                    "on",
                    "true",
                    "1",
                }:
                    raise Phase1GPostgresReadOnlyError(
                        REASON_G2_UNEXPECTED_ERROR,
                        "PostgreSQL did not confirm transaction_read_only=on",
                    )
                if str(state["transaction_isolation"]).lower() != "repeatable read":
                    raise Phase1GPostgresReadOnlyError(
                        REASON_G2_UNEXPECTED_ERROR,
                        "PostgreSQL did not confirm transaction_isolation=repeatable read",
                    )
                yield Phase1GPostgresHistoricalSnapshot(cur)


class Phase1GPostgresHistoricalSnapshot:
    """Exact readers sharing one injected repeatable-read read-only cursor."""

    def __init__(self, cursor: Any) -> None:
        self._cursor = cursor
        self.source_events = Phase1GSourceReplayPostgresReader(cursor)

    def _one(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any] | None:
        self._cursor.execute(sql, params)
        row = self._cursor.fetchone()
        return dict(row) if row is not None else None

    def dse(self, evidence_id: str) -> dict[str, Any]:
        row = self._one(DSE_SELECT_EXACT_SQL, (evidence_id,))
        if row is None:
            raise Phase1GHistoricalTraceError(
                REASON_DSE_NOT_FOUND,
                "exact daily selection evidence row is missing",
                context={"evidence_id": evidence_id},
            )
        return row

    def artifact(self, artifact_id: str) -> dict[str, Any]:
        row = self._one(ARTIFACT_SELECT_EXACT_SQL, (artifact_id,))
        if row is None:
            raise Phase1GHistoricalTraceError(
                REASON_ARTIFACT_NOT_FOUND,
                "exact selection artifact row is missing",
                context={"artifact_id": artifact_id},
            )
        return row

    def package(self, package_id: str, manifest_sha256: str) -> dict[str, Any]:
        row = self._one(PACKAGE_SELECT_EXACT_SQL, (package_id, manifest_sha256))
        if row is None:
            raise Phase1GHistoricalTraceError(
                REASON_PACKAGE_NOT_FOUND,
                "exact package manifest row is missing",
                context={"package_id": package_id, "manifest_sha256": manifest_sha256},
            )
        return row

    def binding(self, binding_version_id: str, program_id: str) -> dict[str, Any]:
        row = self._one(BINDING_SELECT_EXACT_SQL, (binding_version_id, program_id))
        if row is None:
            raise Phase1GHistoricalTraceError(
                REASON_G2_UNEXPECTED_ERROR,
                "exact advisory binding row is missing",
                context={
                    "binding_version_id": binding_version_id,
                    "program_id": program_id,
                },
            )
        return row


def project_phase1g_target_snapshot(
    *,
    conn_factory: ConnFactory,
    phase1e_plan: Phase1EExecutionPlanProjection,
    target_request: Phase1GTargetExecutionRequest,
) -> Phase1GTargetProjectionSnapshot:
    """Project one target atomically from one read-only PostgreSQL snapshot."""

    source_operation = parse_phase1g_source_operation(
        phase1e_plan=phase1e_plan,
        target_request=target_request,
    )
    try:
        with Phase1GPostgresHistoricalProjection(conn_factory).snapshot() as snapshot:
            events = snapshot.source_events.load_events(
                source_operation.requirement_set
            )
            source_replay = replay_phase1g_source_operation(
                projection=source_operation,
                availability_events=events,
            )
            dse = project_phase1g_dse(
                snapshot.dse(phase1e_plan.evidence_binding.selection_evidence_id)
            )
            artifact_id = str(
                dse.evidence.phase0a_candidate_lineage["selection_score_artifact_id"]
            )
            artifact = project_phase1g_artifact(snapshot.artifact(artifact_id))
            package_manifest = project_phase1g_manifest(
                snapshot.package(dse.package_id, dse.manifest_sha256)
            )
            binding_row = snapshot.binding(
                phase1e_plan.evidence_binding.binding_version_id,
                phase1e_plan_program_id(phase1e_plan),
            )
            historical_trace = build_phase1g_historical_trace_projection(
                phase1e_plan=phase1e_plan,
                source_operation=source_operation,
                source_replay=source_replay,
                dse=dse,
                artifact=artifact,
                package_manifest=package_manifest,
                binding_row=binding_row,
            )
            return build_phase1g_target_projection_snapshot(
                source_operation=source_operation,
                source_replay=source_replay,
                historical_trace=historical_trace,
            )
    except (Phase1GSourceReplayError, Phase1GHistoricalTraceError):
        raise
    except Exception as exc:
        redacted = RuntimeError("redacted unexpected PostgreSQL projection failure")
        redacted = redacted.with_traceback(exc.__traceback__)
        logger.error(
            "Phase 1G G2 target projection failed unexpectedly plan_id=%s target_hash=%s exception_type=%s",
            phase1e_plan.plan_id,
            target_request.request_hash,
            type(exc).__name__,
            exc_info=(type(redacted), redacted, redacted.__traceback__),
        )
        raise Phase1GSourceReplayError(
            REASON_G2_UNEXPECTED_ERROR,
            "unexpected PostgreSQL projection failure",
            context={
                "phase1e_plan_id": phase1e_plan.plan_id,
                "target_request_hash": target_request.request_hash,
                "exception_type": type(exc).__name__,
            },
        ) from exc
