"""Versioned R5 request factories derived from frozen R1-R4 facts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
import hashlib
from pathlib import Path
import subprocess
from typing import Any

import psycopg2.extras

from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.label_policy import TradingCalendar
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
)

from .api_models import (
    HistoricalRangeBuildBridgeRequest,
    HistoricalRangeRefreshOutcomesRequest,
)
from .artifact_store import HistoricalRangeArtifactStore
from .canonical import canonical_json_sha256
from .dataset_bridge import _eligible_executable_results
from .models import (
    HistoricalRangeArtifactRefV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSummaryPolicyV1,
    derive_prefixed_id,
)
from .outcome_policy_catalog import (
    HistoricalRangeOutcomePolicyResolutionV1,
    freeze_historical_range_outcome_policy,
)
from .outcome_policy_provider import ArtifactHistoricalRangeOutcomePolicyProvider
from .query_repository import PostgresHistoricalRangeQueryRepository
from .repository import PostgresHistoricalRangeRepository


R5_OUTCOME_CONTRACT_VERSION = "advisory_phase1r_r4_outcome_v1"
R5_QUERY_REGISTRY_VERSION = "advisory_hr_r4_query_registry_v1"
R5_PARTITION_POLICY_ID = "ADVISORY_PHASE1R_RETROSPECTIVE_RANGE_PARTITION_V1"

_OUTCOME_SOURCE_FILES = (
    "backend/services/advisory_historical_range/outcome_evaluator.py",
    "backend/services/advisory_historical_range/outcome_policy_catalog.py",
    "backend/services/advisory_historical_range/outcome_policy_provider.py",
    "backend/services/advisory_historical_range/outcome_projection.py",
    "backend/services/advisory_historical_range/outcome_source.py",
    "backend/services/advisory_historical_range/retrospective_projection.py",
    "backend/services/advisory_phase1/observation_capture.py",
    "backend/services/advisory_phase1/outcome_engine.py",
)
_BRIDGE_BUILDER_SOURCE_FILES = (
    "backend/services/advisory_historical_range/dataset_bridge.py",
    "backend/services/advisory_historical_range/dataset_bridge_postgres.py",
    "backend/services/advisory_historical_range/retrospective_projection.py",
    "backend/services/advisory_phase1/capture_foundation.py",
    "backend/services/advisory_phase1/observation_capture.py",
    "backend/services/advisory_phase1/observation_capture_postgres.py",
    "backend/services/advisory_phase1/label_capture.py",
    "backend/services/advisory_phase1/label_builder_postgres.py",
    "backend/services/advisory_phase1/retrospective_selector.py",
    "backend/services/advisory_phase1/retrospective_selector_postgres.py",
    "backend/services/advisory_phase1/dataset_build.py",
    "backend/services/advisory_phase1/dataset_build_postgres.py",
)
_BRIDGE_WRITER_SOURCE_FILES = (
    "backend/services/advisory_phase1/dataset_store.py",
    "backend/services/advisory_phase1/snapshot_writer.py",
)


@dataclass(frozen=True)
class HistoricalRangeOutcomeCommandPlan:
    request_hash: str
    requests: tuple[HistoricalRangeOutcomeRefreshRequestV1, ...]


@dataclass(frozen=True)
class HistoricalRangeR5DerivedIdentities:
    outcome_producer_hash: str
    summary_producer_hash: str
    bridge_builder_hash: str
    bridge_writer_hash: str
    code_commit: str

    @classmethod
    def from_repository(cls, repository_root: Path) -> "HistoricalRangeR5DerivedIdentities":
        return cls(
            outcome_producer_hash=_code_set_hash(repository_root, _OUTCOME_SOURCE_FILES),
            summary_producer_hash=_code_set_hash(
                repository_root,
                ("backend/services/advisory_historical_range/summary_service.py",),
            ),
            bridge_builder_hash=_code_set_hash(repository_root, _BRIDGE_BUILDER_SOURCE_FILES),
            bridge_writer_hash=_code_set_hash(repository_root, _BRIDGE_WRITER_SOURCE_FILES),
            code_commit=_git_commit(repository_root),
        )


class HistoricalRangeR5PolicyRegistry:
    """Freeze and register exact per-Program R4 policy bundles on demand."""

    def __init__(
        self,
        *,
        conn_factory: Callable[[], Any],
        repository: PostgresHistoricalRangeRepository,
        query: PostgresHistoricalRangeQueryRepository,
        artifact_store: HistoricalRangeArtifactStore,
        component_root: Path,
        provider: ArtifactHistoricalRangeOutcomePolicyProvider,
    ) -> None:
        self._conn_factory = conn_factory
        self._repository = repository
        self._query = query
        self._artifact_store = artifact_store
        self._component_root = component_root
        self._provider = provider

    def resolve(
        self, *, batch_id: str, requested_run_ids: Sequence[str]
    ) -> tuple[tuple[str, HistoricalRangeOutcomePolicyResolutionV1], ...]:
        batch = self._query.get_batch(batch_id)
        ref = HistoricalRangeArtifactRefV1.model_validate(batch.get("requirement_plan_ref"))
        plan = HistoricalRangeSourceRequirementPlanV1.model_validate(
            self._artifact_store.load_planning(ref).payload
        )
        run_by_program = {
            run.research_program_id: run
            for run in self._repository.list_all_execution_runs(batch_id=batch_id)
        }
        selected = tuple(sorted(set(requested_run_ids))) or tuple(
            sorted(run.range_run_id for run in run_by_program.values())
        )
        by_run = {run.range_run_id: run for run in run_by_program.values()}
        missing = sorted(set(selected) - set(by_run))
        if missing:
            raise ValueError(f"range_run_ids do not belong to batch: {missing}")
        program_by_id = {item.research_program_id: item for item in plan.frozen_programs}
        calendar = _calendar(
            conn_factory=self._conn_factory,
            start_trade_date=plan.request.start_trade_date,
            end_trade_date=plan.request.end_trade_date,
        )
        resolved: list[tuple[str, HistoricalRangeOutcomePolicyResolutionV1]] = []
        for run_id in selected:
            run = by_run[run_id]
            program = program_by_id[run.research_program_id]
            if program.style_profile_ref is not None:
                raise ValueError(
                    "frozen style profile requires an explicit versioned profile loader"
                )
            resolution = freeze_historical_range_outcome_policy(
                frozen_program=program,
                calendar=calendar,
                artifact_store=self._artifact_store,
                component_root=self._component_root,
                resolved_request_hash=str(batch["request_payload_sha256"]),
            )
            self._provider.register(
                str(resolution.bundle.policy_bundle_hash), resolution.bundle_ref
            )
            resolved.append((run_id, resolution))
        return tuple(resolved)


class HistoricalRangeR5OutcomeRequestFactory:
    def __init__(
        self,
        *,
        policy_registry: HistoricalRangeR5PolicyRegistry,
        identities: HistoricalRangeR5DerivedIdentities,
    ) -> None:
        self._policy_registry = policy_registry
        self._identities = identities

    def build(
        self, batch_id: str, request: HistoricalRangeRefreshOutcomesRequest
    ) -> HistoricalRangeOutcomeCommandPlan:
        resolutions = self._policy_registry.resolve(
            batch_id=batch_id,
            requested_run_ids=request.range_run_ids,
        )
        requests = []
        for run_id, resolution in resolutions:
            unsupported = sorted(set(request.horizons) - set(resolution.bundle.horizons))
            if unsupported:
                raise ValueError(
                    f"requested horizons are not present in the run policy catalog: {unsupported}"
                )
            requests.append(
                HistoricalRangeOutcomeRefreshRequestV1(
                    batch_id=batch_id,
                    range_run_ids=(run_id,),
                    label_as_of_trade_date=request.label_as_of_trade_date,
                    policy_bundle_ref=resolution.bundle_ref,
                    policy_bundle_hash=str(resolution.bundle.policy_bundle_hash),
                    requested_subject_types=tuple(
                        sorted(HistoricalRangeOutcomeSubjectType, key=lambda item: item.value)
                    ),
                    requested_projections=tuple(
                        sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)
                    ),
                    horizons=tuple(request.horizons),
                    producer_code_hash=self._identities.outcome_producer_hash,
                    outcome_contract_version=R5_OUTCOME_CONTRACT_VERSION,
                    operation_idempotency_key=derive_prefixed_id(
                        "ahroutkey",
                        {
                            "parent_key": request.operation_idempotency_key,
                            "range_run_id": run_id,
                            "policy_bundle_hash": resolution.bundle.policy_bundle_hash,
                        },
                    ),
                    expected_batch_row_version=request.expected_row_version,
                    max_items_per_slice=500,
                    max_parallel_runs=1,
                    lease_seconds=900,
                )
            )
        ordered = tuple(sorted(requests, key=lambda item: item.range_run_ids))
        return HistoricalRangeOutcomeCommandPlan(
            request_hash=canonical_json_sha256(
                {
                    "schema_version": "advisory_historical_range_r5_outcome_command_v1",
                    "batch_id": batch_id,
                    "operation_idempotency_key": request.operation_idempotency_key,
                    "expected_batch_row_version": request.expected_row_version,
                    "subrequest_hashes": [item.request_hash for item in ordered],
                }
            ),
            requests=ordered,
        )

    def summary_policy(
        self, request: HistoricalRangeOutcomeRefreshRequestV1
    ) -> HistoricalRangeSummaryPolicyV1:
        return HistoricalRangeSummaryPolicyV1(
            subject_types=tuple(
                sorted(HistoricalRangeOutcomeSubjectType, key=lambda item: item.value)
            ),
            projection_groups=tuple(
                sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)
            ),
            evaluation_window_types=tuple(
                sorted(HistoricalRangeEvaluationWindowType, key=lambda item: item.value)
            ),
            horizons=(0, *request.horizons),
            outcome_policy_bundle_hash=request.policy_bundle_hash,
        )


class HistoricalRangeR5BridgeRequestFactory:
    def __init__(
        self,
        *,
        conn_factory: Callable[[], Any],
        policy_registry: HistoricalRangeR5PolicyRegistry,
        artifact_store: HistoricalRangeArtifactStore,
        identities: HistoricalRangeR5DerivedIdentities,
    ) -> None:
        self._conn_factory = conn_factory
        self._policy_registry = policy_registry
        self._artifact_store = artifact_store
        self._identities = identities

    def build(
        self, batch_id: str, request: HistoricalRangeBuildBridgeRequest
    ) -> HistoricalRangeDatasetBridgeRequestV1:
        if "CENSORED" in request.requested_maturity_statuses:
            raise ValueError("CENSORED outcomes are not label-eligible for the R4 dataset bridge")
        resolutions = self._policy_registry.resolve(
            batch_id=batch_id,
            requested_run_ids=request.range_run_ids,
        )
        successful_day_refs: list[HistoricalRangeArtifactRefV1] = []
        candidate_refs: list[HistoricalRangeArtifactRefV1] = []
        outcome_refs: list[HistoricalRangeArtifactRefV1] = []
        summary_refs: list[HistoricalRangeArtifactRefV1] = []
        policy_refs: list[HistoricalRangeArtifactRefV1] = []
        policy_components: dict[str, dict[str, str]] = {}
        for run_id, resolution in resolutions:
            unsupported = sorted(set(request.requested_horizons) - set(resolution.bundle.horizons))
            if unsupported:
                raise ValueError(
                    f"requested horizons are not present in the run policy catalog: {unsupported}"
                )
            refs = _bridge_refs(
                conn_factory=self._conn_factory,
                range_run_id=run_id,
                policy_hash=str(resolution.bundle.policy_bundle_hash),
                horizons=request.requested_horizons,
                maturity_statuses=request.requested_maturity_statuses,
            )
            successful_day_refs.extend(refs["successful_day_refs"])
            candidate_refs.extend(refs["candidate_refs"])
            outcome_refs.extend(refs["outcome_refs"])
            summary_refs.extend(refs["summary_refs"])
            policy_refs.append(resolution.bundle_ref)
            policy_components[str(resolution.bundle.policy_bundle_hash)] = {
                item.component_role: item.component_hash
                for item in resolution.bundle.components
            }
        run_ids = tuple(run_id for run_id, _resolution in resolutions)
        return HistoricalRangeDatasetBridgeRequestV1(
            batch_id=batch_id,
            range_run_ids=run_ids,
            successful_day_refs=_sorted_refs(successful_day_refs),
            candidate_refs=_sorted_refs(candidate_refs),
            outcome_refs=_sorted_refs(outcome_refs),
            summary_refs=_sorted_refs(summary_refs),
            requested_horizons=tuple(request.requested_horizons),
            requested_maturity_statuses=tuple(
                HistoricalRangeOutcomeStatus(item)
                for item in request.requested_maturity_statuses
            ),
            policy_bundle_refs=_sorted_refs(policy_refs),
            policy_component_hashes=policy_components,
            canonical_signal_dedup_policy_hash=canonical_json_sha256(
                {"policy_id": "CANONICAL_SIGNAL_EXACT_STAGE_AND_ECONOMIC_RESULT_V1"}
            ),
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            dataset_schema_hash=canonical_json_sha256(
                {"schema_version": "advisory_phase1_retrospective_dataset_v1"}
            ),
            builder_hash=self._identities.bridge_builder_hash,
            writer_hash=self._identities.bridge_writer_hash,
            partition_policy_hash=canonical_json_sha256(
                {"partition_policy_id": R5_PARTITION_POLICY_ID}
            ),
            compression_config_hash=canonical_json_sha256(
                {"codec": "zstd", "level": 3}
            ),
            artifact_root_identity_hash=self._artifact_store.root_identity_hash,
            operation_idempotency_key=request.operation_idempotency_key,
            expected_batch_row_version=request.expected_row_version,
            lease_seconds=900,
        )


def historical_range_store_identity() -> dict[str, str]:
    return {
        "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
    }


def _calendar(
    *, conn_factory: Callable[[], Any], start_trade_date: date, end_trade_date: date
) -> TradingCalendar:
    with conn_factory() as conn:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date FROM market.trading_calendar
                WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
                ORDER BY cal_date
                """,
                (start_trade_date, end_trade_date + timedelta(days=400)),
            )
            dates = tuple(row[0] for row in cur.fetchall())
        conn.rollback()
    if not dates or start_trade_date not in dates:
        raise ValueError("R4 calendar slice does not cover the frozen historical range")
    identity = canonical_json_sha256([item.isoformat() for item in dates])
    return TradingCalendar(
        calendar_version=f"market.trading_calendar:{identity}", trading_dates=dates
    )


def _bridge_refs(
    *,
    conn_factory: Callable[[], Any],
    range_run_id: str,
    policy_hash: str,
    horizons: Sequence[int],
    maturity_statuses: Sequence[str],
) -> Mapping[str, tuple[HistoricalRangeArtifactRefV1, ...]]:
    requested_maturity_statuses = tuple(
        HistoricalRangeOutcomeStatus(value) for value in maturity_statuses
    )
    with conn_factory() as conn:
        conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT day.day_receipt_ref, day.candidate_artifact_ref
                FROM app.advisory_historical_range_day_run day
                WHERE day.range_run_id = %s
                  AND day.status IN ('COMPLETE', 'VALID_NO_CANDIDATE')
                ORDER BY day.day_receipt_ref, day.candidate_artifact_ref
                """,
                (range_run_id,),
            )
            day_rows = tuple(dict(row) for row in cur.fetchall())
            cur.execute(
                """
                SELECT outcome.outcome_artifact_ref, outcome.outcome_json
                FROM app.advisory_historical_range_outcome outcome
                JOIN app.advisory_historical_range_candidate candidate
                  ON candidate.candidate_id = outcome.subject_id
                JOIN app.advisory_historical_range_day_run day
                  ON day.day_run_id = candidate.day_run_id
                WHERE day.range_run_id = %s
                  AND outcome.subject_type = 'CANDIDATE'
                  AND outcome.projection = 'EXECUTABLE'
                  AND outcome.evaluation_window_type = 'FIXED_HORIZON'
                  AND outcome.horizon_trade_days = ANY(%s)
                  AND outcome.historical_range_policy_bundle_hash = %s
                  AND outcome.outcome_version = (
                      SELECT MAX(newer.outcome_version)
                      FROM app.advisory_historical_range_outcome newer
                      WHERE newer.outcome_logical_id = outcome.outcome_logical_id
                  )
                ORDER BY outcome.outcome_artifact_ref
                """,
                (range_run_id, list(horizons), policy_hash),
            )
            outcome_rows = tuple(
                row
                for row in (dict(item) for item in cur.fetchall())
                if _eligible_executable_results(
                    row["outcome_json"],
                    requested_maturity_statuses=requested_maturity_statuses,
                )
            )
            cur.execute(
                """
                SELECT summary.summary_artifact_ref
                FROM app.advisory_historical_range_summary summary
                WHERE summary.range_run_id = %s
                ORDER BY summary.summary_version DESC, summary.summary_id DESC
                """,
                (range_run_id,),
            )
            summary_rows = tuple(dict(row) for row in cur.fetchall())
        conn.rollback()
    return {
        "successful_day_refs": _sorted_refs(
            HistoricalRangeArtifactRefV1.model_validate(row["day_receipt_ref"])
            for row in day_rows
            if row.get("day_receipt_ref") is not None
        ),
        "candidate_refs": _sorted_refs(
            HistoricalRangeArtifactRefV1.model_validate(row["candidate_artifact_ref"])
            for row in day_rows
            if row.get("candidate_artifact_ref") is not None
        ),
        "outcome_refs": _sorted_refs(
            HistoricalRangeArtifactRefV1.model_validate(row["outcome_artifact_ref"])
            for row in outcome_rows
        ),
        "summary_refs": _sorted_refs(
            HistoricalRangeArtifactRefV1.model_validate(row["summary_artifact_ref"])
            for row in summary_rows
            if row.get("summary_artifact_ref") is not None
        ),
    }


def _sorted_refs(
    refs: Sequence[HistoricalRangeArtifactRefV1] | Any,
) -> tuple[HistoricalRangeArtifactRefV1, ...]:
    by_identity = {
        (ref.artifact_kind.value, ref.semantic_content_hash, ref.relative_path): ref
        for ref in refs
    }
    return tuple(by_identity[key] for key in sorted(by_identity))


def _code_set_hash(repository_root: Path, relative_paths: Sequence[str]) -> str:
    root = repository_root.resolve(strict=True)
    digest = hashlib.sha256()
    for relative_path in relative_paths:
        path = (root / relative_path).resolve(strict=True)
        path.relative_to(root)
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _git_commit(repository_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    commit = completed.stdout.strip().lower()
    if completed.returncode != 0 or len(commit) != 40:
        raise ValueError("R5 runtime could not resolve the explicit repository commit")
    return commit
