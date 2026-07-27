"""Explicit-target R4 historical Outcome/Summary/bridge E2E.

The test is skipped unless every external target is supplied.  It is the
repeatable production-like acceptance path for an existing completed R3 batch;
it never discovers a database, artifact root, or batch implicitly.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, timedelta
import hashlib
import json
import os
from pathlib import Path
import subprocess
from typing import Any, Callable

from dotenv import dotenv_values
import psycopg2
import psycopg2.extras
import psycopg2.pool
import pytest

from backend.services.advisory_historical_range.artifact_store import (
    HistoricalRangeArtifactStore,
)
from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.composition import (
    build_artifact_historical_range_outcome_policy_provider,
    build_historical_range_dataset_bridge_service,
    build_historical_range_outcome_application_service,
    build_historical_range_summary_coordinator,
)
from backend.services.advisory_historical_range.models import (
    HistoricalRangeArtifactKind,
    HistoricalRangeArtifactRefV1,
    HistoricalRangeDatasetBridgeRequestV1,
    HistoricalRangeEvaluationWindowType,
    HistoricalRangeOutcomeProjection,
    HistoricalRangeOutcomeRefreshRequestV1,
    HistoricalRangeOutcomeRevisionReason,
    HistoricalRangeOutcomeStatus,
    HistoricalRangeOutcomeSubjectType,
    HistoricalRangeSourceRequirementPlanV1,
    HistoricalRangeSummaryPolicyV1,
)
from backend.services.advisory_historical_range.outcome_policy_catalog import (
    freeze_historical_range_outcome_policy,
)
from backend.services.advisory_historical_range.repository import (
    PostgresHistoricalRangeRepository,
)
from backend.services.advisory_phase1.dataset_store import LocalContentAddressedStore
from backend.services.advisory_phase1.retrospective_selector import (
    RETROSPECTIVE_SELECTOR_POLICY_HASH,
)
from backend.services.advisory_phase1.label_policy import TradingCalendar


_BATCH_ID = os.getenv("AISTOCK_R4_E2E_BATCH_ID")
_ENV_FILE = os.getenv("AISTOCK_R4_E2E_ENV_FILE")
_DB_PREFIX = os.getenv("AISTOCK_R4_E2E_DB_PREFIX")
_ARTIFACT_ROOT = os.getenv("AISTOCK_R4_E2E_ARTIFACT_ROOT")
_EVIDENCE_ROOT = os.getenv("AISTOCK_R4_E2E_EVIDENCE_ROOT")
_LABEL_AS_OF = os.getenv("AISTOCK_R4_E2E_LABEL_AS_OF")

pytestmark = pytest.mark.skipif(
    not all(
        (
            _BATCH_ID,
            _ENV_FILE,
            _DB_PREFIX,
            _ARTIFACT_ROOT,
            _EVIDENCE_ROOT,
            _LABEL_AS_OF,
        )
    ),
    reason="R4 E2E requires explicit DB, batch, date, artifact, and evidence targets",
)


def _connection_factory() -> Callable[[], Any]:
    env_path = Path(str(_ENV_FILE)).resolve(strict=True)
    values = dotenv_values(env_path, interpolate=False)
    prefix = str(_DB_PREFIX)
    required = {name: values.get(f"{prefix}_{name}") for name in ("HOST", "PORT", "NAME", "USER", "PASSWORD")}
    missing = sorted(name for name, value in required.items() if not value)
    if missing:
        raise RuntimeError(f"explicit R4 E2E database target lacks keys: {missing}")

    pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=8,
        host=required["HOST"],
        port=int(str(required["PORT"])),
        dbname=required["NAME"],
        user=required["USER"],
        password=required["PASSWORD"],
        connect_timeout=10,
        application_name="advisory_phase1r_r4_historical_e2e",
    )

    @contextmanager
    def connect():  # type: ignore[no-untyped-def]
        conn = pool.getconn()
        discard = False
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
        finally:
            try:
                if conn.closed:
                    discard = True
                else:
                    conn.reset()
            except Exception:
                discard = True
            pool.putconn(conn, close=discard)

    return connect


def _producer_hash(repository_root: Path) -> str:
    # Every file participating in the outcome derivation, including the
    # retrospective observation materialization that defines the calculation
    # owner identity (observation/stage evidence ids).
    relative_paths = (
        "backend/services/advisory_historical_range/outcome_evaluator.py",
        "backend/services/advisory_historical_range/outcome_policy_catalog.py",
        "backend/services/advisory_historical_range/outcome_policy_provider.py",
        "backend/services/advisory_historical_range/outcome_projection.py",
        "backend/services/advisory_historical_range/outcome_source.py",
        "backend/services/advisory_historical_range/retrospective_projection.py",
        "backend/services/advisory_phase1/observation_capture.py",
        "backend/services/advisory_phase1/outcome_engine.py",
    )
    digest = hashlib.sha256()
    for relative in relative_paths:
        path = repository_root / relative
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def _summary_producer_hash(repository_root: Path) -> str:
    path = repository_root / "backend/services/advisory_historical_range/summary_service.py"
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _code_set_hash(repository_root: Path, relative_paths: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    for relative_path in relative_paths:
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update((repository_root / relative_path).read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


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


def _bridge_builder_hash(repository_root: Path) -> str:
    return _code_set_hash(repository_root, _BRIDGE_BUILDER_SOURCE_FILES)


def _bridge_writer_hash(repository_root: Path) -> str:
    return _code_set_hash(
        repository_root,
        (
            "backend/services/advisory_phase1/dataset_store.py",
            "backend/services/advisory_phase1/snapshot_writer.py",
        ),
    )


def _calendar(
    *,
    conn_factory: Callable[[], Any],
    start_trade_date: date,
    end_trade_date: date,
) -> TradingCalendar:
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cal_date
                FROM market.trading_calendar
                WHERE cal_date >= %s AND cal_date <= %s AND is_trading = TRUE
                ORDER BY cal_date
                """,
                (start_trade_date, end_trade_date + timedelta(days=400)),
            )
            dates = tuple(row[0] for row in cur.fetchall())
        conn.rollback()
    if not dates or start_trade_date not in dates:
        raise RuntimeError("explicit R4 calendar slice does not cover the R3 range")
    identity = canonical_json_sha256([item.isoformat() for item in dates])
    return TradingCalendar(
        calendar_version=f"market.trading_calendar:{identity}",
        trading_dates=dates,
    )


def _sorted_refs(
    refs: tuple[HistoricalRangeArtifactRefV1, ...] | list[HistoricalRangeArtifactRefV1],
) -> tuple[HistoricalRangeArtifactRefV1, ...]:
    return tuple(
        sorted(
            set(refs),
            key=lambda item: (
                item.artifact_kind.value,
                item.semantic_content_hash,
                item.relative_path,
            ),
        )
    )


def _bridge_refs(
    *,
    conn_factory: Callable[[], Any],
    range_run_id: str,
    policy_hash: str,
    horizon: int,
) -> dict[str, tuple[HistoricalRangeArtifactRefV1, ...]]:
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
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
                SELECT outcome.outcome_artifact_ref
                FROM app.advisory_historical_range_outcome outcome
                JOIN app.advisory_historical_range_candidate candidate
                  ON candidate.candidate_id = outcome.subject_id
                JOIN app.advisory_historical_range_day_run day
                  ON day.day_run_id = candidate.day_run_id
                WHERE day.range_run_id = %s
                  AND outcome.subject_type = 'CANDIDATE'
                  AND outcome.projection = 'EXECUTABLE'
                  AND outcome.evaluation_window_type = 'FIXED_HORIZON'
                      AND outcome.horizon_trade_days = %s
                      AND outcome.historical_range_policy_bundle_hash = %s
                      AND EXISTS (
                          SELECT 1
                          FROM jsonb_array_elements(
                              outcome.outcome_json -> 'calculation_results'
                          ) AS calculation
                          WHERE calculation ->> 'projection' IN (
                              'RETURN_GROSS',
                              'RETURN_NET_ABSOLUTE',
                              'RETURN_NET_EXCESS',
                              'EXECUTABLE_MFE',
                              'EXECUTABLE_MAE'
                          )
                            AND calculation ->> 'maturity_status' = 'MATURED'
                            AND COALESCE(
                                calculation ->> 'outcome_event_status',
                                'NONE'
                            ) IN ('NONE', 'BARRIER')
                      )
                      AND outcome.outcome_version = (
                      SELECT MAX(newer.outcome_version)
                      FROM app.advisory_historical_range_outcome newer
                      WHERE newer.outcome_logical_id = outcome.outcome_logical_id
                  )
                ORDER BY outcome.outcome_artifact_ref
                """,
                (range_run_id, horizon, policy_hash),
            )
            outcome_rows = tuple(dict(row) for row in cur.fetchall())
        conn.rollback()
    day_refs = _sorted_refs([HistoricalRangeArtifactRefV1.model_validate(row["day_receipt_ref"]) for row in day_rows])
    candidate_refs = _sorted_refs(
        [HistoricalRangeArtifactRefV1.model_validate(row["candidate_artifact_ref"]) for row in day_rows]
    )
    outcome_refs = _sorted_refs(
        [HistoricalRangeArtifactRefV1.model_validate(row["outcome_artifact_ref"]) for row in outcome_rows]
    )
    if not day_refs or not candidate_refs or not outcome_refs:
        raise RuntimeError("R4 bridge exact-ref selection is unexpectedly empty")
    return {
        "successful_day_refs": day_refs,
        "candidate_refs": candidate_refs,
        "outcome_refs": outcome_refs,
    }


def _database_counts(*, conn_factory: Callable[[], Any], batch_id: str) -> dict[str, int]:
    relations = {
        "outcomes": "app.advisory_historical_range_outcome",
        "summaries": "app.advisory_historical_range_summary",
        "operations": "app.advisory_historical_range_operation",
        "capture_batches": "app.advisory_capture_batch",
        "dataset_builds": "app.advisory_dataset_build",
        "snapshots": "app.advisory_dataset_snapshot",
    }
    counts: dict[str, int] = {}
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            for name, relation in relations.items():
                if name == "operations":
                    cur.execute(f"SELECT COUNT(*) FROM {relation} WHERE batch_id = %s", (batch_id,))
                else:
                    cur.execute(f"SELECT COUNT(*) FROM {relation}")
                counts[name] = int(cur.fetchone()[0])
        conn.rollback()
    return counts


def _calculation_correction_evidence(
    *,
    conn_factory: Callable[[], Any],
    artifact_store: HistoricalRangeArtifactStore,
    resolved_request_hash: str,
    batch_id: str,
    range_run_id: str,
    policy_hash: str,
    producer_code_hash: str,
) -> HistoricalRangeArtifactRefV1 | None:
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT outcome.producer_code_hash
                FROM app.advisory_historical_range_outcome outcome
                JOIN app.advisory_historical_range_candidate candidate
                  ON candidate.candidate_id = outcome.subject_id
                JOIN app.advisory_historical_range_day_run day
                  ON day.day_run_id = candidate.day_run_id
                WHERE day.range_run_id = %s
                  AND outcome.historical_range_policy_bundle_hash = %s
                ORDER BY outcome.producer_code_hash
                """,
                (range_run_id, policy_hash),
            )
            prior_hashes = tuple(str(row[0]) for row in cur.fetchall())
        conn.rollback()
    differing = tuple(item for item in prior_hashes if item != producer_code_hash)
    if not differing:
        return None
    payload = {
        "schema_version": "advisory_phase1r_r4_calculation_correction_evidence_v1",
        "batch_id": batch_id,
        "range_run_id": range_run_id,
        "policy_bundle_hash": policy_hash,
        "prior_producer_code_hashes": differing,
        "corrected_producer_code_hash": producer_code_hash,
        "reason_codes": ("R4_PRODUCTION_CALLBACK_AND_ARTIFACT_REUSE_CORRECTION",),
    }
    return artifact_store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="advisory_phase1r_r4_calculation_correction_v1",
        payload_schema_version=str(payload["schema_version"]),
        resolved_request_hash=resolved_request_hash,
        payload=payload,
    ).ref


def _aggregate_source_correction_evidence(
    *,
    repository: PostgresHistoricalRangeRepository,
    artifact_store: HistoricalRangeArtifactStore,
    resolved_request_hash: str,
    batch_id: str,
    range_run_id: str,
    label_as_of_trade_date: date,
    target_subject_type: HistoricalRangeOutcomeSubjectType,
) -> HistoricalRangeArtifactRefV1 | None:
    child_type = {
        HistoricalRangeOutcomeSubjectType.LIST_VERSION: HistoricalRangeOutcomeSubjectType.CANDIDATE,
        HistoricalRangeOutcomeSubjectType.RANGE: HistoricalRangeOutcomeSubjectType.LIST_VERSION,
    }.get(target_subject_type)
    if child_type is None:
        raise ValueError("aggregate source-correction evidence requires LIST_VERSION or RANGE")
    latest: dict[str, Any] = {}
    for fact in repository.list_outcomes_for_summary(
        range_run_id=range_run_id,
        label_as_of_trade_date=label_as_of_trade_date,
    ):
        current = latest.get(str(fact.outcome_logical_id))
        if current is None or fact.outcome_version > current.outcome_version:
            latest[str(fact.outcome_logical_id)] = fact
    child_facts = tuple(
        sorted(
            (fact for fact in latest.values() if fact.subject_type is child_type),
            key=lambda fact: str(fact.outcome_logical_id),
        )
    )
    predecessors = tuple(
        sorted(
            (fact for fact in latest.values() if fact.subject_type is target_subject_type),
            key=lambda fact: str(fact.outcome_logical_id),
        )
    )
    if not predecessors:
        return None
    if not child_facts:
        raise RuntimeError("aggregate source correction lacks its exact latest child outcomes")
    child_refs = tuple(fact.outcome_artifact_ref for fact in child_facts)
    predecessor_refs = tuple(fact.outcome_artifact_ref for fact in predecessors)
    payload = {
        "schema_version": "advisory_phase1r_r4_aggregate_source_correction_evidence_v1",
        "batch_id": batch_id,
        "range_run_id": range_run_id,
        "target_subject_type": target_subject_type.value,
        "child_subject_type": child_type.value,
        "label_as_of_trade_date": label_as_of_trade_date,
        "latest_child_outcome_refs": [ref.model_dump(mode="json") for ref in child_refs],
        "latest_child_outcome_ref_set_hash": canonical_json_sha256(
            [ref.model_dump(mode="json") for ref in child_refs]
        ),
        "predecessor_outcomes": [
            {
                "outcome_logical_id": str(fact.outcome_logical_id),
                "source_revision_set_hash": fact.source_revision_set_hash,
                "outcome_ref": fact.outcome_artifact_ref.model_dump(mode="json"),
            }
            for fact in predecessors
        ],
        "reason_codes": ("AGGREGATE_CHILD_OUTCOME_REVISION_SET_CHANGED",),
    }
    upstream_refs = _sorted_refs([*child_refs, *predecessor_refs])
    return artifact_store.publish_payload(
        artifact_kind=HistoricalRangeArtifactKind.REQUEST,
        producer_contract_version="advisory_phase1r_r4_aggregate_source_correction_v1",
        payload_schema_version=str(payload["schema_version"]),
        resolved_request_hash=resolved_request_hash,
        payload=payload,
        upstream_refs=upstream_refs,
    ).ref


def _load_requirement_plan(
    *,
    conn_factory: Callable[[], Any],
    artifact_store: HistoricalRangeArtifactStore,
    batch_id: str,
) -> HistoricalRangeSourceRequirementPlanV1:
    with conn_factory() as conn:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT requirement_plan_ref
                FROM app.advisory_historical_range_batch
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            row = cur.fetchone()
        conn.rollback()
    if row is None or row[0] is None:
        raise RuntimeError("R3 batch lacks its exact requirement-plan ref")
    ref = HistoricalRangeArtifactRefV1.model_validate(row[0])
    return HistoricalRangeSourceRequirementPlanV1.model_validate(artifact_store.load_planning(ref).payload)


def test_completed_r3_batch_runs_r4_outcome_summary_and_nonempty_bridge() -> None:
    repository_root = Path(__file__).resolve().parents[3]
    artifact_root = Path(str(_ARTIFACT_ROOT)).resolve(strict=True)
    evidence_root = Path(str(_EVIDENCE_ROOT)).resolve()
    if repository_root == evidence_root or repository_root in evidence_root.parents:
        raise RuntimeError("R4 E2E evidence root must be outside the repository")
    evidence_root.mkdir(parents=True, exist_ok=True)
    component_root = evidence_root / "policy-components"
    calculation_root = evidence_root / "calculation-evidence"
    dataset_root = evidence_root / "dataset-cas"
    component_root.mkdir(parents=True, exist_ok=True)
    calculation_root.mkdir(parents=True, exist_ok=True)
    dataset_root.mkdir(parents=True, exist_ok=True)
    label_as_of = date.fromisoformat(str(_LABEL_AS_OF))
    conn_factory = _connection_factory()
    artifact_store = HistoricalRangeArtifactStore(root=artifact_root)
    repository = PostgresHistoricalRangeRepository(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
    )
    batch = repository.load_execution_batch(batch_id=str(_BATCH_ID))
    if batch.status.value != "COMPLETED":
        raise RuntimeError("R4 E2E requires a completed R3 batch")
    plan = _load_requirement_plan(
        conn_factory=conn_factory,
        artifact_store=artifact_store,
        batch_id=str(_BATCH_ID),
    )
    runs = {item.research_program_id: item for item in repository.list_all_execution_runs(batch_id=str(_BATCH_ID))}
    if set(runs) != {item.research_program_id for item in plan.frozen_programs}:
        raise RuntimeError("R3 run/Program identity closure failed")
    calendar = _calendar(
        conn_factory=conn_factory,
        start_trade_date=plan.request.start_trade_date,
        end_trade_date=plan.request.end_trade_date,
    )
    resolutions = {
        program.research_program_id: freeze_historical_range_outcome_policy(
            frozen_program=program,
            calendar=calendar,
            artifact_store=artifact_store,
            component_root=component_root,
            resolved_request_hash=batch.resolved_request_hash,
        )
        for program in plan.frozen_programs
    }
    policy_provider = build_artifact_historical_range_outcome_policy_provider(
        artifact_root=artifact_root,
        component_root=component_root,
        policy_bundle_refs={str(item.bundle.policy_bundle_hash): item.bundle_ref for item in resolutions.values()},
    )
    producer_hash = _producer_hash(repository_root)
    summary_producer_hash = _summary_producer_hash(repository_root)
    outcome_receipts = []
    exact_retry_refs = []
    summary_refs_by_run: dict[str, tuple[HistoricalRangeArtifactRefV1, ...]] = {}
    for program in plan.frozen_programs:
        run = runs[program.research_program_id]
        resolution = resolutions[program.research_program_id]
        policy_hash = str(resolution.bundle.policy_bundle_hash)
        summary_policy = HistoricalRangeSummaryPolicyV1(
            subject_types=tuple(sorted(HistoricalRangeOutcomeSubjectType, key=lambda item: item.value)),
            projection_groups=tuple(sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)),
            evaluation_window_types=tuple(sorted(HistoricalRangeEvaluationWindowType, key=lambda item: item.value)),
            horizons=(0, *resolution.bundle.horizons),
            outcome_policy_bundle_hash=policy_hash,
        )
        summary_coordinator = build_historical_range_summary_coordinator(
            conn_factory=conn_factory,
            artifact_root=artifact_root,
            policy=summary_policy,
            label_as_of_trade_date=label_as_of,
            producer_code_hash=summary_producer_hash,
        )
        correction_evidence_ref = _calculation_correction_evidence(
            conn_factory=conn_factory,
            artifact_store=artifact_store,
            resolved_request_hash=batch.resolved_request_hash,
            batch_id=str(_BATCH_ID),
            range_run_id=run.range_run_id,
            policy_hash=policy_hash,
            producer_code_hash=producer_hash,
        )
        raw_request = HistoricalRangeOutcomeRefreshRequestV1(
            batch_id=str(_BATCH_ID),
            range_run_ids=(run.range_run_id,),
            label_as_of_trade_date=label_as_of,
            policy_bundle_ref=resolution.bundle_ref,
            policy_bundle_hash=policy_hash,
            requested_subject_types=(
                HistoricalRangeOutcomeSubjectType.CANDIDATE,
                HistoricalRangeOutcomeSubjectType.EPISODE,
            ),
            requested_projections=tuple(sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)),
            horizons=resolution.bundle.horizons,
            producer_code_hash=producer_hash,
            outcome_contract_version="advisory_phase1r_r4_outcome_v1",
            correction_reason=(
                HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION
                if correction_evidence_ref is not None
                else None
            ),
            correction_evidence_ref=correction_evidence_ref,
            operation_idempotency_key=(
                f"r4-e2e-outcome-raw-{_BATCH_ID}-{run.range_run_id}-{label_as_of.isoformat()}-{producer_hash[:12]}"
            ),
            expected_batch_row_version=batch.row_version,
            max_items_per_slice=500,
            max_parallel_runs=1,
            lease_seconds=900,
        )
        raw_service = build_historical_range_outcome_application_service(
            conn_factory=conn_factory,
            artifact_root=artifact_root,
            policy_provider=policy_provider,
            producer_code_hash=producer_hash,
            outcome_contract_version="advisory_phase1r_r4_outcome_v1",
        )
        receipt, receipt_ref = raw_service.refresh_until_stable_boundary(
            request=raw_request,
            resolved_request_hash=batch.resolved_request_hash,
            worker_id="r4-e2e-outcome-raw",
        )
        if receipt.status != "COMPLETED":
            raise RuntimeError(f"R4 raw outcome refresh did not complete: {receipt.reason_codes}")
        retry, retry_ref = raw_service.refresh_until_stable_boundary(
            request=raw_request,
            resolved_request_hash=batch.resolved_request_hash,
            worker_id="r4-e2e-exact-retry-raw",
        )
        if retry_ref != receipt_ref or retry.receipt_hash != receipt.receipt_hash:
            raise RuntimeError("R4 raw exact retry did not return the committed receipt")
        outcome_receipts.append(receipt)
        exact_retry_refs.append(retry_ref)

        for aggregate_type in (
            HistoricalRangeOutcomeSubjectType.LIST_VERSION,
            HistoricalRangeOutcomeSubjectType.RANGE,
        ):
            source_evidence_ref = _aggregate_source_correction_evidence(
                repository=repository,
                artifact_store=artifact_store,
                resolved_request_hash=batch.resolved_request_hash,
                batch_id=str(_BATCH_ID),
                range_run_id=run.range_run_id,
                label_as_of_trade_date=label_as_of,
                target_subject_type=aggregate_type,
            )
            probe_reason = (
                HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION
                if source_evidence_ref is not None
                else (
                    HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION
                    if correction_evidence_ref is not None
                    else None
                )
            )
            probe_evidence_ref = (
                source_evidence_ref
                if source_evidence_ref is not None
                else correction_evidence_ref
            )
            probe_request = HistoricalRangeOutcomeRefreshRequestV1(
                batch_id=str(_BATCH_ID),
                range_run_ids=(run.range_run_id,),
                label_as_of_trade_date=label_as_of,
                policy_bundle_ref=resolution.bundle_ref,
                policy_bundle_hash=policy_hash,
                requested_subject_types=(aggregate_type,),
                requested_projections=tuple(
                    sorted(HistoricalRangeOutcomeProjection, key=lambda item: item.value)
                ),
                horizons=resolution.bundle.horizons,
                producer_code_hash=producer_hash,
                outcome_contract_version="advisory_phase1r_r4_outcome_v1",
                correction_reason=probe_reason,
                correction_evidence_ref=probe_evidence_ref,
                operation_idempotency_key=(
                    f"r4-e2e-probe-{aggregate_type.value.lower()}-{run.range_run_id}"
                ),
                expected_batch_row_version=batch.row_version,
                max_items_per_slice=10000,
                max_parallel_runs=1,
                lease_seconds=900,
            )
            probe_service = build_historical_range_outcome_application_service(
                conn_factory=conn_factory,
                artifact_root=artifact_root,
                policy_provider=policy_provider,
                producer_code_hash=producer_hash,
                outcome_contract_version="advisory_phase1r_r4_outcome_v1",
            )
            # Classification freezes current logical/source identity only. The
            # grouped production operations below re-enable the strict latest
            # predecessor transition checks for every persisted outcome.
            probe_service._planner._latest_outcome = lambda _logical_id: None
            planned = probe_service._planner.plan_slice(
                request=probe_request,
                cursor=None,
                limit=10000,
            )
            if not planned.exhausted:
                raise RuntimeError(
                    f"R4 {aggregate_type.value} correction classifier exceeded its bounded plan"
                )
            grouped: dict[
                tuple[str, str],
                tuple[
                    HistoricalRangeOutcomeRevisionReason | None,
                    HistoricalRangeArtifactRefV1 | None,
                    list[str],
                ],
            ] = {}

            def add_group(
                *,
                group_name: str,
                reason: HistoricalRangeOutcomeRevisionReason | None,
                evidence_ref: HistoricalRangeArtifactRefV1 | None,
                logical_id: str,
            ) -> None:
                evidence_hash = (
                    evidence_ref.semantic_content_hash
                    if evidence_ref is not None
                    else "NONE"
                )
                key = (group_name, evidence_hash)
                current = grouped.get(key)
                if current is None:
                    grouped[key] = (reason, evidence_ref, [logical_id])
                else:
                    current[2].append(logical_id)

            for item in planned.items:
                predecessor = repository.load_latest_outcome(
                    outcome_logical_id=str(item.outcome_logical_id)
                )
                if predecessor is None:
                    add_group(
                        group_name="INITIAL",
                        reason=None,
                        evidence_ref=None,
                        logical_id=str(item.outcome_logical_id),
                    )
                elif predecessor.source_revision_set_hash != item.source_revision_set_hash:
                    if source_evidence_ref is None:
                        raise RuntimeError(
                            "source-correction group lacks exact lineage evidence"
                        )
                    add_group(
                        group_name="SOURCE_CORRECTION",
                        reason=HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
                        evidence_ref=source_evidence_ref,
                        logical_id=str(item.outcome_logical_id),
                    )
                elif (
                    predecessor.producer_code_hash != producer_hash
                    or predecessor.outcome_contract_version
                    != "advisory_phase1r_r4_outcome_v1"
                ):
                    if correction_evidence_ref is None:
                        raise RuntimeError(
                            "calculation-correction group lacks exact code evidence"
                        )
                    add_group(
                        group_name="CALCULATION_CORRECTION",
                        reason=HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
                        evidence_ref=correction_evidence_ref,
                        logical_id=str(item.outcome_logical_id),
                    )
                elif (
                    predecessor.revision_reason
                    in {
                        HistoricalRangeOutcomeRevisionReason.SOURCE_CORRECTION,
                        HistoricalRangeOutcomeRevisionReason.CALCULATION_CORRECTION,
                    }
                    and predecessor.revision_evidence_ref is not None
                ):
                    add_group(
                        group_name=predecessor.revision_reason.value,
                        reason=predecessor.revision_reason,
                        evidence_ref=predecessor.revision_evidence_ref,
                        logical_id=str(item.outcome_logical_id),
                    )

            group_specs = [
                (
                    group_name,
                    reason,
                    evidence_ref,
                    tuple(sorted(set(logical_ids))),
                )
                for (group_name, _evidence_hash), (
                    reason,
                    evidence_ref,
                    logical_ids,
                ) in sorted(grouped.items())
            ]

            if not group_specs and aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE:
                summary_ref = summary_coordinator.refresh(range_run_id=run.range_run_id)
                if summary_ref is None:
                    raise RuntimeError("R4 range summary refresh returned no exact artifact")
                summary_refs_by_run[run.range_run_id] = (summary_ref,)

            for group_index, (group_name, group_reason, group_ref, logical_ids) in enumerate(
                group_specs
            ):
                logical_set_hash = canonical_json_sha256(logical_ids)
                aggregate_request = HistoricalRangeOutcomeRefreshRequestV1(
                    batch_id=str(_BATCH_ID),
                    range_run_ids=(run.range_run_id,),
                    label_as_of_trade_date=label_as_of,
                    policy_bundle_ref=resolution.bundle_ref,
                    policy_bundle_hash=policy_hash,
                    requested_subject_types=(aggregate_type,),
                    requested_outcome_logical_ids=logical_ids,
                    requested_projections=tuple(
                        sorted(
                            HistoricalRangeOutcomeProjection,
                            key=lambda item: item.value,
                        )
                    ),
                    horizons=resolution.bundle.horizons,
                    producer_code_hash=producer_hash,
                    outcome_contract_version="advisory_phase1r_r4_outcome_v1",
                    correction_reason=group_reason,
                    correction_evidence_ref=group_ref,
                    operation_idempotency_key=(
                        f"r4-e2e-outcome-{aggregate_type.value.lower()}-{group_name.lower()}-"
                        f"{run.range_run_id}-{producer_hash[:12]}-{logical_set_hash[:12]}"
                        f"{'-' + summary_producer_hash[:12] if aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE else ''}"
                        f"{'-lease3600' if aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE else ''}"
                    ),
                    expected_batch_row_version=batch.row_version,
                    max_items_per_slice=500,
                    max_parallel_runs=1,
                    lease_seconds=(
                        3600
                        if aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE
                        else 900
                    ),
                )
                aggregate_service = build_historical_range_outcome_application_service(
                    conn_factory=conn_factory,
                    artifact_root=artifact_root,
                    policy_provider=policy_provider,
                    producer_code_hash=producer_hash,
                    outcome_contract_version="advisory_phase1r_r4_outcome_v1",
                    summary_coordinator=(
                        summary_coordinator
                        if aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE
                        and group_index == len(group_specs) - 1
                        else None
                    ),
                )
                receipt, receipt_ref = aggregate_service.refresh_until_stable_boundary(
                    request=aggregate_request,
                    resolved_request_hash=batch.resolved_request_hash,
                    worker_id=(
                        f"r4-e2e-{aggregate_type.value.lower()}-{group_name.lower()}"
                    ),
                )
                if receipt.status != "COMPLETED":
                    raise RuntimeError(
                        f"R4 {aggregate_type.value}/{group_name} outcome refresh "
                        f"did not complete: {receipt.reason_codes}"
                    )
                retry, retry_ref = aggregate_service.refresh_until_stable_boundary(
                    request=aggregate_request,
                    resolved_request_hash=batch.resolved_request_hash,
                    worker_id=(
                        f"r4-e2e-exact-retry-{aggregate_type.value.lower()}-"
                        f"{group_name.lower()}"
                    ),
                )
                if retry_ref != receipt_ref or retry.receipt_hash != receipt.receipt_hash:
                    raise RuntimeError(
                        f"R4 {aggregate_type.value}/{group_name} exact retry "
                        "did not return the committed receipt"
                    )
                outcome_receipts.append(receipt)
                exact_retry_refs.append(retry_ref)
                if (
                    aggregate_type is HistoricalRangeOutcomeSubjectType.RANGE
                    and group_index == len(group_specs) - 1
                ):
                    summary_refs_by_run[run.range_run_id] = receipt.summary_refs

    store_identity = {
        "durability_mode": LocalContentAddressedStore.expected_durability_mode(),
        "atomic_publish_mode": "HARDLINK_CREATE_IF_ABSENT_V1",
    }
    query_registry_version = "advisory_hr_r4_query_registry_v1"
    builder_hash = _bridge_builder_hash(repository_root)
    writer_hash = _bridge_writer_hash(repository_root)
    partition_policy_id = "ADVISORY_PHASE1R_RETROSPECTIVE_RANGE_PARTITION_V1"
    bridge_service = build_historical_range_dataset_bridge_service(
        conn_factory=conn_factory,
        artifact_root=artifact_root,
        repository_root=repository_root,
        calculation_evidence_root=calculation_root,
        calculation_evidence_store_identity=store_identity,
        dataset_store_root=dataset_root,
        dataset_store_identity=store_identity,
        policy_provider=policy_provider,
        producer_code_hash=producer_hash,
        code_commit=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository_root, text=True).strip(),
        query_registry_version=query_registry_version,
        builder_hash=builder_hash,
        writer_hash=writer_hash,
        partition_policy_id=partition_policy_id,
    )
    bridge_receipts = []
    for program in plan.frozen_programs:
        run = runs[program.research_program_id]
        resolution = resolutions[program.research_program_id]
        policy_hash = str(resolution.bundle.policy_bundle_hash)
        refs = _bridge_refs(
            conn_factory=conn_factory,
            range_run_id=run.range_run_id,
            policy_hash=policy_hash,
            horizon=1,
        )
        request = HistoricalRangeDatasetBridgeRequestV1(
            batch_id=str(_BATCH_ID),
            range_run_ids=(run.range_run_id,),
            successful_day_refs=refs["successful_day_refs"],
            candidate_refs=refs["candidate_refs"],
            outcome_refs=refs["outcome_refs"],
            summary_refs=summary_refs_by_run[run.range_run_id],
            requested_horizons=(1,),
            requested_maturity_statuses=(HistoricalRangeOutcomeStatus.COMPLETE,),
            policy_bundle_refs=(resolution.bundle_ref,),
            policy_component_hashes={
                policy_hash: {
                    component.component_role: component.component_hash for component in resolution.bundle.components
                }
            },
            canonical_signal_dedup_policy_hash=canonical_json_sha256(
                {"policy_id": "CANONICAL_SIGNAL_EXACT_STAGE_AND_ECONOMIC_RESULT_V1"}
            ),
            retrospective_selector_policy_hash=RETROSPECTIVE_SELECTOR_POLICY_HASH,
            dataset_schema_hash=canonical_json_sha256({"schema_version": "advisory_phase1_retrospective_dataset_v1"}),
            builder_hash=builder_hash,
            writer_hash=writer_hash,
            partition_policy_hash=canonical_json_sha256({"partition_policy_id": partition_policy_id}),
            compression_config_hash=canonical_json_sha256({"codec": "zstd", "level": 3}),
            artifact_root_identity_hash=artifact_store.root_identity_hash,
            operation_idempotency_key=(
                f"r4-e2e-bridge-{_BATCH_ID}-{run.range_run_id}-"
                f"{label_as_of.isoformat()}-{builder_hash[:12]}-{producer_hash[:12]}"
            ),
            expected_batch_row_version=batch.row_version,
            lease_seconds=900,
        )
        receipt, _ = bridge_service.build_until_stable_boundary(
            request=request,
            resolved_request_hash=batch.resolved_request_hash,
            worker_id="r4-e2e-bridge",
        )
        if receipt.result_status.value != "SEALED":
            raise RuntimeError(f"R4 bridge did not produce a non-empty SEALED snapshot: {receipt.reason_codes}")
        bridge_receipts.append(receipt)

    result = {
        "batch_id": _BATCH_ID,
        "label_as_of_trade_date": label_as_of.isoformat(),
        "producer_code_hash": producer_hash,
        "summary_producer_code_hash": summary_producer_hash,
        "artifact_root_identity_hash": artifact_store.root_identity_hash,
        "policy_bundles": {
            program_id: resolution.bundle.model_dump(mode="json") for program_id, resolution in resolutions.items()
        },
        "outcome_receipts": [item.model_dump(mode="json") for item in outcome_receipts],
        "exact_retry_receipt_refs": [item.model_dump(mode="json") for item in exact_retry_refs],
        "bridge_receipts": [item.model_dump(mode="json") for item in bridge_receipts],
        "database_counts": _database_counts(conn_factory=conn_factory, batch_id=str(_BATCH_ID)),
    }
    result_path = evidence_root / "r4_historical_e2e_result.json"
    result_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    assert len(outcome_receipts) >= len(plan.frozen_programs)
    assert len(exact_retry_refs) == len(outcome_receipts)
    assert set(summary_refs_by_run) == {
        runs[program.research_program_id].range_run_id
        for program in plan.frozen_programs
    }
    assert len(bridge_receipts) == 2
    assert all(item.processed_count > 0 for item in outcome_receipts)
    assert all(item.observation_count > 0 for item in bridge_receipts)
