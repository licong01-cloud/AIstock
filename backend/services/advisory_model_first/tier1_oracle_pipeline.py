from __future__ import annotations

import gc
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import numpy as np
import pandas as pd
from pydantic import BaseModel

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows imports this WSL pipeline for tests and request prep.
    _resource = None

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    CATEGORICAL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.feature_schema_v2 import (
    FEATURE_SCHEMA_VERSION,
    MODEL_FEATURE_COLUMNS,
    feature_schema_hash,
)
from backend.services.advisory_model_first.meta_label_features import (
    build_meta_label_feature_matrix,
)
from backend.services.advisory_model_first.policy_contracts import (
    FrozenAdvisoryPolicyDatasetRequestV1,
)
from backend.services.advisory_model_first.policy_cpcv import build_policy_cpcv_paths
from backend.services.advisory_model_first.policy_dataset_bundle import (
    load_policy_dataset_bundle,
)
from backend.services.advisory_model_first.policy_rank_source import build_policy_rankings
from backend.services.advisory_model_first.policy_utility_contracts import (
    FrozenAdvisoryPolicyUtilityTrainingRequestV2,
)
from backend.services.advisory_model_first.prediction_source import (
    ExactPredictionSource,
    sha256_file,
)
from backend.services.advisory_model_first.qe_file_source import (
    STATIC_FACTOR_COLUMNS,
    all_qlib_instruments,
    initialize_qlib,
    load_qlib_daily,
    load_static_factors,
    load_suspend_rows,
    load_trading_calendar,
    validate_factor_file_schemas,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    authorize_research_window_access,
    evidence_reference_for_file,
    generate_current_route,
    load_window_contract,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    AdvisoryResearchWindowContractV1,
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    N0CompletionReceiptV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
    build_window_access_request,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    AdvisoryN1Tier1RequestV1,
    AdvisoryTier1LearnabilityReceiptV1,
    AdvisoryTier1OracleReceiptV1,
    AdvisoryTier1QuadrantReceiptV1,
    N1_DATASET_IDENTITY,
    N1_DATA_CUTOFF,
    N1_DECISION_END,
    N1_DECISION_START,
    Tier1EvidenceState,
    Tier1InterventionSupportV1,
    Tier1MetricInferenceV1,
    Tier1PitSnapshotIdentityV1,
    Tier1Quadrant,
    build_learnability_receipt,
    build_n1_tier1_request,
    build_oracle_receipt,
    build_quadrant_receipt,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
)
from backend.services.dataset_release.pit import (
    FrozenPitSnapshot,
    filter_frame_to_pit_spans,
    freeze_pit_snapshot,
    frozen_pit_snapshot_from_mapping,
    write_frozen_pit_snapshot,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


N1_ORACLE_EXPERIMENT_ID = "ADVISORY-N1-TIER1-ORACLE"
N1_LEARNABILITY_EXPERIMENT_ID = "ADVISORY-N1-TIER1-LEARNABILITY"
N1_PARENT_LINEAGE = ("N0-RESEARCH-CONTROL",)
N1_BUNDLE_SCHEMA = "advisory_n1_tier1_bundle_v1"
_KNOWN_CASH_STATUSES = frozenset(
    {
        "NOT_ELIGIBLE_ENTRY_DATE",
        "NOT_ENTERED_SUSPENDED",
        "NOT_ENTERED_LIMIT_UP",
    }
)
_Z_975 = 1.959963984540054
_Z_80 = 0.8416212335729143


@dataclass(frozen=True)
class Tier1OracleResult:
    candidate_labels: pd.DataFrame
    oracle_daily: pd.DataFrame
    recall_daily: pd.DataFrame
    outcome_coverage: pd.DataFrame
    recall_summary: dict[str, Any]
    rank_bucket_summary: tuple[dict[str, Any], ...]
    universe_summary: dict[str, Any]
    perfect_top5_lift: Tier1MetricInferenceV1
    intervention_support: Tier1InterventionSupportV1
    evidence_sufficient: bool
    evidence_reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class Tier1LearnabilityResult:
    oof_predictions: pd.DataFrame
    daily: pd.DataFrame
    lift: Tier1MetricInferenceV1
    intervention_support: Tier1InterventionSupportV1
    evidence_sufficient: bool
    evidence_reason_codes: tuple[str, ...]


@dataclass(frozen=True)
class Tier1FullUniverseOutcomeResult:
    outcomes: pd.DataFrame
    coverage: pd.DataFrame


class Tier1Progress:
    def __init__(self, *, limit_bytes: int) -> None:
        self.limit_bytes = int(limit_bytes)
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def stage(self, name: str, started: float, **details: Any) -> None:
        peak = _peak_rss_bytes()
        receipt = {
            "stage": name,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": peak,
            **details,
        }
        self.stages.append(receipt)
        print(
            json.dumps(receipt, ensure_ascii=True, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        if peak > self.limit_bytes:
            _raise(
                "N1 exceeded the approved RSS limit",
                "ADVISORY_N1_MEMORY_LIMIT_EXCEEDED",
                stage=name,
                peak_rss_bytes=peak,
                limit_bytes=self.limit_bytes,
            )

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_n1_resource_report_v1",
            "peak_rss_bytes": _peak_rss_bytes(),
            "limit_bytes": self.limit_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def freeze_canonical_pit_snapshot(
    *,
    output_path: str | Path,
    connection_factory: Callable[[], Any] = get_conn,
) -> dict[str, Any]:
    """Freeze the exact canonical PIT development slice without DB mutation."""

    output = Path(output_path)
    try:
        context = connection_factory()
        with context as conn:
            if hasattr(conn, "set_session"):
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT universe_key, rule_version, scope, start_date, end_date,
                           status, dirty, source_fingerprint_sha256,
                           generated_at, updated_at
                      FROM market.stock_universe_pit_state
                     WHERE universe_key = %s
                    """,
                    (CANONICAL_PIT_UNIVERSE_KEY,),
                )
                columns = [item[0] for item in cur.description]
                raw_state = cur.fetchone()
                if raw_state is None:
                    _raise(
                        "canonical PIT state is missing",
                        "ADVISORY_N1_PIT_STATE_NOT_READY",
                    )
                state = dict(zip(columns, raw_state, strict=True))
                _validate_pit_state(state)
                cur.execute(
                    """
                    SELECT ts_code, eligible_start, eligible_end, entry_reason, exit_reason
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (CANONICAL_PIT_UNIVERSE_KEY, N1_DATA_CUTOFF, N1_DECISION_START),
                )
                span_columns = [item[0] for item in cur.description]
                rows = [dict(zip(span_columns, row, strict=True)) for row in cur.fetchall()]
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "canonical PIT snapshot could not be read",
            "ADVISORY_N1_PIT_STATE_NOT_READY",
            error_type=type(exc).__name__,
        )

    state_identity = canonical_json_sha256(
        {
            key: _json_ready(state[key])
            for key in (
                "universe_key",
                "rule_version",
                "scope",
                "start_date",
                "end_date",
                "status",
                "dirty",
                "source_fingerprint_sha256",
                "generated_at",
                "updated_at",
            )
        }
    )
    snapshot = freeze_pit_snapshot(
        rows,
        universe_key=CANONICAL_PIT_UNIVERSE_KEY,
        rule_version=CANONICAL_PIT_RULE_VERSION,
        scope_start=N1_DECISION_START,
        cutoff=N1_DATA_CUTOFF,
        state_identity=state_identity,
        source_fingerprint_sha256=str(state["source_fingerprint_sha256"]),
        parameter_hash=canonical_rule_parameters_digest(),
        state_status=str(state["status"]),
        state_dirty=bool(state["dirty"]),
        state_start=_as_date(state["start_date"]),
        state_end=_as_date(state["end_date"]),
    )
    if output.exists():
        try:
            existing = frozen_pit_snapshot_from_mapping(json.loads(output.read_text(encoding="utf-8")))
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            _raise(
                "existing PIT snapshot cannot be read",
                "ADVISORY_N1_PIT_SNAPSHOT_CONFLICT",
                error_type=type(exc).__name__,
            )
        if existing.canonical_bytes() != snapshot.canonical_bytes():
            _raise(
                "existing PIT snapshot differs from the current exact request",
                "ADVISORY_N1_PIT_SNAPSHOT_CONFLICT",
            )
        write_result = {
            "path": str(output),
            "sha256": hashlib.sha256(existing.canonical_bytes()).hexdigest(),
            "size_bytes": output.stat().st_size,
            "spans_sha256": existing.spans_sha256,
        }
        status = "EXISTING_SNAPSHOT"
    else:
        try:
            write_result = write_frozen_pit_snapshot(output, snapshot)
        except FileExistsError:
            _raise(
                "PIT snapshot appeared concurrently",
                "ADVISORY_N1_PIT_SNAPSHOT_CONFLICT",
            )
        status = "FROZEN"
    return {
        "status": status,
        **write_result,
        "span_count": len(snapshot.spans),
        "instrument_count": snapshot.unique_instruments,
        "universe_key": snapshot.universe_key,
        "rule_version": snapshot.rule_version,
        "scope_start": snapshot.scope_start.isoformat(),
        "cutoff": snapshot.cutoff.isoformat(),
        "database_write": False,
    }


def prepare_n1_tier1_request(
    *,
    n0_completion_path: str | Path,
    research_window_contract_path: str | Path,
    registry_path: str | Path,
    route_path: str | Path,
    policy_dataset_bundle_root: str | Path,
    feature_reference_request_path: str | Path,
    pit_snapshot_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> AdvisoryN1Tier1RequestV1:
    n0_path = Path(n0_completion_path)
    window_path = Path(research_window_contract_path)
    bundle_root = Path(policy_dataset_bundle_root)
    pit_path = Path(pit_snapshot_path)
    try:
        n0 = N0CompletionReceiptV1.model_validate_json(n0_path.read_text(encoding="utf-8"))
        window = load_window_contract(window_path)
        policy_manifest = load_policy_dataset_bundle(bundle_root, expected_bundle_id=N1_DATASET_IDENTITY)
        policy_request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
            (bundle_root / "request.json").read_text(encoding="utf-8")
        )
        feature_reference = FrozenAdvisoryPolicyUtilityTrainingRequestV2.model_validate_json(
            Path(feature_reference_request_path).read_text(encoding="utf-8")
        )
        pit_snapshot = frozen_pit_snapshot_from_mapping(json.loads(pit_path.read_text(encoding="utf-8")))
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "N1 request source cannot be read",
            "ADVISORY_N1_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if n0.status != "COMPLETE" or n0.next_task != "N1_TIER1_ORACLE_LEARNABILITY":
        _raise(
            "N0 does not authorize N1",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
        )
    if sha256_file(window_path) != n0.window_contract_ref.sha256:
        _raise(
            "N0/window identity differs from formal evidence",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
        )
    _validate_policy_and_feature_reference(
        policy_manifest=policy_manifest,
        policy_request=policy_request,
        feature_reference=feature_reference,
        window=window,
    )
    commit = _git_commit_for_worktree(Path(repository_root))
    pit_ref = evidence_reference_for_file(pit_path, role="n1_frozen_pit_snapshot")
    pit_identity = Tier1PitSnapshotIdentityV1(
        artifact_ref=pit_ref,
        spans_sha256=pit_snapshot.spans_sha256,
        source_fingerprint_sha256=pit_snapshot.source_fingerprint_sha256,
        parameter_hash=pit_snapshot.parameter_hash,
        universe_key=pit_snapshot.universe_key,
        rule_version=pit_snapshot.rule_version,
        scope_start=pit_snapshot.scope_start,
        cutoff=pit_snapshot.cutoff,
        span_count=len(pit_snapshot.spans),
        instrument_count=pit_snapshot.unique_instruments,
    )
    request = build_n1_tier1_request(
        n0_completion_ref=evidence_reference_for_file(n0_path, role="n0_completion"),
        n0_completion_receipt_sha256=n0.receipt_sha256,
        research_window_contract_ref=evidence_reference_for_file(window_path, role="n0_window_contract"),
        research_window_contract_sha256=window.contract_sha256,
        research_window_contract_path=str(window_path),
        registry_path=str(registry_path),
        route_path=str(route_path),
        policy_dataset_bundle_root=str(bundle_root),
        policy_dataset_bundle_id=N1_DATASET_IDENTITY,
        policy_dataset_manifest_file_sha256=sha256_file(bundle_root / "manifest.json"),
        policy_dataset_request_sha256=policy_request.request_sha256,
        program_id=policy_request.program_id,
        binding_version_id=policy_request.binding_version_id,
        package_id=policy_request.package_id,
        manifest_sha256=policy_request.manifest_sha256,
        selection_runtime_semantics_hash=policy_request.selection_runtime_semantics_hash,
        style_profile_id=policy_request.style_profile_id,
        style_profile_hash=policy_request.style_profile_hash,
        baseline_policy_sha256=policy_request.baseline_policy_sha256,
        shadow_policy_sha256=policy_request.shadow_policy_sha256,
        cost_policy=policy_request.cost_policy,
        cost_policy_sha256=policy_request.cost_policy_sha256,
        split_policy=policy_request.split_policy,
        split_policy_sha256=policy_request.split_policy_sha256,
        representative_seed_run_ids=policy_request.representative_seed_run_ids,
        prediction_artifacts=policy_request.prediction_artifacts,
        terminal_weights=policy_request.terminal_weights,
        pit_snapshot=pit_identity,
        qlib_daily_root=policy_request.qlib_daily_root,
        factor_data_root=feature_reference.factor_data_root,
        factor_data_cutoff=date.fromisoformat(feature_reference.factor_data_cutoff),
        suspend_data_root=feature_reference.suspend_data_root,
        prediction_store_root=policy_request.prediction_store_root,
        market_calendar_identity=feature_reference.market_calendar_identity,
        suspend_sidecar_identity=feature_reference.suspend_sidecar_identity,
        feature_schema_hash=feature_reference.feature_schema_hash,
        repository_root=str(repository_root),
        repository_commit=commit,
        output_root=str(output_root),
    )
    _write_immutable_json_model(
        Path(output_path),
        request,
        model_type=AdvisoryN1Tier1RequestV1,
        hash_field="request_sha256",
        reason_code="ADVISORY_N1_REQUEST_INVALID",
    )
    return request


def authorize_n1_development_access(
    request: AdvisoryN1Tier1RequestV1,
) -> tuple[AdvisoryResearchWindowContractV1, dict[str, Any], dict[str, Any]]:
    n0_path = Path(request.n0_completion_ref.artifact_uri)
    try:
        n0 = N0CompletionReceiptV1.model_validate_json(n0_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "N1 cannot read its bound N0 completion receipt",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if (
        sha256_file(n0_path) != request.n0_completion_ref.sha256
        or n0.receipt_sha256 != request.n0_completion_receipt_sha256
        or n0.status != "COMPLETE"
        or n0.next_task != "N1_TIER1_ORACLE_LEARNABILITY"
    ):
        _raise(
            "N1 bound N0 completion identity is invalid",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
        )
    try:
        window = load_window_contract(request.research_window_contract_path)
    except AdvisoryModelFirstError as exc:
        _raise(
            "N1 research window contract cannot be authorized",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
            upstream_reason=exc.reason_code,
        )
    if (
        sha256_file(request.research_window_contract_path) != request.research_window_contract_ref.sha256
        or window.contract_sha256 != request.research_window_contract_sha256
    ):
        _raise(
            "N1 research window contract hash drifted",
            "ADVISORY_N1_N0_IDENTITY_MISMATCH",
        )
    policy_identity = research_policy_identity(
        baseline_policy_sha256=request.baseline_policy_sha256,
        shadow_policy_sha256=request.shadow_policy_sha256,
        cost_policy_sha256=request.cost_policy_sha256,
    )
    common = {
        "contract_sha256": window.contract_sha256,
        "objective_contract": ObjectiveContract.ALPHA_RANKING,
        "decision_use": DecisionUse.DIRECTION_GATE,
        "dataset_identity": request.dataset_identity,
        "policy_identity": policy_identity,
        "start_date": N1_DECISION_START,
        "end_date": N1_DATA_CUTOFF,
    }
    try:
        oracle_access = authorize_research_window_access(
            contract=window,
            request=build_window_access_request(
                study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
                **common,
            ),
        )
        learnability_access = authorize_research_window_access(
            contract=window,
            request=build_window_access_request(
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                **common,
            ),
        )
    except AdvisoryModelFirstError as exc:
        _raise(
            "N1 development request was rejected before data loading",
            "ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED",
            upstream_reason=exc.reason_code,
            upstream_context=exc.context,
        )
    consume_path = Path(window.sealed_consumption_receipt_uri)
    if consume_path.exists():
        _raise(
            "N1 requires the declared sealed holdout to remain unconsumed",
            "ADVISORY_N1_SEALED_HOLDOUT_ACCESS_DENIED",
            consume_receipt=str(consume_path),
        )
    return window, oracle_access, learnability_access


def run_n1_tier1_pipeline(request_path: str | Path) -> dict[str, Any]:
    """Execute the frozen development-only N1 batch and resume delivery exactly."""

    try:
        request = AdvisoryN1Tier1RequestV1.model_validate_json(Path(request_path).read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "N1 request cannot be read",
            "ADVISORY_N1_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    # Holdout authorization intentionally precedes environment checks and every
    # prediction/Qlib/factor/suspend loader.
    _, oracle_access, learnability_access = authorize_n1_development_access(request)
    progress = Tier1Progress(limit_bytes=request.resource_max_rss_bytes)
    existing = _find_existing_bundle(request)
    if existing is not None:
        environment = _verify_wsl_environment(
            request,
            require_repository_identity=False,
        )
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return {
            "status": "EXISTING_BUNDLE",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "bundle_id": existing.name,
            "bundle_path": str(existing),
            "sealed_holdout_accessed": False,
            "environment": environment,
            "delivery": delivery,
            "backend_restart": "noop",
            "production_ddl_gate": "noop",
            "runtime_activation": "noop",
        }
    environment = _verify_wsl_environment(request, require_repository_identity=True)

    started = time.monotonic()
    sources = _load_and_verify_n1_sources(request)
    progress.stage(
        "source_identity",
        started,
        decision_date_count=len(sources["decision_dates"]),
        cpcv_ready_path_count=len(sources["ready_paths"]),
        pit_span_count=len(sources["pit_snapshot"].spans),
    )

    started = time.monotonic()
    prediction_source = ExactPredictionSource(request.prediction_store_root)
    descriptors = prediction_source.describe_all(request.representative_seed_run_ids.values())
    mismatches = {
        run_id: {
            "expected": request.prediction_artifacts[run_id].model_dump(mode="json"),
            "actual": descriptor.model_dump(mode="json"),
        }
        for run_id, descriptor in descriptors.items()
        if descriptor.model_dump(mode="json") != request.prediction_artifacts[run_id].model_dump(mode="json")
    }
    if mismatches:
        _raise(
            "N1 Prediction Store descriptors changed after request freeze",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            mismatched_run_ids=sorted(mismatches),
        )
    leg_frames = {
        leg_id: filter_prediction_frame_to_pit(
            prediction_source.load_scores(
                run_id,
                decision_dates=sources["decision_dates"],
                verify_artifact=False,
            ),
            sources["pit_snapshot"],
        )
        for leg_id, run_id in request.representative_seed_run_ids.items()
    }
    common_prediction_count = _common_prediction_counts(leg_frames)
    rank_result = build_policy_rankings(
        leg_frames=leg_frames,
        terminal_weights=request.terminal_weights,
        decision_dates=sources["decision_dates"],
        trading_calendar=sources["n1_calendar"],
        identity={
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
        },
        required_depth=50,
    )
    rank_result.rankings["is_candidate_decision"] = True
    del leg_frames
    gc.collect()
    progress.stage(
        "pit_top50_rankings",
        started,
        row_count=len(rank_result.rankings),
        minimum_common_prediction_count=min(common_prediction_count.values()),
        maximum_common_prediction_count=max(common_prediction_count.values()),
    )

    started = time.monotonic()
    pit_symbols = sorted({span.ts_code for span in sources["pit_snapshot"].spans})
    daily = load_qlib_daily(
        pit_symbols,
        start=N1_DECISION_START.isoformat(),
        end=N1_DATA_CUTOFF.isoformat(),
    )
    benchmark = load_qlib_daily(
        [request.cost_policy.benchmark_instrument],
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
        fields=("$open", "$close"),
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start=N1_DECISION_START.isoformat(),
        end=N1_DATA_CUTOFF.isoformat(),
        instruments=pit_symbols,
        full_day_only=True,
    )
    oracle = build_tier1_outcomes_and_oracle(
        rankings=rank_result.rankings,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=sources["pit_snapshot"],
        trading_calendar=sources["n1_calendar"],
        decision_dates=sources["decision_dates"],
        request=request,
        common_prediction_count_by_date=common_prediction_count,
    )
    del daily, suspend
    gc.collect()
    progress.stage(
        "oracle",
        started,
        candidate_label_rows=len(oracle.candidate_labels),
        oracle_evaluable_days=len(oracle.oracle_daily),
        oracle_lift_bps=oracle.perfect_top5_lift.point_estimate_bps,
        oracle_state=oracle.perfect_top5_lift.evidence_state.value,
    )

    started = time.monotonic()
    n1_cpcv_payload = build_n1_cpcv_payload(
        candidate_labels=oracle.candidate_labels,
        trading_calendar=sources["n1_calendar"],
        request=request,
    )
    progress.stage(
        "n1_label_interval_cpcv",
        started,
        ready_path_count=len(n1_cpcv_payload["paths"]),
    )

    started = time.monotonic()
    top20_symbols = sorted(
        rank_result.rankings.loc[rank_result.rankings["selection_effective_rank"] <= 20, "instrument"].unique()
    )
    candidate_daily = load_qlib_daily(
        top20_symbols,
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
    )
    candidate_static = load_static_factors(
        request.factor_data_root,
        columns=STATIC_FACTOR_COLUMNS,
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
        instruments=top20_symbols,
    )
    market_daily = load_qlib_daily(
        all_qlib_instruments(),
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
        fields=("$close", "$limit_up"),
    )
    static_all = load_static_factors(
        request.factor_data_root,
        columns=("l2_code_id", "sw2_close", "sw2_amount"),
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
    )
    candidate_suspend = load_suspend_rows(
        request.suspend_data_root,
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
        instruments=top20_symbols,
    )
    feature_result = build_meta_label_feature_matrix(
        rankings=rank_result.rankings,
        block_by_date=n1_cpcv_payload["block_by_date"],
        candidate_daily=candidate_daily,
        candidate_static=candidate_static,
        market_daily=market_daily,
        benchmark_daily=benchmark,
        suspend_rows=candidate_suspend,
        static_all=static_all,
        trading_calendar=sources["feature_calendar"],
        hmm_history_start="2023-12-01",
        runtime_cutoff=N1_DECISION_END.isoformat(),
        feature_schema_version=FEATURE_SCHEMA_VERSION,
    )
    del candidate_daily, candidate_static, market_daily, static_all, candidate_suspend
    gc.collect()
    progress.stage(
        "feature_schema_v2",
        started,
        feature_rows=len(feature_result.features),
        available_date_count=int((feature_result.coverage["status"] == "available").sum()),
    )

    started = time.monotonic()
    learnability = run_fixed_learnability_audit(
        features=feature_result.features,
        candidate_labels=oracle.candidate_labels,
        cpcv_payload=n1_cpcv_payload,
        benchmark_daily=benchmark,
        request=request,
    )
    progress.stage(
        "fixed_crossfitted_learnability",
        started,
        oof_row_count=len(learnability.oof_predictions),
        learnability_evaluable_days=len(learnability.daily),
        learnability_lift_bps=learnability.lift.point_estimate_bps,
        learnability_state=learnability.lift.evidence_state.value,
        evidence_sufficient=learnability.evidence_sufficient,
    )

    quadrant, typed_result, direction_ready, quadrant_reasons = build_quadrant_result(
        oracle_lift=oracle.perfect_top5_lift,
        learnability=learnability,
        oracle_evidence_sufficient=oracle.evidence_sufficient,
    )
    oracle_decision_use = (
        DecisionUse.DIRECTION_GATE
        if oracle.perfect_top5_lift.evidence_state != Tier1EvidenceState.INCONCLUSIVE and oracle.evidence_sufficient
        else DecisionUse.NAVIGATION_ONLY
    )
    learnability_decision_use = (
        DecisionUse.DIRECTION_GATE
        if learnability.lift.evidence_state != Tier1EvidenceState.INCONCLUSIVE and learnability.evidence_sufficient
        else DecisionUse.NAVIGATION_ONLY
    )
    oracle_result_class = _result_class(
        oracle.perfect_top5_lift.evidence_state,
        oracle_decision_use,
    )
    learnability_result_class = _result_class(
        learnability.lift.evidence_state,
        learnability_decision_use,
    )
    created_at = request.created_at
    oracle_receipt = build_oracle_receipt(
        request_sha256=request.request_sha256,
        decision_date_count=len(sources["decision_dates"]),
        universe_summary=oracle.universe_summary,
        recall_summary=oracle.recall_summary,
        rank_bucket_summary=oracle.rank_bucket_summary,
        perfect_top5_lift=oracle.perfect_top5_lift,
        intervention_support=oracle.intervention_support,
        evidence_sufficient=oracle.evidence_sufficient,
        evidence_reason_codes=oracle.evidence_reason_codes,
        result_class=oracle_result_class,
        decision_use=oracle_decision_use,
        sealed_holdout_accessed=False,
        created_at=created_at,
    )
    learnability_receipt = build_learnability_receipt(
        request_sha256=request.request_sha256,
        feature_schema_hash=request.feature_schema_hash,
        oof_row_count=len(learnability.oof_predictions),
        oof_predictions_per_row=request.learnability_spec.expected_oof_predictions_per_row,
        learnability_lift=learnability.lift,
        intervention_support=learnability.intervention_support,
        evidence_sufficient=learnability.evidence_sufficient,
        evidence_reason_codes=learnability.evidence_reason_codes,
        result_class=learnability_result_class,
        decision_use=learnability_decision_use,
        sealed_holdout_accessed=False,
        created_at=created_at,
    )
    quadrant_receipt = build_quadrant_receipt(
        request_sha256=request.request_sha256,
        oracle_receipt_sha256=oracle_receipt.receipt_sha256,
        learnability_receipt_sha256=learnability_receipt.receipt_sha256,
        point_quadrant=quadrant,
        typed_result=typed_result,
        direction_ready=direction_ready,
        reason_codes=quadrant_reasons,
        created_at=created_at,
    )
    started = time.monotonic()
    resource_report = progress.report()
    bundle_path = _publish_n1_bundle(
        request=request,
        environment=environment,
        source_receipt=sources["source_receipt"],
        rankings=rank_result.rankings,
        oracle=oracle,
        learnability=learnability,
        n1_cpcv_payload=n1_cpcv_payload,
        oracle_receipt=oracle_receipt,
        learnability_receipt=learnability_receipt,
        quadrant_receipt=quadrant_receipt,
        resource_report=resource_report,
        walk_forward_hmm_receipt=feature_result.walk_forward_hmm_receipt,
    )
    progress.stage("bundle_publish", started, bundle_id=bundle_path.name)
    delivery = _deliver_bundle(request=request, bundle_path=bundle_path)
    return {
        "status": "COMPLETE",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_id": bundle_path.name,
        "bundle_path": str(bundle_path),
        "oracle_state": oracle.perfect_top5_lift.evidence_state.value,
        "learnability_state": learnability.lift.evidence_state.value,
        "typed_quadrant": typed_result,
        "direction_ready": direction_ready,
        "sealed_holdout_accessed": False,
        "oracle_access": oracle_access,
        "learnability_access": learnability_access,
        "delivery": delivery,
        "resource_report": progress.report(),
        "backend_restart": "noop",
        "production_ddl_gate": "noop",
        "runtime_activation": "noop",
    }


def inspect_n1_bundle(bundle_path: str | Path) -> dict[str, Any]:
    bundle = _read_n1_bundle(Path(bundle_path))
    return {
        "status": "VALID",
        "bundle_id": bundle["manifest"]["bundle_id"],
        "request_sha256": bundle["manifest"]["request_sha256"],
        "typed_quadrant": bundle["quadrant"].typed_result,
        "direction_ready": bundle["quadrant"].direction_ready,
        "oracle_state": bundle["oracle"].perfect_top5_lift.evidence_state.value,
        "learnability_state": bundle["learnability"].learnability_lift.evidence_state.value,
        "sealed_holdout_accessed": False,
    }


def filter_prediction_frame_to_pit(
    frame: pd.DataFrame,
    snapshot: FrozenPitSnapshot,
) -> pd.DataFrame:
    required = {"trade_date", "instrument", "score"}
    if not required.issubset(frame.columns):
        _raise(
            "prediction frame cannot be PIT-filtered",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            missing_columns=sorted(required - set(frame.columns)),
        )
    indexed = frame.copy()
    indexed["trade_date"] = pd.to_datetime(indexed["trade_date"]).dt.normalize()
    indexed["instrument"] = indexed["instrument"].astype(str).str.upper()
    indexed = indexed.set_index(["trade_date", "instrument"])
    indexed.index = indexed.index.set_names(["datetime", "instrument"])
    filtered, _ = filter_frame_to_pit_spans(indexed, snapshot)
    result = filtered.reset_index().rename(columns={"datetime": "trade_date"})
    return (
        result[["trade_date", "instrument", "score"]].sort_values(["trade_date", "instrument"]).reset_index(drop=True)
    )


def build_tier1_full_universe_outcomes(
    *,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    pit_snapshot: FrozenPitSnapshot,
    trading_calendar: Sequence[pd.Timestamp],
    decision_dates: Sequence[pd.Timestamp],
    request: AdvisoryN1Tier1RequestV1,
) -> Tier1FullUniverseOutcomeResult:
    """Build the frozen N1 H20 outcome panel once for multi-arm diagnostics.

    The helper deliberately exposes the existing one-date N1 outcome engine;
    outcomes remain labels and must only be joined after each arm is ranked.
    """

    # ALGO-COMPLEXITY-001: this deliberately performs one bounded 386-date
    # pass over the frozen PIT snapshot. Market, benchmark and suspend inputs
    # are preloaded/batched once; each date emits at most the PIT member count,
    # and no arm-specific copy or database query occurs inside the loop.
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize()
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize()
    if decisions.empty or decisions[0].date() != N1_DECISION_START or decisions[-1].date() != N1_DECISION_END:
        _raise(
            "N1 full-universe outcome calendar differs from the frozen development window",
            "ADVISORY_N1_LABEL_CLOCK_INVALID",
        )
    positions = {value: index for index, value in enumerate(calendar)}
    if any(value not in positions for value in decisions):
        _raise(
            "one or more N1 decisions are absent from the Qlib calendar",
            "ADVISORY_N1_LABEL_CLOCK_INVALID",
        )
    market = _normalize_market_frame(daily)
    benchmark_open = _benchmark_series(benchmark_daily, "open")
    suspended_by_date = _suspend_map(suspend_rows)
    eligible_by_date = _eligible_symbols_by_date(pit_snapshot, calendar)
    outcome_chunks: list[pd.DataFrame] = []
    coverage_rows: list[dict[str, Any]] = []
    for decision in decisions:
        outcome = _build_one_date_outcomes(
            decision=decision,
            calendar=calendar,
            positions=positions,
            eligible_by_date=eligible_by_date,
            market=market,
            benchmark_open=benchmark_open,
            suspended_by_date=suspended_by_date,
            request=request,
        )
        known = outcome["outcome_known"].astype(bool)
        matured = outcome["outcome_status"].eq("MATURED")
        known_fraction = float(known.mean()) if len(outcome) else 0.0
        coverage_rows.append(
            {
                "decision_as_of_trade_date": decision,
                "pit_member_count": len(outcome),
                "known_outcome_count": int(known.sum()),
                "matured_outcome_count": int(matured.sum()),
                "not_entered_count": int(outcome["outcome_status"].isin(_KNOWN_CASH_STATUSES).sum()),
                "unknown_outcome_count": int((~known).sum()),
                "known_outcome_fraction": known_fraction,
                "status": (
                    "AVAILABLE"
                    if known_fraction >= request.outcome_policy.minimum_full_universe_known_fraction
                    and int(matured.sum()) >= request.outcome_policy.winner_count
                    else "DATA_UNAVAILABLE"
                ),
            }
        )
        outcome_chunks.append(outcome)
    return Tier1FullUniverseOutcomeResult(
        outcomes=pd.concat(outcome_chunks, ignore_index=True)
        .sort_values(["decision_as_of_trade_date", "instrument"])
        .reset_index(drop=True),
        coverage=pd.DataFrame(coverage_rows).sort_values("decision_as_of_trade_date").reset_index(drop=True),
    )


def build_tier1_benchmark_regimes(
    benchmark_daily: pd.DataFrame,
    decisions: Sequence[pd.Timestamp],
) -> dict[pd.Timestamp, str]:
    """Expose N1's frozen T-visible regime classification for diagnostics."""

    return _benchmark_regimes(
        benchmark_daily,
        pd.DatetimeIndex(pd.to_datetime(list(decisions))).normalize(),
    )


def load_verified_n1_sources(request: AdvisoryN1Tier1RequestV1) -> dict[str, Any]:
    """Public read-only adapter for diagnostics bound to the exact N1 request."""

    return _load_and_verify_n1_sources(request)


def build_tier1_outcomes_and_oracle(
    *,
    rankings: pd.DataFrame,
    daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    pit_snapshot: FrozenPitSnapshot,
    trading_calendar: Sequence[pd.Timestamp],
    decision_dates: Sequence[pd.Timestamp],
    request: AdvisoryN1Tier1RequestV1,
    common_prediction_count_by_date: Mapping[pd.Timestamp, int] | None = None,
) -> Tier1OracleResult:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize()
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize()
    if decisions.empty or decisions[0].date() != N1_DECISION_START or decisions[-1].date() != N1_DECISION_END:
        _raise(
            "N1 candidate decision calendar differs from the frozen development window",
            "ADVISORY_N1_LABEL_CLOCK_INVALID",
        )
    positions = {value: index for index, value in enumerate(calendar)}
    if any(value not in positions for value in decisions):
        _raise(
            "one or more N1 decisions are absent from the Qlib calendar",
            "ADVISORY_N1_LABEL_CLOCK_INVALID",
        )
    market = _normalize_market_frame(daily)
    benchmark_open = _benchmark_series(benchmark_daily, "open")
    suspended_by_date = _suspend_map(suspend_rows)
    eligible_by_date = _eligible_symbols_by_date(pit_snapshot, calendar)
    rank_frames = {
        value: group.sort_values("selection_effective_rank")
        for value, group in rankings.groupby(
            pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize(),
            sort=False,
        )
    }
    candidate_chunks: list[pd.DataFrame] = []
    recall_rows: list[dict[str, Any]] = []
    coverage_rows: list[dict[str, Any]] = []
    oracle_rows: list[dict[str, Any]] = []
    full_known_fractions: list[float] = []
    for decision in decisions:
        rank_frame = rank_frames.get(decision)
        if rank_frame is None or len(rank_frame) != 50:
            _raise(
                "N1 ranking does not contain exact Top50 for one decision",
                "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
                decision_date=decision.date().isoformat(),
                row_count=0 if rank_frame is None else len(rank_frame),
            )
        outcome = _build_one_date_outcomes(
            decision=decision,
            calendar=calendar,
            positions=positions,
            eligible_by_date=eligible_by_date,
            market=market,
            benchmark_open=benchmark_open,
            suspended_by_date=suspended_by_date,
            request=request,
        )
        known = outcome["outcome_known"].astype(bool)
        matured = outcome["outcome_status"] == "MATURED"
        known_fraction = float(known.mean()) if len(outcome) else 0.0
        full_known_fractions.append(known_fraction)
        date_evaluable = bool(
            known_fraction >= request.outcome_policy.minimum_full_universe_known_fraction
            and int(matured.sum()) >= request.outcome_policy.winner_count
        )
        winners = (
            outcome.loc[matured]
            .sort_values(
                ["economic_net_excess_bps", "instrument"],
                ascending=[False, True],
            )
            .head(request.outcome_policy.winner_count)["instrument"]
            .tolist()
            if date_evaluable
            else []
        )
        candidate_rank_frame = rank_frame
        if "target_trade_date" in rank_frame.columns:
            rank_targets = pd.DatetimeIndex(pd.to_datetime(rank_frame["target_trade_date"])).normalize().unique()
            outcome_targets = pd.DatetimeIndex(pd.to_datetime(outcome["target_trade_date"])).normalize().unique()
            if len(rank_targets) != 1 or len(outcome_targets) != 1 or rank_targets[0] != outcome_targets[0]:
                _raise(
                    "parent ranking and N1 outcome disagree on the T+1 target date",
                    "ADVISORY_N1_LABEL_CLOCK_INVALID",
                    decision_date=decision.date().isoformat(),
                    ranking_targets=[item.date().isoformat() for item in rank_targets],
                    outcome_targets=[item.date().isoformat() for item in outcome_targets],
                )
            candidate_rank_frame = rank_frame.drop(columns=["target_trade_date"])
        candidate = candidate_rank_frame.merge(
            outcome,
            on=["decision_as_of_trade_date", "instrument"],
            how="left",
            validate="one_to_one",
        )
        missing_universe = candidate["outcome_status"].isna()
        if missing_universe.any():
            candidate.loc[missing_universe, "outcome_status"] = "DATA_UNAVAILABLE_UNIVERSE"
            candidate.loc[missing_universe, "outcome_known"] = False
            candidate.loc[missing_universe, "slot_return_bps"] = np.nan
        candidate_chunks.append(candidate)
        top_sets = {
            depth: set(candidate.loc[candidate["selection_effective_rank"] <= depth, "instrument"].astype(str))
            for depth in (20, 40, 50)
        }
        recall_row: dict[str, Any] = {
            "decision_as_of_trade_date": decision,
            "status": "AVAILABLE" if date_evaluable else "DATA_UNAVAILABLE",
            "winner_count": len(winners),
            "winner_instruments": winners,
        }
        for depth in (20, 40, 50):
            overlap = len(set(winners) & top_sets[depth])
            recall_row[f"top{depth}_winner_count"] = overlap
            recall_row[f"top{depth}_winner_recall"] = overlap / len(winners) if winners else np.nan
        recall_rows.append(recall_row)
        coverage_rows.append(
            {
                "decision_as_of_trade_date": decision,
                "pit_member_count": len(outcome),
                "common_parent_prediction_count": int((common_prediction_count_by_date or {}).get(decision, 0)),
                "known_outcome_count": int(known.sum()),
                "matured_outcome_count": int(matured.sum()),
                "not_entered_count": int(outcome["outcome_status"].isin(_KNOWN_CASH_STATUSES).sum()),
                "unknown_outcome_count": int((~known).sum()),
                "known_outcome_fraction": known_fraction,
                "status": "AVAILABLE" if date_evaluable else "DATA_UNAVAILABLE",
            }
        )
        top20 = candidate[candidate["selection_effective_rank"] <= 20].copy()
        top20_known = bool(top20["outcome_known"].fillna(False).all())
        if top20_known:
            baseline = top20[top20["selection_effective_rank"] <= 5]
            perfect = top20.sort_values(
                ["slot_return_bps", "selection_effective_rank", "instrument"],
                ascending=[False, True, True],
            ).head(5)
            baseline_symbols = tuple(sorted(baseline["instrument"].astype(str)))
            perfect_symbols = tuple(sorted(perfect["instrument"].astype(str)))
            baseline_return = float(baseline["slot_return_bps"].sum() / 5.0)
            perfect_return = float(perfect["slot_return_bps"].sum() / 5.0)
            oracle_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "baseline_top5_return_bps": baseline_return,
                    "perfect_top5_return_bps": perfect_return,
                    "perfect_top5_lift_bps": perfect_return - baseline_return,
                    "baseline_instruments": baseline_symbols,
                    "perfect_instruments": perfect_symbols,
                    "intervened": baseline_symbols != perfect_symbols,
                }
            )
    candidate_labels = pd.concat(candidate_chunks, ignore_index=True)
    recall_daily = pd.DataFrame(recall_rows)
    outcome_coverage = pd.DataFrame(coverage_rows)
    oracle_daily = pd.DataFrame(oracle_rows).sort_values("decision_as_of_trade_date").reset_index(drop=True)
    if len(oracle_daily) < 60:
        _raise(
            "N1 produced too few evaluable Top20 oracle days",
            "ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT",
            evaluable_day_count=len(oracle_daily),
        )
    inference = infer_daily_lift(
        oracle_daily["perfect_top5_lift_bps"].to_numpy(dtype=float),
        request=request,
    )
    regimes = _benchmark_regimes(
        benchmark_daily,
        pd.DatetimeIndex(oracle_daily["decision_as_of_trade_date"]),
    )
    oracle_daily["regime"] = oracle_daily["decision_as_of_trade_date"].map(regimes)
    intervention_support = _intervention_support(oracle_daily, request=request)
    oracle_power_sufficient = bool(
        inference.mde_bps <= max(inference.point_estimate_bps, inference.economic_threshold_bps)
    )
    evidence_reasons = list(intervention_support.reason_codes)
    if not oracle_power_sufficient:
        evidence_reasons.append("EXPLORATORY_UNDERPOWERED")
    evidence_sufficient = intervention_support.support_sufficient and oracle_power_sufficient
    recall_summary = _summarize_recall(recall_daily, request=request)
    rank_bucket_summary = _summarize_rank_buckets(candidate_labels)
    universe_summary = {
        "pit_snapshot_spans_sha256": pit_snapshot.spans_sha256,
        "pit_snapshot_span_count": len(pit_snapshot.spans),
        "pit_snapshot_instrument_count": pit_snapshot.unique_instruments,
        "decision_date_count": len(decisions),
        "mean_pit_member_count": float(outcome_coverage["pit_member_count"].mean()),
        "mean_common_parent_prediction_count": float(outcome_coverage["common_parent_prediction_count"].mean()),
        "mean_known_outcome_fraction": float(np.mean(full_known_fractions)),
        "minimum_known_outcome_fraction": float(np.min(full_known_fractions)),
        "oracle_evaluable_day_count": len(oracle_daily),
    }
    return Tier1OracleResult(
        candidate_labels=candidate_labels,
        oracle_daily=oracle_daily,
        recall_daily=recall_daily,
        outcome_coverage=outcome_coverage,
        recall_summary=recall_summary,
        rank_bucket_summary=rank_bucket_summary,
        universe_summary=universe_summary,
        perfect_top5_lift=inference,
        intervention_support=intervention_support,
        evidence_sufficient=evidence_sufficient,
        evidence_reason_codes=tuple(sorted(set(evidence_reasons))),
    )


def run_fixed_learnability_audit(
    *,
    features: pd.DataFrame,
    candidate_labels: pd.DataFrame,
    cpcv_payload: Mapping[str, Any],
    benchmark_daily: pd.DataFrame,
    request: AdvisoryN1Tier1RequestV1,
) -> Tier1LearnabilityResult:
    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.impute import SimpleImputer
        from sklearn.linear_model import Ridge
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder, StandardScaler
    except ImportError as exc:
        _raise(
            "scikit-learn is unavailable for the frozen learnability audit",
            "ADVISORY_N1_CROSSFIT_INVALID",
            error_type=type(exc).__name__,
        )
    required_identity = {
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
    }
    missing_features = set(MODEL_FEATURE_COLUMNS) - set(features)
    if missing_features or not required_identity.issubset(features):
        _raise(
            "learnability feature matrix does not match schema v2",
            "ADVISORY_N1_CROSSFIT_INVALID",
            missing_columns=sorted(missing_features | (required_identity - set(features))),
        )
    labels = candidate_labels[candidate_labels["selection_effective_rank"] <= 20].copy()
    label_columns = [
        "decision_as_of_trade_date",
        "instrument",
        "slot_return_bps",
        "outcome_known",
        "selection_effective_rank",
    ]
    labels = labels[label_columns]
    matrix = features.merge(
        labels,
        on=["decision_as_of_trade_date", "instrument", "selection_effective_rank"],
        how="inner",
        validate="one_to_one",
    )
    if len(matrix) != len(labels) or len(matrix) != 20 * 386:
        _raise(
            "learnability features and labels do not have exact Top20 coverage",
            "ADVISORY_N1_CROSSFIT_INVALID",
            feature_rows=len(features),
            label_rows=len(labels),
            merged_rows=len(matrix),
        )
    matrix["decision_as_of_trade_date"] = pd.to_datetime(matrix["decision_as_of_trade_date"]).dt.normalize()
    paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if len(paths) != request.learnability_spec.expected_ready_path_count:
        _raise(
            "learnability audit requires all 28 READY paths",
            "ADVISORY_N1_CROSSFIT_INVALID",
            ready_path_count=len(paths),
        )
    categorical = [column for column in CATEGORICAL_FEATURE_COLUMNS if column in MODEL_FEATURE_COLUMNS]
    numeric = [column for column in MODEL_FEATURE_COLUMNS if column not in categorical]
    if any(column in {"slot_return_bps", "economic_net_excess_bps"} for column in MODEL_FEATURE_COLUMNS):
        _raise(
            "future outcome leaked into the frozen feature schema",
            "ADVISORY_N1_CROSSFIT_INVALID",
        )
    predictions: list[pd.DataFrame] = []
    for path in paths:
        train_dates = pd.DatetimeIndex(pd.to_datetime(path["train_dates"])).normalize()
        validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
        if set(train_dates) & set(validation_dates):
            _raise(
                "CPCV train and validation dates overlap",
                "ADVISORY_N1_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        train_mask = matrix["decision_as_of_trade_date"].isin(train_dates) & matrix["outcome_known"].astype(bool)
        validation_mask = matrix["decision_as_of_trade_date"].isin(validation_dates)
        train = matrix.loc[train_mask]
        validation = matrix.loc[validation_mask]
        if train.empty or validation.empty:
            _raise(
                "CPCV path has no train or validation rows",
                "ADVISORY_N1_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        numeric_pipeline = Pipeline(
            steps=[
                (
                    "imputer",
                    SimpleImputer(strategy="median", keep_empty_features=True),
                ),
                ("scaler", StandardScaler()),
            ]
        )
        categorical_pipeline = Pipeline(
            steps=[
                ("imputer", SimpleImputer(strategy="most_frequent")),
                (
                    "encoder",
                    OneHotEncoder(
                        handle_unknown="ignore",
                        sparse_output=False,
                        dtype=np.float64,
                    ),
                ),
            ]
        )
        preprocess = ColumnTransformer(
            transformers=[
                ("numeric", numeric_pipeline, numeric),
                ("categorical", categorical_pipeline, categorical),
            ],
            sparse_threshold=0.0,
        )
        model = Pipeline(
            steps=[
                ("preprocess", preprocess),
                (
                    "ridge",
                    Ridge(
                        alpha=request.learnability_spec.alpha,
                        solver=request.learnability_spec.solver,
                        fit_intercept=request.learnability_spec.fit_intercept,
                    ),
                ),
            ]
        )
        model.fit(train[list(MODEL_FEATURE_COLUMNS)], train["slot_return_bps"].astype(float))
        predicted = model.predict(validation[list(MODEL_FEATURE_COLUMNS)])
        if not np.isfinite(predicted).all():
            _raise(
                "learnability model produced non-finite OOF values",
                "ADVISORY_N1_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        item = validation[["decision_as_of_trade_date", "instrument", "selection_effective_rank"]].copy()
        item["path_id"] = str(path["path_id"])
        item["predicted_slot_return_bps"] = predicted
        predictions.append(item)
    raw_oof = pd.concat(predictions, ignore_index=True)
    keys = ["decision_as_of_trade_date", "instrument", "selection_effective_rank"]
    counts = raw_oof.groupby(keys).size()
    expected_count = request.learnability_spec.expected_oof_predictions_per_row
    if len(counts) != len(matrix) or not (counts == expected_count).all():
        _raise(
            "learnability OOF multiplicity differs from the frozen 28-path design",
            "ADVISORY_N1_CROSSFIT_INVALID",
            row_count=len(counts),
            min_count=int(counts.min()) if len(counts) else 0,
            max_count=int(counts.max()) if len(counts) else 0,
        )
    oof = (
        raw_oof.groupby(keys, as_index=False)
        .agg(
            predicted_slot_return_bps=("predicted_slot_return_bps", "mean"),
            oof_prediction_count=("path_id", "count"),
        )
        .merge(labels, on=keys, how="left", validate="one_to_one")
    )
    daily_rows: list[dict[str, Any]] = []
    for decision, group in oof.groupby("decision_as_of_trade_date", sort=True):
        if len(group) != 20 or not group["outcome_known"].astype(bool).all():
            continue
        baseline = group[group["selection_effective_rank"] <= 5]
        selected = group.sort_values(
            ["predicted_slot_return_bps", "selection_effective_rank", "instrument"],
            ascending=[False, True, True],
        ).head(5)
        baseline_symbols = tuple(sorted(baseline["instrument"].astype(str)))
        selected_symbols = tuple(sorted(selected["instrument"].astype(str)))
        baseline_return = float(baseline["slot_return_bps"].sum() / 5.0)
        selected_return = float(selected["slot_return_bps"].sum() / 5.0)
        daily_rows.append(
            {
                "decision_as_of_trade_date": pd.Timestamp(decision).normalize(),
                "baseline_top5_return_bps": baseline_return,
                "learnability_top5_return_bps": selected_return,
                "learnability_lift_bps": selected_return - baseline_return,
                "baseline_instruments": baseline_symbols,
                "learnability_instruments": selected_symbols,
                "intervened": baseline_symbols != selected_symbols,
            }
        )
    daily = pd.DataFrame(daily_rows).sort_values("decision_as_of_trade_date").reset_index(drop=True)
    if len(daily) < 60:
        _raise(
            "learnability audit produced too few evaluable days",
            "ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT",
            evaluable_day_count=len(daily),
        )
    regimes = _benchmark_regimes(
        benchmark_daily,
        pd.DatetimeIndex(daily["decision_as_of_trade_date"]),
    )
    daily["regime"] = daily["decision_as_of_trade_date"].map(regimes)
    support = _intervention_support(daily, request=request)
    lift = infer_daily_lift(daily["learnability_lift_bps"].to_numpy(dtype=float), request=request)
    evidence_sufficient = bool(
        support.support_sufficient and lift.mde_bps <= max(lift.point_estimate_bps, lift.economic_threshold_bps)
    )
    reasons = list(support.reason_codes)
    if lift.mde_bps > max(lift.point_estimate_bps, lift.economic_threshold_bps):
        reasons.append("EXPLORATORY_UNDERPOWERED")
    return Tier1LearnabilityResult(
        oof_predictions=oof,
        daily=daily,
        lift=lift,
        intervention_support=support,
        evidence_sufficient=evidence_sufficient,
        evidence_reason_codes=tuple(sorted(set(reasons))),
    )


def build_n1_cpcv_payload(
    *,
    candidate_labels: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    request: AdvisoryN1Tier1RequestV1,
) -> dict[str, Any]:
    top20 = candidate_labels[candidate_labels["selection_effective_rank"] <= 20].copy()
    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "effective_exit_trade_date",
        "outcome_known",
        "slot_return_bps",
    }
    if not required.issubset(top20):
        _raise(
            "N1 labels cannot define their information intervals",
            "ADVISORY_N1_CROSSFIT_INVALID",
            missing_columns=sorted(required - set(top20)),
        )
    known = top20["outcome_known"].fillna(False).astype(bool)
    effective_exit = pd.to_datetime(top20["effective_exit_trade_date"])
    entry = pd.to_datetime(top20["target_trade_date"])
    top20["label_information_start"] = entry
    top20["label_information_end"] = effective_exit.where(effective_exit.notna(), entry)
    top20["label_status"] = np.where(known, "MATURED", "DATA_UNAVAILABLE")
    top20["take_label"] = np.where(
        known & (pd.to_numeric(top20["slot_return_bps"], errors="coerce") > 0),
        1,
        0,
    )
    result = build_policy_cpcv_paths(
        top20,
        split_policy=request.split_policy,
        trading_calendar=trading_calendar,
        request_sha256=request.request_sha256,
    )
    paths = tuple(result.paths)
    if len(paths) != request.learnability_spec.expected_ready_path_count or any(
        item.get("status") != "READY" for item in paths
    ):
        _raise(
            "N1 label intervals do not produce the frozen 28 READY paths",
            "ADVISORY_N1_CROSSFIT_INVALID",
            path_count=len(paths),
            status_counts=pd.Series([str(item.get("status")) for item in paths]).value_counts().to_dict(),
        )
    return {
        "schema_version": "advisory_n1_label_interval_cpcv_v1",
        "split_policy": request.split_policy.model_dump(mode="json"),
        "request_sha256": request.request_sha256,
        "block_by_date": result.block_by_date,
        "paths": paths,
    }


def infer_daily_lift(
    values: np.ndarray | Sequence[float],
    *,
    request: AdvisoryN1Tier1RequestV1,
) -> Tier1MetricInferenceV1:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if array.size < 2:
        _raise(
            "daily lift inference requires at least two finite observations",
            "ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT",
            observation_count=int(array.size),
        )
    policy = request.inference_policy
    rng = np.random.default_rng(policy.random_seed)
    block_length = min(policy.block_length_trading_days, len(array))
    block_count = math.ceil(len(array) / block_length)
    sample_means = np.empty(policy.bootstrap_repetitions, dtype=float)
    offsets = np.arange(block_length)
    for index in range(policy.bootstrap_repetitions):
        starts = rng.integers(0, len(array), size=block_count)
        positions = ((starts[:, None] + offsets[None, :]) % len(array)).reshape(-1)
        sample_means[index] = float(array[positions[: len(array)]].mean())
    point = float(array.mean())
    lower, upper = np.quantile(sample_means, [0.025, 0.975]).tolist()
    lower = min(float(lower), point)
    upper = max(float(upper), point)
    standard_error = float(sample_means.std(ddof=1))
    mde = float((_Z_975 + _Z_80) * standard_error)
    threshold = request.outcome_policy.minimum_economic_benefit_bps
    state = (
        Tier1EvidenceState.HIGH
        if lower > threshold
        else Tier1EvidenceState.LOW
        if upper <= threshold
        else Tier1EvidenceState.INCONCLUSIVE
    )
    return Tier1MetricInferenceV1(
        point_estimate_bps=point,
        confidence_lower_bps=lower,
        confidence_upper_bps=upper,
        bootstrap_standard_error_bps=standard_error,
        mde_bps=mde,
        economic_threshold_bps=threshold,
        evidence_state=state,
        evaluated_day_count=len(array),
    )


def build_quadrant_result(
    *,
    oracle_lift: Tier1MetricInferenceV1,
    learnability: Tier1LearnabilityResult,
    oracle_evidence_sufficient: bool,
) -> tuple[Tier1Quadrant, str, bool, tuple[str, ...]]:
    theoretical_high = _point_high(oracle_lift)
    learnability_high = _point_high(learnability.lift)
    if theoretical_high and learnability_high:
        quadrant = Tier1Quadrant.THEORETICAL_HIGH_LEARNABILITY_HIGH
    elif theoretical_high:
        quadrant = Tier1Quadrant.THEORETICAL_HIGH_LEARNABILITY_LOW
    elif learnability_high:
        quadrant = Tier1Quadrant.THEORETICAL_LOW_LEARNABILITY_HIGH_ANOMALY
    else:
        quadrant = Tier1Quadrant.THEORETICAL_LOW_LEARNABILITY_LOW
    decisive = (
        oracle_lift.evidence_state != Tier1EvidenceState.INCONCLUSIVE
        and learnability.lift.evidence_state != Tier1EvidenceState.INCONCLUSIVE
        and oracle_evidence_sufficient
        and learnability.evidence_sufficient
    )
    reasons: list[str] = []
    if oracle_lift.evidence_state == Tier1EvidenceState.INCONCLUSIVE:
        reasons.append("ORACLE_INTERVAL_INCONCLUSIVE")
    if not oracle_evidence_sufficient:
        reasons.append("ORACLE_INSUFFICIENT_POWER_OR_INTERVENTION_SUPPORT")
    if learnability.lift.evidence_state == Tier1EvidenceState.INCONCLUSIVE:
        reasons.append("LEARNABILITY_INTERVAL_INCONCLUSIVE")
    reasons.extend(learnability.evidence_reason_codes)
    typed = quadrant.value if decisive else f"INCONCLUSIVE__{quadrant.value}"
    return quadrant, typed, decisive, tuple(sorted(set(reasons)))


def _build_one_date_outcomes(
    *,
    decision: pd.Timestamp,
    calendar: pd.DatetimeIndex,
    positions: Mapping[pd.Timestamp, int],
    eligible_by_date: Mapping[pd.Timestamp, tuple[str, ...]],
    market: pd.DataFrame,
    benchmark_open: pd.Series,
    suspended_by_date: Mapping[pd.Timestamp, frozenset[str]],
    request: AdvisoryN1Tier1RequestV1,
) -> pd.DataFrame:
    policy = request.outcome_policy
    decision_position = positions[decision]
    entry_position = decision_position + policy.entry_offset_trading_days
    # H20 counts the entry session as holding session 1: decision T, entry T+1,
    # planned exit T+20.  Adding 20 again after entry would incorrectly be H21.
    planned_exit_position = entry_position + policy.holding_period_trading_days - 1
    if entry_position >= len(calendar) or planned_exit_position >= len(calendar):
        _raise(
            "N1 label clock extends beyond the frozen data cutoff",
            "ADVISORY_N1_LABEL_CLOCK_INVALID",
            decision_date=decision.date().isoformat(),
        )
    entry_date = calendar[entry_position]
    planned_exit_date = calendar[planned_exit_position]
    decision_eligible = eligible_by_date.get(decision, ())
    entry_eligible = set(eligible_by_date.get(entry_date, ()))
    if not decision_eligible:
        _raise(
            "N1 PIT universe is empty for a decision date",
            "ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT",
            decision_date=decision.date().isoformat(),
        )
    frame = pd.DataFrame({"instrument": list(decision_eligible)}).set_index("instrument")
    frame["decision_as_of_trade_date"] = decision
    frame["target_trade_date"] = entry_date
    frame["planned_exit_trade_date"] = planned_exit_date
    frame["effective_exit_trade_date"] = pd.NaT
    frame["outcome_status"] = "PENDING_EXIT"
    frame["entry_price"] = np.nan
    frame["exit_price"] = np.nan
    frame["gross_excess_return_bps"] = np.nan
    frame["economic_net_excess_bps"] = np.nan
    not_entry_eligible = ~frame.index.isin(entry_eligible)
    frame.loc[not_entry_eligible, "outcome_status"] = "NOT_ELIGIBLE_ENTRY_DATE"
    suspended_entry = suspended_by_date.get(entry_date, frozenset())
    entry_suspended = frame.index.isin(suspended_entry) & ~not_entry_eligible
    frame.loc[entry_suspended, "outcome_status"] = "NOT_ENTERED_SUSPENDED"
    entry_cross = _market_cross_section(market, entry_date)
    entry_rows = entry_cross.reindex(frame.index)
    entry_open = pd.to_numeric(entry_rows.get("open"), errors="coerce")
    missing_entry = frame["outcome_status"].eq("PENDING_EXIT") & (~np.isfinite(entry_open) | (entry_open <= 0))
    frame.loc[missing_entry, "outcome_status"] = "DATA_UNAVAILABLE_ENTRY"
    one_price_up = _one_price_limit_mask(entry_rows, direction="up")
    blocked_up = frame["outcome_status"].eq("PENDING_EXIT") & one_price_up
    frame.loc[blocked_up, "outcome_status"] = "NOT_ENTERED_LIMIT_UP"
    pending = frame["outcome_status"].eq("PENDING_EXIT")
    frame.loc[pending, "entry_price"] = entry_open.loc[pending]
    benchmark_entry = _finite_series_value(benchmark_open, entry_date)
    if benchmark_entry is None or benchmark_entry <= 0:
        frame.loc[pending, "outcome_status"] = "DATA_UNAVAILABLE_BENCHMARK_ENTRY"
        pending = frame["outcome_status"].eq("PENDING_EXIT")
    for offset in range(policy.max_exit_defer_trading_days + 1):
        unresolved = frame["outcome_status"].eq("PENDING_EXIT")
        if not unresolved.any():
            break
        position = planned_exit_position + offset
        if position >= len(calendar):
            break
        exit_date = calendar[position]
        exit_suspended = suspended_by_date.get(exit_date, frozenset())
        can_inspect = unresolved & ~frame.index.isin(exit_suspended)
        if not can_inspect.any():
            continue
        exit_cross = _market_cross_section(market, exit_date)
        exit_rows = exit_cross.reindex(frame.index)
        exit_open = pd.to_numeric(exit_rows.get("open"), errors="coerce")
        invalid = can_inspect & (~np.isfinite(exit_open) | (exit_open <= 0))
        frame.loc[invalid, "outcome_status"] = "DATA_UNAVAILABLE_EXIT"
        one_price_down = _one_price_limit_mask(exit_rows, direction="down")
        executable = can_inspect & ~invalid & ~one_price_down
        if executable.any():
            frame.loc[executable, "exit_price"] = exit_open.loc[executable]
            frame.loc[executable, "effective_exit_trade_date"] = exit_date
            frame.loc[executable, "outcome_status"] = "MATURED"
    unresolved = frame["outcome_status"].eq("PENDING_EXIT")
    frame.loc[unresolved, "outcome_status"] = "CENSORED_EXIT_UNEXECUTABLE"
    matured = frame["outcome_status"].eq("MATURED")
    round_trip = request.cost_policy.buy_cost_bps + request.cost_policy.sell_cost_bps
    for exit_date, indexes in frame.loc[matured].groupby("effective_exit_trade_date").groups.items():
        benchmark_exit = _finite_series_value(benchmark_open, pd.Timestamp(exit_date))
        if benchmark_exit is None or benchmark_exit <= 0:
            frame.loc[indexes, "outcome_status"] = "DATA_UNAVAILABLE_BENCHMARK_EXIT"
            continue
        stock_return = (frame.loc[indexes, "exit_price"] / frame.loc[indexes, "entry_price"] - 1.0) * 10_000.0
        benchmark_return = (benchmark_exit / benchmark_entry - 1.0) * 10_000.0
        gross_excess = stock_return - benchmark_return
        frame.loc[indexes, "gross_excess_return_bps"] = gross_excess
        frame.loc[indexes, "economic_net_excess_bps"] = gross_excess - round_trip - policy.capacity_haircut_bps
    frame["outcome_known"] = frame["outcome_status"].eq("MATURED") | frame["outcome_status"].isin(_KNOWN_CASH_STATUSES)
    frame["slot_return_bps"] = np.where(
        frame["outcome_status"].eq("MATURED"),
        frame["economic_net_excess_bps"],
        np.where(frame["outcome_status"].isin(_KNOWN_CASH_STATUSES), 0.0, np.nan),
    )
    return frame.reset_index()


def _normalize_market_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame.index, pd.MultiIndex) or set(frame.index.names) != {
        "datetime",
        "instrument",
    }:
        _raise(
            "N1 daily market frame must use datetime/instrument index",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
        )
    normalized = frame.reset_index()
    normalized["datetime"] = pd.to_datetime(normalized["datetime"]).dt.normalize()
    normalized["instrument"] = normalized["instrument"].astype(str).str.upper()
    if normalized.duplicated(["datetime", "instrument"]).any():
        _raise(
            "N1 daily market frame contains duplicate rows",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
        )
    return normalized.set_index(["datetime", "instrument"]).sort_index()


def _market_cross_section(market: pd.DataFrame, trade_date: pd.Timestamp) -> pd.DataFrame:
    try:
        result = market.xs(pd.Timestamp(trade_date).normalize(), level="datetime")
    except KeyError:
        return pd.DataFrame(columns=market.columns)
    return result[~result.index.duplicated(keep=False)]


def _one_price_limit_mask(frame: pd.DataFrame, *, direction: str) -> pd.Series:
    index = frame.index
    columns = (
        f"limit_{direction}",
        "factor",
        f"{direction}_limit_price",
        "low" if direction == "up" else "high",
    )
    if any(column not in frame.columns for column in columns):
        return pd.Series(False, index=index, dtype=bool)
    flag = pd.to_numeric(frame[columns[0]], errors="coerce")
    factor = pd.to_numeric(frame[columns[1]], errors="coerce")
    limit_price = pd.to_numeric(frame[columns[2]], errors="coerce")
    observed = pd.to_numeric(frame[columns[3]], errors="coerce")
    complete = flag.notna() & factor.notna() & limit_price.notna() & observed.notna()
    adjusted = limit_price * factor
    comparison = observed >= adjusted - 1e-10 if direction == "up" else observed <= adjusted + 1e-10
    return pd.Series(complete & (flag > 0) & comparison, index=index).fillna(False)


def _benchmark_series(frame: pd.DataFrame, column: str) -> pd.Series:
    market = _normalize_market_frame(frame)
    if column not in market.columns:
        _raise(
            "benchmark frame is missing a required column",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            column=column,
        )
    reset = market.reset_index()
    counts = reset.groupby("datetime").size()
    if (counts != 1).any():
        _raise(
            "benchmark frame must have exactly one row per date",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
        )
    return reset.set_index("datetime")[column].sort_index()


def _suspend_map(frame: pd.DataFrame) -> dict[pd.Timestamp, frozenset[str]]:
    if frame.empty:
        return {}
    required = {"trade_date", "instrument"}
    if not required.issubset(frame):
        _raise(
            "suspend frame schema is invalid",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
        )
    normalized = frame.copy()
    normalized["trade_date"] = pd.to_datetime(normalized["trade_date"]).dt.normalize()
    normalized["instrument"] = normalized["instrument"].astype(str).str.upper()
    return {value: frozenset(group["instrument"]) for value, group in normalized.groupby("trade_date", sort=False)}


def _eligible_symbols_by_date(
    snapshot: FrozenPitSnapshot,
    calendar: pd.DatetimeIndex,
) -> dict[pd.Timestamp, tuple[str, ...]]:
    mutable: dict[pd.Timestamp, list[str]] = {value: [] for value in calendar}
    for span in snapshot.spans:
        mask = (calendar.date >= span.eligible_start) & (calendar.date <= span.eligible_end)
        for value in calendar[mask]:
            mutable[value].append(span.ts_code)
    return {value: tuple(sorted(symbols)) for value, symbols in mutable.items()}


def _summarize_recall(
    recall_daily: pd.DataFrame,
    *,
    request: AdvisoryN1Tier1RequestV1,
) -> dict[str, Any]:
    available = recall_daily[recall_daily["status"] == "AVAILABLE"]
    if available.empty:
        _raise(
            "N1 winner recall has no evaluable dates",
            "ADVISORY_N1_OUTCOME_COVERAGE_INSUFFICIENT",
        )
    summary: dict[str, Any] = {"evaluated_day_count": len(available)}
    for depth in (20, 40, 50):
        values = available[f"top{depth}_winner_recall"].to_numpy(dtype=float)
        interval = _moving_block_interval(
            values,
            block_length=request.inference_policy.block_length_trading_days,
            repetitions=request.inference_policy.bootstrap_repetitions,
            seed=request.inference_policy.random_seed + depth,
        )
        summary[f"top{depth}"] = {
            "mean_winner_recall": float(values.mean()),
            "confidence_lower": interval[0],
            "confidence_upper": interval[1],
            "captured_winner_count": int(available[f"top{depth}_winner_count"].sum()),
            "total_winner_count": int(available["winner_count"].sum()),
        }
    return summary


def _summarize_rank_buckets(candidate_labels: pd.DataFrame) -> tuple[dict[str, Any], ...]:
    buckets = ((1, 5), (6, 10), (11, 20), (21, 40), (41, 50))
    rows: list[dict[str, Any]] = []
    for start, end in buckets:
        frame = candidate_labels[candidate_labels["selection_effective_rank"].between(start, end)]
        known = frame[frame["outcome_known"].fillna(False)]
        values = known["slot_return_bps"].astype(float)
        rows.append(
            {
                "rank_start": start,
                "rank_end": end,
                "row_count": len(frame),
                "known_count": len(known),
                "coverage": float(len(known) / len(frame)) if len(frame) else 0.0,
                "mean_economic_net_excess_bps": float(values.mean()) if len(values) else None,
                "median_economic_net_excess_bps": float(values.median()) if len(values) else None,
                "positive_fraction": float((values > 0).mean()) if len(values) else None,
            }
        )
    return tuple(rows)


def _moving_block_interval(
    values: np.ndarray,
    *,
    block_length: int,
    repetitions: int,
    seed: int,
) -> tuple[float, float]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if len(array) < 2:
        value = float(array[0]) if len(array) else float("nan")
        return value, value
    rng = np.random.default_rng(seed)
    length = min(block_length, len(array))
    block_count = math.ceil(len(array) / length)
    offsets = np.arange(length)
    samples = np.empty(repetitions, dtype=float)
    for index in range(repetitions):
        starts = rng.integers(0, len(array), size=block_count)
        positions = ((starts[:, None] + offsets[None, :]) % len(array)).reshape(-1)
        samples[index] = float(array[positions[: len(array)]].mean())
    lower, upper = np.quantile(samples, [0.025, 0.975])
    point = float(array.mean())
    return min(float(lower), point), max(float(upper), point)


def _benchmark_regimes(
    benchmark_daily: pd.DataFrame,
    decisions: pd.DatetimeIndex,
) -> dict[pd.Timestamp, str]:
    close = _benchmark_series(benchmark_daily, "close")
    result: dict[pd.Timestamp, str] = {}
    index = pd.DatetimeIndex(close.index)
    positions = {value: idx for idx, value in enumerate(index)}
    for decision in decisions:
        position = positions.get(pd.Timestamp(decision).normalize())
        if position is None or position < 20:
            result[pd.Timestamp(decision).normalize()] = "UNAVAILABLE"
            continue
        current = _finite_series_value(close, index[position])
        prior = _finite_series_value(close, index[position - 20])
        if current is None or prior is None or prior <= 0:
            result[pd.Timestamp(decision).normalize()] = "UNAVAILABLE"
        else:
            result[pd.Timestamp(decision).normalize()] = "UP_OR_FLAT" if current / prior - 1.0 >= 0 else "DOWN"
    return result


def _intervention_support(
    daily: pd.DataFrame,
    *,
    request: AdvisoryN1Tier1RequestV1,
) -> Tier1InterventionSupportV1:
    policy = request.inference_policy
    interventions = daily[daily["intervened"].astype(bool)]
    observed_regimes = sorted(str(value) for value in daily["regime"].dropna().unique() if str(value) != "UNAVAILABLE")
    intervention_counts = interventions["regime"].value_counts()
    counts = {regime: int(intervention_counts.get(regime, 0)) for regime in observed_regimes}
    fraction = float(len(interventions) / len(daily))
    reasons: list[str] = []
    if len(interventions) < policy.min_intervention_days:
        reasons.append("EXPLORATORY_INSUFFICIENT_INTERVENTION_DAYS")
    if fraction < policy.min_intervention_fraction:
        reasons.append("EXPLORATORY_INSUFFICIENT_INTERVENTION_FRACTION")
    if not observed_regimes or any(
        value < policy.min_intervention_days_per_observed_regime for value in counts.values()
    ):
        reasons.append("EXPLORATORY_INSUFFICIENT_REGIME_SUPPORT")
    return Tier1InterventionSupportV1(
        evaluated_day_count=len(daily),
        intervention_day_count=len(interventions),
        intervention_fraction=fraction,
        intervention_days_by_regime=counts,
        minimum_day_count=policy.min_intervention_days,
        minimum_fraction=policy.min_intervention_fraction,
        minimum_days_per_observed_regime=policy.min_intervention_days_per_observed_regime,
        support_sufficient=not reasons,
        reason_codes=tuple(reasons),
    )


def _point_high(metric: Tier1MetricInferenceV1) -> bool:
    if metric.evidence_state == Tier1EvidenceState.HIGH:
        return True
    if metric.evidence_state == Tier1EvidenceState.LOW:
        return False
    return metric.point_estimate_bps > metric.economic_threshold_bps


def _validate_pit_state(state: Mapping[str, Any]) -> None:
    mismatches: dict[str, Any] = {}
    expected = {
        "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "scope": CANONICAL_PIT_SCOPE,
        "status": "ready",
        "dirty": False,
    }
    for key, value in expected.items():
        if state.get(key) != value:
            mismatches[key] = {"expected": value, "actual": state.get(key)}
    try:
        start = _as_date(state["start_date"])
        end = _as_date(state["end_date"])
    except (KeyError, TypeError, ValueError):
        mismatches["coverage"] = "invalid"
    else:
        if start > N1_DECISION_START or end < N1_DATA_CUTOFF:
            mismatches["coverage"] = {
                "required_start": N1_DECISION_START.isoformat(),
                "required_end": N1_DATA_CUTOFF.isoformat(),
                "actual_start": start.isoformat(),
                "actual_end": end.isoformat(),
            }
    fingerprint = str(state.get("source_fingerprint_sha256") or "")
    if len(fingerprint) != 64 or any(value not in "0123456789abcdef" for value in fingerprint):
        mismatches["source_fingerprint_sha256"] = "invalid"
    if mismatches:
        _raise(
            "canonical PIT state is not ready for N1",
            "ADVISORY_N1_PIT_STATE_NOT_READY",
            mismatches=mismatches,
        )


def _validate_policy_and_feature_reference(
    *,
    policy_manifest: Mapping[str, Any],
    policy_request: FrozenAdvisoryPolicyDatasetRequestV1,
    feature_reference: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    window: AdvisoryResearchWindowContractV1,
) -> None:
    expected = {
        "program_id": policy_request.program_id,
        "binding_version_id": policy_request.binding_version_id,
        "package_id": policy_request.package_id,
        "manifest_sha256": policy_request.manifest_sha256,
        "shadow_policy_sha256": policy_request.shadow_policy_sha256,
        "cost_policy_sha256": policy_request.cost_policy_sha256,
        "split_policy_sha256": policy_request.split_policy_sha256,
    }
    mismatches = {
        key: {"expected": value, "manifest": policy_manifest.get(key)}
        for key, value in expected.items()
        if policy_manifest.get(key) != value
    }
    for key, value in expected.items():
        if hasattr(feature_reference, key) and getattr(feature_reference, key) != value:
            mismatches[f"feature_reference.{key}"] = {
                "expected": value,
                "actual": getattr(feature_reference, key),
            }
    if feature_reference.policy_dataset_bundle_id != N1_DATASET_IDENTITY:
        mismatches["feature_reference.policy_dataset_bundle_id"] = feature_reference.policy_dataset_bundle_id
    expected_feature_hash = feature_schema_hash(
        market_calendar_identity=feature_reference.market_calendar_identity.model_dump(mode="json"),
        suspend_sidecar_identity=feature_reference.suspend_sidecar_identity.model_dump(mode="json"),
    )
    if (
        feature_reference.feature_schema_version != FEATURE_SCHEMA_VERSION
        or feature_reference.feature_schema_hash != expected_feature_hash
    ):
        mismatches["feature_schema"] = {
            "expected_version": FEATURE_SCHEMA_VERSION,
            "expected_hash": expected_feature_hash,
            "actual_version": feature_reference.feature_schema_version,
            "actual_hash": feature_reference.feature_schema_hash,
        }
    if (
        window.package_id != policy_request.package_id
        or window.manifest_sha256 != policy_request.manifest_sha256
        or window.runtime_semantics_hash != policy_request.selection_runtime_semantics_hash
    ):
        mismatches["window_identity"] = "package/manifest/runtime semantics differ"
    if mismatches:
        _raise(
            "N1 source identities are not closed",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            mismatches=mismatches,
        )


def _load_and_verify_n1_sources(request: AdvisoryN1Tier1RequestV1) -> dict[str, Any]:
    bundle_root = Path(request.policy_dataset_bundle_root)
    pit_path = Path(request.pit_snapshot.artifact_ref.artifact_uri)
    try:
        manifest = load_policy_dataset_bundle(bundle_root, expected_bundle_id=request.policy_dataset_bundle_id)
        policy_request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
            (bundle_root / "request.json").read_text(encoding="utf-8")
        )
        pit_snapshot = frozen_pit_snapshot_from_mapping(json.loads(pit_path.read_text(encoding="utf-8")))
        historical_rankings = pd.read_parquet(bundle_root / "candidate_rankings.parquet")
        cpcv_payload = json.loads((bundle_root / "cpcv_paths.json").read_text(encoding="utf-8"))
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "N1 frozen source bundle cannot be read",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    mismatches: dict[str, Any] = {}
    if sha256_file(bundle_root / "manifest.json") != request.policy_dataset_manifest_file_sha256:
        mismatches["policy_dataset_manifest_file_sha256"] = "drift"
    if policy_request.request_sha256 != request.policy_dataset_request_sha256:
        mismatches["policy_dataset_request_sha256"] = "drift"
    for key in (
        "program_id",
        "binding_version_id",
        "package_id",
        "manifest_sha256",
        "selection_runtime_semantics_hash",
        "baseline_policy_sha256",
        "shadow_policy_sha256",
        "cost_policy_sha256",
        "split_policy_sha256",
    ):
        if getattr(policy_request, key) != getattr(request, key):
            mismatches[key] = {
                "expected": getattr(request, key),
                "actual": getattr(policy_request, key),
            }
    if manifest.get("policy_dataset_bundle_id") != request.policy_dataset_bundle_id:
        mismatches["policy_dataset_bundle_id"] = manifest.get("policy_dataset_bundle_id")
    if (
        sha256_file(pit_path) != request.pit_snapshot.artifact_ref.sha256
        or pit_path.stat().st_size != request.pit_snapshot.artifact_ref.size_bytes
        or pit_snapshot.spans_sha256 != request.pit_snapshot.spans_sha256
        or pit_snapshot.source_fingerprint_sha256 != request.pit_snapshot.source_fingerprint_sha256
        or pit_snapshot.parameter_hash != request.pit_snapshot.parameter_hash
        or len(pit_snapshot.spans) != request.pit_snapshot.span_count
        or pit_snapshot.unique_instruments != request.pit_snapshot.instrument_count
    ):
        mismatches["pit_snapshot"] = "identity drift"
    if mismatches:
        _raise(
            "N1 source identity changed after request freeze",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            mismatches=mismatches,
        )
    candidate_dates = (
        pd.DatetimeIndex(
            pd.to_datetime(
                historical_rankings.loc[
                    historical_rankings["is_candidate_decision"],
                    "decision_as_of_trade_date",
                ]
            )
        )
        .normalize()
        .sort_values()
        .unique()
    )
    candidate_counts = (
        historical_rankings.loc[historical_rankings["is_candidate_decision"]]
        .groupby(
            pd.to_datetime(
                historical_rankings.loc[
                    historical_rankings["is_candidate_decision"],
                    "decision_as_of_trade_date",
                ]
            ).dt.normalize()
        )
        .size()
    )
    if (
        len(candidate_dates) != 386
        or candidate_dates[0].date() != N1_DECISION_START
        or candidate_dates[-1].date() != N1_DECISION_END
        or not (candidate_counts == 40).all()
    ):
        _raise(
            "P0-C candidate calendar/shape differs from N1",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            candidate_date_count=len(candidate_dates),
            minimum_rank_count=int(candidate_counts.min()) if len(candidate_counts) else 0,
            maximum_rank_count=int(candidate_counts.max()) if len(candidate_counts) else 0,
        )
    ready_paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if len(ready_paths) != 28:
        _raise(
            "P0-C does not contain the exact 28 READY CPCV paths",
            "ADVISORY_N1_CROSSFIT_INVALID",
            ready_path_count=len(ready_paths),
        )
    initialize_qlib(request.qlib_daily_root)
    # The feature bundle's calendar identity was frozen against the P0-C market
    # data cutoff.  Its H5 schema may extend to factor_data_cutoff, but those are
    # separate identities and must not be conflated.
    feature_calendar = load_trading_calendar("2023-09-01", request.data_cutoff.isoformat())
    calendar_hash = canonical_json_sha256({"market_sessions": [item.date().isoformat() for item in feature_calendar]})
    suspend_path = Path(request.suspend_data_root) / "suspend_d.parquet"
    try:
        suspend_row_count = len(pd.read_parquet(suspend_path, columns=["trade_date"]))
    except Exception as exc:
        _raise(
            "N1 suspend sidecar identity cannot be read",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if (
        calendar_hash != request.market_calendar_identity.sha256
        or len(feature_calendar) != request.market_calendar_identity.row_count
        or sha256_file(suspend_path) != request.suspend_sidecar_identity.sha256
        or suspend_row_count != request.suspend_sidecar_identity.row_count
    ):
        _raise(
            "N1 calendar or suspend identity changed after request freeze",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
            calendar_sha256=calendar_hash,
            calendar_row_count=len(feature_calendar),
            suspend_row_count=suspend_row_count,
        )
    n1_calendar = feature_calendar[
        (feature_calendar >= pd.Timestamp("2023-09-01")) & (feature_calendar <= pd.Timestamp(N1_DATA_CUTOFF))
    ]
    factor_schema = validate_factor_file_schemas(
        request.factor_data_root,
        data_cutoff=request.factor_data_cutoff.isoformat(),
    )
    source_receipt = {
        "schema_version": "advisory_n1_source_identity_receipt_v1",
        "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
        "policy_dataset_manifest_file_sha256": request.policy_dataset_manifest_file_sha256,
        "policy_dataset_request_sha256": request.policy_dataset_request_sha256,
        "pit_snapshot_file_sha256": request.pit_snapshot.artifact_ref.sha256,
        "pit_spans_sha256": request.pit_snapshot.spans_sha256,
        "feature_schema_hash": request.feature_schema_hash,
        "market_calendar_identity": request.market_calendar_identity.model_dump(mode="json"),
        "suspend_sidecar_identity": request.suspend_sidecar_identity.model_dump(mode="json"),
        "factor_schema": {
            "factor_root": factor_schema.factor_root,
            "data_cutoff": factor_schema.data_cutoff,
            "h5_schema_hashes": factor_schema.h5_schema_hashes,
            "static_factor_schema_hash": factor_schema.static_factor_schema_hash,
        },
        "decision_date_count": len(candidate_dates),
        "cpcv_ready_path_count": len(ready_paths),
        "sealed_holdout_accessed": False,
    }
    return {
        "manifest": manifest,
        "policy_request": policy_request,
        "pit_snapshot": pit_snapshot,
        "historical_rankings": historical_rankings,
        "cpcv_payload": cpcv_payload,
        "ready_paths": ready_paths,
        "decision_dates": candidate_dates,
        "feature_calendar": feature_calendar,
        "n1_calendar": n1_calendar,
        "source_receipt": source_receipt,
    }


def _verify_wsl_environment(
    request: AdvisoryN1Tier1RequestV1,
    *,
    require_repository_identity: bool,
) -> dict[str, Any]:
    if os.name == "nt" or "microsoft" not in platform.release().lower():
        _raise(
            "N1 batch must run inside WSL",
            "ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        _raise(
            "N1 batch requires the rdagent-gpu Conda environment",
            "ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            conda_env=conda_env or None,
        )
    actual_commit = _git_commit_for_worktree(Path(request.repository_root))
    if require_repository_identity and actual_commit != request.repository_commit:
        _raise(
            "N1 repository commit differs from the frozen request",
            "ADVISORY_N1_REQUEST_INVALID",
            expected=request.repository_commit,
            actual=actual_commit,
        )
    return {
        "platform_release": platform.release(),
        "conda_env": conda_env,
        "repository_commit": actual_commit,
        "requested_repository_commit": request.repository_commit,
        "repository_identity_check": (
            "MATCHED_FOR_COMPUTE" if require_repository_identity else "NOT_REQUIRED_FOR_IMMUTABLE_DELIVERY_ONLY_RESUME"
        ),
        "python": platform.python_version(),
        "pandas": importlib.metadata.version("pandas"),
        "numpy": importlib.metadata.version("numpy"),
        "scikit_learn": importlib.metadata.version("scikit-learn"),
    }


def _common_prediction_counts(
    leg_frames: Mapping[str, pd.DataFrame],
) -> dict[pd.Timestamp, int]:
    aligned: pd.DataFrame | None = None
    for leg_id, frame in sorted(leg_frames.items()):
        keys = frame[["trade_date", "instrument"]].copy()
        keys = keys.rename(columns={"instrument": f"instrument__{leg_id}"})
        keys["instrument"] = keys[f"instrument__{leg_id}"]
        keys = keys[["trade_date", "instrument"]]
        aligned = (
            keys
            if aligned is None
            else aligned.merge(keys, on=["trade_date", "instrument"], how="inner", validate="one_to_one")
        )
    if aligned is None or aligned.empty:
        _raise(
            "PIT-filtered parent legs have no common predictions",
            "ADVISORY_N1_SOURCE_IDENTITY_MISMATCH",
        )
    aligned["trade_date"] = pd.to_datetime(aligned["trade_date"]).dt.normalize()
    return {
        pd.Timestamp(value).normalize(): int(count) for value, count in aligned.groupby("trade_date").size().items()
    }


def _result_class(
    evidence_state: Tier1EvidenceState,
    decision_use: DecisionUse,
) -> ResearchResultClass:
    if decision_use == DecisionUse.NAVIGATION_ONLY:
        return ResearchResultClass.EXPLORATORY
    return (
        ResearchResultClass.CONTROL_READY if evidence_state == Tier1EvidenceState.HIGH else ResearchResultClass.NEGATIVE
    )


def _find_existing_bundle(request: AdvisoryN1Tier1RequestV1) -> Path | None:
    root = Path(request.output_root) / "tier1_bundles"
    if not root.is_dir():
        return None
    matches: list[Path] = []
    for path in sorted(root.iterdir()):
        manifest_path = path / "manifest.json"
        if not path.is_dir() or not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            _raise(
                "N1 bundle manifest is unreadable",
                "ADVISORY_N1_BUNDLE_CONFLICT",
                path=str(manifest_path),
                error_type=type(exc).__name__,
            )
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise(
            "one N1 request resolves to multiple immutable bundles",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            bundle_paths=[str(item) for item in matches],
        )
    if not matches:
        return None
    _read_n1_bundle(matches[0])
    return matches[0]


def _publish_n1_bundle(
    *,
    request: AdvisoryN1Tier1RequestV1,
    environment: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
    rankings: pd.DataFrame,
    oracle: Tier1OracleResult,
    learnability: Tier1LearnabilityResult,
    n1_cpcv_payload: Mapping[str, Any],
    oracle_receipt: AdvisoryTier1OracleReceiptV1,
    learnability_receipt: AdvisoryTier1LearnabilityReceiptV1,
    quadrant_receipt: AdvisoryTier1QuadrantReceiptV1,
    resource_report: Mapping[str, Any],
    walk_forward_hmm_receipt: Mapping[str, Any],
) -> Path:
    semantic_identity = {
        "schema_version": N1_BUNDLE_SCHEMA,
        "request_sha256": request.request_sha256,
        "oracle_receipt_sha256": oracle_receipt.receipt_sha256,
        "learnability_receipt_sha256": learnability_receipt.receipt_sha256,
        "quadrant_receipt_sha256": quadrant_receipt.receipt_sha256,
    }
    bundle_id = canonical_json_sha256(semantic_identity)
    bundle_root = Path(request.output_root) / "tier1_bundles"
    bundle_root.mkdir(parents=True, exist_ok=True)
    final = bundle_root / bundle_id
    if final.exists():
        loaded = _read_n1_bundle(final)
        if loaded["manifest"]["request_sha256"] != request.request_sha256:
            _raise(
                "N1 bundle id conflicts with another request",
                "ADVISORY_N1_BUNDLE_CONFLICT",
                bundle_id=bundle_id,
            )
        return final
    temp = Path(tempfile.mkdtemp(prefix=f".tmp_{request.request_id}_", dir=bundle_root))
    _write_json(temp / "request.json", request.model_dump(mode="json"))
    _write_parquet(temp / "candidate_rankings_top50.parquet", rankings)
    _write_parquet(temp / "candidate_outcomes_top50.parquet", oracle.candidate_labels)
    _write_parquet(temp / "oracle_daily.parquet", oracle.oracle_daily)
    _write_parquet(temp / "oracle_recall_daily.parquet", oracle.recall_daily)
    _write_parquet(temp / "outcome_coverage.parquet", oracle.outcome_coverage)
    _write_parquet(temp / "learnability_oof.parquet", learnability.oof_predictions)
    _write_parquet(temp / "learnability_daily.parquet", learnability.daily)
    _write_json(temp / "n1_label_interval_cpcv.json", dict(n1_cpcv_payload))
    _write_json(temp / "oracle_receipt.json", oracle_receipt.model_dump(mode="json"))
    _write_json(
        temp / "learnability_receipt.json",
        learnability_receipt.model_dump(mode="json"),
    )
    _write_json(temp / "quadrant_receipt.json", quadrant_receipt.model_dump(mode="json"))
    _write_json(temp / "source_identity_receipt.json", dict(source_receipt))
    _write_json(temp / "environment.json", dict(environment))
    bundle_resource_report = dict(resource_report)
    bundle_peak = max(
        int(bundle_resource_report.get("peak_rss_bytes") or 0),
        _peak_rss_bytes(),
    )
    if bundle_peak > request.resource_max_rss_bytes:
        _raise(
            "N1 exceeded the approved RSS limit while materializing its bundle",
            "ADVISORY_N1_MEMORY_LIMIT_EXCEEDED",
            peak_rss_bytes=bundle_peak,
            limit_bytes=request.resource_max_rss_bytes,
        )
    bundle_resource_report["peak_rss_bytes"] = bundle_peak
    _write_json(temp / "resource_report.json", bundle_resource_report)
    _write_json(temp / "walk_forward_hmm_receipt.json", dict(walk_forward_hmm_receipt))
    oracle_ref = _future_evidence_ref(
        temp / "oracle_receipt.json",
        final / "oracle_receipt.json",
        role="n1_tier1_oracle_receipt",
    )
    learnability_ref = _future_evidence_ref(
        temp / "learnability_receipt.json",
        final / "learnability_receipt.json",
        role="n1_tier1_learnability_receipt",
    )
    policy_identity = research_policy_identity(
        baseline_policy_sha256=request.baseline_policy_sha256,
        shadow_policy_sha256=request.shadow_policy_sha256,
        cost_policy_sha256=request.cost_policy_sha256,
    )
    consumed_window = ConsumedWindowV1(
        window_id=request.window_id,
        dataset_identity=request.dataset_identity,
        start_date=N1_DECISION_START,
        end_date=N1_DATA_CUTOFF,
    )
    records = (
        build_trial_record(
            experiment_id=N1_ORACLE_EXPERIMENT_ID,
            attempt_id=request.request_id,
            research_stage="N1_TIER1",
            study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
            hypothesis_family_id="N1_CANDIDATE_RANKING_BOTTLENECK_DIAGNOSTIC_V1",
            parent_lineage=N1_PARENT_LINEAGE,
            unique_variable="PIT_TOP50_RECALL_AND_PERFECT_TOP5_H20_V1",
            objective_contract=ObjectiveContract.ALPHA_RANKING,
            dataset_identity=request.dataset_identity,
            schema_identity=request.feature_schema_hash,
            policy_identity=policy_identity,
            planned_trial_count=1,
            generated_trial_count=1,
            evaluated_trial_count=1,
            selected_trial_count=0,
            consumed_windows=(consumed_window,),
            result_class=oracle_receipt.result_class,
            decision_use=oracle_receipt.decision_use,
            evidence_refs=(oracle_ref,),
            recorded_at=request.created_at,
        ),
        build_trial_record(
            experiment_id=N1_LEARNABILITY_EXPERIMENT_ID,
            attempt_id=request.request_id,
            research_stage="N1_TIER1",
            study_type=ResearchStudyType.LEARNABILITY_AUDIT,
            hypothesis_family_id="N1_CANDIDATE_RANKING_BOTTLENECK_DIAGNOSTIC_V1",
            parent_lineage=N1_PARENT_LINEAGE,
            unique_variable="FIXED_RIDGE_SCHEMA_V2_H20_V1",
            objective_contract=ObjectiveContract.ALPHA_RANKING,
            dataset_identity=request.dataset_identity,
            schema_identity=request.feature_schema_hash,
            policy_identity=policy_identity,
            planned_trial_count=1,
            generated_trial_count=1,
            evaluated_trial_count=1,
            selected_trial_count=0,
            consumed_windows=(consumed_window,),
            result_class=learnability_receipt.result_class,
            decision_use=learnability_receipt.decision_use,
            evidence_refs=(learnability_ref,),
            recorded_at=request.created_at,
        ),
    )
    _write_json(
        temp / "registry_records.json",
        [item.model_dump(mode="json") for item in records],
    )
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(temp.iterdir()):
        if not path.is_file():
            continue
        descriptor: dict[str, Any] = {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        if path.suffix == ".parquet":
            descriptor["row_count"] = _parquet_row_count(path)
        files[path.name] = descriptor
    manifest = {
        **semantic_identity,
        "bundle_id": bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "dataset_identity": request.dataset_identity,
        "policy_identity": policy_identity,
        "pit_spans_sha256": request.pit_snapshot.spans_sha256,
        "feature_schema_hash": request.feature_schema_hash,
        "typed_quadrant": quadrant_receipt.typed_result,
        "direction_ready": quadrant_receipt.direction_ready,
        "sealed_holdout_accessed": False,
        "runtime_eligible": False,
        "activated": False,
        "files": files,
    }
    _write_json(temp / "manifest.json", manifest)
    _validate_bundle_files(temp, manifest)
    try:
        temp.replace(final)
    except FileExistsError:
        _raise(
            "N1 bundle appeared concurrently",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            bundle_id=bundle_id,
        )
    _read_n1_bundle(final)
    return final


def _read_n1_bundle(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "N1 bundle manifest cannot be read",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            path=str(path),
            error_type=type(exc).__name__,
        )
    if (
        manifest.get("schema_version") != N1_BUNDLE_SCHEMA
        or manifest.get("bundle_id") != path.name
        or canonical_json_sha256(
            {
                "schema_version": manifest.get("schema_version"),
                "request_sha256": manifest.get("request_sha256"),
                "oracle_receipt_sha256": manifest.get("oracle_receipt_sha256"),
                "learnability_receipt_sha256": manifest.get("learnability_receipt_sha256"),
                "quadrant_receipt_sha256": manifest.get("quadrant_receipt_sha256"),
            }
        )
        != path.name
    ):
        _raise(
            "N1 bundle identity is invalid",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            path=str(path),
        )
    _validate_bundle_files(path, manifest)
    try:
        request = AdvisoryN1Tier1RequestV1.model_validate_json((path / "request.json").read_text(encoding="utf-8"))
        oracle = AdvisoryTier1OracleReceiptV1.model_validate_json(
            (path / "oracle_receipt.json").read_text(encoding="utf-8")
        )
        learnability = AdvisoryTier1LearnabilityReceiptV1.model_validate_json(
            (path / "learnability_receipt.json").read_text(encoding="utf-8")
        )
        quadrant = AdvisoryTier1QuadrantReceiptV1.model_validate_json(
            (path / "quadrant_receipt.json").read_text(encoding="utf-8")
        )
        raw_records = json.loads((path / "registry_records.json").read_text(encoding="utf-8"))
        records = tuple(AdvisoryResearchTrialRecordV1.model_validate(item) for item in raw_records)
        resource_report = json.loads((path / "resource_report.json").read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "N1 bundle contract readback failed",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            path=str(path),
            error_type=type(exc).__name__,
        )
    if (
        len(records) != 2
        or {item.experiment_id for item in records} != {N1_ORACLE_EXPERIMENT_ID, N1_LEARNABILITY_EXPERIMENT_ID}
        or request.request_sha256 != manifest["request_sha256"]
        or oracle.receipt_sha256 != manifest["oracle_receipt_sha256"]
        or learnability.receipt_sha256 != manifest["learnability_receipt_sha256"]
        or quadrant.receipt_sha256 != manifest["quadrant_receipt_sha256"]
        or int(resource_report.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
    ):
        _raise(
            "N1 bundle relational identity is invalid",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            path=str(path),
        )
    return {
        "manifest": manifest,
        "request": request,
        "oracle": oracle,
        "learnability": learnability,
        "quadrant": quadrant,
        "records": records,
    }


def _deliver_bundle(
    *,
    request: AdvisoryN1Tier1RequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_n1_bundle(bundle_path)
    if loaded["request"].request_sha256 != request.request_sha256:
        _raise(
            "N1 bundle belongs to another request",
            "ADVISORY_N1_BUNDLE_CONFLICT",
        )
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path)
    registry_summary = registry.append_batch(loaded["records"])
    route_summary = generate_current_route(
        registry_path=request.registry_path,
        parent_spike_path=_n0_sibling_path(request, "parent_prediction_extension_receipt.json"),
        window_contract_path=request.research_window_contract_path,
        output_path=request.route_path,
    )
    if route_summary["next_task"] != "N2_ENTRY_EXIT_QE_PREPARATION":
        _raise(
            "N1 registry delivery did not advance the derived route to N2",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            next_task=route_summary["next_task"],
        )
    return {
        "registry": registry_summary,
        "route": route_summary,
        "next_task": route_summary["next_task"],
    }


def _n0_sibling_path(request: AdvisoryN1Tier1RequestV1, filename: str) -> str:
    return str(Path(request.n0_completion_ref.artifact_uri).parent / filename)


def _future_evidence_ref(
    source: Path,
    final: Path,
    *,
    role: str,
) -> EvidenceReferenceV1:
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=final.resolve().as_posix(),
        sha256=sha256_file(source),
        size_bytes=source.stat().st_size,
    )


def _validate_bundle_files(root: Path, manifest: Mapping[str, Any]) -> None:
    files = manifest.get("files")
    if not isinstance(files, Mapping) or not files:
        _raise(
            "N1 bundle manifest has no file descriptors",
            "ADVISORY_N1_BUNDLE_CONFLICT",
        )
    actual_names = {item.name for item in root.iterdir() if item.is_file()} - {"manifest.json"}
    if actual_names != set(files):
        _raise(
            "N1 bundle file roster differs from its manifest",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            missing=sorted(set(files) - actual_names),
            extra=sorted(actual_names - set(files)),
        )
    for name, descriptor in files.items():
        path = root / str(name)
        if (
            not path.is_file()
            or sha256_file(path) != descriptor.get("sha256")
            or path.stat().st_size != descriptor.get("size_bytes")
        ):
            _raise(
                "N1 bundle file hash/size differs from its manifest",
                "ADVISORY_N1_BUNDLE_CONFLICT",
                filename=str(name),
            )
        if "row_count" in descriptor:
            try:
                row_count = _parquet_row_count(path)
            except Exception as exc:
                _raise(
                    "N1 bundle parquet cannot be read",
                    "ADVISORY_N1_BUNDLE_CONFLICT",
                    filename=str(name),
                    error_type=type(exc).__name__,
                )
            if row_count != descriptor["row_count"]:
                _raise(
                    "N1 bundle parquet row count differs",
                    "ADVISORY_N1_BUNDLE_CONFLICT",
                    filename=str(name),
                )


def _write_json(path: Path, payload: Any) -> None:
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    normalized = frame.copy()
    for column in normalized.columns:
        if normalized[column].dtype != object:
            continue
        if normalized[column].map(lambda value: isinstance(value, (dict, list, tuple))).any():
            normalized[column] = normalized[column].map(
                lambda value: (
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    if isinstance(value, (dict, list, tuple))
                    else value
                )
            )
    normalized.to_parquet(path, index=False)


def _parquet_row_count(path: Path) -> int:
    try:
        import pyarrow.parquet as pq

        metadata = pq.ParquetFile(path).metadata
    except Exception as exc:
        _raise(
            "N1 parquet metadata cannot be read",
            "ADVISORY_N1_BUNDLE_CONFLICT",
            filename=path.name,
            error_type=type(exc).__name__,
        )
    return int(metadata.num_rows)


def _git_commit_for_worktree(repository_root: Path) -> str:
    root = repository_root.resolve()
    command = ["git"]
    pointer = root / ".git"
    if pointer.is_file():
        raw = pointer.read_text(encoding="utf-8").strip()
        if not raw.startswith("gitdir: "):
            _raise(
                "N1 repository .git pointer is invalid",
                "ADVISORY_N1_REQUEST_INVALID",
            )
        git_dir = raw.removeprefix("gitdir: ").strip()
        if os.name != "nt" and ":/" in git_dir:
            try:
                git_dir = subprocess.run(
                    ["wslpath", "-u", git_dir],
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout.strip()
            except (OSError, subprocess.CalledProcessError) as exc:
                _raise(
                    "N1 could not translate the worktree git directory",
                    "ADVISORY_N1_REQUEST_INVALID",
                    error_type=type(exc).__name__,
                )
        command.append(f"--git-dir={git_dir}")
    try:
        commit = (
            subprocess.run(
                [*command, "rev-parse", "HEAD"],
                cwd=root,
                check=True,
                capture_output=True,
                text=True,
            )
            .stdout.strip()
            .lower()
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise(
            "N1 repository commit cannot be read",
            "ADVISORY_N1_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40:
        _raise(
            "N1 repository commit is invalid",
            "ADVISORY_N1_REQUEST_INVALID",
            commit=commit,
        )
    return commit


def _write_immutable_json_model(
    path: Path,
    model: Any,
    *,
    model_type: type[BaseModel],
    hash_field: str,
    reason_code: str,
) -> Any:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = (
        json.dumps(
            model.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n"
    )
    if path.exists():
        try:
            existing = model_type.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "existing immutable N1 artifact is invalid",
                reason_code,
                path=str(path),
                error_type=type(exc).__name__,
            )
        if getattr(existing, hash_field) != getattr(model, hash_field):
            _raise(
                "existing immutable N1 artifact conflicts with the request",
                reason_code,
                path=str(path),
            )
        return existing
    try:
        with path.open("x", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
    except FileExistsError:
        _raise(
            "immutable N1 artifact appeared concurrently",
            reason_code,
            path=str(path),
        )
    return model_type.model_validate_json(path.read_text(encoding="utf-8"))


def _finite_series_value(values: pd.Series, index: pd.Timestamp) -> float | None:
    if index not in values.index:
        return None
    try:
        value = float(values.loc[index])
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _as_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "N1_LEARNABILITY_EXPERIMENT_ID",
    "N1_ORACLE_EXPERIMENT_ID",
    "Tier1FullUniverseOutcomeResult",
    "Tier1LearnabilityResult",
    "Tier1OracleResult",
    "authorize_n1_development_access",
    "build_tier1_benchmark_regimes",
    "build_tier1_full_universe_outcomes",
    "build_quadrant_result",
    "build_tier1_outcomes_and_oracle",
    "filter_prediction_frame_to_pit",
    "freeze_canonical_pit_snapshot",
    "infer_daily_lift",
    "load_verified_n1_sources",
    "prepare_n1_tier1_request",
    "run_fixed_learnability_audit",
]
