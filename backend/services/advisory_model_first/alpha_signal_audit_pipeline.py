from __future__ import annotations

import gc
import importlib.metadata
import json
import math
import os
import platform
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows imports request preparation and inspection.
    _resource = None

from backend.services.advisory_model_first.alpha_signal_audit_contracts import (
    ALPHA_AUDIT_BUNDLE_SCHEMA,
    ALPHA_AUDIT_EXPERIMENT_ID,
    ALPHA_AUDIT_PARENT_LINEAGE,
    ARM_IDS,
    FUNDGROWTH_LEG_ID,
    LSTM_LEG_ID,
    PARENT_ARM_ID,
    AdvisoryThreeArmAlphaAuditReceiptV1,
    AdvisoryThreeArmAlphaAuditRequestV1,
    build_three_arm_alpha_audit_receipt,
    build_three_arm_alpha_audit_request,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_rank_source import build_policy_rankings
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource, sha256_file
from backend.services.advisory_model_first.qe_file_source import load_qlib_daily, load_suspend_rows
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    generate_current_route,
    research_policy_identity,
)
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import (
    AdvisoryN1Tier1RequestV1,
    N1_DATA_CUTOFF,
    N1_DECISION_START,
)
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    authorize_n1_development_access,
    build_tier1_benchmark_regimes,
    build_tier1_full_universe_outcomes,
    filter_prediction_frame_to_pit,
    inspect_n1_bundle,
    load_verified_n1_sources,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


_KNOWN_CASH_STATUSES = frozenset(
    {
        "NOT_ELIGIBLE_ENTRY_DATE",
        "NOT_ENTERED_SUSPENDED",
        "NOT_ENTERED_LIMIT_UP",
    }
)
_PAIRWISE_ARMS = (
    (PARENT_ARM_ID, "LSTM_ONLY"),
    (PARENT_ARM_ID, "FUNDGROWTH_ONLY"),
    ("LSTM_ONLY", "FUNDGROWTH_ONLY"),
)
_RESULT_IDENTITY_EXCLUDED_FILES = frozenset(
    {
        "request.json",
        "source_identity_receipt.json",
        "audit_receipt.json",
        "registry_record.json",
        "environment.json",
        "resource_report.json",
    }
)


@dataclass(frozen=True)
class AlphaAuditMetricResult:
    coverage_daily: pd.DataFrame
    full_signal_outcomes: pd.DataFrame
    rankings_top50: pd.DataFrame
    recall_daily: pd.DataFrame
    top5_daily: pd.DataFrame
    oracle_daily: pd.DataFrame
    signal_metrics_daily: pd.DataFrame
    arm_summary: dict[str, Any]
    pairwise_summary: dict[str, Any]
    regime_quarter_summary: pd.DataFrame


class AlphaAuditProgress:
    def __init__(self, *, limit_bytes: int) -> None:
        self.limit_bytes = int(limit_bytes)
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def stage(self, name: str, started: float, **details: Any) -> None:
        peak = _peak_rss_bytes()
        item = {
            "stage": name,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": peak,
            **details,
        }
        self.stages.append(item)
        if peak > self.limit_bytes:
            _raise(
                "alpha audit exceeded the approved RSS limit",
                "ADVISORY_ALPHA_AUDIT_MEMORY_LIMIT_EXCEEDED",
                stage=name,
                peak_rss_bytes=peak,
                limit_bytes=self.limit_bytes,
            )

    def report(self) -> dict[str, Any]:
        return {
            "schema_version": "advisory_three_arm_alpha_audit_resource_report_v1",
            "peak_rss_bytes": _peak_rss_bytes(),
            "limit_bytes": self.limit_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def prepare_three_arm_alpha_audit_request(
    *,
    n1_request_path: str | Path,
    n1_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
) -> AdvisoryThreeArmAlphaAuditRequestV1:
    n1_request_local = _local_path(n1_request_path)
    n1_bundle_local = _local_path(n1_bundle_path)
    try:
        n1_request = AdvisoryN1Tier1RequestV1.model_validate_json(n1_request_local.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "alpha audit cannot read the bound N1 request",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    inspected = inspect_n1_bundle(n1_bundle_local)
    if inspected["request_sha256"] != n1_request.request_sha256:
        _raise(
            "N1 formal bundle belongs to another request",
            "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
        )
    manifest_path = n1_bundle_local / "manifest.json"
    repository_local = _local_path(repository_root)
    dirty_paths = _git_dirty_paths(repository_local)
    if dirty_paths:
        _raise(
            "alpha audit request must bind a clean committed worktree",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            dirty_paths=dirty_paths[:50],
        )
    request = build_three_arm_alpha_audit_request(
        n0_completion_ref=n1_request.n0_completion_ref,
        n0_completion_receipt_sha256=n1_request.n0_completion_receipt_sha256,
        research_window_contract_ref=n1_request.research_window_contract_ref,
        research_window_contract_sha256=n1_request.research_window_contract_sha256,
        n1_request_ref=_evidence_reference(
            n1_request_local,
            declared_uri=_wsl_path(n1_request_path),
            role="n1_frozen_request",
        ),
        n1_request_sha256=n1_request.request_sha256,
        n1_bundle_path=_wsl_path(n1_bundle_path),
        n1_bundle_manifest_ref=_evidence_reference(
            manifest_path,
            declared_uri=_wsl_path(Path(n1_bundle_path) / "manifest.json"),
            role="n1_formal_bundle_manifest",
        ),
        n1_bundle_id=inspected["bundle_id"],
        registry_path=n1_request.registry_path,
        program_id=n1_request.program_id,
        binding_version_id=n1_request.binding_version_id,
        package_id=n1_request.package_id,
        manifest_sha256=n1_request.manifest_sha256,
        selection_runtime_semantics_hash=n1_request.selection_runtime_semantics_hash,
        baseline_policy_sha256=n1_request.baseline_policy_sha256,
        shadow_policy_sha256=n1_request.shadow_policy_sha256,
        cost_policy_sha256=n1_request.cost_policy_sha256,
        split_policy_sha256=n1_request.split_policy_sha256,
        policy_dataset_bundle_id=n1_request.policy_dataset_bundle_id,
        pit_spans_sha256=n1_request.pit_snapshot.spans_sha256,
        feature_schema_hash=n1_request.feature_schema_hash,
        representative_seed_run_ids=n1_request.representative_seed_run_ids,
        prediction_artifacts=n1_request.prediction_artifacts,
        parent_terminal_weights=n1_request.terminal_weights,
        repository_root=_wsl_path(repository_root),
        repository_commit=_git_commit(repository_local),
        output_root=_wsl_path(output_root),
    )
    _write_immutable_request(_local_path(output_path), request)
    return request


def run_three_arm_alpha_audit(request_path: str | Path) -> dict[str, Any]:
    try:
        request = AdvisoryThreeArmAlphaAuditRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "alpha audit request cannot be read",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    n1_request = _load_and_verify_bound_n1(request)
    # This authorization is intentionally before Prediction Store, Qlib, PIT,
    # factor, market, or suspend loaders.
    authorize_n1_development_access(n1_request)
    existing = _find_existing_bundle(request)
    if existing is not None:
        environment = _verify_wsl_environment(request, require_repository_identity=False)
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response("EXISTING_BUNDLE", request, existing, environment, delivery)
    environment = _verify_wsl_environment(request, require_repository_identity=True)
    progress = AlphaAuditProgress(limit_bytes=request.resource_max_rss_bytes)

    started = time.monotonic()
    sources = load_verified_n1_sources(n1_request)
    _verify_n1_source_copy(request, n1_request, sources)
    progress.stage("source_identity", started, decision_date_count=len(sources["decision_dates"]))

    started = time.monotonic()
    prediction_source = ExactPredictionSource(n1_request.prediction_store_root)
    descriptors = prediction_source.describe_all(n1_request.representative_seed_run_ids.values())
    for run_id, descriptor in descriptors.items():
        if descriptor.model_dump(mode="json") != n1_request.prediction_artifacts[run_id].model_dump(mode="json"):
            _raise(
                "Prediction Store descriptor changed after N1 freeze",
                "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
                run_id=run_id,
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
        for leg_id, run_id in n1_request.representative_seed_run_ids.items()
    }
    progress.stage("prediction_load", started, row_counts={key: len(value) for key, value in leg_frames.items()})

    started = time.monotonic()
    pit_symbols = sorted({span.ts_code for span in sources["pit_snapshot"].spans})
    daily = load_qlib_daily(
        pit_symbols,
        start=N1_DECISION_START.isoformat(),
        end=N1_DATA_CUTOFF.isoformat(),
    )
    benchmark = load_qlib_daily(
        [n1_request.cost_policy.benchmark_instrument],
        start="2023-09-01",
        end=N1_DATA_CUTOFF.isoformat(),
        fields=("$open", "$close"),
    )
    suspend = load_suspend_rows(
        n1_request.suspend_data_root,
        start=N1_DECISION_START.isoformat(),
        end=N1_DATA_CUTOFF.isoformat(),
        instruments=pit_symbols,
        full_day_only=True,
    )
    full_outcome = build_tier1_full_universe_outcomes(
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=sources["pit_snapshot"],
        trading_calendar=sources["n1_calendar"],
        decision_dates=sources["decision_dates"],
        request=n1_request,
    )
    del daily, suspend
    gc.collect()
    progress.stage("full_universe_outcomes", started, row_count=len(full_outcome.outcomes))

    started = time.monotonic()
    metric_result = build_three_arm_alpha_metrics(
        leg_frames=leg_frames,
        outcomes=full_outcome.outcomes,
        outcome_coverage=full_outcome.coverage,
        benchmark_daily=benchmark,
        decision_dates=sources["decision_dates"],
        trading_calendar=sources["n1_calendar"],
        n1_request=n1_request,
        n1_bundle_path=Path(request.n1_bundle_path),
    )
    progress.stage(
        "three_arm_metrics",
        started,
        common_signal_row_count=len(metric_result.full_signal_outcomes),
        ranking_row_count=len(metric_result.rankings_top50),
    )
    del leg_frames, full_outcome, benchmark
    gc.collect()

    source_receipt = _source_identity_receipt(request, n1_request, sources, descriptors)
    bundle_path = _publish_bundle(
        request=request,
        environment=environment,
        source_receipt=source_receipt,
        metrics=metric_result,
        resource_report=progress.report(),
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle_path)
    return _run_response("COMPLETE", request, bundle_path, environment, delivery)


def build_three_arm_alpha_metrics(
    *,
    leg_frames: Mapping[str, pd.DataFrame],
    outcomes: pd.DataFrame,
    outcome_coverage: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    decision_dates: Sequence[pd.Timestamp],
    trading_calendar: Sequence[pd.Timestamp],
    n1_request: AdvisoryN1Tier1RequestV1,
    n1_bundle_path: Path | None = None,
) -> AlphaAuditMetricResult:
    # ALGO-COMPLEXITY-001: the frozen window is exactly 386 dates. The only
    # result-sized frame is the PIT common prediction panel (bounded by the N1
    # snapshot, about two million instrument-dates); all joins below declare
    # one-to-one semantics. Full outcomes are built once, while arm policy
    # frames are bounded to 3 * 386 * 50 rows.
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    signal_panel, prediction_coverage = build_common_signal_panel(
        leg_frames=leg_frames,
        outcomes=outcomes,
        decision_dates=decisions,
        parent_terminal_weights=n1_request.terminal_weights,
    )
    identity = {
        "program_id": n1_request.program_id,
        "binding_version_id": n1_request.binding_version_id,
        "package_id": n1_request.package_id,
        "manifest_sha256": n1_request.manifest_sha256,
        "selection_runtime_semantics_hash": n1_request.selection_runtime_semantics_hash,
    }
    ranking_chunks: list[pd.DataFrame] = []
    arm_weights = {item.arm_id: item.terminal_weights for item in _request_arms_from_n1(n1_request)}
    common_keys = signal_panel[["decision_as_of_trade_date", "instrument"]].rename(
        columns={"decision_as_of_trade_date": "trade_date"}
    )
    for arm_id in ARM_IDS:
        weights = arm_weights[arm_id]
        ranking_leg_frames = {
            leg_id: (
                leg_frames[leg_id]
                if arm_id == PARENT_ARM_ID
                else leg_frames[leg_id].merge(
                    common_keys,
                    on=["trade_date", "instrument"],
                    how="inner",
                    validate="one_to_one",
                )
            )
            for leg_id in weights
        }
        result = build_policy_rankings(
            leg_frames=ranking_leg_frames,
            terminal_weights=weights,
            decision_dates=decisions,
            trading_calendar=trading_calendar,
            identity=identity,
            required_depth=50,
        )
        arm_frame = result.rankings.copy()
        arm_frame.insert(0, "arm_id", arm_id)
        ranking_chunks.append(arm_frame)
    rankings = pd.concat(ranking_chunks, ignore_index=True)
    if n1_bundle_path is not None:
        verify_parent_ranking_parity(
            rankings.loc[rankings["arm_id"].eq(PARENT_ARM_ID)].drop(columns=["arm_id"]),
            n1_bundle_path / "candidate_rankings_top50.parquet",
        )
    coverage = outcome_coverage.merge(
        prediction_coverage,
        on="decision_as_of_trade_date",
        how="left",
        validate="one_to_one",
    )
    rank_counts = rankings.groupby(["decision_as_of_trade_date", "arm_id"]).size().unstack("arm_id")
    for arm_id in ARM_IDS:
        coverage[f"rank_count__{arm_id}"] = (
            coverage["decision_as_of_trade_date"].map(rank_counts[arm_id]).fillna(0).astype(int)
        )
        coverage[f"rank_status__{arm_id}"] = np.where(
            coverage[f"rank_count__{arm_id}"].eq(50), "COMPLETE", "DATA_UNAVAILABLE"
        )
    recall_daily, top5_daily, oracle_daily = evaluate_arm_policy_metrics(
        rankings=rankings,
        outcomes=outcomes,
        outcome_coverage=coverage,
        selectable_universe=signal_panel[["decision_as_of_trade_date", "instrument"]],
        winner_count=n1_request.outcome_policy.winner_count,
    )
    signal_daily = build_signal_metrics_daily(signal_panel)
    pairwise = build_pairwise_summary(
        signal_panel=signal_panel,
        rankings=rankings,
        signal_metrics_daily=signal_daily,
        top5_daily=top5_daily,
        block_length=n1_request.inference_policy.block_length_trading_days,
        repetitions=n1_request.inference_policy.bootstrap_repetitions,
        seed=n1_request.inference_policy.random_seed,
    )
    arm_summary = build_arm_summary(
        signal_panel=signal_panel,
        signal_metrics_daily=signal_daily,
        recall_daily=recall_daily,
        top5_daily=top5_daily,
        oracle_daily=oracle_daily,
        block_length=n1_request.inference_policy.block_length_trading_days,
        repetitions=n1_request.inference_policy.bootstrap_repetitions,
        seed=n1_request.inference_policy.random_seed,
    )
    regime_map = build_tier1_benchmark_regimes(benchmark_daily, decisions)
    regime_quarter = build_regime_quarter_summary(
        signal_metrics_daily=signal_daily,
        top5_daily=top5_daily,
        recall_daily=recall_daily,
        regime_map=regime_map,
    )
    return AlphaAuditMetricResult(
        coverage_daily=coverage,
        full_signal_outcomes=signal_panel,
        rankings_top50=rankings,
        recall_daily=recall_daily,
        top5_daily=top5_daily,
        oracle_daily=oracle_daily,
        signal_metrics_daily=signal_daily,
        arm_summary=arm_summary,
        pairwise_summary=pairwise,
        regime_quarter_summary=regime_quarter,
    )


def build_common_signal_panel(
    *,
    leg_frames: Mapping[str, pd.DataFrame],
    outcomes: pd.DataFrame,
    decision_dates: Sequence[pd.Timestamp],
    parent_terminal_weights: Mapping[str, float],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # ALGO-COMPLEXITY-001: two bounded Prediction Store legs are normalized
    # vectorially and inner-joined on the unique (trade_date, instrument) key.
    # validate="one_to_one" prevents row explosion; no cartesian or per-symbol
    # market-data query is permitted.
    if set(leg_frames) != {LSTM_LEG_ID, FUNDGROWTH_LEG_ID}:
        _raise(
            "alpha audit requires the exact two parent prediction legs",
            "ADVISORY_ALPHA_AUDIT_ARM_CONTRACT_INVALID",
        )
    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    normalized: dict[str, pd.DataFrame] = {}
    own_counts: dict[str, pd.Series] = {}
    for leg_id, frame in leg_frames.items():
        data = frame[["trade_date", "instrument", "score"]].copy()
        data["trade_date"] = pd.to_datetime(data["trade_date"]).dt.normalize()
        data["instrument"] = data["instrument"].astype(str).str.upper()
        data["raw_score"] = pd.to_numeric(data["score"], errors="coerce")
        data = data[data["trade_date"].isin(decisions)]
        if (
            data.empty
            or data["raw_score"].isna().any()
            or (~np.isfinite(data["raw_score"])).any()
            or data.duplicated(["trade_date", "instrument"]).any()
        ):
            _raise(
                "one alpha leg has invalid prediction rows",
                "ADVISORY_ALPHA_AUDIT_PREDICTION_INVALID",
                leg_id=leg_id,
            )
        grouped = data.groupby("trade_date")["raw_score"]
        mean = grouped.transform("mean")
        std = grouped.transform(lambda values: values.std(ddof=0))
        data["normalized_score"] = np.where(std > 0, (data["raw_score"] - mean) / std, 0.0)
        own_counts[leg_id] = data.groupby("trade_date").size()
        normalized[leg_id] = data[["trade_date", "instrument", "raw_score", "normalized_score"]].rename(
            columns={
                "raw_score": f"raw_score__{leg_id}",
                "normalized_score": f"normalized_score__{leg_id}",
            }
        )
    common = normalized[LSTM_LEG_ID].merge(
        normalized[FUNDGROWTH_LEG_ID],
        on=["trade_date", "instrument"],
        how="inner",
        validate="one_to_one",
    )
    common["score__LSTM_ONLY"] = common[f"normalized_score__{LSTM_LEG_ID}"]
    common["score__FUNDGROWTH_ONLY"] = common[f"normalized_score__{FUNDGROWTH_LEG_ID}"]
    common["score__IC_WEIGHTED_PARENT"] = sum(
        float(parent_terminal_weights[leg_id]) * common[f"normalized_score__{leg_id}"]
        for leg_id in (LSTM_LEG_ID, FUNDGROWTH_LEG_ID)
    )
    common_counts = common.groupby("trade_date").size()
    coverage = pd.DataFrame({"decision_as_of_trade_date": decisions})
    for leg_id in (LSTM_LEG_ID, FUNDGROWTH_LEG_ID):
        coverage[f"prediction_count__{leg_id}"] = (
            coverage["decision_as_of_trade_date"].map(own_counts[leg_id]).fillna(0).astype(int)
        )
    coverage["common_prediction_count"] = coverage["decision_as_of_trade_date"].map(common_counts).fillna(0).astype(int)
    if (coverage["common_prediction_count"] < 50).any():
        _raise(
            "common alpha prediction coverage is below Top50",
            "ADVISORY_ALPHA_AUDIT_PREDICTION_INVALID",
            dates=[
                item.date().isoformat()
                for item in coverage.loc[
                    coverage["common_prediction_count"] < 50,
                    "decision_as_of_trade_date",
                ].head(20)
            ],
        )
    labels = outcomes.rename(columns={"decision_as_of_trade_date": "trade_date"})
    panel = common.merge(labels, on=["trade_date", "instrument"], how="left", validate="one_to_one")
    if panel["outcome_status"].isna().any():
        _raise(
            "common prediction rows are absent from the PIT outcome universe",
            "ADVISORY_ALPHA_AUDIT_OUTCOME_INVALID",
        )
    panel = panel.rename(columns={"trade_date": "decision_as_of_trade_date"})
    return panel.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True), coverage


def verify_parent_ranking_parity(actual: pd.DataFrame, expected_path: Path) -> None:
    try:
        expected = pd.read_parquet(expected_path)
    except Exception as exc:
        _raise(
            "N1 parent ranking artifact cannot be read",
            "ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED",
            error_type=type(exc).__name__,
        )
    key_columns = [
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
        "target_trade_date",
        "combined_score",
        f"raw__{LSTM_LEG_ID}",
        f"raw__{FUNDGROWTH_LEG_ID}",
        f"norm__{LSTM_LEG_ID}",
        f"norm__{FUNDGROWTH_LEG_ID}",
    ]
    if not set(key_columns).issubset(actual) or not set(key_columns).issubset(expected):
        _raise(
            "parent ranking parity columns are missing",
            "ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED",
        )
    left = actual[key_columns].sort_values(key_columns[:3]).reset_index(drop=True)
    right = expected[key_columns].sort_values(key_columns[:3]).reset_index(drop=True)
    exact_columns = key_columns[:4]
    if len(left) != len(right) or any(not left[column].equals(right[column]) for column in exact_columns):
        _raise(
            "parent ranking keys differ from the formal N1 artifact",
            "ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED",
        )
    numeric = key_columns[4:]
    if not np.allclose(left[numeric].to_numpy(float), right[numeric].to_numpy(float), rtol=0.0, atol=1e-12):
        _raise(
            "parent ranking scores differ from the formal N1 artifact",
            "ADVISORY_ALPHA_AUDIT_PARENT_PARITY_FAILED",
        )


def evaluate_arm_policy_metrics(
    *,
    rankings: pd.DataFrame,
    outcomes: pd.DataFrame,
    outcome_coverage: pd.DataFrame,
    selectable_universe: pd.DataFrame,
    winner_count: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    coverage_by_date = outcome_coverage.set_index("decision_as_of_trade_date")
    outcome_by_date = {
        pd.Timestamp(key).normalize(): frame
        for key, frame in outcomes.groupby(pd.to_datetime(outcomes["decision_as_of_trade_date"]).dt.normalize())
    }
    selectable_by_date = {
        pd.Timestamp(key).normalize(): set(frame["instrument"].astype(str))
        for key, frame in selectable_universe.groupby(
            pd.to_datetime(selectable_universe["decision_as_of_trade_date"]).dt.normalize()
        )
    }
    recall_rows: list[dict[str, Any]] = []
    top5_rows: list[dict[str, Any]] = []
    oracle_rows: list[dict[str, Any]] = []
    for (arm_id, decision), rank_frame in rankings.groupby(
        ["arm_id", pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize()],
        sort=True,
    ):
        decision = pd.Timestamp(decision).normalize()
        outcome = outcome_by_date[decision]
        coverage = coverage_by_date.loc[decision]
        evaluable = coverage["status"] == "AVAILABLE"
        matured = outcome[outcome["outcome_status"].eq("MATURED")]
        winners = (
            matured.sort_values(["economic_net_excess_bps", "instrument"], ascending=[False, True])
            .head(winner_count)["instrument"]
            .astype(str)
            .tolist()
            if evaluable
            else []
        )
        recall: dict[str, Any] = {
            "arm_id": arm_id,
            "decision_as_of_trade_date": decision,
            "status": "AVAILABLE" if evaluable else "DATA_UNAVAILABLE",
            "winner_count": len(winners),
        }
        selectable = selectable_by_date[decision]
        selectable_winner_count = len(selectable & set(winners))
        recall["selectable_universe_count"] = len(selectable)
        recall["selectable_winner_count"] = selectable_winner_count
        recall["selectable_winner_recall"] = selectable_winner_count / len(winners) if winners else np.nan
        for depth in (20, 40, 50):
            selected = set(rank_frame.loc[rank_frame["selection_effective_rank"] <= depth, "instrument"].astype(str))
            observed = len(selected & set(winners)) / len(winners) if winners else np.nan
            random_expected = (
                (selectable_winner_count / len(winners)) * min(depth / len(selectable), 1.0)
                if winners and selectable
                else np.nan
            )
            recall[f"top{depth}_winner_recall"] = observed
            recall[f"top{depth}_random_expected_recall"] = random_expected
            recall[f"top{depth}_recall_lift"] = (
                observed / random_expected
                if np.isfinite(observed) and np.isfinite(random_expected) and random_expected > 0
                else np.nan
            )
        recall_rows.append(recall)
        candidate = rank_frame.merge(
            outcome,
            on=["decision_as_of_trade_date", "instrument"],
            how="left",
            validate="one_to_one",
            suffixes=("", "__outcome"),
        )
        top5 = candidate[candidate["selection_effective_rank"] <= 5]
        top5_known = len(top5) == 5 and bool(top5["outcome_known"].fillna(False).all())
        top5_rows.append(
            {
                "arm_id": arm_id,
                "decision_as_of_trade_date": decision,
                "status": "AVAILABLE" if top5_known else "DATA_UNAVAILABLE",
                "top5_net_excess_bps": float(top5["slot_return_bps"].sum() / 5.0) if top5_known else np.nan,
                "positive": bool(top5["slot_return_bps"].sum() > 0) if top5_known else None,
                "instruments": tuple(top5.sort_values("selection_effective_rank")["instrument"].astype(str)),
            }
        )
        top20 = candidate[candidate["selection_effective_rank"] <= 20]
        top20_known = len(top20) == 20 and bool(top20["outcome_known"].fillna(False).all())
        if top20_known:
            baseline = top20[top20["selection_effective_rank"] <= 5]
            perfect = top20.sort_values(
                ["slot_return_bps", "selection_effective_rank", "instrument"],
                ascending=[False, True, True],
            ).head(5)
            baseline_return = float(baseline["slot_return_bps"].sum() / 5.0)
            perfect_return = float(perfect["slot_return_bps"].sum() / 5.0)
            oracle_rows.append(
                {
                    "arm_id": arm_id,
                    "decision_as_of_trade_date": decision,
                    "baseline_top5_return_bps": baseline_return,
                    "perfect_top5_return_bps": perfect_return,
                    "perfect_top5_lift_bps": perfect_return - baseline_return,
                    "intervened": set(baseline["instrument"]) != set(perfect["instrument"]),
                }
            )
    recall_frame = pd.DataFrame(recall_rows).sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)
    top5_frame = pd.DataFrame(top5_rows).sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)
    top5_frame["cumulative_episode_mean_bps"] = top5_frame.groupby("arm_id")["top5_net_excess_bps"].transform(
        lambda values: values.expanding().mean()
    )
    oracle_frame = pd.DataFrame(oracle_rows).sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)
    return recall_frame, top5_frame, oracle_frame


def build_signal_metrics_daily(signal_panel: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for decision, frame in signal_panel.groupby("decision_as_of_trade_date", sort=True):
        matured = frame[frame["outcome_status"].eq("MATURED")]
        known = frame[frame["outcome_known"].fillna(False)]
        for arm_id in ARM_IDS:
            score_column = f"score__{arm_id}"
            rows.append(
                {
                    "arm_id": arm_id,
                    "decision_as_of_trade_date": pd.Timestamp(decision).normalize(),
                    "common_row_count": len(frame),
                    "matured_row_count": len(matured),
                    "known_row_count": len(known),
                    "matured_pearson_ic": _safe_corr(
                        matured[score_column], matured["economic_net_excess_bps"], method="pearson"
                    ),
                    "matured_rank_ic": _safe_corr(
                        matured[score_column], matured["economic_net_excess_bps"], method="spearman"
                    ),
                    "policy_rank_ic": _safe_corr(known[score_column], known["slot_return_bps"], method="spearman"),
                    "quintile_spread_bps": _bucket_spread(
                        known[score_column], known["slot_return_bps"], bucket_count=5
                    ),
                    "decile_spread_bps": _bucket_spread(known[score_column], known["slot_return_bps"], bucket_count=10),
                }
            )
    return pd.DataFrame(rows).sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)


def build_arm_summary(
    *,
    signal_panel: pd.DataFrame,
    signal_metrics_daily: pd.DataFrame,
    recall_daily: pd.DataFrame,
    top5_daily: pd.DataFrame,
    oracle_daily: pd.DataFrame,
    block_length: int,
    repetitions: int,
    seed: int,
) -> dict[str, Any]:
    arms: dict[str, Any] = {}
    for arm_index, arm_id in enumerate(ARM_IDS):
        signal = signal_metrics_daily[signal_metrics_daily["arm_id"].eq(arm_id)]
        recall = recall_daily[recall_daily["arm_id"].eq(arm_id)]
        top5 = top5_daily[top5_daily["arm_id"].eq(arm_id)]
        oracle = oracle_daily[oracle_daily["arm_id"].eq(arm_id)]
        metrics = {
            name: _describe_daily_metric(
                signal[name],
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + arm_index * 100 + metric_index,
            )
            for metric_index, name in enumerate(
                (
                    "matured_pearson_ic",
                    "matured_rank_ic",
                    "policy_rank_ic",
                    "quintile_spread_bps",
                    "decile_spread_bps",
                )
            )
        }
        metrics["top5_net_excess_bps"] = _describe_daily_metric(
            top5["top5_net_excess_bps"],
            block_length=block_length,
            repetitions=repetitions,
            seed=seed + arm_index * 100 + 10,
        )
        metrics["perfect_top5_lift_bps"] = _describe_daily_metric(
            oracle["perfect_top5_lift_bps"],
            block_length=block_length,
            repetitions=repetitions,
            seed=seed + arm_index * 100 + 11,
        )
        for depth in (20, 40, 50):
            metrics[f"top{depth}_winner_recall"] = _describe_daily_metric(
                recall[f"top{depth}_winner_recall"],
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + arm_index * 100 + 20 + depth,
            )
        arms[arm_id] = {
            "signal_day_count": len(signal),
            "top5_evaluable_day_count": int(top5["status"].eq("AVAILABLE").sum()),
            "top5_evaluable_day_fraction": float(top5["status"].eq("AVAILABLE").mean()),
            "top5_positive_day_fraction": _finite_mean(top5.loc[top5["status"].eq("AVAILABLE"), "positive"]),
            "oracle_evaluable_day_count": len(oracle),
            "oracle_intervention_day_count": int(oracle["intervened"].sum()) if len(oracle) else 0,
            "metrics": metrics,
            "bucket_returns": {
                f"{bucket_count}_bucket": _bucket_return_summary(
                    signal_panel,
                    arm_id=arm_id,
                    bucket_count=bucket_count,
                    block_length=block_length,
                    repetitions=repetitions,
                    seed=seed + arm_index * 1000 + bucket_count * 10,
                )
                for bucket_count in (5, 10)
            },
        }
    return {
        "schema_version": "advisory_three_arm_alpha_audit_summary_v1",
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "arms": arms,
    }


def build_pairwise_summary(
    *,
    signal_panel: pd.DataFrame,
    rankings: pd.DataFrame,
    signal_metrics_daily: pd.DataFrame,
    top5_daily: pd.DataFrame,
    block_length: int,
    repetitions: int,
    seed: int,
) -> dict[str, Any]:
    ranking_sets: dict[tuple[str, pd.Timestamp, int], set[str]] = {}
    for (arm_id, decision), frame in rankings.groupby(
        ["arm_id", pd.to_datetime(rankings["decision_as_of_trade_date"]).dt.normalize()], sort=True
    ):
        for depth in (5, 20):
            ranking_sets[(str(arm_id), pd.Timestamp(decision), depth)] = set(
                frame.loc[frame["selection_effective_rank"] <= depth, "instrument"].astype(str)
            )
    churn: dict[str, dict[str, float | None]] = {}
    for arm_id in ARM_IDS:
        dates = sorted({key[1] for key in ranking_sets if key[0] == arm_id})
        churn[arm_id] = {}
        for depth in (5, 20):
            values = [
                1.0
                - len(ranking_sets[(arm_id, dates[index - 1], depth)] & ranking_sets[(arm_id, dates[index], depth)])
                / depth
                for index in range(1, len(dates))
            ]
            churn[arm_id][f"top{depth}_mean_churn"] = float(np.mean(values)) if values else None
    pairs: dict[str, Any] = {}
    for pair_index, (left, right) in enumerate(_PAIRWISE_ARMS):
        daily_rows: list[dict[str, Any]] = []
        for decision, frame in signal_panel.groupby("decision_as_of_trade_date", sort=True):
            daily_rows.append(
                {
                    "decision_as_of_trade_date": pd.Timestamp(decision).date().isoformat(),
                    "score_pearson": _safe_corr(frame[f"score__{left}"], frame[f"score__{right}"], method="pearson"),
                    "score_spearman": _safe_corr(frame[f"score__{left}"], frame[f"score__{right}"], method="spearman"),
                    "top5_jaccard": _jaccard(
                        ranking_sets[(left, pd.Timestamp(decision), 5)],
                        ranking_sets[(right, pd.Timestamp(decision), 5)],
                    ),
                    "top20_jaccard": _jaccard(
                        ranking_sets[(left, pd.Timestamp(decision), 20)],
                        ranking_sets[(right, pd.Timestamp(decision), 20)],
                    ),
                }
            )
        daily = pd.DataFrame(daily_rows)
        rank_ic = signal_metrics_daily.loc[
            signal_metrics_daily["arm_id"].isin([left, right]),
            ["arm_id", "decision_as_of_trade_date", "matured_rank_ic"],
        ].pivot(index="decision_as_of_trade_date", columns="arm_id", values="matured_rank_ic")
        top5 = top5_daily.loc[
            top5_daily["arm_id"].isin([left, right]),
            ["arm_id", "decision_as_of_trade_date", "top5_net_excess_bps"],
        ].pivot(index="decision_as_of_trade_date", columns="arm_id", values="top5_net_excess_bps")
        rank_delta = rank_ic[left] - rank_ic[right]
        top5_delta = top5[left] - top5[right]
        pair_key = f"{left}_MINUS_{right}"
        pairs[pair_key] = {
            "left_arm": left,
            "right_arm": right,
            "top5_net_excess_delta": _describe_daily_metric(
                top5_delta,
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + pair_index * 100 + 1,
            ),
            "matured_rank_ic_delta": _describe_daily_metric(
                rank_delta,
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + pair_index * 100 + 2,
            ),
            "mean_score_pearson": _finite_mean(daily["score_pearson"]),
            "mean_score_spearman": _finite_mean(daily["score_spearman"]),
            "mean_top5_jaccard": _finite_mean(daily["top5_jaccard"]),
            "mean_top20_jaccard": _finite_mean(daily["top20_jaccard"]),
        }
    return {
        "schema_version": "advisory_three_arm_alpha_pairwise_summary_v1",
        "pairs": pairs,
        "arm_churn": churn,
    }


def build_regime_quarter_summary(
    *,
    signal_metrics_daily: pd.DataFrame,
    top5_daily: pd.DataFrame,
    recall_daily: pd.DataFrame,
    regime_map: Mapping[pd.Timestamp, str],
) -> pd.DataFrame:
    metrics = (
        signal_metrics_daily[["arm_id", "decision_as_of_trade_date", "matured_rank_ic", "policy_rank_ic"]]
        .merge(
            top5_daily[["arm_id", "decision_as_of_trade_date", "top5_net_excess_bps"]],
            on=["arm_id", "decision_as_of_trade_date"],
            how="left",
            validate="one_to_one",
        )
        .merge(
            recall_daily[["arm_id", "decision_as_of_trade_date", "top20_winner_recall"]],
            on=["arm_id", "decision_as_of_trade_date"],
            how="left",
            validate="one_to_one",
        )
    )
    metrics["regime"] = metrics["decision_as_of_trade_date"].map(regime_map)
    metrics["quarter"] = pd.to_datetime(metrics["decision_as_of_trade_date"]).dt.to_period("Q").astype(str)
    rows: list[dict[str, Any]] = []
    value_columns = ("matured_rank_ic", "policy_rank_ic", "top5_net_excess_bps", "top20_winner_recall")
    for period_type, period_column in (("REGIME", "regime"), ("QUARTER", "quarter")):
        for (arm_id, period), frame in metrics.groupby(["arm_id", period_column], sort=True):
            for metric in value_columns:
                values = pd.to_numeric(frame[metric], errors="coerce")
                finite = values[np.isfinite(values)]
                rows.append(
                    {
                        "period_type": period_type,
                        "period": str(period),
                        "arm_id": arm_id,
                        "metric": metric,
                        "observation_count": len(finite),
                        "mean": float(finite.mean()) if len(finite) else np.nan,
                        "median": float(finite.median()) if len(finite) else np.nan,
                        "descriptive_only": True,
                    }
                )
    return pd.DataFrame(rows)


def inspect_three_arm_alpha_audit_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_bundle(Path(bundle_path))
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_sha256": loaded["request"].request_sha256,
        "receipt_sha256": loaded["receipt"].receipt_sha256,
        "arm_ids": list(loaded["receipt"].arm_ids),
        "sealed_holdout_accessed": False,
        "runtime_eligible": False,
    }


def _request_arms_from_n1(n1_request: AdvisoryN1Tier1RequestV1) -> tuple[Any, ...]:
    from backend.services.advisory_model_first.alpha_signal_audit_contracts import frozen_alpha_audit_arms

    if any(
        abs(float(n1_request.terminal_weights[key]) - value) > 1e-12
        for key, value in {LSTM_LEG_ID: 0.6966591521, FUNDGROWTH_LEG_ID: 0.3033408479}.items()
    ):
        _raise(
            "N1 parent weights differ from the frozen alpha audit",
            "ADVISORY_ALPHA_AUDIT_ARM_CONTRACT_INVALID",
        )
    return frozen_alpha_audit_arms()


def _load_and_verify_bound_n1(request: AdvisoryThreeArmAlphaAuditRequestV1) -> AdvisoryN1Tier1RequestV1:
    n1_path = Path(request.n1_request_ref.artifact_uri)
    manifest_path = Path(request.n1_bundle_manifest_ref.artifact_uri)
    try:
        n1 = AdvisoryN1Tier1RequestV1.model_validate_json(n1_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "bound N1 request cannot be read",
            "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if (
        sha256_file(n1_path) != request.n1_request_ref.sha256
        or n1_path.stat().st_size != request.n1_request_ref.size_bytes
        or n1.request_sha256 != request.n1_request_sha256
        or sha256_file(manifest_path) != request.n1_bundle_manifest_ref.sha256
        or manifest_path.stat().st_size != request.n1_bundle_manifest_ref.size_bytes
    ):
        _raise(
            "bound N1 evidence changed after alpha audit freeze",
            "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
        )
    inspected = inspect_n1_bundle(Path(request.n1_bundle_path))
    if inspected["bundle_id"] != request.n1_bundle_id or inspected["request_sha256"] != n1.request_sha256:
        _raise(
            "bound N1 bundle identity changed",
            "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
        )
    _verify_n1_source_copy(request, n1, None)
    return n1


def _verify_n1_source_copy(
    request: AdvisoryThreeArmAlphaAuditRequestV1,
    n1: AdvisoryN1Tier1RequestV1,
    sources: Mapping[str, Any] | None,
) -> None:
    expected = {
        "n0_completion_ref": n1.n0_completion_ref,
        "n0_completion_receipt_sha256": n1.n0_completion_receipt_sha256,
        "research_window_contract_ref": n1.research_window_contract_ref,
        "research_window_contract_sha256": n1.research_window_contract_sha256,
        "program_id": n1.program_id,
        "binding_version_id": n1.binding_version_id,
        "package_id": n1.package_id,
        "manifest_sha256": n1.manifest_sha256,
        "selection_runtime_semantics_hash": n1.selection_runtime_semantics_hash,
        "baseline_policy_sha256": n1.baseline_policy_sha256,
        "shadow_policy_sha256": n1.shadow_policy_sha256,
        "cost_policy_sha256": n1.cost_policy_sha256,
        "split_policy_sha256": n1.split_policy_sha256,
        "policy_dataset_bundle_id": n1.policy_dataset_bundle_id,
        "pit_spans_sha256": n1.pit_snapshot.spans_sha256,
        "feature_schema_hash": n1.feature_schema_hash,
        "representative_seed_run_ids": n1.representative_seed_run_ids,
        "prediction_artifacts": n1.prediction_artifacts,
        "parent_terminal_weights": n1.terminal_weights,
    }
    mismatches = {
        key: {"request": getattr(request, key), "n1": value}
        for key, value in expected.items()
        if getattr(request, key) != value
    }
    if sources is not None and sources["pit_snapshot"].spans_sha256 != request.pit_spans_sha256:
        mismatches["loaded_pit_spans_sha256"] = sources["pit_snapshot"].spans_sha256
    if mismatches:
        _raise(
            "alpha audit copied N1 identity differs from its bound source",
            "ADVISORY_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH",
            mismatches=mismatches,
        )


def _source_identity_receipt(
    request: AdvisoryThreeArmAlphaAuditRequestV1,
    n1: AdvisoryN1Tier1RequestV1,
    sources: Mapping[str, Any],
    descriptors: Mapping[str, Any],
) -> dict[str, Any]:
    parent_ranking_path = Path(request.n1_bundle_path) / "candidate_rankings_top50.parquet"
    payload = {
        "schema_version": "advisory_three_arm_alpha_source_identity_receipt_v1",
        "request_sha256": request.request_sha256,
        "n1_request_sha256": n1.request_sha256,
        "n1_bundle_id": request.n1_bundle_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "pit_spans_sha256": request.pit_spans_sha256,
        "feature_schema_hash": request.feature_schema_hash,
        "representative_seed_run_ids": request.representative_seed_run_ids,
        "prediction_artifacts": {
            run_id: descriptor.model_dump(mode="json") for run_id, descriptor in sorted(descriptors.items())
        },
        "parent_ranking_artifact_sha256": sha256_file(parent_ranking_path),
        "parent_ranking_parity": "MATCHED_EXACT_KEYS_AND_1E12_SCORES",
        "decision_date_count": len(sources["decision_dates"]),
        "sealed_holdout_accessed": False,
    }
    payload["source_identity_sha256"] = canonical_json_sha256(payload)
    return payload


def _publish_bundle(
    *,
    request: AdvisoryThreeArmAlphaAuditRequestV1,
    environment: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
    metrics: AlphaAuditMetricResult,
    resource_report: Mapping[str, Any],
) -> Path:
    existing = _find_existing_bundle(request)
    if existing is not None:
        return existing
    root = Path(request.output_root) / "alpha_signal_audit_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".tmp_{request.request_id}_", dir=root))
    _write_json(temp / "request.json", request.model_dump(mode="json"))
    _write_json(temp / "source_identity_receipt.json", source_receipt)
    _write_parquet(temp / "coverage_daily.parquet", metrics.coverage_daily)
    _write_parquet(temp / "full_universe_signal_outcomes.parquet", metrics.full_signal_outcomes)
    _write_parquet(temp / "arm_rankings_top50.parquet", metrics.rankings_top50)
    _write_parquet(temp / "arm_recall_daily.parquet", metrics.recall_daily)
    _write_parquet(temp / "arm_top5_daily.parquet", metrics.top5_daily)
    _write_parquet(temp / "arm_oracle_daily.parquet", metrics.oracle_daily)
    _write_parquet(temp / "signal_metrics_daily.parquet", metrics.signal_metrics_daily)
    _write_json(temp / "arm_summary.json", metrics.arm_summary)
    _write_json(temp / "pairwise_summary.json", metrics.pairwise_summary)
    _write_parquet(temp / "regime_quarter_summary.parquet", metrics.regime_quarter_summary)
    _write_json(temp / "environment.json", dict(environment))
    report = dict(resource_report)
    report["peak_rss_bytes"] = max(int(report.get("peak_rss_bytes") or 0), _peak_rss_bytes())
    if report["peak_rss_bytes"] > request.resource_max_rss_bytes:
        _raise(
            "alpha audit exceeded the approved RSS limit while publishing",
            "ADVISORY_ALPHA_AUDIT_MEMORY_LIMIT_EXCEEDED",
            peak_rss_bytes=report["peak_rss_bytes"],
        )
    _write_json(temp / "resource_report.json", report)
    result_files = _file_descriptors(temp)
    result_files_sha256 = canonical_json_sha256(
        {name: descriptor for name, descriptor in result_files.items() if name not in _RESULT_IDENTITY_EXCLUDED_FILES}
    )
    receipt = build_three_arm_alpha_audit_receipt(
        request_sha256=request.request_sha256,
        source_identity_sha256=str(source_receipt["source_identity_sha256"]),
        result_files_sha256=result_files_sha256,
        arm_ids=ARM_IDS,
        decision_date_count=int(metrics.coverage_daily["decision_as_of_trade_date"].nunique()),
        common_signal_row_count=len(metrics.full_signal_outcomes),
        evaluable_recall_day_count_by_arm={
            arm_id: int(
                metrics.recall_daily.loc[metrics.recall_daily["arm_id"].eq(arm_id), "status"].eq("AVAILABLE").sum()
            )
            for arm_id in ARM_IDS
        },
        evaluable_top5_day_count_by_arm={
            arm_id: int(metrics.top5_daily.loc[metrics.top5_daily["arm_id"].eq(arm_id), "status"].eq("AVAILABLE").sum())
            for arm_id in ARM_IDS
        },
        created_at=request.created_at,
    )
    _write_json(temp / "audit_receipt.json", receipt.model_dump(mode="json"))
    semantic_identity = {
        "schema_version": ALPHA_AUDIT_BUNDLE_SCHEMA,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
    }
    bundle_id = canonical_json_sha256(semantic_identity)
    final = root / bundle_id
    receipt_ref = EvidenceReferenceV1(
        role="n2a_three_arm_alpha_audit_receipt",
        artifact_uri=(final / "audit_receipt.json").as_posix(),
        sha256=sha256_file(temp / "audit_receipt.json"),
        size_bytes=(temp / "audit_receipt.json").stat().st_size,
    )
    record = build_trial_record(
        experiment_id=ALPHA_AUDIT_EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N2A_ALPHA_SIGNAL_DIAGNOSTIC",
        study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
        hypothesis_family_id="N2A_CURRENT_STRATEGY_PACKAGE_SIGNAL_DECOMPOSITION_V1",
        parent_lineage=ALPHA_AUDIT_PARENT_LINEAGE,
        unique_variable="FROZEN_LSTM_VS_FUNDGROWTH_VS_IC_WEIGHTED_PARENT_V1",
        objective_contract=ObjectiveContract.ALPHA_RANKING,
        dataset_identity=request.dataset_identity,
        schema_identity=request.feature_schema_hash,
        policy_identity=research_policy_identity(
            baseline_policy_sha256=request.baseline_policy_sha256,
            shadow_policy_sha256=request.shadow_policy_sha256,
            cost_policy_sha256=request.cost_policy_sha256,
        ),
        planned_trial_count=0,
        generated_trial_count=0,
        evaluated_trial_count=0,
        selected_trial_count=0,
        consumed_windows=(
            ConsumedWindowV1(
                window_id=request.window_id,
                dataset_identity=request.dataset_identity,
                start_date=request.decision_date_start,
                end_date=request.data_cutoff,
            ),
        ),
        result_class=ResearchResultClass.EXPLORATORY,
        decision_use=DecisionUse.NAVIGATION_ONLY,
        evidence_refs=(receipt_ref,),
        recorded_at=request.created_at,
    )
    _write_json(temp / "registry_record.json", record.model_dump(mode="json"))
    files = _file_descriptors(temp)
    manifest = {
        **semantic_identity,
        "bundle_id": bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "dataset_identity": request.dataset_identity,
        "policy_identity": record.policy_identity,
        "arm_ids": list(ARM_IDS),
        "planned_trial_count": 0,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
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
            "alpha audit bundle appeared concurrently",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            bundle_id=bundle_id,
        )
    _read_bundle(final)
    return final


def _read_bundle(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        request = AdvisoryThreeArmAlphaAuditRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = AdvisoryThreeArmAlphaAuditReceiptV1.model_validate_json(
            (path / "audit_receipt.json").read_text(encoding="utf-8")
        )
        raw_record = json.loads((path / "registry_record.json").read_text(encoding="utf-8"))
        source_receipt = json.loads((path / "source_identity_receipt.json").read_text(encoding="utf-8"))
        resource_report = json.loads((path / "resource_report.json").read_text(encoding="utf-8"))
        record = build_trial_record(
            **{key: value for key, value in raw_record.items() if key not in {"registry_entry_id", "record_sha256"}}
        )
    except Exception as exc:
        _raise(
            "alpha audit bundle cannot be read",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            path=str(path),
            error_type=type(exc).__name__,
        )
    expected_id = canonical_json_sha256(
        {
            "schema_version": manifest.get("schema_version"),
            "request_sha256": manifest.get("request_sha256"),
            "receipt_sha256": manifest.get("receipt_sha256"),
        }
    )
    source_functional = {key: value for key, value in source_receipt.items() if key != "source_identity_sha256"}
    receipt_descriptor = manifest.get("files", {}).get("audit_receipt.json", {})
    result_file_descriptors = {
        name: descriptor
        for name, descriptor in manifest.get("files", {}).items()
        if name not in _RESULT_IDENTITY_EXCLUDED_FILES
    }
    if (
        manifest.get("schema_version") != ALPHA_AUDIT_BUNDLE_SCHEMA
        or manifest.get("bundle_id") != path.name
        or expected_id != path.name
        or request.request_sha256 != manifest.get("request_sha256")
        or receipt.receipt_sha256 != manifest.get("receipt_sha256")
        or record.registry_entry_id != raw_record.get("registry_entry_id")
        or record.record_sha256 != raw_record.get("record_sha256")
        or source_receipt.get("source_identity_sha256") != canonical_json_sha256(source_functional)
        or source_receipt.get("source_identity_sha256") != receipt.source_identity_sha256
        or receipt.result_files_sha256 != canonical_json_sha256(result_file_descriptors)
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].role != "n2a_three_arm_alpha_audit_receipt"
        # Windows and WSL spell the same durable file differently. URI is a
        # locator; role + filename + hash + size are the cross-OS identity.
        or Path(record.evidence_refs[0].artifact_uri.replace("\\", "/")).name != "audit_receipt.json"
        or record.evidence_refs[0].sha256 != receipt_descriptor.get("sha256")
        or record.evidence_refs[0].size_bytes != receipt_descriptor.get("size_bytes")
        or int(resource_report.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
        or tuple(manifest.get("arm_ids", ())) != ARM_IDS
        or any(
            int(manifest.get(key, -1)) != 0
            for key in (
                "planned_trial_count",
                "generated_trial_count",
                "evaluated_trial_count",
                "selected_trial_count",
            )
        )
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("activated") is not False
    ):
        _raise(
            "alpha audit bundle relational identity is invalid",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            path=str(path),
        )
    _validate_bundle_files(path, manifest)
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "record": record,
        "source_receipt": source_receipt,
        "resource_report": resource_report,
    }


def _deliver_bundle(*, request: AdvisoryThreeArmAlphaAuditRequestV1, bundle_path: Path) -> dict[str, Any]:
    loaded = _read_bundle(bundle_path)
    if loaded["request"].request_sha256 != request.request_sha256:
        _raise(
            "alpha audit bundle belongs to another request",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
        )
    n1 = _load_and_verify_bound_n1(request)
    summary = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = generate_current_route(
        registry_path=request.registry_path,
        parent_spike_path=(Path(n1.n0_completion_ref.artifact_uri).parent / "parent_prediction_extension_receipt.json"),
        window_contract_path=n1.research_window_contract_path,
        output_path=n1.route_path,
    )
    if route["next_task"] != "N2_ENTRY_EXIT_QE_PREPARATION":
        _raise(
            "alpha audit delivery changed the frozen N2 main route",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            next_task=route["next_task"],
        )
    return {
        "registry": summary,
        "route": route,
        "next_task": route["next_task"],
    }


def _find_existing_bundle(request: AdvisoryThreeArmAlphaAuditRequestV1) -> Path | None:
    root = Path(request.output_root) / "alpha_signal_audit_bundles"
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
                "alpha audit bundle manifest is unreadable",
                "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
                path=str(manifest_path),
                error_type=type(exc).__name__,
            )
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise(
            "one alpha audit request resolves to multiple bundles",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            bundle_paths=[str(item) for item in matches],
        )
    if not matches:
        return None
    _read_bundle(matches[0])
    return matches[0]


def _verify_wsl_environment(
    request: AdvisoryThreeArmAlphaAuditRequestV1,
    *,
    require_repository_identity: bool,
) -> dict[str, Any]:
    if os.name == "nt" or "microsoft" not in platform.release().lower():
        _raise(
            "formal alpha audit must run inside WSL",
            "ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        _raise(
            "formal alpha audit requires the rdagent-gpu Conda environment",
            "ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            conda_env=conda_env or None,
        )
    actual_commit = _git_commit(Path(request.repository_root))
    if require_repository_identity and actual_commit != request.repository_commit:
        _raise(
            "alpha audit repository commit differs from the frozen request",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            expected=request.repository_commit,
            actual=actual_commit,
        )
    if require_repository_identity:
        dirty_paths = _git_dirty_paths(Path(request.repository_root))
        if dirty_paths:
            _raise(
                "formal alpha audit worktree is dirty",
                "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
                dirty_paths=dirty_paths[:50],
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
    }


def _run_response(
    status: str,
    request: AdvisoryThreeArmAlphaAuditRequestV1,
    bundle_path: Path,
    environment: Mapping[str, Any],
    delivery: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_id": bundle_path.name,
        "bundle_path": str(bundle_path),
        "arm_ids": list(ARM_IDS),
        "sealed_holdout_accessed": False,
        "environment": dict(environment),
        "delivery": dict(delivery),
        "backend_restart": "noop",
        "production_ddl_gate": "noop",
        "production_dml_gate": "noop",
        "runtime_activation": "noop",
    }


def _safe_corr(left: pd.Series, right: pd.Series, *, method: str) -> float:
    data = (
        pd.DataFrame(
            {
                "left": pd.to_numeric(left, errors="coerce"),
                "right": pd.to_numeric(right, errors="coerce"),
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
    )
    if len(data) < 3 or data["left"].nunique() < 2 or data["right"].nunique() < 2:
        return np.nan
    return float(data["left"].corr(data["right"], method=method))


def _bucket_spread(scores: pd.Series, values: pd.Series, *, bucket_count: int) -> float:
    data = (
        pd.DataFrame(
            {
                "score": pd.to_numeric(scores, errors="coerce"),
                "value": pd.to_numeric(values, errors="coerce"),
            }
        )
        .replace([np.inf, -np.inf], np.nan)
        .dropna()
        .sort_values("score", ascending=False)
    )
    if len(data) < bucket_count * 2:
        return np.nan
    buckets = np.array_split(data["value"].to_numpy(float), bucket_count)
    return float(np.mean(buckets[0]) - np.mean(buckets[-1]))


def _bucket_return_summary(
    signal_panel: pd.DataFrame,
    *,
    arm_id: str,
    bucket_count: int,
    block_length: int,
    repetitions: int,
    seed: int,
) -> list[dict[str, Any]]:
    daily_by_bucket: dict[int, list[float]] = {index: [] for index in range(1, bucket_count + 1)}
    score_column = f"score__{arm_id}"
    for _, frame in signal_panel.groupby("decision_as_of_trade_date", sort=True):
        known = frame.loc[
            frame["outcome_known"].fillna(False),
            [score_column, "slot_return_bps"],
        ].copy()
        known[score_column] = pd.to_numeric(known[score_column], errors="coerce")
        known["slot_return_bps"] = pd.to_numeric(known["slot_return_bps"], errors="coerce")
        known = known.replace([np.inf, -np.inf], np.nan).dropna().sort_values(score_column, ascending=False)
        if len(known) < bucket_count * 2:
            continue
        for bucket_index, positions in enumerate(np.array_split(np.arange(len(known)), bucket_count), start=1):
            daily_by_bucket[bucket_index].append(float(known.iloc[positions]["slot_return_bps"].mean()))
    return [
        {
            "bucket": bucket_index,
            "high_score_bucket": bucket_index == 1,
            "low_score_bucket": bucket_index == bucket_count,
            **_describe_daily_metric(
                values,
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + bucket_index,
            ),
        }
        for bucket_index, values in daily_by_bucket.items()
    ]


def _describe_daily_metric(
    values: Sequence[Any] | pd.Series,
    *,
    block_length: int,
    repetitions: int,
    seed: int,
) -> dict[str, Any]:
    array = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(float)
    array = array[np.isfinite(array)]
    if not len(array):
        return {
            "observation_count": 0,
            "mean": None,
            "median": None,
            "std": None,
            "icir": None,
            "positive_fraction": None,
            "confidence_lower": None,
            "confidence_upper": None,
        }
    mean = float(array.mean())
    std = float(array.std(ddof=1)) if len(array) > 1 else 0.0
    lower, upper = _moving_block_interval(
        array,
        block_length=block_length,
        repetitions=repetitions,
        seed=seed,
    )
    return {
        "observation_count": len(array),
        "mean": mean,
        "median": float(np.median(array)),
        "std": std,
        "icir": mean / std if std > 0 else None,
        "positive_fraction": float((array > 0).mean()),
        "confidence_lower": lower,
        "confidence_upper": upper,
    }


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


def _finite_mean(values: Sequence[Any] | pd.Series) -> float | None:
    array = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(float)
    array = array[np.isfinite(array)]
    return float(array.mean()) if len(array) else None


def _jaccard(left: set[str], right: set[str]) -> float:
    union = left | right
    return len(left & right) / len(union) if union else np.nan


def _write_immutable_request(path: Path, request: AdvisoryThreeArmAlphaAuditRequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if path.exists():
        existing = AdvisoryThreeArmAlphaAuditRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        if existing.request_sha256 != request.request_sha256:
            _raise(
                "existing immutable alpha audit request conflicts",
                "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
                path=str(path),
            )
        return
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())


def _write_json(path: Path, payload: Any) -> None:
    encoded = (
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")
    with path.open("xb") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    normalized = frame.copy()
    for column in normalized.columns:
        if (
            normalized[column].dtype == object
            and normalized[column].map(lambda value: isinstance(value, (dict, list, tuple, set))).any()
        ):
            normalized[column] = normalized[column].map(
                lambda value: (
                    json.dumps(
                        sorted(value) if isinstance(value, set) else value,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    if isinstance(value, (dict, list, tuple, set))
                    else value
                )
            )
    normalized.to_parquet(path, index=False)


def _parquet_row_count(path: Path) -> int:
    try:
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(path).metadata.num_rows)
    except Exception as exc:
        _raise(
            "alpha audit parquet metadata cannot be read",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            filename=path.name,
            error_type=type(exc).__name__,
        )


def _file_descriptors(root: Path) -> dict[str, dict[str, Any]]:
    files: dict[str, dict[str, Any]] = {}
    for path in sorted(root.iterdir()):
        if not path.is_file() or path.name == "manifest.json":
            continue
        descriptor: dict[str, Any] = {
            "sha256": sha256_file(path),
            "size_bytes": path.stat().st_size,
        }
        if path.suffix == ".parquet":
            descriptor["row_count"] = _parquet_row_count(path)
        files[path.name] = descriptor
    return files


def _validate_bundle_files(root: Path, manifest: Mapping[str, Any]) -> None:
    files = manifest.get("files")
    if not isinstance(files, Mapping) or not files:
        _raise(
            "alpha audit manifest has no file descriptors",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
        )
    actual = {path.name for path in root.iterdir() if path.is_file()} - {"manifest.json"}
    if actual != set(files):
        _raise(
            "alpha audit file roster differs from its manifest",
            "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
            missing=sorted(set(files) - actual),
            extra=sorted(actual - set(files)),
        )
    for name, descriptor in files.items():
        path = root / str(name)
        if (
            not path.is_file()
            or sha256_file(path) != descriptor.get("sha256")
            or path.stat().st_size != descriptor.get("size_bytes")
            or ("row_count" in descriptor and _parquet_row_count(path) != descriptor["row_count"])
        ):
            _raise(
                "alpha audit file differs from its manifest",
                "ADVISORY_ALPHA_AUDIT_BUNDLE_CONFLICT",
                filename=str(name),
            )


def _evidence_reference(path: Path, *, declared_uri: str, role: str) -> EvidenceReferenceV1:
    if not path.is_file():
        _raise(
            "alpha audit evidence file is missing",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            path=str(path),
        )
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=declared_uri,
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
    )


def _running_on_posix() -> bool:
    return os.name != "nt"


def _git_command_for_worktree(repository_root: Path) -> tuple[list[str], Path]:
    root = repository_root.resolve()
    pointer = root / ".git"
    if not pointer.is_file():
        return ["git", "-C", str(root)], root

    raw = pointer.read_text(encoding="utf-8").strip()
    if not raw.startswith("gitdir: "):
        _raise(
            "alpha audit repository .git pointer is invalid",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            repository_root=str(root),
        )
    git_dir = raw.removeprefix("gitdir: ").strip()
    command = ["git"]
    if _running_on_posix() and ":/" in git_dir:
        try:
            git_dir = subprocess.run(
                ["wslpath", "-u", git_dir],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            _raise(
                "alpha audit could not translate the worktree git directory",
                "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
                error_type=type(exc).__name__,
                repository_root=str(root),
            )
        # The linked checkout was materialized by Windows Git. Match its
        # checkout normalization so Linux Git does not report the entire CRLF
        # worktree as dirty while still detecting real tracked/untracked edits.
        command.extend(["-c", "core.fileMode=false", "-c", "core.autocrlf=true"])
    command.extend([f"--git-dir={git_dir}", f"--work-tree={root}"])
    return command, root


def _git_commit(repository_root: Path) -> str:
    command, root = _git_command_for_worktree(repository_root)
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
            "alpha audit repository commit cannot be read",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise(
            "alpha audit repository commit is invalid",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            commit=commit,
        )
    return commit


def _git_dirty_paths(repository_root: Path) -> list[str]:
    command, root = _git_command_for_worktree(repository_root)
    try:
        output = subprocess.run(
            [*command, "status", "--porcelain", "--untracked-files=all"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise(
            "alpha audit repository cleanliness cannot be read",
            "ADVISORY_ALPHA_AUDIT_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    return [line[3:] if len(line) > 3 else line for line in output.splitlines() if line.strip()]


def _local_path(path: str | Path) -> Path:
    raw = str(path)
    if os.name == "nt" and raw.startswith("/mnt/") and len(raw) > 6:
        drive = raw[5].upper()
        tail = raw[6:].replace("/", "\\")
        return Path(f"{drive}:\\{tail}")
    return Path(raw)


def _wsl_path(path: str | Path) -> str:
    raw = str(path).replace("\\", "/")
    if len(raw) >= 3 and raw[1:3] == ":/":
        return f"/mnt/{raw[0].lower()}/{raw[3:]}"
    return raw


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "AlphaAuditMetricResult",
    "build_arm_summary",
    "build_common_signal_panel",
    "build_pairwise_summary",
    "build_regime_quarter_summary",
    "build_signal_metrics_daily",
    "build_three_arm_alpha_metrics",
    "evaluate_arm_policy_metrics",
    "inspect_three_arm_alpha_audit_bundle",
    "prepare_three_arm_alpha_audit_request",
    "run_three_arm_alpha_audit",
    "verify_parent_ranking_parity",
]
