from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

try:
    import resource as _resource
except ModuleNotFoundError:  # Windows imports prepare/inspect paths.
    _resource = None

from backend.services.advisory_model_first.alpha_signal_audit_contracts import (
    PARENT_ARM_ID as N2A_PARENT_ARM_ID,
    AdvisoryThreeArmAlphaAuditRequestV1,
)
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _bucket_return_summary,
    _describe_daily_metric,
    _file_descriptors,
    _finite_mean,
    _git_commit,
    _git_dirty_paths,
    _jaccard,
    _local_path,
    _safe_corr,
    _validate_bundle_files,
    _write_json,
    _write_parquet,
    _wsl_path,
    inspect_three_arm_alpha_audit_bundle,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.independent_package_alpha_audit_contracts import (
    ARM_IDS,
    BUNDLE_SCHEMA_VERSION,
    CURRENT_PARENT_ARM_ID,
    EXPERIMENT_ID,
    FACTOR_CLOSURE_50,
    FACTOR_CLOSURE_57,
    PACKAGE_ARM_IDS,
    PACKAGE_IDS,
    PACKAGE_STATUSES,
    PARENT_LINEAGE,
    PKG_378_ARM_ID,
    PKG_5A5_ARM_ID,
    PKG_B668_ARM_ID,
    AdvisoryIndependentPackageAlphaAuditReceiptV1,
    AdvisoryIndependentPackageAlphaAuditRequestV1,
    FrozenPackageAuditArmV1,
    WorkspaceFileDescriptorV1,
    build_independent_package_alpha_audit_receipt,
    build_independent_package_alpha_audit_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file
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
from backend.services.advisory_model_first.strategy_package_batch_prediction import (
    FACTOR_INPUT_COPY_MODE_COW,
    FACTOR_IO_MODE_IN_MEMORY,
    FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES,
    PackagePredictionBatchResult,
    StrategyPackageBatchPredictionRunner,
)
from backend.services.advisory_model_first.tier1_oracle_contracts import AdvisoryN1Tier1RequestV1
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    authorize_n1_development_access,
    build_tier1_benchmark_regimes,
    build_tier1_full_universe_outcomes,
    inspect_n1_bundle,
    load_verified_n1_sources,
)
from backend.services.market_data.instrument_validator import normalize_ts_code
from backend.services.strategy_package.live_inference import QEExperimentRuntimeAssetResolver, win_to_wsl_path
from backend.services.strategy_package.models import AlphaMode
from backend.services.strategy_package.repository import StrategyPackageRecord, StrategyPackageRepository
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


REASON_REQUEST_INVALID = "ADVISORY_PACKAGE_ALPHA_AUDIT_REQUEST_INVALID"
REASON_WINDOW_FORBIDDEN = "ADVISORY_PACKAGE_ALPHA_AUDIT_WINDOW_FORBIDDEN"
REASON_SOURCE_MISMATCH = "ADVISORY_PACKAGE_ALPHA_AUDIT_SOURCE_IDENTITY_MISMATCH"
REASON_ROSTER_INVALID = "ADVISORY_PACKAGE_ALPHA_AUDIT_ROSTER_INVALID"
REASON_OUTCOME_INVALID = "ADVISORY_PACKAGE_ALPHA_AUDIT_OUTCOME_INVALID"
REASON_BUNDLE_CONFLICT = "ADVISORY_PACKAGE_ALPHA_AUDIT_BUNDLE_CONFLICT"
REASON_RESOURCE_LIMIT = "ADVISORY_PACKAGE_ALPHA_AUDIT_RESOURCE_LIMIT_EXCEEDED"

_PAIRWISE_ARMS = (
    (CURRENT_PARENT_ARM_ID, PKG_378_ARM_ID),
    (CURRENT_PARENT_ARM_ID, PKG_5A5_ARM_ID),
    (CURRENT_PARENT_ARM_ID, PKG_B668_ARM_ID),
    (PKG_378_ARM_ID, PKG_5A5_ARM_ID),
    (PKG_378_ARM_ID, PKG_B668_ARM_ID),
    (PKG_5A5_ARM_ID, PKG_B668_ARM_ID),
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
_OUTCOME_COLUMNS = (
    "target_trade_date",
    "planned_exit_trade_date",
    "effective_exit_trade_date",
    "outcome_status",
    "entry_price",
    "exit_price",
    "gross_excess_return_bps",
    "economic_net_excess_bps",
    "outcome_known",
    "slot_return_bps",
)
_FACTOR_FILE_LITERAL = re.compile(r"(?P<quote>['\"])(?P<path>[^'\"]+\.py)(?P=quote)")


@dataclass(frozen=True)
class IndependentPackageAuditMetricResult:
    coverage_daily: pd.DataFrame
    arm_signal_outcomes: pd.DataFrame
    rankings_top50: pd.DataFrame
    recall_daily: pd.DataFrame
    top5_daily: pd.DataFrame
    oracle_daily: pd.DataFrame
    signal_metrics_daily: pd.DataFrame
    arm_summary: dict[str, Any]
    pairwise_summary: dict[str, Any]
    regime_quarter_summary: pd.DataFrame


class IndependentPackageAuditProgress:
    def __init__(self, request: AdvisoryIndependentPackageAlphaAuditRequestV1) -> None:
        self.request = request
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def stage(self, name: str, started: float, **details: Any) -> None:
        item = {
            "stage": name,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": _peak_rss_bytes(),
            **details,
        }
        self.stages.append(item)
        self._assert_limits(item)

    def _assert_limits(self, item: Mapping[str, Any]) -> None:
        peak = int(item.get("peak_rss_bytes") or 0)
        if peak > self.request.resource_max_rss_bytes:
            _raise(
                "N2-B audit exceeded its frozen resource limit",
                REASON_RESOURCE_LIMIT,
                stage=item.get("stage"),
                peak_rss_bytes=peak,
                rss_limit_bytes=self.request.resource_max_rss_bytes,
                elapsed_seconds=float(item.get("elapsed_seconds") or 0.0),
            )

    def report(self, *, batch_receipt: Mapping[str, Any] | None = None) -> dict[str, Any]:
        report = {
            "schema_version": "advisory_independent_package_alpha_audit_resource_report_v1",
            "peak_rss_bytes": _peak_rss_bytes(),
            "rss_limit_bytes": self.request.resource_max_rss_bytes,
            "temp_peak_bytes": int((batch_receipt or {}).get("temp_peak_bytes") or 0),
            "temp_limit_bytes": self.request.resource_max_temp_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "wall_limit_enabled": False,
            "wall_limit_seconds": None,
            "stages": self.stages,
        }
        self._assert_limits(
            {
                "stage": "resource_report",
                "peak_rss_bytes": report["peak_rss_bytes"],
                "elapsed_seconds": report["total_wall_seconds"],
            }
        )
        if report["temp_peak_bytes"] > self.request.resource_max_temp_bytes:
            _raise(
                "N2-B audit exceeded its frozen temporary-space limit",
                REASON_RESOURCE_LIMIT,
                temp_peak_bytes=report["temp_peak_bytes"],
                temp_limit_bytes=self.request.resource_max_temp_bytes,
            )
        return report


def build_independent_package_metrics(
    *,
    parent_signal_outcomes: pd.DataFrame,
    parent_rankings_top50: pd.DataFrame,
    package_predictions: Mapping[str, pd.DataFrame],
    package_coverage: pd.DataFrame,
    outcomes: pd.DataFrame,
    outcome_coverage: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    decision_dates: Sequence[pd.Timestamp],
    trading_calendar: Sequence[pd.Timestamp],
    n1_request: AdvisoryN1Tier1RequestV1,
) -> IndependentPackageAuditMetricResult:
    """Evaluate four frozen arms without forcing a four-arm intersection."""

    decisions = pd.DatetimeIndex(pd.to_datetime(list(decision_dates))).normalize().sort_values().unique()
    if len(decisions) != 386:
        _raise("N2-B metrics require the exact 386-day window", REASON_OUTCOME_INVALID)
    normalized_outcomes = _normalize_outcomes(outcomes, decisions)
    parent_signal = _parent_signal_frame(parent_signal_outcomes, normalized_outcomes, decisions)
    package_scores = {
        arm_id: _package_prediction_frame(
            arm_id=arm_id,
            prediction=package_predictions[arm_id],
            decisions=decisions,
        )
        for arm_id in PACKAGE_ARM_IDS
    }
    target_dates = _target_trade_dates(decisions, trading_calendar)
    package_rankings = [
        _rank_one_arm(package_scores[arm_id], arm_id=arm_id, target_trade_dates=target_dates)
        for arm_id in PACKAGE_ARM_IDS
    ]
    package_signals = {
        arm_id: _attach_outcomes(package_scores[arm_id], normalized_outcomes, arm_id=arm_id)
        for arm_id in PACKAGE_ARM_IDS
    }
    signal_parts = [parent_signal, *(package_signals[arm_id] for arm_id in PACKAGE_ARM_IDS)]
    arm_signals = pd.concat(signal_parts, ignore_index=True).sort_values(
        ["arm_id", "decision_as_of_trade_date", "instrument"]
    )

    parent_ranking = _parent_rankings(parent_rankings_top50, parent_signal)
    rankings = pd.concat([parent_ranking, *package_rankings], ignore_index=True).sort_values(
        ["arm_id", "decision_as_of_trade_date", "selection_effective_rank"]
    )

    coverage = _build_coverage_daily(
        decisions=decisions,
        outcome_coverage=outcome_coverage,
        arm_signals=arm_signals,
        rankings=rankings,
        package_coverage=package_coverage,
    )
    recall_parts: list[pd.DataFrame] = []
    top5_parts: list[pd.DataFrame] = []
    oracle_parts: list[pd.DataFrame] = []
    # Reuse the frozen N2-A policy evaluator independently for each arm. This
    # keeps random-recall support arm-local and never constructs all-arm keys.
    from backend.services.advisory_model_first.alpha_signal_audit_pipeline import evaluate_arm_policy_metrics

    for arm_id in ARM_IDS:
        arm_signal = arm_signals[arm_signals["arm_id"].eq(arm_id)]
        arm_rank = rankings[rankings["arm_id"].eq(arm_id)]
        recall, top5, oracle = evaluate_arm_policy_metrics(
            rankings=arm_rank,
            outcomes=normalized_outcomes,
            outcome_coverage=coverage[coverage["arm_id"].eq(arm_id)],
            selectable_universe=arm_signal[["decision_as_of_trade_date", "instrument"]],
            winner_count=n1_request.outcome_policy.winner_count,
        )
        recall_parts.append(recall)
        top5_parts.append(top5)
        oracle_parts.append(oracle)
    recall_daily = pd.concat(recall_parts, ignore_index=True).sort_values(
        ["arm_id", "decision_as_of_trade_date"]
    )
    top5_daily = pd.concat(top5_parts, ignore_index=True).sort_values(
        ["arm_id", "decision_as_of_trade_date"]
    )
    oracle_daily = pd.concat(oracle_parts, ignore_index=True).sort_values(
        ["arm_id", "decision_as_of_trade_date"]
    )
    signal_daily = build_own_universe_signal_metrics_daily(arm_signals)
    block = n1_request.inference_policy.block_length_trading_days
    repetitions = n1_request.inference_policy.bootstrap_repetitions
    seed = n1_request.inference_policy.random_seed
    arm_summary = build_independent_arm_summary(
        arm_signals=arm_signals,
        signal_metrics_daily=signal_daily,
        recall_daily=recall_daily,
        top5_daily=top5_daily,
        oracle_daily=oracle_daily,
        coverage_daily=coverage,
        block_length=block,
        repetitions=repetitions,
        seed=seed,
    )
    pairwise = build_independent_pairwise_summary(
        arm_signals=arm_signals,
        rankings=rankings,
        top5_daily=top5_daily,
        block_length=block,
        repetitions=repetitions,
        seed=seed,
    )
    regime_map = build_tier1_benchmark_regimes(benchmark_daily, decisions)
    from backend.services.advisory_model_first.alpha_signal_audit_pipeline import build_regime_quarter_summary

    regime_quarter = build_regime_quarter_summary(
        signal_metrics_daily=signal_daily,
        top5_daily=top5_daily,
        recall_daily=recall_daily,
        regime_map=regime_map,
    )
    return IndependentPackageAuditMetricResult(
        coverage_daily=coverage,
        arm_signal_outcomes=arm_signals,
        rankings_top50=rankings,
        recall_daily=recall_daily,
        top5_daily=top5_daily,
        oracle_daily=oracle_daily,
        signal_metrics_daily=signal_daily,
        arm_summary=arm_summary,
        pairwise_summary=pairwise,
        regime_quarter_summary=regime_quarter,
    )


def build_own_universe_signal_metrics_daily(arm_signals: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (arm_id, decision), frame in arm_signals.groupby(
        ["arm_id", "decision_as_of_trade_date"], sort=True
    ):
        matured = frame[frame["outcome_status"].eq("MATURED")]
        known = frame[frame["outcome_known"].fillna(False)]
        rows.append(
            {
                "arm_id": str(arm_id),
                "decision_as_of_trade_date": pd.Timestamp(decision).normalize(),
                "own_row_count": len(frame),
                "matured_row_count": len(matured),
                "known_row_count": len(known),
                "matured_pearson_ic": _safe_corr(
                    matured["score"], matured["economic_net_excess_bps"], method="pearson"
                ),
                "matured_rank_ic": _safe_corr(
                    matured["score"], matured["economic_net_excess_bps"], method="spearman"
                ),
                "policy_rank_ic": _safe_corr(known["score"], known["slot_return_bps"], method="spearman"),
                "quintile_spread_bps": _bucket_spread(known["score"], known["slot_return_bps"], 5),
                "decile_spread_bps": _bucket_spread(known["score"], known["slot_return_bps"], 10),
            }
        )
    return pd.DataFrame(rows).sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)


def build_independent_arm_summary(
    *,
    arm_signals: pd.DataFrame,
    signal_metrics_daily: pd.DataFrame,
    recall_daily: pd.DataFrame,
    top5_daily: pd.DataFrame,
    oracle_daily: pd.DataFrame,
    coverage_daily: pd.DataFrame,
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
        coverage = coverage_daily[coverage_daily["arm_id"].eq(arm_id)]
        arm_panel = arm_signals[arm_signals["arm_id"].eq(arm_id)].copy()
        arm_panel[f"score__{arm_id}"] = arm_panel["score"]
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
            metrics[f"top{depth}_recall_lift"] = _describe_daily_metric(
                recall[f"top{depth}_recall_lift"],
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + arm_index * 100 + 50 + depth,
            )
        arms[arm_id] = {
            "signal_day_count": len(signal),
            "own_signal_row_count": int(len(arm_panel)),
            "mean_prediction_count": _finite_mean(coverage["prediction_count"]),
            "minimum_prediction_count": int(coverage["prediction_count"].min()),
            "top50_complete_day_count": int(coverage["rank_status"].eq("COMPLETE").sum()),
            "top5_evaluable_day_count": int(top5["status"].eq("AVAILABLE").sum()),
            "top5_positive_day_fraction": _finite_mean(
                top5.loc[top5["status"].eq("AVAILABLE"), "positive"]
            ),
            "oracle_evaluable_day_count": len(oracle),
            "oracle_intervention_day_count": int(oracle["intervened"].sum()) if len(oracle) else 0,
            "metrics": metrics,
            "bucket_returns": {
                f"{bucket_count}_bucket": _bucket_return_summary(
                    arm_panel,
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
        "schema_version": "advisory_independent_package_alpha_audit_summary_v1",
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "universe_semantics": "ARM_OWN_UNIVERSE_NO_FOUR_ARM_INTERSECTION",
        "arms": arms,
    }


def build_independent_pairwise_summary(
    *,
    arm_signals: pd.DataFrame,
    rankings: pd.DataFrame,
    top5_daily: pd.DataFrame,
    block_length: int,
    repetitions: int,
    seed: int,
) -> dict[str, Any]:
    ranking_sets: dict[tuple[str, pd.Timestamp, int], set[str]] = {}
    ranking_maps: dict[tuple[str, pd.Timestamp], dict[str, int]] = {}
    for (arm_id, decision), frame in rankings.groupby(
        ["arm_id", "decision_as_of_trade_date"], sort=True
    ):
        normalized_day = pd.Timestamp(decision).normalize()
        ranking_maps[(str(arm_id), normalized_day)] = dict(
            zip(frame["instrument"].astype(str), frame["selection_effective_rank"].astype(int))
        )
        for depth in (5, 20):
            ranking_sets[(str(arm_id), normalized_day, depth)] = set(
                frame.loc[frame["selection_effective_rank"] <= depth, "instrument"].astype(str)
            )
    churn = _arm_churn(ranking_sets)
    signal_by_arm = {
        arm_id: arm_signals.loc[
            arm_signals["arm_id"].eq(arm_id),
            [
                "decision_as_of_trade_date",
                "instrument",
                "score",
                "outcome_status",
                "economic_net_excess_bps",
            ],
        ].copy()
        for arm_id in ARM_IDS
    }
    top5_pivot = top5_daily.pivot(
        index="decision_as_of_trade_date", columns="arm_id", values="top5_net_excess_bps"
    )
    pairs: dict[str, Any] = {}
    for pair_index, (left, right) in enumerate(_PAIRWISE_ARMS):
        common = signal_by_arm[left].merge(
            signal_by_arm[right],
            on=["decision_as_of_trade_date", "instrument"],
            how="inner",
            validate="one_to_one",
            suffixes=("__left", "__right"),
        )
        if common.empty:
            _raise("one frozen pair has no common prediction keys", REASON_OUTCOME_INVALID, left=left, right=right)
        daily_rows: list[dict[str, Any]] = []
        for decision, frame in common.groupby("decision_as_of_trade_date", sort=True):
            decision = pd.Timestamp(decision).normalize()
            matured = frame[
                frame["outcome_status__left"].eq("MATURED")
                & frame["outcome_status__right"].eq("MATURED")
            ]
            left_norm = _zscore(frame["score__left"])
            right_norm = _zscore(frame["score__right"])
            left_ranks = ranking_maps.get((left, decision), {})
            right_ranks = ranking_maps.get((right, decision), {})
            shared_ranked = sorted(set(left_ranks) & set(right_ranks))
            daily_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "common_row_count": len(frame),
                    "raw_score_pearson": _safe_corr(frame["score__left"], frame["score__right"], method="pearson"),
                    "raw_score_spearman": _safe_corr(frame["score__left"], frame["score__right"], method="spearman"),
                    "normalized_score_pearson": _safe_corr(
                        pd.Series(left_norm), pd.Series(right_norm), method="pearson"
                    ),
                    "normalized_score_spearman": _safe_corr(
                        pd.Series(left_norm), pd.Series(right_norm), method="spearman"
                    ),
                    "left_common_matured_rank_ic": _safe_corr(
                        matured["score__left"], matured["economic_net_excess_bps__left"], method="spearman"
                    ),
                    "right_common_matured_rank_ic": _safe_corr(
                        matured["score__right"], matured["economic_net_excess_bps__right"], method="spearman"
                    ),
                    "top5_jaccard": _jaccard(
                        ranking_sets.get((left, decision, 5), set()),
                        ranking_sets.get((right, decision, 5), set()),
                    ),
                    "top20_jaccard": _jaccard(
                        ranking_sets.get((left, decision, 20), set()),
                        ranking_sets.get((right, decision, 20), set()),
                    ),
                    "top50_common_rank_spearman": _safe_corr(
                        pd.Series([left_ranks[key] for key in shared_ranked], dtype=float),
                        pd.Series([right_ranks[key] for key in shared_ranked], dtype=float),
                        method="spearman",
                    ),
                }
            )
        daily = pd.DataFrame(daily_rows)
        daily["common_matured_rank_ic_delta"] = (
            daily["left_common_matured_rank_ic"] - daily["right_common_matured_rank_ic"]
        )
        top5_delta = top5_pivot[left] - top5_pivot[right]
        pair_key = f"{left}_MINUS_{right}"
        pairs[pair_key] = {
            "left_arm": left,
            "right_arm": right,
            "own_row_count": {
                left: int(len(signal_by_arm[left])),
                right: int(len(signal_by_arm[right])),
            },
            "pairwise_common_row_count": int(len(common)),
            "pairwise_common_day_count": int(common["decision_as_of_trade_date"].nunique()),
            "top5_net_excess_delta": _describe_daily_metric(
                top5_delta,
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + pair_index * 100 + 1,
            ),
            "common_matured_rank_ic_delta": _describe_daily_metric(
                daily["common_matured_rank_ic_delta"],
                block_length=block_length,
                repetitions=repetitions,
                seed=seed + pair_index * 100 + 2,
            ),
            "mean_raw_score_pearson": _finite_mean(daily["raw_score_pearson"]),
            "mean_raw_score_spearman": _finite_mean(daily["raw_score_spearman"]),
            "mean_normalized_score_pearson": _finite_mean(daily["normalized_score_pearson"]),
            "mean_normalized_score_spearman": _finite_mean(daily["normalized_score_spearman"]),
            "two_way_residual_score_pearson": _two_way_residual_correlation(common),
            "mean_top5_jaccard": _finite_mean(daily["top5_jaccard"]),
            "mean_top20_jaccard": _finite_mean(daily["top20_jaccard"]),
            "mean_top50_common_rank_spearman": _finite_mean(daily["top50_common_rank_spearman"]),
        }
    return {
        "schema_version": "advisory_independent_package_alpha_pairwise_summary_v1",
        "pair_roster": [f"{left}_MINUS_{right}" for left, right in _PAIRWISE_ARMS],
        "pairwise_universe_semantics": "PAIR_ONLY_COMMON_KEYS",
        "pairs": pairs,
        "arm_churn": churn,
    }


def _normalize_outcomes(outcomes: pd.DataFrame, decisions: pd.DatetimeIndex) -> pd.DataFrame:
    required = {"decision_as_of_trade_date", "instrument", *_OUTCOME_COLUMNS}
    if not required.issubset(outcomes):
        _raise("full-universe outcome columns are incomplete", REASON_OUTCOME_INVALID)
    value = outcomes[list(required)].copy()
    value["decision_as_of_trade_date"] = pd.to_datetime(value["decision_as_of_trade_date"]).dt.normalize()
    value["instrument"] = value["instrument"].map(normalize_ts_code)
    value = value[value["decision_as_of_trade_date"].isin(decisions)]
    if value.empty or value.duplicated(["decision_as_of_trade_date", "instrument"]).any():
        _raise("full-universe outcomes are empty or duplicate", REASON_OUTCOME_INVALID)
    return value.sort_values(["decision_as_of_trade_date", "instrument"]).reset_index(drop=True)


def _parent_signal_frame(
    parent: pd.DataFrame,
    outcomes: pd.DataFrame,
    decisions: pd.DatetimeIndex,
) -> pd.DataFrame:
    score_column = f"score__{N2A_PARENT_ARM_ID}"
    required = {"decision_as_of_trade_date", "instrument", score_column, *_OUTCOME_COLUMNS}
    if not required.issubset(parent):
        _raise("N2-A parent signal artifact is incomplete", REASON_SOURCE_MISMATCH)
    signal = parent[["decision_as_of_trade_date", "instrument", score_column, *_OUTCOME_COLUMNS]].copy()
    signal["decision_as_of_trade_date"] = pd.to_datetime(signal["decision_as_of_trade_date"]).dt.normalize()
    signal["instrument"] = signal["instrument"].map(normalize_ts_code)
    signal = signal[signal["decision_as_of_trade_date"].isin(decisions)]
    signal["score"] = pd.to_numeric(signal.pop(score_column), errors="coerce")
    if signal.empty or signal.duplicated(["decision_as_of_trade_date", "instrument"]).any() or not np.isfinite(
        signal["score"].to_numpy(float)
    ).all():
        _raise("N2-A parent scores are invalid", REASON_SOURCE_MISMATCH)
    check = signal.merge(
        outcomes,
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
        suffixes=("__n2a", "__rebuilt"),
    )
    for column in _OUTCOME_COLUMNS:
        if not _series_semantically_equal(check[f"{column}__n2a"], check[f"{column}__rebuilt"]):
            _raise(
                "N2-A parent outcome semantics differ from the rebuilt N1 outcomes",
                REASON_OUTCOME_INVALID,
                column=column,
            )
    signal.insert(0, "arm_id", CURRENT_PARENT_ARM_ID)
    return signal[["arm_id", "decision_as_of_trade_date", "instrument", "score", *_OUTCOME_COLUMNS]]


def _package_prediction_frame(
    *,
    arm_id: str,
    prediction: pd.DataFrame,
    decisions: pd.DatetimeIndex,
) -> pd.DataFrame:
    if not isinstance(prediction.index, pd.MultiIndex) or set(prediction.index.names) != {"datetime", "instrument"}:
        _raise("package prediction index is invalid", REASON_SOURCE_MISMATCH, arm_id=arm_id)
    value = prediction[["score"]].reset_index().rename(columns={"datetime": "decision_as_of_trade_date"})
    value["decision_as_of_trade_date"] = pd.to_datetime(value["decision_as_of_trade_date"]).dt.normalize()
    value["instrument"] = value["instrument"].map(normalize_ts_code)
    value["score"] = pd.to_numeric(value["score"], errors="coerce")
    value = value[value["decision_as_of_trade_date"].isin(decisions)]
    if value.empty or value.duplicated(["decision_as_of_trade_date", "instrument"]).any() or not np.isfinite(
        value["score"].to_numpy(float)
    ).all():
        _raise("package prediction rows are invalid", REASON_SOURCE_MISMATCH, arm_id=arm_id)
    value.insert(0, "arm_id", arm_id)
    return value[["arm_id", "decision_as_of_trade_date", "instrument", "score"]]


def _attach_outcomes(signal: pd.DataFrame, outcomes: pd.DataFrame, *, arm_id: str) -> pd.DataFrame:
    merged = signal.merge(
        outcomes,
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
    )
    if merged["outcome_status"].isna().any():
        _raise("package predictions contain keys outside N1 PIT outcomes", REASON_OUTCOME_INVALID, arm_id=arm_id)
    return merged[["arm_id", "decision_as_of_trade_date", "instrument", "score", *_OUTCOME_COLUMNS]]


def _parent_rankings(parent_rankings: pd.DataFrame, parent_signal: pd.DataFrame) -> pd.DataFrame:
    required = {
        "arm_id",
        "decision_as_of_trade_date",
        "instrument",
        "selection_effective_rank",
        "target_trade_date",
        "combined_score",
    }
    if not required.issubset(parent_rankings):
        _raise("N2-A parent ranking artifact is incomplete", REASON_SOURCE_MISMATCH)
    value = parent_rankings[parent_rankings["arm_id"].eq(N2A_PARENT_ARM_ID)].copy()
    value["decision_as_of_trade_date"] = pd.to_datetime(value["decision_as_of_trade_date"]).dt.normalize()
    value["instrument"] = value["instrument"].map(normalize_ts_code)
    value["score"] = pd.to_numeric(value["combined_score"], errors="coerce")
    value["arm_id"] = CURRENT_PARENT_ARM_ID
    parity = value.merge(
        parent_signal[["decision_as_of_trade_date", "instrument", "score"]],
        on=["decision_as_of_trade_date", "instrument"],
        how="left",
        validate="one_to_one",
        suffixes=("__ranking", "__signal"),
    )
    if len(value) != 386 * 50 or parity["score__signal"].isna().any() or not np.allclose(
        parity["score__ranking"], parity["score__signal"], rtol=0.0, atol=1e-12
    ):
        _raise("N2-A parent Top50 does not match its frozen score artifact", REASON_SOURCE_MISMATCH)
    return value[
        ["arm_id", "decision_as_of_trade_date", "instrument", "selection_effective_rank", "target_trade_date", "score"]
    ]


def _rank_one_arm(
    signal: pd.DataFrame,
    *,
    arm_id: str,
    target_trade_dates: Mapping[pd.Timestamp, pd.Timestamp],
) -> pd.DataFrame:
    rows: list[pd.DataFrame] = []
    for decision, frame in signal.groupby("decision_as_of_trade_date", sort=True):
        if len(frame) < 50:
            continue
        ranked = frame.sort_values(["score", "instrument"], ascending=[False, True]).head(50).copy()
        ranked["selection_effective_rank"] = np.arange(1, len(ranked) + 1)
        ranked["target_trade_date"] = target_trade_dates[pd.Timestamp(decision).normalize()]
        rows.append(
            ranked[
                ["decision_as_of_trade_date", "instrument", "selection_effective_rank", "target_trade_date", "score"]
            ]
        )
    if not rows:
        _raise("package arm has no Top50-complete day", REASON_OUTCOME_INVALID, arm_id=arm_id)
    value = pd.concat(rows, ignore_index=True)
    value.insert(0, "arm_id", arm_id)
    return value


def _target_trade_dates(
    decisions: pd.DatetimeIndex,
    trading_calendar: Sequence[pd.Timestamp],
) -> dict[pd.Timestamp, pd.Timestamp]:
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    positions = calendar.get_indexer(decisions)
    if (positions < 0).any() or (positions + 1 >= len(calendar)).any():
        _raise("N1 calendar cannot derive every T+1 target date", REASON_OUTCOME_INVALID)
    return {
        pd.Timestamp(decision).normalize(): pd.Timestamp(calendar[position + 1]).normalize()
        for decision, position in zip(decisions, positions)
    }


def _build_coverage_daily(
    *,
    decisions: pd.DatetimeIndex,
    outcome_coverage: pd.DataFrame,
    arm_signals: pd.DataFrame,
    rankings: pd.DataFrame,
    package_coverage: pd.DataFrame,
) -> pd.DataFrame:
    outcome = outcome_coverage.copy()
    outcome["decision_as_of_trade_date"] = pd.to_datetime(outcome["decision_as_of_trade_date"]).dt.normalize()
    rows = pd.MultiIndex.from_product([ARM_IDS, decisions], names=["arm_id", "decision_as_of_trade_date"]).to_frame(
        index=False
    )
    rows = rows.merge(outcome, on="decision_as_of_trade_date", how="left", validate="many_to_one")
    signal_counts = arm_signals.groupby(["arm_id", "decision_as_of_trade_date"]).size()
    rank_counts = rankings.groupby(["arm_id", "decision_as_of_trade_date"]).size()
    keys = pd.MultiIndex.from_frame(rows[["arm_id", "decision_as_of_trade_date"]])
    rows["prediction_count"] = signal_counts.reindex(keys, fill_value=0).to_numpy(dtype=int)
    rows["rank_count"] = rank_counts.reindex(keys, fill_value=0).to_numpy(dtype=int)
    rows["rank_status"] = np.where(rows["rank_count"].eq(50), "COMPLETE", "DATA_UNAVAILABLE")
    if not package_coverage.empty:
        detail = package_coverage.copy()
        detail["decision_as_of_trade_date"] = pd.to_datetime(detail["decision_as_of_trade_date"]).dt.normalize()
        detail = detail.rename(columns={"finite_score_count": "batch_finite_score_count"})
        keep = [
            "arm_id",
            "decision_as_of_trade_date",
            "feature_input_count",
            "fully_scorable_feature_count",
            "batch_finite_score_count",
            "missing_feature_row_count",
            "missing_feature_cell_count",
            "pit_or_market_absent_count",
        ]
        rows = rows.merge(detail[keep], on=["arm_id", "decision_as_of_trade_date"], how="left", validate="one_to_one")
        package_mask = rows["arm_id"].isin(PACKAGE_ARM_IDS)
        mismatch = package_mask & rows["batch_finite_score_count"].notna() & (
            rows["prediction_count"] != rows["batch_finite_score_count"]
        )
        if mismatch.any():
            _raise("package batch coverage differs from metric input", REASON_SOURCE_MISMATCH)
    return rows.sort_values(["arm_id", "decision_as_of_trade_date"]).reset_index(drop=True)


def _arm_churn(ranking_sets: Mapping[tuple[str, pd.Timestamp, int], set[str]]) -> dict[str, dict[str, float | None]]:
    result: dict[str, dict[str, float | None]] = {}
    for arm_id in ARM_IDS:
        dates = sorted({key[1] for key in ranking_sets if key[0] == arm_id})
        result[arm_id] = {}
        for depth in (5, 20):
            values = [
                1.0
                - len(
                    ranking_sets[(arm_id, dates[index - 1], depth)]
                    & ranking_sets[(arm_id, dates[index], depth)]
                )
                / depth
                for index in range(1, len(dates))
            ]
            result[arm_id][f"top{depth}_mean_churn"] = float(np.mean(values)) if values else None
    return result


def _two_way_residual_correlation(common: pd.DataFrame) -> float:
    values = common[["decision_as_of_trade_date", "instrument", "score__left", "score__right"]].copy()
    residuals: dict[str, pd.Series] = {}
    for side in ("left", "right"):
        column = f"score__{side}"
        date_mean = values.groupby("decision_as_of_trade_date")[column].transform("mean")
        instrument_mean = values.groupby("instrument")[column].transform("mean")
        residuals[side] = values[column] - date_mean - instrument_mean + float(values[column].mean())
    return _safe_corr(residuals["left"], residuals["right"], method="pearson")


def _bucket_spread(scores: pd.Series, returns: pd.Series, bucket_count: int) -> float:
    data = pd.DataFrame(
        {
            "score": pd.to_numeric(scores, errors="coerce"),
            "return": pd.to_numeric(returns, errors="coerce"),
        }
    ).replace([np.inf, -np.inf], np.nan).dropna().sort_values("score", ascending=False)
    if len(data) < bucket_count * 2:
        return np.nan
    buckets = np.array_split(data["return"].to_numpy(float), bucket_count)
    return float(np.mean(buckets[0]) - np.mean(buckets[-1]))


def _zscore(values: pd.Series) -> np.ndarray:
    numeric = pd.to_numeric(values, errors="coerce").to_numpy(float)
    std = float(np.std(numeric, ddof=0))
    return (numeric - float(np.mean(numeric))) / std if std > 0 else np.zeros(len(numeric), dtype=float)


def _series_semantically_equal(left: pd.Series, right: pd.Series) -> bool:
    if len(left) != len(right):
        return False
    numeric_left = pd.to_numeric(left, errors="coerce")
    numeric_right = pd.to_numeric(right, errors="coerce")
    if numeric_left.notna().any() or numeric_right.notna().any():
        numeric_equal = np.isclose(
            numeric_left.to_numpy(float), numeric_right.to_numpy(float), rtol=0.0, atol=1e-12, equal_nan=True
        )
        non_numeric = numeric_left.isna() & numeric_right.isna()
        if not bool((numeric_equal | non_numeric.to_numpy()).all()):
            return False
    text_mask = numeric_left.isna() & numeric_right.isna()
    return left[text_mask].astype(str).reset_index(drop=True).equals(
        right[text_mask].astype(str).reset_index(drop=True)
    )


def prepare_independent_package_alpha_audit_request(
    *,
    n1_request_path: str | Path,
    n1_bundle_path: str | Path,
    n2a_request_path: str | Path,
    n2a_bundle_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
    package_repository: StrategyPackageRepository | None = None,
    runtime_asset_resolver: QEExperimentRuntimeAssetResolver | None = None,
) -> AdvisoryIndependentPackageAlphaAuditRequestV1:
    """Freeze the exact package-owned sources without reading scientific data."""

    n1_path = _local_path(n1_request_path)
    n1_bundle_local = _local_path(n1_bundle_path)
    n2a_path = _local_path(n2a_request_path)
    n2a_bundle_local = _local_path(n2a_bundle_path)
    try:
        n1 = AdvisoryN1Tier1RequestV1.model_validate_json(n1_path.read_text(encoding="utf-8"))
        n2a = AdvisoryThreeArmAlphaAuditRequestV1.model_validate_json(n2a_path.read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            "N2-B cannot read its frozen N1/N2-A request",
            REASON_REQUEST_INVALID,
            error_type=type(exc).__name__,
        )
    n1_inspected = _read_bound_bundle_manifest(n1_bundle_local, label="N1")
    n2a_inspected = _read_bound_bundle_manifest(n2a_bundle_local, label="N2-A")
    if (
        n1_inspected["request_sha256"] != n1.request_sha256
        or n2a_inspected["request_sha256"] != n2a.request_sha256
        or n2a.n1_request_sha256 != n1.request_sha256
        or n2a.n1_bundle_id != n1_inspected["bundle_id"]
    ):
        _raise("N1/N2-A request and bundle lineage differs", REASON_SOURCE_MISMATCH)
    repository_local = _local_path(repository_root)
    dirty = _git_dirty_paths(repository_local)
    if dirty:
        _raise(
            "N2-B request must bind a clean committed worktree",
            REASON_REQUEST_INVALID,
            dirty_paths=dirty[:50],
        )
    output_local = _local_path(output_root)
    output_local.mkdir(parents=True, exist_ok=True)
    package_repository = package_repository or StrategyPackageRepository()
    runtime_asset_resolver = runtime_asset_resolver or QEExperimentRuntimeAssetResolver(
        cache_root=output_local / "_inputs" / "package_runtime"
    )
    packages = _freeze_package_roster(
        repository=package_repository,
        resolver=runtime_asset_resolver,
        output_root=output_local,
    )
    request = build_independent_package_alpha_audit_request(
        n0_completion_ref=n1.n0_completion_ref,
        n0_completion_receipt_sha256=n1.n0_completion_receipt_sha256,
        research_window_contract_ref=n1.research_window_contract_ref,
        research_window_contract_sha256=n1.research_window_contract_sha256,
        n1_request_ref=_evidence_reference(n1_path, _wsl_path(n1_request_path), "n1_frozen_request"),
        n1_request_sha256=n1.request_sha256,
        n1_bundle_path=_wsl_path(n1_bundle_path),
        n1_bundle_manifest_ref=_evidence_reference(
            n1_bundle_local / "manifest.json",
            _wsl_path(Path(n1_bundle_path) / "manifest.json"),
            "n1_formal_bundle_manifest",
        ),
        n1_bundle_id=n1_inspected["bundle_id"],
        n2a_request_ref=_evidence_reference(n2a_path, _wsl_path(n2a_request_path), "n2a_frozen_request"),
        n2a_request_sha256=n2a.request_sha256,
        n2a_bundle_path=_wsl_path(n2a_bundle_path),
        n2a_bundle_manifest_ref=_evidence_reference(
            n2a_bundle_local / "manifest.json",
            _wsl_path(Path(n2a_bundle_path) / "manifest.json"),
            "n2a_formal_bundle_manifest",
        ),
        n2a_bundle_id=n2a_inspected["bundle_id"],
        registry_path=n1.registry_path,
        program_id=n1.program_id,
        binding_version_id=n1.binding_version_id,
        current_parent_package_id=n1.package_id,
        current_parent_manifest_sha256=n1.manifest_sha256,
        selection_runtime_semantics_hash=n1.selection_runtime_semantics_hash,
        baseline_policy_sha256=n1.baseline_policy_sha256,
        shadow_policy_sha256=n1.shadow_policy_sha256,
        cost_policy_sha256=n1.cost_policy_sha256,
        split_policy_sha256=n1.split_policy_sha256,
        policy_dataset_bundle_id=n1.policy_dataset_bundle_id,
        pit_spans_sha256=n1.pit_snapshot.spans_sha256,
        feature_schema_hash=n1.feature_schema_hash,
        packages=packages,
        repository_root=_wsl_path(repository_root),
        repository_commit=_git_commit(repository_local),
        prediction_store_root=_wsl_path(output_local / "prediction_store"),
        output_root=_wsl_path(output_root),
    )
    _write_immutable_request(_local_path(output_path), request)
    return request


def run_independent_package_alpha_audit(request_path: str | Path) -> dict[str, Any]:
    try:
        request = AdvisoryIndependentPackageAlphaAuditRequestV1.model_validate_json(
            Path(request_path).read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "N2-B request cannot be read",
            REASON_REQUEST_INVALID,
            error_type=type(exc).__name__,
        )
    n1, n2a = _load_and_verify_bound_requests(request)
    # The access decision must precede workspace, Prediction Store, Qlib, PIT,
    # factor, market, suspend, and outcome loaders.
    authorize_n1_development_access(n1)
    _verify_bound_bundle_identities(request, n1, n2a)
    existing = _find_existing_bundle(request)
    if existing is not None:
        environment = _verify_wsl_environment(request, require_repository_identity=False)
        delivery = _deliver_bundle(request=request, bundle_path=existing, n1=n1)
        return _run_response("EXISTING_BUNDLE", request, existing, environment, delivery)
    environment = _verify_wsl_environment(request, require_repository_identity=True)
    progress = IndependentPackageAuditProgress(request)

    started = time.monotonic()
    sources = load_verified_n1_sources(n1)
    _verify_source_contract(request, n1, n2a, sources)
    progress.stage("source_identity", started, decision_date_count=len(sources["decision_dates"]))

    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"aistock_n2b_{request.request_id}_") as batch_temp:
        batch = StrategyPackageBatchPredictionRunner().run(
            request=request,
            pit_snapshot=sources["pit_snapshot"],
            decision_dates=sources["decision_dates"],
            temp_root=Path(batch_temp),
        )
    progress.stage(
        "package_batch_prediction",
        started,
        prediction_rows={arm_id: len(frame) for arm_id, frame in batch.predictions.items()},
        temp_peak_bytes=int(batch.batch_receipt.get("temp_peak_bytes") or 0),
    )

    started = time.monotonic()
    pit_symbols = sorted({span.ts_code for span in sources["pit_snapshot"].spans})
    daily = load_qlib_daily(
        pit_symbols,
        start=request.decision_date_start.isoformat(),
        end=request.data_cutoff.isoformat(),
    )
    benchmark = load_qlib_daily(
        [n1.cost_policy.benchmark_instrument],
        start="2023-09-01",
        end=request.data_cutoff.isoformat(),
        fields=("$open", "$close"),
    )
    suspend = load_suspend_rows(
        n1.suspend_data_root,
        start=request.decision_date_start.isoformat(),
        end=request.data_cutoff.isoformat(),
        instruments=pit_symbols,
        full_day_only=True,
    )
    outcome = build_tier1_full_universe_outcomes(
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        pit_snapshot=sources["pit_snapshot"],
        trading_calendar=sources["n1_calendar"],
        decision_dates=sources["decision_dates"],
        request=n1,
    )
    del daily, suspend
    gc.collect()
    progress.stage("full_universe_outcomes", started, row_count=len(outcome.outcomes))

    started = time.monotonic()
    parent_signal_path = Path(request.n2a_bundle_path) / "full_universe_signal_outcomes.parquet"
    parent_ranking_path = Path(request.n2a_bundle_path) / "arm_rankings_top50.parquet"
    try:
        parent_signal = pd.read_parquet(parent_signal_path)
        parent_rankings = pd.read_parquet(parent_ranking_path)
    except Exception as exc:
        _raise(
            "N2-A parent scientific artifacts cannot be read",
            REASON_SOURCE_MISMATCH,
            error_type=type(exc).__name__,
        )
    metrics = build_independent_package_metrics(
        parent_signal_outcomes=parent_signal,
        parent_rankings_top50=parent_rankings,
        package_predictions=batch.predictions,
        package_coverage=batch.coverage_daily,
        outcomes=outcome.outcomes,
        outcome_coverage=outcome.coverage,
        benchmark_daily=benchmark,
        decision_dates=sources["decision_dates"],
        trading_calendar=sources["n1_calendar"],
        n1_request=n1,
    )
    progress.stage(
        "four_arm_metrics",
        started,
        arm_signal_rows={
            arm_id: int(metrics.arm_signal_outcomes["arm_id"].eq(arm_id).sum()) for arm_id in ARM_IDS
        },
        pair_count=len(metrics.pairwise_summary["pairs"]),
    )
    del parent_signal, parent_rankings, outcome, benchmark
    gc.collect()

    source_receipt = _source_identity_receipt(request, n1, n2a, sources, batch)
    inventory = _package_inventory_context(request)
    bundle = _publish_bundle(
        request=request,
        environment=environment,
        source_receipt=source_receipt,
        package_inventory=inventory,
        batch=batch,
        metrics=metrics,
        resource_report=progress.report(batch_receipt=batch.batch_receipt),
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle, n1=n1)
    return _run_response("COMPLETE", request, bundle, environment, delivery)


def inspect_independent_package_alpha_audit_bundle(bundle_path: str | Path) -> dict[str, Any]:
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


def _freeze_package_roster(
    *,
    repository: StrategyPackageRepository,
    resolver: QEExperimentRuntimeAssetResolver,
    output_root: Path,
) -> tuple[FrozenPackageAuditArmV1, ...]:
    arms: list[FrozenPackageAuditArmV1] = []
    expected_closures = (FACTOR_CLOSURE_57, FACTOR_CLOSURE_57, FACTOR_CLOSURE_50)
    for index, (arm_id, package_id, status, expected_closure) in enumerate(
        zip(PACKAGE_ARM_IDS, PACKAGE_IDS, PACKAGE_STATUSES, expected_closures)
    ):
        try:
            record = repository.get(package_id)
        except Exception as exc:
            _raise(
                "one frozen StrategyPackage cannot be read",
                REASON_ROSTER_INVALID,
                package_id=package_id,
                error_type=type(exc).__name__,
            )
        _validate_package_record(record, package_id=package_id, status=status)
        manifest = record.current_manifest()
        snapshot_path = output_root / "_inputs" / "package_snapshots" / f"{index + 1:02d}_{package_id}.json"
        _write_immutable_json(snapshot_path, manifest.model_dump(mode="json"), REASON_ROSTER_INVALID)
        try:
            source = resolver.load_frozen_source_for_strategy_package(
                manifest=manifest,
                package_id=package_id,
                cache_namespace="n2b_independent_alpha_audit",
            )
            prepared = resolver.prepare_workspace(
                package_id=package_id,
                manifest_sha256=record.manifest_sha256,
                source=source,
                path_converter=win_to_wsl_path,
                cache_namespace="n2b_independent_alpha_audit",
                verify_model_code_contract=True,
            )
            _self_contain_workspace(prepared, path_converter=win_to_wsl_path)
        except AdvisoryModelFirstError:
            raise
        except Exception as exc:
            _raise(
                "package-owned CAS workspace cannot be materialized",
                REASON_ROSTER_INVALID,
                package_id=package_id,
                error_type=type(exc).__name__,
                error=str(exc),
            )
        factor_closure = _factor_closure(manifest, prepared.factor_order)
        if factor_closure != expected_closure:
            _raise(
                "frozen package factor closure differs from the accepted roster",
                REASON_ROSTER_INVALID,
                package_id=package_id,
                expected=expected_closure,
                actual=factor_closure,
            )
        workspace = Path(prepared.workspace_path)
        descriptors = _workspace_descriptors(workspace)
        arms.append(
            FrozenPackageAuditArmV1(
                arm_id=arm_id,
                package_id=package_id,
                package_status=status,
                manifest_sha256=record.manifest_sha256,
                package_snapshot_ref=_evidence_reference(
                    snapshot_path,
                    _wsl_path(snapshot_path),
                    f"n2b_package_snapshot__{arm_id}",
                ),
                factor_count=len(prepared.factor_order),
                factor_closure_sha256=factor_closure,
                model_closure_sha256=_model_closure(manifest),
                workspace_root=_wsl_path(workspace),
                workspace_files=descriptors,
            )
        )
    return tuple(arms)


def _validate_package_record(record: StrategyPackageRecord, *, package_id: str, status: str) -> None:
    manifest = record.current_manifest()
    component = manifest.alpha_components[0] if len(manifest.alpha_components) == 1 else None
    if (
        record.package_id != package_id
        or record.package_status.value != status
        or record.alpha_mode != AlphaMode.SINGLE_ALPHA
        or manifest.alpha_mode != AlphaMode.SINGLE_ALPHA
        or component is None
        or component.score_direction != "higher_better"
        or not record.manifest_sha256
    ):
        _raise(
            "StrategyPackage roster/status/alpha semantics differ from the frozen design",
            REASON_ROSTER_INVALID,
            package_id=package_id,
            actual_status=record.package_status.value,
            actual_alpha_mode=record.alpha_mode.value,
            score_direction=component.score_direction if component is not None else None,
        )


def _self_contain_workspace(prepared: Any, *, path_converter: Any) -> None:
    """Bind factor code into the described workspace instead of external cache paths."""

    workspace = Path(prepared.workspace_path)
    destination = workspace / "frozen_factor_sources"
    destination.mkdir(parents=True, exist_ok=True)
    entry_path = Path(prepared.factor_entry_path)
    entry = entry_path.read_text(encoding="utf-8")
    replacements = 0
    for factor_name in prepared.dynamic_factors:
        source = Path(prepared.factor_source_dir) / f"{factor_name}.py"
        if not source.is_file():
            _raise(
                "frozen factor source is absent while self-containing workspace",
                REASON_ROSTER_INVALID,
                factor_name=factor_name,
            )
        target = destination / source.name
        shutil.copy2(source, target)
        old_variants = {str(source), source.as_posix(), path_converter(str(source))}
        new_value = path_converter(str(target))
        matched = False
        for old in sorted(old_variants, key=len, reverse=True):
            if old and old in entry:
                entry = entry.replace(old, new_value)
                matched = True
        if not matched:
            _raise(
                "factor entry does not reference its package-owned source",
                REASON_ROSTER_INVALID,
                factor_name=factor_name,
            )
        replacements += 1
    entry_path.write_text(entry, encoding="utf-8", newline="\n")
    if replacements != len(prepared.dynamic_factors) or any(
        Path(match.group("path")).is_absolute()
        and "frozen_factor_sources" not in match.group("path").replace("\\", "/")
        for match in _FACTOR_FILE_LITERAL.finditer(entry)
    ):
        _raise("self-contained factor entry still references an external source", REASON_ROSTER_INVALID)


def _factor_closure(manifest: Any, factor_order: Sequence[str]) -> str:
    factors = list(manifest.factor_set)
    if len(factors) != len(factor_order):
        _raise("manifest factor roster differs from prepared order", REASON_ROSTER_INVALID)
    payload = [
        {"name": str(name), "sha256": str(asset.sha256 or "").lower()}
        for name, asset in zip(factor_order, factors)
    ]
    if any(len(item["sha256"]) != 64 for item in payload):
        _raise("manifest factor asset has no frozen sha256", REASON_ROSTER_INVALID)
    return canonical_json_sha256(payload)


def _model_closure(manifest: Any) -> str:
    assets = manifest.model_asset if isinstance(manifest.model_asset, list) else [manifest.model_asset]
    payload: list[dict[str, Any]] = []
    for asset in assets:
        if not asset.sha256:
            _raise("manifest model asset has no frozen sha256", REASON_ROSTER_INVALID)
        payload.append(
            {
                "model_id": asset.model_id,
                "sha256": asset.sha256,
                "model_code": [
                    {
                        "module_name": item.module_name,
                        "relative_path": item.relative_path,
                        "sha256": item.sha256,
                    }
                    for item in asset.model_code_assets
                ],
            }
        )
    return canonical_json_sha256(payload)


def _workspace_descriptors(root: Path) -> tuple[WorkspaceFileDescriptorV1, ...]:
    files = sorted(
        (path for path in root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(root).as_posix(),
    )
    descriptors = tuple(
        WorkspaceFileDescriptorV1(
            relative_path=path.relative_to(root).as_posix(),
            sha256=sha256_file(path),
            size_bytes=path.stat().st_size,
        )
        for path in files
    )
    if not descriptors:
        _raise("prepared package workspace is empty", REASON_ROSTER_INVALID)
    return descriptors


def _load_and_verify_bound_requests(
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
) -> tuple[AdvisoryN1Tier1RequestV1, AdvisoryThreeArmAlphaAuditRequestV1]:
    _verify_evidence_ref(request.n1_request_ref)
    _verify_evidence_ref(request.n1_bundle_manifest_ref)
    _verify_evidence_ref(request.n2a_request_ref)
    _verify_evidence_ref(request.n2a_bundle_manifest_ref)
    try:
        n1 = AdvisoryN1Tier1RequestV1.model_validate_json(
            Path(request.n1_request_ref.artifact_uri).read_text(encoding="utf-8")
        )
        n2a = AdvisoryThreeArmAlphaAuditRequestV1.model_validate_json(
            Path(request.n2a_request_ref.artifact_uri).read_text(encoding="utf-8")
        )
    except Exception as exc:
        _raise(
            "bound N1/N2-A request cannot be read",
            REASON_SOURCE_MISMATCH,
            error_type=type(exc).__name__,
        )
    if (
        n1.request_sha256 != request.n1_request_sha256
        or n2a.request_sha256 != request.n2a_request_sha256
        or n2a.n1_request_sha256 != n1.request_sha256
        or n2a.n1_bundle_id != request.n1_bundle_id
    ):
        _raise("bound N1/N2-A lineage identity changed", REASON_SOURCE_MISMATCH)
    return n1, n2a


def _verify_bound_bundle_identities(
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    n1: AdvisoryN1Tier1RequestV1,
    n2a: AdvisoryThreeArmAlphaAuditRequestV1,
) -> None:
    n1_inspected = inspect_n1_bundle(Path(request.n1_bundle_path))
    n2a_inspected = inspect_three_arm_alpha_audit_bundle(Path(request.n2a_bundle_path))
    if (
        n1_inspected["bundle_id"] != request.n1_bundle_id
        or n2a_inspected["bundle_id"] != request.n2a_bundle_id
        or n1_inspected["request_sha256"] != n1.request_sha256
        or n2a_inspected["request_sha256"] != n2a.request_sha256
    ):
        _raise("authorized N1/N2-A bundle identity changed", REASON_SOURCE_MISMATCH)


def _read_bound_bundle_manifest(bundle_path: Path, *, label: str) -> dict[str, Any]:
    try:
        manifest = json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8"))
    except Exception as exc:
        _raise(
            f"{label} bundle manifest cannot be read",
            REASON_SOURCE_MISMATCH,
            error_type=type(exc).__name__,
        )
    bundle_id = str(manifest.get("bundle_id") or "")
    request_sha256 = str(manifest.get("request_sha256") or "")
    if (
        len(bundle_id) != 64
        or bundle_path.name != bundle_id
        or len(request_sha256) != 64
        or any(value not in "0123456789abcdef" for value in bundle_id + request_sha256)
    ):
        _raise(f"{label} bundle manifest identity is invalid", REASON_SOURCE_MISMATCH)
    return {"bundle_id": bundle_id, "request_sha256": request_sha256}


def _verify_source_contract(
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    n1: AdvisoryN1Tier1RequestV1,
    n2a: AdvisoryThreeArmAlphaAuditRequestV1,
    sources: Mapping[str, Any],
) -> None:
    mismatches: dict[str, Any] = {}
    expected = {
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.current_parent_package_id,
        "manifest_sha256": request.current_parent_manifest_sha256,
        "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
        "baseline_policy_sha256": request.baseline_policy_sha256,
        "shadow_policy_sha256": request.shadow_policy_sha256,
        "cost_policy_sha256": request.cost_policy_sha256,
        "split_policy_sha256": request.split_policy_sha256,
        "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
        "feature_schema_hash": request.feature_schema_hash,
    }
    for name, value in expected.items():
        actual = getattr(n1, name)
        if actual != value:
            mismatches[name] = {"expected": value, "actual": actual}
    if sources["pit_snapshot"].spans_sha256 != request.pit_spans_sha256:
        mismatches["pit_spans_sha256"] = sources["pit_snapshot"].spans_sha256
    decisions = pd.DatetimeIndex(pd.to_datetime(sources["decision_dates"])).normalize()
    if (
        len(decisions) != 386
        or decisions.min().date() != request.decision_date_start
        or decisions.max().date() != request.decision_date_end
    ):
        mismatches["decision_dates"] = {
            "count": len(decisions),
            "start": decisions.min().date().isoformat() if len(decisions) else None,
            "end": decisions.max().date().isoformat() if len(decisions) else None,
        }
    if (
        n2a.package_id != request.current_parent_package_id
        or n2a.manifest_sha256 != request.current_parent_manifest_sha256
        or n2a.pit_spans_sha256 != request.pit_spans_sha256
    ):
        mismatches["n2a_parent_identity"] = True
    if mismatches:
        _raise("N2-B frozen scientific source identity changed", REASON_SOURCE_MISMATCH, mismatches=mismatches)


def _source_identity_receipt(
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    n1: AdvisoryN1Tier1RequestV1,
    n2a: AdvisoryThreeArmAlphaAuditRequestV1,
    sources: Mapping[str, Any],
    batch: PackagePredictionBatchResult,
) -> dict[str, Any]:
    parent_signal = Path(request.n2a_bundle_path) / "full_universe_signal_outcomes.parquet"
    parent_ranking = Path(request.n2a_bundle_path) / "arm_rankings_top50.parquet"
    payload: dict[str, Any] = {
        "schema_version": "advisory_independent_package_alpha_source_identity_receipt_v1",
        "request_sha256": request.request_sha256,
        "n1_request_sha256": n1.request_sha256,
        "n1_bundle_id": request.n1_bundle_id,
        "n2a_request_sha256": n2a.request_sha256,
        "n2a_bundle_id": request.n2a_bundle_id,
        "pit_spans_sha256": sources["pit_snapshot"].spans_sha256,
        "feature_schema_hash": request.feature_schema_hash,
        "parent_signal_sha256": sha256_file(parent_signal),
        "parent_rankings_top50_sha256": sha256_file(parent_ranking),
        "package_manifest_sha256_by_arm": {
            item.arm_id: item.manifest_sha256 for item in request.packages
        },
        "package_factor_closure_sha256_by_arm": {
            item.arm_id: item.factor_closure_sha256 for item in request.packages
        },
        "package_model_closure_sha256_by_arm": {
            item.arm_id: item.model_closure_sha256 for item in request.packages
        },
        "workspace_closure_sha256_by_arm": {
            item.arm_id: canonical_json_sha256(
                [descriptor.model_dump(mode="json") for descriptor in item.workspace_files]
            )
            for item in request.packages
        },
        "prediction_descriptors": {
            arm_id: descriptor.model_dump(mode="json")
            for arm_id, descriptor in sorted(batch.prediction_descriptors.items())
        },
        "prediction_store_run_ids": dict(sorted(batch.prediction_store_run_ids.items())),
        "decision_date_count": len(sources["decision_dates"]),
        "sealed_holdout_accessed": False,
    }
    payload["source_identity_sha256"] = canonical_json_sha256(payload)
    return payload


def _package_inventory_context(request: AdvisoryIndependentPackageAlphaAuditRequestV1) -> dict[str, Any]:
    packages: list[dict[str, Any]] = []
    for arm in request.packages:
        _verify_evidence_ref(arm.package_snapshot_ref)
        snapshot = json.loads(Path(arm.package_snapshot_ref.artifact_uri).read_text(encoding="utf-8"))
        packages.append(
            {
                "arm_id": arm.arm_id,
                "package_id": arm.package_id,
                "package_status": arm.package_status,
                "manifest_sha256": arm.manifest_sha256,
                "factor_count": arm.factor_count,
                "factor_closure_sha256": arm.factor_closure_sha256,
                "model_closure_sha256": arm.model_closure_sha256,
                "native_backtest_summary": snapshot.get("backtest_summary"),
                "native_metric_comparability": "NOT_COMPARABLE_TO_COMMON_N1_AUDIT",
            }
        )
    return {
        "schema_version": "advisory_independent_package_inventory_context_v1",
        "arm_order": list(ARM_IDS),
        "package_order": list(PACKAGE_IDS),
        "packages": packages,
        "sealed_holdout_accessed": False,
    }


def _publish_bundle(
    *,
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    environment: Mapping[str, Any],
    source_receipt: Mapping[str, Any],
    package_inventory: Mapping[str, Any],
    batch: PackagePredictionBatchResult,
    metrics: IndependentPackageAuditMetricResult,
    resource_report: Mapping[str, Any],
) -> Path:
    existing = _find_existing_bundle(request)
    if existing is not None:
        return existing
    root = Path(request.output_root) / "independent_package_alpha_audit_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix=f".tmp_{request.request_id}_", dir=root))
    _write_json(temp / "request.json", request.model_dump(mode="json"))
    _write_json(temp / "source_identity_receipt.json", source_receipt)
    _write_json(temp / "package_inventory_context.json", package_inventory)
    _write_json(temp / "batch_prediction_receipt.json", dict(batch.batch_receipt))
    _write_json(temp / "causality_parity_receipt.json", dict(batch.causality_parity_receipt))
    _write_json(
        temp / "prediction_descriptors.json",
        {
            "schema_version": "advisory_n2b_prediction_descriptors_v1",
            "run_ids": dict(sorted(batch.prediction_store_run_ids.items())),
            "descriptors": {
                arm_id: descriptor.model_dump(mode="json")
                for arm_id, descriptor in sorted(batch.prediction_descriptors.items())
            },
        },
    )
    _write_parquet(temp / "coverage_daily.parquet", metrics.coverage_daily)
    _write_parquet(temp / "arm_signal_outcomes.parquet", metrics.arm_signal_outcomes)
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
    report["temp_peak_bytes"] = max(
        int(report.get("temp_peak_bytes") or 0),
        _tree_physical_size(temp),
    )
    if (
        report["peak_rss_bytes"] > request.resource_max_rss_bytes
        or int(report.get("temp_peak_bytes") or 0) > request.resource_max_temp_bytes
    ):
        _raise("N2-B exceeded a frozen resource limit while publishing", REASON_RESOURCE_LIMIT)
    _write_json(temp / "resource_report.json", report)
    result_descriptors = _file_descriptors(temp)
    result_files_sha256 = canonical_json_sha256(
        {
            name: descriptor
            for name, descriptor in result_descriptors.items()
            if name not in _RESULT_IDENTITY_EXCLUDED_FILES
        }
    )
    receipt = build_independent_package_alpha_audit_receipt(
        request_sha256=request.request_sha256,
        source_identity_sha256=str(source_receipt["source_identity_sha256"]),
        prediction_identity_sha256=str(batch.batch_receipt["prediction_identity_sha256"]),
        causality_parity_sha256=str(batch.batch_receipt["causality_parity_sha256"]),
        result_files_sha256=result_files_sha256,
        arm_ids=ARM_IDS,
        decision_date_count=int(metrics.coverage_daily["decision_as_of_trade_date"].nunique()),
        signal_row_count_by_arm={
            arm_id: int(metrics.arm_signal_outcomes["arm_id"].eq(arm_id).sum()) for arm_id in ARM_IDS
        },
        evaluable_recall_day_count_by_arm={
            arm_id: int(
                metrics.recall_daily.loc[
                    metrics.recall_daily["arm_id"].eq(arm_id), "status"
                ].eq("AVAILABLE").sum()
            )
            for arm_id in ARM_IDS
        },
        evaluable_top5_day_count_by_arm={
            arm_id: int(
                metrics.top5_daily.loc[
                    metrics.top5_daily["arm_id"].eq(arm_id), "status"
                ].eq("AVAILABLE").sum()
            )
            for arm_id in ARM_IDS
        },
        created_at=request.created_at,
    )
    _write_json(temp / "audit_receipt.json", receipt.model_dump(mode="json"))
    semantic_identity = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "request_sha256": request.request_sha256,
        "receipt_sha256": receipt.receipt_sha256,
    }
    bundle_id = canonical_json_sha256(semantic_identity)
    final = root / bundle_id
    receipt_ref = EvidenceReferenceV1(
        role="n2b_independent_package_alpha_audit_receipt",
        artifact_uri=(final / "audit_receipt.json").as_posix(),
        sha256=sha256_file(temp / "audit_receipt.json"),
        size_bytes=(temp / "audit_receipt.json").stat().st_size,
    )
    record = build_trial_record(
        experiment_id=EXPERIMENT_ID,
        attempt_id=request.request_id,
        research_stage="N2B_INDEPENDENT_PACKAGE_ALPHA_DIAGNOSTIC",
        study_type=ResearchStudyType.ORACLE_DIAGNOSTIC,
        hypothesis_family_id="N2B_FROZEN_INDEPENDENT_STRATEGY_PACKAGE_SIGNAL_AUDIT_V1",
        parent_lineage=PARENT_LINEAGE,
        unique_variable="THREE_FROZEN_NON_RETIRED_INDEPENDENT_SINGLE_ALPHA_PACKAGES_V1",
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
    manifest = {
        **semantic_identity,
        "bundle_id": bundle_id,
        "objective_contract": ObjectiveContract.ALPHA_RANKING.value,
        "study_type": ResearchStudyType.ORACLE_DIAGNOSTIC.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "dataset_identity": request.dataset_identity,
        "policy_identity": record.policy_identity,
        "arm_ids": list(ARM_IDS),
        "package_ids": list(PACKAGE_IDS),
        "planned_trial_count": 0,
        "generated_trial_count": 0,
        "evaluated_trial_count": 0,
        "selected_trial_count": 0,
        "sealed_holdout_accessed": False,
        "runtime_eligible": False,
        "activated": False,
        "files": _file_descriptors(temp),
    }
    _write_json(temp / "manifest.json", manifest)
    _validate_bundle_files(temp, manifest)
    try:
        temp.replace(final)
    except FileExistsError:
        _raise("N2-B bundle appeared concurrently", REASON_BUNDLE_CONFLICT, bundle_id=bundle_id)
    _read_bundle(final)
    return final


def _read_bundle(path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
        request = AdvisoryIndependentPackageAlphaAuditRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = AdvisoryIndependentPackageAlphaAuditReceiptV1.model_validate_json(
            (path / "audit_receipt.json").read_text(encoding="utf-8")
        )
        source_receipt = json.loads((path / "source_identity_receipt.json").read_text(encoding="utf-8"))
        batch_receipt = json.loads((path / "batch_prediction_receipt.json").read_text(encoding="utf-8"))
        causality_receipt = json.loads((path / "causality_parity_receipt.json").read_text(encoding="utf-8"))
        resource_report = json.loads((path / "resource_report.json").read_text(encoding="utf-8"))
        raw_record = json.loads((path / "registry_record.json").read_text(encoding="utf-8"))
        record = build_trial_record(
            **{
                key: value
                for key, value in raw_record.items()
                if key not in {"registry_entry_id", "record_sha256"}
            }
        )
    except Exception as exc:
        _raise(
            "N2-B immutable bundle cannot be read",
            REASON_BUNDLE_CONFLICT,
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
    source_functional = {
        key: value for key, value in source_receipt.items() if key != "source_identity_sha256"
    }
    result_descriptors = {
        name: descriptor
        for name, descriptor in manifest.get("files", {}).items()
        if name not in _RESULT_IDENTITY_EXCLUDED_FILES
    }
    receipt_descriptor = manifest.get("files", {}).get("audit_receipt.json", {})
    invalid = (
        manifest.get("schema_version") != BUNDLE_SCHEMA_VERSION
        or manifest.get("bundle_id") != path.name
        or expected_id != path.name
        or request.request_sha256 != manifest.get("request_sha256")
        or receipt.receipt_sha256 != manifest.get("receipt_sha256")
        or source_receipt.get("source_identity_sha256") != canonical_json_sha256(source_functional)
        or receipt.source_identity_sha256 != source_receipt.get("source_identity_sha256")
        or receipt.prediction_identity_sha256 != batch_receipt.get("prediction_identity_sha256")
        or receipt.causality_parity_sha256 != batch_receipt.get("causality_parity_sha256")
        or batch_receipt.get("causality_parity_sha256") != causality_receipt.get("receipt_sha256")
        or not _valid_batch_execution_receipt(batch_receipt, request=request)
        or receipt.result_files_sha256 != canonical_json_sha256(result_descriptors)
        or record.registry_entry_id != raw_record.get("registry_entry_id")
        or record.record_sha256 != raw_record.get("record_sha256")
        or len(record.evidence_refs) != 1
        or record.evidence_refs[0].role != "n2b_independent_package_alpha_audit_receipt"
        or Path(record.evidence_refs[0].artifact_uri.replace("\\", "/")).name != "audit_receipt.json"
        or record.evidence_refs[0].sha256 != receipt_descriptor.get("sha256")
        or record.evidence_refs[0].size_bytes != receipt_descriptor.get("size_bytes")
        or tuple(manifest.get("arm_ids", ())) != ARM_IDS
        or tuple(manifest.get("package_ids", ())) != PACKAGE_IDS
        or any(
            int(manifest.get(name, -1)) != 0
            for name in (
                "planned_trial_count",
                "generated_trial_count",
                "evaluated_trial_count",
                "selected_trial_count",
            )
        )
        or manifest.get("sealed_holdout_accessed") is not False
        or manifest.get("runtime_eligible") is not False
        or manifest.get("activated") is not False
        or resource_report.get("wall_limit_enabled") is not False
        or resource_report.get("wall_limit_seconds") is not None
        or int(resource_report.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
        or int(resource_report.get("temp_peak_bytes") or 0) > request.resource_max_temp_bytes
    )
    if invalid:
        _raise("N2-B bundle relational identity is invalid", REASON_BUNDLE_CONFLICT, path=str(path))
    _validate_bundle_files(path, manifest)
    return {
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "record": record,
        "source_receipt": source_receipt,
        "batch_receipt": batch_receipt,
        "causality_receipt": causality_receipt,
        "resource_report": resource_report,
    }


def _valid_batch_execution_receipt(
    receipt: Mapping[str, Any],
    *,
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
) -> bool:
    windows = receipt.get("required_window_by_closure")
    model_loads = receipt.get("model_load_count_by_arm")
    return bool(
        receipt.get("market_interval_read_count") == 1
        and receipt.get("static_interval_read_count") == 1
        and receipt.get("rolling_live_window_semantics") is True
        and receipt.get("window_buffer_trading_days") == 5
        and isinstance(windows, dict)
        and set(windows) == set(request.factor_group_closures)
        and all(isinstance(value, int) and not isinstance(value, bool) and value > 0 for value in windows.values())
        and receipt.get("factor_io_mode") == FACTOR_IO_MODE_IN_MEMORY
        and receipt.get("factor_input_copy_mode") == FACTOR_INPUT_COPY_MODE_COW
        and receipt.get("factor_result_projection_mode")
        == FACTOR_RESULT_PROJECTION_MODE_DECISION_DATES
        and receipt.get("wall_limit_enabled") is False
        and receipt.get("wall_limit_seconds") is None
        and receipt.get("temp_storage_mode") == "ENVIRONMENT_LOCAL_EPHEMERAL"
        and receipt.get("static_h5_physical_file_count") == 1
        and receipt.get("static_h5_hardlink_alias_count") == 6
        and receipt.get("primary_decision_batch_count") == 386
        and receipt.get("primary_factor_group_run_count_per_decision") == 2
        and receipt.get("primary_factor_group_run_count") == 772
        and receipt.get("diagnostic_factor_group_run_count") == 6
        and receipt.get("factor_group_total_run_count") == 778
        and receipt.get("file_backed_parity_factor_group_run_count") == 2
        and receipt.get("all_factor_group_run_count") == 780
        and receipt.get("factor_calculation_count") == 30731
        and receipt.get("factor_reuse_count") == 10892
        and receipt.get("result_write_count") == 30731
        and receipt.get("projected_result_write_count") == 30731
        and receipt.get("fallback_result_write_count") == 0
        and receipt.get("reference_factor_calculation_count") == 107
        and isinstance(receipt.get("file_backed_parity_receipts"), list)
        and len(receipt["file_backed_parity_receipts"]) == 2
        and all(
            isinstance(item, dict)
            and item.get("status") == "PASS"
            and item.get("in_memory_feature_sha256") == item.get("file_backed_feature_sha256")
            and isinstance(item.get("in_memory_feature_sha256"), str)
            and len(item["in_memory_feature_sha256"]) == 64
            for item in receipt["file_backed_parity_receipts"]
        )
        and {
            item.get("closure_sha256") for item in receipt["file_backed_parity_receipts"]
        }
        == set(request.factor_group_closures)
        and receipt.get("daily_wsl_process_count") == 0
        and receipt.get("daily_db_query_count") == 0
        and isinstance(model_loads, dict)
        and set(model_loads) == set(PACKAGE_ARM_IDS)
        and all(model_loads[arm_id] == 1 for arm_id in PACKAGE_ARM_IDS)
    )


def _find_existing_bundle(request: AdvisoryIndependentPackageAlphaAuditRequestV1) -> Path | None:
    root = Path(request.output_root) / "independent_package_alpha_audit_bundles"
    if not root.is_dir():
        return None
    matches: list[Path] = []
    for path in sorted(root.iterdir()):
        manifest_path = path / "manifest.json"
        if path.name.startswith(".tmp_") or not path.is_dir() or not manifest_path.is_file():
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            _raise(
                "N2-B bundle manifest is unreadable",
                REASON_BUNDLE_CONFLICT,
                path=str(manifest_path),
                error_type=type(exc).__name__,
            )
        if manifest.get("request_sha256") == request.request_sha256:
            matches.append(path)
    if len(matches) > 1:
        _raise(
            "one N2-B request resolves to multiple immutable bundles",
            REASON_BUNDLE_CONFLICT,
            bundle_paths=[str(path) for path in matches],
        )
    if not matches:
        return None
    _read_bundle(matches[0])
    return matches[0]


def _deliver_bundle(
    *,
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    bundle_path: Path,
    n1: AdvisoryN1Tier1RequestV1,
) -> dict[str, Any]:
    loaded = _read_bundle(bundle_path)
    if loaded["request"].request_sha256 != request.request_sha256:
        _raise("N2-B bundle belongs to another request", REASON_BUNDLE_CONFLICT)
    registry = AdvisoryResearchTrialRegistryV1(request.registry_path).append_batch((loaded["record"],))
    route = generate_current_route(
        registry_path=request.registry_path,
        parent_spike_path=Path(n1.n0_completion_ref.artifact_uri).parent
        / "parent_prediction_extension_receipt.json",
        window_contract_path=n1.research_window_contract_path,
        output_path=n1.route_path,
    )
    if route["next_task"] != "N2_ENTRY_EXIT_QE_PREPARATION":
        _raise(
            "N2-B delivery changed the frozen N2 route",
            "ADVISORY_RESEARCH_ROUTE_INCONSISTENT",
            next_task=route["next_task"],
        )
    return {"registry": registry, "route": route, "next_task": route["next_task"]}


def _verify_wsl_environment(
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
    *,
    require_repository_identity: bool,
) -> dict[str, Any]:
    if os.name == "nt" or "microsoft" not in platform.release().lower():
        _raise("formal N2-B audit must run inside WSL", "ADVISORY_MODEL_TRAINING_REQUIRES_WSL")
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        _raise(
            "formal N2-B audit requires the rdagent-gpu environment",
            "ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            conda_env=conda_env or None,
        )
    actual_commit = _git_commit(Path(request.repository_root))
    if require_repository_identity and actual_commit != request.repository_commit:
        _raise(
            "N2-B repository commit differs from its frozen request",
            REASON_REQUEST_INVALID,
            expected=request.repository_commit,
            actual=actual_commit,
        )
    if require_repository_identity:
        dirty = _git_dirty_paths(Path(request.repository_root))
        if dirty:
            _raise("formal N2-B worktree is dirty", REASON_REQUEST_INVALID, dirty_paths=dirty[:50])
    return {
        "platform_release": platform.release(),
        "conda_env": conda_env,
        "repository_commit": actual_commit,
        "requested_repository_commit": request.repository_commit,
        "repository_identity_check": (
            "MATCHED_FOR_COMPUTE"
            if require_repository_identity
            else "NOT_REQUIRED_FOR_IMMUTABLE_DELIVERY_ONLY_RESUME"
        ),
        "python": platform.python_version(),
        "pandas": importlib.metadata.version("pandas"),
        "numpy": importlib.metadata.version("numpy"),
    }


def _run_response(
    status: str,
    request: AdvisoryIndependentPackageAlphaAuditRequestV1,
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
        "package_ids": list(PACKAGE_IDS),
        "sealed_holdout_accessed": False,
        "environment": dict(environment),
        "delivery": dict(delivery),
        "backend_restart": "noop",
        "production_ddl_gate": "noop",
        "production_dml_gate": "noop",
        "runtime_activation": "noop",
    }


def _verify_evidence_ref(reference: EvidenceReferenceV1) -> None:
    path = Path(reference.artifact_uri)
    if (
        not path.is_file()
        or sha256_file(path) != reference.sha256
        or path.stat().st_size != reference.size_bytes
    ):
        _raise(
            "bound N2-B evidence identity changed",
            REASON_SOURCE_MISMATCH,
            role=reference.role,
            path=str(path),
        )


def _evidence_reference(path: Path, declared_uri: str, role: str) -> EvidenceReferenceV1:
    if not path.is_file():
        _raise("N2-B evidence file is missing", REASON_REQUEST_INVALID, role=role, path=str(path))
    return EvidenceReferenceV1(
        role=role,
        artifact_uri=declared_uri,
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
    )


def _write_immutable_request(path: Path, request: AdvisoryIndependentPackageAlphaAuditRequestV1) -> None:
    _write_immutable_json(path, request.model_dump(mode="json"), REASON_REQUEST_INVALID)
    existing = AdvisoryIndependentPackageAlphaAuditRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
    if existing.request_sha256 != request.request_sha256:
        _raise("immutable N2-B request readback differs", REASON_REQUEST_INVALID, path=str(path))


def _write_immutable_json(path: Path, payload: Mapping[str, Any], reason_code: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n"
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise(
                "immutable JSON artifact cannot be read",
                reason_code,
                path=str(path),
                error_type=type(exc).__name__,
            )
        if canonical_json_sha256(existing) != canonical_json_sha256(payload):
            _raise("immutable JSON artifact conflicts", reason_code, path=str(path))
        return
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())


def _peak_rss_bytes() -> int:
    if _resource is None:
        return 0
    value = int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
    return value if platform.system() == "Darwin" else value * 1024


def _tree_physical_size(root: Path) -> int:
    seen: set[tuple[int, int]] = set()
    total = 0
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        stat = path.stat()
        identity = (int(stat.st_dev), int(stat.st_ino))
        if identity in seen:
            continue
        seen.add(identity)
        total += int(stat.st_size)
    return total


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)


__all__ = [
    "IndependentPackageAuditMetricResult",
    "build_independent_arm_summary",
    "build_independent_package_metrics",
    "build_independent_pairwise_summary",
    "build_own_universe_signal_metrics_daily",
    "inspect_independent_package_alpha_audit_bundle",
    "prepare_independent_package_alpha_audit_request",
    "run_independent_package_alpha_audit",
]
