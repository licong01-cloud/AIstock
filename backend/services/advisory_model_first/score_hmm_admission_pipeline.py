from __future__ import annotations

import gc
import json
import math
import os
import re
import shutil
import subprocess
import tempfile
import time
import warnings
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, NoReturn, Sequence

import numpy as np
import pandas as pd
from pydantic import ValidationError
from scipy.special import logsumexp
from sklearn.exceptions import ConvergenceWarning
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import _git_command_for_worktree
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.outcome_calibration import expected_calibration_error
from backend.services.advisory_model_first.outcome_labels import build_multi_horizon_outcome_labels
from backend.services.advisory_model_first.policy_contracts import (
    FrozenAdvisoryPolicyDatasetRequestV1,
    transition_policy_from_payload,
)
from backend.services.advisory_model_first.policy_dataset_bundle import load_policy_dataset_bundle
from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels
from backend.services.advisory_model_first.policy_rank_source import build_policy_rankings
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource, sha256_file
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _cross_os_git_commit,
    _cross_os_git_dirty_paths,
    _deflated_sharpe_diagnostic,
    _file_descriptors,
    _moving_block_interval,
    _parquet_row_count,
    _peak_rss_bytes,
    _safe_correlation,
)
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_suspend_rows,
    load_trading_calendar,
    validate_factor_file_schemas,
)
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    AdvisoryAdmissionDecisionV1,
    MARKET_HMM_FEATURE_COLUMNS,
    PACKAGE_SCORE_CALIBRATION_ONLY,
    RAW_MARKET_FEATURE_COLUMNS,
    SCORE_FEATURE_COLUMNS,
    SCORE_HMM_ARM_IDS,
    SCORE_HMM_ARM_SCHEMA_HASHES,
    SCORE_HMM_DATA_CUTOFF,
    SCORE_HMM_EVIDENCE_ROLES,
    SCORE_HMM_EXECUTABLE_ARM_IDS,
    SCORE_HMM_EXPERIMENT_ID,
    SCORE_HMM_FEATURES_BY_ARM,
    SCORE_HMM_HYPOTHESIS_FAMILY_ID,
    SCORE_HMM_MARKET_HISTORY_START,
    SCORE_HMM_RESEARCH_STAGE,
    SCORE_HMM_SECONDARY_HORIZONS,
    SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS,
    SCORE_PLUS_MARKET_AND_SECTOR_HMM,
    SCORE_PLUS_MARKET_HMM,
    SCORE_PLUS_RAW_MARKET_SHAPE,
    SCORE_PLUS_SECTOR_HMM,
    FrozenAdvisoryScoreHMMAdmissionRequestV1,
    ScoreHMMAdmissionFrontierReceiptV1,
    build_score_hmm_frontier_receipt,
    build_score_hmm_request,
)
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.services.advisory_model_first.tier1_oracle_contracts import AdvisoryN1Tier1RequestV1
from backend.services.advisory_model_first.tier1_oracle_pipeline import (
    _read_n1_bundle,
    filter_prediction_frame_to_pit,
)
from backend.services.dataset_release.pit import (
    FrozenPitSnapshot,
    canonicalize_pit_spans,
    filter_frame_to_pit_spans,
    freeze_pit_snapshot,
    frozen_pit_snapshot_from_mapping,
    write_frozen_pit_snapshot,
)
from backend.services.canonical_equity_pit import (
    CANONICAL_PIT_RULE_VERSION,
    CANONICAL_PIT_SCOPE,
    CANONICAL_PIT_UNIVERSE_KEY,
    canonical_rule_parameters_digest,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


SCORE_HMM_BUNDLE_SCHEMA = "advisory_score_hmm_admission_bundle_v1"
BASELINE_ARM_ID = "BASELINE_ALL_TAKE"
TARGET_HEADS = ("PRIMARY", "H1", "H5", "H10", "H20")
TARGET_VALUE_COLUMN = {
    "PRIMARY": "primary_net_return_bps",
    "H1": "h1_net_return_bps",
    "H5": "h5_net_return_bps",
    "H10": "h10_net_return_bps",
    "H20": "h20_net_return_bps",
}
TARGET_KNOWN_COLUMN = {
    "PRIMARY": "primary_target_known",
    "H1": "h1_target_known",
    "H5": "h5_target_known",
    "H10": "h10_target_known",
    "H20": "h20_target_known",
}
RESULT_IDENTITY_MEMBERS = frozenset(
    {
        "source_preflight.json",
        "parent_context_exposure.json",
        "feature_schema_by_arm.json",
        "aligned_parent_rankings_top50.parquet",
        "primary_policy_labels.parquet",
        "target_coverage.parquet",
        "hmm_fold_receipts.json",
        "oof_predictions.parquet",
        "calibration_metrics.parquet",
        "admission_decisions.parquet",
        "policy_daily.parquet",
        "policy_episodes.parquet",
        "arm_summary.json",
        "frontier_receipt.json",
    }
)
SCORE_HMM_BUNDLE_MEMBERS = RESULT_IDENTITY_MEMBERS | {
    "request.json",
    "resource_report.json",
    "registry_records.json",
}


@dataclass(frozen=True)
class RawMarketShapeResult:
    features: pd.DataFrame
    coverage: pd.DataFrame
    pit_filter_receipt: dict[str, Any]


@dataclass(frozen=True)
class FoldLocalMarketHMMResult:
    states: pd.DataFrame
    receipts: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ScoreHMMTargetResult:
    panel: pd.DataFrame
    coverage: pd.DataFrame


@dataclass(frozen=True)
class ScoreHMMCrossfitResult:
    oof_predictions: pd.DataFrame
    fold_receipts: dict[str, Any]


@dataclass(frozen=True)
class ScoreHMMEvaluationResult:
    policy_daily: pd.DataFrame
    policy_episodes: pd.DataFrame
    arm_summary: dict[str, Any]
    selected_arm_id: str | None
    eligible_arm_ids: tuple[str, ...]
    evidence_class: str


def build_package_score_features(
    rankings: pd.DataFrame,
    *,
    expected_row_count: int | None = 7_720,
) -> pd.DataFrame:
    """Build the frozen same-day, affine-invariant package-score schema."""

    required = {
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "combined_score",
        "selection_effective_rank",
        f"norm__{LSTM_LEG_ID}",
        f"norm__{FUND_LEG_ID}",
        f"rank__{LSTM_LEG_ID}",
        f"rank__{FUND_LEG_ID}",
    }
    missing = sorted(required - set(rankings.columns))
    if missing:
        _raise("parent Top50 score panel is incomplete", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH", missing=missing)
    rows = rankings.copy()
    rows["decision_as_of_trade_date"] = pd.to_datetime(rows["decision_as_of_trade_date"]).dt.normalize()
    rows["target_trade_date"] = pd.to_datetime(rows["target_trade_date"]).dt.normalize()
    rows["instrument"] = rows["instrument"].astype(str).str.upper()
    numeric = [
        "combined_score",
        "selection_effective_rank",
        f"norm__{LSTM_LEG_ID}",
        f"norm__{FUND_LEG_ID}",
        f"rank__{LSTM_LEG_ID}",
        f"rank__{FUND_LEG_ID}",
    ]
    rows[numeric] = rows[numeric].apply(pd.to_numeric, errors="coerce")
    if rows.duplicated(["decision_as_of_trade_date", "instrument"]).any() or rows[numeric].isna().any().any():
        _raise("parent Top50 score keys or values are invalid", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    output: list[pd.DataFrame] = []
    for decision, raw_group in rows.groupby("decision_as_of_trade_date", sort=True):
        group = raw_group.sort_values(["combined_score", "instrument"], ascending=[False, True]).reset_index(drop=True)
        expected_rank = np.arange(1, len(group) + 1)
        if len(group) != 50 or not np.array_equal(group["selection_effective_rank"].to_numpy(int), expected_rank):
            _raise(
                "parent score panel is not an exact Top50",
                "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
                decision_date=decision.date().isoformat(),
                row_count=len(group),
            )
        if group["target_trade_date"].nunique() != 1:
            _raise("parent decision maps to multiple target dates", "ADVISORY_SCORE_HMM_CLOCK_MISMATCH")
        parent = group["combined_score"].to_numpy(float)
        lstm = group[f"norm__{LSTM_LEG_ID}"].to_numpy(float)
        fund = group[f"norm__{FUND_LEG_ID}"].to_numpy(float)
        parent_iqr = _iqr(parent)
        lstm_iqr = _iqr(lstm)
        fund_iqr = _iqr(fund)
        if min(parent_iqr, lstm_iqr, fund_iqr) <= 1e-12:
            _raise(
                "same-day score distribution has zero or non-finite IQR",
                "ADVISORY_SCORE_HMM_SCORE_TRANSFORM_INVALID",
                decision_date=decision.date().isoformat(),
            )
        parent_median = float(np.median(parent))
        lstm_robust = (lstm - float(np.median(lstm))) / lstm_iqr
        fund_robust = (fund - float(np.median(fund))) / fund_iqr
        group["parent_rank_pct_top20"] = 1.0 - (group["selection_effective_rank"] - 1.0) / 19.0
        group["parent_score_percentile_top50"] = 1.0 - np.arange(len(group), dtype=float) / 49.0
        group["parent_score_robust_z_top50"] = (parent - parent_median) / parent_iqr
        group["parent_score_gap_to_rank6_iqr"] = (parent - parent[5]) / parent_iqr
        group["lstm_rank_pct_top50"] = _within_group_percentile(lstm, group["instrument"])
        group["fund_rank_pct_top50"] = _within_group_percentile(fund, group["instrument"])
        group["leg_rank_gap_pct"] = group["lstm_rank_pct_top50"] - group["fund_rank_pct_top50"]
        group["leg_norm_gap_abs_robust"] = np.abs(lstm_robust - fund_robust)
        group["day_top5_vs_rank6_gap_iqr"] = (float(parent[:5].mean()) - parent[5]) / parent_iqr
        group["day_top20_iqr_over_top50_iqr"] = _iqr(parent[:20]) / parent_iqr
        group["day_top20_score_range_over_iqr"] = (float(parent[:20].max()) - float(parent[:20].min())) / parent_iqr
        group["day_top5_minus_top20_mean_over_iqr"] = (
            float(parent[:5].mean()) - float(parent[:20].mean())
        ) / parent_iqr
        output.append(group.iloc[:20])
    result = pd.concat(output, ignore_index=True)
    keep = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
        *SCORE_FEATURE_COLUMNS,
    ]
    result = result[keep].sort_values(["decision_as_of_trade_date", "selection_effective_rank"]).reset_index(drop=True)
    values = result[list(SCORE_FEATURE_COLUMNS)].to_numpy(float)
    if (expected_row_count is not None and len(result) != expected_row_count) or not np.isfinite(values).all():
        _raise(
            "score transform did not preserve the exact Top20 panel",
            "ADVISORY_SCORE_HMM_SCORE_TRANSFORM_INVALID",
            row_count=len(result),
        )
    return result


def build_raw_market_shape(
    *,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    pit_snapshot: FrozenPitSnapshot | pd.DataFrame,
    calendar: Sequence[pd.Timestamp],
) -> RawMarketShapeResult:
    """Build T-visible market shape on the canonical PIT universe."""

    required = {"close", "prev_close", "volume", "amount", "limit_up"}
    missing = sorted(required - set(market_daily.columns))
    if missing:
        _raise("raw market source is incomplete", "ADVISORY_SCORE_HMM_RAW_MARKET_INVALID", missing=missing)
    market = market_daily.copy().sort_index()
    if not isinstance(market.index, pd.MultiIndex) or list(market.index.names) != ["datetime", "instrument"]:
        _raise("raw market index is invalid", "ADVISORY_SCORE_HMM_RAW_MARKET_INVALID")
    market, pit_receipt = filter_frame_to_pit_spans(market, pit_snapshot)
    suspended = {
        (pd.Timestamp(row.trade_date).normalize(), str(row.instrument).upper())
        for row in suspend_rows.itertuples(index=False)
    }
    dates = pd.DatetimeIndex(market.index.get_level_values("datetime")).normalize()
    symbols = market.index.get_level_values("instrument").astype(str).str.upper()
    close = pd.to_numeric(market["close"], errors="coerce")
    previous = pd.to_numeric(market["prev_close"], errors="coerce")
    volume = pd.to_numeric(market["volume"], errors="coerce")
    amount = pd.to_numeric(market["amount"], errors="coerce")
    returns = close / previous - 1.0
    valid = (
        np.isfinite(close)
        & np.isfinite(previous)
        & close.gt(0)
        & previous.gt(0)
        & volume.gt(0)
        & amount.gt(0)
        & np.asarray([(value, symbol) not in suspended for value, symbol in zip(dates, symbols, strict=True)])
    )
    valid_returns = returns.where(valid)
    group_dates = pd.Series(dates, index=market.index)
    counts = valid_returns.groupby(group_dates).count()
    up = valid_returns.gt(0).where(valid_returns.notna()).groupby(group_dates).mean()
    limit_up = pd.to_numeric(market["limit_up"], errors="coerce").where(valid_returns.notna()).groupby(group_dates).mean()
    volatility = valid_returns.groupby(group_dates).std(ddof=1)
    benchmark = _benchmark_frame(benchmark_daily)
    benchmark_close = benchmark["close"]
    features = pd.DataFrame(index=benchmark.index)
    for horizon in (1, 5, 20):
        features[f"csi300_ret_{horizon}"] = benchmark_close.pct_change(horizon, fill_method=None)
    for horizon in (20, 60):
        rolling_high = benchmark_close.rolling(horizon, min_periods=horizon).max()
        features[f"csi300_drawdown_{horizon}"] = benchmark_close / rolling_high - 1.0
    features["market_up_ratio"] = up
    features["market_limit_up_ratio"] = limit_up
    features["market_cross_section_vol"] = volatility
    feature_calendar = pd.DatetimeIndex(pd.to_datetime(list(calendar))).normalize().sort_values().unique()
    features = features.reindex(feature_calendar)
    counts = counts.reindex(feature_calendar).fillna(0).astype(int)
    complete = features[list(RAW_MARKET_FEATURE_COLUMNS)].notna().all(axis=1)
    available = complete & counts.ge(100)
    coverage = pd.DataFrame(
        {
            "decision_as_of_trade_date": feature_calendar,
            "valid_market_member_count": counts.to_numpy(),
            "observation_completeness": features[list(RAW_MARKET_FEATURE_COLUMNS)].notna().mean(axis=1).to_numpy(),
            "status": np.where(available, "AVAILABLE", "SOURCE_UNAVAILABLE"),
        }
    )
    features.index.name = "decision_as_of_trade_date"
    features["raw_market_status"] = np.where(available, "AVAILABLE", "SOURCE_UNAVAILABLE")
    return RawMarketShapeResult(features=features.reset_index(), coverage=coverage, pit_filter_receipt=pit_receipt)


def build_fold_local_market_hmm(
    *,
    raw_market_features: pd.DataFrame,
    cpcv_payload: Mapping[str, Any],
    trading_calendar: Sequence[pd.Timestamp],
    expected_path_count: int = 28,
    warmup_days: int = 60,
) -> FoldLocalMarketHMMResult:
    """Fit one K=2 market HMM per outer fold and causally filter every block."""

    required = {"decision_as_of_trade_date", "raw_market_status", *RAW_MARKET_FEATURE_COLUMNS}
    missing = sorted(required - set(raw_market_features.columns))
    if missing:
        _raise("market HMM input schema is incomplete", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", missing=missing)
    raw = raw_market_features.copy()
    raw["decision_as_of_trade_date"] = pd.to_datetime(raw["decision_as_of_trade_date"]).dt.normalize()
    if raw["decision_as_of_trade_date"].duplicated().any():
        _raise("market HMM input contains duplicate dates", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID")
    raw = raw.set_index("decision_as_of_trade_date").sort_index()
    ready_paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if len(ready_paths) != expected_path_count:
        _raise(
            "market HMM did not receive the exact READY CPCV paths",
            "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
            ready_path_count=len(ready_paths),
        )
    calendar = pd.DatetimeIndex(pd.to_datetime(list(trading_calendar))).normalize().sort_values().unique()
    states: list[pd.DataFrame] = []
    receipts: list[dict[str, Any]] = []
    for path in ready_paths:
        path_states, receipt = _fit_market_hmm_path(
            raw=raw,
            path=path,
            calendar=calendar,
            warmup_days=warmup_days,
        )
        states.append(path_states)
        receipts.append(receipt)
    output = pd.concat(states, ignore_index=True) if states else pd.DataFrame()
    if not output.empty:
        output = output.sort_values(["path_id", "phase", "decision_as_of_trade_date"]).reset_index(drop=True)
    return FoldLocalMarketHMMResult(states=output, receipts=tuple(receipts))


def _fit_market_hmm_path(
    *,
    raw: pd.DataFrame,
    path: Mapping[str, Any],
    calendar: pd.DatetimeIndex,
    warmup_days: int,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    path_id = str(path.get("path_id") or "")
    train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize().sort_values().unique()
    validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize().sort_values().unique()
    if not path_id or len(train_dates) == 0 or len(validation_dates) == 0 or set(train_dates) & set(validation_dates):
        _raise("market HMM path identity is invalid", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", path_id=path_id)
    train_observations = raw.reindex(train_dates)[list(RAW_MARKET_FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce")
    medians = train_observations.median(axis=0)
    if medians.isna().any():
        _raise("market HMM train median is unavailable", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", path_id=path_id)
    train_filled = train_observations.fillna(medians)
    means = train_filled.mean(axis=0)
    std = train_filled.std(axis=0, ddof=0)
    if (std <= 0).any() or not np.isfinite(std.to_numpy(float)).all():
        _raise("market HMM train scaler is degenerate", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", path_id=path_id)
    train_matrix = ((train_filled - means) / std).to_numpy(float)
    train_block_lengths = [len(block) for block in _contiguous_blocks(train_dates, calendar)]
    try:
        from hmmlearn.hmm import GaussianHMM

        model = GaussianHMM(
            n_components=2,
            covariance_type="full",
            n_iter=200,
            tol=1e-4,
            random_state=42,
            min_covar=1e-5,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            model.fit(train_matrix, lengths=train_block_lengths)
    except Exception as exc:
        _raise(
            "market HMM fit failed",
            "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID",
            path_id=path_id,
            error_type=type(exc).__name__,
        )
    final_likelihood_delta = _validated_hmm_final_delta(model, path_id=path_id)
    semantic = model.means_[:, RAW_MARKET_FEATURE_COLUMNS.index("csi300_ret_20")] + model.means_[
        :, RAW_MARKET_FEATURE_COLUMNS.index("market_up_ratio")
    ]
    if not np.isfinite(semantic).all() or abs(float(semantic[0] - semantic[1])) <= 1e-12:
        _raise("market HMM state semantics are ambiguous", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", path_id=path_id)
    risk_on_raw_state = int(np.argmax(semantic))
    parameter_payload = {
        "observation_order": list(RAW_MARKET_FEATURE_COLUMNS),
        "imputer_median": medians.tolist(),
        "scaler_mean": means.tolist(),
        "scaler_std": std.tolist(),
        "startprob": model.startprob_.tolist(),
        "transmat": model.transmat_.tolist(),
        "means": model.means_.tolist(),
        "covariances": model.covars_.tolist(),
        "risk_on_raw_state": risk_on_raw_state,
    }
    model_identity = canonical_json_sha256(parameter_payload)
    path_outputs: list[pd.DataFrame] = []
    block_receipts: list[dict[str, Any]] = []
    for phase, dates in (("TRAIN", train_dates), ("VALIDATION", validation_dates)):
        for block_index, block in enumerate(_contiguous_blocks(dates, calendar)):
            start_position = int(calendar.get_indexer([block[0]])[0])
            if start_position < warmup_days:
                block_receipts.append(
                    {
                        "phase": phase,
                        "block_index": block_index,
                        "status": "SOURCE_UNAVAILABLE",
                        "reason": "INSUFFICIENT_PAST_ONLY_WARMUP",
                    }
                )
                continue
            warmup = calendar[start_position - warmup_days : start_position]
            inference_dates = warmup.append(block)
            inference = raw.reindex(inference_dates)[list(RAW_MARKET_FEATURE_COLUMNS)].apply(pd.to_numeric, errors="coerce")
            if len(inference) != warmup_days + len(block) or inference.isna().any().any():
                block_receipts.append(
                    {
                        "phase": phase,
                        "block_index": block_index,
                        "status": "SOURCE_UNAVAILABLE",
                        "reason": "PAST_ONLY_WARMUP_OR_BLOCK_GAP",
                        "missing_cell_count": int(inference.isna().sum().sum()),
                    }
                )
                continue
            matrix = ((inference - means) / std).to_numpy(float)
            posterior = _causal_forward_filter(model, matrix)
            raw_state = posterior.argmax(axis=1)
            state = (raw_state == risk_on_raw_state).astype(np.int8)
            duration = _state_duration(state)
            selected = np.arange(len(warmup), len(inference))
            if (
                not np.isfinite(posterior[selected]).all()
                or not np.allclose(posterior[selected].sum(axis=1), 1.0, atol=1e-10)
            ):
                _raise("market HMM posterior is invalid", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", path_id=path_id)
            path_outputs.append(
                pd.DataFrame(
                    {
                        "path_id": path_id,
                        "phase": phase,
                        "block_index": block_index,
                        "decision_as_of_trade_date": block,
                        "market_risk_on_posterior": posterior[selected, risk_on_raw_state],
                        "market_state": state[selected],
                        "market_state_duration": duration[selected],
                        "market_hmm_observation_completeness": 1.0,
                        "market_hmm_model_identity": model_identity,
                    }
                )
            )
            block_receipts.append(
                {
                    "phase": phase,
                    "block_index": block_index,
                    "status": "AVAILABLE",
                    "date_start": block[0].date().isoformat(),
                    "date_end": block[-1].date().isoformat(),
                    "warmup_start": warmup[0].date().isoformat(),
                    "warmup_end": warmup[-1].date().isoformat(),
                    "warmup_row_count": len(warmup),
                }
            )
    output = pd.concat(path_outputs, ignore_index=True) if path_outputs else pd.DataFrame()
    receipt = {
        "schema_version": "advisory_score_hmm_market_hmm_fold_receipt_v1",
        "path_id": path_id,
        "status": "AVAILABLE" if not output.empty else "SOURCE_UNAVAILABLE",
        "train_row_count": len(train_dates),
        "validation_row_count": len(validation_dates),
        "hmm_family": {
            "n_components": 2,
            "covariance_type": "full",
            "n_iter": 200,
            "tol": 1e-4,
            "random_state": 42,
            "min_covar": 1e-5,
        },
        "model_identity": model_identity,
        "convergence_rule": "FINITE_ABS_FINAL_LOG_LIKELIHOOD_DELTA_LT_TOL",
        "final_log_likelihood_delta": final_likelihood_delta,
        "train_block_lengths": train_block_lengths,
        "parameters": parameter_payload,
        "blocks": block_receipts,
        "causal_filter": "PAST_ONLY_FORWARD_FILTER_RESET_PER_DISCONTIGUOUS_BLOCK",
        "smoothed_or_viterbi_used": False,
    }
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    return output, receipt


def _validated_hmm_final_delta(model: Any, *, path_id: str) -> float:
    history = np.asarray(tuple(model.monitor_.history), dtype=float)
    final_delta = float(history[-1] - history[-2]) if len(history) >= 2 else math.nan
    if (
        len(history) < 2
        or not np.isfinite(history).all()
        or not np.isfinite(final_delta)
        or abs(final_delta) >= float(model.tol)
    ):
        _raise("market HMM did not converge", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID", path_id=path_id)
    return final_delta


def build_score_hmm_targets(
    *,
    score_features: pd.DataFrame,
    primary_labels: pd.DataFrame,
    secondary_labels: pd.DataFrame,
) -> ScoreHMMTargetResult:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    base = score_features.copy()
    primary_source = primary_labels.copy()
    secondary_source = secondary_labels.copy()
    for name, frame in (("score", base), ("primary", primary_source), ("secondary", secondary_source)):
        missing = sorted(set(keys) - set(frame.columns))
        if missing:
            _raise(
                "score/HMM target clock columns are incomplete",
                "ADVISORY_SCORE_HMM_TARGET_INVALID",
                source=name,
                missing=missing,
            )
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
        frame["target_trade_date"] = pd.to_datetime(frame["target_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
    if primary_source.duplicated(keys).any() or secondary_source.duplicated(keys).any():
        _raise("score/HMM targets contain duplicate keys", "ADVISORY_SCORE_HMM_TARGET_INVALID")
    base_keys = set(map(tuple, base[keys].itertuples(index=False, name=None)))
    if base_keys != set(map(tuple, primary_source[keys].itertuples(index=False, name=None))):
        _raise("primary policy labels do not match exact Top20 keys", "ADVISORY_SCORE_HMM_TARGET_INVALID")
    if base_keys != set(map(tuple, secondary_source[keys].itertuples(index=False, name=None))):
        _raise("secondary labels do not match exact Top20 keys", "ADVISORY_SCORE_HMM_TARGET_INVALID")
    primary = primary_source[keys + ["net_return_bps", "net_excess_return_bps", "label_status"]].copy()
    primary["primary_net_return_bps"] = pd.to_numeric(primary["net_return_bps"], errors="coerce")
    primary["primary_net_excess_return_bps"] = pd.to_numeric(primary["net_excess_return_bps"], errors="coerce")
    primary["primary_target_known"] = primary["label_status"].eq("MATURED") & primary[
        "primary_net_return_bps"
    ].notna()
    panel = base.merge(
        primary[keys + ["primary_net_return_bps", "primary_net_excess_return_bps", "primary_target_known", "label_status"]],
        on=keys,
        how="left",
        validate="one_to_one",
    ).rename(columns={"label_status": "primary_label_status"})
    coverage_rows: list[dict[str, Any]] = []
    for horizon in SCORE_HMM_SECONDARY_HORIZONS:
        status_col = f"label_status_{horizon}"
        value_col = f"stock_net_return_{horizon}"
        excess_col = f"excess_return_{horizon}"
        required = {status_col, value_col, excess_col}
        if not required.issubset(secondary_source.columns):
            _raise(
                "secondary outcome labels are incomplete",
                "ADVISORY_SCORE_HMM_TARGET_INVALID",
                horizon=horizon,
                missing=sorted(required - set(secondary_source.columns)),
            )
        selected = secondary_source[keys + [status_col, value_col, excess_col]].copy()
        selected[f"h{horizon}_net_return_bps"] = pd.to_numeric(selected[value_col], errors="coerce") * 10_000.0
        selected[f"h{horizon}_net_excess_return_bps"] = pd.to_numeric(selected[excess_col], errors="coerce") * 10_000.0
        selected[f"h{horizon}_target_known"] = selected[status_col].eq("MATURE_EXECUTABLE") & selected[
            f"h{horizon}_net_return_bps"
        ].notna()
        panel = panel.merge(
            selected[
                keys
                + [
                    f"h{horizon}_net_return_bps",
                    f"h{horizon}_net_excess_return_bps",
                    f"h{horizon}_target_known",
                    status_col,
                ]
            ],
            on=keys,
            how="left",
            validate="one_to_one",
        )
    for decision, group in panel.groupby("decision_as_of_trade_date", sort=True):
        for head in TARGET_HEADS:
            coverage_rows.append(
                {
                    "decision_as_of_trade_date": decision,
                    "target_head": head,
                    "candidate_count": len(group),
                    "known_count": int(group[TARGET_KNOWN_COLUMN[head]].sum()),
                    "unknown_count": int((~group[TARGET_KNOWN_COLUMN[head]]).sum()),
                    "status": "AVAILABLE" if bool(group[TARGET_KNOWN_COLUMN[head]].any()) else "TARGET_UNAVAILABLE",
                }
            )
    return ScoreHMMTargetResult(panel=panel, coverage=pd.DataFrame(coverage_rows))


def validate_score_hmm_label_interval_isolation(
    *,
    primary_labels: pd.DataFrame,
    secondary_labels: pd.DataFrame,
    cpcv_payload: Mapping[str, Any],
) -> dict[str, Any]:
    """Prove the reused CPCV date paths also purge every new label interval."""

    key_columns = {"decision_as_of_trade_date", "target_trade_date"}
    primary_required = {*key_columns, "label_status", "label_information_end"}
    secondary_required = {
        *key_columns,
        *(f"label_status_{horizon}" for horizon in SCORE_HMM_SECONDARY_HORIZONS),
        *(f"actual_exit_date_{horizon}" for horizon in SCORE_HMM_SECONDARY_HORIZONS),
    }
    missing = {
        "primary": sorted(primary_required - set(primary_labels.columns)),
        "secondary": sorted(secondary_required - set(secondary_labels.columns)),
    }
    if any(missing.values()):
        _raise(
            "score/HMM labels omit information-interval columns",
            "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
            missing=missing,
        )

    interval_frames: dict[str, pd.DataFrame] = {}
    primary = primary_labels.loc[primary_labels["label_status"].eq("MATURED")].copy()
    interval_frames["PRIMARY"] = primary[
        ["decision_as_of_trade_date", "target_trade_date", "label_information_end"]
    ].rename(columns={"target_trade_date": "information_start", "label_information_end": "information_end"})
    for horizon in SCORE_HMM_SECONDARY_HORIZONS:
        known = secondary_labels.loc[
            secondary_labels[f"label_status_{horizon}"].eq("MATURE_EXECUTABLE")
        ].copy()
        interval_frames[f"H{horizon}"] = known[
            ["decision_as_of_trade_date", "target_trade_date", f"actual_exit_date_{horizon}"]
        ].rename(
            columns={
                "target_trade_date": "information_start",
                f"actual_exit_date_{horizon}": "information_end",
            }
        )

    ready_paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if not ready_paths:
        _raise("score/HMM label isolation received no READY paths", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID")
    checks: list[dict[str, Any]] = []
    for head, raw_intervals in interval_frames.items():
        intervals = raw_intervals.copy()
        for column in ("decision_as_of_trade_date", "information_start", "information_end"):
            intervals[column] = pd.to_datetime(intervals[column], errors="coerce").dt.normalize()
        invalid = (
            intervals[["decision_as_of_trade_date", "information_start", "information_end"]].isna().any(axis=1)
            | intervals["information_end"].lt(intervals["information_start"])
        )
        if intervals.empty or invalid.any():
            _raise(
                "score/HMM known labels have invalid information intervals",
                "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
                target_head=head,
                invalid_count=int(invalid.sum()),
            )
        for path in ready_paths:
            train_dates = pd.DatetimeIndex(pd.to_datetime(path.get("train_dates", ()))).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path.get("validation_dates", ()))).normalize()
            train = intervals.loc[intervals["decision_as_of_trade_date"].isin(train_dates)]
            validation = intervals.loc[intervals["decision_as_of_trade_date"].isin(validation_dates)]
            overlap_count = _label_interval_overlap_count(train=train, validation=validation)
            if overlap_count:
                _raise(
                    "score/HMM reused CPCV path leaks a new label interval",
                    "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
                    target_head=head,
                    path_id=str(path.get("path_id") or ""),
                    overlap_count=overlap_count,
                )
            checks.append(
                {
                    "target_head": head,
                    "path_id": str(path.get("path_id") or ""),
                    "train_interval_count": len(train),
                    "validation_interval_count": len(validation),
                    "overlap_count": 0,
                }
            )
    receipt = {
        "schema_version": "advisory_score_hmm_label_interval_isolation_v1",
        "target_heads": list(TARGET_HEADS),
        "ready_path_count": len(ready_paths),
        "check_count": len(checks),
        "checks": checks,
        "status": "PASS_NO_INFORMATION_INTERVAL_OVERLAP",
    }
    receipt["receipt_sha256"] = canonical_json_sha256(receipt)
    return receipt


def _label_interval_overlap_count(*, train: pd.DataFrame, validation: pd.DataFrame) -> int:
    if train.empty or validation.empty:
        return 0
    distinct = validation[["information_start", "information_end"]].drop_duplicates().sort_values(
        ["information_start", "information_end"]
    )
    merged: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for start, end in distinct.itertuples(index=False, name=None):
        if not merged or start > merged[-1][1]:
            merged.append((start, end))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], end))
    overlap = pd.Series(False, index=train.index)
    for validation_start, validation_end in merged:
        overlap |= (train["information_start"] <= validation_end) & (
            train["information_end"] >= validation_start
        )
    return int(overlap.sum())


def run_score_hmm_crossfit(
    *,
    target_panel: pd.DataFrame,
    raw_market_features: pd.DataFrame,
    cpcv_payload: Mapping[str, Any],
    hmm_result: FoldLocalMarketHMMResult,
    label_interval_isolation: Mapping[str, Any],
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> ScoreHMMCrossfitResult:
    """Run the three executable arms with fixed nested OOF calibration."""

    panel = target_panel.copy()
    panel["decision_as_of_trade_date"] = pd.to_datetime(panel["decision_as_of_trade_date"]).dt.normalize()
    raw = raw_market_features.copy()
    raw["decision_as_of_trade_date"] = pd.to_datetime(raw["decision_as_of_trade_date"]).dt.normalize()
    panel = panel.merge(raw, on="decision_as_of_trade_date", how="left", validate="many_to_one")
    block_by_date = {
        pd.Timestamp(key).normalize(): int(value) for key, value in cpcv_payload.get("block_by_date", {}).items()
    }
    ready_paths = [item for item in cpcv_payload.get("paths", ()) if item.get("status") == "READY"]
    if len(ready_paths) != request.expected_ready_path_count:
        _raise("score/HMM CPCV path count drift", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID")
    path_outputs: list[pd.DataFrame] = []
    model_receipts: list[dict[str, Any]] = []
    hmm_states = hmm_result.states.copy()
    if not hmm_states.empty:
        hmm_states["decision_as_of_trade_date"] = pd.to_datetime(hmm_states["decision_as_of_trade_date"]).dt.normalize()
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        feature_columns = SCORE_HMM_FEATURES_BY_ARM[arm_id]
        for path in ready_paths:
            path_id = str(path["path_id"])
            train_dates = pd.DatetimeIndex(pd.to_datetime(path["train_dates"])).normalize()
            validation_dates = pd.DatetimeIndex(pd.to_datetime(path["validation_dates"])).normalize()
            path_panel = panel.copy()
            if arm_id == SCORE_PLUS_MARKET_HMM:
                path_hmm = hmm_states.loc[hmm_states["path_id"].eq(path_id), [
                    "phase",
                    "decision_as_of_trade_date",
                    *MARKET_HMM_FEATURE_COLUMNS,
                ]]
                if path_hmm.duplicated(["phase", "decision_as_of_trade_date"]).any():
                    _raise("market HMM emitted duplicate fold/date states", "ADVISORY_SCORE_HMM_MARKET_HMM_INVALID")
                train_hmm = path_hmm.loc[path_hmm["phase"].eq("TRAIN")].drop(columns="phase")
                validation_hmm = path_hmm.loc[path_hmm["phase"].eq("VALIDATION")].drop(columns="phase")
                path_panel["_phase"] = np.where(
                    path_panel["decision_as_of_trade_date"].isin(train_dates), "TRAIN", "VALIDATION"
                )
                path_panel = path_panel.merge(
                    pd.concat(
                        [train_hmm.assign(_phase="TRAIN"), validation_hmm.assign(_phase="VALIDATION")],
                        ignore_index=True,
                    ),
                    on=["_phase", "decision_as_of_trade_date"],
                    how="left",
                    validate="many_to_one",
                )
            train_mask = path_panel["decision_as_of_trade_date"].isin(train_dates)
            validation_mask = path_panel["decision_as_of_trade_date"].isin(validation_dates)
            if arm_id == PACKAGE_SCORE_CALIBRATION_ONLY:
                available = pd.Series(True, index=path_panel.index)
            else:
                available = path_panel["raw_market_status"].eq("AVAILABLE")
                if arm_id == SCORE_PLUS_MARKET_HMM:
                    available &= path_panel[list(MARKET_HMM_FEATURE_COLUMNS)].notna().all(axis=1)
            validation_rows = path_panel.loc[validation_mask & available].copy()
            if validation_rows.empty:
                model_receipts.append(
                    {
                        "arm_id": arm_id,
                        "path_id": path_id,
                        "status": "SOURCE_UNAVAILABLE",
                        "validation_row_count": 0,
                    }
                )
                continue
            base_output = validation_rows[
                [
                    "decision_as_of_trade_date",
                    "target_trade_date",
                    "instrument",
                    "selection_effective_rank",
                    *TARGET_VALUE_COLUMN.values(),
                    *TARGET_KNOWN_COLUMN.values(),
                ]
            ].copy()
            base_output.insert(0, "path_id", path_id)
            base_output.insert(0, "arm_id", arm_id)
            head_receipts: list[dict[str, Any]] = []
            for head in TARGET_HEADS:
                predictions, head_receipt = _fit_crossfit_head(
                    frame=path_panel,
                    train_mask=train_mask & available,
                    validation_mask=validation_mask & available,
                    feature_columns=feature_columns,
                    value_column=TARGET_VALUE_COLUMN[head],
                    known_column=TARGET_KNOWN_COLUMN[head],
                    block_by_date=block_by_date,
                    request=request,
                    arm_id=arm_id,
                    path_id=path_id,
                    head=head,
                )
                for name, values in predictions.items():
                    base_output[f"{name}_{head.lower()}"] = values
                head_receipts.append(head_receipt)
            path_outputs.append(base_output)
            model_receipts.append(
                {
                    "arm_id": arm_id,
                    "path_id": path_id,
                    "status": "AVAILABLE",
                    "train_row_count": int(train_mask.sum()),
                    "validation_row_count": len(validation_rows),
                    "heads": head_receipts,
                }
            )
    raw_oof = pd.concat(path_outputs, ignore_index=True) if path_outputs else pd.DataFrame()
    aggregated = _aggregate_oof_predictions(raw_oof, panel=panel, request=request)
    return ScoreHMMCrossfitResult(
        oof_predictions=aggregated,
        fold_receipts={
            "schema_version": "advisory_score_hmm_fold_receipts_v1",
            "request_sha256": request.request_sha256,
            "hmm_folds": list(hmm_result.receipts),
            "label_interval_isolation": dict(label_interval_isolation),
            "model_folds": model_receipts,
            "expected_ready_path_count": request.expected_ready_path_count,
            "expected_oof_predictions_per_row": request.expected_oof_predictions_per_row,
        },
    )


def _fit_crossfit_head(
    *,
    frame: pd.DataFrame,
    train_mask: pd.Series,
    validation_mask: pd.Series,
    feature_columns: Sequence[str],
    value_column: str,
    known_column: str,
    block_by_date: Mapping[pd.Timestamp, int],
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    arm_id: str,
    path_id: str,
    head: str,
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    model_features = list(feature_columns)
    known_train = train_mask & frame[known_column].astype(bool) & pd.to_numeric(frame[value_column], errors="coerce").notna()
    train = frame.loc[known_train].copy()
    validation = frame.loc[validation_mask].copy()
    if len(train) < 100 or validation.empty:
        _raise(
            "score/HMM model fold lacks target support",
            "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
            arm_id=arm_id,
            path_id=path_id,
            head=head,
        )
    truth = pd.to_numeric(train[value_column], errors="raise").to_numpy(float)
    binary = (truth > 0.0).astype(int)
    if len(np.unique(binary)) != 2:
        _raise("score/HMM outer train lacks class variation", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
    train_groups = train["decision_as_of_trade_date"].map(block_by_date)
    if train_groups.isna().any() or train_groups.nunique() != 6:
        _raise("score/HMM inner OOF group roster drift", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
    inner_prediction = np.full(len(train), np.nan, dtype=float)
    inner_probability = np.full(len(train), np.nan, dtype=float)
    for group in sorted(train_groups.unique()):
        inner_validation = train_groups.eq(group).to_numpy()
        inner_train = ~inner_validation
        if len(np.unique(binary[inner_train])) != 2:
            _raise("score/HMM inner train lacks class variation", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
        ridge, logistic = _model_pair(request)
        ridge.fit(train.loc[inner_train, model_features], truth[inner_train])
        _fit_logistic(logistic, train.loc[inner_train, model_features], binary[inner_train], head=head)
        inner_prediction[inner_validation] = ridge.predict(train.loc[inner_validation, model_features])
        inner_probability[inner_validation] = logistic.predict_proba(train.loc[inner_validation, model_features])[:, 1]
    if not np.isfinite(inner_prediction).all() or not np.isfinite(inner_probability).all():
        _raise("score/HMM inner OOF output is non-finite", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
    residual = truth - inner_prediction
    lower_residual = float(np.quantile(residual, request.conformal_lower_quantile))
    upper_residual = float(np.quantile(residual, request.conformal_upper_quantile))
    ridge, logistic = _model_pair(request)
    ridge.fit(train[model_features], truth)
    _fit_logistic(logistic, train[model_features], binary, head=head)
    expected = np.asarray(ridge.predict(validation[model_features]), dtype=float)
    probability = np.asarray(logistic.predict_proba(validation[model_features])[:, 1], dtype=float)
    if not np.isfinite(expected).all() or not np.isfinite(probability).all() or ((probability < 0) | (probability > 1)).any():
        _raise("score/HMM outer prediction is invalid", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
    base_rate = float(binary.mean())
    return (
        {
            "expected_net_return_bps": expected,
            "expected_net_return_lcb80_bps": expected + lower_residual,
            "expected_net_return_ucb80_bps": expected + upper_residual,
            "positive_probability": probability,
            "train_base_rate": np.full(len(validation), base_rate),
        },
        {
            "head": head,
            "train_row_count": len(train),
            "train_positive_count": int(binary.sum()),
            "validation_row_count": len(validation),
            "inner_oof_row_count": len(inner_prediction),
            "inner_oof_group_count": int(train_groups.nunique()),
            "conformal_lower_residual_bps": lower_residual,
            "conformal_upper_residual_bps": upper_residual,
            "train_base_rate": base_rate,
        },
    )


def _model_pair(request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> tuple[Pipeline, Pipeline]:
    ridge = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=request.ridge_alpha, solver=request.ridge_solver, fit_intercept=True)),
        ]
    )
    logistic = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
            (
                "model",
                LogisticRegression(
                    C=request.logistic_c,
                    penalty="l2",
                    solver=request.logistic_solver,
                    fit_intercept=True,
                    max_iter=request.logistic_max_iter,
                    class_weight=None,
                    random_state=request.model_random_state,
                ),
            ),
        ]
    )
    return ridge, logistic


def _fit_logistic(model: Pipeline, features: pd.DataFrame, truth: np.ndarray, *, head: str) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", ConvergenceWarning)
        model.fit(features, truth)
    if any(issubclass(item.category, ConvergenceWarning) for item in caught):
        _raise("score/HMM logistic model did not converge", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)
    estimator = model.named_steps["model"]
    if int(np.max(estimator.n_iter_)) >= int(estimator.max_iter):
        _raise("score/HMM logistic model exhausted max_iter", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID", head=head)


def _aggregate_oof_predictions(
    raw_oof: pd.DataFrame,
    *,
    panel: pd.DataFrame,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> pd.DataFrame:
    keys = ["arm_id", "decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]
    prediction_columns = [
        f"{prefix}_{head.lower()}"
        for head in TARGET_HEADS
        for prefix in (
            "expected_net_return_bps",
            "expected_net_return_lcb80_bps",
            "expected_net_return_ucb80_bps",
            "positive_probability",
            "train_base_rate",
        )
    ]
    truth_columns = [*TARGET_VALUE_COLUMN.values(), *TARGET_KNOWN_COLUMN.values()]
    if raw_oof.empty:
        aggregated = pd.DataFrame(columns=[*keys, *prediction_columns, "oof_prediction_count", *truth_columns])
    else:
        grouped = raw_oof.groupby(keys, sort=True, observed=True)
        aggregated = grouped[prediction_columns].mean().reset_index()
        counts = grouped["path_id"].nunique().rename("oof_prediction_count").reset_index()
        aggregated = aggregated.merge(counts, on=keys, validate="one_to_one")
        truth = raw_oof[keys + truth_columns].drop_duplicates(keys)
        aggregated = aggregated.merge(truth, on=keys, validate="one_to_one")
    base = panel[
        ["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank", *truth_columns]
    ].drop_duplicates(["decision_as_of_trade_date", "instrument"])
    grids = []
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        grid = base.copy()
        grid.insert(0, "arm_id", arm_id)
        grids.append(grid)
    full = pd.concat(grids, ignore_index=True)
    merged = full.merge(
        aggregated.drop(columns=truth_columns, errors="ignore"),
        on=keys,
        how="left",
        validate="one_to_one",
    )
    merged["oof_prediction_count"] = merged["oof_prediction_count"].fillna(0).astype(int)
    bad = ~merged["oof_prediction_count"].isin({0, request.expected_oof_predictions_per_row})
    if bad.any():
        _raise(
            "score/HMM row has partial OOF multiplicity",
            "ADVISORY_SCORE_HMM_CROSSFIT_INVALID",
            samples=merged.loc[bad, keys + ["oof_prediction_count"]].head(10).to_dict("records"),
        )
    merged["prediction_status"] = np.where(
        merged["oof_prediction_count"].eq(request.expected_oof_predictions_per_row),
        "AVAILABLE",
        "SOURCE_UNAVAILABLE",
    )
    score_rows = merged["arm_id"].eq(PACKAGE_SCORE_CALIBRATION_ONLY)
    if not merged.loc[score_rows, "oof_prediction_count"].eq(request.expected_oof_predictions_per_row).all():
        _raise("score-only arm lost OOF coverage", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID")
    return merged.sort_values(["arm_id", "decision_as_of_trade_date", "selection_effective_rank"]).reset_index(drop=True)


def build_calibration_metrics(oof_predictions: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        arm = oof_predictions.loc[
            oof_predictions["arm_id"].eq(arm_id) & oof_predictions["prediction_status"].eq("AVAILABLE")
        ]
        if arm.empty:
            rows.extend(
                {
                    "arm_id": arm_id,
                    "target_head": head,
                    "status": "SOURCE_UNAVAILABLE",
                    "known_row_count": 0,
                    "reason_code": "ARM_HAS_NO_AVAILABLE_OOF_PREDICTIONS",
                }
                for head in TARGET_HEADS
            )
            continue
        for head in TARGET_HEADS:
            known = arm.loc[arm[TARGET_KNOWN_COLUMN[head]].astype(bool)].copy()
            expected_col = f"expected_net_return_bps_{head.lower()}"
            lower_col = f"expected_net_return_lcb80_bps_{head.lower()}"
            upper_col = f"expected_net_return_ucb80_bps_{head.lower()}"
            probability_col = f"positive_probability_{head.lower()}"
            base_rate_col = f"train_base_rate_{head.lower()}"
            truth = pd.to_numeric(known[TARGET_VALUE_COLUMN[head]], errors="coerce").to_numpy(float)
            expected = pd.to_numeric(known[expected_col], errors="coerce").to_numpy(float)
            lower = pd.to_numeric(known[lower_col], errors="coerce").to_numpy(float)
            upper = pd.to_numeric(known[upper_col], errors="coerce").to_numpy(float)
            probability = pd.to_numeric(known[probability_col], errors="coerce").to_numpy(float)
            base_rate = pd.to_numeric(known[base_rate_col], errors="coerce").to_numpy(float)
            if not len(truth) or not all(np.isfinite(value).all() for value in (truth, expected, lower, upper, probability)):
                _raise("score/HMM calibration inputs are invalid", "ADVISORY_SCORE_HMM_EVALUATION_INVALID")
            binary = (truth > 0.0).astype(int)
            clipped = np.clip(probability, np.finfo(float).eps, 1.0 - np.finfo(float).eps)
            deciles = _decile_calibration(expected, truth)
            rows.append(
                {
                    "arm_id": arm_id,
                    "target_head": head,
                    "status": "EVALUATED",
                    "reason_code": None,
                    "known_row_count": len(truth),
                    "mae_bps": float(np.mean(np.abs(expected - truth))),
                    "rmse_bps": float(np.sqrt(np.mean((expected - truth) ** 2))),
                    "spearman": _safe_correlation(pd.Series(expected), pd.Series(truth), method="spearman"),
                    "auc": float(roc_auc_score(binary, probability)) if len(np.unique(binary)) == 2 else np.nan,
                    "brier": float(np.mean((probability - binary) ** 2)),
                    "base_rate_brier": float(np.mean((base_rate - binary) ** 2)),
                    "brier_improvement": float(np.mean((base_rate - binary) ** 2) - np.mean((probability - binary) ** 2)),
                    "logloss": float(log_loss(binary, clipped, labels=[0, 1])),
                    "ece_10_bin": float(expected_calibration_error(binary, probability, bin_count=10)["value"]),
                    "positive_rate": float(binary.mean()),
                    "interval_coverage": float(((truth >= lower) & (truth <= upper)).mean()),
                    "interval_mean_width_bps": float(np.mean(upper - lower)),
                    "prediction_std_bps": float(np.std(expected, ddof=1)),
                    "probability_std": float(np.std(probability, ddof=1)),
                    "decile_calibration_json": json.dumps(deciles, sort_keys=True, separators=(",", ":")),
                }
            )
    return pd.DataFrame(rows)


def build_admission_decisions(
    *,
    oof_predictions: pd.DataFrame,
    parent_top20: pd.DataFrame,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> pd.DataFrame:
    top5 = parent_top20.loc[parent_top20["selection_effective_rank"].le(5)].copy()
    if len(top5) != request.expected_top5_row_count:
        _raise("parent Top5 action grid is incomplete", "ADVISORY_SCORE_HMM_ADMISSION_INVALID")
    records: list[dict[str, Any]] = []
    executable = oof_predictions.loc[oof_predictions["selection_effective_rank"].le(5)]
    for row in executable.itertuples(index=False):
        available = row.prediction_status == "AVAILABLE"
        expected = _finite(getattr(row, "expected_net_return_bps_primary")) if available else None
        lower = _finite(getattr(row, "expected_net_return_lcb80_bps_primary")) if available else None
        probability = _finite(getattr(row, "positive_probability_primary")) if available else None
        if not available:
            action, reason = "UNAVAILABLE", "MODEL_INPUT_UNAVAILABLE"
        elif lower is None or lower <= 0.0:
            action, reason = "SKIP", "SKIP_NONPOSITIVE_LOWER_BOUND"
        elif probability is None or probability < 0.5:
            action, reason = "SKIP", "SKIP_NONPOSITIVE_PROBABILITY"
        else:
            action, reason = "TAKE", "TAKE_POSITIVE_VALUE"
        known = bool(getattr(row, "primary_target_known"))
        decision = AdvisoryAdmissionDecisionV1(
            request_sha256=request.request_sha256,
            arm_id=row.arm_id,
            decision_as_of_trade_date=pd.Timestamp(row.decision_as_of_trade_date).date(),
            target_trade_date=pd.Timestamp(row.target_trade_date).date(),
            instrument=str(row.instrument),
            parent_rank=int(row.selection_effective_rank),
            action=action,
            reason_code=reason,
            arm_available=available,
            label_evaluable=known,
            evaluation_reason_code=None if known else "LABEL_NOT_EVALUABLE",
            primary_expected_net_return_bps=expected,
            primary_expected_net_return_lcb80_bps=lower,
            primary_positive_probability=probability,
        )
        records.append(decision.model_dump(mode="json"))
    for arm_id in SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS:
        for row in top5.itertuples(index=False):
            decision = AdvisoryAdmissionDecisionV1(
                request_sha256=request.request_sha256,
                arm_id=arm_id,
                decision_as_of_trade_date=pd.Timestamp(row.decision_as_of_trade_date).date(),
                target_trade_date=pd.Timestamp(row.target_trade_date).date(),
                instrument=str(row.instrument),
                parent_rank=int(row.selection_effective_rank),
                action="UNAVAILABLE",
                reason_code="NOT_RUN_SOURCE_UNAVAILABLE",
                arm_available=False,
                label_evaluable=False,
                evaluation_reason_code="LABEL_NOT_EVALUABLE",
                primary_expected_net_return_bps=None,
                primary_expected_net_return_lcb80_bps=None,
                primary_positive_probability=None,
            )
            records.append(decision.model_dump(mode="json"))
    decisions = pd.DataFrame(records)
    decisions["decision_as_of_trade_date"] = pd.to_datetime(decisions["decision_as_of_trade_date"])
    decisions["target_trade_date"] = pd.to_datetime(decisions["target_trade_date"])
    day_state: dict[tuple[str, pd.Timestamp], str] = {}
    for key, group in decisions.groupby(["arm_id", "decision_as_of_trade_date"], sort=True):
        if len(group) != 5 or tuple(sorted(group["parent_rank"].tolist())) != (1, 2, 3, 4, 5):
            _raise("admission action grid changed parent Top5", "ADVISORY_SCORE_HMM_ADMISSION_INVALID")
        if group["action"].eq("UNAVAILABLE").any():
            state = "ADMISSION_UNAVAILABLE"
        elif group["action"].eq("TAKE").any():
            state = "TAKE_SOME"
        else:
            state = "NO_ELIGIBLE_RECOMMENDATION"
        day_state[key] = state
    decisions["day_state"] = [
        day_state[(row.arm_id, row.decision_as_of_trade_date)] for row in decisions.itertuples(index=False)
    ]
    return decisions.sort_values(["arm_id", "decision_as_of_trade_date", "parent_rank"]).reset_index(drop=True)


def evaluate_admission_policies(
    *,
    decisions: pd.DataFrame,
    policy_rankings: pd.DataFrame,
    market_daily: pd.DataFrame,
    benchmark_daily: pd.DataFrame,
    suspend_rows: pd.DataFrame,
    trading_calendar: Sequence[pd.Timestamp],
    policy_request: FrozenAdvisoryPolicyDatasetRequestV1,
    stored_baseline_daily: pd.DataFrame,
    stored_baseline_episodes: pd.DataFrame,
    regime_daily: pd.DataFrame,
    calibration_metrics: pd.DataFrame,
    parent_context_exposure: Mapping[str, Any],
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> ScoreHMMEvaluationResult:
    policy = transition_policy_from_payload(policy_request.shadow_policy)
    candidate_dates = pd.DatetimeIndex(
        pd.to_datetime(policy_rankings.loc[policy_rankings["is_candidate_decision"], "decision_as_of_trade_date"])
    ).normalize().unique()
    baseline = replay_shadow_portfolio(
        rankings=policy_rankings,
        daily=market_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        trading_calendar=trading_calendar,
        policy=policy,
        policy_sha256=policy_request.shadow_policy_sha256,
        cost_policy=policy_request.cost_policy,
        request_id=policy_request.request_id,
        rank_depth=request.score_distribution_depth,
        candidate_decision_dates=candidate_dates,
    )
    _assert_baseline_parity(baseline.daily, baseline.episodes, stored_baseline_daily, stored_baseline_episodes)
    daily_frames = [baseline.daily.assign(arm_id=BASELINE_ARM_ID)]
    episode_frames = [baseline.episodes.assign(arm_id=BASELINE_ARM_ID)]
    metrics_by_arm: dict[str, dict[str, Any]] = {}
    decisions = decisions.copy()
    decisions["decision_as_of_trade_date"] = pd.to_datetime(decisions["decision_as_of_trade_date"]).dt.normalize()
    regime = regime_daily[["decision_as_of_trade_date", "regime"]].copy()
    regime["decision_as_of_trade_date"] = pd.to_datetime(regime["decision_as_of_trade_date"]).dt.normalize()
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        arm_decisions = decisions.loc[decisions["arm_id"].eq(arm_id)].copy()
        unavailable_days = arm_decisions.loc[
            arm_decisions["action"].eq("UNAVAILABLE"), "decision_as_of_trade_date"
        ].nunique()
        if unavailable_days:
            metrics_by_arm[arm_id] = {
                "arm_id": arm_id,
                "status": "SOURCE_UNAVAILABLE_NO_POLICY_EVALUATION",
                "unavailable_day_count": int(unavailable_days),
                "eligible": False,
                "reason_codes": ["EXECUTABLE_ARM_HAS_UNAVAILABLE_ACTION_DAYS"],
            }
            continue
        priorities = arm_decisions.loc[arm_decisions["action"].eq("TAKE"), [
            "decision_as_of_trade_date",
            "instrument",
            "parent_rank",
        ]].rename(columns={"parent_rank": "entry_priority_rank"})
        arm_result = replay_shadow_portfolio(
            rankings=policy_rankings,
            daily=market_daily,
            benchmark_daily=benchmark_daily,
            suspend_rows=suspend_rows,
            trading_calendar=trading_calendar,
            policy=policy,
            policy_sha256=policy_request.shadow_policy_sha256,
            cost_policy=policy_request.cost_policy,
            request_id=f"{request.request_id}:{arm_id}",
            rank_depth=request.score_distribution_depth,
            candidate_decision_dates=candidate_dates,
            entry_priorities=priorities,
        )
        daily_frames.append(arm_result.daily.assign(arm_id=arm_id))
        episode_frames.append(arm_result.episodes.assign(arm_id=arm_id))
        metrics_by_arm[arm_id] = _evaluate_one_arm(
            arm_id=arm_id,
            decisions=arm_decisions,
            arm_daily=arm_result.daily,
            arm_episodes=arm_result.episodes,
            baseline_daily=baseline.daily,
            baseline_episodes=baseline.episodes,
            regime_daily=regime,
            calibration_metrics=calibration_metrics,
            request=request,
        )
    for arm_id in SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS:
        metrics_by_arm[arm_id] = {
            "arm_id": arm_id,
            "status": "NOT_RUN_SOURCE_UNAVAILABLE",
            "eligible": False,
            "reason_codes": ["CANONICAL_CAUSAL_SECTOR_OOF_SOURCE_UNAVAILABLE"],
        }
    for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
        metric = metrics_by_arm[arm_id]
        predecessors = {
            PACKAGE_SCORE_CALIBRATION_ONLY: (),
            SCORE_PLUS_RAW_MARKET_SHAPE: (PACKAGE_SCORE_CALIBRATION_ONLY,),
            SCORE_PLUS_MARKET_HMM: (SCORE_PLUS_RAW_MARKET_SHAPE,),
        }[arm_id]
        if metric.get("status") != "EVALUATED" or not predecessors:
            metric["predecessor_increment_lower_bps"] = None
            continue
        predecessor = metrics_by_arm[predecessors[0]]
        if predecessor.get("status") != "EVALUATED":
            metric["reason_codes"].append("PREDECESSOR_NOT_EVALUATED")
            metric["eligible"] = False
            continue
        daily = pd.concat(daily_frames, ignore_index=True)
        left = daily.loc[daily["arm_id"].eq(arm_id), ["decision_as_of_trade_date", "net_return_bps"]]
        right = daily.loc[daily["arm_id"].eq(predecessors[0]), ["decision_as_of_trade_date", "net_return_bps"]]
        paired = left.merge(right, on="decision_as_of_trade_date", suffixes=("_arm", "_predecessor"), validate="one_to_one")
        increment = paired["net_return_bps_arm"] - paired["net_return_bps_predecessor"]
        lower, upper = _moving_block_interval(
            increment,
            block_length=request.block_length_trading_days,
            repetitions=request.bootstrap_repetitions,
            seed=request.bootstrap_seed,
            alpha=request.familywise_alpha,
        )
        metric["predecessor_increment_mean_bps"] = float(increment.mean())
        metric["predecessor_increment_lower_bps"] = lower
        metric["predecessor_increment_upper_bps"] = upper
        if lower is None or lower <= 0.0:
            metric["reason_codes"].append("PREDECESSOR_INCREMENT_LOWER_NOT_POSITIVE")
            metric["eligible"] = False
    if parent_context_exposure.get("market_hmm_attribution_status") != "ATTRIBUTABLE_NO_EXPLICIT_PARENT_HMM_OUTPUT":
        metrics_by_arm[SCORE_PLUS_MARKET_HMM]["reason_codes"].append("MARKET_HMM_DUPLICATE_OR_UNKNOWN_EXPOSURE")
        metrics_by_arm[SCORE_PLUS_MARKET_HMM]["eligible"] = False
    frontier_complete = all(
        metrics_by_arm[arm_id].get("status") == "EVALUATED" for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS
    )
    if not frontier_complete:
        for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS:
            metric = metrics_by_arm[arm_id]
            if metric.get("status") == "EVALUATED":
                metric["reason_codes"].append("EXECUTABLE_FRONTIER_INCOMPLETE_SOURCE_UNAVAILABLE")
                metric["eligible"] = False
    eligible = tuple(
        arm_id for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS if bool(metrics_by_arm[arm_id].get("eligible"))
    )
    selected = None
    if eligible:
        selected = sorted(
            eligible,
            key=lambda arm_id: (
                -float(metrics_by_arm[arm_id]["daily_lift_familywise_lower_bps"]),
                -float(metrics_by_arm[arm_id]["maximum_drawdown"]),
                arm_id,
            ),
        )[0]
    support_sufficient = all(
        metrics_by_arm[arm_id].get("support_sufficient") is True
        for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS
        if metrics_by_arm[arm_id].get("status") == "EVALUATED"
    )
    evidence_class = (
        "AUX_CANDIDATE_SELECTED_NAVIGATION_ONLY"
        if selected
        else "AUX_PARTIAL_SOURCE_UNAVAILABLE"
        if not frontier_complete
        else "AUX_EXECUTED_FRONTIER_SELECTED_ZERO"
        if support_sufficient
        else "AUX_EXECUTED_FRONTIER_INSUFFICIENT_SUPPORT"
    )
    summary = {
        "schema_version": "advisory_score_hmm_arm_summary_v1",
        "request_sha256": request.request_sha256,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY.value,
        "baseline_arm_id": BASELINE_ARM_ID,
        "baseline_metrics": baseline.metrics,
        "arms": [metrics_by_arm[arm_id] for arm_id in SCORE_HMM_ARM_IDS],
        "eligible_arm_ids": list(eligible),
        "selected_arm_id": selected,
        "selected_arm_count": int(selected is not None),
        "generated_trial_count": len(SCORE_HMM_EXECUTABLE_ARM_IDS),
        "evaluated_trial_count": sum(
            metrics_by_arm[arm_id].get("status") == "EVALUATED" for arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS
        ),
        "evidence_class": evidence_class,
        "candidate_reselection_allowed": False,
        "confirmation_required": selected is not None,
        "sealed_holdout_accessed": False,
    }
    summary["summary_sha256"] = canonical_json_sha256(summary)
    return ScoreHMMEvaluationResult(
        policy_daily=pd.concat(daily_frames, ignore_index=True),
        policy_episodes=pd.concat(episode_frames, ignore_index=True),
        arm_summary=summary,
        selected_arm_id=selected,
        eligible_arm_ids=eligible,
        evidence_class=evidence_class,
    )


def _evaluate_one_arm(
    *,
    arm_id: str,
    decisions: pd.DataFrame,
    arm_daily: pd.DataFrame,
    arm_episodes: pd.DataFrame,
    baseline_daily: pd.DataFrame,
    baseline_episodes: pd.DataFrame,
    regime_daily: pd.DataFrame,
    calibration_metrics: pd.DataFrame,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> dict[str, Any]:
    paired = arm_daily.merge(
        baseline_daily[["decision_as_of_trade_date", "net_return_bps", "net_excess_return_bps"]],
        on="decision_as_of_trade_date",
        suffixes=("_arm", "_baseline"),
        validate="one_to_one",
    )
    paired = paired.sort_values("decision_as_of_trade_date").reset_index(drop=True)
    lift = paired["net_return_bps_arm"] - paired["net_return_bps_baseline"]
    lower, upper = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed,
        alpha=request.familywise_alpha,
    )
    exited = arm_episodes.loc[arm_episodes["status"].eq("EXITED")].copy()
    for date_column in ("entry_trade_date", "entry_date", "decision_as_of_trade_date"):
        if date_column in exited.columns:
            exited = exited.sort_values([date_column, "instrument"] if "instrument" in exited.columns else date_column)
            break
    episode_values = pd.to_numeric(exited["net_return_bps"], errors="coerce").dropna()
    episode_lower, episode_upper = _moving_block_interval(
        episode_values,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed,
        alpha=request.familywise_alpha,
    )
    by_day = decisions.groupby("decision_as_of_trade_date", sort=True).agg(
        take_count=("action", lambda values: int((values == "TAKE").sum())),
        skip_count=("action", lambda values: int((values == "SKIP").sum())),
    )
    by_day["intervened"] = by_day["skip_count"].gt(0)
    intervention_days = int(by_day["intervened"].sum())
    take_days = int(by_day["take_count"].gt(0).sum())
    skip_days = int(by_day["skip_count"].gt(0).sum())
    regime_frame = by_day.reset_index().merge(
        regime_daily,
        on="decision_as_of_trade_date",
        how="left",
        validate="one_to_one",
    )
    expected_regimes = tuple(sorted(regime_daily["regime"].dropna().astype(str).unique()))
    regime_support = {
        regime: int(regime_frame.loc[regime_frame["regime"].astype(str).eq(regime), "intervened"].sum())
        for regime in expected_regimes
    }
    support_reasons = []
    if len(paired) < request.minimum_paired_days:
        support_reasons.append("PAIRED_DAYS_BELOW_MINIMUM")
    if intervention_days < request.minimum_intervention_days:
        support_reasons.append("INTERVENTION_DAYS_BELOW_MINIMUM")
    if intervention_days / max(1, len(by_day)) < request.minimum_intervention_fraction:
        support_reasons.append("INTERVENTION_FRACTION_BELOW_MINIMUM")
    if take_days < request.minimum_take_days:
        support_reasons.append("TAKE_DAYS_BELOW_MINIMUM")
    if skip_days < request.minimum_skip_days:
        support_reasons.append("SKIP_DAYS_BELOW_MINIMUM")
    if regime_frame["regime"].isna().any() or not expected_regimes:
        support_reasons.append("REGIME_COVERAGE_INCOMPLETE")
    if any(value < request.minimum_intervention_days_per_regime for value in regime_support.values()):
        support_reasons.append("REGIME_INTERVENTION_DAYS_BELOW_MINIMUM")
    if by_day[["take_count", "skip_count"]].drop_duplicates().shape[0] < 2 or not by_day["skip_count"].gt(0).any():
        support_reasons.append("ACTION_VECTOR_DEGENERATE")
    support_sufficient = not support_reasons
    block_means = [float(value.mean()) for value in np.array_split(lift.to_numpy(float), 4)]
    late_half = float(lift.iloc[len(lift) // 2 :].mean())
    arm_cvar = _cvar_5(pd.to_numeric(arm_daily["net_return_bps"], errors="coerce"))
    baseline_cvar = _cvar_5(pd.to_numeric(baseline_daily["net_return_bps"], errors="coerce"))
    arm_mdd = float(arm_daily["drawdown"].min())
    baseline_mdd = float(baseline_daily["drawdown"].min())
    primary_calibration = calibration_metrics.loc[
        calibration_metrics["arm_id"].eq(arm_id)
        & calibration_metrics["target_head"].eq("PRIMARY")
        & calibration_metrics["status"].eq("EVALUATED")
    ]
    if len(primary_calibration) != 1:
        _raise("primary calibration row missing", "ADVISORY_SCORE_HMM_EVALUATION_INVALID", arm_id=arm_id)
    calibration = primary_calibration.iloc[0]
    reasons = list(support_reasons)
    if episode_lower is None or episode_lower <= 0.0:
        reasons.append("ACCEPTED_EPISODE_RETURN_LOWER_NOT_POSITIVE")
    if lower is None or lower <= request.minimum_economic_lift_bps:
        reasons.append("DAILY_LIFT_FAMILYWISE_LOWER_NOT_ABOVE_5BPS")
    if not float(calibration["brier"]) < float(calibration["base_rate_brier"]):
        reasons.append("PROBABILITY_BRIER_NOT_BETTER_THAN_BASE_RATE")
    if not float(calibration["probability_std"]) > 0.0:
        reasons.append("PROBABILITY_PREDICTION_CONSTANT")
    if arm_mdd < baseline_mdd:
        reasons.append("MAXIMUM_DRAWDOWN_WORSE_THAN_BASELINE")
    if arm_cvar < baseline_cvar:
        reasons.append("CVAR_5_WORSE_THAN_BASELINE")
    if late_half <= 0.0:
        reasons.append("LATE_HALF_LIFT_NOT_POSITIVE")
    if sum(value > 0.0 for value in block_means) < 3:
        reasons.append("FEWER_THAN_THREE_POSITIVE_TIME_BLOCKS")
    dsr = _deflated_sharpe_diagnostic(lift.tolist(), trial_count=request.reserved_candidate_indices[-1])
    return {
        "arm_id": arm_id,
        "status": "EVALUATED",
        "paired_day_count": len(paired),
        "daily_lift_mean_bps": float(lift.mean()),
        "daily_lift_familywise_lower_bps": lower,
        "daily_lift_familywise_upper_bps": upper,
        "accepted_episode_count": len(exited),
        "accepted_episode_mean_net_return_bps": float(episode_values.mean()) if len(episode_values) else None,
        "accepted_episode_familywise_lower_bps": episode_lower,
        "accepted_episode_familywise_upper_bps": episode_upper,
        "mean_daily_net_return_bps": float(arm_daily["net_return_bps"].mean()),
        "mean_daily_net_excess_return_bps": float(arm_daily["net_excess_return_bps"].mean()),
        "maximum_drawdown": arm_mdd,
        "baseline_maximum_drawdown": baseline_mdd,
        "cvar_5_bps": arm_cvar,
        "baseline_cvar_5_bps": baseline_cvar,
        "downside_deviation_bps": _downside_deviation(arm_daily["net_return_bps"]),
        "mean_cash_slot_count": float(arm_daily["cash_slot_count"].mean()),
        "mean_turnover_fraction": float(arm_daily["turnover_fraction"].mean()),
        "intervention_days": intervention_days,
        "intervention_fraction": intervention_days / max(1, len(by_day)),
        "take_days": take_days,
        "skip_days": skip_days,
        "skip_all_days": int(by_day["take_count"].eq(0).sum()),
        "regime_intervention_days": regime_support,
        "time_block_lift_mean_bps": block_means,
        "positive_time_block_count": sum(value > 0.0 for value in block_means),
        "late_half_lift_mean_bps": late_half,
        "support_sufficient": support_sufficient,
        "pre_run_mde_bps": request.pre_run_mde_bps,
        "pre_run_power_sufficient_for_5bps": request.pre_run_power_sufficient_for_5bps,
        "brier": float(calibration["brier"]),
        "base_rate_brier": float(calibration["base_rate_brier"]),
        **dsr,
        "eligible": not reasons,
        "reason_codes": reasons,
    }


def compute_pre_run_mde(
    baseline_daily: pd.DataFrame,
    *,
    block_length: int = 20,
    minimum_effect_bps: float = 5.0,
) -> dict[str, Any]:
    values = pd.to_numeric(baseline_daily["net_return_bps"], errors="coerce").dropna().to_numpy(float)
    if len(values) < block_length * 2 or not np.isfinite(values).all():
        _raise("baseline daily series cannot support pre-run MDE", "ADVISORY_SCORE_HMM_REQUEST_INVALID")
    centered = values - values.mean()
    variance = float(np.dot(centered, centered) / len(values))
    autocorrelation_sum = 0.0
    if variance > 0.0:
        for lag in range(1, min(block_length, len(values) - 1) + 1):
            rho = float(np.dot(centered[:-lag], centered[lag:]) / ((len(values) - lag) * variance))
            if rho > 0.0:
                autocorrelation_sum += rho
    effective = max(1.0, min(float(len(values)), len(values) / (1.0 + 2.0 * autocorrelation_sum)))
    standard_deviation = float(np.std(values, ddof=1))
    mde = (1.959963984540054 + 0.8416212335729143) * standard_deviation / math.sqrt(effective)
    return {
        "schema_version": "advisory_score_hmm_pre_run_mde_v1",
        "daily_row_count": len(values),
        "block_length": block_length,
        "effective_sample_size": effective,
        "baseline_standard_deviation_bps": standard_deviation,
        "minimum_economic_effect_bps": minimum_effect_bps,
        "two_sided_alpha": 0.05,
        "power": 0.80,
        "mde_bps": mde,
        "power_sufficient_for_5bps": mde <= minimum_effect_bps,
    }


def freeze_score_hmm_market_pit_snapshot(
    *,
    output_path: str | Path,
    connection_factory: Callable[[], Any] = get_conn,
) -> dict[str, Any]:
    """Freeze the wider PIT history needed only before the offline formal request."""

    try:
        context = connection_factory(autocommit=False, manage_transaction=True)
        with context as conn:
            if hasattr(conn, "set_session"):
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT universe_key, rule_version, scope, start_date, end_date,
                           status, dirty, source_fingerprint_sha256,
                           generated_at, updated_at
                      FROM market.stock_universe_pit_state
                     WHERE universe_key = %s
                    """,
                    (CANONICAL_PIT_UNIVERSE_KEY,),
                )
                columns = [item[0] for item in cursor.description]
                raw_state = cursor.fetchone()
                if raw_state is None:
                    _raise("canonical PIT state is missing", "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY")
                state = dict(zip(columns, raw_state, strict=True))
                _validate_market_pit_state(state)
                cursor.execute(
                    """
                    SELECT ts_code, eligible_start, eligible_end, entry_reason, exit_reason
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code, eligible_start, eligible_end
                    """,
                    (CANONICAL_PIT_UNIVERSE_KEY, SCORE_HMM_DATA_CUTOFF, SCORE_HMM_MARKET_HISTORY_START),
                )
                span_columns = [item[0] for item in cursor.description]
                rows = [dict(zip(span_columns, row, strict=True)) for row in cursor.fetchall()]
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        _raise(
            "score/HMM market PIT snapshot could not be read",
            "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY",
            error_type=type(exc).__name__,
        )
    state_identity = canonical_json_sha256(
        {
            key: _json_scalar(state[key])
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
        scope_start=SCORE_HMM_MARKET_HISTORY_START,
        cutoff=SCORE_HMM_DATA_CUTOFF,
        state_identity=state_identity,
        source_fingerprint_sha256=str(state["source_fingerprint_sha256"]),
        parameter_hash=canonical_rule_parameters_digest(),
        state_status=str(state["status"]),
        state_dirty=bool(state["dirty"]),
        state_start=_to_date(state["start_date"]),
        state_end=_to_date(state["end_date"]),
    )
    output = _resolve_bound_path(output_path)
    if output.exists():
        try:
            existing = frozen_pit_snapshot_from_mapping(_read_json(output, "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY"))
        except AdvisoryModelFirstError:
            raise
        if existing.canonical_bytes() != snapshot.canonical_bytes():
            _raise(
                "existing market PIT snapshot differs from current canonical state",
                "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY",
            )
        status = "EXACT_NOOP"
    else:
        write_frozen_pit_snapshot(output, snapshot)
        status = "WRITTEN"
    return {
        "status": status,
        "path": output.as_posix(),
        "sha256": sha256_file(output),
        "spans_sha256": snapshot.spans_sha256,
        "span_count": len(snapshot.spans),
        "instrument_count": snapshot.unique_instruments,
        "scope_start": snapshot.scope_start.isoformat(),
        "cutoff": snapshot.cutoff.isoformat(),
        "database_access": "READ_ONLY_REPEATABLE_READ",
        "database_write": False,
    }


def _validate_market_pit_state(state: Mapping[str, Any]) -> None:
    expected = {
        "universe_key": CANONICAL_PIT_UNIVERSE_KEY,
        "rule_version": CANONICAL_PIT_RULE_VERSION,
        "scope": CANONICAL_PIT_SCOPE,
        "status": "ready",
        "dirty": False,
    }
    mismatches = {key: {"expected": value, "actual": state.get(key)} for key, value in expected.items() if state.get(key) != value}
    try:
        state_start = _to_date(state["start_date"])
        state_end = _to_date(state["end_date"])
    except (KeyError, TypeError, ValueError):
        mismatches["coverage"] = "invalid"
    else:
        if state_start > SCORE_HMM_MARKET_HISTORY_START or state_end < SCORE_HMM_DATA_CUTOFF:
            mismatches["coverage"] = {
                "required_start": SCORE_HMM_MARKET_HISTORY_START.isoformat(),
                "required_end": SCORE_HMM_DATA_CUTOFF.isoformat(),
                "actual_start": state_start.isoformat(),
                "actual_end": state_end.isoformat(),
            }
    fingerprint = str(state.get("source_fingerprint_sha256") or "")
    if len(fingerprint) != 64 or any(value not in "0123456789abcdef" for value in fingerprint):
        mismatches["source_fingerprint_sha256"] = "invalid"
    if mismatches:
        _raise(
            "canonical PIT state is not ready for score/HMM market warm-up",
            "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY",
            mismatches=mismatches,
        )


def _validate_market_pit_snapshot(*, market_snapshot: FrozenPitSnapshot, n1_snapshot: FrozenPitSnapshot) -> None:
    clipped = canonicalize_pit_spans(
        market_snapshot.to_frame(),
        scope_start=n1_snapshot.scope_start,
        cutoff=n1_snapshot.cutoff,
    )
    if (
        market_snapshot.universe_key != CANONICAL_PIT_UNIVERSE_KEY
        or market_snapshot.rule_version != CANONICAL_PIT_RULE_VERSION
        or market_snapshot.scope_start != SCORE_HMM_MARKET_HISTORY_START
        or market_snapshot.cutoff != SCORE_HMM_DATA_CUTOFF
        or market_snapshot.parameter_hash != n1_snapshot.parameter_hash
        or _pit_membership_projection_sha256(clipped)
        != _pit_membership_projection_sha256(n1_snapshot.to_frame())
    ):
        _raise(
            "market warm-up PIT snapshot does not extend the exact N1 PIT lineage",
            "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY",
        )


def _pit_membership_projection_sha256(spans: pd.DataFrame) -> str:
    frame = canonicalize_pit_spans(spans)
    rows = [
        [row.ts_code, row.eligible_start.isoformat(), row.eligible_end.isoformat()]
        for row in frame.itertuples(index=False)
    ]
    return canonical_json_sha256(rows)


def prepare_score_hmm_admission_request(
    *,
    n1_bundle_path: str | Path,
    market_warmup_pit_snapshot_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path,
    output_path: str | Path,
    auxiliary_route_path: str | Path | None = None,
) -> FrozenAdvisoryScoreHMMAdmissionRequestV1:
    """Freeze the single score/raw-market/market-HMM auxiliary request."""

    _require_formal_environment()
    n1_path = _resolve_bound_path(n1_bundle_path)
    repository = _resolve_bound_path(repository_root)
    output = _resolve_bound_path(output_root)
    n1 = _read_n1_bundle(n1_path)
    n1_request: AdvisoryN1Tier1RequestV1 = n1["request"]
    policy_path = _resolve_bound_path(n1_request.policy_dataset_bundle_root)
    policy_manifest = load_policy_dataset_bundle(
        policy_path,
        expected_bundle_id=n1_request.policy_dataset_bundle_id,
    )
    policy_request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
        (policy_path / "request.json").read_text(encoding="utf-8")
    )
    _validate_parent_source_relation(
        n1_path=n1_path,
        n1_manifest=n1["manifest"],
        n1_request=n1_request,
        policy_path=policy_path,
        policy_manifest=policy_manifest,
        policy_request=policy_request,
    )
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise(
            "score/HMM request requires a clean repository",
            "ADVISORY_SCORE_HMM_REQUEST_INVALID",
            dirty_paths=dirty[:20],
        )
    commit = _cross_os_git_commit(repository)
    origin_main = _git_origin_main_commit(repository)
    if commit != origin_main:
        _raise(
            "score/HMM request requires HEAD to equal origin/main",
            "ADVISORY_SCORE_HMM_REQUEST_INVALID",
            repository_commit=commit,
            origin_main_commit=origin_main,
        )
    calendar = _load_and_verify_calendar(n1_request)
    calendar_dates = tuple(value.date() for value in calendar)
    calendar_sha = canonical_json_sha256({"market_sessions": [value.isoformat() for value in calendar_dates]})
    factor_schema = validate_factor_file_schemas(
        _resolve_bound_path(n1_request.factor_data_root),
        data_cutoff=n1_request.factor_data_cutoff.isoformat(),
    )
    factor_schema_identity = _factor_schema_identity(factor_schema)
    suspend_path = _resolve_bound_path(n1_request.suspend_data_root) / "suspend_d.parquet"
    suspend_row_count = _parquet_row_count(suspend_path)
    if (
        sha256_file(suspend_path) != n1_request.suspend_sidecar_identity.sha256
        or suspend_row_count != n1_request.suspend_sidecar_identity.row_count
    ):
        _raise("score/HMM suspend sidecar identity drift", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    pit_path = _resolve_bound_path(n1_request.pit_snapshot.artifact_ref.artifact_uri)
    pit_snapshot = frozen_pit_snapshot_from_mapping(_read_json(pit_path, "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH"))
    market_pit_path = _resolve_bound_path(market_warmup_pit_snapshot_path)
    market_pit_snapshot = frozen_pit_snapshot_from_mapping(
        _read_json(market_pit_path, "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY")
    )
    _validate_market_pit_snapshot(market_snapshot=market_pit_snapshot, n1_snapshot=pit_snapshot)
    registry_path = _resolve_bound_path(n1_request.registry_path)
    current_route_path = _resolve_bound_path(n1_request.route_path)
    auxiliary_path = (
        _resolve_bound_path(auxiliary_route_path)
        if auxiliary_route_path is not None
        else current_route_path.with_name("current_auxiliary_route.md")
    )
    registry = AdvisoryResearchTrialRegistryV1(registry_path)
    registry_records = registry.read()
    registry_sha = sha256_file(registry_path)
    route = _parse_current_route(current_route_path)
    if route["next_task"] != "N3_SCORE_HMM_ADMISSION_MVE_IMPLEMENTATION":
        _raise(
            "main research route does not authorize score/HMM implementation",
            "ADVISORY_SCORE_HMM_ROUTE_INVALID",
            next_task=route["next_task"],
        )
    prior_candidate_index = int(route["cumulative_candidate_index"])
    reserved_indices = tuple(range(prior_candidate_index + 1, prior_candidate_index + 6))
    baseline_daily = pd.read_parquet(policy_path / "shadow_selection_daily.parquet")
    mde = compute_pre_run_mde(baseline_daily)
    evidence_paths = (
        ("score_hmm_n1_manifest", n1_path / "manifest.json"),
        ("score_hmm_n1_request", n1_path / "request.json"),
        ("score_hmm_n1_rankings", n1_path / "candidate_rankings_top50.parquet"),
        ("score_hmm_n1_cpcv", n1_path / "n1_label_interval_cpcv.json"),
        ("score_hmm_n1_regime_daily", n1_path / "learnability_daily.parquet"),
        ("score_hmm_policy_manifest", policy_path / "manifest.json"),
        ("score_hmm_policy_request", policy_path / "request.json"),
        ("score_hmm_policy_labels", policy_path / "candidate_episode_labels.parquet"),
        ("score_hmm_policy_rankings", policy_path / "candidate_rankings.parquet"),
        ("score_hmm_policy_shadow_daily", policy_path / "shadow_selection_daily.parquet"),
        ("score_hmm_policy_shadow_episodes", policy_path / "shadow_selection_episodes.parquet"),
        ("score_hmm_policy_baseline", policy_path / "baseline_policy.json"),
        ("score_hmm_policy_shadow", policy_path / "shadow_policy.json"),
        ("score_hmm_policy_cost", policy_path / "cost_policy.json"),
        ("score_hmm_pit_snapshot", pit_path),
        ("score_hmm_market_warmup_pit_snapshot", market_pit_path),
        ("score_hmm_trial_registry", registry_path),
        ("score_hmm_main_route", current_route_path),
    )
    evidence_refs = tuple(evidence_reference_for_file(path, role=role) for role, path in evidence_paths)
    if tuple(item.role for item in evidence_refs) != SCORE_HMM_EVIDENCE_ROLES:
        _raise("score/HMM evidence role order drift", "ADVISORY_SCORE_HMM_REQUEST_INVALID")
    policy_identity = str(n1["manifest"]["policy_identity"])
    dataset_identity = canonical_json_sha256(
        {
            "n1_bundle_id": n1_path.name,
            "policy_bundle_id": policy_path.name,
            "pit_spans_sha256": pit_snapshot.spans_sha256,
            "market_warmup_pit_spans_sha256": market_pit_snapshot.spans_sha256,
            "market_calendar_sha256": n1_request.market_calendar_identity.sha256,
            "suspend_sidecar_sha256": n1_request.suspend_sidecar_identity.sha256,
            "factor_schema_identity": factor_schema_identity,
            "arm_schema_hashes": SCORE_HMM_ARM_SCHEMA_HASHES,
            "policy_identity": policy_identity,
            "evidence_refs": [item.model_dump(mode="json") for item in evidence_refs],
        }
    )
    request = build_score_hmm_request(
        reserved_candidate_indices=reserved_indices,
        program_id=policy_request.program_id,
        binding_version_id=policy_request.binding_version_id,
        package_id=policy_request.package_id,
        manifest_sha256=policy_request.manifest_sha256,
        style_profile_id=policy_request.style_profile_id,
        style_profile_hash=policy_request.style_profile_hash,
        package_asset_closure_hash=policy_request.package_asset_closure_hash,
        selection_runtime_semantics_id=policy_request.selection_runtime_semantics_id,
        selection_runtime_semantics_hash=policy_request.selection_runtime_semantics_hash,
        terminal_weights=dict(policy_request.terminal_weights),
        representative_model_asset_sha256=dict(policy_request.representative_model_asset_sha256),
        n1_bundle_path=n1_path.as_posix(),
        n1_bundle_id=n1_path.name,
        n1_request_sha256=n1_request.request_sha256,
        n1_manifest_file_sha256=sha256_file(n1_path / "manifest.json"),
        n1_rankings_sha256=sha256_file(n1_path / "candidate_rankings_top50.parquet"),
        n1_cpcv_sha256=sha256_file(n1_path / "n1_label_interval_cpcv.json"),
        n1_regime_daily_sha256=sha256_file(n1_path / "learnability_daily.parquet"),
        policy_bundle_path=policy_path.as_posix(),
        policy_bundle_id=policy_path.name,
        policy_request_sha256=policy_request.request_sha256,
        policy_manifest_file_sha256=sha256_file(policy_path / "manifest.json"),
        policy_labels_sha256=sha256_file(policy_path / "candidate_episode_labels.parquet"),
        policy_rankings_sha256=sha256_file(policy_path / "candidate_rankings.parquet"),
        baseline_policy_sha256=policy_request.baseline_policy_sha256,
        shadow_policy_sha256=policy_request.shadow_policy_sha256,
        cost_policy_sha256=policy_request.cost_policy_sha256,
        split_policy_sha256=policy_request.split_policy_sha256,
        policy_identity=policy_identity,
        pit_snapshot_path=pit_path.as_posix(),
        pit_snapshot_file_sha256=sha256_file(pit_path),
        pit_spans_sha256=pit_snapshot.spans_sha256,
        market_warmup_pit_snapshot_path=market_pit_path.as_posix(),
        market_warmup_pit_snapshot_file_sha256=sha256_file(market_pit_path),
        market_warmup_pit_spans_sha256=market_pit_snapshot.spans_sha256,
        qlib_daily_root=n1_request.qlib_daily_root,
        factor_data_root=n1_request.factor_data_root,
        suspend_data_root=n1_request.suspend_data_root,
        market_calendar_sha256=n1_request.market_calendar_identity.sha256,
        market_calendar_row_count=n1_request.market_calendar_identity.row_count,
        suspend_sidecar_sha256=n1_request.suspend_sidecar_identity.sha256,
        suspend_sidecar_row_count=n1_request.suspend_sidecar_identity.row_count,
        factor_schema_identity=factor_schema_identity,
        trading_calendar=calendar_dates,
        trading_calendar_sha256=calendar_sha,
        pre_run_effective_sample_size=mde["effective_sample_size"],
        pre_run_mde_bps=mde["mde_bps"],
        pre_run_power_sufficient_for_5bps=mde["power_sufficient_for_5bps"],
        registry_path=registry_path.as_posix(),
        registry_sha256_at_request=registry_sha,
        registry_record_count_at_request=len(registry_records),
        cumulative_evaluated_trial_count_prior=sum(item.evaluated_trial_count for item in registry_records),
        current_route_path=current_route_path.as_posix(),
        current_route_sha256=sha256_file(current_route_path),
        current_route_next_task=route["next_task"],
        cumulative_candidate_index_prior=prior_candidate_index,
        auxiliary_route_path=auxiliary_path.as_posix(),
        evidence_refs=evidence_refs,
        dataset_identity=dataset_identity,
        repository_root=repository.as_posix(),
        repository_commit=commit,
        output_root=output.as_posix(),
    )
    _write_immutable_request(_resolve_bound_path(output_path), request)
    return request


def run_score_hmm_admission_mve(request_path: str | Path) -> dict[str, Any]:
    """Run and atomically deliver the frozen development-window MVE."""

    _require_formal_environment()
    started = time.monotonic()
    path = _resolve_bound_path(request_path)
    try:
        request = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValidationError, ValueError) as exc:
        _raise(
            "score/HMM frozen request cannot be read",
            "ADVISORY_SCORE_HMM_REQUEST_INVALID",
            path=str(path),
            error_type=type(exc).__name__,
        )
    existing = _find_existing_bundle(request)
    if existing is not None:
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return _run_response(request=request, bundle_path=existing, delivery=delivery, elapsed_seconds=0.0)
    _verify_run_environment(request)
    stages: list[dict[str, Any]] = []

    stage_started = time.monotonic()
    sources = _load_verified_sources(request)
    stages.append(_stage("source_preflight", stage_started, n1_rows=len(sources["rankings_top50"])))

    stage_started = time.monotonic()
    score_features = build_package_score_features(sources["rankings_top50"])
    stages.append(_stage("score_transforms", stage_started, row_count=len(score_features)))

    stage_started = time.monotonic()
    secondary = build_multi_horizon_outcome_labels(
        candidates=score_features[
            ["decision_as_of_trade_date", "target_trade_date", "instrument"]
        ],
        daily=sources["candidate_daily"],
        benchmark_daily=sources["benchmark_daily"],
        suspend_rows=sources["suspend_rows"],
        trading_calendar=sources["trading_calendar"],
    )
    targets = build_score_hmm_targets(
        score_features=score_features,
        primary_labels=sources["primary_labels"],
        secondary_labels=secondary.labels,
    )
    label_interval_isolation = validate_score_hmm_label_interval_isolation(
        primary_labels=sources["primary_labels"],
        secondary_labels=secondary.labels,
        cpcv_payload=sources["cpcv_payload"],
    )
    stages.append(_stage("targets", stage_started, row_count=len(targets.panel)))

    stage_started = time.monotonic()
    raw_market = build_raw_market_shape(
        market_daily=sources["market_daily"],
        benchmark_daily=sources["benchmark_daily"],
        suspend_rows=sources["suspend_rows"],
        pit_snapshot=sources["market_pit_snapshot"],
        calendar=sources["trading_calendar"],
    )
    stages.append(
        _stage(
            "raw_market",
            stage_started,
            available_days=int(raw_market.coverage["status"].eq("AVAILABLE").sum()),
        )
    )

    stage_started = time.monotonic()
    hmm_result = build_fold_local_market_hmm(
        raw_market_features=raw_market.features,
        cpcv_payload=sources["cpcv_payload"],
        trading_calendar=sources["trading_calendar"],
        expected_path_count=request.expected_ready_path_count,
        warmup_days=request.hmm_warmup_trading_days,
    )
    parent_context = _build_parent_context_exposure(sources["policy_request"], request=request)
    stages.append(_stage("fold_local_market_hmm", stage_started, state_rows=len(hmm_result.states)))

    stage_started = time.monotonic()
    crossfit = run_score_hmm_crossfit(
        target_panel=targets.panel,
        raw_market_features=raw_market.features,
        cpcv_payload=sources["cpcv_payload"],
        hmm_result=hmm_result,
        label_interval_isolation=label_interval_isolation,
        request=request,
    )
    calibration = build_calibration_metrics(crossfit.oof_predictions)
    decisions = build_admission_decisions(
        oof_predictions=crossfit.oof_predictions,
        parent_top20=targets.panel,
        request=request,
    )
    stages.append(
        _stage(
            "crossfit_and_admission",
            stage_started,
            oof_rows=len(crossfit.oof_predictions),
            decision_rows=len(decisions),
        )
    )

    stage_started = time.monotonic()
    evaluation = evaluate_admission_policies(
        decisions=decisions,
        policy_rankings=sources["policy_rankings"],
        market_daily=sources["candidate_daily"],
        benchmark_daily=sources["benchmark_daily"],
        suspend_rows=sources["suspend_rows"],
        trading_calendar=sources["trading_calendar"],
        policy_request=sources["policy_request"],
        stored_baseline_daily=sources["stored_baseline_daily"],
        stored_baseline_episodes=sources["stored_baseline_episodes"],
        regime_daily=sources["regime_daily"],
        calibration_metrics=calibration,
        parent_context_exposure=parent_context,
        request=request,
    )
    stages.append(_stage("shared_policy_evaluation", stage_started, selected_arm=evaluation.selected_arm_id))
    source_preflight = _build_source_preflight(
        request=request,
        sources=sources,
        raw_market=raw_market,
        score_features=score_features,
    )
    feature_schema = _build_feature_schema_receipt(request)
    total_elapsed = time.monotonic() - started
    resource_report = {
        "schema_version": "advisory_score_hmm_resource_report_v1",
        "request_sha256": request.request_sha256,
        "stages": stages,
        "total_wall_seconds": total_elapsed,
        "peak_rss_bytes": _peak_rss_bytes(),
        "max_rss_bytes": request.resource_max_rss_bytes,
        "max_temp_bytes": request.resource_max_temp_bytes,
        "wall_time_limit_seconds": None,
        "database_reads": 0,
        "database_writes": 0,
        "network_reads": 0,
        "tushare_reads": 0,
        "sealed_holdout_accessed": False,
    }
    if resource_report["peak_rss_bytes"] > request.resource_max_rss_bytes:
        _raise("score/HMM RSS limit exceeded", "ADVISORY_SCORE_HMM_RESOURCE_LIMIT")
    bundle_path = _publish_bundle(
        request=request,
        source_preflight=source_preflight,
        parent_context_exposure=parent_context,
        feature_schema_by_arm=feature_schema,
        aligned_parent_rankings_top50=sources["policy_rankings"],
        primary_policy_labels=sources["primary_labels"],
        target_coverage=targets.coverage,
        fold_receipts=crossfit.fold_receipts,
        oof_predictions=crossfit.oof_predictions,
        calibration_metrics=calibration,
        admission_decisions=decisions,
        policy_daily=evaluation.policy_daily,
        policy_episodes=evaluation.policy_episodes,
        arm_summary=evaluation.arm_summary,
        evaluation=evaluation,
        resource_report=resource_report,
    )
    delivery = _deliver_bundle(request=request, bundle_path=bundle_path)
    gc.collect()
    return _run_response(
        request=request,
        bundle_path=bundle_path,
        delivery=delivery,
        elapsed_seconds=time.monotonic() - started,
    )


def inspect_score_hmm_admission_bundle(bundle_path: str | Path) -> dict[str, Any]:
    loaded = _read_score_hmm_bundle(_resolve_bound_path(bundle_path))
    receipt: ScoreHMMAdmissionFrontierReceiptV1 = loaded["frontier"]
    return {
        "status": "VALID",
        "bundle_id": loaded["manifest"]["bundle_id"],
        "request_sha256": loaded["request"].request_sha256,
        "selected_arm_id": receipt.selected_arm_id,
        "selected_arm_count": receipt.selected_trial_count,
        "generated_trial_count": receipt.generated_trial_count,
        "evaluated_trial_count": receipt.evaluated_trial_count,
        "eligible_arm_ids": list(receipt.eligible_arm_ids),
        "evidence_class": receipt.evidence_class,
        "next_task": receipt.next_task,
        "source_unavailable_arm_ids": list(receipt.source_unavailable_arm_ids),
        "sealed_holdout_accessed": False,
        "deployable": False,
        "runtime_activation": False,
    }


def _load_verified_sources(request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> dict[str, Any]:
    n1_path = _resolve_bound_path(request.n1_bundle_path)
    policy_path = _resolve_bound_path(request.policy_bundle_path)
    n1 = _read_n1_bundle(n1_path)
    policy_manifest = load_policy_dataset_bundle(policy_path, expected_bundle_id=request.policy_bundle_id)
    policy_request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
        (policy_path / "request.json").read_text(encoding="utf-8")
    )
    _validate_parent_source_relation(
        n1_path=n1_path,
        n1_manifest=n1["manifest"],
        n1_request=n1["request"],
        policy_path=policy_path,
        policy_manifest=policy_manifest,
        policy_request=policy_request,
    )
    for reference in request.evidence_refs:
        _verify_evidence_ref(reference)
    expected_hashes = {
        "n1_manifest": (n1_path / "manifest.json", request.n1_manifest_file_sha256),
        "n1_rankings": (n1_path / "candidate_rankings_top50.parquet", request.n1_rankings_sha256),
        "n1_cpcv": (n1_path / "n1_label_interval_cpcv.json", request.n1_cpcv_sha256),
        "n1_regime": (n1_path / "learnability_daily.parquet", request.n1_regime_daily_sha256),
        "policy_manifest": (policy_path / "manifest.json", request.policy_manifest_file_sha256),
        "policy_labels": (policy_path / "candidate_episode_labels.parquet", request.policy_labels_sha256),
        "policy_rankings": (policy_path / "candidate_rankings.parquet", request.policy_rankings_sha256),
    }
    drift = [name for name, (path, expected) in expected_hashes.items() if sha256_file(path) != expected]
    if drift:
        _raise(
            "score/HMM bound source file changed after request freeze",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            drift=drift,
        )
    calendar = _load_and_verify_calendar(n1["request"])
    if tuple(value.date() for value in calendar) != request.trading_calendar:
        _raise("score/HMM trading calendar changed", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    factor_schema = validate_factor_file_schemas(
        _resolve_bound_path(request.factor_data_root),
        data_cutoff=request.data_cutoff.isoformat(),
    )
    if _factor_schema_identity(factor_schema) != request.factor_schema_identity:
        _raise("score/HMM factor schema identity changed", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    pit_path = _resolve_bound_path(request.pit_snapshot_path)
    pit_snapshot = frozen_pit_snapshot_from_mapping(_read_json(pit_path, "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH"))
    if sha256_file(pit_path) != request.pit_snapshot_file_sha256 or pit_snapshot.spans_sha256 != request.pit_spans_sha256:
        _raise("score/HMM PIT snapshot identity changed", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    market_pit_path = _resolve_bound_path(request.market_warmup_pit_snapshot_path)
    market_pit_snapshot = frozen_pit_snapshot_from_mapping(
        _read_json(market_pit_path, "ADVISORY_SCORE_HMM_MARKET_PIT_NOT_READY")
    )
    if (
        sha256_file(market_pit_path) != request.market_warmup_pit_snapshot_file_sha256
        or market_pit_snapshot.spans_sha256 != request.market_warmup_pit_spans_sha256
    ):
        _raise("score/HMM market PIT snapshot identity changed", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    _validate_market_pit_snapshot(market_snapshot=market_pit_snapshot, n1_snapshot=pit_snapshot)
    rankings_top50 = pd.read_parquet(n1_path / "candidate_rankings_top50.parquet")
    regime_daily = pd.read_parquet(n1_path / "learnability_daily.parquet")
    cpcv_payload = _read_json(n1_path / "n1_label_interval_cpcv.json", "ADVISORY_SCORE_HMM_CROSSFIT_INVALID")
    candidate_dates = pd.DatetimeIndex(
        pd.to_datetime(rankings_top50["decision_as_of_trade_date"])
    ).normalize().sort_values().unique()
    context_dates = calendar[
        (calendar >= pd.Timestamp(request.decision_start)) & (calendar < pd.Timestamp(request.data_cutoff))
    ]
    prediction_source = ExactPredictionSource(n1["request"].prediction_store_root)
    descriptors = prediction_source.describe_all(n1["request"].representative_seed_run_ids.values())
    descriptor_mismatches = {
        run_id: {
            "expected": n1["request"].prediction_artifacts[run_id].model_dump(mode="json"),
            "actual": descriptor.model_dump(mode="json"),
        }
        for run_id, descriptor in descriptors.items()
        if descriptor.model_dump(mode="json")
        != n1["request"].prediction_artifacts[run_id].model_dump(mode="json")
    }
    if descriptor_mismatches:
        _raise(
            "score/HMM Prediction Store descriptors changed",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            run_ids=sorted(descriptor_mismatches),
        )
    leg_frames = {
        leg_id: filter_prediction_frame_to_pit(
            prediction_source.load_scores(run_id, decision_dates=context_dates, verify_artifact=False),
            pit_snapshot,
        )
        for leg_id, run_id in n1["request"].representative_seed_run_ids.items()
    }
    aligned_rank_result = build_policy_rankings(
        leg_frames=leg_frames,
        terminal_weights=request.terminal_weights,
        decision_dates=context_dates,
        trading_calendar=calendar,
        identity={
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
        },
        required_depth=request.score_distribution_depth,
    )
    policy_rankings = aligned_rank_result.rankings
    policy_rankings["is_candidate_decision"] = policy_rankings["decision_as_of_trade_date"].isin(candidate_dates)
    _assert_parent_ranking_parity(
        expected=rankings_top50,
        actual=policy_rankings.loc[policy_rankings["is_candidate_decision"]],
    )
    del leg_frames
    gc.collect()
    symbols = sorted(policy_rankings["instrument"].astype(str).str.upper().unique())
    market_pit_symbols = sorted({span.ts_code for span in market_pit_snapshot.spans})
    candidate_daily = load_qlib_daily(
        symbols,
        start=request.decision_start.isoformat(),
        end=request.data_cutoff.isoformat(),
    )
    benchmark_daily = load_qlib_daily(
        [policy_request.cost_policy.benchmark_instrument],
        start="2023-09-01",
        end=request.data_cutoff.isoformat(),
        fields=("$open", "$close"),
    )
    market_daily = load_qlib_daily(
        market_pit_symbols,
        start="2023-09-01",
        end=request.data_cutoff.isoformat(),
        fields=("$close", "$prev_close", "$volume", "$amount", "$limit_up"),
    )
    suspend_rows = load_suspend_rows(
        _resolve_bound_path(request.suspend_data_root),
        start="2023-09-01",
        end=request.data_cutoff.isoformat(),
        instruments=sorted(set(market_pit_symbols) | set(symbols)),
    )
    policy = transition_policy_from_payload(policy_request.shadow_policy)
    label_result = build_policy_episode_labels(
        rankings=policy_rankings,
        daily=candidate_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=policy_request.shadow_policy_sha256,
        cost_policy=policy_request.cost_policy,
        request_identity={
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
        },
        candidate_decision_dates=candidate_dates,
        candidate_depth=request.model_candidate_depth,
        rank_depth=request.score_distribution_depth,
    )
    aligned_baseline = replay_shadow_portfolio(
        rankings=policy_rankings,
        daily=candidate_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        trading_calendar=calendar,
        policy=policy,
        policy_sha256=policy_request.shadow_policy_sha256,
        cost_policy=policy_request.cost_policy,
        request_id=request.request_id,
        rank_depth=request.score_distribution_depth,
        candidate_decision_dates=candidate_dates,
    )
    legacy_labels = pd.read_parquet(policy_path / "candidate_episode_labels.parquet")
    legacy_overlap = _target_key_overlap(rankings_top50, legacy_labels, depth=request.model_candidate_depth)
    return {
        "n1": n1,
        "n1_path": n1_path,
        "policy_path": policy_path,
        "policy_manifest": policy_manifest,
        "policy_request": policy_request,
        "pit_snapshot": pit_snapshot,
        "market_pit_snapshot": market_pit_snapshot,
        "rankings_top50": rankings_top50,
        "primary_labels": label_result.labels,
        "policy_rankings": policy_rankings,
        "stored_baseline_daily": aligned_baseline.daily,
        "stored_baseline_episodes": aligned_baseline.episodes,
        "aligned_rank_coverage": aligned_rank_result.coverage,
        "aligned_label_coverage": label_result.coverage,
        "prediction_descriptors": {
            run_id: descriptor.model_dump(mode="json") for run_id, descriptor in sorted(descriptors.items())
        },
        "legacy_policy_label_overlap": legacy_overlap,
        "regime_daily": regime_daily,
        "cpcv_payload": cpcv_payload,
        "trading_calendar": calendar,
        "candidate_daily": candidate_daily,
        "benchmark_daily": benchmark_daily,
        "market_daily": market_daily,
        "suspend_rows": suspend_rows,
    }


def _assert_parent_ranking_parity(*, expected: pd.DataFrame, actual: pd.DataFrame) -> None:
    key_columns = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "instrument",
        "selection_effective_rank",
    ]
    numeric_columns = [
        "combined_score",
        f"raw__{LSTM_LEG_ID}",
        f"norm__{LSTM_LEG_ID}",
        f"rank__{LSTM_LEG_ID}",
        f"weight__{LSTM_LEG_ID}",
        f"raw__{FUND_LEG_ID}",
        f"norm__{FUND_LEG_ID}",
        f"rank__{FUND_LEG_ID}",
        f"weight__{FUND_LEG_ID}",
    ]
    required = set(key_columns + numeric_columns)
    if not required.issubset(expected.columns) or not required.issubset(actual.columns):
        _raise(
            "aligned PIT parent ranking schema is incomplete",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            expected_missing=sorted(required - set(expected.columns)),
            actual_missing=sorted(required - set(actual.columns)),
        )
    left = expected[key_columns + numeric_columns].copy()
    right = actual[key_columns + numeric_columns].copy()
    for frame in (left, right):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
        frame["target_trade_date"] = pd.to_datetime(frame["target_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
        frame.sort_values(key_columns, inplace=True)
        frame.reset_index(drop=True, inplace=True)
    key_match = len(left) == len(right) and left[key_columns].equals(right[key_columns])
    numeric_match = key_match and np.allclose(
        left[numeric_columns].to_numpy(float),
        right[numeric_columns].to_numpy(float),
        rtol=1e-12,
        atol=1e-12,
        equal_nan=False,
    )
    if not numeric_match:
        _raise(
            "extended PIT Top50 does not reproduce the frozen N1 candidate projection",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            expected_rows=len(left),
            actual_rows=len(right),
            key_match=key_match,
        )


def _target_key_overlap(parent_rankings: pd.DataFrame, labels: pd.DataFrame, *, depth: int) -> dict[str, Any]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    left = parent_rankings.loc[parent_rankings["selection_effective_rank"].le(depth), keys].copy()
    right = labels[keys].copy()
    for frame in (left, right):
        frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
        frame["target_trade_date"] = pd.to_datetime(frame["target_trade_date"]).dt.normalize()
        frame["instrument"] = frame["instrument"].astype(str).str.upper()
    left_keys = set(map(tuple, left.itertuples(index=False, name=None)))
    right_keys = set(map(tuple, right.itertuples(index=False, name=None)))
    return {
        "legacy_policy_rank_semantics": "advisory_exact_weighted_top40_v1",
        "aligned_parent_rank_semantics": "advisory_exact_weighted_pit_top50_v1",
        "matched_key_count": len(left_keys & right_keys),
        "aligned_only_key_count": len(left_keys - right_keys),
        "legacy_only_key_count": len(right_keys - left_keys),
        "legacy_labels_used_as_target": False,
    }


def _validate_parent_source_relation(
    *,
    n1_path: Path,
    n1_manifest: Mapping[str, Any],
    n1_request: AdvisoryN1Tier1RequestV1,
    policy_path: Path,
    policy_manifest: Mapping[str, Any],
    policy_request: FrozenAdvisoryPolicyDatasetRequestV1,
) -> None:
    mismatches: dict[str, Any] = {}
    if n1_path.name != str(n1_manifest.get("bundle_id")):
        mismatches["n1_bundle_id"] = n1_manifest.get("bundle_id")
    if policy_path.name != n1_request.policy_dataset_bundle_id:
        mismatches["policy_bundle_path"] = policy_path.name
    if policy_path.name != str(policy_manifest.get("policy_dataset_bundle_id")):
        mismatches["policy_manifest_bundle_id"] = policy_manifest.get("policy_dataset_bundle_id")
    for field in (
        "program_id",
        "binding_version_id",
        "package_id",
        "manifest_sha256",
        "selection_runtime_semantics_hash",
        "shadow_policy_sha256",
        "cost_policy_sha256",
        "split_policy_sha256",
    ):
        left = getattr(n1_request, field)
        right = getattr(policy_request, field)
        if left != right:
            mismatches[field] = {"n1": left, "policy": right}
    if n1_manifest.get("dataset_identity") != policy_path.name:
        mismatches["dataset_identity"] = n1_manifest.get("dataset_identity")
    if mismatches:
        _raise(
            "score/HMM N1 and policy identities are not closed",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            mismatches=mismatches,
        )


def _build_parent_context_exposure(
    policy_request: FrozenAdvisoryPolicyDatasetRequestV1,
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> dict[str, Any]:
    semantics = dict(policy_request.selection_runtime_semantics)
    hmm_enabled = bool(semantics.get("hmm_enabled"))
    status = "UNATTRIBUTABLE_UNKNOWN_PARENT_HMM_LINEAGE" if hmm_enabled else "ATTRIBUTABLE_NO_EXPLICIT_PARENT_HMM_OUTPUT"
    payload = {
        "schema_version": "advisory_score_hmm_parent_context_exposure_v1",
        "request_sha256": request.request_sha256,
        "package_id": policy_request.package_id,
        "manifest_sha256": policy_request.manifest_sha256,
        "selection_runtime_semantics_hash": policy_request.selection_runtime_semantics_hash,
        "parent_component_ids": sorted(policy_request.terminal_weights),
        "parent_component_weights": dict(policy_request.terminal_weights),
        "parent_model_asset_sha256": dict(policy_request.representative_model_asset_sha256),
        "parent_hmm_enabled": hmm_enabled,
        "explicit_parent_hmm_output_identity": None,
        "raw_market_ancestor_overlap": "DISCLOSED_POSSIBLE_CONDITIONAL_INCREMENT_ONLY",
        "market_hmm_attribution_status": status,
        "sector_hmm_attribution_status": "NOT_RUN_SOURCE_UNAVAILABLE",
        "name_only_duplicate_detection_used": False,
    }
    payload["exposure_sha256"] = canonical_json_sha256(payload)
    return payload


def _build_source_preflight(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    sources: Mapping[str, Any],
    raw_market: RawMarketShapeResult,
    score_features: pd.DataFrame,
) -> dict[str, Any]:
    decision_dates = pd.DatetimeIndex(score_features["decision_as_of_trade_date"].unique())
    raw_coverage = raw_market.coverage.loc[
        raw_market.coverage["decision_as_of_trade_date"].isin(decision_dates)
    ]
    payload = {
        "schema_version": "advisory_score_hmm_source_preflight_v1",
        "request_sha256": request.request_sha256,
        "n1_bundle_id": request.n1_bundle_id,
        "policy_bundle_id": request.policy_bundle_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
        "top50_row_count": len(sources["rankings_top50"]),
        "top20_row_count": len(score_features),
        "top5_row_count": int(score_features["selection_effective_rank"].le(5).sum()),
        "decision_day_count": len(decision_dates),
        "aligned_rank_context_row_count": len(sources["policy_rankings"]),
        "aligned_rank_context_day_count": int(
            sources["policy_rankings"]["decision_as_of_trade_date"].nunique()
        ),
        "aligned_primary_label_row_count": len(sources["primary_labels"]),
        "aligned_primary_matured_count": int(sources["primary_labels"]["label_status"].eq("MATURED").sum()),
        "n1_candidate_projection_parity": True,
        "legacy_policy_label_overlap": dict(sources["legacy_policy_label_overlap"]),
        "prediction_descriptors": dict(sources["prediction_descriptors"]),
        "market_pit_membership_projection_matches_n1": True,
        "market_pit_source_fingerprint_matches_n1": (
            sources["market_pit_snapshot"].source_fingerprint_sha256
            == sources["pit_snapshot"].source_fingerprint_sha256
        ),
        "market_pit_reason_only_metadata_drift_allowed": True,
        "raw_market_available_decision_days": int(raw_coverage["status"].eq("AVAILABLE").sum()),
        "raw_market_unavailable_decision_days": int(raw_coverage["status"].ne("AVAILABLE").sum()),
        "pit_filter_receipt": raw_market.pit_filter_receipt,
        "sector_source_status": "NOT_AVAILABLE",
        "sector_arm_statuses": {
            SCORE_PLUS_SECTOR_HMM: "NOT_RUN_SOURCE_UNAVAILABLE",
            SCORE_PLUS_MARKET_AND_SECTOR_HMM: "NOT_RUN_SOURCE_UNAVAILABLE",
        },
        "database_reads": 0,
        "network_reads": 0,
        "tushare_reads": 0,
        "sealed_holdout_accessed": False,
    }
    if (
        payload["top50_row_count"] != request.expected_top50_row_count
        or payload["top20_row_count"] != request.expected_top20_row_count
        or payload["top5_row_count"] != request.expected_top5_row_count
        or payload["decision_day_count"] != request.expected_decision_day_count
    ):
        _raise("score/HMM source preflight row contract drift", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    payload["preflight_sha256"] = canonical_json_sha256(payload)
    return payload


def _build_feature_schema_receipt(request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> dict[str, Any]:
    return {
        "schema_version": "advisory_score_hmm_feature_schema_by_arm_v1",
        "request_sha256": request.request_sha256,
        "arms": [
            {
                "arm_id": spec.arm_id,
                "trial_candidate_index": spec.trial_candidate_index,
                "run_status": spec.run_status,
                "source_requirement": spec.source_requirement,
                "predecessor_arm_ids": list(spec.predecessor_arm_ids),
                "feature_columns": list(spec.feature_columns),
                "feature_schema_hash": spec.feature_schema_hash,
            }
            for spec in request.arm_specs
        ],
        "raw_cross_date_threshold_allowed": False,
        "parent_rank_change_allowed": False,
        "rank6_backfill_allowed": False,
        "dynamic_position_weight_allowed": False,
    }


def _publish_bundle(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    source_preflight: Mapping[str, Any],
    parent_context_exposure: Mapping[str, Any],
    feature_schema_by_arm: Mapping[str, Any],
    aligned_parent_rankings_top50: pd.DataFrame,
    primary_policy_labels: pd.DataFrame,
    target_coverage: pd.DataFrame,
    fold_receipts: Mapping[str, Any],
    oof_predictions: pd.DataFrame,
    calibration_metrics: pd.DataFrame,
    admission_decisions: pd.DataFrame,
    policy_daily: pd.DataFrame,
    policy_episodes: pd.DataFrame,
    arm_summary: Mapping[str, Any],
    evaluation: ScoreHMMEvaluationResult,
    resource_report: Mapping[str, Any],
) -> Path:
    root = _resolve_bound_path(request.output_root) / "score_hmm_admission_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{request.request_id}.", dir=root))
    try:
        _write_json(temporary / "source_preflight.json", source_preflight)
        _write_json(temporary / "parent_context_exposure.json", parent_context_exposure)
        _write_json(temporary / "feature_schema_by_arm.json", feature_schema_by_arm)
        _write_parquet(temporary / "aligned_parent_rankings_top50.parquet", aligned_parent_rankings_top50)
        _write_parquet(temporary / "primary_policy_labels.parquet", primary_policy_labels)
        _write_parquet(temporary / "target_coverage.parquet", target_coverage)
        _write_json(temporary / "hmm_fold_receipts.json", fold_receipts)
        _write_parquet(temporary / "oof_predictions.parquet", oof_predictions)
        _write_parquet(temporary / "calibration_metrics.parquet", calibration_metrics)
        _write_parquet(temporary / "admission_decisions.parquet", admission_decisions)
        _write_parquet(temporary / "policy_daily.parquet", policy_daily)
        _write_parquet(temporary / "policy_episodes.parquet", policy_episodes)
        _write_json(temporary / "arm_summary.json", arm_summary)
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "resource_report.json", resource_report)
        core_names = RESULT_IDENTITY_MEMBERS - {"frontier_receipt.json"}
        core_descriptors = {name: _file_descriptors(temporary)[name] for name in sorted(core_names)}
        core_result_sha = canonical_json_sha256(core_descriptors)
        arm_statuses = {
            arm_id: next(item["status"] for item in arm_summary["arms"] if item["arm_id"] == arm_id)
            for arm_id in SCORE_HMM_ARM_IDS
        }
        frontier = build_score_hmm_frontier_receipt(
            request_sha256=request.request_sha256,
            selected_trial_count=int(evaluation.selected_arm_id is not None),
            selected_arm_id=evaluation.selected_arm_id,
            eligible_arm_ids=evaluation.eligible_arm_ids,
            arm_statuses=arm_statuses,
            evidence_class=evaluation.evidence_class,
            next_task=(
                "N3_AUX_SCORE_HMM_ADMISSION_CONFIRMATION_DESIGN"
                if evaluation.selected_arm_id
                else "N3_AUX_SCORE_HMM_SOURCE_READINESS_REVIEW"
                if evaluation.evidence_class == "AUX_PARTIAL_SOURCE_UNAVAILABLE"
                else "N3_AUX_SCORE_HMM_EXECUTED_FRONTIER_CLOSED"
            ),
            result_files_sha256=core_result_sha,
            resource_report_sha256=sha256_file(temporary / "resource_report.json"),
        )
        _write_json(temporary / "frontier_receipt.json", frontier.model_dump(mode="json"))
        result_descriptors = {name: _file_descriptors(temporary)[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
        bundle_id = canonical_json_sha256(
            {
                "schema_version": SCORE_HMM_BUNDLE_SCHEMA,
                "request_sha256": request.request_sha256,
                "result_identity_files": result_descriptors,
            }
        )
        final = root / bundle_id
        records = _build_registry_records(
            request=request,
            frontier=frontier,
            frontier_source=temporary / "frontier_receipt.json",
            frontier_final=final / "frontier_receipt.json",
        )
        _write_json(temporary / "registry_records.json", [item.model_dump(mode="json") for item in records])
        files = _file_descriptors(temporary)
        bundle_member_bytes = sum(int(descriptor["size_bytes"]) for descriptor in files.values())
        if bundle_member_bytes > request.resource_max_temp_bytes:
            _raise("score/HMM temporary bundle limit exceeded", "ADVISORY_SCORE_HMM_RESOURCE_LIMIT")
        manifest = {
            "schema_version": SCORE_HMM_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "frontier_receipt_sha256": frontier.receipt_sha256,
            "result_files_sha256": core_result_sha,
            "result_identity_files": result_descriptors,
            "files": files,
            "bundle_member_bytes": bundle_member_bytes,
            "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY.value,
            "selected_arm_id": evaluation.selected_arm_id,
            "selected_arm_count": int(evaluation.selected_arm_id is not None),
            "generated_trial_count": frontier.generated_trial_count,
            "evaluated_trial_count": frontier.evaluated_trial_count,
            "source_unavailable_arm_ids": list(SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS),
            "sealed_holdout_accessed": False,
            "runtime_eligible": False,
            "activated": False,
        }
        _write_json(temporary / "manifest.json", manifest)
        if sum(item.stat().st_size for item in temporary.iterdir() if item.is_file()) > request.resource_max_temp_bytes:
            _raise("score/HMM temporary bundle limit exceeded", "ADVISORY_SCORE_HMM_RESOURCE_LIMIT")
        if final.exists():
            loaded = _read_score_hmm_bundle(final)
            if loaded["request"].request_sha256 != request.request_sha256:
                _raise("existing score/HMM bundle identity conflicts", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
            shutil.rmtree(temporary)
            return final
        temporary.replace(final)
        _read_score_hmm_bundle(final)
        return final
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def _read_score_hmm_bundle(path: Path) -> dict[str, Any]:
    try:
        manifest = _read_json(path / "manifest.json", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    except AdvisoryModelFirstError:
        raise
    expected_manifest_fields = {
        "schema_version",
        "bundle_id",
        "request_sha256",
        "frontier_receipt_sha256",
        "result_files_sha256",
        "result_identity_files",
        "files",
        "bundle_member_bytes",
        "objective_contract",
        "selected_arm_id",
        "selected_arm_count",
        "generated_trial_count",
        "evaluated_trial_count",
        "source_unavailable_arm_ids",
        "sealed_holdout_accessed",
        "runtime_eligible",
        "activated",
    }
    if (
        set(manifest) != expected_manifest_fields
        or manifest.get("schema_version") != SCORE_HMM_BUNDLE_SCHEMA
        or manifest.get("bundle_id") != path.name
    ):
        _raise("score/HMM manifest identity is invalid", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    files = manifest.get("files")
    if not isinstance(files, Mapping) or set(files) != SCORE_HMM_BUNDLE_MEMBERS:
        _raise("score/HMM bundle member roster is invalid", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    actual_names = {item.name for item in path.iterdir() if item.is_file()} - {"manifest.json"}
    if actual_names != SCORE_HMM_BUNDLE_MEMBERS:
        _raise(
            "score/HMM bundle is partial or contains extra files",
            "ADVISORY_SCORE_HMM_BUNDLE_INVALID",
            missing=sorted(SCORE_HMM_BUNDLE_MEMBERS - actual_names),
            extra=sorted(actual_names - SCORE_HMM_BUNDLE_MEMBERS),
        )
    for name, descriptor in files.items():
        member = path / name
        actual: dict[str, Any] = {"sha256": sha256_file(member), "size_bytes": member.stat().st_size}
        if member.suffix == ".parquet":
            actual["row_count"] = _parquet_row_count(member)
        if actual != descriptor:
            _raise("score/HMM bundle member identity drift", "ADVISORY_SCORE_HMM_BUNDLE_INVALID", member=name)
    result_identity_files = {name: files[name] for name in sorted(RESULT_IDENTITY_MEMBERS)}
    core_identity_files = {
        name: files[name] for name in sorted(RESULT_IDENTITY_MEMBERS - {"frontier_receipt.json"})
    }
    core_result_sha = canonical_json_sha256(core_identity_files)
    expected_bundle = canonical_json_sha256(
        {
            "schema_version": SCORE_HMM_BUNDLE_SCHEMA,
            "request_sha256": manifest.get("request_sha256"),
            "result_identity_files": result_identity_files,
        }
    )
    if expected_bundle != path.name or result_identity_files != manifest.get("result_identity_files"):
        _raise("score/HMM result identity is invalid", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    try:
        request = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        frontier = ScoreHMMAdmissionFrontierReceiptV1.model_validate_json(
            (path / "frontier_receipt.json").read_text(encoding="utf-8")
        )
        raw_records = _read_json(path / "registry_records.json", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
        records = tuple(AdvisoryResearchTrialRecordV1.model_validate(item) for item in raw_records)
        resource = _read_json(path / "resource_report.json", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    except (ValidationError, ValueError) as exc:
        _raise(
            "score/HMM bundle contract readback failed",
            "ADVISORY_SCORE_HMM_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    member_bytes = sum(int(descriptor["size_bytes"]) for descriptor in files.values())
    total_bundle_bytes = member_bytes + (path / "manifest.json").stat().st_size
    if (
        request.request_sha256 != manifest["request_sha256"]
        or frontier.receipt_sha256 != manifest["frontier_receipt_sha256"]
        or frontier.request_sha256 != request.request_sha256
        or frontier.result_files_sha256 != core_result_sha
        or frontier.resource_report_sha256 != sha256_file(path / "resource_report.json")
        or manifest["result_files_sha256"] != core_result_sha
        or manifest["bundle_member_bytes"] != member_bytes
        or manifest["objective_contract"] != ObjectiveContract.RISK_MANAGED_ADVISORY.value
        or manifest["selected_arm_id"] != frontier.selected_arm_id
        or manifest["selected_arm_count"] != frontier.selected_trial_count
        or manifest["generated_trial_count"] != frontier.generated_trial_count
        or manifest["evaluated_trial_count"] != frontier.evaluated_trial_count
        or tuple(manifest["source_unavailable_arm_ids"]) != SCORE_HMM_SOURCE_UNAVAILABLE_ARM_IDS
        or manifest["sealed_holdout_accessed"] is not False
        or manifest["runtime_eligible"] is not False
        or manifest["activated"] is not False
        or len(records) != 5
        or {item.experiment_id for item in records}
        != {f"{SCORE_HMM_EXPERIMENT_ID}:{arm_id}" for arm_id in SCORE_HMM_ARM_IDS}
        or sum(item.generated_trial_count for item in records) != frontier.generated_trial_count
        or sum(item.evaluated_trial_count for item in records) != frontier.evaluated_trial_count
        or sum(item.selected_trial_count for item in records) != frontier.selected_trial_count
        or int(resource.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
        or total_bundle_bytes > request.resource_max_temp_bytes
    ):
        _raise("score/HMM bundle relational identity is invalid", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    return {
        "manifest": manifest,
        "request": request,
        "frontier": frontier,
        "records": records,
        "resource": resource,
    }


def _build_registry_records(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    frontier: ScoreHMMAdmissionFrontierReceiptV1,
    frontier_source: Path,
    frontier_final: Path,
) -> tuple[AdvisoryResearchTrialRecordV1, ...]:
    unique_variable = {
        PACKAGE_SCORE_CALIBRATION_ONLY: "PACKAGE_BOUND_SAME_DAY_SCORE_CALIBRATION_AND_SLOT_ADMISSION",
        SCORE_PLUS_RAW_MARKET_SHAPE: "RAW_MARKET_SHAPE_INCREMENT_OVER_PACKAGE_SCORE",
        SCORE_PLUS_MARKET_HMM: "FOLD_LOCAL_CAUSAL_MARKET_HMM_INCREMENT_OVER_RAW_MARKET",
        SCORE_PLUS_SECTOR_HMM: "CANONICAL_CAUSAL_SECTOR_CONTEXT_UNAVAILABLE",
        SCORE_PLUS_MARKET_AND_SECTOR_HMM: "MARKET_AND_SECTOR_FACTORIAL_CONTEXT_UNAVAILABLE",
    }
    records = []
    for arm_id in SCORE_HMM_ARM_IDS:
        generated = arm_id in SCORE_HMM_EXECUTABLE_ARM_IDS
        evaluated = frontier.arm_statuses[arm_id] == "EVALUATED"
        evidence = evidence_reference_for_file(frontier_source, role=f"score_hmm_frontier_{arm_id.lower()}")
        evidence = evidence.model_copy(update={"artifact_uri": frontier_final.resolve().as_posix()})
        records.append(
            build_trial_record(
                experiment_id=f"{SCORE_HMM_EXPERIMENT_ID}:{arm_id}",
                attempt_id=f"{request.request_id}:{arm_id}",
                research_stage=SCORE_HMM_RESEARCH_STAGE,
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                hypothesis_family_id=SCORE_HMM_HYPOTHESIS_FAMILY_ID,
                parent_lineage=(
                    "ADVISORY-N1-TIER1-LEARNABILITY",
                    "ADVISORY-N3-FINANCIAL-EVENT-INFORMATION-SET-MVE-V1",
                ),
                unique_variable=unique_variable[arm_id],
                objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
                dataset_identity=request.dataset_identity,
                schema_identity=SCORE_HMM_ARM_SCHEMA_HASHES[arm_id],
                policy_identity=request.policy_identity,
                planned_trial_count=1,
                generated_trial_count=int(generated),
                evaluated_trial_count=int(evaluated),
                selected_trial_count=int(frontier.selected_arm_id == arm_id),
                consumed_windows=(
                    ConsumedWindowV1(
                        window_id="P0_C_DEVELOPMENT_CONSUMED",
                        dataset_identity=request.dataset_identity,
                        start_date=request.decision_start,
                        end_date=request.decision_end,
                    ),
                ),
                result_class=ResearchResultClass.EXPLORATORY,
                decision_use=DecisionUse.NAVIGATION_ONLY,
                evidence_refs=(evidence,),
                recorded_at=frontier.created_at,
            )
        )
    return tuple(records)


def _deliver_bundle(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    bundle_path: Path,
) -> dict[str, Any]:
    loaded = _read_score_hmm_bundle(bundle_path)
    if loaded["request"].request_sha256 != request.request_sha256:
        _raise("score/HMM bundle belongs to another request", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    main_route = _resolve_bound_path(request.current_route_path)
    before = main_route.read_bytes()
    registry = AdvisoryResearchTrialRegistryV1(_resolve_bound_path(request.registry_path))
    registry_summary = registry.append_batch(loaded["records"])
    route_summary = _write_auxiliary_route(
        request=request,
        bundle_path=bundle_path,
        frontier=loaded["frontier"],
        registry_sha256=str(registry_summary["registry_sha256"]),
    )
    after = main_route.read_bytes()
    if before != after:
        _raise("score/HMM delivery changed the main research route", "ADVISORY_SCORE_HMM_ROUTE_INVALID")
    return {
        "registry": registry_summary,
        "auxiliary_route": route_summary,
        "main_route_unchanged": True,
        "next_task": loaded["frontier"].next_task,
    }


def _write_auxiliary_route(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    bundle_path: Path,
    frontier: ScoreHMMAdmissionFrontierReceiptV1,
    registry_sha256: str,
) -> dict[str, Any]:
    path = _resolve_bound_path(request.auxiliary_route_path)
    text = "\n".join(
        [
            "# Advisory 当前辅助研究路线",
            "",
            "- active_auxiliary_line: N3_AUX_SCORE_HMM_CONDITIONED_ADMISSION",
            f"- next_task: {frontier.next_task}",
            f"- selected_arm: {frontier.selected_arm_id or 'NONE'}",
            f"- evidence_class: {frontier.evidence_class}",
            f"- bundle_id: {bundle_path.name}",
            f"- request_sha256: {request.request_sha256}",
            f"- objective_contract: {request.objective_contract.value}",
            f"- study_type: {request.study_type.value}",
            f"- decision_use: {request.decision_use.value}",
            f"- cumulative_candidate_index: {request.reserved_candidate_indices[-1]}",
            f"- trial_registry_sha256: {registry_sha256}",
            "- sector_arms: NOT_RUN_SOURCE_UNAVAILABLE",
            "- sealed_holdout_accessed: false",
            "- deployable/runtime/rerank/position_weight: false/false/false/false",
            "",
            "本页只记录同包评分、原始市场形态与fold-local causal market HMM的开发窗口导航。",
            "它不改写主线current_route.md；selected仅允许进入独立confirmation设计。",
            "",
        ]
    )
    encoded = text.encode("utf-8")
    if path.exists():
        if path.read_bytes() == encoded:
            return {"status": "EXACT_NOOP", "path": path.as_posix(), "sha256": sha256_file(path)}
        _raise("auxiliary route already contains a different lineage", "ADVISORY_SCORE_HMM_ROUTE_INVALID")
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return {"status": "WRITTEN", "path": path.as_posix(), "sha256": sha256_file(path)}


def _find_existing_bundle(request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> Path | None:
    root = _resolve_bound_path(request.output_root) / "score_hmm_admission_bundles"
    if not root.is_dir():
        return None
    matches = []
    for candidate in sorted(root.iterdir()):
        if not candidate.is_dir() or candidate.name.startswith("."):
            continue
        try:
            loaded = _read_score_hmm_bundle(candidate)
        except AdvisoryModelFirstError as exc:
            _raise(
                "score/HMM bundle root contains an invalid published bundle",
                "ADVISORY_SCORE_HMM_BUNDLE_INVALID",
                candidate=candidate.as_posix(),
                cause_reason_code=exc.reason_code,
            )
        if loaded["request"].request_sha256 == request.request_sha256:
            matches.append(candidate)
    if len(matches) > 1:
        _raise("one score/HMM request maps to multiple bundles", "ADVISORY_SCORE_HMM_BUNDLE_INVALID")
    return matches[0] if matches else None


def _verify_run_environment(request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> None:
    repository = _resolve_bound_path(request.repository_root)
    dirty = _cross_os_git_dirty_paths(repository)
    if dirty:
        _raise("score/HMM run repository is dirty", "ADVISORY_SCORE_HMM_REQUEST_INVALID", dirty_paths=dirty[:20])
    if _cross_os_git_commit(repository) != request.repository_commit:
        _raise("score/HMM repository commit drift", "ADVISORY_SCORE_HMM_REQUEST_INVALID")
    registry_path = _resolve_bound_path(request.registry_path)
    records = AdvisoryResearchTrialRegistryV1(registry_path).read()
    route_path = _resolve_bound_path(request.current_route_path)
    route = _parse_current_route(route_path)
    if (
        sha256_file(registry_path) != request.registry_sha256_at_request
        or len(records) != request.registry_record_count_at_request
        or sum(item.evaluated_trial_count for item in records) != request.cumulative_evaluated_trial_count_prior
        or sha256_file(route_path) != request.current_route_sha256
        or route["next_task"] != request.current_route_next_task
        or int(route["cumulative_candidate_index"]) != request.cumulative_candidate_index_prior
    ):
        _raise("score/HMM trial reservation or route head changed", "ADVISORY_SCORE_HMM_TRIAL_RESERVATION_INVALID")


def _load_and_verify_calendar(n1_request: AdvisoryN1Tier1RequestV1) -> pd.DatetimeIndex:
    initialize_qlib(_resolve_bound_path(n1_request.qlib_daily_root))
    full = load_trading_calendar("2023-01-01", n1_request.data_cutoff.isoformat())
    identity_window = full[full >= pd.Timestamp("2023-09-01")]
    identity_hash = canonical_json_sha256({"market_sessions": [item.date().isoformat() for item in identity_window]})
    if (
        len(identity_window) != n1_request.market_calendar_identity.row_count
        or identity_hash != n1_request.market_calendar_identity.sha256
    ):
        _raise("score/HMM N1 calendar identity drift", "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH")
    return full


def _factor_schema_identity(receipt: Any) -> str:
    return canonical_json_sha256(
        {
            "factor_root": str(receipt.factor_root),
            "data_cutoff": str(receipt.data_cutoff),
            "h5_schema_hashes": dict(receipt.h5_schema_hashes),
            "static_factor_schema_hash": str(receipt.static_factor_schema_hash),
        }
    )


def _parse_current_route(path: Path) -> dict[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        _raise("main research route cannot be read", "ADVISORY_SCORE_HMM_ROUTE_INVALID", error_type=type(exc).__name__)
    next_match = re.search(r"^- next_task:\s*(\S+)\s*$", text, flags=re.MULTILINE)
    index_match = re.search(r"^- cumulative_candidate_index:\s*(\d+)\s*$", text, flags=re.MULTILINE)
    if not next_match or not index_match:
        _raise("main research route omits score/HMM reservation fields", "ADVISORY_SCORE_HMM_ROUTE_INVALID")
    return {"next_task": next_match.group(1), "cumulative_candidate_index": int(index_match.group(1))}


def _verify_evidence_ref(reference: Any) -> None:
    path = _resolve_bound_path(reference.artifact_uri)
    if not path.is_file() or sha256_file(path) != reference.sha256 or path.stat().st_size != reference.size_bytes:
        _raise(
            "score/HMM evidence reference changed",
            "ADVISORY_SCORE_HMM_SOURCE_IDENTITY_MISMATCH",
            role=reference.role,
        )


def _git_origin_main_commit(repository_root: Path) -> str:
    command, root = _git_command_for_worktree(repository_root)
    try:
        commit = subprocess.run(
            [*command, "rev-parse", "origin/main"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip().lower()
    except (OSError, subprocess.CalledProcessError) as exc:
        _raise("origin/main commit cannot be read", "ADVISORY_SCORE_HMM_REQUEST_INVALID", error_type=type(exc).__name__)
    if len(commit) != 40 or any(value not in "0123456789abcdef" for value in commit):
        _raise("origin/main commit is invalid", "ADVISORY_SCORE_HMM_REQUEST_INVALID")
    return commit


def _write_immutable_request(path: Path, request: FrozenAdvisoryScoreHMMAdmissionRequestV1) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            request.model_dump(mode="json"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")
    if path.exists():
        try:
            existing = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_validate_json(path.read_text(encoding="utf-8"))
        except Exception as exc:
            _raise("existing score/HMM request is invalid", "ADVISORY_SCORE_HMM_REQUEST_INVALID", error_type=type(exc).__name__)
        if existing.request_sha256 != request.request_sha256:
            _raise("score/HMM request path contains another identity", "ADVISORY_SCORE_HMM_REQUEST_INVALID")
        return
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _run_response(
    *,
    request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
    bundle_path: Path,
    delivery: Mapping[str, Any],
    elapsed_seconds: float,
) -> dict[str, Any]:
    inspected = inspect_score_hmm_admission_bundle(bundle_path)
    return {
        **inspected,
        "request_id": request.request_id,
        "bundle_path": bundle_path.as_posix(),
        "planned_trial_count": request.planned_trial_count,
        "generated_trial_count": inspected["generated_trial_count"],
        "evaluated_trial_count": inspected["evaluated_trial_count"],
        "cumulative_candidate_index": request.reserved_candidate_indices[-1],
        "delivery": delivery,
        "elapsed_seconds": elapsed_seconds,
        "backend_restart": "noop",
        "production_ddl_gate": "noop",
        "database_access": False,
        "network_access": False,
    }


def _stage(name: str, started: float, **facts: Any) -> dict[str, Any]:
    return {
        "stage": name,
        "wall_seconds": time.monotonic() - started,
        "peak_rss_bytes": _peak_rss_bytes(),
        **facts,
    }


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n",
        encoding="utf-8",
    )


def _read_json(path: Path, reason_code: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise("score/HMM JSON artifact cannot be read", reason_code, path=str(path), error_type=type(exc).__name__)


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    try:
        frame.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
    except Exception as exc:
        _raise(
            "score/HMM parquet artifact cannot be written",
            "ADVISORY_SCORE_HMM_BUNDLE_INVALID",
            path=str(path),
            error_type=type(exc).__name__,
        )


def _resolve_bound_path(value: str | Path) -> Path:
    text = str(value).replace("\\", "/")
    if os.name == "nt" and text.startswith("/mnt/") and len(text) > 6 and text[5].isalpha() and text[6] == "/":
        return Path(f"{text[5].upper()}:/{text[7:]}").resolve()
    if os.name != "nt" and len(text) > 2 and text[0].isalpha() and text[1:3] == ":/":
        return Path(f"/mnt/{text[0].lower()}/{text[3:]}").resolve()
    return Path(value).resolve()


def _require_formal_environment() -> None:
    if (
        os.name == "nt"
        or os.environ.get("CONDA_DEFAULT_ENV") != "rdagent-gpu"
        or os.environ.get("AISTOCK_ADVISORY_SCORE_HMM_FORMAL_RUN") != "1"
    ):
        _raise(
            "score/HMM formal prepare/run requires WSL rdagent-gpu and explicit flag",
            "ADVISORY_SCORE_HMM_REQUEST_INVALID",
            os_name=os.name,
            conda_default_env=os.environ.get("CONDA_DEFAULT_ENV"),
        )


def _assert_baseline_parity(
    actual_daily: pd.DataFrame,
    actual_episodes: pd.DataFrame,
    expected_daily: pd.DataFrame,
    expected_episodes: pd.DataFrame,
) -> None:
    daily_columns = [
        "decision_as_of_trade_date",
        "target_trade_date",
        "active_count",
        "cash_slot_count",
        "entered_count",
        "held_count",
        "exited_count",
        "waiting_count",
        "replacement_budget_used",
        "turnover_fraction",
        "gross_return_bps",
        "transaction_cost_bps",
        "net_return_bps",
        "benchmark_return_bps",
        "net_excess_return_bps",
        "cumulative_nav",
        "drawdown",
    ]
    if len(actual_daily) != len(expected_daily) or list(actual_daily.columns) != list(expected_daily.columns):
        _raise("baseline policy simulator shape drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID")
    for column in daily_columns:
        left = actual_daily[column]
        right = expected_daily[column]
        if pd.api.types.is_numeric_dtype(left):
            if not np.allclose(left.to_numpy(float), right.to_numpy(float), rtol=1e-10, atol=1e-10, equal_nan=True):
                _raise("baseline policy simulator numeric drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID", column=column)
        elif not left.astype(str).equals(right.astype(str)):
            _raise("baseline policy simulator identity drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID", column=column)
    episode_columns = [column for column in expected_episodes.columns if column != "episode_id"]
    if len(actual_episodes) != len(expected_episodes):
        _raise("baseline policy episode count drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID")
    actual = actual_episodes[episode_columns].reset_index(drop=True)
    expected = expected_episodes[episode_columns].reset_index(drop=True)
    for column in episode_columns:
        if pd.api.types.is_numeric_dtype(actual[column]):
            if not np.allclose(
                pd.to_numeric(actual[column], errors="coerce"),
                pd.to_numeric(expected[column], errors="coerce"),
                rtol=1e-10,
                atol=1e-10,
                equal_nan=True,
            ):
                _raise("baseline policy episode numeric drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID", column=column)
        elif not actual[column].astype(str).equals(expected[column].astype(str)):
            _raise("baseline policy episode identity drift", "ADVISORY_SCORE_HMM_POLICY_PARITY_INVALID", column=column)


def _benchmark_frame(frame: pd.DataFrame) -> pd.DataFrame:
    reset = frame.reset_index()
    required = {"datetime", "open", "close"}
    if not required.issubset(reset.columns):
        _raise("CSI300 source is incomplete", "ADVISORY_SCORE_HMM_RAW_MARKET_INVALID")
    reset["datetime"] = pd.to_datetime(reset["datetime"]).dt.normalize()
    if reset["datetime"].duplicated().any():
        _raise("CSI300 source has duplicate dates", "ADVISORY_SCORE_HMM_RAW_MARKET_INVALID")
    return reset.set_index("datetime")[["open", "close"]].apply(pd.to_numeric, errors="coerce").sort_index()


def _within_group_percentile(values: np.ndarray, instruments: pd.Series) -> np.ndarray:
    order = pd.DataFrame({"value": values, "instrument": instruments.astype(str)}).sort_values(
        ["value", "instrument"], ascending=[False, True]
    )
    percentile = pd.Series(1.0 - np.arange(len(order), dtype=float) / max(1, len(order) - 1), index=order.index)
    return percentile.reindex(np.arange(len(order))).to_numpy(float)


def _iqr(values: Sequence[float] | np.ndarray) -> float:
    array = np.asarray(values, dtype=float)
    if not np.isfinite(array).all() or len(array) < 2:
        return float("nan")
    return float(np.quantile(array, 0.75) - np.quantile(array, 0.25))


def _contiguous_blocks(dates: pd.DatetimeIndex, calendar: pd.DatetimeIndex) -> tuple[pd.DatetimeIndex, ...]:
    positions = calendar.get_indexer(dates)
    if (positions < 0).any():
        _raise("HMM block date is absent from calendar", "ADVISORY_SCORE_HMM_CLOCK_MISMATCH")
    starts = np.r_[0, np.flatnonzero(np.diff(positions) != 1) + 1]
    ends = np.r_[starts[1:], len(dates)]
    return tuple(dates[start:end] for start, end in zip(starts, ends, strict=True))


def _causal_forward_filter(model: Any, matrix: np.ndarray) -> np.ndarray:
    log_emission = np.asarray(model._compute_log_likelihood(matrix), dtype=float)
    log_start = np.log(np.clip(model.startprob_, 1e-300, None))
    log_transition = np.log(np.clip(model.transmat_, 1e-300, None))
    posterior = np.empty_like(log_emission)
    alpha = log_start + log_emission[0]
    alpha -= logsumexp(alpha)
    posterior[0] = np.exp(alpha)
    for index in range(1, len(matrix)):
        alpha = log_emission[index] + logsumexp(alpha[:, None] + log_transition, axis=0)
        alpha -= logsumexp(alpha)
        posterior[index] = np.exp(alpha)
    return posterior


def _state_duration(states: np.ndarray) -> np.ndarray:
    duration = np.ones(len(states), dtype=np.int32)
    for index in range(1, len(states)):
        duration[index] = duration[index - 1] + 1 if states[index] == states[index - 1] else 1
    return duration


def _decile_calibration(prediction: np.ndarray, truth: np.ndarray) -> list[dict[str, Any]]:
    bins = min(10, len(prediction))
    rank = pd.Series(prediction).rank(method="first")
    bucket = pd.qcut(rank, bins, labels=False, duplicates="drop")
    frame = pd.DataFrame({"bucket": bucket, "prediction": prediction, "truth": truth})
    return [
        {
            "bucket": int(index),
            "row_count": len(group),
            "predicted_mean_bps": float(group["prediction"].mean()),
            "realized_mean_bps": float(group["truth"].mean()),
        }
        for index, group in frame.groupby("bucket", sort=True)
    ]


def _cvar_5(values: pd.Series) -> float:
    array = pd.to_numeric(values, errors="coerce").dropna().to_numpy(float)
    if not len(array):
        return float("nan")
    threshold = float(np.quantile(array, 0.05))
    return float(array[array <= threshold].mean())


def _downside_deviation(values: pd.Series) -> float:
    array = pd.to_numeric(values, errors="coerce").dropna().to_numpy(float)
    downside = np.minimum(array, 0.0)
    return float(np.sqrt(np.mean(downside**2))) if len(downside) else float("nan")


def _finite(value: object) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _json_scalar(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (date, datetime)) else value


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context)
