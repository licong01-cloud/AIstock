from __future__ import annotations

import json
import hashlib
import math
import os
import shutil
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, NoReturn, Sequence

import numpy as np
import pandas as pd
from pydantic import ValidationError
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.metrics import log_loss
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from backend.services.advisory_model_first.causal_admission_v2_contracts import (
    CAUSAL_ADMISSION_ARM_IDS,
    CAUSAL_ADMISSION_EXPERIMENT_ID,
    CAUSAL_ADMISSION_FAMILY_ID,
    CAUSAL_ADMISSION_FEATURE_COLUMNS,
    CAUSAL_ADMISSION_FEATURE_SCHEMA,
    CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH,
    CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE,
    CAUSAL_ADMISSION_STAGE,
    EXPANDING_ARM_ID,
    INNER_END,
    INNER_START,
    OUTER_END,
    OUTER_START,
    STATIC_ARM_ID,
    CausalAdmissionFrontierReceiptV1,
    FrozenAdvisoryCausalAdmissionRequestV2,
    build_causal_admission_receipt,
    build_causal_admission_request,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.alpha_signal_audit_pipeline import (
    _git_command_for_worktree,
)
from backend.services.advisory_model_first.qe_alpha_mve_pipeline import (
    _moving_block_interval,
    _peak_rss_bytes,
)
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
from backend.services.advisory_model_first.research_control_contracts import (
    AdvisoryResearchTrialRecordV1,
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)
from backend.services.advisory_model_first.score_hmm_admission_contracts import (
    FrozenAdvisoryScoreHMMAdmissionRequestV1,
)
from backend.services.advisory_model_first.score_hmm_admission_pipeline import (
    _load_verified_sources,
    _resolve_bound_path,
    build_package_score_features,
    build_raw_market_shape,
    inspect_score_hmm_admission_bundle,
)
from backend.services.advisory_model_first.policy_contracts import transition_policy_from_payload
from backend.services.advisory_model_first.outcome_calibration import expected_calibration_error
from backend.services.advisory_model_first.shadow_portfolio_policy import replay_shadow_portfolio
from backend.services.strategy_package.runtime_variant import canonical_json_sha256

BASELINE_ARM_ID = "BASELINE_ALL_TAKE"
CAUSAL_BUNDLE_SCHEMA = "advisory_causal_admission_bundle_v1"
RESULT_MEMBERS = frozenset(
    {
        "source_preflight.json",
        "feature_schema.json",
        "fit_receipts.json",
        "predictions.parquet",
        "admission_decisions.parquet",
        "policy_daily.parquet",
        "policy_episodes.parquet",
        "frontier_summary.json",
        "frontier_receipt.json",
    }
)
BUNDLE_MEMBERS = RESULT_MEMBERS | {"request.json", "resource_report.json", "registry_records.json"}
MUTABLE_PARENT_CONTROL_EVIDENCE_ROLES = frozenset({"score_hmm_trial_registry", "score_hmm_main_route"})


@dataclass(frozen=True)
class ChronologicalPredictionResult:
    predictions: pd.DataFrame
    fit_receipts: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class PhaseEvaluationResult:
    daily: pd.DataFrame
    episodes: pd.DataFrame
    metrics: dict[str, Any]


def build_causal_admission_panel(
    *,
    score_features: pd.DataFrame,
    raw_market_features: pd.DataFrame,
    primary_labels: pd.DataFrame,
) -> pd.DataFrame:
    """Build the exact score/raw panel without changing the candidate population."""

    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    score = score_features.copy()
    labels = primary_labels.copy()
    raw = raw_market_features.copy()
    for frame in (score, labels, raw):
        if "decision_as_of_trade_date" in frame:
            frame["decision_as_of_trade_date"] = pd.to_datetime(frame["decision_as_of_trade_date"]).dt.normalize()
    score["target_trade_date"] = pd.to_datetime(score["target_trade_date"]).dt.normalize()
    labels["target_trade_date"] = pd.to_datetime(labels["target_trade_date"]).dt.normalize()
    score["instrument"] = score["instrument"].astype(str).str.upper()
    labels["instrument"] = labels["instrument"].astype(str).str.upper()
    required_score = {*keys, "selection_effective_rank", *CAUSAL_ADMISSION_FEATURE_COLUMNS[:12]}
    required_raw = {"decision_as_of_trade_date", *CAUSAL_ADMISSION_FEATURE_COLUMNS[12:]}
    required_label = {*keys, "net_return_bps", "label_information_end", "label_status"}
    if not required_score.issubset(score) or not required_raw.issubset(raw) or not required_label.issubset(labels):
        _raise("causal Admission source schema is incomplete", "ADVISORY_CAUSAL_SOURCE_INVALID")
    if score.duplicated(keys).any() or labels.duplicated(keys).any():
        _raise("causal Admission source keys are duplicated", "ADVISORY_CAUSAL_SOURCE_INVALID")
    if raw.duplicated(["decision_as_of_trade_date"]).any():
        _raise("causal Admission raw-market dates are duplicated", "ADVISORY_CAUSAL_SOURCE_INVALID")
    label_columns = [*keys, "net_return_bps", "label_information_end", "label_status"]
    panel = score.merge(labels[label_columns], on=keys, how="left", validate="one_to_one")
    panel = panel.merge(
        raw[["decision_as_of_trade_date", *CAUSAL_ADMISSION_FEATURE_COLUMNS[12:]]],
        on="decision_as_of_trade_date",
        how="left",
        validate="many_to_one",
    )
    panel["label_information_end"] = pd.to_datetime(panel["label_information_end"]).dt.normalize()
    expected = score[keys].sort_values(keys).reset_index(drop=True)
    actual = panel[keys].sort_values(keys).reset_index(drop=True)
    if not expected.equals(actual) or len(panel) != len(score):
        _raise("causal Admission panel changed parent candidates", "ADVISORY_CAUSAL_POPULATION_DRIFT")
    counts = panel.groupby("decision_as_of_trade_date").size()
    if len(counts) != 386 or not counts.eq(20).all():
        _raise(
            "causal Admission requires exact 386 x Top20 population",
            "ADVISORY_CAUSAL_POPULATION_DRIFT",
            day_count=int(len(counts)),
        )
    return panel.sort_values(["decision_as_of_trade_date", "selection_effective_rank"]).reset_index(drop=True)


def run_chronological_predictions(
    *,
    panel: pd.DataFrame,
    prediction_dates: Sequence[pd.Timestamp],
    arm_id: str,
    request: FrozenAdvisoryCausalAdmissionRequestV2,
    phase: str,
) -> ChronologicalPredictionResult:
    """Produce one past-only prediction per candidate/date for a frozen arm."""

    if arm_id not in CAUSAL_ADMISSION_ARM_IDS or phase not in {"INNER", "OUTER"}:
        raise ValueError("unknown causal Admission arm or phase")
    dates = pd.DatetimeIndex(pd.to_datetime(list(prediction_dates))).normalize().sort_values().unique()
    if not len(dates):
        raise ValueError("prediction_dates cannot be empty")
    expected_range = (INNER_START, INNER_END) if phase == "INNER" else (OUTER_START, OUTER_END)
    if (dates[0].date(), dates[-1].date()) != expected_range:
        _raise("causal Admission phase dates differ from R0", "ADVISORY_CAUSAL_CLOCK_INVALID")
    update_positions = {0}
    if arm_id == EXPANDING_ARM_ID:
        update_positions.update(range(20, len(dates), 20))
    frames: list[pd.DataFrame] = []
    receipts: list[dict[str, Any]] = []
    regression: Pipeline | None = None
    classifier: Pipeline | None = None
    fit_identity = ""
    for position, decision_date in enumerate(dates):
        if position in update_positions:
            train = panel.loc[
                (panel["decision_as_of_trade_date"] < decision_date)
                & (panel["label_status"] == "MATURED")
                & (panel["label_information_end"] < decision_date)
                & pd.to_numeric(panel["net_return_bps"], errors="coerce").notna()
            ].copy()
            mature_days = int(train["decision_as_of_trade_date"].nunique())
            if mature_days < request.minimum_mature_train_days or len(train) < request.minimum_mature_train_rows:
                _raise(
                    "causal Admission has insufficient mature training rows",
                    "ADVISORY_CAUSAL_INSUFFICIENT_MATURE_TRAINING_ROWS",
                    phase=phase,
                    arm_id=arm_id,
                    fit_cutoff=str(decision_date.date()),
                    mature_days=mature_days,
                    mature_rows=int(len(train)),
                )
            matrix = train.loc[:, CAUSAL_ADMISSION_FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
            all_missing = [column for column in matrix if matrix[column].notna().sum() == 0]
            if all_missing:
                _raise(
                    "causal Admission training features are entirely missing",
                    "ADVISORY_CAUSAL_SOURCE_INVALID",
                    columns=all_missing,
                )
            truth = pd.to_numeric(train["net_return_bps"], errors="raise").to_numpy(float)
            regression, classifier = _model_pair(request)
            regression.fit(matrix, truth)
            binary = (truth > 0.0).astype(int)
            if len(np.unique(binary)) != 2:
                _raise("causal Admission probability class variation is absent", "ADVISORY_CAUSAL_MODEL_INVALID")
            classifier.fit(matrix, binary)
            fit_payload = {
                "schema_version": "advisory_causal_admission_fit_receipt_v1",
                "phase": phase,
                "arm_id": arm_id,
                "fit_ordinal": len(receipts) + 1,
                "fit_cutoff": decision_date.date().isoformat(),
                "mature_train_days": mature_days,
                "mature_train_rows": int(len(train)),
                "train_date_start": train["decision_as_of_trade_date"].min().date().isoformat(),
                "train_date_end": train["decision_as_of_trade_date"].max().date().isoformat(),
                "max_label_information_end": train["label_information_end"].max().date().isoformat(),
                "feature_schema_hash": CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH,
                "regression": {"family": "Ridge", "alpha": 100.0, "solver": "lsqr"},
                "probability": {"family": "LogisticRegression", "C": 1.0, "solver": "lbfgs"},
            }
            fit_payload["fit_identity"] = canonical_json_sha256(fit_payload)
            fit_identity = fit_payload["fit_identity"]
            receipts.append(fit_payload)
        if regression is None or classifier is None:
            _raise("causal Admission model was not fitted", "ADVISORY_CAUSAL_MODEL_INVALID")
        current = panel.loc[panel["decision_as_of_trade_date"].eq(decision_date)].copy()
        if len(current) != 20:
            _raise("causal Admission prediction date is not exact Top20", "ADVISORY_CAUSAL_POPULATION_DRIFT")
        matrix = current.loc[:, CAUSAL_ADMISSION_FEATURE_COLUMNS].apply(pd.to_numeric, errors="coerce")
        predicted = regression.predict(matrix)
        probability = classifier.predict_proba(matrix)[:, 1]
        if not np.isfinite(predicted).all() or not np.isfinite(probability).all():
            _raise("causal Admission model output is non-finite", "ADVISORY_CAUSAL_MODEL_INVALID")
        frame = current[
            [
                "decision_as_of_trade_date",
                "target_trade_date",
                "instrument",
                "selection_effective_rank",
            ]
        ].copy()
        frame.insert(0, "arm_id", arm_id)
        frame.insert(1, "phase", phase)
        frame["expected_net_return_bps"] = predicted
        frame["positive_probability"] = probability
        frame["predictive_return_q20_bps"] = np.nan
        frame["predictive_return_q80_bps"] = np.nan
        frame["mean_estimation_interval_status"] = "UNAVAILABLE_NOT_ESTIMATED"
        frame["fit_identity"] = fit_identity
        frames.append(frame)
    result = pd.concat(frames, ignore_index=True)
    if len(result) != len(dates) * 20 or result.duplicated(["arm_id", "decision_as_of_trade_date", "instrument"]).any():
        _raise("causal Admission chronological prediction coverage is invalid", "ADVISORY_CAUSAL_CLOCK_INVALID")
    return ChronologicalPredictionResult(result, tuple(receipts))


def build_causal_admission_decisions(
    predictions: pd.DataFrame, request: FrozenAdvisoryCausalAdmissionRequestV2
) -> pd.DataFrame:
    top5 = predictions.loc[predictions["selection_effective_rank"].between(1, 5)].copy()
    top5["utility_bps"] = top5["expected_net_return_bps"] - request.action_utility_buffer_bps
    top5["action"] = np.where(top5["utility_bps"] > 0.0, "TAKE", "SKIP")
    top5["reason_code"] = np.where(
        top5["action"].eq("TAKE"), "TAKE_POSITIVE_EXPECTED_NET_VALUE", "SKIP_BELOW_ECONOMIC_BUFFER"
    )
    top5["parent_rank"] = top5["selection_effective_rank"].astype(int)
    top5["day_state"] = top5.groupby(["phase", "arm_id", "decision_as_of_trade_date"])["action"].transform(
        lambda values: "TAKE_SOME" if (values == "TAKE").any() else "NO_ELIGIBLE_RECOMMENDATION"
    )
    if top5.groupby(["phase", "arm_id", "decision_as_of_trade_date"]).size().ne(5).any():
        _raise("causal Admission action grid changed parent Top5", "ADVISORY_CAUSAL_ACTION_INVALID")
    top5["action_contract_sha256"] = ""
    for _, indexes in top5.groupby(["phase", "arm_id"], sort=True).groups.items():
        frozen = top5.loc[indexes].drop(columns=["action_contract_sha256"])
        top5.loc[indexes, "action_contract_sha256"] = _frame_sha256(frozen)
    return top5.reset_index(drop=True)


def evaluate_causal_phase(
    *,
    arm_id: str,
    phase: str,
    decisions: pd.DataFrame,
    sources: Mapping[str, Any],
    request: FrozenAdvisoryCausalAdmissionRequestV2,
) -> PhaseEvaluationResult:
    phase_decisions = decisions.loc[decisions["arm_id"].eq(arm_id) & decisions["phase"].eq(phase)].copy()
    action_identities = phase_decisions["action_contract_sha256"].drop_duplicates()
    if len(action_identities) != 1 or action_identities.iloc[0] != _frame_sha256(
        phase_decisions.drop(columns=["action_contract_sha256"])
    ):
        _raise("causal Admission action contract identity differs", "ADVISORY_CAUSAL_ACTION_INVALID")
    candidate_dates = pd.DatetimeIndex(phase_decisions["decision_as_of_trade_date"].unique()).sort_values()
    rankings = sources["policy_rankings"]
    policy_request = sources["policy_request"]
    policy = transition_policy_from_payload(policy_request.shadow_policy)
    common = {
        "rankings": rankings,
        "daily": sources["candidate_daily"],
        "benchmark_daily": sources["benchmark_daily"],
        "suspend_rows": sources["suspend_rows"],
        "trading_calendar": sources["trading_calendar"],
        "policy": policy,
        "policy_sha256": policy_request.shadow_policy_sha256,
        "cost_policy": policy_request.cost_policy,
        "rank_depth": 50,
        "candidate_decision_dates": candidate_dates,
    }
    baseline = replay_shadow_portfolio(request_id=f"{request.request_id}:{phase}:baseline", **common)
    priorities = phase_decisions.loc[
        phase_decisions["action"].eq("TAKE"),
        ["decision_as_of_trade_date", "instrument", "parent_rank"],
    ].rename(columns={"parent_rank": "entry_priority_rank"})
    arm = replay_shadow_portfolio(
        request_id=f"{request.request_id}:{phase}:{arm_id}", entry_priorities=priorities, **common
    )
    baseline_daily = baseline.daily.loc[baseline.daily["decision_as_of_trade_date"].isin(candidate_dates)].copy()
    arm_daily = arm.daily.loc[arm.daily["decision_as_of_trade_date"].isin(candidate_dates)].copy()
    paired = arm_daily[["decision_as_of_trade_date", "net_return_bps"]].merge(
        baseline_daily[["decision_as_of_trade_date", "net_return_bps"]],
        on="decision_as_of_trade_date",
        suffixes=("_arm", "_baseline"),
        validate="one_to_one",
    )
    lift = paired["net_return_bps_arm"] - paired["net_return_bps_baseline"]
    # Inner multiplicity is handled explicitly after both arms are evaluated.
    # alpha=0.10 gives the required one-sided 95% lower readout for outer.
    alpha = 0.05 if phase == "INNER" else 0.10
    lower, upper = _moving_block_interval(
        lift,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + (0 if arm_id == STATIC_ARM_ID else 1),
        alpha=alpha,
    )
    one_sided_p = _moving_block_one_sided_p_value(
        lift,
        threshold=request.minimum_economic_lift_bps,
        block_length=request.block_length_trading_days,
        repetitions=request.bootstrap_repetitions,
        seed=request.bootstrap_seed + (0 if arm_id == STATIC_ARM_ID else 1),
    )
    by_day = phase_decisions.groupby("decision_as_of_trade_date")["action"]
    intervention_days = int(by_day.apply(lambda values: (values == "SKIP").any()).sum())
    take_days = int(by_day.apply(lambda values: (values == "TAKE").any()).sum())
    skip_days = int(by_day.apply(lambda values: (values == "SKIP").any()).sum())
    regime = sources["regime_daily"][["decision_as_of_trade_date", "regime"]].copy()
    regime["decision_as_of_trade_date"] = pd.to_datetime(regime["decision_as_of_trade_date"]).dt.normalize()
    if regime.duplicated(["decision_as_of_trade_date"]).any():
        _raise("causal Admission regime dates are duplicated", "ADVISORY_CAUSAL_SOURCE_INVALID")
    phase_regime = regime.loc[regime["decision_as_of_trade_date"].isin(candidate_dates)]
    regime_unavailable_days = len(set(candidate_dates) - set(phase_regime["decision_as_of_trade_date"])) + int(
        phase_regime["regime"].isna().sum()
    )
    intervention_dates = set(by_day.apply(lambda values: (values == "SKIP").any()).loc[lambda value: value].index)
    regime_counts = (
        regime.loc[regime["decision_as_of_trade_date"].isin(intervention_dates), "regime"]
        .value_counts()
        .sort_index()
        .astype(int)
        .to_dict()
    )
    min_paired = request.inner_minimum_paired_days if phase == "INNER" else request.outer_minimum_paired_days
    min_intervention = (
        request.inner_minimum_intervention_days if phase == "INNER" else request.outer_minimum_intervention_days
    )
    min_fraction = (
        request.inner_minimum_intervention_fraction if phase == "INNER" else request.outer_minimum_intervention_fraction
    )
    min_take = request.inner_minimum_take_days if phase == "INNER" else request.outer_minimum_take_days
    min_skip = request.inner_minimum_skip_days if phase == "INNER" else request.outer_minimum_skip_days
    support_reasons: list[str] = []
    if len(paired) < min_paired:
        support_reasons.append("PAIRED_DAYS_BELOW_MINIMUM")
    if intervention_days < min_intervention or intervention_days / len(candidate_dates) < min_fraction:
        support_reasons.append("INTERVENTION_SUPPORT_BELOW_MINIMUM")
    if take_days < min_take or skip_days < min_skip:
        support_reasons.append("ACTION_SIDE_SUPPORT_BELOW_MINIMUM")
    observed_regimes = sorted(phase_regime["regime"].dropna().astype(str).unique())
    regime_minimum = 1 if phase == "INNER" else request.outer_minimum_intervention_days_per_regime
    if any(regime_counts.get(value, 0) < regime_minimum for value in observed_regimes):
        support_reasons.append("REGIME_INTERVENTION_SUPPORT_BELOW_MINIMUM")
    arm_mdd = float(arm.metrics["maximum_drawdown"])
    baseline_mdd = float(baseline.metrics["maximum_drawdown"])
    arm_cvar = _cvar_5(arm_daily["net_return_bps"])
    baseline_cvar = _cvar_5(baseline_daily["net_return_bps"])
    risk_pass = (
        abs(min(0.0, arm_mdd)) <= abs(min(0.0, baseline_mdd)) * request.risk_loss_ratio_limit
        and abs(min(0.0, arm_cvar)) <= abs(min(0.0, baseline_cvar)) * request.risk_loss_ratio_limit
    )
    accepted_mean = float(arm.episodes["net_return_bps"].mean()) if len(arm.episodes) else float("nan")
    eligible = (
        not support_reasons
        and risk_pass
        and float(lift.mean()) > request.minimum_economic_lift_bps
        and np.isfinite(accepted_mean)
        and accepted_mean > 0.0
    )
    metrics = {
        "schema_version": "advisory_causal_admission_phase_metrics_v1",
        "phase": phase,
        "arm_id": arm_id,
        "paired_days": int(len(paired)),
        "intervention_days": intervention_days,
        "intervention_fraction": intervention_days / len(candidate_dates),
        "take_days": take_days,
        "skip_days": skip_days,
        "regime_intervention_days": regime_counts,
        "regime_unavailable_days": regime_unavailable_days,
        "support_sufficient": not support_reasons,
        "support_reason_codes": support_reasons,
        "daily_net_absolute_lift_mean_bps": float(lift.mean()),
        "daily_net_absolute_lift_lower_bps": lower,
        "daily_net_absolute_lift_upper_bps": upper,
        "one_sided_p_value_vs_minimum_economic_lift": one_sided_p,
        "holm_rank": None,
        "holm_adjusted_alpha": None,
        "holm_reject": None,
        "accepted_episode_mean_net_return_bps": accepted_mean,
        "maximum_drawdown": arm_mdd,
        "baseline_maximum_drawdown": baseline_mdd,
        "cvar_5_bps": arm_cvar,
        "baseline_cvar_5_bps": baseline_cvar,
        "risk_budget_pass": risk_pass,
        "mean_daily_net_return_bps": float(arm_daily["net_return_bps"].mean()),
        "baseline_mean_daily_net_return_bps": float(baseline_daily["net_return_bps"].mean()),
        "mean_daily_net_excess_return_bps": _optional_mean(arm_daily, "net_excess_return_bps"),
        "baseline_mean_daily_net_excess_return_bps": _optional_mean(baseline_daily, "net_excess_return_bps"),
        "mean_turnover_fraction": _optional_mean(arm_daily, "turnover_fraction"),
        "mean_active_count": _optional_mean(arm_daily, "active_count"),
        "mean_cash_slot_count": _optional_mean(arm_daily, "cash_slot_count"),
        "recommendation_slot_coverage": float(phase_decisions["action"].eq("TAKE").sum() / len(phase_decisions)),
        "action_contract_sha256": str(action_identities.iloc[0]),
        "eligible": eligible,
    }
    daily = pd.concat(
        [baseline_daily.assign(phase=phase, arm_id=BASELINE_ARM_ID), arm_daily.assign(phase=phase, arm_id=arm_id)],
        ignore_index=True,
    )
    episodes = pd.concat(
        [
            baseline.episodes.assign(phase=phase, arm_id=BASELINE_ARM_ID),
            arm.episodes.assign(phase=phase, arm_id=arm_id),
        ],
        ignore_index=True,
    )
    return PhaseEvaluationResult(daily=daily, episodes=episodes, metrics=metrics)


def select_inner_candidate(metrics_by_arm: Mapping[str, Mapping[str, Any]]) -> str | None:
    eligible = [arm_id for arm_id in CAUSAL_ADMISSION_ARM_IDS if metrics_by_arm[arm_id]["eligible"]]
    if not eligible:
        return None
    return sorted(
        eligible,
        key=lambda arm_id: (-float(metrics_by_arm[arm_id]["daily_net_absolute_lift_mean_bps"]), arm_id),
    )[0]


def apply_inner_holm_bonferroni(metrics_by_arm: Mapping[str, dict[str, Any]], *, familywise_alpha: float) -> None:
    """Attach deterministic Holm-Bonferroni readouts without changing point selection."""

    ordered = sorted(
        CAUSAL_ADMISSION_ARM_IDS,
        key=lambda arm_id: (
            float(metrics_by_arm[arm_id]["one_sided_p_value_vs_minimum_economic_lift"]),
            arm_id,
        ),
    )
    keep_rejecting = True
    family_size = len(ordered)
    for rank, arm_id in enumerate(ordered, start=1):
        adjusted_alpha = familywise_alpha / (family_size - rank + 1)
        p_value = float(metrics_by_arm[arm_id]["one_sided_p_value_vs_minimum_economic_lift"])
        reject = keep_rejecting and p_value <= adjusted_alpha
        if not reject:
            keep_rejecting = False
        metrics_by_arm[arm_id]["holm_rank"] = rank
        metrics_by_arm[arm_id]["holm_adjusted_alpha"] = adjusted_alpha
        metrics_by_arm[arm_id]["holm_reject"] = reject


def build_probability_readout(predictions: pd.DataFrame, panel: pd.DataFrame) -> dict[str, Any]:
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    truth = panel[[*keys, "net_return_bps", "label_status"]]
    scored = predictions.merge(truth, on=keys, how="left", validate="one_to_one")
    values = pd.to_numeric(scored["net_return_bps"], errors="coerce")
    probability = pd.to_numeric(scored["positive_probability"], errors="coerce")
    if scored["label_status"].isna().any() or probability.isna().any():
        _raise("causal Admission probability truth is incomplete", "ADVISORY_CAUSAL_SOURCE_INVALID")
    evaluable = scored["label_status"].eq("MATURED") & values.notna()
    if not evaluable.any():
        _raise("causal Admission has no evaluable probability truth", "ADVISORY_CAUSAL_SOURCE_INVALID")
    binary = values.loc[evaluable].gt(0.0).astype(int).to_numpy()
    prediction = probability.loc[evaluable].clip(1e-8, 1.0 - 1e-8).to_numpy(float)
    base_rate = float(binary.mean())
    return {
        "row_count": int(len(scored)),
        "evaluable_row_count": int(evaluable.sum()),
        "normal_unavailable_row_count": int((~evaluable).sum()),
        "label_status_counts": {
            str(key): int(value) for key, value in scored["label_status"].value_counts().sort_index().items()
        },
        "positive_rate": base_rate,
        "prediction_mean": float(prediction.mean()),
        "prediction_std": float(prediction.std(ddof=0)),
        "brier": float(np.mean((prediction - binary) ** 2)),
        "base_rate_brier": float(np.mean((base_rate - binary) ** 2)),
        "logloss": float(log_loss(binary, prediction, labels=[0, 1])),
        "ece_10_bin": float(expected_calibration_error(binary, prediction, bin_count=10)["value"]),
    }


def prepare_causal_admission_request(
    *,
    parent_v1_bundle_path: str | Path,
    output_root: str | Path,
    repository_root: str | Path,
    request_path: str | Path,
) -> FrozenAdvisoryCausalAdmissionRequestV2:
    parent_path = _resolve_bound_path(parent_v1_bundle_path)
    inspected = inspect_score_hmm_admission_bundle(parent_path)
    parent_request = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_validate_json(
        (parent_path / "request.json").read_text(encoding="utf-8")
    )
    parent_manifest = _read_json(parent_path / "manifest.json", "ADVISORY_CAUSAL_SOURCE_INVALID")
    if inspected["bundle_id"] != parent_path.name:
        _raise("parent v1 bundle directory identity is invalid", "ADVISORY_CAUSAL_SOURCE_INVALID")
    registry_path = _resolve_bound_path(parent_request.registry_path)
    route_path = _resolve_bound_path(parent_request.auxiliary_route_path)
    registry = AdvisoryResearchTrialRegistryV1(registry_path)
    records = registry.read()
    repository = Path(repository_root).resolve()
    commit = _repository_git_commit(repository)
    dirty = _repository_git_dirty_paths(repository)
    if dirty:
        _raise("causal Admission request repository is dirty", "ADVISORY_CAUSAL_REPOSITORY_INVALID")
    request = build_causal_admission_request(
        parent_v1_bundle_path=str(parent_path),
        parent_v1_bundle_id=parent_path.name,
        parent_v1_request_sha256=parent_request.request_sha256,
        parent_v1_manifest_sha256=sha256_file(parent_path / "manifest.json"),
        package_id=parent_request.package_id,
        manifest_sha256=parent_request.manifest_sha256,
        program_id=parent_request.program_id,
        binding_version_id=parent_request.binding_version_id,
        policy_identity=parent_request.policy_identity,
        shadow_policy_sha256=parent_request.shadow_policy_sha256,
        cost_policy_sha256=parent_request.cost_policy_sha256,
        dataset_identity=parent_request.dataset_identity,
        cumulative_evaluated_trial_count_prior=(
            parent_request.cumulative_evaluated_trial_count_prior
            + int(parent_manifest.get("frontier", {}).get("evaluated_trial_count", 3))
        ),
        cumulative_candidate_index_prior=max(parent_request.reserved_candidate_indices),
        registry_path=str(registry_path),
        registry_sha256_at_request=sha256_file(registry_path),
        registry_record_count_at_request=len(records),
        auxiliary_route_path=str(route_path),
        auxiliary_route_sha256_at_request=sha256_file(route_path),
        repository_root=str(repository),
        repository_commit=commit,
        output_root=str(_resolve_bound_path(output_root)),
    )
    target = Path(request_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(request.model_dump(mode="json"), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if target.exists() and target.read_text(encoding="utf-8") != payload:
        _raise("causal Admission request path already has different content", "ADVISORY_CAUSAL_REQUEST_CONFLICT")
    if not target.exists():
        _write_atomic_text(target, payload)
    return request


def run_causal_admission_mve(request_path: str | Path) -> dict[str, Any]:
    started = time.monotonic()
    path = _resolve_bound_path(request_path)
    try:
        request = FrozenAdvisoryCausalAdmissionRequestV2.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, ValidationError) as exc:
        _raise(
            "causal Admission frozen request cannot be read",
            "ADVISORY_CAUSAL_REQUEST_INVALID",
            error_type=type(exc).__name__,
        )
    existing = _find_existing_bundle(request)
    if existing is not None:
        inspected = inspect_causal_admission_bundle(existing)
        delivery = _deliver_bundle(request=request, bundle_path=existing)
        return {**inspected, "delivery": delivery, "exact_retry": True}
    _verify_run_environment(request)
    parent_path = _resolve_bound_path(request.parent_v1_bundle_path)
    if sha256_file(parent_path / "manifest.json") != request.parent_v1_manifest_sha256:
        _raise("parent v1 manifest changed after R1 freeze", "ADVISORY_CAUSAL_SOURCE_IDENTITY_MISMATCH")
    inspect_score_hmm_admission_bundle(parent_path)
    parent_request = FrozenAdvisoryScoreHMMAdmissionRequestV1.model_validate_json(
        (parent_path / "request.json").read_text(encoding="utf-8")
    )
    if parent_request.request_sha256 != request.parent_v1_request_sha256:
        _raise("parent v1 request differs from R1 binding", "ADVISORY_CAUSAL_SOURCE_IDENTITY_MISMATCH")
    stages: list[dict[str, Any]] = []
    stage_start = time.monotonic()
    sources = _load_r1_verified_sources(parent_request)
    score = build_package_score_features(sources["rankings_top50"])
    raw = build_raw_market_shape(
        market_daily=sources["market_daily"],
        benchmark_daily=sources["benchmark_daily"],
        suspend_rows=sources["suspend_rows"],
        pit_snapshot=sources["market_pit_snapshot"],
        calendar=sources["trading_calendar"],
    )
    panel = build_causal_admission_panel(
        score_features=score,
        raw_market_features=raw.features,
        primary_labels=sources["primary_labels"],
    )
    stages.append(_stage("source_and_panel", stage_start, row_count=len(panel)))
    inner_dates = pd.DatetimeIndex(
        panel.loc[
            panel["decision_as_of_trade_date"].between(str(INNER_START), str(INNER_END)), "decision_as_of_trade_date"
        ]
        .sort_values()
        .unique()
    )
    outer_dates = pd.DatetimeIndex(
        panel.loc[
            panel["decision_as_of_trade_date"].between(str(OUTER_START), str(OUTER_END)), "decision_as_of_trade_date"
        ]
        .sort_values()
        .unique()
    )
    if len(inner_dates) != request.expected_inner_days or len(outer_dates) != request.expected_outer_days:
        _raise("causal Admission phase calendar count differs from R0", "ADVISORY_CAUSAL_CLOCK_INVALID")
    predictions: list[pd.DataFrame] = []
    decisions: list[pd.DataFrame] = []
    daily: list[pd.DataFrame] = []
    episodes: list[pd.DataFrame] = []
    fit_receipts: list[dict[str, Any]] = []
    inner_metrics: dict[str, dict[str, Any]] = {}
    stage_start = time.monotonic()
    for arm_id in CAUSAL_ADMISSION_ARM_IDS:
        result = run_chronological_predictions(
            panel=panel, prediction_dates=inner_dates, arm_id=arm_id, request=request, phase="INNER"
        )
        action = build_causal_admission_decisions(result.predictions, request)
        evaluation = evaluate_causal_phase(
            arm_id=arm_id, phase="INNER", decisions=action, sources=sources, request=request
        )
        evaluation.metrics["probability_calibration"] = build_probability_readout(result.predictions, panel)
        predictions.append(result.predictions)
        decisions.append(action)
        if not daily:
            daily.append(evaluation.daily)
            episodes.append(evaluation.episodes)
        else:
            daily.append(evaluation.daily.loc[evaluation.daily["arm_id"].ne(BASELINE_ARM_ID)])
            episodes.append(evaluation.episodes.loc[evaluation.episodes["arm_id"].ne(BASELINE_ARM_ID)])
        fit_receipts.extend(result.fit_receipts)
        inner_metrics[arm_id] = evaluation.metrics
    apply_inner_holm_bonferroni(inner_metrics, familywise_alpha=request.familywise_alpha)
    inner_selected = select_inner_candidate(inner_metrics)
    stages.append(_stage("inner_selection", stage_start, selected_arm_id=inner_selected))
    outer_metrics: dict[str, Any] | None = None
    final_selected: str | None = None
    if inner_selected is not None:
        stage_start = time.monotonic()
        result = run_chronological_predictions(
            panel=panel,
            prediction_dates=outer_dates,
            arm_id=inner_selected,
            request=request,
            phase="OUTER",
        )
        action = build_causal_admission_decisions(result.predictions, request)
        evaluation = evaluate_causal_phase(
            arm_id=inner_selected, phase="OUTER", decisions=action, sources=sources, request=request
        )
        evaluation.metrics["probability_calibration"] = build_probability_readout(result.predictions, panel)
        predictions.append(result.predictions)
        decisions.append(action)
        daily.append(evaluation.daily)
        episodes.append(evaluation.episodes)
        fit_receipts.extend(result.fit_receipts)
        outer_metrics = evaluation.metrics
        outer_pass = bool(
            outer_metrics["eligible"]
            and outer_metrics["daily_net_absolute_lift_lower_bps"] is not None
            and outer_metrics["daily_net_absolute_lift_lower_bps"] > request.minimum_economic_lift_bps
        )
        final_selected = inner_selected if outer_pass else None
        stages.append(_stage("outer_readout", stage_start, selected_arm_id=final_selected))
    if final_selected is not None:
        evidence_class = "CAUSAL_ADMISSION_V2_1_CANDIDATE_SELECTED_NAVIGATION_ONLY"
    elif (inner_selected is not None and outer_metrics is not None and not outer_metrics["support_sufficient"]) or (
        inner_selected is None and any(not item["support_sufficient"] for item in inner_metrics.values())
    ):
        evidence_class = "CAUSAL_ADMISSION_V2_1_EXPLORATORY_INSUFFICIENT_SUPPORT"
    else:
        evidence_class = "CAUSAL_ADMISSION_V2_1_SELECTED_ZERO"
    summary = {
        "schema_version": "advisory_causal_admission_frontier_summary_v1",
        "request_sha256": request.request_sha256,
        "objective_contract": ObjectiveContract.RISK_MANAGED_ADVISORY.value,
        "decision_use": DecisionUse.NAVIGATION_ONLY.value,
        "inner_metrics": [inner_metrics[arm_id] for arm_id in CAUSAL_ADMISSION_ARM_IDS],
        "inner_selected_arm_id": inner_selected,
        "outer_metrics": outer_metrics,
        "selected_arm_id": final_selected,
        "selected_trial_count": int(final_selected is not None),
        "evaluated_trial_count": 2,
        "confirmatory_capable": False,
        "evidence_class": evidence_class,
        "next_task": CAUSAL_ADMISSION_NEXT_TASK_BY_EVIDENCE[evidence_class],
        "sealed_holdout_accessed": False,
        "candidate_reselection_allowed": False,
    }
    summary["summary_sha256"] = canonical_json_sha256(summary)
    resource = {
        "schema_version": "advisory_causal_admission_resource_report_v1",
        "request_sha256": request.request_sha256,
        "stages": stages,
        "total_wall_seconds": time.monotonic() - started,
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
    if resource["peak_rss_bytes"] > request.resource_max_rss_bytes:
        _raise("causal Admission RSS limit exceeded", "ADVISORY_CAUSAL_RESOURCE_LIMIT")
    if len(fit_receipts) * 2 > request.max_estimator_fit_count:
        _raise("causal Admission estimator fit count exceeded", "ADVISORY_CAUSAL_MODEL_INVALID")
    source_preflight = {
        "schema_version": "advisory_causal_admission_source_preflight_v1",
        "request_sha256": request.request_sha256,
        "parent_v1_bundle_id": request.parent_v1_bundle_id,
        "parent_v1_request_sha256": request.parent_v1_request_sha256,
        "panel_rows": len(panel),
        "decision_days": int(panel["decision_as_of_trade_date"].nunique()),
        "inner_days": len(inner_dates),
        "outer_days": len(outer_dates),
        "feature_schema_hash": CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH,
        "normal_missing_preserved": True,
    }
    bundle = _publish_bundle(
        request=request,
        source_preflight=source_preflight,
        fit_receipts=fit_receipts,
        predictions=pd.concat(predictions, ignore_index=True),
        decisions=pd.concat(decisions, ignore_index=True),
        daily=pd.concat(daily, ignore_index=True),
        episodes=pd.concat(episodes, ignore_index=True),
        summary=summary,
        resource_report=resource,
    )
    inspected = inspect_causal_admission_bundle(bundle)
    delivery = _deliver_bundle(request=request, bundle_path=bundle)
    return {**inspected, "delivery": delivery, "exact_retry": False}


def inspect_causal_admission_bundle(bundle_path: str | Path) -> dict[str, Any]:
    path = _resolve_bound_path(bundle_path)
    manifest = _read_json(path / "manifest.json", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    if manifest.get("schema_version") != CAUSAL_BUNDLE_SCHEMA or manifest.get("bundle_id") != path.name:
        _raise("causal Admission manifest identity is invalid", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    manifest_payload = dict(manifest)
    manifest_sha256 = manifest_payload.pop("manifest_sha256", None)
    if manifest_sha256 != canonical_json_sha256(manifest_payload):
        _raise("causal Admission manifest hash differs", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    actual = {item.name for item in path.iterdir() if item.is_file()}
    expected = set(BUNDLE_MEMBERS) | {"manifest.json"}
    if actual != expected:
        _raise("causal Admission bundle members differ", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    descriptors = manifest.get("files", {})
    for name in BUNDLE_MEMBERS:
        descriptor = descriptors.get(name, {})
        if descriptor != {
            "sha256": sha256_file(path / name),
            "size_bytes": (path / name).stat().st_size,
        }:
            _raise("causal Admission bundle member hash differs", "ADVISORY_CAUSAL_BUNDLE_INVALID", file=name)
    core = {name: descriptors[name]["sha256"] for name in sorted(RESULT_MEMBERS - {"frontier_receipt.json"})}
    expected_bundle_id = canonical_json_sha256(
        {
            "schema_version": CAUSAL_BUNDLE_SCHEMA,
            "request_sha256": manifest.get("request_sha256"),
            "core": core,
        }
    )
    try:
        request = FrozenAdvisoryCausalAdmissionRequestV2.model_validate_json(
            (path / "request.json").read_text(encoding="utf-8")
        )
        receipt = CausalAdmissionFrontierReceiptV1.model_validate_json(
            (path / "frontier_receipt.json").read_text(encoding="utf-8")
        )
        summary = _read_json(path / "frontier_summary.json", "ADVISORY_CAUSAL_BUNDLE_INVALID")
        records = tuple(
            AdvisoryResearchTrialRecordV1.model_validate(item)
            for item in _read_json(path / "registry_records.json", "ADVISORY_CAUSAL_BUNDLE_INVALID")
        )
        resource = _read_json(path / "resource_report.json", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    except (OSError, ValueError, ValidationError) as exc:
        _raise(
            "causal Admission bundle contract readback failed",
            "ADVISORY_CAUSAL_BUNDLE_INVALID",
            error_type=type(exc).__name__,
        )
    for record in records:
        for reference in record.evidence_refs:
            reference_path = _resolve_bound_path(reference.artifact_uri)
            if (
                not reference_path.is_file()
                or reference_path.parent != path
                or sha256_file(reference_path) != reference.sha256
                or reference_path.stat().st_size != reference.size_bytes
            ):
                _raise(
                    "causal Admission registry evidence reference differs",
                    "ADVISORY_CAUSAL_BUNDLE_INVALID",
                    role=reference.role,
                )
    bundle_bytes = sum(item.stat().st_size for item in path.iterdir() if item.is_file())
    if (
        expected_bundle_id != path.name
        or request.request_sha256 != manifest.get("request_sha256")
        or receipt.bundle_id != path.name
        or receipt.request_sha256 != request.request_sha256
        or receipt.evidence_class != summary.get("evidence_class")
        or receipt.selected_arm_id != summary.get("selected_arm_id")
        or receipt.selected_trial_count != summary.get("selected_trial_count")
        or receipt.evaluated_trial_count != summary.get("evaluated_trial_count")
        or receipt.next_task != summary.get("next_task")
        or summary.get("summary_sha256")
        != canonical_json_sha256({key: value for key, value in summary.items() if key != "summary_sha256"})
        or len(records) != len(CAUSAL_ADMISSION_ARM_IDS)
        or {item.unique_variable for item in records} != set(CAUSAL_ADMISSION_ARM_IDS)
        or sum(item.evaluated_trial_count for item in records) != receipt.evaluated_trial_count
        or sum(item.selected_trial_count for item in records) != receipt.selected_trial_count
        or any(item.decision_use != DecisionUse.NAVIGATION_ONLY for item in records)
        or any(item.objective_contract != ObjectiveContract.RISK_MANAGED_ADVISORY for item in records)
        or int(resource.get("peak_rss_bytes") or 0) > request.resource_max_rss_bytes
        or bundle_bytes > request.resource_max_temp_bytes
    ):
        _raise("causal Admission bundle closure differs", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    return {
        "bundle_path": str(path),
        "bundle_id": path.name,
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "evidence_class": receipt.evidence_class,
        "selected_arm_id": receipt.selected_arm_id,
        "next_task": receipt.next_task,
    }


def _publish_bundle(
    *,
    request: FrozenAdvisoryCausalAdmissionRequestV2,
    source_preflight: Mapping[str, Any],
    fit_receipts: Sequence[Mapping[str, Any]],
    predictions: pd.DataFrame,
    decisions: pd.DataFrame,
    daily: pd.DataFrame,
    episodes: pd.DataFrame,
    summary: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> Path:
    root = _resolve_bound_path(request.output_root) / "causal_admission_bundles"
    root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".causal-admission-", dir=root))
    try:
        _write_json(temporary / "request.json", request.model_dump(mode="json"))
        _write_json(temporary / "source_preflight.json", source_preflight)
        _write_json(
            temporary / "feature_schema.json",
            {
                "schema_version": CAUSAL_ADMISSION_FEATURE_SCHEMA,
                "feature_columns": list(CAUSAL_ADMISSION_FEATURE_COLUMNS),
                "feature_schema_hash": CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH,
            },
        )
        _write_json(temporary / "fit_receipts.json", list(fit_receipts))
        predictions.to_parquet(temporary / "predictions.parquet", index=False)
        decisions.to_parquet(temporary / "admission_decisions.parquet", index=False)
        daily.to_parquet(temporary / "policy_daily.parquet", index=False)
        episodes.to_parquet(temporary / "policy_episodes.parquet", index=False)
        _write_json(temporary / "frontier_summary.json", summary)
        _write_json(temporary / "resource_report.json", resource_report)
        temporary_bytes = sum(item.stat().st_size for item in temporary.iterdir() if item.is_file())
        if temporary_bytes > request.resource_max_temp_bytes:
            _raise("causal Admission temporary storage limit exceeded", "ADVISORY_CAUSAL_RESOURCE_LIMIT")
        bounded_resource_report = dict(resource_report)
        bounded_resource_report["temporary_bytes"] = temporary_bytes
        _write_json(temporary / "resource_report.json", bounded_resource_report)
        core = {name: sha256_file(temporary / name) for name in sorted(RESULT_MEMBERS - {"frontier_receipt.json"})}
        bundle_id = canonical_json_sha256(
            {"schema_version": CAUSAL_BUNDLE_SCHEMA, "request_sha256": request.request_sha256, "core": core}
        )
        receipt = build_causal_admission_receipt(
            request_sha256=request.request_sha256,
            bundle_id=bundle_id,
            evidence_class=summary["evidence_class"],
            evaluated_trial_count=summary["evaluated_trial_count"],
            selected_trial_count=summary["selected_trial_count"],
            selected_arm_id=summary["selected_arm_id"],
            created_at=request.created_at,
        )
        _write_json(temporary / "frontier_receipt.json", receipt.model_dump(mode="json"))
        records = _build_registry_records(
            request=request,
            receipt=receipt,
            temporary=temporary,
            final_bundle_path=root / bundle_id,
            summary=summary,
        )
        _write_json(temporary / "registry_records.json", [item.model_dump(mode="json") for item in records])
        files = {
            name: {"sha256": sha256_file(temporary / name), "size_bytes": (temporary / name).stat().st_size}
            for name in sorted(BUNDLE_MEMBERS)
        }
        manifest = {
            "schema_version": CAUSAL_BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "frontier": {
                "evidence_class": receipt.evidence_class,
                "evaluated_trial_count": receipt.evaluated_trial_count,
                "selected_trial_count": receipt.selected_trial_count,
            },
            "files": files,
        }
        manifest["manifest_sha256"] = canonical_json_sha256(manifest)
        _write_json(temporary / "manifest.json", manifest)
        if sum(item.stat().st_size for item in temporary.iterdir() if item.is_file()) > request.resource_max_temp_bytes:
            _raise("causal Admission temporary storage limit exceeded", "ADVISORY_CAUSAL_RESOURCE_LIMIT")
        target = root / bundle_id
        if target.exists():
            shutil.rmtree(temporary)
            inspect_causal_admission_bundle(target)
            return target
        os.replace(temporary, target)
        inspect_causal_admission_bundle(target)
        return target
    except Exception:
        if temporary.exists():
            shutil.rmtree(temporary)
        raise


def _build_registry_records(
    *,
    request: FrozenAdvisoryCausalAdmissionRequestV2,
    receipt: CausalAdmissionFrontierReceiptV1,
    temporary: Path,
    final_bundle_path: Path,
    summary: Mapping[str, Any],
) -> tuple[Any, ...]:
    refs = tuple(
        EvidenceReferenceV1(
            role=role,
            artifact_uri=(final_bundle_path / name).resolve().as_posix(),
            sha256=sha256_file(temporary / name),
            size_bytes=(temporary / name).stat().st_size,
        )
        for role, name in (
            ("causal_admission_receipt", "frontier_receipt.json"),
            ("causal_admission_summary", "frontier_summary.json"),
        )
    )
    records = []
    for arm_id in CAUSAL_ADMISSION_ARM_IDS:
        windows = [
            ConsumedWindowV1(
                window_id="CAUSAL_ADMISSION_V2_1_INNER",
                dataset_identity=request.dataset_identity,
                start_date=request.inner_start,
                end_date=request.inner_end,
            )
        ]
        if summary["inner_selected_arm_id"] == arm_id:
            windows.append(
                ConsumedWindowV1(
                    window_id="CAUSAL_ADMISSION_V2_1_OUTER",
                    dataset_identity=request.dataset_identity,
                    start_date=request.outer_start,
                    end_date=request.outer_end,
                )
            )
        records.append(
            build_trial_record(
                experiment_id=CAUSAL_ADMISSION_EXPERIMENT_ID,
                attempt_id=request.request_id,
                research_stage=CAUSAL_ADMISSION_STAGE,
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                hypothesis_family_id=CAUSAL_ADMISSION_FAMILY_ID,
                parent_lineage=("ADVISORY-N3-AUX-SCORE-HMM-ADMISSION-V1",),
                unique_variable=arm_id,
                objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
                dataset_identity=request.dataset_identity,
                schema_identity=CAUSAL_ADMISSION_FEATURE_SCHEMA_HASH,
                policy_identity=request.policy_identity,
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                selected_trial_count=int(summary["selected_arm_id"] == arm_id),
                consumed_windows=tuple(windows),
                result_class=ResearchResultClass.EXPLORATORY,
                decision_use=DecisionUse.NAVIGATION_ONLY,
                evidence_refs=refs,
                recorded_at=receipt.created_at,
            )
        )
    return tuple(records)


def _deliver_bundle(*, request: FrozenAdvisoryCausalAdmissionRequestV2, bundle_path: Path) -> dict[str, Any]:
    payload = _read_json(bundle_path / "registry_records.json", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    registry = AdvisoryResearchTrialRegistryV1(_resolve_bound_path(request.registry_path))
    append = registry.append_batch(payload)
    receipt = CausalAdmissionFrontierReceiptV1.model_validate_json(
        (bundle_path / "frontier_receipt.json").read_text(encoding="utf-8")
    )
    route_path = _resolve_bound_path(request.auxiliary_route_path)
    route = (
        "# Advisory auxiliary research route\n\n"
        f"- experiment_id: `{CAUSAL_ADMISSION_EXPERIMENT_ID}`\n"
        f"- request_sha256: `{request.request_sha256}`\n"
        f"- bundle_id: `{bundle_path.name}`\n"
        f"- evidence_class: `{receipt.evidence_class}`\n"
        f"- next_task: `{receipt.next_task}`\n"
        f"- selected_arm_id: `{receipt.selected_arm_id or 'NONE'}`\n"
        "- decision_use: `NAVIGATION_ONLY`\n"
        "- runtime_activation: `false`\n"
    )
    prior_hash = sha256_file(route_path)
    if prior_hash != request.auxiliary_route_sha256_at_request and route_path.read_text(encoding="utf-8") != route:
        _raise("auxiliary route changed after request freeze", "ADVISORY_CAUSAL_ROUTE_CONFLICT")
    route_status = "EXACT_NOOP"
    if route_path.read_text(encoding="utf-8") != route:
        _write_atomic_text(route_path, route)
        route_status = "WRITTEN"
    return {
        "registry": append,
        "auxiliary_route_path": str(route_path),
        "auxiliary_route_status": route_status,
        "next_task": receipt.next_task,
    }


def _find_existing_bundle(request: FrozenAdvisoryCausalAdmissionRequestV2) -> Path | None:
    root = _resolve_bound_path(request.output_root) / "causal_admission_bundles"
    if not root.exists():
        return None
    matches: list[Path] = []
    for candidate in root.iterdir():
        if not candidate.is_dir() or candidate.name.startswith(".") or not (candidate / "request.json").is_file():
            continue
        try:
            existing = FrozenAdvisoryCausalAdmissionRequestV2.model_validate_json(
                (candidate / "request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError, ValidationError) as exc:
            _raise(
                "causal Admission bundle request is corrupt",
                "ADVISORY_CAUSAL_BUNDLE_INVALID",
                bundle_path=str(candidate),
                error_type=type(exc).__name__,
            )
        if existing.request_sha256 == request.request_sha256:
            matches.append(candidate)
    if len(matches) > 1:
        _raise("multiple causal Admission bundles share one request", "ADVISORY_CAUSAL_BUNDLE_INVALID")
    return matches[0] if matches else None


def _verify_run_environment(request: FrozenAdvisoryCausalAdmissionRequestV2) -> None:
    repository = Path(request.repository_root)
    if _repository_git_commit(repository) != request.repository_commit:
        _raise("causal Admission repository commit differs", "ADVISORY_CAUSAL_REPOSITORY_INVALID")
    if _repository_git_dirty_paths(repository):
        _raise("causal Admission repository is dirty", "ADVISORY_CAUSAL_REPOSITORY_INVALID")
    registry = _resolve_bound_path(request.registry_path)
    route = _resolve_bound_path(request.auxiliary_route_path)
    if sha256_file(registry) != request.registry_sha256_at_request:
        _raise("trial registry changed after request freeze", "ADVISORY_CAUSAL_REGISTRY_CONFLICT")
    if sha256_file(route) != request.auxiliary_route_sha256_at_request:
        _raise("auxiliary route changed after request freeze", "ADVISORY_CAUSAL_ROUTE_CONFLICT")


def _load_r1_verified_sources(
    parent_request: FrozenAdvisoryScoreHMMAdmissionRequestV1,
) -> dict[str, Any]:
    """Revalidate immutable v1 data while rebinding its mutable control plane in R1."""

    retained = tuple(
        reference
        for reference in parent_request.evidence_refs
        if reference.role not in MUTABLE_PARENT_CONTROL_EVIDENCE_ROLES
    )
    removed = {reference.role for reference in parent_request.evidence_refs} - {
        reference.role for reference in retained
    }
    if removed != MUTABLE_PARENT_CONTROL_EVIDENCE_ROLES:
        _raise(
            "parent v1 mutable control evidence roles differ",
            "ADVISORY_CAUSAL_SOURCE_INVALID",
            removed_roles=sorted(removed),
        )
    return _load_verified_sources(parent_request.model_copy(update={"evidence_refs": retained}))


def _repository_git_command(repository: Path) -> tuple[list[str], Path]:
    command, root = _git_command_for_worktree(repository)
    normalized_root = root.as_posix().lower()
    if os.name != "nt" and normalized_root.startswith("/mnt/") and "core.autocrlf=true" not in command:
        command = [
            command[0],
            "-c",
            "core.fileMode=false",
            "-c",
            "core.autocrlf=true",
            *command[1:],
        ]
    return command, root


def _repository_git_commit(repository: Path) -> str:
    command, root = _repository_git_command(repository)
    return _run_repository_git(command, root, "rev-parse", "HEAD").strip().lower()


def _repository_git_dirty_paths(repository: Path) -> list[str]:
    command, root = _repository_git_command(repository)
    output = _run_repository_git(command, root, "status", "--porcelain", "--untracked-files=all")
    return [line[3:] if len(line) > 3 else line for line in output.splitlines() if line.strip()]


def _run_repository_git(command: Sequence[str], root: Path, *args: str) -> str:
    import subprocess

    result = subprocess.run([*command, *args], cwd=root, text=True, capture_output=True, check=False)
    if result.returncode != 0:
        _raise(
            "causal Admission git identity command failed",
            "ADVISORY_CAUSAL_REPOSITORY_INVALID",
            stderr=result.stderr,
        )
    return result.stdout.rstrip("\r\n")


def _read_json(path: Path, reason_code: str) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        _raise("JSON artifact cannot be read", reason_code, path=str(path), error_type=type(exc).__name__)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _write_atomic_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _stage(name: str, started: float, **facts: Any) -> dict[str, Any]:
    return {"name": name, "elapsed_seconds": time.monotonic() - started, **facts}


def _model_pair(request: FrozenAdvisoryCausalAdmissionRequestV2) -> tuple[Pipeline, Pipeline]:
    return (
        Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                ("model", Ridge(alpha=request.ridge_alpha, solver=request.ridge_solver, fit_intercept=True)),
            ]
        ),
        Pipeline(
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
        ),
    )


def _cvar_5(values: pd.Series) -> float:
    finite = pd.to_numeric(values, errors="coerce").dropna().sort_values()
    if finite.empty:
        return float("nan")
    count = max(1, int(math.ceil(len(finite) * 0.05)))
    return float(finite.iloc[:count].mean())


def _optional_mean(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame:
        return None
    finite = pd.to_numeric(frame[column], errors="coerce").dropna()
    return float(finite.mean()) if len(finite) else None


def _frame_sha256(frame: pd.DataFrame) -> str:
    keys = [column for column in ("phase", "arm_id", "decision_as_of_trade_date", "instrument") if column in frame]
    value = frame.sort_values(keys).reset_index(drop=True) if keys else frame.reset_index(drop=True)
    value = value.sort_index(axis=1)
    digest = hashlib.sha256()
    digest.update(json.dumps(list(map(str, value.columns)), separators=(",", ":")).encode())
    digest.update(pd.util.hash_pandas_object(value, index=True).to_numpy().tobytes())
    return digest.hexdigest()


def _moving_block_one_sided_p_value(
    values: Sequence[float] | np.ndarray,
    *,
    threshold: float,
    block_length: int,
    repetitions: int,
    seed: int,
) -> float:
    """Test mean(values) > threshold with a moving-block null bootstrap."""

    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if not len(array):
        return 1.0
    observed = float(array.mean() - threshold)
    if len(array) == 1:
        return 0.0 if observed > 0.0 else 1.0
    null = array - array.mean() + threshold
    rng = np.random.default_rng(seed)
    block = max(1, min(int(block_length), len(null)))
    blocks_needed = int(math.ceil(len(null) / block))
    starts = rng.integers(0, len(null), size=(repetitions, blocks_needed))
    offsets = np.arange(block)
    indexes = (starts[:, :, None] + offsets[None, None, :]) % len(null)
    samples = null[indexes.reshape(repetitions, -1)[:, : len(null)]]
    null_statistics = samples.mean(axis=1) - threshold
    return float((np.count_nonzero(null_statistics >= observed) + 1) / (repetitions + 1))


def _raise(message: str, reason_code: str, **context: Any) -> NoReturn:
    raise AdvisoryModelFirstError(message, reason_code=reason_code, context=context or None)
