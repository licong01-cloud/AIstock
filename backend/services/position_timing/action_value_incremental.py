"""Immutable history-first comparison for one frozen optional information block.

Each request compares one explicitly supported block against a newly trained
matched core policy.  It has no API, card, alert, serving, database-write, or
automatic-trading path.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import date, datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
from backend.services.advisory_model_first.research_control_contracts import (
    ConsumedWindowV1,
    DecisionUse,
    EvidenceReferenceV1,
    ObjectiveContract,
    ResearchResultClass,
    ResearchStudyType,
    build_trial_record,
)

from .action_value import (
    ATR14_INFORMATION_BLOCK,
    CORE_INFORMATION_BLOCK,
    FEATURE_ORDER,
    MARKET_FEATURES,
    MONEYFLOW_INFORMATION_BLOCK,
    MONEYFLOW_MARKET_FEATURES,
    SW_L2_INFORMATION_BLOCK,
    SW_L2_MARKET_FEATURES,
    TZ,
    ActionValueError,
    feature_contract,
    policy_sha256_for,
)
from .action_value_corporate_actions import CorporateActionBook, freeze_corporate_action_snapshot
from .action_value_data import DailyCandidate, file_reference
from .action_value_moneyflow import MoneyflowAugmentedCandidate
from .action_value_sector import SectorAugmentedCandidate
from .action_value_pipeline import _clean_repository_commit
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY,
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
    REFERENCE_CAPITAL_CNY,
    ActionValuePopulationSpec,
    circular_block_interval,
    classify_effect,
    build_action_value_rows,
    deterministic_symbols,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .action_value_suspensions import (
    SuspensionSnapshotBook,
    freeze_suspension_snapshot,
)
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V1"
REQUEST_SCHEMA = "position_timing_action_value_increment_request_v3"
LEGACY_REQUEST_SCHEMAS = {
    "position_timing_action_value_increment_request_v1",
    "position_timing_action_value_increment_request_v2",
}
RECEIPT_SCHEMA = "position_timing_action_value_increment_receipt_v2"
LEGACY_RECEIPT_SCHEMA = "position_timing_action_value_increment_receipt_v1"
BUNDLE_SCHEMA = "position_timing_action_value_increment_bundle_v1"
ARTIFACT_FOLDER = "action_value_incremental_v1"
BOOTSTRAP_SAMPLES = 5_000
BOOTSTRAP_BLOCK_SESSIONS = 25
BOOTSTRAP_SEED = 20260908
ARTIFACT_FILES = (
    "request.json",
    "coverage.json",
    "core_oof_action_predictions.parquet",
    "atr14_oof_action_predictions.parquet",
    "core_continuous_sleeve_days.parquet",
    "atr14_continuous_sleeve_days.parquet",
    "paired_daily_increment.parquet",
    "receipt.json",
)


@dataclass(frozen=True)
class IncrementProfile:
    information_block: str
    added_features: tuple[str, ...]
    hypothesis: str
    main_comparison: str
    estimand: str
    artifact_prefix: str
    evidence_role: str
    experiment_id: str


ATR14_PROFILE = IncrementProfile(
    information_block=ATR14_INFORMATION_BLOCK,
    added_features=("atr14_sma_bps",),
    hypothesis="CORE_PLUS_ATR14_POLICY_MINUS_MATCHED_CORE_POLICY",
    main_comparison="CORE_PLUS_ATR14_MINUS_MATCHED_CORE",
    estimand="CORE_PLUS_ATR14_POLICY_MINUS_MATCHED_CORE_POLICY_DAILY_BPS",
    artifact_prefix="atr14",
    evidence_role="position_timing_action_value_atr14_increment_receipt",
    experiment_id="position_timing_action_value_atr14_sma_gap_range_v1",
)
SW_L2_PROFILE = IncrementProfile(
    information_block=SW_L2_INFORMATION_BLOCK,
    added_features=tuple(name for name in SW_L2_MARKET_FEATURES if name not in MARKET_FEATURES),
    hypothesis="CORE_PLUS_SW_L2_RELATIVE_MOMENTUM_POLICY_MINUS_MATCHED_CORE_POLICY",
    main_comparison="CORE_PLUS_SW_L2_RELATIVE_MOMENTUM_MINUS_MATCHED_CORE",
    estimand="CORE_PLUS_SW_L2_RELATIVE_MOMENTUM_POLICY_MINUS_MATCHED_CORE_POLICY_DAILY_BPS",
    artifact_prefix="sw_l2",
    evidence_role="position_timing_action_value_sw_l2_increment_receipt",
    experiment_id="position_timing_action_value_sw_l2_relative_momentum_20d_v1",
)
MONEYFLOW_PROFILE = IncrementProfile(
    information_block=MONEYFLOW_INFORMATION_BLOCK,
    added_features=tuple(name for name in MONEYFLOW_MARKET_FEATURES if name not in MARKET_FEATURES),
    hypothesis="CORE_PLUS_MAIN_NET_FLOW_RATIO_5D_LAG1_POLICY_MINUS_MATCHED_CORE_POLICY",
    main_comparison="CORE_PLUS_MAIN_NET_FLOW_RATIO_5D_LAG1_MINUS_MATCHED_CORE",
    estimand="CORE_PLUS_MAIN_NET_FLOW_RATIO_5D_LAG1_POLICY_MINUS_MATCHED_CORE_POLICY_DAILY_BPS",
    artifact_prefix="moneyflow_5d_lag1",
    evidence_role="position_timing_action_value_moneyflow_5d_lag1_increment_receipt",
    experiment_id="position_timing_action_value_main_net_flow_ratio_5d_lag1_v1",
)
PROFILES = {
    ATR14_PROFILE.information_block: ATR14_PROFILE,
    SW_L2_PROFILE.information_block: SW_L2_PROFILE,
    MONEYFLOW_PROFILE.information_block: MONEYFLOW_PROFILE,
}


def _profile(information_block: str) -> IncrementProfile:
    try:
        return PROFILES[information_block]
    except KeyError as exc:
        raise ActionValueError("INFORMATION_BLOCK_UNSUPPORTED", information_block=information_block) from exc


def _artifact_files(profile: IncrementProfile) -> tuple[str, ...]:
    return (
        "request.json",
        "coverage.json",
        "core_oof_action_predictions.parquet",
        f"{profile.artifact_prefix}_oof_action_predictions.parquet",
        "core_continuous_sleeve_days.parquet",
        f"{profile.artifact_prefix}_continuous_sleeve_days.parquet",
        "paired_daily_increment.parquet",
        "receipt.json",
    )


def prepare_increment_request(
    *,
    candidate_root: Path,
    timing_root: Path,
    repository_root: Path,
    historical_registry: Path,
    population_start: date,
    population_end: date,
    symbol_limit: int = 64,
    review_stride: int = 10,
    information_block: str = ATR14_INFORMATION_BLOCK,
) -> Path:
    """Freeze source-only coverage before any label or outcome is read."""

    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    profile = _profile(information_block)
    base_candidate = DailyCandidate.open(candidate_root)
    candidate = _research_candidate(base_candidate, profile)
    spec = ActionValuePopulationSpec(
        start=population_start,
        end=population_end,
        symbol_limit=symbol_limit,
        review_stride=review_stride,
    )
    selected_symbols = deterministic_symbols(candidate.symbols, limit=symbol_limit, seed=spec.seed)
    if not historical_registry.is_file():
        raise ActionValueError("HISTORICAL_REGISTRY_UNAVAILABLE")

    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False) as connection:
        connection.set_session(
            isolation_level="REPEATABLE READ",
            readonly=True,
            autocommit=False,
        )
        corporate_action_path = freeze_corporate_action_snapshot(
            connection,
            symbols=selected_symbols,
            start=population_start,
            end=population_end,
            timing_root=timing_root,
        )
        suspension_path = freeze_suspension_snapshot(
            connection,
            symbols=selected_symbols,
            start=population_start,
            end=population_end,
            timing_root=timing_root,
        )
    corporate_action_ref = file_reference(corporate_action_path)
    suspension_ref = file_reference(suspension_path)
    candidate, _ = _apply_suspension_snapshot(candidate, suspension_path)
    source_coverage = _source_coverage(candidate, selected_symbols, profile)
    _require_matched_source_coverage(source_coverage, information_block=profile.information_block)
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "candidate_root": candidate.root.as_posix(),
        "timing_root": timing_root.resolve().as_posix(),
        "research_evidence_clock": "HISTORICAL_CAUSAL_REPLAY_PRIMARY_PROSPECTIVE_NONBLOCKING",
        "information_block": profile.information_block,
        "hypothesis": profile.hypothesis,
        "planned_trial_count": 1,
        "feature_contract": {
            "block_id": profile.information_block,
            "added_features": profile.added_features,
            "feature_order": feature_contract(profile.information_block)[1],
            "feature_spec_sha256": feature_contract(profile.information_block)[2],
            "policy_sha256": policy_sha256_for(profile.information_block),
        },
        "matched_core_contract": {
            "information_block": CORE_INFORMATION_BLOCK,
            "feature_order": FEATURE_ORDER,
            "feature_spec_sha256": feature_contract(CORE_INFORMATION_BLOCK)[2],
            "policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
        },
        "initial_holding_contract": {
            "policy": EXOGENOUS_INITIAL_HOLDING_POLICY,
            "policy_sha256": EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
        },
        "population_spec": {
            "start": population_start.isoformat(),
            "end": population_end.isoformat(),
            "symbol_limit": symbol_limit,
            "review_stride": review_stride,
            "seed": spec.seed,
            "primary_horizon": spec.primary_horizon,
            "terminal_max_defer": spec.terminal_max_defer,
            "reference_capital_cny": str(spec.reference_capital_cny),
            "selected_symbols": selected_symbols,
            "selection": _selection_contract(profile),
        },
        "training_spec": {
            "initial_sessions": 756,
            "retrain": "MONTH_END_EXPANDING",
            "estimator": "LIGHTGBM_GBDT_V2_ENTRY_EXIT_FROZEN",
            "bootstrap_block_sessions": BOOTSTRAP_BLOCK_SESSIONS,
            "bootstrap_samples": BOOTSTRAP_SAMPLES,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "main_comparison": profile.main_comparison,
            "interval_level": 0.95,
            "economic_threshold_bps": 0.0,
        },
        "source_coverage": source_coverage,
        "corporate_action_snapshot": corporate_action_ref,
        "suspension_snapshot": suspension_ref,
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "historical_registry": file_reference(historical_registry),
        "historical_registry_context_count": len(
            AdvisoryResearchTrialRegistryV1(historical_registry).read()
        ),
        "global_registry_write": False,
        "database_write": False,
        "runtime_write": False,
        "order_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = (
        timing_root.resolve()
        / "research"
        / ARTIFACT_FOLDER
        / "requests"
        / f"{request['request_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def run_increment_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    profile = _profile(request["information_block"])
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    global_registry = Path(request["historical_registry"]["path"])
    global_before = file_reference(global_registry)
    if bundle.exists():
        inspected = inspect_increment_bundle(bundle)
        registry = _deliver_registry(request, bundle, inspected["receipt"])
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspected,
            "registry": registry,
            "global_registry_observation": _global_observation(global_before, global_registry),
        }

    if request["schema_version"] != REQUEST_SCHEMA:
        raise ActionValueError("LEGACY_INCREMENT_REQUEST_REPLAY_UNSUPPORTED")

    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("ACTION_VALUE_CODE_IDENTITY_MISMATCH")
    base_candidate = DailyCandidate.open(Path(request["candidate_root"]))
    candidate = _research_candidate(base_candidate, profile)
    selected_symbols = tuple(request["population_spec"]["selected_symbols"])
    expected_symbols = deterministic_symbols(
        candidate.symbols,
        limit=int(request["population_spec"]["symbol_limit"]),
        seed=int(request["population_spec"]["seed"]),
    )
    if selected_symbols != expected_symbols:
        raise ActionValueError("INCREMENT_POPULATION_SELECTION_MISMATCH")
    suspensions = None
    if request["schema_version"] == REQUEST_SCHEMA:
        suspension_ref = request.get("suspension_snapshot")
        if not isinstance(suspension_ref, Mapping) or "path" not in suspension_ref:
            raise ActionValueError("SUSPENSION_SNAPSHOT_NOT_BOUND")
        suspension_path = Path(str(suspension_ref["path"]))
        if file_reference(suspension_path) != suspension_ref:
            raise ActionValueError("SUSPENSION_SNAPSHOT_REFERENCE_MISMATCH")
        candidate, suspensions = _apply_suspension_snapshot(candidate, suspension_path)
        expected_suspension_scope = (
            tuple(sorted(selected_symbols)),
            date.fromisoformat(request["population_spec"]["start"]),
            date.fromisoformat(request["population_spec"]["end"]),
        )
        if (suspensions.symbols, suspensions.start, suspensions.end) != expected_suspension_scope:
            raise ActionValueError("SUSPENSION_SNAPSHOT_SCOPE_MISMATCH")
    source_coverage = _source_coverage(candidate, selected_symbols, profile)
    if canonical_sha256(source_coverage) != canonical_sha256(request["source_coverage"]):
        raise ActionValueError("INCREMENT_SOURCE_COVERAGE_IDENTITY_MISMATCH")
    _require_matched_source_coverage(source_coverage, information_block=profile.information_block)
    corporate_action_ref = request["corporate_action_snapshot"]
    if file_reference(Path(corporate_action_ref["path"])) != corporate_action_ref:
        raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_REFERENCE_MISMATCH")
    corporate_actions = CorporateActionBook.open(Path(corporate_action_ref["path"]))

    spec = ActionValuePopulationSpec(
        start=date.fromisoformat(request["population_spec"]["start"]),
        end=date.fromisoformat(request["population_spec"]["end"]),
        symbol_limit=int(request["population_spec"]["symbol_limit"]),
        review_stride=int(request["population_spec"]["review_stride"]),
    )
    augmented = build_action_value_rows(
        candidate,
        spec,
        corporate_actions=corporate_actions,
        information_block=profile.information_block,
    )
    core_rows = augmented.rows.drop(columns=list(profile.added_features))
    if tuple(core_rows.columns.intersection(FEATURE_ORDER)) != FEATURE_ORDER:
        raise ActionValueError("MATCHED_CORE_FEATURE_ORDER_MISMATCH")
    source_identity = {
        "source_coverage_sha256": source_coverage["source_sha256"],
        "corporate_action_snapshot_sha256": corporate_actions.snapshot_sha256,
    }
    if suspensions is not None:
        source_identity["suspension_snapshot_sha256"] = suspensions.snapshot_sha256
    derived_identity = {
        "population_coverage_sha256": augmented.coverage["coverage_sha256"],
        "matched_row_identity_sha256": _matched_row_identity(augmented.rows),
    }
    source_sha256 = canonical_sha256(source_identity)
    calendar = [day.date() for day in candidate.calendar]
    common = {
        "calendar": calendar,
        "source_sha256": source_sha256,
        "request_sha256": request["request_sha256"],
        "source_commit": request["repository_commit"],
    }
    core_forward = walk_forward_action_values(
        core_rows,
        **common,
        information_block=CORE_INFORMATION_BLOCK,
    )
    optional_forward = walk_forward_action_values(
        augmented.rows,
        **common,
        information_block=profile.information_block,
    )
    replay_args = {
        "candidate": candidate,
        "symbols": selected_symbols,
        "corporate_actions": corporate_actions,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "block_sessions": BOOTSTRAP_BLOCK_SESSIONS,
        "seed": BOOTSTRAP_SEED,
        "initial_holding_policy_id": EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    }
    core_replay = replay_continuous_cohorts(
        models=core_forward.models,
        information_block=CORE_INFORMATION_BLOCK,
        **replay_args,
    )
    optional_replay = replay_continuous_cohorts(
        models=optional_forward.models,
        information_block=profile.information_block,
        **replay_args,
    )
    try:
        paired_daily, comparison = _paired_policy_comparison(
            core_replay.sleeve_days,
            optional_replay.sleeve_days,
            optional_column=f"{profile.artifact_prefix}_policy_wealth_cny",
            estimand=profile.estimand,
        )
    except ActionValueError as exc:
        if exc.code != "INCREMENT_POLICY_PATH_IDENTITY_MISMATCH":
            raise
        paired_daily, comparison = _path_identity_inconclusive(
            estimand=profile.estimand,
            differences=exc.details,
            core_excluded=core_replay.receipt["excluded"],
            optional_excluded=optional_replay.receipt["excluded"],
        )
    coverage_support = bool(
        core_replay.receipt["coverage_can_support_policy"]
        and optional_replay.receipt["coverage_can_support_policy"]
    )
    if not coverage_support:
        comparison["effect_evidence_before_coverage_constraint"] = comparison["effect_evidence"]
        comparison["effect_evidence"] = "INCONCLUSIVE"
        comparison["coverage_reason_code"] = "SOURCE_OR_CORPORATE_ACTION_PATH_UNAVAILABLE"

    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "repository_commit": request["repository_commit"],
        "completed_at": datetime.now(TZ).isoformat(),
        "source_identity": source_identity,
        "source_sha256": source_sha256,
        "derived_identity": derived_identity,
        "information_block": profile.information_block,
        "feature_spec_sha256": feature_contract(profile.information_block)[2],
        "augmented_policy_sha256": policy_sha256_for(profile.information_block),
        "matched_core_policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
        "initial_holding_policy_sha256": EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256,
        "planned_trial_count": 1,
        "generated_trial_count": 1,
        "evaluated_trial_count": 1,
        "selected_trial_count": int(comparison["effect_evidence"] == "SUPPORTED"),
        "population": augmented.coverage,
        "core_walk_forward": core_forward.diagnostics,
        "optional_walk_forward": optional_forward.diagnostics,
        "core_continuous": core_replay.receipt,
        "optional_continuous": optional_replay.receipt,
        "incremental_comparison": comparison,
        "effect_evidence": comparison["effect_evidence"],
        "serving_status": "RESEARCH_ONLY_NO_RUNTIME_MODEL",
        "runtime_card_changed": False,
        "automatic_trading": False,
        "global_registry_written": False,
        "database_written": False,
    }
    if request.get("source_correction"):
        receipt["source_correction"] = request["source_correction"]
    if profile.information_block == ATR14_INFORMATION_BLOCK:
        receipt["atr14_walk_forward"] = optional_forward.diagnostics
        receipt["atr14_continuous"] = optional_replay.receipt
    receipt["receipt_sha256"] = canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    )
    _publish_profile_bundle(
        bundle,
        profile=profile,
        request=request,
        coverage={
            "source_coverage": source_coverage,
            "population_coverage": augmented.coverage,
            "source_identity": source_identity,
        },
        core_oof=core_forward.predictions,
        optional_oof=optional_forward.predictions,
        core_sleeves=core_replay.sleeve_days,
        optional_sleeves=optional_replay.sleeve_days,
        paired_daily=paired_daily,
        receipt=receipt,
    )
    inspected = inspect_increment_bundle(bundle)
    registry = _deliver_registry(request, bundle, inspected["receipt"])
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspected,
        "registry": registry,
        "global_registry_observation": _global_observation(global_before, global_registry),
    }


def _paired_policy_comparison(
    core_sleeves: pd.DataFrame,
    atr_sleeves: pd.DataFrame,
    *,
    optional_column: str = "atr14_policy_wealth_cny",
    estimand: str = "CORE_PLUS_ATR14_POLICY_MINUS_MATCHED_CORE_POLICY_DAILY_BPS",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compare bounded research paths with one vectorized one-to-one join.

    Each input has at most one BUY_AND_HOLD row per (sleeve_id, valuation_date);
    duplicate checks plus ``validate="one_to_one"`` prevent row multiplication.
    The frozen 64-symbol research population is currently about 270k rows per
    side, so a second batching or distributed-join layer is not justified.
    """

    keys = ["sleeve_id", "valuation_date"]
    core = core_sleeves.loc[
        core_sleeves["baseline"].eq("BUY_AND_HOLD"),
        [*keys, "policy_wealth_cny"],
    ].rename(columns={"policy_wealth_cny": "core_policy_wealth_cny"})
    atr = atr_sleeves.loc[
        atr_sleeves["baseline"].eq("BUY_AND_HOLD"),
        [*keys, "policy_wealth_cny"],
    ].rename(columns={"policy_wealth_cny": optional_column})
    if core.duplicated(keys).any() or atr.duplicated(keys).any():
        raise ActionValueError("INCREMENT_POLICY_PATH_DUPLICATE")
    try:
        paired = core.merge(atr, on=keys, how="outer", validate="one_to_one", indicator=True)
    except pd.errors.MergeError as exc:
        raise ActionValueError("INCREMENT_POLICY_PATH_IDENTITY_MISMATCH") from exc
    if not paired["_merge"].eq("both").all():
        core_only = paired.loc[paired["_merge"].eq("left_only"), keys]
        optional_only = paired.loc[paired["_merge"].eq("right_only"), keys]
        raise ActionValueError(
            "INCREMENT_POLICY_PATH_IDENTITY_MISMATCH",
            core_only_count=int(len(core_only)),
            optional_only_count=int(len(optional_only)),
            core_only_sleeves=sorted(core_only["sleeve_id"].astype(str).unique())[:10],
            optional_only_sleeves=sorted(optional_only["sleeve_id"].astype(str).unique())[:10],
            core_only_dates=sorted(core_only["valuation_date"].astype(str).unique())[:10],
            optional_only_dates=sorted(optional_only["valuation_date"].astype(str).unique())[:10],
        )
    paired = paired.drop(columns="_merge").sort_values(keys).reset_index(drop=True)
    paired["wealth_difference_cny"] = (
        paired[optional_column] - paired["core_policy_wealth_cny"]
    )
    paired["incremental_net_value_cny"] = paired.groupby("sleeve_id")[
        "wealth_difference_cny"
    ].diff()
    paired["incremental_net_value_cny"] = paired["incremental_net_value_cny"].fillna(
        paired["wealth_difference_cny"]
    )
    daily = paired.groupby("valuation_date", as_index=False).agg(
        incremental_net_value_cny=("incremental_net_value_cny", "sum"),
        sleeve_count=("sleeve_id", "nunique"),
    )
    if daily["sleeve_count"].nunique() != 1:
        raise ActionValueError("INCREMENT_DAILY_POPULATION_DRIFT")
    daily["incremental_net_value_bps"] = (
        daily["incremental_net_value_cny"]
        / (daily["sleeve_count"] * float(REFERENCE_CAPITAL_CNY))
        * 10_000
    )
    values = daily["incremental_net_value_bps"].to_numpy(float)
    interval = circular_block_interval(
        values,
        block_sessions=BOOTSTRAP_BLOCK_SESSIONS,
        samples=BOOTSTRAP_SAMPLES,
        seed=BOOTSTRAP_SEED,
        alpha=0.05,
    )
    comparison = {
        "estimand": estimand,
        "daily_mean_incremental_bps": float(values.mean()),
        "period_cumulative_incremental_bps": float(values.sum()),
        "interval_level": 0.95,
        "interval_bps": interval,
        "mde_bps": max(
            interval["point_bps"] - interval["lower_bps"],
            interval["upper_bps"] - interval["point_bps"],
        ),
        "economic_threshold_bps": 0.0,
        "effect_evidence": classify_effect(interval["lower_bps"], interval["upper_bps"]),
        "power_status": "NOT_COMPUTABLE",
        "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
        "effective_trading_days": int(len(values)),
        "sleeve_count": int(daily["sleeve_count"].iloc[0]),
        "paired_path_rows": int(len(paired)),
        "planned_trial_count": 1,
    }
    comparison["comparison_sha256"] = canonical_sha256(comparison)
    return daily, comparison


def _path_identity_inconclusive(
    *,
    estimand: str,
    differences: Mapping[str, Any],
    core_excluded: Mapping[str, Any],
    optional_excluded: Mapping[str, Any],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Retain asymmetric paths without estimating on a selected intersection."""

    comparison = {
        "estimand": estimand,
        "daily_mean_incremental_bps": None,
        "period_cumulative_incremental_bps": None,
        "interval_level": 0.95,
        "interval_bps": None,
        "mde_bps": None,
        "economic_threshold_bps": 0.0,
        "effect_evidence": "INCONCLUSIVE",
        "effect_reason_code": "ASYMMETRIC_POLICY_PATH_UNAVAILABLE",
        "power_status": "NOT_COMPUTABLE",
        "power_reason_code": "MATCHED_POLICY_PATH_IDENTITY_UNAVAILABLE",
        "effective_trading_days": 0,
        "sleeve_count": 0,
        "paired_path_rows": 0,
        "planned_trial_count": 1,
        "path_identity_status": "MISMATCH_NO_INTERSECTION_ESTIMATE",
        "path_identity_differences": dict(differences),
        "core_excluded": dict(core_excluded),
        "optional_excluded": dict(optional_excluded),
    }
    comparison["comparison_sha256"] = canonical_sha256(comparison)
    empty = pd.DataFrame(
        columns=("valuation_date", "incremental_net_value_cny", "sleeve_count", "incremental_net_value_bps")
    )
    return empty, comparison


def inspect_increment_bundle(bundle: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
        request = json.loads((bundle / "request.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("INCREMENT_BUNDLE_UNAVAILABLE") from exc
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or request.get("request_sha256") != manifest.get("request_sha256")
        or receipt.get("receipt_sha256") != manifest.get("receipt_sha256")
    ):
        raise ActionValueError("INCREMENT_BUNDLE_IDENTITY_MISMATCH")
    profile = _profile(request.get("information_block", ""))
    for name in _artifact_files(profile):
        expected = manifest["files"].get(name)
        observed = file_reference(bundle / name)
        observed.pop("path", None)
        if expected != observed:
            raise ActionValueError("INCREMENT_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    _load_request(bundle / "request.json")
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    expected_receipt_schema = (
        RECEIPT_SCHEMA
        if request.get("schema_version") == REQUEST_SCHEMA
        else LEGACY_RECEIPT_SCHEMA
    )
    if (
        receipt.get("schema_version") != expected_receipt_schema
        or receipt.get("request_sha256") != request.get("request_sha256")
        or receipt.get("information_block") != profile.information_block
        or receipt.get("feature_spec_sha256") != feature_contract(profile.information_block)[2]
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or (
            request.get("schema_version") == REQUEST_SCHEMA
            and receipt.get("initial_holding_policy_sha256")
            != EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256
        )
    ):
        raise ActionValueError("INCREMENT_RECEIPT_IDENTITY_MISMATCH")
    return {"manifest": manifest, "request": request, "receipt": receipt}


def _publish_profile_bundle(
    bundle: Path,
    *,
    profile: IncrementProfile,
    request: Mapping[str, Any],
    coverage: Mapping[str, Any],
    core_oof: pd.DataFrame,
    optional_oof: pd.DataFrame,
    core_sleeves: pd.DataFrame,
    optional_sleeves: pd.DataFrame,
    paired_daily: pd.DataFrame,
    receipt: Mapping[str, Any],
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(staging / "request.json", canonical_json_bytes(request))
        PositionTimingArtifactStore._publish_immutable(staging / "coverage.json", canonical_json_bytes(coverage))
        core_oof.to_parquet(staging / "core_oof_action_predictions.parquet", index=False)
        optional_oof.to_parquet(
            staging / f"{profile.artifact_prefix}_oof_action_predictions.parquet",
            index=False,
        )
        core_sleeves.to_parquet(staging / "core_continuous_sleeve_days.parquet", index=False)
        optional_sleeves.to_parquet(
            staging / f"{profile.artifact_prefix}_continuous_sleeve_days.parquet",
            index=False,
        )
        paired_daily.to_parquet(staging / "paired_daily_increment.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(staging / "receipt.json", canonical_json_bytes(receipt))
        files = {}
        for name in _artifact_files(profile):
            reference = file_reference(staging / name)
            reference.pop("path", None)
            files[name] = reference
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "request_sha256": request["request_sha256"],
            "receipt_sha256": receipt["receipt_sha256"],
            "files": files,
        }
        manifest["manifest_sha256"] = canonical_sha256(manifest)
        PositionTimingArtifactStore._publish_immutable(staging / "manifest.json", canonical_json_bytes(manifest))
        lock_path = bundle.parents[3] / "locks" / f"action-value-increment-{bundle.name}.lock"
        with _exclusive_file_lock(lock_path):
            if bundle.exists():
                existing = inspect_increment_bundle(bundle)
                if existing["request"]["request_sha256"] != request["request_sha256"]:
                    raise ActionValueError("INCREMENT_BUNDLE_CONFLICT")
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def _publish_bundle(
    bundle: Path,
    *,
    request: Mapping[str, Any],
    coverage: Mapping[str, Any],
    core_oof: pd.DataFrame,
    atr_oof: pd.DataFrame,
    core_sleeves: pd.DataFrame,
    atr_sleeves: pd.DataFrame,
    paired_daily: pd.DataFrame,
    receipt: Mapping[str, Any],
) -> None:
    """Backward-compatible ATR14 test/helper surface."""

    _publish_profile_bundle(
        bundle,
        profile=ATR14_PROFILE,
        request=request,
        coverage=coverage,
        core_oof=core_oof,
        optional_oof=atr_oof,
        core_sleeves=core_sleeves,
        optional_sleeves=atr_sleeves,
        paired_daily=paired_daily,
        receipt=receipt,
    )


def _deliver_registry(
    request: Mapping[str, Any],
    bundle: Path,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    profile = _profile(str(request.get("information_block") or ""))
    reference = file_reference(bundle / "receipt.json")
    evidence = EvidenceReferenceV1(
        role=profile.evidence_role,
        artifact_uri=reference["path"],
        sha256=reference["sha256"],
        size_bytes=reference["size_bytes"],
    )
    effect = receipt["effect_evidence"]
    if effect == "SUPPORTED":
        result_class, decision_use = ResearchResultClass.CONTROL_READY, DecisionUse.DIRECTION_GATE
    elif effect == "NEGATIVE":
        result_class, decision_use = ResearchResultClass.NEGATIVE, DecisionUse.DIRECTION_GATE
    else:
        result_class, decision_use = ResearchResultClass.EXPLORATORY, DecisionUse.NAVIGATION_ONLY
    record = build_trial_record(
        experiment_id=_experiment_id(request, profile),
        attempt_id=request["request_sha256"][:24],
        research_stage=_research_stage(request),
        study_type=ResearchStudyType.LEARNABILITY_AUDIT,
        hypothesis_family_id="POSITION_TIMING_ACTION_VALUE_OPTIONAL_BLOCK_V1",
        parent_lineage=("POSITION_TIMING_ADVICE_V1", "POSITION_TIMING_ACTION_VALUE_V4"),
        unique_variable=profile.information_block,
        objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
        dataset_identity=receipt["source_sha256"],
        schema_identity=feature_contract(profile.information_block)[2],
        policy_identity=policy_sha256_for(profile.information_block),
        planned_trial_count=1,
        generated_trial_count=1,
        evaluated_trial_count=1,
        selected_trial_count=int(effect == "SUPPORTED"),
        consumed_windows=(
            ConsumedWindowV1(
                window_id="POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_FORWARD",
                dataset_identity=receipt["source_sha256"],
                start_date=date.fromisoformat(request["population_spec"]["start"]),
                end_date=date.fromisoformat(request["population_spec"]["end"]),
            ),
        ),
        result_class=result_class,
        decision_use=decision_use,
        evidence_refs=(evidence,),
    )
    registry_path = Path(request["timing_root"]) / "research_registry" / "timing_trial_registry_v1.jsonl"
    try:
        return AdvisoryResearchTrialRegistryV1(registry_path).append_batch((record,))
    except AdvisoryModelFirstError as exc:
        raise ActionValueError(
            "TIMING_INCREMENT_REGISTRY_DELIVERY_FAILED",
            reason_code=exc.reason_code,
        ) from exc


def _experiment_id(request: Mapping[str, Any], profile: IncrementProfile) -> str:
    if request.get("schema_version") == REQUEST_SCHEMA:
        return f"{profile.experiment_id}_exogenous_holding_endowment_v3"
    if request.get("schema_version") == "position_timing_action_value_increment_request_v2":
        return f"{profile.experiment_id}_explicit_suspension_source_v2"
    return profile.experiment_id


def _research_stage(request: Mapping[str, Any]) -> str:
    schema_version = request.get("schema_version")
    if schema_version == REQUEST_SCHEMA:
        return "POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V3"
    if schema_version == "position_timing_action_value_increment_request_v2":
        return "POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V2"
    return "POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V1"


def _global_observation(before: Mapping[str, Any], path: Path) -> dict[str, Any]:
    after = file_reference(path)
    return {
        "before": before,
        "after": after,
        "this_pipeline_writes_global_registry": False,
        "concurrent_change_observed": after != before,
    }


def _matched_row_identity(rows: pd.DataFrame) -> str:
    keys = rows.loc[:, ["symbol", "decision_as_of", "objective", "planned_delta_qty", "net_action_value_bps"]]
    payload = [
        {
            "symbol": str(row.symbol),
            "decision_as_of": pd.Timestamp(row.decision_as_of).isoformat(),
            "objective": str(row.objective),
            "planned_delta_qty": int(row.planned_delta_qty),
            "net_action_value_bps": float(row.net_action_value_bps),
        }
        for row in keys.itertuples(index=False)
    ]
    return canonical_sha256(payload)


def _research_candidate(
    candidate: DailyCandidate,
    profile: IncrementProfile,
) -> DailyCandidate | SectorAugmentedCandidate | MoneyflowAugmentedCandidate:
    if profile.information_block == SW_L2_INFORMATION_BLOCK:
        return SectorAugmentedCandidate.open(candidate)
    if profile.information_block == MONEYFLOW_INFORMATION_BLOCK:
        return MoneyflowAugmentedCandidate.open(candidate)
    return candidate


def _apply_suspension_snapshot(
    candidate: DailyCandidate | SectorAugmentedCandidate | MoneyflowAugmentedCandidate,
    snapshot_path: Path,
) -> tuple[
    DailyCandidate | SectorAugmentedCandidate | MoneyflowAugmentedCandidate,
    SuspensionSnapshotBook,
]:
    book = SuspensionSnapshotBook.open(snapshot_path)
    base = candidate.base if isinstance(candidate, (SectorAugmentedCandidate, MoneyflowAugmentedCandidate)) else candidate
    augmented_base = book.apply(base, snapshot_path=snapshot_path)
    if isinstance(candidate, (SectorAugmentedCandidate, MoneyflowAugmentedCandidate)):
        return replace(candidate, base=augmented_base), book
    return augmented_base, book


def _source_coverage(
    candidate: DailyCandidate | SectorAugmentedCandidate | MoneyflowAugmentedCandidate,
    symbols: Sequence[str],
    profile: IncrementProfile,
) -> dict[str, Any]:
    if profile.information_block == SW_L2_INFORMATION_BLOCK:
        if not isinstance(candidate, SectorAugmentedCandidate):
            raise ActionValueError("SW_L2_RESEARCH_SOURCE_INVALID")
        return candidate.coverage(symbols)
    if profile.information_block == MONEYFLOW_INFORMATION_BLOCK:
        if not isinstance(candidate, MoneyflowAugmentedCandidate):
            raise ActionValueError("MONEYFLOW_RESEARCH_SOURCE_INVALID")
        return candidate.coverage(symbols)
    if not isinstance(candidate, DailyCandidate):
        raise ActionValueError("ATR14_RESEARCH_SOURCE_INVALID")
    return candidate.coverage(symbols, information_block=profile.information_block)


def _require_matched_source_coverage(
    coverage: Mapping[str, Any],
    *,
    information_block: str = ATR14_INFORMATION_BLOCK,
) -> None:
    profile = _profile(information_block)
    if coverage.get("information_block") != profile.information_block or coverage.get("outcomes_read") is not False:
        raise ActionValueError("INCREMENT_SOURCE_COVERAGE_CONTRACT_INVALID")
    rows = tuple(coverage.get("coverage", {}).values())
    if not rows:
        raise ActionValueError("INCREMENT_SOURCE_COVERAGE_CONTRACT_INVALID")
    for item in rows:
        core = int(item["complete_core_sessions"])
        selected = int(item["complete_selected_feature_sessions"])
        nonmissing = item.get("feature_nonmissing", {})
        if selected <= 0 or selected > core or any(int(nonmissing.get(name, 0)) < selected for name in profile.added_features):
            raise ActionValueError("INCREMENT_OPTIONAL_COVERAGE_LOSS")
        if profile.information_block == ATR14_INFORMATION_BLOCK and selected != core:
            raise ActionValueError("INCREMENT_OPTIONAL_COVERAGE_LOSS")


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("INCREMENT_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    profile = _profile(str(request.get("information_block") or ""))
    feature = request.get("feature_contract") or {}
    matched = request.get("matched_core_contract") or {}
    training = request.get("training_spec") or {}
    population = request.get("population_spec") or {}
    initial_holding = request.get("initial_holding_contract") or {}
    expected_selection = _selection_contract(profile)
    schema_version = request.get("schema_version")
    corrected_source_contract = (
        schema_version == "position_timing_action_value_increment_request_v1"
        or (
            request.get("source_correction") == "EXPLICIT_DB_SUSPENSION_UNION_V1"
            and isinstance(request.get("suspension_snapshot"), Mapping)
            and "path" in request["suspension_snapshot"]
            and "sha256" in request["suspension_snapshot"]
            and "size_bytes" in request["suspension_snapshot"]
        )
    )
    initial_holding_contract = (
        schema_version in LEGACY_REQUEST_SCHEMAS
        or (
            initial_holding.get("policy") == EXOGENOUS_INITIAL_HOLDING_POLICY
            and initial_holding.get("policy_sha256")
            == EXOGENOUS_INITIAL_HOLDING_POLICY_SHA256
        )
    )
    if (
        schema_version not in {REQUEST_SCHEMA, *LEGACY_REQUEST_SCHEMAS}
        or not corrected_source_contract
        or not initial_holding_contract
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("information_block") not in PROFILES
        or request.get("planned_trial_count") != 1
        or request.get("hypothesis") != profile.hypothesis
        or feature.get("block_id") != profile.information_block
        or tuple(feature.get("added_features") or ()) != profile.added_features
        or tuple(feature.get("feature_order") or ()) != feature_contract(profile.information_block)[1]
        or feature.get("feature_spec_sha256") != feature_contract(profile.information_block)[2]
        or feature.get("policy_sha256") != policy_sha256_for(profile.information_block)
        or matched.get("information_block") != CORE_INFORMATION_BLOCK
        or tuple(matched.get("feature_order") or ()) != FEATURE_ORDER
        or matched.get("feature_spec_sha256") != feature_contract(CORE_INFORMATION_BLOCK)[2]
        or matched.get("policy_sha256") != policy_sha256_for(CORE_INFORMATION_BLOCK)
        or training.get("main_comparison") != profile.main_comparison
        or training.get("economic_threshold_bps") != 0.0
        or population.get("selection") != expected_selection
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("INCREMENT_REQUEST_IDENTITY_MISMATCH")
    return request


def _selection_contract(profile: IncrementProfile) -> str:
    if profile.information_block == SW_L2_INFORMATION_BLOCK:
        return "SHA256_SEED_SECTOR_SOURCE_COVERAGE_ONLY"
    if profile.information_block == MONEYFLOW_INFORMATION_BLOCK:
        return "SHA256_SEED_MONEYFLOW_SOURCE_COVERAGE_ONLY"
    return "SHA256_SEED_SYMBOL_SOURCE_ONLY"


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--candidate-root", required=True, type=Path)
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--historical-registry", required=True, type=Path)
    prepare.add_argument("--population-start", required=True, type=_parse_date)
    prepare.add_argument("--population-end", required=True, type=_parse_date)
    prepare.add_argument("--symbol-limit", type=int, default=64)
    prepare.add_argument("--review-stride", type=int, default=10)
    prepare.add_argument(
        "--information-block",
        choices=tuple(PROFILES),
        default=ATR14_INFORMATION_BLOCK,
    )
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            from dotenv import load_dotenv

            load_dotenv(args.repository_root.resolve() / ".env", override=False)
            result = {
                "status": "PREPARED",
                "request": prepare_increment_request(
                    candidate_root=args.candidate_root,
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    historical_registry=args.historical_registry,
                    population_start=args.population_start,
                    population_end=args.population_end,
                    symbol_limit=args.symbol_limit,
                    review_stride=args.review_stride,
                    information_block=args.information_block,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_increment_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_increment_bundle(args.bundle)}
    except ActionValueError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "details": exc.details}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARTIFACT_FOLDER",
    "PIPELINE_ID",
    "REQUEST_SCHEMA",
    "RECEIPT_SCHEMA",
    "inspect_increment_bundle",
    "prepare_increment_request",
    "run_increment_request",
]
