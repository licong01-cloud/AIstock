"""Frozen offline L2 learnability audit for position-timing advice.

This module deliberately has no router, scheduler, worker, model-serving, or
order-writing surface.  It consumes an explicit canonical-v2 file candidate,
freezes one request, evaluates the two pre-registered models, and appends only
to the position-timing-owned research registry.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
import shutil
from statistics import NormalDist
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from backend.execution_algos.board_lot import round_to_board_lot
from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicySplitV1
from backend.services.advisory_model_first.policy_cpcv import build_policy_cpcv_paths
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.research_control import (
    AdvisoryResearchTrialRegistryV1,
    evidence_reference_for_file,
)
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

from .artifact_store import PositionTimingArtifactStore
from .contracts import (
    POSITION_TIMING_L2_RESEARCH_CONTRACT_V1,
    TriggerSide,
    canonical_json_bytes,
    canonical_sha256,
)
from .policy import split_legal_parent_order_quantities


REQUEST_SCHEMA = "position_timing_l2_learnability_request_v1"
BUNDLE_SCHEMA = "position_timing_l2_learnability_bundle_v1"
POPULATION_SCHEMA = "position_timing_l2_population_dataset_v1"
RECEIPT_SCHEMA = "position_timing_l2_learnability_receipt_v1"
MODEL_ORDER = ("SKLEARN_RIDGE_V1", "LIGHTGBM_GBDT_V1")
PARENT_ORDER_SCENARIOS = (1, 2, 3)
POPULATION_START = date(2018, 8, 1)
POPULATION_END = date(2026, 6, 30)
BENCHMARK = "000300.SH"
_SHA256_PATTERN = r"^[0-9a-f]{64}$"
_Z_80 = 0.8416212335729143
SOURCE_ROLES = (
    "candidate_state",
    "daily_meta",
    "trading_calendar",
    "pit_universe",
    "pit_universe_summary",
    "suspend_meta",
    "suspend_rows",
    "benchmark_receipt",
    "ranking_manifest",
    "ranking_rows",
    "historical_registry",
    "cost_policy",
    "exit_guard",
)


class PositionTimingL2Error(RuntimeError):
    """Typed, fail-closed L2 audit failure."""

    def __init__(self, message: str, *, reason_code: str, context: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


class FrozenL2LearnabilityRequestV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["position_timing_l2_learnability_request_v1"] = REQUEST_SCHEMA
    request_id: str = Field(pattern=r"^ptl2req_[0-9a-f]{24}$")
    request_sha256: str = Field(pattern=_SHA256_PATTERN)
    created_at: datetime
    contract_sha256: str = Field(pattern=_SHA256_PATTERN)
    dataset_identity_sha256: str = Field(pattern=_SHA256_PATTERN)
    feature_schema_sha256: str = Field(pattern=_SHA256_PATTERN)
    candidate_root: str = Field(min_length=1)
    daily_provider_root: str = Field(min_length=1)
    suspend_root: str = Field(min_length=1)
    ranking_bundle_root: str = Field(min_length=1)
    timing_artifact_root: str = Field(min_length=1)
    historical_registry_path: str = Field(min_length=1)
    output_root: str = Field(min_length=1)
    repository_root: str = Field(min_length=1)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    population_start: date = POPULATION_START
    population_end: date = POPULATION_END
    source_refs: dict[str, EvidenceReferenceV1]
    model_runtime_identities: dict[str, dict[str, Any]]
    notional_observations: tuple[dict[str, str], ...]
    notional_distribution_sha256: str = Field(pattern=_SHA256_PATTERN)
    prospective_event_counts: dict[str, int]
    prospective_outcome_event_count: int = Field(ge=0)
    prospective_intervention_intent_count: int = Field(ge=0)
    deployment_cell_counts: dict[str, int]
    historical_registry_context_count: int = Field(ge=0)
    cost_policy: dict[str, Any]
    cost_policy_sha256: str = Field(pattern=_SHA256_PATTERN)
    exit_guard_policy: dict[str, Any]
    exit_guard_snapshot_sha256: str = Field(pattern=_SHA256_PATTERN)
    split_policy: AdvisoryPolicySplitV1

    @model_validator(mode="after")
    def validate_identity(self) -> "FrozenL2LearnabilityRequestV1":
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("L2 request created_at must be timezone-aware")
        if self.population_start != POPULATION_START or self.population_end != POPULATION_END:
            raise ValueError("L2 request population window drift")
        if set(self.model_runtime_identities) != set(MODEL_ORDER):
            raise ValueError("L2 request must freeze Ridge then GBDT and no other model")
        if set(self.source_refs) != set(SOURCE_ROLES):
            raise ValueError("L2 request source roles drift")
        if any(reference.role != f"position_timing_l2_{role}" for role, reference in self.source_refs.items()):
            raise ValueError("L2 request evidence role drift")
        if not self.notional_observations:
            raise ValueError("L2 request requires a non-empty held-position notional distribution")
        if self.notional_distribution_sha256 != canonical_sha256(self.notional_observations):
            raise ValueError("L2 notional distribution hash mismatch")
        if self.cost_policy_sha256 != canonical_sha256(self.cost_policy):
            raise ValueError("L2 cost policy hash mismatch")
        if Path(self.source_refs["historical_registry"].artifact_uri) != Path(self.historical_registry_path):
            raise ValueError("L2 historical-registry path mismatch")
        if self.split_policy.group_count != 8 or self.split_policy.validation_group_count != 2:
            raise ValueError("L2 split must be exact 8-block/2-validation CPCV")
        if self.split_policy.embargo_trading_days != 20:
            raise ValueError("L2 split must use a 20-trading-day embargo")
        expected = canonical_sha256(self.functional_payload())
        if self.request_sha256 != expected or self.request_id != f"ptl2req_{expected[:24]}":
            raise ValueError("L2 request identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"request_id", "request_sha256", "created_at"},
        )


class L2IntervalV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    lower_bps: float
    upper_bps: float
    alpha: float = Field(gt=0, lt=1)


class L2HypothesisResultV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    model_id: Literal["SKLEARN_RIDGE_V1", "LIGHTGBM_GBDT_V1"]
    point_estimate_bps: float
    nominal_interval: L2IntervalV1
    adjusted_interval: L2IntervalV1
    effect_evidence: Literal["SUPPORTED", "NEGATIVE", "INCONCLUSIVE"]
    evidence_reason_codes: tuple[str, ...]
    power_status: Literal["ADEQUATE", "UNDERPOWERED"]
    mde_bps: float
    oracle_mean_lift_bps: float
    mde_oracle_ratio: float | None
    cohort_count: int = Field(gt=1)
    paired_episode_count: int = Field(gt=0)
    cost_sensitivity: dict[str, dict[str, Any]]
    cost_assumption_sensitive: bool
    deployment_weighted_point_bps: float | None
    deployment_weighted_status: str
    unsupported_deployment_cells: tuple[str, ...]


class L2LearnabilityReceiptV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)

    schema_version: Literal["position_timing_l2_learnability_receipt_v1"] = RECEIPT_SCHEMA
    receipt_id: str = Field(pattern=r"^ptl2rcpt_[0-9a-f]{24}$")
    receipt_sha256: str = Field(pattern=_SHA256_PATTERN)
    request_sha256: str = Field(pattern=_SHA256_PATTERN)
    dataset_identity_sha256: str = Field(pattern=_SHA256_PATTERN)
    derived_dataset_sha256: str = Field(pattern=_SHA256_PATTERN)
    contract_sha256: str = Field(pattern=_SHA256_PATTERN)
    repository_commit: str = Field(pattern=r"^[0-9a-f]{40}$")
    created_at: datetime
    effect_evidence: Literal["SUPPORTED", "NEGATIVE", "INCONCLUSIVE"]
    selected_model_id: Literal["SKLEARN_RIDGE_V1", "LIGHTGBM_GBDT_V1"] | None
    hypothesis_count: Literal[2] = 2
    economic_threshold_bps: Literal[0.0] = 0.0
    hypotheses: tuple[L2HypothesisResultV1, L2HypothesisResultV1]
    population_counts: dict[str, int]
    feature_availability: dict[str, dict[str, int]]
    prospective_context: dict[str, Any]
    deployment_weighted_status: str
    historical_registry_context_count: int = Field(ge=0)
    sealed_holdout_accessed: Literal[False] = False
    runtime_model_written: Literal[False] = False
    l1_l1a_gate_applied: Literal[False] = False
    global_registry_written: Literal[False] = False
    current_route_written: Literal[False] = False

    @model_validator(mode="after")
    def validate_identity(self) -> "L2LearnabilityReceiptV1":
        if self.created_at.tzinfo is None or self.created_at.utcoffset() is None:
            raise ValueError("L2 receipt created_at must be timezone-aware")
        if tuple(item.model_id for item in self.hypotheses) != MODEL_ORDER:
            raise ValueError("L2 receipt hypothesis order drift")
        expected = canonical_sha256(self.functional_payload())
        if self.receipt_sha256 != expected or self.receipt_id != f"ptl2rcpt_{expected[:24]}":
            raise ValueError("L2 receipt identity mismatch")
        return self

    def functional_payload(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json",
            exclude={"receipt_id", "receipt_sha256", "created_at"},
        )


@dataclass(frozen=True)
class PopulationBuildResult:
    episodes: pd.DataFrame
    rows: pd.DataFrame
    source_identity: dict[str, Any]
    feature_availability: dict[str, dict[str, int]]


@dataclass(frozen=True)
class CrossfitResult:
    model_id: str
    predictions: np.ndarray
    target_exposures: np.ndarray
    oof_counts: np.ndarray
    path_diagnostics: tuple[dict[str, Any], ...]


def frozen_model_runtime_identities() -> dict[str, dict[str, Any]]:
    """Return exact estimator identities; no model is fitted here."""

    sklearn_version = importlib.metadata.version("scikit-learn")
    lightgbm_version = importlib.metadata.version("lightgbm")
    if sklearn_version != "1.8.0" or lightgbm_version != "4.6.0":
        _raise(
            "frozen L2 model package versions are unavailable",
            "POSITION_TIMING_L2_ENVIRONMENT_MISMATCH",
            scikit_learn=sklearn_version,
            lightgbm=lightgbm_version,
        )
    from lightgbm import LGBMRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler

    contract = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1
    ridge_spec, gbdt_spec = contract.models
    ridge = Ridge(**ridge_spec.parameters)
    gbdt_parameters = dict(gbdt_spec.parameters)
    early_stopping = bool(gbdt_parameters.pop("early_stopping"))
    gbdt = LGBMRegressor(**gbdt_parameters)
    values = {
        ridge_spec.model_id: {
            "package": ridge_spec.package,
            "package_version": sklearn_version,
            "estimator": "sklearn.linear_model.Ridge",
            "get_params_deep_false": _json_ready(ridge.get_params(deep=False)),
            "preprocessing": {
                "contract": ridge_spec.preprocessing,
                "imputer": _json_ready(SimpleImputer(strategy="median").get_params(deep=False)),
                "scaler": _json_ready(StandardScaler().get_params(deep=False)),
            },
            "feature_order": list(contract.feature_order),
            "target": contract.supervised_target,
        },
        gbdt_spec.model_id: {
            "package": gbdt_spec.package,
            "package_version": lightgbm_version,
            "estimator": "lightgbm.LGBMRegressor",
            "get_params_deep_false": _json_ready(gbdt.get_params(deep=False)),
            "preprocessing": {
                "contract": gbdt_spec.preprocessing,
                "imputer": _json_ready(SimpleImputer(strategy="median").get_params(deep=False)),
                "scaler": None,
            },
            "feature_order": list(contract.feature_order),
            "target": contract.supervised_target,
            "early_stopping": early_stopping,
        },
    }
    return {
        model_id: {**values[model_id], "identity_sha256": canonical_sha256(values[model_id])}
        for model_id in MODEL_ORDER
    }


def build_l2_cpcv_paths(rows: pd.DataFrame, *, request_sha256: str) -> tuple[dict[str, Any], ...]:
    """Build the exact 28 information-overlap-purged paths grouped by entry date."""

    required = {
        "entry_decision_date",
        "entry_trade_date",
        "effective_terminal_trade_date",
        "target_available",
        "full_exit_incremental_net_value_bps",
    }
    missing = required - set(rows)
    if missing:
        _raise("L2 rows omit CPCV fields", "POSITION_TIMING_L2_CPCV_INVALID", missing=sorted(missing))
    labels = rows.loc[rows["target_available"].astype(bool)].copy()
    labels["decision_as_of_trade_date"] = pd.to_datetime(labels["entry_decision_date"]).dt.normalize()
    labels["label_information_start"] = pd.to_datetime(labels["entry_trade_date"]).dt.normalize()
    labels["label_information_end"] = pd.to_datetime(labels["effective_terminal_trade_date"]).dt.normalize()
    labels["label_status"] = "MATURED"
    labels["take_label"] = (pd.to_numeric(labels["full_exit_incremental_net_value_bps"], errors="coerce") > 0).astype(
        int
    )
    calendar = pd.DatetimeIndex(sorted(labels["label_information_end"].dropna().unique()))
    # The caller replaces this sparse set with the full calendar in the frame attrs.
    if "trading_calendar" in rows.attrs:
        calendar = pd.DatetimeIndex(pd.to_datetime(rows.attrs["trading_calendar"])).normalize()
    split = AdvisoryPolicySplitV1(
        group_count=8,
        validation_group_count=2,
        embargo_trading_days=20,
        random_seed=20260903,
    )
    result = build_policy_cpcv_paths(
        labels,
        split_policy=split,
        trading_calendar=calendar,
        request_sha256=request_sha256,
    )
    paths = tuple(result.paths)
    if len(paths) != math.comb(8, 2) or any(path.get("status") != "READY" for path in paths):
        _raise(
            "L2 data do not produce all 28 frozen CPCV paths",
            "POSITION_TIMING_L2_CPCV_INVALID",
            path_count=len(paths),
            status_counts=pd.Series([path.get("status") for path in paths]).value_counts().to_dict(),
        )
    return paths


def map_monotone_exposure(
    predictions: np.ndarray,
    q50: np.ndarray | float,
    q75: np.ndarray | float,
) -> np.ndarray:
    """Map OOF exit-value predictions to the one frozen monotone exposure policy."""

    values = np.asarray(predictions, dtype=float)
    median = np.broadcast_to(np.asarray(q50, dtype=float), values.shape)
    upper = np.broadcast_to(np.asarray(q75, dtype=float), values.shape)
    if not np.isfinite(values).all() or not np.isfinite(median).all() or not np.isfinite(upper).all():
        _raise("non-finite L2 policy input", "POSITION_TIMING_L2_POLICY_INVALID")
    result = np.ones(values.shape, dtype=np.float32)
    positive = values > 0
    result[positive & (values <= median)] = 0.50
    result[positive & (values > median) & (values <= upper)] = 0.25
    result[positive & (values > upper)] = 0.0
    return result


def circular_block_interval(
    values: Sequence[float] | np.ndarray,
    *,
    alpha: float,
    seed: int,
    repetitions: int = 2000,
    block_length: int = 2,
) -> tuple[float, float, float, float]:
    """Return point, lower, upper, and bootstrap standard error."""

    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array)]
    if array.size < 2:
        _raise(
            "L2 inference requires at least two finite cohorts",
            "POSITION_TIMING_L2_INFERENCE_UNAVAILABLE",
            cohort_count=int(array.size),
        )
    rng = np.random.default_rng(seed)
    width = min(block_length, len(array))
    count = math.ceil(len(array) / width)
    offsets = np.arange(width)
    means = np.empty(repetitions, dtype=float)
    for index in range(repetitions):
        starts = rng.integers(0, len(array), size=count)
        positions = ((starts[:, None] + offsets[None, :]) % len(array)).reshape(-1)
        means[index] = float(array[positions[: len(array)]].mean())
    point = float(array.mean())
    lower, upper = np.quantile(means, [alpha / 2.0, 1.0 - alpha / 2.0]).tolist()
    return point, float(lower), float(upper), float(means.std(ddof=1))


def classify_effect(*, lower_bps: float, upper_bps: float) -> str:
    if lower_bps > 0:
        return "SUPPORTED"
    if upper_bps <= 0:
        return "NEGATIVE"
    return "INCONCLUSIVE"


def choose_supported_model(results: Sequence[L2HypothesisResultV1]) -> str | None:
    supported = {item.model_id for item in results if item.effect_evidence == "SUPPORTED"}
    if "SKLEARN_RIDGE_V1" in supported:
        return "SKLEARN_RIDGE_V1"
    if "LIGHTGBM_GBDT_V1" in supported:
        return "LIGHTGBM_GBDT_V1"
    return None


def prepare_l2_learnability_request(
    *,
    candidate_root: str | Path,
    ranking_bundle_root: str | Path,
    timing_artifact_root: str | Path,
    historical_registry_path: str | Path,
    repository_root: str | Path,
    output_root: str | Path | None = None,
) -> dict[str, Any]:
    """Freeze one exact, idempotent L2 request without fitting a model."""

    candidate = Path(candidate_root).resolve()
    rankings = Path(ranking_bundle_root).resolve()
    timing_root = Path(timing_artifact_root).resolve()
    historical_registry = Path(historical_registry_path).resolve()
    repository = Path(repository_root).resolve()
    target_root = Path(output_root or timing_root).resolve()
    try:
        target_root.relative_to(timing_root)
    except ValueError:
        _raise(
            "L2 output root must remain inside the position-timing artifact root",
            "POSITION_TIMING_L2_ISOLATION_VIOLATION",
            output_root=target_root.as_posix(),
            timing_artifact_root=timing_root.as_posix(),
        )
    state_path = candidate / "direct_monthly_state.json"
    daily_root = candidate / "components" / "daily_bin_candidate"
    suspend_root = candidate / "components" / "suspend_d_daily_candidate_v2"
    source_paths = {
        "candidate_state": state_path,
        "daily_meta": daily_root / "meta_export.json",
        "trading_calendar": daily_root / "calendars" / "day.txt",
        "pit_universe": daily_root / "instruments" / "stock_universe.txt",
        "pit_universe_summary": daily_root / "instruments" / "all_pit_universe_summary.json",
        "suspend_meta": suspend_root / "meta.json",
        "suspend_rows": suspend_root / "suspend_d.parquet",
        "benchmark_receipt": candidate / "reports" / "daily_benchmark_000300_completion.json",
        "ranking_manifest": rankings / "manifest.json",
        "ranking_rows": rankings / "candidate_rankings.parquet",
        "historical_registry": historical_registry,
    }
    for role, path in source_paths.items():
        if not path.is_file():
            _raise(
                "L2 source file is missing",
                "POSITION_TIMING_L2_SOURCE_UNAVAILABLE",
                role=role,
                path=path.as_posix(),
            )
    state = _read_json(state_path)
    daily_meta = _read_json(source_paths["daily_meta"])
    suspend_meta = _read_json(source_paths["suspend_meta"])
    _validate_candidate_source(state=state, daily_meta=daily_meta, suspend_meta=suspend_meta)

    store = PositionTimingArtifactStore(timing_root)
    card_events = store.list_events(event_type="CARD_ISSUED")
    held = sorted(
        (
            {
                "card_id": str(item["card_id"]),
                "planned_full_notional_cny": format(
                    Decimal(str(item["event_payload"]["planned_full_notional_cny"])), "f"
                ),
            }
            for item in card_events
            if int(item["event_payload"]["pre_action_qty"]) > 0
            and Decimal(str(item["event_payload"]["planned_full_notional_cny"])) > 0
        ),
        key=lambda item: (item["card_id"], Decimal(item["planned_full_notional_cny"])),
    )
    if not held:
        _raise(
            "no held-position CARD_ISSUED notional is available",
            "POSITION_TIMING_L2_DEPLOYMENT_NOTIONAL_UNAVAILABLE",
        )
    held_payloads = [
        item["event_payload"]
        for item in card_events
        if int(item["event_payload"]["pre_action_qty"]) > 0
        and Decimal(str(item["event_payload"]["planned_full_notional_cny"])) > 0
    ]
    deployment_cells: dict[str, int] = {}
    for payload in held_payloads:
        key = _deployment_cell_key(
            source_role=str(payload["primary_source_role"]),
            action_side=str(payload["action_side"]),
            holding_age_bucket=str(payload["holding_age_bucket"]),
            market_regime=str(payload["market_regime"]),
            st_flag=bool(payload["st_flag"]),
        )
        deployment_cells[key] = deployment_cells.get(key, 0) + 1
    card_sets = store.list_card_sets()
    if not card_sets:
        _raise("no immutable card set is available", "POSITION_TIMING_L2_SOURCE_UNAVAILABLE")
    latest_policy = card_sets[-1].policy_identity
    cost_hash = str(latest_policy.get("cost_policy_sha256") or "")
    exit_hash = str(latest_policy.get("exit_guard_snapshot_sha256") or "")
    cost_path = timing_root / "policy_snapshots" / f"personal-manual-component-cost-v1-{cost_hash}.json"
    exit_path = timing_root / "policy_snapshots" / f"exit-guard-v1-{exit_hash}.json"
    for role, path in (("cost_policy", cost_path), ("exit_guard", exit_path)):
        if not path.is_file():
            _raise("L2 policy snapshot is missing", "POSITION_TIMING_L2_SOURCE_UNAVAILABLE", role=role)
        source_paths[role] = path
    cost_policy = _read_json(cost_path)
    exit_snapshot = _read_json(exit_path)
    if canonical_sha256(cost_policy) != cost_hash or canonical_sha256(exit_snapshot) != exit_hash:
        _raise("L2 policy snapshot identity drift", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    exit_policy = dict(exit_snapshot.get("policy") or {})

    commit = _repository_commit(repository)
    dirty = _repository_dirty(repository)
    if dirty:
        _raise(
            "L2 request requires a clean repository",
            "POSITION_TIMING_L2_REPOSITORY_DIRTY",
            dirty_paths=dirty[:50],
        )
    source_refs = {
        role: evidence_reference_for_file(path, role=f"position_timing_l2_{role}")
        for role, path in source_paths.items()
    }
    model_identities = frozen_model_runtime_identities()
    feature_schema = {
        "feature_order": list(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.feature_order),
        "selection_feature_join": "ENTRY_DECISION_DATE_LEFT_JOIN_ELSE_MISSING",
        "market_regime": "CSI300_TRAILING20_CLOSE_RETURN_SIGN_AT_REVIEW_CLOSE_V1",
        "adjusted_return": "RAW_PRICE_TIMES_ADJ_FACTOR_RATIO_V1",
        "exit_guard_snapshot_sha256": exit_hash,
    }
    dataset_identity = canonical_sha256(
        {
            "candidate_status": state.get("status"),
            "candidate_cutoff": state.get("cutoff"),
            "source_refs": {
                role: source_refs[role]
                for role in (
                    "candidate_state",
                    "daily_meta",
                    "trading_calendar",
                    "pit_universe",
                    "pit_universe_summary",
                    "suspend_meta",
                    "suspend_rows",
                    "benchmark_receipt",
                    "ranking_manifest",
                    "ranking_rows",
                )
            },
            "population_contract": POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.population_spec,
        }
    )
    event_counts = store.event_counts()
    outcome_event_count = int(event_counts.get("OUTCOME_EVALUATED", 0))
    values: dict[str, Any] = {
        "schema_version": REQUEST_SCHEMA,
        "contract_sha256": canonical_sha256(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1),
        "dataset_identity_sha256": dataset_identity,
        "feature_schema_sha256": canonical_sha256(feature_schema),
        "candidate_root": candidate.as_posix(),
        "daily_provider_root": daily_root.as_posix(),
        "suspend_root": suspend_root.as_posix(),
        "ranking_bundle_root": rankings.as_posix(),
        "timing_artifact_root": timing_root.as_posix(),
        "historical_registry_path": historical_registry.as_posix(),
        "output_root": target_root.as_posix(),
        "repository_root": repository.as_posix(),
        "repository_commit": commit,
        "population_start": POPULATION_START,
        "population_end": POPULATION_END,
        "source_refs": source_refs,
        "model_runtime_identities": model_identities,
        "notional_observations": tuple(held),
        "notional_distribution_sha256": canonical_sha256(tuple(held)),
        "prospective_event_counts": event_counts,
        "prospective_outcome_event_count": outcome_event_count,
        "prospective_intervention_intent_count": sum(
            any(int(value) != 0 for value in payload.get("planned_trigger_deltas", ())) for payload in held_payloads
        ),
        "deployment_cell_counts": dict(sorted(deployment_cells.items())),
        "historical_registry_context_count": len(AdvisoryResearchTrialRegistryV1(historical_registry).read()),
        "cost_policy": cost_policy,
        "cost_policy_sha256": cost_hash,
        "exit_guard_policy": exit_policy,
        "exit_guard_snapshot_sha256": exit_hash,
        "split_policy": AdvisoryPolicySplitV1(
            group_count=8,
            validation_group_count=2,
            embargo_trading_days=20,
            random_seed=20260903,
        ),
    }
    functional = FrozenL2LearnabilityRequestV1.model_construct(
        request_id="ptl2req_" + "0" * 24,
        request_sha256="0" * 64,
        created_at=datetime.now(timezone.utc),
        **values,
    ).functional_payload()
    digest = canonical_sha256(functional)
    request = FrozenL2LearnabilityRequestV1(
        request_id=f"ptl2req_{digest[:24]}",
        request_sha256=digest,
        created_at=datetime.now(timezone.utc),
        **values,
    )
    request_path = target_root / "research" / "l2_requests" / f"{request.request_id}.json"
    if request_path.exists():
        existing = FrozenL2LearnabilityRequestV1.model_validate_json(request_path.read_text(encoding="utf-8"))
        if existing.functional_payload() != request.functional_payload():
            _raise(
                "existing L2 request id has different functional content",
                "POSITION_TIMING_L2_IMMUTABLE_CONFLICT",
                request_path=request_path.as_posix(),
            )
        request = existing
    else:
        _write_immutable(request_path, canonical_json_bytes(request) + b"\n")
    return {
        "status": "REQUEST_FROZEN",
        "request_path": request_path.as_posix(),
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "prospective_outcome_event_count": request.prospective_outcome_event_count,
        "historical_registry_context_count": request.historical_registry_context_count,
    }


def materialize_l2_population(request: FrozenL2LearnabilityRequestV1) -> PopulationBuildResult:
    """Build the frozen all-PIT synthetic episode and review-row dataset."""

    _verify_request_sources(request)
    calendar = _read_calendar(Path(request.source_refs["trading_calendar"].artifact_uri))
    population_calendar = calendar[(calendar.date >= POPULATION_START) & (calendar.date <= POPULATION_END)]
    if population_calendar.empty or population_calendar[0].date() != POPULATION_START:
        _raise("L2 population calendar does not start on the frozen date", "POSITION_TIMING_L2_CALENDAR_INVALID")
    cohort_dates = population_calendar[::20]
    calendar_positions = {value: index for index, value in enumerate(calendar)}
    last_position = calendar_positions.get(cohort_dates[-1])
    if last_position is None or last_position + 26 >= len(calendar):
        _raise("L2 calendar does not cover terminal deferral", "POSITION_TIMING_L2_CALENDAR_INVALID")
    load_end = calendar[last_position + 26]

    spans = _read_pit_spans(Path(request.source_refs["pit_universe"].artifact_uri))
    symbols = tuple(sorted(spans["instrument"].unique()))
    if BENCHMARK in symbols:
        _raise("benchmark leaked into the L2 selection universe", "POSITION_TIMING_L2_POPULATION_INVALID")
    market = _load_qlib_market(
        provider_root=Path(request.daily_provider_root),
        instruments=(*symbols, BENCHMARK),
        start=POPULATION_START,
        end=load_end.date(),
    )
    try:
        suspend = pd.read_parquet(Path(request.source_refs["suspend_rows"].artifact_uri))
    except Exception as exc:
        _raise(
            "L2 suspension source cannot be read",
            "POSITION_TIMING_L2_SOURCE_UNAVAILABLE",
            error_type=type(exc).__name__,
        )
    required_suspend = {"trade_date", "ts_code", "suspend_type"}
    if not required_suspend.issubset(suspend):
        _raise(
            "L2 suspension source schema drift",
            "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
            missing=sorted(required_suspend - set(suspend)),
        )
    suspend = suspend.loc[suspend["suspend_type"].astype(str).eq("S")].copy()
    suspend["trade_date"] = pd.to_datetime(suspend["trade_date"]).dt.normalize()
    suspend["ts_code"] = suspend["ts_code"].astype(str).str.upper()
    suspended = set(zip(suspend["trade_date"], suspend["ts_code"], strict=False))

    try:
        ranking = pd.read_parquet(Path(request.source_refs["ranking_rows"].artifact_uri))
    except Exception as exc:
        _raise(
            "L2 ranking source cannot be read",
            "POSITION_TIMING_L2_SOURCE_UNAVAILABLE",
            error_type=type(exc).__name__,
        )
    required_ranking = (
        "trade_date",
        "instrument",
        "selection_effective_rank",
        "combined_score",
    )
    if not set(required_ranking).issubset(ranking):
        _raise(
            "L2 ranking source schema drift",
            "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
            missing=sorted(set(required_ranking) - set(ranking)),
        )
    ranking = ranking[list(required_ranking)].copy()
    ranking["trade_date"] = pd.to_datetime(ranking["trade_date"]).dt.normalize()
    ranking["instrument"] = ranking["instrument"].astype(str).str.upper()
    if ranking.duplicated(["trade_date", "instrument"]).any():
        _raise("L2 ranking identity is duplicated", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    ranking = ranking.set_index(["trade_date", "instrument"]).sort_index()

    market = market.loc[market["datetime"] <= load_end].copy()
    if market.duplicated(["datetime", "instrument"]).any():
        _raise("L2 market rows are duplicated", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    stock_market = market.loc[market["instrument"].isin(symbols)]
    benchmark_market = market.loc[market["instrument"].eq(BENCHMARK)].set_index("datetime").sort_index()
    if benchmark_market.empty:
        _raise("L2 benchmark is unavailable", "POSITION_TIMING_L2_SOURCE_UNAVAILABLE")
    wide = {
        column: stock_market.pivot(index="datetime", columns="instrument", values=column)
        .reindex(index=calendar[calendar <= load_end], columns=symbols)
        .astype("float32")
        for column in (
            "open",
            "high",
            "low",
            "close",
            "volume",
            "factor",
            "up_limit_price",
            "down_limit_price",
        )
    }
    adjusted_close = wide["close"] * wide["factor"]
    daily_return = adjusted_close.pct_change(fill_method=None)
    derived = {
        f"return_{window}d_bps": (adjusted_close / adjusted_close.shift(window) - 1.0) * 10000.0
        for window in (1, 3, 5, 10)
    }
    derived.update(
        {
            f"realized_vol_{window}d_bps": daily_return.rolling(window, min_periods=window).std(ddof=1) * 10000.0
            for window in (5, 10, 20)
        }
    )
    derived["volume_ratio_5d_to_20d"] = (
        wide["volume"].rolling(5, min_periods=5).mean() / wide["volume"].rolling(20, min_periods=20).mean()
    )
    benchmark_close = pd.to_numeric(benchmark_market["close"], errors="coerce")
    benchmark_factor = pd.to_numeric(benchmark_market["factor"], errors="coerce")
    if benchmark_factor.notna().any():
        benchmark_adjusted = benchmark_close * benchmark_factor
    else:
        # The same-release index component explicitly has raw OHLC but no
        # corporate-action factor.  CSI 300 regime therefore uses raw close.
        benchmark_adjusted = benchmark_close
    benchmark_regime = benchmark_adjusted / benchmark_adjusted.shift(20) - 1.0

    try:
        stop_loss_bps = float(request.exit_guard_policy["stop_loss"]["max_loss_bps"])
        take_profit_bps = float(request.exit_guard_policy["take_profit"]["take_profit_bps"])
        trailing_stop_bps = float(request.exit_guard_policy["take_profit"]["trailing_stop_bps"])
        time_stop_days = int(request.exit_guard_policy["time_stop"]["max_holding_days"])
    except (KeyError, TypeError, ValueError) as exc:
        _raise(
            "L2 exit-guard snapshot omits a frozen feature parameter",
            "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
            error_type=type(exc).__name__,
        )
    if (
        not all(np.isfinite(value) and value > 0 for value in (stop_loss_bps, take_profit_bps, trailing_stop_bps))
        or time_stop_days <= 0
    ):
        _raise(
            "L2 exit-guard feature parameters are invalid",
            "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
        )

    notional_values = tuple(Decimal(item["planned_full_notional_cny"]) for item in request.notional_observations)
    episode_frames: list[pd.DataFrame] = []
    row_frames: list[pd.DataFrame] = []
    next_episode_ordinal = 0
    for cohort_index, entry_decision in enumerate(cohort_dates):
        decision_position = calendar_positions[entry_decision]
        entry_trade = calendar[decision_position + 1]
        review_dates = calendar[decision_position + 1 : decision_position + 20]
        action_dates = calendar[decision_position + 2 : decision_position + 21]
        nominal_terminal = calendar[decision_position + 20]
        active = tuple(
            sorted(
                spans.loc[
                    (spans["eligible_start"] <= entry_decision) & (spans["eligible_end"] >= entry_decision),
                    "instrument",
                ].unique()
            )
        )
        if not active:
            _raise(
                "L2 cohort has no PIT-active symbols",
                "POSITION_TIMING_L2_POPULATION_INVALID",
                entry_decision_date=entry_decision.date().isoformat(),
            )
        ordinals = np.arange(next_episode_ordinal, next_episode_ordinal + len(active), dtype=np.int32)
        next_episode_ordinal += len(active)
        episode_ids = np.asarray(
            [
                "ptl2ep_"
                + canonical_sha256(
                    {
                        "population_identity": request.dataset_identity_sha256,
                        "canonical_symbol": symbol,
                        "entry_decision_date": entry_decision.date(),
                        "entry_trade_date": entry_trade.date(),
                    }
                )[:24]
                for symbol in active
            ],
            dtype=object,
        )
        assigned_notionals = np.asarray(
            [
                float(
                    notional_values[
                        int(
                            canonical_sha256(
                                {
                                    "episode_id": episode_id,
                                    "distribution_sha256": request.notional_distribution_sha256,
                                }
                            ),
                            16,
                        )
                        % len(notional_values)
                    ]
                )
                for episode_id in episode_ids
            ],
            dtype=np.float64,
        )
        entry_values = {
            field: wide[field].loc[entry_trade, list(active)].to_numpy(dtype=float)
            for field in ("open", "high", "low", "close", "factor", "up_limit_price")
        }
        entry_suspended = np.asarray([(entry_trade, symbol) in suspended for symbol in active], dtype=bool)
        entry_status = _directional_status(
            side="BUY",
            open_values=entry_values["open"],
            high_values=entry_values["high"],
            low_values=entry_values["low"],
            close_values=entry_values["close"],
            factor_values=entry_values["factor"],
            limit_values=entry_values["up_limit_price"],
            suspended=entry_suspended,
        )
        quantities = np.zeros(len(active), dtype=np.int64)
        for index, symbol in enumerate(active):
            price = entry_values["open"][index]
            if entry_status[index] == 0 and np.isfinite(price) and price > 0:
                quantities[index] = round_to_board_lot(
                    int(math.floor(assigned_notionals[index] / price)), symbol, side="BUY"
                )
        entry_ready = (entry_status == 0) & (quantities > 0)
        entry_adjusted_open = entry_values["open"] * entry_values["factor"]
        entry_gross = entry_values["open"] * quantities

        terminal_status = np.full(len(active), 2, dtype=np.int8)
        terminal_close = np.full(len(active), np.nan, dtype=float)
        terminal_factor = np.full(len(active), np.nan, dtype=float)
        terminal_dates = np.full(len(active), np.datetime64("NaT"), dtype="datetime64[ns]")
        terminal_defer = np.full(len(active), -1, dtype=np.int8)
        unresolved = entry_ready.copy()
        for defer in range(6):
            candidate_date = calendar[decision_position + 20 + defer]
            values = {
                field: wide[field].loc[candidate_date, list(active)].to_numpy(dtype=float)
                for field in ("open", "high", "low", "close", "factor", "down_limit_price")
            }
            status = _directional_status(
                side="SELL",
                open_values=values["open"],
                high_values=values["high"],
                low_values=values["low"],
                close_values=values["close"],
                factor_values=values["factor"],
                limit_values=values["down_limit_price"],
                suspended=np.asarray([(candidate_date, symbol) in suspended for symbol in active], dtype=bool),
            )
            chosen = unresolved & (status == 0)
            terminal_status[chosen] = 0
            terminal_close[chosen] = values["close"][chosen]
            terminal_factor[chosen] = values["factor"][chosen]
            terminal_dates[chosen] = candidate_date.to_datetime64()
            terminal_defer[chosen] = defer
            unresolved[chosen] = False
        terminal_ready = entry_ready & (terminal_status == 0)
        terminal_full_gross = terminal_close * quantities * terminal_factor / entry_values["factor"]
        episode_status = np.full(len(active), "READY", dtype=object)
        episode_status[entry_status == 1] = "ENTRY_BLOCKED"
        episode_status[entry_status == 2] = "ENTRY_DATA_UNAVAILABLE"
        episode_status[(entry_status == 0) & (quantities == 0)] = "ENTRY_QUANTITY_ZERO"
        episode_status[entry_ready & ~terminal_ready] = "TERMINAL_UNAVAILABLE"
        episode_frames.append(
            pd.DataFrame(
                {
                    "episode_ordinal": ordinals,
                    "episode_id": episode_ids,
                    "cohort_ordinal": cohort_index,
                    "canonical_symbol": active,
                    "entry_decision_date": entry_decision,
                    "entry_trade_date": entry_trade,
                    "nominal_terminal_trade_date": nominal_terminal,
                    "effective_terminal_trade_date": terminal_dates,
                    "terminal_deferred_trading_days": terminal_defer,
                    "planned_full_notional_cny": assigned_notionals,
                    "initial_quantity": quantities,
                    "entry_raw_open": entry_values["open"],
                    "entry_gross_notional_cny": entry_gross,
                    "terminal_full_gross_notional_cny": terminal_full_gross,
                    "population_status": episode_status,
                }
            )
        )
        valid_indices = np.flatnonzero(entry_ready)
        if not len(valid_indices):
            continue
        valid_symbols = tuple(active[index] for index in valid_indices)
        n_valid = len(valid_indices)
        review_matrix = {
            field: wide[field].loc[review_dates, list(valid_symbols)].to_numpy(dtype=float).T
            for field in ("open", "high", "low", "close", "factor")
        }
        action_matrix = {
            field: wide[field].loc[action_dates, list(valid_symbols)].to_numpy(dtype=float).T
            for field in ("open", "high", "low", "close", "factor", "down_limit_price")
        }
        action_suspended = np.asarray(
            [[(action_date, symbol) in suspended for action_date in action_dates] for symbol in valid_symbols],
            dtype=bool,
        )
        action_status = _directional_status(
            side="SELL",
            open_values=action_matrix["open"],
            high_values=action_matrix["high"],
            low_values=action_matrix["low"],
            close_values=action_matrix["close"],
            factor_values=action_matrix["factor"],
            limit_values=action_matrix["down_limit_price"],
            suspended=action_suspended,
        )
        valid_quantity = quantities[valid_indices].astype(float)
        valid_entry_factor = entry_values["factor"][valid_indices]
        action_full_gross = (
            action_matrix["open"] * valid_quantity[:, None] * action_matrix["factor"] / valid_entry_factor[:, None]
        )
        valid_terminal_ready = terminal_ready[valid_indices]
        valid_terminal_gross = terminal_full_gross[valid_indices]
        target_available = valid_terminal_ready[:, None] & (action_status != 2)
        baseline_terminal_net = valid_terminal_gross - _sell_cost_array(
            valid_terminal_gross, parent_order_count=1, policy=request.cost_policy
        )
        action_net = action_full_gross - _sell_cost_array(
            action_full_gross, parent_order_count=1, policy=request.cost_policy
        )
        target = np.where(
            action_status == 0,
            (action_net - baseline_terminal_net[:, None]) / entry_gross[valid_indices, None] * 10000.0,
            0.0,
        )
        target[~target_available] = np.nan

        adjusted_review_close = review_matrix["close"] * review_matrix["factor"]
        entry_adjusted = entry_adjusted_open[valid_indices, None]
        unrealized = (adjusted_review_close / entry_adjusted - 1.0) * 10000.0
        peak = np.fmax.accumulate(adjusted_review_close, axis=1)
        drawdown = (adjusted_review_close / peak - 1.0) * 10000.0
        runup = (peak / entry_adjusted - 1.0) * 10000.0
        intraday_range = (review_matrix["high"] - review_matrix["low"]) / review_matrix["close"] * 10000.0
        close_location = (review_matrix["close"] - review_matrix["low"]) / (
            review_matrix["high"] - review_matrix["low"]
        )
        rank_keys = pd.MultiIndex.from_arrays(
            [
                np.repeat(entry_decision, n_valid),
                np.asarray(valid_symbols, dtype=object),
            ],
            names=["trade_date", "instrument"],
        )
        rank_rows = ranking.reindex(rank_keys)
        entry_rank = pd.to_numeric(rank_rows["selection_effective_rank"], errors="coerce").to_numpy()
        entry_score = pd.to_numeric(rank_rows["combined_score"], errors="coerce").to_numpy()
        elapsed = np.arange(1, 20, dtype=np.float32)
        regime_values = benchmark_regime.reindex(review_dates).to_numpy(dtype=float)
        regime_down = np.broadcast_to((regime_values < 0).astype(np.float32), (n_valid, 19))
        regime_up = np.broadcast_to((regime_values >= 0).astype(np.float32), (n_valid, 19))
        regime_unknown = np.broadcast_to((~np.isfinite(regime_values)).astype(np.float32), (n_valid, 19))
        stock_unrealized = unrealized
        benchmark_entry_open = float(benchmark_market.reindex([entry_trade])["open"].iloc[0])
        benchmark_review_close = pd.to_numeric(
            benchmark_market.reindex(review_dates)["close"], errors="coerce"
        ).to_numpy(dtype=float)
        benchmark_relative = stock_unrealized - (benchmark_review_close[None, :] / benchmark_entry_open - 1.0) * 10000.0
        block: dict[str, Any] = {
            "episode_ordinal": np.repeat(ordinals[valid_indices], 19),
            "canonical_symbol": np.repeat(np.asarray(valid_symbols, dtype=object), 19),
            "cohort_ordinal": cohort_index,
            "entry_decision_date": entry_decision,
            "entry_trade_date": entry_trade,
            "review_decision_date": np.tile(review_dates.to_numpy(), n_valid),
            "target_action_date": np.tile(action_dates.to_numpy(), n_valid),
            "effective_terminal_trade_date": np.repeat(terminal_dates[valid_indices], 19),
            "holding_session": np.tile(np.arange(1, 20, dtype=np.int8), n_valid),
            "initial_quantity": np.repeat(quantities[valid_indices], 19),
            "st_flag": False,
            "action_status_code": action_status.reshape(-1).astype(np.int8),
            "action_full_gross_notional_cny": action_full_gross.reshape(-1),
            "terminal_full_gross_notional_cny": np.repeat(valid_terminal_gross, 19),
            "entry_gross_notional_cny": np.repeat(entry_gross[valid_indices], 19),
            "target_available": target_available.reshape(-1),
            "full_exit_incremental_net_value_bps": target.reshape(-1),
            "selection_rank": np.repeat(entry_rank, 19),
            "selection_score": np.repeat(entry_score, 19),
            "holding_trading_days_elapsed": np.tile(elapsed, n_valid),
            "holding_fraction_of_time_stop": np.tile(elapsed / float(time_stop_days), n_valid),
            "unrealized_close_return_bps": unrealized.reshape(-1),
            "relative_return_since_entry_bps": benchmark_relative.reshape(-1),
            "drawdown_from_peak_since_entry_bps": drawdown.reshape(-1),
            "runup_from_entry_peak_bps": runup.reshape(-1),
            "distance_to_stop_bps": (unrealized + stop_loss_bps).reshape(-1),
            "distance_to_take_profit_bps": (take_profit_bps - unrealized).reshape(-1),
            "distance_to_trailing_stop_bps": (drawdown + trailing_stop_bps).reshape(-1),
            "intraday_range_bps": intraday_range.reshape(-1),
            "close_location_in_day": close_location.reshape(-1),
            "market_regime_down": regime_down.reshape(-1),
            "market_regime_up_or_flat": regime_up.reshape(-1),
            "market_regime_unknown": regime_unknown.reshape(-1),
        }
        for name, frame in derived.items():
            block[name] = frame.loc[review_dates, list(valid_symbols)].to_numpy(dtype=float).T.reshape(-1)
        row_frames.append(pd.DataFrame(block))

    if not episode_frames or not row_frames:
        _raise("L2 population is empty", "POSITION_TIMING_L2_POPULATION_INVALID")
    episodes = pd.concat(episode_frames, ignore_index=True)
    rows = pd.concat(row_frames, ignore_index=True)
    if (
        len(episodes) != next_episode_ordinal
        or episodes["episode_id"].duplicated().any()
        or episodes.duplicated(["canonical_symbol", "entry_decision_date"]).any()
        or rows.duplicated(["episode_ordinal", "holding_session"]).any()
    ):
        _raise("L2 episode or review-row identity drift", "POSITION_TIMING_L2_POPULATION_INVALID")
    expected_review_rows = int(episodes["population_status"].isin(("READY", "TERMINAL_UNAVAILABLE")).sum()) * 19
    if len(rows) != expected_review_rows:
        _raise(
            "L2 review-row count differs from the frozen 19-row episode contract",
            "POSITION_TIMING_L2_POPULATION_INVALID",
            expected_review_rows=expected_review_rows,
            actual_review_rows=len(rows),
        )
    rows.attrs["trading_calendar"] = [value.isoformat() for value in calendar]
    feature_order = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.feature_order
    for column in feature_order:
        rows[column] = pd.to_numeric(rows[column], errors="coerce").astype("float32")
    evaluable = rows["target_available"].astype(bool)
    availability_matrix = rows.loc[evaluable, list(feature_order)].to_numpy(dtype=np.float32)
    finite_by_column = np.isfinite(availability_matrix).sum(axis=0)
    availability = {
        column: {
            "available": int(finite_by_column[index]),
            "missing": int(len(availability_matrix) - finite_by_column[index]),
        }
        for index, column in enumerate(feature_order)
    }
    del availability_matrix
    if any(value["available"] == 0 for value in availability.values()):
        _raise(
            "one or more frozen L2 features are wholly unavailable",
            "POSITION_TIMING_L2_FEATURE_UNAVAILABLE",
            columns=sorted(name for name, value in availability.items() if value["available"] == 0),
        )
    source_identity = {
        "schema_version": POPULATION_SCHEMA,
        "request_sha256": request.request_sha256,
        "dataset_identity_sha256": request.dataset_identity_sha256,
        "population_start": POPULATION_START.isoformat(),
        "population_end": POPULATION_END.isoformat(),
        "cohort_count": len(cohort_dates),
        "expected_episode_count": next_episode_ordinal,
        "episode_count": len(episodes),
        "review_row_count": len(rows),
        "evaluable_review_row_count": int(evaluable.sum()),
        "population_status_counts": {
            str(key): int(value) for key, value in episodes["population_status"].value_counts().items()
        },
        "parent_candidate_full_history_content_hash": False,
        "entry_universe_excludes_st": True,
        "synthetic_st_flag": False,
        "derived_payload_is_content_hashed": True,
    }
    return PopulationBuildResult(
        episodes=episodes,
        rows=rows,
        source_identity=source_identity,
        feature_availability=availability,
    )


def _verify_request_sources(request: FrozenL2LearnabilityRequestV1) -> None:
    if request.contract_sha256 != canonical_sha256(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1):
        _raise("L2 contract changed after request freeze", "POSITION_TIMING_L2_REQUEST_STALE")
    if request.model_runtime_identities != frozen_model_runtime_identities():
        _raise("L2 model runtime identity changed", "POSITION_TIMING_L2_ENVIRONMENT_MISMATCH")
    if _repository_commit(Path(request.repository_root)) != request.repository_commit:
        _raise("L2 repository commit changed", "POSITION_TIMING_L2_REQUEST_STALE")
    dirty = _repository_dirty(Path(request.repository_root))
    if dirty:
        _raise("L2 repository is dirty", "POSITION_TIMING_L2_REPOSITORY_DIRTY", dirty_paths=dirty[:50])
    for role, reference in request.source_refs.items():
        path = Path(reference.artifact_uri)
        if not path.is_file() or path.stat().st_size != reference.size_bytes or sha256_file(path) != reference.sha256:
            _raise(
                "L2 source changed after request freeze",
                "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
                role=role,
                path=path.as_posix(),
            )


def _read_calendar(path: Path) -> pd.DatetimeIndex:
    values = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    try:
        calendar = pd.DatetimeIndex(pd.to_datetime(values, errors="raise")).normalize()
    except (TypeError, ValueError) as exc:
        _raise(
            "L2 trading calendar contains an invalid date",
            "POSITION_TIMING_L2_CALENDAR_INVALID",
            error_type=type(exc).__name__,
        )
    if calendar.empty or calendar.has_duplicates or not calendar.is_monotonic_increasing:
        _raise("L2 trading calendar is invalid", "POSITION_TIMING_L2_CALENDAR_INVALID")
    return calendar


def _read_pit_spans(path: Path) -> pd.DataFrame:
    rows: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            _raise(
                "L2 PIT universe row is malformed",
                "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
                line_number=line_number,
            )
        try:
            eligible_start = pd.Timestamp(parts[1]).normalize()
            eligible_end = pd.Timestamp(parts[2]).normalize()
        except (TypeError, ValueError) as exc:
            _raise(
                "L2 PIT universe row contains an invalid date",
                "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
                line_number=line_number,
                error_type=type(exc).__name__,
            )
        rows.append((parts[0].strip().upper(), eligible_start, eligible_end))
    frame = pd.DataFrame(rows, columns=["instrument", "eligible_start", "eligible_end"])
    if frame.empty or (frame["eligible_end"] < frame["eligible_start"]).any():
        _raise("L2 PIT universe is invalid", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    if frame["instrument"].eq(BENCHMARK).any():
        _raise("L2 stock universe contains the benchmark", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    for _, group in frame.sort_values(["instrument", "eligible_start"]).groupby("instrument"):
        if (
            len(group) > 1
            and (group["eligible_start"].iloc[1:].to_numpy() <= group["eligible_end"].iloc[:-1].to_numpy()).any()
        ):
            _raise("L2 PIT spans overlap", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    return frame


def _load_qlib_market(*, provider_root: Path, instruments: Sequence[str], start: date, end: date) -> pd.DataFrame:
    try:
        import qlib
        from qlib.data import D

        qlib.init(
            provider_uri=str(provider_root),
            region="cn",
            dataset_cache=None,
            expression_cache=None,
            kernels=1,
        )
        raw = D.features(
            instruments=list(instruments),
            fields=[
                "$open",
                "$high",
                "$low",
                "$close",
                "$volume",
                "$factor",
                "$up_limit_price",
                "$down_limit_price",
            ],
            start_time=start.isoformat(),
            end_time=end.isoformat(),
            freq="day",
        )
    except Exception as exc:
        _raise(
            "L2 Qlib daily source cannot be read",
            "POSITION_TIMING_L2_SOURCE_UNAVAILABLE",
            error_type=type(exc).__name__,
        )
    if raw is None or raw.empty:
        _raise("L2 Qlib daily source is empty", "POSITION_TIMING_L2_SOURCE_UNAVAILABLE")
    frame = raw.copy()
    frame.columns = [str(column).lstrip("$") for column in frame.columns]
    frame = frame.reset_index()
    instrument_column = "instrument" if "instrument" in frame else frame.columns[0]
    datetime_column = "datetime" if "datetime" in frame else frame.columns[1]
    frame = frame.rename(columns={instrument_column: "instrument", datetime_column: "datetime"})
    frame["instrument"] = frame["instrument"].astype(str).str.upper()
    frame["datetime"] = pd.to_datetime(frame["datetime"]).dt.normalize()
    return frame.replace([np.inf, -np.inf], np.nan).sort_values(["datetime", "instrument"])


def _directional_status(
    *,
    side: Literal["BUY", "SELL"],
    open_values: np.ndarray,
    high_values: np.ndarray,
    low_values: np.ndarray,
    close_values: np.ndarray,
    factor_values: np.ndarray,
    limit_values: np.ndarray,
    suspended: np.ndarray,
) -> np.ndarray:
    """Return 0 sellable/buyable, 1 explicit directional block, 2 unknown."""

    if side not in {"BUY", "SELL"}:
        raise ValueError("directional status side must be BUY or SELL")

    arrays = [
        np.asarray(open_values, dtype=float),
        np.asarray(high_values, dtype=float),
        np.asarray(low_values, dtype=float),
        np.asarray(close_values, dtype=float),
        np.asarray(factor_values, dtype=float),
        np.asarray(limit_values, dtype=float),
    ]
    shape = arrays[0].shape
    if any(value.shape != shape for value in arrays) or np.asarray(suspended).shape != shape:
        raise ValueError("directional status arrays must have equal shapes")
    known = np.ones(shape, dtype=bool)
    for value in arrays:
        known &= np.isfinite(value) & (value > 0)
    prices = arrays[:4]
    at_limit = np.ones(shape, dtype=bool)
    for value in prices:
        at_limit &= np.abs(value - arrays[5]) < 0.005
    result = np.full(shape, 2, dtype=np.int8)
    result[known] = 0
    result[np.asarray(suspended, dtype=bool)] = 1
    result[known & at_limit] = 1
    return result


def _sell_cost_array(notionals: np.ndarray, *, parent_order_count: int, policy: Mapping[str, Any]) -> np.ndarray:
    if parent_order_count not in PARENT_ORDER_SCENARIOS:
        raise ValueError("parent_order_count must be 1, 2, or 3")
    values = np.asarray(notionals, dtype=float)
    commission_rate = float(policy["net_commission_rate"])
    minimum = float(policy["minimum_commission_cny"])
    other_rate = sum(
        float(policy[name])
        for name in (
            "transfer_fee_rate",
            "regulatory_fee_rate",
            "handling_fee_rate",
            "stamp_duty_sell_rate",
        )
    )
    commission = parent_order_count * np.maximum(minimum, values / parent_order_count * commission_rate)
    result = commission + values * other_rate
    return np.where(np.isfinite(values) & (values > 0), result, np.nan)


def _sell_leg_cost(
    *,
    quantity: int,
    unit_gross_cny: float,
    symbol: str,
    parent_order_count: int,
    full_exit: bool,
    policy: Mapping[str, Any],
) -> float | None:
    parts = split_legal_parent_order_quantities(
        quantity=quantity,
        symbol=symbol,
        side=TriggerSide.SELL,
        requested_count=parent_order_count,
        full_exit=full_exit,
    )
    if parts is None:
        return None
    notionals = np.asarray(parts, dtype=float) * unit_gross_cny
    commission_rate = float(policy["net_commission_rate"])
    minimum = float(policy["minimum_commission_cny"])
    other_rate = sum(
        float(policy[name])
        for name in (
            "transfer_fee_rate",
            "regulatory_fee_rate",
            "handling_fee_rate",
            "stamp_duty_sell_rate",
        )
    )
    return float(np.maximum(minimum, notionals * commission_rate).sum() + notionals.sum() * other_rate)


def run_l2_crossfit(*, rows: pd.DataFrame, paths: Sequence[Mapping[str, Any]], model_id: str) -> CrossfitResult:
    """Fit one frozen model on every CPCV path and aggregate seven OOF values."""

    if model_id not in MODEL_ORDER:
        raise ValueError(f"unknown frozen L2 model: {model_id}")
    feature_order = list(POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.feature_order)
    missing = set(feature_order) | {
        "entry_decision_date",
        "target_available",
        "full_exit_incremental_net_value_bps",
    }
    missing -= set(rows)
    if missing:
        _raise("L2 crossfit input schema drift", "POSITION_TIMING_L2_CROSSFIT_INVALID", missing=sorted(missing))
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import StandardScaler

    features = rows[feature_order].to_numpy(dtype=np.float64)
    target = pd.to_numeric(rows["full_exit_incremental_net_value_bps"], errors="coerce").to_numpy(dtype=float)
    available = rows["target_available"].astype(bool).to_numpy() & np.isfinite(target)
    entry_dates = pd.to_datetime(rows["entry_decision_date"]).to_numpy(dtype="datetime64[D]")
    prediction_sum = np.zeros(len(rows), dtype=np.float64)
    exposure_votes = np.zeros((len(rows), 4), dtype=np.int8)
    counts = np.zeros(len(rows), dtype=np.int8)
    diagnostics: list[dict[str, Any]] = []
    model_spec = next(item for item in POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.models if item.model_id == model_id)
    for path in paths:
        train_dates = np.asarray(path["train_dates"], dtype="datetime64[D]")
        validation_dates = np.asarray(path["validation_dates"], dtype="datetime64[D]")
        train_mask = available & np.isin(entry_dates, train_dates)
        validation_mask = available & np.isin(entry_dates, validation_dates)
        if not train_mask.any() or not validation_mask.any() or np.any(train_mask & validation_mask):
            _raise(
                "L2 CPCV path has invalid row membership",
                "POSITION_TIMING_L2_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
            )
        train_x_raw = features[train_mask]
        validation_x_raw = features[validation_mask]
        finite_counts = np.isfinite(train_x_raw).sum(axis=0)
        validation_finite_counts = np.isfinite(validation_x_raw).sum(axis=0)
        if np.any(finite_counts == 0):
            _raise(
                "L2 training fold has an all-missing frozen feature",
                "POSITION_TIMING_L2_FEATURE_UNAVAILABLE",
                path_id=path.get("path_id"),
                columns=[feature_order[index] for index in np.flatnonzero(finite_counts == 0)],
            )
        imputer = SimpleImputer(strategy="median")
        train_x = imputer.fit_transform(train_x_raw)
        validation_x = imputer.transform(validation_x_raw)
        if model_id == "SKLEARN_RIDGE_V1":
            scaler = StandardScaler()
            train_x = scaler.fit_transform(train_x)
            validation_x = scaler.transform(validation_x)
            estimator: Any = Ridge(**model_spec.parameters)
        else:
            from lightgbm import LGBMRegressor

            parameters = dict(model_spec.parameters)
            parameters.pop("early_stopping")
            estimator = LGBMRegressor(**parameters)
        estimator.fit(train_x, target[train_mask])
        train_prediction = np.asarray(estimator.predict(train_x), dtype=float)
        validation_prediction = np.asarray(estimator.predict(validation_x), dtype=float)
        if not np.isfinite(train_prediction).all() or not np.isfinite(validation_prediction).all():
            _raise(
                "L2 estimator produced non-finite predictions",
                "POSITION_TIMING_L2_CROSSFIT_INVALID",
                path_id=path.get("path_id"),
                model_id=model_id,
            )
        positive = train_prediction[train_prediction > 0]
        if positive.size:
            q50, q75 = np.quantile(positive, [0.50, 0.75]).tolist()
            reason_code = None
            path_exposure = map_monotone_exposure(validation_prediction, q50, q75)
        else:
            q50 = q75 = 0.0
            reason_code = "NO_POSITIVE_TRAIN_PREDICTIONS"
            path_exposure = np.ones(len(validation_prediction), dtype=np.float32)
        indices = np.flatnonzero(validation_mask)
        prediction_sum[indices] += validation_prediction
        for vote_index, level in enumerate((0.0, 0.25, 0.50, 1.0)):
            exposure_votes[indices, vote_index] += path_exposure == level
        counts[indices] += 1
        diagnostics.append(
            {
                "path_id": str(path["path_id"]),
                "model_id": model_id,
                "train_row_count": int(train_mask.sum()),
                "validation_row_count": int(validation_mask.sum()),
                "positive_train_prediction_count": int(positive.size),
                "positive_train_q50_bps": q50 if positive.size else None,
                "positive_train_q75_bps": q75 if positive.size else None,
                "reason_code": reason_code,
                "feature_availability": {
                    feature: {
                        "train_available": int(finite_counts[index]),
                        "train_missing": int(len(train_x_raw) - finite_counts[index]),
                        "validation_available": int(validation_finite_counts[index]),
                        "validation_missing": int(len(validation_x_raw) - validation_finite_counts[index]),
                    }
                    for index, feature in enumerate(feature_order)
                },
            }
        )
        del train_x_raw, validation_x_raw, train_x, validation_x, estimator, train_prediction
    expected = POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.cross_validation_spec["oof_predictions_per_row"]
    if not np.all(counts[available] == expected) or np.any(counts[~available] != 0):
        _raise(
            "L2 OOF multiplicity differs from the frozen seven predictions",
            "POSITION_TIMING_L2_CROSSFIT_INVALID",
            minimum=int(counts[available].min()) if available.any() else 0,
            maximum=int(counts[available].max()) if available.any() else 0,
        )
    predictions = np.full(len(rows), np.nan, dtype=np.float64)
    predictions[available] = prediction_sum[available] / counts[available]
    exposures = np.full(len(rows), np.nan, dtype=np.float32)
    # Seven paths give an unambiguous fourth-order statistic.  Aggregating the
    # per-path decisions (rather than their quantiles) preserves the frozen
    # four-level action set and the all-HOLD semantics of a no-positive fold.
    cumulative = np.cumsum(exposure_votes[available], axis=1)
    median_indices = np.argmax(cumulative >= 4, axis=1)
    exposures[available] = np.asarray((0.0, 0.25, 0.50, 1.0), dtype=np.float32)[median_indices]
    return CrossfitResult(
        model_id=model_id,
        predictions=predictions,
        target_exposures=exposures,
        oof_counts=counts,
        path_diagnostics=tuple(diagnostics),
    )


def evaluate_l2_policy(
    *,
    rows: pd.DataFrame,
    crossfit: CrossfitResult,
    cost_policy: Mapping[str, Any],
    model_offset: int,
    deployment_cell_counts: Mapping[str, int] | None = None,
) -> tuple[L2HypothesisResultV1, pd.DataFrame, pd.DataFrame]:
    """Evaluate the complete monotone exposure path against do-nothing."""

    required = {
        "episode_ordinal",
        "cohort_ordinal",
        "canonical_symbol",
        "entry_decision_date",
        "holding_session",
        "initial_quantity",
        "st_flag",
        "action_status_code",
        "action_full_gross_notional_cny",
        "terminal_full_gross_notional_cny",
        "entry_gross_notional_cny",
        "target_available",
        "full_exit_incremental_net_value_bps",
        "market_regime_down",
        "market_regime_up_or_flat",
    }
    if missing := required - set(rows):
        _raise(
            "L2 policy input schema drift",
            "POSITION_TIMING_L2_INFERENCE_UNAVAILABLE",
            missing=sorted(missing),
        )
    if len(rows) != len(crossfit.target_exposures):
        raise ValueError("crossfit exposure length differs from L2 review rows")
    order = np.lexsort(
        (
            pd.to_numeric(rows["holding_session"], errors="raise").to_numpy(),
            pd.to_numeric(rows["episode_ordinal"], errors="raise").to_numpy(),
        )
    )
    frame = rows.iloc[order].reset_index(drop=True)
    exposures = crossfit.target_exposures[order]
    episode_values = frame["episode_ordinal"].to_numpy(dtype=np.int64)
    starts = np.r_[0, np.flatnonzero(episode_values[1:] != episode_values[:-1]) + 1]
    ends = np.r_[starts[1:], len(frame)]
    records: list[dict[str, Any]] = []
    for start, end in zip(starts, ends, strict=True):
        group = frame.iloc[start:end]
        entry_gross = float(group["entry_gross_notional_cny"].iloc[0])
        terminal_gross = float(group["terminal_full_gross_notional_cny"].iloc[0])
        initial_quantity = int(group["initial_quantity"].iloc[0])
        symbol = str(group["canonical_symbol"].iloc[0])
        if (
            not np.isfinite(entry_gross)
            or entry_gross <= 0
            or not np.isfinite(terminal_gross)
            or terminal_gross <= 0
            or initial_quantity <= 0
        ):
            continue
        action_legs: list[tuple[int, float, bool]] = []
        effective_quantity = initial_quantity
        first_action_session: int | None = None
        first_action_regime = "UNKNOWN"
        for local_index, item in enumerate(group.itertuples(index=False)):
            mapped = float(exposures[start + local_index])
            if not np.isfinite(mapped) or int(item.action_status_code) != 0:
                continue
            desired_remaining = int(math.floor(initial_quantity * mapped + 1e-9))
            desired_sell = effective_quantity - desired_remaining
            if desired_sell <= 0:
                continue
            full_exit = mapped <= 0 or desired_sell >= effective_quantity
            if full_exit:
                sell_quantity = effective_quantity
            else:
                sell_quantity = round_to_board_lot(
                    desired_sell,
                    symbol,
                    side="SELL",
                    allow_sell_residual=False,
                )
            if sell_quantity <= 0:
                continue
            full_gross = float(item.action_full_gross_notional_cny)
            unit_gross = full_gross / initial_quantity
            if not np.isfinite(unit_gross) or unit_gross <= 0:
                continue
            action_legs.append((sell_quantity, unit_gross, full_exit))
            effective_quantity -= sell_quantity
            if first_action_session is None:
                first_action_session = int(item.holding_session)
                if float(item.market_regime_down) == 1.0:
                    first_action_regime = "DOWN"
                elif float(item.market_regime_up_or_flat) == 1.0:
                    first_action_regime = "UP_OR_FLAT"

        scenario_lifts: dict[int, float] = {}
        scenario_available: dict[int, bool] = {}
        terminal_unit_gross = terminal_gross / initial_quantity
        for parent_orders in PARENT_ORDER_SCENARIOS:
            cash = 0.0
            available = True
            for sell_quantity, unit_gross, full_exit in action_legs:
                cost = _sell_leg_cost(
                    quantity=sell_quantity,
                    unit_gross_cny=unit_gross,
                    symbol=symbol,
                    parent_order_count=parent_orders,
                    full_exit=full_exit,
                    policy=cost_policy,
                )
                if cost is None:
                    available = False
                    break
                cash += sell_quantity * unit_gross - cost
            if available and effective_quantity > 0:
                cost = _sell_leg_cost(
                    quantity=effective_quantity,
                    unit_gross_cny=terminal_unit_gross,
                    symbol=symbol,
                    parent_order_count=parent_orders,
                    full_exit=True,
                    policy=cost_policy,
                )
                if cost is None:
                    available = False
                else:
                    cash += effective_quantity * terminal_unit_gross - cost
            baseline_cost = _sell_leg_cost(
                quantity=initial_quantity,
                unit_gross_cny=terminal_unit_gross,
                symbol=symbol,
                parent_order_count=parent_orders,
                full_exit=True,
                policy=cost_policy,
            )
            if baseline_cost is None:
                available = False
            scenario_available[parent_orders] = available
            scenario_lifts[parent_orders] = (
                (cash - (terminal_gross - baseline_cost)) / entry_gross * 10000.0
                if available and baseline_cost is not None
                else math.nan
            )
        available_targets = pd.to_numeric(
            group.loc[group["target_available"].astype(bool), "full_exit_incremental_net_value_bps"],
            errors="coerce",
        ).dropna()
        oracle = max(0.0, float(available_targets.max())) if len(available_targets) else 0.0
        records.append(
            {
                "episode_ordinal": int(group["episode_ordinal"].iloc[0]),
                "cohort_ordinal": int(group["cohort_ordinal"].iloc[0]),
                "entry_decision_date": pd.Timestamp(group["entry_decision_date"].iloc[0]).normalize(),
                "oracle_lift_bps": oracle,
                "action_side": "SELL" if first_action_session is not None else "NONE",
                "policy_action_sell_quantity": initial_quantity - effective_quantity,
                "policy_terminal_sell_quantity": effective_quantity,
                "holding_age_bucket": _holding_age_bucket(first_action_session),
                "market_regime": first_action_regime,
                "st_flag": bool(group["st_flag"].iloc[0]),
                **{f"policy_lift_parent_orders_{scenario}_bps": value for scenario, value in scenario_lifts.items()},
                **{f"parent_orders_{scenario}_available": value for scenario, value in scenario_available.items()},
            }
        )
    episode = pd.DataFrame(records)
    if episode.empty:
        _raise("L2 policy has no paired episodes", "POSITION_TIMING_L2_INFERENCE_UNAVAILABLE")
    cohort_columns = {
        "oracle_lift_bps": ("oracle_lift_bps", "mean"),
        **{
            f"policy_lift_parent_orders_{scenario}_bps": (
                f"policy_lift_parent_orders_{scenario}_bps",
                "mean",
            )
            for scenario in PARENT_ORDER_SCENARIOS
        },
    }
    cohort = (
        episode.groupby(["cohort_ordinal", "entry_decision_date"], as_index=False)
        .agg(**cohort_columns)
        .sort_values("entry_decision_date")
        .reset_index(drop=True)
    )
    oracle_mean = float(cohort["oracle_lift_bps"].mean())
    sensitivity: dict[str, dict[str, Any]] = {}
    base_values: tuple[float, float, float, float] | None = None
    base_adjusted: tuple[float, float, float, float] | None = None
    for scenario in PARENT_ORDER_SCENARIOS:
        column = f"policy_lift_parent_orders_{scenario}_bps"
        values = pd.to_numeric(cohort[column], errors="coerce").dropna().to_numpy(dtype=float)
        episode_available_count = int(pd.to_numeric(episode[column], errors="coerce").notna().sum())
        if len(values) < 2:
            sensitivity[f"parent_orders_{scenario}"] = {
                "status": "UNAVAILABLE_LEGAL_SPLIT_COVERAGE",
                "episode_available_count": episode_available_count,
                "episode_total_count": len(episode),
                "cohort_available_count": len(values),
                "cohort_total_count": len(cohort),
                "point_estimate_bps": None,
                "adjusted_lower_bps": None,
                "adjusted_upper_bps": None,
                "effect_evidence": None,
            }
            if scenario == 1:
                _raise(
                    "base parent-order policy lacks paired cohorts",
                    "POSITION_TIMING_L2_INFERENCE_UNAVAILABLE",
                )
            continue
        nominal = circular_block_interval(
            values,
            alpha=0.05,
            seed=20260903 + model_offset,
        )
        adjusted = circular_block_interval(
            values,
            alpha=0.025,
            seed=20260903 + model_offset,
        )
        sensitivity_status = (
            "AVAILABLE_DIAGNOSTIC_ONLY"
            if episode_available_count == len(episode)
            else "AVAILABLE_PARTIAL_LEGAL_SPLIT_DIAGNOSTIC_ONLY"
        )
        sensitivity[f"parent_orders_{scenario}"] = {
            "status": sensitivity_status,
            "episode_available_count": episode_available_count,
            "episode_total_count": len(episode),
            "cohort_available_count": len(values),
            "cohort_total_count": len(cohort),
            "point_estimate_bps": adjusted[0],
            "adjusted_lower_bps": adjusted[1],
            "adjusted_upper_bps": adjusted[2],
            "effect_evidence": classify_effect(lower_bps=adjusted[1], upper_bps=adjusted[2]),
        }
        if scenario == 1:
            base_values = (*nominal[:3], adjusted[3])
            base_adjusted = adjusted
    if base_values is None or base_adjusted is None:
        _raise("base L2 inference was not materialized", "POSITION_TIMING_L2_INFERENCE_UNAVAILABLE")
    _, nominal_lower, nominal_upper, _ = base_values
    adjusted_point, adjusted_lower, adjusted_upper, adjusted_se = base_adjusted
    effect = classify_effect(lower_bps=adjusted_lower, upper_bps=adjusted_upper)
    reasons: list[str] = []
    if nominal_lower > 0 and adjusted_lower <= 0:
        reasons.append("MULTIPLICITY_ADJUSTMENT_ERASED_NOMINAL_SIGNAL")
    z_adjusted = NormalDist().inv_cdf(1.0 - 0.025 / 2.0)
    mde = float((z_adjusted + _Z_80) * adjusted_se)
    ratio = mde / oracle_mean if oracle_mean > 0 else None
    power_status = "ADEQUATE" if ratio is not None and ratio <= 0.25 else "UNDERPOWERED"
    if power_status == "UNDERPOWERED":
        reasons.append("EXPLORATORY_UNDERPOWERED")
    cost_sensitive = effect == "SUPPORTED" and any(
        sensitivity[f"parent_orders_{scenario}"]["status"] == "AVAILABLE_DIAGNOSTIC_ONLY"
        and sensitivity[f"parent_orders_{scenario}"]["adjusted_lower_bps"] <= 0
        for scenario in (2, 3)
    )
    if cost_sensitive:
        reasons.append("COST_ASSUMPTION_SENSITIVE")
    if effect == "SUPPORTED" and any(
        sensitivity[f"parent_orders_{scenario}"]["status"] != "AVAILABLE_DIAGNOSTIC_ONLY" for scenario in (2, 3)
    ):
        reasons.append("COST_SENSITIVITY_PARTIALLY_UNAVAILABLE")
    deployment_point, deployment_status, unsupported_cells = _deployment_weighted_result(
        episode,
        deployment_cell_counts or {},
    )
    result = L2HypothesisResultV1(
        model_id=crossfit.model_id,
        point_estimate_bps=adjusted_point,
        nominal_interval=L2IntervalV1(lower_bps=nominal_lower, upper_bps=nominal_upper, alpha=0.05),
        adjusted_interval=L2IntervalV1(
            lower_bps=adjusted_lower,
            upper_bps=adjusted_upper,
            alpha=0.025,
        ),
        effect_evidence=effect,
        evidence_reason_codes=tuple(reasons),
        power_status=power_status,
        mde_bps=mde,
        oracle_mean_lift_bps=oracle_mean,
        mde_oracle_ratio=ratio,
        cohort_count=len(cohort),
        paired_episode_count=len(episode),
        cost_sensitivity=sensitivity,
        cost_assumption_sensitive=cost_sensitive,
        deployment_weighted_point_bps=deployment_point,
        deployment_weighted_status=deployment_status,
        unsupported_deployment_cells=unsupported_cells,
    )
    return result, cohort, episode


def _sell_cost_scalar(notional: float, *, parent_order_count: int, policy: Mapping[str, Any]) -> float:
    return float(
        _sell_cost_array(
            np.asarray([notional], dtype=float),
            parent_order_count=parent_order_count,
            policy=policy,
        )[0]
    )


def _holding_age_bucket(session: int | None) -> str:
    if session is None:
        return "UNKNOWN"
    if session == 0:
        return "AGE_0"
    if session <= 3:
        return "AGE_1_3"
    if session <= 5:
        return "AGE_4_5"
    if session <= 10:
        return "AGE_6_10"
    if session <= 20:
        return "AGE_11_20"
    return "AGE_21_PLUS"


def _deployment_cell_key(
    *,
    source_role: str,
    action_side: str,
    holding_age_bucket: str,
    market_regime: str,
    st_flag: bool,
) -> str:
    return "|".join(
        (
            source_role,
            action_side,
            holding_age_bucket,
            market_regime,
            "1" if st_flag else "0",
        )
    )


def _deployment_weighted_result(
    episode: pd.DataFrame, deployment_cell_counts: Mapping[str, int]
) -> tuple[float | None, str, tuple[str, ...]]:
    if not deployment_cell_counts:
        return None, "UNAVAILABLE_NO_DEPLOYMENT_CELLS", ()
    frame = episode.copy()
    frame["deployment_cell"] = [
        _deployment_cell_key(
            source_role="HOLDING",
            action_side=str(item.action_side),
            holding_age_bucket=str(item.holding_age_bucket),
            market_regime=str(item.market_regime),
            st_flag=bool(item.st_flag),
        )
        for item in frame.itertuples(index=False)
    ]
    means = frame.groupby("deployment_cell")["policy_lift_parent_orders_1_bps"].mean().to_dict()
    unsupported = tuple(sorted(key for key in deployment_cell_counts if key not in means))
    if unsupported:
        return None, "UNAVAILABLE_UNSUPPORTED_DEPLOYMENT_CELLS", unsupported
    total = sum(int(value) for value in deployment_cell_counts.values())
    if total <= 0:
        return None, "UNAVAILABLE_NO_DEPLOYMENT_CELLS", ()
    weighted = sum(float(means[key]) * int(count) for key, count in deployment_cell_counts.items()) / total
    return float(weighted), "AVAILABLE_DIAGNOSTIC_ONLY", ()


def run_l2_learnability_audit(request_path: str | Path) -> dict[str, Any]:
    """Run and publish the two-hypothesis audit, then append its own registry."""

    request_file = Path(request_path).resolve()
    request = FrozenL2LearnabilityRequestV1.model_validate_json(request_file.read_text(encoding="utf-8"))
    existing = _find_existing_bundle(request)
    if existing is not None:
        loaded = inspect_l2_learnability_bundle(existing)
        registry = _deliver_registry(request=request, bundle=existing, loaded=loaded)
        return {
            "status": "EXACT_RETRY",
            "bundle_path": existing.as_posix(),
            "bundle_id": loaded["bundle_id"],
            "receipt_sha256": loaded["receipt"].receipt_sha256,
            "effect_evidence": loaded["receipt"].effect_evidence,
            "registry": registry,
        }
    _verify_request_sources(request)
    population = materialize_l2_population(request)
    paths = build_l2_cpcv_paths(population.rows, request_sha256=request.request_sha256)
    crossfits = [run_l2_crossfit(rows=population.rows, paths=paths, model_id=model_id) for model_id in MODEL_ORDER]
    evaluated = [
        evaluate_l2_policy(
            rows=population.rows,
            crossfit=crossfit,
            cost_policy=request.cost_policy,
            model_offset=index,
            deployment_cell_counts=request.deployment_cell_counts,
        )
        for index, crossfit in enumerate(crossfits)
    ]
    hypotheses = tuple(item[0] for item in evaluated)
    if any(item.effect_evidence == "SUPPORTED" for item in hypotheses):
        study_effect = "SUPPORTED"
    elif all(item.effect_evidence == "NEGATIVE" for item in hypotheses):
        study_effect = "NEGATIVE"
    else:
        study_effect = "INCONCLUSIVE"
    selected_model = choose_supported_model(hypotheses)

    bundle_parent = Path(request.output_root) / "research" / "l2_learnability_bundles"
    bundle_parent.mkdir(parents=True, exist_ok=True)
    staging: Path | None = Path(tempfile.mkdtemp(prefix=".ptl2-staging-", dir=bundle_parent))
    try:
        _write_json(staging / "request.json", request.model_dump(mode="json"))
        _write_json(staging / "source_identity_receipt.json", population.source_identity)
        _write_json(
            staging / "cpcv_paths.json",
            {
                "schema_version": "position_timing_l2_cpcv_paths_v1",
                "request_sha256": request.request_sha256,
                "paths": list(paths),
            },
        )
        _write_json(
            staging / "model_runtime_identities.json",
            request.model_runtime_identities,
        )
        _write_parquet(staging / "episodes.parquet", population.episodes)
        _write_parquet(staging / "review_rows.parquet", population.rows)
        oof = population.rows[
            [
                "episode_ordinal",
                "cohort_ordinal",
                "entry_decision_date",
                "review_decision_date",
                "holding_session",
                "target_available",
                "full_exit_incremental_net_value_bps",
            ]
        ].copy()
        for crossfit in crossfits:
            prefix = "ridge" if crossfit.model_id == "SKLEARN_RIDGE_V1" else "gbdt"
            oof[f"{prefix}_prediction_bps"] = crossfit.predictions
            oof[f"{prefix}_target_exposure"] = crossfit.target_exposures
            oof[f"{prefix}_oof_count"] = crossfit.oof_counts
        _write_parquet(staging / "oof_predictions.parquet", oof)
        cohort_frames: list[pd.DataFrame] = []
        episode_policy_frames: list[pd.DataFrame] = []
        for crossfit, (_, cohort, episode) in zip(crossfits, evaluated, strict=True):
            cohort = cohort.copy()
            cohort.insert(0, "model_id", crossfit.model_id)
            cohort_frames.append(cohort)
            episode = episode.copy()
            episode.insert(0, "model_id", crossfit.model_id)
            episode_policy_frames.append(episode)
        _write_parquet(staging / "cohort_policy.parquet", pd.concat(cohort_frames, ignore_index=True))
        _write_parquet(
            staging / "episode_policy.parquet",
            pd.concat(episode_policy_frames, ignore_index=True),
        )
        _write_json(
            staging / "path_diagnostics.json",
            {
                "schema_version": "position_timing_l2_path_diagnostics_v1",
                "models": {item.model_id: list(item.path_diagnostics) for item in crossfits},
            },
        )
        preliminary = _file_descriptors(staging)
        derived_dataset_sha256 = canonical_sha256(
            {name: preliminary[name] for name in ("episodes.parquet", "review_rows.parquet")}
        )
        deployment_statuses = sorted({item.deployment_weighted_status for item in hypotheses})
        receipt_values: dict[str, Any] = {
            "schema_version": RECEIPT_SCHEMA,
            "request_sha256": request.request_sha256,
            "dataset_identity_sha256": request.dataset_identity_sha256,
            "derived_dataset_sha256": derived_dataset_sha256,
            "contract_sha256": request.contract_sha256,
            "repository_commit": request.repository_commit,
            "effect_evidence": study_effect,
            "selected_model_id": selected_model,
            "hypothesis_count": 2,
            "economic_threshold_bps": 0.0,
            "hypotheses": hypotheses,
            "population_counts": {
                "cohort_count": int(population.source_identity["cohort_count"]),
                "episode_count": int(population.source_identity["episode_count"]),
                "review_row_count": int(population.source_identity["review_row_count"]),
                "evaluable_review_row_count": int(population.source_identity["evaluable_review_row_count"]),
                **{
                    f"status_{key}": int(value)
                    for key, value in population.source_identity["population_status_counts"].items()
                },
            },
            "feature_availability": population.feature_availability,
            "prospective_context": {
                "event_counts": request.prospective_event_counts,
                "outcome_event_count": request.prospective_outcome_event_count,
                "intervention_intent_count": request.prospective_intervention_intent_count,
                "deployment_cell_counts": request.deployment_cell_counts,
                "classification_gate": False,
                "l1_l1a_gate": False,
            },
            "deployment_weighted_status": ",".join(deployment_statuses),
            "historical_registry_context_count": request.historical_registry_context_count,
            "sealed_holdout_accessed": False,
            "runtime_model_written": False,
            "l1_l1a_gate_applied": False,
            "global_registry_written": False,
            "current_route_written": False,
        }
        constructed = L2LearnabilityReceiptV1.model_construct(
            receipt_id="ptl2rcpt_" + "0" * 24,
            receipt_sha256="0" * 64,
            created_at=datetime.now(timezone.utc),
            **receipt_values,
        )
        receipt_digest = canonical_sha256(constructed.functional_payload())
        receipt = L2LearnabilityReceiptV1(
            receipt_id=f"ptl2rcpt_{receipt_digest[:24]}",
            receipt_sha256=receipt_digest,
            created_at=datetime.now(timezone.utc),
            **receipt_values,
        )
        _write_json(staging / "learnability_receipt.json", receipt.model_dump(mode="json"))
        bundle_id = canonical_sha256(
            {
                "request_sha256": request.request_sha256,
                "derived_dataset_sha256": derived_dataset_sha256,
                "receipt_sha256": receipt.receipt_sha256,
            }
        )
        final = bundle_parent / bundle_id
        receipt_descriptor = _descriptor(staging / "learnability_receipt.json")
        records = _build_registry_records(
            request=request,
            receipt=receipt,
            receipt_descriptor=receipt_descriptor,
            final_receipt_path=final / "learnability_receipt.json",
        )
        _write_json(
            staging / "registry_records.json",
            [item.model_dump(mode="json") for item in records],
        )
        descriptors = _file_descriptors(staging)
        manifest = {
            "schema_version": BUNDLE_SCHEMA,
            "bundle_id": bundle_id,
            "request_sha256": request.request_sha256,
            "receipt_sha256": receipt.receipt_sha256,
            "dataset_identity_sha256": request.dataset_identity_sha256,
            "derived_dataset_sha256": derived_dataset_sha256,
            "effect_evidence": study_effect,
            "selected_model_id": selected_model,
            "hypothesis_count": 2,
            "runtime_model_written": False,
            "global_registry_written": False,
            "current_route_written": False,
            "files": descriptors,
        }
        _write_json(staging / "manifest.json", manifest)
        if final.exists():
            loaded = inspect_l2_learnability_bundle(final)
            if loaded["receipt"].receipt_sha256 != receipt.receipt_sha256:
                _raise(
                    "L2 bundle identity conflicts with existing content",
                    "POSITION_TIMING_L2_IMMUTABLE_CONFLICT",
                    bundle_path=final.as_posix(),
                )
        else:
            os.replace(staging, final)
            staging = None
        loaded = inspect_l2_learnability_bundle(final)
        registry = _deliver_registry(request=request, bundle=final, loaded=loaded)
        return {
            "status": "AUDIT_PUBLISHED",
            "bundle_path": final.as_posix(),
            "bundle_id": bundle_id,
            "receipt_sha256": receipt.receipt_sha256,
            "effect_evidence": study_effect,
            "selected_model_id": selected_model,
            "prospective_outcome_event_count": request.prospective_outcome_event_count,
            "registry": registry,
        }
    finally:
        if staging is not None and staging.exists():
            try:
                staging.relative_to(bundle_parent)
            except ValueError:
                pass
            else:
                shutil.rmtree(staging)


def inspect_l2_learnability_bundle(bundle_path: str | Path) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    manifest = _read_json(root / "manifest.json")
    if manifest.get("schema_version") != BUNDLE_SCHEMA or manifest.get("bundle_id") != root.name:
        _raise("L2 bundle manifest identity drift", "POSITION_TIMING_L2_BUNDLE_INVALID")
    request = FrozenL2LearnabilityRequestV1.model_validate_json((root / "request.json").read_text(encoding="utf-8"))
    receipt = L2LearnabilityReceiptV1.model_validate_json(
        (root / "learnability_receipt.json").read_text(encoding="utf-8")
    )
    records_payload = json.loads((root / "registry_records.json").read_text(encoding="utf-8"))
    records = tuple(AdvisoryResearchTrialRecordV1.model_validate(item) for item in records_payload)
    source_identity = _read_json(root / "source_identity_receipt.json")
    descriptors = manifest.get("files")
    if not isinstance(descriptors, dict):
        _raise("L2 bundle file manifest is missing", "POSITION_TIMING_L2_BUNDLE_INVALID")
    actual = _file_descriptors(root, exclude_manifest=True)
    derived_dataset_sha256 = canonical_sha256(
        {name: actual[name] for name in ("episodes.parquet", "review_rows.parquet")}
    )
    expected_records = _build_registry_records(
        request=request,
        receipt=receipt,
        receipt_descriptor=actual["learnability_receipt.json"],
        final_receipt_path=root / "learnability_receipt.json",
    )
    expected_id = canonical_sha256(
        {
            "request_sha256": request.request_sha256,
            "derived_dataset_sha256": receipt.derived_dataset_sha256,
            "receipt_sha256": receipt.receipt_sha256,
        }
    )
    if (
        actual != descriptors
        or expected_id != root.name
        or request.request_sha256 != manifest.get("request_sha256")
        or receipt.request_sha256 != request.request_sha256
        or receipt.receipt_sha256 != manifest.get("receipt_sha256")
        or receipt.dataset_identity_sha256 != request.dataset_identity_sha256
        or receipt.derived_dataset_sha256 != derived_dataset_sha256
        or manifest.get("derived_dataset_sha256") != derived_dataset_sha256
        or manifest.get("effect_evidence") != receipt.effect_evidence
        or manifest.get("selected_model_id") != receipt.selected_model_id
        or source_identity.get("request_sha256") != request.request_sha256
        or source_identity.get("dataset_identity_sha256") != request.dataset_identity_sha256
        or len(records) != 2
        or tuple(record.record_sha256 for record in records)
        != tuple(record.record_sha256 for record in expected_records)
        or tuple(record.unique_variable for record in records) != MODEL_ORDER
        or sum(record.selected_trial_count for record in records) > 1
        or bool(manifest.get("runtime_model_written"))
        or bool(manifest.get("global_registry_written"))
        or bool(manifest.get("current_route_written"))
    ):
        _raise("L2 bundle readback validation failed", "POSITION_TIMING_L2_BUNDLE_INVALID")
    return {
        "bundle_id": root.name,
        "manifest": manifest,
        "request": request,
        "receipt": receipt,
        "records": records,
    }


def _build_registry_records(
    *,
    request: FrozenL2LearnabilityRequestV1,
    receipt: L2LearnabilityReceiptV1,
    receipt_descriptor: Mapping[str, Any],
    final_receipt_path: Path,
) -> tuple[Any, Any]:
    receipt_ref = EvidenceReferenceV1(
        role="position_timing_l2_learnability_receipt",
        artifact_uri=final_receipt_path.as_posix(),
        sha256=str(receipt_descriptor["sha256"]),
        size_bytes=int(receipt_descriptor["size_bytes"]),
    )
    selected = receipt.selected_model_id
    records = []
    for result in receipt.hypotheses:
        if result.effect_evidence == "SUPPORTED":
            result_class = ResearchResultClass.CONTROL_READY
            decision_use = DecisionUse.DIRECTION_GATE
        elif result.effect_evidence == "NEGATIVE":
            result_class = ResearchResultClass.NEGATIVE
            decision_use = DecisionUse.DIRECTION_GATE
        else:
            result_class = ResearchResultClass.EXPLORATORY
            decision_use = DecisionUse.NAVIGATION_ONLY
        model_identity = request.model_runtime_identities[result.model_id]
        records.append(
            build_trial_record(
                experiment_id=f"position_timing_l2_{result.model_id.lower()}",
                attempt_id=request.request_id,
                research_stage="POSITION_TIMING_L2_LEARNABILITY_AUDIT",
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                hypothesis_family_id="POSITION_TIMING_L2_RIDGE_GBDT_V1",
                parent_lineage=("POSITION_TIMING_ADVICE_V1", request.dataset_identity_sha256),
                unique_variable=result.model_id,
                objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
                dataset_identity=request.dataset_identity_sha256,
                schema_identity=request.feature_schema_sha256,
                policy_identity=canonical_sha256(
                    {
                        "model_identity_sha256": model_identity["identity_sha256"],
                        "policy_id": POSITION_TIMING_L2_RESEARCH_CONTRACT_V1.policy_id,
                        "cost_policy_sha256": request.cost_policy_sha256,
                    }
                ),
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                selected_trial_count=int(result.model_id == selected),
                consumed_windows=(
                    ConsumedWindowV1(
                        window_id="POSITION_TIMING_L2_DEVELOPMENT_V1",
                        dataset_identity=request.dataset_identity_sha256,
                        start_date=POPULATION_START,
                        end_date=POPULATION_END,
                    ),
                ),
                result_class=result_class,
                decision_use=decision_use,
                evidence_refs=(receipt_ref,),
            )
        )
    return tuple(records)  # type: ignore[return-value]


def _deliver_registry(
    *, request: FrozenL2LearnabilityRequestV1, bundle: Path, loaded: Mapping[str, Any]
) -> dict[str, Any]:
    historical_ref = request.source_refs["historical_registry"]
    historical_path = Path(historical_ref.artifact_uri)
    if not historical_path.is_file():
        _raise("global N0 registry is unavailable for zero-write proof", "POSITION_TIMING_L2_SOURCE_UNAVAILABLE")
    historical_before = (historical_path.stat().st_size, sha256_file(historical_path))
    current_context_count = len(AdvisoryResearchTrialRegistryV1(historical_path).read())
    expected_registry = Path(request.timing_artifact_root) / "research_registry" / "timing_trial_registry_v1.jsonl"
    summary = AdvisoryResearchTrialRegistryV1(expected_registry).append_batch(loaded["records"])
    historical_after = (historical_path.stat().st_size, sha256_file(historical_path))
    if historical_after != historical_before:
        _raise("L2 delivery changed the global N0 registry", "POSITION_TIMING_L2_ISOLATION_VIOLATION")
    return {
        **summary,
        "global_registry_unchanged": True,
        "request_historical_registry_context_count": request.historical_registry_context_count,
        "delivery_historical_registry_context_count": current_context_count,
        "current_route_written": False,
        "bundle_path": bundle.as_posix(),
    }


def _find_existing_bundle(request: FrozenL2LearnabilityRequestV1) -> Path | None:
    parent = Path(request.output_root) / "research" / "l2_learnability_bundles"
    if not parent.is_dir():
        return None
    matches: list[Path] = []
    for child in parent.iterdir():
        manifest = child / "manifest.json"
        if not child.is_dir() or child.name.startswith(".") or not manifest.is_file():
            continue
        try:
            value = json.loads(manifest.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if value.get("request_sha256") == request.request_sha256:
            matches.append(child)
    if len(matches) > 1:
        _raise("one L2 request resolved multiple bundles", "POSITION_TIMING_L2_BUNDLE_INVALID")
    return matches[0] if matches else None


def _descriptor(path: Path) -> dict[str, Any]:
    value: dict[str, Any] = {
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        value["row_count"] = pq.ParquetFile(path).metadata.num_rows
    return value


def _file_descriptors(root: Path, *, exclude_manifest: bool = False) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {}
    for path in sorted(root.iterdir(), key=lambda item: item.name):
        if not path.is_file() or (exclude_manifest and path.name == "manifest.json"):
            continue
        if path.name == "manifest.json":
            continue
        values[path.name] = _descriptor(path)
    return values


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(canonical_json_bytes(value) + b"\n")


def _write_parquet(path: Path, frame: pd.DataFrame) -> None:
    frame.to_parquet(path, index=False, compression="zstd")


def _parse_cli(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Position timing frozen L2 learnability audit")
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--candidate-root", required=True)
    prepare.add_argument("--ranking-bundle-root", required=True)
    prepare.add_argument("--timing-artifact-root", required=True)
    prepare.add_argument("--historical-registry-path", required=True)
    prepare.add_argument("--repository-root", required=True)
    prepare.add_argument("--output-root")
    run = sub.add_parser("run")
    run.add_argument("--request", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--bundle", required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_cli(argv)
    try:
        if args.command == "prepare":
            result = prepare_l2_learnability_request(
                candidate_root=args.candidate_root,
                ranking_bundle_root=args.ranking_bundle_root,
                timing_artifact_root=args.timing_artifact_root,
                historical_registry_path=args.historical_registry_path,
                repository_root=args.repository_root,
                output_root=args.output_root,
            )
        elif args.command == "run":
            result = run_l2_learnability_audit(args.request)
        else:
            loaded = inspect_l2_learnability_bundle(args.bundle)
            result = {
                "status": "BUNDLE_VALID",
                "bundle_id": loaded["bundle_id"],
                "receipt_sha256": loaded["receipt"].receipt_sha256,
                "effect_evidence": loaded["receipt"].effect_evidence,
            }
    except PositionTimingL2Error as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "context": exc.context,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    except (ValidationError, ValueError) as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reason_code": "POSITION_TIMING_L2_CONTRACT_INVALID",
                    "message": "L2 contract or artifact validation failed",
                    "context": {"error_type": type(exc).__name__, "error": str(exc)},
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2
    print(json.dumps({"ok": True, **result}, ensure_ascii=False, sort_keys=True))
    return 0


def _validate_candidate_source(
    *, state: Mapping[str, Any], daily_meta: Mapping[str, Any], suspend_meta: Mapping[str, Any]
) -> None:
    components = state.get("components") or {}
    required_components = ("daily_bin", "suspend_d", "index_context")
    try:
        candidate_cutoff = date.fromisoformat(str(state.get("cutoff")))
        daily_start = date.fromisoformat(str(daily_meta.get("start")))
        daily_end = date.fromisoformat(str(daily_meta.get("end")))
    except ValueError:
        candidate_cutoff = date.min
        daily_start = date.max
        daily_end = date.min
    try:
        production_writes = int(state.get("production_writes", -1))
        production_pointer_changes = int(state.get("production_pointer_changes", -1))
    except (TypeError, ValueError):
        production_writes = production_pointer_changes = -1
    valid = (
        state.get("schema_version") == "qe_direct_monthly_state_v3"
        and state.get("status") == "CANDIDATE_READY"
        and candidate_cutoff >= POPULATION_END
        and all((components.get(name) or {}).get("status") == "PASS" for name in required_components)
        and production_writes == 0
        and production_pointer_changes == 0
        and daily_meta.get("universe_key") == "aistock_equity_pit_canonical_v2"
        and daily_meta.get("rule_version") == "shsz_a_252td_st_delist_asof_v2"
        and daily_meta.get("survivorship_bias") == "canonical_lifecycle_pit"
        and daily_meta.get("st_pit") is True
        and daily_meta.get("exclude_st") is True
        and daily_start <= POPULATION_START
        and daily_end >= POPULATION_END
        and suspend_meta.get("universe_key") == "aistock_equity_pit_canonical_v2"
    )
    if not valid:
        _raise(
            "L2 source is not the required canonical-v2 candidate",
            "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH",
        )


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _raise(
            "L2 JSON source cannot be read",
            "POSITION_TIMING_L2_SOURCE_UNAVAILABLE",
            path=path.as_posix(),
            error_type=type(exc).__name__,
        )
    if not isinstance(value, dict):
        _raise("L2 JSON source is not an object", "POSITION_TIMING_L2_SOURCE_IDENTITY_MISMATCH")
    return value


def _repository_commit(root: Path) -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _repository_dirty(root: Path) -> list[str]:
    result = subprocess.run(["git", "status", "--porcelain"], cwd=root, check=True, capture_output=True, text=True)
    return [line[3:] for line in result.stdout.splitlines() if line.strip()]


def _json_ready(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_ready(item) for item in value]
    return str(value)


def _write_immutable(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temp = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp, path)
        except FileExistsError:
            if path.read_bytes() != content:
                _raise(
                    "immutable L2 artifact conflicts with existing content",
                    "POSITION_TIMING_L2_IMMUTABLE_CONFLICT",
                    path=path.as_posix(),
                )
    finally:
        if temp.exists():
            temp.unlink()


def _raise(message: str, reason_code: str, **context: Any) -> None:
    raise PositionTimingL2Error(message, reason_code=reason_code, context=context)


__all__ = [
    "CrossfitResult",
    "FrozenL2LearnabilityRequestV1",
    "L2HypothesisResultV1",
    "L2IntervalV1",
    "L2LearnabilityReceiptV1",
    "PopulationBuildResult",
    "PositionTimingL2Error",
    "SOURCE_ROLES",
    "build_l2_cpcv_paths",
    "choose_supported_model",
    "circular_block_interval",
    "classify_effect",
    "frozen_model_runtime_identities",
    "inspect_l2_learnability_bundle",
    "map_monotone_exposure",
    "materialize_l2_population",
    "prepare_l2_learnability_request",
    "run_l2_crossfit",
    "run_l2_learnability_audit",
]


if __name__ == "__main__":  # pragma: no cover - exercised by formal CLI execution.
    raise SystemExit(main())
