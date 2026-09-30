"""PT-NEXT-017 same-stock one-session deferred-entry historical study.

This pipeline writes one immutable, timing-owned research bundle.  It does not
write a registry/current pointer, runtime model, card, alert, order, database,
API, scheduler, worker, or any artifact owned by another module.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import ActionValueError, TZ
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import DailyCandidate, file_reference
from .action_value_entry_only import _daily_replay_source_identity
from .action_value_entry_timing_model import (
    ALWAYS_OPEN_ENTRY_COMPARATOR,
    ENTRY_TIMING_ACTION_AUTHORITY,
    ENTRY_TIMING_ACTION_CONTRACT,
    ENTRY_TIMING_OBJECTIVE,
    ENTRY_TIMING_POLICY_SHA256,
    walk_forward_entry_timing_values,
)
from .action_value_heldout import (
    _freeze_source_snapshots,
    _joint_evidence,
    _parent_reference,
    _without_path,
    prior_request_identity,
    select_heldout_symbols,
)
from .action_value_open_only import (
    PRIOR_REQUEST_FOLDERS as PRE_OPEN_ONLY_REQUEST_FOLDERS,
    PRIOR_REQUEST_SCHEMAS as PRE_OPEN_ONLY_REQUEST_SCHEMAS,
)
from .action_value_pipeline import _clean_repository_commit
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    REFERENCE_CAPITAL_CNY,
    ActionValuePopulationSpec,
    build_deferred_entry_timing_rows,
    circular_block_interval,
    classify_effect,
    replay_continuous_cohorts,
)
from .action_value_state_matched_add import inspect_state_matched_add_bundle
from .action_value_suspensions import SuspensionSnapshotBook
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_DEFERRED_ENTRY_HELDOUT_V1"
ARTIFACT_FOLDER = "action_value_deferred_entry_v1"
REQUEST_SCHEMA = "position_timing_deferred_entry_request_v1"
RECEIPT_SCHEMA = "position_timing_deferred_entry_receipt_v1"
BUNDLE_SCHEMA = "position_timing_deferred_entry_bundle_v1"
RESULT_CLASS = "CROSS_SYMBOL_HELDOUT_DEFERRED_ENTRY_TIMING_CONFIRMATION"
PROVENANCE_REASON = "ENTRY_TARGET_REDEFINED_AS_SAME_STOCK_IMMEDIATE_VS_DEFERRED"
EVALUATION_SYMBOL_LIMIT = 64
INFERENCE_SEED = 20260911
ALWAYS_OPEN_COMPARATOR = ALWAYS_OPEN_ENTRY_COMPARATOR
PRIOR_REQUEST_FOLDERS = (
    *PRE_OPEN_ONLY_REQUEST_FOLDERS,
    "action_value_open_only_v1",
    "action_value_state_matched_add_v1",
)
PRIOR_REQUEST_SCHEMAS = {
    *PRE_OPEN_ONLY_REQUEST_SCHEMAS,
    "position_timing_open_only_heldout_request_v1",
    "position_timing_state_matched_add_request_v1",
}
LABEL_CONTRACT = {
    "schema_version": "position_timing_deferred_entry_label_contract_v1",
    "objective": ENTRY_TIMING_OBJECTIVE,
    "decision_clock": "T_20_00_ASIA_SHANGHAI",
    "state": "CASH",
    "candidate": "SAME_STOCK_SAME_POSITIVE_QUANTITY_OPEN_T_PLUS_1",
    "baseline": "WAIT_T_PLUS_1_THEN_SAME_QUANTITY_OPEN_T_PLUS_2",
    "immediate_fill_support": "FILLED_ONLY_NO_FILL_OR_UNKNOWN_EXCLUDED_TYPED",
    "deferred_fill_support": "FILLED_OR_NO_FILL_UNKNOWN_EXCLUDED_TYPED",
    "post_entry_policy": "FROZEN_RISK_EXIT_OR_HOLD_ONLY",
    "horizon_trading_days": 20,
    "terminal_max_defer_trading_days": 5,
    "cost": "SHARED_COMPONENTIZED_PARENT_ORDER_PER_LEG",
    "corporate_actions": "IMMUTABLE_IMPLEMENTED_DIVIDEND_QUANTITY_CASH_V1",
    "interpretation": "ONE_STEP_TIMING_VALUE_NOT_GLOBAL_OPTIMAL_STOPPING",
}
LABEL_CONTRACT_SHA256 = canonical_sha256(LABEL_CONTRACT)
STUDY_CONTRACT = {
    "schema_version": "position_timing_deferred_entry_study_contract_v1",
    "research_question": "DOES_ONE_SESSION_ENTRY_TIMING_ADD_NET_POLICY_VALUE",
    "candidate_policy": ENTRY_TIMING_ACTION_CONTRACT,
    "training_population": "PARENT_V4_SYMBOLS_NEW_DEFERRED_ENTRY_LABELS",
    "evaluation_population": {
        "selection": "SHA256_SEED_FIFTH_BATCH_AFTER_ALL_PRIOR_REQUEST_SYMBOLS",
        "symbol_limit": EVALUATION_SYMBOL_LIMIT,
        "prior_request_folders": PRIOR_REQUEST_FOLDERS,
        "outcomes_read_during_selection": False,
    },
    "comparators": (
        "BUY_AND_HOLD",
        "FROZEN_L1_V1",
        ALWAYS_OPEN_COMPARATOR,
    ),
    "planned_candidate_policy_count": 1,
    "planned_trial_count": 3,
    "familywise_hypothesis_count": 3,
    "economic_threshold_bps": 0.0,
    "inference": {
        "aggregation": "TRADING_DATE",
        "bootstrap": "CIRCULAR_MOVING_BLOCK",
        "block_sessions": 25,
        "samples": 5000,
        "seed": INFERENCE_SEED,
        "nominal_interval_level": 0.95,
        "simultaneous_interval_level": 1 - 0.05 / 3,
    },
    "alpha_evidence": "BOTH_BUY_AND_HOLD_AND_FROZEN_L1_SUPPORTED",
    "timing_component_evidence": "ALWAYS_OPEN_COMPARATOR_SUPPORTED",
    "joint_evidence": "ALL_THREE_COMPARISONS_SUPPORTED",
    "evidence_scope": "CROSS_SYMBOL_HELDOUT_SAME_MARKET_DATES_NOT_TEMPORAL_HOLDOUT",
    "result_follow_up": "NO_RESULT_CONDITIONED_NEW_HORIZON_FACTOR_THRESHOLD_POSITION_OR_SYMBOL_BATCH",
    "serving": "FORBIDDEN_IN_THIS_TASK",
}
STUDY_CONTRACT_SHA256 = canonical_sha256(STUDY_CONTRACT)


def _validate_parent(inspected: Mapping[str, Any]) -> None:
    request = inspected["request"]
    receipt = inspected["receipt"]
    if (
        request.get("schema_version") != "position_timing_state_matched_add_request_v1"
        or receipt.get("schema_version") != "position_timing_state_matched_add_receipt_v1"
        or receipt.get("selected_trial_count") not in {0, 1}
        or receipt.get("serving_status")
        != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
    ):
        raise ActionValueError("DEFERRED_ENTRY_PARENT_CONTRACT_MISMATCH")


def prepare_deferred_entry_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_state_matched_add_bundle: Path,
) -> Path:
    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    timing_root = timing_root.resolve()
    parent_bundle = parent_state_matched_add_bundle.resolve()
    parent = inspect_state_matched_add_bundle(parent_bundle)
    _validate_parent(parent)
    parent_request = parent["request"]
    prior = prior_request_identity(
        timing_root / "research",
        request_folders=PRIOR_REQUEST_FOLDERS,
        request_schemas=tuple(PRIOR_REQUEST_SCHEMAS),
    )
    candidate = DailyCandidate.open(Path(parent_request["candidate_root"]))
    population = parent_request["population_spec"]
    evaluation_symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=prior["forbidden_symbols"],
        seed=int(population["seed"]),
        limit=EVALUATION_SYMBOL_LIMIT,
    )
    training_symbols = tuple(parent_request["training_symbols"])
    if (
        not set(parent_request["evaluation_symbols"]).issubset(
            prior["forbidden_symbols"]
        )
        or set(training_symbols).intersection(evaluation_symbols)
    ):
        raise ActionValueError("DEFERRED_ENTRY_POPULATION_OVERLAP")
    start = date.fromisoformat(population["start"])
    end = date.fromisoformat(population["end"])
    snapshot_symbols = tuple(sorted(set(training_symbols).union(evaluation_symbols)))
    corporate_action_path, suspension_path = _freeze_source_snapshots(
        timing_root=timing_root,
        symbols=snapshot_symbols,
        start=start,
        end=end,
    )
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.as_posix(),
        "parent_state_matched_add": _parent_reference(parent_bundle, parent),
        "candidate_root": Path(parent_request["candidate_root"]).resolve().as_posix(),
        "training_symbols": training_symbols,
        "evaluation_symbols": evaluation_symbols,
        "snapshot_symbols": snapshot_symbols,
        "population_spec": {
            **population,
            "symbol_limit": EVALUATION_SYMBOL_LIMIT,
            "selected_symbols": evaluation_symbols,
            "selection": "SHA256_SEED_FIFTH_BATCH_AFTER_ALL_PRIOR_REQUEST_SYMBOLS",
            "forbidden_symbol_count": prior["forbidden_symbol_count"],
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "training_daily_source_identity": _daily_replay_source_identity(
            candidate.root, training_symbols
        ),
        "evaluation_daily_source_identity": _daily_replay_source_identity(
            candidate.root, evaluation_symbols
        ),
        "corporate_action_snapshot": file_reference(corporate_action_path),
        "suspension_snapshot": file_reference(suspension_path),
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "candidate_policy_sha256": ENTRY_TIMING_POLICY_SHA256,
        "label_contract": LABEL_CONTRACT,
        "label_contract_sha256": LABEL_CONTRACT_SHA256,
        "study_contract": STUDY_CONTRACT,
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "result_class": RESULT_CLASS,
        "registry_write": False,
        "current_write": False,
        "model_artifact_write": False,
        "card_write": False,
        "alert_write": False,
        "order_write": False,
        "database_write": False,
        "runtime_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = (
        timing_root
        / "research"
        / ARTIFACT_FOLDER
        / "requests"
        / f"{request['request_sha256']}.json"
    )
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("DEFERRED_ENTRY_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    false_flags = (
        "registry_write",
        "current_write",
        "model_artifact_write",
        "card_write",
        "alert_write",
        "order_write",
        "database_write",
        "runtime_write",
    )
    evaluation = tuple(request.get("evaluation_symbols") or ())
    training = tuple(request.get("training_symbols") or ())
    prior = request.get("prior_request_identity") or {}
    population = request.get("population_spec") or {}
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("label_contract_sha256") != LABEL_CONTRACT_SHA256
        or canonical_sha256(request.get("label_contract")) != LABEL_CONTRACT_SHA256
        or request.get("study_contract_sha256") != STUDY_CONTRACT_SHA256
        or canonical_sha256(request.get("study_contract")) != STUDY_CONTRACT_SHA256
        or request.get("candidate_policy_sha256") != ENTRY_TIMING_POLICY_SHA256
        or request.get("result_class") != RESULT_CLASS
        or len(evaluation) != EVALUATION_SYMBOL_LIMIT
        or len(evaluation) != len(set(evaluation))
        or not training
        or set(training).intersection(evaluation)
        or tuple(population.get("selected_symbols") or ()) != evaluation
        or prior.get("outcomes_read") is not False
        or tuple(prior.get("request_folders") or ()) != PRIOR_REQUEST_FOLDERS
        or set(prior.get("forbidden_symbols") or ()).intersection(evaluation)
        or population.get("prior_requests_sha256") != prior.get("aggregate_sha256")
        or prior.get("aggregate_sha256")
        != canonical_sha256(
            {key: value for key, value in prior.items() if key != "aggregate_sha256"}
        )
        or request.get("source_correction") != "EXPLICIT_DB_SUSPENSION_UNION_V1"
        or any(request.get(flag) is not False for flag in false_flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("DEFERRED_ENTRY_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_bound_parent(request: Mapping[str, Any]) -> dict[str, Any]:
    reference = request["parent_state_matched_add"]
    bundle = Path(reference["bundle_path"]).resolve()
    if file_reference(bundle / "manifest.json") != reference["manifest_file"]:
        raise ActionValueError("DEFERRED_ENTRY_PARENT_MANIFEST_CHANGED")
    parent = inspect_state_matched_add_bundle(bundle)
    _validate_parent(parent)
    if (
        parent["manifest"]["manifest_sha256"] != reference["manifest_sha256"]
        or parent["request"]["request_sha256"] != reference["request_sha256"]
        or parent["receipt"]["receipt_sha256"] != reference["receipt_sha256"]
        or tuple(parent["request"]["training_symbols"])
        != tuple(request["training_symbols"])
        or Path(parent["request"]["candidate_root"]).resolve()
        != Path(request["candidate_root"]).resolve()
    ):
        raise ActionValueError("DEFERRED_ENTRY_PARENT_IDENTITY_MISMATCH")
    return parent


def _candidate_minus_always_open_daily(
    candidate: pd.DataFrame, always_open: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = ("sleeve_id", "symbol", "initial_state", "valuation_date")
    identity = (
        *keys,
        "continuous_start",
        "decision_trade_date",
        "target_trade_date",
        "baseline_wealth_cny",
        "baseline_fractional_share_discarded",
        "corporate_action_applied",
        "corporate_action_source_rows_sha256",
    )
    left = candidate.loc[candidate["baseline"].eq("BUY_AND_HOLD")].sort_values(
        list(keys)
    ).reset_index(drop=True)
    right = always_open.loc[always_open["baseline"].eq("BUY_AND_HOLD")].sort_values(
        list(keys)
    ).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(
            left.loc[:, list(identity)],
            right.loc[:, list(identity)],
            check_dtype=False,
            check_exact=True,
        )
    except AssertionError as exc:
        raise ActionValueError("DEFERRED_ENTRY_COMPARATOR_PATH_IDENTITY_MISMATCH") from exc
    difference = (
        pd.to_numeric(left["incremental_net_value_cny"]).to_numpy(float)
        - pd.to_numeric(right["incremental_net_value_cny"]).to_numpy(float)
    )
    daily = pd.DataFrame(
        {"valuation_date": left["valuation_date"].to_numpy(), "difference_cny": difference}
    ).groupby("valuation_date", as_index=False).agg(
        incremental_net_value_cny=("difference_cny", "sum"),
        sleeve_count=("difference_cny", "size"),
    )
    daily["incremental_net_value_bps"] = (
        daily["incremental_net_value_cny"]
        / (daily["sleeve_count"] * float(REFERENCE_CAPITAL_CNY))
        * 10000.0
    )
    daily["baseline"] = ALWAYS_OPEN_COMPARATOR
    path = {
        "key_count": len(left),
        "sleeve_count": int(left["sleeve_id"].nunique()),
        "valuation_date_count": int(left["valuation_date"].nunique()),
        "identity_columns": identity,
        "exact_match": True,
    }
    path["identity_sha256"] = canonical_sha256(path)
    return daily, path


def _comparison(values: np.ndarray, *, seed: int) -> dict[str, Any]:
    nominal = circular_block_interval(
        values, block_sessions=25, samples=5000, seed=seed, alpha=0.05
    )
    adjusted = circular_block_interval(
        values, block_sessions=25, samples=5000, seed=seed, alpha=0.05 / 3
    )
    return {
        "daily_mean_incremental_bps": float(values.mean()),
        "period_cumulative_incremental_bps": float(values.sum()),
        "nominal_interval_level": 0.95,
        "nominal_interval_bps": nominal,
        "simultaneous_interval_level": 1 - 0.05 / 3,
        "adjusted_interval_bps": adjusted,
        "mde_bps": max(
            adjusted["point_bps"] - adjusted["lower_bps"],
            adjusted["upper_bps"] - adjusted["point_bps"],
        ),
        "power_status": "NOT_COMPUTABLE",
        "power_reason_code": "NO_PREREGISTERED_SAME_ESTIMAND_EFFECT_SCALE",
        "effect_evidence": classify_effect(
            adjusted["lower_bps"], adjusted["upper_bps"]
        ),
        "effective_trading_days": len(values),
    }


def _manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "request.json",
        "label_coverage.json",
        "training_rows.parquet",
        "oof_predictions.parquet",
        "candidate_sleeve_days.parquet",
        "always_open_sleeve_days.parquet",
        "daily_comparisons.parquet",
        "receipt.json",
    )
    manifest = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": {name: _without_path(file_reference(root / name)) for name in names},
    }
    manifest["manifest_sha256"] = canonical_sha256(manifest)
    return manifest


def _publish_bundle(
    bundle: Path,
    *,
    request: Mapping[str, Any],
    coverage: Mapping[str, Any],
    rows: pd.DataFrame,
    oof: pd.DataFrame,
    candidate_sleeves: pd.DataFrame,
    always_open_sleeves: pd.DataFrame,
    daily: pd.DataFrame,
    receipt: Mapping[str, Any],
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(
            staging / "request.json", canonical_json_bytes(request)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "label_coverage.json", canonical_json_bytes(coverage)
        )
        rows.to_parquet(staging / "training_rows.parquet", index=False)
        oof.to_parquet(staging / "oof_predictions.parquet", index=False)
        candidate_sleeves.to_parquet(
            staging / "candidate_sleeve_days.parquet", index=False
        )
        always_open_sleeves.to_parquet(
            staging / "always_open_sleeve_days.parquet", index=False
        )
        daily.to_parquet(staging / "daily_comparisons.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"deferred-entry-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_deferred_entry_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_deferred_entry_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("DEFERRED_ENTRY_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    comparisons = receipt.get("comparisons") or {}
    false_flags = (
        "registry_written",
        "current_written",
        "model_artifact_written",
        "card_written",
        "alert_written",
        "order_written",
        "database_written",
        "runtime_written",
    )
    if (
        manifest.get("schema_version") != BUNDLE_SCHEMA
        or manifest.get("manifest_sha256") != canonical_sha256(manifest_identity)
        or receipt.get("schema_version") != RECEIPT_SCHEMA
        or receipt.get("pipeline_id") != PIPELINE_ID
        or receipt.get("request_sha256") != request.get("request_sha256")
        or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity)
        or manifest.get("request_sha256") != request.get("request_sha256")
        or manifest.get("receipt_sha256") != receipt.get("receipt_sha256")
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("trial_count") != 3
        or receipt.get("planned_candidate_policy_count") != 1
        or receipt.get("familywise_hypothesis_count") != 3
        or set(comparisons)
        != {"BUY_AND_HOLD", "FROZEN_L1_V1", ALWAYS_OPEN_COMPARATOR}
        or receipt.get("candidate_policy_sha256") != ENTRY_TIMING_POLICY_SHA256
        or receipt.get("serving_status")
        != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
        or any(receipt.get(flag) is not False for flag in false_flags)
    ):
        raise ActionValueError("DEFERRED_ENTRY_BUNDLE_IDENTITY_MISMATCH")
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError("DEFERRED_ENTRY_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_deferred_entry_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("DEFERRED_ENTRY_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {"status": "ALREADY_MATERIALIZED", "bundle": bundle.as_posix(), **inspect_deferred_entry_bundle(bundle)}
    observed_prior = prior_request_identity(
        timing_root / "research",
        request_folders=PRIOR_REQUEST_FOLDERS,
        request_schemas=tuple(PRIOR_REQUEST_SCHEMAS),
    )
    if canonical_sha256(observed_prior) != canonical_sha256(request["prior_request_identity"]):
        raise ActionValueError("DEFERRED_ENTRY_PRIOR_REQUEST_SET_CHANGED")
    _validate_bound_parent(request)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    training_symbols = tuple(request["training_symbols"])
    evaluation_symbols = tuple(request["evaluation_symbols"])
    expected = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=request["prior_request_identity"]["forbidden_symbols"],
        seed=int(request["population_spec"]["seed"]),
        limit=EVALUATION_SYMBOL_LIMIT,
    )
    if expected != evaluation_symbols:
        raise ActionValueError("DEFERRED_ENTRY_POPULATION_IDENTITY_MISMATCH")
    for field, symbols in (
        ("training_daily_source_identity", training_symbols),
        ("evaluation_daily_source_identity", evaluation_symbols),
    ):
        if _daily_replay_source_identity(candidate.root, symbols) != request[field]:
            raise ActionValueError("DEFERRED_ENTRY_DAILY_SOURCE_CHANGED", field=field)
    for field in ("corporate_action_snapshot", "suspension_snapshot"):
        if file_reference(Path(request[field]["path"])) != request[field]:
            raise ActionValueError("DEFERRED_ENTRY_SNAPSHOT_CHANGED", field=field)
    corporate_actions = CorporateActionBook.open(
        Path(request["corporate_action_snapshot"]["path"])
    )
    suspensions = SuspensionSnapshotBook.open(Path(request["suspension_snapshot"]["path"]))
    expected_scope = (
        tuple(request["snapshot_symbols"]),
        date.fromisoformat(request["population_spec"]["start"]),
        date.fromisoformat(request["population_spec"]["end"]),
    )
    if (suspensions.symbols, suspensions.start, suspensions.end) != expected_scope:
        raise ActionValueError("DEFERRED_ENTRY_SUSPENSION_SCOPE_MISMATCH")
    candidate = suspensions.apply(
        candidate, snapshot_path=Path(request["suspension_snapshot"]["path"])
    )
    population_spec = request["population_spec"]
    labels = build_deferred_entry_timing_rows(
        candidate,
        ActionValuePopulationSpec(
            start=date.fromisoformat(population_spec["start"]),
            end=date.fromisoformat(population_spec["end"]),
            symbol_limit=len(training_symbols),
            review_stride=int(population_spec["review_stride"]),
            seed=int(population_spec["seed"]),
        ),
        symbols=training_symbols,
        corporate_actions=corporate_actions,
    )
    forward = walk_forward_entry_timing_values(
        labels.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=labels.coverage["coverage_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    replay_args = {
        "models": (),
        "entry_timing_models": forward.models,
        "symbols": evaluation_symbols,
        "corporate_actions": corporate_actions,
        "bootstrap_samples": 5000,
        "block_sessions": 25,
        "seed": INFERENCE_SEED,
        "initial_holding_policy_id": EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
        "model_action_authority": ENTRY_TIMING_ACTION_AUTHORITY,
    }
    policy = replay_continuous_cohorts(candidate, **replay_args)
    always_open = replay_continuous_cohorts(
        candidate, **replay_args, entry_timing_force_open=True
    )
    component_daily, path_identity = _candidate_minus_always_open_daily(
        policy.sleeve_days, always_open.sleeve_days
    )
    daily = pd.concat([policy.daily_comparisons, component_daily], ignore_index=True)
    comparisons = {}
    for offset, baseline in enumerate(
        ("BUY_AND_HOLD", "FROZEN_L1_V1", ALWAYS_OPEN_COMPARATOR)
    ):
        values = daily.loc[
            daily["baseline"].eq(baseline), "incremental_net_value_bps"
        ].to_numpy(float)
        comparisons[baseline] = _comparison(values, seed=INFERENCE_SEED + offset)
    unresolved = (
        not policy.receipt["coverage_can_support_policy"]
        or not always_open.receipt["coverage_can_support_policy"]
    )
    if unresolved:
        for comparison in comparisons.values():
            comparison["effect_evidence_before_coverage_constraint"] = comparison["effect_evidence"]
            comparison["effect_evidence"] = "INCONCLUSIVE"
            comparison["coverage_reason_code"] = "SOURCE_OR_CORPORATE_ACTION_PATH_UNAVAILABLE"
    alpha = _joint_evidence(
        {key: comparisons[key] for key in ("BUY_AND_HOLD", "FROZEN_L1_V1")}
    )
    timing = comparisons[ALWAYS_OPEN_COMPARATOR]["effect_evidence"]
    joint = _joint_evidence(comparisons)
    target = pd.to_numeric(labels.rows["net_entry_timing_value_bps"]).to_numpy(float)
    predicted = pd.to_numeric(
        forward.predictions["predicted_entry_timing_value_bps"]
    ).to_numpy(float)
    oof_target = pd.to_numeric(
        forward.predictions["net_entry_timing_value_bps"]
    ).to_numpy(float)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_state_matched_add_request_sha256": request["parent_state_matched_add"]["request_sha256"],
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "label_contract_sha256": LABEL_CONTRACT_SHA256,
        "candidate_policy_sha256": ENTRY_TIMING_POLICY_SHA256,
        "result_class": RESULT_CLASS,
        "provenance_reason": PROVENANCE_REASON,
        "trial_count": 3,
        "planned_candidate_policy_count": 1,
        "familywise_hypothesis_count": 3,
        "selected_trial_count": 1 if joint == "SUPPORTED" else 0,
        "alpha_effect_evidence": alpha,
        "timing_component_effect_evidence": timing,
        "joint_effect_evidence": joint,
        "comparisons": comparisons,
        "heldout_population": {
            "training_symbol_count": len(training_symbols),
            "evaluation_symbol_count": len(evaluation_symbols),
            "prior_forbidden_symbol_count": request["prior_request_identity"]["forbidden_symbol_count"],
            "training_evaluation_overlap": len(set(training_symbols).intersection(evaluation_symbols)),
            "prior_evaluation_overlap": len(set(request["prior_request_identity"]["forbidden_symbols"]).intersection(evaluation_symbols)),
            "outcomes_read_during_selection": False,
            "same_market_dates_not_temporal_holdout": True,
        },
        "label_population_coverage_sha256": labels.coverage["coverage_sha256"],
        "label_training_row_count": len(labels.rows),
        "label_target_diagnostics": {
            "mean_bps": float(target.mean()),
            "median_bps": float(np.median(target)),
            "std_bps": float(target.std(ddof=1)),
            "positive_share": float((target > 0).mean()),
        },
        "oof_diagnostics": {
            "prediction_row_count": len(forward.predictions),
            "pearson_target_correlation": float(np.corrcoef(predicted, oof_target)[0, 1]),
            "spearman_target_correlation": float(pd.Series(predicted).corr(pd.Series(oof_target), method="spearman")),
        },
        "model_count": len(forward.models),
        "model_hashes": tuple(model.metadata["model_sha256"] for model in forward.models),
        "always_open_path_identity": path_identity,
        "candidate_policy": policy.receipt,
        "always_open_comparator": always_open.receipt,
        "result_follow_up": "NO_AUTOMATIC_FOLLOW_UP_OR_SERVING",
        "serving_status": "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED",
        "registry_written": False,
        "current_written": False,
        "model_artifact_written": False,
        "card_written": False,
        "alert_written": False,
        "order_written": False,
        "database_written": False,
        "runtime_written": False,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    _publish_bundle(
        bundle,
        request=request,
        coverage=labels.coverage,
        rows=labels.rows,
        oof=forward.predictions,
        candidate_sleeves=policy.sleeve_days,
        always_open_sleeves=always_open.sleeve_days,
        daily=daily,
        receipt=receipt,
    )
    return {"status": "MATERIALIZED", "bundle": bundle.as_posix(), **inspect_deferred_entry_bundle(bundle)}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-state-matched-add-bundle", required=True, type=Path)
    prepare.add_argument("--env-file", type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            from dotenv import load_dotenv

            load_dotenv(args.env_file or args.repository_root.resolve() / ".env", override=False)
            result = {
                "status": "PREPARED",
                "request": prepare_deferred_entry_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    parent_state_matched_add_bundle=args.parent_state_matched_add_bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_deferred_entry_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_deferred_entry_bundle(args.bundle)}
    except ActionValueError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "details": exc.details}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ARTIFACT_FOLDER",
    "BUNDLE_SCHEMA",
    "LABEL_CONTRACT",
    "LABEL_CONTRACT_SHA256",
    "PIPELINE_ID",
    "RECEIPT_SCHEMA",
    "REQUEST_SCHEMA",
    "STUDY_CONTRACT",
    "STUDY_CONTRACT_SHA256",
    "inspect_deferred_entry_bundle",
    "prepare_deferred_entry_request",
    "run_deferred_entry_request",
]
