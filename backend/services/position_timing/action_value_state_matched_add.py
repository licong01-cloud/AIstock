"""PT-NEXT-016 state-matched ADD action-value historical replay.

The study derives ADD-vs-HOLD labels from causal OPEN-only policy states,
trains a separate single-head ADD model, and evaluates one composite policy on
symbols absent from every earlier action-value request.  Only an immutable
timing-owned research bundle is written; serving and runtime state are outside
this task.
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

from .action_value import CORE_INFORMATION_BLOCK, ActionValueError, TZ
from .action_value_add_model import (
    ADD_OBJECTIVE,
    walk_forward_add_action_values,
)
from .action_value_advice import (
    OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
    STATE_MATCHED_ADD_MODEL_ACTION_CONTRACT,
    action_authority_policy_sha256,
)
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import DailyCandidate, file_reference
from .action_value_entry_only import (
    _assert_oof_equivalence,
    _daily_replay_source_identity,
)
from .action_value_heldout import (
    _freeze_source_snapshots,
    _joint_evidence,
    _parent_reference,
    _validate_bound_parents,
    _without_path,
    prior_request_identity,
    select_heldout_symbols,
)
from .action_value_open_only import (
    PRIOR_REQUEST_FOLDERS as PREVIOUS_PRIOR_REQUEST_FOLDERS,
    PRIOR_REQUEST_SCHEMAS as PREVIOUS_PRIOR_REQUEST_SCHEMAS,
    inspect_open_only_bundle,
)
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    REFERENCE_CAPITAL_CNY,
    ActionValuePopulationSpec,
    build_state_matched_add_rows,
    circular_block_interval,
    classify_effect,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .action_value_suspensions import SuspensionSnapshotBook
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_STATE_MATCHED_ADD_HELDOUT_V1"
ARTIFACT_FOLDER = "action_value_state_matched_add_v1"
REQUEST_SCHEMA = "position_timing_state_matched_add_request_v1"
RECEIPT_SCHEMA = "position_timing_state_matched_add_receipt_v1"
BUNDLE_SCHEMA = "position_timing_state_matched_add_bundle_v1"
RESULT_CLASS = "CROSS_SYMBOL_HELDOUT_STATE_MATCHED_ADD_CONFIRMATION"
PROVENANCE_REASON = "ADD_SUPERVISION_MATCHED_TO_CAUSAL_HELD_POSITION_STATES"
EVALUATION_SYMBOL_LIMIT = 64
INFERENCE_SEED = 20260911
PRIOR_REQUEST_FOLDERS = (
    *PREVIOUS_PRIOR_REQUEST_FOLDERS,
    "action_value_open_only_v1",
)
PRIOR_REQUEST_SCHEMAS = {
    *PREVIOUS_PRIOR_REQUEST_SCHEMAS,
    "position_timing_open_only_heldout_request_v1",
}
ADD_LABEL_CONTRACT = {
    "schema_version": "position_timing_state_matched_add_label_contract_v1",
    "objective": ADD_OBJECTIVE,
    "state_path_policy": OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    "state_selection": (
        "HELD_STATE_AND_NO_FROZEN_RISK_EXIT_AND_AT_LEAST_ONE_LEGAL_POSITIVE_ADD"
    ),
    "state_selection_outcomes_read": False,
    "baseline_action": "HOLD_FROM_EXACT_SAME_STATE",
    "horizon_trading_days": 20,
    "terminal_max_defer_trading_days": 5,
    "cost": "SHARED_COMPONENTIZED_PARENT_ORDER_PER_LEG",
    "corporate_actions": "IMMUTABLE_IMPLEMENTED_DIVIDEND_QUANTITY_CASH_V1",
    "model": "SEPARATE_SINGLE_HEAD_GBDT_SAME_FROZEN_ESTIMATOR_SPEC",
}
ADD_LABEL_CONTRACT_SHA256 = canonical_sha256(ADD_LABEL_CONTRACT)
STUDY_CONTRACT = {
    "schema_version": "position_timing_state_matched_add_study_contract_v1",
    "research_question": "DOES_STATE_MATCHED_ADD_SUPERVISION_ADD_POLICY_VALUE",
    "candidate_policy": STATE_MATCHED_ADD_MODEL_ACTION_CONTRACT,
    "entry_training_population": "IMMUTABLE_PARENT_V4_ONLY",
    "add_training_population": "PARENT_SYMBOL_CAUSAL_OPEN_ONLY_PATH_STATES",
    "add_label_contract_sha256": ADD_LABEL_CONTRACT_SHA256,
    "evaluation_population": {
        "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
        "symbol_limit": EVALUATION_SYMBOL_LIMIT,
        "prior_request_folders": PRIOR_REQUEST_FOLDERS,
        "outcomes_read_during_selection": False,
    },
    "comparators": (
        "BUY_AND_HOLD",
        "FROZEN_L1_V1",
        OPEN_ONLY_MODEL_ACTION_AUTHORITY,
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
    "evidence_scope": "CROSS_SYMBOL_HELDOUT_SAME_MARKET_DATES_NOT_TEMPORAL_HOLDOUT",
    "serving": "FORBIDDEN_IN_THIS_TASK",
}
STUDY_CONTRACT_SHA256 = canonical_sha256(STUDY_CONTRACT)


def _validate_parent_open_only(inspected: Mapping[str, Any]) -> None:
    receipt = inspected["receipt"]
    if (
        receipt.get("joint_effect_evidence") != "INCONCLUSIVE"
        or receipt.get("selected_trial_count") != 0
        or receipt.get("serving_status")
        != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
    ):
        raise ActionValueError("STATE_MATCHED_ADD_PARENT_OPEN_ONLY_CONTRACT_MISMATCH")


def prepare_state_matched_add_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_open_only_bundle: Path,
) -> Path:
    from .action_value_pipeline import _clean_repository_commit

    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    timing_root = timing_root.resolve()
    parent_open_only_bundle = parent_open_only_bundle.resolve()
    parent = inspect_open_only_bundle(parent_open_only_bundle)
    _validate_parent_open_only(parent)
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
    )
    training_symbols = tuple(parent_request["training_symbols"])
    if (
        not set(parent_request["evaluation_symbols"]).issubset(
            prior["forbidden_symbols"]
        )
        or set(training_symbols).intersection(evaluation_symbols)
    ):
        raise ActionValueError("STATE_MATCHED_ADD_POPULATION_OVERLAP")
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
        "parent_open_only": _parent_reference(parent_open_only_bundle, parent),
        "parent_entry_only": parent_request["parent_entry_only"],
        "parent_v4": parent_request["parent_v4"],
        "candidate_root": Path(parent_request["candidate_root"]).resolve().as_posix(),
        "training_symbols": training_symbols,
        "evaluation_symbols": evaluation_symbols,
        "snapshot_symbols": snapshot_symbols,
        "population_spec": {
            **population,
            "symbol_limit": EVALUATION_SYMBOL_LIMIT,
            "selected_symbols": evaluation_symbols,
            "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": prior["forbidden_symbol_count"],
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "training_daily_source_identity": _daily_replay_source_identity(
            Path(parent_request["candidate_root"]), training_symbols
        ),
        "evaluation_daily_source_identity": _daily_replay_source_identity(
            Path(parent_request["candidate_root"]), evaluation_symbols
        ),
        "corporate_action_snapshot": file_reference(corporate_action_path),
        "suspension_snapshot": file_reference(suspension_path),
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "parent_source_sha256": parent_request["parent_source_sha256"],
        "parent_feature_spec_sha256": parent_request["parent_feature_spec_sha256"],
        "parent_policy_sha256": parent_request["parent_policy_sha256"],
        "candidate_policy_sha256": action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY
        ),
        "add_label_contract": ADD_LABEL_CONTRACT,
        "add_label_contract_sha256": ADD_LABEL_CONTRACT_SHA256,
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
    PositionTimingArtifactStore._publish_immutable(
        path, canonical_json_bytes(request)
    )
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("STATE_MATCHED_ADD_REQUEST_UNAVAILABLE") from exc
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
    snapshots = tuple(request.get("snapshot_symbols") or ())
    population = request.get("population_spec") or {}
    prior = request.get("prior_request_identity") or {}
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("study_contract_sha256") != STUDY_CONTRACT_SHA256
        or canonical_sha256(request.get("study_contract")) != STUDY_CONTRACT_SHA256
        or request.get("add_label_contract_sha256") != ADD_LABEL_CONTRACT_SHA256
        or canonical_sha256(request.get("add_label_contract"))
        != ADD_LABEL_CONTRACT_SHA256
        or request.get("result_class") != RESULT_CLASS
        or request.get("candidate_policy_sha256")
        != action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY
        )
        or len(evaluation) != EVALUATION_SYMBOL_LIMIT
        or len(set(evaluation)) != len(evaluation)
        or not training
        or set(training).intersection(evaluation)
        or snapshots != tuple(sorted(set(training).union(evaluation)))
        or tuple(population.get("selected_symbols") or ()) != evaluation
        or population.get("selection")
        != "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY"
        or prior.get("outcomes_read") is not False
        or tuple(prior.get("request_folders") or ()) != PRIOR_REQUEST_FOLDERS
        or set(prior.get("forbidden_symbols") or ()).intersection(evaluation)
        or population.get("forbidden_symbol_count")
        != prior.get("forbidden_symbol_count")
        or population.get("prior_requests_sha256") != prior.get("aggregate_sha256")
        or prior.get("aggregate_sha256")
        != canonical_sha256(
            {key: value for key, value in prior.items() if key != "aggregate_sha256"}
        )
        or not all(
            (request.get("parent_open_only") or {}).get(field)
            for field in (
                "bundle_path",
                "manifest_file",
                "manifest_sha256",
                "request_sha256",
                "receipt_sha256",
            )
        )
        or not all(
            (request.get("parent_v4") or {}).get(field)
            for field in (
                "bundle_path",
                "manifest_file",
                "manifest_sha256",
                "request_sha256",
                "receipt_sha256",
                "training_rows_file",
                "oof_predictions_file",
            )
        )
        or any(
            not (request.get(field) or {}).get("aggregate_sha256")
            for field in (
                "training_daily_source_identity",
                "evaluation_daily_source_identity",
            )
        )
        or not (request.get("corporate_action_snapshot") or {}).get("sha256")
        or not (request.get("suspension_snapshot") or {}).get("sha256")
        or request.get("source_correction") != "EXPLICIT_DB_SUSPENSION_UNION_V1"
        or any(request.get(flag) is not False for flag in false_flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("STATE_MATCHED_ADD_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_parents(request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    parent_ref = request["parent_open_only"]
    parent_bundle = Path(parent_ref["bundle_path"]).resolve()
    if file_reference(parent_bundle / "manifest.json") != parent_ref["manifest_file"]:
        raise ActionValueError("STATE_MATCHED_ADD_PARENT_MANIFEST_CHANGED")
    parent = inspect_open_only_bundle(parent_bundle)
    _validate_parent_open_only(parent)
    if (
        parent["manifest"]["manifest_sha256"] != parent_ref["manifest_sha256"]
        or parent["request"]["request_sha256"] != parent_ref["request_sha256"]
        or parent["receipt"]["receipt_sha256"] != parent_ref["receipt_sha256"]
        or tuple(parent["request"]["training_symbols"])
        != tuple(request["training_symbols"])
        or Path(parent["request"]["candidate_root"]).resolve()
        != Path(request["candidate_root"]).resolve()
    ):
        raise ActionValueError("STATE_MATCHED_ADD_PARENT_IDENTITY_MISMATCH")
    return _validate_bound_parents(request)


def _candidate_minus_open_only_daily(
    candidate: pd.DataFrame, open_only: pd.DataFrame
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
    left = (
        candidate.loc[candidate["baseline"].eq("BUY_AND_HOLD")]
        .sort_values(list(keys))
        .reset_index(drop=True)
    )
    right = (
        open_only.loc[open_only["baseline"].eq("BUY_AND_HOLD")]
        .sort_values(list(keys))
        .reset_index(drop=True)
    )
    try:
        pd.testing.assert_frame_equal(
            left.loc[:, list(identity)],
            right.loc[:, list(identity)],
            check_dtype=False,
            check_exact=True,
        )
    except AssertionError as exc:
        raise ActionValueError(
            "STATE_MATCHED_ADD_OPEN_ONLY_PATH_IDENTITY_MISMATCH"
        ) from exc
    increments = (
        pd.to_numeric(left["incremental_net_value_cny"]).to_numpy(float)
        - pd.to_numeric(right["incremental_net_value_cny"]).to_numpy(float)
    )
    daily = pd.DataFrame(
        {
            "valuation_date": left["valuation_date"].to_numpy(),
            "incremental_net_value_cny": increments,
        }
    ).groupby("valuation_date", as_index=False).agg(
        incremental_net_value_cny=("incremental_net_value_cny", "sum"),
        sleeve_count=("incremental_net_value_cny", "size"),
    )
    daily["incremental_net_value_bps"] = (
        daily["incremental_net_value_cny"]
        / (daily["sleeve_count"] * float(REFERENCE_CAPITAL_CNY))
        * 10000.0
    )
    daily["baseline"] = OPEN_ONLY_MODEL_ACTION_AUTHORITY
    path_identity = {
        "key_count": len(left),
        "sleeve_count": int(left["sleeve_id"].nunique()),
        "valuation_date_count": int(left["valuation_date"].nunique()),
        "identity_columns": identity,
        "exact_match": True,
    }
    path_identity["identity_sha256"] = canonical_sha256(path_identity)
    return daily, path_identity


def _comparison(values: np.ndarray, *, seed: int) -> dict[str, Any]:
    nominal = circular_block_interval(
        values, block_sessions=25, samples=5000, seed=seed, alpha=0.05
    )
    adjusted = circular_block_interval(
        values,
        block_sessions=25,
        samples=5000,
        seed=seed,
        alpha=0.05 / 3,
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
        "add_population_coverage.json",
        "add_training_rows.parquet",
        "add_oof_predictions.parquet",
        "composite_sleeve_days.parquet",
        "open_only_sleeve_days.parquet",
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
    composite_sleeves: pd.DataFrame,
    open_only_sleeves: pd.DataFrame,
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
            staging / "add_population_coverage.json", canonical_json_bytes(coverage)
        )
        rows.to_parquet(staging / "add_training_rows.parquet", index=False)
        oof.to_parquet(staging / "add_oof_predictions.parquet", index=False)
        composite_sleeves.to_parquet(
            staging / "composite_sleeve_days.parquet", index=False
        )
        open_only_sleeves.to_parquet(
            staging / "open_only_sleeve_days.parquet", index=False
        )
        daily.to_parquet(staging / "daily_comparisons.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"state-matched-add-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_state_matched_add_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_state_matched_add_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
        coverage = json.loads(
            (bundle / "add_population_coverage.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise ActionValueError("STATE_MATCHED_ADD_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {
        key: value for key, value in manifest.items() if key != "manifest_sha256"
    }
    receipt_identity = {
        key: value for key, value in receipt.items() if key != "receipt_sha256"
    }
    coverage_identity = {
        key: value for key, value in coverage.items() if key != "coverage_sha256"
    }
    comparisons = receipt.get("comparisons") or {}
    expected_comparators = {
        "BUY_AND_HOLD",
        "FROZEN_L1_V1",
        OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    }
    joint = _joint_evidence(comparisons)
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
        or coverage.get("coverage_sha256") != canonical_sha256(coverage_identity)
        or coverage.get("objective") != ADD_OBJECTIVE
        or coverage.get("state_selection_outcomes_read") is not False
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("trial_count") != 3
        or receipt.get("familywise_hypothesis_count") != 3
        or receipt.get("planned_candidate_policy_count") != 1
        or set(comparisons) != expected_comparators
        or receipt.get("joint_effect_evidence") != joint
        or receipt.get("selected_trial_count") != (1 if joint == "SUPPORTED" else 0)
        or receipt.get("candidate_policy_sha256")
        != request.get("candidate_policy_sha256")
        or receipt.get("serving_status")
        != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
        or any(receipt.get(flag) is not False for flag in false_flags)
    ):
        raise ActionValueError("STATE_MATCHED_ADD_BUNDLE_IDENTITY_MISMATCH")
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError(
                "STATE_MATCHED_ADD_BUNDLE_FILE_IDENTITY_MISMATCH", file=name
            )
    return {
        "manifest": manifest,
        "request": request,
        "coverage": coverage,
        "receipt": receipt,
    }


def run_state_matched_add_request(request_path: Path) -> dict[str, Any]:
    from .action_value_pipeline import _clean_repository_commit

    request = _load_request(request_path)
    repository_root = Path(request["repository_root"])
    if _clean_repository_commit(repository_root) != request["repository_commit"]:
        raise ActionValueError("STATE_MATCHED_ADD_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = (
        timing_root
        / "research"
        / ARTIFACT_FOLDER
        / "bundles"
        / request["request_sha256"]
    )
    if bundle.exists():
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspect_state_matched_add_bundle(bundle),
        }
    observed_prior = prior_request_identity(
        timing_root / "research",
        request_folders=PRIOR_REQUEST_FOLDERS,
        request_schemas=tuple(PRIOR_REQUEST_SCHEMAS),
    )
    if canonical_sha256(observed_prior) != canonical_sha256(
        request["prior_request_identity"]
    ):
        raise ActionValueError("STATE_MATCHED_ADD_PRIOR_REQUEST_SET_CHANGED")
    v4_bundle, _ = _validate_parents(request)

    training_symbols = tuple(request["training_symbols"])
    evaluation_symbols = tuple(request["evaluation_symbols"])
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    expected_symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=request["prior_request_identity"]["forbidden_symbols"],
        seed=int(request["population_spec"]["seed"]),
    )
    if expected_symbols != evaluation_symbols:
        raise ActionValueError("STATE_MATCHED_ADD_POPULATION_IDENTITY_MISMATCH")
    for field, symbols in (
        ("training_daily_source_identity", training_symbols),
        ("evaluation_daily_source_identity", evaluation_symbols),
    ):
        if _daily_replay_source_identity(
            candidate.root, symbols
        ) != request[field]:
            raise ActionValueError("STATE_MATCHED_ADD_DAILY_SOURCE_CHANGED", field=field)
    if file_reference(Path(request["corporate_action_snapshot"]["path"])) != request[
        "corporate_action_snapshot"
    ]:
        raise ActionValueError("STATE_MATCHED_ADD_CORPORATE_ACTION_SOURCE_CHANGED")
    if file_reference(Path(request["suspension_snapshot"]["path"])) != request[
        "suspension_snapshot"
    ]:
        raise ActionValueError("STATE_MATCHED_ADD_SUSPENSION_SOURCE_CHANGED")
    corporate_actions = CorporateActionBook.open(
        Path(request["corporate_action_snapshot"]["path"])
    )
    suspensions = SuspensionSnapshotBook.open(
        Path(request["suspension_snapshot"]["path"])
    )
    expected_scope = (
        tuple(request["snapshot_symbols"]),
        date.fromisoformat(request["population_spec"]["start"]),
        date.fromisoformat(request["population_spec"]["end"]),
    )
    if (suspensions.symbols, suspensions.start, suspensions.end) != expected_scope:
        raise ActionValueError("STATE_MATCHED_ADD_SUSPENSION_SCOPE_MISMATCH")
    candidate = suspensions.apply(
        candidate, snapshot_path=Path(request["suspension_snapshot"]["path"])
    )

    parent_rows = pd.read_parquet(v4_bundle / "training_rows.parquet")
    if set(parent_rows["symbol"].astype(str)) != set(training_symbols):
        raise ActionValueError("STATE_MATCHED_ADD_TRAINING_POPULATION_MISMATCH")
    entry_forward = walk_forward_action_values(
        parent_rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=request["parent_source_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    oof_identity = _assert_oof_equivalence(
        entry_forward.predictions,
        pd.read_parquet(v4_bundle / "oof_action_predictions.parquet"),
    )
    population_spec = request["population_spec"]
    add_population = build_state_matched_add_rows(
        candidate,
        ActionValuePopulationSpec(
            start=date.fromisoformat(population_spec["start"]),
            end=date.fromisoformat(population_spec["end"]),
            symbol_limit=len(training_symbols),
            review_stride=int(population_spec["review_stride"]),
            seed=int(population_spec["seed"]),
        ),
        entry_models=entry_forward.models,
        symbols=training_symbols,
        corporate_actions=corporate_actions,
    )
    add_forward = walk_forward_add_action_values(
        add_population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=add_population.coverage["coverage_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    common_start = max(
        datetime.fromisoformat(entry_forward.models[0].metadata["available_at"]).date(),
        datetime.fromisoformat(add_forward.models[0].metadata["available_at"]).date(),
    )
    common_end = min(
        datetime.fromisoformat(entry_forward.models[-1].metadata["available_at"]).date(),
        datetime.fromisoformat(add_forward.models[-1].metadata["available_at"]).date(),
    )
    composite = replay_continuous_cohorts(
        candidate,
        models=entry_forward.models,
        add_models=add_forward.models,
        symbols=evaluation_symbols,
        corporate_actions=corporate_actions,
        bootstrap_samples=5000,
        block_sessions=25,
        seed=INFERENCE_SEED,
        initial_holding_policy_id=EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
        model_action_authority=STATE_MATCHED_ADD_MODEL_ACTION_AUTHORITY,
        evaluation_start=common_start,
        evaluation_end=common_end,
    )
    open_only = replay_continuous_cohorts(
        candidate,
        models=entry_forward.models,
        symbols=evaluation_symbols,
        corporate_actions=corporate_actions,
        bootstrap_samples=5000,
        block_sessions=25,
        seed=INFERENCE_SEED,
        initial_holding_policy_id=EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
        model_action_authority=OPEN_ONLY_MODEL_ACTION_AUTHORITY,
        evaluation_start=common_start,
        evaluation_end=common_end,
    )
    component_daily, path_identity = _candidate_minus_open_only_daily(
        composite.sleeve_days, open_only.sleeve_days
    )
    daily = pd.concat(
        [composite.daily_comparisons, component_daily], ignore_index=True
    )
    comparisons = {}
    for offset, baseline in enumerate(
        ("BUY_AND_HOLD", "FROZEN_L1_V1", OPEN_ONLY_MODEL_ACTION_AUTHORITY)
    ):
        values = daily.loc[
            daily["baseline"].eq(baseline), "incremental_net_value_bps"
        ].to_numpy(float)
        comparisons[baseline] = _comparison(values, seed=INFERENCE_SEED + offset)
    unresolved = (
        not composite.receipt["coverage_can_support_policy"]
        or not open_only.receipt["coverage_can_support_policy"]
    )
    if unresolved:
        for comparison in comparisons.values():
            comparison["effect_evidence_before_coverage_constraint"] = comparison[
                "effect_evidence"
            ]
            comparison["effect_evidence"] = "INCONCLUSIVE"
            comparison["coverage_reason_code"] = (
                "SOURCE_OR_CORPORATE_ACTION_PATH_UNAVAILABLE"
            )
    alpha_evidence = _joint_evidence(
        {key: comparisons[key] for key in ("BUY_AND_HOLD", "FROZEN_L1_V1")}
    )
    add_component_evidence = comparisons[OPEN_ONLY_MODEL_ACTION_AUTHORITY][
        "effect_evidence"
    ]
    joint = _joint_evidence(comparisons)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_open_only_request_sha256": request["parent_open_only"][
            "request_sha256"
        ],
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "add_label_contract_sha256": ADD_LABEL_CONTRACT_SHA256,
        "candidate_policy_sha256": request["candidate_policy_sha256"],
        "result_class": RESULT_CLASS,
        "provenance_reason": PROVENANCE_REASON,
        "trial_count": 3,
        "planned_candidate_policy_count": 1,
        "familywise_hypothesis_count": 3,
        "selected_trial_count": 1 if joint == "SUPPORTED" else 0,
        "alpha_effect_evidence": alpha_evidence,
        "add_component_effect_evidence": add_component_evidence,
        "joint_effect_evidence": joint,
        "comparisons": comparisons,
        "heldout_population": {
            "training_symbol_count": len(training_symbols),
            "evaluation_symbol_count": len(evaluation_symbols),
            "prior_forbidden_symbol_count": request["prior_request_identity"][
                "forbidden_symbol_count"
            ],
            "training_evaluation_overlap": len(
                set(training_symbols).intersection(evaluation_symbols)
            ),
            "prior_evaluation_overlap": len(
                set(request["prior_request_identity"]["forbidden_symbols"]).intersection(
                    evaluation_symbols
                )
            ),
            "same_market_dates_not_temporal_holdout": True,
        },
        "add_population_coverage_sha256": add_population.coverage[
            "coverage_sha256"
        ],
        "add_training_row_count": len(add_population.rows),
        "add_oof_prediction_row_count": len(add_forward.predictions),
        "add_model_count": len(add_forward.models),
        "add_model_hashes": tuple(
            model.metadata["model_sha256"] for model in add_forward.models
        ),
        "entry_model_count": len(entry_forward.models),
        "entry_oof_equivalence": oof_identity,
        "open_only_path_identity": path_identity,
        "composite_policy": composite.receipt,
        "open_only_comparator": open_only.receipt,
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
        coverage=add_population.coverage,
        rows=add_population.rows,
        oof=add_forward.predictions,
        composite_sleeves=composite.sleeve_days,
        open_only_sleeves=open_only.sleeve_days,
        daily=daily,
        receipt=receipt,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspect_state_matched_add_bundle(bundle),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-open-only-bundle", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = {
                "status": "PREPARED",
                "request": prepare_state_matched_add_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    parent_open_only_bundle=args.parent_open_only_bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_state_matched_add_request(args.request)
        else:
            result = {
                "status": "BUNDLE_VALID",
                **inspect_state_matched_add_bundle(args.bundle),
            }
    except ActionValueError as exc:
        print(
            json.dumps(
                {"status": "FAILED", "error_code": exc.code, "details": exc.details},
                ensure_ascii=False,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "inspect_state_matched_add_bundle",
    "prepare_state_matched_add_request",
    "run_state_matched_add_request",
]
