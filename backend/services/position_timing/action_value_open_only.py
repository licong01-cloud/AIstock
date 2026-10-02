"""PT-NEXT-015 label-support-aligned OPEN-only held-out replay.

The ENTRY head was trained only on cash states.  This study removes ADD from
model authority, keeps the frozen rule risk exit, and evaluates one frozen
policy on symbols absent from every earlier action-value request.  It writes
only a content-addressed position-timing research bundle.
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

import pandas as pd

from .action_value import CORE_INFORMATION_BLOCK, ActionValueError, TZ
from .action_value_advice import (
    OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    OPEN_ONLY_MODEL_ACTION_CONTRACT,
    action_authority_policy_sha256,
)
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import DailyCandidate, file_reference
from .action_value_entry_only import _assert_oof_equivalence, _daily_replay_source_identity
from .action_value_heldout import (
    PRIOR_REQUEST_FOLDERS as PREVIOUS_PRIOR_REQUEST_FOLDERS,
    PRIOR_REQUEST_SCHEMAS as PREVIOUS_PRIOR_REQUEST_SCHEMAS,
    _freeze_source_snapshots,
    _joint_evidence,
    _parent_reference,
    _validate_bound_parents,
    _without_path,
    inspect_heldout_bundle,
    prior_request_identity,
    select_heldout_symbols,
)
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .action_value_suspensions import SuspensionSnapshotBook
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_OPEN_ONLY_HELDOUT_V1"
ARTIFACT_FOLDER = "action_value_open_only_v1"
REQUEST_SCHEMA = "position_timing_open_only_heldout_request_v1"
RECEIPT_SCHEMA = "position_timing_open_only_heldout_receipt_v1"
BUNDLE_SCHEMA = "position_timing_open_only_heldout_bundle_v1"
RESULT_CLASS = "CROSS_SYMBOL_HELDOUT_POLICY_STRUCTURE_CONFIRMATION"
PROVENANCE_REASON = "ENTRY_HEAD_CASH_STATE_SUPPORT_ALIGNED_ON_UNSEEN_SYMBOLS"
EVALUATION_SYMBOL_LIMIT = 64
PRIOR_REQUEST_FOLDERS = (*PREVIOUS_PRIOR_REQUEST_FOLDERS, "action_value_heldout_v1")
PRIOR_REQUEST_SCHEMAS = {
    *PREVIOUS_PRIOR_REQUEST_SCHEMAS,
    "position_timing_entry_only_heldout_request_v1",
    "position_timing_entry_only_heldout_request_v2",
}
STUDY_CONTRACT = {
    "schema_version": "position_timing_open_only_heldout_study_contract_v1",
    "research_question": "DOES_CASH_STATE_LABEL_SUPPORT_ALIGNMENT_IMPROVE_POLICY_VALUE",
    "candidate_policy": OPEN_ONLY_MODEL_ACTION_CONTRACT,
    "training_population": "IMMUTABLE_PARENT_V4_ONLY",
    "evaluation_population": {
        "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
        "symbol_limit": EVALUATION_SYMBOL_LIMIT,
        "prior_request_folders": PRIOR_REQUEST_FOLDERS,
        "outcomes_read_during_selection": False,
    },
    "co_primary_comparators": ("BUY_AND_HOLD", "FROZEN_L1_V1"),
    "planned_candidate_policy_count": 1,
    "planned_trial_count": 2,
    "familywise_hypothesis_count": 2,
    "economic_threshold_bps": 0.0,
    "inference": {
        "aggregation": "TRADING_DATE",
        "bootstrap": "CIRCULAR_MOVING_BLOCK",
        "block_sessions": 25,
        "samples": 5000,
        "seed": 20260910,
        "simultaneous_interval_level": 0.975,
    },
    "evidence_scope": "CROSS_SYMBOL_HELDOUT_SAME_MARKET_DATES_NOT_TEMPORAL_HOLDOUT",
    "serving": "FORBIDDEN_IN_THIS_TASK",
}
STUDY_CONTRACT_SHA256 = canonical_sha256(STUDY_CONTRACT)


def _validate_parent_heldout(inspected: Mapping[str, Any]) -> None:
    receipt = inspected["receipt"]
    if (
        receipt.get("joint_effect_evidence") != "INCONCLUSIVE"
        or receipt.get("selected_trial_count") != 0
        or receipt.get("serving_status") != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
    ):
        raise ActionValueError("OPEN_ONLY_PARENT_HELDOUT_CONTRACT_MISMATCH")


def label_support_diagnostic(*, v4_bundle: Path, heldout_bundle: Path) -> dict[str, Any]:
    """Describe the support mismatch without using return outcomes."""

    training_path = v4_bundle / "training_rows.parquet"
    sleeves_path = heldout_bundle / "heldout_sleeve_days.parquet"
    training_columns = (
        "objective",
        "holding_exposure",
        "cash_fraction",
        "holding_age_missing",
        "entry_cost_missing",
    )
    try:
        training = pd.read_parquet(training_path, columns=list(training_columns))
    except (OSError, ValueError, KeyError) as exc:
        raise ActionValueError("OPEN_ONLY_TRAINING_SUPPORT_SCHEMA_MISSING") from exc
    entry = training.loc[training["objective"].eq("ENTRY_ACTION_VALUE_V2")]
    cash_support = (
        len(entry) > 0
        and entry["holding_exposure"].eq(0).all()
        and entry["cash_fraction"].eq(1).all()
        and entry["holding_age_missing"].eq(1).all()
        and entry["entry_cost_missing"].eq(1).all()
    )
    sleeve_columns = (
        "sleeve_id",
        "baseline",
        "action",
        "pre_quantity",
        "symbol",
        "decision_trade_date",
    )
    try:
        sleeves = pd.read_parquet(sleeves_path, columns=list(sleeve_columns))
    except (OSError, ValueError, KeyError) as exc:
        raise ActionValueError("OPEN_ONLY_PARENT_PATH_SCHEMA_MISSING") from exc
    baselines = sorted(str(value) for value in sleeves["baseline"].dropna().unique())
    if baselines != ["BUY_AND_HOLD", "FROZEN_L1_V1"]:
        raise ActionValueError("OPEN_ONLY_PARENT_PATH_EMPTY")
    path_keys = ["sleeve_id", "symbol", "decision_trade_date"]
    if sleeves.groupby(path_keys, sort=False)[["action", "pre_quantity"]].nunique().gt(1).any().any():
        raise ActionValueError("OPEN_ONLY_PARENT_PATH_ACTION_MISMATCH")
    path = sleeves.loc[sleeves["baseline"].astype(str).eq("BUY_AND_HOLD")]
    add = path.loc[path["action"].eq("ADD")]
    if not cash_support or add.empty or not add["pre_quantity"].gt(0).all():
        raise ActionValueError("OPEN_ONLY_HYPOTHESIS_EVIDENCE_UNAVAILABLE")
    payload = {
        "schema_version": "position_timing_action_value_label_support_diagnostic_v1",
        "outcomes_read": False,
        "training_rows_file": file_reference(training_path),
        "heldout_sleeve_days_file": file_reference(sleeves_path),
        "entry_training_row_count": int(len(entry)),
        "entry_training_holding_exposure_values": tuple(
            sorted(float(value) for value in entry["holding_exposure"].unique())
        ),
        "entry_training_cash_fraction_values": tuple(
            sorted(float(value) for value in entry["cash_fraction"].unique())
        ),
        "entry_training_missing_state_only": bool(
            entry["holding_age_missing"].eq(1).all()
            and entry["entry_cost_missing"].eq(1).all()
        ),
        "parent_heldout_add_decision_count": int(len(add)),
        "parent_heldout_add_symbol_count": int(add["symbol"].nunique()),
        "parent_heldout_add_decision_date_count": int(add["decision_trade_date"].nunique()),
        "interpretation": "ENTRY_HEAD_WAS_USED_FOR_ADD_OUTSIDE_ITS_CASH_STATE_LABEL_SUPPORT",
    }
    return {**payload, "diagnostic_sha256": canonical_sha256(payload)}


def prepare_open_only_request(
    *,
    timing_root: Path,
    repository_root: Path,
    parent_heldout_bundle: Path,
) -> Path:
    from .action_value_pipeline import _clean_repository_commit

    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    timing_root = timing_root.resolve()
    parent_heldout_bundle = parent_heldout_bundle.resolve()
    parent_heldout = inspect_heldout_bundle(parent_heldout_bundle)
    _validate_parent_heldout(parent_heldout)
    heldout_request = parent_heldout["request"]
    parent_v4_bundle = Path(heldout_request["parent_v4"]["bundle_path"]).resolve()

    prior = prior_request_identity(
        timing_root / "research",
        request_folders=PRIOR_REQUEST_FOLDERS,
        request_schemas=tuple(PRIOR_REQUEST_SCHEMAS),
    )
    candidate = DailyCandidate.open(Path(heldout_request["candidate_root"]))
    population = heldout_request["population_spec"]
    symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=prior["forbidden_symbols"],
        seed=int(population["seed"]),
    )
    if not set(heldout_request["evaluation_symbols"]).issubset(prior["forbidden_symbols"]):
        raise ActionValueError("OPEN_ONLY_PARENT_EVALUATION_NOT_FORBIDDEN")
    start = date.fromisoformat(population["start"])
    end = date.fromisoformat(population["end"])
    corporate_action_path, suspension_path = _freeze_source_snapshots(
        timing_root=timing_root,
        symbols=symbols,
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
        "parent_heldout": _parent_reference(parent_heldout_bundle, parent_heldout),
        "parent_entry_only": heldout_request["parent_entry_only"],
        "parent_v4": heldout_request["parent_v4"],
        "candidate_root": Path(heldout_request["candidate_root"]).resolve().as_posix(),
        "training_symbols": tuple(heldout_request["training_symbols"]),
        "evaluation_symbols": symbols,
        "population_spec": {
            **population,
            "symbol_limit": EVALUATION_SYMBOL_LIMIT,
            "selected_symbols": symbols,
            "selection": "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": prior["forbidden_symbol_count"],
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "hypothesis_evidence": label_support_diagnostic(
            v4_bundle=parent_v4_bundle,
            heldout_bundle=parent_heldout_bundle,
        ),
        "daily_replay_source_identity": _daily_replay_source_identity(
            Path(heldout_request["candidate_root"]), symbols
        ),
        "corporate_action_snapshot": file_reference(corporate_action_path),
        "suspension_snapshot": file_reference(suspension_path),
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "parent_source_sha256": heldout_request["parent_source_sha256"],
        "parent_feature_spec_sha256": heldout_request["parent_feature_spec_sha256"],
        "parent_policy_sha256": heldout_request["parent_policy_sha256"],
        "candidate_policy_sha256": action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, OPEN_ONLY_MODEL_ACTION_AUTHORITY
        ),
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
    path = timing_root / "research" / ARTIFACT_FOLDER / "requests" / f"{request['request_sha256']}.json"
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("OPEN_ONLY_REQUEST_UNAVAILABLE") from exc
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
    population = request.get("population_spec") or {}
    prior = request.get("prior_request_identity") or {}
    evidence = request.get("hypothesis_evidence") or {}
    parent_entry = request.get("parent_entry_only") or {}
    parent_v4 = request.get("parent_v4") or {}
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("study_contract_sha256") != STUDY_CONTRACT_SHA256
        or canonical_sha256(request.get("study_contract")) != STUDY_CONTRACT_SHA256
        or request.get("result_class") != RESULT_CLASS
        or request.get("candidate_policy_sha256")
        != action_authority_policy_sha256(CORE_INFORMATION_BLOCK, OPEN_ONLY_MODEL_ACTION_AUTHORITY)
        or len(evaluation) != EVALUATION_SYMBOL_LIMIT
        or len(set(evaluation)) != len(evaluation)
        or not training
        or set(training).intersection(evaluation)
        or tuple(population.get("selected_symbols") or ()) != evaluation
        or population.get("selection")
        != "SHA256_SEED_AFTER_ALL_EARLIER_ACTION_VALUE_SYMBOLS_SOURCE_ONLY"
        or prior.get("schema_version") != "position_timing_prior_action_value_requests_v1"
        or prior.get("outcomes_read") is not False
        or tuple(prior.get("request_folders") or ()) != PRIOR_REQUEST_FOLDERS
        or set(prior.get("forbidden_symbols") or ()).intersection(evaluation)
        or population.get("forbidden_symbol_count") != prior.get("forbidden_symbol_count")
        or population.get("prior_requests_sha256") != prior.get("aggregate_sha256")
        or prior.get("aggregate_sha256")
        != canonical_sha256({key: value for key, value in prior.items() if key != "aggregate_sha256"})
        or evidence.get("outcomes_read") is not False
        or evidence.get("parent_heldout_add_decision_count", 0) <= 0
        or evidence.get("diagnostic_sha256")
        != canonical_sha256({key: value for key, value in evidence.items() if key != "diagnostic_sha256"})
        or not all(
            (request.get("parent_heldout") or {}).get(field)
            for field in (
                "bundle_path",
                "manifest_file",
                "manifest_sha256",
                "request_sha256",
                "receipt_sha256",
            )
        )
        or not all(
            parent_entry.get(field)
            for field in (
                "bundle_path",
                "manifest_file",
                "manifest_sha256",
                "request_sha256",
                "receipt_sha256",
            )
        )
        or not all(
            parent_v4.get(field)
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
        or not (request.get("daily_replay_source_identity") or {}).get("aggregate_sha256")
        or not (request.get("corporate_action_snapshot") or {}).get("sha256")
        or not (request.get("suspension_snapshot") or {}).get("sha256")
        or request.get("source_correction") != "EXPLICIT_DB_SUSPENSION_UNION_V1"
        or any(request.get(flag) is not False for flag in false_flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("OPEN_ONLY_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_parents(request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    heldout_ref = request["parent_heldout"]
    heldout_bundle = Path(heldout_ref["bundle_path"]).resolve()
    if file_reference(heldout_bundle / "manifest.json") != heldout_ref["manifest_file"]:
        raise ActionValueError("OPEN_ONLY_PARENT_HELDOUT_MANIFEST_CHANGED")
    heldout = inspect_heldout_bundle(heldout_bundle)
    _validate_parent_heldout(heldout)
    if (
        heldout["manifest"]["manifest_sha256"] != heldout_ref["manifest_sha256"]
        or heldout["request"]["request_sha256"] != heldout_ref["request_sha256"]
        or heldout["receipt"]["receipt_sha256"] != heldout_ref["receipt_sha256"]
    ):
        raise ActionValueError("OPEN_ONLY_PARENT_HELDOUT_IDENTITY_MISMATCH")
    v4_bundle, v4 = _validate_bound_parents(request)
    if canonical_sha256(
        label_support_diagnostic(v4_bundle=v4_bundle, heldout_bundle=heldout_bundle)
    ) != canonical_sha256(request["hypothesis_evidence"]):
        raise ActionValueError("OPEN_ONLY_HYPOTHESIS_EVIDENCE_CHANGED")
    return v4_bundle, v4


def _manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "request.json",
        "oof_action_predictions.parquet",
        "open_only_sleeve_days.parquet",
        "open_only_daily_comparisons.parquet",
        "receipt.json",
    )
    manifest = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": {name: _without_path(file_reference(root / name)) for name in names},
    }
    return {**manifest, "manifest_sha256": canonical_sha256(manifest)}


def _publish_bundle(
    bundle: Path,
    *,
    request: Mapping[str, Any],
    receipt: Mapping[str, Any],
    oof: pd.DataFrame,
    sleeves: pd.DataFrame,
    daily: pd.DataFrame,
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(
            staging / "request.json", canonical_json_bytes(request)
        )
        oof.to_parquet(staging / "oof_action_predictions.parquet", index=False)
        sleeves.to_parquet(staging / "open_only_sleeve_days.parquet", index=False)
        daily.to_parquet(staging / "open_only_daily_comparisons.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"open-only-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_open_only_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_open_only_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("OPEN_ONLY_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    comparisons = (receipt.get("open_only_policy") or {}).get("comparisons") or {}
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
        or receipt.get("result_class") != RESULT_CLASS
        or receipt.get("provenance_reason") != PROVENANCE_REASON
        or receipt.get("trial_count") != 2
        or receipt.get("familywise_hypothesis_count") != 2
        or receipt.get("planned_candidate_policy_count") != 1
        or receipt.get("selected_trial_count") != (1 if joint == "SUPPORTED" else 0)
        or set(comparisons) != {"BUY_AND_HOLD", "FROZEN_L1_V1"}
        or receipt.get("joint_effect_evidence") != joint
        or receipt.get("candidate_policy_sha256") != request.get("candidate_policy_sha256")
        or receipt.get("serving_status") != "NOT_SERVING_SEPARATE_RUNTIME_DECISION_REQUIRED"
        or any(receipt.get(flag) is not False for flag in false_flags)
    ):
        raise ActionValueError("OPEN_ONLY_BUNDLE_IDENTITY_MISMATCH")
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError("OPEN_ONLY_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_open_only_request(request_path: Path) -> dict[str, Any]:
    from .action_value_pipeline import _clean_repository_commit

    request = _load_request(request_path)
    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("OPEN_ONLY_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspect_open_only_bundle(bundle),
        }
    observed_prior = prior_request_identity(
        timing_root / "research",
        request_folders=PRIOR_REQUEST_FOLDERS,
        request_schemas=tuple(PRIOR_REQUEST_SCHEMAS),
    )
    if canonical_sha256(observed_prior) != canonical_sha256(request["prior_request_identity"]):
        raise ActionValueError("OPEN_ONLY_PRIOR_REQUEST_SET_CHANGED")
    v4_bundle, v4 = _validate_parents(request)
    symbols = tuple(request["evaluation_symbols"])
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    expected_symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=request["prior_request_identity"]["forbidden_symbols"],
        seed=int(request["population_spec"]["seed"]),
    )
    if expected_symbols != symbols:
        raise ActionValueError("OPEN_ONLY_POPULATION_IDENTITY_MISMATCH")
    if _daily_replay_source_identity(candidate.root, symbols) != request["daily_replay_source_identity"]:
        raise ActionValueError("OPEN_ONLY_DAILY_REPLAY_SOURCE_CHANGED")
    if file_reference(Path(request["corporate_action_snapshot"]["path"])) != request[
        "corporate_action_snapshot"
    ]:
        raise ActionValueError("OPEN_ONLY_CORPORATE_ACTION_SOURCE_CHANGED")
    if file_reference(Path(request["suspension_snapshot"]["path"])) != request["suspension_snapshot"]:
        raise ActionValueError("OPEN_ONLY_SUSPENSION_SOURCE_CHANGED")

    corporate_actions = CorporateActionBook.open(Path(request["corporate_action_snapshot"]["path"]))
    suspensions = SuspensionSnapshotBook.open(Path(request["suspension_snapshot"]["path"]))
    expected_scope = (
        tuple(sorted(symbols)),
        date.fromisoformat(request["population_spec"]["start"]),
        date.fromisoformat(request["population_spec"]["end"]),
    )
    if (suspensions.symbols, suspensions.start, suspensions.end) != expected_scope:
        raise ActionValueError("OPEN_ONLY_SUSPENSION_SCOPE_MISMATCH")
    candidate = suspensions.apply(
        candidate, snapshot_path=Path(request["suspension_snapshot"]["path"])
    )
    rows = pd.read_parquet(v4_bundle / "training_rows.parquet")
    if set(rows["symbol"].astype(str)) != set(request["training_symbols"]):
        raise ActionValueError("OPEN_ONLY_TRAINING_POPULATION_MISMATCH")
    forward = walk_forward_action_values(
        rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=request["parent_source_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    oof_identity = _assert_oof_equivalence(
        forward.predictions,
        pd.read_parquet(v4_bundle / "oof_action_predictions.parquet"),
    )
    replay = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=symbols,
        corporate_actions=corporate_actions,
        bootstrap_samples=5000,
        block_sessions=25,
        seed=20260910,
        initial_holding_policy_id=EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
        model_action_authority=OPEN_ONLY_MODEL_ACTION_AUTHORITY,
    )
    comparisons = replay.receipt["comparisons"]
    joint = _joint_evidence(comparisons)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_heldout_request_sha256": request["parent_heldout"]["request_sha256"],
        "parent_heldout_receipt_sha256": request["parent_heldout"]["receipt_sha256"],
        "parent_v4_request_sha256": v4["request"]["request_sha256"],
        "parent_v4_receipt_sha256": v4["receipt"]["receipt_sha256"],
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "candidate_policy_sha256": request["candidate_policy_sha256"],
        "hypothesis_evidence_sha256": request["hypothesis_evidence"]["diagnostic_sha256"],
        "result_class": RESULT_CLASS,
        "provenance_reason": PROVENANCE_REASON,
        "evidence_scope": STUDY_CONTRACT["evidence_scope"],
        "trial_count": 2,
        "planned_candidate_policy_count": 1,
        "familywise_hypothesis_count": 2,
        "selected_trial_count": 1 if joint == "SUPPORTED" else 0,
        "joint_effect_evidence": joint,
        "heldout_population": {
            "training_symbol_count": len(request["training_symbols"]),
            "evaluation_symbol_count": len(symbols),
            "prior_forbidden_symbol_count": request["prior_request_identity"][
                "forbidden_symbol_count"
            ],
            "training_evaluation_overlap": len(set(request["training_symbols"]).intersection(symbols)),
            "prior_evaluation_overlap": len(
                set(request["prior_request_identity"]["forbidden_symbols"]).intersection(symbols)
            ),
            "selection": request["population_spec"]["selection"],
            "same_market_dates_not_temporal_holdout": True,
        },
        "open_only_policy": replay.receipt,
        "source_correction": request["source_correction"],
        "suspension_snapshot_sha256": suspensions.snapshot_sha256,
        "oof_equivalence": oof_identity,
        "model_training_identity": {
            "model_count": len(forward.models),
            "model_sha256": tuple(model.metadata["model_sha256"] for model in forward.models),
            "training_rows_file": request["parent_v4"]["training_rows_file"],
            "evaluation_symbols_never_enter_training": True,
        },
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
        receipt=receipt,
        oof=forward.predictions,
        sleeves=replay.sleeve_days,
        daily=replay.daily_comparisons,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspect_open_only_bundle(bundle),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-heldout-bundle", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = {
                "status": "PREPARED",
                "request": prepare_open_only_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    parent_heldout_bundle=args.parent_heldout_bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_open_only_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_open_only_bundle(args.bundle)}
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
    "inspect_open_only_bundle",
    "label_support_diagnostic",
    "prepare_open_only_request",
    "run_open_only_request",
]
