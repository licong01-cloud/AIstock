"""PT-NEXT-014 cross-symbol held-out replay for the frozen entry-only policy.

The training population, core feature contract, model family, hyperparameters,
policy authority, cost model, and comparators are inherited from immutable
PT-NEXT-013/v4 evidence.  Evaluation symbols are selected source-only after
excluding every symbol named by an existing position-timing action-value
request.  The pipeline writes only a content-addressed research bundle.
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
    ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    ENTRY_ONLY_MODEL_ACTION_CONTRACT,
    action_authority_policy_sha256,
)
from .action_value_corporate_actions import (
    CorporateActionBook,
    freeze_corporate_action_snapshot,
)
from .action_value_data import DailyCandidate, file_reference
from .action_value_entry_only import (
    RESULT_CLASS as ENTRY_ONLY_RESULT_CLASS,
    _assert_oof_equivalence,
    _daily_replay_source_identity,
    _without_path,
    inspect_entry_only_bundle,
)
from .action_value_pipeline import _clean_repository_commit, inspect_bundle
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
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


PIPELINE_ID = "POSITION_TIMING_ENTRY_ONLY_HELDOUT_V1"
ARTIFACT_FOLDER = "action_value_heldout_v1"
LEGACY_REQUEST_SCHEMA = "position_timing_entry_only_heldout_request_v1"
REQUEST_SCHEMA = "position_timing_entry_only_heldout_request_v2"
RECEIPT_SCHEMA = "position_timing_entry_only_heldout_receipt_v1"
BUNDLE_SCHEMA = "position_timing_entry_only_heldout_bundle_v1"
RESULT_CLASS = "CROSS_SYMBOL_HELDOUT_CONFIRMATION"
PROVENANCE_REASON = "FROZEN_PT_NEXT_013_POLICY_ON_UNSEEN_SYMBOL_POPULATION"
EVALUATION_SYMBOL_LIMIT = 64
PRIOR_REQUEST_FOLDERS = (
    "action_value_v2",
    "action_value_incremental_v1",
    "action_value_entry_only_v1",
)
PRIOR_REQUEST_SCHEMAS = {
    "position_timing_action_value_request_v2",
    "position_timing_action_value_request_v3",
    "position_timing_action_value_request_v4",
    "position_timing_action_value_increment_request_v1",
    "position_timing_action_value_increment_request_v2",
    "position_timing_action_value_increment_request_v3",
    "position_timing_entry_only_policy_request_v1",
}
STUDY_CONTRACT = {
    "schema_version": "position_timing_entry_only_heldout_study_contract_v1",
    "research_question": "ENTRY_ONLY_POLICY_TRANSFERS_TO_UNSEEN_SYMBOLS",
    "candidate_policy": ENTRY_ONLY_MODEL_ACTION_CONTRACT,
    "training_population": "IMMUTABLE_PARENT_V4_ONLY",
    "evaluation_population": {
        "selection": "SHA256_SEED_AFTER_ALL_PRIOR_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
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


def _request_symbols(request: Mapping[str, Any]) -> tuple[str, ...]:
    raw = request.get("selected_symbols") or (request.get("population_spec") or {}).get("selected_symbols")
    symbols = tuple(str(symbol).upper() for symbol in (raw or ()))
    if not symbols or len(symbols) != len(set(symbols)):
        raise ActionValueError("HELDOUT_PRIOR_REQUEST_POPULATION_INVALID")
    return symbols


def prior_request_identity(research_root: Path) -> dict[str, Any]:
    """Bind every prior formal action-value request without reading outcomes."""

    root = research_root.resolve()
    records: list[dict[str, Any]] = []
    forbidden: set[str] = set()
    folder_counts: dict[str, int] = {}
    for folder in PRIOR_REQUEST_FOLDERS:
        request_root = root / folder / "requests"
        paths = sorted(request_root.glob("*.json")) if request_root.is_dir() else []
        if not paths:
            raise ActionValueError("HELDOUT_PRIOR_REQUEST_SET_UNAVAILABLE", folder=folder)
        folder_counts[folder] = len(paths)
        for path in paths:
            before = file_reference(path)
            try:
                request = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                raise ActionValueError("HELDOUT_PRIOR_REQUEST_UNREADABLE", path=path.as_posix()) from exc
            if file_reference(path) != before:
                raise ActionValueError("SOURCE_CHANGED_WHILE_READING", path=path.as_posix())
            identity = {key: value for key, value in request.items() if key != "request_sha256"}
            if request.get("schema_version") not in PRIOR_REQUEST_SCHEMAS or request.get(
                "request_sha256"
            ) != canonical_sha256(identity):
                raise ActionValueError("HELDOUT_PRIOR_REQUEST_IDENTITY_MISMATCH", path=path.as_posix())
            symbols = _request_symbols(request)
            forbidden.update(symbols)
            records.append(
                {
                    "folder": folder,
                    "relative_path": path.resolve().relative_to(root).as_posix(),
                    "schema_version": request["schema_version"],
                    "request_sha256": request["request_sha256"],
                    "file_sha256": before["sha256"],
                    "size_bytes": before["size_bytes"],
                    "selected_symbol_count": len(symbols),
                    "selected_symbols_sha256": canonical_sha256(symbols),
                }
            )
    payload = {
        "schema_version": "position_timing_prior_action_value_requests_v1",
        "research_root": root.as_posix(),
        "request_folders": PRIOR_REQUEST_FOLDERS,
        "request_count": len(records),
        "folder_counts": folder_counts,
        "requests": records,
        "forbidden_symbols": tuple(sorted(forbidden)),
        "forbidden_symbol_count": len(forbidden),
        "outcomes_read": False,
    }
    return {**payload, "aggregate_sha256": canonical_sha256(payload)}


def select_heldout_symbols(
    candidate_symbols: Sequence[str],
    *,
    forbidden_symbols: Sequence[str],
    seed: int,
    limit: int = EVALUATION_SYMBOL_LIMIT,
) -> tuple[str, ...]:
    if limit <= 0:
        raise ActionValueError("HELDOUT_POPULATION_LIMIT_INVALID")
    forbidden = {str(symbol).upper() for symbol in forbidden_symbols}
    ranked = deterministic_symbols(candidate_symbols, limit=len(set(candidate_symbols)), seed=seed)
    selected = tuple(symbol for symbol in ranked if symbol not in forbidden)[:limit]
    if len(selected) != limit or forbidden.intersection(selected):
        raise ActionValueError("HELDOUT_POPULATION_UNAVAILABLE")
    return selected


def _parent_reference(bundle: Path, inspected: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_path": bundle.resolve().as_posix(),
        "manifest_file": file_reference(bundle / "manifest.json"),
        "manifest_sha256": inspected["manifest"]["manifest_sha256"],
        "request_sha256": inspected["request"]["request_sha256"],
        "receipt_sha256": inspected["receipt"]["receipt_sha256"],
    }


def _validate_parent_entry_only(inspected: Mapping[str, Any]) -> None:
    request = inspected["request"]
    receipt = inspected["receipt"]
    if (
        receipt.get("result_class") != ENTRY_ONLY_RESULT_CLASS
        or receipt.get("joint_effect_evidence") != "INCONCLUSIVE"
        or receipt.get("selected_trial_count") != 0
        or request.get("candidate_policy_sha256")
        != action_authority_policy_sha256(CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY)
    ):
        raise ActionValueError("HELDOUT_PARENT_ENTRY_ONLY_CONTRACT_MISMATCH")


def _freeze_source_snapshots(*, timing_root: Path, symbols: Sequence[str], start: date, end: date) -> tuple[Path, Path]:
    from backend.db.pg_pool import get_conn

    with get_conn(autocommit=False) as connection:
        connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
        corporate_action_path = freeze_corporate_action_snapshot(
            connection,
            symbols=symbols,
            start=start,
            end=end,
            timing_root=timing_root,
        )
        suspension_path = freeze_suspension_snapshot(
            connection,
            symbols=symbols,
            start=start,
            end=end,
            timing_root=timing_root,
        )
    return corporate_action_path, suspension_path


def prepare_heldout_request(*, timing_root: Path, repository_root: Path, parent_entry_only_bundle: Path) -> Path:
    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    timing_root = timing_root.resolve()
    parent_entry_only_bundle = parent_entry_only_bundle.resolve()
    parent = inspect_entry_only_bundle(parent_entry_only_bundle)
    _validate_parent_entry_only(parent)
    parent_request = parent["request"]
    parent_v4_bundle = Path(parent_request["parent_v4"]["bundle_path"]).resolve()
    parent_v4 = inspect_bundle(parent_v4_bundle)
    if (
        parent_v4["request"]["request_sha256"] != parent_request["parent_v4"]["request_sha256"]
        or parent_v4["receipt"]["receipt_sha256"] != parent_request["parent_v4"]["receipt_sha256"]
    ):
        raise ActionValueError("HELDOUT_PARENT_V4_IDENTITY_MISMATCH")

    prior = prior_request_identity(timing_root / "research")
    candidate = DailyCandidate.open(Path(parent_request["candidate_root"]))
    population = parent_request["population_spec"]
    seed = int(population["seed"])
    symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=prior["forbidden_symbols"],
        seed=seed,
    )
    hypothesis_symbols = set(parent_request["selected_symbols"])
    if not hypothesis_symbols.issubset(set(prior["forbidden_symbols"])):
        raise ActionValueError("HELDOUT_HYPOTHESIS_POPULATION_NOT_FORBIDDEN")
    start = date.fromisoformat(population["start"])
    end = date.fromisoformat(population["end"])
    corporate_action_path, suspension_path = _freeze_source_snapshots(
        timing_root=timing_root, symbols=symbols, start=start, end=end
    )
    corporate_action_ref = file_reference(corporate_action_path)
    suspension_ref = file_reference(suspension_path)
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.as_posix(),
        "parent_entry_only": _parent_reference(parent_entry_only_bundle, parent),
        "parent_v4": {
            **parent_request["parent_v4"],
            "training_rows_file": file_reference(parent_v4_bundle / "training_rows.parquet"),
            "oof_predictions_file": file_reference(parent_v4_bundle / "oof_action_predictions.parquet"),
        },
        "candidate_root": Path(parent_request["candidate_root"]).resolve().as_posix(),
        "training_symbols": tuple(parent_request["selected_symbols"]),
        "evaluation_symbols": symbols,
        "population_spec": {
            **population,
            "symbol_limit": EVALUATION_SYMBOL_LIMIT,
            "selected_symbols": symbols,
            "selection": "SHA256_SEED_AFTER_ALL_PRIOR_ACTION_VALUE_SYMBOLS_SOURCE_ONLY",
            "forbidden_symbol_count": prior["forbidden_symbol_count"],
            "prior_requests_sha256": prior["aggregate_sha256"],
        },
        "prior_request_identity": prior,
        "daily_replay_source_identity": _daily_replay_source_identity(Path(parent_request["candidate_root"]), symbols),
        "corporate_action_snapshot": corporate_action_ref,
        "suspension_snapshot": suspension_ref,
        "source_correction": "EXPLICIT_DB_SUSPENSION_UNION_V1",
        "parent_source_sha256": parent_v4["receipt"]["source_sha256"],
        "parent_feature_spec_sha256": parent_v4["receipt"]["feature_spec_sha256"],
        "parent_policy_sha256": parent_v4["receipt"]["policy_sha256"],
        "candidate_policy_sha256": action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY
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
        raise ActionValueError("HELDOUT_REQUEST_UNAVAILABLE") from exc
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
    parent_entry = request.get("parent_entry_only") or {}
    parent_v4 = request.get("parent_v4") or {}
    replay_source = request.get("daily_replay_source_identity") or {}
    corporate_action = request.get("corporate_action_snapshot") or {}
    suspension = request.get("suspension_snapshot") or {}
    schema = request.get("schema_version")
    suspension_contract_valid = (
        schema == LEGACY_REQUEST_SCHEMA and not suspension and request.get("source_correction") is None
    ) or (
        schema == REQUEST_SCHEMA
        and suspension.get("path")
        and suspension.get("sha256")
        and request.get("source_correction") == "EXPLICIT_DB_SUSPENSION_UNION_V1"
    )
    if (
        schema not in {LEGACY_REQUEST_SCHEMA, REQUEST_SCHEMA}
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("study_contract_sha256") != STUDY_CONTRACT_SHA256
        or canonical_sha256(request.get("study_contract")) != STUDY_CONTRACT_SHA256
        or request.get("result_class") != RESULT_CLASS
        or request.get("candidate_policy_sha256")
        != action_authority_policy_sha256(CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY)
        or len(evaluation) != EVALUATION_SYMBOL_LIMIT
        or len(set(evaluation)) != len(evaluation)
        or not training
        or set(training).intersection(evaluation)
        or tuple(population.get("selected_symbols") or ()) != evaluation
        or population.get("selection") != "SHA256_SEED_AFTER_ALL_PRIOR_ACTION_VALUE_SYMBOLS_SOURCE_ONLY"
        or prior.get("schema_version") != "position_timing_prior_action_value_requests_v1"
        or prior.get("outcomes_read") is not False
        or tuple(prior.get("request_folders") or ()) != PRIOR_REQUEST_FOLDERS
        or Path(str(prior.get("research_root", ""))).resolve()
        != Path(request.get("timing_root", ""), "research").resolve()
        or set(prior.get("forbidden_symbols") or ()).intersection(evaluation)
        or population.get("forbidden_symbol_count") != prior.get("forbidden_symbol_count")
        or population.get("prior_requests_sha256") != prior.get("aggregate_sha256")
        or prior.get("aggregate_sha256")
        != canonical_sha256({key: value for key, value in prior.items() if key != "aggregate_sha256"})
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
        or replay_source.get("schema_version") != "position_timing_daily_replay_source_identity_v1"
        or not replay_source.get("aggregate_sha256")
        or not corporate_action.get("path")
        or not corporate_action.get("sha256")
        or not suspension_contract_valid
        or any(request.get(flag) is not False for flag in false_flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("HELDOUT_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_bound_parents(request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    entry_ref = request["parent_entry_only"]
    entry_bundle = Path(entry_ref["bundle_path"]).resolve()
    if file_reference(entry_bundle / "manifest.json") != entry_ref["manifest_file"]:
        raise ActionValueError("HELDOUT_PARENT_ENTRY_ONLY_MANIFEST_CHANGED")
    entry = inspect_entry_only_bundle(entry_bundle)
    _validate_parent_entry_only(entry)
    if (
        entry["manifest"]["manifest_sha256"] != entry_ref["manifest_sha256"]
        or entry["request"]["request_sha256"] != entry_ref["request_sha256"]
        or entry["receipt"]["receipt_sha256"] != entry_ref["receipt_sha256"]
    ):
        raise ActionValueError("HELDOUT_PARENT_ENTRY_ONLY_IDENTITY_MISMATCH")
    entry_request = entry["request"]
    if (
        tuple(entry_request["selected_symbols"]) != tuple(request["training_symbols"])
        or Path(entry_request["candidate_root"]).resolve() != Path(request["candidate_root"]).resolve()
        or int(entry_request["population_spec"]["seed"]) != int(request["population_spec"]["seed"])
        or entry_request["population_spec"]["start"] != request["population_spec"]["start"]
        or entry_request["population_spec"]["end"] != request["population_spec"]["end"]
    ):
        raise ActionValueError("HELDOUT_PARENT_POPULATION_IDENTITY_MISMATCH")
    v4_ref = request["parent_v4"]
    v4_bundle = Path(v4_ref["bundle_path"]).resolve()
    v4 = inspect_bundle(v4_bundle)
    if (
        v4["manifest"]["manifest_sha256"] != v4_ref["manifest_sha256"]
        or v4["request"]["request_sha256"] != v4_ref["request_sha256"]
        or v4["receipt"]["receipt_sha256"] != v4_ref["receipt_sha256"]
        or file_reference(v4_bundle / "training_rows.parquet") != v4_ref["training_rows_file"]
        or file_reference(v4_bundle / "oof_action_predictions.parquet") != v4_ref["oof_predictions_file"]
        or v4["receipt"]["source_sha256"] != request["parent_source_sha256"]
        or v4["receipt"]["feature_spec_sha256"] != request["parent_feature_spec_sha256"]
        or v4["receipt"]["policy_sha256"] != request["parent_policy_sha256"]
    ):
        raise ActionValueError("HELDOUT_PARENT_V4_IDENTITY_MISMATCH")
    return v4_bundle, v4


def _joint_evidence(comparisons: Mapping[str, Mapping[str, Any]]) -> str:
    evidence = {item.get("effect_evidence") for item in comparisons.values()}
    if evidence == {"SUPPORTED"}:
        return "SUPPORTED"
    if "NEGATIVE" in evidence:
        return "NEGATIVE"
    return "INCONCLUSIVE"


def _manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "request.json",
        "oof_action_predictions.parquet",
        "heldout_sleeve_days.parquet",
        "heldout_daily_comparisons.parquet",
        "receipt.json",
    )
    files = {name: _without_path(file_reference(root / name)) for name in names}
    manifest = {
        "schema_version": BUNDLE_SCHEMA,
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": files,
    }
    manifest["manifest_sha256"] = canonical_sha256(manifest)
    return manifest


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
        PositionTimingArtifactStore._publish_immutable(staging / "request.json", canonical_json_bytes(request))
        oof.to_parquet(staging / "oof_action_predictions.parquet", index=False)
        sleeves.to_parquet(staging / "heldout_sleeve_days.parquet", index=False)
        daily.to_parquet(staging / "heldout_daily_comparisons.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(staging / "receipt.json", canonical_json_bytes(receipt))
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"heldout-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_heldout_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_heldout_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("HELDOUT_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    comparisons = (receipt.get("heldout_policy") or {}).get("comparisons") or {}
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
        raise ActionValueError("HELDOUT_BUNDLE_IDENTITY_MISMATCH")
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError("HELDOUT_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_heldout_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("HELDOUT_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspect_heldout_bundle(bundle),
        }
    if request["schema_version"] == LEGACY_REQUEST_SCHEMA:
        raise ActionValueError("HELDOUT_LEGACY_REQUEST_NOT_RUNNABLE")
    observed_prior = prior_request_identity(timing_root / "research")
    if canonical_sha256(observed_prior) != canonical_sha256(request["prior_request_identity"]):
        raise ActionValueError("HELDOUT_PRIOR_REQUEST_SET_CHANGED")
    v4_bundle, v4 = _validate_bound_parents(request)
    symbols = tuple(request["evaluation_symbols"])
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    expected_symbols = select_heldout_symbols(
        candidate.symbols,
        forbidden_symbols=request["prior_request_identity"]["forbidden_symbols"],
        seed=int(request["population_spec"]["seed"]),
    )
    if expected_symbols != symbols:
        raise ActionValueError("HELDOUT_POPULATION_IDENTITY_MISMATCH")
    if _daily_replay_source_identity(candidate.root, symbols) != request["daily_replay_source_identity"]:
        raise ActionValueError("HELDOUT_DAILY_REPLAY_SOURCE_CHANGED")
    if file_reference(Path(request["corporate_action_snapshot"]["path"])) != request["corporate_action_snapshot"]:
        raise ActionValueError("HELDOUT_CORPORATE_ACTION_SOURCE_CHANGED")
    if file_reference(Path(request["suspension_snapshot"]["path"])) != request["suspension_snapshot"]:
        raise ActionValueError("HELDOUT_SUSPENSION_SOURCE_CHANGED")

    corporate_actions = CorporateActionBook.open(Path(request["corporate_action_snapshot"]["path"]))
    suspensions = SuspensionSnapshotBook.open(Path(request["suspension_snapshot"]["path"]))
    expected_scope = (
        tuple(sorted(symbols)),
        date.fromisoformat(request["population_spec"]["start"]),
        date.fromisoformat(request["population_spec"]["end"]),
    )
    if (suspensions.symbols, suspensions.start, suspensions.end) != expected_scope:
        raise ActionValueError("HELDOUT_SUSPENSION_SCOPE_MISMATCH")
    candidate = suspensions.apply(candidate, snapshot_path=Path(request["suspension_snapshot"]["path"]))
    rows = pd.read_parquet(v4_bundle / "training_rows.parquet")
    if set(rows["symbol"].astype(str)) != set(request["training_symbols"]):
        raise ActionValueError("HELDOUT_TRAINING_POPULATION_MISMATCH")
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
    heldout = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=symbols,
        corporate_actions=corporate_actions,
        bootstrap_samples=5000,
        block_sessions=25,
        seed=20260910,
        initial_holding_policy_id=EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
        model_action_authority=ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    )
    comparisons = heldout.receipt["comparisons"]
    joint = _joint_evidence(comparisons)
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_entry_only_request_sha256": request["parent_entry_only"]["request_sha256"],
        "parent_entry_only_receipt_sha256": request["parent_entry_only"]["receipt_sha256"],
        "parent_v4_request_sha256": v4["request"]["request_sha256"],
        "parent_v4_receipt_sha256": v4["receipt"]["receipt_sha256"],
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "candidate_policy_sha256": request["candidate_policy_sha256"],
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
            "prior_forbidden_symbol_count": request["prior_request_identity"]["forbidden_symbol_count"],
            "training_evaluation_overlap": len(set(request["training_symbols"]).intersection(symbols)),
            "prior_evaluation_overlap": len(
                set(request["prior_request_identity"]["forbidden_symbols"]).intersection(symbols)
            ),
            "selection": request["population_spec"]["selection"],
            "same_market_dates_not_temporal_holdout": True,
        },
        "heldout_policy": heldout.receipt,
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
        sleeves=heldout.sleeve_days,
        daily=heldout.daily_comparisons,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspect_heldout_bundle(bundle),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-entry-only-bundle", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = {
                "status": "PREPARED",
                "request": prepare_heldout_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    parent_entry_only_bundle=args.parent_entry_only_bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_heldout_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_heldout_bundle(args.bundle)}
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
    "ARTIFACT_FOLDER",
    "BUNDLE_SCHEMA",
    "PIPELINE_ID",
    "RECEIPT_SCHEMA",
    "REQUEST_SCHEMA",
    "STUDY_CONTRACT",
    "STUDY_CONTRACT_SHA256",
    "inspect_heldout_bundle",
    "prepare_heldout_request",
    "prior_request_identity",
    "run_heldout_request",
    "select_heldout_symbols",
]
