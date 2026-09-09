"""PT-NEXT-013 historical replay for one frozen entry-only model policy.

The pipeline retrains the already-frozen core GBDT from the immutable v4
training rows, then replays one candidate policy and the unchanged full-v4
control with the same model objects and market path.  It writes only a
content-addressed position-timing research bundle: no registry, current
pointer, card, alert, order, database, API, scheduler, or model artifact.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from .action_value import CORE_INFORMATION_BLOCK, ActionValueError, TZ
from .action_value_advice import (
    ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    ENTRY_ONLY_MODEL_ACTION_CONTRACT,
    FULL_MODEL_ACTION_AUTHORITY,
    action_authority_policy_sha256,
)
from .action_value_corporate_actions import CorporateActionBook
from .action_value_data import BENCHMARK, DAILY_FIELDS, DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit, inspect_bundle
from .action_value_research import (
    EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    REFERENCE_CAPITAL_CNY,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_ENTRY_ONLY_POLICY_V1"
ARTIFACT_FOLDER = "action_value_entry_only_v1"
REQUEST_SCHEMA = "position_timing_entry_only_policy_request_v1"
RECEIPT_SCHEMA = "position_timing_entry_only_policy_receipt_v1"
BUNDLE_SCHEMA = "position_timing_entry_only_policy_bundle_v1"
RESULT_CLASS = "EXPLORATORY_HYPOTHESIS_GENERATED"
PROVENANCE_REASON = "HYPOTHESIS_GENERATED_FROM_PT_NEXT_012"
REPLAY_KEYS = (
    "sleeve_id",
    "symbol",
    "initial_state",
    "decision_trade_date",
    "valuation_date",
    "baseline",
)
BASELINE_IDENTITY_COLUMNS = (
    *REPLAY_KEYS,
    "continuous_start",
    "target_trade_date",
    "baseline_wealth_cny",
    "baseline_fractional_share_discarded",
    "corporate_action_applied",
    "corporate_action_source_rows_sha256",
)
STUDY_CONTRACT = {
    "schema_version": "position_timing_entry_only_study_contract_v1",
    "candidate_policy": ENTRY_ONLY_MODEL_ACTION_CONTRACT,
    "full_policy_control": FULL_MODEL_ACTION_AUTHORITY,
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
        "seed": 20260908,
        "simultaneous_interval_level": 0.975,
    },
    "candidate_minus_full_v4": "DIAGNOSTIC_ONLY_NOT_A_TRIAL",
    "result_provenance": PROVENANCE_REASON,
    "serving": "FORBIDDEN_IN_THIS_TASK",
}
STUDY_CONTRACT_SHA256 = canonical_sha256(STUDY_CONTRACT)


def _without_path(reference: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in reference.items() if key != "path"}


def _daily_replay_source_identity(candidate_root: Path, symbols: Sequence[str]) -> dict[str, Any]:
    root = candidate_root.resolve()
    daily = root / "components" / "daily_bin_candidate"
    paths = {
        "candidate": root / "direct_monthly_state.json",
        "daily_meta": daily / "meta_export.json",
        "calendar": daily / "calendars" / "day.txt",
        "pit": daily / "instruments" / "stock_universe.txt",
        "suspend": root
        / "components"
        / "suspend_d_daily_candidate_v2"
        / "suspend_d.parquet",
    }
    for symbol in (*symbols, BENCHMARK):
        fields = ("open", "high", "low", "close", "volume") if symbol == BENCHMARK else DAILY_FIELDS
        for field in fields:
            paths[f"daily_feature:{symbol}:{field}"] = (
                daily / "features" / symbol.lower() / f"{field}.day.bin"
            )
    records = []
    for role, path in sorted(paths.items()):
        reference = file_reference(path)
        records.append(
            {
                "role": role,
                "relative_path": path.resolve().relative_to(root).as_posix(),
                "sha256": reference["sha256"],
                "size_bytes": reference["size_bytes"],
            }
        )
    return {
        "schema_version": "position_timing_daily_replay_source_identity_v1",
        "candidate_root": root.as_posix(),
        "file_count": len(records),
        "aggregate_sha256": canonical_sha256(records),
    }


def _parent_reference(bundle: Path, inspected: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "bundle_path": bundle.resolve().as_posix(),
        "manifest_file": file_reference(bundle / "manifest.json"),
        "manifest_sha256": inspected["manifest"]["manifest_sha256"],
        "request_sha256": inspected["request"]["request_sha256"],
        "receipt_sha256": inspected["receipt"]["receipt_sha256"],
        "training_rows_file": file_reference(bundle / "training_rows.parquet"),
        "oof_predictions_file": file_reference(bundle / "oof_action_predictions.parquet"),
    }


def _validate_parent_contract(inspected: Mapping[str, Any]) -> None:
    request = inspected["request"]
    receipt = inspected["receipt"]
    continuous = receipt.get("continuous_policy") or {}
    training = request.get("training_spec") or {}
    if (
        request.get("schema_version") != "position_timing_action_value_request_v4"
        or receipt.get("schema_version") != "position_timing_action_value_receipt_v4"
        or continuous.get("policy_id") != "DAILY_ACTION_VALUE_POLICY_V2"
        or training.get("retrain") != "MONTH_END_EXPANDING"
        or int(training.get("bootstrap_block_sessions", 0)) != 25
        or int(training.get("bootstrap_samples", 0)) != 5000
        or float(training.get("economic_threshold_bps", float("nan"))) != 0.0
    ):
        raise ActionValueError("ENTRY_ONLY_PARENT_V4_CONTRACT_MISMATCH")


def prepare_entry_only_request(
    *, timing_root: Path, repository_root: Path, parent_bundle: Path
) -> Path:
    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    parent_bundle = parent_bundle.resolve()
    inspected = inspect_bundle(parent_bundle)
    _validate_parent_contract(inspected)
    parent_request = inspected["request"]
    symbols = tuple(parent_request["population_spec"]["selected_symbols"])
    source_identity = _daily_replay_source_identity(
        Path(parent_request["candidate_root"]), symbols
    )
    corporate_action_ref = file_reference(
        Path(parent_request["corporate_action_snapshot"]["path"])
    )
    if corporate_action_ref != parent_request["corporate_action_snapshot"]:
        raise ActionValueError("ENTRY_ONLY_CORPORATE_ACTION_SOURCE_CHANGED")
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "timing_root": timing_root.resolve().as_posix(),
        "parent_v4": _parent_reference(parent_bundle, inspected),
        "candidate_root": Path(parent_request["candidate_root"]).resolve().as_posix(),
        "selected_symbols": symbols,
        "population_spec": parent_request["population_spec"],
        "parent_source_sha256": inspected["receipt"]["source_sha256"],
        "parent_feature_spec_sha256": inspected["receipt"]["feature_spec_sha256"],
        "parent_policy_sha256": inspected["receipt"]["policy_sha256"],
        "daily_replay_source_identity": source_identity,
        "corporate_action_snapshot": corporate_action_ref,
        "study_contract": STUDY_CONTRACT,
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "candidate_policy_sha256": action_authority_policy_sha256(
            CORE_INFORMATION_BLOCK, ENTRY_ONLY_MODEL_ACTION_AUTHORITY
        ),
        "same_history_interpretation": RESULT_CLASS,
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
        timing_root.resolve()
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
        raise ActionValueError("ENTRY_ONLY_REQUEST_UNAVAILABLE") from exc
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
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("study_contract_sha256") != STUDY_CONTRACT_SHA256
        or canonical_sha256(request.get("study_contract")) != STUDY_CONTRACT_SHA256
        or request.get("same_history_interpretation") != RESULT_CLASS
        or any(request.get(flag) is not False for flag in false_flags)
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("ENTRY_ONLY_REQUEST_IDENTITY_MISMATCH")
    return request


def _validate_bound_parent(request: Mapping[str, Any]) -> tuple[Path, dict[str, Any]]:
    parent = request["parent_v4"]
    bundle = Path(parent["bundle_path"]).resolve()
    if file_reference(bundle / "manifest.json") != parent["manifest_file"]:
        raise ActionValueError("ENTRY_ONLY_PARENT_MANIFEST_CHANGED")
    inspected = inspect_bundle(bundle)
    _validate_parent_contract(inspected)
    if (
        inspected["manifest"]["manifest_sha256"] != parent["manifest_sha256"]
        or inspected["request"]["request_sha256"] != parent["request_sha256"]
        or inspected["receipt"]["receipt_sha256"] != parent["receipt_sha256"]
        or file_reference(bundle / "training_rows.parquet") != parent["training_rows_file"]
        or file_reference(bundle / "oof_action_predictions.parquet")
        != parent["oof_predictions_file"]
    ):
        raise ActionValueError("ENTRY_ONLY_PARENT_IDENTITY_MISMATCH")
    return bundle, inspected


def _normalized(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    if frame.empty or not set(columns).issubset(frame):
        raise ActionValueError("ENTRY_ONLY_REPLAY_SCHEMA_MISMATCH")
    if frame.duplicated(list(REPLAY_KEYS)).any():
        raise ActionValueError("ENTRY_ONLY_REPLAY_KEY_DUPLICATE")
    return frame.loc[:, list(columns)].sort_values(list(REPLAY_KEYS)).reset_index(drop=True)


def _assert_same_path_identity(candidate: pd.DataFrame, full: pd.DataFrame) -> dict[str, Any]:
    left = _normalized(candidate, BASELINE_IDENTITY_COLUMNS)
    right = _normalized(full, BASELINE_IDENTITY_COLUMNS)
    try:
        pd.testing.assert_frame_equal(left, right, check_dtype=False, check_exact=True)
    except AssertionError as exc:
        raise ActionValueError("ENTRY_ONLY_REPLAY_PATH_IDENTITY_MISMATCH") from exc
    return {
        "key_count": len(left),
        "sleeve_count": int(left["sleeve_id"].nunique()),
        "valuation_date_count": int(left["valuation_date"].nunique()),
        "baseline_identity_columns": BASELINE_IDENTITY_COLUMNS,
        "exact_match": True,
    }


def _assert_oof_equivalence(current: pd.DataFrame, parent: pd.DataFrame) -> dict[str, Any]:
    keys = ("symbol", "decision_as_of", "objective", "planned_delta_qty")
    columns = (*keys, "net_action_value_bps", "predicted_action_value_bps")
    if current.empty or parent.empty or not set(columns).issubset(current) or not set(columns).issubset(parent):
        raise ActionValueError("ENTRY_ONLY_OOF_SCHEMA_MISMATCH")
    left = current.loc[:, list(columns)].sort_values(list(keys)).reset_index(drop=True)
    right = parent.loc[:, list(columns)].sort_values(list(keys)).reset_index(drop=True)
    if not left.loc[:, list(keys)].astype(str).equals(right.loc[:, list(keys)].astype(str)):
        raise ActionValueError("ENTRY_ONLY_OOF_KEYS_MISMATCH")
    for column in ("net_action_value_bps", "predicted_action_value_bps"):
        if not np.allclose(
            pd.to_numeric(left[column]).to_numpy(float),
            pd.to_numeric(right[column]).to_numpy(float),
            rtol=0.0,
            atol=1e-10,
        ):
            raise ActionValueError("ENTRY_ONLY_OOF_VALUES_MISMATCH", field=column)
    return {"row_count": len(left), "keys_exact": True, "values_tolerance": 1e-10}


def _candidate_minus_full_daily(
    candidate: pd.DataFrame, full: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = ("sleeve_id", "valuation_date")
    left = (
        candidate.loc[candidate["baseline"].eq("BUY_AND_HOLD")]
        .sort_values(list(keys))
        .reset_index(drop=True)
    )
    right = (
        full.loc[full["baseline"].eq("BUY_AND_HOLD")]
        .sort_values(list(keys))
        .reset_index(drop=True)
    )
    if not left.loc[:, list(keys)].astype(str).equals(right.loc[:, list(keys)].astype(str)):
        raise ActionValueError("ENTRY_ONLY_FULL_V4_DIAGNOSTIC_KEYS_MISMATCH")
    difference = (
        pd.to_numeric(left["incremental_net_value_cny"]).to_numpy(float)
        - pd.to_numeric(right["incremental_net_value_cny"]).to_numpy(float)
    )
    daily = pd.DataFrame(
        {"valuation_date": left["valuation_date"].to_numpy(), "difference_cny": difference}
    )
    daily = daily.groupby("valuation_date", as_index=False).agg(
        incremental_net_value_cny=("difference_cny", "sum"),
        sleeve_count=("difference_cny", "size"),
    )
    daily["incremental_net_value_bps"] = (
        daily["incremental_net_value_cny"]
        / (daily["sleeve_count"] * float(REFERENCE_CAPITAL_CNY))
        * 10000.0
    )
    values = daily["incremental_net_value_bps"].to_numpy(float)
    candidate_actions = left["action"].astype(str)
    full_actions = right["action"].astype(str)
    return daily, {
        "interpretation": "DIAGNOSTIC_ONLY_NOT_A_TRIAL",
        "daily_mean_incremental_bps": float(values.mean()),
        "period_cumulative_incremental_bps": float(values.sum()),
        "effective_trading_days": len(values),
        "action_changed_count": int(candidate_actions.ne(full_actions).sum()),
        "action_changed_rate": float(candidate_actions.ne(full_actions).mean()),
        "interval": None,
    }


def _manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "request.json",
        "oof_action_predictions.parquet",
        "entry_only_sleeve_days.parquet",
        "full_v4_sleeve_days.parquet",
        "entry_only_daily_comparisons.parquet",
        "entry_only_minus_full_v4_daily.parquet",
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
    candidate_sleeves: pd.DataFrame,
    full_sleeves: pd.DataFrame,
    candidate_daily: pd.DataFrame,
    diagnostic_daily: pd.DataFrame,
) -> None:
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(
            staging / "request.json", canonical_json_bytes(request)
        )
        oof.to_parquet(staging / "oof_action_predictions.parquet", index=False)
        candidate_sleeves.to_parquet(staging / "entry_only_sleeve_days.parquet", index=False)
        full_sleeves.to_parquet(staging / "full_v4_sleeve_days.parquet", index=False)
        candidate_daily.to_parquet(
            staging / "entry_only_daily_comparisons.parquet", index=False
        )
        diagnostic_daily.to_parquet(
            staging / "entry_only_minus_full_v4_daily.parquet", index=False
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "receipt.json", canonical_json_bytes(receipt)
        )
        PositionTimingArtifactStore._publish_immutable(
            staging / "manifest.json", canonical_json_bytes(_manifest(staging, receipt))
        )
        lock = bundle.parents[3] / "locks" / f"entry-only-{bundle.name}.lock"
        with _exclusive_file_lock(lock):
            if bundle.exists():
                inspect_entry_only_bundle(bundle)
            else:
                os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def inspect_entry_only_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("ENTRY_ONLY_BUNDLE_UNAVAILABLE") from exc
    request = _load_request(bundle / "request.json")
    manifest_identity = {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    comparisons = (receipt.get("entry_only_policy") or {}).get("comparisons") or {}
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
        or receipt.get("selected_trial_count") != 0
        or set(comparisons) != {"BUY_AND_HOLD", "FROZEN_L1_V1"}
        or receipt.get("serving_status") != "NOT_SERVING_SAME_HISTORY_HYPOTHESIS_GENERATED"
    ):
        raise ActionValueError("ENTRY_ONLY_BUNDLE_IDENTITY_MISMATCH")
    for name, expected in manifest.get("files", {}).items():
        if _without_path(file_reference(bundle / name)) != expected:
            raise ActionValueError("ENTRY_ONLY_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    return {"manifest": manifest, "request": request, "receipt": receipt}


def run_entry_only_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("ENTRY_ONLY_CODE_IDENTITY_MISMATCH")
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / ARTIFACT_FOLDER / "bundles" / request["request_sha256"]
    if bundle.exists():
        return {
            "status": "ALREADY_MATERIALIZED",
            "bundle": bundle.as_posix(),
            **inspect_entry_only_bundle(bundle),
        }
    parent_bundle, parent = _validate_bound_parent(request)
    symbols = tuple(request["selected_symbols"])
    observed_source = _daily_replay_source_identity(Path(request["candidate_root"]), symbols)
    if observed_source != request["daily_replay_source_identity"]:
        raise ActionValueError("ENTRY_ONLY_DAILY_REPLAY_SOURCE_CHANGED")
    if file_reference(Path(request["corporate_action_snapshot"]["path"])) != request[
        "corporate_action_snapshot"
    ]:
        raise ActionValueError("ENTRY_ONLY_CORPORATE_ACTION_SOURCE_CHANGED")
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    corporate_actions = CorporateActionBook.open(
        Path(request["corporate_action_snapshot"]["path"])
    )
    rows = pd.read_parquet(parent_bundle / "training_rows.parquet")
    forward = walk_forward_action_values(
        rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=request["parent_source_sha256"],
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    oof_identity = _assert_oof_equivalence(
        forward.predictions,
        pd.read_parquet(parent_bundle / "oof_action_predictions.parquet"),
    )
    replay_args = {
        "models": forward.models,
        "symbols": symbols,
        "corporate_actions": corporate_actions,
        "bootstrap_samples": 5000,
        "block_sessions": 25,
        "seed": 20260908,
        "initial_holding_policy_id": EXOGENOUS_INITIAL_HOLDING_POLICY_ID,
    }
    full = replay_continuous_cohorts(candidate, **replay_args)
    entry_only = replay_continuous_cohorts(
        candidate,
        **replay_args,
        model_action_authority=ENTRY_ONLY_MODEL_ACTION_AUTHORITY,
    )
    same_path = _assert_same_path_identity(entry_only.sleeve_days, full.sleeve_days)
    diagnostic_daily, diagnostic = _candidate_minus_full_daily(
        entry_only.sleeve_days, full.sleeve_days
    )
    comparisons = entry_only.receipt["comparisons"]
    joint_supported = all(
        comparison["effect_evidence"] == "SUPPORTED"
        for comparison in comparisons.values()
    )
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "completed_at": datetime.now(TZ).isoformat(),
        "repository_commit": request["repository_commit"],
        "request_sha256": request["request_sha256"],
        "parent_v4_request_sha256": parent["request"]["request_sha256"],
        "parent_v4_receipt_sha256": parent["receipt"]["receipt_sha256"],
        "study_contract_sha256": STUDY_CONTRACT_SHA256,
        "candidate_policy_sha256": request["candidate_policy_sha256"],
        "result_class": RESULT_CLASS,
        "provenance_reason": PROVENANCE_REASON,
        "trial_count": 2,
        "planned_candidate_policy_count": 1,
        "familywise_hypothesis_count": 2,
        "selected_trial_count": 0,
        "joint_effect_evidence": "SUPPORTED" if joint_supported else "INCONCLUSIVE",
        "entry_only_policy": entry_only.receipt,
        "full_v4_control": full.receipt,
        "candidate_minus_full_v4_diagnostic": diagnostic,
        "oof_equivalence": oof_identity,
        "same_path_identity": same_path,
        "parent_continuous_path_reused": False,
        "parent_continuous_path_reason": (
            "PARENT_V4_PRECEDES_EXOGENOUS_INITIAL_HOLDING_CORRECTION;"
            "BOTH_NEW_POLICY_PATHS_USE_EXOGENOUS_ENDOWMENT"
        ),
        "model_training_identity": {
            "model_count": len(forward.models),
            "model_sha256": tuple(model.metadata["model_sha256"] for model in forward.models),
            "same_model_objects_used_by_candidate_and_full_v4": True,
            "parent_training_rows_file": request["parent_v4"]["training_rows_file"],
        },
        "serving_status": "NOT_SERVING_SAME_HISTORY_HYPOTHESIS_GENERATED",
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
        candidate_sleeves=entry_only.sleeve_days,
        full_sleeves=full.sleeve_days,
        candidate_daily=entry_only.daily_comparisons,
        diagnostic_daily=diagnostic_daily,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        **inspect_entry_only_bundle(bundle),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--parent-bundle", required=True, type=Path)
    run = commands.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = {
                "status": "PREPARED",
                "request": prepare_entry_only_request(
                    timing_root=args.timing_root,
                    repository_root=args.repository_root,
                    parent_bundle=args.parent_bundle,
                ).as_posix(),
            }
        elif args.command == "run":
            result = run_entry_only_request(args.request)
        else:
            result = {"status": "BUNDLE_VALID", **inspect_entry_only_bundle(args.bundle)}
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
    "inspect_entry_only_bundle",
    "prepare_entry_only_request",
    "run_entry_only_request",
]
