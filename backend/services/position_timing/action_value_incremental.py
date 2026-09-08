"""Immutable history-first comparison for one frozen optional information block.

PT-NEXT-006 compares core plus ATR14 against a newly trained matched core policy.
It has no API, card, alert, serving, database-write, or automatic-trading path.
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
    ATR14_FEATURE_SPEC_SHA256,
    ATR14_INFORMATION_BLOCK,
    ATR14_MARKET_FEATURES,
    CORE_INFORMATION_BLOCK,
    FEATURE_ORDER,
    TZ,
    ActionValueError,
    feature_contract,
    policy_sha256_for,
)
from .action_value_corporate_actions import CorporateActionBook, freeze_corporate_action_snapshot
from .action_value_data import DailyCandidate, file_reference
from .action_value_pipeline import _clean_repository_commit
from .action_value_research import (
    REFERENCE_CAPITAL_CNY,
    ActionValuePopulationSpec,
    circular_block_interval,
    classify_effect,
    build_action_value_rows,
    deterministic_symbols,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .artifact_store import PositionTimingArtifactStore, _exclusive_file_lock
from .contracts import canonical_json_bytes, canonical_sha256


PIPELINE_ID = "POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V1"
REQUEST_SCHEMA = "position_timing_action_value_increment_request_v1"
RECEIPT_SCHEMA = "position_timing_action_value_increment_receipt_v1"
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
) -> Path:
    """Freeze source-only coverage before any label or outcome is read."""

    repository_root = repository_root.resolve()
    source_commit = _clean_repository_commit(repository_root)
    candidate = DailyCandidate.open(candidate_root)
    spec = ActionValuePopulationSpec(
        start=population_start,
        end=population_end,
        symbol_limit=symbol_limit,
        review_stride=review_stride,
    )
    selected_symbols = deterministic_symbols(candidate.symbols, limit=symbol_limit, seed=spec.seed)
    source_coverage = candidate.coverage(
        selected_symbols,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    _require_matched_source_coverage(source_coverage)
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
    corporate_action_ref = file_reference(corporate_action_path)
    request = {
        "schema_version": REQUEST_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "created_at": datetime.now(TZ).isoformat(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "candidate_root": candidate.root.as_posix(),
        "timing_root": timing_root.resolve().as_posix(),
        "research_evidence_clock": "HISTORICAL_CAUSAL_REPLAY_PRIMARY_PROSPECTIVE_NONBLOCKING",
        "information_block": ATR14_INFORMATION_BLOCK,
        "hypothesis": "CORE_PLUS_ATR14_POLICY_MINUS_MATCHED_CORE_POLICY",
        "planned_trial_count": 1,
        "feature_contract": {
            "block_id": ATR14_INFORMATION_BLOCK,
            "added_features": tuple(name for name in ATR14_MARKET_FEATURES if name not in FEATURE_ORDER),
            "feature_order": feature_contract(ATR14_INFORMATION_BLOCK)[1],
            "feature_spec_sha256": ATR14_FEATURE_SPEC_SHA256,
            "policy_sha256": policy_sha256_for(ATR14_INFORMATION_BLOCK),
        },
        "matched_core_contract": {
            "information_block": CORE_INFORMATION_BLOCK,
            "feature_order": FEATURE_ORDER,
            "feature_spec_sha256": feature_contract(CORE_INFORMATION_BLOCK)[2],
            "policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
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
            "selection": "SHA256_SEED_SYMBOL_SOURCE_ONLY",
        },
        "training_spec": {
            "initial_sessions": 756,
            "retrain": "MONTH_END_EXPANDING",
            "estimator": "LIGHTGBM_GBDT_V2_ENTRY_EXIT_FROZEN",
            "bootstrap_block_sessions": BOOTSTRAP_BLOCK_SESSIONS,
            "bootstrap_samples": BOOTSTRAP_SAMPLES,
            "bootstrap_seed": BOOTSTRAP_SEED,
            "main_comparison": "CORE_PLUS_ATR14_MINUS_MATCHED_CORE",
            "interval_level": 0.95,
            "economic_threshold_bps": 0.0,
        },
        "source_coverage": source_coverage,
        "corporate_action_snapshot": corporate_action_ref,
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

    if _clean_repository_commit(Path(request["repository_root"])) != request["repository_commit"]:
        raise ActionValueError("ACTION_VALUE_CODE_IDENTITY_MISMATCH")
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    selected_symbols = tuple(request["population_spec"]["selected_symbols"])
    source_coverage = candidate.coverage(
        selected_symbols,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    if source_coverage != request["source_coverage"]:
        raise ActionValueError("INCREMENT_SOURCE_COVERAGE_IDENTITY_MISMATCH")
    _require_matched_source_coverage(source_coverage)
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
        information_block=ATR14_INFORMATION_BLOCK,
    )
    core_rows = augmented.rows.drop(columns=["atr14_sma_bps"])
    if tuple(core_rows.columns.intersection(FEATURE_ORDER)) != FEATURE_ORDER:
        raise ActionValueError("MATCHED_CORE_FEATURE_ORDER_MISMATCH")
    source_identity = {
        "source_coverage_sha256": source_coverage["source_sha256"],
        "corporate_action_snapshot_sha256": corporate_actions.snapshot_sha256,
    }
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
    atr_forward = walk_forward_action_values(
        augmented.rows,
        **common,
        information_block=ATR14_INFORMATION_BLOCK,
    )
    replay_args = {
        "candidate": candidate,
        "symbols": selected_symbols,
        "corporate_actions": corporate_actions,
        "bootstrap_samples": BOOTSTRAP_SAMPLES,
        "block_sessions": BOOTSTRAP_BLOCK_SESSIONS,
        "seed": BOOTSTRAP_SEED,
    }
    core_replay = replay_continuous_cohorts(
        models=core_forward.models,
        information_block=CORE_INFORMATION_BLOCK,
        **replay_args,
    )
    atr_replay = replay_continuous_cohorts(
        models=atr_forward.models,
        information_block=ATR14_INFORMATION_BLOCK,
        **replay_args,
    )
    paired_daily, comparison = _paired_policy_comparison(core_replay.sleeve_days, atr_replay.sleeve_days)
    coverage_support = bool(
        core_replay.receipt["coverage_can_support_policy"]
        and atr_replay.receipt["coverage_can_support_policy"]
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
        "information_block": ATR14_INFORMATION_BLOCK,
        "feature_spec_sha256": ATR14_FEATURE_SPEC_SHA256,
        "augmented_policy_sha256": policy_sha256_for(ATR14_INFORMATION_BLOCK),
        "matched_core_policy_sha256": policy_sha256_for(CORE_INFORMATION_BLOCK),
        "planned_trial_count": 1,
        "generated_trial_count": 1,
        "evaluated_trial_count": 1,
        "selected_trial_count": int(comparison["effect_evidence"] == "SUPPORTED"),
        "population": augmented.coverage,
        "core_walk_forward": core_forward.diagnostics,
        "atr14_walk_forward": atr_forward.diagnostics,
        "core_continuous": core_replay.receipt,
        "atr14_continuous": atr_replay.receipt,
        "incremental_comparison": comparison,
        "effect_evidence": comparison["effect_evidence"],
        "serving_status": "RESEARCH_ONLY_NO_RUNTIME_MODEL",
        "runtime_card_changed": False,
        "automatic_trading": False,
        "global_registry_written": False,
        "database_written": False,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    _publish_bundle(
        bundle,
        request=request,
        coverage={
            "source_coverage": source_coverage,
            "population_coverage": augmented.coverage,
            "source_identity": source_identity,
        },
        core_oof=core_forward.predictions,
        atr_oof=atr_forward.predictions,
        core_sleeves=core_replay.sleeve_days,
        atr_sleeves=atr_replay.sleeve_days,
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
) -> tuple[pd.DataFrame, dict[str, Any]]:
    keys = ["sleeve_id", "valuation_date"]
    core = core_sleeves.loc[
        core_sleeves["baseline"].eq("BUY_AND_HOLD"),
        [*keys, "policy_wealth_cny"],
    ].rename(columns={"policy_wealth_cny": "core_policy_wealth_cny"})
    atr = atr_sleeves.loc[
        atr_sleeves["baseline"].eq("BUY_AND_HOLD"),
        [*keys, "policy_wealth_cny"],
    ].rename(columns={"policy_wealth_cny": "atr14_policy_wealth_cny"})
    if core.duplicated(keys).any() or atr.duplicated(keys).any():
        raise ActionValueError("INCREMENT_POLICY_PATH_DUPLICATE")
    try:
        paired = core.merge(atr, on=keys, how="outer", validate="one_to_one", indicator=True)
    except pd.errors.MergeError as exc:
        raise ActionValueError("INCREMENT_POLICY_PATH_IDENTITY_MISMATCH") from exc
    if not paired["_merge"].eq("both").all():
        raise ActionValueError("INCREMENT_POLICY_PATH_IDENTITY_MISMATCH")
    paired = paired.drop(columns="_merge").sort_values(keys).reset_index(drop=True)
    paired["wealth_difference_cny"] = (
        paired["atr14_policy_wealth_cny"] - paired["core_policy_wealth_cny"]
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
        "estimand": "CORE_PLUS_ATR14_POLICY_MINUS_MATCHED_CORE_POLICY_DAILY_BPS",
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
    for name in ARTIFACT_FILES:
        expected = manifest["files"].get(name)
        observed = file_reference(bundle / name)
        observed.pop("path", None)
        if expected != observed:
            raise ActionValueError("INCREMENT_BUNDLE_FILE_IDENTITY_MISMATCH", file=name)
    _load_request(bundle / "request.json")
    receipt_identity = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    if receipt.get("schema_version") != RECEIPT_SCHEMA or receipt.get("receipt_sha256") != canonical_sha256(receipt_identity):
        raise ActionValueError("INCREMENT_RECEIPT_IDENTITY_MISMATCH")
    return {"manifest": manifest, "request": request, "receipt": receipt}


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
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(staging / "request.json", canonical_json_bytes(request))
        PositionTimingArtifactStore._publish_immutable(staging / "coverage.json", canonical_json_bytes(coverage))
        core_oof.to_parquet(staging / "core_oof_action_predictions.parquet", index=False)
        atr_oof.to_parquet(staging / "atr14_oof_action_predictions.parquet", index=False)
        core_sleeves.to_parquet(staging / "core_continuous_sleeve_days.parquet", index=False)
        atr_sleeves.to_parquet(staging / "atr14_continuous_sleeve_days.parquet", index=False)
        paired_daily.to_parquet(staging / "paired_daily_increment.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(staging / "receipt.json", canonical_json_bytes(receipt))
        files = {}
        for name in ARTIFACT_FILES:
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


def _deliver_registry(
    request: Mapping[str, Any],
    bundle: Path,
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    reference = file_reference(bundle / "receipt.json")
    evidence = EvidenceReferenceV1(
        role="position_timing_action_value_atr14_increment_receipt",
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
        experiment_id="position_timing_action_value_atr14_sma_gap_range_v1",
        attempt_id=request["request_sha256"][:24],
        research_stage="POSITION_TIMING_ACTION_VALUE_SINGLE_BLOCK_V1",
        study_type=ResearchStudyType.LEARNABILITY_AUDIT,
        hypothesis_family_id="POSITION_TIMING_ACTION_VALUE_OPTIONAL_BLOCK_V1",
        parent_lineage=("POSITION_TIMING_ADVICE_V1", "POSITION_TIMING_ACTION_VALUE_V4"),
        unique_variable=ATR14_INFORMATION_BLOCK,
        objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
        dataset_identity=receipt["source_sha256"],
        schema_identity=ATR14_FEATURE_SPEC_SHA256,
        policy_identity=policy_sha256_for(ATR14_INFORMATION_BLOCK),
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


def _require_matched_source_coverage(coverage: Mapping[str, Any]) -> None:
    if coverage.get("information_block") != ATR14_INFORMATION_BLOCK or coverage.get("outcomes_read") is not False:
        raise ActionValueError("INCREMENT_SOURCE_COVERAGE_CONTRACT_INVALID")
    feature_nonmissing = [
        (
            int(item["complete_core_sessions"]),
            int(item["complete_selected_feature_sessions"]),
            int(item["feature_nonmissing"]["atr14_sma_bps"]),
        )
        for item in coverage["coverage"].values()
    ]
    if any(selected != core or atr < core for core, selected, atr in feature_nonmissing):
        raise ActionValueError("INCREMENT_OPTIONAL_COVERAGE_LOSS")


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("INCREMENT_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    if (
        request.get("schema_version") != REQUEST_SCHEMA
        or request.get("pipeline_id") != PIPELINE_ID
        or request.get("information_block") != ATR14_INFORMATION_BLOCK
        or request.get("planned_trial_count") != 1
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("INCREMENT_REQUEST_IDENTITY_MISMATCH")
    return request


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
