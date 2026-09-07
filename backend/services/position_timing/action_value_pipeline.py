"""Immutable CLI pipeline for daily action-value historical research.

The pipeline is intentionally local and explicit: prepare a frozen request,
run monthly forward evaluation, publish a native LightGBM final-fit bundle, and
optionally publish a supported serving pointer.  No API request trains a model.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from typing import Any, Mapping, Sequence

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

from .action_value import ActionValueError, FEATURE_SPEC_SHA256, POLICY_SHA256, TZ, cutoff_on
from .action_value_data import DailyCandidate, file_reference
from .action_value_corporate_actions import (
    CorporateActionBook,
    freeze_corporate_action_snapshot,
)
from .action_value_execution_audit import audit_minute_execution
from .action_value_model import fit_local_model, write_local_model
from .action_value_research import (
    ActionValuePopulationSpec,
    build_action_value_rows,
    deterministic_symbols,
    replay_continuous_cohorts,
    walk_forward_action_values,
)
from .artifact_store import PositionTimingArtifactStore
from .contracts import canonical_json_bytes, canonical_sha256


REQUEST_SCHEMA = "position_timing_action_value_request_v3"
LEGACY_REQUEST_SCHEMA = "position_timing_action_value_request_v2"
RECEIPT_SCHEMA = "position_timing_action_value_receipt_v3"
PIPELINE_ID = "POSITION_TIMING_ACTION_VALUE_V2"


def prepare_request(
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
        "candidate_root": candidate.root.as_posix(),
        "minute_root": (candidate.root / "components" / "minute_bin_candidate").as_posix(),
        "timing_root": timing_root.resolve().as_posix(),
        "repository_root": repository_root.as_posix(),
        "repository_commit": source_commit,
        "research_evidence_clock": "HISTORICAL_CAUSAL_REPLAY_PRIMARY_PROSPECTIVE_NONBLOCKING",
        "created_at": datetime.now(TZ).isoformat(),
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
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "policy_sha256": POLICY_SHA256,
        "training_spec": {
            "initial_sessions": 756,
            "retrain": "MONTH_END_EXPANDING",
            "bootstrap_block_sessions": 25,
            "bootstrap_samples": 5000,
            "bootstrap_seed": 20260907,
            "main_comparisons": ("BUY_AND_HOLD", "FROZEN_L1_V1"),
            "simultaneous_alpha": "0.05/2",
            "economic_threshold_bps": 0.0,
        },
        "source_refs": {
            **candidate.references,
            "minute_meta": file_reference(candidate.root / "components" / "minute_bin_candidate" / "meta_export.json"),
            "minute_calendar": file_reference(candidate.root / "components" / "minute_bin_candidate" / "calendars" / "1min.txt"),
            "minute_instruments": file_reference(candidate.root / "components" / "minute_bin_candidate" / "instruments" / "all.txt"),
            "corporate_action_snapshot": corporate_action_ref,
        },
        "corporate_action_snapshot": corporate_action_ref,
        "historical_registry": file_reference(historical_registry),
        "historical_registry_context_count": len(AdvisoryResearchTrialRegistryV1(historical_registry).read()),
        "global_registry_write": False,
        "database_write": False,
        "order_write": False,
    }
    request["request_sha256"] = canonical_sha256(request)
    path = timing_root.resolve() / "research" / "action_value_v2" / "requests" / f"{request['request_sha256']}.json"
    PositionTimingArtifactStore._publish_immutable(path, canonical_json_bytes(request))
    return path


def run_request(request_path: Path) -> dict[str, Any]:
    request = _load_request(request_path)
    timing_root = Path(request["timing_root"]).resolve()
    bundle = timing_root / "research" / "action_value_v2" / "bundles" / request["request_sha256"]
    historical_registry = Path(request["historical_registry"]["path"])
    global_before = file_reference(historical_registry)
    if bundle.exists():
        loaded = inspect_bundle(bundle)
        delivered = _deliver_completed_bundle(
            request=request,
            bundle=bundle,
            receipt=loaded["receipt"],
            global_before=global_before,
        )
        return {"status": "ALREADY_MATERIALIZED", "bundle": bundle.as_posix(), **loaded, **delivered}
    _assert_repository_identity(request)
    candidate = DailyCandidate.open(Path(request["candidate_root"]))
    corporate_action_ref = request.get("corporate_action_snapshot")
    if not isinstance(corporate_action_ref, Mapping) or "path" not in corporate_action_ref:
        raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_NOT_BOUND")
    corporate_action_path = Path(str(corporate_action_ref["path"]))
    if file_reference(corporate_action_path) != corporate_action_ref:
        raise ActionValueError("CORPORATE_ACTION_SNAPSHOT_SOURCE_CHANGED")
    corporate_actions = CorporateActionBook.open(corporate_action_path)
    raw_spec = request["population_spec"]
    spec = ActionValuePopulationSpec(
        start=date.fromisoformat(raw_spec["start"]),
        end=date.fromisoformat(raw_spec["end"]),
        symbol_limit=int(raw_spec["symbol_limit"]),
        review_stride=int(raw_spec["review_stride"]),
        seed=int(raw_spec["seed"]),
    )
    population = build_action_value_rows(candidate, spec, corporate_actions=corporate_actions)
    if tuple(population.coverage["symbols"]) != tuple(raw_spec["selected_symbols"]):
        raise ActionValueError("POPULATION_SELECTION_IDENTITY_MISMATCH")
    source_sha256 = canonical_sha256({
        "request_sources": request["source_refs"],
        "coverage": population.coverage,
    })
    forward = walk_forward_action_values(
        population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=source_sha256,
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
    )
    replay = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=tuple(raw_spec["selected_symbols"]),
        corporate_actions=corporate_actions,
        bootstrap_samples=int(request["training_spec"]["bootstrap_samples"]),
        block_sessions=int(request["training_spec"]["bootstrap_block_sessions"]),
        seed=int(request["training_spec"]["bootstrap_seed"]),
    )
    execution_audit = audit_minute_execution(
        replay.sleeve_days,
        minute_root=Path(request["minute_root"]),
    )
    final_cutoff = cutoff_on(spec.end)
    final_model = fit_local_model(
        population.rows,
        cutoff=final_cutoff,
        available_at=datetime.now(TZ),
        source_sha256=source_sha256,
        request_sha256=request["request_sha256"],
        source_commit=request["repository_commit"],
        temporal_mode="LIVE_FINAL_FIT",
    )
    model_folder = write_local_model(final_model, timing_root=timing_root)

    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "pipeline_id": PIPELINE_ID,
        "request_sha256": request["request_sha256"],
        "repository_commit": request["repository_commit"],
        "completed_at": datetime.now(TZ).isoformat(),
        "source_sha256": source_sha256,
        "feature_spec_sha256": FEATURE_SPEC_SHA256,
        "policy_sha256": POLICY_SHA256,
        "population": population.coverage,
        "walk_forward": forward.diagnostics,
        "continuous_policy": replay.receipt,
        "execution_realism": execution_audit,
        "final_model_sha256": final_model.metadata["model_sha256"],
        "final_model_path": model_folder.as_posix(),
        "effect_evidence": replay.receipt["study_effect_evidence"],
        "serving_status": (
            "SUPPORTED_NOT_PUBLISHED" if replay.receipt["study_effect_evidence"] == "SUPPORTED"
            else "EXPERIMENTAL_MODEL_ADVICE_ONLY"
        ),
        "runtime_card_changed": False,
        "automatic_trading": False,
        "global_registry_written": False,
        "global_registry_request_era": request["historical_registry"],
        "global_registry_run_observed_before": global_before,
        "global_registry_run_observed_after_compute": file_reference(historical_registry),
        "database_written": False,
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt)
    bundle.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{bundle.name}.", dir=bundle.parent))
    try:
        PositionTimingArtifactStore._publish_immutable(staging / "request.json", canonical_json_bytes(request))
        PositionTimingArtifactStore._publish_immutable(staging / "coverage.json", canonical_json_bytes(population.coverage))
        population.rows.to_parquet(staging / "training_rows.parquet", index=False)
        forward.predictions.to_parquet(staging / "oof_action_predictions.parquet", index=False)
        replay.sleeve_days.to_parquet(staging / "continuous_sleeve_days.parquet", index=False)
        replay.daily_comparisons.to_parquet(staging / "daily_comparisons.parquet", index=False)
        PositionTimingArtifactStore._publish_immutable(
            staging / "execution_realism.json", canonical_json_bytes(execution_audit)
        )
        PositionTimingArtifactStore._publish_immutable(staging / "receipt.json", canonical_json_bytes(receipt))
        manifest = _bundle_manifest(staging, receipt)
        PositionTimingArtifactStore._publish_immutable(staging / "manifest.json", canonical_json_bytes(manifest))
        os.replace(staging, bundle)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    loaded = inspect_bundle(bundle)
    delivered = _deliver_completed_bundle(
        request=request,
        bundle=bundle,
        receipt=loaded["receipt"],
        global_before=global_before,
    )
    return {
        "status": "MATERIALIZED",
        "bundle": bundle.as_posix(),
        "receipt": receipt,
        **delivered,
    }


def _deliver_completed_bundle(
    *,
    request: Mapping[str, Any],
    bundle: Path,
    receipt: Mapping[str, Any],
    global_before: Mapping[str, Any],
) -> dict[str, Any]:
    """Repair exact retries without ever touching the global N0 control plane."""

    historical_registry = Path(request["historical_registry"]["path"])
    timing_root = Path(request["timing_root"]).resolve()
    registry = _deliver_registry(request=request, bundle=bundle, receipt=receipt)
    global_after_registry = file_reference(historical_registry)
    current = {
        "schema_version": "position_timing_action_value_current_research_v2",
        "model_sha256": receipt["final_model_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "bundle_path": bundle.as_posix(),
        "effect_evidence": receipt["effect_evidence"],
        "advice_tier": "EXPERIMENTAL_MODEL_ADVICE",
        "updated_at": receipt["completed_at"],
    }
    current["state_sha256"] = canonical_sha256(current)
    PositionTimingArtifactStore._atomic_replace(
        timing_root / "research" / "action_value_v2" / "current.json",
        canonical_json_bytes(current),
    )
    global_after = file_reference(historical_registry)
    return {
        "registry": registry,
        "current_research": current,
        "global_registry_observation": {
            "before": global_before,
            "after_own_registry_delivery": global_after_registry,
            "after_current_pointer_delivery": global_after,
            "this_pipeline_writes_global_registry": False,
            "concurrent_change_observed": global_after != global_before,
        },
    }


def publish_serving(bundle: Path, *, timing_root: Path) -> dict[str, Any]:
    loaded = inspect_bundle(bundle)
    receipt = loaded["receipt"]
    if receipt["effect_evidence"] != "SUPPORTED":
        raise ActionValueError("MODEL_POLICY_NOT_SUPPORTED")
    state = {
        "schema_version": "position_timing_serving_policy_v2",
        "policy_id": "DAILY_ACTION_VALUE_POLICY_V2",
        "model_sha256": receipt["final_model_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "bundle_path": bundle.resolve().as_posix(),
        "evidence_tier": "MODEL_ASSISTED",
        "published_at": datetime.now(TZ).isoformat(),
        "effective_card_policy": "NEXT_UNSIGNED_CARD_ONLY",
    }
    state["state_sha256"] = canonical_sha256(state)
    path = timing_root.resolve() / "serving_policy_v2" / "current.json"
    PositionTimingArtifactStore._atomic_replace(path, canonical_json_bytes(state))
    return state


def inspect_bundle(bundle: Path) -> dict[str, Any]:
    bundle = bundle.resolve()
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
        receipt = json.loads((bundle / "receipt.json").read_text(encoding="utf-8"))
        request = json.loads((bundle / "request.json").read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError) as exc:
        raise ActionValueError("ACTION_VALUE_BUNDLE_UNAVAILABLE", cause=type(exc).__name__) from exc
    if receipt.get("receipt_sha256") != canonical_sha256(
        {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    ):
        raise ActionValueError("ACTION_VALUE_RECEIPT_IDENTITY_MISMATCH")
    expected = _bundle_manifest(bundle, receipt)
    if manifest != expected:
        raise ActionValueError("ACTION_VALUE_BUNDLE_MANIFEST_MISMATCH")
    if request.get("request_sha256") != receipt.get("request_sha256"):
        raise ActionValueError("ACTION_VALUE_REQUEST_RECEIPT_MISMATCH")
    return {"receipt": receipt, "manifest": manifest, "request": request}


def load_current_research(timing_root: Path) -> dict[str, Any] | None:
    path = timing_root.resolve() / "research" / "action_value_v2" / "current.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("ACTION_VALUE_CURRENT_STATE_INVALID") from exc
    identity = {key: value for key, value in payload.items() if key != "state_sha256"}
    if payload.get("state_sha256") != canonical_sha256(identity):
        raise ActionValueError("ACTION_VALUE_CURRENT_STATE_IDENTITY_MISMATCH")
    return payload


def _load_request(path: Path) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ActionValueError("ACTION_VALUE_REQUEST_UNAVAILABLE") from exc
    identity = {key: value for key, value in request.items() if key != "request_sha256"}
    if (
        request.get("schema_version") not in {REQUEST_SCHEMA, LEGACY_REQUEST_SCHEMA}
        or request.get("request_sha256") != canonical_sha256(identity)
    ):
        raise ActionValueError("ACTION_VALUE_REQUEST_IDENTITY_MISMATCH")
    return request


def _assert_repository_identity(request: Mapping[str, Any]) -> None:
    repository = Path(request["repository_root"])
    if _clean_repository_commit(repository) != request["repository_commit"]:
        raise ActionValueError("ACTION_VALUE_CODE_IDENTITY_MISMATCH")


def _clean_repository_commit(repository: Path) -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip()
    if status:
        raise ActionValueError("REPOSITORY_NOT_CLEAN")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repository, check=True, capture_output=True, text=True
    ).stdout.strip().lower()
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise ActionValueError("MODEL_CODE_IDENTITY_INVALID")
    return commit


def _bundle_manifest(root: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    names = (
        "request.json",
        "coverage.json",
        "training_rows.parquet",
        "oof_action_predictions.parquet",
        "continuous_sleeve_days.parquet",
        "daily_comparisons.parquet",
        "execution_realism.json",
        "receipt.json",
    )
    files = {name: file_reference(root / name) for name in names}
    # Bundle identity is content, not the staging/final absolute directory.
    for value in files.values():
        value.pop("path", None)
    manifest = {
        "schema_version": (
            "position_timing_action_value_bundle_v3"
            if receipt.get("schema_version") == RECEIPT_SCHEMA
            else "position_timing_action_value_bundle_v2"
        ),
        "request_sha256": receipt["request_sha256"],
        "receipt_sha256": receipt["receipt_sha256"],
        "files": files,
    }
    manifest["manifest_sha256"] = canonical_sha256(manifest)
    return manifest


def _deliver_registry(*, request: Mapping[str, Any], bundle: Path, receipt: Mapping[str, Any]) -> dict[str, Any]:
    receipt_path = bundle / "receipt.json"
    ref = file_reference(receipt_path)
    evidence = EvidenceReferenceV1(
        role=(
            "position_timing_action_value_v3_receipt"
            if receipt.get("schema_version") == RECEIPT_SCHEMA
            else "position_timing_action_value_v2_receipt"
        ),
        artifact_uri=receipt_path.as_posix(),
        sha256=ref["sha256"],
        size_bytes=ref["size_bytes"],
    )
    joint_supported = receipt["effect_evidence"] == "SUPPORTED"
    records = []
    for baseline, comparison in receipt["continuous_policy"]["comparisons"].items():
        effect = comparison["effect_evidence"]
        if effect == "SUPPORTED":
            result_class, decision_use = ResearchResultClass.CONTROL_READY, DecisionUse.DIRECTION_GATE
        elif effect == "NEGATIVE":
            result_class, decision_use = ResearchResultClass.NEGATIVE, DecisionUse.DIRECTION_GATE
        else:
            result_class, decision_use = ResearchResultClass.EXPLORATORY, DecisionUse.NAVIGATION_ONLY
        records.append(
            build_trial_record(
                experiment_id=f"position_timing_action_value_v2_{baseline.lower()}",
                attempt_id=request["request_sha256"][:24],
                research_stage="POSITION_TIMING_ACTION_VALUE_V2",
                study_type=ResearchStudyType.LEARNABILITY_AUDIT,
                hypothesis_family_id="POSITION_TIMING_ACTION_VALUE_V2_TWO_BASELINES",
                parent_lineage=("POSITION_TIMING_ADVICE_V1", request["request_sha256"]),
                unique_variable=baseline,
                objective_contract=ObjectiveContract.RISK_MANAGED_ADVISORY,
                dataset_identity=receipt["source_sha256"],
                schema_identity=FEATURE_SPEC_SHA256,
                policy_identity=POLICY_SHA256,
                planned_trial_count=1,
                generated_trial_count=1,
                evaluated_trial_count=1,
                # The two records are baseline comparisons for one frozen policy.
                # Anchor the joint selection on exactly one canonical comparison so
                # registry consumers never count one policy twice.
                selected_trial_count=int(joint_supported and baseline == "BUY_AND_HOLD"),
                consumed_windows=(
                    ConsumedWindowV1(
                        window_id="POSITION_TIMING_ACTION_VALUE_V2_FORWARD",
                        dataset_identity=receipt["source_sha256"],
                        start_date=date.fromisoformat(request["population_spec"]["start"]),
                        end_date=date.fromisoformat(request["population_spec"]["end"]),
                    ),
                ),
                result_class=result_class,
                decision_use=decision_use,
                evidence_refs=(evidence,),
            )
        )
    registry = Path(request["timing_root"]) / "research_registry" / "timing_trial_registry_v1.jsonl"
    return AdvisoryResearchTrialRegistryV1(registry).append_batch(records)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--candidate-root", required=True, type=Path)
    prepare.add_argument("--timing-root", required=True, type=Path)
    prepare.add_argument("--repository-root", required=True, type=Path)
    prepare.add_argument("--historical-registry", required=True, type=Path)
    prepare.add_argument("--population-start", required=True, type=_parse_date)
    prepare.add_argument("--population-end", required=True, type=_parse_date)
    prepare.add_argument("--symbol-limit", type=int, default=64)
    prepare.add_argument("--review-stride", type=int, default=10)
    run = sub.add_parser("run")
    run.add_argument("--request", required=True, type=Path)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--bundle", required=True, type=Path)
    publish = sub.add_parser("publish-serving")
    publish.add_argument("--bundle", required=True, type=Path)
    publish.add_argument("--timing-root", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            from dotenv import load_dotenv

            load_dotenv(args.repository_root.resolve() / ".env", override=False)
            result = {"status": "PREPARED", "request": prepare_request(
                candidate_root=args.candidate_root,
                timing_root=args.timing_root,
                repository_root=args.repository_root,
                historical_registry=args.historical_registry,
                population_start=args.population_start,
                population_end=args.population_end,
                symbol_limit=args.symbol_limit,
                review_stride=args.review_stride,
            ).as_posix()}
        elif args.command == "run":
            result = run_request(args.request)
        elif args.command == "inspect":
            result = {"status": "BUNDLE_VALID", **inspect_bundle(args.bundle)}
        else:
            result = {"status": "PUBLISHED", "serving": publish_serving(args.bundle, timing_root=args.timing_root)}
    except ActionValueError as exc:
        print(json.dumps({"status": "FAILED", "error_code": exc.code, "details": exc.details}, ensure_ascii=False))
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "inspect_bundle",
    "load_current_research",
    "main",
    "prepare_request",
    "publish_serving",
    "run_request",
]
