from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import resource
import subprocess
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pandas as pd

from backend.services.advisory_model_first.candidate_group import build_runtime_equivalent_candidates
from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.diagnostics import build_parent_diagnostics
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.fresh_hmm import fit_fresh_sector_hmm
from backend.services.advisory_model_first.labels import build_five_day_labels, filter_labels_for_purged_split
from backend.services.advisory_model_first.model_bundle import publish_model_bundle
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource, sha256_file
from backend.services.advisory_model_first.qe_file_source import (
    STATIC_FACTOR_COLUMNS,
    all_qlib_instruments,
    initialize_qlib,
    load_qlib_daily,
    load_static_factors,
    load_suspend_rows,
    load_trading_calendar,
    validate_factor_file_schemas,
)
from backend.services.advisory_model_first.reranker_training import train_lambdarank
from backend.services.advisory_model_first.shared_feature_builder import build_advisory_feature_matrix
from backend.services.advisory_model_first.target_binding import FUND_LEG_ID, LSTM_LEG_ID
from backend.services.advisory_model_first.time_split import fixed_406_date_split


class TrainingProgress:
    def __init__(self, *, limit_bytes: int) -> None:
        self.limit_bytes = limit_bytes
        self.started = time.monotonic()
        self.stages: list[dict[str, Any]] = []

    def stage(self, name: str, started: float, **details: Any) -> None:
        peak = _peak_rss_bytes()
        receipt = {
            "stage": name,
            "wall_seconds": round(time.monotonic() - started, 3),
            "elapsed_seconds": round(time.monotonic() - self.started, 3),
            "peak_rss_bytes": peak,
            **details,
        }
        self.stages.append(receipt)
        print(json.dumps(receipt, ensure_ascii=True, sort_keys=True), flush=True)
        if peak > self.limit_bytes:
            raise AdvisoryModelFirstError(
                "model-first training exceeded the approved RSS limit",
                reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
                context={"stage": name, "peak_rss_bytes": peak, "limit_bytes": self.limit_bytes},
            )

    def report(self) -> dict[str, Any]:
        return {
            "peak_rss_bytes": _peak_rss_bytes(),
            "limit_bytes": self.limit_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def run_training_pipeline(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryTrainingRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    environment_report = _verify_training_environment(request)
    progress = TrainingProgress(limit_bytes=request.resource_max_rss_bytes)
    run_root = Path(request.output_root).resolve() / "runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    source = ExactPredictionSource(request.prediction_store_root)
    actual_descriptors = source.describe_all(
        run_id for runs in request.full_seed_roster.values() for run_id in runs
    )
    mismatches = []
    for run_id, expected in request.prediction_artifacts.items():
        actual = actual_descriptors.get(run_id)
        if actual is None or actual.model_dump(mode="json") != expected.model_dump(mode="json"):
            mismatches.append(run_id)
    if mismatches:
        raise AdvisoryModelFirstError(
            "Prediction Store artifacts changed after request freeze",
            reason_code="ADVISORY_MODEL_PREDICTION_HASH_MISMATCH",
            context={"run_ids": mismatches[:20], "mismatch_count": len(mismatches)},
        )
    combined_path = Path(request.combined_reference_path)
    if sha256_file(combined_path) != request.combined_reference_sha256:
        raise AdvisoryModelFirstError(
            "combined reference changed after request freeze",
            reason_code="ADVISORY_MODEL_REFERENCE_COMBINATION_MISMATCH",
            context={"path": str(combined_path)},
        )
    combined = pd.read_pickle(combined_path)
    decision_dates = pd.DatetimeIndex(
        pd.to_datetime(combined.index.get_level_values("datetime"))
    ).normalize().sort_values().unique()
    split = fixed_406_date_split(decision_dates)
    progress.stage("input_identity", started, prediction_artifact_count=len(actual_descriptors), decision_date_count=len(decision_dates))

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    calendar = load_trading_calendar("2024-03-01", request.data_cutoff)
    representative = {
        leg_id: source.load_scores(run_id, decision_dates=decision_dates, verify_artifact=False)
        for leg_id, run_id in request.representative_seed_run_ids.items()
    }
    identity = {
        "program_id": request.program_id,
        "binding_version_id": request.binding_version_id,
        "package_id": request.package_id,
        "manifest_sha256": request.manifest_sha256,
        "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
    }
    candidate_result = build_runtime_equivalent_candidates(
        leg_frames={LSTM_LEG_ID: representative[LSTM_LEG_ID], FUND_LEG_ID: representative[FUND_LEG_ID]},
        terminal_weights=request.terminal_weights,
        decision_dates=decision_dates,
        trading_calendar=calendar,
        identity=identity,
    )
    _write_parquet_atomic(candidate_result.candidates, run_root / "candidates.parquet")
    _write_json_atomic(candidate_result.coverage.to_dict("records"), run_root / "candidate_coverage.json")
    progress.stage(
        "runtime_equivalent_candidates",
        started,
        candidate_row_count=len(candidate_result.candidates),
        candidate_date_count=len(candidate_result.coverage),
        minimum_group_size=int(candidate_result.coverage["candidate_count"].min()),
    )

    started = time.monotonic()
    diagnostics = build_parent_diagnostics(
        source=source,
        full_seed_roster=request.full_seed_roster,
        decision_dates=decision_dates,
        current_candidates=candidate_result.candidates,
        combined_reference=combined,
        historical_weight_rows=request.historical_weight_rows,
    )
    _write_json_atomic(diagnostics, run_root / "parent_diagnostics.json")
    del combined, representative
    gc.collect()
    progress.stage(
        "parent_diagnostics",
        started,
        full_ensemble_status=diagnostics.get("full_ensemble_walk_forward_reference", {}).get("status"),
    )

    started = time.monotonic()
    schema_receipt = validate_factor_file_schemas(request.factor_data_root, data_cutoff=request.data_cutoff)
    candidate_symbols = sorted(candidate_result.candidates["instrument"].unique().tolist())
    history_start = "2024-03-01"
    candidate_daily = load_qlib_daily(
        candidate_symbols,
        start=history_start,
        end=request.data_cutoff,
    )
    candidate_static = load_static_factors(
        request.factor_data_root,
        columns=STATIC_FACTOR_COLUMNS,
        start=history_start,
        end=request.data_cutoff,
        instruments=candidate_symbols,
    )
    market_daily = load_qlib_daily(
        all_qlib_instruments(),
        start=history_start,
        end=request.data_cutoff,
        fields=("$close", "$limit_up"),
    )
    benchmark_daily = load_qlib_daily(
        ["000300.SH"],
        start=history_start,
        end=request.data_cutoff,
        fields=("$open", "$close"),
    )
    static_all = load_static_factors(
        request.factor_data_root,
        columns=("l2_code_id", "sw2_close", "sw2_amount"),
        start=history_start,
        end=request.data_cutoff,
    )
    suspend_rows = load_suspend_rows(
        request.suspend_data_root,
        start=history_start,
        end=request.data_cutoff,
        instruments=candidate_symbols,
    )
    progress.stage(
        "file_market_projection",
        started,
        candidate_symbol_count=len(candidate_symbols),
        candidate_daily_rows=len(candidate_daily),
        market_daily_rows=len(market_daily),
        static_candidate_rows=len(candidate_static),
        static_all_rows=len(static_all),
    )

    started = time.monotonic()
    hmm_result = fit_fresh_sector_hmm(
        static_all=static_all,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
        trading_calendar=calendar,
        train_dates=split.train,
        continuation_cutoff=request.hmm_continuation_cutoff,
    )
    progress.stage(
        "fresh_hmm",
        started,
        model_count=len(hmm_result.models["models"]),
        unavailable_count=len(hmm_result.unavailable),
        state_row_count=len(hmm_result.states),
    )

    started = time.monotonic()
    feature_result = build_advisory_feature_matrix(
        candidates=candidate_result.candidates,
        candidate_daily=candidate_daily,
        candidate_static=candidate_static,
        market_daily=market_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        hmm_states=hmm_result.states,
    )
    _write_parquet_atomic(feature_result.features, run_root / "features.parquet")
    _write_json_atomic(feature_result.coverage.to_dict("records"), run_root / "feature_coverage.json")
    progress.stage(
        "shared_features",
        started,
        feature_row_count=len(feature_result.features),
        modelable_date_count=int((feature_result.coverage["status"] == "available").sum()),
        unavailable_date_count=int((feature_result.coverage["status"] != "available").sum()),
    )

    started = time.monotonic()
    label_result = build_five_day_labels(
        candidates=candidate_result.candidates,
        daily=candidate_daily,
        benchmark_daily=benchmark_daily,
        suspend_rows=suspend_rows,
        trading_calendar=calendar,
    )
    labels = filter_labels_for_purged_split(label_result.labels, split, data_cutoff=request.data_cutoff)
    _write_parquet_atomic(labels, run_root / "labels.parquet")
    _write_json_atomic(label_result.coverage.to_dict("records"), run_root / "label_coverage.json")
    progress.stage(
        "labels",
        started,
        label_row_count=len(labels),
        modelable_date_count=int((label_result.coverage["status"] == "available").sum()),
    )

    del static_all
    gc.collect()
    started = time.monotonic()
    training_result = train_lambdarank(
        features=feature_result.features,
        labels=labels,
        split=split,
    )
    progress.stage(
        "lambdarank",
        started,
        best_iteration=training_result.metrics["best_iteration"],
        test_date_count=training_result.metrics["test_date_count"],
        test_row_count=training_result.metrics["test_row_count"],
    )

    started = time.monotonic()
    resource_report = progress.report()
    bundle_id, bundle_path, manifest = publish_model_bundle(
        model_root=request.output_root,
        request=request,
        split=split,
        hmm_models=hmm_result.models,
        hmm_unavailable=hmm_result.unavailable,
        training=training_result,
        diagnostics=diagnostics,
        schema_receipt=asdict(schema_receipt),
        environment_report=environment_report,
        resource_report=resource_report,
    )
    progress.stage("bundle_publish", started, bundle_id=bundle_id, bundle_path=str(bundle_path))
    receipt = {
        "status": "trained",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "metrics": training_result.metrics,
        "baseline_comparison": training_result.baseline_comparison,
        "resource_report": progress.report(),
        "shadow_binding_activated": False,
    }
    _write_json_atomic(receipt, run_root / "training_receipt.json")
    return receipt


def _verify_training_environment(request: FrozenAdvisoryTrainingRequestV1) -> dict[str, Any]:
    release = platform.release().lower()
    if os.name == "nt" or "microsoft" not in release:
        raise AdvisoryModelFirstError(
            "model-first training must run inside WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"os_name": os.name, "platform_release": platform.release()},
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        raise AdvisoryModelFirstError(
            "model-first training requires the rdagent-gpu Conda environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"conda_env": conda_env or None},
        )
    repository_root = Path(request.repository_root).resolve()
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    actual_commit = result.stdout.strip().lower()
    if actual_commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "training repository commit does not match the frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"expected_commit": request.repository_commit, "actual_commit": actual_commit},
        )
    roots = {
        "qlib_daily_root": request.qlib_daily_root,
        "factor_data_root": request.factor_data_root,
        "suspend_data_root": request.suspend_data_root,
        "prediction_store_root": request.prediction_store_root,
        "repository_root": request.repository_root,
    }
    missing = {name: value for name, value in roots.items() if not Path(value).exists()}
    if missing:
        raise AdvisoryModelFirstError(
            "one or more explicit training roots do not exist",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_roots": missing},
        )
    return {
        "platform_release": platform.release(),
        "python_version": platform.python_version(),
        "conda_environment": conda_env,
        "lightgbm_version": importlib.metadata.version("lightgbm"),
        "hmmlearn_version": importlib.metadata.version("hmmlearn"),
        "pyarrow_version": importlib.metadata.version("pyarrow"),
    }


def _peak_rss_bytes() -> int:
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def _write_json_atomic(payload: Any, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=True, sort_keys=True, default=_json_default), encoding="utf-8")
    tmp.replace(path)


def _write_parquet_atomic(frame: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(tmp, index=False)
    tmp.replace(path)


def _json_default(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")
