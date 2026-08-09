from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import Any

import pandas as pd

from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH, MODEL_FEATURE_COLUMNS
from backend.services.advisory_model_first.model_bundle import _read_and_validate_bundle
from backend.services.advisory_model_first.outcome_bundle import publish_outcome_bundle
from backend.services.advisory_model_first.outcome_contracts import FrozenAdvisoryOutcomeTrainingRequestV1
from backend.services.advisory_model_first.outcome_labels import (
    apply_outcome_split,
    build_multi_horizon_outcome_labels,
)
from backend.services.advisory_model_first.outcome_split import fixed_406_outcome_split
from backend.services.advisory_model_first.outcome_training import train_outcome_models
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_suspend_rows,
    load_trading_calendar,
)


class OutcomeTrainingProgress:
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
                "outcome training exceeded the approved RSS limit",
                reason_code="ADVISORY_MODEL_TRAINING_MEMORY_LIMIT_EXCEEDED",
                context={
                    "stage": name,
                    "peak_rss_bytes": peak,
                    "limit_bytes": self.limit_bytes,
                },
            )

    def report(self) -> dict[str, Any]:
        return {
            "peak_rss_bytes": _peak_rss_bytes(),
            "limit_bytes": self.limit_bytes,
            "total_wall_seconds": round(time.monotonic() - self.started, 3),
            "stages": self.stages,
        }


def run_outcome_training_pipeline(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryOutcomeTrainingRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    environment_report = _verify_outcome_training_environment(request)
    progress = OutcomeTrainingProgress(limit_bytes=request.resource_max_rss_bytes)
    run_root = Path(request.output_root).resolve() / "outcome_runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    parent_bundle_path = Path(request.parent_feature_schema_path).resolve().parent
    expected_parent_members = {
        Path(request.parent_training_request_path).resolve(): "training_request.json",
        Path(request.parent_feature_schema_path).resolve(): "feature_schema.json",
        Path(request.parent_test_predictions_artifact.path).resolve(): "test_predictions.parquet",
    }
    if any(path.parent != parent_bundle_path for path in expected_parent_members):
        raise AdvisoryModelFirstError(
            "outcome parent artifacts do not share one exact bundle root",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"parent_bundle_path": str(parent_bundle_path)},
        )
    try:
        parent_manifest, feature_schema, _hmm_models, _baselines = _read_and_validate_bundle(
            parent_bundle_path,
            expected_bundle_id=request.parent_bundle_id,
            expected_manifest_file_sha256=request.parent_bundle_manifest_file_sha256,
        )
    except AdvisoryModelFirstError as exc:
        raise AdvisoryModelFirstError(
            "outcome parent model bundle failed exact readback",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"parent_reason_code": exc.reason_code},
        ) from exc
    parent_files = parent_manifest.get("files") or {}
    parent_test_descriptor = parent_files.get("test_predictions.parquet") or {}
    if (
        parent_test_descriptor.get("sha256") != request.parent_test_predictions_artifact.sha256
        or parent_test_descriptor.get("size_bytes")
        != request.parent_test_predictions_artifact.size_bytes
    ):
        raise AdvisoryModelFirstError(
            "outcome parent test predictions differ from the parent bundle manifest",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
        )
    parent_request = FrozenAdvisoryTrainingRequestV1.model_validate_json(
        Path(request.parent_training_request_path).read_text(encoding="utf-8")
    )
    if (
        parent_request.request_id != request.parent_request_id
        or parent_request.request_sha256 != request.parent_request_sha256
        or parent_request.package_id != request.package_id
        or parent_request.manifest_sha256 != request.manifest_sha256
        or parent_request.feature_schema_version != request.feature_schema_version
    ):
        raise AdvisoryModelFirstError(
            "outcome parent training request identity is inconsistent",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
        )
    if (
        feature_schema.get("feature_schema_hash") != request.feature_schema_hash
        or request.feature_schema_hash != FEATURE_SCHEMA_HASH
        or tuple(feature_schema.get("trained_feature_names") or ()) != tuple(MODEL_FEATURE_COLUMNS)
    ):
        raise AdvisoryModelFirstError(
            "outcome parent feature schema identity is inconsistent",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
        )
    candidates = _read_bound_parquet(request.candidates_artifact)
    features = _read_bound_parquet(request.features_artifact)
    parent_test_predictions = _read_bound_parquet(request.parent_test_predictions_artifact)
    decision_dates = pd.DatetimeIndex(
        pd.to_datetime(candidates["decision_as_of_trade_date"])
    ).normalize().sort_values().unique()
    split = fixed_406_outcome_split(decision_dates)
    if (
        decision_dates[0].date().isoformat() != request.decision_date_start
        or decision_dates[-1].date().isoformat() != request.decision_date_end
    ):
        raise AdvisoryModelFirstError(
            "outcome candidate decision range differs from its request",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
        )
    progress.stage(
        "outcome_input_identity",
        started,
        candidate_row_count=len(candidates),
        feature_row_count=len(features),
        decision_date_count=len(decision_dates),
    )

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    calendar = load_trading_calendar("2024-03-01", request.data_cutoff)
    symbols = sorted(candidates["instrument"].astype(str).unique().tolist())
    daily = load_qlib_daily(symbols, start="2024-03-01", end=request.data_cutoff)
    benchmark = load_qlib_daily(
        ["000300.SH"],
        start="2024-03-01",
        end=request.data_cutoff,
        fields=("$open", "$close"),
    )
    suspend = load_suspend_rows(
        request.suspend_data_root,
        start="2024-03-01",
        end=request.data_cutoff,
        instruments=symbols,
    )
    progress.stage(
        "outcome_file_projection",
        started,
        symbol_count=len(symbols),
        daily_row_count=len(daily),
        benchmark_row_count=len(benchmark),
    )

    started = time.monotonic()
    label_result = build_multi_horizon_outcome_labels(
        candidates=candidates,
        daily=daily,
        benchmark_daily=benchmark,
        suspend_rows=suspend,
        trading_calendar=calendar,
    )
    labels = apply_outcome_split(label_result.labels, split, data_cutoff=request.data_cutoff)
    labels.to_parquet(run_root / "outcome_labels.parquet", index=False)
    _write_json(run_root / "outcome_label_coverage.json", label_result.coverage.to_dict("records"))
    _write_json(run_root / "outcome_split.json", split.as_dict())
    progress.stage(
        "outcome_labels",
        started,
        label_row_count=len(labels),
        holding_modelable_count=int(labels["holding_modelable"].sum()),
    )

    del daily, benchmark, suspend
    gc.collect()
    started = time.monotonic()
    training = train_outcome_models(
        features=features,
        labels=labels,
        parent_test_predictions=parent_test_predictions,
        seed=request.trainer_seed,
    )
    progress.stage(
        "outcome_heads",
        started,
        model_count=len(training.models),
        test_row_count=len(training.test_predictions),
        test_date_count=training.metrics["test_date_count"],
    )

    started = time.monotonic()
    resource_report = progress.report()
    bundle_id, bundle_path, manifest = publish_outcome_bundle(
        model_root=request.output_root,
        request=request,
        split=split,
        training=training,
        environment_report=environment_report,
        resource_report=resource_report,
    )
    progress.stage("outcome_bundle_publish", started, outcome_bundle_id=bundle_id, bundle_path=str(bundle_path))
    receipt = {
        "status": "trained",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "outcome_bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "metrics": training.metrics,
        "resource_report": progress.report(),
        "outcome_binding_activated": False,
    }
    _write_json(run_root / "outcome_training_receipt.json", receipt)
    return receipt


def _read_bound_parquet(descriptor: Any) -> pd.DataFrame:
    path = Path(descriptor.path)
    if (
        not path.is_file()
        or path.stat().st_size != descriptor.size_bytes
        or sha256_file(path) != descriptor.sha256
    ):
        raise AdvisoryModelFirstError(
            "outcome input artifact differs from its frozen descriptor",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"path": str(path)},
        )
    frame = pd.read_parquet(path)
    if len(frame) != descriptor.row_count or tuple(frame.columns) != descriptor.columns:
        raise AdvisoryModelFirstError(
            "outcome input artifact shape differs from its frozen descriptor",
            reason_code="ADVISORY_OUTCOME_PARENT_ARTIFACT_MISMATCH",
            context={"path": str(path)},
        )
    return frame


def _verify_outcome_training_environment(
    request: FrozenAdvisoryOutcomeTrainingRequestV1,
) -> dict[str, Any]:
    release = platform.release().lower()
    if os.name == "nt" or "microsoft" not in release:
        raise AdvisoryModelFirstError(
            "outcome model training must run inside WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"os_name": os.name, "platform_release": platform.release()},
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        raise AdvisoryModelFirstError(
            "outcome model training requires the rdagent-gpu Conda environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"conda_env": conda_env or None},
        )
    repository_root = Path(request.repository_root).resolve()
    actual_commit = _resolve_wsl_repository_commit(repository_root)
    if actual_commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "outcome training repository commit does not match the frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={
                "expected_commit": request.repository_commit,
                "actual_commit": actual_commit,
            },
        )
    roots = {
        "qlib_daily_root": request.qlib_daily_root,
        "suspend_data_root": request.suspend_data_root,
        "repository_root": request.repository_root,
        "output_root": request.output_root,
    }
    missing = {name: value for name, value in roots.items() if not Path(value).exists()}
    if missing:
        raise AdvisoryModelFirstError(
            "one or more explicit outcome training roots do not exist",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_roots": missing},
        )
    return {
        "platform_release": platform.release(),
        "python_version": platform.python_version(),
        "conda_environment": conda_env,
        "lightgbm_version": importlib.metadata.version("lightgbm"),
        "pyarrow_version": importlib.metadata.version("pyarrow"),
    }


def _resolve_wsl_repository_commit(repository_root: Path) -> str:
    git_command = ["git"]
    git_pointer = repository_root / ".git"
    if git_pointer.is_file():
        pointer = git_pointer.read_text(encoding="utf-8").strip()
        if not pointer.startswith("gitdir: "):
            raise AdvisoryModelFirstError(
                "WSL outcome training worktree has an invalid .git pointer",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
                context={"repository_root": str(repository_root)},
            )
        raw_git_dir = pointer.removeprefix("gitdir: ").strip()
        try:
            translated = subprocess.run(
                ["wslpath", "-u", raw_git_dir],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except subprocess.CalledProcessError as exc:
            raise AdvisoryModelFirstError(
                "WSL could not translate the outcome worktree git directory",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
                context={"error_type": type(exc).__name__},
            ) from exc
        git_command.append(f"--git-dir={translated}")
    try:
        result = subprocess.run(
            [*git_command, "rev-parse", "HEAD"],
            cwd=repository_root,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise AdvisoryModelFirstError(
            "WSL could not resolve the frozen outcome training repository commit",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"error_type": type(exc).__name__, "git_exit_code": exc.returncode},
        ) from exc
    return result.stdout.strip().lower()


def _peak_rss_bytes() -> int:
    try:
        import resource
    except ImportError as exc:
        raise AdvisoryModelFirstError(
            "outcome model resource accounting requires WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"error_type": type(exc).__name__},
        ) from exc
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)
