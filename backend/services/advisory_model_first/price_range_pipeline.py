from __future__ import annotations

import gc
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import tempfile
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import (
    FEATURE_SCHEMA_HASH,
    MODEL_FEATURE_COLUMNS,
)
from backend.services.advisory_model_first.outcome_bundle import read_outcome_bundle_manifest
from backend.services.advisory_model_first.outcome_contracts import (
    FrozenAdvisoryOutcomeTrainingRequestV1,
)
from backend.services.advisory_model_first.outcome_pipeline import (
    _read_and_validate_parent_bundle,
)
from backend.services.advisory_model_first.outcome_split import fixed_406_outcome_split
from backend.services.advisory_model_first.prediction_source import sha256_file
from backend.services.advisory_model_first.price_range_bundle import publish_price_range_bundle
from backend.services.advisory_model_first.price_range_contracts import (
    FrozenAdvisoryPriceRangeTrainingRequestV1,
)
from backend.services.advisory_model_first.price_range_labels import (
    PriceRangeLabelBuildResult,
    apply_price_range_split,
    build_price_range_labels,
)
from backend.services.advisory_model_first.price_range_training import (
    train_price_range_models,
)
from backend.services.advisory_model_first.qe_file_source import (
    initialize_qlib,
    load_qlib_daily,
    load_suspend_rows,
    load_trading_calendar,
)


class PriceRangeTrainingProgress:
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
                "price-range training exceeded the approved RSS limit",
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


LABEL_DECISION_BATCH_SIZE = 32


def run_price_range_training_pipeline(request_path: str | Path) -> dict[str, Any]:
    request = FrozenAdvisoryPriceRangeTrainingRequestV1.model_validate_json(
        Path(request_path).read_text(encoding="utf-8")
    )
    environment_report = _verify_price_range_training_environment(request)
    progress = PriceRangeTrainingProgress(limit_bytes=request.resource_max_rss_bytes)
    run_root = Path(request.output_root).resolve() / "price_range_runs" / request.request_id
    run_root.mkdir(parents=True, exist_ok=True)

    started = time.monotonic()
    parent_bundle_path, parent_request, feature_schema = _validate_parent_identity(request)
    outcome_bundle_path, outcome_request, split = _validate_outcome_identity(request)
    candidates = _read_bound_parquet(request.candidates_artifact)
    features = _read_bound_parquet(request.features_artifact)
    decision_dates = (
        pd.DatetimeIndex(pd.to_datetime(candidates["decision_as_of_trade_date"]))
        .normalize()
        .sort_values()
        .unique()
    )
    recomputed = fixed_406_outcome_split(decision_dates)
    if split.as_dict() != recomputed.as_dict():
        raise AdvisoryModelFirstError(
            "price-range split differs from the frozen outcome split",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        )
    if (
        decision_dates[0].date().isoformat() != request.decision_date_start
        or decision_dates[-1].date().isoformat() != request.decision_date_end
    ):
        raise AdvisoryModelFirstError(
            "price-range candidate decision range differs from its request",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    progress.stage(
        "price_range_input_identity",
        started,
        parent_bundle_path=str(parent_bundle_path),
        outcome_bundle_path=str(outcome_bundle_path),
        parent_request_id=parent_request.request_id,
        outcome_request_id=outcome_request.request_id,
        feature_schema_hash=feature_schema["feature_schema_hash"],
        candidate_row_count=len(candidates),
        feature_row_count=len(features),
        decision_date_count=len(decision_dates),
    )

    started = time.monotonic()
    initialize_qlib(request.qlib_daily_root)
    calendar = load_trading_calendar("2024-03-01", request.data_cutoff)
    label_result, projection_stats = _build_labels_in_date_batches(
        candidates=candidates,
        trading_calendar=calendar,
        suspend_data_root=request.suspend_data_root,
        scratch_parent=run_root,
    )
    progress.stage(
        "price_range_file_projection",
        started,
        **projection_stats,
    )

    started = time.monotonic()
    labels = apply_price_range_split(label_result.labels, split)
    labels.to_parquet(run_root / "price_range_labels.parquet", index=False)
    _write_json(
        run_root / "price_range_label_coverage.json",
        label_result.coverage.to_dict("records"),
    )
    _write_json(run_root / "price_range_split.json", split.as_dict())
    progress.stage(
        "price_range_labels",
        started,
        label_row_count=len(labels),
        binary_modelable_count=int(labels["binary_modelable"].sum()),
        gap_modelable_count=int(labels["gap_modelable"].sum()),
        unavailable_count=int(labels["entry_label_status"].eq("UNAVAILABLE").sum()),
    )

    gc.collect()
    started = time.monotonic()
    training = train_price_range_models(
        features=features,
        labels=labels,
        seed=request.trainer_seed,
    )
    progress.stage(
        "price_range_heads",
        started,
        model_count=len(training.models),
        test_row_count=len(training.test_predictions),
        test_date_count=training.metrics["test_date_count"],
    )

    started = time.monotonic()
    resource_report = progress.report()
    bundle_id, bundle_path, manifest = publish_price_range_bundle(
        model_root=request.output_root,
        request=request,
        split=split,
        training=training,
        environment_report=environment_report,
        resource_report=resource_report,
    )
    progress.stage(
        "price_range_bundle_publish",
        started,
        price_range_bundle_id=bundle_id,
        bundle_path=str(bundle_path),
    )
    receipt = {
        "status": "trained",
        "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "parent_bundle_id": request.parent_bundle_id,
        "outcome_bundle_id": request.outcome_bundle_id,
        "price_range_bundle_id": bundle_id,
        "bundle_path": str(bundle_path),
        "manifest": manifest,
        "metrics": training.metrics,
        "resource_report": progress.report(),
        "price_range_binding_activated": False,
    }
    _write_json(run_root / "price_range_training_receipt.json", receipt)
    return receipt


def _build_labels_in_date_batches(
    *,
    candidates: pd.DataFrame,
    trading_calendar: pd.DatetimeIndex,
    suspend_data_root: str,
    scratch_parent: Path,
) -> tuple[PriceRangeLabelBuildResult, dict[str, int]]:
    decision_values = pd.to_datetime(candidates["decision_as_of_trade_date"]).dt.normalize()
    decision_dates = pd.DatetimeIndex(decision_values.sort_values().unique())
    label_part_paths: list[Path] = []
    coverage_parts: list[pd.DataFrame] = []
    total_daily_rows = 0
    total_suspend_rows = 0
    maximum_batch_symbols = 0
    scratch_root = Path(
        tempfile.mkdtemp(prefix=".price-range-label-parts-", dir=scratch_parent)
    )
    try:
        for batch_index, offset in enumerate(
            range(0, len(decision_dates), LABEL_DECISION_BATCH_SIZE)
        ):
            batch_dates = decision_dates[offset : offset + LABEL_DECISION_BATCH_SIZE]
            batch_candidates = candidates.loc[decision_values.isin(batch_dates)].copy()
            symbols = sorted(
                batch_candidates["instrument"].astype(str).str.upper().unique().tolist()
            )
            projection_start = pd.to_datetime(
                batch_candidates["decision_as_of_trade_date"]
            ).min().date().isoformat()
            projection_end = pd.to_datetime(
                batch_candidates["target_trade_date"]
            ).max().date().isoformat()
            daily = load_qlib_daily(
                symbols,
                start=projection_start,
                end=projection_end,
                fields=(
                    "$open",
                    "$low",
                    "$close",
                    "$factor",
                    "$up_limit_price",
                    "$prev_close",
                    "$limit_up",
                ),
            )
            suspend = load_suspend_rows(
                suspend_data_root,
                start=projection_start,
                end=projection_end,
                instruments=symbols,
            )
            result = build_price_range_labels(
                candidates=batch_candidates,
                daily=daily,
                suspend_rows=suspend,
                trading_calendar=trading_calendar,
            )
            part_path = scratch_root / f"labels-{batch_index:04d}.parquet"
            result.labels.to_parquet(part_path, index=False)
            label_part_paths.append(part_path)
            coverage_parts.append(result.coverage)
            total_daily_rows += len(daily)
            total_suspend_rows += len(suspend)
            maximum_batch_symbols = max(maximum_batch_symbols, len(symbols))
            del daily, suspend, result
            gc.collect()
        labels = pd.concat(
            (pd.read_parquet(path) for path in label_part_paths), ignore_index=True
        )
        coverage = pd.concat(coverage_parts, ignore_index=True)
    finally:
        if scratch_root.exists():
            try:
                shutil.rmtree(scratch_root)
            except OSError as exc:
                raise AdvisoryModelFirstError(
                    "price-range temporary label parts could not be removed",
                    reason_code="ADVISORY_PRICE_RANGE_TRAINING_FAILED",
                    context={
                        "scratch_root": str(scratch_root),
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    },
                ) from exc
    keys = ["decision_as_of_trade_date", "target_trade_date", "instrument"]
    if len(labels) != len(candidates) or labels.duplicated(keys).any():
        raise AdvisoryModelFirstError(
            "price-range batched label projection did not preserve candidate identity",
            reason_code="ADVISORY_PRICE_RANGE_LABEL_INPUT_UNAVAILABLE",
            context={"candidate_rows": len(candidates), "label_rows": len(labels)},
        )
    return (
        PriceRangeLabelBuildResult(
            labels=labels.sort_values(keys).reset_index(drop=True),
            coverage=coverage.sort_values("decision_as_of_trade_date").reset_index(drop=True),
        ),
        {
            "decision_batch_count": len(range(0, len(decision_dates), LABEL_DECISION_BATCH_SIZE)),
            "decision_batch_size": LABEL_DECISION_BATCH_SIZE,
            "daily_row_count": total_daily_rows,
            "suspend_row_count": total_suspend_rows,
            "maximum_batch_symbol_count": maximum_batch_symbols,
        },
    )


def _validate_parent_identity(
    request: FrozenAdvisoryPriceRangeTrainingRequestV1,
) -> tuple[Path, FrozenAdvisoryTrainingRequestV1, dict[str, Any]]:
    parent_request_path = Path(request.parent_training_request_path).resolve()
    feature_schema_path = Path(request.parent_feature_schema_path).resolve()
    parent_bundle_path = feature_schema_path.parent
    if parent_request_path.parent != parent_bundle_path:
        raise AdvisoryModelFirstError(
            "price-range parent artifacts do not share one exact bundle root",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    try:
        _manifest, feature_schema = _read_and_validate_parent_bundle(
            parent_bundle_path,
            expected_bundle_id=request.parent_bundle_id,
            expected_manifest_file_sha256=request.parent_bundle_manifest_file_sha256,
        )
        parent_request = FrozenAdvisoryTrainingRequestV1.model_validate_json(
            parent_request_path.read_text(encoding="utf-8")
        )
    except (AdvisoryModelFirstError, OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "price-range parent bundle identity cannot be validated",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context=_source_error_context(exc),
        ) from exc
    if (
        parent_request.request_id != request.parent_request_id
        or parent_request.request_sha256 != request.parent_request_sha256
        or parent_request.package_id != request.package_id
        or parent_request.manifest_sha256 != request.manifest_sha256
        or parent_request.style_profile_id != request.style_profile_id
        or parent_request.style_profile_hash != request.style_profile_hash
        or parent_request.feature_schema_version != request.feature_schema_version
        or feature_schema.get("feature_schema_hash") != request.feature_schema_hash
        or request.feature_schema_hash != FEATURE_SCHEMA_HASH
        or tuple(feature_schema.get("trained_feature_names") or ()) != tuple(MODEL_FEATURE_COLUMNS)
    ):
        raise AdvisoryModelFirstError(
            "price-range parent bundle semantic identities are inconsistent",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
        )
    return parent_bundle_path, parent_request, feature_schema


def _validate_outcome_identity(
    request: FrozenAdvisoryPriceRangeTrainingRequestV1,
) -> tuple[Path, FrozenAdvisoryOutcomeTrainingRequestV1, Any]:
    outcome_request_path = Path(request.outcome_training_request_path).resolve()
    split_path = Path(request.outcome_split_path).resolve()
    bundle_path = outcome_request_path.parent
    manifest_path = bundle_path / "manifest.json"
    if split_path.parent != bundle_path or not manifest_path.is_file() or (
        sha256_file(manifest_path) != request.outcome_bundle_manifest_file_sha256
    ):
        raise AdvisoryModelFirstError(
            "price-range outcome artifacts do not match one exact bundle root",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        )
    try:
        manifest = read_outcome_bundle_manifest(
            bundle_path, expected_bundle_id=request.outcome_bundle_id
        )
        outcome_request = FrozenAdvisoryOutcomeTrainingRequestV1.model_validate_json(
            outcome_request_path.read_text(encoding="utf-8")
        )
        split_payload = json.loads(split_path.read_text(encoding="utf-8"))
    except (AdvisoryModelFirstError, OSError, json.JSONDecodeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "price-range outcome bundle identity cannot be validated",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
            context=_source_error_context(exc),
        ) from exc
    split_descriptor = (manifest.get("files") or {}).get("split.json") or {}
    if (
        outcome_request.request_id != request.outcome_request_id
        or outcome_request.request_sha256 != request.outcome_request_sha256
        or outcome_request.parent_bundle_id != request.parent_bundle_id
        or outcome_request.parent_request_id != request.parent_request_id
        or outcome_request.parent_request_sha256 != request.parent_request_sha256
        or outcome_request.package_id != request.package_id
        or outcome_request.manifest_sha256 != request.manifest_sha256
        or outcome_request.style_profile_id != request.style_profile_id
        or outcome_request.style_profile_hash != request.style_profile_hash
        or outcome_request.feature_schema_version != request.feature_schema_version
        or outcome_request.feature_schema_hash != request.feature_schema_hash
        or outcome_request.candidate_semantics_id != request.candidate_semantics_id
        or manifest.get("parent_bundle_id") != request.parent_bundle_id
        or split_descriptor.get("sha256") != sha256_file(split_path)
        or split_descriptor.get("size_bytes") != split_path.stat().st_size
    ):
        raise AdvisoryModelFirstError(
            "price-range outcome bundle semantic identities are inconsistent",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        )
    expected_keys = {"train", "purge_1", "validation", "purge_2", "test"}
    if not isinstance(split_payload, dict) or set(split_payload) != expected_keys:
        raise AdvisoryModelFirstError(
            "price-range outcome split member has an invalid shape",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        )
    from backend.services.advisory_model_first.outcome_split import OutcomeDateSplit

    try:
        split = OutcomeDateSplit(
            **{
                name: tuple(pd.to_datetime(split_payload[name]).normalize())
                for name in expected_keys
            }
        )
    except (TypeError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "price-range outcome split member cannot be parsed",
            reason_code="ADVISORY_PRICE_RANGE_OUTCOME_IDENTITY_MISMATCH",
        ) from exc
    return bundle_path, outcome_request, split


def _read_bound_parquet(descriptor: Any) -> pd.DataFrame:
    path = Path(descriptor.path)
    if (
        not path.is_file()
        or path.stat().st_size != descriptor.size_bytes
        or sha256_file(path) != descriptor.sha256
    ):
        raise AdvisoryModelFirstError(
            "price-range input artifact differs from its frozen descriptor",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"path": str(path)},
        )
    frame = pd.read_parquet(path)
    if len(frame) != descriptor.row_count or tuple(frame.columns) != descriptor.columns:
        raise AdvisoryModelFirstError(
            "price-range input artifact shape differs from its frozen descriptor",
            reason_code="ADVISORY_PRICE_RANGE_BUNDLE_IDENTITY_MISMATCH",
            context={"path": str(path)},
        )
    return frame


def _verify_price_range_training_environment(
    request: FrozenAdvisoryPriceRangeTrainingRequestV1,
) -> dict[str, Any]:
    release = platform.release().lower()
    if os.name == "nt" or "microsoft" not in release:
        raise AdvisoryModelFirstError(
            "price-range model training must run inside WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"os_name": os.name, "platform_release": platform.release()},
        )
    conda_env = str(os.getenv("CONDA_DEFAULT_ENV") or "")
    if conda_env != "rdagent-gpu":
        raise AdvisoryModelFirstError(
            "price-range model training requires the rdagent-gpu Conda environment",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"conda_env": conda_env or None},
        )
    repository_root = Path(request.repository_root).resolve()
    actual_commit = _resolve_wsl_repository_commit(repository_root)
    if actual_commit != request.repository_commit:
        raise AdvisoryModelFirstError(
            "price-range training repository commit does not match the frozen request",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"expected_commit": request.repository_commit, "actual_commit": actual_commit},
        )
    roots = {
        "qlib_daily_root": request.qlib_daily_root,
        "suspend_data_root": request.suspend_data_root,
        "repository_root": request.repository_root,
        "output_root": request.output_root,
    }
    missing = {name: value for name, value in roots.items() if not Path(value).is_dir()}
    if missing:
        raise AdvisoryModelFirstError(
            "one or more explicit price-range training roots do not exist",
            reason_code="ADVISORY_MODEL_QE_SCHEMA_MISMATCH",
            context={"missing_roots": missing},
        )
    try:
        lightgbm_version = importlib.metadata.version("lightgbm")
        pyarrow_version = importlib.metadata.version("pyarrow")
    except importlib.metadata.PackageNotFoundError as exc:
        raise AdvisoryModelFirstError(
            "price-range WSL training dependency is unavailable",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
            context={"package": exc.name},
        ) from exc
    return {
        "platform_release": platform.release(),
        "python_version": platform.python_version(),
        "conda_environment": conda_env,
        "lightgbm_version": lightgbm_version,
        "pyarrow_version": pyarrow_version,
    }


def _resolve_wsl_repository_commit(repository_root: Path) -> str:
    git_command = ["git"]
    git_pointer = repository_root / ".git"
    if git_pointer.is_file():
        pointer = git_pointer.read_text(encoding="utf-8").strip()
        if not pointer.startswith("gitdir: "):
            raise AdvisoryModelFirstError(
                "WSL price-range training worktree has an invalid .git pointer",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
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
                "WSL could not translate the price-range worktree git directory",
                reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
                context={"wslpath_exit_code": exc.returncode},
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
            "WSL could not resolve the frozen price-range repository commit",
            reason_code="ADVISORY_MODEL_TARGET_IDENTITY_MISMATCH",
            context={"git_exit_code": exc.returncode},
        ) from exc
    return result.stdout.strip().lower()


def _peak_rss_bytes() -> int:
    try:
        import resource
    except ImportError as exc:
        raise AdvisoryModelFirstError(
            "price-range resource accounting requires WSL",
            reason_code="ADVISORY_MODEL_TRAINING_REQUIRES_WSL",
        ) from exc
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * 1024


def _write_json(path: Path, value: Any) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            value,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            default=_json_default,
        ),
        encoding="utf-8",
    )
    temporary.replace(path)


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    raise TypeError(f"unsupported price-range JSON value: {type(value).__name__}")


def _source_error_context(exc: Exception) -> dict[str, Any]:
    context: dict[str, Any] = {
        "error_type": type(exc).__name__,
        "error_message": str(exc),
    }
    if isinstance(exc, AdvisoryModelFirstError):
        context["source_reason_code"] = exc.reason_code
        context["source_context"] = exc.context
    return context
