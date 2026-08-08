from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH, FEATURE_SCHEMA_PAYLOAD
from backend.services.advisory_model_first.reranker_training import RerankerTrainingResult
from backend.services.advisory_model_first.time_split import PurgedDateSplit
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def publish_model_bundle(
    *,
    model_root: str | Path,
    request: FrozenAdvisoryTrainingRequestV1,
    split: PurgedDateSplit,
    hmm_models: Mapping[str, Any],
    hmm_unavailable: tuple[dict[str, Any], ...],
    training: RerankerTrainingResult,
    diagnostics: Mapping[str, Any],
    schema_receipt: Mapping[str, Any],
    environment_report: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(model_root).resolve()
    bundles_root = root / "bundles"
    bundles_root.mkdir(parents=True, exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix="advisory_bundle_", dir=root))
    try:
        training.booster.save_model(str(tmp / "model.txt"), num_iteration=training.booster.best_iteration)
        _write_json(tmp / "fresh_hmm_models.json", dict(hmm_models))
        _write_json(tmp / "fresh_hmm_unavailable.json", list(hmm_unavailable))
        _write_json(
            tmp / "feature_schema.json",
            {
                **FEATURE_SCHEMA_PAYLOAD,
                "feature_schema_hash": FEATURE_SCHEMA_HASH,
                "categorical_vocabulary": training.categorical_vocabulary,
                "trained_feature_names": training.feature_names,
            },
        )
        request.write_json(tmp / "training_request.json")
        _write_json(tmp / "split.json", split.as_dict())
        _write_json(
            tmp / "label_policy.json",
            {
                "schema_version": "advisory_label_policy_v1",
                "nominal_horizon_trading_days": 5,
                "maximum_exit_delay_trading_days": 5,
                "open_cost": 0.000095,
                "close_cost": 0.000595,
                "label_gain": [0, 1, 3, 7, 15],
            },
        )
        _write_json(tmp / "metrics.json", training.metrics)
        training.test_predictions.to_parquet(tmp / "test_predictions.parquet", index=False)
        _write_json(tmp / "baseline_comparison.json", training.baseline_comparison)
        _write_json(
            tmp / "training_log.json",
            {
                "evaluation_history": training.evaluation_history,
                "diagnostics": dict(diagnostics),
                "schema_receipt": dict(schema_receipt),
                "environment_report": dict(environment_report),
                "resource_report": dict(resource_report),
            },
        )
        files = {
            path.name: {"sha256": _sha256_file(path), "size_bytes": path.stat().st_size}
            for path in sorted(tmp.iterdir())
            if path.is_file()
        }
        manifest_without_id = {
            "schema_version": "advisory_model_bundle_v1",
            "status": "EXPERIMENTAL_SHADOW",
            "calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "package_asset_closure_hash": request.package_asset_closure_hash,
            "style_profile_id": request.style_profile_id,
            "style_profile_hash": request.style_profile_hash,
            "selection_runtime_semantics_id": request.selection_runtime_semantics_id,
            "selection_runtime_semantics_hash": request.selection_runtime_semantics_hash,
            "selection_runtime_semantics": request.selection_runtime_semantics,
            "feature_schema_version": request.feature_schema_version,
            "feature_schema_hash": FEATURE_SCHEMA_HASH,
            "label_policy_version": request.label_policy_version,
            "decision_clock_version": request.decision_clock_version,
            "representative_seed_run_ids": request.representative_seed_run_ids,
            "representative_model_asset_sha256": request.representative_model_asset_sha256,
            "full_seed_roster": {
                leg_id: list(run_ids)
                for leg_id, run_ids in sorted(request.full_seed_roster.items())
            },
            "prediction_artifact_sha256": {
                run_id: descriptor.artifact_sha256
                for run_id, descriptor in sorted(request.prediction_artifacts.items())
            },
            "terminal_weights": request.terminal_weights,
            "combined_reference_sha256": request.combined_reference_sha256,
            "combined_reference_diagnostic_only": request.combined_reference_diagnostic_only,
            "qe_dataset": {
                "data_cutoff": request.data_cutoff,
                "hmm_continuation_cutoff": request.hmm_continuation_cutoff,
                "schema_receipt": dict(schema_receipt),
            },
            "decision_date_start": request.decision_date_start,
            "decision_date_end": request.decision_date_end,
            "repository_commit": request.repository_commit,
            "continuation_cutoff": request.hmm_continuation_cutoff,
            "training_created_at": request.created_at,
            "training_environment": dict(environment_report),
            "retrospective_test_status": training.metrics.get("status", "available"),
            "files": files,
        }
        bundle_id = canonical_json_sha256(manifest_without_id)
        manifest = {"bundle_id": bundle_id, **manifest_without_id}
        _write_json(tmp / "manifest.json", manifest)
        readback = json.loads((tmp / "manifest.json").read_text(encoding="utf-8"))
        if readback != manifest:
            raise AdvisoryModelFirstError(
                "model bundle manifest readback mismatch",
                reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
            )
        for filename, descriptor in files.items():
            if _sha256_file(tmp / filename) != descriptor["sha256"]:
                raise AdvisoryModelFirstError(
                    "model bundle file hash changed during publication",
                    reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                    context={"filename": filename},
                )
        target = bundles_root / bundle_id
        if target.exists():
            existing = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
            if existing != manifest:
                raise AdvisoryModelFirstError(
                    "existing bundle identity has different content",
                    reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                    context={"bundle_id": bundle_id},
                )
            for filename, descriptor in files.items():
                existing_file = target / filename
                if not existing_file.is_file() or _sha256_file(existing_file) != descriptor["sha256"]:
                    raise AdvisoryModelFirstError(
                        "existing bundle file is missing or corrupt",
                        reason_code="ADVISORY_MODEL_BUNDLE_INVALID",
                        context={"bundle_id": bundle_id, "filename": filename},
                    )
            shutil.rmtree(tmp)
            return bundle_id, target, manifest
        os.replace(tmp, target)
        return bundle_id, target, manifest
    except Exception:
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        raise


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
