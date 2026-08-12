from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_contracts import FrozenAdvisoryMetaLabelTrainingRequestV1
from backend.services.advisory_model_first.policy_dataset_bundle import _json_ready
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


def publish_meta_label_bundle(
    *,
    request: FrozenAdvisoryMetaLabelTrainingRequestV1,
    booster: Any,
    feature_schema: Mapping[str, Any],
    runtime_hmm_models: Mapping[str, Any],
    runtime_hmm_unavailable: list[dict[str, Any]],
    walk_forward_hmm_receipt: Mapping[str, Any],
    trial_metrics: pd.DataFrame,
    block_scores: pd.DataFrame,
    pbo_receipt: Mapping[str, Any],
    winner_receipt: Mapping[str, Any],
    baseline_comparison: Mapping[str, Any],
    training_log: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(request.output_root).resolve()
    target_root = root / "meta_label_bundles"
    target_root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="advisory_meta_label_", dir=root))
    try:
        booster.save_model(str(temp / "model.txt"))
        request.write_json(temp / "training_request.json")
        source_manifest = Path(request.policy_dataset_bundle_root) / "manifest.json"
        shutil.copyfile(source_manifest, temp / "policy_dataset_manifest.json")
        _write_json(temp / "feature_schema.json", dict(feature_schema))
        _write_json(temp / "fresh_hmm_models.json", dict(runtime_hmm_models))
        _write_json(temp / "fresh_hmm_unavailable.json", runtime_hmm_unavailable)
        _write_json(temp / "walk_forward_hmm_receipt.json", dict(walk_forward_hmm_receipt))
        _write_json(temp / "family_specs.json", [item.model_dump(mode="json") for item in request.family_specs])
        trial_metrics.to_parquet(temp / "cpcv_trial_metrics.parquet", index=False)
        block_scores.to_parquet(temp / "cpcv_block_scores.parquet", index=False)
        _write_json(temp / "pbo_receipt.json", dict(pbo_receipt))
        _write_json(temp / "winner_receipt.json", dict(winner_receipt))
        _write_json(temp / "baseline_comparison.json", dict(baseline_comparison))
        _write_json(temp / "training_log.json", dict(training_log))
        _write_json(temp / "resource_report.json", dict(resource_report))
        files = {
            path.name: {"sha256": _sha256(path), "size_bytes": path.stat().st_size}
            for path in sorted(temp.iterdir())
            if path.is_file()
        }
        identity_files = {
            name: descriptor
            for name, descriptor in files.items()
            if name not in {"training_request.json", "training_log.json", "resource_report.json"}
        }
        payload = {
            "schema_version": "advisory_meta_label_bundle_v1",
            "status": "EXPERIMENTAL_MODEL",
            "calibration_state": "UNCALIBRATED",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "style_profile_id": request.style_profile_id,
            "style_profile_hash": request.style_profile_hash,
            "shadow_policy_sha256": request.shadow_policy_sha256,
            "feature_schema_version": request.feature_schema_version,
            "feature_schema_hash": request.feature_schema_hash,
            "winner_family_id": winner_receipt["family_id"],
            "winner_seed": winner_receipt["seed"],
            "model_role": "meta_label_take_skip_confidence",
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(payload)
        manifest = {"bundle_id": bundle_id, **payload, "files": files}
        _write_json(temp / "manifest.json", manifest)
        target = target_root / bundle_id
        if target.exists():
            existing = load_meta_label_bundle(target, expected_bundle_id=bundle_id, load_booster=False)
            existing_identity = {
                key: value for key, value in existing["manifest"].items() if key not in {"bundle_id", "files"}
            }
            if existing_identity != payload:
                raise AdvisoryModelFirstError(
                    "existing meta-label bundle differs from its content identity",
                    reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
                )
            return bundle_id, target, existing["manifest"]
        os.replace(temp, target)
        load_meta_label_bundle(target, expected_bundle_id=bundle_id, load_booster=False)
        return bundle_id, target, manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp)


def load_meta_label_bundle(
    bundle_path: str | Path, *, expected_bundle_id: str, load_booster: bool = True
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "meta-label manifest cannot be read",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    actual = canonical_json_sha256(
        {key: value for key, value in manifest.items() if key not in {"bundle_id", "files"}}
    )
    if manifest.get("bundle_id") != expected_bundle_id or actual != expected_bundle_id:
        raise AdvisoryModelFirstError(
            "meta-label bundle identity is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    for name, descriptor in (manifest.get("files") or {}).items():
        path = (root / name).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise AdvisoryModelFirstError(
                "meta-label bundle file escapes its root",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            ) from exc
        if not path.is_file() or path.stat().st_size != int(descriptor["size_bytes"]) or _sha256(path) != descriptor["sha256"]:
            raise AdvisoryModelFirstError(
                "meta-label bundle file differs from manifest",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
                context={"filename": name},
            )
    result: dict[str, Any] = {"manifest": manifest, "bundle_path": root}
    if load_booster:
        try:
            import lightgbm as lgb

            result["booster"] = lgb.Booster(model_file=str(root / "model.txt"))
        except Exception as exc:
            raise AdvisoryModelFirstError(
                "meta-label model cannot be loaded",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            ) from exc
    return result


def score_meta_label_bundle(bundle: Mapping[str, Any], features: pd.DataFrame) -> pd.DataFrame:
    root = Path(bundle["bundle_path"])
    schema = json.loads((root / "feature_schema.json").read_text(encoding="utf-8"))
    names = tuple(schema["trained_feature_names"])
    missing = sorted(set(names) - set(features))
    if missing:
        raise AdvisoryModelFirstError(
            "meta-label scoring features are incomplete",
            reason_code="ADVISORY_MODEL_FEATURE_REQUIRED_VALUE_MISSING",
            context={"missing_columns": missing},
        )
    matrix = features.loc[:, names].copy()
    vocabulary = schema.get("categorical_vocabulary") or {}
    for column in matrix:
        matrix[column] = pd.to_numeric(matrix[column], errors="coerce")
    for column, values in vocabulary.items():
        matrix[column] = pd.Categorical(matrix[column], categories=values)
    probability = bundle["booster"].predict(matrix)
    output = features[["decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank"]].copy()
    output["take_probability"] = probability
    output["skip_probability"] = 1.0 - probability
    output["advisory_model_confidence"] = abs(output["take_probability"] - 0.5) * 2.0
    output = output.sort_values(
        ["decision_as_of_trade_date", "take_probability", "selection_effective_rank", "instrument"],
        ascending=[True, False, True, True],
    )
    output["entry_priority_rank"] = output.groupby("decision_as_of_trade_date").cumcount().add(1)
    output["selection_exit_rank"] = output["selection_effective_rank"]
    output["model_status"] = "EXPERIMENTAL_MODEL"
    output["calibration_state"] = "UNCALIBRATED"
    return output.reset_index(drop=True)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            _json_ready(payload),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
