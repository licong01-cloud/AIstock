from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from datetime import date
from math import isfinite
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_contracts import FrozenAdvisoryMetaLabelTrainingRequestV1
from backend.services.advisory_model_first.policy_contracts import (
    AdvisoryPolicyCostV1,
    transition_policy_from_payload,
)
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
        _write_json(temp / "fresh_hmm_models.json", _stable_hmm_payload(dict(runtime_hmm_models)))
        _write_json(temp / "fresh_hmm_unavailable.json", _stable_hmm_payload(runtime_hmm_unavailable))
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


def find_meta_label_bundle_for_request(
    request: FrozenAdvisoryMetaLabelTrainingRequestV1,
) -> tuple[str, Path, dict[str, Any]] | None:
    """Return the one complete bundle bound to this exact frozen request."""
    target_root = Path(request.output_root).resolve() / "meta_label_bundles"
    if not target_root.is_dir():
        return None
    matches: list[tuple[str, Path, dict[str, Any]]] = []
    for candidate in sorted(target_root.iterdir()):
        if not candidate.is_dir():
            continue
        manifest_path = candidate / "manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("request_sha256") != request.request_sha256:
            continue
        loaded = load_meta_label_bundle(
            candidate,
            expected_bundle_id=str(manifest.get("bundle_id", "")),
            load_booster=False,
        )
        try:
            frozen = FrozenAdvisoryMetaLabelTrainingRequestV1.model_validate_json(
                (candidate / "training_request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise AdvisoryModelFirstError(
                "meta-label bundle request cannot be read",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
                context={"bundle_path": str(candidate)},
            ) from exc
        if (
            frozen.request_id != request.request_id
            or frozen.request_sha256 != request.request_sha256
            or frozen.functional_payload() != request.functional_payload()
        ):
            raise AdvisoryModelFirstError(
                "meta-label bundle request differs from the exact retry request",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
                context={"bundle_path": str(candidate)},
            )
        matches.append((str(manifest["bundle_id"]), candidate, loaded["manifest"]))
    if len(matches) > 1:
        raise AdvisoryModelFirstError(
            "multiple meta-label bundles claim the same frozen request",
            reason_code="ADVISORY_META_LABEL_BUNDLE_IDENTITY_CONFLICT",
            context={"request_id": request.request_id, "bundle_ids": [item[0] for item in matches]},
        )
    return matches[0] if matches else None


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
    if (
        root.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or actual != expected_bundle_id
    ):
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


def load_exact_meta_label_runtime_bundle(
    *,
    model_root: str | Path,
    bundle_id: str,
    bundle_manifest_sha256: str,
    load_booster: bool = True,
) -> dict[str, Any]:
    """Load one descriptor-selected meta-label bundle without scanning or fallback."""

    if not _is_sha256(bundle_id) or not _is_sha256(bundle_manifest_sha256):
        raise AdvisoryModelFirstError(
            "meta-label runtime bundle identity is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    root = Path(model_root).resolve()
    bundle_path = (root / "meta_label_bundles" / bundle_id).resolve()
    try:
        bundle_path.relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime bundle escapes its configured root",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    manifest_path = bundle_path / "manifest.json"
    if not manifest_path.is_file() or _sha256(manifest_path) != bundle_manifest_sha256:
        raise AdvisoryModelFirstError(
            "meta-label runtime manifest differs from the descriptor identity",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    loaded = load_meta_label_bundle(
        bundle_path,
        expected_bundle_id=bundle_id,
        load_booster=load_booster,
    )
    policy_dataset_bundle_id = str(loaded["manifest"].get("policy_dataset_bundle_id") or "")
    if not _is_sha256(policy_dataset_bundle_id):
        raise AdvisoryModelFirstError(
            "meta-label runtime policy dataset identity is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    policy_path = (root / "policy_datasets" / policy_dataset_bundle_id / "shadow_policy.json").resolve()
    try:
        policy_path.relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime policy escapes its configured root",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    shadow_policy = _read_runtime_json(policy_path, expected_type=dict)
    if canonical_json_sha256(shadow_policy) != loaded["manifest"].get("shadow_policy_sha256"):
        raise AdvisoryModelFirstError(
            "meta-label runtime policy differs from the frozen bundle identity",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    try:
        transition_policy = transition_policy_from_payload(shadow_policy)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime policy is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    policy_dataset_manifest = _read_runtime_json(
        bundle_path / "policy_dataset_manifest.json",
        expected_type=dict,
    )
    if (
        policy_dataset_manifest.get("policy_dataset_bundle_id") != policy_dataset_bundle_id
        or policy_dataset_manifest.get("shadow_policy_sha256")
        != loaded["manifest"].get("shadow_policy_sha256")
    ):
        raise AdvisoryModelFirstError(
            "meta-label runtime policy dataset manifest differs from the frozen bundle identity",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    cost_policy_path = (root / "policy_datasets" / policy_dataset_bundle_id / "cost_policy.json").resolve()
    try:
        cost_policy_path.relative_to(root)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime cost policy escapes its configured root",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    cost_policy_payload = _read_runtime_json(cost_policy_path, expected_type=dict)
    try:
        cost_policy = AdvisoryPolicyCostV1.model_validate(cost_policy_payload)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime cost policy is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    expected_cost_policy_sha256 = str(policy_dataset_manifest.get("cost_policy_sha256") or "")
    if cost_policy.policy_sha256 != expected_cost_policy_sha256:
        raise AdvisoryModelFirstError(
            "meta-label runtime cost policy differs from the frozen policy dataset",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    feature_schema = _read_runtime_json(bundle_path / "feature_schema.json", expected_type=dict)
    hmm_models = _read_runtime_json(bundle_path / "fresh_hmm_models.json", expected_type=dict)
    hmm_unavailable = _read_runtime_json(
        bundle_path / "fresh_hmm_unavailable.json", expected_type=list
    )
    baselines = _read_runtime_json(bundle_path / "baseline_comparison.json", expected_type=dict)
    models = hmm_models.get("models")
    if hmm_models.get("schema_version") != "fresh_sector_hmm_bundle_v1" or not isinstance(models, Mapping) or not models:
        raise AdvisoryModelFirstError(
            "meta-label runtime HMM bundle is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    cutoffs = {
        str(model.get("continuation_cutoff") or "")
        for model in models.values()
        if isinstance(model, Mapping)
    }
    if len(cutoffs) != 1 or "" in cutoffs or len(models) != sum(
        isinstance(model, Mapping) for model in models.values()
    ):
        raise AdvisoryModelFirstError(
            "meta-label runtime HMM models do not share one continuation cutoff",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        )
    continuation_cutoff = next(iter(cutoffs))
    try:
        date.fromisoformat(continuation_cutoff)
    except ValueError as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime HMM continuation cutoff is invalid",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
        ) from exc
    loaded.update(
        {
            "feature_schema": feature_schema,
            "hmm_models": hmm_models,
            "hmm_unavailable": tuple(hmm_unavailable),
            "baselines": baselines,
            "shadow_policy": shadow_policy,
            "cost_policy": cost_policy.model_dump(mode="json"),
            "cost_policy_sha256": cost_policy.policy_sha256,
            "shadow_policy_maturity_horizon_days": transition_policy.time_stop_days,
            "continuation_cutoff": continuation_cutoff,
            "manifest_file_sha256": bundle_manifest_sha256,
        }
    )
    return loaded


def score_meta_label_bundle(bundle: Mapping[str, Any], features: pd.DataFrame) -> pd.DataFrame:
    root = Path(bundle["bundle_path"])
    schema = bundle.get("feature_schema")
    if not isinstance(schema, Mapping):
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
        numeric = pd.to_numeric(matrix[column], errors="coerce")
        unseen = numeric.notna() & ~numeric.isin(values)
        if unseen.any():
            missing_indicator = f"{column}__missing"
            if missing_indicator not in matrix:
                raise AdvisoryModelFirstError(
                    "meta-label categorical vocabulary has no missing indicator",
                    reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
                    context={"feature": column},
                )
            matrix.loc[unseen, missing_indicator] = 1
            numeric = numeric.mask(unseen)
        matrix[column] = pd.Categorical(numeric, categories=values)
    booster = bundle["booster"]
    try:
        if tuple(booster.feature_name()) != names:
            raise AdvisoryModelFirstError(
                "meta-label model feature order differs from its frozen schema",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            )
        probability = booster.predict(matrix)
    except AdvisoryModelFirstError:
        raise
    except Exception as exc:
        raise AdvisoryModelFirstError(
            "meta-label model scoring failed",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    probability = np.asarray(probability, dtype=float)
    if probability.ndim != 1 or len(probability) != len(features):
        raise AdvisoryModelFirstError(
            "meta-label model returned an invalid probability shape",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
            context={"expected_rows": len(features), "actual_shape": list(probability.shape)},
        )
    if not np.isfinite(probability).all() or ((probability < 0.0) | (probability > 1.0)).any():
        raise AdvisoryModelFirstError(
            "meta-label model returned an invalid probability",
            reason_code="ADVISORY_META_LABEL_SCORING_INVALID",
        )
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


def _read_runtime_json(path: Path, *, expected_type: type) -> Any:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "meta-label runtime bundle file cannot be read",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            context={"filename": path.name},
        ) from exc
    if not isinstance(payload, expected_type):
        raise AdvisoryModelFirstError(
            "meta-label runtime bundle file has an invalid shape",
            reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            context={"filename": path.name},
        )
    return payload


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


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


def _stable_hmm_payload(payload: Any) -> Any:
    """Canonicalize harmless BLAS-scale float jitter in serialized HMM evidence."""
    if isinstance(payload, Mapping):
        result = {str(key): _stable_hmm_payload(value) for key, value in payload.items()}
        if "final_log_likelihood_delta" in result:
            result.pop("final_log_likelihood_delta")
            result["final_log_likelihood_status"] = "NON_REGRESSING"
        if result.get("reason") == "fit_likelihood_regressed" and "log_likelihood_delta" in result:
            result.pop("log_likelihood_delta")
            result["log_likelihood_status"] = "REGRESSED_BEYOND_TOLERANCE"
        return result
    if isinstance(payload, (list, tuple)):
        return [_stable_hmm_payload(value) for value in payload]
    if isinstance(payload, (float, np.floating)):
        value = float(payload)
        if not isfinite(value):
            raise AdvisoryModelFirstError(
                "fresh HMM evidence contains a non-finite value",
                reason_code="ADVISORY_META_LABEL_BUNDLE_INVALID",
            )
        return float(f"{value:.8g}")
    if isinstance(payload, np.integer):
        return int(payload)
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
