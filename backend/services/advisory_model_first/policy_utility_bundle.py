from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from math import isfinite
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_bundle import _stable_hmm_payload
from backend.services.advisory_model_first.policy_dataset_bundle import _json_ready
from backend.services.advisory_model_first.policy_utility_contracts import (
    FrozenAdvisoryPolicyUtilityTrainingRequestV2,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


_ARM_MODEL_FILES = {
    "ARM_P0D_V2_BINARY_PARITY": "p0d_v2_model.txt",
    "ARM_P0E_V2_WEIGHTED_BINARY": "p0e_v2_model.txt",
    "ARM_P0F_V2_HUBER_UTILITY": "p0f_v2_model.txt",
}


def publish_policy_utility_bundle(
    *,
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
    arm_boosters: Mapping[str, Any] | None,
    feature_schema: Mapping[str, Any],
    transform_receipt: Mapping[str, Any],
    runtime_hmm_models: Mapping[str, Any],
    runtime_hmm_unavailable: list[dict[str, Any]],
    walk_forward_hmm_receipt: Mapping[str, Any],
    trial_metrics: pd.DataFrame,
    block_scores: pd.DataFrame,
    pbo_receipt: Mapping[str, Any],
    winner_receipt: Mapping[str, Any],
    baseline_comparison: Mapping[str, Any],
    reference_comparison: Mapping[str, Any],
    advancement_receipt: Mapping[str, Any],
    training_log: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(request.output_root).resolve()
    target_root = root / "policy_utility_bundles"
    target_root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="advisory_policy_utility_", dir=root))
    try:
        if arm_boosters is not None:
            if set(arm_boosters) != set(_ARM_MODEL_FILES):
                raise _bundle_error("policy utility bundle does not contain the exact three-arm models")
            for arm_id, filename in _ARM_MODEL_FILES.items():
                arm_boosters[arm_id].save_model(str(temp / filename))
        request.write_json(temp / "training_request.json")
        shutil.copyfile(
            Path(request.policy_dataset_bundle_root) / "manifest.json", temp / "policy_dataset_manifest.json"
        )
        _write_json(temp / "utility_feature_schema.json", dict(feature_schema))
        _write_json(temp / "utility_transform_receipt.json", dict(transform_receipt))
        _write_json(temp / "fresh_hmm_models.json", _stable_hmm_payload(dict(runtime_hmm_models)))
        _write_json(temp / "fresh_hmm_unavailable.json", _stable_hmm_payload(runtime_hmm_unavailable))
        _write_json(temp / "walk_forward_hmm_receipt.json", dict(walk_forward_hmm_receipt))
        _write_json(temp / "family_specs.json", [item.model_dump(mode="json") for item in request.family_specs])
        _write_json(temp / "arm_specs.json", [item.model_dump(mode="json") for item in request.arm_specs])
        trial_metrics.to_parquet(temp / "cpcv_trial_metrics.parquet", index=False)
        block_scores.to_parquet(temp / "cpcv_block_scores.parquet", index=False)
        _write_json(temp / "pbo_receipt.json", dict(pbo_receipt))
        _write_json(temp / "winner_receipt.json", dict(winner_receipt))
        _write_json(temp / "baseline_comparison.json", dict(baseline_comparison))
        _write_json(temp / "reference_comparison.json", dict(reference_comparison))
        _write_json(temp / "advancement_receipt.json", dict(advancement_receipt))
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
            "schema_version": "advisory_policy_utility_bundle_v2_suspension_aware",
            "stage": "P0_D_E_F_V2_STAGE_A_OFFLINE",
            "experiment_status": advancement_receipt["experiment_status"],
            "stage_b_eligible": bool(advancement_receipt["advanced_to_stage_b"]),
            "runtime_eligible": False,
            "all_arm_models_available": arm_boosters is not None,
            "model_files": _ARM_MODEL_FILES if arm_boosters is not None else {},
            "activated": False,
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "policy_dataset_bundle_id": request.policy_dataset_bundle_id,
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "shadow_policy_sha256": request.shadow_policy_sha256,
            "cost_policy_sha256": request.cost_policy_sha256,
            "split_policy_sha256": request.split_policy_sha256,
            "feature_schema_hash": request.feature_schema_hash,
            "model_information_cutoff_trade_date": request.model_information_cutoff_trade_date,
            "experiment_lineage": list(request.experiment_lineage),
            "winner_by_arm": winner_receipt.get("winner_by_arm", {}),
            "model_role": "offline_advisory_three_arm_comparison_v2",
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(payload)
        manifest = {"bundle_id": bundle_id, **payload, "files": files}
        _write_json(temp / "manifest.json", manifest)
        target = target_root / bundle_id
        if target.exists():
            existing = load_policy_utility_bundle(target, expected_bundle_id=bundle_id, load_booster=False)
            existing_identity = {
                key: value for key, value in existing["manifest"].items() if key not in {"bundle_id", "files"}
            }
            if existing_identity != payload:
                raise _bundle_error("existing policy utility bundle differs from its content identity")
            return bundle_id, target, existing["manifest"]
        os.replace(temp, target)
        load_policy_utility_bundle(target, expected_bundle_id=bundle_id, load_booster=False)
        return bundle_id, target, manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp)


def find_policy_utility_bundle_for_request(
    request: FrozenAdvisoryPolicyUtilityTrainingRequestV2,
) -> tuple[str, Path, dict[str, Any]] | None:
    target_root = Path(request.output_root).resolve() / "policy_utility_bundles"
    if not target_root.is_dir():
        return None
    matches: list[tuple[str, Path, dict[str, Any]]] = []
    for candidate in sorted(target_root.iterdir()):
        if not candidate.is_dir():
            continue
        try:
            manifest = json.loads((candidate / "manifest.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if manifest.get("request_sha256") != request.request_sha256:
            continue
        loaded = load_policy_utility_bundle(
            candidate,
            expected_bundle_id=str(manifest.get("bundle_id", "")),
            load_booster=False,
        )
        try:
            frozen = FrozenAdvisoryPolicyUtilityTrainingRequestV2.model_validate_json(
                (candidate / "training_request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise _bundle_error("policy utility bundle request cannot be read") from exc
        if (
            frozen.functional_payload() != request.functional_payload()
            or frozen.request_sha256 != request.request_sha256
        ):
            raise _bundle_error("policy utility bundle request differs from exact retry request")
        matches.append((str(manifest["bundle_id"]), candidate, loaded["manifest"]))
    if len(matches) > 1:
        raise AdvisoryModelFirstError(
            "multiple policy utility bundles claim the same request",
            reason_code="ADVISORY_POLICY_UTILITY_BUNDLE_IDENTITY_CONFLICT",
        )
    return matches[0] if matches else None


def load_policy_utility_bundle(
    bundle_path: str | Path, *, expected_bundle_id: str, load_booster: bool = True
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _bundle_error("policy utility manifest cannot be read") from exc
    actual = canonical_json_sha256({key: value for key, value in manifest.items() if key not in {"bundle_id", "files"}})
    if (
        root.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or actual != expected_bundle_id
    ):
        raise _bundle_error("policy utility bundle identity is invalid")
    for name, descriptor in (manifest.get("files") or {}).items():
        path = (root / name).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise _bundle_error("policy utility bundle file escapes its root") from exc
        if (
            not path.is_file()
            or path.stat().st_size != int(descriptor["size_bytes"])
            or _sha256(path) != descriptor["sha256"]
        ):
            raise _bundle_error("policy utility bundle file differs from manifest", filename=name)
    result: dict[str, Any] = {"manifest": manifest, "bundle_path": root}
    if load_booster:
        if not manifest.get("all_arm_models_available"):
            raise _bundle_error("incomplete policy utility experiment has no loadable arm models")
        try:
            import lightgbm as lgb

            result["arm_boosters"] = {
                arm_id: lgb.Booster(model_file=str(root / filename)) for arm_id, filename in _ARM_MODEL_FILES.items()
            }
        except Exception as exc:
            raise _bundle_error("policy utility model cannot be loaded") from exc
    return result


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            _finite_json_ready(_json_ready(payload)),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ),
        encoding="utf-8",
    )


def _finite_json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _finite_json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_finite_json_ready(item) for item in value]
    if isinstance(value, float) and not isfinite(value):
        return None
    return value


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _bundle_error(message: str, **context: Any) -> AdvisoryModelFirstError:
    return AdvisoryModelFirstError(
        message,
        reason_code="ADVISORY_POLICY_UTILITY_BUNDLE_INVALID",
        context=context or None,
    )
