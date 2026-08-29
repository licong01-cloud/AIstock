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
from backend.services.advisory_model_first.meta_label_bundle import stable_hmm_payload
from backend.services.advisory_model_first.policy_dataset_bundle import json_ready
from backend.services.advisory_model_first.selection_liability_gate_contracts import (
    FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


LIABILITY_MODEL_FILENAME = "liability_model.txt"
NON_IDENTITY_FILENAMES = {
    "training_request.json",
    "training_log.json",
    "resource_report.json",
}
REQUIRED_EVIDENCE_FILENAMES = {
    "training_request.json",
    "policy_dataset_manifest.json",
    "selection_liability_gate_feature_schema.json",
    "fresh_hmm_models.json",
    "fresh_hmm_unavailable.json",
    "walk_forward_hmm_receipt.json",
    "threshold_receipt.json",
    "coverage_receipt.json",
    "selection_liability_gate_transform_receipt.json",
    "family_specs.json",
    "cpcv_trial_metrics.parquet",
    "cpcv_block_scores.parquet",
    "candidate_diagnostics.json",
    "pbo_receipt.json",
    "winner_receipt.json",
    "baseline_comparison.json",
    "reference_comparison.json",
    "advancement_receipt.json",
    "training_log.json",
    "resource_report.json",
}


def publish_selection_liability_gate_bundle(
    *,
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
    liability_booster: Any | None,
    feature_schema: Mapping[str, Any],
    runtime_hmm_models: Mapping[str, Any],
    runtime_hmm_unavailable: list[dict[str, Any]],
    walk_forward_hmm_receipt: Mapping[str, Any],
    threshold_receipt: Mapping[str, Any],
    coverage_receipt: Mapping[str, Any],
    transform_receipt: Mapping[str, Any],
    trial_metrics: pd.DataFrame,
    block_scores: pd.DataFrame,
    candidate_diagnostics: Mapping[str, Any],
    pbo_receipt: Mapping[str, Any],
    winner_receipt: Mapping[str, Any],
    baseline_comparison: Mapping[str, Any],
    reference_comparison: Mapping[str, Any],
    advancement_receipt: Mapping[str, Any],
    training_log: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(request.output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    target_root = root / "selection_liability_gate_bundles"
    target_root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="advisory_selection_liability_gate_", dir=root))
    try:
        if liability_booster is not None:
            liability_booster.save_model(str(temp / LIABILITY_MODEL_FILENAME))
        request.write_json(temp / "training_request.json")
        shutil.copyfile(
            Path(request.policy_dataset_bundle_root) / "manifest.json",
            temp / "policy_dataset_manifest.json",
        )
        _write_json(temp / "selection_liability_gate_feature_schema.json", dict(feature_schema))
        _write_json(temp / "fresh_hmm_models.json", stable_hmm_payload(dict(runtime_hmm_models)))
        _write_json(temp / "fresh_hmm_unavailable.json", stable_hmm_payload(runtime_hmm_unavailable))
        _write_json(temp / "walk_forward_hmm_receipt.json", dict(walk_forward_hmm_receipt))
        _write_json(temp / "threshold_receipt.json", dict(threshold_receipt))
        _write_json(temp / "coverage_receipt.json", dict(coverage_receipt))
        _write_json(temp / "selection_liability_gate_transform_receipt.json", dict(transform_receipt))
        _write_json(temp / "family_specs.json", [item.model_dump(mode="json") for item in request.family_specs])
        trial_metrics.to_parquet(temp / "cpcv_trial_metrics.parquet", index=False)
        block_scores.to_parquet(temp / "cpcv_block_scores.parquet", index=False)
        _write_json(temp / "candidate_diagnostics.json", dict(candidate_diagnostics))
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
            if name not in NON_IDENTITY_FILENAMES
        }
        model_available = liability_booster is not None
        payload = {
            "schema_version": "advisory_selection_liability_gate_bundle_v1",
            "stage": "P0_K_V1_STAGE_A_OFFLINE",
            "experiment_status": advancement_receipt["experiment_status"],
            "stage_b_eligible": bool(advancement_receipt.get("advanced_to_stage_b", False)),
            "runtime_eligible": False,
            "model_available": model_available,
            "model_files": ({"liability": LIABILITY_MODEL_FILENAME} if model_available else {}),
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
            "winner": dict(winner_receipt) if model_available else {},
            "model_role": request.model_role,
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(payload)
        manifest = {"bundle_id": bundle_id, **payload, "files": files}
        _write_json(temp / "manifest.json", manifest)
        target = target_root / bundle_id
        if target.exists():
            existing = load_selection_liability_gate_bundle(
                target,
                expected_bundle_id=bundle_id,
                load_booster=False,
            )
            existing_identity = {
                key: value
                for key, value in existing["manifest"].items()
                if key not in {"bundle_id", "files"}
            }
            if existing_identity != payload:
                raise _bundle_error("existing selection-liability-gate bundle differs from its content identity")
            return bundle_id, target, existing["manifest"]
        os.replace(temp, target)
        load_selection_liability_gate_bundle(target, expected_bundle_id=bundle_id, load_booster=False)
        return bundle_id, target, manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp)


def find_selection_liability_gate_bundle_for_request(
    request: FrozenAdvisorySelectionLiabilityGateTrainingRequestV1,
) -> tuple[str, Path, dict[str, Any]] | None:
    target_root = Path(request.output_root).resolve() / "selection_liability_gate_bundles"
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
        loaded = load_selection_liability_gate_bundle(
            candidate,
            expected_bundle_id=str(manifest.get("bundle_id", "")),
            load_booster=False,
        )
        try:
            frozen = FrozenAdvisorySelectionLiabilityGateTrainingRequestV1.model_validate_json(
                (candidate / "training_request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise _bundle_error("selection-liability-gate bundle request cannot be read") from exc
        if frozen.functional_payload() != request.functional_payload():
            raise _bundle_error("selection-liability-gate bundle differs from exact retry request")
        matches.append((str(manifest["bundle_id"]), candidate, loaded["manifest"]))
    if len(matches) > 1:
        raise AdvisoryModelFirstError(
            "multiple selection-liability-gate bundles claim the same request",
            reason_code="ADVISORY_P0K_RETRY_MISMATCH",
        )
    return matches[0] if matches else None


def load_selection_liability_gate_bundle(
    bundle_path: str | Path,
    *,
    expected_bundle_id: str,
    load_booster: bool = True,
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _bundle_error("selection-liability-gate manifest cannot be read") from exc
    actual = canonical_json_sha256(
        {key: value for key, value in manifest.items() if key not in {"bundle_id", "files"}}
    )
    if (
        root.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or actual != expected_bundle_id
    ):
        raise _bundle_error("selection-liability-gate bundle identity is invalid")
    declared_files = manifest.get("files")
    if not isinstance(declared_files, dict):
        raise _bundle_error("selection-liability-gate bundle file manifest is invalid")
    actual_files = {
        path.name for path in root.iterdir() if path.is_file() and path.name != "manifest.json"
    }
    model_declared = LIABILITY_MODEL_FILENAME in declared_files
    expected_files = REQUIRED_EVIDENCE_FILENAMES | (
        {LIABILITY_MODEL_FILENAME} if model_declared else set()
    )
    if actual_files != set(declared_files) or set(declared_files) != expected_files:
        raise _bundle_error("selection-liability-gate bundle has undeclared or missing files")
    if bool(manifest.get("model_available")) != model_declared:
        raise _bundle_error("selection-liability-gate model availability differs from file identity")
    experiment_status = manifest.get("experiment_status")
    stage_b_eligible = manifest.get("stage_b_eligible")
    winner = manifest.get("winner")
    if (
        manifest.get("runtime_eligible") is not False
        or manifest.get("activated") is not False
        or (not model_declared and winner)
        or (model_declared and not isinstance(winner, dict))
        or (model_declared and not winner)
        or (not model_declared and experiment_status != "NEGATIVE_STOP_INCOMPLETE_CPCV")
        or (model_declared and experiment_status == "NEGATIVE_STOP_INCOMPLETE_CPCV")
        or (not model_declared and stage_b_eligible is not False)
        or stage_b_eligible is not (experiment_status == "ADVANCED_TO_STAGE_B")
    ):
        raise _bundle_error("selection-liability-gate offline or incomplete boundary is invalid")
    identity_files = manifest.get("identity_files")
    expected_identity_files = {
        name: descriptor
        for name, descriptor in declared_files.items()
        if name not in NON_IDENTITY_FILENAMES
    }
    if identity_files != expected_identity_files:
        raise _bundle_error("selection-liability-gate identity files differ from declared files")
    for name, descriptor in declared_files.items():
        if (
            not isinstance(name, str)
            or not isinstance(descriptor, dict)
            or set(descriptor) != {"sha256", "size_bytes"}
            or not isinstance(descriptor.get("sha256"), str)
            or len(descriptor["sha256"]) != 64
            or not isinstance(descriptor.get("size_bytes"), int)
            or descriptor["size_bytes"] < 0
        ):
            raise _bundle_error("selection-liability-gate file descriptor is invalid")
        path = (root / name).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise _bundle_error("selection-liability-gate bundle file escapes its root") from exc
        if (
            not path.is_file()
            or path.stat().st_size != int(descriptor["size_bytes"])
            or _sha256(path) != descriptor["sha256"]
        ):
            raise _bundle_error("selection-liability-gate bundle file differs from manifest", filename=name)
    result: dict[str, Any] = {"manifest": manifest, "bundle_path": root}
    if load_booster:
        if not manifest.get("model_available"):
            raise _bundle_error("incomplete selection-liability-gate experiment has no loadable model")
        try:
            import lightgbm as lgb

            result["liability_booster"] = lgb.Booster(model_file=str(root / LIABILITY_MODEL_FILENAME))
        except Exception as exc:
            raise _bundle_error("selection-liability-gate model cannot be loaded") from exc
    return result


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            _finite_json_ready(json_ready(payload)),
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
        reason_code="ADVISORY_P0K_RETRY_MISMATCH",
        context=context or None,
    )
