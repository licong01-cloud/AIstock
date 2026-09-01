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
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_contracts import (
    FrozenAdvisoryP0LTrainingRequestV1,
)
from backend.services.advisory_model_first.policy_dataset_bundle import json_ready
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
    "p0g_anchor_identity.json",
    "p0g_anchored_liability_local_reranker_feature_schema.json",
    "fresh_hmm_models.json",
    "fresh_hmm_unavailable.json",
    "walk_forward_hmm_receipt.json",
    "calibration_receipt.json",
    "intervention_receipt.json",
    "coverage_receipt.json",
    "p0g_anchored_liability_local_reranker_transform_receipt.json",
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


def publish_p0g_anchored_liability_local_reranker_bundle(
    *,
    request: FrozenAdvisoryP0LTrainingRequestV1,
    liability_booster: Any | None,
    feature_schema: Mapping[str, Any],
    runtime_hmm_models: Mapping[str, Any],
    runtime_hmm_unavailable: list[dict[str, Any]],
    walk_forward_hmm_receipt: Mapping[str, Any],
    p0g_anchor_identity: Mapping[str, Any],
    calibration_receipt: Mapping[str, Any],
    intervention_receipt: Mapping[str, Any],
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
    _verify_finite_frame(trial_metrics, "trial metrics")
    _verify_finite_frame(block_scores, "block scores")
    root = Path(request.output_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    target_root = root / "p0g_anchored_liability_local_reranker_bundles"
    target_root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="advisory_p0l_", dir=root))
    try:
        if liability_booster is not None:
            liability_booster.save_model(str(temp / LIABILITY_MODEL_FILENAME))
        request.write_json(temp / "training_request.json")
        shutil.copyfile(
            Path(request.policy_dataset_bundle_root) / "manifest.json",
            temp / "policy_dataset_manifest.json",
        )
        payloads = {
            "p0g_anchor_identity.json": p0g_anchor_identity,
            "p0g_anchored_liability_local_reranker_feature_schema.json": feature_schema,
            "fresh_hmm_models.json": stable_hmm_payload(dict(runtime_hmm_models)),
            "fresh_hmm_unavailable.json": stable_hmm_payload(runtime_hmm_unavailable),
            "walk_forward_hmm_receipt.json": walk_forward_hmm_receipt,
            "calibration_receipt.json": calibration_receipt,
            "intervention_receipt.json": intervention_receipt,
            "coverage_receipt.json": coverage_receipt,
            "p0g_anchored_liability_local_reranker_transform_receipt.json": transform_receipt,
            "family_specs.json": [item.model_dump(mode="json") for item in request.family_specs],
            "candidate_diagnostics.json": candidate_diagnostics,
            "pbo_receipt.json": pbo_receipt,
            "winner_receipt.json": winner_receipt,
            "baseline_comparison.json": baseline_comparison,
            "reference_comparison.json": reference_comparison,
            "advancement_receipt.json": advancement_receipt,
            "training_log.json": training_log,
            "resource_report.json": resource_report,
        }
        for name, payload in payloads.items():
            _write_json(temp / name, payload)
        trial_metrics.to_parquet(temp / "cpcv_trial_metrics.parquet", index=False)
        block_scores.to_parquet(temp / "cpcv_block_scores.parquet", index=False)
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
        manifest_payload = {
            "schema_version": "advisory_p0g_anchored_liability_local_reranker_bundle_v1",
            "stage": "P0_L_V1_STAGE_A_OFFLINE",
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
            "p0g_anchor_bundle_id": request.exact_p0g_anchor_reference.bundle_id,
            "winner": dict(winner_receipt) if model_available else {},
            "model_role": request.model_role,
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(manifest_payload)
        manifest = {"bundle_id": bundle_id, **manifest_payload, "files": files}
        _write_json(temp / "manifest.json", manifest)
        target = target_root / bundle_id
        if target.exists():
            existing = load_p0g_anchored_liability_local_reranker_bundle(
                target,
                expected_bundle_id=bundle_id,
                load_booster=False,
            )
            existing_identity = {
                key: value
                for key, value in existing["manifest"].items()
                if key not in {"bundle_id", "files"}
            }
            if existing_identity != manifest_payload:
                raise _bundle_error("existing P0-L bundle differs from its content identity")
            return bundle_id, target, existing["manifest"]
        os.replace(temp, target)
        load_p0g_anchored_liability_local_reranker_bundle(
            target,
            expected_bundle_id=bundle_id,
            load_booster=False,
        )
        return bundle_id, target, manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp)


def find_p0g_anchored_liability_local_reranker_bundle_for_request(
    request: FrozenAdvisoryP0LTrainingRequestV1,
) -> tuple[str, Path, dict[str, Any]] | None:
    target_root = (
        Path(request.output_root).resolve()
        / "p0g_anchored_liability_local_reranker_bundles"
    )
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
        loaded = load_p0g_anchored_liability_local_reranker_bundle(
            candidate,
            expected_bundle_id=str(manifest.get("bundle_id", "")),
            load_booster=False,
        )
        try:
            frozen = FrozenAdvisoryP0LTrainingRequestV1.model_validate_json(
                (candidate / "training_request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise _bundle_error("P0-L bundle request cannot be read") from exc
        if frozen.functional_payload() != request.functional_payload():
            raise _bundle_error("P0-L bundle differs from exact retry request")
        matches.append((str(manifest["bundle_id"]), candidate, loaded["manifest"]))
    if len(matches) > 1:
        raise _bundle_error("multiple P0-L bundles claim the same request")
    return matches[0] if matches else None


def load_p0g_anchored_liability_local_reranker_bundle(
    bundle_path: str | Path,
    *,
    expected_bundle_id: str,
    load_booster: bool = True,
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _bundle_error("P0-L manifest cannot be read") from exc
    actual = canonical_json_sha256(
        {key: value for key, value in manifest.items() if key not in {"bundle_id", "files"}}
    )
    if (
        root.name != expected_bundle_id
        or manifest.get("bundle_id") != expected_bundle_id
        or actual != expected_bundle_id
    ):
        raise _bundle_error("P0-L bundle identity is invalid")
    declared = manifest.get("files")
    if not isinstance(declared, dict):
        raise _bundle_error("P0-L bundle file manifest is invalid")
    actual_files = {
        path.name for path in root.iterdir() if path.is_file() and path.name != "manifest.json"
    }
    model_declared = LIABILITY_MODEL_FILENAME in declared
    expected_files = REQUIRED_EVIDENCE_FILENAMES | (
        {LIABILITY_MODEL_FILENAME} if model_declared else set()
    )
    if actual_files != set(declared) or set(declared) != expected_files:
        raise _bundle_error("P0-L bundle has undeclared or missing files")
    status = manifest.get("experiment_status")
    stage_b = manifest.get("stage_b_eligible")
    winner = manifest.get("winner")
    if (
        bool(manifest.get("model_available")) != model_declared
        or manifest.get("runtime_eligible") is not False
        or manifest.get("activated") is not False
        or (not model_declared and winner)
        or (model_declared and not winner)
        or (not model_declared and status != "NEGATIVE_STOP_INCOMPLETE_CPCV")
        or (model_declared and status == "NEGATIVE_STOP_INCOMPLETE_CPCV")
        or stage_b is not (status == "ADVANCED_TO_STAGE_B")
    ):
        raise _bundle_error("P0-L offline or incomplete boundary is invalid")
    expected_identity = {
        name: descriptor
        for name, descriptor in declared.items()
        if name not in NON_IDENTITY_FILENAMES
    }
    if manifest.get("identity_files") != expected_identity:
        raise _bundle_error("P0-L identity files differ from declared files")
    for name, descriptor in declared.items():
        if (
            not isinstance(name, str)
            or not isinstance(descriptor, dict)
            or set(descriptor) != {"sha256", "size_bytes"}
            or not isinstance(descriptor.get("sha256"), str)
            or len(descriptor["sha256"]) != 64
            or not isinstance(descriptor.get("size_bytes"), int)
            or descriptor["size_bytes"] < 0
        ):
            raise _bundle_error("P0-L file descriptor is invalid")
        path = (root / name).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise _bundle_error("P0-L bundle file escapes its root") from exc
        if (
            not path.is_file()
            or path.stat().st_size != descriptor["size_bytes"]
            or _sha256(path) != descriptor["sha256"]
        ):
            raise _bundle_error("P0-L bundle file differs from manifest", filename=name)
    result: dict[str, Any] = {"manifest": manifest, "bundle_path": root}
    if load_booster:
        if not model_declared:
            raise _bundle_error("incomplete P0-L experiment has no loadable model")
        try:
            import lightgbm as lgb

            result["liability_booster"] = lgb.Booster(
                model_file=str(root / LIABILITY_MODEL_FILENAME)
            )
        except Exception as exc:
            raise _bundle_error("P0-L liability model cannot be loaded") from exc
    return result


def _verify_finite_frame(frame: pd.DataFrame, label: str) -> None:
    numeric = frame.select_dtypes(include="number")
    if not numeric.empty and numeric.isna().any().any():
        raise _bundle_error(f"P0-L {label} contains null numeric evidence")
    if not numeric.empty and not numeric.apply(lambda values: values.map(isfinite).all()).all():
        raise _bundle_error(f"P0-L {label} contains non-finite evidence")


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
        raise _bundle_error("P0-L JSON evidence contains NaN or Infinity")
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
        reason_code="ADVISORY_P0L_RETRY_MISMATCH",
        context=context or None,
    )
