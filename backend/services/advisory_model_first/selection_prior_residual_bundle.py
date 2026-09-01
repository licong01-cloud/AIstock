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

from backend.services.advisory_model_first.selection_prior_residual_contracts import (
    FrozenAdvisorySelectionPriorResidualTrainingRequestV1,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.meta_label_bundle import _stable_hmm_payload
from backend.services.advisory_model_first.policy_dataset_bundle import _json_ready
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


RESIDUAL_MODEL_FILENAME = "residual_return_model.txt"
LIABILITY_MODEL_FILENAME = "liability_model.txt"


def publish_selection_prior_residual_bundle(
    *,
    request: FrozenAdvisorySelectionPriorResidualTrainingRequestV1,
    residual_booster: Any | None,
    liability_booster: Any | None,
    feature_schema: Mapping[str, Any],
    runtime_hmm_models: Mapping[str, Any],
    runtime_hmm_unavailable: list[dict[str, Any]],
    walk_forward_hmm_receipt: Mapping[str, Any],
    inner_oof_constraint_receipt: Mapping[str, Any],
    selection_prior_receipt: Mapping[str, Any],
    residual_reliability_receipt: Mapping[str, Any],
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
    if (residual_booster is None) != (liability_booster is None):
        raise _bundle_error("selection-prior-residual bundle must publish both models or neither")
    root = Path(request.output_root).resolve()
    target_root = root / "selection_prior_residual_bundles"
    target_root.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix="advisory_selection_prior_residual_", dir=root))
    try:
        if residual_booster is not None and liability_booster is not None:
            residual_booster.save_model(str(temp / RESIDUAL_MODEL_FILENAME))
            liability_booster.save_model(str(temp / LIABILITY_MODEL_FILENAME))
        request.write_json(temp / "training_request.json")
        shutil.copyfile(
            Path(request.policy_dataset_bundle_root) / "manifest.json",
            temp / "policy_dataset_manifest.json",
        )
        _write_json(temp / "selection_prior_residual_feature_schema.json", dict(feature_schema))
        _write_json(temp / "fresh_hmm_models.json", _stable_hmm_payload(dict(runtime_hmm_models)))
        _write_json(temp / "fresh_hmm_unavailable.json", _stable_hmm_payload(runtime_hmm_unavailable))
        _write_json(temp / "walk_forward_hmm_receipt.json", dict(walk_forward_hmm_receipt))
        _write_json(temp / "inner_oof_constraint_receipt.json", dict(inner_oof_constraint_receipt))
        _write_json(temp / "selection_prior_receipt.json", dict(selection_prior_receipt))
        _write_json(
            temp / "residual_reliability_receipt.json",
            dict(residual_reliability_receipt),
        )
        _write_json(temp / "selection_prior_residual_transform_receipt.json", dict(transform_receipt))
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
            if name not in {"training_request.json", "training_log.json", "resource_report.json"}
        }
        model_available = residual_booster is not None
        payload = {
            "schema_version": "advisory_selection_prior_residual_bundle_v1",
            "stage": "P0_J_V1_STAGE_A_OFFLINE",
            "experiment_status": advancement_receipt["experiment_status"],
            "stage_b_eligible": bool(advancement_receipt["advanced_to_stage_b"]),
            "runtime_eligible": False,
            "model_available": model_available,
            "model_files": (
                {"residual_return": RESIDUAL_MODEL_FILENAME, "liability": LIABILITY_MODEL_FILENAME}
                if model_available
                else {}
            ),
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
            "selection_prior_method": request.selection_prior_method,
            "reliability_method": request.reliability_method,
            "winner": dict(winner_receipt),
            "model_role": "offline_selection_prior_residual_output_constrained_policy_utility_v1",
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(payload)
        manifest = {"bundle_id": bundle_id, **payload, "files": files}
        _write_json(temp / "manifest.json", manifest)
        target = target_root / bundle_id
        if target.exists():
            existing = load_selection_prior_residual_bundle(target, expected_bundle_id=bundle_id, load_boosters=False)
            existing_identity = {
                key: value
                for key, value in existing["manifest"].items()
                if key not in {"bundle_id", "files"}
            }
            if existing_identity != payload:
                raise _bundle_error("existing selection-prior-residual bundle differs from its content identity")
            return bundle_id, target, existing["manifest"]
        os.replace(temp, target)
        load_selection_prior_residual_bundle(target, expected_bundle_id=bundle_id, load_boosters=False)
        return bundle_id, target, manifest
    finally:
        if temp.exists():
            shutil.rmtree(temp)


def find_selection_prior_residual_bundle_for_request(
    request: FrozenAdvisorySelectionPriorResidualTrainingRequestV1,
) -> tuple[str, Path, dict[str, Any]] | None:
    target_root = Path(request.output_root).resolve() / "selection_prior_residual_bundles"
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
        loaded = load_selection_prior_residual_bundle(
            candidate,
            expected_bundle_id=str(manifest.get("bundle_id", "")),
            load_boosters=False,
        )
        try:
            frozen = FrozenAdvisorySelectionPriorResidualTrainingRequestV1.model_validate_json(
                (candidate / "training_request.json").read_text(encoding="utf-8")
            )
        except (OSError, ValueError) as exc:
            raise _bundle_error("selection-prior-residual bundle request cannot be read") from exc
        if frozen.functional_payload() != request.functional_payload():
            raise _bundle_error("selection-prior-residual bundle request differs from exact retry request")
        matches.append((str(manifest["bundle_id"]), candidate, loaded["manifest"]))
    if len(matches) > 1:
        raise AdvisoryModelFirstError(
            "multiple selection-prior-residual bundles claim the same request",
            reason_code="ADVISORY_SELECTION_PRIOR_RESIDUAL_RETRY_MISMATCH",
        )
    return matches[0] if matches else None


def load_selection_prior_residual_bundle(
    bundle_path: str | Path,
    *,
    expected_bundle_id: str,
    load_boosters: bool = True,
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    try:
        manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise _bundle_error("selection-prior-residual manifest cannot be read") from exc
    actual = canonical_json_sha256(
        {key: value for key, value in manifest.items() if key not in {"bundle_id", "files"}}
    )
    if root.name != expected_bundle_id or manifest.get("bundle_id") != expected_bundle_id or actual != expected_bundle_id:
        raise _bundle_error("selection-prior-residual bundle identity is invalid")
    for name, descriptor in (manifest.get("files") or {}).items():
        path = (root / name).resolve()
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise _bundle_error("selection-prior-residual bundle file escapes its root") from exc
        if (
            not path.is_file()
            or path.stat().st_size != int(descriptor["size_bytes"])
            or _sha256(path) != descriptor["sha256"]
        ):
            raise _bundle_error("selection-prior-residual bundle file differs from manifest", filename=name)
    result: dict[str, Any] = {"manifest": manifest, "bundle_path": root}
    if load_boosters:
        if not manifest.get("model_available"):
            raise _bundle_error("incomplete selection-prior-residual experiment has no loadable models")
        try:
            import lightgbm as lgb

            result["residual_booster"] = lgb.Booster(
                model_file=str(root / RESIDUAL_MODEL_FILENAME)
            )
            result["liability_booster"] = lgb.Booster(model_file=str(root / LIABILITY_MODEL_FILENAME))
        except Exception as exc:
            raise _bundle_error("selection-prior-residual models cannot be loaded") from exc
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
        reason_code="ADVISORY_SELECTION_PRIOR_RESIDUAL_RETRY_MISMATCH",
        context=context or None,
    )
