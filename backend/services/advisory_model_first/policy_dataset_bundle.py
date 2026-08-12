from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import numpy as np
import pyarrow.parquet as pq

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.policy_contracts import FrozenAdvisoryPolicyDatasetRequestV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256


PARQUET_FILES = {
    "candidate_rankings.parquet",
    "candidate_episode_labels.parquet",
    "shadow_selection_daily.parquet",
    "shadow_selection_episodes.parquet",
}


def publish_policy_dataset_bundle(
    *,
    request: FrozenAdvisoryPolicyDatasetRequestV1,
    rankings: pd.DataFrame,
    labels: pd.DataFrame,
    label_coverage: list[dict[str, Any]],
    shadow_daily: pd.DataFrame,
    shadow_episodes: pd.DataFrame,
    shadow_metrics: Mapping[str, Any],
    cpcv_payload: Mapping[str, Any],
    pbo_receipt: Mapping[str, Any],
    source_schema_receipt: Mapping[str, Any],
    resource_report: Mapping[str, Any],
) -> tuple[str, Path, dict[str, Any]]:
    root = Path(request.output_root).resolve()
    bundles_root = root / "policy_datasets"
    bundles_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="advisory_policy_dataset_", dir=root))
    try:
        request.write_json(temporary / "request.json")
        _write_json(temporary / "baseline_policy.json", request.baseline_policy)
        _write_json(temporary / "shadow_policy.json", request.shadow_policy)
        _write_json(temporary / "cost_policy.json", request.cost_policy.model_dump(mode="json"))
        _write_parquet(rankings, temporary / "candidate_rankings.parquet")
        _write_parquet(labels, temporary / "candidate_episode_labels.parquet")
        _write_json(temporary / "candidate_label_coverage.json", label_coverage)
        _write_parquet(shadow_daily, temporary / "shadow_selection_daily.parquet")
        _write_parquet(shadow_episodes, temporary / "shadow_selection_episodes.parquet")
        _write_json(temporary / "shadow_selection_metrics.json", dict(shadow_metrics))
        _write_json(temporary / "cpcv_paths.json", dict(cpcv_payload))
        _write_json(temporary / "pbo_receipt.json", dict(pbo_receipt))
        _write_json(temporary / "source_schema_receipt.json", dict(source_schema_receipt))
        _write_json(temporary / "resource_report.json", dict(resource_report))

        identity_names = {
            "baseline_policy.json",
            "shadow_policy.json",
            "cost_policy.json",
            "candidate_rankings.parquet",
            "candidate_episode_labels.parquet",
            "candidate_label_coverage.json",
            "shadow_selection_daily.parquet",
            "shadow_selection_episodes.parquet",
            "shadow_selection_metrics.json",
            "cpcv_paths.json",
            "pbo_receipt.json",
            "source_schema_receipt.json",
        }
        identity_files = {
            name: _file_descriptor(temporary / name)
            for name in sorted(identity_names)
        }
        bundle_payload = {
            "schema_version": "advisory_policy_dataset_bundle_v1",
            "request_id": request.request_id,
            "request_sha256": request.request_sha256,
            "identity_files": identity_files,
        }
        bundle_id = canonical_json_sha256(bundle_payload)
        all_files = {
            path.name: _file_descriptor(path)
            for path in sorted(temporary.iterdir())
            if path.is_file()
        }
        manifest = {
            **bundle_payload,
            "policy_dataset_bundle_id": bundle_id,
            "program_id": request.program_id,
            "binding_version_id": request.binding_version_id,
            "package_id": request.package_id,
            "manifest_sha256": request.manifest_sha256,
            "shadow_policy_sha256": request.shadow_policy_sha256,
            "cost_policy_sha256": request.cost_policy_sha256,
            "split_policy_sha256": request.split_policy_sha256,
            "files": all_files,
        }
        _write_json(temporary / "manifest.json", manifest)
        target = bundles_root / bundle_id
        if target.exists():
            existing = load_policy_dataset_bundle(target, expected_bundle_id=bundle_id)
            if existing["request_sha256"] != request.request_sha256 or existing["identity_files"] != identity_files:
                raise AdvisoryModelFirstError(
                    "existing policy dataset bundle differs from the same content identity",
                    reason_code="ADVISORY_POLICY_BUNDLE_CONFLICT",
                    context={"bundle_id": bundle_id},
                )
            return bundle_id, target, existing
        os.replace(temporary, target)
        loaded = load_policy_dataset_bundle(target, expected_bundle_id=bundle_id)
        return bundle_id, target, loaded
    finally:
        if temporary.exists():
            shutil.rmtree(temporary)


def load_policy_dataset_bundle(
    bundle_path: str | Path, *, expected_bundle_id: str | None = None
) -> dict[str, Any]:
    root = Path(bundle_path).resolve()
    manifest_path = root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AdvisoryModelFirstError(
            "policy dataset manifest cannot be read",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
            context={"path": str(manifest_path), "error_type": type(exc).__name__},
        ) from exc
    if manifest.get("schema_version") != "advisory_policy_dataset_bundle_v1":
        raise AdvisoryModelFirstError(
            "policy dataset manifest schema is invalid",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
        )
    bundle_id = str(manifest.get("policy_dataset_bundle_id") or "")
    if expected_bundle_id is not None and bundle_id != expected_bundle_id:
        raise AdvisoryModelFirstError(
            "policy dataset bundle identity differs from its expected identity",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
        )
    identity_payload = {
        "schema_version": manifest["schema_version"],
        "request_id": manifest.get("request_id"),
        "request_sha256": manifest.get("request_sha256"),
        "identity_files": manifest.get("identity_files"),
    }
    if canonical_json_sha256(identity_payload) != bundle_id:
        raise AdvisoryModelFirstError(
            "policy dataset bundle content identity is invalid",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
        )
    files = manifest.get("files")
    if not isinstance(files, dict) or not files:
        raise AdvisoryModelFirstError(
            "policy dataset manifest has no file inventory",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
        )
    for name, descriptor in files.items():
        path = root / str(name)
        actual = _file_descriptor(path)
        if actual != descriptor:
            raise AdvisoryModelFirstError(
                "policy dataset file differs from its manifest",
                reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
                context={"filename": name, "expected": descriptor, "actual": actual},
            )
        if name in PARQUET_FILES:
            try:
                actual_rows = int(pq.ParquetFile(path).metadata.num_rows)
            except Exception as exc:
                raise AdvisoryModelFirstError(
                    "policy dataset parquet cannot be read",
                    reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
                    context={"filename": name, "error_type": type(exc).__name__},
                ) from exc
            if int(descriptor.get("row_count", -1)) != actual_rows:
                raise AdvisoryModelFirstError(
                    "policy dataset parquet row count differs from its manifest",
                    reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
                    context={"filename": name},
                )
    try:
        request = FrozenAdvisoryPolicyDatasetRequestV1.model_validate_json(
            (root / "request.json").read_text(encoding="utf-8")
        )
    except (OSError, ValueError) as exc:
        raise AdvisoryModelFirstError(
            "policy dataset frozen request cannot be validated",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
            context={"error_type": type(exc).__name__},
        ) from exc
    expected_request_identity = {
        "request_id": manifest.get("request_id"),
        "request_sha256": manifest.get("request_sha256"),
        "program_id": manifest.get("program_id"),
        "binding_version_id": manifest.get("binding_version_id"),
        "package_id": manifest.get("package_id"),
        "manifest_sha256": manifest.get("manifest_sha256"),
        "shadow_policy_sha256": manifest.get("shadow_policy_sha256"),
        "cost_policy_sha256": manifest.get("cost_policy_sha256"),
        "split_policy_sha256": manifest.get("split_policy_sha256"),
    }
    actual_request_identity = {
        key: getattr(request, key) for key in expected_request_identity
    }
    if actual_request_identity != expected_request_identity:
        raise AdvisoryModelFirstError(
            "policy dataset request identity differs from its manifest",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
            context={"expected": expected_request_identity, "actual": actual_request_identity},
        )
    return manifest


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(
            _json_ready(payload),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_parquet(frame: pd.DataFrame, path: Path) -> None:
    frame.to_parquet(path, index=False)


def _file_descriptor(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise AdvisoryModelFirstError(
            "policy dataset file is missing",
            reason_code="ADVISORY_POLICY_BUNDLE_INVALID",
            context={"path": str(path)},
        )
    descriptor: dict[str, Any] = {
        "sha256": _sha256_file(path),
        "size_bytes": path.stat().st_size,
    }
    if path.suffix == ".parquet":
        descriptor["row_count"] = int(pq.ParquetFile(path).metadata.num_rows)
    return descriptor


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_ready(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return _json_ready(value.item())
    if isinstance(value, Mapping):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported policy dataset JSON value: {type(value).__name__}")
