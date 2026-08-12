from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

import pyarrow.parquet as pq

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.prediction_source import sha256_file  # noqa: E402
from backend.services.advisory_model_first.price_range_bundle import (  # noqa: E402
    read_price_range_bundle_manifest,
)
from backend.services.advisory_model_first.price_range_calibration_contracts import (  # noqa: E402
    PriceRangeCalibrationArtifactV1,
    build_frozen_price_range_calibration_request,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare a frozen Advisory M5C calibration request.")
    parser.add_argument("--parent-bundle-windows", required=True)
    parser.add_argument("--parent-bundle-wsl", required=True)
    parser.add_argument("--features-windows", required=True)
    parser.add_argument("--features-wsl", required=True)
    parser.add_argument("--price-range-labels-windows", required=True)
    parser.add_argument("--price-range-labels-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    parent_root = Path(args.parent_bundle_windows).resolve()
    parent_id = parent_root.name
    manifest = read_price_range_bundle_manifest(parent_root, expected_bundle_id=parent_id)
    if manifest.get("schema_version") != "advisory_price_range_bundle_v1":
        raise ValueError("M5C parent must be an uncalibrated M4 v1 price-range bundle")
    parent_request = json.loads((parent_root / "training_request.json").read_text(encoding="utf-8"))
    request = build_frozen_price_range_calibration_request(
        output_root=args.output_root_wsl,
        parent_price_range_request_id=str(manifest["request_id"]),
        parent_price_range_request_sha256=str(manifest["request_sha256"]),
        parent_price_range_bundle_id=parent_id,
        parent_price_range_manifest_file_sha256=sha256_file(parent_root / "manifest.json"),
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_id=str(manifest["style_profile_id"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
        feature_schema_version=str(manifest["feature_schema_version"]),
        feature_schema_hash=str(manifest["feature_schema_hash"]),
        label_policy_version=str(manifest["label_policy_version"]),
        split_sha256=sha256_file(parent_root / "split.json"),
        parent_bundle_root=args.parent_bundle_wsl,
        features_artifact=_descriptor(args.features_windows, args.features_wsl),
        price_range_labels_artifact=_descriptor(
            args.price_range_labels_windows, args.price_range_labels_wsl
        ),
        repository_root=args.repository_root_wsl,
        repository_commit=_git_commit(Path(args.repository_root_windows).resolve()),
    )
    frozen_features = parent_request.get("features_artifact") or {}
    if (
        frozen_features.get("path") != request.features_artifact.path
        or frozen_features.get("sha256") != request.features_artifact.sha256
        or frozen_features.get("size_bytes") != request.features_artifact.size_bytes
        or tuple(frozen_features.get("columns") or ()) != request.features_artifact.columns
        or frozen_features.get("row_count") != request.features_artifact.row_count
    ):
        raise ValueError("M5C features differ from parent M4 frozen request")
    request.write_json(args.request_output)
    print(json.dumps({
        "status": "ready", "request_id": request.request_id,
        "request_sha256": request.request_sha256,
        "parent_price_range_bundle_id": request.parent_price_range_bundle_id,
        "request_output": str(Path(args.request_output).resolve()),
    }, ensure_ascii=True, sort_keys=True))
    return 0


def _descriptor(windows_path: str, request_path: str) -> PriceRangeCalibrationArtifactV1:
    path = Path(windows_path).resolve()
    parquet = pq.ParquetFile(path)
    return PriceRangeCalibrationArtifactV1(
        path=request_path, sha256=sha256_file(path), size_bytes=path.stat().st_size,
        row_count=parquet.metadata.num_rows, columns=tuple(parquet.schema_arrow.names),
    )


def _git_commit(root: Path) -> str:
    completed = subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
    commit = completed.stdout.strip().lower()
    if len(commit) != 40:
        raise RuntimeError("repository HEAD is not a full commit SHA")
    return commit


if __name__ == "__main__":
    raise SystemExit(main())
