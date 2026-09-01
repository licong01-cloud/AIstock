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

from backend.services.advisory_model_first.outcome_bundle import read_outcome_bundle_manifest  # noqa: E402
from backend.services.advisory_model_first.outcome_calibration_contracts import (  # noqa: E402
    OutcomeCalibrationArtifactV1,
    build_frozen_outcome_calibration_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare a frozen Advisory M5B calibration request.")
    parser.add_argument("--parent-bundle-windows", required=True)
    parser.add_argument("--parent-bundle-wsl", required=True)
    parser.add_argument("--features-windows", required=True)
    parser.add_argument("--features-wsl", required=True)
    parser.add_argument("--outcome-labels-windows", required=True)
    parser.add_argument("--outcome-labels-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    parent_root = Path(args.parent_bundle_windows).resolve()
    parent_id = parent_root.name
    manifest = read_outcome_bundle_manifest(parent_root, expected_bundle_id=parent_id)
    if manifest.get("schema_version") != "advisory_outcome_bundle_v1":
        raise ValueError("M5B parent must be an uncalibrated M3 v1 outcome bundle")
    parent_request = json.loads((parent_root / "training_request.json").read_text(encoding="utf-8"))
    request = build_frozen_outcome_calibration_request(
        output_root=args.output_root_wsl,
        parent_outcome_request_id=str(manifest["request_id"]),
        parent_outcome_request_sha256=str(manifest["request_sha256"]),
        parent_outcome_bundle_id=parent_id,
        parent_outcome_manifest_file_sha256=sha256_file(parent_root / "manifest.json"),
        package_id=str(manifest["package_id"]),
        manifest_sha256=str(manifest["manifest_sha256"]),
        style_profile_id=str(manifest["style_profile_id"]),
        style_profile_hash=str(manifest["style_profile_hash"]),
        feature_schema_version=str(manifest["feature_schema_version"]),
        feature_schema_hash=str(manifest["feature_schema_hash"]),
        label_policy_version=str(manifest["label_policy_version"]),
        split_sha256=sha256_file(parent_root / "split.json"),
        parent_bundle_root=args.parent_bundle_wsl,
        features_artifact=_parquet_descriptor(
            windows_path=args.features_windows,
            request_path=args.features_wsl,
        ),
        outcome_labels_artifact=_parquet_descriptor(
            windows_path=args.outcome_labels_windows,
            request_path=args.outcome_labels_wsl,
        ),
        repository_root=args.repository_root_wsl,
        repository_commit=_git_commit(Path(args.repository_root_windows).resolve()),
    )
    if (
        parent_request.get("features_artifact", {}).get("sha256")
        != request.features_artifact.sha256
        or tuple(parent_request.get("features_artifact", {}).get("columns") or ())
        != request.features_artifact.columns
    ):
        raise ValueError("M5B features differ from the parent M3 frozen training request")
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "ready",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "parent_outcome_bundle_id": request.parent_outcome_bundle_id,
                "request_output": str(Path(args.request_output).resolve()),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


def _parquet_descriptor(*, windows_path: str, request_path: str) -> OutcomeCalibrationArtifactV1:
    path = Path(windows_path).resolve()
    parquet = pq.ParquetFile(path)
    return OutcomeCalibrationArtifactV1(
        path=request_path,
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=parquet.metadata.num_rows,
        columns=tuple(parquet.schema_arrow.names),
    )


def _git_commit(repository_root: Path) -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    commit = completed.stdout.strip().lower()
    if len(commit) != 40:
        raise RuntimeError("repository HEAD is not a full commit SHA")
    return commit


if __name__ == "__main__":
    raise SystemExit(main())
