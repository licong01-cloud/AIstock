from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pandas as pd

from backend.services.advisory_model_first.contracts import FrozenAdvisoryTrainingRequestV1
from backend.services.advisory_model_first.outcome_contracts import (
    OutcomeInputArtifactV1,
    build_frozen_outcome_training_request,
)
from backend.services.advisory_model_first.prediction_source import sha256_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Freeze an Advisory M3 outcome training request.")
    parser.add_argument("--parent-bundle-windows", required=True)
    parser.add_argument("--parent-run-windows", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--request-output-dir-windows", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    bundle = Path(args.parent_bundle_windows).resolve()
    run_root = Path(args.parent_run_windows).resolve()
    repository = Path(args.repository_root_windows).resolve()
    parent_request_path = bundle / "training_request.json"
    feature_schema_path = bundle / "feature_schema.json"
    parent_manifest_path = bundle / "manifest.json"
    parent_request = FrozenAdvisoryTrainingRequestV1.model_validate_json(
        parent_request_path.read_text(encoding="utf-8")
    )
    parent_manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    feature_schema = json.loads(feature_schema_path.read_text(encoding="utf-8"))
    candidates_path = run_root / "candidates.parquet"
    features_path = run_root / "features.parquet"
    parent_test_predictions_path = bundle / "test_predictions.parquet"
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    request = build_frozen_outcome_training_request(
        parent_request_id=parent_request.request_id,
        parent_request_sha256=parent_request.request_sha256,
        parent_bundle_id=str(parent_manifest["bundle_id"]),
        parent_bundle_manifest_file_sha256=sha256_file(parent_manifest_path),
        package_id=parent_request.package_id,
        manifest_sha256=parent_request.manifest_sha256,
        style_profile_id=parent_request.style_profile_id,
        style_profile_hash=parent_request.style_profile_hash,
        feature_schema_version=parent_request.feature_schema_version,
        feature_schema_hash=str(feature_schema["feature_schema_hash"]),
        candidate_semantics_id="OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2",
        candidates_artifact=_artifact(candidates_path),
        features_artifact=_artifact(features_path),
        parent_test_predictions_artifact=_artifact(parent_test_predictions_path),
        parent_training_request_path=_wsl_path(parent_request_path),
        parent_feature_schema_path=_wsl_path(feature_schema_path),
        qlib_daily_root=parent_request.qlib_daily_root,
        suspend_data_root=parent_request.suspend_data_root,
        repository_root=args.repository_root_wsl,
        repository_commit=commit,
        output_root=parent_request.output_root,
        decision_date_start=parent_request.decision_date_start,
        decision_date_end=parent_request.decision_date_end,
        data_cutoff=parent_request.data_cutoff,
    )
    output = Path(args.request_output_dir_windows).resolve() / f"{request.request_id}.json"
    request.write_json(output)
    print(json.dumps({"request_id": request.request_id, "request_sha256": request.request_sha256, "path": str(output)}, sort_keys=True))
    return 0


def _artifact(path: Path) -> OutcomeInputArtifactV1:
    if not path.is_file():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path)
    return OutcomeInputArtifactV1(
        path=_wsl_path(path),
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=len(frame),
        columns=tuple(str(value) for value in frame.columns),
    )


def _wsl_path(path: Path) -> str:
    return subprocess.run(
        ["wsl", "wslpath", "-u", str(path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
