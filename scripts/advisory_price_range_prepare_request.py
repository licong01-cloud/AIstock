from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.contracts import (  # noqa: E402
    FrozenAdvisoryTrainingRequestV1,
)
from backend.services.advisory_model_first.outcome_contracts import (  # noqa: E402
    FrozenAdvisoryOutcomeTrainingRequestV1,
)
from backend.services.advisory_model_first.prediction_source import sha256_file  # noqa: E402
from backend.services.advisory_model_first.price_range_contracts import (  # noqa: E402
    PriceRangeInputArtifactV1,
    build_frozen_price_range_training_request,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Freeze an Advisory M4A price-range training request."
    )
    parser.add_argument("--parent-bundle-windows", required=True)
    parser.add_argument("--outcome-bundle-windows", required=True)
    parser.add_argument("--parent-run-windows", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--request-output-dir-windows", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    parent_bundle = Path(args.parent_bundle_windows).resolve()
    outcome_bundle = Path(args.outcome_bundle_windows).resolve()
    run_root = Path(args.parent_run_windows).resolve()
    repository = Path(args.repository_root_windows).resolve()
    parent_request_path = parent_bundle / "training_request.json"
    parent_schema_path = parent_bundle / "feature_schema.json"
    parent_manifest_path = parent_bundle / "manifest.json"
    outcome_request_path = outcome_bundle / "training_request.json"
    outcome_split_path = outcome_bundle / "split.json"
    outcome_manifest_path = outcome_bundle / "manifest.json"
    parent_request = FrozenAdvisoryTrainingRequestV1.model_validate_json(
        parent_request_path.read_text(encoding="utf-8")
    )
    outcome_request = FrozenAdvisoryOutcomeTrainingRequestV1.model_validate_json(
        outcome_request_path.read_text(encoding="utf-8")
    )
    parent_manifest = json.loads(parent_manifest_path.read_text(encoding="utf-8"))
    outcome_manifest = json.loads(outcome_manifest_path.read_text(encoding="utf-8"))
    feature_schema = json.loads(parent_schema_path.read_text(encoding="utf-8"))
    candidates_path = run_root / "candidates.parquet"
    features_path = run_root / "features.parquet"
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    request = build_frozen_price_range_training_request(
        parent_request_id=parent_request.request_id,
        parent_request_sha256=parent_request.request_sha256,
        parent_bundle_id=str(parent_manifest["bundle_id"]),
        parent_bundle_manifest_file_sha256=sha256_file(parent_manifest_path),
        outcome_request_id=outcome_request.request_id,
        outcome_request_sha256=outcome_request.request_sha256,
        outcome_bundle_id=str(outcome_manifest["outcome_bundle_id"]),
        outcome_bundle_manifest_file_sha256=sha256_file(outcome_manifest_path),
        package_id=parent_request.package_id,
        manifest_sha256=parent_request.manifest_sha256,
        style_profile_id=parent_request.style_profile_id,
        style_profile_hash=parent_request.style_profile_hash,
        feature_schema_version=parent_request.feature_schema_version,
        feature_schema_hash=str(feature_schema["feature_schema_hash"]),
        candidate_semantics_id="OFFLINE_RUNTIME_EQUIVALENT_SELECTION_EFFECTIVE_TOP20_V2",
        candidates_artifact=_artifact(candidates_path),
        features_artifact=_artifact(features_path),
        parent_training_request_path=_wsl_path(parent_request_path),
        parent_feature_schema_path=_wsl_path(parent_schema_path),
        outcome_training_request_path=_wsl_path(outcome_request_path),
        outcome_split_path=_wsl_path(outcome_split_path),
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
    print(
        json.dumps(
            {
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "path": str(output),
            },
            sort_keys=True,
        )
    )
    return 0


def _artifact(path: Path) -> PriceRangeInputArtifactV1:
    if not path.is_file():
        raise FileNotFoundError(path)
    frame = pd.read_parquet(path)
    return PriceRangeInputArtifactV1(
        path=_wsl_path(path),
        sha256=sha256_file(path),
        size_bytes=path.stat().st_size,
        row_count=len(frame),
        columns=tuple(str(value) for value in frame.columns),
    )


def _wsl_path(path: Path) -> str:
    return subprocess.run(
        ["wsl", "bash", "-lc", f"wslpath -u {shlex.quote(str(path))}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


if __name__ == "__main__":
    raise SystemExit(main())
