from __future__ import annotations

import argparse
import json
import shlex
import subprocess
import sys
from pathlib import Path, PurePosixPath

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.quality_contracts import (  # noqa: E402
    M5A_PARENT_BUNDLE_ID,
    QualityProjectionDescriptor,
)
from backend.services.advisory_model_first.quality_pipeline import (  # noqa: E402
    create_quality_train_request,
    prepare_quality_projections,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare the frozen M5A train/validation and test projections.")
    parser.add_argument("--model-root-windows", required=True)
    parser.add_argument("--model-root-wsl", required=True)
    parser.add_argument("--projection-root-windows", required=True)
    parser.add_argument("--projection-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    parser.add_argument("--conda-env", default="rdagent-gpu")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repository_root = Path(args.repository_root_windows).resolve()
    model_root = Path(args.model_root_windows).resolve()
    if not repository_root.is_dir() or not model_root.is_dir():
        raise FileNotFoundError("repository root and model root must exist")
    commit = _git_commit(repository_root)
    lightgbm_version = _wsl_lightgbm_version(args.conda_env)
    receipt = prepare_quality_projections(
        model_root=model_root,
        parent_bundle_id=M5A_PARENT_BUNDLE_ID,
        projection_root=args.projection_root_windows,
        projection_root_for_request=args.projection_root_wsl,
    )
    projection = QualityProjectionDescriptor.model_validate(receipt["train_validation_projection"])
    request = create_quality_train_request(
        model_root=model_root,
        parent_bundle_id=M5A_PARENT_BUNDLE_ID,
        train_validation_projection=projection,
        output_root=args.output_root_wsl,
        repository_root=args.repository_root_wsl,
        repository_commit=commit,
        lightgbm_version=lightgbm_version,
        parent_bundle_root_for_request=str(PurePosixPath(args.model_root_wsl) / "bundles" / M5A_PARENT_BUNDLE_ID),
    )
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "ready",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "projection_receipt": str(Path(args.projection_root_windows).resolve() / "projection_receipt.json"),
                "request_output": str(Path(args.request_output).resolve()),
                "lightgbm_version": lightgbm_version,
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


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


def _wsl_lightgbm_version(conda_env: str) -> str:
    command = " && ".join(
        [
            "source /home/lc999/miniconda3/etc/profile.d/conda.sh",
            f"conda activate {shlex.quote(conda_env)}",
            "python -c 'import lightgbm; print(lightgbm.__version__)'",
        ]
    )
    completed = subprocess.run(
        ["wsl", "bash", "-lc", command],
        check=True,
        capture_output=True,
        text=True,
    )
    version = completed.stdout.strip()
    if not version:
        raise RuntimeError("WSL LightGBM version query returned an empty value")
    return version


if __name__ == "__main__":
    raise SystemExit(main())
