from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.contracts import build_frozen_training_request  # noqa: E402
from backend.services.advisory_model_first.prediction_source import (  # noqa: E402
    ExactPredictionSource,
    sha256_file,
)
from backend.services.advisory_model_first.target_binding import (  # noqa: E402
    BINDING_VERSION_ID,
    EFFECTIVE_PACKAGE_OOS_CUTOFF,
    FULL_SEED_ROSTER,
    MANIFEST_SHA256,
    PACKAGE_ID,
    PROGRAM_ID,
    REPRESENTATIVE_MODEL_ASSET_SHA256,
    REPRESENTATIVE_SEED_RUN_IDS,
    RUNTIME_SEMANTICS_HASH,
    RUNTIME_SEMANTICS_ID,
    RUNTIME_SEMANTICS_PAYLOAD,
    STYLE_PROFILE_HASH,
    STYLE_PROFILE_ID,
    TERMINAL_WEIGHTS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the exact file-only Advisory model-first training request."
    )
    parser.add_argument("--prediction-store-root-windows", required=True)
    parser.add_argument("--prediction-store-root-wsl", required=True)
    parser.add_argument("--combined-reference-windows", required=True)
    parser.add_argument("--combined-reference-wsl", required=True)
    parser.add_argument("--qlib-daily-root-wsl", required=True)
    parser.add_argument("--factor-data-root-wsl", required=True)
    parser.add_argument("--suspend-data-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--package-asset-closure-hash", required=True)
    parser.add_argument(
        "--historical-weight-rows-json",
        help="Optional explicit JSON file containing the scheme-32 per-window rows for diagnostics.",
    )
    parser.add_argument("--request-output", required=True)
    return parser.parse_args()


def _git_commit(repository_root: Path) -> str:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    commit = result.stdout.strip().lower()
    if len(commit) != 40:
        raise RuntimeError(f"repository HEAD is not a full commit SHA: {commit!r}")
    return commit


def _load_weight_rows(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise ValueError("historical weight rows JSON must be an array of objects")
    return tuple(payload)


def main() -> int:
    args = parse_args()
    prediction_store = Path(args.prediction_store_root_windows).resolve()
    combined_reference = Path(args.combined_reference_windows).resolve()
    repository_root = Path(args.repository_root_windows).resolve()
    if not prediction_store.is_dir():
        raise FileNotFoundError(f"prediction store root does not exist: {prediction_store}")
    if not combined_reference.is_file():
        raise FileNotFoundError(f"combined reference does not exist: {combined_reference}")
    if not repository_root.is_dir():
        raise FileNotFoundError(f"repository root does not exist: {repository_root}")

    source = ExactPredictionSource(prediction_store)
    roster = tuple(run_id for values in FULL_SEED_ROSTER.values() for run_id in values)
    descriptors = source.describe_all(roster)
    request = build_frozen_training_request(
        package_id=PACKAGE_ID,
        manifest_sha256=MANIFEST_SHA256,
        package_asset_closure_hash=args.package_asset_closure_hash.lower(),
        program_id=PROGRAM_ID,
        binding_version_id=BINDING_VERSION_ID,
        style_profile_id=STYLE_PROFILE_ID,
        style_profile_hash=STYLE_PROFILE_HASH,
        effective_package_oos_cutoff=EFFECTIVE_PACKAGE_OOS_CUTOFF,
        selection_runtime_semantics_id=RUNTIME_SEMANTICS_ID,
        selection_runtime_semantics_hash=RUNTIME_SEMANTICS_HASH,
        selection_runtime_semantics=RUNTIME_SEMANTICS_PAYLOAD,
        representative_seed_run_ids=REPRESENTATIVE_SEED_RUN_IDS,
        representative_model_asset_sha256=REPRESENTATIVE_MODEL_ASSET_SHA256,
        full_seed_roster={key: tuple(values) for key, values in FULL_SEED_ROSTER.items()},
        prediction_artifacts=descriptors,
        terminal_weights=TERMINAL_WEIGHTS,
        historical_weight_rows=_load_weight_rows(args.historical_weight_rows_json),
        combined_reference_path=args.combined_reference_wsl,
        combined_reference_sha256=sha256_file(combined_reference),
        qlib_daily_root=args.qlib_daily_root_wsl,
        factor_data_root=args.factor_data_root_wsl,
        suspend_data_root=args.suspend_data_root_wsl,
        prediction_store_root=args.prediction_store_root_wsl,
        repository_root=args.repository_root_wsl,
        repository_commit=_git_commit(repository_root),
        output_root=args.output_root_wsl,
    )
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "ready",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "prediction_artifact_count": len(request.prediction_artifacts),
                "historical_weight_row_count": len(request.historical_weight_rows),
                "request_output": str(Path(args.request_output).resolve()),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
