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

from backend.services.advisory_model_first.policy_contracts import (  # noqa: E402
    AdvisoryPolicyCostV1,
    AdvisoryPolicySplitV1,
    build_frozen_policy_dataset_request,
)
from backend.services.advisory_model_first.prediction_source import ExactPredictionSource  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one explicit file-only Advisory P0-C policy dataset request."
    )
    parser.add_argument("--identity-json", required=True)
    parser.add_argument("--prediction-store-root-windows", required=True)
    parser.add_argument("--prediction-store-root-wsl", required=True)
    parser.add_argument("--qlib-daily-root-wsl", required=True)
    parser.add_argument("--suspend-data-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--decision-date-start", required=True)
    parser.add_argument("--decision-date-end", required=True)
    parser.add_argument("--data-cutoff", required=True)
    parser.add_argument("--buy-cost-bps", required=True, type=float)
    parser.add_argument("--sell-cost-bps", required=True, type=float)
    parser.add_argument("--benchmark-instrument", default="000300.SH")
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    identity = _read_identity(Path(args.identity_json))
    prediction_store = Path(args.prediction_store_root_windows).resolve()
    repository_root = Path(args.repository_root_windows).resolve()
    if not prediction_store.is_dir():
        raise FileNotFoundError(f"prediction store root does not exist: {prediction_store}")
    if not repository_root.is_dir():
        raise FileNotFoundError(f"repository root does not exist: {repository_root}")
    representative = identity["representative_seed_run_ids"]
    descriptors = ExactPredictionSource(prediction_store).describe_all(representative.values())
    cost = AdvisoryPolicyCostV1(
        buy_cost_bps=args.buy_cost_bps,
        sell_cost_bps=args.sell_cost_bps,
        benchmark_instrument=args.benchmark_instrument,
    )
    split = AdvisoryPolicySplitV1()
    request = build_frozen_policy_dataset_request(
        **identity,
        prediction_artifacts=descriptors,
        qlib_daily_root=args.qlib_daily_root_wsl,
        suspend_data_root=args.suspend_data_root_wsl,
        prediction_store_root=args.prediction_store_root_wsl,
        repository_root=args.repository_root_wsl,
        repository_commit=_git_commit(repository_root),
        decision_date_start=args.decision_date_start,
        decision_date_end=args.decision_date_end,
        data_cutoff=args.data_cutoff,
        cost_policy=cost,
        split_policy=split,
        output_root=args.output_root_wsl,
    )
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "READY",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "prediction_artifact_count": len(descriptors),
                "request_output": str(Path(args.request_output).resolve()),
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


def _read_identity(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    required = {
        "program_id",
        "binding_version_id",
        "package_id",
        "manifest_sha256",
        "package_asset_closure_hash",
        "style_profile_id",
        "style_profile_hash",
        "selection_runtime_semantics_id",
        "selection_runtime_semantics_hash",
        "selection_runtime_semantics",
        "representative_seed_run_ids",
        "representative_model_asset_sha256",
        "terminal_weights",
        "baseline_policy",
        "shadow_policy",
    }
    if not isinstance(payload, dict) or set(payload) != required:
        raise ValueError(
            f"identity JSON must contain exactly the approved fields; missing={sorted(required - set(payload or {}))} "
            f"extra={sorted(set(payload or {}) - required)}"
        )
    return payload


def _git_commit(root: Path) -> str:
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower()
    if len(commit) != 40:
        raise RuntimeError("repository HEAD is not a full commit SHA")
    return commit


if __name__ == "__main__":
    raise SystemExit(main())
