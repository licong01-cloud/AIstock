from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.policy_utility_bundle import (  # noqa: E402
    load_policy_utility_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_contracts import (  # noqa: E402
    ExactTurnoverUtilityReferenceV1,
    approved_turnover_constrained_utility_families,
    build_frozen_turnover_constrained_utility_request,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one exact Advisory P0-G Stage A turnover-constrained utility request."
    )
    parser.add_argument("--p0f-bundle-root-windows", required=True)
    parser.add_argument("--p0f-bundle-root-wsl", required=True)
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    p0f_root = Path(args.p0f_bundle_root_windows).resolve()
    loaded = load_policy_utility_bundle(
        p0f_root,
        expected_bundle_id=p0f_root.name,
        load_booster=False,
    )
    manifest = loaded["manifest"]
    source = json.loads((p0f_root / "training_request.json").read_text(encoding="utf-8"))
    winners = json.loads((p0f_root / "winner_receipt.json").read_text(encoding="utf-8"))[
        "winner_by_arm"
    ]
    policy_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    policy_manifest = json.loads((policy_root / "manifest.json").read_text(encoding="utf-8"))
    if policy_manifest.get("policy_dataset_bundle_id") != manifest.get("policy_dataset_bundle_id"):
        parser.error("P0-F and P0-C policy dataset identities differ")
    repository_root = Path(args.repository_root_windows).resolve()
    p0d = _reference(
        role="P0D_V2_REFERENCE",
        arm_id="ARM_P0D_V2_BINARY_PARITY",
        bundle_root_windows=p0f_root,
        bundle_root_wsl=args.p0f_bundle_root_wsl,
        winner=winners["ARM_P0D_V2_BINARY_PARITY"],
    )
    p0f = _reference(
        role="P0F_V2_REFERENCE",
        arm_id="ARM_P0F_V2_HUBER_UTILITY",
        bundle_root_windows=p0f_root,
        bundle_root_wsl=args.p0f_bundle_root_wsl,
        winner=winners["ARM_P0F_V2_HUBER_UTILITY"],
    )
    request = build_frozen_turnover_constrained_utility_request(
        policy_dataset_bundle_root=args.policy_dataset_bundle_root_wsl,
        policy_dataset_bundle_id=manifest["policy_dataset_bundle_id"],
        policy_dataset_manifest_file_sha256=_sha256(policy_root / "manifest.json"),
        program_id=manifest["program_id"],
        binding_version_id=manifest["binding_version_id"],
        package_id=manifest["package_id"],
        manifest_sha256=manifest["manifest_sha256"],
        style_profile_id=source["style_profile_id"],
        style_profile_hash=source["style_profile_hash"],
        shadow_policy_sha256=manifest["shadow_policy_sha256"],
        cost_policy_sha256=manifest["cost_policy_sha256"],
        split_policy_sha256=manifest["split_policy_sha256"],
        qlib_daily_root=source["qlib_daily_root"],
        factor_data_root=source["factor_data_root"],
        factor_data_cutoff=source["factor_data_cutoff"],
        suspend_data_root=source["suspend_data_root"],
        market_calendar_identity=source["market_calendar_identity"],
        suspend_sidecar_identity=source["suspend_sidecar_identity"],
        repository_root=args.repository_root_wsl,
        repository_root_windows=str(repository_root),
        repository_commit=_git_commit(repository_root),
        output_root=args.output_root_wsl,
        family_specs=approved_turnover_constrained_utility_families(),
        exact_p0d_reference=p0d,
        exact_p0f_reference=p0f,
        model_information_cutoff_trade_date=source["model_information_cutoff_trade_date"],
        latest_training_decision_trade_date=source["latest_training_decision_trade_date"],
        latest_training_label_observation_trade_date=source[
            "latest_training_label_observation_trade_date"
        ],
    )
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "READY",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "request_output": str(Path(args.request_output).resolve()),
                "repository_commit": request.repository_commit,
            },
            sort_keys=True,
        )
    )
    return 0


def _reference(
    *,
    role: str,
    arm_id: str,
    bundle_root_windows: Path,
    bundle_root_wsl: str,
    winner: dict[str, object],
) -> ExactTurnoverUtilityReferenceV1:
    return ExactTurnoverUtilityReferenceV1(
        role=role,
        bundle_root=bundle_root_wsl,
        bundle_id=bundle_root_windows.name,
        manifest_file_sha256=_sha256(bundle_root_windows / "manifest.json"),
        arm_id=arm_id,
        winner_family_id=str(winner["family_id"]),
        winner_seed=int(winner["seed"]),
        winner_training_objective=str(winner["training_objective"]),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_commit(root: Path) -> str:
    return (
        subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
        .stdout.strip()
        .lower()
    )


if __name__ == "__main__":
    raise SystemExit(main())
