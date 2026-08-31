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

from backend.services.advisory_model_first.grouped_rank_output_constraint_contracts import (  # noqa: E402
    ExactGroupedRankReferenceV1,
    approved_grouped_rank_families,
    build_frozen_grouped_rank_request,
)
from backend.services.advisory_model_first.dual_head_output_constraint_bundle import (  # noqa: E402
    load_dual_head_bundle,
)
from backend.services.advisory_model_first.policy_utility_bundle import (  # noqa: E402
    load_policy_utility_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_bundle import (  # noqa: E402
    load_turnover_constrained_utility_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one exact Advisory P0-I Stage A grouped-rank output-constraint request."
    )
    parser.add_argument("--p0f-bundle-root-windows", required=True)
    parser.add_argument("--p0f-bundle-root-wsl", required=True)
    parser.add_argument("--p0g-bundle-root-windows", required=True)
    parser.add_argument("--p0g-bundle-root-wsl", required=True)
    parser.add_argument("--p0h-bundle-root-windows", required=True)
    parser.add_argument("--p0h-bundle-root-wsl", required=True)
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    p0f_root = Path(args.p0f_bundle_root_windows).resolve()
    p0g_root = Path(args.p0g_bundle_root_windows).resolve()
    p0h_root = Path(args.p0h_bundle_root_windows).resolve()
    p0f_loaded = load_policy_utility_bundle(
        p0f_root,
        expected_bundle_id=p0f_root.name,
        load_booster=False,
    )
    p0g_loaded = load_turnover_constrained_utility_bundle(
        p0g_root,
        expected_bundle_id=p0g_root.name,
        load_booster=False,
    )
    p0h_loaded = load_dual_head_bundle(
        p0h_root,
        expected_bundle_id=p0h_root.name,
        load_boosters=False,
    )
    p0f_manifest = p0f_loaded["manifest"]
    p0g_manifest = p0g_loaded["manifest"]
    p0h_manifest = p0h_loaded["manifest"]
    _verify_shared_identity(parser, p0f_manifest, p0g_manifest, p0h_manifest)
    source = json.loads((p0f_root / "training_request.json").read_text(encoding="utf-8"))
    p0f_winners = json.loads((p0f_root / "winner_receipt.json").read_text(encoding="utf-8"))[
        "winner_by_arm"
    ]
    p0g_winner = json.loads((p0g_root / "winner_receipt.json").read_text(encoding="utf-8"))
    p0h_winner = json.loads((p0h_root / "winner_receipt.json").read_text(encoding="utf-8"))
    policy_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    policy_manifest = json.loads((policy_root / "manifest.json").read_text(encoding="utf-8"))
    if policy_manifest.get("policy_dataset_bundle_id") != p0f_manifest.get("policy_dataset_bundle_id"):
        parser.error("P0-F/P0-G and P0-C policy dataset identities differ")
    repository_root = Path(args.repository_root_windows).resolve()
    request = build_frozen_grouped_rank_request(
        policy_dataset_bundle_root=args.policy_dataset_bundle_root_wsl,
        policy_dataset_bundle_id=p0f_manifest["policy_dataset_bundle_id"],
        policy_dataset_manifest_file_sha256=_sha256(policy_root / "manifest.json"),
        program_id=p0f_manifest["program_id"],
        binding_version_id=p0f_manifest["binding_version_id"],
        package_id=p0f_manifest["package_id"],
        manifest_sha256=p0f_manifest["manifest_sha256"],
        style_profile_id=source["style_profile_id"],
        style_profile_hash=source["style_profile_hash"],
        shadow_policy_sha256=p0f_manifest["shadow_policy_sha256"],
        cost_policy_sha256=p0f_manifest["cost_policy_sha256"],
        split_policy_sha256=p0f_manifest["split_policy_sha256"],
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
        family_specs=approved_grouped_rank_families(),
        exact_p0d_reference=_reference(
            role="P0D_V2_REFERENCE",
            arm_id="ARM_P0D_V2_BINARY_PARITY",
            bundle_root_windows=p0f_root,
            bundle_root_wsl=args.p0f_bundle_root_wsl,
            winner=p0f_winners["ARM_P0D_V2_BINARY_PARITY"],
        ),
        exact_p0f_reference=_reference(
            role="P0F_V2_REFERENCE",
            arm_id="ARM_P0F_V2_HUBER_UTILITY",
            bundle_root_windows=p0f_root,
            bundle_root_wsl=args.p0f_bundle_root_wsl,
            winner=p0f_winners["ARM_P0F_V2_HUBER_UTILITY"],
        ),
        exact_p0g_reference=_reference(
            role="P0G_V1_REFERENCE",
            arm_id="ARM_P0G_V1_TURNOVER_CONSTRAINED_UTILITY",
            bundle_root_windows=p0g_root,
            bundle_root_wsl=args.p0g_bundle_root_wsl,
            winner=p0g_winner,
        ),
        exact_p0h_reference=_reference(
            role="P0H_V1_REFERENCE",
            arm_id="ARM_P0H_V1_DUAL_HEAD_OUTPUT_CONSTRAINED_UTILITY",
            bundle_root_windows=p0h_root,
            bundle_root_wsl=args.p0h_bundle_root_wsl,
            winner=p0h_winner,
        ),
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
) -> ExactGroupedRankReferenceV1:
    if role == "P0G_V1_REFERENCE":
        objective = "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1"
        boost_rounds = int(winner["final_boost_rounds"])
    elif role == "P0H_V1_REFERENCE":
        objective = "P0H_DUAL_HEAD_OUTPUT_CONSTRAINT_V1"
        boost_rounds = int(winner["final_return_boost_rounds"])
    else:
        objective = str(winner["training_objective"])
        boost_rounds = int(winner["final_boost_rounds"])
    return ExactGroupedRankReferenceV1(
        role=role,
        bundle_root=bundle_root_wsl,
        bundle_id=bundle_root_windows.name,
        manifest_file_sha256=_sha256(bundle_root_windows / "manifest.json"),
        arm_id=arm_id,
        winner_family_id=str(winner["family_id"]),
        winner_seed=int(winner["seed"]),
        winner_training_objective=objective,
        winner_boost_rounds=boost_rounds,
    )


def _verify_shared_identity(
    parser: argparse.ArgumentParser,
    p0f: dict[str, object],
    p0g: dict[str, object],
    p0h: dict[str, object],
) -> None:
    keys = (
        "policy_dataset_bundle_id",
        "program_id",
        "binding_version_id",
        "package_id",
        "manifest_sha256",
        "shadow_policy_sha256",
        "cost_policy_sha256",
        "split_policy_sha256",
        "feature_schema_hash",
    )
    mismatches = [key for key in keys if p0f.get(key) != p0g.get(key) or p0f.get(key) != p0h.get(key)]
    if mismatches:
        parser.error(f"P0-F/P0-G/P0-H exact reference identities differ: {mismatches}")


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
