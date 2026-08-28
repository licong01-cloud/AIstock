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

from backend.services.advisory_model_first.dual_head_output_constraint_bundle import (  # noqa: E402
    load_dual_head_bundle,
)
from backend.services.advisory_model_first.grouped_rank_output_constraint_bundle import (  # noqa: E402
    load_grouped_rank_bundle,
)
from backend.services.advisory_model_first.policy_utility_bundle import (  # noqa: E402
    load_policy_utility_bundle,
)
from backend.services.advisory_model_first.selection_liability_gate_contracts import (  # noqa: E402
    ExactP0DSelectionLiabilityGateReferenceV1,
    SelectionLiabilityGateEvidenceReferenceV1,
    approved_selection_liability_gate_families,
    build_frozen_selection_liability_gate_request,
)
from backend.services.advisory_model_first.selection_prior_residual_bundle import (  # noqa: E402
    load_selection_prior_residual_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one exact Advisory P0-K Stage A selection-preserving liability-gate request."
    )
    parser.add_argument("--p0f-bundle-root-windows", required=True)
    parser.add_argument("--p0f-bundle-root-wsl", required=True)
    parser.add_argument("--p0h-bundle-root-windows", required=True)
    parser.add_argument("--p0h-bundle-root-wsl", required=True)
    parser.add_argument("--p0i-bundle-root-windows", required=True)
    parser.add_argument("--p0i-bundle-root-wsl", required=True)
    parser.add_argument("--p0j-bundle-root-windows", required=True)
    parser.add_argument("--p0j-bundle-root-wsl", required=True)
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    p0f_root = Path(args.p0f_bundle_root_windows).resolve()
    p0h_root = Path(args.p0h_bundle_root_windows).resolve()
    p0i_root = Path(args.p0i_bundle_root_windows).resolve()
    p0j_root = Path(args.p0j_bundle_root_windows).resolve()
    p0f = load_policy_utility_bundle(p0f_root, expected_bundle_id=p0f_root.name, load_booster=False)
    p0h = load_dual_head_bundle(p0h_root, expected_bundle_id=p0h_root.name, load_boosters=False)
    p0i = load_grouped_rank_bundle(p0i_root, expected_bundle_id=p0i_root.name, load_boosters=False)
    p0j = load_selection_prior_residual_bundle(
        p0j_root,
        expected_bundle_id=p0j_root.name,
        load_boosters=False,
    )
    manifests = [item["manifest"] for item in (p0f, p0h, p0i, p0j)]
    _verify_shared_identity(parser, manifests)
    expected_states = (
        (p0h["manifest"], "NEGATIVE_STOP_NOT_ADVANCED", True),
        (p0i["manifest"], "NEGATIVE_STOP_INCOMPLETE_CPCV", False),
        (p0j["manifest"], "NEGATIVE_STOP_INCOMPLETE_CPCV", False),
    )
    for manifest, status, model_available in expected_states:
        if (
            manifest.get("experiment_status") != status
            or bool(manifest.get("model_available")) != model_available
        ):
            parser.error("P0-H/P0-I/P0-J evidence terminal states differ from frozen design")

    source = json.loads((p0f_root / "training_request.json").read_text(encoding="utf-8"))
    p0d_winner = json.loads((p0f_root / "winner_receipt.json").read_text(encoding="utf-8"))[
        "winner_by_arm"
    ]["ARM_P0D_V2_BINARY_PARITY"]
    policy_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    policy_manifest = json.loads((policy_root / "manifest.json").read_text(encoding="utf-8"))
    if policy_manifest.get("policy_dataset_bundle_id") != p0f["manifest"].get(
        "policy_dataset_bundle_id"
    ):
        parser.error("P0-D and P0-C policy dataset identities differ")
    repository_root = Path(args.repository_root_windows).resolve()
    request = build_frozen_selection_liability_gate_request(
        policy_dataset_bundle_root=args.policy_dataset_bundle_root_wsl,
        policy_dataset_bundle_id=p0f["manifest"]["policy_dataset_bundle_id"],
        policy_dataset_manifest_file_sha256=_sha256(policy_root / "manifest.json"),
        program_id=p0f["manifest"]["program_id"],
        binding_version_id=p0f["manifest"]["binding_version_id"],
        package_id=p0f["manifest"]["package_id"],
        manifest_sha256=p0f["manifest"]["manifest_sha256"],
        style_profile_id=source["style_profile_id"],
        style_profile_hash=source["style_profile_hash"],
        shadow_policy_sha256=p0f["manifest"]["shadow_policy_sha256"],
        cost_policy_sha256=p0f["manifest"]["cost_policy_sha256"],
        split_policy_sha256=p0f["manifest"]["split_policy_sha256"],
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
        family_specs=approved_selection_liability_gate_families(),
        exact_p0d_reference=ExactP0DSelectionLiabilityGateReferenceV1(
            bundle_root=args.p0f_bundle_root_wsl,
            bundle_id=p0f_root.name,
            manifest_file_sha256=_sha256(p0f_root / "manifest.json"),
            winner_family_id=str(p0d_winner["family_id"]),
            winner_seed=int(p0d_winner["seed"]),
            winner_training_objective=str(p0d_winner["training_objective"]),
            winner_boost_rounds=int(p0d_winner["final_boost_rounds"]),
        ),
        p0h_evidence_reference=_evidence(
            role="P0H_V1_EVIDENCE",
            root=p0h_root,
            wsl_root=args.p0h_bundle_root_wsl,
            status="NEGATIVE_STOP_NOT_ADVANCED",
            model_available=True,
        ),
        p0i_evidence_reference=_evidence(
            role="P0I_V1_EVIDENCE",
            root=p0i_root,
            wsl_root=args.p0i_bundle_root_wsl,
            status="NEGATIVE_STOP_INCOMPLETE_CPCV",
            model_available=False,
        ),
        p0j_evidence_reference=_evidence(
            role="P0J_V1_EVIDENCE",
            root=p0j_root,
            wsl_root=args.p0j_bundle_root_wsl,
            status="NEGATIVE_STOP_INCOMPLETE_CPCV",
            model_available=False,
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


def _evidence(
    *,
    role: str,
    root: Path,
    wsl_root: str,
    status: str,
    model_available: bool,
) -> SelectionLiabilityGateEvidenceReferenceV1:
    return SelectionLiabilityGateEvidenceReferenceV1(
        role=role,
        bundle_root=wsl_root,
        bundle_id=root.name,
        manifest_file_sha256=_sha256(root / "manifest.json"),
        expected_experiment_status=status,
        expected_model_available=model_available,
    )


def _verify_shared_identity(
    parser: argparse.ArgumentParser,
    manifests: list[dict[str, object]],
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
    baseline = manifests[0]
    mismatches = [
        key for key in keys if any(item.get(key) != baseline.get(key) for item in manifests[1:])
    ]
    if mismatches:
        parser.error(f"P0-D/P0-H/P0-I/P0-J exact identities differ: {mismatches}")


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
