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
from backend.services.advisory_model_first.p0g_anchored_liability_local_reranker_contracts import (  # noqa: E402
    ExactP0DReferenceV1,
    ExactP0GAnchorReferenceV1,
    P0LEvidenceReferenceV1,
    approved_p0l_families,
    build_frozen_p0l_request,
)
from backend.services.advisory_model_first.policy_utility_bundle import (  # noqa: E402
    load_policy_utility_bundle,
)
from backend.services.advisory_model_first.selection_liability_gate_bundle import (  # noqa: E402
    load_selection_liability_gate_bundle,
)
from backend.services.advisory_model_first.turnover_constrained_utility_bundle import (  # noqa: E402
    load_turnover_constrained_utility_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one exact Advisory P0-L Stage A anchored local-reranker request."
    )
    for role in ("p0f", "p0g", "p0h", "p0k"):
        parser.add_argument(f"--{role}-bundle-root-windows", required=True)
        parser.add_argument(f"--{role}-bundle-root-wsl", required=True)
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    roots = {
        role: Path(getattr(args, f"{role}_bundle_root_windows")).resolve()
        for role in ("p0f", "p0g", "p0h", "p0k")
    }
    loaded = {
        "p0f": load_policy_utility_bundle(roots["p0f"], expected_bundle_id=roots["p0f"].name, load_booster=False),
        "p0g": load_turnover_constrained_utility_bundle(
            roots["p0g"], expected_bundle_id=roots["p0g"].name, load_booster=False
        ),
        "p0h": load_dual_head_bundle(roots["p0h"], expected_bundle_id=roots["p0h"].name, load_boosters=False),
        "p0k": load_selection_liability_gate_bundle(
            roots["p0k"], expected_bundle_id=roots["p0k"].name, load_booster=False
        ),
    }
    manifests = [loaded[role]["manifest"] for role in ("p0f", "p0g", "p0h", "p0k")]
    _verify_shared_identity(parser, manifests)
    for role in ("p0g", "p0h", "p0k"):
        manifest = loaded[role]["manifest"]
        if (
            manifest.get("experiment_status") != "NEGATIVE_STOP_NOT_ADVANCED"
            or manifest.get("model_available") is not True
        ):
            parser.error(f"{role.upper()} evidence terminal state differs from frozen design")

    p0f_request = _read_json(roots["p0f"] / "training_request.json")
    p0d_winner = _read_json(roots["p0f"] / "winner_receipt.json")["winner_by_arm"][
        "ARM_P0D_V2_BINARY_PARITY"
    ]
    p0g_request = _read_json(roots["p0g"] / "training_request.json")
    p0g_winner = _read_json(roots["p0g"] / "winner_receipt.json")
    expected_p0g = {
        "family_id": "FAMILY_TURNOVER_CONSTRAINED_CORE",
        "seed": 20260817,
        "training_objective": "HUBER_TURNOVER_CONSTRAINED_POLICY_UTILITY_V1",
        "final_boost_rounds": 19,
    }
    actual_p0g = {
        "family_id": p0g_winner.get("family_id"),
        "seed": p0g_winner.get("seed"),
        "training_objective": p0g_request.get("training_objective"),
        "final_boost_rounds": p0g_winner.get("final_boost_rounds"),
    }
    if actual_p0g != expected_p0g:
        parser.error(f"P0-G fixed anchor winner differs from approved design: {actual_p0g}")

    policy_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    policy_manifest = _read_json(policy_root / "manifest.json")
    if policy_manifest.get("policy_dataset_bundle_id") != loaded["p0f"]["manifest"].get(
        "policy_dataset_bundle_id"
    ):
        parser.error("P0-D and P0-C policy dataset identities differ")
    repository_root = Path(args.repository_root_windows).resolve()
    request = build_frozen_p0l_request(
        policy_dataset_bundle_root=args.policy_dataset_bundle_root_wsl,
        policy_dataset_bundle_id=loaded["p0f"]["manifest"]["policy_dataset_bundle_id"],
        policy_dataset_manifest_file_sha256=_sha256(policy_root / "manifest.json"),
        program_id=loaded["p0f"]["manifest"]["program_id"],
        binding_version_id=loaded["p0f"]["manifest"]["binding_version_id"],
        package_id=loaded["p0f"]["manifest"]["package_id"],
        manifest_sha256=loaded["p0f"]["manifest"]["manifest_sha256"],
        style_profile_id=p0f_request["style_profile_id"],
        style_profile_hash=p0f_request["style_profile_hash"],
        shadow_policy_sha256=loaded["p0f"]["manifest"]["shadow_policy_sha256"],
        cost_policy_sha256=loaded["p0f"]["manifest"]["cost_policy_sha256"],
        split_policy_sha256=loaded["p0f"]["manifest"]["split_policy_sha256"],
        qlib_daily_root=p0f_request["qlib_daily_root"],
        factor_data_root=p0f_request["factor_data_root"],
        factor_data_cutoff=p0f_request["factor_data_cutoff"],
        suspend_data_root=p0f_request["suspend_data_root"],
        market_calendar_identity=p0f_request["market_calendar_identity"],
        suspend_sidecar_identity=p0f_request["suspend_sidecar_identity"],
        repository_root=args.repository_root_wsl,
        repository_root_windows=str(repository_root),
        repository_commit=_git_commit(repository_root),
        output_root=args.output_root_wsl,
        family_specs=approved_p0l_families(),
        exact_p0d_reference=ExactP0DReferenceV1(
            bundle_root=args.p0f_bundle_root_wsl,
            bundle_id=roots["p0f"].name,
            manifest_file_sha256=_sha256(roots["p0f"] / "manifest.json"),
            winner_family_id=str(p0d_winner["family_id"]),
            winner_seed=int(p0d_winner["seed"]),
            winner_training_objective=str(p0d_winner["training_objective"]),
            winner_boost_rounds=int(p0d_winner["final_boost_rounds"]),
        ),
        exact_p0g_anchor_reference=ExactP0GAnchorReferenceV1(
            bundle_root=args.p0g_bundle_root_wsl,
            bundle_id=roots["p0g"].name,
            manifest_file_sha256=_sha256(roots["p0g"] / "manifest.json"),
        ),
        p0h_evidence_reference=_evidence("P0H_V1_EVIDENCE", roots["p0h"], args.p0h_bundle_root_wsl),
        p0k_evidence_reference=_evidence("P0K_V1_EVIDENCE", roots["p0k"], args.p0k_bundle_root_wsl),
        model_information_cutoff_trade_date=p0f_request["model_information_cutoff_trade_date"],
        latest_training_decision_trade_date=p0f_request["latest_training_decision_trade_date"],
        latest_training_label_observation_trade_date=p0f_request[
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


def _evidence(role: str, root: Path, wsl_root: str) -> P0LEvidenceReferenceV1:
    return P0LEvidenceReferenceV1(
        role=role,
        bundle_root=wsl_root,
        bundle_id=root.name,
        manifest_file_sha256=_sha256(root / "manifest.json"),
    )


def _verify_shared_identity(
    parser: argparse.ArgumentParser, manifests: list[dict[str, object]]
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
        parser.error(f"P0-D/P0-G/P0-H/P0-K exact identities differ: {mismatches}")


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_commit(root: Path) -> str:
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip().lower()


if __name__ == "__main__":
    raise SystemExit(main())
