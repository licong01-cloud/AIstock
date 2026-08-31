from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.meta_label_bundle import load_meta_label_bundle  # noqa: E402
from backend.services.advisory_model_first.policy_dataset_bundle import load_policy_dataset_bundle  # noqa: E402
from backend.services.advisory_model_first.policy_utility_contracts import (  # noqa: E402
    ExactMetaLabelReferenceV1,
    approved_policy_utility_arms,
    approved_policy_utility_families,
    build_frozen_policy_utility_request,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare one exact Advisory P0-D/E/F v2 suspension-aware Stage A request."
    )
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--p0d-reference-root-windows", required=True)
    parser.add_argument("--p0d-reference-root-wsl", required=True)
    parser.add_argument("--p0e-reference-root-windows", required=True)
    parser.add_argument("--p0e-reference-root-wsl", required=True)
    parser.add_argument("--qlib-daily-root-wsl", required=True)
    parser.add_argument("--factor-data-root-wsl", required=True)
    parser.add_argument("--factor-data-cutoff", required=True)
    parser.add_argument("--suspend-data-root-wsl", required=True)
    parser.add_argument("--market-calendar-sha256", required=True)
    parser.add_argument("--market-calendar-row-count", required=True, type=int)
    parser.add_argument("--suspend-sidecar-sha256", required=True)
    parser.add_argument("--suspend-sidecar-row-count", required=True, type=int)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()

    bundle_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    manifest = load_policy_dataset_bundle(bundle_root, expected_bundle_id=bundle_root.name)
    source_request = json.loads((bundle_root / "request.json").read_text(encoding="utf-8"))
    labels = pd.read_parquet(bundle_root / "candidate_episode_labels.parquet")
    rankings = pd.read_parquet(bundle_root / "candidate_rankings.parquet")
    candidates = rankings.loc[rankings["is_candidate_decision"] & (rankings["selection_effective_rank"] <= 20)].copy()
    candidate_counts = candidates.groupby("decision_as_of_trade_date").size()
    if (
        len(candidates) != 7720
        or len(candidate_counts) != 386
        or not candidate_counts.eq(20).all()
        or candidates.duplicated(["decision_as_of_trade_date", "instrument"]).any()
    ):
        parser.error("P0-C is not exact 7720 rows / 386 dates / 20 candidates per date")
    matured = labels[labels["label_status"] == "MATURED"]
    if matured.empty or "label_information_end" not in matured:
        parser.error("P0-C has no MATURED label_information_end rows")
    latest_decision = pd.to_datetime(matured["decision_as_of_trade_date"]).max().date().isoformat()
    latest_observation = pd.to_datetime(matured["label_information_end"]).max().date().isoformat()
    information_cutoff = (
        max(
            pd.Timestamp(source_request["data_cutoff"]),
            pd.Timestamp(latest_observation),
        )
        .date()
        .isoformat()
    )
    p0d = _reference("LEGACY_P0_D_LINEAGE", args.p0d_reference_root_windows, args.p0d_reference_root_wsl)
    p0e = _reference("LEGACY_P0_E_LINEAGE", args.p0e_reference_root_windows, args.p0e_reference_root_wsl)
    repository_root = Path(args.repository_root_windows).resolve()
    request = build_frozen_policy_utility_request(
        policy_dataset_bundle_root=args.policy_dataset_bundle_root_wsl,
        policy_dataset_bundle_id=manifest["policy_dataset_bundle_id"],
        policy_dataset_manifest_file_sha256=_sha256(bundle_root / "manifest.json"),
        program_id=manifest["program_id"],
        binding_version_id=manifest["binding_version_id"],
        package_id=manifest["package_id"],
        manifest_sha256=manifest["manifest_sha256"],
        style_profile_id=source_request["style_profile_id"],
        style_profile_hash=source_request["style_profile_hash"],
        shadow_policy_sha256=manifest["shadow_policy_sha256"],
        cost_policy_sha256=manifest["cost_policy_sha256"],
        split_policy_sha256=manifest["split_policy_sha256"],
        qlib_daily_root=args.qlib_daily_root_wsl,
        factor_data_root=args.factor_data_root_wsl,
        factor_data_cutoff=args.factor_data_cutoff,
        suspend_data_root=args.suspend_data_root_wsl,
        repository_root=args.repository_root_wsl,
        repository_root_windows=str(repository_root),
        repository_commit=_git_commit(repository_root),
        output_root=args.output_root_wsl,
        family_specs=approved_policy_utility_families(),
        arm_specs=approved_policy_utility_arms(),
        market_calendar_identity={
            "identity_kind": "MARKET_CALENDAR",
            "sha256": args.market_calendar_sha256,
            "cutoff_trade_date": args.factor_data_cutoff,
            "row_count": args.market_calendar_row_count,
        },
        suspend_sidecar_identity={
            "identity_kind": "SUSPEND_SIDECAR",
            "sha256": args.suspend_sidecar_sha256,
            "cutoff_trade_date": args.factor_data_cutoff,
            "row_count": args.suspend_sidecar_row_count,
        },
        legacy_p0d_reference=p0d,
        legacy_p0e_reference=p0e,
        model_information_cutoff_trade_date=information_cutoff,
        latest_training_decision_trade_date=latest_decision,
        latest_training_label_observation_trade_date=latest_observation,
    )
    request.write_json(args.request_output)
    print(
        json.dumps(
            {
                "status": "READY",
                "request_id": request.request_id,
                "request_sha256": request.request_sha256,
                "request_output": str(Path(args.request_output).resolve()),
                "latest_training_decision_trade_date": latest_decision,
                "latest_training_label_observation_trade_date": latest_observation,
                "model_information_cutoff_trade_date": information_cutoff,
            },
            sort_keys=True,
        )
    )
    return 0


def _reference(role: str, windows_root: str, wsl_root: str) -> ExactMetaLabelReferenceV1:
    root = Path(windows_root).resolve()
    load_meta_label_bundle(root, expected_bundle_id=root.name, load_booster=False)
    return ExactMetaLabelReferenceV1(
        role=role,
        bundle_root=wsl_root,
        bundle_id=root.name,
        manifest_file_sha256=_sha256(root / "manifest.json"),
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _git_commit(root: Path) -> str:
    return (
        subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True)
        .stdout.strip()
        .lower()
    )


if __name__ == "__main__":
    raise SystemExit(main())
