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

from backend.services.advisory_model_first.feature_schema_v1 import FEATURE_SCHEMA_HASH  # noqa: E402
from backend.services.advisory_model_first.meta_label_contracts import (  # noqa: E402
    approved_meta_label_families,
    build_frozen_meta_label_request,
)
from backend.services.advisory_model_first.policy_dataset_bundle import (  # noqa: E402
    load_policy_dataset_bundle,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Prepare one exact Advisory P0-D meta-label request.")
    parser.add_argument("--policy-dataset-bundle-root-windows", required=True)
    parser.add_argument("--policy-dataset-bundle-root-wsl", required=True)
    parser.add_argument("--qlib-daily-root-wsl", required=True)
    parser.add_argument("--factor-data-root-wsl", required=True)
    parser.add_argument("--factor-data-cutoff", required=True)
    parser.add_argument("--suspend-data-root-wsl", required=True)
    parser.add_argument("--repository-root-windows", required=True)
    parser.add_argument("--repository-root-wsl", required=True)
    parser.add_argument("--output-root-wsl", required=True)
    parser.add_argument("--request-output", required=True)
    args = parser.parse_args()
    bundle_root = Path(args.policy_dataset_bundle_root_windows).resolve()
    manifest = load_policy_dataset_bundle(bundle_root, expected_bundle_id=bundle_root.name)
    source_request = json.loads((bundle_root / "request.json").read_text(encoding="utf-8"))
    request = build_frozen_meta_label_request(
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
        repository_commit=_git_commit(Path(args.repository_root_windows).resolve()),
        output_root=args.output_root_wsl,
        feature_schema_hash=FEATURE_SCHEMA_HASH,
        family_specs=approved_meta_label_families(),
    )
    request.write_json(args.request_output)
    print(json.dumps({"status": "READY", "request_id": request.request_id, "request_sha256": request.request_sha256, "request_output": str(Path(args.request_output).resolve())}, sort_keys=True))
    return 0


def _sha256(path: Path) -> str:
    digest = hashlib.sha256(path.read_bytes())
    return digest.hexdigest()


def _git_commit(root: Path) -> str:
    return subprocess.run(["git", "rev-parse", "HEAD"], cwd=root, check=True, capture_output=True, text=True).stdout.strip().lower()


if __name__ == "__main__":
    raise SystemExit(main())
