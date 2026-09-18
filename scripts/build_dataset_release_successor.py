"""Build an immutable sector-context dataset successor and profile candidate."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.dataset_release.release_successor import (  # noqa: E402
    build_profile_v3,
    build_successor,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--successor-root", type=Path, required=True)
    parser.add_argument("--sector-component-root", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--generation", required=True)
    parser.add_argument("--expected-baseline-manifest-identity", required=True)
    parser.add_argument("--expected-baseline-manifest-file-sha256", required=True)
    parser.add_argument("--baseline-profile", type=Path, required=True)
    parser.add_argument("--profile-output", type=Path, required=True)
    parser.add_argument("--wsl-candidate-root", required=True)
    parser.add_argument("--node1-candidate-root", required=True)
    parser.add_argument("--receipt-output", type=Path, required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.receipt_output.exists():
        raise FileExistsError(f"successor receipt already exists: {args.receipt_output}")
    receipt = build_successor(
        baseline_root=args.baseline_root,
        successor_root=args.successor_root,
        sector_component_root=args.sector_component_root,
        revision=args.revision,
        expected_baseline_manifest_identity=args.expected_baseline_manifest_identity,
        expected_baseline_manifest_file_sha256=args.expected_baseline_manifest_file_sha256,
    )
    build_profile_v3(
        baseline_profile_path=args.baseline_profile,
        profile_output_path=args.profile_output,
        successor_receipt=receipt,
        generation=args.generation,
        controller_candidate_root=str(args.successor_root.absolute()),
        node_candidate_roots={
            "wsl2-5080": args.wsl_candidate_root,
            "rdagent-node1": args.node1_candidate_root,
        },
    )
    args.receipt_output.parent.mkdir(parents=True, exist_ok=True)
    with args.receipt_output.open("xb") as handle:
        handle.write(canonical_json_bytes(receipt) + b"\n")
        handle.flush()
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
