"""Build the immutable C-012-RL1 H5/Bin input bundle.

This CLI is deliberately separate from the formal 24-fit runner.  It consumes
only explicitly pinned release/C-013 assets and never accepts a database
prefix, latest alias, model output or runtime target.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.cas_store import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.rotation_l1_input_bundle import (  # noqa: E402
    BUILD_RECEIPT_SCHEMA_VERSION,
    REASON_INCOMPLETE,
    RotationL1InputBundleError,
    build_rotation_l1_inputs_from_assets,
    write_rotation_l1_input_bundle,
)
from backend.services.hmm_risk.state_model_set import canonical_sha256  # noqa: E402


def _load_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} cannot be read") from exc
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return value


def _write_failure(output_root: Path, error: Exception) -> Path:
    reason = error.reason_code if isinstance(error, RotationL1InputBundleError) else REASON_INCOMPLETE
    context = (
        error.context if isinstance(error, RotationL1InputBundleError) else {"exception_type": type(error).__name__}
    )
    body = {
        "schema_version": BUILD_RECEIPT_SCHEMA_VERSION,
        "status": "failed",
        "primary_reason_code": reason,
        "failure_reason_codes": [reason],
        "message": str(error),
        "context": context,
        "bundle_write_performed": False,
        "model_write_performed": False,
        "ready_artifact_write_performed": False,
        "database_read_performed": False,
        "database_write_performed": False,
        "runtime_action_performed": False,
    }
    receipt = {**body, "receipt_sha256": canonical_sha256(body)}
    output_root.parent.mkdir(parents=True, exist_ok=True)
    failure_root = output_root.parent / f".{output_root.name}.failed.{uuid.uuid4().hex}"
    failure_root.mkdir(parents=False, exist_ok=False)
    path = failure_root / "build.failure.json"
    with path.open("xb") as handle:
        handle.write(canonical_json_bytes(receipt) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--dataset-release-manifest", type=Path)
    source.add_argument("--direct-v2-candidate-root", type=Path)
    parser.add_argument("--security-identity-manifest", type=Path)
    parser.add_argument("--provider-absence-manifest", type=Path)
    parser.add_argument("--industry-pit-authority", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--source-end", required=True)
    parser.add_argument("--producer-commit", required=True)
    args = parser.parse_args()
    if args.source_end != "2026-03-31":
        parser.error("--source-end must equal the approved boundary 2026-03-31")
    direct_arguments = (args.security_identity_manifest, args.provider_absence_manifest)
    if args.direct_v2_candidate_root is not None and any(value is None for value in direct_arguments):
        parser.error("direct-v2 source requires --security-identity-manifest and --provider-absence-manifest")
    if args.dataset_release_manifest is not None and any(value is not None for value in direct_arguments):
        parser.error("legacy source cannot accept direct-v2 security/provider authority arguments")
    if not args.output_root.is_absolute():
        parser.error("--output-root must be absolute")
    try:
        args.output_root.resolve().relative_to(ROOT.resolve())
    except ValueError:
        pass
    else:
        parser.error("--output-root must be outside the repository")
    try:
        authority = _load_object(args.industry_pit_authority, "industry PIT authority")
        inputs, source, source_identity = build_rotation_l1_inputs_from_assets(
            dataset_release_manifest=args.dataset_release_manifest,
            direct_v2_candidate_root=args.direct_v2_candidate_root,
            security_identity_manifest=args.security_identity_manifest,
            provider_absence_manifest=args.provider_absence_manifest,
            industry_authority=authority,
            forbidden_roots=(ROOT,),
            work_parent=args.output_root.parent,
        )
        receipt = write_rotation_l1_input_bundle(
            inputs=inputs,
            source=source,
            source_identity=source_identity,
            output_root=args.output_root,
            producer_commit=args.producer_commit,
            forbidden_roots=(ROOT,),
        )
    except Exception as exc:
        failure_path = _write_failure(args.output_root, exc)
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": getattr(exc, "reason_code", REASON_INCOMPLETE),
                    "failure_receipt": str(failure_path),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(receipt, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
