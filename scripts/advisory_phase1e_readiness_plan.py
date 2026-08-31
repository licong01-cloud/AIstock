"""Compatibility CLI delegating Phase1E compile to the authoritative Advisory O4 service."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backend.services.advisory_dev_input_onboarding.contracts import (
    AdvisoryImmutableArtifactRef,
    O4_ARTIFACT_STORE_POLICY_HASH,
)
from backend.services.advisory_dev_input_onboarding.phase1e_orchestration import (
    AdvisoryPhase1EOrchestrationService,
)
from backend.services.advisory_phase1.readiness_plan import Phase1EError
from backend.services.advisory_phase1.readiness_plan_store import (
    ContentAddressedPlanStore,
    Phase1EArtifactStoreError,
)


LOGGER = logging.getLogger("aistock.advisory.phase1e.compat")
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    compile_batch = subparsers.add_parser("compile-batch")
    compile_batch.add_argument("--input-bundle-ref", required=True, type=Path)
    compile_batch.add_argument("--env-file", required=True, type=Path)
    compile_batch.add_argument("--artifact-root", required=True, type=Path)
    for command in ("verify-plan", "inspect-plan"):
        action = subparsers.add_parser(command)
        action.add_argument("--kind", choices=("audit", "plan", "batch"), required=True)
        action.add_argument("--identity", required=True)
        action.add_argument("--semantic-hash", required=True)
        action.add_argument("--artifact-root", required=True, type=Path)
    return parser


def _read_ref(path: Path) -> AdvisoryImmutableArtifactRef:
    resolved = path.expanduser().resolve(strict=True)
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("input bundle ref JSON must contain one object")
    return AdvisoryImmutableArtifactRef.model_validate(payload)


def _structured_error(*, stage: str, error: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "stage": stage,
        "reason_code": getattr(error, "reason_code", "ADVISORY_PHASE1E_UNEXPECTED_ERROR"),
        "message": str(error),
        "context": getattr(error, "context", None),
    }


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "compile-batch":
            result = AdvisoryPhase1EOrchestrationService(repository_root=REPOSITORY_ROOT).compile_phase1e(
                input_bundle_ref=_read_ref(args.input_bundle_ref),
                env_file=args.env_file,
                artifact_root=args.artifact_root,
            )
            payload = {
                "ok": result["ok"],
                "command": "compile-batch",
                "delegated_to": "advisory_real_dev_onboarding.compile-phase1e",
                "compile_receipt_ref": result["compile_receipt_ref"].model_dump(mode="json"),
                "aggregate_status": result["compile_receipt"].aggregate_status.value,
            }
            print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
            return 0 if result["ok"] else 3
        store = ContentAddressedPlanStore(
            root=args.artifact_root,
            policy_hash=O4_ARTIFACT_STORE_POLICY_HASH,
        )
        document = store.verify(kind=args.kind, identity=args.identity, semantic_hash=args.semantic_hash)
        payload = (
            {"ok": True, "kind": args.kind, "identity": args.identity, "semantic_hash": args.semantic_hash}
            if args.command == "verify-plan"
            else document
        )
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    except (ValueError, Phase1EError, Phase1EArtifactStoreError) as exc:
        print(json.dumps(_structured_error(stage=args.command, error=exc), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("advisory_phase1e_compat_unexpected_error command=%s", args.command)
        print(json.dumps(_structured_error(stage=args.command, error=exc), ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
