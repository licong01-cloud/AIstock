from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_model_first.errors import (  # noqa: E402
    AdvisoryModelFirstError,
)
from backend.services.advisory_model_first.model_binding_resolution import (  # noqa: E402
    AdvisoryModelDescriptorRotationReceipt,
    publish_program_model_descriptor,
    rollback_program_model_descriptor,
    rotate_program_model_descriptor,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish one exact Advisory Program model descriptor without latest scanning.",
    )
    parser.add_argument(
        "--model-root",
        required=True,
        help="Explicit repo-external Advisory model root.",
    )
    parser.add_argument(
        "--payload", help="Explicit JSON descriptor payload without descriptor_sha256."
    )
    parser.add_argument(
        "--expected-current-descriptor-sha256",
        help="Required compare-and-swap identity when rotating or rolling back an existing descriptor.",
    )
    parser.add_argument(
        "--rollback-descriptor-sha256",
        help="Exact immutable descriptor history identity to restore instead of publishing a payload.",
    )
    parser.add_argument(
        "--program-id", help="Exact Program identity required for rollback."
    )
    parser.add_argument(
        "--binding-version-id", help="Exact binding identity required for rollback."
    )
    args = parser.parse_args()

    model_root = Path(args.model_root).resolve()
    if args.rollback_descriptor_sha256:
        if args.payload:
            parser.error(
                "--payload cannot be combined with --rollback-descriptor-sha256"
            )
        if not args.expected_current_descriptor_sha256:
            parser.error("rollback requires --expected-current-descriptor-sha256")
        if not args.program_id or not args.binding_version_id:
            parser.error("rollback requires --program-id and --binding-version-id")
        receipt = rollback_program_model_descriptor(
            model_root=model_root,
            program_id=args.program_id,
            binding_version_id=args.binding_version_id,
            expected_current_descriptor_sha256=args.expected_current_descriptor_sha256,
            rollback_descriptor_sha256=args.rollback_descriptor_sha256,
        )
        print(json.dumps(_receipt_payload(receipt), ensure_ascii=False))
        return 0

    if not args.payload:
        parser.error("--payload is required for initial publish or rotation")
    if args.program_id or args.binding_version_id:
        parser.error(
            "--program-id and --binding-version-id are only valid for rollback"
        )
    payload_path = Path(args.payload).resolve()
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit(
            f"descriptor payload cannot be read: {type(exc).__name__}"
        ) from exc
    if not isinstance(payload, dict):
        raise SystemExit("descriptor payload must be a JSON object")

    if args.expected_current_descriptor_sha256:
        receipt = rotate_program_model_descriptor(
            model_root=model_root,
            payload=payload,
            expected_current_descriptor_sha256=args.expected_current_descriptor_sha256,
        )
        print(json.dumps(_receipt_payload(receipt), ensure_ascii=False))
        return 0

    target = publish_program_model_descriptor(model_root=model_root, payload=payload)
    print(json.dumps({"ok": True, "descriptor_path": str(target)}, ensure_ascii=False))
    return 0


def _receipt_payload(
    receipt: AdvisoryModelDescriptorRotationReceipt,
) -> dict[str, object]:
    return {
        "ok": True,
        "operation": receipt.operation,
        "descriptor_path": str(receipt.descriptor_path),
        "previous_descriptor_sha256": receipt.previous_descriptor_sha256,
        "descriptor_sha256": receipt.descriptor_sha256,
        "rollback_snapshot_path": (
            str(receipt.rollback_snapshot_path)
            if receipt.rollback_snapshot_path is not None
            else None
        ),
    }


if __name__ == "__main__":
    try:
        exit_code = main()
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=False), file=sys.stderr)
        exit_code = 2
    raise SystemExit(exit_code)
