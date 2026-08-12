from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from backend.services.advisory_model_first.model_binding_resolution import (  # noqa: E402
    publish_program_model_descriptor,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Publish one exact Advisory Program model descriptor without latest scanning.",
    )
    parser.add_argument("--model-root", required=True, help="Explicit repo-external Advisory model root.")
    parser.add_argument("--payload", required=True, help="Explicit JSON descriptor payload without descriptor_sha256.")
    args = parser.parse_args()

    payload_path = Path(args.payload).resolve()
    try:
        payload = json.loads(payload_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SystemExit(f"descriptor payload cannot be read: {type(exc).__name__}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("descriptor payload must be a JSON object")
    target = publish_program_model_descriptor(
        model_root=Path(args.model_root).resolve(),
        payload=payload,
    )
    print(json.dumps({"ok": True, "descriptor_path": str(target)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
