from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.services.multi_alpha.durable_backfill import MultiAlphaLegacyBackfill  # noqa: E402
from backend.services.multi_alpha.durable_repository import MultiAlphaDurableRepositoryError  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run, execute, or read back the QE-only multi-alpha durable historical association backfill."
    )
    parser.add_argument("--mode", choices=("dry-run", "execute", "readback"), required=True)
    parser.add_argument("--pretty", action="store_true", help="Pretty-print the structured JSON receipt.")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    service = MultiAlphaLegacyBackfill()
    try:
        if args.mode == "dry-run":
            result = service.dry_run()
        elif args.mode == "execute":
            result = service.execute()
        else:
            result = service.readback()
    except MultiAlphaDurableRepositoryError as exc:
        payload = {
            "success": False,
            "mode": args.mode,
            "reason_code": exc.reason_code,
            "message": str(exc),
            "context": exc.context,
        }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2 if args.pretty else None))
        return 1

    payload = {"success": True, **result}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2 if args.pretty else None, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
