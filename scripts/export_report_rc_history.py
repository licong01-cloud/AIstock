"""Freeze or independently verify report_rc files; no network, DB or services."""

from pathlib import Path
import argparse
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from backend.services.report_rc_data_delivery import export_delivery, verify_delivery  # noqa: E402


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--destination", type=Path, required=True)
    p.add_argument("--capture-root", type=Path)
    p.add_argument("--population-identity", type=Path)
    p.add_argument("--verify-only", action="store_true")
    args = p.parse_args()
    if args.verify_only:
        result = verify_delivery(args.destination)
    else:
        if args.capture_root is None:
            p.error("--capture-root is required for export")
        result = export_delivery(args.capture_root, args.destination, population_identity=args.population_identity)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, default=str), flush=True)


if __name__ == "__main__":
    main()
