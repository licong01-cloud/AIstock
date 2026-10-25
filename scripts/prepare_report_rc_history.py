"""Collect the user-approved bounded report_rc history; no production writes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from dotenv import dotenv_values  # noqa: E402
from backend.services.report_rc_data import SourceClient, capture  # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--env-file", type=Path, required=True)
    args = parser.parse_args()
    token = dotenv_values(args.env_file).get("TUSHARE_TOKEN")
    summary = capture(args.output, args.batch_id, SourceClient(token))
    print(json.dumps(summary, sort_keys=True), flush=True)
    return 0 if summary["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
