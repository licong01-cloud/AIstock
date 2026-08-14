#!/usr/bin/env python3
"""Aggregate frozen TRANSITION-DWELL-B children without HMM execution."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.hmm_risk.b3_evidence_aggregation import (  # noqa: E402
    B3EvidenceAggregationError,
    aggregate_transition_dwell_evidence,
    build_aggregation_failure,
    write_aggregation_report,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent-report", required=True, help="Frozen TRANSITION-DWELL-B parent JSON.")
    parser.add_argument("--output", required=True, help="Compact content-addressed diagnostic output JSON.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        parent_path = Path(args.parent_report)
        report = aggregate_transition_dwell_evidence(parent_path)
        write_aggregation_report(Path(args.output), report)
    except (B3EvidenceAggregationError, OSError, ValueError) as exc:
        failure = build_aggregation_failure(parent_path=Path(args.parent_report), error=exc)
        output = Path(args.output).resolve()
        failure_path = output.with_name(f"{output.stem}.failure.json")
        try:
            write_aggregation_report(failure_path, failure)
            failure_write_error = None
        except (B3EvidenceAggregationError, OSError, ValueError) as write_exc:
            failure_write_error = f"{type(write_exc).__name__}: {write_exc}"
        print(
            json.dumps(
                {
                    **failure,
                    "failure_output": str(failure_path),
                    "failure_write_error": failure_write_error,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            {
                "status": report["status"],
                "receipt_sha256": report["receipt_sha256"],
                "record_count": report["record_count"],
                "output": str(Path(args.output).resolve()),
                "refit_performed": False,
                "selection_performed": False,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
