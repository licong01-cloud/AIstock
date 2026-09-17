"""Run the approved label-free QE-assistance consumer-effect preflight."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.services.dataset_release.canonical import canonical_json_bytes  # noqa: E402
from backend.services.hmm_risk.qe_assistance_adapter import (  # noqa: E402
    QEAssistanceContractError,
    evaluate_consumer_effect,
)
from scripts.hmm_risk.build_qe_assistance_artifact import (  # noqa: E402
    load_calendar,
    load_json_object,
    load_prediction_rows,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prediction-pickle", type=Path, required=True)
    parser.add_argument("--calendar", type=Path, required=True)
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    calendar = load_calendar(args.calendar)
    result = evaluate_consumer_effect(
        raw_prediction_rows=load_prediction_rows(args.prediction_pickle, calendar),
        calendar=calendar,
        artifact=load_json_object(args.artifact),
    )
    output = args.output
    if not output.is_absolute() or output.is_symlink():
        raise RuntimeError("output must be an absolute non-symlink path")
    output.parent.resolve(strict=True)
    with output.open("xb") as handle:
        handle.write(canonical_json_bytes(result) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    print(
        json.dumps(
            {
                "status": result["status"],
                "result_sha256": result["result_sha256"],
                "changed_score_count": result["changed_score_count"],
                "rank_changed_row_count": result["rank_changed_row_count"],
                "topk_changed_date_count": result["topk_changed_date_count"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (QEAssistanceContractError, RuntimeError) as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": getattr(
                        exc,
                        "reason_code",
                        "hmm_risk_qe_assistance_input_invalid",
                    ),
                    "message": str(exc),
                    "tail_accessed": False,
                    "database_write_performed": False,
                    "runtime_action_performed": False,
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
