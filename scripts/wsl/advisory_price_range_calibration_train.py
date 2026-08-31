from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Advisory M5C calibration inside WSL.")
    parser.add_argument("--request", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    from backend.services.advisory_model_first.price_range_calibration_pipeline import (
        run_price_range_calibration_pipeline,
    )

    try:
        receipt = run_price_range_calibration_pipeline(args.request)
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), ensure_ascii=True, sort_keys=True), file=sys.stderr, flush=True)
        return 2
    except Exception as exc:
        print(json.dumps({
            "status": "failed", "reason_code": "ADVISORY_PRICE_RANGE_CALIBRATION_UNEXPECTED_ERROR",
            "message": str(exc), "context": {"error_type": type(exc).__name__},
        }, ensure_ascii=True, sort_keys=True), file=sys.stderr, flush=True)
        raise
    print(json.dumps({
        "status": receipt["status"], "request_id": receipt["request_id"],
        "price_range_bundle_id": receipt["price_range_bundle_id"],
        "price_range_binding_activated": receipt["price_range_binding_activated"],
        "activation_recommended": receipt["activation_recommended"],
    }, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
