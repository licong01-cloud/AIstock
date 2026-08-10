from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Advisory M4A price-range training inside WSL."
    )
    parser.add_argument("--request", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repository_root = Path(__file__).resolve().parents[2]
    if str(repository_root) not in sys.path:
        sys.path.insert(0, str(repository_root))
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    from backend.services.advisory_model_first.price_range_pipeline import (
        run_price_range_training_pipeline,
    )

    try:
        receipt = run_price_range_training_pipeline(args.request)
    except AdvisoryModelFirstError as exc:
        print(
            json.dumps(exc.as_dict(), ensure_ascii=True, sort_keys=True),
            file=sys.stderr,
            flush=True,
        )
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "reason_code": "ADVISORY_PRICE_RANGE_UNEXPECTED_TRAINING_ERROR",
                    "message": str(exc),
                    "context": {"error_type": type(exc).__name__},
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
            flush=True,
        )
        raise
    print(
        json.dumps(
            {
                "status": receipt["status"],
                "request_id": receipt["request_id"],
                "price_range_bundle_id": receipt["price_range_bundle_id"],
                "bundle_path": receipt["bundle_path"],
                "price_range_binding_activated": receipt["price_range_binding_activated"],
            },
            ensure_ascii=True,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
