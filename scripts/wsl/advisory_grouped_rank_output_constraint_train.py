from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.services.advisory_model_first.grouped_rank_output_constraint_pipeline import (  # noqa: E402
    run_grouped_rank_output_constraint_pipeline,
)
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Train Advisory P0-I Stage A grouped-rank output constraint in WSL."
    )
    parser.add_argument("--request", required=True)
    args = parser.parse_args()
    try:
        result = run_grouped_rank_output_constraint_pipeline(args.request)
    except AdvisoryModelFirstError as exc:
        print(json.dumps(exc.as_dict(), sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
