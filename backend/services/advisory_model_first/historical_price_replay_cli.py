from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Sequence

from dotenv import load_dotenv

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.historical_price_replay import (
    AdvisoryHistoricalPriceReplayService,
    prepare_historical_price_replay_request,
)
from backend.services.stock_universe_pit_service import DEFAULT_ST_PIT_UNIVERSE_KEY


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run Advisory price-envelope PIT batch historical replay"
    )
    parser.add_argument("--env-file", required=True, type=Path)
    parser.add_argument("--model-root", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--price-range-bundle-id", required=True)
    parser.add_argument("--decision-start", required=True, type=date.fromisoformat)
    parser.add_argument("--decision-end", required=True, type=date.fromisoformat)
    parser.add_argument("--replay-as-of", required=True, type=date.fromisoformat)
    parser.add_argument("--pit-universe-key", default=DEFAULT_ST_PIT_UNIVERSE_KEY)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if not args.env_file.is_file():
            raise AdvisoryModelFirstError(
                "historical replay env file is unavailable",
                reason_code="ADVISORY_HISTORICAL_PRICE_REPLAY_ENV_UNAVAILABLE",
            )
        load_dotenv(args.env_file, override=True)
        request, source = prepare_historical_price_replay_request(
            model_root=args.model_root,
            price_range_bundle_id=args.price_range_bundle_id,
            decision_start_trade_date=args.decision_start,
            decision_end_trade_date=args.decision_end,
            replay_as_of_date=args.replay_as_of,
            pit_universe_key=args.pit_universe_key,
        )
        receipt = AdvisoryHistoricalPriceReplayService().run(
            request=request,
            prediction_source_path=source,
            output_root=args.output_root,
        )
        print(
            json.dumps(
                {
                    "command": "historical-price-replay",
                    **receipt.model_dump(mode="json"),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    except AdvisoryModelFirstError as exc:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "reason_code": exc.reason_code,
                    "message": str(exc),
                    "context": exc.context,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
