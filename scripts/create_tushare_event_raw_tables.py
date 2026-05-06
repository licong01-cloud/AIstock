"""Ensure Tushare event-related raw source tables exist.

The migration creates three source-only raw tables:
market.tushare_forecast_raw, market.tushare_express_raw, and
market.tushare_fina_indicator_raw.
"""

from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db.init_tushare_event_raw_schema import init_tushare_event_raw_schema  # noqa: E402


def main() -> int:
    init_tushare_event_raw_schema()
    print("market.tushare_*_raw tables and data_stats_config entries ensured.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
