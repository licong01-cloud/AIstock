from datetime import date

import pandas as pd

from backend.qlib_exporter.db_reader import _filter_instrument_start_dates


def test_filter_instrument_start_dates_drops_pre_listing_source_rows() -> None:
    frame = pd.DataFrame(
        {
            "ts_code": ["001312.SZ", "001312.SZ", "600000.SH"],
            "trade_date": [
                date(2026, 4, 10),
                date(2026, 4, 14),
                date(2018, 8, 1),
            ],
            "close_li": [1000, 1100, 1200],
        }
    )

    result = _filter_instrument_start_dates(
        frame,
        {
            "001312.SZ": date(2026, 4, 14),
            "600000.SH": date(2018, 8, 1),
        },
    )

    assert result[["ts_code", "trade_date"]].to_dict("records") == [
        {"ts_code": "001312.SZ", "trade_date": date(2026, 4, 14)},
        {"ts_code": "600000.SH", "trade_date": date(2018, 8, 1)},
    ]
