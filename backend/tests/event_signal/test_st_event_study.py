import datetime as dt

from backend.services.event_signal.st_event_study import (
    aggregate_details,
    build_detail_rows,
    is_down_limit,
)


def test_is_down_limit_uses_close_and_limit_price():
    assert is_down_limit(9.99, {"down_limit": 10.0}) is True
    assert is_down_limit(10.2, {"down_limit": 10.0}) is False
    assert is_down_limit(None, {"down_limit": 10.0}) is False
    assert is_down_limit(9.99, None) is False


def test_build_detail_rows_computes_t_windows_and_cumulative_returns():
    event = {
        "signal_id": 1,
        "ts_code": "000001.SZ",
        "event_type": "stock_st_imposed",
        "risk_level": "P0_BLOCK",
        "action": "block_buy",
        "source_event_date": dt.date(2026, 1, 3),
        "effective_trade_date": dt.date(2026, 1, 3),
    }
    trading_days = [
        dt.date(2026, 1, 1),
        dt.date(2026, 1, 2),
        dt.date(2026, 1, 3),
        dt.date(2026, 1, 4),
        dt.date(2026, 1, 5),
    ]
    closes = {
        dt.date(2026, 1, 1): 100.0,
        dt.date(2026, 1, 2): 90.0,
        dt.date(2026, 1, 3): 81.0,
        dt.date(2026, 1, 4): 72.9,
        dt.date(2026, 1, 5): 80.19,
    }
    prices = {
        ("000001.SZ", day): {"close_yuan": close, "volume_hand": 1, "amount_li": 1000}
        for day, close in closes.items()
    }
    index_close = {day: 100.0 for day in trading_days}
    limit_rows = {("000001.SZ", dt.date(2026, 1, 3)): {"down_limit": 81.0}}

    details = build_detail_rows(
        [event],
        trading_days,
        prices,
        index_close,
        limit_rows,
        set(),
        benchmark="000300.SH",
    )

    by_window = {row["window_name"]: row for row in details}
    assert round(by_window["T-1"]["raw_return"], 6) == -0.1
    assert round(by_window["T0"]["raw_return"], 6) == -0.1
    assert by_window["T0"]["hit_down_limit"] is True
    assert round(by_window["T+1"]["raw_return"], 6) == -0.1
    assert round(by_window["T+2"]["raw_return"], 6) == 0.1
    assert round(by_window["T0_T2"]["raw_return"], 6) == round((0.9 * 0.9 * 1.1) - 1, 6)


def test_aggregate_details_reports_rates_and_return_distribution():
    rows = [
        {"event_type": "stock_st_imposed", "window_name": "T0", "raw_return": -0.1, "abnormal_return": -0.11, "hit_down_limit": True, "is_suspended": False, "missing_price": False},
        {"event_type": "stock_st_imposed", "window_name": "T0", "raw_return": 0.02, "abnormal_return": 0.01, "hit_down_limit": False, "is_suspended": True, "missing_price": False},
        {"event_type": "stock_st_imposed", "window_name": "T0", "raw_return": None, "abnormal_return": None, "hit_down_limit": False, "is_suspended": False, "missing_price": True},
    ]

    aggregate = aggregate_details(rows)[0]

    assert aggregate["event_type"] == "stock_st_imposed"
    assert aggregate["window_name"] == "T0"
    assert aggregate["rows"] == 3
    assert aggregate["valid_raw_returns"] == 2
    assert aggregate["negative_return_rate"] == 0.5
    assert aggregate["down_limit_rate"] == 1 / 3
    assert aggregate["suspended_rate"] == 1 / 3
    assert aggregate["missing_price_rate"] == 1 / 3
