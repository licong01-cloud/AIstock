import datetime as dt

from backend.services.event_signal.financial_event_study import (
    aggregate_details,
    build_detail_rows,
    is_down_limit,
    required_price_keys,
)


TRADING_DAYS = [
    dt.date(2024, 1, 30),
    dt.date(2024, 1, 31),
    dt.date(2024, 2, 1),
    dt.date(2024, 2, 2),
    dt.date(2024, 2, 5),
    dt.date(2024, 2, 6),
    dt.date(2024, 2, 7),
    dt.date(2024, 2, 8),
    dt.date(2024, 2, 9),
    dt.date(2024, 2, 19),
    dt.date(2024, 2, 20),
    dt.date(2024, 2, 21),
    dt.date(2024, 2, 22),
    dt.date(2024, 2, 23),
    dt.date(2024, 2, 26),
    dt.date(2024, 2, 27),
    dt.date(2024, 2, 28),
    dt.date(2024, 2, 29),
    dt.date(2024, 3, 1),
    dt.date(2024, 3, 4),
    dt.date(2024, 3, 5),
    dt.date(2024, 3, 6),
]


def _event(signal_id: int = 1) -> dict:
    return {
        "signal_id": signal_id,
        "ts_code": "000001.SZ",
        "source_type": "tushare_forecast",
        "event_type": "financial_forecast_loss",
        "risk_level": "P2_REVIEW",
        "action": "warn_review",
        "signal_type": "risk",
        "source_event_date": dt.date(2024, 1, 31),
        "effective_trade_date": dt.date(2024, 1, 31),
        "report_period": dt.date(2023, 12, 31),
    }


def _prices() -> dict:
    closes = {
        dt.date(2024, 1, 30): 10.0,
        dt.date(2024, 1, 31): 9.0,
        dt.date(2024, 2, 1): 8.1,
        dt.date(2024, 2, 2): 8.0,
        dt.date(2024, 2, 5): 8.3,
        dt.date(2024, 2, 6): 8.5,
        dt.date(2024, 2, 7): 8.6,
        dt.date(2024, 2, 8): 8.7,
        dt.date(2024, 2, 9): 8.8,
        dt.date(2024, 2, 19): 8.9,
        dt.date(2024, 2, 20): 9.0,
        dt.date(2024, 2, 21): 9.1,
        dt.date(2024, 2, 22): 9.2,
        dt.date(2024, 2, 23): 9.3,
        dt.date(2024, 2, 26): 9.4,
        dt.date(2024, 2, 27): 9.5,
        dt.date(2024, 2, 28): 9.6,
        dt.date(2024, 2, 29): 9.7,
        dt.date(2024, 3, 1): 9.8,
        dt.date(2024, 3, 4): 9.9,
        dt.date(2024, 3, 5): 10.0,
        dt.date(2024, 3, 6): 10.1,
    }
    return {("000001.SZ", day): {"close_yuan": close, "volume_hand": 100, "amount_li": 100000} for day, close in closes.items()}


def test_is_down_limit_uses_close_against_limit_with_tolerance():
    assert is_down_limit(8.1, {"down_limit": 8.1}) is True
    assert is_down_limit(8.2, {"down_limit": 8.1}) is False
    assert is_down_limit(None, {"down_limit": 8.1}) is False


def test_required_price_keys_include_point_and_cumulative_windows():
    keys = required_price_keys([_event()], TRADING_DAYS)

    assert ("000001.SZ", dt.date(2024, 1, 30)) in keys
    assert ("000001.SZ", dt.date(2024, 1, 31)) in keys
    assert ("000001.SZ", dt.date(2024, 2, 2)) in keys
    assert ("000001.SZ", dt.date(2024, 3, 5)) in keys


def test_build_detail_rows_computes_point_and_cumulative_returns_with_flags():
    prices = _prices()
    index_close = {day: 1000 + idx * 10 for idx, day in enumerate(TRADING_DAYS)}
    limit_rows = {("000001.SZ", dt.date(2024, 2, 1)): {"down_limit": 8.1}}
    suspend_rows = {("000001.SZ", dt.date(2024, 2, 2))}

    details = build_detail_rows(
        [_event()],
        TRADING_DAYS,
        prices,
        index_close,
        limit_rows,
        suspend_rows,
        benchmark="000300.SH",
    )

    by_window = {row["window_name"]: row for row in details}
    assert round(by_window["T0"]["raw_return"], 6) == -0.1
    assert by_window["T+1"]["hit_down_limit"] is True
    assert by_window["T+2"]["is_suspended"] is True
    assert round(by_window["T0_T2"]["raw_return"], 6) == -0.2
    assert by_window["T0_T2"]["hit_down_limit"] is True
    assert by_window["T0_T2"]["is_suspended"] is True
    assert by_window["T0_T2"]["missing_price"] is False


def test_aggregate_details_groups_by_source_event_and_window():
    details = [
        {
            "source_type": "tushare_forecast",
            "event_type": "financial_forecast_loss",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "window_name": "T0",
            "raw_return": -0.1,
            "abnormal_return": -0.11,
            "hit_down_limit": True,
            "is_suspended": False,
            "missing_price": False,
        },
        {
            "source_type": "tushare_forecast",
            "event_type": "financial_forecast_loss",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "window_name": "T0",
            "raw_return": 0.02,
            "abnormal_return": 0.01,
            "hit_down_limit": False,
            "is_suspended": True,
            "missing_price": False,
        },
        {
            "source_type": "tushare_forecast",
            "event_type": "financial_forecast_loss",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "window_name": "T0",
            "raw_return": None,
            "abnormal_return": None,
            "hit_down_limit": False,
            "is_suspended": False,
            "missing_price": True,
        },
    ]

    aggregates = aggregate_details(details)

    assert len(aggregates) == 1
    row = aggregates[0]
    assert row["rows"] == 3
    assert row["valid_raw_returns"] == 2
    assert row["mean_raw_return"] == -0.04
    assert row["negative_return_rate"] == 0.5
    assert row["positive_return_rate"] == 0.5
    assert row["down_limit_rate"] == 1 / 3
    assert row["suspended_rate"] == 1 / 3
    assert row["missing_price_rate"] == 1 / 3
