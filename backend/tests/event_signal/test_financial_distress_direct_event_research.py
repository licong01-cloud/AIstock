import datetime as dt

from backend.services.event_signal.financial_distress_direct_event_research import (
    aggregate_return_rows,
    build_direct_event_rows,
    build_direct_return_rows,
    count_event_rows,
    select_rules,
    top_industry_rows,
)


def test_build_direct_event_rows_filters_rule_and_date_window():
    rules = select_rules(["indicator_large_decline_mv_10_30bn"])
    rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "event_type": "financial_indicator_large_decline",
            "source_type": "tushare_fina_indicator",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "signal_year": 2024,
            "effective_trade_date": dt.date(2024, 1, 2),
            "market_cap_bucket": "mv_10bn_to_30bn_yuan",
            "industry": "bank",
        },
        {
            "signal_id": 2,
            "ts_code": "000002.SZ",
            "event_type": "financial_indicator_large_decline",
            "source_type": "tushare_fina_indicator",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "signal_year": 2024,
            "effective_trade_date": dt.date(2024, 1, 2),
            "market_cap_bucket": "mv_30bn_to_100bn_yuan",
            "industry": "real_estate",
        },
    ]

    events = build_direct_event_rows(rows, rules, date_from=dt.date(2024, 1, 1), date_to=dt.date(2024, 1, 31))

    assert len(events) == 1
    assert events[0]["rule_key"] == "indicator_large_decline_mv_10_30bn"
    assert events[0]["signal_id"] == 1


def test_select_rules_supports_current_small_cap_benchmark():
    rules = select_rules(["loss_to_market_cap_ge_50pct_mv_lt_10bn"])

    assert rules[0].rule_key == "loss_to_market_cap_ge_50pct_mv_lt_10bn"


def test_build_direct_return_rows_uses_event_close_for_post_effective_return():
    events = [
        {
            "rule_key": "indicator_large_decline_mv_10_30bn",
            "rule_title": "rule",
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "event_type": "financial_indicator_large_decline",
            "source_type": "tushare_fina_indicator",
            "effective_trade_date": dt.date(2024, 1, 3),
            "signal_year": 2024,
            "industry": "bank",
            "market_cap_bucket": "mv_10bn_to_30bn_yuan",
        }
    ]
    trading_days = [dt.date(2024, 1, 2), dt.date(2024, 1, 3), dt.date(2024, 1, 4)]
    prices = {
        ("000001.SZ", dt.date(2024, 1, 2)): 10.0,
        ("000001.SZ", dt.date(2024, 1, 3)): 11.0,
        ("000001.SZ", dt.date(2024, 1, 4)): 12.1,
    }
    index_close = {
        dt.date(2024, 1, 2): 100.0,
        dt.date(2024, 1, 3): 110.0,
        dt.date(2024, 1, 4): 115.5,
    }

    returns = build_direct_return_rows(events, trading_days, prices, return_windows=[0, 1], index_close=index_close)
    by_window = {row["window"]: row for row in returns}

    assert round(by_window[0]["cumulative_return_from_prev_close"], 6) == 0.1
    assert round(by_window[0]["post_effective_return_from_t0_close"], 6) == 0.0
    assert round(by_window[1]["post_effective_return_from_t0_close"], 6) == 0.1
    assert round(by_window[1]["post_effective_abnormal_return_from_t0_close"], 6) == 0.05
    assert by_window[1]["missing_benchmark"] is False


def test_aggregate_and_top_industry_rows():
    rows = [
        {"rule_key": "r", "industry": "A", "window": 5, "post_effective_return_from_t0_close": -0.10, "missing_price": False, "signal_id": 1},
        {"rule_key": "r", "industry": "A", "window": 5, "post_effective_return_from_t0_close": 0.20, "missing_price": False, "signal_id": 2},
        {"rule_key": "r", "industry": "B", "window": 5, "post_effective_return_from_t0_close": 0.05, "missing_price": False, "signal_id": 3},
    ]

    agg = aggregate_return_rows(rows, group_fields=("rule_key", "window"))[0]
    top = top_industry_rows(rows, window=5, limit=1)
    counts = count_event_rows(rows, group_fields=("rule_key",))[0]

    assert agg["valid_returns"] == 3
    assert round(agg["mean_return"], 6) == 0.05
    assert round(agg["negative_return_rate"], 6) == round(1 / 3, 6)
    assert top[0]["industry"] == "A"
    assert counts["events"] == 3


def test_aggregate_return_rows_sorts_numeric_windows_numerically():
    rows = [
        {"rule_key": "r", "window": 20, "window_name": "T0_T+20", "post_effective_return_from_t0_close": 0.2, "missing_price": False},
        {"rule_key": "r", "window": 5, "window_name": "T0_T+5", "post_effective_return_from_t0_close": 0.1, "missing_price": False},
        {"rule_key": "r", "window": 1, "window_name": "T0_T+1", "post_effective_return_from_t0_close": 0.0, "missing_price": False},
    ]

    aggregate = aggregate_return_rows(rows, group_fields=("rule_key", "window", "window_name"))

    assert [row["window"] for row in aggregate] == [1, 5, 20]
