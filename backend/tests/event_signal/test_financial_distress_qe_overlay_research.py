import datetime as dt

from backend.services.event_signal.financial_distress_qe_overlay_research import (
    FIRST_BATCH_RULES,
    _active_dates_for_signal,
    _fixed_width_table,
    _rule_applies,
    build_overlay_frame,
)


def _rule(key: str):
    return next(rule for rule in FIRST_BATCH_RULES if rule.rule_key == key)


def test_first_batch_loss_to_market_cap_ge_50_includes_ge_100_bucket():
    row = {
        "event_type": "financial_express_loss",
        "loss_to_market_cap_bucket": "loss_ge_100pct_mv",
        "loss_report_count_730d_bucket": "loss_reports_1",
    }

    assert _rule_applies(row, _rule("loss_to_market_cap_ge_50pct"))
    assert not _rule_applies(row, _rule("forecast_loss_to_market_cap_ge_50pct"))


def test_first_batch_combined_loss_history_rule_requires_20_50_bucket_and_ge_4_reports():
    matching = {
        "event_type": "financial_express_loss",
        "loss_to_market_cap_bucket": "loss_20pct_to_50pct_mv",
        "loss_report_count_730d_bucket": "loss_reports_ge_4",
    }
    wrong_bucket = {
        **matching,
        "loss_to_market_cap_bucket": "loss_50pct_to_100pct_mv",
    }
    too_few_losses = {
        **matching,
        "loss_report_count_730d_bucket": "loss_reports_3",
    }

    assert _rule_applies(matching, _rule("loss_20_50pct_and_loss_reports_ge_4"))
    assert not _rule_applies(wrong_bucket, _rule("loss_20_50pct_and_loss_reports_ge_4"))
    assert not _rule_applies(too_few_losses, _rule("loss_20_50pct_and_loss_reports_ge_4"))


def test_active_dates_use_next_trading_day_and_requested_lifetime():
    trading_days = [
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 3),
        dt.date(2024, 1, 4),
        dt.date(2024, 1, 5),
    ]

    active = _active_dates_for_signal(
        effective_trade_date=dt.date(2024, 1, 1),
        trading_days=trading_days,
        date_from=dt.date(2024, 1, 3),
        date_to=dt.date(2024, 1, 5),
        active_trading_days=3,
    )

    assert active == [dt.date(2024, 1, 3), dt.date(2024, 1, 4)]


def test_build_overlay_frame_is_research_only_buy_filter_not_force_exit():
    trading_days = [
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 3),
        dt.date(2024, 1, 4),
    ]
    rows = [
        {
            "signal_id": 101,
            "ts_code": "000001.SZ",
            "event_type": "financial_forecast_loss",
            "source_type": "tushare_forecast",
            "effective_trade_date": dt.date(2024, 1, 2),
            "loss_to_market_cap_bucket": "loss_50pct_to_100pct_mv",
            "loss_to_market_cap": 0.55,
            "loss_report_count_730d_bucket": "loss_reports_2",
            "loss_report_count_730d": 2,
            "market_cap_bucket": "mv_lt_5bn_yuan",
            "industry": "software",
        }
    ]

    overlay = build_overlay_frame(
        financial_rows=rows,
        trading_days=trading_days,
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 4),
        rule=_rule("loss_to_market_cap_ge_50pct"),
        active_trading_days=2,
    )

    assert list(overlay["trade_date"]) == ["2024-01-02", "2024-01-03"]
    assert overlay["can_buy"].tolist() == [False, False]
    assert overlay["force_exit"].tolist() == [False, False]
    assert overlay["active_signal_count"].tolist() == [1, 1]
    assert overlay["source_signal_ids"].tolist() == ["101", "101"]


def test_fixed_width_table_has_outer_and_inner_borders():
    lines = _fixed_width_table(["a", "longer"], [[1, "x"], [20, "value"]])

    assert lines[0].startswith("+") and lines[0].endswith("+")
    assert lines[1].startswith("| ") and " | " in lines[1]
    assert lines[-1] == lines[0]
