import datetime as dt

from backend.services.event_signal.financial_distress_qe_overlay_research import (
    FIRST_BATCH_RULES,
    SIZE_BUCKET_RULES,
    _active_dates_for_signal,
    _fixed_width_table,
    _rule_applies,
    build_overlay_frame,
    load_loop_specs,
    parse_loop_spec,
    select_research_rules,
    summarize_multiloop_validations,
)


def _rule(key: str):
    return next(rule for rule in (*FIRST_BATCH_RULES, *SIZE_BUCKET_RULES) if rule.rule_key == key)


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


def test_size_bucket_rules_split_loss_to_market_cap_ge_50_by_market_cap():
    base = {
        "event_type": "financial_forecast_loss",
        "loss_to_market_cap_bucket": "loss_50pct_to_100pct_mv",
        "loss_report_count_730d_bucket": "loss_reports_2",
    }

    assert _rule_applies({**base, "market_cap_bucket": "mv_lt_5bn_yuan"}, _rule("loss_to_market_cap_ge_50pct_mv_lt_5bn"))
    assert _rule_applies({**base, "market_cap_bucket": "mv_5bn_to_10bn_yuan"}, _rule("loss_to_market_cap_ge_50pct_mv_5_10bn"))
    assert _rule_applies({**base, "market_cap_bucket": "mv_5bn_to_10bn_yuan"}, _rule("loss_to_market_cap_ge_50pct_mv_lt_10bn"))
    assert _rule_applies({**base, "market_cap_bucket": "mv_10bn_to_30bn_yuan"}, _rule("loss_to_market_cap_ge_50pct_mv_ge_10bn"))
    assert not _rule_applies({**base, "market_cap_bucket": "mv_10bn_to_30bn_yuan"}, _rule("loss_to_market_cap_ge_50pct_mv_lt_10bn"))
    assert not _rule_applies({**base, "market_cap_bucket": "mv_unknown"}, _rule("loss_to_market_cap_ge_50pct_mv_ge_10bn"))


def test_select_research_rules_can_run_size_bucket_only():
    rules = select_research_rules(size_bucket_only=True)

    assert [rule.rule_key for rule in rules] == [rule.rule_key for rule in SIZE_BUCKET_RULES]


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


def test_parse_loop_spec_uses_comma_to_allow_windows_or_wsl_paths():
    spec = parse_loop_spec("qe_x,Loop1,/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_x/Loop1")

    assert spec.experiment_id == "qe_x"
    assert spec.loop_id == "Loop1"
    assert spec.loop_path.endswith("/Loop1")


def test_load_loop_specs_deduplicates_by_experiment_and_loop():
    specs = load_loop_specs(
        loop_specs=[
            "qe_x,Loop1,/tmp/a",
            "qe_x,Loop1,/tmp/a",
            "qe_x,Loop2,/tmp/b",
        ],
        loop_spec_json=None,
    )

    assert [(item.experiment_id, item.loop_id) for item in specs] == [("qe_x", "Loop1"), ("qe_x", "Loop2")]


def test_summarize_multiloop_validations_reports_rule_stability():
    payloads = [
        {
            "experiment_id": "qe_a",
            "loop_id": "Loop1",
            "validations": [
                {
                    "rule_key": "loss_to_market_cap_ge_50pct",
                    "active_trading_days": 60,
                    "simulator_mode": "cash",
                    "delta_metrics": {
                        "total_return_delta": 0.02,
                        "cagr_delta": 0.01,
                        "max_drawdown_delta": 0.0,
                        "final_account_delta": 200.0,
                    },
                    "hit_stats": {"blocked_buy_events": 2, "unique_buy_hit_symbols": 2},
                }
            ],
        },
        {
            "experiment_id": "qe_b",
            "loop_id": "Loop2",
            "validations": [
                {
                    "rule_key": "loss_to_market_cap_ge_50pct",
                    "active_trading_days": 60,
                    "simulator_mode": "cash",
                    "delta_metrics": {
                        "total_return_delta": -0.01,
                        "cagr_delta": -0.005,
                        "max_drawdown_delta": 0.01,
                        "final_account_delta": -100.0,
                    },
                    "hit_stats": {"blocked_buy_events": 3, "unique_buy_hit_symbols": 3},
                }
            ],
        },
    ]

    summary = summarize_multiloop_validations(payloads)
    row = summary["stability_rows"][0]

    assert row["loops"] == 2
    assert row["positive_return_loops"] == 1
    assert row["negative_return_loops"] == 1
    assert row["total_blocked_buy_events"] == 5
    assert round(row["avg_return_delta"], 6) == 0.005
