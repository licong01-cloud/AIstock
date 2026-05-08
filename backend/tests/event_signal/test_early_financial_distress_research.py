import datetime as dt

from backend.services.event_signal.early_financial_distress_research import (
    FinancialRiskSignal,
    StTargetEvent,
    build_candidate_precision_rows,
    build_candidate_return_rows,
    build_candidate_stability_decisions,
    enrich_precision_rows_with_industry,
    enrich_precision_rows_with_loss_history,
    enrich_precision_rows_with_market_cap,
    extract_report_period,
    aggregate_cycle_return_rows,
    aggregate_precision_rows,
    aggregate_return_rows,
    build_candidate_rules,
    build_cycle_pre_st_return_rows,
    build_precision_rows,
    build_signal_return_rows,
    build_st_cycles,
    classify_metric_bucket,
    match_signals_to_cycles,
    required_cycle_return_price_keys,
    required_market_cap_keys,
    required_signal_return_price_keys,
    summarize_cycle_coverage,
)


def _signal(
    signal_id: int,
    ts_code: str = "000001.SZ",
    source_type: str = "tushare_forecast",
    event_type: str = "financial_forecast_loss",
    effective_date: dt.date = dt.date(2024, 1, 10),
    report_period: dt.date | None = None,
) -> FinancialRiskSignal:
    return FinancialRiskSignal(
        signal_id=signal_id,
        ts_code=ts_code,
        source_type=source_type,
        event_type=event_type,
        risk_level="P2_REVIEW",
        action="warn_review",
        source_event_date=effective_date,
        effective_trade_date=effective_date,
        report_period=report_period,
    )


def _st_event(
    signal_id: int,
    ts_code: str = "000001.SZ",
    event_type: str = "stock_st_imposed",
    effective_date: dt.date = dt.date(2024, 4, 10),
) -> StTargetEvent:
    return StTargetEvent(
        signal_id=signal_id,
        ts_code=ts_code,
        event_type=event_type,
        source_event_date=effective_date,
        effective_trade_date=effective_date,
    )


def test_build_st_cycles_merges_repeated_events_within_gap_and_splits_later_cycle():
    cycles = build_st_cycles(
        [
            _st_event(1, effective_date=dt.date(2024, 4, 10)),
            _st_event(2, event_type="stock_delisting_risk_warning", effective_date=dt.date(2024, 6, 1)),
            _st_event(3, effective_date=dt.date(2025, 2, 1)),
        ],
        cycle_gap_days=180,
    )

    assert len(cycles) == 2
    assert cycles[0].primary_event_type == "stock_st_imposed"
    assert cycles[0].event_types == ("stock_st_imposed", "stock_delisting_risk_warning")
    assert cycles[0].signal_ids == (1, 2)
    assert cycles[1].start_effective_trade_date == dt.date(2025, 2, 1)


def test_match_signals_to_cycles_uses_only_prior_signals_inside_lookback():
    cycle = build_st_cycles([_st_event(10, effective_date=dt.date(2024, 4, 10))])[0]
    matches = match_signals_to_cycles(
        [cycle],
        [
            _signal(1, effective_date=dt.date(2023, 3, 1)),
            _signal(2, source_type="tushare_express", effective_date=dt.date(2024, 1, 10)),
            _signal(3, source_type="tushare_fina_indicator", effective_date=dt.date(2024, 4, 10)),
            _signal(4, source_type="tushare_forecast", effective_date=dt.date(2024, 4, 11)),
        ],
        lookback_days=365,
    )

    assert len(matches) == 1
    row = matches[0]
    assert row["matched"] is True
    assert row["matched_signal_count"] == 1
    assert row["matched_source_types"] == ["tushare_express"]
    assert row["latest_lead_days"] == 91
    assert row["earliest_lead_days"] == 91


def test_summarize_cycle_coverage_reports_source_and_lead_bucket():
    cycles = build_st_cycles(
        [
            _st_event(10, effective_date=dt.date(2024, 4, 10)),
            _st_event(11, ts_code="000002.SZ", effective_date=dt.date(2024, 5, 10)),
        ]
    )
    matches = match_signals_to_cycles(
        cycles,
        [
            _signal(1, effective_date=dt.date(2024, 1, 10)),
            _signal(2, ts_code="000002.SZ", source_type="tushare_express", effective_date=dt.date(2024, 5, 1)),
        ],
        lookback_days=365,
    )

    summary = summarize_cycle_coverage(matches)

    assert summary["cycles"] == 2
    assert summary["matched_cycles"] == 2
    assert summary["coverage_rate"] == 1.0
    assert summary["source_coverage"]["tushare_forecast"] == 1
    assert summary["source_coverage"]["tushare_express"] == 1
    assert summary["latest_lead_buckets"]["8-30"] == 1
    assert summary["latest_lead_buckets"]["61-120"] == 1


def test_build_precision_rows_uses_future_st_horizons_and_censoring():
    cycles = build_st_cycles([_st_event(10, effective_date=dt.date(2024, 4, 10))])
    rows = build_precision_rows(
        [
            _signal(1, effective_date=dt.date(2024, 1, 10)),
            _signal(2, effective_date=dt.date(2024, 3, 1)),
            _signal(3, effective_date=dt.date(2024, 12, 1)),
        ],
        cycles,
        horizons=(30, 90, 180),
        study_start=dt.date(2024, 1, 1),
        study_end=dt.date(2024, 12, 31),
        combo_window_days=120,
    )

    by_id = {row["signal_id"]: row for row in rows}
    assert by_id[1]["hit_90d"] is False
    assert by_id[1]["hit_180d"] is True
    assert by_id[2]["hit_90d"] is True
    assert by_id[3]["eligible_90d"] is False


def test_market_cap_enrichment_adds_size_bucket_without_changing_labels():
    precision_rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "effective_trade_date": dt.date(2024, 1, 10),
            "event_type": "financial_forecast_loss",
            "metric_detail": {"worst_loss_wan": -100000.0},
            "eligible_365d": True,
            "hit_365d": True,
        },
        {
            "signal_id": 2,
            "ts_code": "000002.SZ",
            "effective_trade_date": dt.date(2024, 1, 10),
            "eligible_365d": True,
            "hit_365d": False,
        },
    ]
    keys = required_market_cap_keys(precision_rows)

    rows = enrich_precision_rows_with_market_cap(
        precision_rows,
        {("000001.SZ", dt.date(2024, 1, 10)): 400000.0},
    )

    assert ("000001.SZ", dt.date(2024, 1, 10)) in keys
    assert rows[0]["market_cap_bucket"] == "mv_lt_5bn_yuan"
    assert rows[0]["loss_to_market_cap_bucket"] == "loss_20pct_to_50pct_mv"
    assert rows[0]["hit_365d"] is True
    assert rows[1]["market_cap_bucket"] == "mv_unknown"
    assert rows[1]["loss_to_market_cap_bucket"] == "loss_mv_unknown"


def test_industry_enrichment_uses_pit_or_unknown_without_changing_labels():
    precision_rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "effective_trade_date": dt.date(2024, 1, 10),
            "eligible_365d": True,
            "hit_365d": True,
        }
    ]

    rows = enrich_precision_rows_with_industry(
        precision_rows,
        {("000001.SZ", dt.date(2024, 1, 10)): {"industry": "银行", "industry_source": "bak_basic"}},
    )

    assert rows[0]["industry"] == "银行"
    assert rows[0]["industry_source"] == "bak_basic"
    assert rows[0]["hit_365d"] is True


def test_loss_history_enrichment_counts_strict_streak_and_rolling_periods():
    cycles = build_st_cycles([_st_event(10, effective_date=dt.date(2024, 6, 3))])
    rows = build_precision_rows(
        [
            _signal(1, effective_date=dt.date(2023, 5, 1), report_period=dt.date(2023, 3, 31)),
            _signal(2, effective_date=dt.date(2023, 8, 1), report_period=dt.date(2023, 6, 30)),
            _signal(3, effective_date=dt.date(2023, 11, 1), report_period=dt.date(2023, 9, 30)),
            _signal(4, effective_date=dt.date(2024, 2, 1), report_period=dt.date(2023, 12, 31)),
        ],
        cycles,
        horizons=(365,),
        study_start=dt.date(2023, 1, 1),
        study_end=dt.date(2024, 12, 31),
    )

    enriched = enrich_precision_rows_with_loss_history(rows)
    by_id = {row["signal_id"]: row for row in enriched}

    assert by_id[1]["loss_report_streak"] == 1
    assert by_id[4]["loss_report_streak"] == 4
    assert by_id[4]["loss_report_streak_bucket"] == "loss_reports_ge_4"
    assert by_id[4]["loss_report_count_730d"] == 4


def test_aggregate_precision_rows_groups_by_source_and_combo_count():
    cycles = build_st_cycles([_st_event(10, effective_date=dt.date(2024, 4, 10))])
    rows = build_precision_rows(
        [
            _signal(1, source_type="tushare_forecast", effective_date=dt.date(2024, 1, 10)),
            _signal(2, source_type="tushare_express", effective_date=dt.date(2024, 3, 1)),
            _signal(3, ts_code="000002.SZ", source_type="tushare_forecast", effective_date=dt.date(2024, 3, 1)),
        ],
        cycles,
        horizons=(90,),
        study_start=dt.date(2024, 1, 1),
        study_end=dt.date(2024, 12, 31),
        combo_window_days=120,
    )

    by_source = aggregate_precision_rows(rows, group_fields=("source_type",), horizons=(90,))
    source_map = {row["source_type"]: row for row in by_source}

    assert source_map["tushare_forecast"]["signals"] == 2
    assert source_map["tushare_forecast"]["hits_90d"] == 0
    assert source_map["tushare_express"]["signals"] == 1
    assert source_map["tushare_express"]["precision_90d"] == 1.0

    by_combo = aggregate_precision_rows(rows, group_fields=("combo_source_count",), horizons=(90,))
    combo_map = {row["combo_source_count"]: row for row in by_combo}
    assert combo_map[2]["signals"] == 1




def test_signal_return_rows_compute_cumulative_and_post_effective_returns():
    trading_days = [
        dt.date(2024, 1, 9),
        dt.date(2024, 1, 10),
        dt.date(2024, 1, 11),
        dt.date(2024, 1, 17),
    ]
    precision_rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "source_type": "tushare_forecast",
            "event_type": "financial_forecast_loss",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "signal_year": 2024,
            "effective_trade_date": dt.date(2024, 1, 10),
            "combo_source_count": 1,
            "hit_90d": True,
            "hit_180d": True,
            "hit_365d": True,
            "days_to_next_st_cycle": 20,
        }
    ]
    prices = {
        ("000001.SZ", dt.date(2024, 1, 9)): 10.0,
        ("000001.SZ", dt.date(2024, 1, 10)): 9.0,
        ("000001.SZ", dt.date(2024, 1, 11)): 8.1,
        ("000001.SZ", dt.date(2024, 1, 17)): 8.5,
    }

    keys = required_signal_return_price_keys(precision_rows, trading_days, return_windows=(0, 1))
    assert ("000001.SZ", dt.date(2024, 1, 9)) in keys
    assert ("000001.SZ", dt.date(2024, 1, 11)) in keys

    rows = build_signal_return_rows(precision_rows, trading_days, prices, return_windows=(0, 1))
    by_window = {row["window"]: row for row in rows}

    assert round(by_window[0]["cumulative_return_from_prev_close"], 6) == -0.1
    assert round(by_window[1]["cumulative_return_from_prev_close"], 6) == -0.19
    assert round(by_window[1]["post_effective_return_from_t0_close"], 6) == -0.1

    aggregates = aggregate_return_rows(rows, group_fields=("event_type", "window"))
    agg_map = {(row["event_type"], row["window"]): row for row in aggregates}
    assert agg_map[("financial_forecast_loss", 1)]["negative_cumulative_return_rate"] == 1.0


def test_cycle_pre_st_returns_use_only_matched_cycle_dates():
    trading_days = [
        dt.date(2024, 1, 10),
        dt.date(2024, 3, 1),
        dt.date(2024, 4, 9),
        dt.date(2024, 4, 10),
    ]
    cycle_matches = [
        {
            "cycle_id": "000001.SZ:20240410:001",
            "ts_code": "000001.SZ",
            "cycle_start_effective_trade_date": dt.date(2024, 4, 10),
            "cycle_primary_event_type": "stock_st_imposed",
            "matched": True,
            "source_type_count": 2,
            "earliest_signal_effective_trade_date": dt.date(2024, 1, 10),
            "latest_signal_effective_trade_date": dt.date(2024, 3, 1),
            "earliest_lead_days": 91,
            "latest_lead_days": 40,
        }
    ]
    prices = {
        ("000001.SZ", dt.date(2024, 1, 10)): 10.0,
        ("000001.SZ", dt.date(2024, 3, 1)): 8.0,
        ("000001.SZ", dt.date(2024, 4, 9)): 7.0,
    }

    keys = required_cycle_return_price_keys(cycle_matches, trading_days)
    assert ("000001.SZ", dt.date(2024, 4, 9)) in keys

    rows = build_cycle_pre_st_return_rows(cycle_matches, trading_days, prices)
    by_kind = {row["signal_kind"]: row for row in rows}
    assert round(by_kind["earliest"]["signal_to_pre_st_return"], 6) == -0.3
    assert round(by_kind["latest"]["signal_to_pre_st_return"], 6) == -0.125

    aggregate = aggregate_cycle_return_rows(rows, group_fields=("signal_kind",))
    agg_map = {row["signal_kind"]: row for row in aggregate}
    assert agg_map["earliest"]["negative_return_rate"] == 1.0


def test_candidate_rules_keep_financial_signals_research_only():
    precision_by_event = [
        {
            "event_type": "financial_forecast_loss",
            "signals": 100,
            "precision_180d": 0.06,
            "precision_365d": 0.12,
        }
    ]
    precision_by_combo = [
        {
            "combo_source_count": 3,
            "signals": 50,
            "precision_180d": 0.04,
            "precision_365d": 0.11,
        }
    ]
    returns = [
        {
            "event_type": "financial_forecast_loss",
            "window": 20,
            "mean_cumulative_return": -0.03,
        }
    ]

    candidates = build_candidate_rules(precision_by_event, precision_by_combo, [], [], returns)

    assert candidates[0]["recommended_next_step"] == "research_score_down_candidate"
    assert candidates[0]["hard_block_allowed"] is False
    assert candidates[0]["force_exit_allowed"] is False
    assert candidates[1]["recommended_next_step"] == "research_combo_threshold"


def test_candidate_precision_rows_support_event_combo_and_metric_combo_rules():
    precision_rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "event_type": "financial_forecast_loss",
            "source_type": "tushare_forecast",
            "risk_level": "P2_REVIEW",
            "action": "warn_review",
            "signal_year": 2024,
            "effective_trade_date": dt.date(2024, 1, 10),
            "combo_source_count": 2,
            "combo_source_key": "sourceA+sourceB",
            "metric_bucket": "bucketA",
            "loss_to_market_cap_bucket": "loss_20pct_to_50pct_mv",
            "eligible_365d": True,
            "hit_365d": True,
            "days_to_next_st_cycle": 90,
        }
    ]
    candidate_rules = [
        {
            "candidate_type": "event_type",
            "key": "financial_forecast_loss",
            "recommended_next_step": "research_score_down_candidate",
        },
        {
            "candidate_type": "combo_source_count",
            "key": "trailing_source_count>=3",
            "recommended_next_step": "research_combo_threshold",
        },
        {
            "candidate_type": "metric_bucket_combo_source_key",
            "key": "financial_forecast_loss|bucketA|sourceA+sourceB",
            "recommended_next_step": "research_metric_combo_threshold",
        },
        {
            "candidate_type": "event_loss_to_market_cap_bucket",
            "key": "financial_forecast_loss|loss_20pct_to_50pct_mv",
            "recommended_next_step": "research_event_relative_loss_threshold",
        },
    ]

    rows = build_candidate_precision_rows(precision_rows, candidate_rules)
    keys = {row["candidate_key"] for row in rows}

    assert keys == {
        "financial_forecast_loss",
        "financial_forecast_loss|bucketA|sourceA+sourceB",
        "financial_forecast_loss|loss_20pct_to_50pct_mv",
    }


def test_candidate_return_rows_duplicate_signal_returns_by_candidate_membership():
    candidate_precision_rows = [
        {
            "signal_id": 1,
            "candidate_type": "event_type",
            "candidate_key": "financial_forecast_loss",
            "recommended_next_step": "research_score_down_candidate",
        },
        {
            "signal_id": 1,
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "recommended_next_step": "research_metric_threshold",
        },
    ]
    signal_return_rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "window": 20,
            "window_name": "T0_T+20",
            "cumulative_return_from_prev_close": -0.1,
            "post_effective_return_from_t0_close": -0.05,
            "missing_price": False,
        }
    ]

    rows = build_candidate_return_rows(signal_return_rows, candidate_precision_rows)

    assert len(rows) == 2
    assert {row["candidate_type"] for row in rows} == {"event_type", "metric_bucket"}


def test_candidate_stability_decision_keeps_hard_actions_disabled():
    precision_overall = [
        {
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "signals": 20,
            "eligible_180d": 20,
            "hits_180d": 4,
            "precision_180d": 0.2,
            "eligible_365d": 20,
            "hits_365d": 5,
            "precision_365d": 0.25,
        }
    ]
    precision_yearly = [
        {
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "signal_year": 2022,
            "eligible_365d": 10,
            "precision_365d": 0.2,
        },
        {
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "signal_year": 2023,
            "eligible_365d": 10,
            "precision_365d": 0.3,
        },
    ]
    return_yearly = [
        {
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "signal_year": 2022,
            "window": 20,
            "valid_cumulative_returns": 10,
            "median_cumulative_return": -0.03,
            "negative_cumulative_return_rate": 0.6,
        },
        {
            "candidate_type": "metric_bucket",
            "candidate_key": "financial_forecast_loss|bucketA",
            "signal_year": 2023,
            "window": 20,
            "valid_cumulative_returns": 10,
            "median_cumulative_return": -0.01,
            "negative_cumulative_return_rate": 0.55,
        },
    ]

    decisions = build_candidate_stability_decisions(
        precision_overall,
        precision_yearly,
        return_yearly,
        min_eligible_365=1,
        min_yearly_eligible_365=1,
        min_stable_years=2,
    )

    assert decisions[0]["decision"] == "qe_overlay_research_candidate"
    assert decisions[0]["hard_block_allowed"] is False
    assert decisions[0]["force_exit_allowed"] is False
    assert decisions[0]["alpha_boost_allowed"] is False




def test_classify_metric_bucket_extracts_forecast_and_express_loss_size():
    forecast_bucket, forecast_detail = classify_metric_bucket(
        "financial_forecast_loss",
        {"raw_payload": {"type": "??", "net_profit_min": -2800.0, "net_profit_max": -2200.0}},
    )
    assert forecast_bucket == "forecast_loss:type=??|loss_100m_to_1bn_yuan"
    assert forecast_detail["worst_loss_wan"] == -2800.0

    express_bucket, express_detail = classify_metric_bucket(
        "financial_express_loss",
        {"raw_payload": {"n_income": -150000000.0, "total_hldr_eqy_exc_min_int": -1.0}},
    )
    assert express_bucket == "express_loss:loss_100m_to_1bn_yuan|negative_equity"
    assert express_detail["equity_bucket"] == "negative_equity"


def test_extract_report_period_from_raw_payload_or_source_key():
    assert extract_report_period({"raw_payload": {"end_date": "20231231"}}) == dt.date(2023, 12, 31)
    assert extract_report_period({"source_record_key": "x:000001.SZ:20240131:20231231"}) == dt.date(2023, 12, 31)
