import datetime as dt

import pandas as pd

from backend.services.event_signal.early_financial_distress_research import (
    classify_metric_bucket,
    enrich_precision_rows_with_loss_history,
)
from backend.services.event_signal.financial_distress_qe_overlay_research import (
    FIRST_BATCH_RULES,
    LOSS_HISTORY_RULES,
    MID_LARGE_EVENT_RULES,
    PHASE24_RESEARCH_RULES,
    REFINEMENT_RULES,
    SIZE_BUCKET_RULES,
    CandidateScore,
    CONTEXT_SCORE_DOWN_PROFILES,
    SEVERITY_PROFILES,
    _active_dates_for_signal,
    _fixed_width_table,
    _rule_applies,
    build_context_score_down_penalty_by_date,
    build_market_cap_bucket_summary,
    build_severity_penalty_by_date,
    build_score_down_ranking,
    build_variable_score_down_ranking,
    build_overlay_frame,
    expand_simulator_scenarios,
    filter_research_rules_by_key,
    load_loop_specs,
    normalize_market_cap_bucket_counter,
    parse_loop_spec,
    run_score_down_rerank_counterfactual,
    select_research_rules,
    summarize_multiloop_validations,
)


def _rule(key: str):
    return next(
        rule
        for rule in (
            *FIRST_BATCH_RULES,
            *SIZE_BUCKET_RULES,
            *LOSS_HISTORY_RULES,
            *MID_LARGE_EVENT_RULES,
            *REFINEMENT_RULES,
            *PHASE24_RESEARCH_RULES,
        )
        if rule.rule_key == key
    )


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


def test_select_research_rules_can_run_loss_history_only():
    rules = select_research_rules(loss_history_only=True)

    assert [rule.rule_key for rule in rules] == [rule.rule_key for rule in LOSS_HISTORY_RULES]


def test_select_research_rules_can_run_mid_large_only():
    rules = select_research_rules(mid_large_only=True)

    assert [rule.rule_key for rule in rules] == [rule.rule_key for rule in MID_LARGE_EVENT_RULES]


def test_select_research_rules_can_run_refinement_only():
    rules = select_research_rules(refinement_only=True)

    assert [rule.rule_key for rule in rules] == [rule.rule_key for rule in REFINEMENT_RULES]


def test_select_research_rules_can_run_phase24_only():
    rules = select_research_rules(phase24_only=True)

    assert [rule.rule_key for rule in rules] == [rule.rule_key for rule in PHASE24_RESEARCH_RULES]


def test_select_research_rules_can_include_loss_history_with_first_batch():
    rules = select_research_rules(include_loss_history_rules=True)

    assert [rule.rule_key for rule in rules] == [
        *(rule.rule_key for rule in FIRST_BATCH_RULES),
        *(rule.rule_key for rule in LOSS_HISTORY_RULES),
    ]


def test_filter_research_rules_by_key_keeps_requested_rule_only():
    rules = filter_research_rules_by_key(
        SIZE_BUCKET_RULES,
        ["loss_to_market_cap_ge_50pct_mv_lt_10bn"],
    )

    assert [rule.rule_key for rule in rules] == ["loss_to_market_cap_ge_50pct_mv_lt_10bn"]


def test_normalize_market_cap_counter_splits_composite_buckets():
    normalized = normalize_market_cap_bucket_counter(
        {
            "mv_lt_5bn_yuan": 2,
            "mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan": 3,
            "unexpected_bucket": 4,
        }
    )

    assert normalized["mv_lt_5bn_yuan"] == 5
    assert normalized["mv_5bn_to_10bn_yuan"] == 3
    assert normalized["mv_unknown"] == 4


def test_market_cap_bucket_summary_reports_every_bucket_for_each_rule_mode():
    validation_summary = {
        "stability_rows": [
            {
                "rule_key": "loss_to_market_cap_ge_50pct_mv_lt_10bn",
                "active_trading_days": 60,
                "simulator_mode": "score_down_rank_20pct_top50_previous",
                "total_score_down_evaluated_topk_buy_events": 5,
                "total_score_down_dropped_from_topk_events": 2,
                "evaluated_market_cap_buckets": {
                    "mv_lt_5bn_yuan": 3,
                    "mv_5bn_to_10bn_yuan": 2,
                },
                "dropped_market_cap_buckets": {
                    "mv_lt_5bn_yuan": 1,
                    "mv_5bn_to_10bn_yuan": 1,
                },
                "still_market_cap_buckets": {
                    "mv_lt_5bn_yuan": 2,
                    "mv_5bn_to_10bn_yuan": 1,
                },
            }
        ]
    }
    exposure_summary = [
        {
            "rule_key": "loss_to_market_cap_ge_50pct_mv_lt_10bn",
            "active_trading_days": 60,
            "overlay_rows": 10,
            "market_cap_buckets": {
                "mv_lt_5bn_yuan": 6,
                "mv_5bn_to_10bn_yuan": 4,
            },
        }
    ]

    rows = build_market_cap_bucket_summary(
        validation_summary=validation_summary,
        exposure_summary=exposure_summary,
    )

    assert len(rows) == 6
    by_bucket = {row["market_cap_bucket"]: row for row in rows}
    assert by_bucket["mv_lt_5bn_yuan"]["overlay_rows"] == 6
    assert by_bucket["mv_lt_5bn_yuan"]["evaluated_topk_buy_events"] == 3
    assert by_bucket["mv_lt_5bn_yuan"]["dropped_from_topk_events"] == 1
    assert by_bucket["mv_lt_5bn_yuan"]["drop_rate_within_bucket"] == 1 / 3
    assert by_bucket["mv_ge_100bn_yuan"]["evaluated_topk_buy_events"] == 0


def test_loss_history_rules_match_repeated_losses_and_small_cap_variants():
    base = {
        "event_type": "financial_express_loss",
        "loss_to_market_cap_bucket": "loss_20pct_to_50pct_mv",
        "loss_report_count_730d_bucket": "loss_reports_ge_4",
        "market_cap_bucket": "mv_lt_5bn_yuan",
    }

    assert _rule_applies(base, _rule("loss_reports_ge_4"))
    assert _rule_applies(base, _rule("loss_reports_ge_4_mv_lt_10bn"))
    assert _rule_applies(base, _rule("loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss"))
    assert not _rule_applies(
        {**base, "loss_to_market_cap_bucket": "loss_50pct_to_100pct_mv"},
        _rule("loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss"),
    )
    assert not _rule_applies(
        {**base, "market_cap_bucket": "mv_10bn_to_30bn_yuan"},
        _rule("loss_reports_ge_4_mv_lt_10bn"),
    )


def test_forecast_loss_history_rule_requires_forecast_event_and_small_cap():
    matching = {
        "event_type": "financial_forecast_loss",
        "loss_to_market_cap_bucket": "loss_20pct_to_50pct_mv",
        "loss_report_count_730d_bucket": "loss_reports_ge_4",
        "market_cap_bucket": "mv_5bn_to_10bn_yuan",
    }

    assert _rule_applies(matching, _rule("forecast_loss_reports_ge_4_mv_lt_10bn"))
    assert not _rule_applies(
        {**matching, "event_type": "financial_express_loss"},
        _rule("forecast_loss_reports_ge_4_mv_lt_10bn"),
    )
    assert not _rule_applies(
        {**matching, "loss_report_count_730d_bucket": "loss_reports_3"},
        _rule("forecast_loss_reports_ge_4_mv_lt_10bn"),
    )
    assert not _rule_applies(
        {**matching, "market_cap_bucket": "mv_10bn_to_30bn_yuan"},
        _rule("forecast_loss_reports_ge_4_mv_lt_10bn"),
    )


def test_mid_large_event_rules_match_expectation_miss_and_declines():
    miss = {
        "event_type": "financial_positive_but_miss_expectation",
        "market_cap_bucket": "mv_10bn_to_30bn_yuan",
        "metric_detail": {"miss_gap": 55.0},
    }
    small_miss = {**miss, "market_cap_bucket": "mv_5bn_to_10bn_yuan"}
    mild_miss = {**miss, "metric_detail": {"miss_gap": 35.0}}
    large_miss = {**miss, "market_cap_bucket": "mv_30bn_to_100bn_yuan"}

    assert _rule_applies(miss, _rule("expectation_miss_mv_ge_10bn"))
    assert _rule_applies(miss, _rule("expectation_miss_gap_ge_50_mv_ge_10bn"))
    assert not _rule_applies(mild_miss, _rule("expectation_miss_gap_ge_50_mv_ge_10bn"))
    assert _rule_applies(large_miss, _rule("expectation_miss_mv_ge_30bn"))
    assert not _rule_applies(small_miss, _rule("expectation_miss_mv_ge_10bn"))

    assert _rule_applies(
        {"event_type": "financial_forecast_large_decline", "market_cap_bucket": "mv_10bn_to_30bn_yuan"},
        _rule("forecast_express_large_decline_mv_ge_10bn"),
    )
    assert _rule_applies(
        {"event_type": "financial_indicator_large_decline", "market_cap_bucket": "mv_30bn_to_100bn_yuan"},
        _rule("indicator_large_decline_mv_ge_10bn"),
    )
    assert _rule_applies(
        {"event_type": "financial_express_loss", "market_cap_bucket": "mv_ge_100bn_yuan"},
        _rule("structured_financial_risk_mv_ge_10bn"),
    )


def test_expectation_miss_metric_bucket_exposes_gap_for_overlay_rules():
    metric_bucket, detail = classify_metric_bucket(
        "financial_positive_but_miss_expectation",
        {
            "metrics": {
                "forecast_mid": "120.5",
                "actual_yoy": "55.0",
                "miss_gap": "65.5",
                "actual_source_type": "tushare_express",
            }
        },
    )

    assert metric_bucket == "expectation_miss:miss_gap_50pct_to_100pct|actual_source=tushare_express"
    assert detail["miss_gap"] == 65.5
    assert detail["miss_gap_bucket"] == "miss_gap_50pct_to_100pct"


def test_indicator_large_decline_metric_bucket_exposes_phase24_quality_fields():
    metric_bucket, detail = classify_metric_bucket(
        "financial_indicator_large_decline",
        {
            "metrics": {"actual_yoy": "-95.5"},
            "raw_payload": {
                "or_yoy": "12.3",
                "ocf_yoy": "-65.0",
                "debt_to_assets": "76.5",
                "current_ratio": "0.92",
                "netprofit_margin": "-4.2",
            },
        },
    )

    assert metric_bucket == "financial_indicator_large_decline:default"
    assert detail["actual_yoy"] == -95.5
    assert detail["or_yoy"] == 12.3
    assert detail["ocf_yoy"] == -65.0
    assert detail["debt_to_assets"] == 76.5
    assert detail["current_ratio"] == 0.92
    assert detail["netprofit_margin"] == -4.2


def test_prior_loss_history_is_available_for_non_loss_events():
    rows = [
        {
            "signal_id": 1,
            "ts_code": "000001.SZ",
            "event_type": "financial_forecast_loss",
            "report_period": dt.date(2023, 12, 31),
            "effective_trade_date": dt.date(2024, 1, 31),
        },
        {
            "signal_id": 2,
            "ts_code": "000001.SZ",
            "event_type": "financial_express_loss",
            "report_period": dt.date(2024, 3, 31),
            "effective_trade_date": dt.date(2024, 4, 30),
        },
        {
            "signal_id": 3,
            "ts_code": "000001.SZ",
            "event_type": "financial_indicator_large_decline",
            "report_period": dt.date(2024, 6, 30),
            "effective_trade_date": dt.date(2024, 8, 30),
        },
    ]

    enriched = enrich_precision_rows_with_loss_history(rows)
    indicator = enriched[-1]

    assert indicator["loss_report_count_730d"] == 0
    assert indicator["prior_loss_report_count_730d"] == 2
    assert indicator["prior_loss_report_count_730d_bucket"] == "loss_reports_2"


def test_refinement_rules_split_size_and_prior_loss_history():
    base = {
        "event_type": "financial_indicator_large_decline",
        "market_cap_bucket": "mv_10bn_to_30bn_yuan",
        "prior_loss_report_count_730d_bucket": "loss_reports_2",
    }

    assert _rule_applies(base, _rule("indicator_large_decline_mv_10_30bn"))
    assert _rule_applies(base, _rule("indicator_large_decline_mv_ge_10bn_prior_loss_ge_2"))
    assert _rule_applies(base, _rule("indicator_large_decline_mv_10_30bn_prior_loss_ge_2"))
    assert not _rule_applies({**base, "market_cap_bucket": "mv_30bn_to_100bn_yuan"}, _rule("indicator_large_decline_mv_10_30bn"))
    assert _rule_applies(
        {"event_type": "financial_forecast_loss", "market_cap_bucket": "mv_30bn_to_100bn_yuan"},
        _rule("structured_financial_risk_mv_ge_30bn"),
    )
    assert _rule_applies(
        {"event_type": "financial_express_loss", "market_cap_bucket": "mv_10bn_to_30bn_yuan"},
        _rule("structured_financial_risk_mv_10_30bn"),
    )


def test_phase24_expectation_miss_rules_match_gap_source_and_size():
    row = {
        "event_type": "financial_positive_but_miss_expectation",
        "market_cap_bucket": "mv_10bn_to_30bn_yuan",
        "metric_detail": {"miss_gap": 120.0, "actual_source_type": "tushare_fina_indicator"},
    }

    assert _rule_applies(row, _rule("expectation_miss_gap_ge_100_mv_ge_10bn"))
    assert _rule_applies(row, _rule("expectation_miss_gap_ge_100_mv_10_30bn"))
    assert _rule_applies(row, _rule("expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn"))
    assert not _rule_applies(
        {**row, "metric_detail": {"miss_gap": 80.0, "actual_source_type": "tushare_express"}},
        _rule("expectation_miss_gap_ge_100_mv_ge_10bn"),
    )
    assert not _rule_applies(
        {**row, "market_cap_bucket": "mv_5bn_to_10bn_yuan"},
        _rule("expectation_miss_gap_ge_100_mv_ge_10bn"),
    )


def test_phase24_indicator_rules_match_deterioration_quality_fields():
    base = {
        "event_type": "financial_indicator_large_decline",
        "market_cap_bucket": "mv_10bn_to_30bn_yuan",
        "metric_detail": {
            "actual_yoy": -120.0,
            "or_yoy": 10.0,
            "ocf_yoy": -55.0,
            "q_ocf_to_sales": -3.0,
            "debt_to_assets": 75.0,
            "current_ratio": 0.8,
            "netprofit_margin": -2.0,
        },
    }

    assert _rule_applies(base, _rule("indicator_decline_actual_yoy_le_minus100_mv_10_30bn"))
    assert _rule_applies(base, _rule("indicator_decline_actual_yoy_le_minus80_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_profit_revenue_diverge_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_negative_margin_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_debt_assets_ge_70_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_current_ratio_lt_1_mv_ge_10bn"))
    assert _rule_applies(base, _rule("indicator_decline_ocf_negative_or_leverage_mv_ge_10bn"))
    assert _rule_applies(
        {**base, "metric_detail": {**base["metric_detail"], "or_yoy": -25.0}},
        _rule("indicator_decline_profit_revenue_both_down_mv_ge_10bn"),
    )
    assert not _rule_applies(
        {**base, "market_cap_bucket": "mv_5bn_to_10bn_yuan"},
        _rule("indicator_decline_actual_yoy_le_minus80_mv_ge_10bn"),
    )


def test_expand_simulator_scenarios_adds_score_down_penalty_modes():
    scenarios = expand_simulator_scenarios(
        simulator_modes=["score_down"],
        score_down_rank_penalty_pcts=[5, 0.10],
        score_down_top_k=50,
        score_down_ranking_date_mode="previous",
    )

    assert [scenario.mode_key for scenario in scenarios] == [
        "score_down_rank_5pct_top50_previous",
        "score_down_rank_10pct_top50_previous",
    ]
    assert [scenario.rank_penalty_pct for scenario in scenarios] == [0.05, 0.10]


def test_expand_simulator_scenarios_adds_severity_profiles():
    scenarios = expand_simulator_scenarios(
        simulator_modes=["score_down_severity"],
        score_down_severity_profiles=["balanced", "conservative"],
        score_down_top_k=50,
        score_down_ranking_date_mode="previous",
    )

    assert [scenario.mode_key for scenario in scenarios] == [
        "score_down_severity_balanced_top50_previous",
        "score_down_severity_conservative_top50_previous",
    ]
    assert [scenario.severity_profile for scenario in scenarios] == ["balanced", "conservative"]


def test_expand_simulator_scenarios_adds_context_profiles():
    scenarios = expand_simulator_scenarios(
        simulator_modes=["score_down_context"],
        score_down_context_profiles=["rank_decay_light", "rank_decay_sector_relief"],
        score_down_top_k=50,
        score_down_ranking_date_mode="previous",
    )

    assert [scenario.mode_key for scenario in scenarios] == [
        "score_down_context_rank_decay_light_top50_previous",
        "score_down_context_rank_decay_sector_relief_top50_previous",
    ]
    assert [scenario.context_profile for scenario in scenarios] == ["rank_decay_light", "rank_decay_sector_relief"]


def test_build_score_down_ranking_demotes_blocked_candidates_by_topk_pct():
    candidates = [
        CandidateScore("A", 0.9),
        CandidateScore("BAD", 0.8),
        CandidateScore("C", 0.7),
        CandidateScore("D", 0.6),
    ]

    ranking = build_score_down_ranking(
        candidates=candidates,
        blocked_symbols={"BAD"},
        rank_penalty_pct=0.50,
        top_k=4,
    )
    by_symbol = {row.ts_code: row for row in ranking}

    assert by_symbol["BAD"].original_rank == 2
    assert by_symbol["BAD"].adjusted_rank == 4
    assert [row.ts_code for row in ranking] == ["A", "C", "D", "BAD"]


def test_variable_score_down_ranking_uses_symbol_specific_penalties():
    candidates = [
        CandidateScore("A", 0.9),
        CandidateScore("BAD", 0.8),
        CandidateScore("C", 0.7),
        CandidateScore("D", 0.6),
    ]

    ranking = build_variable_score_down_ranking(
        candidates=candidates,
        symbol_rank_penalty_pct={"BAD": 0.25},
        top_k=4,
    )
    by_symbol = {row.ts_code: row for row in ranking}

    assert by_symbol["BAD"].original_rank == 2
    assert by_symbol["BAD"].adjusted_rank == 3
    assert [row.ts_code for row in ranking] == ["A", "C", "BAD", "D"]


def test_severity_penalty_profile_uses_loss_size_and_loss_history():
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "BAD",
                "can_buy": False,
                "force_exit": False,
                "max_loss_to_market_cap": 1.2,
                "market_cap_buckets": "mv_lt_5bn_yuan",
                "loss_report_count_730d_max": 4,
            }
        ]
    )

    penalties = build_severity_penalty_by_date(overlay, profile=SEVERITY_PROFILES["balanced"])

    assert penalties[dt.date(2024, 1, 3)]["BAD"] == 0.25


def test_context_score_down_penalty_uses_rank_severity_decay_and_sector_relief():
    records = [
        {
            "trade_date": dt.date(2024, 1, 4),
            "ts_code": "BAD",
            "can_buy": False,
            "force_exit": False,
            "active_trading_days": 5,
            "active_signal_count": 2,
            "max_loss_to_market_cap": 1.2,
            "max_miss_gap": 55.0,
            "loss_report_count_730d_max": 1,
            "prior_loss_report_count_730d_max": 2,
            "min_active_age_trading_days": 2,
            "earliest_effective_trade_date": dt.date(2024, 1, 2),
            "industries": "software",
        },
        {
            "trade_date": dt.date(2024, 1, 4),
            "ts_code": "PEER",
            "can_buy": False,
            "force_exit": False,
            "active_trading_days": 5,
            "active_signal_count": 1,
            "max_loss_to_market_cap": None,
            "max_miss_gap": None,
            "loss_report_count_730d_max": 0,
            "prior_loss_report_count_730d_max": 0,
            "min_active_age_trading_days": 0,
            "earliest_effective_trade_date": dt.date(2024, 1, 4),
            "industries": "software",
        },
    ]
    filler = {
        "trade_date": dt.date(2024, 1, 4),
        "can_buy": False,
        "force_exit": False,
        "active_trading_days": 5,
        "active_signal_count": 1,
        "max_loss_to_market_cap": None,
        "max_miss_gap": None,
        "loss_report_count_730d_max": 0,
        "prior_loss_report_count_730d_max": 0,
        "min_active_age_trading_days": 0,
        "earliest_effective_trade_date": dt.date(2024, 1, 4),
        "industries": "software",
    }
    overlay = pd.DataFrame([*records, *[{**filler, "ts_code": f"FILL{idx:02d}"} for idx in range(18)]])
    trading_days = [
        dt.date(2024, 1, 2),
        dt.date(2024, 1, 3),
        dt.date(2024, 1, 4),
        dt.date(2024, 1, 5),
    ]
    candidate_scores = {
        dt.date(2024, 1, 3): [
            CandidateScore("BAD", 0.9),
            CandidateScore("PEER", 0.8),
            CandidateScore("OTHER", 0.7),
        ]
    }

    penalties = build_context_score_down_penalty_by_date(
        overlay,
        candidate_scores=candidate_scores,
        trading_days=trading_days,
        profile=CONTEXT_SCORE_DOWN_PROFILES["rank_decay_sector_relief"],
        top_k=3,
        ranking_date_mode="previous",
    )

    assert round(penalties[dt.date(2024, 1, 4)]["BAD"], 6) == 0.196875
    assert round(penalties[dt.date(2024, 1, 4)]["PEER"], 6) == 0.075


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
    assert overlay["min_active_age_trading_days"].tolist() == [0, 1]


def test_score_down_rerank_counterfactual_replaces_dropped_topk_buy():
    index = dt.datetime(2024, 1, 2), dt.datetime(2024, 1, 3), dt.datetime(2024, 1, 4)
    report = pd.DataFrame(
        {
            "account": [100.0, 100.0, 90.0],
            "return": [0.0, 0.0, -0.10],
            "cash": [100.0, 0.0, 0.0],
        },
        index=index,
    )
    positions = {
        dt.date(2024, 1, 2): {},
        dt.date(2024, 1, 3): {"BAD": {"amount": 1.0, "price": 100.0, "weight": 1.0}},
        dt.date(2024, 1, 4): {"BAD": {"amount": 1.0, "price": 90.0, "weight": 1.0}},
    }
    overlay = pd.DataFrame(
        [
            {
                "trade_date": dt.date(2024, 1, 3),
                "ts_code": "BAD",
                "can_buy": False,
                "force_exit": False,
                "market_cap_buckets": "mv_lt_5bn_yuan",
                "industries": "real_estate",
            },
            {
                "trade_date": dt.date(2024, 1, 4),
                "ts_code": "BAD",
                "can_buy": False,
                "force_exit": False,
                "market_cap_buckets": "mv_lt_5bn_yuan",
                "industries": "real_estate",
            },
        ]
    )
    candidate_scores = {
        dt.date(2024, 1, 2): [
            CandidateScore("A", 0.9),
            CandidateScore("B", 0.8),
            CandidateScore("BAD", 0.7),
            CandidateScore("GOOD", 0.6),
        ],
        dt.date(2024, 1, 3): [
            CandidateScore("A", 0.9),
            CandidateScore("B", 0.8),
            CandidateScore("BAD", 0.7),
            CandidateScore("GOOD", 0.6),
        ],
    }

    account, stats = run_score_down_rerank_counterfactual(
        positions=positions,
        report=report,
        overlay=overlay,
        candidate_scores=candidate_scores,
        price_returns={dt.date(2024, 1, 4): {"GOOD": 0.20}},
        date_from=dt.date(2024, 1, 2),
        date_to=dt.date(2024, 1, 4),
        rank_penalty_pct=0.50,
        top_k=3,
        ranking_date_mode="previous",
    )

    assert round(float(account.iloc[-1]), 6) == 120.0
    assert stats["score_down_evaluated_topk_buy_events"] == 1
    assert stats["score_down_dropped_from_topk_events"] == 1
    assert stats["replacement_open_events"] == 1
    assert stats["score_down_dropped_by_market_cap_buckets"] == {"mv_lt_5bn_yuan": 1}
    assert stats["score_down_dropped_by_industries_top20"] == {"real_estate": 1}
    assert stats["sample_replacement_events"][0]["replacement_symbol"] == "GOOD"


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
