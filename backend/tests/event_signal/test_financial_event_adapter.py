import datetime as dt
from decimal import Decimal

from backend.services.event_signal.announcement_adapter import unified_rule_config
from backend.services.event_signal.financial_event_adapter import (
    FINANCIAL_RULE_VERSION,
    SOURCE_TABLES,
    build_fact,
    build_miss_relations,
    build_relation_signal_key,
    build_signal_tuple,
    classify_actual,
    classify_forecast,
    infer_effective_date,
    next_trading_day,
)


TRADING_DAYS = [
    dt.date(2024, 1, 31),
    dt.date(2024, 2, 1),
    dt.date(2024, 2, 2),
    dt.date(2024, 2, 5),
]


def _raw_row(source_type: str, raw_id: int, payload: dict, ann_date: dt.date = dt.date(2024, 1, 31)) -> dict:
    return {
        "source_type": source_type,
        "raw_observation_id": raw_id,
        "source_api": f"{source_type}_vip",
        "source_record_key": f"{source_type}:{raw_id}",
        "ts_code": "000001.SZ",
        "ann_date": ann_date,
        "report_period": dt.date(2023, 12, 31),
        "source_row_hash": f"hash-{raw_id}",
        "raw_payload": payload,
        "first_seen_at": dt.datetime(2024, 2, 1, 8, 0, tzinfo=dt.timezone.utc),
    }


def test_unified_rule_config_includes_financial_sources_but_keeps_consumption_disabled():
    config = unified_rule_config()

    assert "tushare_financial_raw" in config["adapters"]
    assert config["adapters"]["tushare_financial_raw"]["source_tables"] == list(SOURCE_TABLES.values())
    assert config["adapters"]["tushare_financial_raw"]["positive_alpha_enabled"] is False
    assert config["trading_consumption_enabled"] is False


def test_classify_forecast_large_growth_is_record_only_research():
    result = classify_forecast({"type": "预增", "p_change_min": 80, "p_change_max": 120})

    assert result.event_type == "financial_forecast_large_growth"
    assert result.risk_level == "P3_POSITIVE_CANDIDATE"
    assert result.action == "record_only"
    assert result.signal_type == "research"
    assert result.metrics["forecast_mid"] == Decimal("100")
    assert result.should_signal is True


def test_classify_forecast_loss_is_review_risk():
    result = classify_forecast({"type": "首亏", "p_change_min": -200, "p_change_max": -100})

    assert result.event_type == "financial_forecast_loss"
    assert result.risk_level == "P2_REVIEW"
    assert result.action == "warn_review"
    assert result.signal_type == "risk"


def test_classify_actual_uses_express_and_fina_yoy_fields():
    express = classify_actual({"yoy_dedu_np": -60, "n_income": 100}, "tushare_express")
    fina = classify_actual({"dt_netprofit_yoy": 85}, "tushare_fina_indicator")

    assert express.event_type == "financial_express_large_decline"
    assert express.risk_level == "P2_REVIEW"
    assert fina.event_type == "financial_indicator_large_growth"
    assert fina.risk_level == "P3_POSITIVE_CANDIDATE"


def test_effective_date_backtest_uses_next_trading_day_for_date_only_sources():
    quality, available_at, effective, rule = infer_effective_date(
        dt.date(2024, 1, 31),
        TRADING_DAYS,
        time_mode="backtest",
        first_seen_at=dt.datetime(2024, 1, 31, 7, 0),
    )

    assert quality == "DATE_ONLY"
    assert available_at is None
    assert effective == dt.date(2024, 2, 1)
    assert rule == "tushare_date_only_next_trading_day"


def test_effective_date_observed_can_use_local_first_seen():
    quality, available_at, effective, rule = infer_effective_date(
        dt.date(2024, 1, 31),
        TRADING_DAYS,
        time_mode="paper",
        first_seen_at=dt.datetime(2024, 2, 1, 8, 0),
    )

    assert quality == "LOCAL_FIRST_SEEN"
    assert available_at is not None
    assert effective == dt.date(2024, 2, 1)
    assert rule == "local_first_seen_before_preopen"


def test_build_fact_and_signal_keep_financial_alpha_disabled():
    row = _raw_row("tushare_forecast", 1, {"type": "预增", "p_change_min": 80, "p_change_max": 120})
    fact = build_fact(
        row,
        trading_days=TRADING_DAYS,
        run_id="run-1",
        rule_version="unified_event_signal_rules_v0_20260506",
        time_mode="backtest",
    )
    signal = build_signal_tuple(
        fact,
        event_id=101,
        run_id="run-1",
        rule_version="unified_event_signal_rules_v0_20260506",
        time_mode="backtest",
    )

    assert fact.event_key == "event_fact:tushare_forecast:1:unified_event_signal_rules_v0_20260506:backtest"
    assert fact.classification.event_type == "financial_forecast_large_growth"
    assert signal[14] == "P3_POSITIVE_CANDIDATE"
    assert signal[15] == "record_only"
    assert signal[16] == "research"
    assert signal[20] == Decimal("0.0")
    assert signal[22].adapted["financial_rule_version"] == FINANCIAL_RULE_VERSION


def test_build_miss_relations_detects_positive_forecast_but_actual_miss():
    forecast = build_fact(
        _raw_row("tushare_forecast", 1, {"type": "预增", "p_change_min": 100, "p_change_max": 140}),
        trading_days=TRADING_DAYS,
        run_id="run-1",
        rule_version="unified_event_signal_rules_v0_20260506",
        time_mode="backtest",
    )
    actual = build_fact(
        _raw_row("tushare_fina_indicator", 2, {"dt_netprofit_yoy": 45}),
        trading_days=TRADING_DAYS,
        run_id="run-1",
        rule_version="unified_event_signal_rules_v0_20260506",
        time_mode="backtest",
    )

    relations = build_miss_relations(
        [forecast, actual],
        {forecast.event_key: 101, actual.event_key: 202},
        rule_version="unified_event_signal_rules_v0_20260506",
        run_id="run-1",
    )

    assert len(relations) == 1
    relation_tuple, _, _, strength = relations[0]
    assert relation_tuple[1] == "misses_prior_expectation"
    assert relation_tuple[4] == 101
    assert relation_tuple[5] == 202
    assert relation_tuple[11].adapted["forecast_mid"] == Decimal("120")
    assert relation_tuple[11].adapted["actual_yoy"] == Decimal("45")
    assert strength == Decimal("0.75")
    assert build_relation_signal_key(relation_tuple[0], rule_version="v", time_mode="backtest").startswith(
        "event_signal:financial_relation:"
    )


def test_next_trading_day_can_select_same_day_for_observed_preopen():
    assert next_trading_day(TRADING_DAYS, dt.date(2024, 2, 1), strictly_after=False) == dt.date(2024, 2, 1)
    assert next_trading_day(TRADING_DAYS, dt.date(2024, 2, 1), strictly_after=True) == dt.date(2024, 2, 2)
