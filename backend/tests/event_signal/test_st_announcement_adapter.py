import datetime as dt
from decimal import Decimal

from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION
from backend.services.event_signal.st_announcement_adapter import (
    ENGINE_NAME,
    ST_FIRST_EVENT_TYPES,
    ST_SIGNAL_EVENT_TYPES,
    ST_UNIFIED_RULE_VERSION,
    attach_st_cross_checks,
    select_best_st_event,
    st_first_rule_config,
)


def _sample_row() -> dict:
    return {
        "classification_id": 1,
        "ann_id": 100,
        "ts_code": "000001.SZ",
        "ann_date": dt.date(2026, 4, 30),
        "title_hash": "hash",
        "source_rule_version": ANNOUNCEMENT_RULE_VERSION,
        "event_type": "stock_st_imposed",
        "risk_level": "P0_BLOCK",
        "action": "block_buy",
        "needs_llm": "NO",
        "matched_rule": "stock_st_imposed",
        "matched_text": "退市风险警示",
        "source_time_quality": "EXACT",
        "effective_trade_date": dt.date(2026, 5, 6),
        "effective_rule": "exact_after_preopen_next_trading_day",
        "available_at": dt.datetime(2026, 4, 30, 18, 0, tzinfo=dt.timezone.utc),
        "confidence": Decimal("0.95"),
        "severity_score": Decimal("1.00"),
        "classification_detail": {"title": "sample"},
        "time_mode": "backtest",
        "title": "关于公司股票被实施退市风险警示暨停牌的公告",
        "ann_signal_evidence": {},
    }


def test_st_first_rule_config_is_isolated_from_trading_consumers():
    config = st_first_rule_config()

    assert config["version"] == ST_UNIFIED_RULE_VERSION
    assert config["engine_name"] == ENGINE_NAME
    assert config["llm_enabled"] is False
    assert config["pdf_enabled"] is False
    assert config["trading_consumption_enabled"] is False
    assert config["adapters"]["announcement_st_first"]["source_rule_version"] == ANNOUNCEMENT_RULE_VERSION
    assert config["adapters"]["announcement_st_first"]["event_types"] == list(ST_FIRST_EVENT_TYPES)
    assert config["adapters"]["announcement_st_first"]["signal_event_types"] == list(ST_SIGNAL_EVENT_TYPES)


def test_select_best_st_event_matches_nearest_pub_or_effective_date():
    row = _sample_row()
    candidates = [
        {
            "ts_code": "000001.SZ",
            "pub_date": dt.date(2026, 4, 10),
            "imp_date": dt.date(2026, 4, 11),
            "st_type": "old",
            "st_reason": "old",
            "st_explain": None,
            "source_api": "test",
        },
        {
            "ts_code": "000001.SZ",
            "pub_date": dt.date(2026, 4, 30),
            "imp_date": dt.date(2026, 5, 6),
            "st_type": "*ST",
            "st_reason": "financial",
            "st_explain": "matched",
            "source_api": "test",
        },
    ]

    result = select_best_st_event(row, candidates)

    assert result["checked"] is True
    assert result["matched"] is True
    assert result["pub_date"] == dt.date(2026, 4, 30)
    assert result["imp_date"] == dt.date(2026, 5, 6)
    assert result["st_type"] == "*ST"
    assert result["distance_days"] == 0


def test_select_best_st_event_reports_unmatched_outside_window():
    row = _sample_row()
    candidates = [
        {
            "ts_code": "000001.SZ",
            "pub_date": dt.date(2025, 12, 1),
            "imp_date": dt.date(2025, 12, 2),
            "st_type": "old",
        }
    ]

    result = select_best_st_event(row, candidates)

    assert result["checked"] is True
    assert result["matched"] is False
    assert result["match_reason"] == "nearest_stock_st_event_outside_5_day_window"


def test_attach_st_cross_checks_adds_evidence_without_mutating_source_row():
    row = _sample_row()
    st_event = {
        "ts_code": "000001.SZ",
        "pub_date": dt.date(2026, 4, 30),
        "imp_date": dt.date(2026, 5, 6),
        "st_type": "*ST",
        "st_reason": "financial",
        "st_explain": "matched",
        "source_api": "test",
    }

    rows, matched = attach_st_cross_checks([row], {"000001.SZ": [st_event]})

    assert matched == 1
    assert "st_cross_check" not in row["classification_detail"]
    enriched = rows[0]
    assert enriched["classification_detail"]["st_cross_check"]["matched"] is True
    assert enriched["ann_signal_evidence"]["st_cross_check"]["st_type"] == "*ST"
    assert enriched["ann_signal_evidence"]["adapter"] == ENGINE_NAME
    assert enriched["ann_signal_reason"].startswith("P0_BLOCK stock_st_imposed")
