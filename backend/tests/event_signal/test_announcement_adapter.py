import datetime as dt
from decimal import Decimal

from backend.services.event_signal.announcement_adapter import (
    ENGINE_NAME,
    SIGNAL_RISK_LEVELS,
    UNIFIED_RULE_VERSION,
    build_event_key,
    build_fact_tuple,
    build_signal_key,
    build_signal_tuple,
    unified_rule_config,
)
from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION


def _sample_row(risk_level: str = "P0_BLOCK") -> dict:
    return {
        "classification_id": 11,
        "ann_id": 123,
        "ts_code": "000001.SZ",
        "ann_date": dt.date(2026, 5, 6),
        "title_hash": "abc123",
        "source_rule_version": ANNOUNCEMENT_RULE_VERSION,
        "event_type": "delisting_or_risk_warning",
        "risk_level": risk_level,
        "action": "block_buy" if risk_level == "P0_BLOCK" else "warn_review",
        "needs_llm": "NO",
        "matched_rule": "delisting_or_risk_warning",
        "matched_text": "ST",
        "source_time_quality": "LOCAL_FIRST_SEEN",
        "effective_trade_date": dt.date(2026, 5, 7),
        "effective_rule": "local_first_seen_after_preopen_next_trading_day",
        "available_at": dt.datetime(2026, 5, 6, 18, 30, tzinfo=dt.timezone.utc),
        "confidence": Decimal("0.9500"),
        "severity_score": Decimal("1.0000"),
        "classification_detail": {"title": "sample"},
        "time_mode": "paper",
        "title": "sample announcement title",
        "ann_signal_id": 99,
        "ann_signal_status": "ACTIVE",
        "ann_signal_reason": "P0 sample",
        "ann_signal_evidence": {"matched_rule": "delisting_or_risk_warning"},
    }


def test_unified_rule_config_keeps_phase_two_isolated():
    config = unified_rule_config()

    assert config["adapters"]["announcement"]["source_rule_version"] == ANNOUNCEMENT_RULE_VERSION
    assert config["adapters"]["announcement"]["signal_risk_levels"] == list(SIGNAL_RISK_LEVELS)
    assert config["llm_enabled"] is False
    assert config["pdf_enabled"] is False
    assert config["trading_consumption_enabled"] is False


def test_announcement_event_and_signal_keys_include_time_mode():
    assert build_event_key(123, time_mode="backtest") != build_event_key(123, time_mode="paper")
    assert build_signal_key(123, time_mode="backtest") != build_signal_key(123, time_mode="paper")
    assert build_event_key(123, time_mode="paper") == (
        f"event_fact:announcement:123:{UNIFIED_RULE_VERSION}:paper"
    )
    assert build_signal_key(123, time_mode="paper") == (
        f"event_signal:announcement:123:{UNIFIED_RULE_VERSION}:paper:risk"
    )


def test_build_fact_tuple_preserves_source_classification_semantics():
    row = _sample_row()
    fact = build_fact_tuple(row, run_id="run-1")

    assert fact[0] == f"event_fact:announcement:123:{UNIFIED_RULE_VERSION}:paper"
    assert fact[1] == "000001.SZ"
    assert fact[2] == "announcement"
    assert fact[3] == "delisting_or_risk_warning"
    assert fact[5] == "announcement"
    assert fact[6] == "123"
    assert fact[10] == "LOCAL_FIRST_SEEN"
    assert fact[12] == dt.date(2026, 5, 7)
    assert fact[15] == UNIFIED_RULE_VERSION
    assert fact[16] == "run-1"
    assert fact[19] == "abc123"


def test_build_signal_tuple_is_risk_only_and_keeps_alpha_disabled():
    row = _sample_row()
    signal = build_signal_tuple(row, event_id=456, run_id="run-1")

    assert signal[0] == f"event_signal:announcement:123:{UNIFIED_RULE_VERSION}:paper:risk"
    assert signal[2] == 456
    assert signal[3] == [456]
    assert signal[5] == "announcement"
    assert signal[12] == "announcement"
    assert signal[15] == "block_buy"
    assert signal[16] == "risk"
    assert signal[20] == 0
    assert signal[24] == UNIFIED_RULE_VERSION
    assert signal[25] == "run-1"


def test_signal_evidence_records_adapter_without_consumption_path():
    row = _sample_row()
    signal = build_signal_tuple(row, event_id=456, run_id="run-1")
    evidence_json = signal[22]

    assert ENGINE_NAME in repr(evidence_json.adapted)
    assert evidence_json.adapted["unified_rule_version"] == UNIFIED_RULE_VERSION
    assert evidence_json.adapted["source_ann_rule_version"] == ANNOUNCEMENT_RULE_VERSION
