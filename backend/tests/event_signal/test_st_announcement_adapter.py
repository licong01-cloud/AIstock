import datetime as dt
from decimal import Decimal

from backend.services.announcements.title_classifier import RULE_VERSION as ANNOUNCEMENT_RULE_VERSION
from backend.services.canonical_equity_pit import CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT
from backend.services.event_signal.st_announcement_adapter import (
    ENGINE_NAME,
    ST_FIRST_EVENT_TYPES,
    ST_SIGNAL_EVENT_TYPES,
    ST_UNIFIED_RULE_VERSION,
    attach_st_cross_checks,
    select_best_st_event,
    st_first_rule_config,
)
from backend.services.event_signal.announcement_issuer_binding import attach_announcement_issuer_bindings


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
        "issuer_candidate_ts_codes": ["000001.SZ"],
    }


def test_st_first_rule_config_is_isolated_from_trading_consumers():
    config = st_first_rule_config()

    assert config["version"] == ST_UNIFIED_RULE_VERSION
    assert config["phase"] == "st_first_announcement_rules_v2"
    assert config["engine_name"] == ENGINE_NAME
    assert config["llm_enabled"] is False
    assert config["pdf_enabled"] is False
    assert config["trading_consumption_enabled"] is False
    assert config["adapters"]["announcement_st_first"]["source_rule_version"] == ANNOUNCEMENT_RULE_VERSION
    assert config["adapters"]["announcement_st_first"]["event_types"] == list(ST_FIRST_EVENT_TYPES)
    assert config["adapters"]["announcement_st_first"]["signal_event_types"] == list(ST_SIGNAL_EVENT_TYPES)
    assert "stock_delisting_predecision" in ST_FIRST_EVENT_TYPES
    assert "stock_delisting_predecision" in ST_SIGNAL_EVENT_TYPES


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


def test_select_best_st_event_rejects_future_cross_check_for_pit_signal() -> None:
    row = _sample_row()
    result = select_best_st_event(
        row,
        [
            {
                "ts_code": "000001.SZ",
                "pub_date": dt.date(2026, 5, 2),
                "imp_date": dt.date(2026, 5, 6),
                "st_type": "终止上市",
            }
        ],
    )

    assert result["matched"] is False
    assert result["match_reason"] == (
        "no_stock_st_events_available_by_announcement_known_date"
    )
    assert result["announcement_known_date"] == dt.date(2026, 5, 1)


def test_select_best_st_event_marks_only_terminal_evidence():
    row = _sample_row()
    result = select_best_st_event(
        row,
        [
            {
                "ts_code": "000001.SZ",
                "pub_date": dt.date(2026, 4, 30),
                "imp_date": dt.date(2026, 5, 6),
                "st_type": "终止上市并摘牌",
                "st_reason": "交易所决定",
            }
        ],
    )

    assert result["matched"] is True
    assert result["terminal"] is True


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


def test_cross_check_preserves_classifier_binding_without_pre_authorizing_terminal_contract():
    row = _sample_row()
    row.update(
        {
            "event_type": "stock_delisting_confirmed",
            "classification_detail": {
                "issuer_binding": {
                    "status": "verified",
                    "terminal_subject": "self",
                    "ts_code": "000001.SZ",
                }
            },
        }
    )

    rows, _ = attach_st_cross_checks([row], {})

    evidence = rows[0]["ann_signal_evidence"]
    assert evidence["classifier_issuer_binding"]["status"] == "verified"
    assert "terminal_evidence_contract" not in evidence


def test_predecision_is_high_risk_signal_but_never_terminal_evidence():
    row = _sample_row()
    row.update(
        {
            "event_type": "stock_delisting_predecision",
            "matched_rule": "stock_delisting_predecision",
            "classification_detail": {
                "issuer_binding": {
                    "status": "verified",
                    "terminal_subject": "not_required",
                }
            },
        }
    )

    rows, _ = attach_st_cross_checks([row], {})
    bound, counts = attach_announcement_issuer_bindings(
        rows,
        require_terminal_cross_check=True,
    )

    evidence = bound[0]["ann_signal_evidence"]
    assert bound[0]["risk_level"] == "P0_BLOCK"
    assert bound[0]["action"] == "block_buy"
    assert bound[0]["ann_signal_status"] == "ACTIVE"
    assert "terminal_evidence_contract" not in evidence
    assert counts == {"EXACT": 1}


def test_terminal_signal_is_suppressed_when_st_cross_check_is_missing():
    row = _sample_row()
    row["event_type"] = "stock_delisting_confirmed"
    rows, _matched = attach_st_cross_checks([row], {})
    bound, counts = attach_announcement_issuer_bindings(rows, require_terminal_cross_check=True)

    assert bound[0]["ann_signal_status"] == "SUPPRESSED"
    assert bound[0]["issuer_fact_status"] == "UNKNOWN"
    assert bound[0]["ann_signal_evidence"]["issuer_binding"]["reason_code"] == (
        "announcement_terminal_evidence_cross_check_missing"
    )
    assert counts == {"TERMINAL_EVIDENCE_UNCONFIRMED": 1}


def test_exact_terminal_pipeline_emits_v2_contract_after_binding() -> None:
    row = _sample_row()
    row["event_type"] = "stock_delisting_confirmed"
    st_event = {
        "ts_code": "000001.SZ",
        "pub_date": dt.date(2026, 4, 30),
        "imp_date": dt.date(2026, 5, 6),
        "st_type": "终止上市并摘牌",
        "st_reason": "交易所决定",
    }

    rows, matched = attach_st_cross_checks([row], {"000001.SZ": [st_event]})
    bound, counts = attach_announcement_issuer_bindings(
        rows,
        require_terminal_cross_check=True,
    )

    evidence = bound[0]["ann_signal_evidence"]
    assert matched == 1
    assert bound[0]["ann_signal_status"] == "ACTIVE"
    assert evidence["issuer_binding"]["status"] == "EXACT"
    assert evidence["terminal_evidence_contract"] == (
        CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT
    )
    assert counts == {"EXACT": 1}
