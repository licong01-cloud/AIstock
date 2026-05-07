import datetime as dt

from backend.services.event_signal.st_signal_quality_report import (
    build_quality_payload,
    signal_matches_stock_st_event,
    summarize_bond_leakage,
    summarize_signal_cross_checks,
    summarize_stock_st_recall,
)


def _signal(
    signal_id: int,
    event_type: str = "stock_st_imposed",
    *,
    matched: bool = True,
    distance_days: int | None = 0,
    ts_code: str = "000001.SZ",
) -> dict:
    cross_check = {
        "checked": True,
        "matched": matched,
        "match_reason": "nearest_stock_st_event_within_5_day_window" if matched else "no_stock_st_events_for_symbol_in_window",
    }
    if distance_days is not None:
        cross_check["distance_days"] = distance_days
    return {
        "signal_id": signal_id,
        "ts_code": ts_code,
        "event_type": event_type,
        "risk_level": "P0_BLOCK",
        "action": "block_buy",
        "source_event_date": dt.date(2026, 4, 30),
        "effective_trade_date": dt.date(2026, 5, 6),
        "evidence": {"st_cross_check": cross_check},
    }


def _st_event(ts_code: str = "000001.SZ", pub_date: dt.date = dt.date(2026, 4, 30)) -> dict:
    return {
        "ts_code": ts_code,
        "pub_date": pub_date,
        "imp_date": dt.date(2026, 5, 6),
        "st_type": "*ST",
        "st_reason": "financial",
    }


def test_summarize_signal_cross_checks_counts_matches_and_reasons():
    summary = summarize_signal_cross_checks(
        [
            _signal(1, matched=True, distance_days=0),
            _signal(2, matched=False, distance_days=None),
        ]
    )

    assert summary["signal_rows"] == 2
    assert summary["checked_rows"] == 2
    assert summary["matched_rows"] == 1
    assert summary["match_rate"] == 0.5
    assert summary["by_event_type"]["stock_st_imposed"]["matched_rows"] == 1
    assert summary["match_reasons"]["no_stock_st_events_for_symbol_in_window"] == 1


def test_signal_matches_stock_st_event_within_pub_or_effective_window():
    assert signal_matches_stock_st_event(_signal(1), _st_event()) is True
    assert signal_matches_stock_st_event(_signal(1), _st_event(pub_date=dt.date(2025, 1, 1))) is True
    assert signal_matches_stock_st_event(_signal(1), _st_event(ts_code="000002.SZ")) is False


def test_summarize_stock_st_recall_reports_unmatched_examples():
    signals = [_signal(1, ts_code="000001.SZ")]
    st_events = [_st_event("000001.SZ"), _st_event("000002.SZ")]

    summary = summarize_stock_st_recall(st_events, signals)

    assert summary["stock_st_event_rows"] == 2
    assert summary["matched_stock_st_event_rows"] == 1
    assert summary["recall_rate"] == 0.5
    assert summary["unmatched_examples"][0]["ts_code"] == "000002.SZ"


def test_summarize_bond_leakage_detects_bond_facts_without_active_signals():
    facts = [
        {"event_type": "convertible_bond_delisting_or_redemption"},
        {"event_type": "stock_st_imposed"},
    ]
    signals = [_signal(1)]

    summary = summarize_bond_leakage(facts, signals)

    assert summary["bond_like_fact_rows"] == 1
    assert summary["bond_like_active_signal_rows"] == 0
    assert summary["leakage_detected"] is False


def test_build_quality_payload_combines_signal_recall_and_bond_checks():
    payload = build_quality_payload(
        signals=[_signal(1)],
        stock_st_events=[_st_event()],
        facts=[
            {"event_type": "convertible_bond_delisting_or_redemption"},
            {"event_type": "stock_st_imposed"},
        ],
    )

    assert payload["signal_rows"] == 1
    assert payload["event_type_counts"]["stock_st_imposed"] == 1
    assert payload["cross_check"]["matched_rows"] == 1
    assert payload["stock_st_recall"]["matched_stock_st_event_rows"] == 1
    assert payload["bond_leakage"]["bond_like_active_signal_rows"] == 0
