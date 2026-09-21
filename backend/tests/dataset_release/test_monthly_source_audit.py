from __future__ import annotations

from datetime import date

import pytest

from backend.services.dataset_release.monthly_source_audit import (
    MonthlySourceAuditError,
    SourceGateEvidence,
    TypedGap,
    classify_adj_factor_change,
    close_source_audit,
    cn_a_share_minute_labels,
    validate_daily_basic_rows,
    validate_daily_rows,
    validate_daily_minute_parity,
    validate_six_pool_market_coverage,
    validate_trading_calendar,
)
from backend.services.dataset_release.monthly_unified import SOURCE_GATES


SHA = "a" * 64


def _gate(name: str, snapshot: str = "snapshot-1", *, observed: int = 2, unexplained: int = 0) -> SourceGateEvidence:
    return SourceGateEvidence(
        gate=name,
        snapshot_group_id=snapshot,
        expectation_contract_ref=f"contracts/{name}.json",
        readback_ref=f"readbacks/{name}.json",
        expected_count=observed + unexplained,
        observed_count=observed,
        unexplained_missing_count=unexplained,
    )


def test_source_audit_requires_all_gates_from_one_snapshot() -> None:
    gates = [_gate(name) for name in SOURCE_GATES]
    receipt = close_source_audit(
        cutoff=date(2026, 9, 30),
        predecessor_cutoff=date(2026, 8, 31),
        gates=gates,
        changes=[],
    )
    assert receipt["unexplained_gap_count"] == 0
    assert receipt["database_write_performed"] is False
    first = receipt["gates"][SOURCE_GATES[0]]
    assert first["schema_version"] == "aistock_monthly_source_gate_v2"
    assert first["status"] == "PASS"
    mixed = list(gates)
    mixed[-1] = _gate(SOURCE_GATES[-1], "snapshot-2")
    with pytest.raises(MonthlySourceAuditError, match="different snapshots"):
        close_source_audit(
            cutoff=date(2026, 9, 30),
            predecessor_cutoff=date(2026, 8, 31),
            gates=mixed,
            changes=[],
        )


def test_source_audit_fails_closed_on_unexplained_gap() -> None:
    gates = [_gate(name) for name in SOURCE_GATES]
    gates[0] = _gate(SOURCE_GATES[0], observed=1, unexplained=1)
    with pytest.raises(MonthlySourceAuditError, match="blocking"):
        close_source_audit(
            cutoff=date(2026, 9, 30),
            predecessor_cutoff=date(2026, 8, 31),
            gates=gates,
            changes=[],
        )


def test_minute_contract_uses_240_close_labels_and_cross_checks_daily() -> None:
    labels = cn_a_share_minute_labels(date(2026, 9, 30))
    assert len(labels) == 240
    assert labels[0].strftime("%H:%M") == "09:31"
    assert labels[119].strftime("%H:%M") == "11:30"
    assert labels[120].strftime("%H:%M") == "13:01"
    rows = [
        {
            "datetime": stamp,
            "open": 10.0,
            "high": 10.0,
            "low": 10.0,
            "close": 10.0,
            "vol": 1.0,
            "amount": 10.0,
        }
        for stamp in labels
    ]
    result = validate_daily_minute_parity(
        symbol="000001.SZ",
        trade_date=date(2026, 9, 30),
        daily={"open": 10, "high": 10, "low": 10, "close": 10, "vol": 240, "amount": 2400},
        minute_rows=rows,
        suspended=False,
    )
    assert result["bar_count"] == 240
    rows[120] = {**rows[120], "datetime": "2026-09-30 13:00:00"}
    with pytest.raises(MonthlySourceAuditError, match="non-session"):
        validate_daily_minute_parity(
            symbol="000001.SZ",
            trade_date=date(2026, 9, 30),
            daily={"open": 10, "high": 10, "low": 10, "close": 10, "vol": 240, "amount": 2400},
            minute_rows=rows,
            suspended=False,
        )


def test_daily_basic_checks_rows_and_all_required_fields() -> None:
    rows = [
        {"ts_code": "000001.SZ", "turnover_rate": 1.0, "turnover_rate_f": 1.1, "volume_ratio": 0.9},
        {"ts_code": "000002.SZ", "turnover_rate": 2.0, "turnover_rate_f": 2.1, "volume_ratio": None},
    ]
    with pytest.raises(MonthlySourceAuditError, match="fields=1"):
        validate_daily_basic_rows(expected_symbols=("000001.SZ", "000002.SZ"), rows=rows)
    result = validate_daily_basic_rows(
        expected_symbols=("000001.SZ", "000002.SZ"),
        rows=rows,
        nullable={
            ("000002.SZ", "volume_ratio"): TypedGap(
                dataset="daily_basic",
                symbol="000002.SZ",
                start="2026-09-30",
                end="2026-09-30",
                field="volume_ratio",
                reason_code="SOURCE_NOT_APPLICABLE",
                authority_sha256=SHA,
            )
        },
    )
    assert result["gap_count"] == 0


def test_adj_change_classification_distinguishes_tail_rebase_and_restatement() -> None:
    old = {date(2026, 8, 29): 1.0, date(2026, 8, 30): 1.1}
    tail = {**old, date(2026, 8, 31): 1.2}
    assert classify_adj_factor_change(symbol="000001.SZ", old=old, new=tail, receipt_sha256=SHA).kind == "TAIL_APPEND"
    rebased = {date(2026, 8, 29): 2.0, date(2026, 8, 30): 2.2, date(2026, 8, 31): 2.4}
    assert (
        classify_adj_factor_change(symbol="000001.SZ", old=old, new=rebased, receipt_sha256=SHA).kind
        == "ADJ_DENOMINATOR_CHANGE"
    )
    restated = {date(2026, 8, 29): 1.0, date(2026, 8, 30): 1.2, date(2026, 8, 31): 1.3}
    assert (
        classify_adj_factor_change(symbol="000001.SZ", old=old, new=restated, receipt_sha256=SHA).kind
        == "ADJ_HISTORY_RESTATEMENT"
    )


def test_daily_and_six_pool_contract_preserves_delisted_history() -> None:
    sessions = (date(2026, 9, 29), date(2026, 9, 30))
    assert validate_trading_calendar(sessions=sessions, cutoff=sessions[-1])["session_count"] == 2
    rows = [
        {
            "ts_code": "000001.SZ",
            "trade_date": sessions[0],
            "open": 10.0,
            "high": 10.5,
            "low": 9.8,
            "close": 10.2,
            "pre_close": 10.0,
            "vol": 100.0,
            "amount": 1020.0,
        }
    ]
    exception = TypedGap(
        dataset="kline_daily_raw",
        symbol="000001.SZ",
        start=sessions[1].isoformat(),
        end=sessions[1].isoformat(),
        field="ohlcv",
        reason_code="SUSPEND_FULL_DAY",
        authority_sha256=SHA,
    )
    result = validate_daily_rows(
        expected_keys=(("000001.SZ", value) for value in sessions),
        rows=rows,
        explained_absences={("000001.SZ", sessions[1]): exception},
    )
    assert result["explained_missing_count"] == 1
    spans = [("000001.SZ", sessions[0], sessions[1])]
    coverage = validate_six_pool_market_coverage(
        pools={
            name: spans
            for name in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
        },
        sessions=sessions,
        daily_keys=(("000001.SZ", sessions[0]),),
        minute_keys=(("000001.SZ", sessions[0]),),
        suspended_keys=(("000001.SZ", sessions[1]),),
        minute_start=sessions[0],
    )
    assert all(item["day_gap_count"] == 0 for item in coverage.values())


def test_typed_exception_cannot_be_free_text_or_unpinned() -> None:
    with pytest.raises(ValueError, match="not registered"):
        TypedGap(
            dataset="daily_basic",
            symbol="000001.SZ",
            start="2026-09-30",
            end="2026-09-30",
            field="volume_ratio",
            reason_code="IGNORE",
            authority_sha256=SHA,
        )
    with pytest.raises(ValueError, match="authority"):
        TypedGap(
            dataset="daily_basic",
            symbol="000001.SZ",
            start="2026-09-30",
            end="2026-09-30",
            field="volume_ratio",
            reason_code="SOURCE_NOT_APPLICABLE",
        )
