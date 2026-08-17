from __future__ import annotations

import datetime as dt

import pytest

from backend.services.canonical_equity_pit import CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT

from scripts.build_stock_universe_pit_spans import (
    EventRow,
    SpanRow,
    StockRow,
    TradingCalendar,
    audit_canonical_terminal_evidence,
    audit_canonical_stock_lifecycle,
    audit_st_snapshot_continuity,
    _build_spans,
    _classify_st_event,
    _canonical_calendar_start,
    _ipo_eligible_date,
    _load_initial_st_events,
    _load_confirmed_delisting_events,
    _load_st_events,
    _load_stock_basic,
    _load_stock_basic_scope_counts,
    _stock_basic_terminal_events,
    _validate,
    a_share_ts_code_filter,
    is_b_share_ts_code,
    reconstruct_missing_st_snapshot,
)


def test_classify_st_event_restore_vs_still_risky() -> None:
    assert _classify_st_event("*ST", None, None) == ("st_negative", False)
    assert _classify_st_event("撤销ST", None, None) == ("st_restore", False)
    assert _classify_st_event("撤销ST", None, None, "*ST示例") == ("st_negative", False)
    assert _classify_st_event("撤销叠加*ST", "重整完成", "其他风险警示保持不变") == ("st_negative", False)
    assert _classify_st_event("撤消*ST并实行ST", None, None) == ("st_negative", False)
    assert _classify_st_event("退市整理期", None, None) == ("delist_event", True)
    assert _classify_st_event("退市整理期", None, None, terminal_as_negative=True) == ("st_negative", False)


def test_validate_rejects_overlapping_spans() -> None:
    spans = [
        SpanRow(
            universe_key="u",
            ts_code="000001.SZ",
            eligible_start=dt.date(2020, 1, 1),
            eligible_end=dt.date(2020, 1, 10),
            entry_reason="ipo_365d",
            exit_reason="st_negative",
            base_list_date=dt.date(2010, 1, 1),
            ipo_eligible_date=dt.date(2011, 1, 1),
        ),
        SpanRow(
            universe_key="u",
            ts_code="000001.SZ",
            eligible_start=dt.date(2020, 1, 10),
            eligible_end=dt.date(2020, 1, 20),
            entry_reason="st_restore",
            exit_reason="generation_end",
            base_list_date=dt.date(2010, 1, 1),
            ipo_eligible_date=dt.date(2011, 1, 1),
        ),
    ]

    result = _validate(spans, [])

    assert result["overlap_error_count"] == 1


def test_252_completed_exchange_sessions_define_ipo_entry_not_first_data_date() -> None:
    days = [dt.date(2020, 1, 1) + dt.timedelta(days=offset) for offset in range(300)]
    calendar = TradingCalendar(days)
    eligible = _ipo_eligible_date(
        list_date=days[0],
        filter_value=252,
        filter_unit="trading_sessions",
        calendar=calendar,
    )
    assert eligible == days[252]

    spans = _build_spans(
        [StockRow("000001.SZ", "sample", "SZSE", "D", days[0], days[280])],
        [
            EventRow(
                ts_code="000001.SZ",
                event_kind="delisted",
                action_date=days[280],
                source="stock_basic",
                terminal=True,
            )
        ],
        universe_key="canonical",
        start_date=days[0],
        end_date=days[299],
        ipo_filter_days=252,
        ipo_filter_unit="trading_sessions",
        calendar=calendar,
    )
    assert [(span.eligible_start, span.eligible_end, span.entry_reason) for span in spans] == [
        (days[252], days[279], "ipo_252td")
    ]


def test_canonical_calendar_load_begins_at_earliest_historical_listing() -> None:
    stocks = [
        StockRow("000001.SZ", "old", "SZSE", "L", dt.date(1991, 4, 3), None),
        StockRow("688001.SH", "new", "SSE", "L", dt.date(2025, 1, 2), None),
    ]
    assert _canonical_calendar_start(
        stocks,
        start_date=dt.date(2018, 8, 1),
        minimum_lookback_days=400,
    ) == dt.date(1991, 4, 3)


def test_canonical_lifecycle_evidence_is_fail_closed() -> None:
    ready = audit_canonical_stock_lifecycle(
        [StockRow("000001.SZ", "sample", "SZSE", "L", dt.date(1991, 4, 3), None)]
    )
    assert ready["status"] == "ready"
    with pytest.raises(RuntimeError, match="lifecycle evidence is incomplete"):
        audit_canonical_stock_lifecycle(
            [StockRow("600000.SH", "broken", "SSE", "D", None, None)]
        )


def test_missing_st_snapshot_requires_exact_event_closure() -> None:
    assert reconstruct_missing_st_snapshot(
        {"000001.SZ", "600000.SH"},
        {"000001.SZ", "300001.SZ"},
        [("600000.SH", "st_restore"), ("300001.SZ", "st_negative")],
    ) == frozenset({"000001.SZ", "300001.SZ"})
    try:
        reconstruct_missing_st_snapshot({"000001.SZ"}, {"000001.SZ", "300001.SZ"}, [])
    except RuntimeError as exc:
        assert "cannot be closed" in str(exc)
    else:
        raise AssertionError("ambiguous ST snapshot gap must fail closed")


def test_st_snapshot_continuity_records_only_event_closed_interior_gaps() -> None:
    days = [dt.date(2026, 7, 1) + dt.timedelta(days=offset) for offset in range(4)]
    result = audit_st_snapshot_continuity(
        {
            days[0]: {"000001.SZ", "600000.SH"},
            days[2]: {"000001.SZ", "300001.SZ"},
            days[3]: {"000001.SZ", "300001.SZ"},
        },
        trading_days=days,
        events=[
            EventRow("600000.SH", "st_restore", days[1], "test"),
            EventRow("300001.SZ", "st_negative", days[1], "test"),
        ],
    )
    assert result["missing_snapshot_dates"] == [days[1].isoformat()]
    assert result["reconstructed_snapshot_day_count"] == 1


def test_st_snapshot_continuity_rejects_unanchored_cutoff_gap() -> None:
    days = [dt.date(2026, 7, 1) + dt.timedelta(days=offset) for offset in range(3)]
    try:
        audit_st_snapshot_continuity(
            {days[0]: {"000001.SZ"}, days[1]: {"000001.SZ"}},
            trading_days=days,
            events=[],
        )
    except RuntimeError as exc:
        assert "boundary is missing" in str(exc)
    else:
        raise AssertionError("unanchored cutoff gap must fail closed")


def test_confirmed_delisting_knowledge_date_terminates_before_delist_date() -> None:
    days = [dt.date(2025, 1, 1) + dt.timedelta(days=offset) for offset in range(20)]
    spans = _build_spans(
        [StockRow("600000.SH", "sample", "SSE", "D", days[0], days[19])],
        [
            EventRow(
                ts_code="600000.SH",
                event_kind="delisting_confirmed",
                action_date=days[12],
                source="market.event_signal",
                terminal=True,
            )
        ],
        universe_key="canonical",
        start_date=days[0],
        end_date=days[19],
        ipo_filter_days=0,
        ipo_filter_unit="trading_sessions",
        calendar=TradingCalendar(days),
    )
    assert len(spans) == 1
    assert spans[0].eligible_end == days[11]
    assert spans[0].exit_reason == "delisting_confirmed"


def test_confirmed_delisting_loader_acts_next_session_after_knowledge() -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            self.sql = sql

        def fetchall(self):
            return [
                {
                    "ts_code": "600000.SH",
                    "source_event_date": dt.date(2026, 7, 1),
                    "available_at": dt.datetime(2026, 7, 2, 15, 30),
                    "known_date": dt.date(2026, 7, 2),
                    "effective_trade_date": dt.date(2026, 7, 20),
                    "source_type": "tushare",
                    "source_pk": "x",
                    "reason": "confirmed",
                    "evidence": {},
                }
            ]

    class Conn:
        def cursor(self, cursor_factory=None):
            return Cursor()

    days = [dt.date(2026, 7, 1), dt.date(2026, 7, 2), dt.date(2026, 7, 3), dt.date(2026, 7, 20)]
    events = _load_confirmed_delisting_events(Conn(), TradingCalendar(days), dt.date(2026, 7, 31))
    assert events[0].action_date == dt.date(2026, 7, 20)
    assert events[0].source_effective_date == dt.date(2026, 7, 20)


def test_confirmed_delisting_loader_preserves_preopen_same_day_effective_date() -> None:
    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params=None):
            pass

        def fetchall(self):
            return [
                {
                    "ts_code": "600000.SH",
                    "source_event_date": dt.date(2026, 7, 2),
                    "available_at": dt.datetime(2026, 7, 2, 8, 0),
                    "known_date": dt.date(2026, 7, 2),
                    "effective_trade_date": dt.date(2026, 7, 2),
                    "source_type": "announcement",
                    "source_pk": "preopen",
                    "reason": "confirmed",
                    "evidence": {},
                }
            ]

    class Conn:
        def cursor(self, cursor_factory=None):
            return Cursor()

    events = _load_confirmed_delisting_events(
        Conn(),
        TradingCalendar([dt.date(2026, 7, 2), dt.date(2026, 7, 3)]),
        dt.date(2026, 7, 31),
    )
    assert events[0].action_date == dt.date(2026, 7, 2)


def test_canonical_paused_stock_never_uses_listing_date_as_pause_date() -> None:
    days = [dt.date(2020, 1, 1), dt.date(2020, 1, 2), dt.date(2026, 7, 31)]
    stock = StockRow("600001.SH", "paused", "SSE", "P", days[0], None)
    legacy = _stock_basic_terminal_events([stock], TradingCalendar(days))
    canonical = _stock_basic_terminal_events(
        [stock],
        TradingCalendar(days),
        allow_paused_list_date_fallback=False,
    )
    assert legacy[0].action_date == days[0]
    assert canonical == []


def test_confirmed_delisting_event_can_close_st_snapshot_gap() -> None:
    days = [dt.date(2026, 7, 1), dt.date(2026, 7, 2), dt.date(2026, 7, 3)]
    result = audit_st_snapshot_continuity(
        {days[0]: {"600000.SH"}, days[2]: set()},
        trading_days=days,
        events=[EventRow("600000.SH", "delisting_confirmed", days[1], "event_signal", terminal=True)],
    )
    assert result["status"] == "ready"
    assert result["reconstructed_snapshot_day_count"] == 1


def test_canonical_terminal_evidence_rejects_unclassified_delisted_security() -> None:
    stock = StockRow("600000.SH", "sample", "SSE", "D", dt.date(2000, 1, 1), dt.date(2025, 1, 1))
    try:
        audit_canonical_terminal_evidence([stock], announcement_events=[])
    except RuntimeError as exc:
        assert "600000.SH" in str(exc)
    else:
        raise AssertionError("delisted security without announcement evidence must fail closed")
    receipt = audit_canonical_terminal_evidence(
        [stock],
        announcement_events=[
            EventRow(
                "600000.SH",
                "delisting_confirmed",
                dt.date(2024, 12, 1),
                "market.event_signal",
                terminal=True,
            )
        ],
    )
    assert receipt["status"] == "ready"


def test_terminal_evidence_does_not_require_stock_delisted_after_cutoff() -> None:
    stock = StockRow("600000.SH", "future", "SSE", "D", dt.date(2000, 1, 1), dt.date(2026, 8, 10))
    receipt = audit_canonical_terminal_evidence(
        [stock],
        announcement_events=[],
        end_date=dt.date(2026, 7, 31),
    )
    assert receipt["required_terminal_security_count"] == 0
    assert receipt["status"] == "ready"


def test_terminal_evidence_reports_but_does_not_require_pre_window_delisting() -> None:
    stock = StockRow("600000.SH", "historical", "SSE", "D", dt.date(2000, 1, 1), dt.date(2017, 12, 29))

    receipt = audit_canonical_terminal_evidence(
        [stock],
        announcement_events=[],
        start_date=dt.date(2018, 8, 1),
        end_date=dt.date(2026, 7, 31),
    )

    assert receipt["required_terminal_security_count"] == 0
    assert receipt["pre_window_terminal_security_count"] == 1
    assert receipt["pre_window_terminal_security_codes"] == ["600000.SH"]
    assert receipt["status"] == "ready"


def test_terminal_evidence_still_requires_in_window_delisting() -> None:
    stock = StockRow("600000.SH", "in-window", "SSE", "D", dt.date(2000, 1, 1), dt.date(2025, 1, 1))

    try:
        audit_canonical_terminal_evidence(
            [stock],
            announcement_events=[],
            start_date=dt.date(2018, 8, 1),
            end_date=dt.date(2026, 7, 31),
        )
    except RuntimeError as exc:
        assert "600000.SH" in str(exc)
    else:
        raise AssertionError("in-window delisting without announcement evidence must fail closed")


class _FakeCursor:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *args: object) -> bool:
        return False

    def execute(self, sql: str, params: object = None) -> None:
        self.queries.append(sql)

    def fetchall(self) -> list[dict]:
        return []

    def fetchone(self) -> None:
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.cursors: list[_FakeCursor] = []

    def cursor(self, cursor_factory: object = None) -> _FakeCursor:
        cur = _FakeCursor()
        self.cursors.append(cur)
        return cur


_B_SHARE_EXCLUDES = ("NOT LIKE '200%%.SZ'", "NOT LIKE '201%%.SZ'", "NOT LIKE '900%%.SH'")


def test_is_b_share_ts_code() -> None:
    for code in ("200011.SZ", "201872.SZ", "900901.SH", "200011.sz"):
        assert is_b_share_ts_code(code), code
    for code in (
        "000001.SZ",
        "002594.SZ",
        "300750.SZ",
        "301269.SZ",
        "600000.SH",
        "601318.SH",
        "603259.SH",
        "605499.SH",
        "688981.SH",
        "689009.SH",
    ):
        assert not is_b_share_ts_code(code), code
    assert not is_b_share_ts_code("")
    assert not is_b_share_ts_code(None)


def test_a_share_ts_code_filter_fragment() -> None:
    fragment = a_share_ts_code_filter("s.ts_code")
    for exclude in _B_SHARE_EXCLUDES:
        assert f"s.ts_code {exclude}" in fragment


def test_universe_source_queries_exclude_b_shares() -> None:
    calendar = TradingCalendar([dt.date(2026, 1, 2), dt.date(2026, 1, 5)])

    conn = _FakeConn()
    _load_stock_basic(conn, active_only=True, active_as_of=dt.date(2026, 1, 5))
    _load_stock_basic_scope_counts(conn, active_as_of=dt.date(2026, 1, 5))
    _load_st_events(conn, calendar, dt.date(2026, 1, 5))
    _load_initial_st_events(conn, calendar, dt.date(2026, 1, 1))
    _load_confirmed_delisting_events(conn, calendar, dt.date(2026, 1, 5))

    executed = [sql for cur in conn.cursors for sql in cur.queries]
    assert len(executed) == 6
    for sql in executed:
        for exclude in _B_SHARE_EXCLUDES:
            assert exclude in sql, sql
    delisting_sql = executed[-1]
    assert "time_mode = 'backtest'" in delisting_sql
    assert "terminal_evidence_contract' = %s" in delisting_sql
    assert CANONICAL_PIT_TERMINAL_EVIDENCE_CONTRACT == "issuer_bound_stock_delisting_v1"
    assert "'{issuer_binding,status}' = 'verified'" in delisting_sql
    assert "'{issuer_binding,terminal_subject}' = 'self'" in delisting_sql
    assert "signal_status IN ('ACTIVE', 'RESOLVED', 'EXPIRED')" in delisting_sql
