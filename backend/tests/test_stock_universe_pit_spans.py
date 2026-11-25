from __future__ import annotations

import datetime as dt
import inspect
import re
from types import SimpleNamespace

import pytest

from scripts import build_stock_universe_pit_spans as pit_builder

from scripts.build_stock_universe_pit_spans import (
    EventRow,
    SpanRow,
    StockRow,
    TradingCalendar,
    audit_canonical_stock_lifecycle,
    audit_st_snapshot_continuity,
    _build_spans,
    _classify_st_event,
    _canonical_calendar_start,
    _ipo_eligible_date,
    _load_initial_st_events,
    _load_st_events,
    _load_stock_basic,
    _load_stock_basic_scope_counts,
    _stock_basic_terminal_events,
    _validate,
    a_share_ts_code_filter,
    is_b_share_ts_code,
    is_authoritative_st_name,
    reconstruct_missing_st_snapshot,
)


@pytest.mark.parametrize("missing_schema", [False, True])
def test_existing_schema_builder_checks_only_selects(missing_schema) -> None:
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql):
            queries.append(sql)
            assert sql.lstrip().upper().startswith("SELECT")

        def fetchone(self):
            return (None, None) if missing_schema else ("market.stock_universe_pit_spans", "market.stock_universe_pit_events")

    connection = SimpleNamespace(cursor=Cursor)
    if missing_schema:
        with pytest.raises(RuntimeError, match="schema contract is missing"):
            pit_builder._require_existing_tables(connection)
    else:
        pit_builder._require_existing_tables(connection)
    assert len(queries) == 1


def test_build_existing_schema_mode_does_not_call_bootstrap(monkeypatch) -> None:
    checked = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(pit_builder, "_db_config", lambda: {})
    monkeypatch.setattr(pit_builder.psycopg2, "connect", lambda **_kwargs: Connection())
    monkeypatch.setattr(pit_builder, "_require_existing_tables", lambda conn: checked.append(conn))
    monkeypatch.setattr(pit_builder, "_ensure_tables", lambda _conn: pytest.fail("DDL is forbidden"))

    def observe_source(*_args, **_kwargs):
        assert len(checked) == 1
        raise RuntimeError("source reached after read-only schema validation")

    monkeypatch.setattr(pit_builder, "_load_stock_basic_scope_counts", observe_source)
    args = SimpleNamespace(
        start_date="2018-08-01", end_date="2026-09-30", rule_version=None,
        universe_key="test", ipo_filter_days=365, existing_schema_only=True,
    )
    with pytest.raises(RuntimeError, match="source reached"):
        pit_builder.build(args)


def test_classify_st_event_restore_vs_still_risky() -> None:
    assert _classify_st_event("*ST", None, None) == ("st_negative", False)
    assert _classify_st_event("撤销ST", None, None) == ("st_restore", False)
    assert _classify_st_event("撤销ST", None, None, "*ST示例") == ("st_negative", False)
    assert _classify_st_event("撤销叠加*ST", "重整完成", "其他风险警示保持不变") == ("st_negative", False)
    assert _classify_st_event("撤消*ST并实行ST", None, None) == ("st_negative", False)
    assert _classify_st_event("退市整理期", None, None) == ("delist_event", True)
    assert _classify_st_event("退市整理期", None, None, terminal_as_negative=True) == ("st_negative", False)


def test_st_risk_narrative_does_not_become_terminal_evidence() -> None:
    assert _classify_st_event(
        "*ST",
        "法院受理重整",
        "如果重整失败，公司股票将面临被终止上市的风险。",
    ) == ("st_negative", False)


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


@pytest.mark.parametrize("restored", [False, True])
def test_monthly_st_exclusion_requires_positive_restore_evidence(restored) -> None:
    days = [dt.date(2026, 9, 29), dt.date(2026, 9, 30)]
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            queries.append((sql, params))
            assert sql.lstrip().startswith("SELECT")

        def fetchall(self):
            return [(days[0], "301139.SZ")]

    span = SpanRow("u", "301139.SZ", days[1], days[1], "ipo", "generation_end", days[0], days[0])
    events = [EventRow("301139.SZ", "st_restore", days[1], "market.stock_st_events")] if restored else []
    conn = SimpleNamespace(cursor=lambda: Cursor())
    if restored:
        receipt = pit_builder._audit_st_span_exclusion(
            conn, spans=[span], events=events, calendar=TradingCalendar(days),
            start_date=days[0], end_date=days[1],
        )
        assert receipt["conflict_key_count"] == 0
    else:
        with pytest.raises(pit_builder.CanonicalPitEvidenceError) as failure:
            pit_builder._audit_st_span_exclusion(
                conn, spans=[span], events=events, calendar=TradingCalendar(days),
                start_date=days[0], end_date=days[1],
            )
        assert failure.value.context["conflicts"] == [{"ts_code": "301139.SZ", "trade_date": "2026-09-30"}]
    assert len(queries) == 1


def test_st_exclusion_audits_only_target_month_and_preserves_pre_risk_history() -> None:
    days = [dt.date(2026, 8, 31), dt.date(2026, 9, 1), dt.date(2026, 9, 2)]
    queries = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            queries.append((sql, params))

        def fetchall(self):
            return [(days[2], "301139.SZ"), (days[2], "301139.SZ")]

    span = SpanRow("u", "301139.SZ", days[0], days[1], "ipo", "st_negative", days[0], days[0])
    receipt = pit_builder._audit_st_span_exclusion(
        SimpleNamespace(cursor=lambda: Cursor()), spans=[span], events=[],
        calendar=TradingCalendar(days), start_date=dt.date(2018, 8, 1), end_date=days[2],
    )
    assert receipt["audit_start"] == "2026-09-01"
    assert receipt["trading_day_count"] == 2
    assert receipt["conflict_key_count"] == 0
    assert queries[0][1][0] == days[0]  # One causal boundary anchor, not a history scan.
    assert queries[0][1][1] == days[2]


def test_builder_checks_st_exclusion_before_any_span_write() -> None:
    source = inspect.getsource(pit_builder.build)
    assert source.index("_audit_st_span_exclusion(") < source.index("_write_incremental_extension(")
    assert source.index("_audit_st_span_exclusion(") < source.index("_write_results(")


@pytest.mark.parametrize("case", ["same_day_st", "future_restore", "terminal_restore"])
def test_st_exclusion_does_not_accept_stale_or_noncausal_restoration(case) -> None:
    day = dt.date(2026, 9, 30)
    events = [EventRow("301139.SZ", "st_restore", day, "market.stock_st_events")]
    snapshots = [(day, "301139.SZ")]
    if case == "future_restore":
        events = [EventRow("301139.SZ", "st_restore", dt.date(2026, 10, 1), "market.stock_st_events")]
    elif case == "terminal_restore":
        events.insert(0, EventRow("301139.SZ", "delist_event", day, "market.stock_st_events", terminal=True))
        snapshots = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            pass

        def fetchall(self):
            return snapshots

    span = SpanRow("u", "301139.SZ", day, day, "ipo", "generation_end", day, day)
    with pytest.raises(pit_builder.CanonicalPitEvidenceError) as failure:
        pit_builder._audit_st_span_exclusion(
            SimpleNamespace(cursor=lambda: Cursor()), spans=[span], events=events,
            calendar=TradingCalendar([day]), start_date=day, end_date=day,
        )
    assert failure.value.context["conflict_key_count"] == 1


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


def test_lifecycle_rejects_terminal_date_before_listing() -> None:
    stock = StockRow("600000.SH", "invalid", "SSE", "D", dt.date(2025, 1, 2), dt.date(2025, 1, 1))
    with pytest.raises(RuntimeError, match="lifecycle evidence is incomplete"):
        audit_canonical_stock_lifecycle([stock])


def test_authoritative_delist_date_preserves_history_and_closes_only_on_date() -> None:
    days = [dt.date(2026, 10, 15), dt.date(2026, 10, 16), dt.date(2026, 10, 19)]
    stock = StockRow("601059.SH", "sample", "SSE", "D", dt.date(2023, 2, 1), days[-1])
    calendar = TradingCalendar(days)
    events = _stock_basic_terminal_events([stock], calendar, allow_paused_list_date_fallback=False)
    spans = _build_spans(
        [stock], events, universe_key="canonical", start_date=days[0], end_date=days[-1],
        ipo_filter_days=0, ipo_filter_unit="trading_sessions", calendar=calendar,
    )
    assert [(span.eligible_start, span.eligible_end, span.exit_reason) for span in spans] == [
        (days[0], days[1], "delisted")
    ]


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


def test_global_canonical_pit_policy_explicitly_excludes_b_shares() -> None:
    from backend.services.canonical_equity_pit import CANONICAL_RULE_PARAMETERS

    assert CANONICAL_RULE_PARAMETERS["security_type"] == "A_SHARE_EQUITY"
    assert "B_SHARE" in CANONICAL_RULE_PARAMETERS["excluded_boards"]
    assert is_b_share_ts_code("200706.SZ")


def test_live_roll_preserves_prior_membership_and_rejects_historical_revision() -> None:
    old_end = dt.date(2026, 10, 20)
    old = [("000001.SZ", dt.date(2020, 1, 2), old_end)]
    span = pit_builder.SpanRow(
        "test", "000001.SZ", dt.date(2020, 1, 2), dt.date(2026, 10, 21),
        "ipo", "generation_end", dt.date(1991, 4, 3), dt.date(1992, 4, 3),
    )
    assert pit_builder._membership_prefix_matches(old, [span], old_end)
    assert not pit_builder._membership_prefix_matches([], [span], old_end)
    assert not pit_builder._membership_prefix_matches(old, [], old_end)


def test_live_source_boundary_is_previous_trade_day_not_a_synthetic_snapshot() -> None:
    calendar = TradingCalendar([dt.date(2026, 10, 16), dt.date(2026, 10, 19), dt.date(2026, 10, 20)])
    assert pit_builder._live_source_boundary(calendar, dt.date(2026, 10, 19), "2026-10-16") == dt.date(2026, 10, 16)
    assert pit_builder._live_source_boundary(calendar, dt.date(2026, 10, 19), "2026-10-19") == dt.date(2026, 10, 19)
    with pytest.raises(ValueError, match="previous trading"):
        pit_builder._live_source_boundary(calendar, dt.date(2026, 10, 19), "2026-10-20")


def test_a_share_ts_code_filter_fragment() -> None:
    fragment = a_share_ts_code_filter("s.ts_code")
    predicate = re.search(r"upper\(btrim\(s\.ts_code\)\) ~ '([^']+)'", fragment)
    assert predicate is not None
    pattern = predicate.group(1)
    for code in ("000001.SZ", "300750.SZ", "600000.SH", "688981.SH", "SZ000001", "SH600000"):
        assert re.fullmatch(pattern, code), code
    for code in ("200011.SZ", "201872.SZ", "900901.SH", "920296.BJ", "830001.BJ", "BJ920296"):
        assert re.fullmatch(pattern, code) is None, code


def test_authoritative_st_name_requires_explicit_risk_prefix() -> None:
    for value in ("ST保千里", "*ST保千", "SST中纺", "S*ST佳通", "PT水仙"):
        assert is_authoritative_st_name(value)
    for value in ("保千里", "退市保千", "上海临港", ""):
        assert not is_authoritative_st_name(value)


def test_universe_source_queries_exclude_b_shares() -> None:
    calendar = TradingCalendar([dt.date(2026, 1, 2), dt.date(2026, 1, 5)])

    conn = _FakeConn()
    _load_stock_basic(conn, active_only=True, active_as_of=dt.date(2026, 1, 5))
    _load_stock_basic_scope_counts(conn, active_as_of=dt.date(2026, 1, 5))
    _load_st_events(conn, calendar, dt.date(2026, 1, 5))
    _load_initial_st_events(conn, calendar, dt.date(2026, 1, 1))

    executed = [sql for cur in conn.cursors for sql in cur.queries]
    assert len(executed) == 5
    for sql in executed:
        assert any(a_share_ts_code_filter(column).strip() in sql for column in ("ts_code", "s.ts_code")), sql


def test_live_snapshot_tail_excludes_st_without_announcement_and_preserves_observed_prefix() -> None:
    days = [dt.date(2026, 11, day) for day in (23, 24, 25, 26, 27)]
    calendar = TradingCalendar(days)
    symbol = "000892.SZ"

    class Connection:
        def cursor(self): return self
        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def execute(self, sql, _params):
            assert "stock_st_events" not in sql
            if "FROM market.stock_st" in sql:
                self.rows = [(days[1], symbol), (days[2], symbol)]
            else:
                assert "quality_status='empty_valid'" in sql
                self.rows = [(days[0],), (days[3],)]
        def fetchall(self): return self.rows

    events, receipt = pit_builder._canonical_snapshot_events(
        Connection(), calendar=calendar, start_date=days[0], source_end=days[3], live=True,
    )
    assert receipt["observed_snapshot_day_count"] == 4
    assert [(e.action_date, e.event_kind) for e in events] == [
        (days[1], "st_negative"), (days[3], "st_restore"),
    ]

    stock = StockRow(symbol, "sample", "SZSE", "L", days[0], None)
    old_span = SpanRow("test", symbol, days[0], days[1], "ipo", "generation_end", days[0], days[0])
    tail = _build_spans(
        [stock], events, universe_key="test", start_date=days[1], end_date=days[4],
        ipo_filter_days=0, calendar=calendar,
    )
    rolled = pit_builder._join_published_prefix([old_span], tail, events, calendar=calendar, start=days[1])
    assert [(span.eligible_start, span.eligible_end) for span in rolled] == [
        (days[0], days[0]), (days[3], days[4]),
    ]
    assert rolled[0].exit_reason == "st_negative"
    assert pit_builder._membership_prefix_matches([(symbol, days[0], days[1])], rolled, days[0])
    assert not pit_builder._membership_prefix_matches([(symbol, days[0], days[1])], rolled, days[1])
