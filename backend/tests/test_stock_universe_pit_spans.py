from __future__ import annotations

import datetime as dt

from scripts.build_stock_universe_pit_spans import (
    SpanRow,
    TradingCalendar,
    _classify_st_event,
    _load_initial_st_events,
    _load_st_events,
    _load_stock_basic,
    _load_stock_basic_scope_counts,
    _validate,
    a_share_ts_code_filter,
    is_b_share_ts_code,
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

    executed = [sql for cur in conn.cursors for sql in cur.queries]
    assert len(executed) == 5
    for sql in executed:
        for exclude in _B_SHARE_EXCLUDES:
            assert exclude in sql, sql
