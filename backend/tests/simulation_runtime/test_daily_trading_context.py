from __future__ import annotations

from datetime import UTC, date, datetime

import psycopg2
import pytest

from backend.services.data_refresh_audit import DatasetRefreshStatus
from backend.services.paper_trading_v2.market_data import PaperV2MinuteMarketDataProvider
from backend.services.simulation_data.daily_context_provider import DailyTradingContextProvider
from backend.services.simulation_data.daily_context import DailyTradingContextV1
from backend.services.trading_core.errors import DataUnavailableError
from backend.tests.paper_trading_v2.fixtures_dev_db import DevDbTargetMisconfigured, _dev_dsn


TRADE_DATE = date(2026, 8, 21)
SYMBOLS = ["000001.SZ", "600000.SH"]


class FakeAuditRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, date]] = []

    def require_success(self, *, dataset: str, trade_date: date):
        self.calls.append((dataset, trade_date))
        return DatasetRefreshStatus(
            dataset=dataset,
            trade_date=trade_date,
            data_source="tushare",
            status="success",
            row_count=2,
            refreshed_at=datetime(2026, 8, 21, 1, 10, tzinfo=UTC),
            job_id=f"job-{dataset}",
            quality_status="ok",
        )


class FakeCursor:
    def __init__(self, owner: "FakeConn") -> None:
        self.owner = owner
        self.rows: list[tuple] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def execute(self, sql: str, params: tuple) -> None:
        self.owner.queries.append(" ".join(sql.split()))
        if "FROM market.stk_limit" in sql:
            self.rows = [
                ("000001.SZ", TRADE_DATE, 10.0, 10.77, 9.23),
                ("600000.SH", TRADE_DATE, 8.0, 8.8, 7.2),
            ]
        elif "FROM market.suspend_d" in sql:
            self.rows = [("600000.SH", TRADE_DATE, "S", "09:30")]
        elif "FROM market.stock_st" in sql:
            self.rows = [
                ("000001.SZ", False, None, None, TRADE_DATE),
                ("600000.SH", True, TRADE_DATE, None, TRADE_DATE),
            ]
        else:
            raise AssertionError(sql)

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return False

    def cursor(self):
        return FakeCursor(self)


def _provider(conn: FakeConn, audit: FakeAuditRepository) -> DailyTradingContextProvider:
    return DailyTradingContextProvider(conn_factory=lambda: conn, audit_repository=audit)


def test_before_0910_waits_without_any_database_probe() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    with pytest.raises(DataUnavailableError) as error:
        _provider(conn, audit).load(
            symbols=SYMBOLS,
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 8, 21, 9, 9, 59),
            calendar_service_snapshot={"is_trading_day": True, "as_of_date": TRADE_DATE.isoformat()},
            binding_identity="binding:hash",
            package_identity="package:manifest",
            release_identity="release:hash",
        )

    assert error.value.context["reason_code"] == "DAILY_TRADING_CONTEXT_WAITING_STK_LIMIT_WINDOW"
    assert audit.calls == []
    assert conn.queries == []


def test_0910_materializes_exact_raw_authority_with_constant_query_count() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    context = _provider(conn, audit).load(
        symbols=list(reversed(SYMBOLS)),
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True, "as_of_date": TRADE_DATE.isoformat()},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
    )

    assert audit.calls == [("stk_limit", TRADE_DATE), ("suspend_d", TRADE_DATE)]
    assert len(conn.queries) == 3
    assert context.sources["stk_limit"]["source"] == "market.stk_limit"
    assert context.symbols["000001.SZ"].up_limit == 10.77
    assert context.symbols["600000.SH"].is_st is True
    assert context.symbols["600000.SH"].is_suspended is True
    statuses = DailyTradingContextProvider.to_pre_trade_statuses(context)
    assert statuses["000001.SZ"]["daily_trading_context"]["context_hash"] == context.context_hash
    assert statuses["600000.SH"]["reason_code"] == "SUSPENDED_BY_SUSPEND_D"


def test_missing_raw_pre_close_uses_one_exact_broker_bound_quote_batch() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    class NullPreCloseCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                self.rows = [(row[0], row[1], None, row[3], row[4]) for row in self.rows]

    conn.cursor = lambda: NullPreCloseCursor(conn)  # type: ignore[method-assign]
    quote_calls: list[list[str]] = []

    def fetch_quotes(symbols: list[str]) -> dict[str, dict]:
        quote_calls.append(list(symbols))
        return {
            "000001.SZ": {
                "K": {"Last": 10_000},
                "time": "20260821091000",
                "price_basis": "yuan",
            },
            "600000.SH": {"K": {"Last": 8_000}, "time": "20260821091000"},
        }

    context = _provider(conn, audit).load(
        symbols=list(reversed(SYMBOLS)),
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
        pre_close_quote_fetcher=fetch_quotes,
        pre_close_quote_source="TDX_REALTIME.batch_quote.pre_close",
    )

    assert quote_calls == [SYMBOLS]
    assert len(conn.queries) == 3
    assert context.symbols["000001.SZ"].pre_close == 10.0
    assert context.symbols["000001.SZ"].pre_close_source == "TDX_REALTIME.batch_quote.pre_close"
    assert context.symbols["000001.SZ"].pre_close_evidence_hash
    assert context.sources["stk_limit"]["pre_close_authority"] == {
        "policy": "raw_stk_limit_else_broker_bound_plan_quote",
        "sources": ["TDX_REALTIME.batch_quote.pre_close"],
    }
    DailyTradingContextV1.model_validate(context.carrier_payload())
    reference = DailyTradingContextProvider.to_pre_trade_statuses(context)["000001.SZ"]["daily_trading_context"]
    _, _, _, frozen_pre_close_source, frozen_limit_price_source = (
        PaperV2MinuteMarketDataProvider._frozen_realtime_daily_inputs(
            symbol="000001.SZ",
            trade_date=TRADE_DATE,
            frozen_daily_fact=reference,
        )
    )
    assert frozen_pre_close_source == "TDX_REALTIME.batch_quote.pre_close:frozen_daily_trading_context_v1"
    assert frozen_limit_price_source == "market.stk_limit:frozen_daily_trading_context_v1"


def test_missing_raw_pre_close_keeps_miniqmt_yuan_price_basis() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    class NullPreCloseCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                self.rows = [(row[0], row[1], None, row[3], row[4]) for row in self.rows]

    conn.cursor = lambda: NullPreCloseCursor(conn)  # type: ignore[method-assign]
    context = _provider(conn, audit).load(
        symbols=SYMBOLS,
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
        pre_close_quote_fetcher=lambda symbols: {
            "000001.SZ": {"pre_close": 10.0, "time": "20260821091000"},
            "600000.SH": {"pre_close": 8.0, "time": "20260821091000"},
        },
        pre_close_quote_source="MINIQMT_REALTIME.broker_quote.pre_close",
    )

    assert context.symbols["000001.SZ"].pre_close == 10.0
    assert context.symbols["600000.SH"].pre_close == 8.0


def test_missing_raw_pre_close_fails_closed_without_quote_authority() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    class NullPreCloseCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                self.rows = [(row[0], row[1], None, row[3], row[4]) for row in self.rows]

    conn.cursor = lambda: NullPreCloseCursor(conn)  # type: ignore[method-assign]

    with pytest.raises(DataUnavailableError) as error:
        _provider(conn, audit).load(
            symbols=SYMBOLS,
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 8, 21, 9, 10),
            calendar_service_snapshot={"is_trading_day": True},
            binding_identity="binding:hash",
            package_identity="package:manifest",
            release_identity="release:hash",
        )

    assert error.value.context["reason_code"] == "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_REQUIRED"


def test_missing_raw_pre_close_rejects_unapproved_quote_source_before_fetch() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    class NullPreCloseCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                self.rows = [(row[0], row[1], None, row[3], row[4]) for row in self.rows]

    conn.cursor = lambda: NullPreCloseCursor(conn)  # type: ignore[method-assign]
    quote_calls: list[list[str]] = []

    with pytest.raises(DataUnavailableError) as error:
        _provider(conn, audit).load(
            symbols=SYMBOLS,
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 8, 21, 9, 10),
            calendar_service_snapshot={"is_trading_day": True},
            binding_identity="binding:hash",
            package_identity="package:manifest",
            release_identity="release:hash",
            pre_close_quote_fetcher=lambda symbols: quote_calls.append(symbols) or {},
            pre_close_quote_source="DB_HISTORICAL.previous_close",
        )

    assert error.value.context["reason_code"] == "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_SOURCE_INVALID"
    assert quote_calls == []


@pytest.mark.parametrize(
    ("quote", "reason_code"),
    [
        ({"pre_close": 10_000, "time": "20260821090000"}, "REALTIME_QUOTE_STALE"),
        ({"pre_close": 99_000, "time": "20260821091000"}, "DAILY_TRADING_CONTEXT_PRE_CLOSE_QUOTE_INVALID"),
    ],
)
def test_missing_raw_pre_close_rejects_stale_or_out_of_bounds_quote(
    quote: dict,
    reason_code: str,
) -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()

    class OneNullPreCloseCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                first = self.rows[0]
                self.rows[0] = (first[0], first[1], None, first[3], first[4])

    conn.cursor = lambda: OneNullPreCloseCursor(conn)  # type: ignore[method-assign]

    with pytest.raises(DataUnavailableError) as error:
        _provider(conn, audit).load(
            symbols=SYMBOLS,
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 8, 21, 9, 10),
            calendar_service_snapshot={"is_trading_day": True},
            binding_identity="binding:hash",
            package_identity="package:manifest",
            release_identity="release:hash",
            pre_close_quote_fetcher=lambda symbols: {"000001.SZ": quote},
            pre_close_quote_source="TDX_REALTIME.batch_quote.pre_close",
        )

    assert error.value.context["reason_code"] == reason_code


def test_stk_limit_missing_symbol_fails_without_derived_fallback() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()
    original_cursor = conn.cursor

    class MissingCursor(FakeCursor):
        def execute(self, sql: str, params: tuple) -> None:
            super().execute(sql, params)
            if "FROM market.stk_limit" in sql:
                self.rows = self.rows[:1]

    conn.cursor = lambda: MissingCursor(conn)  # type: ignore[method-assign]
    assert original_cursor is not None

    with pytest.raises(DataUnavailableError) as error:
        _provider(conn, audit).load(
            symbols=SYMBOLS,
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 8, 21, 9, 11),
            calendar_service_snapshot={"is_trading_day": True},
            binding_identity="binding:hash",
            package_identity="package:manifest",
            release_identity="release:hash",
        )

    assert error.value.context["reason_code"] == "DAILY_TRADING_CONTEXT_STK_LIMIT_COVERAGE_INVALID"


def test_daily_context_readback_rejects_hash_drift() -> None:
    conn = FakeConn()
    audit = FakeAuditRepository()
    context = _provider(conn, audit).load(
        symbols=SYMBOLS,
        trade_date=TRADE_DATE,
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
        binding_identity="binding:hash",
        package_identity="package:manifest",
        release_identity="release:hash",
    )
    carrier = context.carrier_payload()
    carrier["symbols"]["000001.SZ"]["up_limit"] = 99.0

    with pytest.raises(ValueError, match="hash mismatch"):
        DailyTradingContextV1.model_validate(carrier)


def test_dev_daily_context_query_budget_is_dataset_constant() -> None:
    """Read-only DEV proof: two readiness probes plus three set-based facts."""

    try:
        dsn = _dev_dsn()
    except DevDbTargetMisconfigured as exc:
        pytest.skip(str(exc))
    raw_conn = psycopg2.connect(**dsn)
    trade_date = date(2099, 1, 5)
    symbols = [f"0099{index:02d}.SZ" for index in range(1, 9)]
    try:
        with raw_conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO market.stk_limit (trade_date, ts_code, pre_close, up_limit, down_limit)
                SELECT %s, symbol, 10 + ordinal, 11 + ordinal, 9 + ordinal
                FROM unnest(%s::text[]) WITH ORDINALITY AS requested(symbol, ordinal)
                """,
                (trade_date, symbols),
            )
            cursor.execute(
                """
                INSERT INTO market.stock_st (ts_code, ann_date, start_date, end_date, market, exchange)
                SELECT symbol, %s, NULL, NULL, 'Main Board', 'SZSE'
                FROM unnest(%s::text[]) AS requested(symbol)
                """,
                (trade_date, symbols),
            )
            cursor.execute(
                """
                INSERT INTO market.suspend_d (trade_date, ts_code, suspend_type, suspend_timing)
                VALUES (%s, %s, 'S', '09:30')
                """,
                (trade_date, symbols[-1]),
            )
            cursor.execute(
                """
                INSERT INTO market.dataset_date_refresh_audit (
                    dataset, trade_date, data_source, job_id, status, row_count,
                    refreshed_at, metadata, written_rows, quality_status
                )
                SELECT dataset, %s, 'codex_bug_1143_dev_validation', gen_random_uuid(),
                       'success', row_count, NOW(), '{"scope":"rollback_only"}'::jsonb,
                       row_count, 'ok'
                FROM (
                    VALUES
                      ('stk_limit'::text, (SELECT count(*) FROM market.stk_limit WHERE trade_date = %s)::int),
                      ('suspend_d'::text, (SELECT count(*) FROM market.suspend_d WHERE trade_date = %s)::int)
                ) AS seed(dataset, row_count)
                ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
                    status = EXCLUDED.status,
                    row_count = EXCLUDED.row_count,
                    refreshed_at = EXCLUDED.refreshed_at,
                    metadata = EXCLUDED.metadata,
                    written_rows = EXCLUDED.written_rows,
                    quality_status = EXCLUDED.quality_status
                """,
                (trade_date, trade_date, trade_date),
            )

        class CountingCursor:
            def __init__(self, cursor, owner: "CountingConnection") -> None:
                self._cursor = cursor
                self._owner = owner

            def __enter__(self):
                return self

            def __exit__(self, *_exc):
                self._cursor.close()
                return False

            def execute(self, sql, params=None):
                self._owner.queries.append(" ".join(str(sql).split()))
                return self._cursor.execute(sql, params)

            def __getattr__(self, name):
                return getattr(self._cursor, name)

        class CountingConnection:
            def __init__(self, connection) -> None:
                self._connection = connection
                self.queries: list[str] = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, traceback):
                return False

            def cursor(self, *args, **kwargs):
                return CountingCursor(self._connection.cursor(*args, **kwargs), self)

        counted = CountingConnection(raw_conn)
        context = DailyTradingContextProvider(conn_factory=lambda: counted).load(
            symbols=symbols,
            trade_date=trade_date,
            as_of_time=datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=10),
            calendar_service_snapshot={"is_trading_day": True, "as_of_date": trade_date.isoformat()},
            binding_identity="dev-readonly-binding",
            package_identity="dev-readonly-package",
            release_identity="dev-readonly-release",
        )

        assert len(counted.queries) == 5
        assert sum("dataset_date_refresh_audit" in query for query in counted.queries) == 2
        assert sum("FROM market.stk_limit" in query for query in counted.queries) == 1
        assert sum("FROM market.suspend_d" in query for query in counted.queries) == 1
        assert sum("FROM market.stock_st" in query for query in counted.queries) == 1
        assert set(context.symbols) == set(symbols)
    finally:
        raw_conn.rollback()
        try:
            with raw_conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                      (SELECT count(*) FROM market.stk_limit WHERE trade_date = %s AND ts_code = ANY(%s)),
                      (SELECT count(*) FROM market.stock_st WHERE ann_date = %s AND ts_code = ANY(%s)),
                      (SELECT count(*) FROM market.suspend_d WHERE trade_date = %s AND ts_code = ANY(%s)),
                      (SELECT count(*) FROM market.dataset_date_refresh_audit
                       WHERE trade_date = %s AND data_source = 'codex_bug_1143_dev_validation')
                    """,
                    (trade_date, symbols, trade_date, symbols, trade_date, symbols, trade_date),
                )
                assert cursor.fetchone() == (0, 0, 0, 0)
        finally:
            raw_conn.close()
