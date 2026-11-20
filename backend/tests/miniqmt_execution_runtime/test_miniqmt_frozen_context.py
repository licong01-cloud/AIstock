"""Archived MiniQMT authority and reconciliation inputs."""


from __future__ import annotations

import re
from datetime import datetime

import pytest

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.daily_context import (
    DailyLimitAuthorityV2,
    DailyLimitResolverV2,
    SimulationBrokerBackend,
)
from backend.services.simulation_runtime.daily_limit_authority import (
    DailyLimitAuthorityContractError,
    allowed_daily_limit_authorities,
    assert_daily_limit_authorities_for_broker,
    required_daily_limit_resolver,
)
from backend.tests.qmt_strategy_ledger.test_order_service_preflight import (
    REPO_ROOT,
)
from backend.tests.simulation_runtime.test_daily_trading_context import (
    SYMBOLS,
    TRADE_DATE,
    FakeAuditRepository,
    FakeConn,
    FakeCursor,
    _provider,
)
from backend.tests.simulation_runtime.test_daily_trading_context_v2 import (
    _context_v2,
    _fact_v2,
)
from backend.tests.test_data_quality_smoke_env import (
    _FakeCursor,
    _smoke_without_db,
    smoke,
)


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


def test_miniqmt_v2_accepts_only_direct_instrument_authority() -> None:
    symbols = {"600000.SH": _fact_v2("600000.SH", authority=DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1)}
    context = _context_v2(SimulationBrokerBackend.MINIQMT_SIM, symbols)

    assert context.sources.stk_limit is None
    assert context.sources.tdx_reference is None
    assert context.sources.miniqmt_instrument is not None


def test_miniqmt_broker_matrix_allows_only_direct_instrument_and_explicit_states() -> None:
    allowed = allowed_daily_limit_authorities(SimulationBrokerBackend.MINIQMT_SIM)

    assert allowed == frozenset(
        {
            DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1,
            DailyLimitAuthorityV2.NO_DAILY_LIMIT,
            DailyLimitAuthorityV2.UNAVAILABLE,
        }
    )
    assert required_daily_limit_resolver("minqmt_sim") is DailyLimitResolverV2.MINIQMT_INSTRUMENT_DETAIL_V1
    assert_daily_limit_authorities_for_broker(
        broker_backend="minqmt_sim",
        authorities={DailyLimitAuthorityV2.MINIQMT_INSTRUMENT_DETAIL_V1},
    )
    for forbidden in (
        DailyLimitAuthorityV2.TUSHARE_STK_LIMIT,
        DailyLimitAuthorityV2.TDX_REFERENCE_DERIVED_V1,
    ):
        with pytest.raises(DailyLimitAuthorityContractError, match="not permitted for minqmt_sim"):
            assert_daily_limit_authorities_for_broker(
                broker_backend="minqmt_sim",
                authorities={forbidden},
            )


def test_miniqmt_realtime_enum_value_exists() -> None:
    assert MinuteDataSource.MINIQMT_REALTIME.value == "MINIQMT_REALTIME"
    assert MinuteDataSource("MINIQMT_REALTIME") is MinuteDataSource.MINIQMT_REALTIME


def test_miniqmt_preflight_does_not_reintroduce_hard_coded_100_share_lot_gate() -> None:
    scanned_files = [
        REPO_ROOT / "backend/services/qmt_strategy_ledger/order_service.py",
        REPO_ROOT / "backend/services/qmt_strategy_ledger/selection_order_builder.py",
        REPO_ROOT / "backend/routers/qmt_strategy_ledger.py",
        REPO_ROOT / "backend/routers/qmt.py",
    ]
    forbidden_patterns = [
        re.compile(r"\b(?:quantity|order_volume|qty)\s*%\s*100\b"),
        re.compile(r"\b100\s*%\s*(?:quantity|order_volume|qty)\b"),
    ]

    violations: list[str] = []
    for path in scanned_files:
        text = path.read_text(encoding="utf-8")
        for pattern in forbidden_patterns:
            if pattern.search(text):
                violations.append(f"{path.relative_to(REPO_ROOT)} matches {pattern.pattern}")

    assert violations == []


def test_paper_v2_run_traceability_accepts_minqmt_success_events():
    runner = _smoke_without_db(portfolio_ids=["paper_minqmt"])
    cur = _FakeCursor(rows=[(3, 0, 0)])

    runner._check_paper_v2_runs(cur)

    sql, params = cur.calls[0]
    success_events = params[-1]
    assert "ev.event_type = ANY(%s)" in sql
    assert success_events == list(smoke.PAPER_V2_SUCCESS_RUN_EVENTS)
    assert "MINIQMT_RUN_RECONCILED" in success_events
    assert "MINIQMT_NATIVE_RUN_RECONCILED" in success_events
    assert runner.results[0].status == "PASS"
