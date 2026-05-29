from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, date, datetime
from typing import Any

from backend.services.paper_trading_v2.repository import PaperTradingV2Repository
from backend.services.paper_trading_v2.symbol_names import PaperV2SymbolNameResolver
from backend.services.trading_core.ledger import CashLedgerEntry
from backend.services.trading_core.models import Fill, Order, OrderSide, OrderStatus, OrderType, PositionLot


class StaticResolver(PaperV2SymbolNameResolver):
    def __init__(self) -> None:
        pass

    def resolve(self, symbols):  # type: ignore[override]
        return {"000001.SZ": "Ping An Bank"}


def test_symbol_name_enrichment_is_additive_and_preserves_symbol() -> None:
    rows = [
        {"symbol": "000001.SZ", "quantity": 100},
        {"symbol": "000002.SZ", "quantity": 200},
    ]

    enriched = StaticResolver().enrich_rows(rows)

    assert enriched[0]["symbol"] == "000001.SZ"
    assert enriched[0]["stock_name"] == "Ping An Bank"
    assert enriched[0]["symbol_name"] == "Ping An Bank"
    assert "stock_name" not in rows[0]
    assert "stock_name" not in enriched[1]


def test_symbol_name_enrichment_prefers_persisted_metadata_name() -> None:
    rows = [{"symbol": "000001.SZ", "metadata": {"stock_name": "Persisted Audit Name"}}]

    enriched = StaticResolver().enrich_rows(rows)

    assert enriched[0]["stock_name"] == "Persisted Audit Name"


class CaptureCursor:
    def __init__(self, conn: "CaptureConnection") -> None:
        self.conn = conn

    def __enter__(self) -> "CaptureCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def execute(self, sql: str, params: object | None = None) -> None:
        self.conn.executed.append((sql, params))


class CaptureConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object | None]] = []

    def cursor(self, *args: object, **kwargs: object) -> CaptureCursor:
        return CaptureCursor(self)


@contextmanager
def capture_conn_factory(conn: CaptureConnection):
    yield conn


def _repository(conn: CaptureConnection, resolver: object | None = None) -> PaperTradingV2Repository:
    return PaperTradingV2Repository(
        conn_factory=lambda: capture_conn_factory(conn),
        symbol_name_resolver=resolver or StaticResolver(),
    )


def _params_for(conn: CaptureConnection, needle: str) -> tuple[Any, ...]:
    sql, params = next((sql, params) for sql, params in conn.executed if needle in sql)
    assert "stock_name" in sql
    assert isinstance(params, tuple)
    return params


def test_repository_persists_stock_name_on_trading_records() -> None:
    conn = CaptureConnection()
    repo = _repository(conn)

    order = Order(
        order_id="ord_test",
        intent_id="intent_test",
        package_id="pkg_test",
        portfolio_id="portfolio_test",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=100,
        order_type=OrderType.MARKET,
        status=OrderStatus.SUBMITTED,
    )
    fill = Fill(
        fill_id="fill_test",
        order_id=order.order_id,
        symbol=order.symbol,
        side=OrderSide.BUY,
        quantity=100,
        price=10.0,
        trade_time=datetime(2026, 5, 14, 9, 31, tzinfo=UTC),
        reason="test_fill",
    )

    repo.save_order("run_test", order)
    repo.save_fill("run_test", fill)
    repo.save_cash_entry(
        "run_test",
        CashLedgerEntry(
            fill_id=fill.fill_id,
            portfolio_id=order.portfolio_id,
            trade_date=date(2026, 5, 14),
            symbol=order.symbol,
            side=OrderSide.BUY,
            notional=1000.0,
            fee=5.0,
            cash_delta=-1005.0,
            cash_after=998995.0,
        ),
    )
    repo.save_positions(
        run_id="run_test",
        trade_date=date(2026, 5, 14),
        positions=[
            PositionLot(
                portfolio_id=order.portfolio_id,
                symbol=order.symbol,
                quantity=100,
                available_quantity=0,
                avg_cost=10.0,
                trade_date=date(2026, 5, 14),
            )
        ],
        prices={order.symbol: 10.0},
    )

    order_sql, _ = next((sql, params) for sql, params in conn.executed if "INSERT INTO paper_v2.orders" in sql)
    assert "metadata = EXCLUDED.metadata" in order_sql
    assert _params_for(conn, "INSERT INTO paper_v2.orders")[6] == "Ping An Bank"
    assert _params_for(conn, "INSERT INTO paper_v2.fills")[4] == "Ping An Bank"
    assert _params_for(conn, "INSERT INTO paper_v2.cash_ledger")[5] == "Ping An Bank"
    assert _params_for(conn, "INSERT INTO paper_v2.positions")[4] == "Ping An Bank"


def test_repository_stock_name_persistence_fails_open() -> None:
    class RaisingResolver:
        def resolve(self, symbols: object) -> dict[str, str]:
            raise RuntimeError("reference table offline")

    conn = CaptureConnection()
    repo = _repository(conn, resolver=RaisingResolver())

    repo.save_order(
        "run_test",
        Order(
            order_id="ord_test",
            intent_id="intent_test",
            package_id="pkg_test",
            portfolio_id="portfolio_test",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.MARKET,
            status=OrderStatus.SUBMITTED,
        ),
    )

    assert _params_for(conn, "INSERT INTO paper_v2.orders")[6] is None
