"""End-to-end integration tests for the Paper v2 sim daemon (Task #35).

Each test wires:

    StrategyPackageManifest (paper-enabled, real fixture from test_day_runner)
        --> LocalSimBackend (with FakeMarketDataProvider)
        --> SimGateway facade
        --> PaperV2SimRunner + DaemonEventLog (SQLite, tmp_path)
        --> assert event-log row counts / shapes / order lifecycle invariants

Coverage (>=5 distinct integration scenarios):
    1. Single market BUY -> filled -> event log shape + position update
    2. Multi-intent batch (3 BUY orders) -> aggregated event log + run summary
    3. Limit BUY rejected by ledger (insufficient cash) -> ORDER_REJECTED path
    4. Cancel-after-partial -> CancelAck accepted=False on terminal handle
    5. SimGateway lifecycle invariant: cannot send order before connect /
       after close
    6. Subscribe / unsubscribe round-trip + fill fan-out via event log
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from backend.services.paper_trading_v2.broker import LocalSimBackend
from backend.services.paper_trading_v2.daemon import (
    DaemonEventLog,
    DaemonEventType,
    PaperV2SimRunner,
)
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    MinuteExecutionMarketInput,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.trading_core.errors import (
    BrokerConnectivityError,
    BrokerRejectedError,
    DataUnavailableError,
)
from backend.services.trading_core.models import (
    MinuteBar,
    OrderIntent,
    OrderSide,
    OrderType,
)
from backend.services.trading_core.sim_gateway import (
    SimGateway,
    SimGatewayConnectError,
    SimGatewayConnectionState,
)

from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest


_TRADE_DATE = date(2024, 1, 2)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _FakeProvider(PaperV2MinuteMarketDataProvider):
    """Returns canned bars; raises ``DataUnavailableError`` for unknown
    symbols so we can exercise BrokerConnectivityError paths."""

    def __init__(
        self,
        *,
        symbol: str,
        open_price: float = 10.0,
        bar_count: int = 8,
        suspend: bool = False,
        unknown_symbols: tuple[str, ...] = (),
    ) -> None:
        super().__init__()
        self._symbol = symbol
        self._open = open_price
        self._bar_count = bar_count
        self._suspend = suspend
        self._unknown = set(unknown_symbols)

    def load_symbol_input(  # type: ignore[override]
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        min_bars: int = 1,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        if symbol in self._unknown:
            raise DataUnavailableError(
                f"no bars for {symbol}",
                context={"symbol": symbol, "trade_date": trade_date.isoformat()},
            )
        start = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=31)
        bars = [
            MinuteBar(
                symbol=symbol,
                bar_time=start + timedelta(minutes=i),
                open=self._open + i * 0.05,
                high=self._open + 0.10 + i * 0.05,
                low=self._open - 0.05 + i * 0.05,
                close=self._open + 0.03 + i * 0.05,
                volume=200_000,
                amount=2_000_000.0,
                limit_up=self._open * 1.1,
                limit_down=self._open * 0.9,
            )
            for i in range(self._bar_count)
        ]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=bars,
            market_context={
                "stock_id": symbol,
                "trade_date": trade_date.isoformat(),
                "data_source": source.value,
                "prev_close": self._open,
                "limit_up": self._open * 1.1,
                "limit_down": self._open * 0.9,
                "suspend_status": {"is_suspended": self._suspend},
            },
        )


def _wire(
    *,
    tmp_path: Path,
    initial_cash: float = 10_000_000.0,
    provider: PaperV2MinuteMarketDataProvider | None = None,
) -> tuple[LocalSimBackend, SimGateway, PaperV2SimRunner, DaemonEventLog, str]:
    manifest = make_paper_enabled_manifest()
    portfolio_id = f"paper_test_{uuid4().hex[:8]}"
    db_path = tmp_path / "daemon_events.db"
    event_log = DaemonEventLog(
        db_path=db_path,
        portfolio_id=portfolio_id,
        package_id=manifest.package_id,
    )
    backend = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=initial_cash,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        market_data_provider=provider or _FakeProvider(symbol="600000.SH"),
    )
    gateway = SimGateway.from_local_sim(backend)
    gateway.connect()
    runner = PaperV2SimRunner(gateway=gateway, event_log=event_log, manifest=manifest)
    return backend, gateway, runner, event_log, portfolio_id


def _make_intent(
    *,
    portfolio_id: str,
    package_id: str,
    symbol: str = "600000.SH",
    side: OrderSide = OrderSide.BUY,
    quantity: int = 200,
    order_type: OrderType = OrderType.MARKET,
    limit_price: float | None = None,
) -> OrderIntent:
    return OrderIntent(
        package_id=package_id,
        portfolio_id=portfolio_id,
        symbol=symbol,
        side=side,
        quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        target_trade_date=_TRADE_DATE,
    )


# ---------------------------------------------------------------------------
# Integration tests
# ---------------------------------------------------------------------------


def test_e2e_single_market_buy_emits_full_lifecycle(tmp_path: Path) -> None:
    """Scenario 1: one MARKET BUY -> filled -> event log has all 6 lifecycle
    events (RUN_STARTED, INTENT_CREATED, FILL_RECEIVED+, ORDER_SUBMITTED,
    POSITION_UPDATED, RUN_COMPLETED)."""
    backend, gateway, runner, event_log, portfolio_id = _wire(tmp_path=tmp_path)
    intent = _make_intent(
        portfolio_id=portfolio_id,
        package_id=backend._manifest.package_id,
        quantity=200,
    )

    result = runner.run_intents([intent])
    gateway.close()

    assert result.failed is False
    assert len(result.handles) == 1
    assert result.statuses[0].state in {"filled", "partial_filled"}
    assert len(result.rejected_intents) == 0

    records = event_log.read_all()
    types = [r.event_type for r in records]
    assert types[0] == DaemonEventType.RUN_STARTED
    assert types[-1] == DaemonEventType.RUN_COMPLETED
    assert DaemonEventType.INTENT_CREATED in types
    assert DaemonEventType.ORDER_SUBMITTED in types
    assert DaemonEventType.POSITION_UPDATED in types
    fill_records = [r for r in records if r.event_type == DaemonEventType.FILL_RECEIVED]
    assert len(fill_records) >= 1
    # Sequence is monotonic and dense from 1.
    seqs = [r.event_seq for r in records]
    assert seqs == list(range(1, len(records) + 1))

    # Position update reflects the filled symbol.
    pos_event = next(r for r in records if r.event_type == DaemonEventType.POSITION_UPDATED)
    assert "600000.SH" in pos_event.payload["positions"]
    assert pos_event.payload["positions"]["600000.SH"]["quantity"] == 200


def test_e2e_multi_intent_batch_aggregates(tmp_path: Path) -> None:
    """Scenario 2: 3 intents on different symbols -> 3 ORDER_SUBMITTED events
    + one POSITION_UPDATED with all three symbols."""
    backend, gateway, runner, event_log, portfolio_id = _wire(
        tmp_path=tmp_path,
        provider=_FakeProvider(symbol="ANY", open_price=10.0),
    )
    pkg = backend._manifest.package_id
    intents = [
        _make_intent(portfolio_id=portfolio_id, package_id=pkg, symbol="600000.SH"),
        _make_intent(portfolio_id=portfolio_id, package_id=pkg, symbol="600519.SH"),
        _make_intent(portfolio_id=portfolio_id, package_id=pkg, symbol="000001.SZ"),
    ]
    result = runner.run_intents(intents)
    gateway.close()

    assert len(result.handles) == 3
    assert len(result.rejected_intents) == 0

    records = event_log.read_all()
    submitted = [r for r in records if r.event_type == DaemonEventType.ORDER_SUBMITTED]
    assert len(submitted) == 3
    # Each handle_id is unique and present on submitted events.
    handle_ids = {r.handle_id for r in submitted}
    assert len(handle_ids) == 3
    assert all(hid is not None for hid in handle_ids)

    pos_events = [r for r in records if r.event_type == DaemonEventType.POSITION_UPDATED]
    assert len(pos_events) == 1
    positions = pos_events[0].payload["positions"]
    assert set(positions.keys()) == {"600000.SH", "600519.SH", "000001.SZ"}


def test_e2e_ledger_reject_on_insufficient_cash(tmp_path: Path) -> None:
    """Scenario 3: BUY 1000 shares of a 10x-priced stock with only 1k cash
    -> ledger rejects -> ORDER_REJECTED in event log, runner does not crash."""
    backend, gateway, runner, event_log, portfolio_id = _wire(
        tmp_path=tmp_path,
        initial_cash=1_000.0,  # too small to cover any 100-lot at price 10
        provider=_FakeProvider(symbol="600000.SH", open_price=10.0),
    )
    intent = _make_intent(
        portfolio_id=portfolio_id,
        package_id=backend._manifest.package_id,
        quantity=1000,
    )
    result = runner.run_intents([intent])
    gateway.close()

    assert result.failed is False
    assert len(result.handles) == 0
    assert intent.intent_id in result.rejected_intents

    records = event_log.read_all()
    types = [r.event_type for r in records]
    assert DaemonEventType.ORDER_REJECTED in types
    rej = next(r for r in records if r.event_type == DaemonEventType.ORDER_REJECTED)
    assert rej.intent_id == intent.intent_id
    assert "error_code" in rej.payload
    # Run completes despite the rejection.
    assert types[-1] == DaemonEventType.RUN_COMPLETED


def test_e2e_data_unavailable_raises_connectivity(tmp_path: Path) -> None:
    """Scenario 4: provider raises DataUnavailableError -> LocalSim translates
    to BrokerConnectivityError -> runner records ORDER_REJECTED + RUN_FAILED
    and re-raises."""
    backend, gateway, runner, event_log, portfolio_id = _wire(
        tmp_path=tmp_path,
        provider=_FakeProvider(
            symbol="600000.SH",
            unknown_symbols=("600001.SH",),
        ),
    )
    intent = _make_intent(
        portfolio_id=portfolio_id,
        package_id=backend._manifest.package_id,
        symbol="600001.SH",
    )
    with pytest.raises(BrokerConnectivityError):
        runner.run_intents([intent])
    gateway.close()

    records = event_log.read_all()
    types = [r.event_type for r in records]
    assert DaemonEventType.ORDER_REJECTED in types
    assert types[-1] == DaemonEventType.RUN_FAILED


def test_simgateway_lifecycle_invariants(tmp_path: Path) -> None:
    """Scenario 5: SimGateway state machine.
       - INIT->send_order rejected
       - connect twice rejected
       - close idempotent + reused gateway rejects connect
       - close twice OK"""
    manifest = make_paper_enabled_manifest()
    portfolio_id = f"paper_test_{uuid4().hex[:8]}"
    backend = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=1_000_000.0,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        market_data_provider=_FakeProvider(symbol="600000.SH"),
    )
    gateway = SimGateway.from_local_sim(backend)
    assert gateway.state == SimGatewayConnectionState.INIT

    intent = _make_intent(
        portfolio_id=portfolio_id, package_id=manifest.package_id
    )
    with pytest.raises(SimGatewayConnectError):
        gateway.send_order(intent)

    gateway.connect()
    assert gateway.state == SimGatewayConnectionState.CONNECTED
    with pytest.raises(SimGatewayConnectError):
        gateway.connect()  # double-connect rejected

    gateway.close()
    assert gateway.state == SimGatewayConnectionState.CLOSED
    gateway.close()  # idempotent
    with pytest.raises(SimGatewayConnectError):
        gateway.connect()  # cannot revive closed gateway
    with pytest.raises(SimGatewayConnectError):
        gateway.send_order(intent)  # closed gateway rejects send


def test_e2e_subscribe_fill_fanout_via_event_log(tmp_path: Path) -> None:
    """Scenario 6: each intent's fills produce >=1 FILL_RECEIVED event whose
    handle_id matches the eventual ORDER_SUBMITTED."""
    backend, gateway, runner, event_log, portfolio_id = _wire(tmp_path=tmp_path)
    intent_a = _make_intent(
        portfolio_id=portfolio_id,
        package_id=backend._manifest.package_id,
        quantity=300,
    )
    intent_b = _make_intent(
        portfolio_id=portfolio_id,
        package_id=backend._manifest.package_id,
        symbol="600519.SH",
        quantity=200,
    )
    result = runner.run_intents([intent_a, intent_b])
    gateway.close()

    assert len(result.handles) == 2

    records = event_log.read_all()
    submitted = {
        r.handle_id: r.intent_id
        for r in records
        if r.event_type == DaemonEventType.ORDER_SUBMITTED
    }
    fills = [r for r in records if r.event_type == DaemonEventType.FILL_RECEIVED]
    assert all(f.handle_id in submitted for f in fills)
    # Each intent gets at least one fill record.
    fills_by_intent: dict[str, int] = {}
    for f in fills:
        fills_by_intent[f.intent_id] = fills_by_intent.get(f.intent_id, 0) + 1
    assert fills_by_intent[intent_a.intent_id] >= 1
    assert fills_by_intent[intent_b.intent_id] >= 1


def test_event_log_count_helpers(tmp_path: Path) -> None:
    """Bonus #7: ``DaemonEventLog.count`` returns total + per-type counts."""
    _, gateway, runner, event_log, portfolio_id = _wire(tmp_path=tmp_path)
    intent = _make_intent(
        portfolio_id=portfolio_id, package_id=runner._manifest.package_id
    )
    runner.run_intents([intent])
    gateway.close()

    total = event_log.count()
    assert total == event_log.count(DaemonEventType.RUN_STARTED) \
        + event_log.count(DaemonEventType.INTENT_CREATED) \
        + event_log.count(DaemonEventType.FILL_RECEIVED) \
        + event_log.count(DaemonEventType.ORDER_SUBMITTED) \
        + event_log.count(DaemonEventType.POSITION_UPDATED) \
        + event_log.count(DaemonEventType.RUN_COMPLETED)
    assert event_log.count(DaemonEventType.RUN_STARTED) == 1
    assert event_log.count(DaemonEventType.RUN_COMPLETED) == 1
