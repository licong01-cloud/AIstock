from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.runner import PaperTradingV2Runner
from backend.services.paper_trading_v2.market_data import (
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    ExecutionAlgoError,
    InvalidStateTransitionError,
)
from backend.services.trading_core.limit_price_provider import DailyLimitPrice
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def make_ready_manifest(algo_code: str = "TWAP"):
    manifest = make_manifest(algo_code=algo_code).model_copy(
        update={"package_status": PackageStatus.PAPER_ENABLED}
    )
    return freeze_manifest(manifest)


def make_bars(symbol: str = "000001.SZ") -> list[MinuteBar]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        MinuteBar(
            symbol=symbol,
            bar_time=start + timedelta(minutes=i),
            open=10.0,
            high=10.2,
            low=9.9,
            close=10.1,
            volume=10000,
            limit_up=11.0,
            limit_down=9.0,
        )
        for i in range(3)
    ]


def make_raw_tdx_bars(symbol: str = "000001.SZ") -> list[dict]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        {
            "time": start + timedelta(minutes=i),
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "volume": 1000,
            "amount": 1_000_000.0,
        }
        for i in range(3)
    ]


class FakeLimitProvider:
    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        return DailyLimitPrice(
            symbol=symbol,
            trade_date=trade_date,
            pre_close=10.0,
            up_limit=11.0,
            down_limit=9.0,
        )


def test_runner_executes_strategy_package_single_order() -> None:
    manifest = make_ready_manifest(algo_code="TWAP")
    intent = OrderIntent(
        package_id=manifest.package_id,
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=300,
        target_trade_date=date(2024, 1, 2),
    )

    result = PaperTradingV2Runner().run_single_order(
        manifest=manifest,
        portfolio_id="paper_1",
        initial_cash=100_000.0,
        order_intent=intent,
        minute_bars=make_bars(),
        market_context={},
        snapshot_prices={"000001.SZ": 10.2},
        snapshot_time=datetime(2024, 1, 2, 15, 0),
    )

    assert result.order.status == OrderStatus.FILLED
    assert sum(fill.quantity for fill in result.fills) == 300
    assert result.account_snapshot.nav > 0


def test_runner_loads_tdx_market_data_and_executes_single_order() -> None:
    manifest = make_ready_manifest(algo_code="TWAP")
    intent = OrderIntent(
        package_id=manifest.package_id,
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=300,
        target_trade_date=date(2024, 1, 2),
    )
    market_provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_tdx_bars(),
    )

    result = PaperTradingV2Runner(market_data_provider=market_provider).run_single_order_from_market_data(
        manifest=manifest,
        portfolio_id="paper_1",
        initial_cash=100_000.0,
        order_intent=intent,
        data_source=MinuteDataSource.TDX_REALTIME,
        min_bars=3,
    )

    assert result.order.status == OrderStatus.FILLED
    assert result.account_snapshot.snapshot_time == datetime(2024, 1, 2, 9, 33)
    assert result.account_snapshot.market_value > 0


def test_runner_batch_executes_multiple_buy_orders_on_one_ledger() -> None:
    manifest = make_ready_manifest(algo_code="TWAP")
    intents = [
        OrderIntent(
            package_id=manifest.package_id,
            portfolio_id="paper_1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=300,
            target_trade_date=date(2024, 1, 2),
        ),
        OrderIntent(
            package_id=manifest.package_id,
            portfolio_id="paper_1",
            symbol="000002.SZ",
            side=OrderSide.BUY,
            quantity=300,
            target_trade_date=date(2024, 1, 2),
        ),
    ]

    result = PaperTradingV2Runner().run_order_batch(
        manifest=manifest,
        portfolio_id="paper_1",
        initial_cash=100_000.0,
        order_intents=intents,
        minute_bars_by_symbol={
            "000001.SZ": make_bars("000001.SZ"),
            "000002.SZ": make_bars("000002.SZ"),
        },
        market_context_by_symbol={"000001.SZ": {}, "000002.SZ": {}},
        snapshot_prices={"000001.SZ": 10.2, "000002.SZ": 10.2},
        snapshot_time=datetime(2024, 1, 2, 15, 0),
    )

    assert len(result.orders) == 2
    assert all(order.status == OrderStatus.FILLED for order in result.orders)
    assert sum(fill.quantity for fill in result.fills) == 600
    assert result.account_snapshot.nav > 0


def test_runner_batch_rejects_empty_order_list() -> None:
    manifest = make_ready_manifest(algo_code="TWAP")

    with pytest.raises(ArtifactGenerationFailedError, match="at least one"):
        PaperTradingV2Runner().run_order_batch(
            manifest=manifest,
            portfolio_id="paper_1",
            initial_cash=100_000.0,
            order_intents=[],
            minute_bars_by_symbol={},
            market_context_by_symbol={},
            snapshot_prices={},
            snapshot_time=datetime(2024, 1, 2, 15, 0),
        )


def test_runner_fails_for_package_mismatch() -> None:
    manifest = make_ready_manifest(algo_code="TWAP")
    intent = OrderIntent(
        package_id="wrong_pkg",
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=300,
        target_trade_date=date(2024, 1, 2),
    )

    with pytest.raises(InvalidStateTransitionError, match="package_id"):
        PaperTradingV2Runner().run_single_order(
            manifest=manifest,
            portfolio_id="paper_1",
            initial_cash=100_000.0,
            order_intent=intent,
            minute_bars=make_bars(),
            market_context={},
            snapshot_prices={"000001.SZ": 10.2},
            snapshot_time=datetime(2024, 1, 2, 15, 0),
        )


def test_runner_fails_for_v24_model_unavailable() -> None:
    manifest = make_ready_manifest(algo_code="V24_PLAN")
    intent = OrderIntent(
        package_id=manifest.package_id,
        portfolio_id="paper_1",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        quantity=300,
        target_trade_date=date(2024, 1, 2),
    )

    with pytest.raises(ExecutionAlgoError, match="authoritative minute execution") as exc_info:
        PaperTradingV2Runner().run_single_order(
            manifest=manifest,
            portfolio_id="paper_1",
            initial_cash=100_000.0,
            order_intent=intent,
            minute_bars=make_bars(),
            market_context={},
            snapshot_prices={"000001.SZ": 10.2},
            snapshot_time=datetime(2024, 1, 2, 15, 0),
        )
    assert "model_path" in exc_info.value.context["reason"]
