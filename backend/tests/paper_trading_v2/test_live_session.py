from __future__ import annotations

from datetime import date, datetime, timedelta

from backend.services.paper_trading_v2.live_session import PaperTradingLiveMinuteExecutor
from backend.services.paper_trading_v2.market_data import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.paper_trading_v2.models import OrderExecutionState, PaperRun, PaperSessionMode, PaperSessionStatus
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.paper_trading_v2.session import PaperTradingSessionRunner, PaperTradingSessionService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, RunStatus
from backend.services.trading_core.oms import OMS
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class FakeCalendar:
    def ensure_trading_day(self, trade_date: date) -> None:
        return None


class FakeLiveMarket:
    def __init__(self, bars: list[MinuteBar]) -> None:
        self.bars = bars

    def load_observed_intraday(self, *, symbol, trade_date, source, until_time, require_suspend_status=False):
        observed = [bar for bar in self.bars if bar.symbol == symbol and bar.bar_time <= until_time]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=observed,
            market_context={
                "stock_id": symbol,
                "trade_date": trade_date.isoformat(),
                "data_source": source.value,
                "observed_only": True,
                "observed_bar_count": len(observed),
                "prev_close": 10.0,
                "limit_up": 11.0,
                "limit_down": 9.0,
                "suspend_status": {"is_suspended": False},
                "full_day_open": [bar.open for bar in observed],
                "full_day_close": [bar.close for bar in observed],
                "full_day_volume": [bar.volume for bar in observed],
                "full_day_high": [bar.high for bar in observed],
                "full_day_low": [bar.low for bar in observed],
            },
        )

    def latest_available_bar_time(self, *, symbols, trade_date, source, as_of_time):
        times = [bar.bar_time for bar in self.bars if bar.symbol in symbols and bar.bar_time <= as_of_time]
        return max(times) if times else None


def make_portfolio_repo() -> tuple[InMemoryPaperTradingV2Repository, str]:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED}))
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="live session test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    return paper_repo, portfolio.portfolio_id


def make_bars() -> list[MinuteBar]:
    start = datetime(2024, 1, 2, 9, 31)
    return [
        MinuteBar(
            symbol="000001.SZ",
            bar_time=start + timedelta(minutes=i),
            open=10.0 + i * 0.01,
            high=10.2 + i * 0.01,
            low=9.9 + i * 0.01,
            close=10.1 + i * 0.01,
            volume=100_000,
            limit_up=11.0,
            limit_down=9.0,
        )
        for i in range(2)
    ]


def test_live_session_tick_processes_new_minute_bar_once() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.TDX_REALTIME,
            runtime_config={
                "validated_execution_policy": {
                    "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 2, "allow_partial_fill": True}},
                }
            },
        )
    )
    order = OMS().create_order(
        OrderIntent(
            package_id="pkg_test",
            portfolio_id=portfolio_id,
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=600,
            target_trade_date=date(2024, 1, 2),
        )
    )
    paper_repo.save_order(run.run_id, order)
    paper_repo.save_order_execution_state(
        OrderExecutionState(
            session_id=session.session_id,
            run_id=run.run_id,
            order_id=order.order_id,
            symbol=order.symbol,
            trade_date=run.trade_date,
            algo_code="TWAP",
            filled_quantity=0,
            remaining_quantity=order.quantity,
            status=order.status.value,
        )
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket(make_bars()),
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 31),
    )

    fills = paper_repo.list_fills_for_run(run.run_id)
    states = paper_repo.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)
    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_FOR_BAR
    assert len(fills) == 1
    assert fills[0]["quantity"] == 300
    assert states[0].last_processed_bar_time == datetime(2024, 1, 2, 9, 31)

    PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 31),
    )
    assert len(paper_repo.list_fills_for_run(run.run_id)) == 1
