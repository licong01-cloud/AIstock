from __future__ import annotations

from datetime import date, datetime, timedelta

from backend.services.paper_trading_v2.live_session import PaperTradingLiveMinuteExecutor
from backend.services.paper_trading_v2.market_data import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.paper_trading_v2.models import (
    OrderExecutionState,
    PaperReplayDayResult,
    PaperReplayResult,
    PaperRun,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionStatus,
    PortfolioStatus,
)
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

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        days: list[date] = []
        current = start_date
        while current <= end_date:
            days.append(current)
            current += timedelta(days=1)
        return days


class FakeReplayService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def run(self, **kwargs):
        self.calls.append(kwargs)
        return PaperReplayResult(
            portfolio_id=kwargs["portfolio_id"],
            start_date=kwargs["start_date"],
            end_date=kwargs["end_date"],
            data_source=MinuteDataSource.DB_HISTORICAL,
            trading_days=[kwargs["start_date"], kwargs["end_date"]],
            day_results=[
                PaperReplayDayResult(
                    trade_date=kwargs["start_date"],
                    run_id="catchup_run_start",
                    status=RunStatus.SUCCEEDED,
                    nav=100_001,
                    order_count=1,
                    fill_count=1,
                    position_count=1,
                ),
                PaperReplayDayResult(
                    trade_date=kwargs["end_date"],
                    run_id="catchup_run_end",
                    status=RunStatus.SUCCEEDED,
                    nav=100_002,
                    order_count=1,
                    fill_count=1,
                    position_count=1,
                ),
            ],
        )


class FakeLiveMarket:
    def __init__(self, bars: list[MinuteBar]) -> None:
        self.bars = bars

    def load_observed_intraday(
        self,
        *,
        symbol,
        trade_date,
        source,
        until_time,
        require_suspend_status=False,
        require_day_features=False,
    ):
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


def make_portfolio_repo(*, data_source: MinuteDataSource = MinuteDataSource.TDX_REALTIME) -> tuple[InMemoryPaperTradingV2Repository, str]:
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
        data_source=data_source,
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


def test_live_session_injects_previous_trading_day_selection_cutoff() -> None:
    executor = PaperTradingLiveMinuteExecutor(
        repository=InMemoryPaperTradingV2Repository(),
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket([]),
    )
    config = {"selection_artifact_config": {"auto_generate": True}, "paper_v2_session": {}}

    executor._ensure_live_selection_cutoff(config, trade_date=date(2024, 1, 4))

    assert config["selection_artifact_config"]["cutoff_date"] == "2024-01-03"
    assert config["paper_v2_session"]["selection_cutoff_date"] == "2024-01-03"


def test_catchup_replay_end_includes_current_day_only_after_close() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(data_source=MinuteDataSource.DB_HISTORICAL)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.CATCHUP_THEN_LIVE,
        start_date=date(2024, 1, 2),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )

    assert PaperTradingLiveMinuteExecutor._catchup_replay_end(
        session=session,
        as_of_time=datetime(2024, 1, 4, 10, 0),
    ) == date(2024, 1, 3)
    assert PaperTradingLiveMinuteExecutor._catchup_replay_end(
        session=session,
        as_of_time=datetime(2024, 1, 4, 16, 0),
    ) == date(2024, 1, 4)


def test_live_waiting_next_day_keeps_portfolio_running_for_active_session() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 3),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket([]),
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 16, 0),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY
    assert paper_repo.get_portfolio(portfolio_id).status == PortfolioStatus.RUNNING


def test_catchup_then_live_replays_previous_days_and_processes_current_live_bar() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(data_source=MinuteDataSource.DB_HISTORICAL)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.CATCHUP_THEN_LIVE,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 4),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio_id,
            trade_date=date(2024, 1, 4),
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
            target_trade_date=date(2024, 1, 4),
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
    bars = [
        bar.model_copy(update={"bar_time": datetime(2024, 1, 4, 9, 31)})
        for bar in make_bars()[:1]
    ]
    replay = FakeReplayService()
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket(bars),
        replay_service=replay,  # type: ignore[arg-type]
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 4, 9, 31),
    )

    days = paper_repo.list_session_days(session.session_id)
    historical_days = [item for item in days if item.phase == PaperSessionPhase.HISTORICAL_REPLAY]
    live_days = [item for item in days if item.phase == PaperSessionPhase.LIVE_INTRADAY]
    fills = paper_repo.list_fills_for_run(run.run_id)
    assert replay.calls[0]["start_date"] == date(2024, 1, 2)
    assert replay.calls[0]["end_date"] == date(2024, 1, 3)
    assert [item.trade_date for item in historical_days] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert live_days[-1].trade_date == date(2024, 1, 4)
    assert live_days[-1].last_processed_bar_time == datetime(2024, 1, 4, 9, 31)
    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_FOR_BAR
    assert len(fills) == 1
