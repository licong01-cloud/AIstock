from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.live_session import PaperTradingLiveMinuteExecutor
from backend.services.paper_trading_v2.market_data import MinuteDataSource, MinuteExecutionMarketInput
from backend.services.paper_trading_v2.models import (
    OrderExecutionState,
    PaperDayRunResult,
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
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import AccountSnapshot, MinuteBar, OrderIntent, OrderSide, PositionLot, RunStatus
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


class MissingStkLimitAudit:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def require_success(self, *, dataset, trade_date, data_source=None, max_age_minutes=None):
        self.calls.append(
            {
                "dataset": dataset,
                "trade_date": trade_date,
                "data_source": data_source,
                "max_age_minutes": max_age_minutes,
            }
        )
        if dataset == "stk_limit":
            raise DataUnavailableError(
                "required dataset refresh status is missing",
                context={"dataset": dataset, "trade_date": trade_date.isoformat()},
            )
        return object()


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


class FakeTdxFetchFailureMarket:
    def _raise(self, symbol: str, trade_date: date) -> None:
        raise DataUnavailableError(
            "TDX minute data fetch failed",
            context={"symbol": symbol, "trade_date": trade_date.isoformat()},
        )

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
        self._raise(symbol, trade_date)

    def latest_available_bar_time(self, *, symbols, trade_date, source, as_of_time):
        self._raise(symbols[0], trade_date)


class ExplodingLiveMarket:
    def load_observed_intraday(self, **kwargs):
        raise AssertionError("MiniQMT live session must not use TDX observed minute market data")

    def latest_available_bar_time(self, **kwargs):
        raise AssertionError("MiniQMT live session must not use TDX latest bar lookup")


class FakeTradabilityFilter:
    def filter_candidates(self, *, candidates, trade_date, top_k, package_id, manifest_sha256, enabled=True, industry_blacklist=None):
        return candidates[:top_k], []


class FakeSucceededArtifactStatus:
    value = "SUCCEEDED"


class FakeSelectionArtifactRepository:
    def get(self, **kwargs):
        class Artifact:
            status = FakeSucceededArtifactStatus()
            scores_json = [{"symbol": "000001.SZ", "score": 0.9, "rank": 1}]
            metadata = {
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
            }

        return Artifact()


class FakeRuntime:
    def __init__(self, candidates: list[dict] | None = None) -> None:
        self.artifact_repository = FakeSelectionArtifactRepository()
        self.candidates = candidates or [
            {"symbol": "000001.SZ", "score": 0.9, "rank": 1, "target_weight": 0.5, "reference_price": 10.0},
        ]

    def build_signal_snapshot(self, *, manifest, trade_date, data_source, runtime_config):
        from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot

        return SignalSnapshot(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=trade_date,
            data_source=data_source,
            candidates=[SelectionCandidate(**item) for item in self.candidates],
            runtime_config=runtime_config,
        )


class FakeTargetEngine:
    def build_targets(self, *, snapshot, total_equity, top_k, manifest=None, current_positions=None, current_prices=None):
        from backend.services.selection_center.models import TargetPosition

        candidate = snapshot.candidates[0]
        return [
            TargetPosition(
                symbol=candidate.symbol,
                target_quantity=600,
                target_weight=0.5,
                reference_price=candidate.reference_price,
                score=candidate.score,
                rank=candidate.rank,
                reason="test_live_target",
            )
        ]


class FakeRefreshAuditOk:
    def require_success(self, **kwargs):
        return {"status": "ok", **kwargs}


class FakeRiskPolicyService:
    def evaluate(self, *, symbols, trade_date, profile, current_positions):
        return {}

    def apply_to_candidates(self, *, candidates, decisions, trade_date, top_k, package_id, manifest_sha256, allow_empty=False):
        return candidates, []

    def forced_exit_targets(self, *, decisions, current_positions, trade_date, package_id, manifest_sha256, existing_target_symbols):
        return []


class FakeMiniQMTLiveDayHelper:
    def __init__(self, repository: InMemoryPaperTradingV2Repository) -> None:
        self.repository = repository
        self.calls: list[dict] = []

    def run_day(self, *, portfolio_id, trade_date, runtime_config=None, fee_model=None):
        self.calls.append(
            {
                "portfolio_id": portfolio_id,
                "trade_date": trade_date,
                "runtime_config": runtime_config,
                "fee_model": fee_model,
            }
        )
        portfolio = self.repository.get_portfolio(portfolio_id)
        run = self.repository.create_run(
            PaperRun(
                portfolio_id=portfolio_id,
                trade_date=trade_date,
                status=RunStatus.RUNNING,
                data_source=MinuteDataSource.MINIQMT_REALTIME,
                runtime_config=runtime_config or {},
            )
        )
        snapshot = AccountSnapshot(
            portfolio_id=portfolio_id,
            cash=100_000,
            market_value=0,
            nav=100_000,
            snapshot_time=datetime(2024, 1, 2, 10, 0),
        )
        self.repository.save_daily_snapshot(
            run_id=run.run_id,
            trade_date=trade_date,
            snapshot=snapshot,
            metadata={"broker_backend": "minqmt_sim", "authority_source": "MINIQMT_QUERY"},
        )
        succeeded = self.repository.update_run_status(run, RunStatus.SUCCEEDED)
        return PaperDayRunResult(
            portfolio=portfolio,
            run=succeeded,
            orders=[],
            fills=[],
            events=[],
            positions=[],
            account_snapshot=snapshot,
        )


def make_portfolio_repo(
    *,
    data_source: MinuteDataSource = MinuteDataSource.TDX_REALTIME,
    broker_backend: str = "local_sim",
) -> tuple[InMemoryPaperTradingV2Repository, str]:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = freeze_manifest(make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED}))
    package_repo.save_manifest(manifest)
    policy_json = manifest.minute_execution_policy.model_dump(mode="json")
    policy_json["data_requirements"] = {
        "requires_minute_bar": True,
        "requires_limit_price": True,
        "requires_trade_calendar": True,
        "requires_suspend_status": True,
    }
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="live session test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=data_source,
        broker_backend=broker_backend,
        execution_policy={"validated_execution_policy_id": StrategyPackageService(repository=package_repo).create_execution_policy(
            package_id=manifest.package_id,
            policy_name="live_session_test_policy",
            policy_json=policy_json,
            source_backtest_id="bt_live_session_test",
            source_backtest_status="BACKTEST_VALIDATED",
            paper_enabled=True,
        ).policy_id},
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


def test_minqmt_live_session_tick_uses_broker_day_path_without_tdx_market() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    fake_day_helper = FakeMiniQMTLiveDayHelper(paper_repo)
    live_executor.day_helper = fake_day_helper  # type: ignore[assignment]

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 10, 0),
    )

    run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert run is not None
    assert run.status == RunStatus.SUCCEEDED
    assert fake_day_helper.calls[0]["portfolio_id"] == portfolio_id
    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY
    assert paper_repo.get_portfolio(portfolio_id).status == PortfolioStatus.RUNNING
    days = paper_repo.list_session_days(session.session_id)
    assert days[-1].run_id == run.run_id
    assert days[-1].data_source == MinuteDataSource.MINIQMT_REALTIME
    snapshots = paper_repo.list_intraday_snapshots(session_id=session.session_id)
    assert snapshots[0]["source"] == "MINIQMT_REALTIME"
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert "MINIQMT_LIVE_TICK_STARTED" in event_types
    assert "MINIQMT_LIVE_TICK_RECONCILED" in event_types


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


def test_live_session_waits_for_preopen_stk_limit_until_0914() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    audit = MissingStkLimitAudit()
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket([]),
        refresh_audit=audit,
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 13, 59),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_FOR_BAR
    assert progress.session.phase == PaperSessionPhase.LIVE_INTRADAY
    assert paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2)) is None
    events = paper_repo.list_session_events(session.session_id)
    assert events[-1]["event_type"] == "LIVE_WAITING_FOR_DATA"
    assert events[-1]["context"]["deadline_time"] == "09:14"
    assert audit.calls[-1]["dataset"] == "stk_limit"


def test_live_session_fails_if_stk_limit_missing_at_0914() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket([]),
        refresh_audit=MissingStkLimitAudit(),
    )

    with pytest.raises(DataUnavailableError, match="requires stk_limit refresh by 09:14"):
        PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
            session.session_id,
            as_of_time=datetime(2024, 1, 2, 9, 14, 0),
        )

    failed = paper_repo.get_session(session.session_id)
    assert failed.status == PaperSessionStatus.FAILED
    assert failed.last_error is not None
    assert failed.last_error["error_code"] == "DATA_UNAVAILABLE"
    assert paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2)) is None


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

def test_live_prepare_seeds_order_cursor_after_existing_completed_bars() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    bars = [
        bar.model_copy(update={"bar_time": datetime(2024, 1, 2, 9, 31) + timedelta(minutes=i)})
        for i, bar in enumerate(make_bars() * 5)
    ]
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket(bars),
        runtime=FakeRuntime(),
        target_engine=FakeTargetEngine(),
        refresh_audit=FakeRefreshAuditOk(),
        risk_policy_service=FakeRiskPolicyService(),
        tradability_filter=FakeTradabilityFilter(),
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 39, 43),
    )

    run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert run is not None
    states = paper_repo.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)
    events = paper_repo.list_run_events(portfolio_id, run_id=run.run_id)
    assert progress.last_processed_bar_time == datetime(2024, 1, 2, 9, 39, 43)
    assert states[0].last_processed_bar_time == datetime(2024, 1, 2, 9, 39, 43)
    assert states[0].algo_state["live_causality_mode"] == "strict_no_backfill"
    assert states[0].algo_state["strict_live_start_bar_time"] == "2024-01-02T09:39:43"
    assert paper_repo.list_fills_for_run(run.run_id) == []
    assert [item["event_type"] for item in events[:2]] == ["TARGETS_GENERATED", "ORDER_INTENTS_GENERATED"]
    assert events[2]["context"]["latest_prepared_bar_time"] == "2024-01-02T09:39:00"
    assert events[2]["context"]["strict_live_start_bar_time"] == "2024-01-02T09:39:43"


def test_live_tick_never_backfills_prepared_order_with_existing_bars() -> None:
    paper_repo, portfolio_id = make_portfolio_repo()
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    bars = [
        bar.model_copy(update={"bar_time": datetime(2024, 1, 2, 9, 31) + timedelta(minutes=i)})
        for i, bar in enumerate(make_bars() * 6)
    ]
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket(bars),
        runtime=FakeRuntime(),
        target_engine=FakeTargetEngine(),
        refresh_audit=FakeRefreshAuditOk(),
        risk_policy_service=FakeRiskPolicyService(),
        tradability_filter=FakeTradabilityFilter(),
    )

    PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 39, 43),
    )
    run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert run is not None
    assert paper_repo.list_fills_for_run(run.run_id) == []

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 40, 0),
    )

    fills = paper_repo.list_fills_for_run(run.run_id)
    assert progress.last_processed_bar_time == datetime(2024, 1, 2, 9, 40, 0)
    assert len(fills) == 1
    assert fills[0]["trade_time"] == "2024-01-02T09:40:00"
    state = paper_repo.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)[0]
    assert datetime.fromisoformat(fills[0]["trade_time"]) > datetime.fromisoformat(state.algo_state["strict_live_start_bar_time"])
    assert all(datetime.fromisoformat(fill["trade_time"]).minute >= 40 for fill in fills)


def test_live_mark_to_market_continues_after_orders_filled() -> None:
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
                    "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 1, "allow_partial_fill": True}},
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
    ).model_copy(update={"status": "FILLED", "filled_quantity": 600, "avg_fill_price": 10.1})
    paper_repo.save_order(run.run_id, order)
    paper_repo.save_order_execution_state(
        OrderExecutionState(
            session_id=session.session_id,
            run_id=run.run_id,
            order_id=order.order_id,
            symbol=order.symbol,
            trade_date=run.trade_date,
            algo_code="TWAP",
            filled_quantity=600,
            remaining_quantity=0,
            status="FILLED",
            last_processed_bar_time=datetime(2024, 1, 2, 9, 31),
        )
    )
    paper_repo.save_positions(
        run_id=run.run_id,
        trade_date=run.trade_date,
        positions=[
            PositionLot(
                portfolio_id=portfolio_id,
                symbol="000001.SZ",
                quantity=600,
                available_quantity=600,
                avg_cost=10.1,
                trade_date=run.trade_date,
            )
        ],
        prices={"000001.SZ": 10.1},
    )
    bars = [
        make_bars()[0].model_copy(update={"bar_time": datetime(2024, 1, 2, 9, 31), "close": 10.1}),
        make_bars()[0].model_copy(update={"bar_time": datetime(2024, 1, 2, 9, 32), "close": 10.5}),
    ]
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeLiveMarket(bars),
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 9, 32),
    )

    snapshots = paper_repo.list_intraday_snapshots(session_id=session.session_id)
    assert progress.last_processed_bar_time == datetime(2024, 1, 2, 9, 32)
    assert len(snapshots) == 1
    assert snapshots[0]["snapshot_time"] == "2024-01-02T09:32:00"
    assert snapshots[0]["market_value"] == pytest.approx(6300.0)
    assert snapshots[0]["nav"] == pytest.approx(106300.0)
    events = paper_repo.list_session_events(session.session_id)
    assert events[-1]["event_type"] == "LIVE_MARK_TO_MARKET_SNAPSHOT"


def test_live_tdx_fetch_failure_waits_without_failing_session() -> None:
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
                    "policy_json": {"algo_code": "TWAP", "algo_config": {"split_count": 1, "allow_partial_fill": True}},
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
            last_processed_bar_time=datetime(2024, 1, 2, 9, 31),
        )
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeTdxFetchFailureMarket(),
    )

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 10, 0),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_FOR_BAR
    assert progress.session.last_error is not None
    assert progress.session.last_error["message"] == "TDX minute data fetch failed"
    assert paper_repo.get_run(run.run_id).status == RunStatus.RUNNING
    assert paper_repo.get_portfolio(portfolio_id).status == PortfolioStatus.RUNNING
    assert paper_repo.list_errors(portfolio_id) == []
    events = paper_repo.list_session_events(session.session_id)
    assert events[-1]["event_type"] == "LIVE_DATA_FETCH_RETRYABLE"
    assert events[-1]["context"]["retryable"] is True
    run_events = paper_repo.list_run_events(portfolio_id, run_id=run.run_id)
    assert run_events[-1]["event_type"] == "LIVE_DATA_FETCH_RETRYABLE"
    assert run_events[-1]["context"]["retryable"] is True
    days = paper_repo.list_session_days(session.session_id)
    assert days[-1].run_id == run.run_id
    assert days[-1].last_processed_bar_time is None
