"""Retired MiniQMT broker paths in Paper v2."""


from __future__ import annotations

from datetime import date, datetime

import pytest

from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS
from backend.services.paper_trading_v2.auto_run import miniqmt_account_group_id
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner, miniqmt_account_slot_context
from backend.services.paper_trading_v2.live_session import PaperTradingLiveMinuteExecutor
from backend.services.paper_trading_v2.models import (
    PaperRun,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionStatus,
    PortfolioStatus,
)
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import VNPY_STYLE_TEMPLATE_POLICY_PREFIX, PaperTradingV2PortfolioService
from backend.services.paper_trading_v2.session import PaperTradingSessionRunner, PaperTradingSessionService
from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.daily_context_provider import PreTradeTradabilityProvider
from backend.services.simulation_data.tdx_causal_minute import quote_tradability_evidence
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import Order, OrderSide, OrderStatus, OrderType, RunStatus
from backend.tests.paper_trading_v2.test_auto_run import (
    _seed_service,
)
from backend.tests.paper_trading_v2.test_execution_policy_router import (
    _service_and_portfolio,
)
from backend.tests.paper_trading_v2.test_live_session import (
    BrokerDownMiniQMTLiveDayHelper,
    ExplodingLiveMarket,
    FakeCalendar,
    FakeMiniQMTLiveDayHelper,
    PlatformDataDownMiniQMTLiveDayHelper,
    SubmitTimeoutMiniQMTLiveDayHelper,
    make_portfolio_repo,
)
from backend.tests.paper_trading_v2.test_market_data import (
    FakeStStatusProvider,
    frozen_daily_fact,
)
from backend.tests.paper_trading_v2.test_portfolio_broker_backend import (
    _seed_paper_enabled_package,
)
from backend.tests.paper_trading_v2.test_session import (
    ExplodingTradingCalendar,
    make_portfolio,
)


def test_create_minqmt_auto_run_portfolio_binds_account_and_creates_live_session() -> None:
    service, paper_repo, package_id = _seed_service()

    result = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        top_k=10,
        created_by="pytest",
    )

    portfolio = result["portfolio"]
    session = result["session"]
    binding = result["binding"]
    assert portfolio.auto_run_enabled is True
    assert portfolio.broker_backend == "minqmt_sim"
    assert portfolio.data_source == MinuteDataSource.MINIQMT_REALTIME
    assert binding.broker_account_id == "acct-a"
    assert session.mode == PaperSessionMode.LIVE_ONLY
    assert session.status == PaperSessionStatus.CREATED
    assert session.live_data_source == MinuteDataSource.MINIQMT_REALTIME
    assert paper_repo.list_active_sessions(portfolio.portfolio_id)[0].session_id == session.session_id
    assert result["auto_run"]["config"]["runtime_profile"]["selection"]["top_k"] == 10


def test_minqmt_auto_run_allows_same_account_strategy_slots() -> None:
    service, paper_repo, package_id = _seed_service()
    first = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )
    second = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run two",
        initial_cash=80_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )

    assert len(paper_repo.portfolios) == 2
    first_binding = first["binding"]
    second_binding = second["binding"]
    assert first_binding.allocation_mode == "account_group_slots"
    assert second_binding.allocation_mode == "account_group_slots"
    assert first_binding.broker_account_id == second_binding.broker_account_id == "acct-a"
    assert first_binding.account_group_id == second_binding.account_group_id == miniqmt_account_group_id("acct-a")
    assert first_binding.strategy_slot_id == first["portfolio"].portfolio_id
    assert second_binding.strategy_slot_id == second["portfolio"].portfolio_id
    assert first_binding.strategy_slot_id != second_binding.strategy_slot_id
    assert first["auto_run"]["config"]["broker"]["account_group_id"] == first_binding.account_group_id
    assert second["auto_run"]["config"]["broker"]["strategy_slot_id"] == second_binding.strategy_slot_id


def test_minqmt_auto_run_context_exposes_account_group_slot_for_runtime() -> None:
    service, paper_repo, package_id = _seed_service()
    result = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )

    context = miniqmt_account_slot_context(paper_repo, result["portfolio"])

    assert context == {
        "account_mode": "account_group_slots",
        "account_group_id": miniqmt_account_group_id("acct-a"),
        "strategy_slot_id": result["portfolio"].portfolio_id,
    }


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


def test_minqmt_live_session_passes_previous_trading_day_cutoff_to_day_runner() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={
            "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
            "selection_artifact_config": {"auto_generate": True, "inference_backend": "wsl"},
        },
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    fake_day_helper = FakeMiniQMTLiveDayHelper(paper_repo)
    live_executor.day_helper = fake_day_helper  # type: ignore[assignment]

    PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 4, 10, 0),
    )

    runtime_config = fake_day_helper.calls[0]["runtime_config"]
    assert runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-03"
    assert runtime_config["paper_v2_session"]["selection_cutoff_date"] == "2024-01-03"
    assert "cutoff_date" not in session.runtime_config["selection_artifact_config"]


def test_minqmt_live_session_waits_before_submit_window_without_orders() -> None:
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
        as_of_time=datetime(2024, 1, 2, 8, 0),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_MARKET_WINDOW
    assert fake_day_helper.calls == []
    assert paper_repo.runs == {}
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert "MINIQMT_LIVE_WAITING_MARKET_WINDOW" in event_types


def test_minqmt_live_session_waits_for_broker_before_cutoff() -> None:
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
    broker_down = BrokerDownMiniQMTLiveDayHelper()
    live_executor.day_helper = broker_down  # type: ignore[assignment]

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 13, 1),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_BROKER
    assert broker_down.calls
    assert paper_repo.runs == {}
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert "MINIQMT_LIVE_WAITING_BROKER" in event_types


def test_minqmt_live_session_throttles_repeated_platform_data_live_inference_retry() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={
            "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
            "trade_window_policy": {
                "prepare_start": "08:50",
                "submit_windows": [{"start": "09:25", "end": "11:30"}],
                "final_submit_cutoff": "14:55",
                "platform_data_retry_after_seconds": 600,
            },
        },
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    platform_down = PlatformDataDownMiniQMTLiveDayHelper()
    live_executor.day_helper = platform_down  # type: ignore[assignment]
    runner = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor)

    first = runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 30))
    second = runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 31))

    assert first.session.status == PaperSessionStatus.LIVE_WAITING_PLATFORM_DATA
    assert second.session.status == PaperSessionStatus.LIVE_WAITING_PLATFORM_DATA
    assert len(platform_down.calls) == 1
    assert paper_repo.runs == {}
    events = paper_repo.list_session_events(session.session_id)
    event_types = [event["event_type"] for event in events]
    assert event_types.count("MINIQMT_LIVE_WAITING_PLATFORM_DATA") == 1
    assert event_types.count("MINIQMT_LIVE_PLATFORM_DATA_RETRY_THROTTLED") == 1
    wait_event = next(event for event in events if event["event_type"] == "MINIQMT_LIVE_WAITING_PLATFORM_DATA")
    throttle_event = next(
        event for event in events if event["event_type"] == "MINIQMT_LIVE_PLATFORM_DATA_RETRY_THROTTLED"
    )
    assert wait_event["context"]["platform_data_retry"]["sql_storm_guard"] is True
    assert throttle_event["context"]["platform_data_retry"]["retry_allowed"] is False


def test_minqmt_live_session_retries_platform_data_after_retry_interval() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={
            "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
            "trade_window_policy": {
                "prepare_start": "08:50",
                "submit_windows": [{"start": "09:25", "end": "11:30"}],
                "final_submit_cutoff": "14:55",
                "platform_data_retry_after_seconds": 60,
            },
        },
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    platform_down = PlatformDataDownMiniQMTLiveDayHelper()
    live_executor.day_helper = platform_down  # type: ignore[assignment]
    runner = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor)

    runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 30))
    runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 31, 1))

    assert len(platform_down.calls) == 2
    events = paper_repo.list_session_events(session.session_id)
    event_types = [event["event_type"] for event in events]
    assert event_types.count("MINIQMT_LIVE_WAITING_PLATFORM_DATA") == 2
    assert "MINIQMT_LIVE_PLATFORM_DATA_RETRY_THROTTLED" not in event_types


def test_minqmt_live_session_treats_submit_timeout_as_broker_wait_before_cutoff() -> None:
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
    submit_timeout = SubmitTimeoutMiniQMTLiveDayHelper()
    live_executor.day_helper = submit_timeout  # type: ignore[assignment]

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 13, 1),
    )

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_BROKER
    assert progress.session.last_error["context"]["submit_diagnostic"]["classification"] == "adapter_timeout"
    assert submit_timeout.calls
    assert paper_repo.runs == {}
    events = paper_repo.list_session_events(session.session_id)
    event_types = [event["event_type"] for event in events]
    assert "MINIQMT_LIVE_WAITING_BROKER" in event_types
    wait_event = next(event for event in events if event["event_type"] == "MINIQMT_LIVE_WAITING_BROKER")
    assert wait_event["context"]["broker_retry"]["retry_after_seconds"] == 120.0
    assert wait_event["context"]["broker_retry"]["native_order_reconcile_required"] is True


def test_minqmt_live_session_throttles_repeated_broker_submit_timeout_retry() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={
            "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
            "trade_window_policy": {
                "prepare_start": "08:50",
                "submit_windows": [{"start": "09:25", "end": "11:30"}],
                "final_submit_cutoff": "14:55",
                "broker_retry_after_seconds": 120,
            },
        },
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    submit_timeout = SubmitTimeoutMiniQMTLiveDayHelper()
    live_executor.day_helper = submit_timeout  # type: ignore[assignment]
    runner = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor)

    first = runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 30))
    second = runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 31))

    assert first.session.status == PaperSessionStatus.LIVE_WAITING_BROKER
    assert second.session.status == PaperSessionStatus.LIVE_WAITING_BROKER
    assert len(submit_timeout.calls) == 1
    events = paper_repo.list_session_events(session.session_id)
    event_types = [event["event_type"] for event in events]
    assert event_types.count("MINIQMT_LIVE_WAITING_BROKER") == 1
    assert event_types.count("MINIQMT_LIVE_BROKER_RETRY_THROTTLED") == 1
    throttle_event = next(event for event in events if event["event_type"] == "MINIQMT_LIVE_BROKER_RETRY_THROTTLED")
    assert throttle_event["context"]["broker_retry"]["retry_allowed"] is False
    assert throttle_event["context"]["broker_retry"]["native_order_reconcile_required"] is True


def test_minqmt_live_session_retries_broker_after_retry_interval() -> None:
    paper_repo, portfolio_id = make_portfolio_repo(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={
            "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
            "trade_window_policy": {
                "prepare_start": "08:50",
                "submit_windows": [{"start": "09:25", "end": "11:30"}],
                "final_submit_cutoff": "14:55",
                "broker_retry_after_seconds": 60,
            },
        },
    )
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    submit_timeout = SubmitTimeoutMiniQMTLiveDayHelper()
    live_executor.day_helper = submit_timeout  # type: ignore[assignment]
    runner = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor)

    runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 30))
    runner.tick(session.session_id, as_of_time=datetime(2024, 1, 2, 9, 31, 1))

    assert len(submit_timeout.calls) == 2
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert event_types.count("MINIQMT_LIVE_WAITING_BROKER") == 2
    assert "MINIQMT_LIVE_BROKER_RETRY_THROTTLED" not in event_types


def test_minqmt_live_session_resets_stale_empty_running_run_before_cutoff() -> None:
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
    stale_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            started_at=datetime(2024, 1, 2, 10, 0),
        )
    )
    paper_repo.save_run_event(
        run_id=stale_run.run_id,
        event_type="MINIQMT_ORDER_SUBMISSION_SEQUENCE",
        message="submission sequence created without persisted broker orders",
    )
    paper_repo.update_session_status(
        session.session_id,
        status=PaperSessionStatus.LIVE_RETRYING,
        phase=PaperSessionPhase.LIVE_INTRADAY,
    )
    paper_repo.update_portfolio_status(portfolio_id, PortfolioStatus.RUNNING)
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    fake_day_helper = FakeMiniQMTLiveDayHelper(paper_repo)
    live_executor.day_helper = fake_day_helper  # type: ignore[assignment]

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 13, 1),
    )

    retried_run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert retried_run is not None
    assert retried_run.run_id != stale_run.run_id
    assert retried_run.status == RunStatus.SUCCEEDED
    assert fake_day_helper.calls
    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert "MINIQMT_LIVE_STALE_EMPTY_RUN_RESET" in event_types
    reset_event = next(
        event
        for event in paper_repo.list_session_events(session.session_id)
        if event["event_type"] == "MINIQMT_LIVE_STALE_EMPTY_RUN_RESET"
    )
    assert reset_event["context"]["run_id"] == stale_run.run_id
    assert reset_event["context"]["reset_counts"]["run"] == 1


def test_minqmt_live_session_final_fails_stale_empty_running_run_after_cutoff() -> None:
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
    stale_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            started_at=datetime(2024, 1, 2, 10, 0),
        )
    )
    paper_repo.save_run_event(
        run_id=stale_run.run_id,
        event_type="MINIQMT_ORDER_SUBMISSION_SEQUENCE",
        message="submission sequence created without persisted broker orders",
    )
    paper_repo.update_session_status(
        session.session_id,
        status=PaperSessionStatus.LIVE_RETRYING,
        phase=PaperSessionPhase.LIVE_INTRADAY,
    )
    paper_repo.update_portfolio_status(portfolio_id, PortfolioStatus.RUNNING)
    live_executor = PaperTradingLiveMinuteExecutor(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=ExplodingLiveMarket(),  # type: ignore[arg-type]
    )
    fake_day_helper = FakeMiniQMTLiveDayHelper(paper_repo)
    live_executor.day_helper = fake_day_helper  # type: ignore[assignment]

    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=live_executor).tick(
        session.session_id,
        as_of_time=datetime(2024, 1, 2, 14, 56),
    )

    failed_run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert failed_run is not None
    assert failed_run.run_id == stale_run.run_id
    assert failed_run.status == RunStatus.FAILED
    assert fake_day_helper.calls == []
    assert progress.session.status == PaperSessionStatus.FAILED
    assert progress.session.last_error is not None
    assert progress.session.last_error["error_code"] == "MINIQMT_LIVE_STALE_EMPTY_RUN_AFTER_CUTOFF"


def test_minqmt_live_session_after_cutoff_final_fails_without_fake_success() -> None:
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
        as_of_time=datetime(2024, 1, 2, 14, 56),
    )

    run = paper_repo.get_run_by_portfolio_date(portfolio_id, date(2024, 1, 2))
    assert progress.session.status == PaperSessionStatus.FAILED
    assert run is not None
    assert run.status == RunStatus.FAILED
    assert fake_day_helper.calls == []
    assert paper_repo.get_portfolio(portfolio_id).status == PortfolioStatus.FAILED
    assert paper_repo.list_errors(portfolio_id)


def test_pre_trade_miniqmt_quote_uses_yuan_basis_for_limit_range() -> None:
    payload = quote_tradability_evidence(
        symbol="603303.SH",
        quote={
            "price_basis": "yuan",
            "lastPrice": 30.23,
            "pre_close": 30.14,
            "open": 30.27,
            "high": 31.8,
            "low": 29.01,
            "volume": 66974,
            "amount": 203373536,
            "bid_price_1": 30.23,
            "bid_volume_1": 108,
            "ask_price_1": 30.26,
            "ask_volume_1": 17,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=False),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_up"] == pytest.approx(33.15)
    assert payload["limit_down"] == pytest.approx(27.13)
    assert payload["no_tradable_market"] is False
    assert payload["blocked_sides"] == []


def test_pre_trade_miniqmt_quote_uses_frozen_stk_limit_without_static_provider_calls() -> None:
    reference = frozen_daily_fact(
        symbol="000001.SZ",
        trade_date=date(2026, 6, 23),
        pre_close=10.0,
        up_limit=10.77,
        down_limit=9.23,
    )

    class PoisonStaticProvider:
        def get_suspend_status(self, *_args, **_kwargs):
            raise AssertionError("suspend_d was queried after daily context freeze")

        def get_st_status(self, *_args, **_kwargs):
            raise AssertionError("stock_st was queried after daily context freeze")

    provider = PreTradeTradabilityProvider(
        suspend_status_provider=PoisonStaticProvider(),
        st_status_provider=PoisonStaticProvider(),
        realtime_quote_source="MINIQMT_REALTIME.broker_quote",
        realtime_quote_fetcher=lambda _symbols: {
            "000001.SZ": {
                "lastPrice": 10.77,
                "pre_close": 9.5,
                "open": 10.1,
                "high": 10.77,
                "low": 10.0,
                "volume": 100,
                "amount": 1000,
                "bid_price_1": 10.76,
                "bid_volume_1": 100,
                "ask_price_1": 10.77,
                "ask_volume_1": 100,
                "time": "2026-06-23 14:05:00",
            }
        },
    )
    frozen_status = {
        "000001.SZ": {
            "daily_trading_context": reference,
        }
    }

    status = provider.get_statuses(
        ["000001.SZ"],
        date(2026, 6, 23),
        require_realtime_quote=True,
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        side_by_symbol={"000001.SZ": "BUY"},
        frozen_daily_statuses=frozen_status,
    )["000001.SZ"]

    assert status["reason_code"] == "LIMIT_UP_BUY_BLOCKED"
    assert status["quote_evidence"]["pre_close"] == 10.0
    assert status["quote_evidence"]["limit_up"] == 10.77
    assert status["quote_evidence"]["limit_price_source"] == "market.stk_limit:frozen_daily_trading_context_v1"
    assert status["daily_trading_context"]["context_id"] == reference["context_id"]


def test_pre_trade_miniqmt_quote_ignores_degenerate_raw_li_limit_metadata() -> None:
    payload = quote_tradability_evidence(
        symbol="000048.SZ",
        quote={
            "quote_price_basis": "raw_li",
            "lastPrice": 20.66,
            "pre_close": 20.75,
            "open": 20.7,
            "high": 20.88,
            "low": 20.3,
            "volume": 12345,
            "amount": 25432100,
            "bid_price_1": 20.65,
            "bid_volume_1": 100,
            "ask_price_1": 20.66,
            "ask_volume_1": 100,
            "limit_up": 20.0,
            "limit_down": 20.0,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=False),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_up"] == pytest.approx(22.83)
    assert payload["limit_down"] == pytest.approx(18.68)
    assert payload["limit_down"] < payload["pre_close"] < payload["limit_up"]
    assert payload["no_tradable_market"] is False


def test_pre_trade_miniqmt_quote_fails_with_miniqmt_source_label_when_pre_close_missing() -> None:
    with pytest.raises(DataUnavailableError) as exc_info:
        quote_tradability_evidence(
            symbol="603303.SH",
            quote={
                "lastPrice": 30.23,
                "open": 30.27,
                "high": 31.8,
                "low": 29.01,
                "volume": 66974,
                "amount": 203373536,
                "bid_price_1": 30.23,
                "bid_volume_1": 108,
                "ask_price_1": 30.26,
                "ask_volume_1": 17,
                "time": "2026-06-23 14:05:00",
            },
            source="MINIQMT_REALTIME.broker_quote",
            trade_date=date(2026, 6, 23),
            as_of_time=datetime(2026, 6, 23, 14, 5, 30),
            st_status_provider=FakeStStatusProvider(is_st=False),
        )

    assert exc_info.value.context["reason_code"] == "REALTIME_QUOTE_PRE_CLOSE_MISSING"
    assert exc_info.value.context["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert str(exc_info.value).startswith("MiniQMT realtime quote previous close")
    assert "TDX" not in str(exc_info.value)


def test_pre_trade_miniqmt_st_quote_uses_five_percent_limit() -> None:
    payload = quote_tradability_evidence(
        symbol="000048.SZ",
        quote={
            "quote_price_basis": "raw_li",
            "lastPrice": 20.66,
            "pre_close": 20.75,
            "open": 20.7,
            "high": 20.88,
            "low": 20.3,
            "volume": 12345,
            "amount": 25432100,
            "bid_price_1": 20.65,
            "bid_volume_1": 100,
            "ask_price_1": 20.66,
            "ask_volume_1": 100,
            "time": "2026-06-23 14:05:00",
        },
        source="MINIQMT_REALTIME.broker_quote",
        trade_date=date(2026, 6, 23),
        as_of_time=datetime(2026, 6, 23, 14, 5, 30),
        st_status_provider=FakeStStatusProvider(is_st=True),
    )

    assert payload["quote_price_basis"] == "yuan"
    assert payload["limit_pct"] == pytest.approx(0.05)
    assert payload["limit_up"] == pytest.approx(21.79)
    assert payload["limit_down"] == pytest.approx(19.71)


def test_minqmt_reconcile_accepts_broker_partial_odd_lot_trade_row() -> None:
    order = Order(
        order_id="ord_minqmt_partial_odd_lot",
        intent_id="intent_minqmt_partial_odd_lot",
        package_id="pkg_unit",
        portfolio_id="paper_unit",
        symbol="301135.SZ",
        side=OrderSide.SELL,
        quantity=30_100,
        order_type=OrderType.LIMIT,
        limit_price=18.1,
        status=OrderStatus.SUBMITTED,
    )

    fills = PaperTradingDayRunner._miniqmt_fills_from_trades(
        [
            {
                "traded_id": "1010000037440494",
                "stock_code": "301135.SZ",
                "stock_name": "閻熺偛鐥夐弲楦垮厴",
                "order_type": 24,
                "traded_time": "144315",
                "traded_price": 18.11,
                "traded_volume": 190,
                "traded_amount": 3440.9,
                "order_id": "1090535320",
                "order_sysid": "4933",
                "commission": 0.0,
                "strategy_name": "paper_1d9b1f03700f4810a",
                "order_remark": "paper-v2-miniqmt",
            }
        ],
        order=order,
        native={"miniqmt_order_id": "1090535320"},
        trade_date=date(2026, 6, 3),
    )

    assert len(fills) == 1
    assert fills[0].quantity == 190
    assert fills[0].metadata["authority_source"] == "MINIQMT_TRADE"
    assert fills[0].metadata["miniqmt_trade_raw"]["traded_volume"] == 190


def test_minqmt_runtime_defaults_use_db_historical_signal_source() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    config = PaperTradingV2PortfolioService(repository=paper_repo).resolve_runtime_config_for_date(
        portfolio=portfolio,
        trade_date=date(2024, 1, 2),
        runtime_config={},
    )

    assert config["selection_artifact_config"]["auto_generate"] is True
    assert config["selection_artifact_config"]["signal_data_source"] == MinuteDataSource.DB_HISTORICAL.value
    assert config["paper_v2_session"]["signal_data_source"] == MinuteDataSource.DB_HISTORICAL.value


def test_minqmt_sim_live_session_creation_is_not_time_window_blocked() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=ExplodingTradingCalendar(),
        enforce_non_trading_window=True,
    )

    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.MINIQMT_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
        as_of_time=datetime(2024, 1, 2, 13, 5),
    )

    assert session.live_data_source == MinuteDataSource.MINIQMT_REALTIME
    assert session.status == PaperSessionStatus.CREATED


def test_session_capabilities_expose_minqmt_live_source() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )

    capabilities = PaperTradingSessionService(repository=paper_repo).session_capabilities(portfolio.portfolio_id)

    assert capabilities["broker_backend"] == "minqmt_sim"
    assert capabilities["portfolio_data_source"] == "MINIQMT_REALTIME"
    assert capabilities["modes"]["LIVE_ONLY"]["can_start"] is True
    assert capabilities["modes"]["REPLAY_ONLY"]["can_start"] is False


def test_minqmt_portfolio_lists_vnpy_style_runtime_template_policies() -> None:
    service, _manifest, portfolio = _service_and_portfolio(broker_backend="minqmt_sim")

    rows = service.list_execution_policies(portfolio.portfolio_id)
    templates = {
        item["algo_code"]: item for item in rows if item.get("activation_policy_source") == "vnpy_style_asset_template"
    }

    assert set(templates) == set(VNPY_STYLE_ASSETS)
    sniper = templates["SNIPER_MINIQMT"]
    assert sniper["policy_id"] == f"{VNPY_STYLE_TEMPLATE_POLICY_PREFIX}SNIPER_MINIQMT"
    assert sniper["validated_execution_policy_id"] == sniper["policy_id"]
    assert sniper["runtime_selectable"] is True
    assert sniper["matches_portfolio_manifest"] is True
    assert sniper["policy_json"]["data_requirements"]["requires_broker_quote"] is True
    assert sniper["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")


def test_create_portfolio_minqmt_sim_with_miniqmt_realtime() -> None:
    package_repo, manifest = _seed_paper_enabled_package()
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="minqmt_sim happy",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.MINIQMT_REALTIME,
        broker_backend="minqmt_sim",
    )
    assert portfolio.broker_backend == "minqmt_sim"
    assert portfolio.data_source == MinuteDataSource.MINIQMT_REALTIME
