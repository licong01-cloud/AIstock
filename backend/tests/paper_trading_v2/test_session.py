from __future__ import annotations

from datetime import UTC, date, datetime

import time

import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import (
    PaperReplayDayResult,
    PaperReplayResult,
    PaperRun,
    PaperSessionProgress,
    PaperSessionMode,
    PaperSessionStatus,
    PortfolioStatus,
)
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.paper_trading_v2.scheduler import PaperTradingV2SessionScheduler
from backend.services.paper_trading_v2.session import PaperTradingSessionRunner, PaperTradingSessionService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    SessionAlreadyRunningError,
    SessionConfigError,
    SessionSourceUnsupportedError,
)
from backend.services.trading_core.models import RunStatus
from backend.tests.paper_trading_v2.test_day_runner import save_manifest_with_default_execution_policy
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def make_paper_manifest():
    manifest = make_manifest().model_copy(
        update={
            "package_status": PackageStatus.PAPER_ENABLED,
            "portfolio_policy": PortfolioPolicy(topk=20, n_drop=5),
        }
    )
    return freeze_manifest(manifest)


def make_portfolio(
    *,
    data_source: MinuteDataSource = MinuteDataSource.DB_HISTORICAL,
    broker_backend: str = "local_sim",
):
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="session test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=data_source,
        broker_backend=broker_backend,
    )
    return package_repo, paper_repo, portfolio


def test_realtime_portfolio_runtime_defaults_auto_generate_selection_artifact() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    config = PaperTradingV2PortfolioService(repository=paper_repo).resolve_runtime_config_for_date(
        portfolio=portfolio,
        trade_date=date(2024, 1, 2),
        runtime_config={},
    )

    assert config["selection_artifact_config"]["auto_generate"] is True
    assert config["selection_artifact_config"]["signal_data_source"] == MinuteDataSource.DB_HISTORICAL.value
    assert config["paper_v2_session"]["signal_data_source"] == MinuteDataSource.DB_HISTORICAL.value


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
                    run_id="run_start",
                    status=RunStatus.SUCCEEDED,
                    nav=100_001,
                    order_count=1,
                    fill_count=1,
                    position_count=1,
                    actual_bar_count=240,
                ),
                PaperReplayDayResult(
                    trade_date=kwargs["end_date"],
                    run_id="run_end",
                    status=RunStatus.SUCCEEDED,
                    nav=100_002,
                    order_count=1,
                    fill_count=1,
                    position_count=1,
                    actual_bar_count=241,
                ),
            ],
        )


class FakeLiveExecutor:
    def __init__(self, repo: InMemoryPaperTradingV2Repository) -> None:
        self.repo = repo
        self.calls: list[dict] = []

    def tick(self, session, *, as_of_time=None):
        self.calls.append({"session_id": session.session_id, "mode": session.mode.value, "as_of_time": as_of_time})
        updated = self.repo.update_session_status(
            session.session_id,
            status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
        )
        return PaperSessionProgress(session=updated, day_count=0, events=self.repo.list_session_events(session.session_id))


class FailingLiveExecutor:
    def __init__(self, repo: InMemoryPaperTradingV2Repository, *, trade_date: date) -> None:
        self.repo = repo
        self.trade_date = trade_date

    def tick(self, session, *, as_of_time=None):
        run = PaperRun(
            run_id="run_live_terminal_failure",
            portfolio_id=session.portfolio_id,
            trade_date=self.trade_date,
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.TDX_REALTIME,
            runtime_config={},
        )
        self.repo.create_run(run)
        raise DataUnavailableError(
            "live data integrity failure",
            context={
                "session_id": session.session_id,
                "trade_date": self.trade_date.isoformat(),
                "symbol": "000001.SZ",
            },
        )


class FakeTradingCalendar:
    def __init__(self, *, trading_day: bool = True) -> None:
        self.trading_day = trading_day

    def ensure_trading_day(self, trade_date: date) -> None:
        if not self.trading_day:
            raise DataUnavailableError("not trading day", context={"trade_date": trade_date.isoformat()})


class ExplodingTradingCalendar:
    def ensure_trading_day(self, trade_date: date) -> None:
        raise RuntimeError("calendar backend unavailable")


def test_replay_only_session_create_tick_and_progress() -> None:
    package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    portfolio_service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    profile, version = portfolio_service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="session top20 profile",
        config_json={"runtime_profile": {"selection": {"top_k": 20}}},
        created_by="unit_test",
    )
    activation = portfolio_service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="session replay runtime profile",
    )
    service = PaperTradingSessionService(repository=paper_repo, package_repository=package_repo)
    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        created_by="unit_test",
    )

    assert session.status == PaperSessionStatus.CREATED
    fake_replay = FakeReplayService()
    progress = PaperTradingSessionRunner(
        repository=paper_repo,
        replay_service=fake_replay,  # type: ignore[arg-type]
    ).tick(session.session_id)

    assert progress.session.status == PaperSessionStatus.SUCCEEDED
    assert progress.day_count == 2
    assert [day.actual_bar_count for day in paper_repo.list_session_days(session.session_id)] == [240, 241]
    assert fake_replay.calls[0]["rerun_policy"] == "reject_existing"
    assert session.runtime_config["runtime_profile_activation"]["activation_id"] == activation.activation_id
    assert session.runtime_config["runtime_profile_activation"]["config_sha256"] == version.config_sha256
    assert fake_replay.calls[0]["runtime_config"]["runtime_profile"]["selection"]["top_k"] == 20
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert event_types == ["SESSION_CREATED", "SESSION_REPLAY_STARTED", "SESSION_REPLAY_SUCCEEDED"]


def test_manual_tick_only_session_starts_paused_and_runs_only_when_allowed() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        runtime_config={"paper_v2_session": {"manual_tick_only": True}},
        created_by="unit_test",
    )

    assert session.status == PaperSessionStatus.PAUSED
    scheduler_result = PaperTradingV2SessionScheduler(repository=paper_repo).run_once(limit=10)
    assert scheduler_result["session_count"] == 0

    fake_replay = FakeReplayService()
    skipped = PaperTradingSessionRunner(
        repository=paper_repo,
        replay_service=fake_replay,  # type: ignore[arg-type]
    ).tick(session.session_id)
    assert skipped.session.status == PaperSessionStatus.PAUSED
    assert fake_replay.calls == []

    progress = PaperTradingSessionRunner(
        repository=paper_repo,
        replay_service=fake_replay,  # type: ignore[arg-type]
    ).tick(session.session_id, allow_paused=True)

    assert progress.session.status == PaperSessionStatus.SUCCEEDED
    assert fake_replay.calls[0]["runtime_config"]["paper_v2_session"]["manual_tick_only"] is True
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert "SESSION_TICK_SKIPPED" in event_types
    assert "SESSION_MANUAL_TICK_STARTED" in event_types


def test_session_rejects_historical_tdx_source_instead_of_fallback() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)

    with pytest.raises(SessionSourceUnsupportedError, match="historical"):
        PaperTradingSessionService(repository=paper_repo).create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.REPLAY_ONLY,
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 3),
            historical_data_source=MinuteDataSource.TDX_REALTIME,
        )


def test_session_rejects_raw_execution_override() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)

    with pytest.raises(SessionConfigError, match="backtest-validated policy"):
        PaperTradingSessionService(repository=paper_repo).create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.REPLAY_ONLY,
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 3),
            historical_data_source=MinuteDataSource.DB_HISTORICAL,
            runtime_config={"algo_code": "TWAP"},
        )


def test_session_rejects_second_active_session() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(repository=paper_repo)
    service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
    )

    with pytest.raises(SessionAlreadyRunningError, match="active trade session"):
        service.create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.REPLAY_ONLY,
            start_date=date(2024, 1, 4),
            end_date=date(2024, 1, 5),
            historical_data_source=MinuteDataSource.DB_HISTORICAL,
        )


def test_live_session_create_and_tick_uses_incremental_executor() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)

    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    fake_live = FakeLiveExecutor(paper_repo)
    progress = PaperTradingSessionRunner(repository=paper_repo, live_executor=fake_live).tick(session.session_id)

    assert progress.session.status == PaperSessionStatus.LIVE_WAITING_FOR_BAR
    assert fake_live.calls[0]["mode"] == "LIVE_ONLY"


def test_live_terminal_failure_marks_current_run_failed() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )

    with pytest.raises(DataUnavailableError, match="live data integrity failure"):
        PaperTradingSessionRunner(
            repository=paper_repo,
            live_executor=FailingLiveExecutor(paper_repo, trade_date=date(2024, 1, 2)),  # type: ignore[arg-type]
        ).tick(session.session_id)

    failed_session = paper_repo.get_session(session.session_id)
    failed_run = paper_repo.get_run("run_live_terminal_failure")
    assert failed_session.status == PaperSessionStatus.FAILED
    assert failed_run.status == RunStatus.FAILED
    assert failed_run.completed_at is not None
    assert failed_run.error is not None
    assert failed_run.error["error_code"] == "DATA_UNAVAILABLE"
    run_errors = [item for item in paper_repo.list_errors(portfolio.portfolio_id) if item.get("run_id") == failed_run.run_id]
    assert run_errors and run_errors[0]["error"]["context"]["symbol"] == "000001.SZ"
    run_events = paper_repo.list_run_events(portfolio.portfolio_id, run_id=failed_run.run_id)
    assert run_events[-1]["event_type"] == "RUN_FAILED"


def test_catchup_session_can_be_created_with_explicit_sources() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)

    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.CATCHUP_THEN_LIVE,
        start_date=date(2024, 1, 2),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
    )

    assert session.mode == PaperSessionMode.CATCHUP_THEN_LIVE
    assert session.historical_data_source == MinuteDataSource.DB_HISTORICAL
    assert session.live_data_source == MinuteDataSource.TDX_REALTIME


def test_replay_auto_switch_normalizes_to_catchup_session() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)

    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
        auto_switch_to_live=True,
    )

    assert session.mode == PaperSessionMode.CATCHUP_THEN_LIVE
    assert session.runtime_config["paper_v2_session"]["auto_switch_to_live"] is True


def test_session_mutation_guard_allows_intraday_recovery_when_enabled() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=FakeTradingCalendar(trading_day=True),
        enforce_non_trading_window=True,
    )

    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        as_of_time=datetime(2024, 1, 2, 10, 0),
    )
    assert session.status == PaperSessionStatus.CREATED


def test_session_mutation_guard_does_not_query_weekday_fallback_calendar() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=ExplodingTradingCalendar(),
        enforce_non_trading_window=True,
    )

    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        as_of_time=datetime(2024, 1, 2, 10, 0),
    )
    assert session.status == PaperSessionStatus.CREATED


def test_session_mutation_guard_allows_minqmt_and_future_live_intraday_actions() -> None:
    service = PaperTradingSessionService(
        repository=InMemoryPaperTradingV2Repository(),
        calendar_provider=ExplodingTradingCalendar(),
        enforce_non_trading_window=True,
    )

    for action in [
        "create_minqmt_sim_portfolio",
        "resume_minqmt_sim_session",
        "create_live_approval_candidate",
        "resume_future_live_session",
    ]:
        service.require_non_trading_operation_window(
            action=action,
            portfolio_id="paper_intraday_recovery",
            session_id="psess_intraday_recovery",
            as_of_time=datetime(2024, 1, 2, 13, 5),
        )


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


def test_local_sim_live_session_rejects_minqmt_source() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)

    with pytest.raises(SessionSourceUnsupportedError, match="broker-bound live source") as exc_info:
        PaperTradingSessionService(repository=paper_repo).create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.LIVE_ONLY,
            start_date=date(2024, 1, 2),
            live_data_source=MinuteDataSource.MINIQMT_REALTIME,
            runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
        )

    assert exc_info.value.context["broker_backend"] == "local_sim"
    assert exc_info.value.context["live_data_source"] == "MINIQMT_REALTIME"


def test_switch_session_mode_stops_source_and_creates_target_after_close() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=FakeTradingCalendar(trading_day=True),
        enforce_non_trading_window=True,
    )
    source = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        as_of_time=datetime(2024, 1, 2, 16, 0),
    )

    target = service.switch_session_mode(
        session_id=source.session_id,
        target_mode=PaperSessionMode.CATCHUP_THEN_LIVE,
        start_date=date(2024, 1, 4),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
        as_of_time=datetime(2024, 1, 2, 16, 5),
        created_by="unit_test",
    )

    assert paper_repo.get_session(source.session_id).status == PaperSessionStatus.STOPPED
    assert target.mode == PaperSessionMode.CATCHUP_THEN_LIVE
    event_types = [event["event_type"] for event in paper_repo.list_session_events(target.session_id)]
    assert "SESSION_MODE_SWITCH_CREATED_TARGET" in event_types


def test_switch_session_mode_rejects_running_run_without_stopping_source() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=FakeTradingCalendar(trading_day=True),
        enforce_non_trading_window=True,
    )
    source = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.CATCHUP_THEN_LIVE,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        live_data_source=MinuteDataSource.TDX_REALTIME,
        as_of_time=datetime(2024, 1, 2, 16, 0),
    )
    paper_repo.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
    paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 4),
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.TDX_REALTIME,
        )
    )

    with pytest.raises(InvalidStateTransitionError, match="still RUNNING"):
        service.switch_session_mode(
            session_id=source.session_id,
            target_mode=PaperSessionMode.LIVE_ONLY,
            start_date=date(2024, 1, 4),
            live_data_source=MinuteDataSource.TDX_REALTIME,
            runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
            as_of_time=datetime(2024, 1, 2, 16, 5),
        )

    assert paper_repo.get_session(source.session_id).status == PaperSessionStatus.CREATED


def test_stop_session_resets_running_portfolio_when_no_run_is_active() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    service = PaperTradingSessionService(
        repository=paper_repo,
        calendar_provider=FakeTradingCalendar(trading_day=False),
        enforce_non_trading_window=True,
    )
    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
        as_of_time=datetime(2024, 1, 2, 16, 0),
    )
    paper_repo.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)

    stopped = service.stop(session.session_id)

    assert stopped.status == PaperSessionStatus.STOPPED
    assert paper_repo.get_portfolio(portfolio.portfolio_id).status == PortfolioStatus.READY


def test_session_capabilities_expose_only_real_startable_modes() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)

    capabilities = PaperTradingSessionService(repository=paper_repo).session_capabilities(portfolio.portfolio_id)

    assert capabilities["modes"]["REPLAY_ONLY"]["can_start"] is True
    assert capabilities["modes"]["LIVE_ONLY"]["can_start"] is True
    assert capabilities["modes"]["CATCHUP_THEN_LIVE"]["can_start"] is True


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


def test_v2_scheduler_ticks_created_sessions_without_fake_success() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    fake_live = FakeLiveExecutor(paper_repo)
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=fake_live),
    )

    result = scheduler.run_once(limit=10)

    assert result["errors"] == []
    assert result["processed"][0]["session_id"] == session.session_id
    assert result["processed"][0]["status"] == PaperSessionStatus.LIVE_WAITING_FOR_BAR.value
    assert fake_live.calls[0]["session_id"] == session.session_id


def test_v2_scheduler_status_reports_in_progress_tick_metadata() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )

    class InspectingLiveExecutor:
        def __init__(self) -> None:
            self.scheduler: PaperTradingV2SessionScheduler | None = None

        def tick(self, session, *, as_of_time=None):
            status = self.scheduler.status()  # type: ignore[union-attr]
            assert status["last_run_at"] is not None
            assert status["last_result"]["in_progress"] is True
            assert status["last_result"]["session_count"] == 1
            assert status["last_result"]["processed"] == []
            updated = paper_repo.update_session_status(
                session.session_id,
                status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
            )
            return PaperSessionProgress(session=updated, day_count=0, events=[])

    fake_live = InspectingLiveExecutor()
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=fake_live),
    )
    fake_live.scheduler = scheduler

    result = scheduler.run_once(limit=10)

    assert result["processed"][0]["session_id"] == session.session_id
    assert result["in_progress"] is False
    assert scheduler.status()["last_result"]["in_progress"] is False


class BlockingLiveExecutor:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def tick(self, session, *, as_of_time=None):
        self.calls.append({"session_id": session.session_id, "as_of_time": as_of_time})
        while True:
            time.sleep(0.05)


class SlowCompletingLiveExecutor:
    def __init__(self, repo: InMemoryPaperTradingV2Repository) -> None:
        self.repo = repo
        self.calls: list[dict] = []

    def tick(self, session, *, as_of_time=None):
        self.calls.append({"session_id": session.session_id, "as_of_time": as_of_time})
        time.sleep(0.15)
        self.repo.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
        updated = self.repo.update_session_status(
            session.session_id,
            status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
        )
        return PaperSessionProgress(session=updated, day_count=0, events=[])


class AliveThreadProbe:
    def is_alive(self) -> bool:
        return True


def test_v2_scheduler_session_tick_timeout_fails_session_and_releases_scheduler_guard(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    blocking_live = BlockingLiveExecutor()
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=blocking_live),
    )

    result = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 30, tzinfo=UTC))

    assert result["in_progress"] is False
    assert result["completed_at"] is not None
    assert result["processed"] == []
    assert result["errors"][0]["error_code"] == "PAPER_V2_SESSION_TICK_TIMEOUT"
    assert result["errors"][0]["context"]["session_id"] == session.session_id
    assert result["errors"][0]["context"]["policy"] == "mark_session_failed_release_scheduler_guard"
    assert result["errors"][0]["context"]["scheduler_guard_released"] is True
    status = scheduler.status()
    assert status["last_result"]["in_progress"] is False
    assert status["active_session_ticks"] == []
    assert status["abandoned_session_ticks"][0]["session_id"] == session.session_id
    updated = paper_repo.get_session(session.session_id)
    assert updated.status == PaperSessionStatus.FAILED
    assert updated.last_error["error_code"] == "PAPER_V2_SESSION_TICK_TIMEOUT"
    assert paper_repo.get_portfolio(session.portfolio_id).status == PortfolioStatus.FAILED
    assert any(item["event_type"] == "SESSION_TICK_TIMEOUT_FAILED" for item in paper_repo.session_events)


def test_v2_scheduler_does_not_retry_failed_timeout_session_while_worker_is_abandoned(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    blocking_live = BlockingLiveExecutor()
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=blocking_live),
    )

    scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 30, tzinfo=UTC))
    result = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 31, tzinfo=UTC))

    assert len(blocking_live.calls) == 1
    assert result["in_progress"] is False
    assert result["session_count"] == 0
    assert result["errors"] == []
    assert scheduler.status()["active_session_ticks"] == []
    assert scheduler.status()["abandoned_session_ticks"][0]["session_id"] == session.session_id


def test_v2_scheduler_converts_stale_active_tick_to_failed_session(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    scheduler = PaperTradingV2SessionScheduler(repository=paper_repo)
    scheduler._active_session_ticks[session.session_id] = {
        "thread": AliveThreadProbe(),
        "started_at": datetime(2024, 1, 2, 9, 30, tzinfo=UTC),
        "portfolio_id": session.portfolio_id,
        "status_before": session.status.value,
        "phase_before": session.phase.value,
    }

    result = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 31, tzinfo=UTC))

    assert result["errors"][0]["error_code"] == "PAPER_V2_SESSION_TICK_TIMEOUT"
    assert result["errors"][0]["context"]["policy"] == "mark_session_failed_release_scheduler_guard"
    assert paper_repo.get_session(session.session_id).status == PaperSessionStatus.FAILED
    assert paper_repo.get_portfolio(session.portfolio_id).status == PortfolioStatus.FAILED
    assert scheduler.status()["active_session_ticks"] == []
    assert scheduler.status()["abandoned_session_ticks"][0]["session_id"] == session.session_id


def test_v2_scheduler_reapplies_failed_state_when_abandoned_worker_completes(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    slow_live = SlowCompletingLiveExecutor(paper_repo)
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=slow_live),
    )

    scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 30, tzinfo=UTC))
    time.sleep(0.25)

    updated = paper_repo.get_session(session.session_id)
    assert updated.status == PaperSessionStatus.FAILED
    assert paper_repo.get_portfolio(session.portfolio_id).status == PortfolioStatus.FAILED
    assert any(item["event_type"] == "SESSION_TICK_TIMEOUT_STALE_WORKER_COMPLETED" for item in paper_repo.session_events)


def test_v2_scheduler_auto_run_timeout_keeps_portfolio_recoverable_without_duplicate_session(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo).enable_auto_run(
        portfolio.portfolio_id,
        broker_account_id="paper-local",
        create_session=False,
        updated_by="pytest",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    blocking_live = BlockingLiveExecutor()
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=blocking_live),
    )

    result = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 30, tzinfo=UTC))
    retry = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 31, tzinfo=UTC))

    assert result["errors"][0]["error_code"] == "PAPER_V2_SESSION_TICK_TIMEOUT"
    assert result["errors"][0]["context"]["auto_run_recoverable"] is True
    assert result["errors"][0]["context"]["portfolio_after_timeout_state"] == PortfolioStatus.READY.value
    assert paper_repo.get_session(session.session_id).status == PaperSessionStatus.FAILED
    assert paper_repo.get_portfolio(portfolio.portfolio_id).status == PortfolioStatus.READY
    assert len(blocking_live.calls) == 1
    assert retry["auto_run_recovery"]["portfolio_count"] == 1
    assert retry["auto_run_recovery"]["blocked_portfolio_ids"] == [portfolio.portfolio_id]
    assert retry["auto_run_recovery"]["skipped"][0]["reason"] == "abandoned_session_tick_active"
    assert paper_repo.list_active_sessions(portfolio.portfolio_id) == []


def test_v2_scheduler_auto_run_recovers_next_session_after_stale_worker_completes(monkeypatch) -> None:
    monkeypatch.setenv("PAPER_TRADING_V2_SCHEDULER_SESSION_TIMEOUT_SECONDS", "0.05")
    package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)
    PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo).enable_auto_run(
        portfolio.portfolio_id,
        broker_account_id="paper-local",
        create_session=False,
        updated_by="pytest",
    )
    session = PaperTradingSessionService(repository=paper_repo).create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.LIVE_ONLY,
        start_date=date(2024, 1, 2),
        live_data_source=MinuteDataSource.TDX_REALTIME,
        runtime_config={"paper_v2_session": {"signal_data_source": "DB_HISTORICAL"}},
    )
    slow_live = SlowCompletingLiveExecutor(paper_repo)
    scheduler = PaperTradingV2SessionScheduler(
        repository=paper_repo,
        runner=PaperTradingSessionRunner(repository=paper_repo, live_executor=slow_live),
    )

    scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 30, tzinfo=UTC))
    time.sleep(0.25)
    scheduler.runner = PaperTradingSessionRunner(repository=paper_repo, live_executor=FakeLiveExecutor(paper_repo))
    recovered = scheduler.run_once(limit=10, as_of_time=datetime(2024, 1, 2, 9, 31, tzinfo=UTC))

    active_sessions = paper_repo.list_active_sessions(portfolio.portfolio_id)
    assert paper_repo.get_session(session.session_id).status == PaperSessionStatus.FAILED
    assert paper_repo.get_portfolio(portfolio.portfolio_id).status == PortfolioStatus.READY
    assert recovered["auto_run_recovery"]["recovered"][0]["portfolio_id"] == portfolio.portfolio_id
    assert recovered["auto_run_recovery"]["blocked_portfolio_ids"] == []
    assert active_sessions and active_sessions[0].session_id != session.session_id
    assert any(item["event_type"] == "SESSION_TICK_TIMEOUT_STALE_WORKER_COMPLETED" for item in paper_repo.session_events)



def test_portfolio_pagination_and_bulk_lifecycle_use_runtime_state_only() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    created = [
        service.create_portfolio(
            package_id=manifest.package_id,
            portfolio_name=f"bulk portfolio {idx}",
            initial_cash=100_000 + idx,
            start_date=date(2024, 1, 2),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
        for idx in range(5)
    ]

    page = service.list_portfolios_page(page=2, page_size=2, search="bulk portfolio")

    assert page["pagination"]["total"] == 5
    assert page["pagination"]["total_pages"] == 3
    assert len(page["portfolios"]) == 2

    result = service.bulk_lifecycle([created[0].portfolio_id, created[1].portfolio_id], "pause")

    assert all(item["ok"] for item in result["results"])
    assert paper_repo.get_portfolio(created[0].portfolio_id).status == PortfolioStatus.PAUSED
    assert paper_repo.get_portfolio(created[1].portfolio_id).status == PortfolioStatus.PAUSED


def test_portfolio_bulk_delete_blocks_active_references_per_item() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    deletable = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="delete ok",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    blocked = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="delete blocked",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo.update_portfolio_status(blocked.portfolio_id, PortfolioStatus.RUNNING)

    result = service.bulk_delete_portfolios([deletable.portfolio_id, blocked.portfolio_id])

    assert result["results"][0]["ok"] is True
    assert result["results"][1]["ok"] is False
    assert paper_repo.get_portfolio(blocked.portfolio_id).status == PortfolioStatus.RUNNING
    with pytest.raises(DataUnavailableError):
        paper_repo.get_portfolio(deletable.portfolio_id)
