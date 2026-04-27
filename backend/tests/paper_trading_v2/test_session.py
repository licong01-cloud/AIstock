from __future__ import annotations

from datetime import date

import pytest

from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import (
    PaperReplayDayResult,
    PaperReplayResult,
    PaperSessionMode,
    PaperSessionStatus,
)
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.paper_trading_v2.session import PaperTradingSessionRunner, PaperTradingSessionService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import SessionAlreadyRunningError, SessionConfigError, SessionSourceUnsupportedError, UnsupportedFeatureError
from backend.services.trading_core.models import RunStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


def make_paper_manifest():
    manifest = make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED})
    return freeze_manifest(manifest)


def make_portfolio(*, data_source: MinuteDataSource = MinuteDataSource.DB_HISTORICAL):
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="session test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=data_source,
    )
    return package_repo, paper_repo, portfolio


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
                ),
                PaperReplayDayResult(
                    trade_date=kwargs["end_date"],
                    run_id="run_end",
                    status=RunStatus.SUCCEEDED,
                    nav=100_002,
                    order_count=1,
                    fill_count=1,
                    position_count=1,
                ),
            ],
        )


def test_replay_only_session_create_tick_and_progress() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)
    service = PaperTradingSessionService(repository=paper_repo)
    session = service.create_session(
        portfolio_id=portfolio.portfolio_id,
        mode=PaperSessionMode.REPLAY_ONLY,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        historical_data_source=MinuteDataSource.DB_HISTORICAL,
        runtime_config={"runtime_profile": {"selection": {"top_k": 20}}},
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
    assert fake_replay.calls[0]["rerun_policy"] == "reject_existing"
    assert fake_replay.calls[0]["runtime_config"]["runtime_profile"]["selection"]["top_k"] == 20
    event_types = [event["event_type"] for event in paper_repo.list_session_events(session.session_id)]
    assert event_types == ["SESSION_CREATED", "SESSION_REPLAY_STARTED", "SESSION_REPLAY_SUCCEEDED"]


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


def test_live_session_fails_fast_until_incremental_executor_exists() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.TDX_REALTIME)

    with pytest.raises(UnsupportedFeatureError, match="real-time incremental"):
        PaperTradingSessionService(repository=paper_repo).create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.LIVE_ONLY,
            start_date=date(2024, 1, 2),
            live_data_source=MinuteDataSource.TDX_REALTIME,
        )
    assert paper_repo.sessions == {}


def test_catchup_session_fails_fast_without_source_role_split() -> None:
    _package_repo, paper_repo, portfolio = make_portfolio(data_source=MinuteDataSource.DB_HISTORICAL)

    with pytest.raises(UnsupportedFeatureError, match="CATCHUP_THEN_LIVE"):
        PaperTradingSessionService(repository=paper_repo).create_session(
            portfolio_id=portfolio.portfolio_id,
            mode=PaperSessionMode.CATCHUP_THEN_LIVE,
            start_date=date(2024, 1, 2),
            historical_data_source=MinuteDataSource.DB_HISTORICAL,
            live_data_source=MinuteDataSource.TDX_REALTIME,
        )

