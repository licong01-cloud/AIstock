from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pytest

from backend.services.paper_trading_v2.auto_run import (
    AutoRunCoordinator,
    compute_auto_run_config_sha256,
    normalize_auto_run_config,
)
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.models import PaperSessionMode, PaperSessionStatus
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.scheduler import PaperTradingV2SessionScheduler
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.tests.paper_trading_v2.test_day_runner import (
    make_paper_enabled_manifest,
    save_manifest_with_default_execution_policy,
)


def _seed_service() -> tuple[PaperTradingV2PortfolioService, InMemoryPaperTradingV2Repository, str]:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    return service, paper_repo, manifest.package_id


def _create_local_sim_portfolio(
    service: PaperTradingV2PortfolioService,
    package_id: str,
    *,
    start_date: date = date(2024, 1, 2),
):
    return service.create_portfolio(
        package_id=package_id,
        portfolio_name="local auto-run",
        initial_cash=100_000,
        start_date=start_date,
        data_source=MinuteDataSource.TDX_REALTIME,
        broker_backend="local_sim",
    )


def test_auto_run_config_hash_is_stable_and_uses_platform_defaults() -> None:
    config = normalize_auto_run_config(
        {
            "runtime_profile": {"selection": {"top_k": 8}},
            "broker": {"account_id": "acct-a"},
        },
        package_id="pkg_a",
        broker_account_id="acct-a",
    )
    same_config = normalize_auto_run_config(
        {
            "broker": {"account_id": "acct-a"},
            "runtime_profile": {"selection": {"top_k": 8}},
        },
        package_id="pkg_a",
        broker_account_id="acct-a",
    )

    assert compute_auto_run_config_sha256(config) == compute_auto_run_config_sha256(same_config)
    assert config["selection_artifact_config"]["signal_data_source"] == MinuteDataSource.DB_HISTORICAL.value
    assert config["broker"]["live_data_source"] == MinuteDataSource.MINIQMT_REALTIME.value
    assert config["runtime_profile"]["hmm"]["auto_compute"] is True
    assert config["runtime_profile"]["hmm"]["manual_snapshot_required"] is False

    local_config = normalize_auto_run_config(
        {"broker": {"account_id": "paper-local"}},
        package_id="pkg_a",
        broker_backend="local_sim",
    )
    assert local_config["broker"]["broker_backend"] == "local_sim"
    assert local_config["broker"]["live_data_source"] == MinuteDataSource.TDX_REALTIME.value
    assert local_config["broker"]["account_binding_mode"] == "virtual_portfolio"
    assert local_config["broker"]["authority_source"] == "LOCAL_SIM_LEDGER"


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


def test_enable_auto_run_supports_existing_local_sim_portfolio_with_tdx_session() -> None:
    service, paper_repo, package_id = _seed_service()
    portfolio = _create_local_sim_portfolio(service, package_id)

    result = service.enable_auto_run(
        portfolio.portfolio_id,
        broker_account_id="paper-local",
        updated_by="pytest",
    )

    session = result["session"]
    binding = result["binding"]
    assert result["portfolio"].auto_run_enabled is True
    assert binding.broker_backend == "local_sim"
    assert binding.broker_account_id == "paper-local"
    assert binding.allocation_mode == "virtual_portfolio"
    assert result["auto_run"]["config"]["broker"]["broker_backend"] == "local_sim"
    assert result["auto_run"]["config"]["broker"]["live_data_source"] == MinuteDataSource.TDX_REALTIME.value
    assert session.mode == PaperSessionMode.LIVE_ONLY
    assert session.live_data_source == MinuteDataSource.TDX_REALTIME
    assert paper_repo.list_active_sessions(portfolio.portfolio_id)[0].session_id == session.session_id


def test_minqmt_auto_run_rejects_second_active_binding_without_strategy_gate() -> None:
    service, paper_repo, package_id = _seed_service()
    first = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        service.create_minqmt_auto_run_portfolio(
            package_id=package_id,
            portfolio_name="auto-run two",
            initial_cash=100_000,
            start_date=date(2024, 1, 2),
            broker_account_id="acct-a",
            create_session=False,
        )

    assert exc_info.value.error_code == "INVALID_STATE_TRANSITION"
    assert exc_info.value.context["existing_portfolio_id"] == first["portfolio"].portfolio_id
    assert len(paper_repo.portfolios) == 1


def test_auto_run_recovery_creates_missing_live_session_without_ticking_orders() -> None:
    service, paper_repo, package_id = _seed_service()
    result = service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )
    portfolio = result["portfolio"]
    assert paper_repo.list_active_sessions(portfolio.portfolio_id) == []

    recovery = AutoRunCoordinator(repository=paper_repo).recover_enabled_portfolios(
        as_of_time=datetime(2024, 1, 2, 8, 0),
    )

    sessions = paper_repo.list_active_sessions(portfolio.portfolio_id)
    assert recovery["recovered"][0]["portfolio_id"] == portfolio.portfolio_id
    assert sessions[0].mode == PaperSessionMode.LIVE_ONLY
    assert sessions[0].live_data_source == MinuteDataSource.MINIQMT_REALTIME
    assert paper_repo.runs == {}


def test_auto_run_recovery_creates_local_sim_live_session_with_tdx_source() -> None:
    service, paper_repo, package_id = _seed_service()
    portfolio = _create_local_sim_portfolio(service, package_id, start_date=date(2024, 1, 1))
    service.enable_auto_run(
        portfolio.portfolio_id,
        broker_account_id="paper-local",
        create_session=False,
        updated_by="pytest",
    )
    assert paper_repo.list_active_sessions(portfolio.portfolio_id) == []

    recovery = AutoRunCoordinator(repository=paper_repo).recover_enabled_portfolios(
        as_of_time=datetime(2024, 1, 2, 8, 0),
    )

    sessions = paper_repo.list_active_sessions(portfolio.portfolio_id)
    assert recovery["recovered"][0]["portfolio_id"] == portfolio.portfolio_id
    assert sessions[0].mode == PaperSessionMode.LIVE_ONLY
    assert sessions[0].start_date == date(2024, 1, 2)
    assert sessions[0].live_data_source == MinuteDataSource.TDX_REALTIME
    assert sessions[0].runtime_config["broker"]["broker_backend"] == "local_sim"
    assert sessions[0].runtime_config["broker"]["live_data_source"] == MinuteDataSource.TDX_REALTIME.value
    assert paper_repo.runs == {}


def test_local_sim_auto_run_rejects_minqmt_live_source_override() -> None:
    service, _paper_repo, package_id = _seed_service()
    portfolio = _create_local_sim_portfolio(service, package_id)

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        service.enable_auto_run(
            portfolio.portfolio_id,
            broker_account_id="paper-local",
            config={"broker": {"live_data_source": MinuteDataSource.MINIQMT_REALTIME.value}},
            create_session=False,
        )

    assert exc_info.value.error_code == "RUNTIME_CONFIG_INVALID"
    assert exc_info.value.context["broker_backend"] == "local_sim"
    assert exc_info.value.context["expected_live_data_source"] == MinuteDataSource.TDX_REALTIME.value


def test_auto_run_recovery_respects_env_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    service, paper_repo, package_id = _seed_service()
    service.create_minqmt_auto_run_portfolio(
        package_id=package_id,
        portfolio_name="auto-run one",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        broker_account_id="acct-a",
        create_session=False,
    )
    monkeypatch.setenv("PAPER_V2_AUTO_RUN_ENABLED", "false")

    recovery = AutoRunCoordinator(repository=paper_repo).recover_enabled_portfolios()

    assert recovery["enabled"] is False
    assert paper_repo.sessions == {}


class _NoopRunner:
    def tick(self, session_id: str, *, as_of_time=None):
        raise AssertionError(f"scheduler should not tick without a recovered session in this test: {session_id}")


def test_scheduler_status_exposes_auto_run_bootstrap_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENABLE_PAPER_TRADING_V2_SCHEDULER", "true")
    scheduler = PaperTradingV2SessionScheduler(
        repository=InMemoryPaperTradingV2Repository(),
        runner=_NoopRunner(),  # type: ignore[arg-type]
    )

    status = scheduler.bootstrap_status()

    assert status["scheduler_autostart_env"] is True
    assert status["auto_run"]["env_enabled"] is True


def test_auto_run_migration_declares_required_comments() -> None:
    sql = Path("backend/migrations/paper_v2_miniqmt_auto_run_20260527.sql").read_text(encoding="utf-8")
    required_fragments = [
        "COMMENT ON COLUMN paper_v2.portfolio.auto_run_enabled",
        "COMMENT ON COLUMN paper_v2.portfolio.auto_run_config",
        "COMMENT ON COLUMN paper_v2.portfolio.auto_run_config_sha256",
        "COMMENT ON COLUMN paper_v2.portfolio.auto_run_updated_at",
        "COMMENT ON COLUMN paper_v2.portfolio.auto_run_updated_by",
        "COMMENT ON TABLE paper_v2.broker_account_binding",
        "COMMENT ON COLUMN paper_v2.broker_account_binding.broker_account_id",
        "COMMENT ON COLUMN paper_v2.broker_account_binding.allocation_mode",
    ]
    for fragment in required_fragments:
        assert fragment in sql
