from __future__ import annotations

from datetime import date

import pytest

from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider
from backend.services.paper_trading_v2.models import PaperRun
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.trading_core.errors import InvalidStateTransitionError, StrategyPackageValidationError
from backend.services.trading_core.models import RunStatus

from backend.tests.paper_trading_v2.test_day_runner import (
    FakeCalendar,
    FakeLimitProvider,
    FakeSuspendLookup,
    FakeSuspendProvider,
    NoopRefreshAudit,
    make_paper_enabled_manifest,
    make_raw_bars,
    runtime_with_authoritative_scores,
)


def _portfolio_fixture():
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="runtime profile paper",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    return package_repo, paper_repo, service, manifest, portfolio


def test_runtime_profile_version_hash_and_audit_are_persisted() -> None:
    _package_repo, paper_repo, service, _manifest, portfolio = _portfolio_fixture()

    profile, version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="开盘前 Top2 配置",
        config_json={"top_k": 2, "exclude_suspended": True},
        created_by="unit_test",
        reason="baseline runtime profile",
    )

    assert profile.current_version_id == version.profile_version_id
    assert version.config_sha256
    assert version.config_json == {
        "runtime_profile": {
            "industry_blacklist": [],
            "hmm": {"enabled": False, "model_snapshot_id": None, "signal_preset": None, "coefficients_path": None},
            "tradability": {"exclude_suspended": True},
            "selection": {"top_k": 2},
            "risk_policy": {
                "enabled": False,
                "policy_version": "stock_event_risk_policy_v1",
                "providers": ["st_pit"],
                "st_universe_key": "shsz_st_pit_active_v1",
                "hard_actions": ["block_buy", "force_exit"],
                "visible_time_mode": "next_trading_session",
                "strict_data_ready": True,
                "score_overlay": {
                    "enabled": False,
                    "negative_multiplier_floor": 0.7,
                    "positive_multiplier_cap": 1.1,
                },
            },
        }
    }
    audit = paper_repo.list_config_change_audit(portfolio.portfolio_id)
    assert audit[0].object_type == "runtime_profile"
    assert audit[0].after_sha256 == version.config_sha256


def test_runtime_profile_rejects_unknown_or_execution_keys() -> None:
    _package_repo, _paper_repo, service, _manifest, portfolio = _portfolio_fixture()

    with pytest.raises(StrategyPackageValidationError, match="unsupported top-level keys"):
        service.create_runtime_profile(
            portfolio_id=portfolio.portfolio_id,
            profile_name="bad unknown",
            config_json={"runtime_profile": {}, "default_price": 10.0},
        )
    with pytest.raises(StrategyPackageValidationError, match="execution/session overrides"):
        service.create_runtime_profile(
            portfolio_id=portfolio.portfolio_id,
            profile_name="bad algo override",
            config_json={"algo_code": "TWAP"},
        )


def test_runtime_profile_activation_is_copied_into_day_run() -> None:
    _package_repo, paper_repo, service, manifest, portfolio = _portfolio_fixture()
    profile, version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="Top2 active",
        config_json={"runtime_profile": {"selection": {"top_k": 2}, "tradability": {"exclude_suspended": False}}},
        created_by="unit_test",
    )
    activation = service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="run top2",
    )

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=PaperV2MinuteMarketDataProvider(
            limit_price_provider=FakeLimitProvider(),
            suspend_status_provider=FakeSuspendProvider(),
            tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
        ),
        runtime=runtime_with_authoritative_scores(
            manifest,
            data_source=MinuteDataSource.TDX_REALTIME.value,
            rows=[
                {"symbol": "000001.SZ", "score": 0.91, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
                {"symbol": "000002.SZ", "score": 0.89, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
            ],
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    runtime_config = result.run.runtime_config
    assert runtime_config["runtime_profile_activation"]["activation_id"] == activation.activation_id
    assert runtime_config["runtime_profile_activation"]["profile_id"] == profile.profile_id
    assert runtime_config["runtime_profile_activation"]["config_sha256"] == version.config_sha256
    assert runtime_config["runtime_profile"]["selection"]["top_k"] == 2
    assert len(result.orders) == 2


def test_runtime_profile_activation_replace_and_late_change_are_rejected() -> None:
    _package_repo, paper_repo, service, _manifest, portfolio = _portfolio_fixture()
    _profile, version = service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="replace test",
        config_json={"runtime_profile": {"selection": {"top_k": 2}}},
    )
    first = service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        profile_version_id=version.profile_version_id,
        reason="first",
    )

    with pytest.raises(InvalidStateTransitionError, match="already exists"):
        service.activate_runtime_config(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            profile_version_id=version.profile_version_id,
        )
    second = service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        profile_version_id=version.profile_version_id,
        replace_existing=True,
        reason="replace with audit",
    )
    assert second.activation_id != first.activation_id
    assert {item.status.value for item in service.list_runtime_config_activations(portfolio.portfolio_id)} == {"ACTIVE", "SUPERSEDED"}

    paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 3),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.TDX_REALTIME,
        )
    )
    with pytest.raises(StrategyPackageValidationError, match="after a paper run exists"):
        service.activate_runtime_config(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 3),
            profile_version_id=version.profile_version_id,
            replace_existing=True,
            reason="too late",
        )
