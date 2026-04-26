from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.paper_trading_v2.models import PaperRun
from backend.services.paper_trading_v2.readiness import PaperTradingReadinessService
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.replay import PaperTradingHistoricalReplay
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.models import PackageStatus
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError
from backend.services.trading_core.limit_price_provider import DailyLimitPrice
from backend.services.trading_core.models import AccountSnapshot, RunStatus
from backend.tests.strategy_package.test_manifest_v1 import make_manifest


class FakeCalendar:
    def ensure_trading_day(self, trade_date: date) -> None:
        return None

    def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
        return [start_date, end_date] if start_date != end_date else [start_date]


class NoopRefreshAudit:
    def require_success(self, **_kwargs):
        return None


class RecordingRefreshAudit:
    def __init__(self) -> None:
        self.calls = []

    def require_success(self, **kwargs):
        self.calls.append(kwargs)
        return None


class FakeSuspendLookup:
    def __init__(self, suspended: set[str] | None = None) -> None:
        self.suspended = suspended or set()

    def get_suspended_symbols(self, symbols: list[str], trade_date: date) -> dict[str, dict]:
        return {
            symbol: {"source": "market.suspend_d", "suspend_type": "S", "suspend_timing": None}
            for symbol in symbols
            if symbol in self.suspended
        }


class FakeLimitProvider:
    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        return DailyLimitPrice(
            symbol=symbol,
            trade_date=trade_date,
            pre_close=10.0,
            up_limit=11.0,
            down_limit=9.0,
        )


class FakeSuspendProvider:
    def __init__(self, *, suspended: bool = False) -> None:
        self.suspended = suspended

    def get_suspend_status(self, symbol: str, trade_date: date) -> DailySuspendStatus:
        return DailySuspendStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=self.suspended,
            suspend_type="S" if self.suspended else None,
        )


def make_raw_bars(*, include_suspend_status: bool = True) -> list[dict]:
    start = datetime(2024, 1, 2, 9, 31)
    rows = []
    for i in range(3):
        row = {
            "time": start + timedelta(minutes=i),
            "open": 10.0,
            "high": 10.2,
            "low": 9.9,
            "close": 10.1,
            "volume": 1000,
            "amount": 1_000_000.0,
        }
        if include_suspend_status:
            row["is_suspended"] = False
        rows.append(row)
    return rows


def make_paper_enabled_manifest():
    manifest = make_manifest().model_copy(update={"package_status": PackageStatus.PAPER_ENABLED})
    return freeze_manifest(manifest)


def runtime_with_authoritative_scores(
    manifest,
    *,
    trade_date: date = date(2024, 1, 2),
    data_source: str = "TDX_REALTIME",
    rows: list[dict] | None = None,
) -> StrategyPackageRuntime:
    score_rows = rows or [
        {
            "symbol": "000001.SZ",
            "score": 0.91,
            "rank": 1,
            "target_weight": 0.03,
            "reference_price": 10.0,
        }
    ]
    artifact_repo = InMemorySelectionScoreArtifactRepository()
    artifact_repo.save(
        SelectionScoreArtifact(
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or "",
            trade_date=trade_date,
            data_source=data_source,
            runtime_config_hash=selection_artifact_runtime_hash({}),
            scores_json=score_rows,
            score_count=len(score_rows),
            universe_count=len(score_rows),
            top_score_symbol=score_rows[0]["symbol"],
            metadata={
                "source_type": AUTHORITATIVE_SELECTION_SOURCE_TYPE,
                "authority_scope": AUTHORITATIVE_SELECTION_SCOPE,
                "test_seeded": True,
            },
        )
    )
    return StrategyPackageRuntime(artifact_repository=artifact_repo)


def test_paper_trading_day_runner_persists_full_day_path() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
    )
    refresh_audit = RecordingRefreshAudit()
    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,
        runtime=runtime_with_authoritative_scores(manifest, data_source=MinuteDataSource.TDX_REALTIME.value),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=refresh_audit,
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    assert result.run.status.value == "SUCCEEDED"
    assert portfolio.execution_policy["validated_execution_policy_id"]
    assert result.run.runtime_config["validated_execution_policy"]["policy_sha256"] == portfolio.execution_policy["policy_sha256"]
    assert sum(fill.quantity for fill in result.fills) == 300
    assert len(paper_repo.orders[result.run.run_id]) == 1
    assert len(paper_repo.fills[result.run.run_id]) > 0
    assert paper_repo.cash_entries[result.run.run_id]
    cash_rows = paper_repo.list_cash_ledger(portfolio.portfolio_id)
    assert cash_rows
    assert cash_rows[0]["portfolio_id"] == portfolio.portfolio_id
    assert cash_rows[0]["run_id"] == result.run.run_id
    assert paper_repo.snapshots[result.run.run_id].nav > 0
    assert [call["dataset"] for call in refresh_audit.calls] == ["suspend_d", "stk_limit"]
    report = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).performance_report(portfolio.portfolio_id)
    assert report["snapshot_count"] == 1
    assert report["final_nav"] == paper_repo.snapshots[result.run.run_id].nav
    assert report["annualized_return"] is None
    assert report["sharpe"] is None
    assert report["insufficient_data_reasons"]
    assert paper_repo.list_runs(portfolio.portfolio_id)[0]["run_id"] == result.run.run_id
    event_types = [
        item["event_type"]
        for item in paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
    ]
    assert event_types == [
        "RUN_STARTED",
        "DATA_READY",
        "SIGNAL_GENERATED",
        "TRADABILITY_FILTERED",
        "TARGETS_GENERATED",
        "ORDER_INTENTS_GENERATED",
        "MARKET_DATA_LOADED",
        "ORDER_EXECUTED",
        "RUN_SUCCEEDED",
    ]


def test_paper_trading_day_runner_rejects_raw_execution_policy_override() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper execution override",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )

    with pytest.raises(StrategyPackageValidationError, match="backtest-validated execution policy"):
        PaperTradingDayRunner(
            repository=paper_repo,
            calendar_provider=FakeCalendar(),
            market_data_provider=PaperV2MinuteMarketDataProvider(
                limit_price_provider=FakeLimitProvider(),
                suspend_status_provider=FakeSuspendProvider(),
                tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
            ),
            tradability_filter=TradabilityFilter(FakeSuspendLookup()),
            refresh_audit=NoopRefreshAudit(),
        ).run_day(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            runtime_config={"algo_code": "CLOSE_PRICE"},
        )


def test_paper_trading_day_runner_fails_when_symbol_is_suspended() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(suspended=True),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(include_suspend_status=False),
    )

    with pytest.raises(DataUnavailableError, match="no executable volume"):
        PaperTradingDayRunner(
            repository=paper_repo,
            calendar_provider=FakeCalendar(),
            market_data_provider=provider,
            runtime=runtime_with_authoritative_scores(manifest, data_source=MinuteDataSource.TDX_REALTIME.value),
            tradability_filter=TradabilityFilter(FakeSuspendLookup()),
            refresh_audit=NoopRefreshAudit(),
        ).run_day(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
        )
    assert paper_repo.errors
    errors = paper_repo.list_errors(portfolio.portfolio_id)
    assert errors[0]["error"]["error_code"] == "DATA_UNAVAILABLE"
    failed_events = paper_repo.list_run_events(portfolio.portfolio_id)
    assert failed_events[-1]["event_type"] == "RUN_FAILED"


def test_paper_trading_day_runner_rejects_duplicate_portfolio_trade_date() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper duplicate",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    existing = PaperRun(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        status=RunStatus.SUCCEEDED,
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    paper_repo.create_run(existing)

    with pytest.raises(StrategyPackageValidationError, match="already exists"):
        PaperTradingDayRunner(
            repository=paper_repo,
            calendar_provider=FakeCalendar(),
            market_data_provider=PaperV2MinuteMarketDataProvider(
                limit_price_provider=FakeLimitProvider(),
                suspend_status_provider=FakeSuspendProvider(),
                tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
            ),
            tradability_filter=TradabilityFilter(FakeSuspendLookup()),
            refresh_audit=NoopRefreshAudit(),
        ).run_day(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
        )


def test_paper_execution_policy_activation_is_used_for_trade_date_run() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio_service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = portfolio_service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper policy activation",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    policy_json = manifest.minute_execution_policy.model_dump(mode="json")
    policy_json["algo_code"] = "CLOSE_PRICE"
    policy_json["algo_config"] = {}
    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="close price validated",
        policy_json=policy_json,
        source_backtest_id="bt_close",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )
    activation = portfolio_service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id=policy.policy_id,
        activated_by="unit_test",
        reason="validate activation path",
    )

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=PaperV2MinuteMarketDataProvider(
            limit_price_provider=FakeLimitProvider(),
            suspend_status_provider=FakeSuspendProvider(),
            tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        runtime=runtime_with_authoritative_scores(manifest, data_source=MinuteDataSource.TDX_REALTIME.value),
        refresh_audit=NoopRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    context = result.run.runtime_config["validated_execution_policy"]
    assert context["activation_id"] == activation.activation_id
    assert context["activation_source"] == "trade_date_activation"
    assert context["validated_execution_policy_id"] == policy.policy_id
    assert context["algo_code"] == "CLOSE_PRICE"
    assert paper_repo.list_execution_policy_activations(portfolio.portfolio_id)[0].activation_id == activation.activation_id


def test_paper_execution_policy_activation_rejects_existing_run() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio_service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = portfolio_service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper policy activation reject",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.TDX_REALTIME,
        )
    )

    with pytest.raises(StrategyPackageValidationError, match="after a paper run exists"):
        portfolio_service.activate_execution_policy(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            policy_id=portfolio.execution_policy["validated_execution_policy_id"],
            activated_by="unit_test",
            reason="too late",
        )


def test_paper_execution_policy_activation_replace_requires_explicit_reason() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio_service = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    )
    portfolio = portfolio_service.create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper policy activation replace",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    policy_id = portfolio.execution_policy["validated_execution_policy_id"]
    first = portfolio_service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id=policy_id,
        reason="first choice",
    )

    with pytest.raises(InvalidStateTransitionError, match="already exists"):
        portfolio_service.activate_execution_policy(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            policy_id=policy_id,
        )
    with pytest.raises(StrategyPackageValidationError, match="requires a reason"):
        portfolio_service.activate_execution_policy(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            policy_id=policy_id,
            replace_existing=True,
        )
    second = portfolio_service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id=policy_id,
        replace_existing=True,
        reason="explicit replacement",
    )

    activations = portfolio_service.list_execution_policy_activations(portfolio.portfolio_id)
    assert second.activation_id != first.activation_id
    assert {item.status.value for item in activations} == {"ACTIVE", "SUPERSEDED"}


def test_paper_portfolio_lifecycle_blocks_paused_runs_until_resumed() -> None:
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
        portfolio_name="lifecycle",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )

    paused = service.pause_portfolio(portfolio.portfolio_id)
    assert paused.status.value == "PAUSED"
    with pytest.raises(InvalidStateTransitionError, match="must be READY"):
        PaperTradingDayRunner(
            repository=paper_repo,
            calendar_provider=FakeCalendar(),
            market_data_provider=PaperV2MinuteMarketDataProvider(
                limit_price_provider=FakeLimitProvider(),
                suspend_status_provider=FakeSuspendProvider(),
                tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
            ),
            tradability_filter=TradabilityFilter(FakeSuspendLookup()),
            refresh_audit=NoopRefreshAudit(),
        ).run_day(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
        )

    resumed = service.resume_portfolio(portfolio.portfolio_id)
    assert resumed.status.value == "READY"
    completed = service.complete_portfolio(portfolio.portfolio_id)
    assert completed.status.value == "COMPLETED"
    retired = service.retire_portfolio(portfolio.portfolio_id)
    assert retired.status.value == "RETIRED"


def test_paper_trading_readiness_checks_rebalance_and_market_data() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="readiness test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
    )

    result = PaperTradingReadinessService(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,
        runtime=runtime_with_authoritative_scores(manifest, data_source=MinuteDataSource.TDX_REALTIME.value),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    assert result.order_intent_count == 1
    assert result.checked_symbols == ["000001.SZ"]
    assert {check.check_name for check in result.checks} >= {
        "strategy_package_manifest",
        "trading_calendar",
        "run_date_available",
        "suspend_d_refresh",
        "stk_limit_refresh",
        "selection_runtime",
        "rebalance",
        "minute_market_data",
    }


class FakeReplayDayRunner:
    def __init__(self) -> None:
        self.calls = []

    def run_day(self, *, portfolio_id: str, trade_date: date, runtime_config: dict):
        from backend.services.paper_trading_v2.models import PaperDayRunResult, PaperRun

        self.calls.append((portfolio_id, trade_date, runtime_config))
        run = PaperRun(
            run_id=f"run_{trade_date:%Y%m%d}",
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
        return PaperDayRunResult(
            portfolio=runtime_config["_portfolio"],
            run=run,
            orders=[],
            fills=[],
            events=[],
            positions=[],
            account_snapshot=AccountSnapshot(
                portfolio_id=portfolio_id,
                cash=100_000,
                market_value=0,
                nav=100_000,
                snapshot_time=datetime(2024, 1, 2, 15, 0),
            ),
        )


def test_historical_replay_runs_paper_day_runner_over_trading_days() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="replay test",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    fake_day_runner = FakeReplayDayRunner()
    replay = PaperTradingHistoricalReplay(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        day_runner=fake_day_runner,
    )

    result = replay.run(
        portfolio_id=portfolio.portfolio_id,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        runtime_config={"_portfolio": portfolio},
    )

    assert result.data_source == MinuteDataSource.DB_HISTORICAL
    assert [item.trade_date for item in result.day_results] == [date(2024, 1, 2), date(2024, 1, 3)]
    assert len(fake_day_runner.calls) == 2


def test_historical_replay_rejects_existing_runs_before_partial_replay() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="replay duplicate",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 3),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    fake_day_runner = FakeReplayDayRunner()
    replay = PaperTradingHistoricalReplay(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        day_runner=fake_day_runner,
    )

    with pytest.raises(StrategyPackageValidationError, match="already has paper v2 runs"):
        replay.run(
            portfolio_id=portfolio.portfolio_id,
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 3),
            runtime_config={"_portfolio": portfolio},
        )
    assert fake_day_runner.calls == []


def test_historical_replay_reset_requires_explicit_confirmation() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="replay reset confirm",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    fake_day_runner = FakeReplayDayRunner()
    replay = PaperTradingHistoricalReplay(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        day_runner=fake_day_runner,
    )

    with pytest.raises(StrategyPackageValidationError, match="explicit confirmation"):
        replay.run(
            portfolio_id=portfolio.portfolio_id,
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 2),
            runtime_config={"_portfolio": portfolio},
            rerun_policy="reset_portfolio",
            confirm_reset=True,
            confirm_text="wrong",
        )
    assert fake_day_runner.calls == []


def test_historical_replay_reset_deletes_existing_runs_before_replay() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="replay reset",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    existing = PaperRun(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
        status=RunStatus.SUCCEEDED,
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    paper_repo.create_run(existing)
    paper_repo.save_run_event(run_id=existing.run_id, event_type="RUN_SUCCEEDED", message="old run")
    fake_day_runner = FakeReplayDayRunner()
    replay = PaperTradingHistoricalReplay(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        day_runner=fake_day_runner,
    )

    result = replay.run(
        portfolio_id=portfolio.portfolio_id,
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        runtime_config={"_portfolio": portfolio},
        rerun_policy="reset_portfolio",
        confirm_reset=True,
        confirm_text=portfolio.portfolio_id,
    )

    assert result.reset_audit is not None
    assert result.reset_audit["deleted_counts"]["run"] == 1
    assert result.reset_audit["deleted_counts"]["run_events"] == 1
    assert len(fake_day_runner.calls) == 2
    assert paper_repo.get_run_by_portfolio_date(portfolio.portfolio_id, date(2024, 1, 3)) is None
