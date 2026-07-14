from __future__ import annotations

import inspect
from contextlib import contextmanager
from datetime import date, datetime, timedelta

import pytest

from backend.db import pg_pool
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import (
    DailySuspendStatus,
    MinuteExecutionMarketInput,
    MinuteDataSource,
    PaperV2MinuteMarketDataProvider,
)
from backend.services.paper_trading_v2.models import PaperRun, PaperSessionDay, PaperSessionPhase, PaperSessionStatus
from backend.services.paper_trading_v2.readiness import PaperTradingReadinessService
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository, PaperTradingV2Repository
from backend.services.paper_trading_v2.replay import PaperTradingHistoricalReplay
from backend.services.paper_trading_v2.selection_cutoff import ensure_previous_trading_day_selection_cutoff
from backend.services.paper_trading_v2.service import PaperTradingV2PortfolioService
from backend.services.selection_center.risk_policy import RiskDecision, StockRiskPolicyService
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.live_inference import AUTHORITATIVE_SELECTION_SCOPE, AUTHORITATIVE_SELECTION_SOURCE_TYPE
from backend.services.strategy_package.model_asset_resolver import DEFAULT_MODEL_CACHE_ROOT
from backend.services.strategy_package.models import PackageStatus, PortfolioPolicy
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.runtime import StrategyPackageRuntime
from backend.services.strategy_package.runtime_variant import RuntimeVariantKind, RuntimeVariantValidationStatus
from backend.services.strategy_package.selection_artifact import (
    InMemorySelectionScoreArtifactRepository,
    SelectionScoreArtifact,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.service import StrategyPackageService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, RuntimeConfigInvalidError
from backend.services.trading_core.ledger import CashLedgerEntry
from backend.services.trading_core.limit_price_provider import DailyLimitPrice
from backend.services.trading_core.models import AccountSnapshot, Fill, MinuteBar, Order, OrderEvent, OrderEventType, OrderSide, OrderStatus, OrderType, PositionLot, RunStatus
from backend.tests.strategy_package.test_manifest_v1 import admit_manifest_for_test, make_manifest


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


class FakeRiskPolicyService(StockRiskPolicyService):
    def __init__(self, decisions: dict[str, RiskDecision]) -> None:
        self._decisions = decisions

    def evaluate(self, *, symbols, trade_date, profile, current_positions=None):
        return {symbol: self._decisions.get(symbol, RiskDecision(symbol=symbol)) for symbol in symbols}


class FakeLimitProvider:
    def get_limit_price(self, symbol: str, trade_date: date) -> DailyLimitPrice:
        return DailyLimitPrice(
            symbol=symbol,
            trade_date=trade_date,
            pre_close=10.0,
            up_limit=11.0,
            down_limit=9.0,
        )


class RecordingConnection:
    def __init__(self) -> None:
        self.autocommit = True
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self.executed: list[tuple[str, tuple | None]] = []
        self.fail_on_sql: str | None = None
        self.connection_ids: list[int] = []
        self.fetchone_queue: list[tuple | dict] = []
        self.factory_calls: list[dict[str, bool]] = []

    def cursor(self, *args, **kwargs):
        self.connection_ids.append(id(self))
        return RecordingCursor(self)

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True

    def putconn(self) -> None:
        self.closed = True


class RecordingCursor:
    rowcount = 1

    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params=None) -> None:
        normalized = " ".join(sql.split())
        self.conn.executed.append((normalized, params))
        if self.conn.fail_on_sql and self.conn.fail_on_sql in normalized:
            raise RuntimeError("forced repository write failure")

    def fetchone(self):
        if self.conn.fetchone_queue:
            return self.conn.fetchone_queue.pop(0)
        return (True,)


def recording_conn_factory(conn: RecordingConnection):
    @contextmanager
    def factory(*, autocommit: bool = True, manage_transaction: bool = False):
        conn.factory_calls.append({"autocommit": autocommit, "manage_transaction": manage_transaction})
        original = conn.autocommit
        conn.autocommit = autocommit
        try:
            yield conn
            if not autocommit and manage_transaction:
                conn.commit()
        except Exception:
            if not autocommit and manage_transaction:
                conn.rollback()
            raise
        finally:
            conn.autocommit = original

    return factory


def test_pg_pool_get_conn_defaults_keep_legacy_autocommit_true(monkeypatch: pytest.MonkeyPatch) -> None:
    signature = inspect.signature(pg_pool.get_conn)

    assert signature.parameters["autocommit"].default is True
    assert signature.parameters["manage_transaction"].default is False

    class FakePoolConnection:
        def __init__(self) -> None:
            self.autocommit = False
            self.commits = 0
            self.rollbacks = 0
            self.transaction_status_checks = 0

        def get_transaction_status(self):
            self.transaction_status_checks += 1
            raise AssertionError("default get_conn must not use explicit transaction preparation")

        def commit(self) -> None:
            self.commits += 1

        def rollback(self) -> None:
            self.rollbacks += 1

    class FakePool:
        maxconn = 1
        _used = {}
        _pool = []

        def __init__(self, conn: FakePoolConnection) -> None:
            self.conn = conn
            self.returned = False

        def getconn(self):
            return self.conn

        def putconn(self, conn: FakePoolConnection) -> None:
            assert conn is self.conn
            self.returned = True

    conn = FakePoolConnection()
    pool = FakePool(conn)
    monkeypatch.setattr(pg_pool, "_DB_POOL", pool)
    monkeypatch.setattr(pg_pool, "_apply_statement_timeout", lambda _conn: 60_000)

    with pg_pool.get_conn() as checked_out:
        assert checked_out is conn
        assert checked_out.autocommit is True

    assert pool.returned is True
    assert conn.commits == 0
    assert conn.rollbacks == 0
    assert conn.transaction_status_checks == 0


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
                "stock_name": "瑞德智能",
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


class FakeDbMinuteProvider:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        min_bars: int,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        self.calls.append(
            {
                "symbol": symbol,
                "trade_date": trade_date,
                "source": source,
                "min_bars": min_bars,
                "require_suspend_status": require_suspend_status,
                "require_day_features": require_day_features,
            }
        )
        start = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=31)
        minute_bars = [
            MinuteBar(
                symbol=symbol,
                bar_time=start + timedelta(minutes=i),
                open=10.0 + i * 0.1,
                high=10.2 + i * 0.1,
                low=9.9 + i * 0.1,
                close=10.1 + i * 0.1,
                volume=100_000,
                amount=1_000_000.0,
                limit_up=11.0,
                limit_down=9.0,
            )
            for i in range(max(min_bars, 3))
        ]
        return MinuteExecutionMarketInput(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            minute_bars=minute_bars,
            market_context={
                "stock_id": symbol,
                "trade_date": trade_date.isoformat(),
                "data_source": source.value,
                "prev_close": 10.0,
                "limit_up": 11.0,
                "limit_down": 9.0,
                "suspend_status": {"is_suspended": False},
            },
        )


class FakeDayFeatureExcludingMinuteProvider(FakeDbMinuteProvider):
    def __init__(self, excluded_symbol: str) -> None:
        super().__init__()
        self.excluded_symbol = excluded_symbol

    def load_symbol_input(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        min_bars: int,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        if require_day_features and symbol == self.excluded_symbol:
            self.calls.append(
                {
                    "symbol": symbol,
                    "trade_date": trade_date,
                    "source": source,
                    "min_bars": min_bars,
                    "require_suspend_status": require_suspend_status,
                    "require_day_features": require_day_features,
                    "excluded": True,
                }
            )
            raise DataUnavailableError(
                "V25 day_features turnover_rate_f is missing",
                context={
                    "call": "DbV25DayFeatureProvider._free_float_turnover_rate",
                    "dataset": "market.daily_basic",
                    "field": "turnover_rate_f",
                    "symbol": symbol,
                    "trade_date": trade_date.isoformat(),
                    "reason_code": "V25_DAY_FEATURE_TURNOVER_RATE_F_MISSING",
                    "fail_closed_policy": "exclude_symbol_for_trade_date",
                    "forbidden_fallback": "turnover_rate",
                },
            )
        return super().load_symbol_input(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            min_bars=min_bars,
            require_suspend_status=require_suspend_status,
            require_day_features=require_day_features,
        )


class FakePartialExecutionEngine:
    def execute_order(self, *, order: Order, minute_bars: list[MinuteBar], **_kwargs):
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=100,
            price=minute_bars[0].close,
            trade_time=minute_bars[0].bar_time,
            bar_time=minute_bars[0].bar_time,
            reason="unit partial fill",
        )
        final_order = order.model_copy(
            update={
                "status": OrderStatus.PARTIALLY_FILLED,
                "filled_quantity": 100,
                "avg_fill_price": fill.price,
            }
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=OrderEventType.PARTIALLY_FILLED,
            fill=fill,
            reason=fill.reason,
        )
        return final_order, [fill], [event]


class FakeFullExecutionEngine:
    def execute_order(self, *, order: Order, minute_bars: list[MinuteBar], **_kwargs):
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=minute_bars[0].close,
            trade_time=minute_bars[0].bar_time,
            bar_time=minute_bars[0].bar_time,
            reason="unit full fill",
        )
        final_order = order.model_copy(
            update={
                "status": OrderStatus.FILLED,
                "filled_quantity": order.quantity,
                "avg_fill_price": fill.price,
            }
        )
        event = OrderEvent(
            order_id=order.order_id,
            event_type=OrderEventType.FILLED,
            fill=fill,
            reason=fill.reason,
        )
        return final_order, [fill], [event]


def make_paper_enabled_manifest(
    *,
    topk: int = 50,
    n_drop: int = 5,
    algo_code: str = "TWAP",
    custom_params: dict | None = None,
):
    base = make_manifest(algo_code=algo_code)
    strategy_config = dict(base.strategy_config)
    if custom_params is not None:
        strategy_config["custom_params"] = custom_params
    manifest = base.model_copy(
        update={
            "package_status": PackageStatus.PAPER_ENABLED,
            "portfolio_policy": PortfolioPolicy(topk=topk, n_drop=n_drop),
            "strategy_config": strategy_config,
        }
    )
    return admit_manifest_for_test(manifest)


def save_manifest_with_default_execution_policy(
    package_repo: InMemoryStrategyPackageRepository,
    manifest,
):
    package_repo.save_manifest(manifest)
    return StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="unit_default_manifest_policy",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="unit_default_manifest_policy_backtest",
        source_backtest_status="COMPLETED",
        paper_enabled=True,
    )


def test_update_failed_run_to_succeeded_raises_invalid_state_transition() -> None:
    paper_repo = InMemoryPaperTradingV2Repository()
    failed_run = paper_repo.create_run(
        PaperRun(
            portfolio_id="paper_status_guard",
            trade_date=date(2024, 1, 2),
            status=RunStatus.FAILED,
            data_source=MinuteDataSource.DB_HISTORICAL,
            error={"error_code": "UNIT_FAILED"},
        )
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        paper_repo.update_run_status(failed_run, RunStatus.SUCCEEDED)

    assert exc_info.value.context["reason_code"] == "PAPER_V2_RUN_TERMINAL_STATE_TRANSITION_BLOCKED"
    assert exc_info.value.context["run_id"] == failed_run.run_id
    assert exc_info.value.context["from_status"] == RunStatus.FAILED.value
    assert exc_info.value.context["to_status"] == RunStatus.SUCCEEDED.value
    assert paper_repo.get_run(failed_run.run_id).status == RunStatus.FAILED


def test_day_runner_does_not_mark_succeeded_with_open_order() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(topk=1)
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="open order guard",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    provider = PaperV2MinuteMarketDataProvider(
        limit_price_provider=FakeLimitProvider(),
        suspend_status_provider=FakeSuspendProvider(),
        tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        PaperTradingDayRunner(
            repository=paper_repo,
            calendar_provider=FakeCalendar(),
            market_data_provider=provider,
            execution_engine=FakePartialExecutionEngine(),
            runtime=runtime_with_authoritative_scores(manifest, data_source=MinuteDataSource.TDX_REALTIME.value),
            tradability_filter=TradabilityFilter(FakeSuspendLookup()),
            refresh_audit=RecordingRefreshAudit(),
        ).run_day(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
        )

    assert exc_info.value.context["reason_code"] == "PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS"
    assert exc_info.value.context["open_order_count"] == 1
    assert exc_info.value.context["open_orders"][0]["status"] == OrderStatus.PARTIALLY_FILLED.value
    run = paper_repo.get_run_by_portfolio_date(portfolio.portfolio_id, date(2024, 1, 2))
    assert run is not None
    assert run.status == RunStatus.FAILED


def test_create_portfolio_uses_manifest_minute_policy_as_platform_default() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    package_repo.save_manifest(manifest)

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="platform default policy",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )

    assert portfolio.execution_policy["validated_execution_policy_id"].startswith("platform_manifest_")
    assert portfolio.execution_policy["source_backtest_id"] == f"strategy_package_manifest:{manifest.manifest_sha256}"
    assert portfolio.execution_policy["policy_json"]["algo_code"] == manifest.minute_execution_policy.algo_code
    assert portfolio.execution_policy["policy_json"]["data_requirements"] == manifest.minute_execution_policy.data_requirements.model_dump(mode="json")
    assert not package_repo.list_execution_policies(manifest.package_id)


def test_create_portfolio_accepts_requested_validated_policy_that_differs_from_manifest() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = admit_manifest_for_test(
        make_manifest(algo_code="V24_PLAN").model_copy(update={"package_status": PackageStatus.PAPER_ENABLED})
    )
    save_manifest_with_default_execution_policy(package_repo, manifest)
    policy_json = {
        "execution_level": "minute",
        "bar_freq": "1m",
        "algo_code": "TWAP",
        "algo_config": {"split_count": 3},
        "fallback_algo_code": None,
        "data_requirements": {
            "requires_minute_bar": True,
            "requires_limit_price": True,
            "requires_suspend_status": True,
            "requires_trade_calendar": True,
        },
        "fallback_policy": {"on_missing_minute_bar": "fail", "on_algo_error": "fail"},
        "quality_report": {
            "record_slippage": True,
            "record_participation_rate": True,
            "record_unfilled_reason": True,
        },
    }

    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="requested_twappolicy",
        policy_json=policy_json,
        source_backtest_id="unit_twappolicy_backtest",
        source_backtest_status="BACKTEST_VALIDATED",
        paper_enabled=True,
    )

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="requested policy paper",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={"validated_execution_policy_id": policy.policy_id},
    )

    assert portfolio.execution_policy["validated_execution_policy_id"] == policy.policy_id
    assert portfolio.execution_policy["algo_code"] == "TWAP"
    assert manifest.minute_execution_policy.algo_code == "V24_PLAN"


def test_create_portfolio_derives_platform_policy_from_qe_backtest_execution_context() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    base = make_manifest(algo_code="TWAP")
    backtest_context = {
        "schema_version": "qe_backtest_context_v1",
        "authority": "source_evidence_not_runtime_authority",
        "execution": {
            "backtest_freq": "1min",
            "execution_algo": "V25_1_SMALL_CAP",
            "execution_algo_params": {"device": "cuda", "min_cost": 5, "max_buckets": 12},
        },
    }
    manifest = admit_manifest_for_test(
        base.model_copy(
            update={
                "package_status": PackageStatus.BACKTEST_APPROVED,
                "minute_execution_policy": None,
                "backtest_context": backtest_context,
            }
        )
    )
    package_repo.save_manifest(manifest)

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="derived platform policy",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )

    policy = portfolio.execution_policy
    config = policy["policy_json"]["algo_config"]
    assert policy["validated_execution_policy_id"].startswith("platform_manifest_")
    assert policy["source_backtest_id"] == f"strategy_package_manifest:{manifest.manifest_sha256}"
    assert policy["policy_json"]["algo_code"] == "V25_1_SMALL_CAP"
    assert policy["policy_json"]["bar_freq"] == "1m"
    assert config["device"] == "cuda"
    assert config["min_cost"] == 5
    assert config["early_model_path"] == str(DEFAULT_MODEL_CACHE_ROOT / "V25_1_SMALL_CAP" / "v25_early_net_joint_fixed.pt")
    assert config["late_model_path"] == str(DEFAULT_MODEL_CACHE_ROOT / "V25_1_SMALL_CAP" / "v25_late_net_joint_fixed.pt")
    assert not package_repo.list_execution_policies(manifest.package_id)


def test_create_portfolio_derives_platform_policy_using_model_cache_env(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    monkeypatch.setenv("AISTOCK_MODEL_CACHE_DIR", str(tmp_path / "model_cache" / "execution"))
    base = make_manifest(algo_code="TWAP")
    manifest = admit_manifest_for_test(
        base.model_copy(
            update={
                "package_status": PackageStatus.BACKTEST_APPROVED,
                "minute_execution_policy": None,
                "backtest_context": {
                    "schema_version": "qe_backtest_context_v1",
                    "execution": {
                        "backtest_freq": "1min",
                        "execution_algo": "V25_1_SMALL_CAP",
                        "execution_algo_params": {"device": "cuda"},
                    },
                },
            }
        )
    )
    package_repo.save_manifest(manifest)

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="derived env model cache",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )

    config = portfolio.execution_policy["policy_json"]["algo_config"]
    expected_root = tmp_path / "model_cache" / "execution" / "V25_1_SMALL_CAP"
    assert config["early_model_path"] == str(expected_root / "v25_early_net_joint_fixed.pt")
    assert config["late_model_path"] == str(expected_root / "v25_late_net_joint_fixed.pt")


def test_create_portfolio_rejects_missing_execution_context_without_manifest_policy() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = admit_manifest_for_test(
        make_manifest().model_copy(
            update={
                "package_status": PackageStatus.BACKTEST_APPROVED,
                "minute_execution_policy": None,
                "backtest_context": {},
            }
        )
    )
    package_repo.save_manifest(manifest)

    with pytest.raises(RuntimeConfigInvalidError, match="platform execution policy"):
        PaperTradingV2PortfolioService(
            package_repository=package_repo,
            repository=paper_repo,
        ).create_portfolio(
            package_id=manifest.package_id,
            portfolio_name="missing execution context",
            initial_cash=100_000,
            start_date=date(2024, 1, 2),
            data_source=MinuteDataSource.DB_HISTORICAL,
        )


def test_create_portfolio_accepts_requested_policy_matching_qe_contract() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    policy = StrategyPackageService(repository=package_repo).create_execution_policy(
        package_id=manifest.package_id,
        policy_name="requested_manifest_policy",
        policy_json=manifest.minute_execution_policy.model_dump(mode="json"),
        source_backtest_id="unit_manifest_policy_backtest",
        source_backtest_status="BACKTEST_VALIDATED",
        paper_enabled=True,
    )

    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="requested matching policy paper",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={"validated_execution_policy_id": policy.policy_id},
    )

    assert portfolio.execution_policy["validated_execution_policy_id"] == policy.policy_id
    assert portfolio.execution_policy["algo_code"] == manifest.minute_execution_policy.algo_code


def runtime_with_authoritative_scores(
    manifest,
    *,
    trade_date: date = date(2024, 1, 2),
    data_source: str = "TDX_REALTIME",
    rows: list[dict] | None = None,
    runtime_config: dict | None = None,
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
    for source in {data_source, MinuteDataSource.DB_HISTORICAL.value}:
        artifact_repo.save(
            SelectionScoreArtifact(
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or "",
                trade_date=trade_date,
                data_source=source,
                runtime_config_hash=selection_artifact_runtime_hash(runtime_config or {}),
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    assert sum(fill.quantity for fill in result.fills) == 9500
    assert result.run.runtime_config["qe_backtest_runtime_contract"]["portfolio_strategy"]["strategy_family"] == "score_weighted_topk_v2"
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
    running_summary = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).running_summary(limit=10)
    assert running_summary == []
    running_page = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).running_summary_page(
        page=1,
        page_size=20,
        statuses=["READY"],
        sort_by="initial_cash",
        sort_dir="asc",
        search=portfolio.package_id,
        search_fields=["package_id"],
        min_initial_cash=50_000,
        max_initial_cash=150_000,
    )
    assert running_page["pagination"]["total"] == 1
    assert running_page["pagination"]["page_size"] == 20
    assert running_page["summaries"][0]["portfolio"].portfolio_id == portfolio.portfolio_id
    assert running_page["summaries"][0]["package"]["package_id"] == portfolio.package_id
    assert "manifest_json" not in running_page["summaries"][0]["package"]
    assert running_page["summaries"][0]["latest_run"]["run_id"] == result.run.run_id
    assert running_page["summaries"][0]["latest_snapshot"]["nav"] == paper_repo.snapshots[result.run.run_id].nav
    assert running_page["summaries"][0]["counts"]["orders"] == 1
    assert running_page["summaries"][0]["counts"]["fills"] == len(paper_repo.fills[result.run.run_id])
    assert running_page["summaries"][0]["counts"]["errors"] == 0
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


def test_localsim_cash_ledger_save_is_idempotent_by_fill_id() -> None:
    paper_repo = InMemoryPaperTradingV2Repository()
    run_id = "run_cash_idempotent"
    entry = CashLedgerEntry(
        fill_id="fill_same",
        portfolio_id="paper_cash_idempotent",
        trade_date=date(2024, 1, 2),
        symbol="000001.SZ",
        side=OrderSide.BUY,
        notional=1000.0,
        fee=5.0,
        cash_delta=-1005.0,
        cash_after=98_995.0,
    )

    paper_repo.save_cash_entry(run_id, entry)
    paper_repo.save_cash_entry(run_id, entry)

    assert paper_repo.cash_entries[run_id] == [entry]


def test_pg_cash_ledger_insert_uses_unique_conflict_guard() -> None:
    conn = RecordingConnection()
    repo = PaperTradingV2Repository(
        conn_factory=recording_conn_factory(conn),
        symbol_name_resolver=type("NoopResolver", (), {"resolve": lambda self, symbols: {}})(),
    )
    entry = CashLedgerEntry(
        fill_id="fill_pg_same",
        portfolio_id="paper_cash_pg",
        trade_date=date(2024, 1, 2),
        symbol="000001.SZ",
        side=OrderSide.BUY,
        notional=1000.0,
        fee=5.0,
        cash_delta=-1005.0,
        cash_after=98_995.0,
    )

    repo.save_cash_entry("run_cash_pg", entry)

    assert any("ON CONFLICT(run_id, fill_id) DO NOTHING" in sql for sql, _ in conn.executed)
    assert conn.factory_calls == [{"autocommit": True, "manage_transaction": False}]
    assert conn.commits == 0
    assert conn.rollbacks == 0


def test_pg_non_localsim_positions_keep_autocommit_default() -> None:
    conn = RecordingConnection()
    conn.fetchone_queue.append(("minqmt_sim",))
    repo = PaperTradingV2Repository(
        conn_factory=recording_conn_factory(conn),
        symbol_name_resolver=type("NoopResolver", (), {"resolve": lambda self, symbols: {}})(),
    )

    repo.save_positions(
        run_id="run_positions_minqmt",
        trade_date=date(2024, 1, 2),
        positions=[
            PositionLot(
                portfolio_id="paper_positions_minqmt",
                symbol="000001.SZ",
                quantity=100,
                available_quantity=100,
                avg_cost=10.0,
                trade_date=date(2024, 1, 2),
            )
        ],
        prices={"000001.SZ": 10.0},
    )

    assert all(call == {"autocommit": True, "manage_transaction": False} for call in conn.factory_calls)
    assert conn.commits == 0
    assert conn.rollbacks == 0


def test_pg_localsim_positions_delete_insert_rollback_is_single_transaction() -> None:
    conn = RecordingConnection()
    conn.fail_on_sql = "INSERT INTO paper_v2.positions"
    conn.fetchone_queue.append(("local_sim",))
    repo = PaperTradingV2Repository(
        conn_factory=recording_conn_factory(conn),
        symbol_name_resolver=type("NoopResolver", (), {"resolve": lambda self, symbols: {}})(),
    )

    with pytest.raises(RuntimeError, match="forced repository write failure"):
        repo.save_positions(
            run_id="run_positions_atomic",
            trade_date=date(2024, 1, 2),
            positions=[
                PositionLot(
                    portfolio_id="paper_positions_atomic",
                    symbol="000001.SZ",
                    quantity=100,
                    available_quantity=100,
                    avg_cost=10.0,
                    trade_date=date(2024, 1, 2),
                )
            ],
            prices={"000001.SZ": 10.0},
        )

    assert [sql for sql, _ in conn.executed if "paper_v2.positions" in sql] == [
        "DELETE FROM paper_v2.positions WHERE run_id = %s",
        next(sql for sql, _ in conn.executed if sql.startswith("INSERT INTO paper_v2.positions")),
    ]
    assert conn.factory_calls == [
        {"autocommit": True, "manage_transaction": False},
        {"autocommit": False, "manage_transaction": True},
    ]
    assert conn.commits == 0
    assert conn.rollbacks == 1


def test_session_tick_lock_reuses_one_connection_for_cursor_and_writes() -> None:
    conn = RecordingConnection()
    conn.fetchone_queue.extend([("local_sim",), (True,)])
    repo = PaperTradingV2Repository(
        conn_factory=recording_conn_factory(conn),
        symbol_name_resolver=type("NoopResolver", (), {"resolve": lambda self, symbols: {}})(),
    )

    with repo.session_tick_lock("session_atomic"):
        repo.save_cash_entry(
            "run_session_atomic",
            CashLedgerEntry(
                fill_id="fill_session_atomic",
                portfolio_id="paper_session_atomic",
                trade_date=date(2024, 1, 2),
                symbol="000001.SZ",
                side=OrderSide.BUY,
                notional=1000.0,
                fee=5.0,
                cash_delta=-1005.0,
                cash_after=98_995.0,
            ),
        )
        repo.save_session_day(
            PaperSessionDay(
                session_day_id="session_day_atomic",
                session_id="session_atomic",
                portfolio_id="paper_session_atomic",
                trade_date=date(2024, 1, 2),
                run_id="run_session_atomic",
                status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                data_source=MinuteDataSource.TDX_REALTIME,
                actual_bar_count=1,
                latest_available_bar_time=datetime(2024, 1, 2, 9, 31),
                last_processed_bar_time=datetime(2024, 1, 2, 9, 31),
                created_at=datetime(2024, 1, 2, 9, 31),
                updated_at=datetime(2024, 1, 2, 9, 31),
            )
        )

    assert conn.commits == 1
    assert conn.rollbacks == 0
    assert conn.factory_calls == [
        {"autocommit": True, "manage_transaction": False},
        {"autocommit": True, "manage_transaction": False},
        {"autocommit": False, "manage_transaction": True},
    ]
    assert "SELECT pg_try_advisory_lock(2402, hashtext(%s))" in [sql for sql, _ in conn.executed]
    assert "SELECT pg_advisory_unlock(2402, hashtext(%s))" in [sql for sql, _ in conn.executed]
    assert len(set(conn.connection_ids)) == 1
    assert any("INSERT INTO paper_v2.cash_ledger" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO paper_v2.session_day" in sql for sql, _ in conn.executed)


def test_session_tick_lock_rolls_back_cash_and_cursor_on_failure() -> None:
    conn = RecordingConnection()
    conn.fetchone_queue.extend([("local_sim", "LIVE_ONLY"), (True,)])
    repo = PaperTradingV2Repository(
        conn_factory=recording_conn_factory(conn),
        symbol_name_resolver=type("NoopResolver", (), {"resolve": lambda self, symbols: {}})(),
    )

    with pytest.raises(RuntimeError, match="forced tick failure"):
        with repo.session_tick_lock("session_atomic_rollback"):
            repo.save_cash_entry(
                "run_session_atomic_rollback",
                CashLedgerEntry(
                    fill_id="fill_session_atomic_rollback",
                    portfolio_id="paper_session_atomic_rollback",
                    trade_date=date(2024, 1, 2),
                    symbol="000001.SZ",
                    side=OrderSide.BUY,
                    notional=1000.0,
                    fee=5.0,
                    cash_delta=-1005.0,
                    cash_after=98_995.0,
                ),
            )
            repo.save_session_day(
                PaperSessionDay(
                    session_day_id="session_day_atomic_rollback",
                    session_id="session_atomic_rollback",
                    portfolio_id="paper_session_atomic_rollback",
                    trade_date=date(2024, 1, 2),
                    run_id="run_session_atomic_rollback",
                    status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                    phase=PaperSessionPhase.LIVE_INTRADAY,
                    data_source=MinuteDataSource.TDX_REALTIME,
                    actual_bar_count=1,
                    latest_available_bar_time=datetime(2024, 1, 2, 9, 31),
                    last_processed_bar_time=datetime(2024, 1, 2, 9, 31),
                    created_at=datetime(2024, 1, 2, 9, 31),
                    updated_at=datetime(2024, 1, 2, 9, 31),
                )
            )
            raise RuntimeError("forced tick failure")

    assert conn.commits == 0
    assert conn.rollbacks == 1
    assert "SELECT pg_advisory_unlock(2402, hashtext(%s))" in [sql for sql, _ in conn.executed]
    assert any("INSERT INTO paper_v2.cash_ledger" in sql for sql, _ in conn.executed)
    assert any("INSERT INTO paper_v2.session_day" in sql for sql, _ in conn.executed)


def test_db_historical_day_runner_loads_real_minute_price_for_existing_position_equity() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="historical existing position",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    previous_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    paper_repo.save_positions(
        run_id=previous_run.run_id,
        trade_date=date(2024, 1, 2),
        positions=[
            PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=100,
                available_quantity=100,
                avg_cost=10.0,
                trade_date=date(2024, 1, 2),
            )
        ],
        prices={"000001.SZ": 10.0},
    )


    provider = FakeDbMinuteProvider()

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,  # type: ignore[arg-type]
        runtime=runtime_with_authoritative_scores(
            manifest,
            trade_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL.value,
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=RecordingRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
    )

    assert result.run.status == RunStatus.SUCCEEDED
    assert result.run.runtime_config["current_prices"]["000001.SZ"] == 10.1
    assert result.run.runtime_config["current_price_context"]["000001.SZ"]["basis"] == "first_observed_minute_close"
    assert provider.calls[0]["min_bars"] == 1
    assert provider.calls[0]["source"] == MinuteDataSource.DB_HISTORICAL
    assert any(
        item["event_type"] == "CURRENT_POSITION_PRICES_LOADED"
        for item in paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
    )


def test_day_runner_excludes_symbol_with_missing_v25_turnover_rate_f_and_continues() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(algo_code="V25_1_SMALL_CAP", topk=2, n_drop=0)
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="v25 exclude missing turnover",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    provider = FakeDayFeatureExcludingMinuteProvider(excluded_symbol="000001.SZ")

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,  # type: ignore[arg-type]
        execution_engine=FakeFullExecutionEngine(),
        runtime=runtime_with_authoritative_scores(
            manifest,
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            rows=[
                {"symbol": "000001.SZ", "score": 0.91, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
                {"symbol": "000002.SZ", "score": 0.89, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
            ],
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=RecordingRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    assert result.run.status == RunStatus.SUCCEEDED
    assert [order.symbol for order in result.orders] == ["000002.SZ"]
    assert [fill.symbol for fill in result.fills] == ["000002.SZ"]
    assert any(call["symbol"] == "000001.SZ" and call["require_day_features"] for call in provider.calls)
    assert any(call["symbol"] == "000002.SZ" and call["require_day_features"] for call in provider.calls)
    assert not any(order.symbol == "000001.SZ" for order in paper_repo.orders[result.run.run_id])

    events = paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
    exclusion = next(item for item in events if item["event_type"] == "DAY_FEATURE_SYMBOL_EXCLUDED")
    assert exclusion["context"]["symbol"] == "000001.SZ"
    assert exclusion["context"]["reason_code"] == "V25_DAY_FEATURE_TURNOVER_RATE_F_MISSING"
    assert exclusion["context"]["fail_closed_policy"] == "exclude_symbol_for_trade_date"
    assert exclusion["context"]["source_error"]["error_code"] == "DATA_UNAVAILABLE"
    assert exclusion["context"]["source_error"]["context"]["forbidden_fallback"] == "turnover_rate"


def test_day_runner_risk_policy_blocks_buy_and_forces_existing_position_exit() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(custom_params={"risk_policy": {"enabled": True}})
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="risk policy forced exit",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    previous_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    paper_repo.save_positions(
        run_id=previous_run.run_id,
        trade_date=date(2024, 1, 2),
        positions=[
            PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=300,
                available_quantity=300,
                avg_cost=10.0,
                trade_date=date(2024, 1, 2),
            )
        ],
        prices={"000001.SZ": 10.0},
    )
    runtime_config = {"runtime_profile": {"risk_policy": {"enabled": True}}}
    profile_service = PaperTradingV2PortfolioService(package_repository=package_repo, repository=paper_repo)
    _profile, version = profile_service.create_runtime_profile(
        portfolio_id=portfolio.portfolio_id,
        profile_name="risk policy active",
        config_json=runtime_config,
        created_by="unit_test",
    )
    activation = profile_service.activate_runtime_config(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
        profile_version_id=version.profile_version_id,
        activated_by="unit_test",
        reason="risk policy forced exit test",
    )
    provider = FakeDbMinuteProvider()

    result = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,  # type: ignore[arg-type]
        runtime=runtime_with_authoritative_scores(
            manifest,
            trade_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            rows=[
                {"symbol": "000001.SZ", "score": 0.91, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
                {"symbol": "000002.SZ", "score": 0.89, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
            ],
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=RecordingRefreshAudit(),
        risk_policy_service=FakeRiskPolicyService(
            {
                "000001.SZ": RiskDecision(
                    symbol="000001.SZ",
                    can_buy=False,
                    force_exit=True,
                    position_target_override=0,
                    reason_codes=["unit_st_pit_not_eligible"],
                )
            }
        ),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
    )

    assert result.run.runtime_config["runtime_profile_activation"]["activation_id"] == activation.activation_id
    orders = paper_repo.orders[result.run.run_id]
    sell_orders = [order for order in orders if order.symbol == "000001.SZ" and order.side.value == "SELL"]
    buy_orders = [order for order in orders if order.symbol == "000001.SZ" and order.side.value == "BUY"]
    assert sell_orders
    assert not buy_orders
    assert sell_orders[0].metadata["rebalance_reason"] == "risk_policy_forced_exit"
    target_event = next(
        item
        for item in paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
        if item["event_type"] == "TARGETS_GENERATED"
    )
    target_symbols = [item["symbol"] for item in target_event["context"]["targets"]]
    assert target_event["context"]["target_count"] == 2
    assert target_symbols.count("000001.SZ") == 1
    assert any(
        item["event_type"] == "RISK_POLICY_APPLIED"
        for item in paper_repo.list_run_events(portfolio.portfolio_id, run_id=result.run.run_id)
    )


def test_day_runner_loads_snapshot_price_for_held_position_without_order_market_data() -> None:
    paper_repo = InMemoryPaperTradingV2Repository()
    run = paper_repo.create_run(
        PaperRun(
            portfolio_id="paper_test",
            trade_date=date(2024, 1, 3),
            status=RunStatus.RUNNING,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    provider = FakeDbMinuteProvider()
    prices = PaperTradingDayRunner(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,  # type: ignore[arg-type]
    )._load_snapshot_prices_for_held_positions(
        symbols=["000002.SZ"],
        trade_date=date(2024, 1, 3),
        data_source=MinuteDataSource.DB_HISTORICAL,
        run_id=run.run_id,
    )

    assert prices["000002.SZ"] == pytest.approx(10.3)
    assert provider.calls[0]["min_bars"] == 1
    assert provider.calls[0]["symbol"] == "000002.SZ"
    assert any(
        item["event_type"] == "HELD_POSITION_SNAPSHOT_PRICES_LOADED"
        for item in paper_repo.run_events
    )


def test_paper_trading_day_runner_rejects_raw_execution_policy_override() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
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

    with pytest.raises(RuntimeConfigInvalidError, match="backtest-validated execution policy"):
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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

    with pytest.raises(InvalidStateTransitionError, match="already exists"):
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


def test_paper_execution_policy_activation_accepts_versioned_policy_that_differs_from_manifest() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
        paper_enabled=False,
    )
    listed = portfolio_service.list_execution_policies(portfolio.portfolio_id)
    listed_policy = next(item for item in listed if item["validated_execution_policy_id"] == policy.policy_id)
    assert listed_policy["matches_portfolio_manifest"] is True
    assert listed_policy["runtime_selectable"] is True
    assert "can_enter_paper" not in listed_policy
    assert "paper_check_error" not in listed_policy

    activation = portfolio_service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id=policy.policy_id,
        activated_by="unit_test",
        reason="validate activation path",
    )

    assert activation.policy_id == policy.policy_id
    assert activation.policy_json["algo_code"] == "CLOSE_PRICE"


def test_paper_execution_policy_activation_matching_qe_contract_is_used_for_trade_date_run() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    policy_id = portfolio.execution_policy["validated_execution_policy_id"]
    activation = portfolio_service.activate_execution_policy(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        policy_id=policy_id,
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
    assert context["validated_execution_policy_id"] == policy_id
    assert context["algo_code"] == manifest.minute_execution_policy.algo_code
    assert paper_repo.list_execution_policy_activations(portfolio.portfolio_id)[0].activation_id == activation.activation_id


def test_day_runner_consumes_validated_runtime_variant_candidate() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(
        topk=2,
        custom_params={
            "strategy_id": "score_weighted_topk_v2",
            "topk": 2,
            "max_single_order_value": 5_000_000.0,
            "max_weight": 0.05,
        },
    )
    save_manifest_with_default_execution_policy(package_repo, manifest)
    package_service = StrategyPackageService(repository=package_repo)
    variant = package_service.create_runtime_variant(
        manifest.package_id,
        variant_name="validated high-cap top1",
        variant_kind=RuntimeVariantKind.COMBINED,
        variant_config={
            "strategy_config": {
                "custom_params": {
                    "strategy_id": "score_weighted_topk_v2",
                    "topk": 1,
                    "max_single_order_value": 10_000.0,
                    "max_weight": 1.0,
                    "max_position_ratio": 1.0,
                }
            }
        },
        created_by="unit_test",
    )
    variant = package_service.mark_runtime_variant_validation(
        manifest.package_id,
        variant.variant_id,
        validation_status=RuntimeVariantValidationStatus.VALIDATION_PASSED,
        paper_candidate=True,
        validation_evidence={"validation_run_id": "vr_runtime_variant_day_runner", "status": "passed"},
    )
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="paper runtime variant",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.TDX_REALTIME,
    )
    runtime_config = {"runtime_variant_id": variant.variant_id}

    result = PaperTradingDayRunner(
        repository=paper_repo,
        package_repository=package_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=PaperV2MinuteMarketDataProvider(
            limit_price_provider=FakeLimitProvider(),
            suspend_status_provider=FakeSuspendProvider(),
            tdx_fetcher=lambda _symbol, _trade_date: make_raw_bars(),
        ),
        runtime=runtime_with_authoritative_scores(
            manifest,
            data_source=MinuteDataSource.TDX_REALTIME.value,
            runtime_config=runtime_config,
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).run_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
        runtime_config=runtime_config,
    )

    stored_config = result.run.runtime_config
    assert stored_config["runtime_variant"]["variant_id"] == variant.variant_id
    assert stored_config["runtime_variant"]["paper_candidate"] is False
    assert stored_config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]["topk"] == 1
    assert stored_config["qe_backtest_runtime_contract"]["portfolio_strategy"]["params"]["max_single_order_value"] == 10_000.0
    assert stored_config["validated_execution_policy"]["activation_source"] == "portfolio_default"
    assert len(result.orders) == 1
    assert result.orders[0].quantity == 1000


def test_paper_execution_policy_activation_rejects_existing_run() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
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

    with pytest.raises(InvalidStateTransitionError, match="after a paper run exists"):
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    with pytest.raises(RuntimeConfigInvalidError, match="requires a reason"):
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    class RaiseStrategyPackageRevalidation(StrategyPackageValidator):
        def validate_manifest_identity_for_paper_trading(self, manifest) -> None:  # noqa: ANN001
            raise AssertionError(f"readiness revalidated StrategyPackage {manifest.package_id}")

    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
        validator=RaiseStrategyPackageRevalidation(),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 2),
    )

    assert result.order_intent_count == 1
    assert result.checked_symbols == ["000001.SZ"]
    manifest_check = next(check for check in result.checks if check.check_name == "strategy_package_manifest")
    assert manifest_check.context["admission_authority"] == "strategy_package_entry"
    assert manifest_check.context["revalidated"] is False
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


def test_selection_cutoff_helper_injects_previous_trading_day_for_auto_generate() -> None:
    runtime_config = {
        "selection_artifact_config": {"auto_generate": True, "inference_backend": "wsl"},
        "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
    }

    cutoff = ensure_previous_trading_day_selection_cutoff(
        runtime_config,
        trade_date=date(2024, 1, 3),
        calendar_provider=FakeCalendar(),
    )

    assert cutoff == date(2024, 1, 2)
    assert runtime_config["selection_artifact_config"]["cutoff_date"] == "2024-01-02"
    assert runtime_config["selection_artifact_config"]["pit_mode"] == "PREVIOUS_TRADING_DAY_CLOSE"
    assert runtime_config["paper_v2_session"]["selection_cutoff_date"] == "2024-01-02"
    assert runtime_config["paper_v2_session"]["selection_cutoff_policy"] == "PREVIOUS_TRADING_DAY_CLOSE"


def test_paper_trading_readiness_auto_generate_uses_previous_trading_day_cutoff() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="readiness auto cutoff",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    runtime_config_without_cutoff = {
        "selection_artifact_config": {"auto_generate": True, "inference_backend": "wsl"},
        "paper_v2_session": {"signal_data_source": "DB_HISTORICAL"},
        "runtime_profile": {
            "selection": {"top_k": 20},
            "tradability": {"exclude_suspended": True},
            "hmm": {"enabled": False},
        },
    }
    runtime_config_with_cutoff = {
        **runtime_config_without_cutoff,
        "selection_artifact_config": {
            "auto_generate": True,
            "inference_backend": "wsl",
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2024-01-02",
        },
        "paper_v2_session": {
            "signal_data_source": "DB_HISTORICAL",
            "selection_cutoff_date": "2024-01-02",
            "selection_cutoff_policy": "PREVIOUS_TRADING_DAY_CLOSE",
        },
    }

    result = PaperTradingReadinessService(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeDbMinuteProvider(),  # type: ignore[arg-type]
        runtime=runtime_with_authoritative_scores(
            manifest,
            trade_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            runtime_config=runtime_config_with_cutoff,
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
        runtime_config=runtime_config_without_cutoff,
    )

    assert result.checked_symbols == ["000001.SZ"]
    assert "paper_v2_session" in result.runtime_config_keys
    assert "selection_artifact_config" in result.runtime_config_keys
    assert any(
        check.check_name == "selection_runtime" and check.context["raw_candidate_count"] == 1
        for check in result.checks
    )


def test_readiness_loads_db_price_for_existing_position_equity() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest()
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="readiness existing position",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    previous_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    paper_repo.save_positions(
        run_id=previous_run.run_id,
        trade_date=date(2024, 1, 2),
        positions=[
            PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=100,
                available_quantity=100,
                avg_cost=10.0,
                trade_date=date(2024, 1, 2),
            )
        ],
        prices={"000001.SZ": 10.0},
    )
    provider = FakeDbMinuteProvider()

    result = PaperTradingReadinessService(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=provider,  # type: ignore[arg-type]
        runtime=runtime_with_authoritative_scores(
            manifest,
            trade_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL.value,
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
    )

    assert result.checked_symbols == ["000001.SZ"]
    assert "current_prices" in result.runtime_config_keys
    assert {check.check_name for check in result.checks} >= {"current_position_prices", "portfolio_state"}
    assert provider.calls[0]["symbol"] == "000001.SZ"
    assert provider.calls[0]["min_bars"] == 1


def test_readiness_risk_policy_forced_exit_overrides_score_sell_target_once() -> None:
    package_repo = InMemoryStrategyPackageRepository()
    paper_repo = InMemoryPaperTradingV2Repository()
    manifest = make_paper_enabled_manifest(custom_params={"risk_policy": {"enabled": True}})
    save_manifest_with_default_execution_policy(package_repo, manifest)
    portfolio = PaperTradingV2PortfolioService(
        package_repository=package_repo,
        repository=paper_repo,
    ).create_portfolio(
        package_id=manifest.package_id,
        portfolio_name="readiness risk forced exit",
        initial_cash=100_000,
        start_date=date(2024, 1, 2),
        data_source=MinuteDataSource.DB_HISTORICAL,
    )
    previous_run = paper_repo.create_run(
        PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=date(2024, 1, 2),
            status=RunStatus.SUCCEEDED,
            data_source=MinuteDataSource.DB_HISTORICAL,
        )
    )
    paper_repo.save_positions(
        run_id=previous_run.run_id,
        trade_date=date(2024, 1, 2),
        positions=[
            PositionLot(
                portfolio_id=portfolio.portfolio_id,
                symbol="000001.SZ",
                quantity=300,
                available_quantity=300,
                avg_cost=10.0,
                trade_date=date(2024, 1, 2),
            )
        ],
        prices={"000001.SZ": 10.0},
    )

    result = PaperTradingReadinessService(
        repository=paper_repo,
        calendar_provider=FakeCalendar(),
        market_data_provider=FakeDbMinuteProvider(),  # type: ignore[arg-type]
        runtime=runtime_with_authoritative_scores(
            manifest,
            trade_date=date(2024, 1, 3),
            data_source=MinuteDataSource.DB_HISTORICAL.value,
            rows=[
                {"symbol": "000001.SZ", "score": 0.91, "rank": 1, "target_weight": 0.03, "reference_price": 10.0},
                {"symbol": "000002.SZ", "score": 0.89, "rank": 2, "target_weight": 0.03, "reference_price": 10.0},
            ],
        ),
        tradability_filter=TradabilityFilter(FakeSuspendLookup()),
        refresh_audit=NoopRefreshAudit(),
        risk_policy_service=FakeRiskPolicyService(
            {
                "000001.SZ": RiskDecision(
                    symbol="000001.SZ",
                    can_buy=False,
                    force_exit=True,
                    position_target_override=0,
                    reason_codes=["unit_st_pit_not_eligible"],
                )
            }
        ),
    ).check_day(
        portfolio_id=portfolio.portfolio_id,
        trade_date=date(2024, 1, 3),
    )

    assert result.target_count == 2
    assert result.order_intent_count == 2
    assert result.checked_symbols == ["000001.SZ", "000002.SZ"]


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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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

    with pytest.raises(InvalidStateTransitionError, match="already has paper v2 runs"):
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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

    with pytest.raises(RuntimeConfigInvalidError, match="explicit confirmation"):
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
    save_manifest_with_default_execution_policy(package_repo, manifest)
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
