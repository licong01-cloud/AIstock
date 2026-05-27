"""Incremental real-time Paper Trading v2 session execution.

This module adds live/replay-catchup orchestration around the existing strict
day-runner components. It does not call the closed-day runner for current-day
live work and it never switches data sources or algorithms implicitly.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, date, datetime, time, timedelta
from typing import Any, Callable
from zoneinfo import ZoneInfo

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider, TradeCalendarProvider
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.runtime_profile import (
    parse_selection_runtime_profile,
    refresh_generated_runtime_profile_binding,
    validate_runtime_profile_binding,
)
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.backtest_contract import (
    normalize_runtime_config_with_backtest_contract,
    validate_runtime_profile_matches_backtest_contract,
)
from backend.services.strategy_package.repository import StrategyPackageRepository
from backend.services.strategy_package.runtime import (
    RebalanceEngine,
    StrategyPackageRuntime,
    TargetPositionEngine,
    apply_runtime_variant_to_manifest,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    ExecutionAlgoError,
    ArtifactGenerationFailedError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
    SessionConfigError,
    TradingCoreError,
)
from backend.services.trading_core.execution_algo_capabilities import require_execution_algo_supports_mode
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import AccountSnapshot, OrderStatus, RunStatus
from backend.services.trading_core.oms import OMS

from .models import (
    IntradaySnapshot,
    OrderExecutionState,
    PaperRun,
    PaperSessionDay,
    PaperSessionMode,
    PaperSessionPhase,
    PaperSessionProgress,
    PaperSessionStatus,
    PaperTradingSession,
    PortfolioStatus,
)
from .replay import PaperTradingHistoricalReplay
from .repository import PaperTradingV2Repository
from .risk_targets import overlay_risk_forced_exit_targets


MARKET_CLOSE = time(15, 0)
LIVE_SESSION_TZ = ZoneInfo("Asia/Shanghai")
STK_LIMIT_PREOPEN_READY_DEADLINE = time(9, 14)
FINAL_ORDER_STATUSES = {OrderStatus.FILLED.value, OrderStatus.CANCELLED.value, OrderStatus.REJECTED.value}


class PaperTradingLiveMinuteExecutor:
    """Process LIVE_ONLY and CATCHUP_THEN_LIVE sessions one bounded tick at a time."""

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        market_data_provider: PaperV2MinuteMarketDataProvider | None = None,
        runtime: StrategyPackageRuntime | None = None,
        target_engine: TargetPositionEngine | None = None,
        rebalance_engine: RebalanceEngine | None = None,
        oms: OMS | None = None,
        execution_engine: MinuteExecutionEngine | None = None,
        validator: StrategyPackageValidator | None = None,
        package_repository: StrategyPackageRepository | Any | None = None,
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        replay_service: PaperTradingHistoricalReplay | None = None,
        risk_policy_service: StockRiskPolicyService | Any | None = None,
        minqmt_broker_factory: Callable[..., Any] | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
        self.package_repository = package_repository
        self.calendar_provider = calendar_provider or TradeCalendarProvider()
        self.market_data_provider = market_data_provider or PaperV2MinuteMarketDataProvider()
        self.runtime = runtime or StrategyPackageRuntime()
        self.target_engine = target_engine or TargetPositionEngine()
        self.rebalance_engine = rebalance_engine or RebalanceEngine()
        self.oms = oms or OMS()
        self.execution_engine = execution_engine or MinuteExecutionEngine(oms=self.oms)
        self.validator = validator or StrategyPackageValidator()
        self.tradability_filter = tradability_filter or TradabilityFilter()
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository()
        self.risk_policy_service = risk_policy_service or StockRiskPolicyService()
        self.day_helper = PaperTradingDayRunner(
            repository=self.repository,
            calendar_provider=self.calendar_provider,
            market_data_provider=self.market_data_provider,
            runtime=self.runtime,
            target_engine=self.target_engine,
            rebalance_engine=self.rebalance_engine,
            oms=self.oms,
            execution_engine=self.execution_engine,
            validator=self.validator,
            package_repository=self.package_repository,
            tradability_filter=self.tradability_filter,
            refresh_audit=self.refresh_audit,
            risk_policy_service=self.risk_policy_service,
            minqmt_broker_factory=minqmt_broker_factory,
        )
        self.replay_service = replay_service or PaperTradingHistoricalReplay(
            repository=self.repository,
            package_repository=self.package_repository,
            calendar_provider=self.calendar_provider,
            day_runner=self.day_helper,
        )

    def tick(self, session: PaperTradingSession, *, as_of_time: datetime | None = None) -> PaperSessionProgress:
        now = as_of_time or datetime.now()
        if session.mode == PaperSessionMode.CATCHUP_THEN_LIVE:
            session = self._run_historical_catchup(session, as_of_time=now)
        try:
            return self._tick_live_intraday(session, as_of_time=now)
        except DataUnavailableError as exc:
            if self._is_retryable_live_minute_fetch_error(exc):
                return self._record_retryable_live_minute_fetch_error(session, exc, as_of_time=now)
            raise

    @staticmethod
    def _manual_tick_only(session: PaperTradingSession) -> bool:
        return bool((session.runtime_config.get("paper_v2_session") or {}).get("manual_tick_only"))

    @staticmethod
    def _is_retryable_live_minute_fetch_error(exc: DataUnavailableError) -> bool:
        return exc.message == "TDX minute data fetch failed"

    def _record_retryable_live_minute_fetch_error(
        self,
        session: PaperTradingSession,
        exc: DataUnavailableError,
        *,
        as_of_time: datetime,
    ) -> PaperSessionProgress:
        local_as_of = self._local_session_time(as_of_time)
        raw_trade_date = exc.context.get("trade_date")
        try:
            trade_date = date.fromisoformat(str(raw_trade_date)) if raw_trade_date else local_as_of.date()
        except ValueError:
            trade_date = local_as_of.date()
        run = self.repository.get_run_by_portfolio_date(session.portfolio_id, trade_date)
        existing_day = next(
            (item for item in self.repository.list_session_days(session.session_id) if item.trade_date == trade_date),
            None,
        )
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=session.portfolio_id,
                trade_date=trade_date,
                run_id=run.run_id if run else (existing_day.run_id if existing_day else None),
                status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                data_source=session.live_data_source or MinuteDataSource.TDX_REALTIME,
                expected_bar_count=existing_day.expected_bar_count if existing_day else None,
                latest_available_bar_time=existing_day.latest_available_bar_time if existing_day else None,
                last_processed_bar_time=existing_day.last_processed_bar_time if existing_day else None,
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id if run else None,
            event_type="LIVE_DATA_FETCH_RETRYABLE",
            message="paper v2 live tick is waiting for TDX minute data fetch to recover",
            context={
                "trade_date": trade_date.isoformat(),
                "as_of_time": local_as_of.isoformat(),
                "retryable": True,
                "reason": exc.to_dict(),
            },
        )
        if run:
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="LIVE_DATA_FETCH_RETRYABLE",
                message="paper v2 live run is waiting for TDX minute data fetch to recover",
                context={
                    "session_id": session.session_id,
                    "trade_date": trade_date.isoformat(),
                    "as_of_time": local_as_of.isoformat(),
                    "retryable": True,
                    "reason": exc.to_dict(),
                },
            )
        self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
        updated = self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            started_at=session.started_at or datetime.now(UTC),
            last_error=exc.to_dict(),
        )
        return self._progress(updated.session_id)

    def _run_historical_catchup(
        self,
        session: PaperTradingSession,
        *,
        as_of_time: datetime,
    ) -> PaperTradingSession:
        if session.historical_data_source != MinuteDataSource.DB_HISTORICAL:
            raise SessionConfigError(
                "CATCHUP_THEN_LIVE currently requires DB_HISTORICAL for the historical catch-up role",
                context={"session_id": session.session_id, "historical_data_source": str(session.historical_data_source)},
            )
        replay_end = self._catchup_replay_end(session=session, as_of_time=as_of_time)
        completed_days = {
            item.trade_date
            for item in self.repository.list_session_days(session.session_id)
            if item.phase == PaperSessionPhase.HISTORICAL_REPLAY and item.status == PaperSessionStatus.SUCCEEDED
        }
        if replay_end is not None:
            trading_days = self.calendar_provider.list_trading_days(session.start_date, replay_end)
            missing_days = [item for item in trading_days if item not in completed_days]
            if missing_days:
                opts = dict(session.runtime_config.get("paper_v2_session") or {})
                catchup_status = PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.CATCHING_UP
                self.repository.update_session_status(
                    session.session_id,
                    status=catchup_status,
                    phase=PaperSessionPhase.HISTORICAL_REPLAY,
                    started_at=session.started_at or datetime.now(UTC),
                )
                self.repository.save_session_event(
                    session_id=session.session_id,
                    event_type="SESSION_CATCHUP_REPLAY_STARTED",
                    message="paper v2 catch-up historical replay started",
                    context={
                        "start_date": missing_days[0].isoformat(),
                        "end_date": missing_days[-1].isoformat(),
                        "historical_data_source": session.historical_data_source.value,
                    },
                )
                result = self.replay_service.run(
                    portfolio_id=session.portfolio_id,
                    start_date=missing_days[0],
                    end_date=missing_days[-1],
                    runtime_config=session.runtime_config,
                    rerun_policy=str(opts.get("rerun_policy") or "reject_existing"),
                    confirm_reset=bool(opts.get("confirm_reset", False)),
                    confirm_text=opts.get("confirm_text"),
                )
                for day in result.day_results:
                    self.repository.save_session_day(
                        PaperSessionDay(
                            session_id=session.session_id,
                            portfolio_id=session.portfolio_id,
                            trade_date=day.trade_date,
                            run_id=day.run_id,
                            status=PaperSessionStatus.SUCCEEDED,
                            phase=PaperSessionPhase.HISTORICAL_REPLAY,
                            data_source=MinuteDataSource.DB_HISTORICAL,
                        )
                    )
                self.repository.save_session_event(
                    session_id=session.session_id,
                    event_type="SESSION_CATCHUP_REPLAY_SUCCEEDED",
                    message="paper v2 catch-up historical replay succeeded",
                    context={"run_ids": [item.run_id for item in result.day_results]},
                )
        return self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.SWITCHING_TO_LIVE,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            started_at=session.started_at or datetime.now(UTC),
        )

    def _tick_live_intraday(
        self,
        session: PaperTradingSession,
        *,
        as_of_time: datetime,
    ) -> PaperSessionProgress:
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        if portfolio.broker_backend == "minqmt_sim":
            return self._tick_minqmt_live_intraday(session, portfolio=portfolio, as_of_time=as_of_time)
        if session.live_data_source != MinuteDataSource.TDX_REALTIME:
            raise SessionConfigError(
                "live Paper v2 sessions require the broker-bound live_data_source",
                context={
                    "session_id": session.session_id,
                    "broker_backend": portfolio.broker_backend,
                    "live_data_source": str(session.live_data_source),
                    "expected_live_data_source": MinuteDataSource.TDX_REALTIME.value,
                },
            )
        if portfolio.status not in {PortfolioStatus.READY, PortfolioStatus.RUNNING}:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY/RUNNING before live session tick",
                context={"portfolio_id": portfolio.portfolio_id, "status": portfolio.status.value},
            )
        if session.start_date > as_of_time.date():
            updated = self._save_waiting_next_day(
                session,
                trade_date=session.start_date,
                message="paper v2 live session is waiting for configured start_date",
                context={"as_of_date": as_of_time.date().isoformat()},
            )
            return self._progress(updated.session_id)
        try:
            self.calendar_provider.ensure_trading_day(as_of_time.date())
        except DataUnavailableError as exc:
            updated = self._save_waiting_next_day(
                session,
                trade_date=as_of_time.date(),
                message="paper v2 live session is waiting for next trading day",
                context=exc.context,
            )
            return self._progress(updated.session_id)

        run = self.repository.get_run_by_portfolio_date(session.portfolio_id, as_of_time.date())
        if run is None:
            waiting = self._wait_for_preopen_stk_limit_if_needed(
                session,
                trade_date=as_of_time.date(),
                as_of_time=as_of_time,
            )
            if waiting is not None:
                return self._progress(waiting.session_id)
            run = self._prepare_live_run(session, trade_date=as_of_time.date(), as_of_time=as_of_time)
            if run.status == RunStatus.SUCCEEDED:
                return self._progress(session.session_id)
        elif run.status == RunStatus.SUCCEEDED:
            updated = self._save_waiting_next_day(
                session,
                trade_date=as_of_time.date(),
                message="paper v2 live day is already finalized",
                context={"run_id": run.run_id},
            )
            return self._progress(updated.session_id)
        elif run.status == RunStatus.FAILED:
            raise InvalidStateTransitionError(
                "cannot continue a failed live paper run",
                context={"session_id": session.session_id, "run_id": run.run_id, "trade_date": run.trade_date.isoformat()},
            )

        progress = self._process_live_run(session, run, as_of_time=as_of_time)
        if self._is_after_market_close(as_of_time):
            session = self.repository.get_session(session.session_id)
            run = self.repository.get_run_by_portfolio_date(session.portfolio_id, as_of_time.date()) or run
            progress = self._finalize_live_day(session, run, as_of_time=as_of_time)
        return progress

    def _tick_minqmt_live_intraday(
        self,
        session: PaperTradingSession,
        *,
        portfolio: Any,
        as_of_time: datetime,
    ) -> PaperSessionProgress:
        if session.live_data_source != MinuteDataSource.MINIQMT_REALTIME:
            raise SessionConfigError(
                "MiniQMT Paper v2 live sessions require MINIQMT_REALTIME live_data_source",
                context={
                    "session_id": session.session_id,
                    "portfolio_id": session.portfolio_id,
                    "broker_backend": portfolio.broker_backend,
                    "live_data_source": str(session.live_data_source),
                },
            )
        if portfolio.status not in {PortfolioStatus.READY, PortfolioStatus.RUNNING}:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY/RUNNING before MiniQMT live session tick",
                context={"portfolio_id": portfolio.portfolio_id, "status": portfolio.status.value},
            )

        local_as_of = self._local_session_time(as_of_time)
        trade_date = local_as_of.date()
        if session.start_date > trade_date:
            updated = self._save_waiting_next_day(
                session,
                trade_date=session.start_date,
                message="MiniQMT live session is waiting for configured start_date",
                context={"as_of_date": trade_date.isoformat(), "broker_backend": portfolio.broker_backend},
            )
            return self._progress(updated.session_id)
        try:
            self.calendar_provider.ensure_trading_day(trade_date)
        except DataUnavailableError as exc:
            updated = self._save_waiting_next_day(
                session,
                trade_date=trade_date,
                message="MiniQMT live session is waiting for next trading day",
                context={**exc.context, "broker_backend": portfolio.broker_backend},
            )
            return self._progress(updated.session_id)

        existing = self.repository.get_run_by_portfolio_date(session.portfolio_id, trade_date)
        if existing is not None:
            if existing.status == RunStatus.SUCCEEDED:
                self.repository.save_session_day(
                    PaperSessionDay(
                        session_id=session.session_id,
                        portfolio_id=session.portfolio_id,
                        trade_date=trade_date,
                        run_id=existing.run_id,
                        status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                        phase=PaperSessionPhase.WAITING_NEXT_DAY,
                        data_source=MinuteDataSource.MINIQMT_REALTIME,
                    )
                )
                self.repository.save_session_event(
                    session_id=session.session_id,
                    run_id=existing.run_id,
                    event_type="MINIQMT_LIVE_DAY_ALREADY_RECONCILED",
                    message="MiniQMT live day is already broker-reconciled",
                    context={"run_id": existing.run_id, "broker_backend": portfolio.broker_backend},
                )
                self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
                self.repository.update_session_status(
                    session.session_id,
                    status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                    phase=PaperSessionPhase.WAITING_NEXT_DAY,
                    started_at=session.started_at or datetime.now(UTC),
                )
                return self._progress(session.session_id)
            if existing.status == RunStatus.FAILED:
                raise InvalidStateTransitionError(
                    "cannot continue a failed MiniQMT live paper run",
                    context={
                        "session_id": session.session_id,
                        "run_id": existing.run_id,
                        "trade_date": existing.trade_date.isoformat(),
                    },
                )
            self.repository.save_session_day(
                PaperSessionDay(
                    session_id=session.session_id,
                    portfolio_id=session.portfolio_id,
                    trade_date=trade_date,
                    run_id=existing.run_id,
                    status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                    phase=PaperSessionPhase.LIVE_INTRADAY,
                    data_source=MinuteDataSource.MINIQMT_REALTIME,
                )
            )
            self.repository.save_session_event(
                session_id=session.session_id,
                run_id=existing.run_id,
                event_type="MINIQMT_LIVE_RUN_ALREADY_ACTIVE",
                message="MiniQMT live tick found an existing active broker-authoritative run; no duplicate orders submitted",
                context={"trade_date": trade_date.isoformat(), "broker_backend": portfolio.broker_backend},
            )
            self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
            self.repository.update_session_status(
                session.session_id,
                status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                started_at=session.started_at or datetime.now(UTC),
            )
            return self._progress(session.session_id)

        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="MINIQMT_LIVE_TICK_STARTED",
            message="MiniQMT live tick is starting broker-authoritative order submission",
            context={
                "trade_date": trade_date.isoformat(),
                "as_of_time": local_as_of.isoformat(),
                "broker_backend": portfolio.broker_backend,
                "live_data_source": MinuteDataSource.MINIQMT_REALTIME.value,
            },
        )
        if portfolio.status == PortfolioStatus.RUNNING:
            # The live session keeps the portfolio operational between days;
            # the strict day runner still expects READY before creating a run.
            self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.READY)
        runtime_config = deepcopy(session.runtime_config)
        self._ensure_live_selection_cutoff(runtime_config, trade_date=trade_date)
        result = self.day_helper.run_day(
            portfolio_id=session.portfolio_id,
            trade_date=trade_date,
            runtime_config=runtime_config,
        )
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=session.portfolio_id,
                trade_date=trade_date,
                run_id=result.run.run_id,
                status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                phase=PaperSessionPhase.WAITING_NEXT_DAY,
                data_source=MinuteDataSource.MINIQMT_REALTIME,
            )
        )
        self.repository.save_intraday_snapshot(
            IntradaySnapshot(
                session_id=session.session_id,
                run_id=result.run.run_id,
                portfolio_id=session.portfolio_id,
                trade_date=trade_date,
                snapshot_time=result.account_snapshot.snapshot_time,
                cash=result.account_snapshot.cash,
                market_value=result.account_snapshot.market_value,
                nav=result.account_snapshot.nav,
                positions=[item.model_dump(mode="json") for item in result.positions],
                source=MinuteDataSource.MINIQMT_REALTIME.value,
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=result.run.run_id,
            event_type="MINIQMT_LIVE_TICK_RECONCILED",
            message="MiniQMT live tick submitted orders and persisted broker-authoritative snapshot without TDX matching",
            context={
                "trade_date": trade_date.isoformat(),
                "run_id": result.run.run_id,
                "order_count": len(result.orders),
                "position_count": len(result.positions),
                "cash": result.account_snapshot.cash,
                "nav": result.account_snapshot.nav,
                "broker_backend": portfolio.broker_backend,
                "authority_source": "MINIQMT_QUERY",
                "live_data_source": MinuteDataSource.MINIQMT_REALTIME.value,
            },
        )
        self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
            phase=PaperSessionPhase.WAITING_NEXT_DAY,
            started_at=session.started_at or datetime.now(UTC),
        )
        return self._progress(session.session_id)

    def _prepare_live_run(
        self,
        session: PaperTradingSession,
        *,
        trade_date: date,
        as_of_time: datetime,
    ) -> PaperRun:
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        if portfolio.status not in {PortfolioStatus.READY, PortfolioStatus.RUNNING}:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY/RUNNING before live session tick",
                context={"portfolio_id": portfolio.portfolio_id, "status": portfolio.status.value},
            )
        manifest = portfolio.frozen_manifest
        self.validator.validate_manifest_identity_for_paper_trading(manifest)
        execution_policy_context = self.day_helper._execution_policy_context_for_date(portfolio, trade_date)
        execution_policy_json = execution_policy_context["policy_json"]
        capability = require_execution_algo_supports_mode(
            execution_policy_json,
            mode="LIVE_ONLY",
            package_id=portfolio.package_id,
        )
        self.validator.validate_execution_policy_for_paper(
            package_id=manifest.package_id,
            policy_json=execution_policy_json,
            instantiate_runtime=False,
            require_runtime_assets=False,
        )
        config = deepcopy(session.runtime_config)
        config["validated_execution_policy"] = execution_policy_context
        config.setdefault("paper_v2_session", {})
        config["paper_v2_session"]["signal_data_source"] = self._signal_data_source(session, portfolio_data_source=portfolio.data_source)
        config["paper_v2_session"]["live_step_mode"] = capability.live_step_mode
        config["paper_v2_session"]["live_data_source"] = session.live_data_source.value if session.live_data_source else None
        self._ensure_live_selection_cutoff(config, trade_date=trade_date)
        config = normalize_runtime_config_with_backtest_contract(
            manifest,
            config,
            context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat(), "check": "live_session"},
            include_contract=True,
        )
        config = refresh_generated_runtime_profile_binding(config)
        validate_runtime_profile_binding(
            config,
            context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat(), "check": "live_session"},
        )
        runtime_profile = parse_selection_runtime_profile(config)
        runtime_contract = validate_runtime_profile_matches_backtest_contract(
            manifest,
            runtime_profile,
            runtime_config=config,
            context={"portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat(), "check": "live_session"},
        )
        effective_manifest = apply_runtime_variant_to_manifest(manifest, config)
        config["qe_backtest_runtime_contract"] = runtime_contract

        ready = self.day_helper._require_data_ready(
            manifest=manifest,
            trade_date=trade_date,
            runtime_config=config,
            execution_policy_json=execution_policy_json,
        )
        current_positions = self.repository.load_latest_positions(portfolio.portfolio_id, trade_date)
        latest_cash = self.repository.load_latest_cash(portfolio, trade_date)
        current_prices = self._current_position_prices(
            symbols=list(current_positions),
            trade_date=trade_date,
            as_of_time=as_of_time,
            live_data_source=session.live_data_source,
        )
        if current_prices:
            config["current_prices"] = current_prices
        total_equity = self.day_helper._resolve_total_equity(
            latest_cash=latest_cash,
            current_positions=current_positions,
            runtime_config=config,
            portfolio_id=portfolio.portfolio_id,
        )
        signal_data_source = str(config["paper_v2_session"]["signal_data_source"])
        self.day_helper._ensure_authoritative_selection_artifact(
            manifest=manifest,
            trade_date=trade_date,
            data_source=signal_data_source,
            runtime_config=config,
        )
        snapshot = self.runtime.build_signal_snapshot(
            manifest=manifest,
            trade_date=trade_date,
            data_source=signal_data_source,
            runtime_config=config,
        )
        top_k = self._require_runtime_top_k(runtime_profile, manifest)
        raw_candidate_count = len(snapshot.candidates)
        risk_decisions = self.risk_policy_service.evaluate(
            symbols=sorted(set(item.symbol for item in snapshot.candidates) | set(current_positions)),
            trade_date=trade_date,
            profile=runtime_profile.risk_policy,
            current_positions=current_positions,
        )
        risk_excluded = []
        if not snapshot.valid_no_candidate:
            risk_adjusted, risk_excluded = self.risk_policy_service.apply_to_candidates(
                candidates=snapshot.candidates,
                decisions=risk_decisions,
                trade_date=trade_date,
                top_k=top_k,
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or portfolio.manifest_sha256,
                allow_empty=bool(current_positions),
            )
            snapshot = snapshot.model_copy(
                update={
                    "candidates": risk_adjusted,
                    "runtime_config": {
                        **snapshot.runtime_config,
                        "risk_policy": {
                            "profile": runtime_profile.risk_policy.model_dump(mode="json"),
                            "excluded_count": len(risk_excluded),
                            "excluded": [item.model_dump(mode="json") for item in risk_excluded],
                        },
                    },
                }
            )
        if not snapshot.valid_no_candidate and snapshot.candidates and (
            runtime_profile.tradability.exclude_suspended or runtime_profile.industry_blacklist
        ):
            tradable, excluded = self.tradability_filter.filter_candidates(
                candidates=snapshot.candidates,
                trade_date=trade_date,
                top_k=top_k,
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or portfolio.manifest_sha256,
                enabled=runtime_profile.tradability.exclude_suspended,
                industry_blacklist=runtime_profile.industry_blacklist,
            )
            snapshot = snapshot.model_copy(
                update={
                    "candidates": tradable,
                    "runtime_config": {
                        **snapshot.runtime_config,
                        "tradability_filter": {
                            "exclude_suspended": runtime_profile.tradability.exclude_suspended,
                            "industry_blacklist": runtime_profile.industry_blacklist,
                            "excluded_count": len(excluded),
                            "excluded": [item.model_dump(mode="json") for item in excluded],
                        },
                    },
                }
            )
        targets = (
            self.target_engine.build_targets(
                snapshot=snapshot,
                total_equity=total_equity,
                top_k=top_k,
                manifest=effective_manifest,
                current_positions=current_positions,
                current_prices=current_prices,
            )
            if snapshot.candidates
            else []
        )
        if runtime_profile.risk_policy.enabled and current_positions:
            forced_exit_targets = self.risk_policy_service.forced_exit_targets(
                decisions=risk_decisions,
                current_positions=current_positions,
                trade_date=trade_date,
                package_id=manifest.package_id,
                manifest_sha256=manifest.manifest_sha256 or portfolio.manifest_sha256,
                existing_target_symbols=set(),
            )
            targets = overlay_risk_forced_exit_targets(targets, forced_exit_targets)
        intents = self.rebalance_engine.build_order_intents(
            package_id=manifest.package_id,
            portfolio_id=portfolio.portfolio_id,
            trade_date=trade_date,
            current_positions=current_positions,
            target_positions=targets,
        )
        run = PaperRun(
            portfolio_id=portfolio.portfolio_id,
            trade_date=trade_date,
            status=RunStatus.RUNNING,
            data_source=session.live_data_source or MinuteDataSource.TDX_REALTIME,
            runtime_config=config,
        )
        latest_prepared_bar_time = self._latest_available_time_for_symbols(
            symbols=[intent.symbol for intent in intents],
            trade_date=trade_date,
            live_data_source=session.live_data_source,
            as_of_time=as_of_time,
        ) if intents else None
        strict_live_start_bar_time = self._live_causality_cursor(as_of_time) if intents else None
        self.repository.create_run(run)
        self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="TARGETS_GENERATED",
            message="live target positions generated from signal snapshot",
            context={
                "target_count": len(targets),
                "targets": [
                    {
                        "symbol": target.symbol,
                        "target_quantity": target.target_quantity,
                        "target_weight": target.target_weight,
                        "rank": target.rank,
                    }
                    for target in targets
                ],
            },
        )
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="ORDER_INTENTS_GENERATED",
            message="live order intents generated from target/current position diff",
            context={
                "order_intent_count": len(intents),
                "intents": [
                    {
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "side": intent.side.value,
                        "quantity": intent.quantity,
                    }
                    for intent in intents
                ],
            },
        )
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="LIVE_RUN_PREPARED",
            message="paper v2 live run prepared orders from StrategyPackage signal",
            context={
                "session_id": session.session_id,
                "raw_candidate_count": raw_candidate_count,
                "target_count": len(targets),
                "order_intent_count": len(intents),
                "risk_policy_enabled": runtime_profile.risk_policy.enabled,
                "risk_excluded_count": len(risk_excluded),
                "risk_force_exit_symbols": [
                    symbol for symbol, decision in risk_decisions.items() if decision.force_exit
                ],
                "data_ready": ready,
                "signal_data_source": signal_data_source,
                "live_data_source": run.data_source.value,
                "algo_code": execution_policy_json.get("algo_code"),
                "live_step_mode": capability.live_step_mode,
                "latest_prepared_bar_time": latest_prepared_bar_time.isoformat() if latest_prepared_bar_time else None,
                "strict_live_start_bar_time": strict_live_start_bar_time.isoformat() if strict_live_start_bar_time else None,
                "live_causality_mode": "strict_no_backfill",
            },
        )
        if not intents:
            if not current_positions:
                raise ArtifactGenerationFailedError(
                    "live rebalance produced no order intents and portfolio has no positions to mark",
                    context={"session_id": session.session_id, "portfolio_id": portfolio.portfolio_id, "trade_date": trade_date.isoformat()},
                )
            prices = self._snapshot_prices_for_positions(
                symbols=list(current_positions),
                trade_date=trade_date,
                as_of_time=as_of_time,
                live_data_source=session.live_data_source,
                known_prices=current_prices,
            )
            snapshot = AccountSnapshot(
                portfolio_id=portfolio.portfolio_id,
                cash=latest_cash,
                market_value=sum(position.quantity * prices[position.symbol] for position in current_positions.values()),
                nav=latest_cash + sum(position.quantity * prices[position.symbol] for position in current_positions.values()),
                snapshot_time=as_of_time,
            )
            self.repository.save_positions(
                run_id=run.run_id,
                trade_date=trade_date,
                positions=list(current_positions.values()),
                prices=prices,
            )
            self.repository.save_daily_snapshot(
                run_id=run.run_id,
                trade_date=trade_date,
                snapshot=snapshot,
                metadata={
                    "position_count": len(current_positions),
                    "order_count": 0,
                    "fill_count": 0,
                    "session_id": session.session_id,
                    "no_rebalance_required": True,
                    "reason": "target_positions_equal_current_positions",
                },
            )
            self.repository.save_session_day(
                PaperSessionDay(
                    session_id=session.session_id,
                    portfolio_id=portfolio.portfolio_id,
                    trade_date=trade_date,
                    run_id=run.run_id,
                    status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                    phase=PaperSessionPhase.WAITING_NEXT_DAY,
                    data_source=run.data_source,
                )
            )
            self.repository.save_session_event(
                session_id=session.session_id,
                run_id=run.run_id,
                event_type="NO_REBALANCE_REQUIRED",
                message="target positions match current positions; live day finalized without orders",
                context={"trade_date": trade_date.isoformat(), "position_count": len(current_positions), "nav": snapshot.nav},
            )
            succeeded = self.repository.update_run_status(run, RunStatus.SUCCEEDED)
            self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
            self.repository.update_session_status(
                session.session_id,
                status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                phase=PaperSessionPhase.WAITING_NEXT_DAY,
                started_at=session.started_at or datetime.now(UTC),
            )
            return succeeded
        for intent in intents:
            order = self.oms.create_order(intent)
            self.repository.save_order(run.run_id, order)
            self.repository.save_order_execution_state(
                OrderExecutionState(
                    session_id=session.session_id,
                    run_id=run.run_id,
                    order_id=order.order_id,
                    symbol=order.symbol,
                    trade_date=trade_date,
                    algo_code=str(execution_policy_json["algo_code"]).upper(),
                    algo_state={
                        "total_quantity": order.quantity,
                        "executed_quantity": 0,
                        "step": 0,
                        "is_complete": False,
                        "live_causality_mode": "strict_no_backfill",
                        "order_created_at": order.created_at.isoformat(),
                        "strict_live_start_bar_time": strict_live_start_bar_time.isoformat() if strict_live_start_bar_time else None,
                    },
                    filled_quantity=0,
                    remaining_quantity=order.quantity,
                    status=order.status.value,
                    last_processed_bar_time=strict_live_start_bar_time,
                )
            )
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=portfolio.portfolio_id,
                trade_date=trade_date,
                run_id=run.run_id,
                status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                data_source=run.data_source,
                latest_available_bar_time=strict_live_start_bar_time,
                last_processed_bar_time=strict_live_start_bar_time,
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id,
            event_type="LIVE_RUN_PREPARED",
            message="paper v2 live run prepared and waiting for minute bars",
            context={"trade_date": trade_date.isoformat(), "order_count": len(intents)},
        )
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            started_at=session.started_at or datetime.now(UTC),
        )
        return run

    @staticmethod
    def _require_runtime_top_k(runtime_profile: Any, manifest: Any) -> int:
        top_k = runtime_profile.selection.top_k
        if top_k is None:
            raise RuntimeConfigInvalidError(
                "Paper v2 live session requires runtime_profile.selection.top_k; StrategyPackage manifest cannot provide runtime top_k",
                context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
            )
        return int(top_k)

    def _wait_for_preopen_stk_limit_if_needed(
        self,
        session: PaperTradingSession,
        *,
        trade_date: date,
        as_of_time: datetime,
    ) -> PaperTradingSession | None:
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        manifest = portfolio.frozen_manifest
        execution_policy_context = self.day_helper._execution_policy_context_for_date(portfolio, trade_date)
        execution_policy_json = execution_policy_context["policy_json"]
        requirements = self.day_helper._data_requirements_for_policy(
            execution_policy_json,
            package_id=manifest.package_id,
        )
        if not requirements["requires_limit_price"]:
            return None
        try:
            self.refresh_audit.require_success(dataset="stk_limit", trade_date=trade_date)
        except DataUnavailableError as exc:
            if self._is_before_stk_limit_preopen_deadline(as_of_time):
                return self._save_waiting_live_data(
                    session,
                    trade_date=trade_date,
                    message="paper v2 live session is waiting for pre-open stk_limit refresh",
                    context={
                        "dataset": "stk_limit",
                        "trade_date": trade_date.isoformat(),
                        "deadline_time": STK_LIMIT_PREOPEN_READY_DEADLINE.isoformat(timespec="minutes"),
                        "timezone": str(LIVE_SESSION_TZ),
                        "as_of_time": self._local_session_time(as_of_time).isoformat(),
                        "reason": exc.to_dict(),
                    },
                )
            raise DataUnavailableError(
                "paper v2 live session requires stk_limit refresh by 09:14 before order preparation",
                context={
                    "dataset": "stk_limit",
                    "trade_date": trade_date.isoformat(),
                    "deadline_time": STK_LIMIT_PREOPEN_READY_DEADLINE.isoformat(timespec="minutes"),
                    "timezone": str(LIVE_SESSION_TZ),
                    "as_of_time": self._local_session_time(as_of_time).isoformat(),
                    "reason": exc.to_dict(),
                },
            ) from exc
        return None

    @staticmethod
    def _local_session_time(as_of_time: datetime) -> datetime:
        if as_of_time.tzinfo is None:
            return as_of_time
        return as_of_time.astimezone(LIVE_SESSION_TZ)

    @classmethod
    def _is_before_stk_limit_preopen_deadline(cls, as_of_time: datetime) -> bool:
        return cls._local_session_time(as_of_time).time() < STK_LIMIT_PREOPEN_READY_DEADLINE

    def _save_waiting_live_data(
        self,
        session: PaperTradingSession,
        *,
        trade_date: date,
        message: str,
        context: dict[str, Any],
    ) -> PaperTradingSession:
        status = PaperSessionStatus.PAUSED if self._manual_tick_only(session) else PaperSessionStatus.LIVE_WAITING_FOR_BAR
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=session.portfolio_id,
                trade_date=trade_date,
                status=status,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                data_source=session.live_data_source or MinuteDataSource.TDX_REALTIME,
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="LIVE_WAITING_FOR_DATA",
            message=message,
            context=context,
        )
        self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
        return self.repository.update_session_status(
            session.session_id,
            status=status,
            phase=PaperSessionPhase.LIVE_INTRADAY,
            started_at=session.started_at or datetime.now(UTC),
        )

    def _process_live_run(
        self,
        session: PaperTradingSession,
        run: PaperRun,
        *,
        as_of_time: datetime,
    ) -> PaperSessionProgress:
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        policy_json = run.runtime_config["validated_execution_policy"]["policy_json"]
        capability = require_execution_algo_supports_mode(policy_json, mode="LIVE_ONLY", package_id=portfolio.package_id)
        require_day_features = capability.algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"}
        algo_config = dict(policy_json.get("algo_config") or {})
        states = self.repository.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)
        if not states:
            raise ExecutionAlgoError(
                "live run has no persisted order execution state",
                context={"session_id": session.session_id, "run_id": run.run_id},
            )
        active_states = [state for state in states if state.status not in FINAL_ORDER_STATUSES and state.remaining_quantity > 0]
        latest_available = self._latest_available_time_for_states(active_states or states, session.live_data_source, as_of_time)
        if not active_states:
            latest_available, last_processed = self._mark_to_market_without_active_orders(
                session,
                run,
                portfolio=portfolio,
                latest_available=latest_available,
                current_last_processed=self._max_processed(states),
                as_of_time=as_of_time,
            )
            self._save_live_day_cursor(session, run, latest_available=latest_available, last_processed=last_processed)
            return self._progress(session.session_id)

        latest_cash = self.repository.load_latest_cash(portfolio, run.trade_date)
        latest_positions = self.repository.load_latest_positions(portfolio.portfolio_id, run.trade_date)
        ledger = InMemoryLedger(
            portfolio_id=portfolio.portfolio_id,
            initial_cash=float(portfolio.initial_cash),
            fee_model=self._fee_model_from_policy(portfolio.fee_policy),
        )
        ledger.cash = latest_cash
        ledger.positions = dict(latest_positions)
        ledger.settle_trade_date(run.trade_date)

        processed_any_bar = False
        new_fill_count = 0
        touched_prices: dict[str, float] = {}
        for state in active_states:
            order = self.repository.get_order(state.order_id)
            market_input = self.market_data_provider.load_observed_intraday(
                symbol=state.symbol,
                trade_date=run.trade_date,
                source=session.live_data_source or MinuteDataSource.TDX_REALTIME,
                until_time=as_of_time,
                require_suspend_status=True,
                require_day_features=require_day_features,
            )
            if market_input.minute_bars:
                touched_prices[state.symbol] = market_input.minute_bars[-1].close
            new_bars = [
                bar
                for bar in market_input.minute_bars
                if self._bar_after_cursor(bar.bar_time, state.last_processed_bar_time)
            ]
            if not new_bars:
                continue
            processed_any_bar = True
            market_context = dict(market_input.market_context)
            market_context.update(
                {
                    "live_step_mode": capability.live_step_mode,
                    "plan_horizon_bars": capability.plan_horizon_bars,
                    "v25_realtime_streaming": capability.algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"},
                }
            )
            final_order, updated_state, fills, events = self.execution_engine.execute_order_incremental(
                order=order,
                execution_state=state,
                new_bars=new_bars,
                algo_code=str(policy_json["algo_code"]),
                algo_config=algo_config,
                market_context=market_context,
            )
            updated_state = self._preserve_live_causality_metadata(state, updated_state)
            # T6.1 capture wiring: intended_price from Order.limit_price
            # (inherited from the original OrderIntent; None for MARKET orders).
            # fill_market_context is the augmented dict actually fed to the
            # execution engine (includes live_step_mode + plan_horizon_bars +
            # v25_realtime_streaming overlay on top of market_input).
            intended_price = order.limit_price
            fill_market_context = dict(market_context)
            for fill in fills:
                ledger.apply_fill(fill)
                self.repository.save_fill(
                    run.run_id,
                    fill,
                    intended_price=intended_price,
                    fill_market_context=fill_market_context,
                )
                new_fill_count += 1
            for event in events:
                self.repository.save_order_event(run.run_id, event)
            self.repository.save_order(run.run_id, final_order)
            self.repository.save_order_execution_state(updated_state)

        if not processed_any_bar:
            self.repository.save_session_event(
                session_id=session.session_id,
                run_id=run.run_id,
                event_type="LIVE_WAITING_FOR_BAR",
                message="paper v2 live tick found no new completed minute bars",
                context={
                    "trade_date": run.trade_date.isoformat(),
                    "latest_available_bar_time": latest_available.isoformat() if latest_available else None,
                },
            )
            self._save_live_day_cursor(session, run, latest_available=latest_available, last_processed=self._max_processed(states))
            self.repository.update_session_status(
                session.session_id,
                status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
            )
            return self._progress(session.session_id)

        if processed_any_bar:
            prices = (
                self._snapshot_prices_for_positions(
                    symbols=list(ledger.positions),
                    trade_date=run.trade_date,
                    as_of_time=as_of_time,
                    live_data_source=session.live_data_source,
                    known_prices=touched_prices,
                )
                if ledger.positions
                else {}
            )
            for entry in ledger.cash_entries:
                self.repository.save_cash_entry(run.run_id, entry)
            if new_fill_count:
                self.repository.save_positions(
                    run_id=run.run_id,
                    trade_date=run.trade_date,
                    positions=list(ledger.positions.values()),
                    prices=prices,
                )
            snapshot = ledger.account_snapshot(prices=prices, snapshot_time=as_of_time)
            self.repository.save_intraday_snapshot(
                IntradaySnapshot(
                    session_id=session.session_id,
                    run_id=run.run_id,
                    portfolio_id=portfolio.portfolio_id,
                    trade_date=run.trade_date,
                    snapshot_time=as_of_time,
                    cash=snapshot.cash,
                    market_value=snapshot.market_value,
                    nav=snapshot.nav,
                    positions=[item.model_dump(mode="json") for item in ledger.positions.values()],
                    source=session.live_data_source.value if session.live_data_source else "TDX_REALTIME",
                )
            )
        states_after = self.repository.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)
        last_processed = self._max_processed(states_after)
        self._save_live_day_cursor(session, run, latest_available=latest_available, last_processed=last_processed)
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id,
            event_type="LIVE_TICK_PROCESSED",
            message="paper v2 live tick processed new minute bars",
            context={
                "trade_date": run.trade_date.isoformat(),
                "new_fill_count": new_fill_count,
                "last_processed_bar_time": last_processed.isoformat() if last_processed else None,
            },
        )
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
            phase=PaperSessionPhase.LIVE_INTRADAY,
        )
        return self._progress(session.session_id)

    def _finalize_live_day(
        self,
        session: PaperTradingSession,
        run: PaperRun,
        *,
        as_of_time: datetime,
    ) -> PaperSessionProgress:
        if run.status != RunStatus.RUNNING:
            return self._progress(session.session_id)
        fills = self.repository.list_fills_for_run(run.run_id)
        policy_json = run.runtime_config["validated_execution_policy"]["policy_json"]
        allow_partial_fill = bool((policy_json.get("algo_config") or {}).get("allow_partial_fill", True))
        states = self.repository.list_order_execution_states(session_id=session.session_id, run_id=run.run_id)
        remaining_states = [state for state in states if state.remaining_quantity > 0 and state.status not in FINAL_ORDER_STATUSES]
        if remaining_states and not allow_partial_fill:
            error = ExecutionAlgoError(
                "live minute execution left unfilled quantity at market close",
                context={
                    "session_id": session.session_id,
                    "run_id": run.run_id,
                    "remaining_orders": [
                        {"order_id": state.order_id, "symbol": state.symbol, "remaining_quantity": state.remaining_quantity}
                        for state in remaining_states
                    ],
                },
            )
            self._mark_run_failed(session, run, error)
            raise error
        if not fills:
            error = ExecutionAlgoError(
                "live paper run produced no fills; no-trade live day is not yet modeled as success",
                context={"session_id": session.session_id, "run_id": run.run_id, "trade_date": run.trade_date.isoformat()},
            )
            self._mark_run_failed(session, run, error)
            raise error
        portfolio = self.repository.get_portfolio(session.portfolio_id)
        latest_cash = self.repository.load_latest_cash(portfolio, run.trade_date)
        latest_positions = self.repository.load_latest_positions(portfolio.portfolio_id, run.trade_date)
        prices = self._snapshot_prices_for_positions(
            symbols=list(latest_positions),
            trade_date=run.trade_date,
            as_of_time=as_of_time,
            live_data_source=session.live_data_source,
            known_prices={},
        )
        snapshot = AccountSnapshot(
            portfolio_id=portfolio.portfolio_id,
            cash=latest_cash,
            market_value=sum(position.quantity * prices[position.symbol] for position in latest_positions.values()),
            nav=latest_cash + sum(position.quantity * prices[position.symbol] for position in latest_positions.values()),
            snapshot_time=as_of_time,
        )
        self.repository.save_daily_snapshot(
            run_id=run.run_id,
            trade_date=run.trade_date,
            snapshot=snapshot,
            metadata={
                "position_count": len(latest_positions),
                "fill_count": len(fills),
                "session_id": session.session_id,
                "finalized_from": "live_intraday",
                "allow_partial_fill": allow_partial_fill,
                "remaining_order_count": len(remaining_states),
            },
        )
        self.repository.update_run_status(run, RunStatus.SUCCEEDED)
        self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
            phase=PaperSessionPhase.WAITING_NEXT_DAY,
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id,
            event_type="LIVE_DAY_FINALIZED",
            message="paper v2 live day finalized after market close",
            context={"trade_date": run.trade_date.isoformat(), "fill_count": len(fills), "nav": snapshot.nav},
        )
        return self._progress(session.session_id)

    def _mark_run_failed(self, session: PaperTradingSession, run: PaperRun, exc: TradingCoreError) -> None:
        error = exc.to_dict()
        self.repository.save_error(run_id=run.run_id, portfolio_id=session.portfolio_id, error=error)
        self.repository.update_run_status(run, RunStatus.FAILED, error=error)
        self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.FAILED)
        self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.FAILED,
            last_error=error,
            completed_at=datetime.now(UTC),
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id,
            event_type="SESSION_FAILED",
            message=exc.message,
            context=exc.context,
        )

    def _save_waiting_next_day(
        self,
        session: PaperTradingSession,
        *,
        trade_date: date,
        message: str,
        context: dict[str, Any],
    ) -> PaperTradingSession:
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=session.portfolio_id,
                trade_date=trade_date,
                status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
                phase=PaperSessionPhase.WAITING_NEXT_DAY,
                data_source=session.live_data_source or MinuteDataSource.TDX_REALTIME,
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            event_type="LIVE_WAITING_NEXT_TRADING_DAY",
            message=message,
            context=context,
        )
        self.repository.update_portfolio_status(session.portfolio_id, PortfolioStatus.RUNNING)
        return self.repository.update_session_status(
            session.session_id,
            status=PaperSessionStatus.LIVE_WAITING_NEXT_TRADING_DAY,
            phase=PaperSessionPhase.WAITING_NEXT_DAY,
            started_at=session.started_at or datetime.now(UTC),
        )

    def _save_live_day_cursor(
        self,
        session: PaperTradingSession,
        run: PaperRun,
        *,
        latest_available: datetime | None,
        last_processed: datetime | None,
    ) -> None:
        self.repository.save_session_day(
            PaperSessionDay(
                session_id=session.session_id,
                portfolio_id=session.portfolio_id,
                trade_date=run.trade_date,
                run_id=run.run_id,
                status=PaperSessionStatus.LIVE_WAITING_FOR_BAR,
                phase=PaperSessionPhase.LIVE_INTRADAY,
                data_source=run.data_source,
                latest_available_bar_time=latest_available,
                last_processed_bar_time=last_processed,
            )
        )

    def _signal_data_source(self, session: PaperTradingSession, *, portfolio_data_source: MinuteDataSource) -> str:
        session_opts = session.runtime_config.get("paper_v2_session") or {}
        explicit = session_opts.get("signal_data_source")
        if explicit:
            if explicit != MinuteDataSource.DB_HISTORICAL.value:
                raise SessionConfigError(
                    "live StrategyPackage signal generation currently supports only DB_HISTORICAL daily data",
                    context={"session_id": session.session_id, "signal_data_source": explicit},
                )
            return str(explicit)
        if session.historical_data_source is not None:
            return session.historical_data_source.value
        if portfolio_data_source == MinuteDataSource.DB_HISTORICAL:
            return portfolio_data_source.value
        raise SessionConfigError(
            "live Paper v2 session requires explicit paper_v2_session.signal_data_source=DB_HISTORICAL",
            context={"session_id": session.session_id, "portfolio_data_source": portfolio_data_source.value},
        )

    def _ensure_live_selection_cutoff(self, config: dict[str, Any], *, trade_date: date) -> None:
        artifact_config = config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = config.get("selection_artifact")
        if artifact_config is None:
            return
        if not isinstance(artifact_config, dict):
            raise SessionConfigError("selection_artifact_config must be an object")
        if artifact_config.get("cutoff_date"):
            return
        if not bool(artifact_config.get("auto_generate")):
            return
        lookup_start = trade_date - timedelta(days=31)
        previous_days = self.calendar_provider.list_trading_days(lookup_start, trade_date - timedelta(days=1))
        cutoff_date = previous_days[-1]
        artifact_config["cutoff_date"] = cutoff_date.isoformat()
        config.setdefault("paper_v2_session", {})["selection_cutoff_date"] = cutoff_date.isoformat()

    def _current_position_prices(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        as_of_time: datetime,
        live_data_source: MinuteDataSource | None,
    ) -> dict[str, float]:
        if not symbols:
            return {}
        return self._snapshot_prices_for_positions(
            symbols=symbols,
            trade_date=trade_date,
            as_of_time=as_of_time,
            live_data_source=live_data_source,
            known_prices={},
        )

    def _mark_to_market_without_active_orders(
        self,
        session: PaperTradingSession,
        run: PaperRun,
        *,
        portfolio,
        latest_available: datetime | None,
        current_last_processed: datetime | None,
        as_of_time: datetime,
    ) -> tuple[datetime | None, datetime | None]:
        current_positions = self.repository.load_latest_positions(portfolio.portfolio_id, run.trade_date)
        if not current_positions:
            return latest_available, current_last_processed
        latest_available = self._latest_available_time_for_symbols(
            symbols=list(current_positions),
            trade_date=run.trade_date,
            live_data_source=session.live_data_source,
            as_of_time=as_of_time,
        )
        if latest_available is None:
            return None, current_last_processed
        if current_last_processed is not None and latest_available <= current_last_processed:
            return latest_available, current_last_processed
        latest_cash = self.repository.load_latest_cash(portfolio, run.trade_date)
        prices = self._snapshot_prices_for_positions(
            symbols=list(current_positions),
            trade_date=run.trade_date,
            as_of_time=latest_available,
            live_data_source=session.live_data_source,
            known_prices={},
        )
        self.repository.save_positions(
            run_id=run.run_id,
            trade_date=run.trade_date,
            positions=list(current_positions.values()),
            prices=prices,
        )
        market_value = sum(position.quantity * prices[position.symbol] for position in current_positions.values())
        self.repository.save_intraday_snapshot(
            IntradaySnapshot(
                session_id=session.session_id,
                run_id=run.run_id,
                portfolio_id=portfolio.portfolio_id,
                trade_date=run.trade_date,
                snapshot_time=latest_available,
                cash=latest_cash,
                market_value=market_value,
                nav=latest_cash + market_value,
                positions=[item.model_dump(mode="json") for item in current_positions.values()],
                source=session.live_data_source.value if session.live_data_source else "TDX_REALTIME",
            )
        )
        self.repository.save_session_event(
            session_id=session.session_id,
            run_id=run.run_id,
            event_type="LIVE_MARK_TO_MARKET_SNAPSHOT",
            message="paper v2 live mark-to-market snapshot recorded without active orders",
            context={
                "trade_date": run.trade_date.isoformat(),
                "snapshot_time": latest_available.isoformat(),
                "position_count": len(current_positions),
                "nav": latest_cash + market_value,
            },
        )
        return latest_available, latest_available

    def _snapshot_prices_for_positions(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        as_of_time: datetime,
        live_data_source: MinuteDataSource | None,
        known_prices: dict[str, float],
    ) -> dict[str, float]:
        prices = dict(known_prices)
        for symbol in symbols:
            if symbol in prices:
                continue
            market_input = self.market_data_provider.load_observed_intraday(
                symbol=symbol,
                trade_date=trade_date,
                source=live_data_source or MinuteDataSource.TDX_REALTIME,
                until_time=as_of_time,
                require_suspend_status=True,
            )
            if not market_input.minute_bars:
                raise DataUnavailableError(
                    "live snapshot price requires at least one observed minute bar",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat(), "as_of_time": as_of_time.isoformat()},
                )
            prices[symbol] = market_input.minute_bars[-1].close
        return prices

    def _latest_available_time_for_states(
        self,
        states: list[OrderExecutionState],
        live_data_source: MinuteDataSource | None,
        as_of_time: datetime,
    ) -> datetime | None:
        symbols = sorted({state.symbol for state in states})
        if not symbols:
            return None
        return self.market_data_provider.latest_available_bar_time(
            symbols=symbols,
            trade_date=as_of_time.date(),
            source=live_data_source or MinuteDataSource.TDX_REALTIME,
            as_of_time=as_of_time,
        )

    def _latest_available_time_for_symbols(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        live_data_source: MinuteDataSource | None,
        as_of_time: datetime,
    ) -> datetime | None:
        normalized = sorted({str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()})
        if not normalized:
            return None
        return self.market_data_provider.latest_available_bar_time(
            symbols=normalized,
            trade_date=trade_date,
            source=live_data_source or MinuteDataSource.TDX_REALTIME,
            as_of_time=as_of_time,
        )

    @staticmethod
    def _live_causality_cursor(as_of_time: datetime) -> datetime:
        """Use the order creation tick as the strict lower bound for live fills."""

        if as_of_time.tzinfo is None:
            return as_of_time
        return as_of_time.astimezone(LIVE_SESSION_TZ).replace(tzinfo=None)

    @staticmethod
    def _bar_after_cursor(bar_time: datetime, cursor: datetime | None) -> bool:
        if cursor is None:
            return True
        lhs = bar_time.replace(tzinfo=None) if bar_time.tzinfo is not None else bar_time
        rhs = cursor.replace(tzinfo=None) if cursor.tzinfo is not None else cursor
        return lhs > rhs

    @staticmethod
    def _preserve_live_causality_metadata(
        previous_state: OrderExecutionState,
        updated_state: OrderExecutionState,
    ) -> OrderExecutionState:
        preserved_keys = {
            "live_causality_mode",
            "order_created_at",
            "strict_live_start_bar_time",
        }
        preserved = {
            key: value
            for key, value in (previous_state.algo_state or {}).items()
            if key in preserved_keys
        }
        if not preserved:
            return updated_state
        return updated_state.model_copy(update={"algo_state": {**updated_state.algo_state, **preserved}})

    @staticmethod
    def _max_processed(states: list[OrderExecutionState]) -> datetime | None:
        values = [state.last_processed_bar_time for state in states if state.last_processed_bar_time is not None]
        return max(values) if values else None

    @staticmethod
    def _catchup_replay_end(*, session: PaperTradingSession, as_of_time: datetime) -> date | None:
        as_of_date = as_of_time.date()
        replay_end = session.end_date or as_of_date
        if replay_end >= as_of_date and as_of_time.time() < MARKET_CLOSE:
            replay_end = as_of_date.fromordinal(as_of_date.toordinal() - 1)
        if replay_end < session.start_date:
            return None
        return replay_end

    @staticmethod
    def _is_after_market_close(as_of_time: datetime) -> bool:
        return as_of_time.time() >= MARKET_CLOSE

    @staticmethod
    def _fee_model_from_policy(policy: dict[str, Any]) -> FeeModel:
        return FeeModel(
            open_cost=float(policy.get("open_cost", FeeModel.open_cost)),
            close_cost=float(policy.get("close_cost", FeeModel.close_cost)),
            min_cost=float(policy.get("min_cost", FeeModel.min_cost)),
        )

    def _progress(self, session_id: str) -> PaperSessionProgress:
        session = self.repository.get_session(session_id)
        days = self.repository.list_session_days(session_id)
        current_day = days[-1] if days else None
        return PaperSessionProgress(
            session=session,
            current_trade_date=current_day.trade_date if current_day else None,
            last_processed_bar_time=current_day.last_processed_bar_time if current_day else None,
            latest_available_bar_time=current_day.latest_available_bar_time if current_day else None,
            next_expected_bar_time=None,
            day_count=len(days),
            events=self.repository.list_session_events(session_id, limit=100),
        )
