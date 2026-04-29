"""Incremental real-time Paper Trading v2 session execution.

This module adds live/replay-catchup orchestration around the existing strict
day-runner components. It does not call the closed-day runner for current-day
live work and it never switches data sources or algorithms implicitly.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider, TradeCalendarProvider
from backend.services.selection_center.runtime_profile import parse_selection_runtime_profile
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.runtime import RebalanceEngine, StrategyPackageRuntime, TargetPositionEngine
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    ExecutionAlgoError,
    InvalidStateTransitionError,
    SessionConfigError,
    TradingCoreError,
)
from backend.services.trading_core.execution_algo_capabilities import require_execution_algo_supports_mode
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import AccountSnapshot, OrderStatus, PositionLot, RunStatus
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


MARKET_CLOSE = time(15, 0)
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
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        replay_service: PaperTradingHistoricalReplay | None = None,
    ) -> None:
        self.repository = repository or PaperTradingV2Repository()
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
            tradability_filter=self.tradability_filter,
            refresh_audit=self.refresh_audit,
        )
        self.replay_service = replay_service or PaperTradingHistoricalReplay(
            repository=self.repository,
            calendar_provider=self.calendar_provider,
        )

    def tick(self, session: PaperTradingSession, *, as_of_time: datetime | None = None) -> PaperSessionProgress:
        now = as_of_time or datetime.now()
        if session.mode == PaperSessionMode.CATCHUP_THEN_LIVE:
            session = self._run_historical_catchup(session, as_of_time=now)
        return self._tick_live_intraday(session, as_of_time=now)

    @staticmethod
    def _manual_tick_only(session: PaperTradingSession) -> bool:
        return bool((session.runtime_config.get("paper_v2_session") or {}).get("manual_tick_only"))

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
        replay_end = self._catchup_replay_end(session=session, as_of_date=as_of_time.date())
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
        if session.live_data_source != MinuteDataSource.TDX_REALTIME:
            raise SessionConfigError(
                "live Paper v2 sessions require TDX_REALTIME live_data_source",
                context={"session_id": session.session_id, "live_data_source": str(session.live_data_source)},
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
        )
        config = dict(session.runtime_config)
        config["validated_execution_policy"] = execution_policy_context
        config.setdefault("paper_v2_session", {})
        config["paper_v2_session"]["signal_data_source"] = self._signal_data_source(session, portfolio_data_source=portfolio.data_source)
        config["paper_v2_session"]["live_step_mode"] = capability.live_step_mode
        config["paper_v2_session"]["live_data_source"] = session.live_data_source.value if session.live_data_source else None
        self._ensure_live_selection_cutoff(config, trade_date=trade_date)

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
        runtime_profile = parse_selection_runtime_profile(config)
        top_k = int(runtime_profile.selection.top_k or manifest.portfolio_policy.topk)
        raw_candidate_count = len(snapshot.candidates)
        if not snapshot.valid_no_candidate and (
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
        targets = self.target_engine.build_targets(snapshot=snapshot, total_equity=total_equity, top_k=top_k)
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
        self.repository.create_run(run)
        self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="LIVE_RUN_PREPARED",
            message="paper v2 live run prepared orders from StrategyPackage signal",
            context={
                "session_id": session.session_id,
                "raw_candidate_count": raw_candidate_count,
                "target_count": len(targets),
                "order_intent_count": len(intents),
                "data_ready": ready,
                "signal_data_source": signal_data_source,
                "live_data_source": run.data_source.value,
                "algo_code": execution_policy_json.get("algo_code"),
                "live_step_mode": capability.live_step_mode,
            },
        )
        if not intents:
            if not current_positions:
                raise StrategyPackageValidationError(
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
            self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
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
                    },
                    filled_quantity=0,
                    remaining_quantity=order.quantity,
                    status=order.status.value,
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
        require_day_features = capability.algo_code == "V25_TWO_STAGE"
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
            self._save_live_day_cursor(session, run, latest_available=latest_available, last_processed=self._max_processed(states))
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
                if state.last_processed_bar_time is None or bar.bar_time > state.last_processed_bar_time
            ]
            if not new_bars:
                continue
            processed_any_bar = True
            market_context = dict(market_input.market_context)
            market_context.update(
                {
                    "live_step_mode": capability.live_step_mode,
                    "plan_horizon_bars": capability.plan_horizon_bars,
                    "v25_realtime_streaming": capability.algo_code == "V25_TWO_STAGE",
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
            for fill in fills:
                ledger.apply_fill(fill)
                self.repository.save_fill(run.run_id, fill)
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
        self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
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

    @staticmethod
    def _max_processed(states: list[OrderExecutionState]) -> datetime | None:
        values = [state.last_processed_bar_time for state in states if state.last_processed_bar_time is not None]
        return max(values) if values else None

    @staticmethod
    def _catchup_replay_end(*, session: PaperTradingSession, as_of_date: date) -> date | None:
        replay_end = session.end_date or as_of_date
        if replay_end >= as_of_date:
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
