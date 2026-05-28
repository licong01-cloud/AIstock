"""Authoritative single-day Paper Trading v2 runner."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from typing import Any, Callable

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider, TradeCalendarProvider
from backend.services.selection_center.risk_policy import StockRiskPolicyService
from backend.services.selection_center.runtime_profile import (
    parse_selection_runtime_profile,
    validate_runtime_profile_binding,
)
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.backtest_contract import (
    normalize_runtime_config_with_backtest_contract,
    validate_runtime_profile_matches_backtest_contract,
)
from backend.services.strategy_package.runtime import (
    RebalanceEngine,
    StrategyPackageRuntime,
    TargetPositionEngine,
    _candidate_selection_artifact_runtime_hashes,
    apply_runtime_variant_to_manifest,
)
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
)
from backend.services.strategy_package.repository import InMemoryStrategyPackageRepository
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    DataUnavailableError,
    InvalidStateTransitionError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    TradingCoreError,
)
from backend.services.trading_core.execution_algo_capabilities import required_minute_bars_for_policy
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import (
    AccountSnapshot,
    Fill,
    OrderEvent,
    OrderEventType,
    OrderIntent,
    OrderSide,
    OrderStatus,
    PositionLot,
    RunStatus,
)
from backend.services.trading_core.oms import OMS

from .broker import MiniQMTSimBackend
from .models import OrderExecutionState, PaperDayRunResult, PaperRun, PortfolioStatus
from .repository import PaperTradingV2Repository
from .risk_targets import overlay_risk_forced_exit_targets
from .service import PaperTradingV2PortfolioService


def _in_memory_package_repository_from_portfolios(repository: Any | None) -> Any | None:
    """Let in-memory Paper tests resolve frozen package manifests without DB."""

    portfolios = getattr(repository, "portfolios", None)
    if not isinstance(portfolios, dict):
        return None
    package_repository = InMemoryStrategyPackageRepository()
    for portfolio in portfolios.values():
        manifest = getattr(portfolio, "frozen_manifest", None)
        if manifest is not None:
            package_repository.save_manifest(manifest)
    return package_repository


class PaperTradingDayRunner:
    """Run one trading day from frozen StrategyPackage to persisted ledger."""

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
        package_repository: Any | None = None,
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
        risk_policy_service: StockRiskPolicyService | Any | None = None,
        minqmt_broker_factory: Callable[..., MiniQMTSimBackend] | None = None,
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
        self.package_repository = package_repository or _in_memory_package_repository_from_portfolios(repository)
        self.tradability_filter = tradability_filter or TradabilityFilter()
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository()
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
            package_repository=self.package_repository,
        )
        self.risk_policy_service = risk_policy_service or StockRiskPolicyService()
        self.minqmt_broker_factory = minqmt_broker_factory or MiniQMTSimBackend

    def run_day(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
        runtime_config: dict[str, Any] | None = None,
        fee_model: FeeModel | None = None,
    ) -> PaperDayRunResult:
        portfolio = self.repository.get_portfolio(portfolio_id)
        if portfolio.status != PortfolioStatus.READY:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY before running a day",
                context={"portfolio_id": portfolio_id, "status": portfolio.status.value},
            )
        if trade_date < portfolio.start_date:
            raise InvalidStateTransitionError(
                "paper run trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        manifest = portfolio.frozen_manifest
        if manifest.package_id != portfolio.package_id or manifest.manifest_sha256 != portfolio.manifest_sha256:
            raise PackageAssetInvalidError(
                "portfolio frozen manifest does not match frozen package invariants",
                context={"portfolio_id": portfolio_id, "package_id": portfolio.package_id},
            )
        self.validator.validate_manifest_identity_for_paper_trading(manifest)
        self.calendar_provider.ensure_trading_day(trade_date)
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise InvalidStateTransitionError(
                "paper v2 run already exists for portfolio trade_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "existing_run_id": existing_run.run_id,
                    "existing_status": existing_run.status.value,
                },
            )

        config = PaperTradingV2PortfolioService(
            package_repository=self.package_repository,
            repository=self.repository,
        ).resolve_runtime_config_for_date(
            portfolio=portfolio,
            trade_date=trade_date,
            runtime_config=runtime_config or {},
        )
        config = normalize_runtime_config_with_backtest_contract(
            manifest,
            config,
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "day_runner"},
            include_contract=True,
        )
        validate_runtime_profile_binding(
            config,
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "day_runner"},
        )
        runtime_profile = parse_selection_runtime_profile(config)
        self._reject_raw_execution_overrides(config)
        execution_policy_context = self._execution_policy_context_for_date(portfolio, trade_date)
        execution_policy_json = execution_policy_context["policy_json"]
        self.validator.validate_execution_policy_for_paper(
            package_id=manifest.package_id,
            policy_json=execution_policy_json,
            instantiate_runtime=False,
            require_runtime_assets=False,
        )
        runtime_contract = validate_runtime_profile_matches_backtest_contract(
            manifest,
            runtime_profile,
            runtime_config=config,
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
        )
        effective_manifest = apply_runtime_variant_to_manifest(manifest, config)
        execution_algo_config = dict(execution_policy_json.get("algo_config") or {})
        config["validated_execution_policy"] = execution_policy_context
        config["qe_backtest_runtime_contract"] = runtime_contract
        run = PaperRun(
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            status=RunStatus.RUNNING,
            data_source=portfolio.data_source,
            runtime_config=config,
        )
        self.repository.create_run(run)
        self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.RUNNING)
        self.repository.save_run_event(run_id=run.run_id, event_type="RUN_STARTED", message="paper v2 day run started")

        minqmt_broker: MiniQMTSimBackend | None = None
        try:
            if portfolio.broker_backend == "minqmt_sim":
                data_ready = self._require_minqmt_data_ready(
                    trade_date=trade_date,
                    runtime_config=config,
                )
            else:
                data_ready = self._require_data_ready(
                    manifest=manifest,
                    trade_date=trade_date,
                    runtime_config=config,
                    execution_policy_json=execution_policy_json,
                )
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="DATA_READY",
                message="required paper v2 datasets are ready",
                context={"datasets": data_ready},
            )
            if portfolio.broker_backend == "minqmt_sim":
                minqmt_broker = self.minqmt_broker_factory(
                    portfolio_id=portfolio.portfolio_id,
                    package_id=manifest.package_id,
                    data_source=MinuteDataSource.MINIQMT_REALTIME,
                    strategy_slot_id=portfolio.portfolio_id,
                )
                broker_account = minqmt_broker.query_account()
                current_positions, current_prices = minqmt_broker.query_position_marks()
                latest_cash = float(broker_account.cash)
                total_equity = float(broker_account.nav)
                if current_prices:
                    config["current_prices"] = current_prices
                    config["current_price_context"] = {
                        symbol: {"price": price, "source": "MINIQMT_QUERY", "basis": "broker_position_mark"}
                        for symbol, price in current_prices.items()
                    }
                    run = self.repository.update_run_runtime_config(run, config)
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MINIQMT_ACCOUNT_SNAPSHOT_LOADED",
                    message="MiniQMT account and positions loaded as broker authority",
                    context={
                        "cash": latest_cash,
                        "nav": total_equity,
                        "position_count": len(current_positions),
                    },
                )
            else:
                current_positions = self.repository.load_latest_positions(portfolio_id, trade_date)
                latest_cash = self.repository.load_latest_cash(portfolio, trade_date)
                if self._ensure_current_prices_for_existing_positions(
                    config=config,
                    current_positions=current_positions,
                    trade_date=trade_date,
                    data_source=portfolio.data_source,
                    run_id=run.run_id,
                ):
                    run = self.repository.update_run_runtime_config(run, config)
                total_equity = self._resolve_total_equity(
                    latest_cash=latest_cash,
                    current_positions=current_positions,
                    runtime_config=config,
                    portfolio_id=portfolio_id,
                )
            selection_data_source = self._selection_data_source(portfolio, config)
            self._ensure_authoritative_selection_artifact(
                manifest=manifest,
                trade_date=trade_date,
                data_source=selection_data_source,
                runtime_config=config,
            )
            snapshot = self.runtime.build_signal_snapshot(
                manifest=manifest,
                trade_date=trade_date,
                data_source=selection_data_source,
                runtime_config=config,
            )
            raw_candidate_count = len(snapshot.candidates)
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="SIGNAL_GENERATED",
                message="strategy package runtime generated signal snapshot",
                context={
                    "package_id": manifest.package_id,
                    "manifest_sha256": manifest.manifest_sha256,
                    "snapshot_id": snapshot.snapshot_id,
                    "candidate_count": raw_candidate_count,
                    "valid_no_candidate": snapshot.valid_no_candidate,
                },
            )
            top_k = self._require_runtime_top_k(runtime_profile, manifest)
            risk_decisions = self.risk_policy_service.evaluate(
                symbols=sorted(set(item.symbol for item in snapshot.candidates) | set(current_positions)),
                trade_date=trade_date,
                profile=runtime_profile.risk_policy,
                current_positions=current_positions,
            )
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
                if runtime_profile.risk_policy.enabled:
                    self.repository.save_run_event(
                        run_id=run.run_id,
                        event_type="RISK_POLICY_APPLIED",
                        message="event risk policy applied to signal candidates and current positions",
                        context={
                            "raw_candidate_count": raw_candidate_count,
                            "risk_adjusted_candidate_count": len(risk_adjusted),
                            "excluded_count": len(risk_excluded),
                            "excluded_symbols": [item.symbol for item in risk_excluded],
                            "force_exit_symbols": [
                                symbol for symbol, decision in risk_decisions.items() if decision.force_exit
                            ],
                            "runtime_profile": runtime_profile.risk_policy.model_dump(mode="json"),
                        },
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
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="TRADABILITY_FILTERED",
                    message="runtime tradability filters applied to signal candidates",
                    context={
                        "raw_candidate_count": raw_candidate_count,
                        "tradable_candidate_count": len(tradable),
                        "excluded_count": len(excluded),
                        "excluded_symbols": [item.symbol for item in excluded],
                        "exclusion_reasons": sorted({item.reason for item in excluded}),
                        "runtime_profile": runtime_profile.model_dump(mode="json"),
                    },
                )
            targets = (
                self.target_engine.build_targets(
                    snapshot=snapshot,
                    total_equity=total_equity,
                    top_k=top_k,
                    manifest=effective_manifest,
                    current_positions=current_positions,
                    current_prices=config.get("current_prices") or {},
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
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="TARGETS_GENERATED",
                message="target positions generated from signal snapshot",
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
            intents = self.rebalance_engine.build_order_intents(
                package_id=manifest.package_id,
                portfolio_id=portfolio_id,
                trade_date=trade_date,
                current_positions=current_positions,
                target_positions=targets,
            )
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="ORDER_INTENTS_GENERATED",
                message="order intents generated from target/current position diff",
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
            if portfolio.broker_backend == "minqmt_sim":
                return self._run_minqmt_sim_orders(
                    portfolio=portfolio,
                    run=run,
                    manifest=manifest,
                    trade_date=trade_date,
                    intents=intents,
                    broker=minqmt_broker,
                )

            ledger = InMemoryLedger(
                portfolio_id=portfolio_id,
                initial_cash=float(portfolio.initial_cash),
                fee_model=fee_model or self._fee_model_from_policy(portfolio.fee_policy),
            )
            ledger.cash = latest_cash
            ledger.positions = dict(current_positions)
            ledger.settle_trade_date(trade_date)
            if not intents:
                if not ledger.positions:
                    raise ArtifactGenerationFailedError(
                        "rebalance produced no order intents and portfolio has no positions to mark",
                        context={"portfolio_id": portfolio_id, "run_id": run.run_id, "trade_date": trade_date.isoformat()},
                    )
                snapshot_prices, snapshot_time = self._load_snapshot_marks_for_held_positions(
                    symbols=list(ledger.positions),
                    trade_date=trade_date,
                    data_source=portfolio.data_source,
                    run_id=run.run_id,
                )
                account_snapshot = ledger.account_snapshot(prices=snapshot_prices, snapshot_time=snapshot_time)
                position_list = list(ledger.positions.values())
                self.repository.save_positions(
                    run_id=run.run_id,
                    trade_date=trade_date,
                    positions=position_list,
                    prices=snapshot_prices,
                )
                self.repository.save_daily_snapshot(
                    run_id=run.run_id,
                    trade_date=trade_date,
                    snapshot=account_snapshot,
                    metadata={
                        "position_count": len(position_list),
                        "order_count": 0,
                        "fill_count": 0,
                        "no_rebalance_required": True,
                        "reason": "target_positions_equal_current_positions",
                    },
                )
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="NO_REBALANCE_REQUIRED",
                    message="target positions match current positions; persisted mark-to-market snapshot without orders",
                    context={"position_count": len(position_list), "snapshot_time": snapshot_time.isoformat()},
                )
                succeeded = self.repository.update_run_status(run, RunStatus.SUCCEEDED)
                ready_portfolio = self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.READY)
                self.repository.save_run_event(run_id=run.run_id, event_type="RUN_SUCCEEDED", message="paper v2 no-rebalance day run succeeded")
                return PaperDayRunResult(
                    portfolio=ready_portfolio,
                    run=succeeded,
                    orders=[],
                    fills=[],
                    events=[],
                    positions=position_list,
                    account_snapshot=account_snapshot,
                )

            orders = []
            fills = []
            events = []
            snapshot_prices: dict[str, float] = {}
            required_bars = self._required_minute_bars_for_policy(
                execution_policy_json,
                package_id=manifest.package_id,
            )
            require_day_features = self._policy_requires_day_features(execution_policy_json)

            for intent in intents:
                market_input = self.market_data_provider.load_symbol_input(
                    symbol=intent.symbol,
                    trade_date=trade_date,
                    source=portfolio.data_source,
                    min_bars=required_bars,
                    require_suspend_status=True,
                    require_day_features=require_day_features,
                )
                if not market_input.minute_bars:
                    raise DataUnavailableError(
                        "market data provider returned no minute bars",
                        context={"portfolio_id": portfolio_id, "symbol": intent.symbol, "trade_date": trade_date.isoformat()},
                    )
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MARKET_DATA_LOADED",
                    message="minute market data loaded for order intent",
                    context={
                        "symbol": intent.symbol,
                        "data_source": portfolio.data_source.value,
                        "bar_count": len(market_input.minute_bars),
                        "required_bars": required_bars,
                        "prev_close": market_input.market_context.get("prev_close"),
                        "limit_up": market_input.market_context.get("limit_up"),
                        "limit_down": market_input.market_context.get("limit_down"),
                        "suspend_status": market_input.market_context.get("suspend_status"),
                        "day_features_schema_version": market_input.market_context.get("day_features_schema_version"),
                        "day_features_trade_date": market_input.market_context.get("day_features_trade_date"),
                    },
                )
                order = self.oms.create_order(intent)
                self.repository.save_order(run.run_id, order)
                final_order, order_fills, order_events = self.execution_engine.execute_order(
                    order=order,
                    minute_bars=market_input.minute_bars,
                    algo_code=str(execution_policy_json["algo_code"]),
                    algo_config=execution_algo_config,
                    market_context=market_input.market_context,
                    allow_partial_fill=bool(execution_algo_config.get("allow_partial_fill", True)),
                )
                # T6.1 capture wiring: intended_price from OrderIntent.limit_price
                # (None for MARKET orders — that's structurally accurate, not a gap),
                # fill_market_context from the same dict that the execution engine
                # consumed, so the saved snapshot matches the matching context.
                intended_price = intent.limit_price
                fill_market_context = dict(market_input.market_context)
                for fill in order_fills:
                    ledger.apply_fill(fill)
                    self.repository.save_fill(
                        run.run_id,
                        fill,
                        intended_price=intended_price,
                        fill_market_context=fill_market_context,
                    )
                for event in order_events:
                    self.repository.save_order_event(run.run_id, event)
                self.repository.save_order(run.run_id, final_order)
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="ORDER_EXECUTED",
                    message="minute execution completed for order",
                    context={
                        "order_id": final_order.order_id,
                        "symbol": final_order.symbol,
                        "status": final_order.status.value,
                        "filled_quantity": final_order.filled_quantity,
                        "avg_fill_price": final_order.avg_fill_price,
                        "fill_count": len(order_fills),
                        "order_event_count": len(order_events),
                    },
                )
                orders.append(final_order)
                fills.extend(order_fills)
                events.extend(order_events)
                snapshot_prices[intent.symbol] = market_input.minute_bars[-1].close

            if not fills:
                raise ArtifactGenerationFailedError(
                    "paper v2 day run produced no fills; no-trade day is not yet modeled as a successful state",
                    context={
                        "portfolio_id": portfolio_id,
                        "run_id": run.run_id,
                        "trade_date": trade_date.isoformat(),
                        "order_count": len(orders),
                        "order_event_count": len(events),
                    },
                )
            missing_snapshot_symbols = [symbol for symbol in ledger.positions if symbol not in snapshot_prices]
            if missing_snapshot_symbols:
                snapshot_prices.update(
                    self._load_snapshot_prices_for_held_positions(
                        symbols=missing_snapshot_symbols,
                        trade_date=trade_date,
                        data_source=portfolio.data_source,
                        run_id=run.run_id,
                    )
                )
            account_snapshot = ledger.account_snapshot(
                prices=snapshot_prices,
                snapshot_time=max(bar_time for bar_time in [fill.trade_time for fill in fills]),
            )
            for entry in ledger.cash_entries:
                self.repository.save_cash_entry(run.run_id, entry)
            position_list = list(ledger.positions.values())
            self.repository.save_positions(
                run_id=run.run_id,
                trade_date=trade_date,
                positions=position_list,
                prices=snapshot_prices,
            )
            self.repository.save_daily_snapshot(
                run_id=run.run_id,
                trade_date=trade_date,
                snapshot=account_snapshot,
                metadata={"position_count": len(position_list), "order_count": len(orders), "fill_count": len(fills)},
            )
            succeeded = self.repository.update_run_status(run, RunStatus.SUCCEEDED)
            ready_portfolio = self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.READY)
            self.repository.save_run_event(run_id=run.run_id, event_type="RUN_SUCCEEDED", message="paper v2 day run succeeded")
            return PaperDayRunResult(
                portfolio=ready_portfolio,
                run=succeeded,
                orders=orders,
                fills=fills,
                events=events,
                positions=position_list,
                account_snapshot=account_snapshot,
            )
        except TradingCoreError as exc:
            if minqmt_broker is not None:
                minqmt_broker.shutdown()
            error = exc.to_dict()
            self.repository.save_error(run_id=run.run_id, portfolio_id=portfolio_id, error=error)
            failed = self.repository.update_run_status(run, RunStatus.FAILED, error=error)
            self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.FAILED)
            self.repository.save_run_event(run_id=run.run_id, event_type="RUN_FAILED", message=exc.message, context=exc.context)
            run = failed
            raise
        except Exception as exc:
            if minqmt_broker is not None:
                minqmt_broker.shutdown()
            error = {"error_code": "PAPER_V2_RUN_ERROR", "message": str(exc), "context": {"portfolio_id": portfolio_id, "run_id": run.run_id}}
            self.repository.save_error(run_id=run.run_id, portfolio_id=portfolio_id, error=error)
            self.repository.update_run_status(run, RunStatus.FAILED, error=error)
            self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.FAILED)
            self.repository.save_run_event(run_id=run.run_id, event_type="RUN_FAILED", message=str(exc), context=error["context"])
            raise

    def _require_data_ready(
        self,
        *,
        manifest: Any,
        trade_date: date,
        runtime_config: dict[str, Any],
        execution_policy_json: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        ready: list[dict[str, Any]] = []
        if execution_policy_json is None:
            raise RuntimeConfigInvalidError(
                "Paper v2 data readiness requires a validated execution policy snapshot",
                context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
            )
        requirements = self._data_requirements_for_policy(
            execution_policy_json,
            package_id=manifest.package_id,
        )
        runtime_profile = parse_selection_runtime_profile(runtime_config)
        if requirements["requires_suspend_status"] or runtime_profile.tradability.exclude_suspended:
            status = self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
            ready.append(self._refresh_status_context("suspend_d", status))
        if requirements["requires_limit_price"]:
            status = self.refresh_audit.require_success(dataset="stk_limit", trade_date=trade_date)
            ready.append(self._refresh_status_context("stk_limit", status))
        return ready

    def _require_minqmt_data_ready(
        self,
        *,
        trade_date: date,
        runtime_config: dict[str, Any],
    ) -> list[dict[str, Any]]:
        ready: list[dict[str, Any]] = [
            {
                "dataset": "miniqmt_account",
                "trade_date": trade_date.isoformat(),
                "data_source": MinuteDataSource.MINIQMT_REALTIME.value,
                "status": "broker_query_required",
            }
        ]
        runtime_profile = parse_selection_runtime_profile(runtime_config)
        if runtime_profile.tradability.exclude_suspended:
            status = self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
            ready.append(self._refresh_status_context("suspend_d", status))
        return ready

    @staticmethod
    def _require_runtime_top_k(runtime_profile: Any, manifest: Any) -> int:
        top_k = runtime_profile.selection.top_k
        if top_k is None:
            raise RuntimeConfigInvalidError(
                "Paper v2 runtime_profile.selection.top_k is required; StrategyPackage manifest cannot provide runtime top_k",
                context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
            )
        return int(top_k)

    @staticmethod
    def _selection_data_source(portfolio: Any, runtime_config: dict[str, Any]) -> str:
        explicit = (
            (runtime_config.get("selection_artifact_config") or {}).get("signal_data_source")
            or runtime_config.get("signal_data_source")
        )
        if explicit:
            return str(explicit)
        if portfolio.broker_backend == "minqmt_sim":
            return MinuteDataSource.DB_HISTORICAL.value
        return portfolio.data_source.value

    def _ensure_authoritative_selection_artifact(
        self,
        *,
        manifest: Any,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> None:
        artifact_config = runtime_config.get("selection_artifact_config")
        if artifact_config is None:
            artifact_config = runtime_config.get("selection_artifact")
        if not isinstance(artifact_config, dict) or not bool(artifact_config.get("auto_generate")):
            return
        cutoff_date = self._parse_selection_cutoff_date(artifact_config, trade_date=trade_date)
        force_regenerate = bool(artifact_config.get("force_regenerate"))
        artifact_repository = getattr(self.runtime, "artifact_repository", None)
        if artifact_repository is not None and not force_regenerate:
            for runtime_hash in _candidate_selection_artifact_runtime_hashes(runtime_config):
                try:
                    artifact = artifact_repository.get(
                        package_id=manifest.package_id,
                        manifest_sha256=manifest.manifest_sha256 or "",
                        trade_date=trade_date,
                        data_source=data_source,
                        runtime_config_hash=runtime_hash,
                    )
                    metadata = artifact.metadata or {}
                    if (
                        artifact.status.value == "SUCCEEDED"
                        and artifact.scores_json
                        and metadata.get("source_type") == AUTHORITATIVE_SELECTION_SOURCE_TYPE
                        and metadata.get("authority_scope") == AUTHORITATIVE_SELECTION_SCOPE
                    ):
                        return
                except DataUnavailableError:
                    pass
        self.selection_artifact_service.generate_from_live_inference(
            package_id=manifest.package_id,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            include_reference_price=True,
            cutoff_date=cutoff_date,
        )

    @staticmethod
    def _parse_selection_cutoff_date(artifact_config: dict[str, Any], *, trade_date: date) -> date | None:
        raw = artifact_config.get("cutoff_date")
        if raw is None or raw == "":
            return None
        try:
            parsed = date.fromisoformat(str(raw))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "selection_artifact_config.cutoff_date must be YYYY-MM-DD",
                context={"cutoff_date": raw},
            ) from exc
        if parsed > trade_date:
            raise RuntimeConfigInvalidError(
                "selection_artifact_config.cutoff_date cannot be after trade_date",
                context={"trade_date": trade_date.isoformat(), "cutoff_date": parsed.isoformat()},
            )
        return parsed

    @staticmethod
    def _refresh_status_context(dataset: str, status: Any) -> dict[str, Any]:
        if status is None:
            return {"dataset": dataset, "audit_status": "provider_did_not_return_status"}
        return {
            "dataset": getattr(status, "dataset", dataset),
            "trade_date": getattr(status, "trade_date").isoformat() if getattr(status, "trade_date", None) else None,
            "data_source": getattr(status, "data_source", None),
            "status": getattr(status, "status", None),
            "row_count": getattr(status, "row_count", None),
            "refreshed_at": getattr(status, "refreshed_at").isoformat() if getattr(status, "refreshed_at", None) else None,
        }

    def _resolve_total_equity(
        self,
        *,
        latest_cash: float,
        current_positions: dict[str, PositionLot],
        runtime_config: dict[str, Any],
        portfolio_id: str,
    ) -> float:
        configured = runtime_config.get("total_equity")
        if configured is not None:
            total = float(configured)
            if total <= 0:
                raise RuntimeConfigInvalidError("runtime_config.total_equity must be positive")
            return total
        prices = runtime_config.get("current_prices") or {}
        if not current_positions:
            return float(latest_cash)
        market_value = 0.0
        for symbol, position in current_positions.items():
            price = prices.get(symbol)
            if price is None or float(price) <= 0:
                raise DataUnavailableError(
                    "current price is required for existing position equity",
                    context={"portfolio_id": portfolio_id, "symbol": symbol},
                )
            market_value += position.quantity * float(price)
        return float(latest_cash) + market_value

    def _ensure_current_prices_for_existing_positions(
        self,
        *,
        config: dict[str, Any],
        current_positions: dict[str, PositionLot],
        trade_date: date,
        data_source: MinuteDataSource,
        run_id: str,
    ) -> bool:
        if not current_positions or config.get("current_prices"):
            return False
        if data_source != MinuteDataSource.DB_HISTORICAL:
            return False

        prices: dict[str, float] = {}
        price_context: dict[str, Any] = {}
        for symbol in sorted(current_positions):
            market_input = self.market_data_provider.load_symbol_input(
                symbol=symbol,
                trade_date=trade_date,
                source=MinuteDataSource.DB_HISTORICAL,
                min_bars=1,
                require_suspend_status=True,
                require_day_features=False,
            )
            if not market_input.minute_bars:
                raise DataUnavailableError(
                    "historical replay current position price requires at least one DB minute bar",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": data_source.value},
                )
            first_bar = market_input.minute_bars[0]
            prices[symbol] = first_bar.close
            price_context[symbol] = {
                "price": first_bar.close,
                "bar_time": first_bar.bar_time.isoformat(),
                "source": data_source.value,
                "basis": "first_observed_minute_close",
            }

        config["current_prices"] = prices
        config["current_price_context"] = price_context
        self.repository.save_run_event(
            run_id=run_id,
            event_type="CURRENT_POSITION_PRICES_LOADED",
            message="historical DB minute prices loaded for existing position equity",
            context={
                "trade_date": trade_date.isoformat(),
                "symbol_count": len(prices),
                "basis": "first_observed_minute_close",
                "data_source": data_source.value,
            },
        )
        return True

    def _load_snapshot_prices_for_held_positions(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        data_source: MinuteDataSource,
        run_id: str,
    ) -> dict[str, float]:
        prices, _snapshot_time = self._load_snapshot_marks_for_held_positions(
            symbols=symbols,
            trade_date=trade_date,
            data_source=data_source,
            run_id=run_id,
        )
        return prices

    def _load_snapshot_marks_for_held_positions(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        data_source: MinuteDataSource,
        run_id: str,
    ) -> tuple[dict[str, float], datetime]:
        prices: dict[str, float] = {}
        context_rows: dict[str, Any] = {}
        snapshot_times: list[datetime] = []
        for symbol in sorted(symbols):
            market_input = self.market_data_provider.load_symbol_input(
                symbol=symbol,
                trade_date=trade_date,
                source=data_source,
                min_bars=1,
                require_suspend_status=True,
                require_day_features=False,
            )
            if not market_input.minute_bars:
                raise DataUnavailableError(
                    "snapshot price is required for held position",
                    context={"symbol": symbol, "trade_date": trade_date.isoformat(), "source": data_source.value},
                )
            last_bar = market_input.minute_bars[-1]
            prices[symbol] = last_bar.close
            snapshot_times.append(last_bar.bar_time)
            context_rows[symbol] = {
                "price": last_bar.close,
                "bar_time": last_bar.bar_time.isoformat(),
                "source": data_source.value,
                "basis": "latest_available_minute_close",
            }
        self.repository.save_run_event(
            run_id=run_id,
            event_type="HELD_POSITION_SNAPSHOT_PRICES_LOADED",
            message="minute prices loaded for held positions without same-day order market data",
            context={
                "trade_date": trade_date.isoformat(),
                "symbol_count": len(prices),
                "basis": "latest_available_minute_close",
                "data_source": data_source.value,
                "prices": context_rows,
            },
        )
        if not snapshot_times:
            raise DataUnavailableError(
                "snapshot price marks require at least one held position",
                context={"trade_date": trade_date.isoformat(), "source": data_source.value},
            )
        return prices, max(snapshot_times)

    @staticmethod
    def _required_minute_bars_for_manifest(manifest) -> int:
        raise RuntimeConfigInvalidError(
            "Paper v2 requires a validated execution policy snapshot; manifest minute policy is not runtime authority",
            context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
        )

    @staticmethod
    def _required_minute_bars_for_policy(policy_json: dict[str, Any], *, package_id: str) -> int:
        return required_minute_bars_for_policy(policy_json, package_id=package_id)

    @staticmethod
    def _policy_requires_day_features(policy_json: dict[str, Any]) -> bool:
        return str(policy_json.get("algo_code") or "").strip().upper() in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"}

    @staticmethod
    def _data_requirements_for_policy(policy_json: dict[str, Any], *, package_id: str) -> dict[str, bool]:
        requirements = policy_json.get("data_requirements")
        if not isinstance(requirements, dict):
            raise RuntimeConfigInvalidError(
                "validated execution policy requires data_requirements",
                context={"package_id": package_id, "algo_code": policy_json.get("algo_code")},
            )
        required_keys = {
            "requires_minute_bar",
            "requires_limit_price",
            "requires_trade_calendar",
            "requires_suspend_status",
        }
        missing = sorted(key for key in required_keys if key not in requirements)
        if missing:
            raise RuntimeConfigInvalidError(
                "validated execution policy data_requirements are incomplete",
                context={"package_id": package_id, "algo_code": policy_json.get("algo_code"), "missing_keys": missing},
            )
        return {key: bool(requirements.get(key)) for key in required_keys}

    def _run_minqmt_sim_orders(
        self,
        *,
        portfolio: Any,
        run: PaperRun,
        manifest: Any,
        trade_date: date,
        intents: list[Any],
        broker: MiniQMTSimBackend | None = None,
    ) -> PaperDayRunResult:
        broker = broker or self.minqmt_broker_factory(
            portfolio_id=portfolio.portfolio_id,
            package_id=manifest.package_id,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            strategy_slot_id=portfolio.portfolio_id,
        )
        orders = []
        fills = []
        events = []
        session_id = self._miniqmt_session_id_from_run(run)
        try:
            if not intents:
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MINIQMT_NO_ORDER_INTENTS",
                    message="no order intents generated; MiniQMT account snapshot will be reconciled without local fills",
                    context={
                        "portfolio_id": portfolio.portfolio_id,
                        "trade_date": trade_date.isoformat(),
                        "broker_backend": "minqmt_sim",
                    },
                )
            ordered_intents = self._miniqmt_order_submission_sequence(intents)
            if ordered_intents:
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MINIQMT_ORDER_SUBMISSION_SEQUENCE",
                    message="MiniQMT order intents sequenced sell-before-buy to release broker cash before buy legs",
                    context={
                        "portfolio_id": portfolio.portfolio_id,
                        "trade_date": trade_date.isoformat(),
                        "broker_backend": "minqmt_sim",
                        "sequence": [
                            {
                                "intent_id": intent.intent_id,
                                "symbol": intent.symbol,
                                "side": intent.side.value,
                                "quantity": intent.quantity,
                            }
                            for intent in ordered_intents
                        ],
                    },
                )
            for intent in ordered_intents:
                order = self.oms.create_order(intent)
                try:
                    handle = broker.submit_order_intent(intent)
                    native = broker.order_context(handle)
                    status = broker.query_status(handle)
                except TradingCoreError as exc:
                    final_order, event = self.oms.reject_order(order, exc.message)
                    metadata = dict(final_order.metadata or {})
                    metadata.update(
                        {
                            "broker_backend": "minqmt_sim",
                            "authority_source": "MINIQMT",
                            "broker_error": exc.to_dict(),
                        }
                    )
                    final_order = final_order.model_copy(update={"metadata": metadata})
                    self.repository.save_order(run.run_id, final_order)
                    self.repository.save_order_event(run.run_id, event)
                    orders.append(final_order)
                    events.append(event)
                    raise

                metadata = dict(order.metadata or {})
                metadata.update(
                    {
                        "broker_backend": "minqmt_sim",
                        "authority_source": "MINIQMT",
                        "broker_handle_id": handle.handle_id,
                        "broker_status": status.state,
                        **native,
                    }
                )
                order = order.model_copy(update={"metadata": metadata})
                order_fills = [
                    self._miniqmt_fill_from_trade(
                        trade,
                        order=order,
                        native=native,
                        trade_date=trade_date,
                    )
                    for trade in broker.query_trades(handle)
                ]
                final_order = order
                order_events = []
                for fill in order_fills:
                    final_order, event = self.oms.apply_fill(final_order, fill)
                    self.repository.save_fill(
                        run.run_id,
                        fill,
                        intended_price=order.limit_price,
                        fill_market_context=self._miniqmt_fill_market_context(
                            trade=fill.metadata.get("miniqmt_trade_raw") if isinstance(fill.metadata, dict) else {},
                            native=native,
                            trade_date=trade_date,
                        ),
                    )
                    self.repository.save_order_event(run.run_id, event)
                    order_events.append(event)
                broker_state = self._miniqmt_order_status_from_handle(status)
                if not order_fills and broker_state in {OrderStatus.REJECTED, OrderStatus.CANCELLED}:
                    if broker_state == OrderStatus.REJECTED:
                        final_order, event = self.oms.reject_order(order, status.rejection_reason or "MiniQMT order rejected")
                    else:
                        final_order, event = self.oms.cancel_order(order, status.rejection_reason or "MiniQMT order cancelled")
                    self.repository.save_order_event(run.run_id, event)
                    order_events.append(event)
                elif (
                    final_order.status != broker_state
                    and broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}
                    and status.filled_quantity > 0
                    and status.filled_quantity >= final_order.filled_quantity
                ):
                    previous_status = final_order.status
                    reconciled_update: dict[str, Any] = {
                        "status": broker_state,
                        "filled_quantity": min(status.filled_quantity, final_order.quantity),
                    }
                    if status.avg_fill_price is not None:
                        reconciled_update["avg_fill_price"] = float(status.avg_fill_price)
                    final_order = final_order.model_copy(update=reconciled_update)
                    if broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED} and previous_status != broker_state:
                        event = OrderEvent(
                            order_id=final_order.order_id,
                            event_type=(
                                OrderEventType.FILLED
                                if broker_state == OrderStatus.FILLED
                                else OrderEventType.PARTIALLY_FILLED
                            ),
                            reason="MiniQMT order status reconciled without visible trade rows",
                            metadata={
                                "broker_backend": "minqmt_sim",
                                "authority_source": "MINIQMT_ORDER_STATUS",
                                "broker_status": status.state,
                                "broker_handle_id": handle.handle_id,
                                "miniqmt_order_id": native["miniqmt_order_id"],
                                "visible_trade_count": 0,
                            },
                        )
                        self.repository.save_order_event(run.run_id, event)
                        order_events.append(event)
                    if broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED} and not order_fills:
                        self.repository.save_run_event(
                            run_id=run.run_id,
                            event_type="MINIQMT_ORDER_FILLED_WITHOUT_VISIBLE_TRADE_ROWS",
                            message="MiniQMT order status reported fills but no trade rows were visible during reconciliation",
                            context={
                                "order_id": final_order.order_id,
                                "symbol": final_order.symbol,
                                "side": final_order.side.value,
                                "filled_quantity": final_order.filled_quantity,
                                "broker_status": status.state,
                                "broker_handle_id": handle.handle_id,
                                "miniqmt_order_id": native["miniqmt_order_id"],
                            },
                        )
                final_order = final_order.model_copy(
                    update={
                        "metadata": {
                            **dict(final_order.metadata or {}),
                            "broker_backend": "minqmt_sim",
                            "authority_source": "MINIQMT",
                            "broker_handle_id": handle.handle_id,
                            "broker_status": status.state,
                            "miniqmt_trade_count": len(order_fills),
                            **native,
                        },
                        "avg_fill_price": float(final_order.avg_fill_price) if final_order.avg_fill_price is not None else None,
                    }
                )
                self.repository.save_order(run.run_id, final_order)
                if session_id:
                    self.repository.save_order_execution_state(
                        OrderExecutionState(
                            session_id=session_id,
                            run_id=run.run_id,
                            order_id=final_order.order_id,
                            symbol=final_order.symbol,
                            trade_date=trade_date,
                            algo_code="MINIQMT_BROKER_AUTHORITY",
                            algo_state={
                                "broker_backend": "minqmt_sim",
                                "authority_source": "MINIQMT",
                                "broker_handle_id": handle.handle_id,
                                "miniqmt_order_id": native["miniqmt_order_id"],
                                "broker_status": status.state,
                                "trade_count": len(order_fills),
                            },
                            filled_quantity=final_order.filled_quantity,
                            remaining_quantity=final_order.remaining_quantity,
                            status=final_order.status.value,
                        )
                    )
                orders.append(final_order)
                fills.extend(order_fills)
                events.extend(order_events)
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MINIQMT_ORDER_SUBMITTED",
                    message="order intent submitted to MiniQMT broker authority",
                    context={
                        "order_id": final_order.order_id,
                        "intent_id": intent.intent_id,
                        "symbol": intent.symbol,
                        "side": intent.side.value,
                        "quantity": intent.quantity,
                        "broker_handle_id": handle.handle_id,
                        "miniqmt_order_id": native["miniqmt_order_id"],
                        "broker_status": status.state,
                        "fill_count": len(order_fills),
                        "paper_order_status": final_order.status.value,
                        "filled_quantity": final_order.filled_quantity,
                    },
                )

            return self._persist_minqmt_authority_snapshot(
                portfolio=portfolio,
                run=run,
                trade_date=trade_date,
                broker=broker,
                orders=orders,
                fills=fills,
                events=events,
            )
        finally:
            if broker is not None:
                broker.shutdown()

    def _persist_minqmt_authority_snapshot(
        self,
        *,
        portfolio: Any,
        run: PaperRun,
        trade_date: date,
        broker: MiniQMTSimBackend,
        orders: list[Any],
        fills: list[Fill],
        events: list[Any],
        fill_count_override: int | None = None,
    ) -> PaperDayRunResult:
        account = broker.query_account()
        positions, prices = broker.query_position_marks()
        position_list = list(positions.values())
        for position in position_list:
            if position.symbol not in prices:
                raise DataUnavailableError(
                    "MiniQMT position mark price is missing",
                    context={"portfolio_id": portfolio.portfolio_id, "symbol": position.symbol, "source": "MINIQMT"},
                )
        market_value = sum(position.quantity * prices[position.symbol] for position in position_list)
        snapshot = AccountSnapshot(
            portfolio_id=portfolio.portfolio_id,
            cash=float(account.cash),
            market_value=float(market_value),
            nav=float(account.nav),
            snapshot_time=account.as_of,
        )
        self.repository.save_positions(
            run_id=run.run_id,
            trade_date=trade_date,
            positions=position_list,
            prices=prices,
        )
        fill_count = len(fills) if fill_count_override is None else int(fill_count_override)
        self.repository.save_daily_snapshot(
            run_id=run.run_id,
            trade_date=trade_date,
            snapshot=snapshot,
            metadata={
                "position_count": len(position_list),
                "order_count": len(orders),
                "fill_count": fill_count,
                "broker_backend": "minqmt_sim",
                "authority_source": "MINIQMT_QUERY",
                "miniqmt_no_local_fills": fill_count == 0,
            },
        )
        succeeded = self.repository.update_run_status(run, RunStatus.SUCCEEDED)
        ready_portfolio = self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.READY)
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="MINIQMT_RUN_RECONCILED",
            message="MiniQMT broker-authoritative snapshots persisted without LocalSim fills",
            context={
                "order_count": len(orders),
                "fill_count": fill_count,
                "new_fill_count": len(fills),
                "position_count": len(position_list),
                "cash": float(account.cash),
                "nav": float(account.nav),
            },
        )
        return PaperDayRunResult(
            portfolio=ready_portfolio,
            run=succeeded,
            orders=orders,
            fills=fills,
            events=events,
            positions=position_list,
            account_snapshot=snapshot,
        )

    def reconcile_minqmt_native_run(
        self,
        *,
        portfolio: Any,
        run: PaperRun,
        trade_date: date,
        broker: MiniQMTSimBackend | None = None,
    ) -> PaperDayRunResult:
        """Idempotently refresh local Paper v2 order/fill state from MiniQMT."""

        owns_broker = broker is None
        broker = broker or self.minqmt_broker_factory(
            portfolio_id=portfolio.portfolio_id,
            package_id=portfolio.package_id,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            strategy_slot_id=portfolio.portfolio_id,
        )
        session_id = self._miniqmt_session_id_from_run(run)
        new_fills: list[Fill] = []
        order_events: list[OrderEvent] = []
        reconciled_orders = []
        try:
            existing_rows = self._miniqmt_existing_fill_rows(run.run_id)
            existing_fill_ids = {str(row.get("fill_id") or "") for row in existing_rows if row.get("fill_id")}
            existing_fill_order_ids = {str(row.get("order_id") or "") for row in existing_rows if row.get("order_id")}
            for order in self.repository.list_orders_for_run(run.run_id):
                native = self._miniqmt_native_context_from_order(order)
                if native is None:
                    reconciled_orders.append(order)
                    self.repository.save_run_event(
                        run_id=run.run_id,
                        event_type="MINIQMT_NATIVE_RECONCILE_SKIPPED",
                        message="MiniQMT native reconciliation skipped an order without persisted native ids",
                        context={"order_id": order.order_id, "symbol": order.symbol},
                    )
                    continue
                intent = self._miniqmt_intent_from_order(order, trade_date=trade_date)
                status = broker.query_status_from_native(intent=intent, **native)
                trade_rows = broker.query_trades_from_native(intent=intent, **native)
                candidate_fills = [
                    self._miniqmt_fill_from_trade(
                        trade,
                        order=order,
                        native=native,
                        trade_date=trade_date,
                    )
                    for trade in trade_rows
                ]
                final_order = order
                if (
                    candidate_fills
                    and order.status in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}
                    and order.order_id not in existing_fill_order_ids
                ):
                    final_order = order.model_copy(
                        update={"status": OrderStatus.SUBMITTED, "filled_quantity": 0, "avg_fill_price": None}
                    )
                for fill in candidate_fills:
                    if fill.fill_id in existing_fill_ids:
                        continue
                    final_order, event = self.oms.apply_fill(final_order, fill)
                    self.repository.save_fill(
                        run.run_id,
                        fill,
                        intended_price=order.limit_price,
                        fill_market_context=self._miniqmt_fill_market_context(
                            trade=fill.metadata.get("miniqmt_trade_raw") if isinstance(fill.metadata, dict) else {},
                            native=native,
                            trade_date=trade_date,
                        ),
                    )
                    self.repository.save_order_event(run.run_id, event)
                    existing_fill_ids.add(fill.fill_id)
                    existing_fill_order_ids.add(order.order_id)
                    new_fills.append(fill)
                    order_events.append(event)
                final_order = self._reconcile_minqmt_order_status(
                    final_order,
                    status=status,
                    native=native,
                    visible_trade_count=len(trade_rows),
                )
                self.repository.save_order(run.run_id, final_order)
                if session_id:
                    self.repository.save_order_execution_state(
                        OrderExecutionState(
                            session_id=session_id,
                            run_id=run.run_id,
                            order_id=final_order.order_id,
                            symbol=final_order.symbol,
                            trade_date=trade_date,
                            algo_code="MINIQMT_BROKER_AUTHORITY",
                            algo_state={
                                "broker_backend": "minqmt_sim",
                                "authority_source": "MINIQMT_NATIVE_RECONCILE",
                                "broker_handle_id": native["handle_id"],
                                "miniqmt_order_id": native["miniqmt_order_id"],
                                "broker_status": status.state,
                                "trade_count": len(trade_rows),
                            },
                            filled_quantity=final_order.filled_quantity,
                            remaining_quantity=final_order.remaining_quantity,
                            status=final_order.status.value,
                        )
                    )
                reconciled_orders.append(final_order)
            persisted_fill_count = len(self._miniqmt_existing_fill_rows(run.run_id))
            result = self._persist_minqmt_authority_snapshot(
                portfolio=portfolio,
                run=run,
                trade_date=trade_date,
                broker=broker,
                orders=reconciled_orders,
                fills=new_fills,
                events=order_events,
                fill_count_override=persisted_fill_count,
            )
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="MINIQMT_NATIVE_RUN_RECONCILED",
                message="MiniQMT native order and trade state reconciled into Paper v2 ledger",
                context={
                    "order_count": len(reconciled_orders),
                    "new_fill_count": len(new_fills),
                    "persisted_fill_count": persisted_fill_count,
                    "broker_backend": "minqmt_sim",
                    "authority_source": "MINIQMT_NATIVE_RECONCILE",
                },
            )
            return result
        finally:
            if owns_broker and broker is not None:
                broker.shutdown()

    @staticmethod
    def _miniqmt_order_submission_sequence(intents: list[Any]) -> list[Any]:
        return sorted(
            intents,
            key=lambda intent: (
                0 if intent.side == OrderSide.SELL else 1,
                str(intent.symbol),
                str(intent.intent_id),
            ),
        )

    @staticmethod
    def _miniqmt_order_status_from_handle(status: Any) -> OrderStatus:
        if status.state == "rejected":
            return OrderStatus.REJECTED
        if status.state == "cancelled":
            return OrderStatus.CANCELLED
        if status.state == "filled":
            return OrderStatus.FILLED
        if status.state == "partial_filled":
            return OrderStatus.PARTIALLY_FILLED
        return OrderStatus.SUBMITTED

    def _reconcile_minqmt_order_status(
        self,
        order: Any,
        *,
        status: Any,
        native: dict[str, Any],
        visible_trade_count: int,
    ) -> Any:
        broker_state = self._miniqmt_order_status_from_handle(status)
        if broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED} and status.filled_quantity > order.filled_quantity:
            update: dict[str, Any] = {
                "status": broker_state,
                "filled_quantity": min(status.filled_quantity, order.quantity),
            }
            if status.avg_fill_price is not None:
                update["avg_fill_price"] = float(status.avg_fill_price)
            return order.model_copy(update=update)
        if broker_state in {OrderStatus.REJECTED, OrderStatus.CANCELLED} and order.status != broker_state:
            update = {"status": broker_state}
            if status.rejection_reason:
                update["metadata"] = {
                    **dict(order.metadata or {}),
                    "broker_status": status.state,
                    "broker_rejection_reason": status.rejection_reason,
                    "visible_trade_count": visible_trade_count,
                    **native,
                }
            return order.model_copy(update=update)
        if order.status != broker_state and broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
            return order.model_copy(update={"status": broker_state})
        return order

    @staticmethod
    def _miniqmt_session_id_from_run(run: PaperRun) -> str | None:
        session = run.runtime_config.get("paper_v2_session") if isinstance(run.runtime_config, dict) else None
        if not isinstance(session, dict):
            return None
        session_id = str(session.get("session_id") or "").strip()
        return session_id or None

    @staticmethod
    def _miniqmt_native_context_from_order(order: Any) -> dict[str, str] | None:
        metadata = dict(order.metadata or {})
        miniqmt_order_id = str(metadata.get("miniqmt_order_id") or "").strip()
        if not miniqmt_order_id:
            return None
        handle_id = str(metadata.get("broker_handle_id") or metadata.get("handle_id") or f"native_{miniqmt_order_id}")
        return {
            "handle_id": handle_id,
            "miniqmt_order_id": miniqmt_order_id,
            "strategy_name": str(metadata.get("strategy_name") or ""),
            "order_remark": str(metadata.get("order_remark") or ""),
        }

    @staticmethod
    def _miniqmt_intent_from_order(order: Any, *, trade_date: date) -> OrderIntent:
        return OrderIntent(
            intent_id=order.intent_id,
            package_id=order.package_id,
            portfolio_id=order.portfolio_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            order_type=order.order_type,
            limit_price=order.limit_price,
            target_trade_date=trade_date,
            metadata=dict(order.metadata or {}),
        )

    def _miniqmt_existing_fill_rows(self, run_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.repository.list_fills_for_run(run_id)]

    @classmethod
    def _miniqmt_fill_from_trade(
        cls,
        trade: dict[str, Any],
        *,
        order: Any,
        native: dict[str, Any],
        trade_date: date,
    ) -> Fill:
        trade_time = cls._parse_minqmt_trade_time(trade_date, trade.get("traded_time"))
        metadata = {
            "broker_backend": "minqmt_sim",
            "authority_source": "MINIQMT_TRADE",
            "traded_id": str(trade.get("traded_id") or ""),
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "order_sysid": str(trade.get("order_sysid") or ""),
            "strategy_name": str(trade.get("strategy_name") or native.get("strategy_name") or ""),
            "order_remark": str(trade.get("order_remark") or native.get("order_remark") or ""),
            "commission": float(trade.get("commission") or 0.0),
            "secu_account": str(trade.get("secu_account") or ""),
            "miniqmt_trade_raw": dict(trade),
        }
        return Fill(
            fill_id=cls._miniqmt_fill_id(trade, order_id=order.order_id),
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=int(trade.get("traded_volume") or 0),
            price=float(trade.get("traded_price") or 0.0),
            trade_time=trade_time,
            bar_time=trade_time,
            reason="miniqmt_trade_reconciliation",
            metadata=metadata,
        )

    @staticmethod
    def _parse_minqmt_trade_time(trade_date: date, value: Any) -> datetime:
        text = str(value or "").strip()
        for fmt in ("%H%M%S", "%H:%M:%S"):
            try:
                parsed = datetime.strptime(text, fmt)
                return datetime.combine(trade_date, parsed.time(), tzinfo=UTC)
            except ValueError:
                continue
        return datetime.now(UTC)

    @staticmethod
    def _miniqmt_fill_id(trade: dict[str, Any], *, order_id: str) -> str:
        traded_id = str(trade.get("traded_id") or "").strip()
        if traded_id:
            return f"fill_minqmt_{traded_id}"
        payload = {
            "order_id": order_id,
            "miniqmt_order_id": str(trade.get("order_id") or ""),
            "order_sysid": str(trade.get("order_sysid") or ""),
            "symbol": str(trade.get("stock_code") or ""),
            "side": str(trade.get("order_type") or ""),
            "traded_time": str(trade.get("traded_time") or ""),
            "quantity": int(trade.get("traded_volume") or 0),
            "price": float(trade.get("traded_price") or 0.0),
        }
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
        return f"fill_minqmt_{digest[:24]}"

    @staticmethod
    def _miniqmt_fill_market_context(*, trade: dict[str, Any], native: dict[str, Any], trade_date: date) -> dict[str, Any]:
        price = float(trade.get("traded_price") or 0.0)
        quantity = int(trade.get("traded_volume") or 0)
        return {
            "stock_id": str(trade.get("stock_code") or ""),
            "trade_date": trade_date.isoformat(),
            "data_source": MinuteDataSource.MINIQMT_REALTIME.value,
            "prev_close": None,
            "limit_up": None,
            "limit_down": None,
            "suspend_status": "MINIQMT_AUTHORITY",
            "full_day_open": price,
            "full_day_close": price,
            "full_day_volume": quantity,
            "full_day_high": price,
            "full_day_low": price,
            "generated_at": datetime.now(UTC).isoformat(),
            "broker_backend": "minqmt_sim",
            "authority_source": "MINIQMT_TRADE",
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "traded_id": str(trade.get("traded_id") or ""),
        }

    @staticmethod
    def _reject_raw_execution_overrides(runtime_config: dict[str, Any]) -> None:
        forbidden = {
            "algo_code",
            "algo_config",
            "execution_policy",
            "unfilled_handler",
            "unfilled_handler_params",
            "unfilled_policy",
            "validated_execution_policy",
        }
        present = sorted(key for key in forbidden if key in runtime_config)
        if present:
            raise RuntimeConfigInvalidError(
                "paper v2 runtime_config cannot override execution policy; use a backtest-validated execution policy",
                context={"forbidden_keys": present},
            )

    @staticmethod
    def _execution_policy_context(portfolio: Any) -> dict[str, Any]:
        policy = dict(portfolio.execution_policy or {})
        policy_json = policy.get("policy_json")
        policy_id = policy.get("validated_execution_policy_id")
        policy_sha256 = policy.get("policy_sha256")
        if not isinstance(policy_json, dict) or not policy_id or not policy_sha256:
            raise RuntimeConfigInvalidError(
                "paper portfolio requires a backtest-validated execution policy snapshot",
                context={"portfolio_id": portfolio.portfolio_id},
            )
        return {
            "validated_execution_policy_id": str(policy_id),
            "policy_sha256": str(policy_sha256),
            "policy_name": policy.get("policy_name"),
            "activation_id": None,
            "activation_source": "portfolio_default",
            "algo_code": policy.get("algo_code") or policy_json.get("algo_code"),
            "source_backtest_id": policy.get("source_backtest_id"),
            "source_backtest_status": policy.get("source_backtest_status"),
            "validation_status": policy.get("validation_status"),
            "policy_json": policy_json,
        }

    def _execution_policy_context_for_date(self, portfolio: Any, trade_date: date) -> dict[str, Any]:
        activation = None
        if hasattr(self.repository, "get_active_execution_policy_activation"):
            activation = self.repository.get_active_execution_policy_activation(portfolio.portfolio_id, trade_date)
        if activation is None:
            return self._execution_policy_context(portfolio)
        return {
            "validated_execution_policy_id": activation.policy_id,
            "policy_sha256": activation.policy_sha256,
            "policy_name": activation.policy_name,
            "activation_id": activation.activation_id,
            "activation_source": "trade_date_activation",
            "activated_at": activation.activated_at.isoformat(),
            "activated_by": activation.activated_by,
            "activation_reason": activation.reason,
            "algo_code": activation.policy_json.get("algo_code"),
            "source_backtest_id": activation.context.get("source_backtest_id"),
            "source_backtest_status": activation.context.get("source_backtest_status"),
            "validation_status": "BACKTEST_VALIDATED",
            "policy_json": activation.policy_json,
        }

    @staticmethod
    def _fee_model_from_policy(policy: dict[str, Any]) -> FeeModel:
        return FeeModel(
            open_cost=float(policy.get("open_cost", FeeModel.open_cost)),
            close_cost=float(policy.get("close_cost", FeeModel.close_cost)),
            min_cost=float(policy.get("min_cost", FeeModel.min_cost)),
        )
