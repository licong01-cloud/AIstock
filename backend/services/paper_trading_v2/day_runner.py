"""Authoritative single-day Paper Trading v2 runner."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, date, datetime
from typing import Any, Callable

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.market_data import MinuteDataSource, PaperV2MinuteMarketDataProvider, TradeCalendarProvider
from backend.services.paper_trading_v2.selection_cutoff import ensure_previous_trading_day_selection_cutoff
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
    BrokerConnectivityError,
    BrokerRejectedError,
    BrokerSubmitError,
    DataUnavailableError,
    InvalidStateTransitionError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
    TradingCoreError,
)
from backend.execution_algos.vnpy_style import VNPY_STYLE_ASSETS, is_vnpy_style_algo
from backend.services.simulation_runtime.models import ExecutionPathNotCanonicalError, MiniQMTUnsupportedExecutionAlgoError
from backend.services.miniqmt_execution_runtime import MiniQMTExecutionRuntimeClient
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

from .broker import MiniQMTSimBackend, OrderHandle
from .execution import MiniQMTAlgoExecutionResult, build_minqmt_execution_quality_report
from .auto_run import (
    MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
    miniqmt_account_group_id,
    miniqmt_order_remark,
    miniqmt_strategy_name,
    miniqmt_strategy_slot_id,
)
from .models import OrderExecutionState, PaperDayRunResult, PaperRun, PortfolioStatus
from .repository import PaperTradingV2Repository, assert_orders_terminal_before_run_success, non_terminal_orders_for_run_success
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


def miniqmt_account_slot_context(repository: Any, portfolio: Any) -> dict[str, str]:
    if not hasattr(repository, "list_active_broker_account_bindings"):
        raise ExecutionPathNotCanonicalError(
            "Paper v2 MiniQMT execution requires an account_group_slots broker binding",
            context={
                "portfolio_id": portfolio.portfolio_id,
                "required_allocation_mode": MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
                "missing_repository_method": "list_active_broker_account_bindings",
                "required_runtime_owner": "MiniQMTExecutionRuntime",
            },
        )
    bindings = repository.list_active_broker_account_bindings(portfolio.portfolio_id)
    binding = next(
        (item for item in bindings if item.allocation_mode == MINIQMT_ACCOUNT_GROUP_BINDING_MODE),
        None,
    )
    if binding is None:
        raise ExecutionPathNotCanonicalError(
            "Paper v2 MiniQMT execution requires an active account_group_slots broker binding",
            context={
                "portfolio_id": portfolio.portfolio_id,
                "active_binding_count": len(bindings),
                "active_allocation_modes": sorted({str(item.allocation_mode) for item in bindings}),
                "required_allocation_mode": MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
                "required_runtime_owner": "MiniQMTExecutionRuntime",
            },
        )
    account_id = str(binding.broker_account_id or ((portfolio.auto_run_config or {}).get("broker") or {}).get("account_id") or "")
    account_group_id = binding.account_group_id or miniqmt_account_group_id(account_id)
    strategy_slot_id = binding.strategy_slot_id or miniqmt_strategy_slot_id(portfolio.portfolio_id)
    if not account_group_id or not strategy_slot_id:
        raise RuntimeConfigInvalidError(
            "MiniQMT account_group_slots binding is missing runtime attribution",
            context={
                "portfolio_id": portfolio.portfolio_id,
                "broker_account_id": binding.broker_account_id,
                "account_group_id": binding.account_group_id,
                "strategy_slot_id": binding.strategy_slot_id,
            },
        )
    return {
        "account_mode": MINIQMT_ACCOUNT_GROUP_BINDING_MODE,
        "account_group_id": account_group_id,
        "strategy_slot_id": strategy_slot_id,
    }


def miniqmt_broker_kwargs_for_portfolio(repository: Any, portfolio: Any, *, package_id: str) -> dict[str, Any]:
    return {
        "portfolio_id": portfolio.portfolio_id,
        "package_id": package_id,
        "data_source": MinuteDataSource.MINIQMT_REALTIME,
        **miniqmt_account_slot_context(repository, portfolio),
    }


def miniqmt_intent_with_account_slot(
    portfolio: Any,
    intent: OrderIntent,
    *,
    account_slot_context: dict[str, str],
) -> OrderIntent:
    if account_slot_context.get("account_mode") != MINIQMT_ACCOUNT_GROUP_BINDING_MODE:
        return intent
    strategy_slot_id = account_slot_context["strategy_slot_id"]
    metadata = {
        **dict(intent.metadata or {}),
        "account_group_id": account_slot_context["account_group_id"],
        "strategy_slot_id": strategy_slot_id,
        "strategy_name": miniqmt_strategy_name(strategy_slot_id),
        "order_remark": miniqmt_order_remark(
            portfolio_id=portfolio.portfolio_id,
            package_id=intent.package_id,
            intent_id=intent.intent_id,
        ),
    }
    return intent.model_copy(update={"metadata": metadata})


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
        minqmt_runtime_client: MiniQMTExecutionRuntimeClient | None = None,
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
        self.minqmt_runtime_client = minqmt_runtime_client or MiniQMTExecutionRuntimeClient()

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
        ensure_previous_trading_day_selection_cutoff(
            config,
            trade_date=trade_date,
            calendar_provider=self.calendar_provider,
        )
        config = normalize_runtime_config_with_backtest_contract(
            manifest,
            config,
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "day_runner"},
            include_contract=True,
        )
        config = refresh_generated_runtime_profile_binding(config)
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
                    **miniqmt_broker_kwargs_for_portfolio(self.repository, portfolio, package_id=manifest.package_id),
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
                    execution_policy_context=execution_policy_context,
                    fee_model=fee_model,
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
                self._assert_orders_terminal_before_success(run, [])
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
            self._assert_orders_terminal_before_success(run, orders)
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
        execution_policy_context: dict[str, Any] | None = None,
        fee_model: FeeModel | None = None,
    ) -> PaperDayRunResult:
        broker = broker or self.minqmt_broker_factory(
            **miniqmt_broker_kwargs_for_portfolio(self.repository, portfolio, package_id=manifest.package_id),
        )
        report_fee_model = fee_model or self._fee_model_from_policy(getattr(portfolio, "fee_policy", {}) or {})
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
            account_slot_context = miniqmt_account_slot_context(self.repository, portfolio)
            ordered_intents = [
                miniqmt_intent_with_account_slot(portfolio, intent, account_slot_context=account_slot_context)
                for intent in self._miniqmt_order_submission_sequence(intents)
            ]
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
            runtime_hash = self._miniqmt_runtime_config_hash(
                portfolio=portfolio,
                run=run,
                manifest=manifest,
                execution_policy_context=execution_policy_context,
            )
            execution_policy_context = self._require_miniqmt_vnpy_style_execution(
                execution_policy_context,
                portfolio_id=portfolio.portfolio_id,
                trade_date=trade_date,
                package_id=manifest.package_id,
            )
            for intent in ordered_intents:
                audit_before = self._miniqmt_broker_audit_snapshot(
                    broker,
                    phase="before_submit",
                    intent=intent,
                )
                algo_result = self._run_minqmt_vnpy_style_intent(
                    run=run,
                    trade_date=trade_date,
                    intent=intent,
                    broker=broker,
                    execution_policy_context=execution_policy_context,
                    session_id=session_id,
                    account_slot_context=account_slot_context,
                    runtime_config_hash=runtime_hash,
                    audit_before=audit_before,
                )
                orders.extend(algo_result["orders"])
                fills.extend(algo_result["fills"])
                events.extend(algo_result["events"])

            return self._persist_minqmt_authority_snapshot(
                portfolio=portfolio,
                run=run,
                trade_date=trade_date,
                broker=broker,
                orders=orders,
                fills=fills,
                events=events,
                fee_model=report_fee_model,
            )
        finally:
            if broker is not None:
                broker.shutdown()

    @staticmethod
    def _miniqmt_runtime_id(run: PaperRun) -> str:
        payload = [run.run_id, run.trade_date.isoformat()]
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
        return f"mqrt_paper_{digest[:24]}"

    @staticmethod
    def _miniqmt_runtime_config_hash(
        *,
        portfolio: Any,
        run: PaperRun,
        manifest: Any,
        execution_policy_context: dict[str, Any] | None,
    ) -> str:
        payload = {
            "portfolio_id": portfolio.portfolio_id,
            "run_id": run.run_id,
            "package_id": manifest.package_id,
            "manifest_sha256": getattr(manifest, "manifest_sha256", None),
            "execution_policy_context": execution_policy_context or {},
        }
        return hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str).encode("utf-8")).hexdigest()

    @staticmethod
    def _require_miniqmt_vnpy_style_execution(
        execution_policy_context: dict[str, Any] | None,
        *,
        portfolio_id: str,
        trade_date: date,
        package_id: str,
    ) -> dict[str, Any]:
        policy_context = dict(execution_policy_context or {})
        policy_json = policy_context.get("policy_json") if isinstance(policy_context.get("policy_json"), dict) else None
        explicit_algo_code = str(
            (policy_json or {}).get("algo_code")
            or policy_context.get("algo_code")
            or policy_context.get("validated_execution_policy_id")
            or ""
        ).strip().upper()
        if isinstance(policy_json, dict):
            algo_code = str(policy_json.get("algo_code") or "").strip().upper()
            if is_vnpy_style_algo(algo_code):
                return {**policy_context, "algo_code": algo_code, "policy_json": {**policy_json, "algo_code": algo_code}}
        context = {
            "portfolio_id": portfolio_id,
            "package_id": package_id,
            "trade_date": trade_date.isoformat(),
            "broker_backend": "minqmt_sim",
            "inferred_algo_code": explicit_algo_code or None,
            "payload_has_policy_json": isinstance(policy_json, dict),
            "allowed_algo_codes": sorted(VNPY_STYLE_ASSETS),
            "required_runtime_owner": "MiniQMTExecutionRuntime",
        }
        if explicit_algo_code.startswith("V25_") or explicit_algo_code in {"V25_TWO_STAGE", "V25_1_SMALL_CAP"}:
            raise MiniQMTUnsupportedExecutionAlgoError(
                "MiniQMT broker execution does not support V25_* execution algorithms",
                context={
                    **context,
                    "required_action": (
                        "activate SNIPER_MINIQMT, BEST_LIMIT_MINIQMT, TWAP_LITE_MINIQMT, "
                        "or another approved MiniQMT vn.py-style execution asset"
                    ),
                },
            )
        if explicit_algo_code:
            raise MiniQMTUnsupportedExecutionAlgoError(
                "MiniQMT broker execution requires an approved MiniQMT vn.py-style execution asset",
                context={
                    **context,
                    "required_action": (
                        "activate SNIPER_MINIQMT, BEST_LIMIT_MINIQMT, TWAP_LITE_MINIQMT, "
                        "or another approved MiniQMT vn.py-style execution asset"
                    ),
                },
            )
        raise ExecutionPathNotCanonicalError(
            "Paper v2 MiniQMT broker execution requires a full vn.py-style execution policy snapshot",
            context={
                **context,
                "required_action": "bind an approved MiniQMT vn.py-style execution policy before broker submit",
            },
        )

    def _run_minqmt_vnpy_style_intent(
        self,
        *,
        run: PaperRun,
        trade_date: date,
        intent: OrderIntent,
        broker: MiniQMTSimBackend,
        execution_policy_context: dict[str, Any],
        session_id: str | None,
        account_slot_context: dict[str, str],
        runtime_config_hash: str,
        audit_before: dict[str, Any] | None,
    ) -> dict[str, list[Any]]:
        result = self.minqmt_runtime_client.execute_paper_vnpy_intent(
            portfolio=type("PaperMiniQMTPortfolioRef", (), {"portfolio_id": intent.portfolio_id})(),
            run=run,
            trade_date=trade_date,
            intent=intent,
            broker=broker,
            execution_policy_context=execution_policy_context,
            runtime_config_hash=runtime_config_hash,
            account_group_id=str(account_slot_context.get("account_group_id") or intent.portfolio_id),
            strategy_slot_id=str(account_slot_context.get("strategy_slot_id") or intent.portfolio_id),
            quote_provider=self._miniqmt_quote_provider(broker),
        )
        runtime_evidence = result.diagnostic.get("runtime_evidence") if isinstance(result.diagnostic, dict) else None
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="MINIQMT_VNPY_STYLE_EXECUTION_STARTED",
            message="MiniQMT order intent routed through MiniQMTExecutionRuntime and selected vn.py-style execution asset",
            context={
                "parent_intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "side": intent.side.value,
                "quantity": intent.quantity,
                "algo_code": result.algo_code,
                "policy_id": result.policy_context.get("validated_execution_policy_id"),
                "policy_sha256": result.policy_sha256,
                "asset_version": result.asset_metadata.get("asset_version"),
                "runtime_owner": "MiniQMTExecutionRuntime",
                "runtime_evidence": runtime_evidence,
            },
        )
        orders: list[Any] = []
        fills: list[Fill] = []
        events: list[Any] = []
        if not result.child_orders:
            order = self.oms.create_order(intent)
            final_order, event = self.oms.cancel_order(order, f"{result.algo_code} produced no executable child order")
            final_order = final_order.model_copy(
                update={
                    "metadata": {
                        **dict(final_order.metadata or {}),
                        "broker_backend": "minqmt_sim",
                        "authority_source": "MINIQMT_VNPY_STYLE",
                        "execution_algo_code": result.algo_code,
                        "execution_policy_id": result.policy_context.get("validated_execution_policy_id"),
                        "execution_policy_sha256": result.policy_sha256,
                        "execution_terminal_state": result.terminal_state,
                        "execution_diagnostic": result.diagnostic,
                    }
                }
            )
            self.repository.save_order(run.run_id, final_order)
            self.repository.save_order_event(run.run_id, event)
            orders.append(final_order)
            events.append(event)
        for child in result.child_orders:
            order = self.oms.create_order(child.intent)
            order = order.model_copy(update={"metadata": self._miniqmt_child_order_metadata(order.metadata, child, result)})
            final_order = order
            order_events: list[Any] = []
            if child.handle is None:
                audit_after = self._miniqmt_broker_audit_snapshot(
                    broker,
                    phase="submit_error",
                    intent=child.intent,
                    native=child.native_context,
                    status=child.status,
                )
                reason = self._miniqmt_child_error_reason(child)
                final_order, event = self.oms.reject_order(order, reason)
                final_order = final_order.model_copy(
                    update={
                        "metadata": self._miniqmt_child_order_metadata(
                            final_order.metadata,
                            child,
                            result,
                            audit_before=audit_before,
                            audit_after=audit_after,
                        )
                    }
                )
                event = event.model_copy(update={"metadata": final_order.metadata})
                self.repository.save_order_event(run.run_id, event)
                self.repository.save_run_event(
                    run_id=run.run_id,
                    event_type="MINIQMT_ORDER_SUBMIT_FAILED",
                    message="MiniQMT vn.py-style child order submit failed with broker diagnostic context",
                    context=final_order.metadata.get("broker_diagnostic") or final_order.metadata,
                )
                order_events.append(event)
                self.repository.save_order(run.run_id, final_order)
                self._raise_minqmt_child_submit_error(child)
            else:
                native = dict(child.native_context or {})
                audit_after = self._miniqmt_broker_audit_snapshot(
                    broker,
                    phase="after_reconcile",
                    intent=child.intent,
                    native=native,
                    status=child.status,
                )
                order_fills = self._miniqmt_fills_from_trades(
                    child.trades,
                    order=order,
                    native=native,
                    trade_date=trade_date,
                )
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
                broker_state = self._miniqmt_order_status_from_handle(child.status)
                if not order_fills and broker_state in {OrderStatus.REJECTED, OrderStatus.CANCELLED}:
                    if broker_state == OrderStatus.REJECTED:
                        final_order, event = self.oms.reject_order(order, child.status.rejection_reason or "MiniQMT child order rejected")
                    else:
                        final_order, event = self.oms.cancel_order(order, child.status.rejection_reason or "MiniQMT child order cancelled")
                    event = event.model_copy(
                        update={
                            "metadata": self._miniqmt_child_order_metadata(
                                dict(event.metadata or {}),
                                child,
                                result,
                                audit_before=audit_before,
                                audit_after=audit_after,
                            )
                        }
                    )
                    self.repository.save_order_event(run.run_id, event)
                    order_events.append(event)
                elif (
                    final_order.status != broker_state
                    and broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}
                    and child.status is not None
                    and child.status.filled_quantity > 0
                    and child.status.filled_quantity >= final_order.filled_quantity
                ):
                    final_order = final_order.model_copy(
                        update={
                            "status": broker_state,
                            "filled_quantity": min(child.status.filled_quantity, final_order.quantity),
                            "avg_fill_price": float(child.status.avg_fill_price) if child.status.avg_fill_price else final_order.avg_fill_price,
                        }
                    )
                final_order = final_order.model_copy(
                    update={
                        "metadata": self._miniqmt_child_order_metadata(
                            final_order.metadata,
                            child,
                            result,
                            audit_before=audit_before,
                            audit_after=audit_after,
                        )
                    }
                )
                fills.extend(order_fills)
            self.repository.save_order(run.run_id, final_order)
            if session_id:
                self.repository.save_order_execution_state(
                    OrderExecutionState(
                        session_id=session_id,
                        run_id=run.run_id,
                        order_id=final_order.order_id,
                        symbol=final_order.symbol,
                        trade_date=trade_date,
                        algo_code=result.algo_code,
                        algo_state={
                            "broker_backend": "minqmt_sim",
                            "authority_source": "MINIQMT_VNPY_STYLE",
                            "execution_terminal_state": result.terminal_state,
                            "broker_handle_id": final_order.metadata.get("broker_handle_id"),
                            "miniqmt_order_id": final_order.metadata.get("miniqmt_order_id"),
                            "broker_status": final_order.metadata.get("broker_status"),
                            "broker_raw_status": final_order.metadata.get("broker_raw_status"),
                            "broker_status_msg": final_order.metadata.get("broker_status_msg"),
                            "broker_rejection_reason": final_order.metadata.get("broker_rejection_reason"),
                            "diagnostic": result.diagnostic,
                            "broker_diagnostic": final_order.metadata.get("broker_diagnostic"),
                        },
                        plan={"asset_metadata": result.asset_metadata, "policy_context": result.policy_context},
                        plan_sha256=result.policy_sha256,
                        filled_quantity=final_order.filled_quantity,
                        remaining_quantity=final_order.remaining_quantity,
                        status=final_order.status.value,
                    )
                )
            orders.append(final_order)
            events.extend(order_events)
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="MINIQMT_VNPY_STYLE_EXECUTION_COMPLETED",
            message="MiniQMT vn.py-style execution asset completed for order intent",
            context={
                "parent_intent_id": intent.intent_id,
                "algo_code": result.algo_code,
                "terminal_state": result.terminal_state,
                "child_order_count": len(result.child_orders),
                "submitted_child_count": result.submitted_child_count,
                "policy_id": result.policy_context.get("validated_execution_policy_id"),
                "policy_sha256": result.policy_sha256,
                "runtime_owner": "MiniQMTExecutionRuntime",
                "runtime_evidence": runtime_evidence,
                "diagnostic": result.diagnostic,
            },
        )
        return {"orders": orders, "fills": fills, "events": events}

    @staticmethod
    def _raise_minqmt_child_submit_error(child: Any) -> None:
        error = child.submit_error if isinstance(child.submit_error, dict) else {}
        error_code = str(error.get("error_code") or "").strip().upper()
        message = str(error.get("message") or "MiniQMT vn.py-style child order submit failed")
        context = error.get("context") if isinstance(error.get("context"), dict) else {}
        if error_code == BrokerRejectedError.error_code:
            raise BrokerRejectedError(message, context=dict(context))
        if error_code == BrokerConnectivityError.error_code:
            raise BrokerConnectivityError(message, context=dict(context))
        raise BrokerSubmitError(message, context=dict(context))

    @staticmethod
    def _miniqmt_child_order_metadata(
        metadata: dict[str, Any],
        child: Any,
        result: MiniQMTAlgoExecutionResult,
        *,
        audit_before: dict[str, Any] | None = None,
        audit_after: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        native = dict(child.native_context or {})
        authority_source = "MINIQMT_SUBMIT_ERROR" if child.submit_error else "MINIQMT_VNPY_STYLE"
        broker_diagnostic = (
            PaperTradingDayRunner._miniqmt_order_diagnostic(
                status=child.status,
                native=native,
                visible_trade_count=len(child.trades),
                audit_before=audit_before,
                audit_after=audit_after,
                authority_source=authority_source,
            )
            if child.status is not None
            else None
        )
        if child.submit_error and broker_diagnostic is not None:
            broker_error_context = child.submit_error.get("context") if isinstance(child.submit_error, dict) else {}
            broker_diagnostic.update(
                {
                    "broker_error": child.submit_error,
                    "submit_diagnostic": broker_error_context.get("submit_diagnostic")
                    if isinstance(broker_error_context, dict)
                    else None,
                }
            )
        return {
            **dict(metadata or {}),
            "broker_backend": "minqmt_sim",
            "authority_source": authority_source,
            "execution_algo_code": result.algo_code,
            "execution_asset_version": result.asset_metadata.get("asset_version"),
            "execution_policy_id": result.policy_context.get("validated_execution_policy_id"),
            "execution_policy_sha256": result.policy_sha256,
            "execution_terminal_state": result.terminal_state,
            "execution_source_attribution": result.asset_metadata.get("source_attribution"),
            "parent_intent_id": result.parent_intent.intent_id,
            "vnpy_vt_orderid": child.vt_orderid,
            "broker_handle_id": child.handle.handle_id if child.handle else native.get("handle_id"),
            "broker_status": "submit_error" if child.submit_error else child.status.state if child.status else None,
            "broker_raw_status": child.status.raw_status if child.status else None,
            "broker_status_msg": child.status.status_msg if child.status else None,
            "broker_rejection_reason": child.status.rejection_reason if child.status else None,
            "broker_status_raw": child.status.raw if child.status else None,
            "broker_error": child.submit_error,
            "broker_diagnostic": broker_diagnostic,
            "broker_audit": broker_diagnostic["broker_audit"] if broker_diagnostic else None,
            "broker_error_code": broker_diagnostic.get("broker_error_code") if broker_diagnostic else None,
            "broker_rejection_classification": (
                broker_diagnostic.get("broker_rejection_classification") if broker_diagnostic else None
            ),
            "diagnostic_completeness": broker_diagnostic.get("diagnostic_completeness") if broker_diagnostic else None,
            "diagnostic_gap": broker_diagnostic.get("diagnostic_gap", False) if broker_diagnostic else False,
            "status_msg_best_available": broker_diagnostic.get("status_msg_best_available") if broker_diagnostic else None,
            "status_msg_maybe_truncated": broker_diagnostic.get("status_msg_maybe_truncated", False)
            if broker_diagnostic
            else False,
            "miniqmt_trade_count": len(child.trades),
            "child_submit_error": child.submit_error,
            **native,
        }

    @staticmethod
    def _miniqmt_child_error_reason(child: Any) -> str:
        error = child.submit_error or {}
        context = error.get("context") if isinstance(error, dict) else None
        if isinstance(context, dict) and context.get("reason"):
            return str(context["reason"])
        if isinstance(error, dict) and error.get("message"):
            return str(error["message"])
        return "MiniQMT vn.py-style child order submit failed"

    @staticmethod
    def _miniqmt_quote_provider(broker: MiniQMTSimBackend):
        if hasattr(broker, "query_quote"):
            return lambda symbol: broker.query_quote(symbol)  # type: ignore[attr-defined]
        return None

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
        fee_model: FeeModel | None = None,
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
        execution_quality_report = build_minqmt_execution_quality_report(
            portfolio_id=portfolio.portfolio_id,
            run_id=run.run_id,
            trade_date=trade_date,
            orders=orders,
            fills=fills,
            fee_model=fee_model or self._fee_model_from_policy(getattr(portfolio, "fee_policy", {}) or {}),
            fill_count_override=fill_count_override,
            report_scope="native_reconcile" if fill_count_override is not None else "current_run_result",
        )
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
                "execution_quality_report": execution_quality_report,
            },
        )
        session_id = self._miniqmt_session_id_from_run(run)
        open_orders = non_terminal_orders_for_run_success(orders)
        if session_id and hasattr(self.repository, "get_session") and open_orders:
            pending_portfolio = self.repository.update_portfolio_status(portfolio.portfolio_id, PortfolioStatus.RUNNING)
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="MINIQMT_RUN_PENDING_RECONCILE",
                message="MiniQMT broker-authoritative snapshot persisted with non-terminal orders; run requires later broker reconciliation",
                context={
                    "reason_code": "PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS",
                    "order_count": len(orders),
                    "fill_count": fill_count,
                    "new_fill_count": len(fills),
                    "open_order_count": len(open_orders),
                    "open_orders": open_orders,
                },
            )
            return PaperDayRunResult(
                portfolio=pending_portfolio,
                run=run,
                orders=orders,
                fills=fills,
                events=events,
                positions=position_list,
                account_snapshot=snapshot,
            )
        if not session_id and hasattr(self.repository, "get_session") and open_orders:
            orders, terminal_events = self._terminalize_minqmt_orders_before_non_live_success(
                run=run,
                trade_date=trade_date,
                broker=broker,
                orders=orders,
            )
            events.extend(terminal_events)
        if hasattr(self.repository, "get_session"):
            self._assert_orders_terminal_before_success(run, orders)
        succeeded = run if run.status == RunStatus.SUCCEEDED else self.repository.update_run_status(run, RunStatus.SUCCEEDED)
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
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="MINIQMT_EXECUTION_QUALITY_REPORTED",
            message="MiniQMT execution quality and broker-cost reconciliation report persisted",
            context=execution_quality_report,
        )
        self.repository.save_run_event(
            run_id=run.run_id,
            event_type="RUN_SUCCEEDED",
            message="paper v2 MiniQMT day run reconciled against broker authority",
            context={
                "broker_backend": "minqmt_sim",
                "authority_source": "MINIQMT_QUERY",
                "order_count": len(orders),
                "fill_count": fill_count,
                "new_fill_count": len(fills),
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

    def _assert_orders_terminal_before_success(self, run: PaperRun, orders: list[Any] | None = None) -> None:
        checked_orders = list(orders) if orders is not None else self.repository.list_orders_for_run(run.run_id)
        assert_orders_terminal_before_run_success(run_id=run.run_id, orders=checked_orders)

    def _terminalize_minqmt_orders_before_non_live_success(
        self,
        *,
        run: PaperRun,
        trade_date: date,
        broker: MiniQMTSimBackend,
        orders: list[Any],
    ) -> tuple[list[Any], list[OrderEvent]]:
        terminalized: list[Any] = []
        events: list[OrderEvent] = []
        for order in orders:
            if not non_terminal_orders_for_run_success([order]):
                terminalized.append(order)
                continue
            native = self._miniqmt_native_context_from_order(order)
            if native is None:
                raise InvalidStateTransitionError(
                    "MiniQMT non-live run cannot be marked SUCCEEDED because an open order lacks native broker ids",
                    context={
                        "reason_code": "PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS",
                        "run_id": run.run_id,
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "status": order.status.value,
                    },
                )
            handle = OrderHandle(
                handle_id=native["handle_id"],
                backend_id="minqmt_sim",
                submitted_at=order.created_at,
                intent_id=order.intent_id,
            )
            try:
                ack = broker.cancel(handle)
            except TradingCoreError:
                raise
            except Exception as exc:
                raise BrokerSubmitError(
                    "MiniQMT non-live run failed to cancel open order before success",
                    context={
                        "reason_code": "PAPER_V2_RUN_TERMINALIZE_CANCEL_FAILED",
                        "run_id": run.run_id,
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "handle_id": handle.handle_id,
                        "exception_type": type(exc).__name__,
                        "exception_message": str(exc),
                    },
                ) from exc
            if not ack.accepted:
                raise InvalidStateTransitionError(
                    "MiniQMT non-live run cannot be marked SUCCEEDED because open order cancel was rejected",
                    context={
                        "reason_code": "PAPER_V2_RUN_TERMINALIZE_CANCEL_REJECTED",
                        "run_id": run.run_id,
                        "order_id": order.order_id,
                        "symbol": order.symbol,
                        "handle_id": handle.handle_id,
                        "cancel_reason": ack.reason,
                    },
                )
            final_order, event = self.oms.cancel_order(
                order,
                ack.reason or "MiniQMT non-live run terminalized open order before success",
            )
            metadata = {
                **dict(final_order.metadata or {}),
                "authority_source": "MINIQMT_NON_LIVE_TERMINALIZE_BEFORE_SUCCESS",
                "terminalize_reason_code": "PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS",
                "terminalize_trade_date": trade_date.isoformat(),
                "terminalize_cancel_ack": ack.model_dump(mode="json"),
            }
            final_order = final_order.model_copy(update={"metadata": metadata})
            event = event.model_copy(update={"metadata": metadata})
            self.repository.save_order_event(run.run_id, event)
            self.repository.save_order(run.run_id, final_order)
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="MINIQMT_NON_LIVE_ORDER_CANCELLED_BEFORE_SUCCESS",
                message="MiniQMT non-live run cancelled an open order before marking the run succeeded",
                context={
                    "reason_code": "PAPER_V2_RUN_SUCCEEDED_REQUIRES_TERMINAL_ORDERS",
                    "order_id": order.order_id,
                    "symbol": order.symbol,
                    "previous_status": order.status.value,
                    "final_status": final_order.status.value,
                    "cancel_ack": ack.model_dump(mode="json"),
                },
            )
            terminalized.append(final_order)
            events.append(event)
        return terminalized, events

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
            **miniqmt_broker_kwargs_for_portfolio(self.repository, portfolio, package_id=portfolio.package_id),
        )
        session_id = self._miniqmt_session_id_from_run(run)
        new_fills: list[Fill] = []
        order_events: list[OrderEvent] = []
        reconciled_orders = []
        try:
            existing_rows = self._miniqmt_existing_fill_rows(run.run_id)
            existing_fill_ids = {str(row.get("fill_id") or "") for row in existing_rows if row.get("fill_id")}
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
                audit_before = self._miniqmt_broker_audit_snapshot(
                    broker,
                    phase="before_native_reconcile",
                    intent=self._miniqmt_intent_from_order(order, trade_date=trade_date),
                    native=native,
                )
                status = broker.query_status_from_native(intent=intent, **native)
                trade_rows = broker.query_trades_from_native(intent=intent, **native)
                audit_after = self._miniqmt_broker_audit_snapshot(
                    broker,
                    phase="after_native_reconcile",
                    intent=intent,
                    native=native,
                    status=status,
                )
                existing_order_fill_rows = [
                    row for row in existing_rows if str(row.get("order_id") or "") == str(order.order_id)
                ]
                fill_base_order = self._miniqmt_reconcile_fill_base_order(order, existing_order_fill_rows)
                candidate_fills = self._miniqmt_new_fills_from_trades(
                    trade_rows,
                    order=fill_base_order,
                    native=native,
                    trade_date=trade_date,
                    existing_fill_rows=existing_order_fill_rows,
                )
                final_order = fill_base_order if candidate_fills else order
                for fill in candidate_fills:
                    if fill.fill_id in existing_fill_ids:
                        continue
                    if isinstance(fill.metadata, dict) and fill.metadata.get("broker_reconcile_delta_capped"):
                        self.repository.save_run_event(
                            run_id=run.run_id,
                            event_type="MINIQMT_NATIVE_RECONCILE_OVERFILL_CAPPED",
                            message="MiniQMT native trade delta exceeded Paper order remaining quantity and was capped",
                            context={
                                "order_id": order.order_id,
                                "symbol": order.symbol,
                                "remaining_quantity": final_order.remaining_quantity,
                                "broker_reported_fill_quantity": fill.metadata.get("broker_reported_fill_quantity"),
                                "applied_fill_quantity": fill.metadata.get("applied_fill_quantity"),
                                "authority_source": "MINIQMT_NATIVE_RECONCILE",
                            },
                        )
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
                    new_fills.append(fill)
                    order_events.append(event)
                final_order = self._reconcile_minqmt_order_status(
                    final_order,
                    status=status,
                    native=native,
                    visible_trade_count=len(trade_rows),
                    audit_before=audit_before,
                    audit_after=audit_after,
                    authority_source="MINIQMT_NATIVE_RECONCILE",
                )
                native_reconcile_event = self._miniqmt_native_terminal_order_event(
                    run_id=run.run_id,
                    previous_order=order,
                    final_order=final_order,
                    status=status,
                    native=native,
                    visible_trade_count=len(trade_rows),
                    audit_before=audit_before,
                    audit_after=audit_after,
                )
                if native_reconcile_event is not None:
                    self.repository.save_order_event(run.run_id, native_reconcile_event)
                    order_events.append(native_reconcile_event)
                    self.repository.save_run_event(
                        run_id=run.run_id,
                        event_type=f"MINIQMT_NATIVE_ORDER_{native_reconcile_event.event_type.value}_RECONCILED",
                        message="MiniQMT native terminal order state reconciled with broker diagnostic context",
                        context=native_reconcile_event.metadata,
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
                                "broker_raw_status": status.raw_status,
                                "broker_status_msg": status.status_msg,
                                "broker_rejection_reason": status.rejection_reason,
                                "trade_count": len(trade_rows),
                                "broker_diagnostic": self._miniqmt_order_diagnostic(
                                    status=status,
                                    native=native,
                                    visible_trade_count=len(trade_rows),
                                    audit_before=audit_before,
                                    audit_after=audit_after,
                                    authority_source="MINIQMT_NATIVE_RECONCILE",
                                ),
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
                fee_model=self._fee_model_from_policy(getattr(portfolio, "fee_policy", {}) or {}),
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
        audit_before: dict[str, Any] | None = None,
        audit_after: dict[str, Any] | None = None,
        authority_source: str = "MINIQMT_NATIVE_RECONCILE",
    ) -> Any:
        broker_state = self._miniqmt_order_status_from_handle(status)
        metadata = self._miniqmt_metadata_with_status(
            order.metadata,
            status=status,
            native=native,
            visible_trade_count=visible_trade_count,
            audit_before=audit_before,
            audit_after=audit_after,
            authority_source=authority_source,
        )
        if broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED} and status.filled_quantity > order.filled_quantity:
            update: dict[str, Any] = {
                "status": broker_state,
                "filled_quantity": min(status.filled_quantity, order.quantity),
                "metadata": metadata,
            }
            if status.avg_fill_price is not None:
                update["avg_fill_price"] = float(status.avg_fill_price)
            return order.model_copy(update=update)
        if broker_state in {OrderStatus.REJECTED, OrderStatus.CANCELLED} and order.status != broker_state:
            return order.model_copy(update={"status": broker_state, "metadata": metadata})
        if order.status != broker_state and broker_state in {OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED}:
            return order.model_copy(update={"status": broker_state, "metadata": metadata})
        return order.model_copy(update={"metadata": metadata})

    @classmethod
    def _miniqmt_native_terminal_order_event(
        cls,
        *,
        run_id: str,
        previous_order: Any,
        final_order: Any,
        status: Any,
        native: dict[str, Any],
        visible_trade_count: int,
        audit_before: dict[str, Any] | None,
        audit_after: dict[str, Any] | None,
    ) -> OrderEvent | None:
        broker_state = cls._miniqmt_order_status_from_handle(status)
        if broker_state not in {OrderStatus.REJECTED, OrderStatus.CANCELLED}:
            return None
        if not cls._miniqmt_needs_terminal_diagnostic_event(previous_order, final_order, status):
            return None
        diagnostic = cls._miniqmt_order_diagnostic(
            status=status,
            native=native,
            visible_trade_count=visible_trade_count,
            audit_before=audit_before,
            audit_after=audit_after,
            authority_source="MINIQMT_NATIVE_RECONCILE",
        )
        event_type = OrderEventType.REJECTED if broker_state == OrderStatus.REJECTED else OrderEventType.CANCELLED
        metadata = {
            **diagnostic,
            "previous_paper_status": previous_order.status.value,
            "paper_order_status": final_order.status.value,
            "terminal_reconcile_event": True,
        }
        return OrderEvent(
            event_id=cls._miniqmt_native_terminal_event_id(
                run_id=run_id,
                order_id=final_order.order_id,
                event_type=event_type,
                status=status,
                native=native,
            ),
            order_id=final_order.order_id,
            event_type=event_type,
            event_time=status.last_event_at,
            reason=status.rejection_reason or status.status_msg or f"MiniQMT order {broker_state.value.lower()}",
            metadata=metadata,
        )

    @classmethod
    def _miniqmt_needs_terminal_diagnostic_event(cls, previous_order: Any, final_order: Any, status: Any) -> bool:
        if previous_order.status != final_order.status:
            return True
        metadata = previous_order.metadata if isinstance(previous_order.metadata, dict) else {}
        diagnostic = metadata.get("broker_diagnostic") if isinstance(metadata.get("broker_diagnostic"), dict) else {}
        if not diagnostic:
            return True
        if metadata.get("broker_status") != status.state:
            return True
        if metadata.get("broker_raw_status") != status.raw_status:
            return True
        if status.status_msg and metadata.get("broker_status_msg") != status.status_msg:
            return True
        return False

    @staticmethod
    def _miniqmt_native_terminal_event_id(
        *,
        run_id: str,
        order_id: str,
        event_type: OrderEventType,
        status: Any,
        native: dict[str, Any],
    ) -> str:
        payload = {
            "run_id": run_id,
            "order_id": order_id,
            "event_type": event_type.value,
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "broker_status": status.state,
            "broker_raw_status": status.raw_status,
            "broker_status_msg": status.status_msg,
        }
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
        return f"evt_minqmt_native_{digest[:24]}"

    @classmethod
    def _miniqmt_metadata_with_status(
        cls,
        metadata: dict[str, Any] | None,
        *,
        status: Any,
        native: dict[str, Any],
        visible_trade_count: int,
        audit_before: dict[str, Any] | None = None,
        audit_after: dict[str, Any] | None = None,
        authority_source: str = "MINIQMT",
    ) -> dict[str, Any]:
        diagnostic = cls._miniqmt_order_diagnostic(
            status=status,
            native=native,
            visible_trade_count=visible_trade_count,
            audit_before=audit_before,
            audit_after=audit_after,
            authority_source=authority_source,
        )
        return {
            **dict(metadata or {}),
            "broker_backend": "minqmt_sim",
            "authority_source": authority_source,
            "broker_handle_id": native.get("handle_id"),
            "broker_status": status.state,
            "broker_raw_status": status.raw_status,
            "broker_status_msg": status.status_msg,
            "broker_rejection_reason": status.rejection_reason,
            "broker_status_raw": status.raw,
            "broker_diagnostic": diagnostic,
            "broker_audit": diagnostic["broker_audit"],
            "broker_error_code": diagnostic.get("broker_error_code"),
            "broker_rejection_classification": diagnostic.get("broker_rejection_classification"),
            "diagnostic_completeness": diagnostic.get("diagnostic_completeness"),
            "diagnostic_gap": diagnostic.get("diagnostic_gap", False),
            "status_msg_best_available": diagnostic.get("status_msg_best_available"),
            "status_msg_maybe_truncated": diagnostic.get("status_msg_maybe_truncated", False),
            "miniqmt_trade_count": visible_trade_count,
            **native,
        }

    @classmethod
    def _miniqmt_order_diagnostic(
        cls,
        *,
        status: Any,
        native: dict[str, Any],
        visible_trade_count: int,
        audit_before: dict[str, Any] | None,
        audit_after: dict[str, Any] | None,
        authority_source: str = "MINIQMT",
    ) -> dict[str, Any]:
        status_msg_quality = cls._miniqmt_status_msg_quality(status.status_msg, status.raw)
        diagnostic_gap = bool(status_msg_quality.get("diagnostic_gap") or status.raw.get("diagnostic_gap"))
        gap_reason = status.raw.get("diagnostic_gap_reason") or status_msg_quality.get("diagnostic_gap_reason")
        return {
            "schema_version": "miniqmt_order_diagnostic_v1",
            "broker_backend": "minqmt_sim",
            "authority_source": authority_source,
            "broker_handle_id": native.get("handle_id"),
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "strategy_name": native.get("strategy_name"),
            "order_remark": native.get("order_remark"),
            "broker_status": status.state,
            "broker_raw_status": status.raw_status,
            "broker_status_msg": status.status_msg,
            "broker_rejection_reason": status.rejection_reason,
            "broker_status_raw": status.raw,
            "broker_error_code": cls._miniqmt_broker_error_code(status.status_msg),
            "broker_rejection_classification": cls._miniqmt_rejection_classification(status),
            "diagnostic_completeness": status_msg_quality["diagnostic_completeness"],
            "diagnostic_gap": diagnostic_gap,
            "diagnostic_gap_reason": gap_reason,
            "status_msg_best_available": status.status_msg,
            "status_msg_present": status_msg_quality["status_msg_present"],
            "status_msg_maybe_truncated": status_msg_quality["status_msg_maybe_truncated"],
            "status_msg_encoding_warning": status_msg_quality["status_msg_encoding_warning"],
            "visible_trade_count": visible_trade_count,
            "broker_audit": cls._miniqmt_audit_pair(audit_before, audit_after),
        }

    @classmethod
    def _miniqmt_submit_error_diagnostic(
        cls,
        exc: TradingCoreError,
        *,
        intent: Any,
        audit_before: dict[str, Any] | None,
        audit_after: dict[str, Any] | None,
    ) -> dict[str, Any]:
        native = dict(audit_after.get("native") or {}) if isinstance(audit_after, dict) else {}
        runtime_evidence = (
            exc.context.get("runtime_evidence")
            if isinstance(exc.context, dict) and isinstance(exc.context.get("runtime_evidence"), dict)
            else None
        )
        return {
            "schema_version": "miniqmt_order_diagnostic_v1",
            "broker_backend": "minqmt_sim",
            "authority_source": "MINIQMT_SUBMIT_ERROR",
            "runtime_owner": "MiniQMTExecutionRuntime" if runtime_evidence is not None else None,
            "runtime_evidence": runtime_evidence,
            "intent_id": intent.intent_id,
            "symbol": intent.symbol,
            "side": intent.side.value,
            "quantity": intent.quantity,
            "broker_handle_id": native.get("handle_id"),
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "strategy_name": native.get("strategy_name"),
            "order_remark": native.get("order_remark"),
            "broker_error": exc.to_dict(),
            "broker_status": "submit_error",
            "broker_status_msg": exc.message,
            "broker_rejection_reason": exc.message,
            "broker_status_raw": exc.to_dict(),
            "submit_diagnostic": exc.context.get("submit_diagnostic") if isinstance(exc.context, dict) else None,
            "broker_audit": cls._miniqmt_audit_pair(audit_before, audit_after),
        }

    @staticmethod
    def _miniqmt_submit_error_native(exc: TradingCoreError) -> dict[str, Any]:
        context = exc.context if isinstance(exc.context, dict) else {}
        native_keys = ("handle_id", "miniqmt_order_id", "strategy_name", "order_remark")
        native = {key: context.get(key) for key in native_keys if context.get(key) is not None}
        if context.get("message") is not None:
            native["broker_submit_message"] = context.get("message")
        return native

    @staticmethod
    def _miniqmt_broker_error_code(status_msg: str | None) -> str | None:
        if not status_msg:
            return None
        match = re.search(r"\[(\d{6})\]", status_msg)
        return match.group(1) if match else None

    @staticmethod
    def _miniqmt_rejection_classification(status: Any) -> str | None:
        if status.state != "rejected":
            return None
        error_code = PaperTradingDayRunner._miniqmt_broker_error_code(status.status_msg)
        if error_code:
            return f"counter_{error_code}"
        return "broker_rejected"

    @staticmethod
    def _miniqmt_status_msg_quality(status_msg: str | None, raw: dict[str, Any]) -> dict[str, Any]:
        message = str(status_msg or "")
        if raw.get("diagnostic_gap"):
            return {
                "diagnostic_completeness": "missing_broker_order_snapshot",
                "diagnostic_gap": True,
                "diagnostic_gap_reason": raw.get("diagnostic_gap_reason") or "native_order_snapshot_not_found",
                "status_msg_present": bool(message),
                "status_msg_maybe_truncated": False,
                "status_msg_encoding_warning": False,
            }
        if not message:
            return {
                "diagnostic_completeness": "broker_status_msg_unavailable",
                "diagnostic_gap": True,
                "diagnostic_gap_reason": "broker_status_msg_missing",
                "status_msg_present": False,
                "status_msg_maybe_truncated": False,
                "status_msg_encoding_warning": False,
            }
        mojibake_tokens = ("\u00c3", "\u00c2", "\u00e5", "\u00e6", "\u00e4")
        encoding_warning = "\ufffd" in message or any(token in message for token in mojibake_tokens)
        maybe_truncated = message.count("[") > message.count("]") or message.endswith(("[", ":", ";"))
        code_only = re.fullmatch(r"(?:\[[^\]]+\])+", message) is not None
        if maybe_truncated or encoding_warning:
            return {
                "diagnostic_completeness": "broker_status_msg_truncated_or_encoding_uncertain",
                "diagnostic_gap": True,
                "diagnostic_gap_reason": "broker_status_msg_truncated_or_encoding_uncertain",
                "status_msg_present": True,
                "status_msg_maybe_truncated": True,
                "status_msg_encoding_warning": encoding_warning,
            }
        if code_only:
            return {
                "diagnostic_completeness": "broker_status_msg_code_only",
                "diagnostic_gap": True,
                "diagnostic_gap_reason": "broker_status_msg_code_only",
                "status_msg_present": True,
                "status_msg_maybe_truncated": False,
                "status_msg_encoding_warning": False,
            }
        return {
            "diagnostic_completeness": "best_available",
            "diagnostic_gap": False,
            "diagnostic_gap_reason": None,
            "status_msg_present": True,
            "status_msg_maybe_truncated": False,
            "status_msg_encoding_warning": False,
        }

    @staticmethod
    def _miniqmt_audit_pair(
        audit_before: dict[str, Any] | None,
        audit_after: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "miniqmt_broker_audit_v1",
            "before_submit": audit_before or {},
            "after_reconcile": audit_after or {},
        }

    @staticmethod
    def _miniqmt_broker_audit_snapshot(
        broker: Any,
        *,
        phase: str,
        intent: Any | None = None,
        native: dict[str, Any] | None = None,
        status: Any | None = None,
    ) -> dict[str, Any]:
        snapshot: dict[str, Any] = {
            "schema_version": "miniqmt_broker_audit_snapshot_v1",
            "phase": phase,
            "captured_at": datetime.now(UTC).isoformat(),
            "intent": PaperTradingDayRunner._miniqmt_intent_audit(intent) if intent is not None else None,
            "native": dict(native or {}),
            "status": PaperTradingDayRunner._miniqmt_status_audit(status) if status is not None else None,
        }
        try:
            account = broker.query_account()
            snapshot["account"] = {
                "cash": float(account.cash),
                "nav": float(account.nav),
                "margin_used": float(account.margin_used) if account.margin_used is not None else None,
                "as_of": account.as_of.isoformat(),
            }
        except Exception as exc:  # noqa: BLE001 - diagnostics must preserve query failure details.
            snapshot["account_error"] = f"{type(exc).__name__}: {exc}"
        try:
            positions, prices = broker.query_position_marks()
            snapshot["positions"] = {
                "count": len(positions),
                "symbols": sorted(positions)[:50],
                "market_value": sum(float(pos.quantity) * float(prices.get(symbol, 0.0)) for symbol, pos in positions.items()),
                "missing_price_symbols": sorted(symbol for symbol in positions if symbol not in prices)[:50],
            }
        except Exception as exc:  # noqa: BLE001 - diagnostics must preserve query failure details.
            snapshot["positions_error"] = f"{type(exc).__name__}: {exc}"
        return snapshot

    @staticmethod
    def _miniqmt_intent_audit(intent: Any) -> dict[str, Any]:
        return {
            "intent_id": intent.intent_id,
            "symbol": intent.symbol,
            "side": intent.side.value,
            "quantity": intent.quantity,
            "order_type": intent.order_type.value,
            "limit_price": intent.limit_price,
            "target_trade_date": intent.target_trade_date.isoformat(),
        }

    @staticmethod
    def _miniqmt_status_audit(status: Any) -> dict[str, Any]:
        return {
            "broker_status": status.state,
            "broker_raw_status": status.raw_status,
            "broker_status_msg": status.status_msg,
            "broker_rejection_reason": status.rejection_reason,
            "filled_quantity": status.filled_quantity,
            "avg_fill_price": float(status.avg_fill_price) if status.avg_fill_price is not None else None,
            "last_event_at": status.last_event_at.isoformat(),
            "raw": status.raw,
        }

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

    @staticmethod
    def _miniqmt_reconcile_fill_base_order(order: Any, existing_fill_rows: list[dict[str, Any]]) -> Any:
        if existing_fill_rows or not order.filled_quantity:
            return order
        if order.status not in {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED, OrderStatus.PARTIALLY_FILLED}:
            return order
        return order.model_copy(update={"status": OrderStatus.SUBMITTED, "filled_quantity": 0, "avg_fill_price": None})

    @classmethod
    def _miniqmt_new_fills_from_trades(
        cls,
        trades: list[dict[str, Any]],
        *,
        order: Any,
        native: dict[str, Any],
        trade_date: date,
        existing_fill_rows: list[dict[str, Any]],
    ) -> list[Fill]:
        existing_trade_keys = cls._miniqmt_existing_trade_keys(existing_fill_rows)
        new_trades: list[dict[str, Any]] = []
        seen_new_trade_keys: set[str] = set()
        for trade in trades:
            trade_key = cls._miniqmt_trade_key(trade)
            if trade_key in existing_trade_keys or trade_key in seen_new_trade_keys:
                continue
            seen_new_trade_keys.add(trade_key)
            new_trades.append(dict(trade))
        if not new_trades:
            return []
        candidate_fills = cls._miniqmt_fills_from_trades(
            new_trades,
            order=order,
            native=native,
            trade_date=trade_date,
        )
        return cls._miniqmt_cap_fills_to_remaining(candidate_fills, order=order)

    @classmethod
    def _miniqmt_existing_trade_keys(cls, existing_fill_rows: list[dict[str, Any]]) -> set[str]:
        keys: set[str] = set()
        for row in existing_fill_rows:
            metadata = row.get("metadata") if isinstance(row, dict) else None
            if not isinstance(metadata, dict):
                metadata = {}
            raw_rows = metadata.get("miniqmt_trade_raw_rows")
            if isinstance(raw_rows, list):
                for raw in raw_rows:
                    if isinstance(raw, dict):
                        keys.add(cls._miniqmt_trade_key(raw))
            raw = metadata.get("miniqmt_trade_raw")
            if isinstance(raw, dict):
                keys.add(cls._miniqmt_trade_key(raw))
            traded_id = str(metadata.get("traded_id") or "").strip()
            if traded_id:
                keys.add(f"traded_id:{traded_id}")
        return keys

    @staticmethod
    def _miniqmt_trade_key(trade: dict[str, Any]) -> str:
        traded_id = str(trade.get("traded_id") or "").strip()
        if traded_id:
            return f"traded_id:{traded_id}"
        payload = {
            "order_id": str(trade.get("order_id") or ""),
            "order_sysid": str(trade.get("order_sysid") or ""),
            "stock_code": str(trade.get("stock_code") or ""),
            "order_type": str(trade.get("order_type") or ""),
            "traded_time": str(trade.get("traded_time") or ""),
            "quantity": int(trade.get("traded_volume") or 0),
            "price": float(trade.get("traded_price") or 0.0),
        }
        encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return f"trade:{encoded}"

    @staticmethod
    def _miniqmt_cap_fills_to_remaining(fills: list[Fill], *, order: Any) -> list[Fill]:
        remaining = max(0, int(order.remaining_quantity))
        capped: list[Fill] = []
        for fill in fills:
            if remaining <= 0:
                break
            if fill.quantity <= remaining:
                capped.append(fill)
                remaining -= fill.quantity
                continue
            metadata = dict(fill.metadata or {})
            metadata.update(
                {
                    "broker_reconcile_delta_capped": True,
                    "broker_reported_fill_quantity": fill.quantity,
                    "applied_fill_quantity": remaining,
                    "cap_reason": "paper_order_remaining_quantity",
                }
            )
            capped.append(fill.model_copy(update={"quantity": remaining, "metadata": metadata}))
            remaining = 0
        return capped

    @classmethod
    def _miniqmt_fills_from_trades(
        cls,
        trades: list[dict[str, Any]],
        *,
        order: Any,
        native: dict[str, Any],
        trade_date: date,
    ) -> list[Fill]:
        unique: list[dict[str, Any]] = []
        seen: set[str] = set()
        for trade in trades:
            fill_id = cls._miniqmt_fill_id(trade, order_id=order.order_id)
            if fill_id in seen:
                continue
            seen.add(fill_id)
            unique.append(dict(trade))
        if not unique:
            return []
        if len(unique) == 1:
            try:
                return [cls._miniqmt_fill_from_trade(unique[0], order=order, native=native, trade_date=trade_date)]
            except ValueError:
                pass
        return cls._miniqmt_aggregate_fill_from_trades(unique, order=order, native=native, trade_date=trade_date)

    @classmethod
    def _miniqmt_aggregate_fill_from_trades(
        cls,
        trades: list[dict[str, Any]],
        *,
        order: Any,
        native: dict[str, Any],
        trade_date: date,
    ) -> list[Fill]:
        total_quantity = sum(max(0, int(trade.get("traded_volume") or 0)) for trade in trades)
        if total_quantity <= 0:
            return []
        total_amount = sum(
            max(0, int(trade.get("traded_volume") or 0)) * float(trade.get("traded_price") or 0.0)
            for trade in trades
        )
        avg_price = total_amount / total_quantity if total_quantity else 0.0
        trade_times = [cls._parse_minqmt_trade_time(trade_date, trade.get("traded_time")) for trade in trades]
        trade_time = max(trade_times) if trade_times else datetime.now(UTC)
        raw = dict(trades[-1])
        raw.update(
            {
                "traded_volume": total_quantity,
                "traded_price": avg_price,
                "traded_amount": total_amount,
                "order_id": native.get("miniqmt_order_id"),
            }
        )
        payload = {
            "order_id": order.order_id,
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "trade_keys": [
                {
                    "traded_id": str(trade.get("traded_id") or ""),
                    "order_sysid": str(trade.get("order_sysid") or ""),
                    "traded_time": str(trade.get("traded_time") or ""),
                    "quantity": int(trade.get("traded_volume") or 0),
                    "price": float(trade.get("traded_price") or 0.0),
                }
                for trade in trades
            ],
        }
        digest = hashlib.sha256(json.dumps(payload, ensure_ascii=True, sort_keys=True).encode("utf-8")).hexdigest()
        metadata = {
            "broker_backend": "minqmt_sim",
            "authority_source": "MINIQMT_TRADE_AGGREGATE",
            "miniqmt_order_id": native.get("miniqmt_order_id"),
            "strategy_name": str(raw.get("strategy_name") or native.get("strategy_name") or ""),
            "order_remark": str(raw.get("order_remark") or native.get("order_remark") or ""),
            "trade_count": len(trades),
            "broker_reported_commission": sum(float(trade.get("commission") or 0.0) for trade in trades),
            "broker_reported_fee_total": sum(float(trade.get("commission") or 0.0) for trade in trades),
            "trade_amount": total_amount,
            "cost_precision_level": "broker_aggregate",
            "cost_breakdown_source": "broker_reported_aggregate",
            "miniqmt_trade_raw": raw,
            "miniqmt_trade_raw_rows": trades,
        }
        try:
            return [
                Fill(
                    fill_id=f"fill_minqmt_agg_{digest[:24]}",
                    order_id=order.order_id,
                    symbol=order.symbol,
                    side=order.side,
                    quantity=total_quantity,
                    price=avg_price,
                    trade_time=trade_time,
                    bar_time=trade_time,
                    reason="miniqmt_trade_reconciliation_aggregate",
                    metadata=metadata,
                )
            ]
        except ValueError:
            return []

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
            "broker_reported_commission": float(trade.get("commission") or 0.0),
            "broker_reported_fee_total": float(trade.get("commission") or 0.0),
            "trade_amount": float(trade.get("traded_amount") or 0.0),
            "cost_precision_level": "broker_aggregate",
            "cost_breakdown_source": "broker_reported_aggregate",
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
