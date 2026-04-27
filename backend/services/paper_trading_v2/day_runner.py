"""Authoritative single-day Paper Trading v2 runner."""

from __future__ import annotations

from datetime import date
from typing import Any

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.paper_trading_v2.market_data import PaperV2MinuteMarketDataProvider, TradeCalendarProvider
from backend.services.selection_center.runtime_profile import normalize_selection_runtime_config, parse_selection_runtime_profile
from backend.services.selection_center.tradability import TradabilityFilter
from backend.services.strategy_package.runtime import RebalanceEngine, StrategyPackageRuntime, TargetPositionEngine
from backend.services.strategy_package.selection_artifact import (
    StrategyPackageSelectionArtifactService,
    selection_artifact_runtime_hash,
)
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import DataUnavailableError, InvalidStateTransitionError, StrategyPackageValidationError, TradingCoreError
from backend.services.trading_core.execution_algo_capabilities import required_minute_bars_for_policy
from backend.services.trading_core.ledger import FeeModel, InMemoryLedger
from backend.services.trading_core.minute_execution import MinuteExecutionEngine
from backend.services.trading_core.models import PositionLot, RunStatus
from backend.services.trading_core.oms import OMS

from .models import PaperDayRunResult, PaperRun, PortfolioStatus
from .repository import PaperTradingV2Repository
from .service import PaperTradingV2PortfolioService


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
        tradability_filter: TradabilityFilter | Any | None = None,
        refresh_audit: DataRefreshAuditRepository | Any | None = None,
        selection_artifact_service: StrategyPackageSelectionArtifactService | Any | None = None,
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
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
        )

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
            raise StrategyPackageValidationError(
                "paper run trade_date cannot be before portfolio start_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "start_date": portfolio.start_date.isoformat(),
                },
            )
        manifest = portfolio.frozen_manifest
        if manifest.package_id != portfolio.package_id or manifest.manifest_sha256 != portfolio.manifest_sha256:
            raise StrategyPackageValidationError(
                "portfolio frozen manifest does not match frozen package invariants",
                context={"portfolio_id": portfolio_id, "package_id": portfolio.package_id},
            )
        self.validator.validate_for_paper_trading(manifest)
        self.calendar_provider.ensure_trading_day(trade_date)
        existing_run = self.repository.get_run_by_portfolio_date(portfolio_id, trade_date)
        if existing_run is not None:
            raise StrategyPackageValidationError(
                "paper v2 run already exists for portfolio trade_date",
                context={
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                    "existing_run_id": existing_run.run_id,
                    "existing_status": existing_run.status.value,
                },
            )

        config = PaperTradingV2PortfolioService(repository=self.repository).resolve_runtime_config_for_date(
            portfolio=portfolio,
            trade_date=trade_date,
            runtime_config=runtime_config or {},
        )
        runtime_profile = parse_selection_runtime_profile(config)
        self._reject_raw_execution_overrides(config)
        execution_policy_context = self._execution_policy_context_for_date(portfolio, trade_date)
        execution_policy_json = execution_policy_context["policy_json"]
        self.validator.validate_execution_policy_for_paper(
            package_id=manifest.package_id,
            policy_json=execution_policy_json,
        )
        execution_algo_config = dict(execution_policy_json.get("algo_config") or {})
        config["validated_execution_policy"] = execution_policy_context
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

        try:
            data_ready = self._require_data_ready(manifest=manifest, trade_date=trade_date, runtime_config=config)
            self.repository.save_run_event(
                run_id=run.run_id,
                event_type="DATA_READY",
                message="required paper v2 datasets are ready",
                context={"datasets": data_ready},
            )
            current_positions = self.repository.load_latest_positions(portfolio_id, trade_date)
            latest_cash = self.repository.load_latest_cash(portfolio, trade_date)
            total_equity = self._resolve_total_equity(
                latest_cash=latest_cash,
                current_positions=current_positions,
                runtime_config=config,
                portfolio_id=portfolio_id,
            )
            self._ensure_authoritative_selection_artifact(
                manifest=manifest,
                trade_date=trade_date,
                data_source=portfolio.data_source.value,
                runtime_config=config,
            )
            snapshot = self.runtime.build_signal_snapshot(
                manifest=manifest,
                trade_date=trade_date,
                data_source=portfolio.data_source.value,
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
            top_k = int(runtime_profile.selection.top_k or manifest.portfolio_policy.topk)
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
            targets = self.target_engine.build_targets(
                snapshot=snapshot,
                total_equity=total_equity,
                top_k=top_k,
            )
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

            ledger = InMemoryLedger(
                portfolio_id=portfolio_id,
                initial_cash=float(portfolio.initial_cash),
                fee_model=fee_model or self._fee_model_from_policy(portfolio.fee_policy),
            )
            ledger.cash = latest_cash
            ledger.positions = dict(current_positions)
            ledger.settle_trade_date(trade_date)

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
                for fill in order_fills:
                    ledger.apply_fill(fill)
                    self.repository.save_fill(run.run_id, fill)
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
                raise StrategyPackageValidationError(
                    "paper v2 day run produced no fills; no-trade day is not yet modeled as a successful state",
                    context={
                        "portfolio_id": portfolio_id,
                        "run_id": run.run_id,
                        "trade_date": trade_date.isoformat(),
                        "order_count": len(orders),
                        "order_event_count": len(events),
                    },
                )
            for symbol in ledger.positions:
                if symbol not in snapshot_prices:
                    raise DataUnavailableError(
                        "snapshot price is required for held position",
                        context={"portfolio_id": portfolio_id, "symbol": symbol, "trade_date": trade_date.isoformat()},
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
            error = exc.to_dict()
            self.repository.save_error(run_id=run.run_id, portfolio_id=portfolio_id, error=error)
            failed = self.repository.update_run_status(run, RunStatus.FAILED, error=error)
            self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.FAILED)
            self.repository.save_run_event(run_id=run.run_id, event_type="RUN_FAILED", message=exc.message, context=exc.context)
            run = failed
            raise
        except Exception as exc:
            error = {"error_code": "PAPER_V2_RUN_ERROR", "message": str(exc), "context": {"portfolio_id": portfolio_id, "run_id": run.run_id}}
            self.repository.save_error(run_id=run.run_id, portfolio_id=portfolio_id, error=error)
            self.repository.update_run_status(run, RunStatus.FAILED, error=error)
            self.repository.update_portfolio_status(portfolio_id, PortfolioStatus.FAILED)
            self.repository.save_run_event(run_id=run.run_id, event_type="RUN_FAILED", message=str(exc), context=error["context"])
            raise

    def _require_data_ready(self, *, manifest: Any, trade_date: date, runtime_config: dict[str, Any]) -> list[dict[str, Any]]:
        ready: list[dict[str, Any]] = []
        requirements = manifest.minute_execution_policy.data_requirements
        runtime_profile = parse_selection_runtime_profile(runtime_config)
        if requirements.requires_suspend_status or runtime_profile.tradability.exclude_suspended:
            status = self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
            ready.append(self._refresh_status_context("suspend_d", status))
        if requirements.requires_limit_price:
            status = self.refresh_audit.require_success(dataset="stk_limit", trade_date=trade_date)
            ready.append(self._refresh_status_context("stk_limit", status))
        return ready

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
        runtime_hash = selection_artifact_runtime_hash(runtime_config)
        force_regenerate = bool(artifact_config.get("force_regenerate"))
        artifact_repository = getattr(self.runtime, "artifact_repository", None)
        if artifact_repository is not None and not force_regenerate:
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
        )

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
                raise StrategyPackageValidationError("runtime_config.total_equity must be positive")
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

    @staticmethod
    def _required_minute_bars_for_manifest(manifest) -> int:
        return PaperTradingDayRunner._required_minute_bars_for_policy(
            manifest.minute_execution_policy.model_dump(mode="json"),
            package_id=manifest.package_id,
        )

    @staticmethod
    def _required_minute_bars_for_policy(policy_json: dict[str, Any], *, package_id: str) -> int:
        return required_minute_bars_for_policy(policy_json, package_id=package_id)

    @staticmethod
    def _policy_requires_day_features(policy_json: dict[str, Any]) -> bool:
        return str(policy_json.get("algo_code") or "").strip().upper() == "V25_TWO_STAGE"

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
            raise StrategyPackageValidationError(
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
            raise StrategyPackageValidationError(
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
