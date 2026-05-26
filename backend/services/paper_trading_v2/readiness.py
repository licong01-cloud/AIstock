"""Pre-run readiness checks for authoritative Paper Trading v2 day runs."""

from __future__ import annotations

from datetime import date
from typing import Any, Callable

from backend.services.data_refresh_audit import DataRefreshAuditRepository, DatasetRefreshStatus
from backend.services.paper_trading_v2.day_runner import PaperTradingDayRunner
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
    apply_runtime_variant_to_manifest,
)
from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactService
from backend.services.strategy_package.validators import StrategyPackageValidator
from backend.services.trading_core.errors import (
    DataUnavailableError,
    InvalidStateTransitionError,
    PackageAssetInvalidError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import PositionLot

from .broker import MiniQMTSimBackend
from .models import PaperDayReadinessResult, PaperReadinessCheck, PortfolioStatus
from .repository import PaperTradingV2Repository
from .risk_targets import overlay_risk_forced_exit_targets
from .service import PaperTradingV2PortfolioService


class PaperTradingReadinessService:
    """Validate a Paper v2 day can enter the same strict path as ``run_day``.

    This service does not persist orders, fills, snapshots, or run rows. It is a
    fail-fast preflight that exercises package runtime, tradability filtering,
    target generation, rebalance, data freshness audit, calendar, limit price,
    suspension, and minute-bar loading.
    """

    def __init__(
        self,
        *,
        repository: PaperTradingV2Repository | Any | None = None,
        calendar_provider: TradeCalendarProvider | Any | None = None,
        market_data_provider: PaperV2MinuteMarketDataProvider | None = None,
        runtime: StrategyPackageRuntime | None = None,
        target_engine: TargetPositionEngine | None = None,
        rebalance_engine: RebalanceEngine | None = None,
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
        self.validator = validator or StrategyPackageValidator()
        self.package_repository = package_repository
        self.tradability_filter = tradability_filter or TradabilityFilter()
        self.refresh_audit = refresh_audit or DataRefreshAuditRepository()
        self.selection_artifact_service = selection_artifact_service or StrategyPackageSelectionArtifactService(
            artifact_repository=getattr(self.runtime, "artifact_repository", None),
        )
        self.risk_policy_service = risk_policy_service or StockRiskPolicyService()
        self.minqmt_broker_factory = minqmt_broker_factory or MiniQMTSimBackend

    def check_day(
        self,
        *,
        portfolio_id: str,
        trade_date: date,
        runtime_config: dict[str, Any] | None = None,
    ) -> PaperDayReadinessResult:
        portfolio = self.repository.get_portfolio(portfolio_id)
        if portfolio.status != PortfolioStatus.READY:
            raise InvalidStateTransitionError(
                "paper v2 portfolio must be READY before readiness check",
                context={"portfolio_id": portfolio_id, "status": portfolio.status.value},
            )
        if trade_date < portfolio.start_date:
            raise InvalidStateTransitionError(
                "paper readiness trade_date cannot be before portfolio start_date",
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

        checks: list[PaperReadinessCheck] = []
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
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "readiness"},
            include_contract=True,
        )
        validate_runtime_profile_binding(
            config,
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "readiness"},
        )
        runtime_profile = parse_selection_runtime_profile(config)
        PaperTradingDayRunner._reject_raw_execution_overrides(config)
        execution_policy_context = self._execution_policy_context_for_date(portfolio, trade_date)
        execution_policy_json = execution_policy_context["policy_json"]
        config["validated_execution_policy"] = execution_policy_context
        self.validator.validate_manifest_identity_for_paper_trading(manifest)
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
            context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat(), "check": "readiness"},
        )
        effective_manifest = apply_runtime_variant_to_manifest(manifest, config)
        config["qe_backtest_runtime_contract"] = runtime_contract
        checks.append(PaperReadinessCheck(check_name="strategy_package_manifest", context={"package_id": manifest.package_id}))

        self.calendar_provider.ensure_trading_day(trade_date)
        checks.append(PaperReadinessCheck(check_name="trading_calendar", context={"trade_date": trade_date.isoformat()}))

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
        checks.append(PaperReadinessCheck(check_name="run_date_available", context={"trade_date": trade_date.isoformat()}))

        if portfolio.broker_backend == "minqmt_sim":
            checks.extend(
                self._prepare_minqmt_authority_state(
                    portfolio=portfolio,
                    manifest=manifest,
                    trade_date=trade_date,
                    runtime_config=config,
                )
            )
            current_positions = config.pop("_miniqmt_current_positions")
            latest_cash = float(config.pop("_miniqmt_latest_cash"))
            total_equity = float(config.pop("_miniqmt_total_equity"))
        else:
            requirements = PaperTradingDayRunner._data_requirements_for_policy(
                execution_policy_json,
                package_id=manifest.package_id,
            )
            if requirements["requires_suspend_status"] or runtime_profile.tradability.exclude_suspended:
                status = self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
                checks.append(self._audit_check("suspend_d_refresh", status))
            if requirements["requires_limit_price"]:
                status = self.refresh_audit.require_success(dataset="stk_limit", trade_date=trade_date)
                checks.append(self._audit_check("stk_limit_refresh", status))

            current_positions = self.repository.load_latest_positions(portfolio_id, trade_date)
            price_check = self._ensure_current_prices_for_existing_positions(
                config=config,
                current_positions=current_positions,
                trade_date=trade_date,
                data_source=portfolio.data_source,
            )
            if price_check is not None:
                checks.append(price_check)
            latest_cash = self.repository.load_latest_cash(portfolio, trade_date)
            total_equity = self._resolve_total_equity(
                latest_cash=latest_cash,
                current_positions=current_positions,
                runtime_config=config,
                portfolio_id=portfolio_id,
            )
        checks.append(
            PaperReadinessCheck(
                check_name="portfolio_state",
                context={
                    "cash": latest_cash,
                    "position_count": len(current_positions),
                    "total_equity": total_equity,
                },
            )
        )

        selection_data_source = PaperTradingDayRunner._selection_data_source(portfolio, config)
        artifact_runner = PaperTradingDayRunner(
            repository=self.repository,
            calendar_provider=self.calendar_provider,
            market_data_provider=self.market_data_provider,
            runtime=self.runtime,
            target_engine=self.target_engine,
            rebalance_engine=self.rebalance_engine,
            validator=self.validator,
            package_repository=self.package_repository,
            tradability_filter=self.tradability_filter,
            refresh_audit=self.refresh_audit,
            selection_artifact_service=self.selection_artifact_service,
        )
        artifact_runner._ensure_authoritative_selection_artifact(
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
        if snapshot.valid_no_candidate and not (current_positions and runtime_profile.risk_policy.enabled):
            raise DataUnavailableError(
                "valid_no_candidate snapshots cannot enter paper v2 trading readiness",
                context={"package_id": manifest.package_id, "reason": snapshot.no_candidate_reason},
            )
        raw_candidate_count = len(snapshot.candidates)
        excluded_count = 0
        top_k = self._require_runtime_top_k(runtime_profile, manifest)
        risk_decisions = self.risk_policy_service.evaluate(
            symbols=sorted(set(item.symbol for item in snapshot.candidates) | set(current_positions)),
            trade_date=trade_date,
            profile=runtime_profile.risk_policy,
            current_positions=current_positions,
        )
        risk_adjusted, risk_excluded = self.risk_policy_service.apply_to_candidates(
            candidates=snapshot.candidates,
            decisions=risk_decisions,
            trade_date=trade_date,
            top_k=top_k,
            package_id=manifest.package_id,
            manifest_sha256=manifest.manifest_sha256 or portfolio.manifest_sha256,
            allow_empty=bool(current_positions),
        )
        snapshot = snapshot.model_copy(update={"candidates": risk_adjusted})
        excluded_count += len(risk_excluded)
        if snapshot.candidates and (
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
            snapshot = snapshot.model_copy(update={"candidates": tradable})
            excluded_count += len(excluded)
        checks.append(
            PaperReadinessCheck(
                check_name="selection_runtime",
                context={
                    "raw_candidate_count": raw_candidate_count,
                    "tradable_candidate_count": len(snapshot.candidates),
                    "excluded_candidate_count": excluded_count,
                    "runtime_profile": runtime_profile.model_dump(mode="json"),
                },
            )
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
        intents = self.rebalance_engine.build_order_intents(
            package_id=manifest.package_id,
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            current_positions=current_positions,
            target_positions=targets,
        )
        checks.append(PaperReadinessCheck(check_name="rebalance", context={"target_count": len(targets), "order_intent_count": len(intents)}))

        checked_symbols = sorted(set(current_positions) | {intent.symbol for intent in intents})
        if portfolio.broker_backend == "minqmt_sim":
            checks.append(
                PaperReadinessCheck(
                    check_name="miniqmt_execution_authority",
                    context={
                        "symbol_count": len(checked_symbols),
                        "order_intent_count": len(intents),
                        "minute_market_data_check": "skipped",
                        "authority_source": "MINIQMT",
                    },
                )
            )
        else:
            required_bars = PaperTradingDayRunner._required_minute_bars_for_policy(
                execution_policy_json,
                package_id=manifest.package_id,
            )
            require_day_features = PaperTradingDayRunner._policy_requires_day_features(execution_policy_json)
            if not checked_symbols:
                raise DataUnavailableError(
                    "paper readiness produced no symbols to check",
                    context={"portfolio_id": portfolio_id, "trade_date": trade_date.isoformat()},
                )
            for symbol in checked_symbols:
                market_input = self.market_data_provider.load_symbol_input(
                    symbol=symbol,
                    trade_date=trade_date,
                    source=portfolio.data_source,
                    min_bars=required_bars,
                    require_suspend_status=True,
                    require_day_features=require_day_features,
                )
                if not market_input.minute_bars:
                    raise DataUnavailableError(
                        "market data provider returned no minute bars",
                        context={"portfolio_id": portfolio_id, "symbol": symbol, "trade_date": trade_date.isoformat()},
                    )
            checks.append(
                PaperReadinessCheck(
                    check_name="minute_market_data",
                    context={
                        "symbol_count": len(checked_symbols),
                        "min_required_bars": required_bars,
                        "require_day_features": require_day_features,
                    },
                )
            )

        return PaperDayReadinessResult(
            portfolio_id=portfolio_id,
            trade_date=trade_date,
            data_source=portfolio.data_source,
            checks=checks,
            raw_candidate_count=raw_candidate_count,
            tradable_candidate_count=len(snapshot.candidates),
            excluded_candidate_count=excluded_count,
            target_count=len(targets),
            order_intent_count=len(intents),
            checked_symbols=checked_symbols,
            runtime_config_keys=sorted(str(key) for key in config),
        )

    @staticmethod
    def _require_runtime_top_k(runtime_profile: Any, manifest: Any) -> int:
        top_k = runtime_profile.selection.top_k
        if top_k is None:
            raise RuntimeConfigInvalidError(
                "Paper v2 readiness requires runtime_profile.selection.top_k; StrategyPackage manifest cannot provide runtime top_k",
                context={"package_id": manifest.package_id, "manifest_version": getattr(manifest, "manifest_version", None)},
            )
        return int(top_k)

    def _prepare_minqmt_authority_state(
        self,
        *,
        portfolio: Any,
        manifest: Any,
        trade_date: date,
        runtime_config: dict[str, Any],
    ) -> list[PaperReadinessCheck]:
        checks: list[PaperReadinessCheck] = [
            PaperReadinessCheck(
                check_name="miniqmt_account_query_required",
                context={
                    "trade_date": trade_date.isoformat(),
                    "data_source": MinuteDataSource.MINIQMT_REALTIME.value,
                    "broker_backend": "minqmt_sim",
                },
            )
        ]
        runtime_profile = parse_selection_runtime_profile(runtime_config)
        if runtime_profile.tradability.exclude_suspended:
            status = self.refresh_audit.require_success(dataset="suspend_d", trade_date=trade_date)
            checks.append(self._audit_check("suspend_d_refresh", status))

        broker = self.minqmt_broker_factory(
            portfolio_id=portfolio.portfolio_id,
            package_id=manifest.package_id,
            data_source=MinuteDataSource.MINIQMT_REALTIME,
            strategy_slot_id=portfolio.portfolio_id,
        )
        try:
            account = broker.query_account()
            current_positions, current_prices = broker.query_position_marks()
        finally:
            broker.shutdown()

        runtime_config["_miniqmt_current_positions"] = current_positions
        runtime_config["_miniqmt_latest_cash"] = float(account.cash)
        runtime_config["_miniqmt_total_equity"] = float(account.nav)
        if current_prices:
            runtime_config["current_prices"] = current_prices
            runtime_config["current_price_context"] = {
                symbol: {"price": price, "source": "MINIQMT_QUERY", "basis": "broker_position_mark"}
                for symbol, price in current_prices.items()
            }
        checks.append(
            PaperReadinessCheck(
                check_name="miniqmt_broker_authority",
                context={
                    "cash": float(account.cash),
                    "nav": float(account.nav),
                    "position_count": len(current_positions),
                    "position_mark_count": len(current_prices),
                    "authority_source": "MINIQMT_QUERY",
                },
            )
        )
        return checks

    @staticmethod
    def _audit_check(check_name: str, status: DatasetRefreshStatus | None) -> PaperReadinessCheck:
        if status is None:
            return PaperReadinessCheck(check_name=check_name, context={"audit_provider": "not_returned"})
        return PaperReadinessCheck(
            check_name=check_name,
            context={
                "dataset": status.dataset,
                "trade_date": status.trade_date.isoformat(),
                "data_source": status.data_source,
                "row_count": status.row_count,
                "refreshed_at": status.refreshed_at.isoformat(),
            },
        )

    def _ensure_current_prices_for_existing_positions(
        self,
        *,
        config: dict[str, Any],
        current_positions: dict[str, PositionLot],
        trade_date: date,
        data_source: MinuteDataSource,
    ) -> PaperReadinessCheck | None:
        if not current_positions or config.get("current_prices"):
            return None
        if data_source != MinuteDataSource.DB_HISTORICAL:
            return None

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
                    "historical replay readiness current position price requires at least one DB minute bar",
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
        return PaperReadinessCheck(
            check_name="current_position_prices",
            context={
                "trade_date": trade_date.isoformat(),
                "symbol_count": len(prices),
                "basis": "first_observed_minute_close",
                "data_source": data_source.value,
            },
        )

    @staticmethod
    def _resolve_total_equity(
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

    def _execution_policy_context_for_date(self, portfolio: Any, trade_date: date) -> dict[str, Any]:
        activation = None
        if hasattr(self.repository, "get_active_execution_policy_activation"):
            activation = self.repository.get_active_execution_policy_activation(portfolio.portfolio_id, trade_date)
        if activation is None:
            return PaperTradingDayRunner._execution_policy_context(portfolio)
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
