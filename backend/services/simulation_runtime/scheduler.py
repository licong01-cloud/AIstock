"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from typing import Any, Protocol

import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.services.paper_trading_v2.market_data import MinuteDataSource
from backend.services.paper_trading_v2.broker.base import BrokerBackend
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.order_service import SELL_ORDER_TYPE, OrderPreflightError, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.models import (
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    new_id as new_qmt_id,
)
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.selection_center.models import SelectionMode, SignalSnapshot
from backend.services.strategy_package.models import StrategyPackageManifest
from backend.services.trading_core.errors import DataUnavailableError
from backend.services.trading_core.models import PositionLot

from .lifecycle import SimulationExecutionResult, SimulationLifecycleOrchestrator, SimulationPlanBuildResult
from .models import (
    ExecutionPlan,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
)
from .repository import InMemorySimulationRuntimeRepository, SimulationRuntimeRepository
from .selection import StrategyPackageSelectionResult, StrategyPackageSelectionService
from .performance import StrategyPerformanceProjectionService
from .tail import TailHandlingPolicyService


DEFAULT_SCHEDULER_APPROVAL_STATES = (
    SimulationBindingApprovalState.SIM_VALIDATING,
    SimulationBindingApprovalState.SIM_PASSED,
    SimulationBindingApprovalState.LIVE_APPROVAL_PENDING,
    SimulationBindingApprovalState.LIVE_APPROVED,
)

DEFAULT_SCHEDULER_WINDOWS = (
    {"window_id": "pre_open", "label": "盘前", "start": "08:50", "end": "09:10", "action": "readiness"},
    {"window_id": "selection", "label": "选股", "start": "09:10", "end": "09:20", "action": "selection_evidence"},
    {"window_id": "planning", "label": "调仓", "start": "09:20", "end": "09:25", "action": "execution_plan"},
    {"window_id": "execution", "label": "盘中/尾盘", "start": "09:25", "end": "15:00", "action": "submit"},
)

logger = logging.getLogger("aistock.simulation_runtime.scheduler")


@dataclass(frozen=True)
class SimulationRunContext:
    """Authoritative run context supplied by the target broker/account adapter."""

    current_positions: dict[str, PositionLot]
    current_prices: dict[str, float] = field(default_factory=dict)
    portfolio_id: str | None = None
    manifest: StrategyPackageManifest | None = None
    top_k: int | None = None
    execution_policy_payload: dict[str, Any] | None = None
    tail_policy_payload: dict[str, Any] | None = None
    local_broker: BrokerBackend | None = None
    managed_order_service: QmtManagedOrderService | None = None
    qmt_sync_service: QmtStrategyLedgerSyncService | None = None
    qmt_reconciliation_service: QmtStrategyLedgerReconciliationService | None = None
    qmt_ledger_repository: Any | None = None
    broker_positions: list[dict[str, Any]] = field(default_factory=list)
    tail_policy_service: TailHandlingPolicyService | None = None
    price_by_symbol: dict[str, Any] | None = None
    cash: float | None = None
    frozen_cash: float = 0.0
    realized_pnl: float = 0.0


class SimulationRunContextProvider(Protocol):
    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        """Return current positions/prices and optional broker services for one binding."""


class FailFastSimulationRunContextProvider:
    """Default provider that prevents silent empty-position or empty-price success."""

    provider_mode = "fail_fast"

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        raise DataUnavailableError(
            "simulation lifecycle scheduler requires an explicit run context provider",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    def status(self) -> dict[str, Any]:
        return {
            "provider_mode": self.provider_mode,
            "provider_name": type(self).__name__,
            "ready": False,
            "diagnostic": (
                "Set SIMULATION_RUNTIME_CONTEXT_PROVIDER=production or "
                "ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER=1 to enable the "
                "production SimulationRunContextProvider."
            ),
        }


class StaticSimulationRunContextProvider:
    """Explicit static provider for deterministic tests and controlled validation."""

    def __init__(
        self,
        *,
        by_binding_id: dict[str, SimulationRunContext] | None = None,
        by_strategy_id: dict[str, SimulationRunContext] | None = None,
    ) -> None:
        self._by_binding_id = dict(by_binding_id or {})
        self._by_strategy_id = dict(by_strategy_id or {})

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        if binding.binding_id in self._by_binding_id:
            return self._by_binding_id[binding.binding_id]
        if binding.strategy_id in self._by_strategy_id:
            return self._by_strategy_id[binding.strategy_id]
        raise DataUnavailableError(
            "static simulation run context is missing binding context",
            context={
                "release_id": runtime_release.release_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )


class ProductionSimulationRunContextProvider:
    """Production provider that loads positions, prices, and broker services from
    persisted state and live market data.

    The provider uses persisted Paper v2 portfolios for LocalSim state and the
    qmt_strategy virtual ledger for MiniQMT strategy state. Optional factories
    keep tests deterministic, but the default path is no longer an empty-state
    placeholder: missing repositories, accounts, positions, or prices fail fast
    with actionable diagnostics.
    """

    provider_mode = "production"

    def __init__(
        self,
        *,
        position_loader: Callable[[str, date], dict[str, PositionLot]] | None = None,
        price_loader: Callable[[list[str], date], dict[str, float]] | None = None,
        paper_repository_factory: Callable[[], Any] | None = None,
        qmt_repository_factory: Callable[[], Any] | None = None,
        qmt_client_factory: Callable[[], Any] | None = None,
        qmt_calendar_provider_factory: Callable[[], Any] | None = None,
        local_broker_factory: Callable[[str], BrokerBackend] | None = None,
        managed_order_service_factory: Callable[[], QmtManagedOrderService] | None = None,
        qmt_sync_service_factory: Callable[[], QmtStrategyLedgerSyncService] | None = None,
        qmt_reconciliation_service_factory: Callable[[], QmtStrategyLedgerReconciliationService] | None = None,
        qmt_ledger_repository: Any | None = None,
        enable_localsim_broker: bool | None = None,
        enable_miniqmt_submit: bool | None = None,
    ) -> None:
        self._position_loader = position_loader
        self._price_loader = price_loader or _default_price_loader
        self._paper_repository_factory = paper_repository_factory or _default_paper_repository_factory
        self._qmt_repository_factory = qmt_repository_factory or _default_qmt_repository_factory
        self._qmt_client_factory = qmt_client_factory or _default_qmt_client_factory
        self._qmt_calendar_provider_factory = qmt_calendar_provider_factory or _default_qmt_calendar_provider
        self._local_broker_factory = local_broker_factory
        self._managed_order_service_factory = managed_order_service_factory
        self._qmt_sync_service_factory = qmt_sync_service_factory
        self._qmt_reconciliation_service_factory = qmt_reconciliation_service_factory
        self._qmt_ledger_repository = qmt_ledger_repository
        self._enable_localsim_broker = (
            _env_flag("SIMULATION_RUNTIME_ENABLE_LOCALSIM_BROKER", default=True)
            if enable_localsim_broker is None
            else bool(enable_localsim_broker)
        )
        self._enable_miniqmt_submit = (
            (
                _env_flag("AISTOCK_ALLOW_MINIQMT_MANAGED_ORDERS", default=False)
                or _env_flag("AISTOCK_ALLOW_MINIQMT_SUBMIT_TEST", default=False)
            )
            if enable_miniqmt_submit is None
            else bool(enable_miniqmt_submit)
        )

    def status(self) -> dict[str, Any]:
        return {
            "provider_mode": self.provider_mode,
            "provider_name": type(self).__name__,
            "ready": True,
            "localsim_state_source": "paper_v2_portfolio",
            "miniqmt_state_source": "qmt_strategy_virtual_ledger",
            "market_price_source": "market.kline_daily_raw_latest_close",
            "localsim_broker_enabled": self._enable_localsim_broker or self._local_broker_factory is not None,
            "miniqmt_preview_enabled": True,
            "miniqmt_submit_enabled": self._enable_miniqmt_submit,
            "miniqmt_submit_default": False,
        }

    def load_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._load_local_sim_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            return self._load_miniqmt_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        raise DataUnavailableError(
            "ProductionSimulationRunContextProvider: unsupported broker backend",
            context={
                "broker_backend": binding.broker_backend.value,
                "strategy_id": binding.strategy_id,
            },
        )

    def _load_local_sim_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        portfolio_id = self._resolve_local_sim_portfolio_id(binding)
        paper_repository = self._build_dependency(
            self._paper_repository_factory,
            "PaperTradingV2Repository",
            binding=binding,
            trade_date=trade_date,
        )
        try:
            portfolio = paper_repository.get_portfolio(portfolio_id)
            positions = (
                self._load_positions_with_injected_loader(binding.strategy_id, trade_date)
                if self._position_loader is not None
                else paper_repository.load_latest_positions(portfolio_id, trade_date)
            )
            cash = float(paper_repository.load_latest_cash(portfolio, trade_date))
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load LocalSim production context from Paper v2 persisted state",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "portfolio_id": portfolio_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        prices = self._load_prices_for_positions(
            positions,
            trade_date,
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
        )
        manifest = getattr(portfolio, "frozen_manifest", None)
        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
        )
        local_broker = self._build_local_sim_broker(
            portfolio_id=portfolio_id,
            portfolio=portfolio,
            binding=binding,
            manifest=manifest,
            cash=cash,
            positions=positions,
        )
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=portfolio_id,
            manifest=manifest,
            local_broker=local_broker,
            cash=cash,
        )

    def _load_miniqmt_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationRunContext:
        qmt_repository = self._qmt_ledger_repository or self._build_dependency(
            self._qmt_repository_factory,
            "QmtStrategyLedgerRepository",
            binding=binding,
            trade_date=trade_date,
        )
        try:
            account = qmt_repository.get_virtual_account(binding.strategy_id)
            positions = (
                self._load_positions_with_injected_loader(binding.strategy_id, trade_date)
                if self._position_loader is not None
                else self._positions_from_qmt_lots(
                    repository=qmt_repository,
                    strategy_id=binding.strategy_id,
                )
            )
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load MiniQMT production context from qmt_strategy virtual ledger",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        prices = self._load_prices_for_positions(
            positions,
            trade_date,
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
        )
        managed_order_service = (
            self._managed_order_service_factory()
            if self._managed_order_service_factory is not None
            else self._build_managed_order_service(qmt_repository)
        )
        if not self._enable_miniqmt_submit and self._managed_order_service_factory is None:
            managed_order_service = PreviewOnlyMiniQMTManagedOrderService(managed_order_service)
        qmt_sync_service = (
            self._qmt_sync_service_factory()
            if self._qmt_sync_service_factory is not None
            else QmtStrategyLedgerSyncService(
                repository=qmt_repository,
                qmt_client=self._qmt_client_factory(),
                account_id=binding.broker_account_id or account.account_id,
                trade_date=trade_date,
                calendar_provider=self._qmt_calendar_provider_factory(),
            )
        )
        qmt_reconciliation_service = (
            self._qmt_reconciliation_service_factory()
            if self._qmt_reconciliation_service_factory is not None
            else QmtStrategyLedgerReconciliationService(repository=qmt_repository)
        )
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=binding.strategy_id,
            managed_order_service=managed_order_service,
            qmt_sync_service=qmt_sync_service,
            qmt_reconciliation_service=qmt_reconciliation_service,
            qmt_ledger_repository=qmt_repository,
            cash=float(account.cash),
            frozen_cash=float(account.frozen_cash),
            realized_pnl=float(account.realized_pnl),
            price_by_symbol=prices,
        )

    def _build_managed_order_service(self, qmt_repository: Any) -> QmtManagedOrderService:
        broker = self._qmt_client_factory()
        return QmtManagedOrderService(
            repository=qmt_repository,
            broker=broker,
            calendar_provider=self._qmt_calendar_provider_factory(),
        )

    def _build_local_sim_broker(
        self,
        *,
        portfolio_id: str,
        portfolio: Any,
        binding: SimulationReleaseBinding,
        manifest: StrategyPackageManifest | None,
        cash: float,
        positions: dict[str, PositionLot],
    ) -> BrokerBackend | None:
        if self._local_broker_factory is not None:
            return self._local_broker_factory(binding.strategy_id)
        if not self._enable_localsim_broker:
            return None
        if manifest is None:
            raise DataUnavailableError(
                "LocalSim production context requires a frozen StrategyPackage manifest",
                context={"portfolio_id": portfolio_id, "binding_id": binding.binding_id},
            )
        try:
            from backend.services.paper_trading_v2.broker.localsim import LocalSimBackend

            data_source = getattr(portfolio, "data_source", MinuteDataSource.DB_HISTORICAL)
            if not isinstance(data_source, MinuteDataSource):
                data_source = MinuteDataSource(str(data_source))
            return LocalSimBackend(
                portfolio_id=portfolio_id,
                initial_cash=float(getattr(portfolio, "initial_cash", binding.capital_allocation)),
                initial_available_cash=cash,
                initial_positions=positions,
                data_source=data_source,
                manifest=manifest,
                package_id=binding.package_id,
            )
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to construct LocalSim production broker from persisted context",
                context={
                    "portfolio_id": portfolio_id,
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                },
            ) from exc

    def _load_positions_with_injected_loader(self, strategy_id: str, trade_date: date) -> dict[str, PositionLot]:
        if self._position_loader is None:
            raise DataUnavailableError(
                "production context provider has no injected position_loader",
                context={"strategy_id": strategy_id, "trade_date": trade_date.isoformat()},
            )
        try:
            return dict(self._position_loader(strategy_id, trade_date))
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "injected production position_loader failed",
                context={"strategy_id": strategy_id, "trade_date": trade_date.isoformat()},
            ) from exc

    def _load_prices_for_positions(
        self,
        positions: dict[str, PositionLot],
        trade_date: date,
        *,
        strategy_id: str,
        binding_id: str,
    ) -> dict[str, float]:
        symbols = sorted(positions)
        try:
            prices = dict(self._price_loader(symbols, trade_date))
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "production price_loader failed",
                context={
                    "strategy_id": strategy_id,
                    "binding_id": binding_id,
                    "symbols": symbols,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc
        missing = sorted(symbol for symbol in symbols if symbol not in prices)
        if missing:
            raise DataUnavailableError(
                "production price_loader did not return marks for all held positions",
                context={
                    "strategy_id": strategy_id,
                    "binding_id": binding_id,
                    "missing_symbols": missing,
                    "trade_date": trade_date.isoformat(),
                },
            )
        return prices

    @staticmethod
    def _positions_from_qmt_lots(*, repository: Any, strategy_id: str) -> dict[str, PositionLot]:
        lots = repository.list_position_lots(strategy_id)
        positions: dict[str, PositionLot] = {}
        for lot in lots:
            remaining = int(getattr(lot, "remaining_quantity", getattr(lot, "quantity", 0)))
            if remaining <= 0:
                continue
            symbol = str(lot.symbol)
            existing = positions.get(symbol)
            quantity = remaining + (existing.quantity if existing else 0)
            available_quantity = int(getattr(lot, "available_quantity", 0)) + (
                existing.available_quantity if existing else 0
            )
            cost_amount = Decimal(str(getattr(lot, "avg_cost"))) * Decimal(str(remaining))
            if existing is not None:
                cost_amount += Decimal(str(existing.avg_cost)) * Decimal(str(existing.quantity))
            avg_cost = float(cost_amount / Decimal(str(quantity))) if quantity else 0.0
            positions[symbol] = PositionLot(
                portfolio_id=strategy_id,
                symbol=symbol,
                quantity=quantity,
                available_quantity=available_quantity,
                avg_cost=avg_cost,
                trade_date=getattr(lot, "open_date"),
            )
        return positions

    @staticmethod
    def _resolve_local_sim_portfolio_id(binding: SimulationReleaseBinding) -> str:
        metadata = binding.binding_config_json.get("metadata") if isinstance(binding.binding_config_json, dict) else {}
        if isinstance(metadata, dict):
            for key in ("paper_v2_portfolio_id", "local_sim_portfolio_id", "portfolio_id"):
                value = str(metadata.get(key) or "").strip()
                if value:
                    return value
        if binding.broker_account_id:
            return binding.broker_account_id
        return binding.strategy_id

    @staticmethod
    def _validate_manifest_identity(
        *,
        manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> None:
        if manifest is None:
            return
        if manifest.package_id != binding.package_id or manifest.package_id != runtime_release.package_id:
            raise DataUnavailableError(
                "LocalSim manifest package_id does not match runtime release binding",
                context={
                    "manifest_package_id": manifest.package_id,
                    "release_package_id": runtime_release.package_id,
                    "binding_package_id": binding.package_id,
                },
            )
        if manifest.manifest_sha256 != binding.manifest_sha256 or manifest.manifest_sha256 != runtime_release.manifest_sha256:
            raise DataUnavailableError(
                "LocalSim manifest hash does not match runtime release binding",
                context={
                    "manifest_sha256": manifest.manifest_sha256,
                    "release_manifest_sha256": runtime_release.manifest_sha256,
                    "binding_manifest_sha256": binding.manifest_sha256,
                },
            )

    @staticmethod
    def _build_dependency(
        factory: Callable[[], Any],
        dependency_name: str,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> Any:
        try:
            return factory()
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                f"failed to construct {dependency_name} for production simulation context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc


class PreviewOnlyMiniQMTManagedOrderService:
    """Managed-order facade that persists preview evidence without calling MiniQMT."""

    def __init__(self, wrapped: QmtManagedOrderService) -> None:
        self._wrapped = wrapped
        self._repository = getattr(wrapped, "_repository", None)
        # Preview mode may read broker positions for SELL availability and
        # reconciliation, but it must never call place_order/cancel_order.
        self._broker = getattr(wrapped, "_broker", None)

    def preview_order(self, request: Any) -> Any:
        return self._wrapped.preview_order(request)

    def _broker_can_sell(self, symbol: str) -> int:
        helper = getattr(self._wrapped, "_broker_can_sell", None)
        if callable(helper):
            return int(helper(symbol))
        return 0

    def submit_batch(self, requests: list[Any]) -> Any:
        requests_list = list(requests)
        if not requests_list:
            raise DataUnavailableError("MiniQMT preview-only submit requires at least one request")
        batch_id = _preview_batch_id(requests_list)
        existing_result = self._existing_preview_result(batch_id, request_count=len(requests_list))
        if existing_result is not None:
            return existing_result
        preflights = self._preview_batch_preflight(requests_list)
        results = tuple(
            self._preview_submit_result(request, preflight)
            for request, preflight in zip(requests_list, preflights, strict=True)
        )
        succeeded = sum(1 for result in results if result.success)
        failed = len(results) - succeeded
        preview_result = MiniQMTPreviewBatchSubmitResult(
            success=failed == 0,
            total=len(results),
            succeeded=succeeded,
            failed=failed,
            results=results,
            compensation_required=False,
            compensation_hint=None,
            batch_id=batch_id,
            batch_status="PREVIEW_SUCCEEDED" if failed == 0 else "PREVIEW_FAILED",
            preflight_passed=failed == 0,
            retry_of_batch_id=None,
            compensation_actions=(),
        )
        return self._persist_preview_result(batch_id=batch_id, requests=requests_list, result=preview_result)

    def _preview_batch_preflight(self, requests: list[Any]) -> tuple[Any, ...]:
        helper = getattr(self._wrapped, "_batch_preflight", None)
        if callable(helper):
            return tuple(helper(requests))
        return tuple(self._wrapped.preview_order(request) for request in requests)

    def _preview_submit_result(self, request: Any, preflight: Any) -> Any:
        broker_can_sell = None
        if getattr(request, "order_type", None) == SELL_ORDER_TYPE:
            broker_can_sell = self._broker_can_sell(str(getattr(request, "symbol", "")))
            if broker_can_sell < int(getattr(request, "quantity", 0) or 0):
                errors = tuple(getattr(preflight, "errors", ()) or ()) + (
                    OrderPreflightError(
                        "INSUFFICIENT_BROKER_CAN_SELL",
                        "MiniQMT account-level can_sell is insufficient",
                        {
                            "broker_can_sell": broker_can_sell,
                            "requested_quantity": int(getattr(request, "quantity", 0) or 0),
                        },
                    ),
                )
                preflight = replace(preflight, allowed=False, errors=errors, broker_can_sell=broker_can_sell)
            else:
                preflight = replace(preflight, broker_can_sell=broker_can_sell)
        success = bool(getattr(preflight, "allowed", False))
        message = "preview-only dry-run accepted" if success else "preview-only preflight failed"
        return MiniQMTPreviewOrderSubmitResult(
            success=success,
            intent_id=None,
            qmt_order_id=None,
            broker_message=message,
            preflight=preflight,
            broker_called=False,
        )

    def _existing_preview_result(self, batch_id: str, *, request_count: int) -> Any | None:
        get_batch = getattr(self._repository, "get_order_batch", None)
        if get_batch is None:
            return None
        batch = get_batch(batch_id)
        if batch is None or not (batch.metadata or {}).get("preview_only"):
            return None
        result_json = batch.result_json if isinstance(batch.result_json, dict) else {}
        stored_results = tuple(
            _preview_result_from_payload(item)
            for item in result_json.get("results", ())
            if isinstance(item, dict)
        )
        if not stored_results:
            return None
        succeeded = sum(1 for item in stored_results if item.success)
        failed = sum(1 for item in stored_results if not item.success) + max(request_count - len(stored_results), 0)
        return MiniQMTPreviewBatchSubmitResult(
            success=failed == 0,
            total=request_count,
            succeeded=succeeded,
            failed=failed,
            results=stored_results,
            compensation_required=False,
            compensation_hint=None,
            batch_id=batch_id,
            batch_status=str(result_json.get("batch_status") or batch.metadata.get("preview_batch_status") or ""),
            preflight_passed=bool(result_json.get("preflight_passed", failed == 0)),
            retry_of_batch_id=batch_id,
            compensation_actions=(),
        )

    def _persist_preview_result(
        self,
        *,
        batch_id: str,
        requests: list[Any],
        result: "MiniQMTPreviewBatchSubmitResult",
    ) -> "MiniQMTPreviewBatchSubmitResult":
        if self._repository is None or getattr(self._repository, "upsert_order_batch", None) is None:
            return result

        persisted_results: list[MiniQMTPreviewOrderSubmitResult] = []
        self._upsert_preview_batch(batch_id=batch_id, requests=requests, result=result)
        if result.success:
            for request, item in zip(requests, result.results, strict=True):
                intent_id = self._create_preview_intent(
                    batch_id=batch_id,
                    request=request,
                    result=item,
                )
                persisted_results.append(replace(item, intent_id=intent_id))
        else:
            persisted_results.extend(result.results)

        persisted = replace(result, results=tuple(persisted_results))
        self._upsert_preview_batch(batch_id=batch_id, requests=requests, result=persisted)
        return persisted

    def _create_preview_intent(
        self,
        *,
        batch_id: str,
        request: Any,
        result: "MiniQMTPreviewOrderSubmitResult",
    ) -> str | None:
        create_intent = getattr(self._repository, "create_order_intent", None)
        if create_intent is None or not result.success:
            return None
        strategy_id = getattr(result.preflight, "strategy_id", None)
        if not strategy_id:
            return None
        metadata = dict(getattr(request, "metadata", {}) or {})
        metadata.update(
            {
                "source": "simulation_runtime_preview_only",
                "preview_only": True,
                "broker_called": False,
                "preview_batch_id": batch_id,
            }
        )
        intent = OrderIntentRecord(
            intent_id=new_qmt_id("qmtpreviewintent"),
            batch_id=batch_id,
            strategy_id=strategy_id,
            strategy_name=str(getattr(request, "strategy_name")),
            symbol=str(getattr(request, "symbol")),
            side=str(getattr(request, "side")),
            order_type=int(getattr(request, "order_type")),
            quantity=int(getattr(request, "quantity")),
            price_type=int(getattr(request, "price_type")),
            order_remark=str(getattr(request, "order_remark")),
            account_id=str(getattr(request, "account_id")),
            trade_date=getattr(request, "trade_date"),
            package_id=getattr(request, "package_id", None),
            selection_run_id=getattr(request, "selection_run_id", None),
            limit_price=getattr(request, "price", None),
            target_weight=getattr(request, "target_weight", None),
            estimated_notional=getattr(result.preflight, "estimated_notional", None),
            estimated_fee=getattr(result.preflight, "estimated_fee", None),
            preflight_status=IntentPreflightStatus.PASSED,
            submit_status=IntentSubmitStatus.CREATED,
            metadata=metadata,
            submitted_at=None,
        )
        return create_intent(intent).intent_id

    def _upsert_preview_batch(
        self,
        *,
        batch_id: str,
        requests: list[Any],
        result: "MiniQMTPreviewBatchSubmitResult",
    ) -> None:
        upsert = getattr(self._repository, "upsert_order_batch", None)
        if upsert is None:
            return
        get_batch = getattr(self._repository, "get_order_batch", lambda _batch_id: None)
        existing = get_batch(batch_id)
        created_at = existing.created_at if existing is not None else datetime.now(UTC)
        first_request = requests[0]
        strategy_ids = sorted(
            {
                item.preflight.strategy_id
                for item in result.results
                if getattr(item.preflight, "strategy_id", None)
            }
        )
        batch_status = OrderBatchStatus.CREATED if result.success else OrderBatchStatus.PREFLIGHT_FAILED
        result_json = result.to_dict()
        result_json.update(
            {
                "preview_only": True,
                "broker_called": False,
                "batch_status": result.batch_status,
                "preflight_passed": result.preflight_passed,
            }
        )
        upsert(
            OrderBatchRecord(
                batch_id=batch_id,
                strategy_id=strategy_ids[0] if len(strategy_ids) == 1 else None,
                account_id=str(getattr(first_request, "account_id")),
                mode=str(getattr(first_request, "mode", "SIM") or "SIM"),
                batch_status=batch_status,
                request_json={"orders": [_preview_request_signature(request) for request in requests]},
                result_json=result_json,
                metadata={
                    "source": "simulation_runtime_preview_only",
                    "preview_only": True,
                    "preview_batch_status": result.batch_status,
                    "preflight_passed": result.preflight_passed,
                    "broker_called": False,
                    "mini_qmt_submit_enabled": False,
                },
                created_at=created_at,
                submitted_at=None,
                completed_at=datetime.now(UTC),
            )
        )


@dataclass(frozen=True)
class MiniQMTPreviewOrderSubmitResult:
    success: bool
    intent_id: str | None
    qmt_order_id: str | None
    broker_message: str
    preflight: Any
    broker_called: bool

    def to_dict(self) -> dict[str, Any]:
        preflight_dict = self.preflight.to_dict() if hasattr(self.preflight, "to_dict") else dict(self.preflight)
        return {
            "success": self.success,
            "intent_id": self.intent_id,
            "qmt_order_id": self.qmt_order_id,
            "broker_message": self.broker_message,
            "preflight": preflight_dict,
            "broker_called": self.broker_called,
            "preview_only": True,
        }


@dataclass(frozen=True)
class MiniQMTPreviewBatchSubmitResult:
    success: bool
    total: int
    succeeded: int
    failed: int
    results: tuple[MiniQMTPreviewOrderSubmitResult, ...]
    compensation_required: bool
    compensation_hint: str | None = None
    batch_id: str | None = None
    batch_status: str | None = None
    preflight_passed: bool = True
    retry_of_batch_id: str | None = None
    compensation_actions: tuple[dict[str, Any], ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "batch_id": self.batch_id,
            "batch_status": self.batch_status,
            "preflight_passed": self.preflight_passed,
            "retry_of_batch_id": self.retry_of_batch_id,
            "total": self.total,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "results": [result.to_dict() for result in self.results],
            "compensation_required": self.compensation_required,
            "compensation_hint": self.compensation_hint,
            "compensation_actions": list(self.compensation_actions),
            "preview_only": True,
        }


def _preview_batch_id(requests: list[Any]) -> str:
    signatures = [_preview_request_signature(request) for request in requests]
    return f"qmtpreview_{canonical_json_sha256(signatures)[:24]}"


def _preview_request_signature(request: Any) -> dict[str, Any]:
    metadata = getattr(request, "metadata", {}) or {}
    return {
        "account_id": str(getattr(request, "account_id", "")),
        "strategy_name": str(getattr(request, "strategy_name", "")),
        "symbol": str(getattr(request, "symbol", "")),
        "side": str(getattr(request, "side", "")),
        "order_type": int(getattr(request, "order_type", 0) or 0),
        "quantity": int(getattr(request, "quantity", 0) or 0),
        "price_type": int(getattr(request, "price_type", 0) or 0),
        "price": str(getattr(request, "price", "")),
        "order_remark": str(getattr(request, "order_remark", "")),
        "trade_date": getattr(request, "trade_date").isoformat()
        if getattr(request, "trade_date", None) is not None
        else None,
        "mode": str(getattr(request, "mode", "SIM") or "SIM"),
        "package_id": getattr(request, "package_id", None),
        "selection_run_id": getattr(request, "selection_run_id", None),
        "target_weight": str(getattr(request, "target_weight", ""))
        if getattr(request, "target_weight", None) is not None
        else None,
        "execution_plan_id": metadata.get("execution_plan_id"),
        "execution_plan_intent_id": metadata.get("execution_plan_intent_id"),
        "metadata": _json_safe_preview(metadata),
    }


def _preview_result_from_payload(payload: dict[str, Any]) -> MiniQMTPreviewOrderSubmitResult:
    return MiniQMTPreviewOrderSubmitResult(
        success=bool(payload.get("success")),
        intent_id=payload.get("intent_id"),
        qmt_order_id=payload.get("qmt_order_id"),
        broker_message=str(payload.get("broker_message") or "existing preview-only dry-run result"),
        preflight=dict(payload.get("preflight") or {}),
        broker_called=bool(payload.get("broker_called", False)),
    )


def _json_safe_preview(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe_preview(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe_preview(item) for item in value]
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def _default_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
    unique_symbols = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
    if not unique_symbols:
        return {}
    try:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ON (ts_code)
                           ts_code, trade_date, close_li
                    FROM market.kline_daily_raw
                    WHERE ts_code = ANY(%s)
                      AND trade_date <= %s
                      AND close_li IS NOT NULL
                      AND close_li > 0
                    ORDER BY ts_code, trade_date DESC
                    """,
                    (unique_symbols, trade_date),
                )
                rows = cur.fetchall()
    except Exception as exc:  # noqa: BLE001
        raise DataUnavailableError(
            "failed to load latest market close prices for production simulation context",
            context={"symbols": unique_symbols, "trade_date": trade_date.isoformat()},
        ) from exc
    prices = {str(row["ts_code"]): float(row["close_li"]) / 1000.0 for row in rows}
    missing = sorted(symbol for symbol in unique_symbols if symbol not in prices)
    if missing:
        raise DataUnavailableError(
            "missing market close prices for production simulation context",
            context={"missing_symbols": missing, "trade_date": trade_date.isoformat()},
        )
    return prices


def _default_paper_repository_factory() -> Any:
    from backend.services.paper_trading_v2.repository import PaperTradingV2Repository

    return PaperTradingV2Repository()


def _default_qmt_repository_factory() -> Any:
    from backend.services.qmt_strategy_ledger.repository import QmtStrategyLedgerRepository

    return QmtStrategyLedgerRepository()


def _default_qmt_client_factory() -> Any:
    from backend.infra.qmt_client import get_qmt_client_singleton

    return get_qmt_client_singleton()


def _default_qmt_calendar_provider() -> Any:
    from backend.services.qmt_strategy_ledger.lot_availability import DbTradingCalendarProvider

    return DbTradingCalendarProvider()


def _env_flag(name: str, *, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _context_provider_status(provider: Any) -> dict[str, Any]:
    status_fn = getattr(provider, "status", None)
    if callable(status_fn):
        try:
            payload = dict(status_fn())
        except Exception as exc:  # noqa: BLE001
            payload = {
                "provider_mode": getattr(provider, "provider_mode", "unknown"),
                "provider_name": type(provider).__name__,
                "ready": False,
                "diagnostic": f"context provider status failed: {type(exc).__name__}: {exc}",
            }
    else:
        payload = {
            "provider_mode": getattr(provider, "provider_mode", type(provider).__name__),
            "provider_name": type(provider).__name__,
            "ready": True,
        }
    payload.setdefault("provider_name", type(provider).__name__)
    payload.setdefault("provider_mode", getattr(provider, "provider_mode", type(provider).__name__))
    return payload


def build_simulation_lifecycle_scheduler_from_env(
    *,
    repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
) -> SimulationLifecycleScheduler:
    mode = (os.getenv("SIMULATION_RUNTIME_CONTEXT_PROVIDER") or "").strip().lower()
    production_enabled = _env_flag("ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER", default=False)
    if mode in {"production", "prod"} or production_enabled:
        provider: SimulationRunContextProvider = ProductionSimulationRunContextProvider()
    else:
        provider = FailFastSimulationRunContextProvider()
    return SimulationLifecycleScheduler(repository=repository, context_provider=provider)


@dataclass(frozen=True)
class SimulationSchedulerBindingResult:
    binding_id: str
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    status: str
    run: SimulationDailyRun | None = None
    execution_plan: ExecutionPlan | None = None
    execution_result: SimulationExecutionResult | None = None
    sync_result: dict[str, Any] | None = None
    reconciliation_result: dict[str, Any] | None = None
    error: dict[str, Any] | None = None

    @property
    def is_success(self) -> bool:
        return self.error is None


@dataclass(frozen=True)
class SimulationSchedulerRunOnceResult:
    trade_date: date
    data_source: str
    submit: bool
    total_bindings: int
    results: tuple[SimulationSchedulerBindingResult, ...]
    as_of_time: datetime | None = None
    schedule_windows: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def planned_count(self) -> int:
        return sum(1 for item in self.results if item.status == "PLANNED")

    @property
    def reused_count(self) -> int:
        return sum(1 for item in self.results if item.status == "REUSED_EXISTING_PLAN")

    @property
    def submitted_count(self) -> int:
        return sum(
            1
            for item in self.results
            if item.status
            in {
                "SUBMITTED",
                "RECONCILED",
                "TAIL_HANDLED",
                "RECONCILIATION_WARNING",
                "NO_REBALANCE",
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.results if item.error is not None)


class SimulationLifecycleScheduler:
    """Run one unattended lifecycle tick for eligible simulation bindings."""

    def __init__(
        self,
        *,
        repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        selection_service: StrategyPackageSelectionService | Any | None = None,
        orchestrator: SimulationLifecycleOrchestrator | None = None,
        context_provider: SimulationRunContextProvider | None = None,
        performance_service: StrategyPerformanceProjectionService | None = None,
    ) -> None:
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(repository=self.repository)
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()
        self.performance_service = performance_service or StrategyPerformanceProjectionService()

    def status(self) -> dict[str, Any]:
        provider_status = _context_provider_status(self.context_provider)
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_APPROVAL_STATES],
            "manual_tick_endpoint_enabled": True,
            "scheduler_control_api_enabled": False,
            "context_provider": provider_status,
            "context_provider_mode": provider_status.get("provider_mode"),
            "schedule_windows": list(DEFAULT_SCHEDULER_WINDOWS),
            "restart_recovery_mode": "persisted_state_only",
            "window_orchestration": {
                "pre_open": "readiness",
                "selection": "daily_selection_evidence",
                "planning": "execution_plan",
                "execution": "submit_and_reconcile",
            },
        }

    def run_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        release_id: str | None = None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None = DEFAULT_SCHEDULER_APPROVAL_STATES,
        submit: bool = False,
        mode: str = "SIM",
        as_of_time: datetime | None = None,
        created_by: str = "simulation_lifecycle_scheduler",
        raise_on_error: bool = False,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        bindings = self.repository.list_simulation_release_bindings(
            strategy_id=strategy_id,
            release_id=release_id,
            broker_backend=broker_backend,
            approval_states=approval_states,
            active_on=trade_date,
            limit=limit,
        )
        results: list[SimulationSchedulerBindingResult] = []
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] = {}
        shared_selection_keys = self._shared_selection_cache_keys(
            bindings=bindings,
            trade_date=trade_date,
            data_source=data_source,
        )
        for binding in bindings:
            try:
                results.append(
                    self._run_binding(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        submit=submit,
                        mode=mode,
                        created_by=created_by,
                        selection_cache=selection_cache,
                        shared_selection_keys=shared_selection_keys,
                    )
                )
            except Exception as exc:
                if raise_on_error:
                    raise
                results.append(
                    SimulationSchedulerBindingResult(
                        binding_id=binding.binding_id,
                        strategy_id=binding.strategy_id,
                        broker_backend=binding.broker_backend,
                        status="FAILED",
                        error={
                            "type": type(exc).__name__,
                            "message": str(exc),
                            "context": getattr(exc, "context", None),
                        },
                    )
                )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            total_bindings=len(bindings),
            results=tuple(results),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    def _run_binding(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None = None,
        shared_selection_keys: set[tuple[Any, ...]] | None = None,
    ) -> SimulationSchedulerBindingResult:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and existing.execution_plan_id:
            return self._existing_plan_result(
                binding=binding,
                run=existing,
                trade_date=trade_date,
                submit=submit,
                mode=mode,
            )

        context = self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        selection = self._run_selection_once_per_release(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            selection_cache=selection_cache,
            shared_selection_keys=shared_selection_keys,
        )
        self._validate_fresh_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            selection=selection,
            trade_date=trade_date,
        )
        build_result = self._build_plan_from_selection(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            selection=selection,
            context=context,
            created_by=created_by,
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="PLANNED",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
            )

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        execution = self.orchestrator.submit_execution_plan(
            build_result=build_result,
            local_broker=context.local_broker,
            managed_order_service=context.managed_order_service,
            mode=mode,
            price_by_symbol=context.price_by_symbol,
        )
        tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
        reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=latest_run,
            execution_plan=execution.execution_plan,
            execution_result=execution,
            sync_result=sync_result,
            reconciliation_result=reconciliation,
        )

    def _existing_plan_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        trade_date: date,
        submit: bool,
        mode: str,
    ) -> SimulationSchedulerBindingResult:
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        status = "REUSED_EXISTING_PLAN"
        if run.status == SimulationDailyRunStatus.SUCCEEDED and not plan.intents:
            status = "NO_REBALANCE"
        if self._should_submit_existing_plan(run, plan=plan, submit=submit):
            runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
            context = self.context_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )
            sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
            execution = self.orchestrator.submit_persisted_execution_plan(
                run=run,
                binding=binding,
                execution_plan=plan,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol,
            )
            tail_result = self._handle_tail_after_submit(binding=binding, run=execution.run, execution=execution, context=context)
            reconciliation = self._reconcile_after_submit(binding=binding, run=execution.run, context=context)
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            status = self._result_status_after_post_submit(execution.status, tail_result=tail_result, reconciliation=reconciliation)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=status,
                run=latest_run,
                execution_plan=plan,
                execution_result=execution,
                sync_result=sync_result,
                reconciliation_result=reconciliation,
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=run,
            execution_plan=plan,
        )

    def _run_selection_once_per_release(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
    ) -> StrategyPackageSelectionResult:
        cache_key = self._selection_cache_key(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
        )
        if shared_selection_keys is not None and cache_key not in shared_selection_keys:
            selection_cache = None
        if selection_cache is not None and cache_key in selection_cache:
            cached = selection_cache[cache_key]
            if isinstance(cached, BaseException):
                raise cached
            return cached
        try:
            selection = self.selection_service.run_selection(
                package_ids=[binding.package_id],
                mode=SelectionMode.SINGLE_PACKAGE,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=StrategyPackageSelectionService.release_selection_runtime_config(runtime_release),
                runtime_release=runtime_release,
                created_by=created_by,
            )
        except Exception as exc:
            if selection_cache is not None:
                selection_cache[cache_key] = exc
            raise
        if selection_cache is not None:
            selection_cache[cache_key] = selection
        return selection

    def _selection_cache_key(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
    ) -> tuple[Any, ...]:
        return (
            binding.package_id,
            binding.manifest_sha256,
            binding.release_id,
            binding.release_hash,
            trade_date.isoformat(),
            data_source,
        )

    def _shared_selection_cache_keys(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        trade_date: date,
        data_source: str,
    ) -> set[tuple[Any, ...]]:
        """Share selection only when the same release fans out to multiple backends.

        Multiple strategies on the same backend keep the historical independent
        selection call contract until account-group slot semantics own that fanout.
        """

        backends_by_key: dict[tuple[Any, ...], set[str]] = {}
        for binding in bindings:
            key = self._selection_cache_key(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
            )
            backend = binding.broker_backend.value if isinstance(binding.broker_backend, SimulationBrokerBackend) else str(binding.broker_backend)
            backends_by_key.setdefault(key, set()).add(backend)
        return {key for key, backends in backends_by_key.items() if len(backends) > 1}

    def _persist_strategy_performance(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        marks = self._performance_marks(context)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and context.qmt_ledger_repository is not None:
            projection = self.performance_service.project_from_qmt_strategy_ledger(
                strategy_id=binding.strategy_id,
                repository=context.qmt_ledger_repository,
                marks=marks,
            )
        else:
            projection = self.performance_service.project_strategy(
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                initial_capital=float(binding.capital_allocation),
                cash=float(context.cash if context.cash is not None else binding.capital_allocation),
                frozen_cash=float(context.frozen_cash),
                realized_pnl=float(context.realized_pnl),
                positions=context.current_positions,
                marks=marks,
            )
        payload = projection.to_dict()
        self.repository.update_simulation_daily_run(
            run.run_id,
            payload_patch={
                "strategy_performance": payload,
                "performance_projection": payload,
            },
        )
        return payload

    @staticmethod
    def _performance_marks(context: SimulationRunContext) -> dict[str, float]:
        marks = dict(context.current_prices or {})
        if context.price_by_symbol:
            marks.update({symbol: float(price) for symbol, price in context.price_by_symbol.items()})
        return marks

    @staticmethod
    def _should_submit_existing_plan(run: SimulationDailyRun, *, plan: ExecutionPlan, submit: bool) -> bool:
        return (
            submit
            and bool(plan.intents)
            and run.status
            in {
                SimulationDailyRunStatus.CREATED,
                SimulationDailyRunStatus.PRECHECKING,
                SimulationDailyRunStatus.SIGNAL_GENERATING,
                SimulationDailyRunStatus.TARGET_GENERATING,
                SimulationDailyRunStatus.PLANNING_EXECUTION,
                SimulationDailyRunStatus.FAILED_RETRYABLE,
            }
            and not run.run_payload_json.get("broker_called")
        )

    def _sync_before_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        sync_service = getattr(context, "qmt_sync_service", None)
        if sync_service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires sync-before-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        summary = sync_service.sync_snapshot()
        payload = summary.to_dict() if hasattr(summary, "to_dict") else dict(summary)
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.RECONCILING,
            payload_patch={
                "last_stage": "RECONCILING",
                "sync_before_submit": payload,
            },
        )
        return payload

    def _reconcile_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        service = getattr(context, "qmt_reconciliation_service", None)
        if service is None:
            raise DataUnavailableError(
                "MiniQMT simulation submit requires reconcile-after-submit service",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "run_id": run.run_id,
                },
            )
        broker_positions = context.broker_positions or []
        if not broker_positions and context.managed_order_service is not None:
            broker = getattr(context.managed_order_service, "_broker", None)
            get_positions = getattr(broker, "get_positions", None)
            if callable(get_positions):
                broker_positions = list(get_positions())
        report = service.reconcile_snapshot(
            account_id=binding.broker_account_id or "",
            trade_date=run.trade_date,
            broker_positions=broker_positions,
        )
        payload = report.to_dict() if hasattr(report, "to_dict") else dict(report)
        report_status = str(payload.get("run", {}).get("status") or "").upper()
        if report_status == "SUCCEEDED":
            next_status = SimulationDailyRunStatus.SUCCEEDED
        else:
            next_status = SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "reconcile_after_submit": payload,
            },
        )
        return payload

    def _validate_fresh_selection_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        selection: StrategyPackageSelectionResult,
        trade_date: date,
    ) -> None:
        evidence = selection.evidence_by_package.get(binding.package_id)
        if evidence is None:
            raise DataUnavailableError(
                "simulation scheduler requires fresh daily selection evidence for the binding package",
                context={
                    "package_id": binding.package_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        stale_reasons: list[str] = []
        if evidence.target_trade_date != trade_date:
            stale_reasons.append("target_trade_date")
        if evidence.package_id != binding.package_id:
            stale_reasons.append("package_id")
        if evidence.manifest_sha256 != binding.manifest_sha256 or evidence.manifest_sha256 != runtime_release.manifest_sha256:
            stale_reasons.append("manifest_sha256")
        if evidence.release_id != runtime_release.release_id or evidence.release_hash != runtime_release.release_hash:
            stale_reasons.append("runtime_release")
        if stale_reasons:
            raise DataUnavailableError(
                "simulation scheduler rejected stale daily selection evidence",
                context={
                    "reasons": stale_reasons,
                    "evidence_id": evidence.evidence_id,
                    "target_trade_date": evidence.target_trade_date.isoformat(),
                    "expected_trade_date": trade_date.isoformat(),
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                },
            )

    def _build_plan_from_selection(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        selection: StrategyPackageSelectionResult,
        context: SimulationRunContext,
        created_by: str,
    ) -> SimulationPlanBuildResult:
        evidence = selection.evidence_by_package[binding.package_id]
        snapshot = SignalSnapshot(
            package_id=binding.package_id,
            manifest_sha256=evidence.manifest_sha256,
            trade_date=trade_date,
            data_source=data_source,
            candidates=selection.package_results.get(binding.package_id, []),
            runtime_config=selection.runtime_config,
            valid_no_candidate=selection.valid_no_candidate,
            no_candidate_reason=selection.no_candidate_reason,
        )
        return self.orchestrator.build_execution_plan(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=evidence,
            signal_snapshot=snapshot,
            current_positions=context.current_positions,
            current_prices=context.current_prices,
            manifest=context.manifest,
            portfolio_id=context.portfolio_id or binding.strategy_id,
            top_k=context.top_k,
            execution_policy_payload=context.execution_policy_payload,
            tail_policy_payload=context.tail_policy_payload,
            created_by=created_by,
        )

    def _handle_tail_after_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        service = context.tail_policy_service
        if service is None:
            return None
        broker_result = execution.broker_result
        handles = getattr(broker_result, "handles", None)
        if not handles:
            return None
        if context.local_broker is None:
            raise DataUnavailableError(
                "LocalSim tail handling requires the original broker instance",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        result = service.handle_local_sim_tail(
            plan=execution.execution_plan,
            broker=context.local_broker,
            handles=handles,
        )
        payload = result.to_dict()
        next_status = SimulationDailyRunStatus.SUCCEEDED if result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "tail_handling": payload,
                "last_stage": next_status.value,
            },
        )
        return payload

    @staticmethod
    def _result_status_after_post_submit(
        execution_status: str,
        *,
        tail_result: dict[str, Any] | None,
        reconciliation: dict[str, Any] | None,
    ) -> str:
        if tail_result is not None:
            return "TAIL_HANDLED" if tail_result.get("success") else "TAIL_HANDLING_FAILED"
        if reconciliation is not None:
            run = reconciliation.get("run") if isinstance(reconciliation.get("run"), dict) else {}
            return "RECONCILED" if run.get("status") == "SUCCEEDED" else "RECONCILIATION_WARNING"
        return execution_status

    @staticmethod
    def _compute_schedule_windows(*, trade_date: date, as_of_time: datetime | None) -> tuple[dict[str, Any], ...]:
        current_time = as_of_time.time().replace(second=0, microsecond=0) if as_of_time is not None else None
        windows: list[dict[str, Any]] = []
        for window in DEFAULT_SCHEDULER_WINDOWS:
            start = datetime.combine(trade_date, time.fromisoformat(window["start"]))
            end = datetime.combine(trade_date, time.fromisoformat(window["end"]))
            state = "PENDING"
            if current_time is not None:
                if current_time < start.time():
                    state = "UPCOMING"
                elif start.time() <= current_time < end.time():
                    state = "ACTIVE"
                else:
                    state = "COMPLETED"
            windows.append(
                {
                    **window,
                    "trade_date": trade_date.isoformat(),
                    "state": state,
                    "start_at": start.isoformat(),
                    "end_at": end.isoformat(),
                }
            )
        return tuple(windows)


simulation_lifecycle_scheduler = build_simulation_lifecycle_scheduler_from_env()


class SimulationLifecycleBackgroundScheduler:
    """Opt-in unattended scheduler wrapper with trading-window orchestration."""

    def __init__(
        self,
        *,
        lifecycle_scheduler: SimulationLifecycleScheduler | None = None,
    ) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler or SimulationLifecycleScheduler()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._interval_seconds = self._default_interval()
        self._default_submit = self._env_flag("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", default=False)
        self._data_source = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL").strip() or "DB_HISTORICAL"
        self._limit = self._default_limit()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None

    def start(self, *, interval_seconds: int | None = None, default_submit: bool | None = None) -> dict[str, Any]:
        interval = int(interval_seconds or self._interval_seconds)
        if interval <= 0:
            raise ValueError("simulation runtime scheduler interval_seconds must be positive")
        with self._lock:
            self._interval_seconds = interval
            if default_submit is not None:
                self._default_submit = bool(default_submit)
            if self._thread and self._thread.is_alive():
                return self.status()
            self._stop_event.clear()
            self._thread = threading.Thread(
                target=self._run_loop,
                name="simulation-runtime-lifecycle-scheduler",
                daemon=True,
            )
            self._thread.start()
            logger.info(
                "Simulation runtime scheduler started interval=%ss default_submit=%s",
                self._interval_seconds,
                self._default_submit,
            )
            return self.status()

    def shutdown(self, wait: bool = False) -> dict[str, Any]:
        with self._lock:
            self._stop_event.set()
            thread = self._thread
        if wait and thread and thread.is_alive():
            thread.join(timeout=5.0)
        logger.info("Simulation runtime scheduler stopped")
        return self.status()

    def status(self) -> dict[str, Any]:
        thread = self._thread
        base = self.lifecycle_scheduler.status()
        running = bool(thread and thread.is_alive() and not self._stop_event.is_set())
        return {
            **base,
            "autostart": running,
            "running": running,
            "thread_alive": bool(thread and thread.is_alive()),
            "scheduler_control_api_enabled": True,
            "manual_tick_endpoint_enabled": True,
            "interval_seconds": self._interval_seconds,
            "default_submit": self._default_submit,
            "data_source": self._data_source,
            "limit": self._limit,
            "last_run_at": self._last_run_at.isoformat() if self._last_run_at else None,
            "last_result": self._last_result,
        }

    def run_once(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = as_of_time or datetime.now()
        trade_date = self._trade_date(now)
        decision = self._window_decision(as_of_time=now, trade_date=trade_date)
        result: dict[str, Any] = {
            "started_at": now.isoformat(),
            "trade_date": trade_date.isoformat(),
            "data_source": self._data_source,
            "window": decision["window"],
            "should_run": decision["should_run"],
            "submit": decision["submit"],
            "processed": [],
            "errors": [],
        }
        if decision["should_run"]:
            try:
                tick = self.lifecycle_scheduler.run_once(
                    trade_date=trade_date,
                    data_source=self._data_source,
                    limit=self._limit,
                    submit=bool(decision["submit"]),
                    as_of_time=now,
                )
                result["processed"] = [
                    {
                        "binding_id": item.binding_id,
                        "strategy_id": item.strategy_id,
                        "broker_backend": item.broker_backend.value,
                        "status": item.status,
                        "run_id": item.run.run_id if item.run else None,
                        "execution_plan_id": item.execution_plan.plan_id if item.execution_plan else None,
                        "error": item.error,
                    }
                    for item in tick.results
                ]
                result["summary"] = {
                    "total_bindings": tick.total_bindings,
                    "planned_count": tick.planned_count,
                    "reused_count": tick.reused_count,
                    "submitted_count": tick.submitted_count,
                    "failed_count": tick.failed_count,
                }
            except Exception as exc:  # scheduler must expose failure, not crash silently
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                }
                result["errors"].append(payload)
                logger.warning("Simulation runtime scheduler tick failed: %s", payload)
        result["completed_at"] = datetime.now().isoformat()
        self._last_run_at = now
        self._last_result = result
        return result

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception:
                logger.exception("Simulation runtime scheduler run_once crashed")
            if self._stop_event.wait(timeout=self._interval_seconds):
                break

    def _window_decision(self, *, as_of_time: datetime, trade_date: date) -> dict[str, Any]:
        windows = SimulationLifecycleScheduler._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)
        active = next((item for item in windows if item["state"] == "ACTIVE"), None)
        if active is None:
            return {"window": None, "should_run": False, "submit": False, "reason": "outside_configured_windows"}
        action = str(active.get("action") or "")
        should_run = action in {"selection_evidence", "execution_plan", "submit"}
        submit = bool(self._default_submit and action == "submit")
        return {"window": active, "should_run": should_run, "submit": submit, "reason": action}

    @staticmethod
    def _trade_date(now: datetime) -> date:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_TRADE_DATE") or "").strip()
        if raw:
            return date.fromisoformat(raw)
        return now.date()

    @staticmethod
    def _default_interval() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC") or "30").strip()
        try:
            value = int(raw)
        except ValueError:
            return 30
        return value if value > 0 else 30

    @staticmethod
    def _default_limit() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_LIMIT") or "100").strip()
        try:
            value = int(raw)
        except ValueError:
            return 100
        return min(max(value, 1), 500)

    @staticmethod
    def _env_flag(name: str, *, default: bool) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "y", "on"}


simulation_lifecycle_background_scheduler = SimulationLifecycleBackgroundScheduler(
    lifecycle_scheduler=simulation_lifecycle_scheduler
)
