"""Scheduler entry point for unified LocalSim and MiniQMT simulation runs.

The scheduler is intentionally broker-neutral until ``submit=True`` is passed.
It drives StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan for
eligible SimulationReleaseBinding rows and reuses persisted plans on restart so
that a backend tick cannot duplicate orders.
"""

from __future__ import annotations

import logging
import math
import os
import queue
import inspect
import threading
import time as monotonic_time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from enum import Enum
from typing import Any, Mapping, Protocol
import psycopg2.extras

from backend.db.pg_pool import get_conn
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.simulation_execution.broker import BrokerBackend
from backend.services.simulation_execution.localsim.economic import (
    canonical_local_sim_json_value,
    local_sim_fact_payload,
    validate_local_sim_duplicate_account_truth,
)
from backend.services.simulation_execution.localsim.projection import (
    LocalSimProjector,
    local_sim_projection_error_is_retryable,
)
from backend.services.simulation_execution.localsim.planning import LocalSimPlanner
from backend.services.simulation_execution.localsim.models import LocalSimPersistenceResult
from backend.services.simulation_execution.localsim.persistence import LocalSimPersistenceCoordinator
from backend.services.simulation_data.contracts import (
    MinuteDataSource,
    pre_trade_tradability_is_suspended,
)
from backend.services.simulation_data.daily_context_provider import (
    DailyTradingContextProvider,
    PreTradeTradabilityProvider,
)
from backend.services.simulation_data.tdx_causal_minute import (
    fetch_tdx_realtime_quotes,
)
from backend.services.paper_trading_v2.models import PaperRun
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.qmt_strategy_ledger.reconciliation import (
    QmtStrategyLedgerReconciliationService,
    broker_authoritative_strategy_projection,
)
from backend.services.qmt_strategy_ledger.order_service import (
    SELL_ORDER_TYPE,
    OrderPreflightError,
    QmtManagedOrderService,
)
from backend.services.qmt_strategy_ledger.models import (
    IntentPreflightStatus,
    IntentSubmitStatus,
    OrderBatchRecord,
    OrderBatchStatus,
    OrderIntentRecord,
    is_open_like_order_status,
    new_id as new_qmt_id,
)
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.miniqmt_execution_runtime import (
    MiniQMTExecutionRuntimeKind,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import BrokerCommandOutboxStatusV1
from backend.services.simulation_runtime.localsim_daily_limit_authority import LocalSimDailyLimitAuthorityProvider
from backend.services.selection_center.models import SelectionMode, SignalSnapshot
from backend.services.strategy_package.live_inference import (
    AUTHORITATIVE_SELECTION_SCOPE,
    AUTHORITATIVE_SELECTION_SOURCE_TYPE,
)
from backend.services.strategy_package.execution_policy import (
    LOCALSIM_TWAP_ONLY_REASON_CODE,
    local_sim_twap_only_policy_snapshot,
)
from backend.services.strategy_package.models import AlphaMode, PackageStatus, StrategyPackageManifest
from backend.services.strategy_package.multi_alpha_live import multi_alpha_selection_artifact_runtime_hash
from backend.services.strategy_package.runtime import _candidate_selection_artifact_runtime_hashes
from backend.services.strategy_package.selection_artifact import selection_artifact_runtime_hash
from backend.services.trading_calendar_status import TradingCalendarStatusService
from backend.services.trading_core.errors import (
    ArtifactGenerationFailedError,
    BrokerRejectedError,
    DataUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import AccountSnapshot, OrderSide, PositionLot, RunStatus

from .bridges import (
    LocalSimExecutionBridge,
    LocalSimExecutionSnapshot,
    LocalSimPlanSubmitResult,
)
from .lifecycle import (
    DEFAULT_SCHEDULER_WINDOWS,
    SCHEDULER_TZ,
    SCHEDULER_TZ_NAME,
    SimulationExecutionResult,
    SimulationLifecycleOrchestrator,
    SimulationPlanBuildResult,
    compute_schedule_windows,
    scheduler_now,
    scheduler_time,
)
from .localsim_dependencies import build_localsim_replay_lifecycle_owner
from .models import (
    ExecutionPlan,
    LocalSimEconomicReceiptV1,
    LocalSimExecutionStateV1,
    LocalSimMarketMarkProvenance,
    LocalSimMarketMarkV1,
    LocalSimProjectionOutboxStatus,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    SimulationBindingApprovalState,
    SimulationDailyRun,
    SimulationDailyRunStatus,
    SimulationReleaseBinding,
    StrategyRuntimeRelease,
    canonical_json_sha256,
    miniqmt_kernel_runtime_id,
)
from backend.services.simulation_data.daily_context import (
    DailyTradingSymbolFactV1,
    DailyTradingSymbolFactV2,
    SimulationBrokerBackend,
)
from backend.services.simulation_signal.contracts import DailySelectionEvidence
from .miniqmt_quote_activation import (
    MiniQMTKernelProductSyncError,
    build_miniqmt_quote_ingress_activation_from_env,
)
from .miniqmt_daily_limit_authority import MiniQMTDailyLimitAuthorityProvider
from .repository import (
    SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY,
    SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY,
    InMemorySimulationRuntimeRepository,
    SimulationRuntimeRepository,
    inspect_simulation_retry_backoff,
    simulation_retry_json_safe_evidence,
)
from backend.services.simulation_signal.strategy_package_selection import (
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
)
from .service import StrategyRuntimeReleaseService
from .tca_eod_observation import TcaEodObservationHook
from .tca_observation_metrics import TcaObservationMetricsEmitter
from .performance import (
    StrategyPerformanceProjectionService,
    with_miniqmt_capacity_residual_observability,
)
from .tail import TailHandlingPolicyService


# SIM scheduling is single-user and must not require an approval transition.
# RETIRED remains an explicit operational stop state, not an approval gate.
DEFAULT_SCHEDULER_SIM_BINDING_STATES = tuple(
    state for state in SimulationBindingApprovalState if state is not SimulationBindingApprovalState.RETIRED
)
MINIQMT_REALTIME_QUOTE_SOURCE = "MINIQMT_REALTIME.broker_quote"
SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV = "SIMULATION_RUNTIME_SELECTION_INFERENCE_TIMEOUT_SEC"
SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV = "SIMULATION_RUNTIME_SELECTION_INFERENCE_MAX_WORKERS"
SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV = "SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC"
SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_SUBMIT_TIMEOUT_SEC"
SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_RECONCILE_TIMEOUT_SEC"
SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV = "SIMULATION_RUNTIME_MINIQMT_TICK_DRIVER_TIMEOUT_SEC"
DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS = 600.0
DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS = 120.0
DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS = 120.0
DEFAULT_MINIQMT_TICK_DRIVER_TIMEOUT_SECONDS = 30.0
_MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE = "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"
_SIMULATION_BINDING_RETRY_KEY = "BINDING_FAILED_RETRYABLE"
_SIMULATION_RECOVERY_RETRY_KEY_PREFIX = "RECOVERY:"
_SIMULATION_RETRY_BASE_DELAY_SECONDS = 60
_SIMULATION_RETRY_MAX_DELAY_SECONDS = 3600
_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS = 600
_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT = 100
# Historical LocalSIM recovery carriers that permanently close a stale failed run. A run
# carrying any of these (as a dict) is already terminally resolved: the stale-run sweep
# must skip it before claiming a retry attempt so the terminalized run does not cost
# claim/clear writes on every scheduler tick. Present-but-non-dict carriers are NOT
# skipped here; the isolated path keeps raising the typed invalid-carrier error. The
# isolated path also validates the outbox before its carrier check, so a run with a
# corrupt outbox AND a valid dict carrier is skipped by this hoist instead of raising;
# that combination is unreachable because the carrier write implies a previously valid
# outbox and nothing mutates the outbox afterwards.
_HISTORICAL_LOCALSIM_RECOVERY_TERMINAL_CARRIER_FIELDS = (
    "local_sim_projection_readback_failure",
    "local_sim_projection_terminal_failure",
    "local_sim_projection_readback_terminal_failure",
    "localsim_historical_legacy_plan_terminalization_v1",
)

logger = logging.getLogger("aistock.simulation_runtime.scheduler")
_POST_CLOSE_RECONCILE_TIME = time(15, 0)


def _build_dynamic_target_equity_basis(
    *,
    binding: SimulationReleaseBinding,
    cash: float,
    frozen_cash: float,
    positions: dict[str, PositionLot],
    prices: dict[str, float],
    source: str,
) -> tuple[float, dict[str, Any]]:
    invalid_marks: list[dict[str, Any]] = []
    market_value = 0.0
    for symbol, position in positions.items():
        quantity = int(position.quantity)
        if quantity <= 0:
            continue
        raw_price = prices.get(symbol)
        try:
            price = float(raw_price) if raw_price is not None else float("nan")
        except (TypeError, ValueError):
            price = float("nan")
        if not math.isfinite(price) or price <= 0:
            invalid_marks.append({"symbol": symbol, "price": raw_price})
            continue
        market_value += quantity * price
    if invalid_marks:
        raise DataUnavailableError(
            "dynamic target sizing requires positive finite marks for all held positions",
            context={
                "strategy_id": binding.strategy_id,
                "binding_id": binding.binding_id,
                "broker_backend": binding.broker_backend.value,
                "invalid_marks": invalid_marks,
            },
        )
    total_equity = float(cash) + float(frozen_cash) + market_value
    if total_equity <= 0:
        raise RuntimeConfigInvalidError(
            "dynamic target sizing requires positive strategy-slot total_equity",
            context={
                "strategy_id": binding.strategy_id,
                "binding_id": binding.binding_id,
                "broker_backend": binding.broker_backend.value,
                "cash": float(cash),
                "frozen_cash": float(frozen_cash),
                "market_value": market_value,
            },
        )
    return total_equity, {
        "schema_version": "simulation_target_equity_basis_v1",
        "source": source,
        "strategy_id": binding.strategy_id,
        "binding_id": binding.binding_id,
        "broker_backend": binding.broker_backend.value,
        "cash": float(cash),
        "frozen_cash": float(frozen_cash),
        "market_value": market_value,
        "total_equity": total_equity,
        "capital_allocation": float(binding.capital_allocation),
        "position_count": sum(1 for position in positions.values() if int(position.quantity) > 0),
    }


_MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES = frozenset(
    {
        "SELL_PROCEEDS_REQUIRED",
        "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
    }
)
_MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES = frozenset(
    {
        "SKIPPED_INSUFFICIENT_CAPITAL",
    }
)
_MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES = (
    _MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES | _MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES
)
_MINIQMT_STALE_ACTIVE_STATUSES = (
    SimulationDailyRunStatus.CREATED,
    SimulationDailyRunStatus.PRECHECKING,
    SimulationDailyRunStatus.SIGNAL_GENERATING,
    SimulationDailyRunStatus.TARGET_GENERATING,
    SimulationDailyRunStatus.PLANNING_EXECUTION,
    SimulationDailyRunStatus.SUBMITTING,
    SimulationDailyRunStatus.INTRADAY_RUNNING,
    SimulationDailyRunStatus.TAIL_HANDLING,
    SimulationDailyRunStatus.RECONCILING,
)
_LOCALSIM_STALE_ACTIVE_STATUSES = (
    SimulationDailyRunStatus.CREATED,
    SimulationDailyRunStatus.PRECHECKING,
    SimulationDailyRunStatus.SIGNAL_GENERATING,
    SimulationDailyRunStatus.TARGET_GENERATING,
    SimulationDailyRunStatus.PLANNING_EXECUTION,
    SimulationDailyRunStatus.SUBMITTING,
    SimulationDailyRunStatus.INTRADAY_RUNNING,
    SimulationDailyRunStatus.TAIL_HANDLING,
    SimulationDailyRunStatus.RECONCILING,
)

_LOCALSIM_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.localsim_roll_forward"
_MINIQMT_ROLL_FORWARD_CREATED_BY = "simulation_lifecycle_scheduler.miniqmt_roll_forward"
_LOCALSIM_PROJECTION_MAX_ATTEMPTS = 3


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
    context_diagnostics: dict[str, Any] = field(default_factory=dict)
    tail_policy_service: TailHandlingPolicyService | None = None
    price_by_symbol: dict[str, Any] | None = None
    paper_repository: Any | None = None
    cash: float | None = None
    frozen_cash: float = 0.0
    realized_pnl: float = 0.0
    market_data_source: str | None = None
    pre_trade_tradability: dict[str, dict[str, Any]] = field(default_factory=dict)
    target_total_equity: float | None = None
    target_equity_context: dict[str, Any] = field(default_factory=dict)


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
        package_manifest_loader: Callable[[str], StrategyPackageManifest | dict[str, Any] | None] | None = None,
        runtime_repository: SimulationRuntimeRepository | InMemorySimulationRuntimeRepository | Any | None = None,
        pre_trade_tradability_provider: PreTradeTradabilityProvider | Any | None = None,
        daily_trading_context_provider: DailyTradingContextProvider | Any | None = None,
        localsim_daily_pre_close_quote_fetcher: Callable[[list[str]], dict[str, dict[str, Any]]] | None = None,
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
        self._package_manifest_loader = package_manifest_loader or _default_strategy_package_manifest_loader
        self._runtime_repository = runtime_repository
        self._pre_trade_tradability_provider_injected = pre_trade_tradability_provider is not None
        self._pre_trade_tradability_provider = pre_trade_tradability_provider or PreTradeTradabilityProvider(
            realtime_quote_fetcher=fetch_tdx_realtime_quotes,
            realtime_quote_source="TDX_REALTIME.batch_quote",
        )
        self._daily_trading_context_provider = daily_trading_context_provider or DailyTradingContextProvider()
        self._localsim_daily_pre_close_quote_fetcher = (
            localsim_daily_pre_close_quote_fetcher or fetch_tdx_realtime_quotes
        )
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
            "miniqmt_state_source": "broker_authoritative_positions_with_strategy_slot_projection",
            "planning_market_price_source": "market.kline_daily_raw_latest_close",
            "existing_plan_market_data_policy": {
                "planning_market_tables_reloaded": False,
                "localsim": "frozen_plan_tradability+TDX_REALTIME_CAUSAL_MINUTE",
                "miniqmt": f"frozen_plan_tradability+{MINIQMT_REALTIME_QUOTE_SOURCE}",
            },
            "pre_trade_tradability_gate": {
                "source": type(self._pre_trade_tradability_provider).__name__,
                "localsim_same_day_quote_required": False,
                "localsim_execution_authority": "TDX_REALTIME_CAUSAL_MINUTE",
                "miniqmt_quote_required": True,
                "miniqmt_quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                "historical_quote_required": False,
            },
            "localsim_market_data_source_policy": {
                "same_day": MinuteDataSource.TDX_REALTIME.value,
                "historical": "persisted_portfolio_data_source",
            },
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
        return self.load_context_for_phase(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=None,
            require_localsim_realtime_quote=(trade_date == date.today()),
        )

    def load_context_for_phase(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None,
        require_localsim_realtime_quote: bool,
    ) -> SimulationRunContext:
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._load_local_sim_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
                require_realtime_quote=require_localsim_realtime_quote,
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

    def load_existing_plan_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        plan: ExecutionPlan,
        trade_date: date,
        as_of_time: datetime | None,
    ) -> SimulationRunContext:
        """Load execution services without re-reading planning market tables.

        Tradability is immutable plan evidence after plan creation.  Active
        LocalSIM gets causal marks from its TDX minute broker; active MiniQMT
        gets marks from the broker quote surface used by B0_QUOTE_V2.
        """

        frozen_pre_trade_tradability = self._frozen_pre_trade_tradability(plan)
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._load_local_sim_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
                require_realtime_quote=False,
                planning_market_data=False,
                frozen_pre_trade_tradability=frozen_pre_trade_tradability,
            )
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            return self._load_miniqmt_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                planning_market_data=False,
                frozen_pre_trade_tradability=frozen_pre_trade_tradability,
            )
        raise DataUnavailableError(
            "ProductionSimulationRunContextProvider: unsupported broker backend for existing plan",
            context={
                "reason_code": "SIMULATION_EXISTING_PLAN_BROKER_BACKEND_UNSUPPORTED",
                "broker_backend": binding.broker_backend.value,
                "binding_id": binding.binding_id,
                "plan_id": plan.plan_id,
            },
        )

    @staticmethod
    def _frozen_pre_trade_tradability(plan: ExecutionPlan) -> dict[str, dict[str, Any]]:
        statuses: dict[str, dict[str, Any]] = {}
        for decision in plan.trading_rule_decisions:
            raw = decision.price_limit_rule.get("pre_trade_tradability")
            if raw is None:
                continue
            if not isinstance(raw, dict):
                raise DataUnavailableError(
                    "frozen execution plan pre-trade tradability carrier is invalid",
                    context={
                        "reason_code": "SIMULATION_FROZEN_PRE_TRADE_TRADABILITY_INVALID",
                        "plan_id": plan.plan_id,
                        "decision_id": decision.decision_id,
                        "symbol": decision.symbol,
                        "actual_type": type(raw).__name__,
                    },
                )
            candidate = deepcopy(raw)
            reference = candidate.get("daily_trading_context")
            if reference is not None:
                if not isinstance(reference, dict) or not isinstance(reference.get("context"), dict):
                    raise DataUnavailableError(
                        "frozen execution plan daily trading context reference is invalid",
                        context={
                            "reason_code": "DAILY_TRADING_CONTEXT_DECISION_REFERENCE_INVALID",
                            "plan_id": plan.plan_id,
                            "symbol": decision.symbol,
                        },
                    )
                try:
                    from .daily_limit_authority import parse_daily_trading_context

                    daily_context = parse_daily_trading_context(reference["context"])
                except Exception as exc:
                    raise DataUnavailableError(
                        "frozen execution plan daily trading context cannot be read back",
                        context={
                            "reason_code": "DAILY_TRADING_CONTEXT_DECISION_REFERENCE_INVALID",
                            "plan_id": plan.plan_id,
                            "symbol": decision.symbol,
                        },
                    ) from exc
                fact = daily_context.symbols.get(decision.symbol)
                if (
                    fact is None
                    or reference.get("context_id") != daily_context.context_id
                    or reference.get("context_hash") != daily_context.context_hash
                    or reference.get("symbol_fact") != fact.canonical_payload()
                    or plan.plan_payload_json.get("daily_trading_context") != daily_context.carrier_payload()
                ):
                    raise DataUnavailableError(
                        "frozen execution plan daily trading context reference conflicts with plan identity",
                        context={
                            "reason_code": "DAILY_TRADING_CONTEXT_DECISION_REFERENCE_CONFLICT",
                            "plan_id": plan.plan_id,
                            "symbol": decision.symbol,
                        },
                    )
            previous = statuses.get(decision.symbol)
            if previous is not None and previous != candidate:
                raise DataUnavailableError(
                    "frozen execution plan contains conflicting pre-trade tradability evidence",
                    context={
                        "reason_code": "SIMULATION_FROZEN_PRE_TRADE_TRADABILITY_CONFLICT",
                        "plan_id": plan.plan_id,
                        "symbol": decision.symbol,
                        "existing": previous,
                        "received": candidate,
                    },
                )
            statuses[decision.symbol] = candidate
        return dict(sorted(statuses.items()))

    def _load_local_sim_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None = None,
        require_realtime_quote: bool | None = None,
        planning_market_data: bool = True,
        frozen_pre_trade_tradability: dict[str, dict[str, Any]] | None = None,
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
            positions, settlement_diagnostics = self._settle_local_sim_positions_for_trade_date(
                positions=positions,
                trade_date=trade_date,
                strategy_id=binding.strategy_id,
                binding_id=binding.binding_id,
            )
            cash = float(paper_repository.load_latest_cash(portfolio, trade_date))
        except (DataUnavailableError, RuntimeConfigInvalidError):
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
        prices = (
            self._load_prices_for_positions(
                positions,
                trade_date,
                strategy_id=binding.strategy_id,
                binding_id=binding.binding_id,
            )
            if planning_market_data
            else {}
        )
        market_data_source = self._resolve_local_sim_market_data_source(
            portfolio=portfolio,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        pre_trade_tradability = (
            self._load_pre_trade_tradability(
                symbols=list(positions),
                trade_date=trade_date,
                require_realtime_quote=False,
                as_of_time=as_of_time,
            )
            if planning_market_data and self._pre_trade_tradability_provider_injected
            else {}
            if planning_market_data
            else deepcopy(frozen_pre_trade_tradability or {})
        )
        manifest, manifest_identity_diagnostics = self._resolve_local_sim_manifest(
            portfolio_manifest=getattr(portfolio, "frozen_manifest", None),
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        effective_execution_policy_payload = self._resolve_local_sim_execution_policy(
            runtime_release=runtime_release,
            binding=binding,
            portfolio=portfolio,
        )
        broker_execution_policy = (
            effective_execution_policy_payload
            if self._local_broker_factory is None and self._enable_localsim_broker
            else None
        )
        local_broker = self._build_local_sim_broker(
            portfolio_id=portfolio_id,
            portfolio=portfolio,
            binding=binding,
            runtime_release=runtime_release,
            manifest=manifest,
            execution_policy=broker_execution_policy,
            cash=cash,
            positions=positions,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        if planning_market_data:
            target_total_equity, target_equity_context = _build_dynamic_target_equity_basis(
                binding=binding,
                cash=cash,
                frozen_cash=0.0,
                positions=positions,
                prices=prices,
                source="paper_v2_portfolio_dynamic_equity",
            )
        else:
            target_total_equity = None
            target_equity_context = {
                "schema_version": "simulation_existing_plan_context_v1",
                "source": "frozen_execution_plan",
                "planning_market_data_reloaded": False,
            }
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=portfolio_id,
            manifest=manifest,
            execution_policy_payload=effective_execution_policy_payload,
            local_broker=local_broker,
            paper_repository=paper_repository,
            cash=cash,
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            context_diagnostics={
                "manifest_identity": manifest_identity_diagnostics,
                "localsim_tplus1_settlement": settlement_diagnostics,
                "pre_trade_tradability": self._pre_trade_tradability_diagnostics(pre_trade_tradability),
                "target_equity_basis": target_equity_context,
            },
            market_data_source=(
                getattr(local_broker, "data_source").value
                if local_broker is not None and getattr(local_broker, "data_source", None) is not None
                else market_data_source.value
            ),
            pre_trade_tradability=pre_trade_tradability,
        )

    @staticmethod
    def _settle_local_sim_positions_for_trade_date(
        *,
        positions: dict[str, PositionLot],
        trade_date: date,
        strategy_id: str,
        binding_id: str,
    ) -> tuple[dict[str, PositionLot], dict[str, Any]]:
        settled: dict[str, PositionLot] = {}
        adjusted: list[dict[str, Any]] = []
        for symbol, position in positions.items():
            updated = position
            if position.trade_date < trade_date and int(position.available_quantity) < int(position.quantity):
                updated = position.model_copy(update={"available_quantity": int(position.quantity)})
                adjusted.append(
                    {
                        "symbol": symbol,
                        "trade_date": position.trade_date.isoformat(),
                        "previous_available_quantity": int(position.available_quantity),
                        "settled_available_quantity": int(updated.available_quantity),
                        "quantity": int(position.quantity),
                    }
                )
            settled[symbol] = updated
        return settled, {
            "schema_version": "localsim_tplus1_settlement_v1",
            "strategy_id": strategy_id,
            "binding_id": binding_id,
            "trade_date": trade_date.isoformat(),
            "position_count": len(positions),
            "settled_position_count": len(adjusted),
            "settled_positions": adjusted,
        }

    def _load_miniqmt_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        planning_market_data: bool = True,
        frozen_pre_trade_tradability: dict[str, dict[str, Any]] | None = None,
    ) -> SimulationRunContext:
        qmt_repository = self._qmt_ledger_repository or self._build_dependency(
            self._qmt_repository_factory,
            "QmtStrategyLedgerRepository",
            binding=binding,
            trade_date=trade_date,
        )
        need_qmt_client = (
            self._position_loader is None
            or self._managed_order_service_factory is None
            or self._qmt_sync_service_factory is None
            or not planning_market_data
        )
        qmt_client = self._qmt_client_factory() if need_qmt_client else None
        try:
            account = qmt_repository.get_virtual_account(binding.strategy_id)
            if self._position_loader is not None:
                broker_positions: list[dict[str, Any]] = []
                reconciliation_diagnostics: dict[str, Any] = {
                    "schema_version": "miniqmt_broker_position_reconciliation_v1",
                    "status": "skipped_injected_position_loader",
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                }
                positions = self._load_positions_with_injected_loader(binding.strategy_id, trade_date)
            else:
                broker_positions = self._load_miniqmt_broker_positions(
                    qmt_client,
                    binding=binding,
                    trade_date=trade_date,
                )
                ledger_positions = self._positions_from_qmt_lots(
                    repository=qmt_repository,
                    strategy_id=binding.strategy_id,
                )
                account_strategy_quantities = self._miniqmt_account_strategy_lot_quantities(
                    repository=qmt_repository,
                    account_id=account.account_id,
                )
                positions, reconciliation_diagnostics = self._reconcile_miniqmt_positions_with_broker(
                    ledger_positions,
                    broker_positions,
                    strategy_id=binding.strategy_id,
                    binding_id=binding.binding_id,
                    trade_date=trade_date,
                    account_strategy_quantities=account_strategy_quantities,
                )
        except (DataUnavailableError, RuntimeConfigInvalidError):
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
        if planning_market_data:
            prices = self._load_prices_for_positions(
                positions,
                trade_date,
                strategy_id=binding.strategy_id,
                binding_id=binding.binding_id,
            )
            pre_trade_tradability = (
                self._load_miniqmt_pre_trade_tradability(
                    symbols=list(positions),
                    trade_date=trade_date,
                    binding=binding,
                    qmt_client=qmt_client,
                    require_realtime_quote=self._position_loader is None and trade_date == date.today(),
                )
                if self._pre_trade_tradability_provider_injected
                else {}
            )
        else:
            prices = self._load_miniqmt_runtime_marks(
                symbols=list(positions),
                trade_date=trade_date,
                binding=binding,
                qmt_client=qmt_client,
            )
            pre_trade_tradability = deepcopy(frozen_pre_trade_tradability or {})
        manifest = self._load_strategy_package_manifest(
            runtime_release=runtime_release,
            binding=binding,
        )
        managed_order_service = (
            self._managed_order_service_factory()
            if self._managed_order_service_factory is not None
            else self._build_managed_order_service(qmt_repository, broker=qmt_client)
        )
        if not self._enable_miniqmt_submit and self._managed_order_service_factory is None:
            managed_order_service = PreviewOnlyMiniQMTManagedOrderService(managed_order_service)
        qmt_sync_service = (
            self._qmt_sync_service_factory()
            if self._qmt_sync_service_factory is not None
            else QmtStrategyLedgerSyncService(
                repository=qmt_repository,
                qmt_client=qmt_client,
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
        cash = float(account.cash)
        frozen_cash = float(account.frozen_cash)
        if planning_market_data:
            target_total_equity, target_equity_context = _build_dynamic_target_equity_basis(
                binding=binding,
                cash=cash,
                frozen_cash=frozen_cash,
                positions=positions,
                prices=prices,
                source="miniqmt_strategy_slot_dynamic_equity",
            )
        else:
            target_total_equity = None
            target_equity_context = {
                "schema_version": "simulation_existing_plan_context_v1",
                "source": MINIQMT_REALTIME_QUOTE_SOURCE,
                "planning_market_data_reloaded": False,
            }
        return SimulationRunContext(
            current_positions=positions,
            current_prices=prices,
            portfolio_id=binding.strategy_id,
            manifest=manifest,
            managed_order_service=managed_order_service,
            qmt_sync_service=qmt_sync_service,
            qmt_reconciliation_service=qmt_reconciliation_service,
            qmt_ledger_repository=qmt_repository,
            broker_positions=broker_positions,
            context_diagnostics={
                "miniqmt_broker_position_reconciliation": reconciliation_diagnostics,
                "pre_trade_tradability": self._pre_trade_tradability_diagnostics(pre_trade_tradability),
                "target_equity_basis": target_equity_context,
            },
            cash=cash,
            frozen_cash=frozen_cash,
            realized_pnl=float(account.realized_pnl),
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            price_by_symbol=prices,
            market_data_source=MinuteDataSource.MINIQMT_REALTIME.value,
            pre_trade_tradability=pre_trade_tradability,
        )

    def _load_miniqmt_runtime_marks(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        qmt_client: Any | None,
    ) -> dict[str, float]:
        normalized_symbols = sorted({str(symbol).strip() for symbol in symbols if str(symbol).strip()})
        if not normalized_symbols:
            return {}
        quote_fetcher = self._build_miniqmt_quote_fetcher(
            qmt_client=qmt_client,
            binding=binding,
            trade_date=trade_date,
        )
        quotes = quote_fetcher(normalized_symbols)
        marks: dict[str, float] = {}
        invalid: list[dict[str, Any]] = []
        for symbol in normalized_symbols:
            quote = quotes.get(symbol)
            if not isinstance(quote, dict):
                invalid.append({"symbol": symbol, "reason_code": "MINIQMT_RUNTIME_QUOTE_MISSING"})
                continue
            kline = quote.get("K") if isinstance(quote.get("K"), dict) else {}
            candidates = (
                kline.get("Close"),
                kline.get("close"),
                quote.get("lastPrice"),
                quote.get("last_price"),
                quote.get("price"),
                quote.get("close"),
            )
            price = None
            for raw in candidates:
                try:
                    candidate = float(raw)
                except (TypeError, ValueError):
                    continue
                if math.isfinite(candidate) and candidate > 0:
                    price = candidate
                    break
            if price is None:
                invalid.append({"symbol": symbol, "reason_code": "MINIQMT_RUNTIME_QUOTE_PRICE_INVALID"})
                continue
            marks[symbol] = price
        if invalid:
            raise DataUnavailableError(
                "MiniQMT existing-plan runtime marks are incomplete",
                context={
                    "reason_code": "MINIQMT_EXISTING_PLAN_RUNTIME_MARKS_INVALID",
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                    "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                    "invalid": invalid,
                },
            )
        return marks

    def load_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        market_data_source: str | None = None,
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
        frozen_daily_statuses: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if require_realtime_quote is None:
            current_trade_date = (
                date.today()
                if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                else scheduler_time(as_of_time).date()
                if as_of_time is not None
                else date.today()
            )
            require_quote = self._position_loader is None and (
                (binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and trade_date == current_trade_date)
                or (
                    binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM
                    and (market_data_source == MinuteDataSource.TDX_REALTIME.value or trade_date == current_trade_date)
                )
            )
        else:
            require_quote = bool(require_realtime_quote and self._position_loader is None)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            qmt_client = self._qmt_client_factory() if require_quote else None
            return self._load_miniqmt_pre_trade_tradability(
                symbols=symbols,
                trade_date=trade_date,
                binding=binding,
                qmt_client=qmt_client,
                require_realtime_quote=require_quote,
                as_of_time=as_of_time,
                side_by_symbol=side_by_symbol,
                frozen_daily_statuses=frozen_daily_statuses,
            )
        return self._load_pre_trade_tradability(
            symbols=symbols,
            trade_date=trade_date,
            require_realtime_quote=require_quote,
            as_of_time=as_of_time,
            side_by_symbol=side_by_symbol,
            frozen_daily_statuses=frozen_daily_statuses,
        )

    def load_daily_trading_context(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        as_of_time: datetime,
        calendar_service_snapshot: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        if binding.broker_backend is SimulationBrokerBackend.MINIQMT_SIM:
            qmt_client = self._qmt_client_factory()
            instrument_batch_reader = getattr(qmt_client, "get_instrument_details", None)
            supporting_fact_loader = getattr(self._daily_trading_context_provider, "load_supporting_facts", None)
            if not callable(instrument_batch_reader) or not callable(supporting_fact_loader):
                raise DataUnavailableError(
                    "MiniQMT planning requires bounded instrument-detail and supporting-fact batch loaders",
                    context={
                        "reason_code": "MINIQMT_DAILY_LIMIT_AUTHORITY_PROVIDER_MISSING",
                        "binding_id": binding.binding_id,
                        "trade_date": trade_date.isoformat(),
                    },
                )
            from backend.services.miniqmt_execution_runtime.b0_quote_v2 import QuoteControlBindingV1

            quote_control = QuoteControlBindingV1.from_binding_config(binding.binding_config_json)
            quote_continuity_identity = canonical_json_sha256(
                {
                    "binding_id": binding.binding_id,
                    "binding_hash": binding.binding_hash,
                    "quote_control": quote_control.canonical_payload(),
                }
            )
            provider = MiniQMTDailyLimitAuthorityProvider(
                instrument_batch_reader=instrument_batch_reader,
                supporting_fact_loader=supporting_fact_loader,
            )
            context = provider.load(
                symbols=symbols,
                trade_date=trade_date,
                as_of_time=scheduler_time(as_of_time),
                calendar_service_snapshot=calendar_service_snapshot,
                binding_identity=f"{binding.binding_id}:{binding.binding_hash}",
                package_identity=f"{runtime_release.package_id}:{runtime_release.manifest_sha256}",
                release_identity=f"{runtime_release.release_id}:{runtime_release.release_hash}",
                runtime_identity=(f"{binding.broker_account_id or binding.strategy_id}:{type(qmt_client).__name__}"),
                quote_continuity_identity=quote_continuity_identity,
            )
            return provider.to_pre_trade_statuses(context)

        stk_limit_attempt_loader = getattr(
            self._daily_trading_context_provider,
            "load_stk_limit_authority_attempt",
            None,
        )
        supporting_fact_loader = getattr(self._daily_trading_context_provider, "load_supporting_facts", None)
        if not callable(stk_limit_attempt_loader) or not callable(supporting_fact_loader):
            raise DataUnavailableError(
                "LocalSIM planning requires stk_limit-attempt and supporting-fact batch loaders",
                context={
                    "reason_code": "LOCALSIM_DAILY_LIMIT_AUTHORITY_PROVIDER_MISSING",
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        provider = LocalSimDailyLimitAuthorityProvider(
            stk_limit_attempt_loader=stk_limit_attempt_loader,
            supporting_fact_loader=supporting_fact_loader,
            tdx_reference_reader=self._localsim_daily_pre_close_quote_fetcher,
        )
        context = provider.load(
            symbols=symbols,
            trade_date=trade_date,
            as_of_time=scheduler_time(as_of_time),
            calendar_service_snapshot=calendar_service_snapshot,
            binding_identity=f"{binding.binding_id}:{binding.binding_hash}",
            package_identity=f"{runtime_release.package_id}:{runtime_release.manifest_sha256}",
            release_identity=f"{runtime_release.release_id}:{runtime_release.release_hash}",
        )
        return provider.to_pre_trade_statuses(context)

    def _load_miniqmt_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        binding: SimulationReleaseBinding,
        qmt_client: Any | None,
        require_realtime_quote: bool,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
        frozen_daily_statuses: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if not require_realtime_quote:
            return {str(symbol): dict(status) for symbol, status in (frozen_daily_statuses or {}).items()}
        quote_fetcher = self._build_miniqmt_quote_fetcher(
            qmt_client=qmt_client,
            binding=binding,
            trade_date=trade_date,
        )
        provider_kwargs: dict[str, Any] = {
            "realtime_quote_fetcher": quote_fetcher,
            "realtime_quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
        }
        injected_provider = (
            self._pre_trade_tradability_provider if self._pre_trade_tradability_provider_injected else None
        )
        suspend_status_provider = getattr(injected_provider, "suspend_status_provider", None)
        st_status_provider = getattr(injected_provider, "st_status_provider", None)
        if suspend_status_provider is not None:
            provider_kwargs["suspend_status_provider"] = suspend_status_provider
        if st_status_provider is not None:
            provider_kwargs["st_status_provider"] = st_status_provider
        provider = PreTradeTradabilityProvider(**provider_kwargs)
        return self._load_pre_trade_tradability(
            symbols=symbols,
            trade_date=trade_date,
            require_realtime_quote=True,
            provider=provider,
            as_of_time=as_of_time,
            side_by_symbol=side_by_symbol,
            frozen_daily_statuses=frozen_daily_statuses,
        )

    def _build_miniqmt_quote_fetcher(
        self,
        *,
        qmt_client: Any | None,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> Callable[[list[str]], dict[str, dict[str, Any]]]:
        if qmt_client is None:
            raise DataUnavailableError(
                "MiniQMT realtime quote fetch requires a MiniQMT broker client",
                context={
                    "reason_code": "MINIQMT_REALTIME_QUOTE_CLIENT_MISSING",
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                    "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                },
            )

        def fetch(symbols: list[str]) -> dict[str, dict[str, Any]]:
            normalized_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
            if not normalized_symbols:
                return {}
            query_quote = getattr(qmt_client, "query_quote", None)
            try:
                if callable(query_quote):
                    quotes: dict[str, dict[str, Any]] = {}
                    for symbol in normalized_symbols:
                        row = query_quote(symbol)
                        if row is None:
                            continue
                        if not isinstance(row, dict):
                            raise DataUnavailableError(
                                "MiniQMT query_quote returned invalid quote payload",
                                context={
                                    "reason_code": "MINIQMT_REALTIME_QUOTE_PAYLOAD_INVALID",
                                    "strategy_id": binding.strategy_id,
                                    "binding_id": binding.binding_id,
                                    "trade_date": trade_date.isoformat(),
                                    "symbol": symbol,
                                    "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                                    "payload_type": type(row).__name__,
                                },
                            )
                        quotes[symbol] = dict(row)
                    return quotes

                get_full_tick = getattr(qmt_client, "get_full_tick", None)
                if not callable(get_full_tick):
                    raise DataUnavailableError(
                        "MiniQMT realtime quote client must expose get_full_tick or query_quote",
                        context={
                            "reason_code": "MINIQMT_REALTIME_QUOTE_FETCHER_MISSING",
                            "strategy_id": binding.strategy_id,
                            "binding_id": binding.binding_id,
                            "broker_account_id": binding.broker_account_id,
                            "trade_date": trade_date.isoformat(),
                            "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                            "client_type": type(qmt_client).__name__,
                        },
                    )
                raw_payload = get_full_tick(normalized_symbols)
                if not isinstance(raw_payload, dict):
                    raise DataUnavailableError(
                        "MiniQMT get_full_tick returned invalid quote payload",
                        context={
                            "reason_code": "MINIQMT_REALTIME_QUOTE_PAYLOAD_INVALID",
                            "strategy_id": binding.strategy_id,
                            "binding_id": binding.binding_id,
                            "trade_date": trade_date.isoformat(),
                            "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                            "payload_type": type(raw_payload).__name__,
                        },
                    )
                from backend.services.paper_trading_v2.broker.minqmtsim import normalize_miniqmt_quote_row

                quotes: dict[str, dict[str, Any]] = {}
                for symbol in normalized_symbols:
                    row = raw_payload.get(symbol)
                    if row is None:
                        raw_code = symbol.split(".")[0]
                        for key, value in raw_payload.items():
                            if str(key).split(".")[0] == raw_code:
                                row = value
                                break
                    if row is None:
                        continue
                    if not isinstance(row, dict):
                        raise DataUnavailableError(
                            "MiniQMT get_full_tick returned invalid quote row",
                            context={
                                "reason_code": "MINIQMT_REALTIME_QUOTE_ROW_INVALID",
                                "strategy_id": binding.strategy_id,
                                "binding_id": binding.binding_id,
                                "trade_date": trade_date.isoformat(),
                                "symbol": symbol,
                                "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                                "payload_type": type(row).__name__,
                            },
                        )
                    quotes[symbol] = dict(normalize_miniqmt_quote_row(symbol, row))
                return quotes
            except DataUnavailableError:
                raise
            except Exception as exc:  # noqa: BLE001
                raise DataUnavailableError(
                    "MiniQMT realtime quote fetch failed",
                    context={
                        "reason_code": "MINIQMT_REALTIME_QUOTE_FETCH_FAILED",
                        "strategy_id": binding.strategy_id,
                        "binding_id": binding.binding_id,
                        "broker_account_id": binding.broker_account_id,
                        "trade_date": trade_date.isoformat(),
                        "symbols": normalized_symbols,
                        "quote_source": MINIQMT_REALTIME_QUOTE_SOURCE,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                ) from exc

        return fetch

    def _load_pre_trade_tradability(
        self,
        *,
        symbols: list[str],
        trade_date: date,
        require_realtime_quote: bool,
        provider: Any | None = None,
        as_of_time: datetime | None = None,
        side_by_symbol: dict[str, Any] | None = None,
        frozen_daily_statuses: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> dict[str, dict[str, Any]]:
        if self._position_loader is not None and not self._pre_trade_tradability_provider_injected:
            return {str(symbol): dict(status) for symbol, status in (frozen_daily_statuses or {}).items()}
        tradability_provider = provider or self._pre_trade_tradability_provider
        loader = getattr(tradability_provider, "get_statuses", None)
        if not callable(loader):
            raise DataUnavailableError(
                "pre-trade tradability provider must expose get_statuses",
                context={
                    "reason_code": "PRE_TRADE_TRADABILITY_PROVIDER_METHOD_MISSING",
                    "provider": type(tradability_provider).__name__,
                },
            )
        signature = inspect.signature(loader)
        accepts_kwargs = any(
            parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
        )
        optional_kwargs = {
            "require_realtime_quote": require_realtime_quote,
            "as_of_time": as_of_time,
            "side_by_symbol": side_by_symbol,
            "frozen_daily_statuses": frozen_daily_statuses,
        }
        supported_kwargs = {
            key: value for key, value in optional_kwargs.items() if accepts_kwargs or key in signature.parameters
        }
        raw = loader(symbols, trade_date, **supported_kwargs)
        if not isinstance(raw, dict):
            raise DataUnavailableError(
                "pre-trade tradability provider returned invalid payload",
                context={
                    "reason_code": "PRE_TRADE_TRADABILITY_PROVIDER_INVALID_PAYLOAD",
                    "provider": type(tradability_provider).__name__,
                    "payload_type": type(raw).__name__,
                },
            )
        return {str(symbol): dict(status) for symbol, status in raw.items() if isinstance(status, dict)}

    @staticmethod
    def _pre_trade_tradability_diagnostics(statuses: dict[str, dict[str, Any]]) -> dict[str, Any]:
        blocked = [
            {
                "symbol": symbol,
                "reason_code": status.get("reason_code"),
                "source": status.get("source"),
            }
            for symbol, status in sorted(statuses.items())
            if isinstance(status, dict) and not bool(status.get("is_tradable", True))
        ]
        return {
            "schema_version": "pre_trade_tradability_diagnostics_v1",
            "symbol_count": len(statuses),
            "blocked_symbol_count": len(blocked),
            "blocked_symbols": blocked,
        }

    def _build_managed_order_service(self, qmt_repository: Any, *, broker: Any | None = None) -> QmtManagedOrderService:
        broker = broker if broker is not None else self._qmt_client_factory()
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
        runtime_release: StrategyRuntimeRelease,
        manifest: StrategyPackageManifest | None,
        execution_policy: dict[str, Any] | None,
        cash: float,
        positions: dict[str, PositionLot],
        trade_date: date,
        as_of_time: datetime | None = None,
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
            from backend.services.simulation_execution.localsim import LocalSimBackend

            data_source = self._resolve_local_sim_market_data_source(
                portfolio=portfolio,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
            return LocalSimBackend(
                portfolio_id=portfolio_id,
                initial_cash=float(getattr(portfolio, "initial_cash", binding.capital_allocation)),
                initial_available_cash=cash,
                initial_positions=positions,
                data_source=data_source,
                manifest=manifest,
                execution_policy=execution_policy,
                package_id=binding.package_id,
                scheduler_as_of_time=as_of_time,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to construct LocalSim production broker from persisted context",
                context={
                    "portfolio_id": portfolio_id,
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "execution_policy_version_id": runtime_release.execution_policy_version_id,
                    "execution_policy_sha256": runtime_release.execution_policy_sha256,
                },
            ) from exc

    @staticmethod
    def _resolve_local_sim_market_data_source(
        *,
        portfolio: Any,
        trade_date: date,
        as_of_time: datetime | None = None,
    ) -> MinuteDataSource:
        data_source = getattr(portfolio, "data_source", MinuteDataSource.DB_HISTORICAL)
        if not isinstance(data_source, MinuteDataSource):
            data_source = MinuteDataSource(str(data_source))
        current_trade_date = scheduler_time(as_of_time).date() if as_of_time is not None else date.today()
        if trade_date == current_trade_date:
            return MinuteDataSource.TDX_REALTIME
        return data_source

    @staticmethod
    def _release_execution_policy_payload(runtime_release: StrategyRuntimeRelease) -> dict[str, Any] | None:
        release_config = (
            runtime_release.release_config_json if isinstance(runtime_release.release_config_json, dict) else {}
        )
        payload = release_config.get("execution_policy")
        if not isinstance(payload, dict):
            return None
        return dict(payload)

    def _resolve_local_sim_execution_policy(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        portfolio: Any,
    ) -> dict[str, Any]:
        release_policy = self._release_execution_policy_payload(runtime_release) or {}
        requested_policy_json = release_policy.get("policy_json")
        requested_algo_code = (
            str(requested_policy_json.get("algo_code") or "").strip().upper()
            if isinstance(requested_policy_json, dict)
            else None
        )
        effective_policy = local_sim_twap_only_policy_snapshot()
        logger.info(
            "LocalSIM execution policy selected",
            extra={
                "reason_code": LOCALSIM_TWAP_ONLY_REASON_CODE,
                "strategy_id": binding.strategy_id,
                "binding_id": binding.binding_id,
                "release_id": runtime_release.release_id,
                "requested_policy_id": runtime_release.execution_policy_version_id,
                "requested_policy_sha256": runtime_release.execution_policy_sha256,
                "requested_algo_code": requested_algo_code,
                "effective_policy_id": effective_policy["policy_version_id"],
                "effective_algo_code": "TWAP",
                "source_policy_consulted_for_execution": False,
                "fallback_used": False,
            },
        )
        return effective_policy

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
    def _miniqmt_account_strategy_lot_quantities(*, repository: Any, account_id: str) -> dict[str, dict[str, int]]:
        quantities: dict[str, dict[str, int]] = {}
        for account in repository.list_virtual_accounts(account_id=account_id):
            by_symbol: dict[str, int] = {}
            for lot in repository.list_position_lots(account.strategy_id):
                remaining = int(getattr(lot, "remaining_quantity", getattr(lot, "quantity", 0)) or 0)
                if remaining <= 0:
                    continue
                symbol = str(lot.symbol)
                by_symbol[symbol] = by_symbol.get(symbol, 0) + remaining
            quantities[account.strategy_id] = dict(sorted(by_symbol.items()))
        return quantities

    def _load_miniqmt_broker_positions(
        self,
        qmt_client: Any,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> list[dict[str, Any]]:
        get_positions = getattr(qmt_client, "get_positions", None)
        if not callable(get_positions):
            raise DataUnavailableError(
                "MiniQMT production context requires broker get_positions",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            )
        try:
            return [dict(position) for position in get_positions()]
        except DataUnavailableError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load MiniQMT broker positions for strategy-lot reconciliation",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "broker_account_id": binding.broker_account_id,
                    "trade_date": trade_date.isoformat(),
                },
            ) from exc

    @staticmethod
    def _reconcile_miniqmt_positions_with_broker(
        ledger_positions: dict[str, PositionLot],
        broker_positions: list[dict[str, Any]],
        *,
        strategy_id: str,
        binding_id: str,
        trade_date: date,
        account_strategy_quantities: dict[str, dict[str, int]] | None = None,
    ) -> tuple[dict[str, PositionLot], dict[str, Any]]:
        broker_totals = _broker_position_totals(broker_positions)
        broker_quantities = {symbol: quantity for symbol, (quantity, _can_sell) in broker_totals.items()}
        strategy_quantities = account_strategy_quantities or {
            strategy_id: {symbol: int(position.quantity) for symbol, position in ledger_positions.items()}
        }
        projection = broker_authoritative_strategy_projection(
            strategy_lot_quantities=strategy_quantities,
            broker_quantities=broker_quantities,
        )
        projected_for_strategy = projection.projected_quantities.get(strategy_id, {})
        reconciled: dict[str, PositionLot] = {}
        dropped: list[dict[str, Any]] = []
        capped: list[dict[str, Any]] = []
        for symbol, position in sorted(ledger_positions.items()):
            broker_quantity, broker_can_sell = broker_totals.get(symbol, (0, 0))
            ledger_quantity = int(position.quantity)
            ledger_available = int(position.available_quantity)
            projected_quantity = int(projected_for_strategy.get(symbol, 0) or 0)
            if broker_quantity <= 0:
                dropped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reason": "missing_or_zero_broker_position",
                    }
                )
                continue
            capped_quantity = min(projected_quantity, broker_quantity)
            capped_available = min(ledger_available, broker_can_sell, capped_quantity)
            if capped_quantity <= 0:
                dropped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reason": "broker_quantity_cap_zero",
                    }
                )
                continue
            if capped_quantity != ledger_quantity or capped_available != ledger_available:
                capped.append(
                    {
                        "symbol": symbol,
                        "ledger_quantity": ledger_quantity,
                        "ledger_available_quantity": ledger_available,
                        "broker_quantity": broker_quantity,
                        "broker_can_sell": broker_can_sell,
                        "projected_quantity": projected_quantity,
                        "reconciled_quantity": capped_quantity,
                        "reconciled_available_quantity": capped_available,
                    }
                )
            reconciled[symbol] = position.model_copy(
                update={
                    "quantity": capped_quantity,
                    "available_quantity": capped_available,
                }
            )
        diagnostics = {
            "schema_version": "miniqmt_broker_position_reconciliation_v1",
            "status": "reconciled",
            "strategy_id": strategy_id,
            "binding_id": binding_id,
            "trade_date": trade_date.isoformat(),
            "ledger_position_count": len(ledger_positions),
            "broker_position_count": len(broker_totals),
            "reconciled_position_count": len(reconciled),
            "dropped_position_count": len(dropped),
            "capped_position_count": len(capped),
            "dropped_positions": dropped,
            "capped_positions": capped,
            "position_authority": "broker_positions",
            "broker_authoritative": True,
            "account_strategy_count": len(strategy_quantities),
            "projection_adjustments": list(projection.adjustments),
        }
        return reconciled, diagnostics

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

    def _resolve_local_sim_manifest(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> tuple[StrategyPackageManifest | None, dict[str, Any]]:
        """Resolve one immutable manifest identity for the complete LocalSIM context.

        Ordinary bindings remain pinned to the Paper v2 portfolio manifest.  The
        StrategyPackage repository is consulted only for an explicit unattended
        successor lineage whose predecessor identity is independently proven by
        the portfolio manifest and by the release/binding metadata written during
        the side-effect-free roll-forward.
        """

        try:
            self._validate_manifest_identity(
                manifest=portfolio_manifest,
                runtime_release=runtime_release,
                binding=binding,
            )
        except DataUnavailableError:
            binding_config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
            binding_metadata = (
                binding_config.get("metadata") if isinstance(binding_config.get("metadata"), dict) else {}
            )
            if binding_metadata.get("manifest_identity_source") != "strategy_package_current_manifest":
                raise
            return self._load_local_sim_successor_manifest(
                portfolio_manifest=portfolio_manifest,
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        return portfolio_manifest, {
            "schema_version": "localsim_manifest_identity_resolution_v1",
            "source": "paper_v2_portfolio_frozen_manifest",
            "package_id": portfolio_manifest.package_id if portfolio_manifest is not None else None,
            "manifest_sha256": portfolio_manifest.manifest_sha256 if portfolio_manifest is not None else None,
            "strategy_package_revalidation_performed": False,
        }

    def _load_local_sim_successor_manifest(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> tuple[StrategyPackageManifest, dict[str, Any]]:
        lineage = self._validate_local_sim_successor_manifest_lineage(
            portfolio_manifest=portfolio_manifest,
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        try:
            raw_manifest = self._package_manifest_loader(binding.package_id)
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load authoritative StrategyPackage manifest for LocalSim successor context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                    "extends_binding_id": lineage["extends_binding_id"],
                    "extends_release_id": lineage["extends_release_id"],
                },
            ) from exc

        if raw_manifest is None:
            raise DataUnavailableError(
                "LocalSim successor context requires the authoritative StrategyPackage manifest",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                    "extends_binding_id": lineage["extends_binding_id"],
                    "extends_release_id": lineage["extends_release_id"],
                },
            )
        try:
            manifest = (
                raw_manifest
                if isinstance(raw_manifest, StrategyPackageManifest)
                else StrategyPackageManifest.model_validate(raw_manifest)
            )
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "authoritative StrategyPackage manifest is invalid for LocalSim successor context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            ) from exc

        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
        )
        return manifest, {
            "schema_version": "localsim_manifest_identity_resolution_v1",
            "source": "strategy_package_current_manifest",
            "package_id": manifest.package_id,
            "manifest_sha256": manifest.manifest_sha256,
            "source_release_manifest_sha256": lineage["source_release_manifest_sha256"],
            "manifest_identity_changed": lineage["manifest_identity_changed"],
            "extends_binding_id": lineage["extends_binding_id"],
            "extends_release_id": lineage["extends_release_id"],
            "source_binding_readback_id": lineage["source_binding_readback_id"],
            "source_release_readback_id": lineage["source_release_readback_id"],
            "strategy_package_revalidation_performed": False,
        }

    def _validate_local_sim_successor_manifest_lineage(
        self,
        *,
        portfolio_manifest: StrategyPackageManifest | None,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> dict[str, Any]:
        binding_config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
        binding_metadata = binding_config.get("metadata") if isinstance(binding_config.get("metadata"), dict) else {}
        release_config = (
            runtime_release.release_config_json if isinstance(runtime_release.release_config_json, dict) else {}
        )
        release_metadata = release_config.get("metadata") if isinstance(release_config.get("metadata"), dict) else {}
        validation_evidence = (
            runtime_release.validation_evidence if isinstance(runtime_release.validation_evidence, dict) else {}
        )
        manifest_evidence = (
            validation_evidence.get("manifest_identity")
            if isinstance(validation_evidence.get("manifest_identity"), dict)
            else {}
        )
        expected_trade_date = trade_date.isoformat()
        expected_purpose = "localsim_unattended_daily_roll_forward"
        extends_release_id = str(binding_metadata.get("extends_release_id") or "").strip()
        extends_binding_id = str(binding_metadata.get("extends_binding_id") or "").strip()
        source_manifest_sha256 = str(binding_metadata.get("source_release_manifest_sha256") or "").strip()
        authoritative_manifest_sha256 = str(binding_metadata.get("authoritative_manifest_sha256") or "").strip()
        manifest_identity_changed = source_manifest_sha256 != authoritative_manifest_sha256

        violations: list[str] = []

        def require_equal(name: str, actual: Any, expected: Any) -> None:
            if actual != expected:
                violations.append(name)

        def require_bool_equal(name: str, actual: Any, expected: bool) -> None:
            if not isinstance(actual, bool) or actual is not expected:
                violations.append(name)

        require_equal("broker_backend", binding.broker_backend, SimulationBrokerBackend.LOCAL_SIM)
        require_equal("binding.release_id", binding.release_id, runtime_release.release_id)
        require_equal("binding.release_hash", binding.release_hash, runtime_release.release_hash)
        require_equal("binding.package_id", binding.package_id, runtime_release.package_id)
        require_equal("binding.manifest_sha256", binding.manifest_sha256, runtime_release.manifest_sha256)
        require_equal("binding.metadata.purpose", binding_metadata.get("purpose"), expected_purpose)
        require_equal("release.metadata.purpose", release_metadata.get("purpose"), expected_purpose)
        require_equal(
            "binding.metadata.broker_backend", binding_metadata.get("broker_backend"), binding.broker_backend.value
        )
        require_equal(
            "binding.metadata.target_trade_date", binding_metadata.get("target_trade_date"), expected_trade_date
        )
        require_equal(
            "release.metadata.target_trade_date", release_metadata.get("target_trade_date"), expected_trade_date
        )
        require_equal(
            "binding.metadata.manifest_identity_source",
            binding_metadata.get("manifest_identity_source"),
            "strategy_package_current_manifest",
        )
        require_equal(
            "release.metadata.manifest_identity_source",
            release_metadata.get("manifest_identity_source"),
            "strategy_package_current_manifest",
        )
        require_bool_equal(
            "binding.metadata.manifest_identity_changed",
            binding_metadata.get("manifest_identity_changed"),
            manifest_identity_changed,
        )
        require_bool_equal(
            "release.metadata.manifest_identity_changed",
            release_metadata.get("manifest_identity_changed"),
            manifest_identity_changed,
        )
        require_equal(
            "binding.metadata.new_release_id", binding_metadata.get("new_release_id"), runtime_release.release_id
        )
        require_equal("binding.metadata.extends_release_id", extends_release_id, runtime_release.base_release_id)
        require_equal(
            "release.metadata.extends_release_id", release_metadata.get("extends_release_id"), extends_release_id
        )
        require_equal(
            "release.metadata.extends_binding_id", release_metadata.get("extends_binding_id"), extends_binding_id
        )
        require_equal(
            "release.metadata.source_release_manifest_sha256",
            release_metadata.get("source_release_manifest_sha256"),
            source_manifest_sha256,
        )
        require_equal(
            "release.metadata.authoritative_manifest_sha256",
            release_metadata.get("authoritative_manifest_sha256"),
            authoritative_manifest_sha256,
        )
        require_equal(
            "binding.metadata.authoritative_manifest_sha256", authoritative_manifest_sha256, binding.manifest_sha256
        )
        require_equal(
            "validation_evidence.target_trade_date", validation_evidence.get("target_trade_date"), expected_trade_date
        )
        require_equal(
            "validation_evidence.extends_release_id", validation_evidence.get("extends_release_id"), extends_release_id
        )
        require_equal(
            "validation_evidence.extends_binding_id", validation_evidence.get("extends_binding_id"), extends_binding_id
        )
        require_equal(
            "validation_evidence.manifest_identity.source",
            manifest_evidence.get("source"),
            "strategy_package_current_manifest",
        )
        require_equal(
            "validation_evidence.manifest_identity.source_release_manifest_sha256",
            manifest_evidence.get("source_release_manifest_sha256"),
            source_manifest_sha256,
        )
        require_equal(
            "validation_evidence.manifest_identity.authoritative_manifest_sha256",
            manifest_evidence.get("authoritative_manifest_sha256"),
            authoritative_manifest_sha256,
        )
        require_bool_equal(
            "validation_evidence.manifest_identity.identity_changed",
            manifest_evidence.get("identity_changed"),
            manifest_identity_changed,
        )
        require_equal(
            "validation_evidence.manifest_identity.strategy_package_revalidation_performed",
            manifest_evidence.get("strategy_package_revalidation_performed"),
            False,
        )

        if not extends_release_id:
            violations.append("binding.metadata.extends_release_id.required")
        if not extends_binding_id or extends_binding_id == binding.binding_id:
            violations.append("binding.metadata.extends_binding_id.invalid")
        if not source_manifest_sha256:
            violations.append("binding.metadata.source_release_manifest_sha256.required")
        if portfolio_manifest is None:
            violations.append("portfolio.frozen_manifest.required")
        else:
            require_equal("portfolio.manifest.package_id", portfolio_manifest.package_id, binding.package_id)

        if violations:
            raise RuntimeConfigInvalidError(
                "LocalSim authoritative manifest successor lineage is invalid",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "binding_manifest_sha256": binding.manifest_sha256,
                    "portfolio_manifest_sha256": (
                        portfolio_manifest.manifest_sha256 if portfolio_manifest is not None else None
                    ),
                    "violations": sorted(set(violations)),
                },
            )

        source_readback = self._validate_local_sim_successor_source_records(
            runtime_release=runtime_release,
            binding=binding,
            extends_release_id=extends_release_id,
            extends_binding_id=extends_binding_id,
            source_manifest_sha256=source_manifest_sha256,
        )

        return {
            "extends_binding_id": extends_binding_id,
            "extends_release_id": extends_release_id,
            "source_release_manifest_sha256": source_manifest_sha256,
            "authoritative_manifest_sha256": authoritative_manifest_sha256,
            "manifest_identity_changed": manifest_identity_changed,
            "source_release_readback_id": source_readback["source_release_id"],
            "source_binding_readback_id": source_readback["source_binding_id"],
        }

    def _validate_local_sim_successor_source_records(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        extends_release_id: str,
        extends_binding_id: str,
        source_manifest_sha256: str,
    ) -> dict[str, str]:
        repository = self._runtime_repository or SimulationRuntimeRepository()
        try:
            source_release = repository.get_strategy_runtime_release(extends_release_id)
            source_binding = repository.get_simulation_release_binding(extends_binding_id)
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to read LocalSim successor source release and binding",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                },
            ) from exc

        if source_release is None or source_binding is None:
            raise DataUnavailableError(
                "LocalSim successor source release and binding are unavailable",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                    "source_release_found": source_release is not None,
                    "source_binding_found": source_binding is not None,
                },
            )

        violations: list[str] = []

        def require_equal(name: str, actual: Any, expected: Any) -> None:
            if actual != expected:
                violations.append(name)

        require_equal("source_release.release_id", source_release.release_id, extends_release_id)
        require_equal("source_binding.binding_id", source_binding.binding_id, extends_binding_id)
        require_equal("source_binding.release_id", source_binding.release_id, source_release.release_id)
        require_equal("source_binding.release_hash", source_binding.release_hash, source_release.release_hash)
        require_equal("source_release.package_id", source_release.package_id, runtime_release.package_id)
        require_equal("source_binding.package_id", source_binding.package_id, runtime_release.package_id)
        require_equal("source_release.manifest_sha256", source_release.manifest_sha256, source_manifest_sha256)
        require_equal("source_binding.manifest_sha256", source_binding.manifest_sha256, source_manifest_sha256)
        require_equal("source_binding.strategy_id", source_binding.strategy_id, binding.strategy_id)
        require_equal("source_binding.broker_backend", source_binding.broker_backend, SimulationBrokerBackend.LOCAL_SIM)
        require_equal(
            "source_binding.portfolio_id",
            self._resolve_local_sim_portfolio_id(source_binding),
            self._resolve_local_sim_portfolio_id(binding),
        )

        if violations:
            raise RuntimeConfigInvalidError(
                "LocalSim authoritative manifest successor source records are inconsistent",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "extends_binding_id": extends_binding_id,
                    "extends_release_id": extends_release_id,
                    "source_release_manifest_sha256": source_manifest_sha256,
                    "violations": sorted(set(violations)),
                },
            )

        return {
            "source_release_id": source_release.release_id,
            "source_binding_id": source_binding.binding_id,
        }

    def _load_strategy_package_manifest(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
    ) -> StrategyPackageManifest:
        try:
            raw_manifest = self._package_manifest_loader(binding.package_id)
        except (DataUnavailableError, RuntimeConfigInvalidError):
            raise
        except Exception as exc:  # noqa: BLE001
            raise DataUnavailableError(
                "failed to load StrategyPackage manifest for MiniQMT simulation context",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            ) from exc

        if raw_manifest is None:
            raise DataUnavailableError(
                "MiniQMT simulation context requires a frozen StrategyPackage manifest",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                    "package_id": binding.package_id,
                    "manifest_sha256": binding.manifest_sha256,
                },
            )
        manifest = (
            raw_manifest
            if isinstance(raw_manifest, StrategyPackageManifest)
            else StrategyPackageManifest.model_validate(raw_manifest)
        )
        self._validate_manifest_identity(
            manifest=manifest,
            runtime_release=runtime_release,
            binding=binding,
        )
        return manifest

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
        if (
            manifest.manifest_sha256 != binding.manifest_sha256
            or manifest.manifest_sha256 != runtime_release.manifest_sha256
        ):
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

    preview_only = True

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
            _preview_result_from_payload(item) for item in result_json.get("results", ()) if isinstance(item, dict)
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
            {item.preflight.strategy_id for item in result.results if getattr(item.preflight, "strategy_id", None)}
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


def _default_strategy_package_manifest_loader(package_id: str) -> StrategyPackageManifest:
    from backend.services.strategy_package.repository import StrategyPackageRepository

    return StrategyPackageRepository().get(package_id).current_manifest()


def _broker_position_totals(positions: list[dict[str, Any]] | tuple[dict[str, Any], ...]) -> dict[str, tuple[int, int]]:
    totals: dict[str, tuple[int, int]] = {}
    for position in positions:
        symbol = str(position.get("stock_code") or position.get("symbol") or "").strip()
        if not symbol:
            continue
        quantity = _safe_non_negative_int(position.get("quantity", position.get("volume", 0)))
        can_sell = _safe_non_negative_int(position.get("can_sell", position.get("can_use_volume", 0)))
        existing_quantity, existing_can_sell = totals.get(symbol, (0, 0))
        totals[symbol] = (existing_quantity + quantity, existing_can_sell + can_sell)
    return dict(sorted(totals.items()))


def _miniqmt_reconciliation_diagnostic_adjustment_symbols(diagnostics: Any) -> set[str]:
    if not isinstance(diagnostics, dict):
        return set()
    symbols: set[str] = set()
    for key in ("dropped_positions", "capped_positions"):
        rows = diagnostics.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip()
            if symbol:
                symbols.add(symbol)
    return symbols


def _safe_non_negative_int(value: Any) -> int:
    try:
        return max(int(value or 0), 0)
    except (TypeError, ValueError):
        return 0


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
    resolved_repository = repository or SimulationRuntimeRepository()
    mode = (os.getenv("SIMULATION_RUNTIME_CONTEXT_PROVIDER") or "").strip().lower()
    production_enabled = _env_flag("ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER", default=False)
    if mode in {"production", "prod"} or production_enabled:
        provider: SimulationRunContextProvider = ProductionSimulationRunContextProvider(
            runtime_repository=resolved_repository,
        )
    else:
        provider = FailFastSimulationRunContextProvider()
    quote_ingress_activation = (
        build_miniqmt_quote_ingress_activation_from_env()
        if _env_flag("MINIQMT_ENABLED", default=False)
        else None
    )
    return SimulationLifecycleScheduler(
        repository=resolved_repository,
        context_provider=provider,
        trading_calendar_service=TradingCalendarStatusService(),
        miniqmt_quote_ingress_activation=quote_ingress_activation,
    )


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
    lifecycle_diagnostic: dict[str, Any] | None = None
    data_source: str | None = None
    retry_claim_token: str | None = None
    retry_source_fingerprint: str | None = None

    @property
    def is_success(self) -> bool:
        return self.error is None


class _BindingRetryAttemptError(Exception):
    def __init__(self, *, original: Exception, claim_token: str, source_fingerprint: str) -> None:
        super().__init__(str(original))
        self.original = original
        self.claim_token = claim_token
        self.source_fingerprint = source_fingerprint


@dataclass(frozen=True)
class SimulationSchedulerRunOnceResult:
    trade_date: date
    data_source: str
    submit: bool
    total_bindings: int
    results: tuple[SimulationSchedulerBindingResult, ...]
    stale_run_results: tuple[dict[str, Any], ...] = field(default_factory=tuple)
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
                "RECONCILIATION_PENDING_OPEN_ORDERS",
                "TAIL_HANDLED",
                "RECONCILIATION_WARNING",
                "NO_REBALANCE",
            }
        )

    @property
    def failed_count(self) -> int:
        return sum(1 for item in self.results if item.error is not None)

    @property
    def stale_terminalized_count(self) -> int:
        return sum(
            1
            for item in self.stale_run_results
            if item.get("status") != "RECOVERY_BACKOFF" and item.get("terminalization_succeeded") is not False
        )

    @property
    def stale_recovery_failed_count(self) -> int:
        return sum(
            1
            for item in self.stale_run_results
            if item.get("status") != "RECOVERY_BACKOFF" and item.get("terminalization_succeeded") is False
        )

    @property
    def recovery_backoff_count(self) -> int:
        return sum(1 for item in self.stale_run_results if item.get("status") == "RECOVERY_BACKOFF")


@dataclass
class _SelectionInferenceInFlight:
    key: tuple[Any, ...]
    future: Future
    started_monotonic: float
    started_at: str
    context: dict[str, Any]
    timed_out: bool = False


@dataclass
class _BindingTickInFlight:
    key: tuple[str, date]
    result_queue: queue.Queue[tuple[str, Any]]
    thread: threading.Thread
    started_monotonic: float
    started_at: str
    context: dict[str, Any]
    timeout_seconds: float
    waiter_active: bool = True
    timed_out: bool = False


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
        trading_calendar_service: Any | None = None,
        miniqmt_quote_context_adapter: Any | None = None,
        b0_quote_v2_controller_factory: Any | None = None,
        miniqmt_quote_ingress_activation: Any | None = None,
        selection_inference_timeout_seconds: float | None = None,
        selection_inference_max_workers: int | None = None,
    ) -> None:
        activation_factory = (
            getattr(miniqmt_quote_ingress_activation, "controller_factory", None)
            if miniqmt_quote_ingress_activation is not None
            else None
        )
        if (
            b0_quote_v2_controller_factory is not None
            and activation_factory is not None
            and b0_quote_v2_controller_factory is not activation_factory
        ):
            raise ValueError("scheduler quote activation and explicit B0_QUOTE_V2 controller factories conflict")
        effective_b0_factory = b0_quote_v2_controller_factory or activation_factory
        activation_context_adapter = (
            getattr(miniqmt_quote_ingress_activation, "quote_context_adapter", None)
            if miniqmt_quote_ingress_activation is not None
            else None
        )
        if (
            miniqmt_quote_context_adapter is not None
            and activation_context_adapter is not None
            and miniqmt_quote_context_adapter is not activation_context_adapter
        ):
            raise ValueError("scheduler quote activation and explicit quote context adapters conflict")
        self.repository = repository or SimulationRuntimeRepository()
        self.selection_service = selection_service or StrategyPackageSelectionService(repository=self.repository)
        self.orchestrator = orchestrator or SimulationLifecycleOrchestrator(
            repository=self.repository,
            b0_quote_v2_controller_factory=effective_b0_factory,
        )
        if orchestrator is not None and effective_b0_factory is not None:
            existing_factory = getattr(orchestrator, "b0_quote_v2_controller_factory", None)
            if existing_factory is not None and existing_factory is not effective_b0_factory:
                raise ValueError("scheduler and orchestrator B0_QUOTE_V2 controller factories conflict")
            orchestrator.b0_quote_v2_controller_factory = effective_b0_factory
        self.context_provider = context_provider or FailFastSimulationRunContextProvider()
        self.performance_service = performance_service or StrategyPerformanceProjectionService()
        self._selection_inference_timeout_seconds = (
            float(selection_inference_timeout_seconds)
            if selection_inference_timeout_seconds is not None
            else self._selection_inference_timeout_seconds_from_env()
        )
        if self._selection_inference_timeout_seconds <= 0:
            raise ValueError("selection_inference_timeout_seconds must be positive")
        max_workers = (
            int(selection_inference_max_workers)
            if selection_inference_max_workers is not None
            else self._selection_inference_max_workers_from_env()
        )
        if max_workers <= 0:
            raise ValueError("selection_inference_max_workers must be positive")
        self._selection_inference_executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix="simulation-selection-inference",
        )
        self._selection_inference_lock = threading.RLock()
        self._selection_inference_inflight: dict[tuple[Any, ...], _SelectionInferenceInFlight] = {}
        self._selection_inference_shutdown = False
        self._binding_tick_lock = threading.RLock()
        self._binding_tick_inflight: dict[tuple[str, date], _BindingTickInFlight] = {}
        if trading_calendar_service is not None:
            self.trading_calendar_service = trading_calendar_service
        elif isinstance(self.repository, InMemorySimulationRuntimeRepository):
            self.trading_calendar_service = None
        else:
            self.trading_calendar_service = TradingCalendarStatusService()
        self._miniqmt_quote_context_adapter = miniqmt_quote_context_adapter or activation_context_adapter
        self._miniqmt_quote_ingress_activation = miniqmt_quote_ingress_activation
        self._b0_quote_v2_controller_factory = effective_b0_factory
        if getattr(self.orchestrator, "miniqmt_product_runtime_factory", None) is None:
            self.orchestrator.miniqmt_product_runtime_factory = self._build_miniqmt_kernel_product_runtime

    def _build_miniqmt_kernel_product_runtime(
        self,
        *,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        managed_order_service: QmtManagedOrderService | None,
        as_of_time: datetime | None,
    ) -> Any:
        """Build the sole KERNEL_V2 product root from live durable authorities."""

        from .miniqmt_kernel_product import build_simulation_miniqmt_product_runtime_v1

        adapter = (
            self._miniqmt_quote_ingress_activation.quote_context_adapter
            if self._miniqmt_quote_ingress_activation is not None
            else self._miniqmt_quote_context_adapter
        )
        return build_simulation_miniqmt_product_runtime_v1(
            simulation_repository=self.repository,
            execution_plan=plan,
            binding=binding,
            managed_order_service=managed_order_service,
            quote_context_adapter=adapter,
            quote_ingress_activation=self._miniqmt_quote_ingress_activation,
            observed_at=scheduler_time(as_of_time),
            broker_side_effects_enabled=(
                managed_order_service is not None
                and getattr(managed_order_service, "_broker", None) is not None
                and not bool(getattr(managed_order_service, "preview_only", False))
            ),
        )

    def status(self) -> dict[str, Any]:
        provider_status = _context_provider_status(self.context_provider)
        return {
            "ok": True,
            "scheduler": "simulation_lifecycle_scheduler",
            "autostart": False,
            "default_submit": False,
            "approval_states": [state.value for state in DEFAULT_SCHEDULER_SIM_BINDING_STATES],
            "sim_binding_selection_policy": "all_non_retired",
            "manual_tick_endpoint_enabled": False,
            "scheduler_control_api_enabled": False,
            "context_provider": provider_status,
            "context_provider_mode": provider_status.get("provider_mode"),
            "schedule_windows": list(DEFAULT_SCHEDULER_WINDOWS),
            "schedule_timezone": SCHEDULER_TZ_NAME,
            "restart_recovery_mode": "persisted_state_only",
            "window_orchestration": {
                "pre_open": "readiness",
                "selection": "daily_selection_evidence",
                "planning": "execution_plan",
                "execution": "submit_and_reconcile",
                "post_close_reconcile": "post_close_terminalization",
            },
            "miniqmt_sim_runtime": {
                "sim_runtime_kind": "KERNEL_V2",
                "runtime_selector_effect": "code_owned_kernel_v2_only",
                "legacy_product_route_retired": True,
                "live_forbidden": True,
            },
            "miniqmt_quote_context": self._miniqmt_quote_context_health(),
            "miniqmt_quote_ingress_activation": self._miniqmt_quote_ingress_activation_health(),
            "b0_quote_v2_controllers": (
                self._b0_quote_v2_controller_factory.health()
                if self._b0_quote_v2_controller_factory is not None
                else {"status": "DISABLED", "controller_count": 0}
            ),
            "binding_watchdog": {
                "timeout_env_var": SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
                "timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
                    DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS,
                ),
                "miniqmt_submit_timeout_env_var": SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                "miniqmt_submit_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS,
                ),
                "miniqmt_reconcile_timeout_env_var": SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                "miniqmt_reconcile_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS,
                ),
                "miniqmt_tick_driver_timeout_env_var": SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV,
                "miniqmt_tick_driver_timeout_seconds": self._timeout_seconds_from_env(
                    SIMULATION_MINIQMT_TICK_DRIVER_TIMEOUT_ENV,
                    DEFAULT_MINIQMT_TICK_DRIVER_TIMEOUT_SECONDS,
                ),
                **self._binding_tick_status(),
            },
            "selection_inference": self._selection_inference_status(),
        }

    def refresh_miniqmt_quote_context(self, **kwargs: Any) -> Any:
        """Scheduler-only P1-C context seam; it never changes run/submit state."""

        adapter = self._miniqmt_quote_context_adapter
        preload = getattr(adapter, "preload", None)
        if not callable(preload):
            raise DataUnavailableError(
                "MiniQMT quote context adapter is not configured for this scheduler",
                context={"reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID", "stage": "CLOCK"},
            )
        return preload(**kwargs)

    def _refresh_miniqmt_quote_context_lifecycle(self) -> None:
        """Refresh read-only quote context without changing lifecycle or submit state."""

        adapter = self._current_miniqmt_quote_context_adapter()
        refresh = getattr(adapter, "refresh_lifecycle", None)
        if not callable(refresh):
            return
        registered_runtime_count = getattr(adapter, "registered_runtime_count", None)
        if (
            self._miniqmt_quote_ingress_activation is not None
            and callable(registered_runtime_count)
            and registered_runtime_count() == 0
        ):
            return
        clock_at_utc = datetime.now(UTC)
        clock_monotonic_ns = monotonic_time.monotonic_ns()
        try:
            refresh(clock_at_utc=clock_at_utc, clock_monotonic_ns=clock_monotonic_ns)
        except QuoteContractError as exc:
            logger.error("MiniQMT quote context lifecycle refresh failed loudly: %s", exc.as_loud_payload())
        except Exception as exc:  # The adapter fault is loud but never rewrites run status.
            logger.error(
                "MiniQMT quote context lifecycle refresh raised unexpectedly: exception_type=%s",
                type(exc).__name__,
                exc_info=True,
            )

    def _miniqmt_quote_context_health(self) -> dict[str, Any]:
        adapter = self._current_miniqmt_quote_context_adapter()
        if adapter is None:
            return {
                "status": "UNCONFIGURED",
                "scope": "P1-C read-only context seam; no runtime submit gate",
            }
        health = getattr(adapter, "health", None)
        if not callable(health):
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "message": "configured MiniQMT quote context adapter has no read-only health method",
            }
        try:
            result = health()
        except Exception as exc:  # Health presentation cannot rewrite lifecycle state.
            logger.error(
                "MiniQMT quote context health lookup failed: exception_type=%s",
                type(exc).__name__,
                exc_info=True,
            )
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "exception_type": type(exc).__name__,
            }
        if not isinstance(result, dict):
            return {
                "status": "INVALID",
                "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                "stage": "CLOCK",
                "message": "configured MiniQMT quote context health is not a mapping",
            }
        return dict(result)

    def _current_miniqmt_quote_context_adapter(self) -> Any | None:
        activation_adapter = (
            getattr(self._miniqmt_quote_ingress_activation, "quote_context_adapter", None)
            if self._miniqmt_quote_ingress_activation is not None
            else None
        )
        return activation_adapter or self._miniqmt_quote_context_adapter

    def _prepare_miniqmt_quote_context_for_plan(
        self,
        *,
        binding: SimulationReleaseBinding,
        plan: ExecutionPlan,
        as_of_time: datetime | None,
        recovering_active: bool,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return None
        if plan.plan_payload_json.get("quote_control") is None:
            return None
        activation = self._miniqmt_quote_ingress_activation
        prepare = getattr(activation, "prepare_runtime_context", None)
        if not callable(prepare):
            raise RuntimeConfigInvalidError(
                "B0_QUOTE_V2 execution requires scheduler-owned authoritative context publication",
                context={
                    "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID",
                    "stage": "CLOCK",
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "broker_called": False,
                    "legacy_fallback": False,
                },
            )
        runtime_id = miniqmt_kernel_runtime_id(
            plan_id=plan.plan_id,
            binding_id=binding.binding_id,
            trade_date=plan.target_trade_date,
        )
        clock_at_utc = (
            as_of_time.astimezone(UTC)
            if isinstance(as_of_time, datetime) and as_of_time.tzinfo is not None
            else datetime.now(UTC)
        )
        return prepare(
            runtime_id=runtime_id,
            plan=plan,
            recovering_active=bool(recovering_active),
            clock_at_utc=clock_at_utc,
            clock_monotonic_ns=monotonic_time.monotonic_ns(),
        )

    def _miniqmt_quote_ingress_activation_health(self) -> dict[str, Any]:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return {
                "schema_version": "miniqmt_quote_ingress_activation_v1",
                "status": "UNCONFIGURED",
                "factory_available": False,
            }
        health = getattr(activation, "health", None)
        if not callable(health):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation has no health method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_HEALTH",
                },
            )
        payload = health()
        if not isinstance(payload, dict):
            raise RuntimeConfigInvalidError(
                "MiniQMT quote ingress activation health must be a mapping",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_HEALTH",
                },
            )
        return dict(payload)

    def _advance_miniqmt_quote_ingress_lifecycle(self) -> tuple[dict[str, Any], ...]:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return ()
        begin_epoch = getattr(activation, "begin_lifecycle_epoch", None)
        if not callable(begin_epoch):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation lacks its scheduler lifecycle method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_LIFECYCLE",
                },
            )
        begin_epoch()
        self._refresh_recovered_miniqmt_quote_activation_authorities()
        watchdog = getattr(activation, "watchdog_tick", None)
        if not callable(watchdog):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation lacks its watchdog method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_WATCHDOG",
                },
            )
        # The same scheduler-owned lifecycle tick that maintains the physical
        # B0 feed also pumps real QMT order/trade snapshots into each durable
        # KERNEL_V2 runtime.  The activation isolates every runtime and raises
        # one aggregate only after all registered runtimes were attempted.
        try:
            watchdog()
        except MiniQMTKernelProductSyncError as exc:
            failures = exc.context.get("ordered_failures")
            if not isinstance(failures, list) or any(
                not isinstance(item, dict)
                or not (
                    (
                        type(item.get("runtime_id")) is str
                        and bool(item["runtime_id"])
                        and item["runtime_id"] == item["runtime_id"].strip()
                        and type(item.get("binding_id")) is str
                        and bool(item["binding_id"])
                        and item["binding_id"] == item["binding_id"].strip()
                    )
                    or self._is_shared_kernel_product_failure(item)
                )
                for item in failures
            ):
                raise
            return tuple(dict(item) for item in failures)
        return ()

    def _refresh_recovered_miniqmt_quote_activation_authorities(self) -> None:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return
        recovered_factory = getattr(activation, "controller_factory", None)
        recovered_adapter = getattr(activation, "quote_context_adapter", None)
        if recovered_factory is not None:
            if (
                self._b0_quote_v2_controller_factory is not None
                and self._b0_quote_v2_controller_factory is not recovered_factory
            ):
                raise RuntimeConfigInvalidError(
                    "recovered MiniQMT quote activation conflicts with the scheduler controller factory",
                    context={
                        "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_AUTHORITY_CONFLICT",
                        "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_LIFECYCLE",
                    },
                )
            orchestrator_factory = getattr(self.orchestrator, "b0_quote_v2_controller_factory", None)
            if orchestrator_factory is not None and orchestrator_factory is not recovered_factory:
                raise RuntimeConfigInvalidError(
                    "recovered MiniQMT quote activation conflicts with the orchestrator controller factory",
                    context={
                        "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_AUTHORITY_CONFLICT",
                        "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_LIFECYCLE",
                    },
                )
            self._b0_quote_v2_controller_factory = recovered_factory
            self.orchestrator.b0_quote_v2_controller_factory = recovered_factory
        if recovered_adapter is not None:
            if (
                self._miniqmt_quote_context_adapter is not None
                and self._miniqmt_quote_context_adapter is not recovered_adapter
            ):
                raise RuntimeConfigInvalidError(
                    "recovered MiniQMT quote activation conflicts with the scheduler quote context adapter",
                    context={
                        "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_AUTHORITY_CONFLICT",
                        "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_LIFECYCLE",
                    },
                )
            self._miniqmt_quote_context_adapter = recovered_adapter

    @staticmethod
    def _is_shared_kernel_product_failure(failure: Mapping[str, Any]) -> bool:
        return (
            failure.get("runtime_id") is None
            and failure.get("binding_id") is None
            and (
                failure.get("operation") == "SUPERVISOR_WATCHDOG"
                or str(failure.get("reason_code") or "").startswith("MINIQMT_SHARED_QUOTE_SUPERVISOR_")
            )
        )

    @staticmethod
    def _bounded_kernel_product_failure_evidence(
        failures: tuple[dict[str, Any], ...],
    ) -> dict[str, Any]:
        normalized_all = simulation_retry_json_safe_evidence([dict(item) for item in failures])
        if not isinstance(normalized_all, list) or any(not isinstance(item, dict) for item in normalized_all):
            raise AssertionError("MiniQMT kernel-product failure evidence must normalize to ordered objects")
        bounded = normalized_all[:_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT]
        omitted = normalized_all[_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT:]
        all_failures_sha256 = canonical_json_sha256(
            {
                "schema_version": "miniqmt_kernel_product_failure_set_identity_v1",
                "ordered_failures": normalized_all,
            }
        )
        omitted_failures_sha256 = (
            canonical_json_sha256(
                {
                    "schema_version": "miniqmt_kernel_product_omitted_failure_identity_v1",
                    "ordered_failures": omitted,
                }
            )
            if omitted
            else None
        )
        return {
            "failure_count": len(normalized_all),
            "evidence_limit": _MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT,
            "truncated_failure_count": len(omitted),
            "all_failures_sha256": all_failures_sha256,
            "omitted_failures_sha256": omitted_failures_sha256,
            "ordered_failures": bounded,
        }

    @staticmethod
    def _unmatched_kernel_product_failure_result(
        *,
        failures: tuple[dict[str, Any], ...],
        data_source: str,
    ) -> SimulationSchedulerBindingResult | None:
        if not failures:
            return None
        bounded_evidence = SimulationLifecycleScheduler._bounded_kernel_product_failure_evidence(failures)
        failure_fingerprint = canonical_json_sha256(
            {
                "schema_version": "miniqmt_kernel_product_unmatched_failure_identity_v1",
                "failure_count": bounded_evidence["failure_count"],
                "all_failures_sha256": bounded_evidence["all_failures_sha256"],
                "omitted_failures_sha256": bounded_evidence["omitted_failures_sha256"],
            }
        )
        context = {
            "schema_version": "miniqmt_kernel_product_unmatched_failure_v1",
            "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_UNMATCHED",
            "stage": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK",
            **bounded_evidence,
            "failure_fingerprint": failure_fingerprint,
            "broker_side_effect_state": "UNKNOWN",
            "execution_gate": False,
            "peer_bindings_attempted": True,
        }
        return SimulationSchedulerBindingResult(
            binding_id="__miniqmt_kernel_product_unmatched__",
            strategy_id="__scheduler__",
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            status="MINIQMT_KERNEL_V2_UNMATCHED_FAILURE",
            error={
                "type": "MiniQMTKernelProductSyncError",
                "message": ("KERNEL_V2 callback or exchange-clock failure did not map to the current binding page"),
                "context": context,
            },
            lifecycle_diagnostic={
                **context,
                "alert": {
                    "severity": "ERROR",
                    "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_UNMATCHED",
                    "failure_count": len(failures),
                },
            },
            data_source=data_source,
        )

    def _kernel_product_attempt_authority(
        self,
        *,
        runtime_id: str,
        runtime: Any,
        failure: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, str | None]:
        failure_generation = failure.get("lifecycle_generation")
        failure_attempt_token = failure.get("attempt_token")
        if (
            type(failure_generation) is not int
            or failure_generation <= 0
            or type(failure_attempt_token) is not int
            or failure_attempt_token <= 0
        ):
            return None, "FAILURE_ATTEMPT_AUTHORITY_MISSING"

        activation = self._miniqmt_quote_ingress_activation
        health_reader = getattr(activation, "health", None)
        try:
            health = health_reader() if callable(health_reader) else {}
        except Exception:  # noqa: BLE001 - classify one owner readback without starving peer bindings.
            return None, "RUNTIME_ATTEMPT_AUTHORITY_READ_FAILED"
        runtime_rows = health.get("kernel_product_runtimes") if isinstance(health, Mapping) else None
        runtime_health = (
            next(
                (
                    dict(item)
                    for item in runtime_rows
                    if isinstance(item, Mapping) and item.get("runtime_id") == runtime_id
                ),
                None,
            )
            if isinstance(runtime_rows, list)
            else None
        )
        ingress_retry = (
            runtime_health.get("ingress_retry")
            if isinstance(runtime_health, dict) and isinstance(runtime_health.get("ingress_retry"), Mapping)
            else {}
        )
        current_generation = ingress_retry.get("lifecycle_generation")
        if type(current_generation) is not int:
            current_generation = getattr(runtime, "lifecycle_generation", None)
        if type(current_generation) is not int or current_generation != failure_generation:
            return None, "RUNTIME_LIFECYCLE_GENERATION_STALE"

        successor_attempts: list[Mapping[str, Any]] = []
        for collection_key in ("kernel_in_flight_attempts", "kernel_watchdog_workers"):
            collection = health.get(collection_key) if isinstance(health, Mapping) else None
            if isinstance(collection, list):
                successor_attempts.extend(item for item in collection if isinstance(item, Mapping))
        callback_workers = health.get("kernel_callback_workers") if isinstance(health, Mapping) else None
        if isinstance(callback_workers, list):
            successor_attempts.extend(
                {
                    "runtime_id": item.get("runtime_id"),
                    "lifecycle_generation": item.get("lifecycle_generation"),
                    "attempt_token": item.get("active_attempt_token"),
                }
                for item in callback_workers
                if isinstance(item, Mapping) and item.get("active_attempt_token") is not None
            )
        if any(
            item.get("runtime_id") == runtime_id
            and item.get("lifecycle_generation") == failure_generation
            and type(item.get("attempt_token")) is int
            and item.get("attempt_token") != failure_attempt_token
            for item in successor_attempts
        ):
            return None, "RUNTIME_ATTEMPT_SUCCESSOR_ACTIVE"

        attempt_candidates: list[Mapping[str, Any]] = []
        for candidate in (ingress_retry.get("active_failure"), ingress_retry.get("last_failure")):
            if isinstance(candidate, Mapping):
                attempt_candidates.append(candidate)
        operations = ingress_retry.get("operations")
        if isinstance(operations, Mapping):
            for operation in operations.values():
                if not isinstance(operation, Mapping):
                    continue
                for candidate in (operation.get("active_failure"), operation.get("last_failure")):
                    if isinstance(candidate, Mapping):
                        attempt_candidates.append(candidate)
        runtime_attempt_token = getattr(runtime, "attempt_token", None)
        attempt_matches = any(
            candidate.get("runtime_id") == runtime_id
            and candidate.get("lifecycle_generation") == failure_generation
            and candidate.get("attempt_token") == failure_attempt_token
            for candidate in attempt_candidates
        ) or (type(runtime_attempt_token) is int and runtime_attempt_token == failure_attempt_token)
        if not attempt_matches:
            return None, "RUNTIME_ATTEMPT_TOKEN_STALE"
        return (
            {
                "schema_version": "miniqmt_kernel_product_attempt_authority_v1",
                "runtime_id": runtime_id,
                "lifecycle_generation": failure_generation,
                "attempt_token": failure_attempt_token,
            },
            None,
        )

    @staticmethod
    def _kernel_carrier_field(carrier: Any, field_name: str) -> Any:
        if isinstance(carrier, Mapping):
            return carrier.get(field_name)
        return getattr(carrier, field_name, None)

    def _kernel_product_outbox_authority(
        self,
        *,
        runtime: Any,
        runtime_id: str,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
    ) -> dict[str, Any]:
        """Snapshot the exact K2 command chain without persisting command payloads."""

        repository = getattr(runtime, "repository", None)
        list_commands = getattr(repository, "list_recovery_outbox_commands", None)
        read_chain = getattr(repository, "read_command_identity_chain", None)
        base = {
            "schema_version": "miniqmt_kernel_product_outbox_authority_v1",
            "runtime_id": runtime_id,
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "broker_account_id": binding.broker_account_id,
            "trade_date": trade_date.isoformat(),
            "execution_plan_id": plan.plan_id,
            "execution_plan_hash": plan.plan_hash,
            "evidence_limit": _MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT,
        }
        if not callable(list_commands) or not callable(read_chain):
            unavailable = {
                **base,
                "complete": False,
                "inventory_state": "REPOSITORY_AUTHORITY_UNAVAILABLE",
                "command_count": 0,
                "retained_command_count": 0,
                "truncated_command_count_lower_bound": 0,
                "identity_conflicts": ["repository_authority_unavailable"],
                "commands": [],
                "replacement_safe": False,
                "ambiguous_command_count": 0,
                "confirmed_broker_side_effect_count": 0,
            }
            return {**unavailable, "authority_sha256": canonical_json_sha256(unavailable)}

        try:
            commands = tuple(
                list_commands(
                    runtime_id=runtime_id,
                    trade_date=trade_date,
                    statuses=tuple(status.value for status in BrokerCommandOutboxStatusV1),
                    limit=_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT + 1,
                )
            )
        except Exception as exc:  # noqa: BLE001 - retain a fail-closed bounded receipt.
            read_failed = {
                **base,
                "complete": False,
                "inventory_state": "INVENTORY_READ_FAILED",
                "command_count": 0,
                "retained_command_count": 0,
                "truncated_command_count_lower_bound": 0,
                "identity_conflicts": ["inventory_read_failed"],
                "read_error": {"type": type(exc).__name__, "message": str(exc)},
                "commands": [],
                "replacement_safe": False,
                "ambiguous_command_count": 0,
                "confirmed_broker_side_effect_count": 0,
            }
            return {**read_failed, "authority_sha256": canonical_json_sha256(read_failed)}

        rows: list[dict[str, Any]] = []
        conflicts: list[str] = []
        for command in commands:
            command_id = str(self._kernel_carrier_field(command, "command_id") or "").strip()
            if not command_id:
                conflicts.append("command_id_missing")
                continue
            try:
                chain = read_chain(command_id)
            except Exception as exc:  # noqa: BLE001 - one broken chain invalidates exact inventory authority.
                conflicts.append(f"command_chain_read_failed:{command_id}:{type(exc).__name__}")
                continue
            chain_outbox = chain.get("outbox") if isinstance(chain, Mapping) else None
            mapping = chain.get("mapping") if isinstance(chain, Mapping) else None
            status_value = self._kernel_carrier_field(command, "status")
            status = status_value.value if isinstance(status_value, Enum) else str(status_value or "")
            row = {
                "command_id": command_id,
                "runtime_id": str(self._kernel_carrier_field(command, "runtime_id") or "").strip(),
                "mapping_id": str(self._kernel_carrier_field(command, "mapping_id") or "").strip(),
                "parent_intent_id": str(self._kernel_carrier_field(command, "parent_intent_id") or "").strip(),
                "status": status,
                "broker_called": self._kernel_carrier_field(command, "broker_called"),
                "broker_order_id": self._kernel_carrier_field(command, "broker_order_id"),
                "deterministic_client_order_ref": self._kernel_carrier_field(mapping, "deterministic_client_order_ref"),
                "order_remark": self._kernel_carrier_field(mapping, "order_remark"),
            }
            chain_identity = {
                "outbox_command_id": self._kernel_carrier_field(chain_outbox, "command_id"),
                "mapping_runtime_id": self._kernel_carrier_field(mapping, "runtime_id"),
                "mapping_mapping_id": self._kernel_carrier_field(mapping, "mapping_id"),
                "mapping_parent_intent_id": self._kernel_carrier_field(mapping, "parent_intent_id"),
            }
            expected_chain_identity = {
                "outbox_command_id": command_id,
                "mapping_runtime_id": runtime_id,
                "mapping_mapping_id": row["mapping_id"],
                "mapping_parent_intent_id": row["parent_intent_id"],
            }
            if row["runtime_id"] != runtime_id:
                conflicts.append(f"command_runtime_conflict:{command_id}")
            if chain_identity != expected_chain_identity:
                conflicts.append(f"command_identity_chain_conflict:{command_id}")
            rows.append(row)

        rows.sort(key=lambda item: item["command_id"])
        normalized_rows = simulation_retry_json_safe_evidence(rows)
        if not isinstance(normalized_rows, list):
            raise AssertionError("MiniQMT outbox authority must normalize to a list")
        retained = normalized_rows[:_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT]
        inventory_exhaustive = len(commands) <= _MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT
        ambiguous_statuses = {
            BrokerCommandOutboxStatusV1.CLAIMED.value,
            BrokerCommandOutboxStatusV1.DISPATCHING.value,
            BrokerCommandOutboxStatusV1.FAILED_RETRYABLE.value,
            BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN.value,
            BrokerCommandOutboxStatusV1.RECONCILING.value,
        }
        outstanding_statuses = {BrokerCommandOutboxStatusV1.PENDING.value, *ambiguous_statuses}
        ambiguous_count = sum(
            1
            for row in rows
            if row["status"] in ambiguous_statuses or row["broker_called"] is None and row["status"] != "PENDING"
        )
        outstanding_count = sum(1 for row in rows if row["status"] in outstanding_statuses)
        confirmed_side_effect_count = sum(
            1 for row in rows if row["broker_called"] is True or bool(row["broker_order_id"])
        )
        safe_terminal_nonacceptance = all(
            row["broker_called"] is False and row["status"] == BrokerCommandOutboxStatusV1.FAILED_TERMINAL.value
            for row in rows
        )
        complete = inventory_exhaustive and not conflicts and len(rows) == len(commands)
        authority_payload = {
            **base,
            "complete": complete,
            "inventory_state": "COMPLETE" if complete else "INCOMPLETE",
            "command_count": len(rows),
            "retained_command_count": len(retained),
            "truncated_command_count_lower_bound": max(0, len(commands) - len(retained)),
            "identity_conflicts": conflicts[:_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT],
            "commands": retained,
            "commands_sha256": canonical_json_sha256(
                {
                    "schema_version": "miniqmt_kernel_product_outbox_command_set_v1",
                    "commands": normalized_rows,
                }
            ),
            "ambiguous_command_count": ambiguous_count,
            "outstanding_command_count": outstanding_count,
            "confirmed_broker_side_effect_count": confirmed_side_effect_count,
            "replacement_safe": bool(
                complete
                and ambiguous_count == 0
                and outstanding_count == 0
                and confirmed_side_effect_count == 0
                and (not rows or safe_terminal_nonacceptance)
            ),
        }
        return {
            **authority_payload,
            "authority_sha256": canonical_json_sha256(authority_payload),
        }

    def _partition_kernel_product_tick_failures(
        self,
        *,
        failures: tuple[dict[str, Any], ...],
        bindings: list[SimulationReleaseBinding],
        trade_date: date,
    ) -> tuple[dict[str, tuple[dict[str, Any], ...]], tuple[dict[str, Any], ...]]:
        current_by_id = {binding.binding_id: binding for binding in bindings}
        matched: dict[str, list[dict[str, Any]]] = {}
        unmatched: list[dict[str, Any]] = []
        outbox_authority_by_runtime: dict[str, dict[str, Any]] = {}
        get_runtime = getattr(self._miniqmt_quote_ingress_activation, "get_kernel_product_runtime", None)
        for raw_failure in failures:
            failure = dict(raw_failure)
            binding_id = failure.get("binding_id")
            runtime_id = failure.get("runtime_id")
            if self._is_shared_kernel_product_failure(failure):
                unmatched.append(
                    {
                        **failure,
                        "scheduler_match_state": "GLOBAL_SHARED_OWNER_FAILURE",
                        "scheduler_trade_date": trade_date.isoformat(),
                    }
                )
                continue
            assert type(binding_id) is str and type(runtime_id) is str
            binding = current_by_id.get(binding_id)
            match_state: str | None = None
            runtime: Any | None = None
            current_run: SimulationDailyRun | None = None
            current_plan: ExecutionPlan | None = None
            lookup_error: dict[str, str] | None = None
            expected_runtime_id: str | None = None
            if binding is None:
                match_state = "BINDING_NOT_IN_CURRENT_PAGE"
            else:
                current_run = self.repository.get_simulation_daily_run_by_key(
                    strategy_id=binding.strategy_id,
                    binding_id=binding.binding_id,
                    trade_date=trade_date,
                )
                if current_run is None:
                    match_state = "CURRENT_RUN_NOT_FOUND"
            if match_state is None and not callable(get_runtime):
                match_state = "RUNTIME_LOOKUP_UNAVAILABLE"
            elif match_state is None:
                try:
                    runtime = get_runtime(runtime_id)
                except Exception as exc:  # noqa: BLE001 - preserve peer isolation and surface bounded diagnostics.
                    match_state = "RUNTIME_LOOKUP_FAILED"
                    lookup_error = {"type": type(exc).__name__, "message": str(exc)}
                if match_state is None and runtime is None:
                    match_state = "RUNTIME_NOT_REGISTERED"
            if match_state is None:
                runtime_binding_id = str(getattr(runtime, "binding_id", "") or "").strip()
                runtime_trade_date = getattr(runtime, "trade_date", None)
                runtime_trade_date_text = (
                    runtime_trade_date.isoformat()
                    if isinstance(runtime_trade_date, date)
                    else str(runtime_trade_date or "").strip()
                )
                if runtime_binding_id != binding_id:
                    match_state = "RUNTIME_BINDING_OWNER_DRIFT"
                elif runtime_trade_date_text != trade_date.isoformat():
                    match_state = "RUNTIME_TRADE_DATE_STALE"
            if match_state is None:
                runtime_plan_id = str(getattr(runtime, "execution_plan_id", "") or "").strip()
                plan_id = str(current_run.execution_plan_id or runtime_plan_id).strip()
                if not plan_id:
                    match_state = "CURRENT_EXECUTION_PLAN_MISSING"
                else:
                    try:
                        current_plan = self.repository.get_execution_plan(plan_id)
                    except Exception as exc:  # noqa: BLE001 - preserve peer isolation and exact diagnostics.
                        match_state = "CURRENT_EXECUTION_PLAN_READBACK_FAILED"
                        lookup_error = {"type": type(exc).__name__, "message": str(exc)}
                    else:
                        if (
                            current_plan.binding_id != binding.binding_id
                            or current_plan.target_trade_date != trade_date
                            or (
                                current_run.execution_plan_id is not None
                                and current_plan.plan_id != current_run.execution_plan_id
                            )
                            or (
                                current_run.execution_plan_hash is not None
                                and current_plan.plan_hash != current_run.execution_plan_hash
                            )
                        ):
                            match_state = "CURRENT_EXECUTION_PLAN_OWNER_DRIFT"
                        else:
                            expected_runtime_id = miniqmt_kernel_runtime_id(
                                plan_id=current_plan.plan_id,
                                binding_id=binding.binding_id,
                                trade_date=trade_date,
                            )
                            if runtime_id != expected_runtime_id:
                                match_state = "RUNTIME_NOT_CURRENT_PLAN_OWNER"
                            elif runtime_plan_id and runtime_plan_id != current_plan.plan_id:
                                match_state = "RUNTIME_EXECUTION_PLAN_OWNER_DRIFT"
            attempt_authority: dict[str, Any] | None = None
            if match_state is None:
                attempt_authority, match_state = self._kernel_product_attempt_authority(
                    runtime_id=runtime_id,
                    runtime=runtime,
                    failure=failure,
                )
            if match_state is None:
                assert binding is not None
                assert current_run is not None
                assert current_plan is not None
                outbox_authority = outbox_authority_by_runtime.get(runtime_id)
                if outbox_authority is None:
                    outbox_authority = self._kernel_product_outbox_authority(
                        runtime=runtime,
                        runtime_id=runtime_id,
                        binding=binding,
                        run=current_run,
                        plan=current_plan,
                        trade_date=trade_date,
                    )
                    outbox_authority_by_runtime[runtime_id] = outbox_authority
                matched.setdefault(binding_id, []).append(
                    {
                        **failure,
                        "scheduler_runtime_authority": {
                            **dict(attempt_authority or {}),
                            "run_id": current_run.run_id,
                            "binding_id": binding_id,
                            "strategy_id": binding.strategy_id,
                            "broker_account_id": binding.broker_account_id,
                            "execution_plan_id": current_plan.plan_id,
                            "execution_plan_hash": current_plan.plan_hash,
                            "trade_date": trade_date.isoformat(),
                            "expected_runtime_id": expected_runtime_id,
                            "outbox_authority": outbox_authority,
                        },
                    }
                )
                continue
            unmatched.append(
                {
                    **failure,
                    "scheduler_match_state": match_state,
                    "scheduler_trade_date": trade_date.isoformat(),
                    **(
                        {"scheduler_expected_runtime_id": expected_runtime_id}
                        if expected_runtime_id is not None
                        else {}
                    ),
                    **({"runtime_lookup_error": lookup_error} if lookup_error is not None else {}),
                }
            )
        return (
            {binding_id: tuple(items) for binding_id, items in matched.items()},
            tuple(unmatched),
        )

    def shutdown_miniqmt_quote_ingress(self) -> None:
        activation = self._miniqmt_quote_ingress_activation
        if activation is None:
            return
        shutdown = getattr(activation, "shutdown", None)
        if not callable(shutdown):
            raise RuntimeConfigInvalidError(
                "configured MiniQMT quote ingress activation has no shutdown method",
                context={
                    "reason_code": "MINIQMT_QUOTE_INGRESS_ACTIVATION_INVALID",
                    "stage": "MINIQMT_QUOTE_INGRESS_ACTIVATION_SHUTDOWN",
                },
            )
        shutdown()

    def shutdown_selection_inference(self, *, wait: bool = True) -> None:
        self._selection_inference_executor.shutdown(wait=wait, cancel_futures=not wait)
        with self._selection_inference_lock:
            self._selection_inference_shutdown = True

    def _selection_inference_status(self) -> dict[str, Any]:
        now = monotonic_time.monotonic()
        with self._selection_inference_lock:
            shutdown = self._selection_inference_shutdown
            in_flight = []
            for entry in self._selection_inference_inflight.values():
                in_flight.append(
                    {
                        **entry.context,
                        "started_at": entry.started_at,
                        "elapsed_seconds": round(max(0.0, now - entry.started_monotonic), 3),
                        "timeout_seconds": self._selection_inference_timeout_seconds,
                        "timed_out": entry.timed_out,
                        "done": entry.future.done(),
                    }
                )
        return {
            "mode": "artifact_hit_sync_else_background",
            "timeout_env_var": SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV,
            "max_workers_env_var": SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV,
            "timeout_seconds": self._selection_inference_timeout_seconds,
            "shutdown": shutdown,
            "in_flight_count": len(in_flight),
            "in_flight": in_flight,
        }

    @staticmethod
    def _selection_inference_timeout_seconds_from_env() -> float:
        raw = (os.getenv(SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV) or "300").strip()
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(f"{SIMULATION_SELECTION_INFERENCE_TIMEOUT_ENV} must be a number") from exc

    @staticmethod
    def _selection_inference_max_workers_from_env() -> int:
        raw = (os.getenv(SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV) or "2").strip()
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(f"{SIMULATION_SELECTION_INFERENCE_MAX_WORKERS_ENV} must be an integer") from exc

    @staticmethod
    def _timeout_seconds_from_env(env_var: str, default_seconds: float) -> float:
        raw = str(os.getenv(env_var) or "").strip()
        if not raw:
            return float(default_seconds)
        try:
            value = float(raw)
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "simulation runtime timeout configuration must be numeric",
                context={
                    "reason_code": "SIMULATION_STAGE_TIMEOUT_CONFIG_INVALID",
                    "stage": "TIMEOUT_CONFIGURATION",
                    "env_var": env_var,
                    "raw_value": raw,
                },
            ) from exc
        if value <= 0:
            raise RuntimeConfigInvalidError(
                "simulation runtime timeout configuration must be positive",
                context={
                    "reason_code": "SIMULATION_STAGE_TIMEOUT_CONFIG_INVALID",
                    "stage": "TIMEOUT_CONFIGURATION",
                    "env_var": env_var,
                    "raw_value": raw,
                    "parsed_value": value,
                },
            )
        return value

    def _run_callable_with_timeout(
        self,
        *,
        stage: str,
        reason_code: str,
        timeout_env_var: str,
        default_timeout_seconds: float,
        context: dict[str, Any],
        func: Callable[[], Any],
    ) -> Any:
        timeout_seconds = self._timeout_seconds_from_env(timeout_env_var, default_timeout_seconds)
        result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

        def target() -> None:
            try:
                result_queue.put(("result", func()))
            except BaseException as exc:  # noqa: BLE001 - propagate worker failures through the scheduler path.
                result_queue.put(("exception", exc))

        thread = threading.Thread(
            target=target,
            name=f"simulation-stage-{stage.lower().replace('_', '-')}",
            daemon=True,
        )
        thread.start()
        try:
            outcome, value = result_queue.get(timeout=timeout_seconds)
        except queue.Empty as exc:
            timeout_context = {
                **context,
                "reason_code": reason_code,
                "stage": stage,
                "failure_stage": stage,
                "timeout_env_var": timeout_env_var,
                "timeout_seconds": timeout_seconds,
                "thread_alive": thread.is_alive(),
            }
            raise RuntimeConfigInvalidError(
                f"simulation runtime stage timed out; reason_code={reason_code}, stage={stage}",
                context=timeout_context,
            ) from exc
        if outcome == "exception":
            raise value
        return value

    def run_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        release_id: str | None = None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None = DEFAULT_SCHEDULER_SIM_BINDING_STATES,
        submit: bool = False,
        mode: str = "SIM",
        as_of_time: datetime | None = None,
        created_by: str = "simulation_lifecycle_scheduler",
        raise_on_error: bool = False,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._ensure_lifecycle_trading_day(trade_date=trade_date)
        as_of_time = self._scheduler_time(as_of_time)
        normalized_backend = self._normalized_backend(broker_backend) if broker_backend is not None else None
        miniqmt_in_scope = normalized_backend in {None, SimulationBrokerBackend.MINIQMT_SIM}
        if miniqmt_in_scope:
            self._refresh_miniqmt_quote_context_lifecycle()
            kernel_product_tick_failures = self._advance_miniqmt_quote_ingress_lifecycle()
        else:
            kernel_product_tick_failures = []
        stale_run_results = self._run_recovery_stage_isolated(
            stage="STALE_MINIQMT_TERMINALIZATION",
            raise_on_error=raise_on_error,
            func=lambda: self._terminalize_stale_miniqmt_active_runs(
                trade_date=trade_date,
                broker_backend=broker_backend,
                strategy_id=strategy_id,
                limit=limit,
                as_of_time=as_of_time,
                raise_on_error=raise_on_error,
            ),
        )
        stale_run_results.extend(
            self._run_recovery_stage_isolated(
                stage="STALE_LOCALSIM_TERMINALIZATION",
                raise_on_error=raise_on_error,
                func=lambda: self._terminalize_stale_localsim_active_runs(
                    trade_date=trade_date,
                    broker_backend=broker_backend,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                    raise_on_error=raise_on_error,
                ),
            )
        )
        stale_run_results.extend(
            self._run_recovery_stage_isolated(
                stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
                raise_on_error=raise_on_error,
                func=lambda: self._terminalize_stale_localsim_failed_runs(
                    trade_date=trade_date,
                    broker_backend=broker_backend,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                    raise_on_error=raise_on_error,
                ),
            )
        )
        eod_terminalized_results = self._run_recovery_stage_isolated(
            stage="POST_CLOSE_MINIQMT_TERMINALIZATION",
            raise_on_error=raise_on_error,
            func=lambda: self._terminalize_post_close_miniqmt_runs(
                trade_date=trade_date,
                broker_backend=broker_backend,
                strategy_id=strategy_id,
                limit=limit,
                as_of_time=as_of_time,
                raise_on_error=raise_on_error,
            ),
        )
        eod_terminalized_results.extend(
            self._run_recovery_stage_isolated(
                stage="POST_CLOSE_LOCALSIM_TERMINALIZATION",
                raise_on_error=raise_on_error,
                func=lambda: self._terminalize_post_close_localsim_runs(
                    trade_date=trade_date,
                    broker_backend=broker_backend,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                    raise_on_error=raise_on_error,
                ),
            )
        )
        bindings = self.repository.list_simulation_release_bindings(
            strategy_id=strategy_id,
            release_id=release_id,
            broker_backend=broker_backend,
            approval_states=approval_states,
            active_on=trade_date,
            limit=limit,
        )
        package_status_cache: dict[str, PackageStatus] = {}
        bindings, lifecycle_skips, blocked_binding_keys = self._partition_retired_package_bindings(
            bindings=bindings,
            data_source=data_source,
            package_status_cache=package_status_cache,
        )
        bindings = self._with_unattended_roll_forward_bindings(
            bindings=bindings,
            trade_date=trade_date,
            limit=limit,
            broker_backend=broker_backend,
            strategy_id=strategy_id,
            release_id=release_id,
            approval_states=approval_states,
            data_source=data_source,
            package_status_cache=package_status_cache,
            lifecycle_skips=lifecycle_skips,
            blocked_binding_keys=blocked_binding_keys,
            created_by=created_by,
            raise_on_error=raise_on_error,
        )
        kernel_product_failures_by_binding, unmatched_kernel_product_tick_failures = (
            self._partition_kernel_product_tick_failures(
                failures=kernel_product_tick_failures,
                bindings=bindings,
                trade_date=trade_date,
            )
        )
        results: list[SimulationSchedulerBindingResult] = list(lifecycle_skips)
        eod_terminalized_run_ids = {
            str(item.get("run_id"))
            for item in eod_terminalized_results
            if item.get("run_id") and item.get("terminalization_succeeded") is not False
        }
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] = {}
        shared_selection_keys = self._shared_selection_cache_keys(
            bindings=bindings,
            trade_date=trade_date,
            data_source=data_source,
        )
        for binding in bindings:
            eod_result = self._post_close_terminalized_binding_result(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                run_ids=eod_terminalized_run_ids,
            )
            if eod_result is not None:
                results.append(eod_result)
                continue
            try:
                binding_tick_failures = kernel_product_failures_by_binding.get(binding.binding_id, ())
                if binding_tick_failures:
                    bounded_failure_evidence = self._bounded_kernel_product_failure_evidence(binding_tick_failures)
                    raise RuntimeConfigInvalidError(
                        "KERNEL_V2 callback or exchange-clock ingress failed for this MiniQMT binding",
                        context={
                            "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED",
                            "stage": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK",
                            "binding_id": binding.binding_id,
                            **bounded_failure_evidence,
                            "broker_side_effect_state": "UNKNOWN",
                            "execution_gate": False,
                        },
                    )
                binding_result = self._run_binding_with_watchdog(
                    binding=binding,
                    trade_date=trade_date,
                    data_source=data_source,
                    submit=submit,
                    mode=mode,
                    created_by=created_by,
                    selection_cache=selection_cache,
                    shared_selection_keys=shared_selection_keys,
                    as_of_time=as_of_time,
                )
                results.append(self._finalize_binding_retry_result(result=binding_result, as_of_time=as_of_time))
            except Exception as exc:  # noqa: BLE001 - isolate one binding without starving later eligible bindings.
                if raise_on_error:
                    raise
                retry_claim_token = exc.claim_token if isinstance(exc, _BindingRetryAttemptError) else None
                retry_source_fingerprint = (
                    exc.source_fingerprint if isinstance(exc, _BindingRetryAttemptError) else None
                )
                original_exc = exc.original if isinstance(exc, _BindingRetryAttemptError) else exc
                results.append(
                    self._record_pre_run_binding_failure_result(
                        binding=binding,
                        trade_date=trade_date,
                        data_source=data_source,
                        created_by=created_by,
                        exc=original_exc,
                        as_of_time=as_of_time,
                        retry_claim_token=retry_claim_token,
                        retry_source_fingerprint=retry_source_fingerprint,
                    )
                )
        unmatched_failure_result = self._unmatched_kernel_product_failure_result(
            failures=unmatched_kernel_product_tick_failures,
            data_source=data_source,
        )
        if unmatched_failure_result is not None:
            results.append(unmatched_failure_result)
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            total_bindings=len(bindings) + len(lifecycle_skips),
            results=tuple(results),
            stale_run_results=tuple([*stale_run_results, *eod_terminalized_results]),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    @staticmethod
    def _run_recovery_stage_isolated(
        *,
        stage: str,
        raise_on_error: bool,
        func: Callable[[], list[dict[str, Any]]],
    ) -> list[dict[str, Any]]:
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - recovery isolation is explicit and diagnostic.
            if raise_on_error:
                raise
            diagnostic = SimulationLifecycleScheduler._recovery_failure_diagnostic(stage=stage, exc=exc)
            logger.error(
                "Simulation scheduler recovery stage failed without starving bindings: %s", diagnostic, exc_info=True
            )
            return [diagnostic]

    def _run_recovery_item_isolated(
        self,
        *,
        stage: str,
        run: SimulationDailyRun,
        raise_on_error: bool,
        func: Callable[[], dict[str, Any] | None],
        as_of_time: datetime | None,
    ) -> dict[str, Any] | None:
        retry_key = f"{_SIMULATION_RECOVERY_RETRY_KEY_PREFIX}{stage}"
        source_fingerprint = self._simulation_retry_source_fingerprint(run=run, retry_key=retry_key)
        try:
            retry_decision = inspect_simulation_retry_backoff(
                run=run,
                retry_key=retry_key,
                source_fingerprint=source_fingerprint,
                as_of_time=self._scheduler_time(as_of_time),
                lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
            )
            if retry_decision is None:
                retry_decision = self.repository.claim_simulation_retry_attempt(
                    run_id=run.run_id,
                    retry_key=retry_key,
                    source_fingerprint=source_fingerprint,
                    as_of_time=self._scheduler_time(as_of_time),
                    lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
                )
        except Exception as exc:  # noqa: BLE001 - corrupt retry authority must stay isolated to its run.
            if raise_on_error:
                raise
            diagnostic = SimulationLifecycleScheduler._recovery_failure_diagnostic(
                stage=f"{stage}:RETRY_CONTROL_CLAIM",
                exc=exc,
                run=run,
            )
            diagnostic["retry_control_claim_failed"] = True
            diagnostic["retry_key"] = retry_key
            logger.error(
                "Simulation scheduler retry-control claim failed without starving peer runs: %s",
                diagnostic,
                exc_info=True,
            )
            return diagnostic
        if not retry_decision.should_execute:
            return self._recovery_backoff_diagnostic(
                stage=stage,
                run=retry_decision.run,
                reason=retry_decision.reason,
                retry_entry=retry_decision.retry_entry,
            )
        try:
            result = func()
            if retry_decision.claim_token is not None:
                self.repository.clear_simulation_retry_control(
                    run_id=run.run_id,
                    retry_key=retry_key,
                    expected_claim_token=retry_decision.claim_token,
                )
            return result
        except Exception as exc:  # noqa: BLE001 - one bad durable run must not starve other runs or bindings.
            if raise_on_error:
                raise
            failure_observed_at = max(
                self._scheduler_time(as_of_time),
                self._scheduler_now(),
            )
            failed_run = self._record_simulation_retry_failure(
                run=self.repository.get_simulation_daily_run(run.run_id),
                retry_key=retry_key,
                failure_stage=stage,
                exc=exc,
                as_of_time=failure_observed_at,
                expected_claim_token=retry_decision.claim_token,
                source_fingerprint=source_fingerprint,
            )
            diagnostic = SimulationLifecycleScheduler._recovery_failure_diagnostic(stage=stage, exc=exc, run=failed_run)
            retry_control = failed_run.run_payload_json.get(SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY)
            if isinstance(retry_control, dict):
                retry_entry = retry_control.get("entries", {}).get(retry_key)
                if isinstance(retry_entry, dict):
                    diagnostic["retry_control"] = deepcopy(retry_entry)
            logger.error(
                "Simulation scheduler recovery item failed without starving peers: %s", diagnostic, exc_info=True
            )
            return diagnostic

    @staticmethod
    def _simulation_retry_source_fingerprint(*, run: SimulationDailyRun, retry_key: str) -> str:
        payload = dict(run.run_payload_json or {})
        payload.pop(SIMULATION_SCHEDULER_RETRY_CONTROL_PAYLOAD_KEY, None)
        payload.pop(SIMULATION_SCHEDULER_RETRY_CLAIMS_PAYLOAD_KEY, None)
        if retry_key == _SIMULATION_BINDING_RETRY_KEY:
            payload = {
                key: deepcopy(payload.get(key))
                for key in (
                    "submit_failure",
                    "pre_run_failure",
                    "broker_called",
                    "submitted_intents",
                    "failed_intents",
                    "qmt_batch_id",
                    "qmt_batch_status",
                    "qmt_batch_result",
                    "pre_trade_blocked_order_generation",
                )
                if key in payload
            }
        elif retry_key.startswith(_SIMULATION_RECOVERY_RETRY_KEY_PREFIX):
            # Recovery attempts may durably advance evidence before failing. Those writes
            # describe progress for the same frozen run authority and must not make the
            # next scheduler tick look like a new source. Keep only the bounded preflight
            # carriers whose repair must bypass backoff; deep execution/economic evidence
            # remains excluded. Frozen run/binding/release/plan identity is hashed below.
            payload = {
                key: deepcopy(payload.get(key))
                for key in (
                    "local_sim_projection_outbox_v1",
                    "local_sim_projection_readback_failure",
                    "local_sim_projection_terminal_failure",
                    "local_sim_projection_readback_terminal_failure",
                    "local_sim_persistence",
                )
                if key in payload
            }
        return canonical_json_sha256(
            {
                "schema_version": "simulation_scheduler_retry_source_v1",
                "retry_key": retry_key,
                "run_id": run.run_id,
                "trade_date": run.trade_date.isoformat(),
                "strategy_id": run.strategy_id,
                "binding_id": run.binding_id,
                "binding_hash": run.binding_hash,
                "release_id": run.release_id,
                "release_hash": run.release_hash,
                "execution_plan_id": run.execution_plan_id,
                "execution_plan_hash": run.execution_plan_hash,
                "status": run.status.value,
                "run_payload_json": payload,
            }
        )

    @staticmethod
    def _simulation_retry_error_evidence(*, exc: BaseException, failure_stage: str) -> dict[str, Any]:
        context = getattr(exc, "context", None)
        reason_code = context.get("reason_code") if isinstance(context, dict) else None
        normalized = simulation_retry_json_safe_evidence(
            {
                "type": type(exc).__name__,
                "message": str(exc),
                "reason_code": str(reason_code) if reason_code is not None else None,
                "context": deepcopy(context) if isinstance(context, dict) else context,
                "failure_stage": failure_stage,
            }
        )
        if not isinstance(normalized, dict):
            raise AssertionError("simulation retry error evidence normalization must return an object")
        return normalized

    def _record_simulation_retry_failure(
        self,
        *,
        run: SimulationDailyRun,
        retry_key: str,
        failure_stage: str,
        exc: BaseException,
        as_of_time: datetime | None,
        expected_claim_token: str | None = None,
        source_fingerprint: str | None = None,
    ) -> SimulationDailyRun:
        error = self._simulation_retry_error_evidence(exc=exc, failure_stage=failure_stage)
        return self._record_simulation_retry_failure_evidence(
            run=run,
            retry_key=retry_key,
            failure_stage=failure_stage,
            error=error,
            as_of_time=as_of_time,
            expected_claim_token=expected_claim_token,
            source_fingerprint=source_fingerprint,
        )

    def _record_simulation_retry_failure_evidence(
        self,
        *,
        run: SimulationDailyRun,
        retry_key: str,
        failure_stage: str,
        error: dict[str, Any],
        as_of_time: datetime | None,
        expected_claim_token: str | None = None,
        source_fingerprint: str | None = None,
    ) -> SimulationDailyRun:
        normalized_error = simulation_retry_json_safe_evidence(error)
        if not isinstance(normalized_error, dict):
            raise InvalidStateTransitionError(
                "simulation retry failure evidence must be an object",
                context={"reason_code": "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"},
            )
        reason_code = normalized_error.get("reason_code")
        failure_fingerprint = canonical_json_sha256(
            {
                "schema_version": "simulation_scheduler_retry_failure_identity_v2",
                "retry_key": retry_key,
                "failure_stage": failure_stage,
                "error_identity": {
                    "type": normalized_error.get("type"),
                    "reason_code": reason_code,
                    # Typed failures use reason_code as their economic identity. Keep
                    # message identity only for untyped failures while persisting the
                    # complete latest error below for diagnostics.
                    "message": normalized_error.get("message") if reason_code is None else None,
                },
            }
        )
        return self.repository.record_simulation_retry_failure(
            run_id=run.run_id,
            retry_key=retry_key,
            source_fingerprint=source_fingerprint
            or self._simulation_retry_source_fingerprint(run=run, retry_key=retry_key),
            failure_fingerprint=failure_fingerprint,
            failure_stage=failure_stage,
            error=normalized_error,
            as_of_time=self._scheduler_time(as_of_time),
            base_delay_seconds=_SIMULATION_RETRY_BASE_DELAY_SECONDS,
            max_delay_seconds=_SIMULATION_RETRY_MAX_DELAY_SECONDS,
            expected_claim_token=expected_claim_token,
        )

    @staticmethod
    def _recovery_backoff_diagnostic(
        *,
        stage: str,
        run: SimulationDailyRun,
        reason: str,
        retry_entry: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return {
            "schema_version": "simulation_scheduler_recovery_backoff_v1",
            "terminalization_succeeded": False,
            "status": "RECOVERY_BACKOFF",
            "stage": stage,
            "reason_code": "SIMULATION_SCHEDULER_RECOVERY_BACKOFF_NOT_DUE",
            "retry_reason": reason,
            "run_id": run.run_id,
            "trade_date": run.trade_date.isoformat(),
            "strategy_id": run.strategy_id,
            "broker_backend": run.broker_backend.value,
            "retry_control": deepcopy(retry_entry),
            "alert": {
                "severity": "WARNING",
                "reason_code": "SIMULATION_SCHEDULER_RECOVERY_BACKOFF_NOT_DUE",
                "stage": stage,
                "auto_retry": True,
                "next_retry_at": (
                    retry_entry.get("next_retry_at") or retry_entry.get("lease_until")
                    if isinstance(retry_entry, dict)
                    else None
                ),
            },
        }

    @staticmethod
    def _recovery_failure_diagnostic(
        *,
        stage: str,
        exc: Exception,
        run: SimulationDailyRun | None = None,
    ) -> dict[str, Any]:
        context = getattr(exc, "context", None)
        reason_code = (
            "SIMULATION_SCHEDULER_RECOVERY_ITEM_FAILED"
            if run is not None
            else "SIMULATION_SCHEDULER_RECOVERY_STAGE_FAILED"
        )
        diagnostic = {
            "schema_version": "simulation_scheduler_recovery_failure_v1",
            "terminalization_succeeded": False,
            "status": "RECOVERY_FAILED",
            "stage": stage,
            "reason_code": reason_code,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": dict(context) if isinstance(context, dict) else context,
            },
            "alert": {
                "severity": "ERROR",
                "reason_code": reason_code,
                "stage": stage,
                "message": str(exc),
            },
        }
        if run is not None:
            diagnostic.update(
                {
                    "run_id": run.run_id,
                    "trade_date": run.trade_date.isoformat(),
                    "strategy_id": run.strategy_id,
                    "broker_backend": run.broker_backend.value,
                }
            )
        return diagnostic

    def _run_binding_with_watchdog(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        context = {
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "broker_backend": binding.broker_backend.value,
            "package_id": binding.package_id,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "submit": bool(submit),
            "mode": str(mode or "SIM").strip().upper(),
        }

        def func() -> SimulationSchedulerBindingResult:
            return self._run_binding(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
                created_by=created_by,
                selection_cache=selection_cache,
                shared_selection_keys=shared_selection_keys,
                as_of_time=as_of_time,
            )

        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return self._run_local_sim_binding_single_flight(
                binding=binding,
                trade_date=trade_date,
                context=context,
                func=func,
            )
        return self._run_callable_with_timeout(
            stage="BINDING_TICK",
            reason_code="SIMULATION_BINDING_STAGE_TIMEOUT",
            timeout_env_var=SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
            default_timeout_seconds=DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS,
            context=context,
            func=func,
        )

    def _run_local_sim_binding_single_flight(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        context: dict[str, Any],
        func: Callable[[], SimulationSchedulerBindingResult],
    ) -> SimulationSchedulerBindingResult:
        key = (binding.binding_id, trade_date)
        timeout_seconds = self._timeout_seconds_from_env(
            SIMULATION_BINDING_WATCHDOG_TIMEOUT_ENV,
            DEFAULT_SIMULATION_BINDING_WATCHDOG_TIMEOUT_SECONDS,
        )
        with self._binding_tick_lock:
            entry = self._binding_tick_inflight.get(key)
            if entry is not None:
                if entry.waiter_active:
                    raise self._binding_tick_in_progress_error(entry)
                return self._consume_or_report_binding_tick(entry)
            result_queue: queue.Queue[tuple[str, Any]] = queue.Queue(maxsize=1)

            def target() -> None:
                try:
                    result_queue.put(("result", func()))
                except BaseException as exc:  # noqa: BLE001 - preserve the exact worker failure for its owner.
                    result_queue.put(("exception", exc))

            thread = threading.Thread(
                target=target,
                name=f"localsim-binding-{binding.binding_id[:32]}",
                daemon=True,
            )
            entry = _BindingTickInFlight(
                key=key,
                result_queue=result_queue,
                thread=thread,
                started_monotonic=monotonic_time.monotonic(),
                started_at=datetime.now(UTC).isoformat(),
                context=deepcopy(context),
                timeout_seconds=timeout_seconds,
            )
            self._binding_tick_inflight[key] = entry
            thread.start()
        try:
            outcome, value = result_queue.get(timeout=timeout_seconds)
        except queue.Empty:
            with self._binding_tick_lock:
                current = self._binding_tick_inflight.get(key)
                if current is not entry:
                    raise RuntimeConfigInvalidError(
                        "LocalSIM binding tick owner changed before timeout readback",
                        context={
                            **context,
                            "reason_code": "LOCALSIM_BINDING_TICK_OWNER_DRIFT",
                            "stage": "BINDING_TICK",
                        },
                    )
                try:
                    outcome, value = result_queue.get_nowait()
                except queue.Empty:
                    entry.waiter_active = False
                    entry.timed_out = True
                    raise self._binding_tick_in_progress_error(entry) from None
                self._binding_tick_inflight.pop(key, None)
        else:
            with self._binding_tick_lock:
                if self._binding_tick_inflight.get(key) is entry:
                    self._binding_tick_inflight.pop(key, None)
        return self._resolve_binding_tick_outcome(entry=entry, outcome=outcome, value=value)

    def _consume_or_report_binding_tick(self, entry: _BindingTickInFlight) -> SimulationSchedulerBindingResult:
        try:
            outcome, value = entry.result_queue.get_nowait()
        except queue.Empty:
            if not entry.thread.is_alive():
                self._binding_tick_inflight.pop(entry.key, None)
                raise RuntimeConfigInvalidError(
                    "LocalSIM binding tick worker ended without a result",
                    context={
                        **entry.context,
                        "reason_code": "LOCALSIM_BINDING_TICK_RESULT_MISSING",
                        "stage": "BINDING_TICK",
                    },
                )
            raise self._binding_tick_in_progress_error(entry) from None
        self._binding_tick_inflight.pop(entry.key, None)
        return self._resolve_binding_tick_outcome(entry=entry, outcome=outcome, value=value)

    @staticmethod
    def _resolve_binding_tick_outcome(
        *,
        entry: _BindingTickInFlight,
        outcome: str,
        value: Any,
    ) -> SimulationSchedulerBindingResult:
        if outcome == "exception" and isinstance(value, BaseException):
            raise value
        if outcome == "result" and isinstance(value, SimulationSchedulerBindingResult):
            return value
        raise RuntimeConfigInvalidError(
            "LocalSIM binding tick worker returned an invalid outcome carrier",
            context={
                **entry.context,
                "reason_code": "LOCALSIM_BINDING_TICK_OUTCOME_INVALID",
                "stage": "BINDING_TICK",
                "outcome": outcome,
                "value_type": type(value).__name__,
            },
        )

    @staticmethod
    def _binding_tick_in_progress_error(entry: _BindingTickInFlight) -> DataUnavailableError:
        elapsed_seconds = max(0.0, monotonic_time.monotonic() - entry.started_monotonic)
        return DataUnavailableError(
            "LocalSIM binding tick remains owned by its original worker; later bindings may continue",
            context={
                **entry.context,
                "reason_code": "LOCALSIM_BINDING_TICK_IN_PROGRESS",
                "stage": "BINDING_TICK",
                "failure_stage": "BINDING_TICK",
                "started_at": entry.started_at,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "timeout_seconds": entry.timeout_seconds,
                "thread_alive": entry.thread.is_alive(),
                "timed_out": entry.timed_out,
            },
        )

    def _binding_tick_status(self) -> dict[str, Any]:
        now = monotonic_time.monotonic()
        with self._binding_tick_lock:
            in_flight = [
                {
                    **entry.context,
                    "started_at": entry.started_at,
                    "elapsed_seconds": round(max(0.0, now - entry.started_monotonic), 3),
                    "timeout_seconds": entry.timeout_seconds,
                    "timed_out": entry.timed_out,
                    "thread_alive": entry.thread.is_alive(),
                    "result_ready": not entry.result_queue.empty(),
                }
                for entry in sorted(self._binding_tick_inflight.values(), key=lambda item: item.key)
            ]
        return {
            "single_flight_scope": "binding_id+trade_date",
            "in_flight_count": len(in_flight),
            "in_flight": in_flight,
        }

    def shutdown_binding_ticks(self, *, wait: bool = True) -> dict[str, Any]:
        with self._binding_tick_lock:
            entries = tuple(self._binding_tick_inflight.values())
        if wait:
            for entry in entries:
                entry.thread.join(timeout=5.0)
        alive = [entry for entry in entries if entry.thread.is_alive()]
        return {
            "schema_version": "localsim_binding_tick_shutdown_observation_v1",
            "wait_requested": bool(wait),
            "observed_owner_count": len(entries),
            "thread_alive_count": len(alive),
            "all_threads_stopped": not alive,
            "alive_binding_ids": sorted(entry.context["binding_id"] for entry in alive),
        }

    def _record_pre_run_binding_failure_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
        as_of_time: datetime | None = None,
        retry_claim_token: str | None = None,
        retry_source_fingerprint: str | None = None,
    ) -> SimulationSchedulerBindingResult:
        if self._is_binding_tick_in_progress_error(exc):
            return self._record_binding_tick_in_progress_result(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                exc=exc,
            )
        if self._is_selection_inference_pending_error(exc):
            pending = self._record_selection_inference_pending_result(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                created_by=created_by,
                exc=exc,
            )
            if pending is not None:
                return pending
        existing_after_failure = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        submit_failure_recorded = (
            existing_after_failure is not None
            and bool(existing_after_failure.execution_plan_id)
            and isinstance(existing_after_failure.run_payload_json.get("submit_failure"), dict)
        )
        if (
            not submit_failure_recorded
            and not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
            and self._has_broker_side_effect_evidence(
                binding=binding,
                trade_date=trade_date,
            )
        ):
            logger.exception(
                "Simulation lifecycle scheduler will not mark side-effect-bearing binding failure as pre-run failure",
                extra={
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "package_id": binding.package_id,
                    "error_type": type(exc).__name__,
                },
            )
            raise exc
        effective_data_source = self._effective_market_data_source_for_binding(
            binding=binding,
            trade_date=trade_date,
            default_data_source=data_source,
        )
        failed_run = self._persist_pre_run_binding_failure(
            binding=binding,
            trade_date=trade_date,
            data_source=effective_data_source,
            created_by=created_by,
            exc=exc,
        )
        if self._run_requires_binding_retry_control(failed_run):
            failed_run = self._record_simulation_retry_failure(
                run=failed_run,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                failure_stage=self._run_failure_stage(failed_run),
                exc=exc,
                as_of_time=as_of_time,
                expected_claim_token=retry_claim_token,
                source_fingerprint=retry_source_fingerprint,
            )
        if not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError)):
            pre_run_failure = failed_run.run_payload_json.get("pre_run_failure")
            reason_code = (
                str(pre_run_failure.get("reason_code"))
                if isinstance(pre_run_failure, dict) and pre_run_failure.get("reason_code")
                else self._pre_run_failure_reason_code(exc, self._exception_context(exc))
            )
            failure_stage = (
                str(pre_run_failure.get("failure_stage"))
                if isinstance(pre_run_failure, dict) and pre_run_failure.get("failure_stage")
                else self._pre_run_failure_stage(exc, self._exception_context(exc))
            )
            logger.warning(
                "Simulation lifecycle scheduler isolated per-binding failure; continuing later bindings: %s",
                {
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "package_id": binding.package_id,
                    "reason_code": reason_code,
                    "failure_stage": failure_stage,
                    "error_type": type(exc).__name__,
                },
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=failed_run.status.value,
            run=failed_run,
            error=self._pre_run_failure_error_payload(failed_run, exc=exc),
            data_source=effective_data_source,
        )

    @staticmethod
    def _is_binding_tick_in_progress_error(exc: Exception) -> bool:
        context = getattr(exc, "context", None)
        return (
            isinstance(exc, DataUnavailableError)
            and isinstance(context, dict)
            and str(context.get("reason_code") or "").upper() == "LOCALSIM_BINDING_TICK_IN_PROGRESS"
        )

    def _record_binding_tick_in_progress_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        exc: Exception,
    ) -> SimulationSchedulerBindingResult:
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "localsim_binding_tick_in_progress_v1",
            "status": "IN_PROGRESS",
            "reason_code": "LOCALSIM_BINDING_TICK_IN_PROGRESS",
            "stage": "BINDING_TICK",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": trade_date.isoformat(),
            "context": context,
            "alert": {
                "severity": "WARNING",
                "reason_code": "LOCALSIM_BINDING_TICK_IN_PROGRESS",
                "binding_id": binding.binding_id,
                "trade_date": trade_date.isoformat(),
                "timed_out": bool(context.get("timed_out")),
                "auto_clear": "owner_result_consumed",
            },
        }
        logger.info("LocalSIM binding tick remains single-flight: %s", diagnostic)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="LOCALSIM_BINDING_TICK_IN_PROGRESS",
            run=existing,
            lifecycle_diagnostic=diagnostic,
            data_source=data_source,
        )

    @staticmethod
    def _is_selection_inference_pending_error(exc: Exception) -> bool:
        context = getattr(exc, "context", None)
        return (
            isinstance(exc, DataUnavailableError)
            and isinstance(context, dict)
            and str(context.get("reason_code") or "").upper() == "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS"
        )

    def _record_selection_inference_pending_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> SimulationSchedulerBindingResult | None:
        """Persist an explicit non-terminal wait without inventing a failed run."""

        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and (
            existing.execution_plan_id
            or self._run_has_broker_side_effect_evidence(existing)
            or existing.status
            in {
                SimulationDailyRunStatus.SUCCEEDED,
                SimulationDailyRunStatus.FAILED_TERMINAL,
                SimulationDailyRunStatus.CANCELLED,
            }
        ):
            logger.error(
                "Selection inference pending collided with a planned, side-effect-bearing, or terminal run",
                extra={
                    "reason_code": "SIMULATION_SELECTION_INFERENCE_PENDING_STATE_CONFLICT",
                    "run_id": existing.run_id,
                    "run_status": existing.status.value,
                    "execution_plan_id": existing.execution_plan_id,
                    "binding_id": binding.binding_id,
                },
            )
            return None

        identity = self._simulation_daily_run_identity(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        context = self._exception_context(exc)
        observed_at = datetime.now(UTC).isoformat()
        diagnostic = {
            "schema_version": "simulation_selection_inference_pending_v1",
            "status": "PENDING",
            "reason_code": "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS",
            "stage": "SELECTION_INFERENCE",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "release_id": binding.release_id,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "observed_at": observed_at,
            "broker_called": False,
            "context": context,
        }
        if existing is None:
            digest = canonical_json_sha256(identity)
            existing = self.repository.save_simulation_daily_run(
                SimulationDailyRun(
                    run_id=f"simrun_{digest[:16]}",
                    trade_date=trade_date,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    package_id=runtime_release.package_id,
                    manifest_sha256=runtime_release.manifest_sha256,
                    release_id=runtime_release.release_id,
                    release_hash=runtime_release.release_hash or "",
                    binding_id=binding.binding_id,
                    binding_hash=binding.binding_hash or "",
                    account_group_id=binding.account_group_id,
                    strategy_slot_id=binding.strategy_slot_id,
                    status=SimulationDailyRunStatus.SIGNAL_GENERATING,
                    run_payload_json={
                        **identity,
                        "created_by": created_by,
                        "last_stage": SimulationDailyRunStatus.SIGNAL_GENERATING.value,
                        "broker_called": False,
                        "submitted_intents": 0,
                        "failed_intents": 0,
                        "selection_inference_pending": diagnostic,
                    },
                )
            )
        else:
            existing = self.repository.update_simulation_daily_run(
                existing.run_id,
                status=SimulationDailyRunStatus.SIGNAL_GENERATING,
                payload_patch={
                    "last_stage": SimulationDailyRunStatus.SIGNAL_GENERATING.value,
                    "broker_called": False,
                    "submitted_intents": 0,
                    "failed_intents": 0,
                    "selection_inference_pending": diagnostic,
                },
                payload_unset=(
                    "pre_run_failure",
                    "pre_run_failure_last_observed_at",
                    "pre_run_failure_observed_after_terminal",
                    "submit_failure",
                ),
            )
        logger.info(
            "Simulation selection inference remains pending without marking the run failed: %s",
            {
                "run_id": existing.run_id,
                "binding_id": binding.binding_id,
                "trade_date": trade_date.isoformat(),
            },
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="SELECTION_INFERENCE_PENDING",
            run=existing,
            lifecycle_diagnostic=diagnostic,
            data_source=data_source,
        )

    def _has_broker_side_effect_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> bool:
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is None:
            return False
        return self._run_has_broker_side_effect_evidence(existing)

    @staticmethod
    def _run_has_broker_side_effect_evidence(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json
        if bool(payload.get("broker_called")):
            return True
        if payload.get("broker_side_effect_state") == "UNKNOWN":
            return True
        if payload.get("miniqmt_side_effect_state") == "UNKNOWN_TIMEOUT":
            return True
        if isinstance(payload.get("miniqmt_submit_timeout"), dict):
            return True
        for key in ("submitted_intents", "failed_intents"):
            try:
                if int(payload.get(key) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                return True
        return any(
            payload.get(key)
            for key in (
                "broker_order_handles",
                "qmt_batch_id",
                "qmt_batch_result",
                "local_sim_persistence",
                "reconcile_after_submit",
                "tail_handling",
            )
        )

    def _persist_pre_run_binding_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> SimulationDailyRun:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        identity = self._simulation_daily_run_identity(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )
        diagnostic = self._pre_run_failure_diagnostic(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            created_by=created_by,
            exc=exc,
        )
        if existing is None:
            digest = canonical_json_sha256(identity)
            existing = self.repository.save_simulation_daily_run(
                SimulationDailyRun(
                    run_id=f"simrun_{digest[:16]}",
                    trade_date=trade_date,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    package_id=runtime_release.package_id,
                    manifest_sha256=runtime_release.manifest_sha256,
                    release_id=runtime_release.release_id,
                    release_hash=runtime_release.release_hash or "",
                    binding_id=binding.binding_id,
                    binding_hash=binding.binding_hash or "",
                    account_group_id=binding.account_group_id,
                    strategy_slot_id=binding.strategy_slot_id,
                    status=SimulationDailyRunStatus.FAILED_RETRYABLE,
                    run_payload_json={**identity, "created_by": created_by},
                )
            )
        side_effect_patch = self._pre_run_failure_side_effect_patch(
            existing=existing,
            diagnostic=diagnostic,
        )
        preplan_unknown_failure = (
            self._preplan_unknown_failure_evidence(binding=binding, diagnostic=diagnostic)
            if side_effect_patch.get("broker_side_effect_state") == "UNKNOWN" and self._is_pre_run_failure_run(existing)
            else None
        )
        if side_effect_patch.get("broker_side_effect_state") == "UNKNOWN" and not self._is_pre_run_failure_run(
            existing
        ):
            diagnostic = self._with_pre_run_failure_observation(existing, diagnostic)
            diagnostic = self._pre_run_failure_diagnostic_with_unknown_side_effect(diagnostic)
            return self.repository.update_simulation_daily_run(
                existing.run_id,
                payload_patch={
                    "broker_side_effect_state": "UNKNOWN",
                    "pre_run_failure": diagnostic,
                    "pre_run_failure_last_observed_at": diagnostic["last_observed_at"],
                },
                payload_unset=self._unknown_side_effect_payload_unset(existing),
            )
        if not self._is_pre_run_failure_run(existing):
            terminal_statuses = {
                SimulationDailyRunStatus.SUCCEEDED,
                SimulationDailyRunStatus.FAILED_TERMINAL,
                SimulationDailyRunStatus.CANCELLED,
            }
            if existing.execution_plan_id and isinstance(existing.run_payload_json.get("submit_failure"), dict):
                return existing
            if (
                (
                    not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
                    or isinstance(existing.run_payload_json.get("selection_inference_pending"), dict)
                )
                and existing.status not in terminal_statuses
                and not self._run_has_broker_side_effect_evidence(existing)
            ):
                return self.repository.update_simulation_daily_run(
                    existing.run_id,
                    status=SimulationDailyRunStatus.FAILED_RETRYABLE,
                    payload_patch={
                        "last_stage": "PRE_RUN_FAILED",
                        **side_effect_patch,
                        "pre_run_failure": diagnostic,
                        "submit_failure": {
                            "stage": "PRE_RUN_FAILED",
                            "failure_stage": diagnostic.get("failure_stage"),
                            "type": diagnostic["error_type"],
                            "message": diagnostic["message"],
                            "context": diagnostic,
                        },
                    },
                )
            return existing
        diagnostic = self._with_pre_run_failure_observation(existing, diagnostic)
        if side_effect_patch.get("broker_side_effect_state") == "UNKNOWN":
            diagnostic = self._pre_run_failure_diagnostic_with_unknown_side_effect(diagnostic)
        terminal_statuses = {
            SimulationDailyRunStatus.SUCCEEDED,
            SimulationDailyRunStatus.FAILED_TERMINAL,
            SimulationDailyRunStatus.CANCELLED,
        }
        if existing.status in terminal_statuses:
            return self.repository.update_simulation_daily_run(
                existing.run_id,
                payload_patch={
                    "pre_run_failure_observed_after_terminal": diagnostic,
                    "pre_run_failure_last_observed_at": diagnostic["last_observed_at"],
                },
            )
        return self.repository.update_simulation_daily_run(
            existing.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": "PRE_RUN_FAILED",
                **side_effect_patch,
                **(
                    {"miniqmt_preplan_unknown_failure": preplan_unknown_failure}
                    if preplan_unknown_failure is not None
                    else {}
                ),
                "pre_run_failure": diagnostic,
                "submit_failure": {
                    "stage": "PRE_RUN_FAILED",
                    "failure_stage": diagnostic.get("failure_stage"),
                    "type": diagnostic["error_type"],
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
            payload_unset=(
                self._unknown_side_effect_payload_unset(existing)
                if side_effect_patch.get("broker_side_effect_state") == "UNKNOWN"
                else ()
            ),
        )

    @staticmethod
    def _unknown_side_effect_payload_unset(existing: SimulationDailyRun) -> tuple[str, ...]:
        """Remove only disproven no-side-effect assumptions, never positive facts."""

        payload = existing.run_payload_json if isinstance(existing.run_payload_json, dict) else {}
        removable: list[str] = []
        if payload.get("broker_called") is False:
            removable.append("broker_called")
        for key in ("submitted_intents", "failed_intents"):
            value = payload.get(key)
            if type(value) is int and value == 0:
                removable.append(key)
        return tuple(removable)

    @staticmethod
    def _pre_run_failure_side_effect_patch(
        *,
        existing: SimulationDailyRun,
        diagnostic: dict[str, Any],
    ) -> dict[str, Any]:
        payload = existing.run_payload_json if isinstance(existing.run_payload_json, dict) else {}
        if (
            diagnostic.get("broker_side_effect_state") == "UNKNOWN"
            or payload.get("broker_side_effect_state") == "UNKNOWN"
        ):
            return {"broker_side_effect_state": "UNKNOWN"}
        if SimulationLifecycleScheduler._run_has_broker_side_effect_evidence(existing):
            return {}
        return {
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": 0,
        }

    @staticmethod
    def _pre_run_failure_diagnostic_with_unknown_side_effect(
        diagnostic: dict[str, Any],
    ) -> dict[str, Any]:
        preserved = {
            key: value
            for key, value in diagnostic.items()
            if key not in {"broker_called", "submitted_intents", "failed_intents"}
        }
        return {
            **preserved,
            "broker_side_effect_state": "UNKNOWN",
            "next_action": (
                "reconcile broker and durable outbox state before any retry; the scheduler cannot prove whether "
                "the callback/clock tick produced a broker side effect"
            ),
        }

    @staticmethod
    def _preplan_unknown_failure_evidence(
        *,
        binding: SimulationReleaseBinding,
        diagnostic: dict[str, Any],
    ) -> dict[str, Any] | None:
        context = diagnostic.get("context") if isinstance(diagnostic.get("context"), dict) else {}
        ordered_failures = context.get("ordered_failures")
        if not isinstance(ordered_failures, list):
            return None
        matching = [
            dict(item)
            for item in ordered_failures
            if isinstance(item, dict) and item.get("binding_id") == binding.binding_id
        ]
        if not matching:
            return None
        bounded = matching[:_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT]
        observed_failure_count = context.get("failure_count")
        failure_count = (
            observed_failure_count
            if type(observed_failure_count) is int and observed_failure_count >= len(matching)
            else len(matching)
        )
        observed_truncated_count = context.get("truncated_failure_count")
        truncated_failure_count = (
            observed_truncated_count
            if type(observed_truncated_count) is int and observed_truncated_count >= 0
            else max(0, failure_count - len(bounded))
        )
        normalized = simulation_retry_json_safe_evidence(bounded)
        if not isinstance(normalized, list):
            raise AssertionError("MiniQMT preplan unknown failure evidence must normalize to a list")
        runtime_ids = sorted(
            {
                str(item.get("runtime_id") or "").strip()
                for item in matching
                if str(item.get("runtime_id") or "").strip()
            }
        )
        runtime_authority_by_sha256: dict[str, dict[str, Any]] = {}
        for item in matching:
            authority = item.get("scheduler_runtime_authority")
            if not isinstance(authority, Mapping):
                continue
            normalized_authority = simulation_retry_json_safe_evidence(dict(authority))
            if not isinstance(normalized_authority, dict):
                raise AssertionError("MiniQMT runtime authority must normalize to an object")
            runtime_authority_by_sha256[canonical_json_sha256(normalized_authority)] = normalized_authority
        runtime_authorities = [runtime_authority_by_sha256[key] for key in sorted(runtime_authority_by_sha256)]
        identity_payload = {
            "schema_version": "miniqmt_preplan_unknown_failure_identity_v2",
            "binding_id": binding.binding_id,
            "runtime_ids": runtime_ids,
            "failure_count": failure_count,
            "all_failures_sha256": context.get("all_failures_sha256"),
            "omitted_failures_sha256": context.get("omitted_failures_sha256"),
            "runtime_authorities": runtime_authorities,
        }
        return {
            "schema_version": "miniqmt_preplan_unknown_failure_v1",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "runtime_ids": runtime_ids,
            "failure_count": failure_count,
            "evidence_limit": _MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT,
            "truncated_failure_count": truncated_failure_count,
            "ordered_failures": normalized,
            "all_failures_sha256": context.get("all_failures_sha256"),
            "omitted_failures_sha256": context.get("omitted_failures_sha256"),
            "runtime_authorities": runtime_authorities,
            "failure_fingerprint": canonical_json_sha256(identity_payload),
        }

    @staticmethod
    def _is_pre_run_failure_run(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json
        if run.execution_plan_id:
            return False
        if isinstance(payload.get("pre_run_failure"), dict):
            return True
        if payload.get("broker_called") is not None:
            return False
        if isinstance(payload.get("submit_failure"), dict):
            return False
        if isinstance(payload.get("qmt_batch_result"), dict):
            return False
        if isinstance(payload.get("local_sim_persistence"), dict):
            return False
        return True

    @staticmethod
    def _simulation_daily_run_identity(
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> dict[str, Any]:
        return {
            "schema_version": "simulation_daily_run_identity_v1",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "broker_backend": binding.broker_backend.value,
            "trade_date": trade_date.isoformat(),
        }

    @staticmethod
    def _pre_run_failure_diagnostic(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: Exception,
    ) -> dict[str, Any]:
        context = SimulationLifecycleScheduler._exception_context(exc)
        reason_code = SimulationLifecycleScheduler._pre_run_failure_reason_code(exc, context)
        failure_stage = SimulationLifecycleScheduler._pre_run_failure_stage(exc, context)
        blocked_check = SimulationLifecycleScheduler._pre_run_failure_blocked_check(context)
        blocked_context = SimulationLifecycleScheduler._pre_run_failure_blocked_check_context(context)
        missing_relative_paths = SimulationLifecycleScheduler._pre_run_failure_missing_relative_paths(
            context,
            blocked_context=blocked_context,
        )
        observed_at = datetime.now(UTC).isoformat()
        legacy_pre_run_error = isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError))
        broker_side_effect_state = str(context.get("broker_side_effect_state") or "").strip().upper()
        diagnostic = {
            "schema_version": "simulation_pre_run_failure_v1",
            "stage": "PRE_RUN_FAILED" if legacy_pre_run_error else failure_stage,
            "lifecycle_stage": "PRE_RUN_FAILED",
            "failure_stage": failure_stage,
            "reason_code": reason_code,
            "reason": "simulation_runtime_unattended_pre_run_binding_failed",
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "broker_backend": binding.broker_backend.value,
            "package_id": binding.package_id,
            "release_id": binding.release_id,
            "release_hash": binding.release_hash,
            "manifest_sha256": binding.manifest_sha256,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "created_by": created_by,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "first_observed_at": observed_at,
            "last_observed_at": observed_at,
            "observed_count": 1,
            "next_action": (
                "reconcile broker and durable outbox state before any retry; the scheduler cannot prove whether "
                "the callback/clock tick produced a broker side effect"
                if broker_side_effect_state == "UNKNOWN"
                else (
                    "fix the data/configuration dependency reported by reason_code and rerun the scheduler tick; "
                    "no broker order was submitted before this failure"
                )
            ),
        }
        if broker_side_effect_state == "UNKNOWN":
            diagnostic["broker_side_effect_state"] = "UNKNOWN"
        else:
            diagnostic.update(
                {
                    "broker_called": False,
                    "submitted_intents": 0,
                    "failed_intents": 0,
                }
            )
        if blocked_check:
            diagnostic["blocked_check"] = blocked_check
        if blocked_context:
            diagnostic["blocked_check_context"] = blocked_context
        if missing_relative_paths:
            diagnostic["missing_relative_paths"] = missing_relative_paths
        return diagnostic

    @staticmethod
    def _with_pre_run_failure_observation(
        existing: SimulationDailyRun,
        diagnostic: dict[str, Any],
    ) -> dict[str, Any]:
        previous = existing.run_payload_json.get("pre_run_failure")
        if not isinstance(previous, dict):
            return diagnostic
        try:
            observed_count = int(previous.get("observed_count") or 0) + 1
        except (TypeError, ValueError):
            observed_count = 1
        return {
            **diagnostic,
            "first_observed_at": str(previous.get("first_observed_at") or diagnostic["first_observed_at"]),
            "observed_count": observed_count,
        }

    @staticmethod
    def _pre_run_failure_error_payload(
        run: SimulationDailyRun,
        *,
        exc: Exception | None = None,
    ) -> dict[str, Any]:
        pre_run_failure = run.run_payload_json.get("pre_run_failure")
        if isinstance(pre_run_failure, dict):
            return {
                "type": str(pre_run_failure.get("error_type") or run.status.value),
                "message": str(pre_run_failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(pre_run_failure),
            }
        terminal_failure = run.run_payload_json.get("pre_run_failure_observed_after_terminal")
        if isinstance(terminal_failure, dict):
            return {
                "type": str(terminal_failure.get("error_type") or run.status.value),
                "message": str(terminal_failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(terminal_failure),
            }
        if exc is not None:
            context = getattr(exc, "context", None)
            return {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": context if isinstance(context, dict) else {"run_id": run.run_id},
            }
        failure = run.run_payload_json.get("submit_failure")
        if isinstance(failure, dict):
            context = failure.get("context") if isinstance(failure.get("context"), dict) else {"run_id": run.run_id}
            return {
                "type": str(failure.get("type") or run.status.value),
                "message": str(failure.get("message") or run.status.value),
                "context": SimulationLifecycleScheduler._flatten_pre_run_failure_context(context),
            }
        return {"type": run.status.value, "message": run.status.value, "context": {"run_id": run.run_id}}

    @staticmethod
    def _flatten_pre_run_failure_context(context: dict[str, Any]) -> dict[str, Any]:
        nested = context.get("context")
        if not isinstance(nested, dict):
            return context
        return {
            **nested,
            "pre_run_failure": context,
            "reason_code": str(context.get("reason_code") or nested.get("reason_code") or "PRE_RUN_FAILED"),
            "stage": str(context.get("stage") or "PRE_RUN_FAILED"),
            "failure_stage": context.get("failure_stage") or nested.get("failure_stage"),
            "binding_id": context.get("binding_id") or nested.get("binding_id"),
            "strategy_id": context.get("strategy_id") or nested.get("strategy_id"),
            "package_id": context.get("package_id") or nested.get("package_id"),
            "release_id": context.get("release_id") or nested.get("release_id"),
            "manifest_sha256": context.get("manifest_sha256") or nested.get("manifest_sha256"),
            "blocked_check": context.get("blocked_check") or nested.get("blocked_check"),
            "missing_relative_paths": context.get("missing_relative_paths") or nested.get("missing_relative_paths"),
            "trade_date": context.get("trade_date") or nested.get("trade_date"),
            "broker_backend": context.get("broker_backend"),
            "data_source": context.get("data_source"),
        }

    @staticmethod
    def _exception_context(exc: Exception) -> dict[str, Any]:
        raw_context = getattr(exc, "context", None)
        return _json_safe_preview(raw_context) if isinstance(raw_context, dict) else {}

    @staticmethod
    def _pre_run_failure_reason_code(exc: Exception, context: dict[str, Any]) -> str:
        reason_code = context.get("reason_code")
        if reason_code:
            return str(reason_code)
        blocked_context = SimulationLifecycleScheduler._pre_run_failure_blocked_check_context(context)
        if blocked_context and blocked_context.get("reason_code"):
            return str(blocked_context["reason_code"])
        return str(getattr(exc, "error_code", type(exc).__name__))

    @staticmethod
    def _pre_run_failure_stage(exc: Exception, context: dict[str, Any]) -> str:
        for key in ("failure_stage", "phase", "stage"):
            value = str(context.get(key) or "").strip().lower()
            if value in {
                "context",
                "selection",
                "selection_inference",
                "preflight",
                "validate",
                "validation",
                "build_plan",
                "planning",
                "binding_tick",
                "miniqmt_event_loop_submit",
                "miniqmt_reconcile_after_submit",
            }:
                return "validate" if value == "validation" else value
            if value in {"submit_preflight", "pre_submit"}:
                return "pre_submit"
        if isinstance(context.get("preflight"), dict) or context.get("blocked_check"):
            return "preflight"
        if type(exc).__name__ == "LiveInferencePreflightError":
            return "preflight"
        return "pre_run"

    @staticmethod
    def _pre_run_failure_blocked_check(context: dict[str, Any]) -> str | None:
        raw_blocked = context.get("blocked_check")
        if isinstance(raw_blocked, dict):
            name = raw_blocked.get("name")
            return str(name) if name else None
        if raw_blocked:
            return str(raw_blocked)
        check = SimulationLifecycleScheduler._pre_run_failure_blocked_check_payload(context)
        if not check:
            return None
        name = check.get("name")
        return str(name) if name else None

    @staticmethod
    def _pre_run_failure_blocked_check_context(context: dict[str, Any]) -> dict[str, Any]:
        raw_blocked = context.get("blocked_check")
        if isinstance(raw_blocked, dict):
            raw_context = raw_blocked.get("context")
            return dict(raw_context) if isinstance(raw_context, dict) else {}
        check = SimulationLifecycleScheduler._pre_run_failure_blocked_check_payload(context)
        if check:
            raw_context = check.get("context")
            return dict(raw_context) if isinstance(raw_context, dict) else {}
        return {}

    @staticmethod
    def _pre_run_failure_blocked_check_payload(context: dict[str, Any]) -> dict[str, Any] | None:
        preflight = context.get("preflight")
        checks = preflight.get("checks") if isinstance(preflight, dict) else None
        if not isinstance(checks, list):
            return None
        for check in checks:
            if isinstance(check, dict) and str(check.get("status") or "").upper() == "BLOCKED":
                return check
        return None

    @staticmethod
    def _pre_run_failure_missing_relative_paths(
        context: dict[str, Any],
        *,
        blocked_context: dict[str, Any] | None = None,
    ) -> list[str]:
        paths = SimulationLifecycleScheduler._string_list(context.get("missing_relative_paths"))
        if paths:
            return paths
        return SimulationLifecycleScheduler._string_list((blocked_context or {}).get("missing_relative_paths"))

    @staticmethod
    def _string_list(value: Any) -> list[str]:
        if isinstance(value, list | tuple | set):
            return [str(item) for item in value if str(item).strip()]
        if value is None:
            return []
        text = str(value).strip()
        return [text] if text else []

    def _preplan_unknown_terminal_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        data_source: str,
    ) -> SimulationSchedulerBindingResult | None:
        proof = run.run_payload_json.get("miniqmt_preplan_unknown_reconciliation")
        if (
            not isinstance(proof, dict)
            or run.execution_plan_id is not None
            or run.status != SimulationDailyRunStatus.FAILED_TERMINAL
            or proof.get("status")
            not in {
                "BROKER_SIDE_EFFECT_RECONCILED_TERMINAL",
                "RUNTIME_IDENTITY_INVALID_TERMINAL",
            }
        ):
            return None
        reason_code = str(proof.get("reason_code") or "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION_TERMINAL")
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=run.status.value,
            run=run,
            error={
                "type": "MiniQMTPreplanUnknownOutcome",
                "message": "preplan UNKNOWN broker outcome was closed without creating a replacement plan",
                "context": dict(proof),
            },
            lifecycle_diagnostic={
                **proof,
                "alert": {
                    "severity": "ERROR",
                    "reason_code": reason_code,
                    "automatic": True,
                },
            },
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=run.trade_date,
                default_data_source=data_source,
            ),
        )

    def _preplan_unknown_reconciliation_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        data_source: str,
        proof: dict[str, Any],
    ) -> SimulationSchedulerBindingResult:
        reason_code = str(proof["reason_code"])
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=run.status.value,
            run=run,
            error={
                "type": "MiniQMTPreplanUnknownOutcome",
                "message": "preplan UNKNOWN broker outcome is not eligible for plan creation",
                "context": dict(proof),
            },
            lifecycle_diagnostic={
                **proof,
                "alert": {
                    "severity": "ERROR" if run.status == SimulationDailyRunStatus.FAILED_TERMINAL else "WARNING",
                    "reason_code": reason_code,
                    "automatic": True,
                    "auto_retry": bool(proof.get("auto_retry")),
                },
            },
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=run.trade_date,
                default_data_source=data_source,
            ),
        )

    def _release_preplan_unknown_kernel_runtime(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_id: str,
        runtime_authority: Mapping[str, Any],
        trade_date: date,
    ) -> tuple[SimulationDailyRun, dict[str, Any], dict[str, Any]]:
        effective_runtime_authority = dict(runtime_authority)
        raw_preflight = run.run_payload_json.get("miniqmt_preplan_unknown_runtime_release_preflight")
        if isinstance(raw_preflight, dict):
            preflight_payload = {key: value for key, value in raw_preflight.items() if key != "preflight_sha256"}
            preflight_authority = raw_preflight.get("effective_runtime_authority")
            if (
                raw_preflight.get("preflight_sha256") != canonical_json_sha256(preflight_payload)
                or raw_preflight.get("run_id") != run.run_id
                or raw_preflight.get("binding_id") != binding.binding_id
                or raw_preflight.get("runtime_id") != runtime_id
                or not isinstance(preflight_authority, Mapping)
            ):
                raise RuntimeConfigInvalidError(
                    "MiniQMT preplan UNKNOWN runtime release preflight identity is invalid",
                    context={
                        "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_PREFLIGHT_INVALID",
                        "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                        "run_id": run.run_id,
                        "binding_id": binding.binding_id,
                        "runtime_id": runtime_id,
                    },
                )
            effective_runtime_authority = dict(preflight_authority)
        existing = run.run_payload_json.get("miniqmt_preplan_unknown_runtime_release")
        if (
            isinstance(existing, dict)
            and existing.get("status") in {"RELEASED", "ALREADY_ABSENT"}
            and existing.get("run_id") == run.run_id
            and existing.get("binding_id") == binding.binding_id
            and existing.get("trade_date") == trade_date.isoformat()
            and existing.get("runtime_id") == runtime_id
            and existing.get("execution_plan_id") == runtime_authority.get("execution_plan_id")
            and existing.get("lifecycle_generation") == runtime_authority.get("lifecycle_generation")
            and existing.get("attempt_token") == runtime_authority.get("attempt_token")
        ):
            return run, dict(existing), effective_runtime_authority
        get_runtime = getattr(self._miniqmt_quote_ingress_activation, "get_kernel_product_runtime", None)
        release = getattr(self._miniqmt_quote_ingress_activation, "release_kernel_product_runtime", None)
        if not callable(get_runtime) or not callable(release):
            raise RuntimeConfigInvalidError(
                "MiniQMT preplan UNKNOWN recovery requires exact KERNEL_V2 runtime lookup and release",
                context={
                    "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_UNAVAILABLE",
                    "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "runtime_id": runtime_id,
                },
            )
        runtime = get_runtime(runtime_id)
        status = "ALREADY_ABSENT"
        if runtime is not None:
            runtime_trade_date = getattr(runtime, "trade_date", None)
            runtime_plan_id = str(getattr(runtime, "execution_plan_id", "") or "").strip()
            identity_conflicts = []
            if str(getattr(runtime, "binding_id", "") or "").strip() != binding.binding_id:
                identity_conflicts.append("runtime_binding_conflict")
            if runtime_trade_date != trade_date:
                identity_conflicts.append("runtime_trade_date_conflict")
            if runtime_plan_id and runtime_plan_id != runtime_authority.get("execution_plan_id"):
                identity_conflicts.append("runtime_execution_plan_conflict")
            attempt_authority, attempt_conflict = self._kernel_product_attempt_authority(
                runtime_id=runtime_id,
                runtime=runtime,
                failure={
                    "lifecycle_generation": runtime_authority.get("lifecycle_generation"),
                    "attempt_token": runtime_authority.get("attempt_token"),
                },
            )
            if attempt_conflict is not None:
                identity_conflicts.append(attempt_conflict.lower())
            if identity_conflicts:
                raise RuntimeConfigInvalidError(
                    "MiniQMT preplan UNKNOWN runtime successor or foreign owner cannot be released",
                    context={
                        "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                        "run_id": run.run_id,
                        "binding_id": binding.binding_id,
                        "runtime_id": runtime_id,
                        "identity_conflicts": identity_conflicts,
                    },
                )
            assert attempt_authority is not None
            authority_plan = self.repository.get_execution_plan(str(runtime_authority.get("execution_plan_id") or ""))
            effective_runtime_authority = {
                **dict(runtime_authority),
                "outbox_authority": self._kernel_product_outbox_authority(
                    runtime=runtime,
                    runtime_id=runtime_id,
                    binding=binding,
                    run=run,
                    plan=authority_plan,
                    trade_date=trade_date,
                ),
            }
            preflight_payload = {
                "schema_version": "miniqmt_preplan_unknown_runtime_release_preflight_v1",
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "runtime_id": runtime_id,
                "execution_plan_id": runtime_authority.get("execution_plan_id"),
                "lifecycle_generation": runtime_authority.get("lifecycle_generation"),
                "attempt_token": runtime_authority.get("attempt_token"),
                "effective_runtime_authority": effective_runtime_authority,
                "prepared_at": datetime.now(UTC).isoformat(),
                "automatic": True,
            }
            preflight = {
                **preflight_payload,
                "preflight_sha256": canonical_json_sha256(preflight_payload),
            }
            try:
                run = self.repository.update_simulation_daily_run(
                    run.run_id,
                    payload_patch={"miniqmt_preplan_unknown_runtime_release_preflight": preflight},
                )
            except Exception as exc:  # noqa: BLE001 - never release before durable current authority exists.
                raise RuntimeConfigInvalidError(
                    "MiniQMT preplan UNKNOWN runtime release preflight was not persisted",
                    context={
                        "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_PREFLIGHT_PERSIST_FAILED",
                        "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                        "run_id": run.run_id,
                        "binding_id": binding.binding_id,
                        "runtime_id": runtime_id,
                        "broker_side_effect_state": "UNKNOWN",
                        "persistence_error": {"type": type(exc).__name__, "message": str(exc)[:2048]},
                    },
                ) from exc
            release(runtime_id)
            if get_runtime(runtime_id) is not None:
                raise RuntimeConfigInvalidError(
                    "MiniQMT preplan UNKNOWN runtime remained registered after release",
                    context={
                        "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_READBACK_FAILED",
                        "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                        "run_id": run.run_id,
                        "binding_id": binding.binding_id,
                        "runtime_id": runtime_id,
                    },
                )
            status = "RELEASED"
        evidence = {
            "schema_version": "miniqmt_preplan_unknown_runtime_release_v1",
            "status": status,
            "run_id": run.run_id,
            "binding_id": run.binding_id,
            "trade_date": trade_date.isoformat(),
            "runtime_id": runtime_id,
            "execution_plan_id": runtime_authority.get("execution_plan_id"),
            "lifecycle_generation": runtime_authority.get("lifecycle_generation"),
            "attempt_token": runtime_authority.get("attempt_token"),
            "effective_runtime_authority": effective_runtime_authority,
            "process_local_runtime_present": runtime is not None,
            "released_at": datetime.now(UTC).isoformat(),
            "automatic": True,
        }
        try:
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                payload_patch={"miniqmt_preplan_unknown_runtime_release": evidence},
            )
        except Exception as exc:  # noqa: BLE001 - release happened; preserve UNKNOWN and retry durably.
            raise RuntimeConfigInvalidError(
                "MiniQMT preplan UNKNOWN runtime release receipt was not persisted",
                context={
                    "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE_RECEIPT_PERSIST_FAILED",
                    "stage": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_RELEASE",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "runtime_id": runtime_id,
                    "runtime_release_status": status,
                    "broker_side_effect_state": "UNKNOWN",
                    "persistence_error": {"type": type(exc).__name__, "message": str(exc)},
                },
            ) from exc
        return updated, evidence, effective_runtime_authority

    @staticmethod
    def _preplan_sync_receipt_conflicts(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        sync_result: dict[str, Any] | None,
    ) -> list[str]:
        sync_readback = dict(sync_result) if isinstance(sync_result, dict) else {}
        conflicts: list[str] = []
        if sync_readback.get("account_id") != binding.broker_account_id:
            conflicts.append("sync_account_id_conflict")
        if sync_readback.get("trade_date") != trade_date.isoformat():
            conflicts.append("sync_trade_date_conflict")
        if sync_readback.get("orders_query_succeeded") is not True:
            conflicts.append("orders_query_not_proven")
        if sync_readback.get("trades_query_succeeded") is not True:
            conflicts.append("trades_query_not_proven")
        for prefix in ("orders", "trades"):
            snapshot_count = sync_readback.get(f"{prefix}_snapshot_count")
            snapshot_sha256 = sync_readback.get(f"{prefix}_snapshot_sha256")
            if type(snapshot_count) is not int or snapshot_count < 0:
                conflicts.append(f"{prefix}_snapshot_count_invalid")
            if not isinstance(snapshot_sha256, str) or not snapshot_sha256:
                conflicts.append(f"{prefix}_snapshot_hash_invalid")
        if bool(sync_readback.get("stale_broker_snapshot")):
            conflicts.append("stale_broker_snapshot")
        return conflicts

    def _preplan_exact_broker_authority(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
        runtime_authority: Mapping[str, Any],
    ) -> dict[str, Any]:
        outbox = runtime_authority.get("outbox_authority")
        outbox_authority = dict(outbox) if isinstance(outbox, Mapping) else {}
        command_rows = outbox_authority.get("commands")
        commands = (
            [dict(item) for item in command_rows if isinstance(item, Mapping)] if isinstance(command_rows, list) else []
        )
        broker_order_ids = {
            str(item.get("broker_order_id") or "").strip()
            for item in commands
            if str(item.get("broker_order_id") or "").strip()
        }
        order_remarks = {
            str(item.get("order_remark") or "").strip()
            for item in commands
            if str(item.get("order_remark") or "").strip()
        }
        deterministic_refs = {
            str(item.get("deterministic_client_order_ref") or "").strip()
            for item in commands
            if str(item.get("deterministic_client_order_ref") or "").strip()
        }
        exact_refs = broker_order_ids | order_remarks | deterministic_refs

        def is_exact_order(*, order_id: Any, order_remark: Any) -> bool:
            return bool(str(order_id or "").strip() in exact_refs or str(order_remark or "").strip() in exact_refs)

        repository = getattr(context, "qmt_ledger_repository", None)
        list_orders = getattr(repository, "list_order_ledger", None)
        list_unattributed_orders = getattr(repository, "list_unattributed_orders", None)
        list_unattributed_trades = getattr(repository, "list_unattributed_trades", None)
        authority_conflicts: list[str] = []
        if not callable(list_orders):
            authority_conflicts.append("order_ledger_authority_unavailable")
            orders: list[Any] = []
        else:
            orders = list_orders(
                account_id=binding.broker_account_id,
                trade_date=run.trade_date,
                strategy_id=binding.strategy_id,
                batch_id=None,
            )
        unattributed_orders = (
            list_unattributed_orders(account_id=binding.broker_account_id, trade_date=run.trade_date)
            if callable(list_unattributed_orders)
            else []
        )
        unattributed_trades = (
            list_unattributed_trades(account_id=binding.broker_account_id, trade_date=run.trade_date)
            if callable(list_unattributed_trades)
            else []
        )
        if not callable(list_unattributed_orders):
            authority_conflicts.append("unattributed_order_authority_unavailable")
        if not callable(list_unattributed_trades):
            authority_conflicts.append("unattributed_trade_authority_unavailable")

        exact_orders = [
            order
            for order in orders
            if is_exact_order(
                order_id=getattr(order, "qmt_order_id", None), order_remark=getattr(order, "order_remark", None)
            )
        ]
        exact_unattributed_orders = [
            order
            for order in unattributed_orders
            if is_exact_order(
                order_id=getattr(order, "qmt_order_id", None), order_remark=getattr(order, "order_remark", None)
            )
        ]
        exact_unattributed_trades = [
            trade
            for trade in unattributed_trades
            if is_exact_order(
                order_id=getattr(trade, "qmt_order_id", None), order_remark=getattr(trade, "order_remark", None)
            )
        ]
        exact_open_orders = [
            order
            for order in exact_orders
            if is_open_like_order_status(getattr(order, "order_status", None))
            and int(getattr(order, "order_volume", 0) or 0) > int(getattr(order, "traded_volume", 0) or 0)
        ]
        exact_unattributed_open_orders = []
        for order in exact_unattributed_orders:
            raw = getattr(order, "raw_json", None)
            raw_payload = raw if isinstance(raw, Mapping) else {}
            if is_open_like_order_status(raw_payload.get("order_status")) and int(
                raw_payload.get("order_volume") or 0
            ) > int(raw_payload.get("traded_volume") or 0):
                exact_unattributed_open_orders.append(order)
        retained_orders = [
            {
                "qmt_order_id": getattr(order, "qmt_order_id", None),
                "order_remark": getattr(order, "order_remark", None),
                "order_status": getattr(order, "order_status", None),
                "order_volume": getattr(order, "order_volume", None),
                "traded_volume": getattr(order, "traded_volume", None),
            }
            for order in exact_orders[:_MINIQMT_KERNEL_PRODUCT_FAILURE_EVIDENCE_LIMIT]
        ]
        exact_side_effect_ids = {
            str(getattr(item, "qmt_order_id", None) or "").strip()
            for item in [*exact_orders, *exact_unattributed_orders, *exact_unattributed_trades]
            if str(getattr(item, "qmt_order_id", None) or "").strip()
        }
        outbox_confirmed = int(outbox_authority.get("confirmed_broker_side_effect_count") or 0)
        outbox_ambiguous = int(outbox_authority.get("ambiguous_command_count") or 0)
        exact_side_effect_count = max(len(exact_side_effect_ids), outbox_confirmed)
        if outbox_authority.get("replacement_safe") is True and exact_side_effect_count > 0:
            authority_conflicts.append("outbox_broker_order_state_conflict")
        payload = {
            "schema_version": "miniqmt_preplan_exact_broker_authority_v1",
            "runtime_id": runtime_authority.get("runtime_id"),
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "broker_account_id": binding.broker_account_id,
            "trade_date": run.trade_date.isoformat(),
            "execution_plan_id": runtime_authority.get("execution_plan_id"),
            "outbox_authority_sha256": outbox_authority.get("authority_sha256"),
            "outbox_complete": outbox_authority.get("complete") is True,
            "outbox_replacement_safe": outbox_authority.get("replacement_safe") is True,
            "outbox_ambiguous_command_count": outbox_ambiguous,
            "outbox_confirmed_broker_side_effect_count": outbox_confirmed,
            "exact_order_count": len(exact_orders),
            "exact_unattributed_order_count": len(exact_unattributed_orders),
            "exact_unattributed_trade_count": len(exact_unattributed_trades),
            "exact_open_order_count": len(exact_open_orders) + len(exact_unattributed_open_orders),
            "exact_unattributed_open_order_count": len(exact_unattributed_open_orders),
            "exact_broker_side_effect_count": exact_side_effect_count,
            "foreign_order_count": len(orders)
            + len(unattributed_orders)
            - len(exact_orders)
            - len(exact_unattributed_orders),
            "foreign_trade_count": len(unattributed_trades) - len(exact_unattributed_trades),
            "identity_conflicts": authority_conflicts,
            "orders": retained_orders,
        }
        return {**payload, "authority_sha256": canonical_json_sha256(payload)}

    def _reconcile_preplan_unknown_miniqmt_run(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
        retry_claim_token: str | None = None,
    ) -> tuple[SimulationDailyRun, SimulationSchedulerBindingResult | None]:
        raw_failure = run.run_payload_json.get("miniqmt_preplan_unknown_failure")
        failure = raw_failure if isinstance(raw_failure, dict) else None
        if failure is None:
            diagnostic = run.run_payload_json.get("pre_run_failure")
            if isinstance(diagnostic, dict):
                failure = self._preplan_unknown_failure_evidence(binding=binding, diagnostic=diagnostic)
        runtime_ids = (
            sorted({str(item or "").strip() for item in failure.get("runtime_ids", []) if str(item or "").strip()})
            if isinstance(failure, dict) and isinstance(failure.get("runtime_ids"), list)
            else []
        )
        runtime_authorities = (
            [dict(item) for item in failure.get("runtime_authorities", []) if isinstance(item, Mapping)]
            if isinstance(failure, dict) and isinstance(failure.get("runtime_authorities"), list)
            else []
        )
        runtime_authority = runtime_authorities[0] if len(runtime_authorities) == 1 else {}
        identity_conflicts: list[str] = []
        if not isinstance(failure, dict):
            identity_conflicts.append("failure_evidence_missing")
        elif failure.get("binding_id") != binding.binding_id:
            identity_conflicts.append("binding_id_conflict")
        if len(runtime_ids) != 1:
            identity_conflicts.append("runtime_identity_not_exactly_one")
        if len(runtime_authorities) != 1:
            identity_conflicts.append("runtime_authority_not_exactly_one")
        if isinstance(failure, dict):
            expected_failure_fingerprint = canonical_json_sha256(
                {
                    "schema_version": "miniqmt_preplan_unknown_failure_identity_v2",
                    "binding_id": binding.binding_id,
                    "runtime_ids": runtime_ids,
                    "failure_count": failure.get("failure_count"),
                    "all_failures_sha256": failure.get("all_failures_sha256"),
                    "omitted_failures_sha256": failure.get("omitted_failures_sha256"),
                    "runtime_authorities": runtime_authorities,
                }
            )
            if failure.get("failure_fingerprint") != expected_failure_fingerprint:
                identity_conflicts.append("failure_fingerprint_conflict")
        runtime_id = runtime_ids[0] if len(runtime_ids) == 1 else None
        if runtime_id is not None and runtime_authority.get("runtime_id") != runtime_id:
            identity_conflicts.append("runtime_authority_id_conflict")
        expected_authority = {
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "broker_account_id": binding.broker_account_id,
            "trade_date": trade_date.isoformat(),
        }
        for field_name, expected_value in expected_authority.items():
            if runtime_authority.get(field_name) != expected_value:
                identity_conflicts.append(f"runtime_authority_{field_name}_conflict")
        authority_plan_id = str(runtime_authority.get("execution_plan_id") or "").strip()
        authority_plan_hash = str(runtime_authority.get("execution_plan_hash") or "").strip()
        authority_plan: ExecutionPlan | None = None
        if not authority_plan_id or not authority_plan_hash:
            identity_conflicts.append("runtime_authority_plan_missing")
        else:
            try:
                authority_plan = self.repository.get_execution_plan(authority_plan_id)
            except Exception:  # noqa: BLE001 - converted to bounded durable identity conflict below.
                identity_conflicts.append("runtime_authority_plan_readback_failed")
            else:
                if (
                    authority_plan.plan_hash != authority_plan_hash
                    or authority_plan.binding_id != binding.binding_id
                    or authority_plan.target_trade_date != trade_date
                ):
                    identity_conflicts.append("runtime_authority_plan_owner_conflict")
                expected_runtime_id = miniqmt_kernel_runtime_id(
                    plan_id=authority_plan.plan_id,
                    binding_id=binding.binding_id,
                    trade_date=trade_date,
                )
                if runtime_id != expected_runtime_id:
                    identity_conflicts.append("runtime_authority_deterministic_id_conflict")
        if type(runtime_authority.get("lifecycle_generation")) is not int:
            identity_conflicts.append("runtime_authority_generation_missing")
        if type(runtime_authority.get("attempt_token")) is not int:
            identity_conflicts.append("runtime_authority_attempt_missing")
        outbox_authority = runtime_authority.get("outbox_authority")
        if not isinstance(outbox_authority, Mapping):
            identity_conflicts.append("outbox_authority_missing")
        else:
            outbox_identity = {
                "runtime_id": runtime_id,
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "broker_account_id": binding.broker_account_id,
                "trade_date": trade_date.isoformat(),
                "execution_plan_id": authority_plan_id,
                "execution_plan_hash": authority_plan_hash,
            }
            for field_name, expected_value in outbox_identity.items():
                if outbox_authority.get(field_name) != expected_value:
                    identity_conflicts.append(f"outbox_authority_{field_name}_conflict")
            authority_sha256 = outbox_authority.get("authority_sha256")
            if not isinstance(authority_sha256, str) or authority_sha256 != canonical_json_sha256(
                {key: value for key, value in outbox_authority.items() if key != "authority_sha256"}
            ):
                identity_conflicts.append("outbox_authority_hash_conflict")
        if identity_conflicts:
            proof = {
                "schema_version": "miniqmt_preplan_unknown_reconciliation_v1",
                "status": "RUNTIME_IDENTITY_INVALID_TERMINAL",
                "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RUNTIME_IDENTITY_INVALID",
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
                "runtime_ids": runtime_ids,
                "identity_conflicts": identity_conflicts,
                "broker_side_effect_state": "UNKNOWN",
                "automatic": True,
                "auto_retry": False,
                "replacement_plan_created": False,
            }
            terminal = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.FAILED_TERMINAL,
                payload_patch={
                    "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                    "miniqmt_preplan_unknown_reconciliation": proof,
                },
            )
            terminal = self.repository.clear_simulation_retry_control(
                run_id=terminal.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                expected_claim_token=retry_claim_token,
            )
            return terminal, self._preplan_unknown_reconciliation_result(
                binding=binding,
                run=terminal,
                data_source=data_source,
                proof=proof,
            )

        assert runtime_id is not None
        run, runtime_release_evidence, effective_runtime_authority = self._release_preplan_unknown_kernel_runtime(
            binding=binding,
            run=run,
            runtime_id=runtime_id,
            runtime_authority=runtime_authority,
            trade_date=trade_date,
        )
        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )
        sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
        sync_conflicts = self._preplan_sync_receipt_conflicts(
            binding=binding,
            trade_date=trade_date,
            sync_result=sync_result,
        )
        if sync_conflicts:
            reconciliation = {
                "run_status_gate": {
                    "status": "WARNING",
                    "reason": "sync_receipt_identity_or_snapshot_invalid",
                    "account_level_issue_count": 0,
                }
            }
        else:
            reconciliation = self._reconcile_after_submit_with_timeout(
                binding=binding,
                run=run,
                context=context,
            )
            if not isinstance(reconciliation, dict):
                raise RuntimeConfigInvalidError(
                    "MiniQMT preplan UNKNOWN reconciliation returned no typed readback",
                    context={
                        "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION_INVALID",
                        "stage": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION",
                        "run_id": run.run_id,
                        "binding_id": binding.binding_id,
                        "runtime_id": runtime_id,
                    },
                )
        run_status_gate = (
            reconciliation.get("run_status_gate") if isinstance(reconciliation.get("run_status_gate"), dict) else {}
        )
        exact_broker_authority = self._preplan_exact_broker_authority(
            binding=binding,
            run=run,
            context=context,
            runtime_authority=effective_runtime_authority,
        )
        open_order_count = int(exact_broker_authority["exact_open_order_count"])
        broker_side_effect_count = int(exact_broker_authority["exact_broker_side_effect_count"])
        run_status = str(run_status_gate.get("status") or "").strip().upper()
        if exact_broker_authority["outbox_complete"] is not True:
            sync_conflicts.append("outbox_authority_incomplete")
        if int(exact_broker_authority["outbox_ambiguous_command_count"]) > 0:
            sync_conflicts.append("outbox_outcome_ambiguous")
        if (
            exact_broker_authority["outbox_replacement_safe"] is not True
            and broker_side_effect_count == 0
            and "outbox_outcome_ambiguous" not in sync_conflicts
        ):
            sync_conflicts.append("outbox_commands_not_closed_safe")
        sync_conflicts.extend(str(item) for item in exact_broker_authority["identity_conflicts"])
        account_level_issue_count = int(run_status_gate.get("account_level_issue_count") or 0)
        if account_level_issue_count > 0:
            sync_conflicts.append("account_level_reconciliation_issues_present")
        proof_base = {
            "schema_version": "miniqmt_preplan_unknown_reconciliation_v1",
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": trade_date.isoformat(),
            "runtime_id": runtime_id,
            "failure_fingerprint": failure.get("failure_fingerprint") if isinstance(failure, dict) else None,
            "run_status_gate": dict(run_status_gate),
            "open_order_count": open_order_count,
            "broker_side_effect_count": broker_side_effect_count,
            "sync_completed": sync_result is not None,
            "sync_conflicts": sync_conflicts,
            "account_level_issue_count": account_level_issue_count,
            "runtime_release_status": runtime_release_evidence.get("status"),
            "runtime_released_at": runtime_release_evidence.get("released_at"),
            "runtime_authority": effective_runtime_authority,
            "exact_broker_authority": exact_broker_authority,
            "automatic": True,
            "replacement_plan_created": False,
        }
        latest = self.repository.get_simulation_daily_run(run.run_id)
        if (
            run_status == "SUCCEEDED"
            and not sync_conflicts
            and open_order_count == 0
            and broker_side_effect_count == 0
            and exact_broker_authority["outbox_replacement_safe"] is True
        ):
            proof = {
                **proof_base,
                "status": "NO_BROKER_SIDE_EFFECT",
                "reason_code": "MINIQMT_PREPLAN_UNKNOWN_NO_BROKER_SIDE_EFFECT",
                "auto_retry": True,
                "replacement_plan_authorized": True,
            }
            cleared = self.repository.update_simulation_daily_run(
                latest.run_id,
                status=SimulationDailyRunStatus.FAILED_RETRYABLE,
                payload_patch={
                    "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                    "broker_called": False,
                    "submitted_intents": 0,
                    "failed_intents": 0,
                    "miniqmt_preplan_unknown_reconciliation": proof,
                },
                payload_unset=(
                    "broker_side_effect_state",
                    "pre_run_failure",
                    "pre_run_failure_last_observed_at",
                    "pre_run_failure_observed_after_terminal",
                    "submit_failure",
                ),
            )
            cleared = self.repository.clear_simulation_retry_control(
                run_id=cleared.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                expected_claim_token=retry_claim_token,
            )
            return cleared, None

        if run_status == "SUCCEEDED" and not sync_conflicts and open_order_count == 0 and broker_side_effect_count > 0:
            proof = {
                **proof_base,
                "status": "BROKER_SIDE_EFFECT_RECONCILED_TERMINAL",
                "reason_code": "MINIQMT_PREPLAN_UNKNOWN_BROKER_SIDE_EFFECT_RECONCILED",
                "auto_retry": False,
            }
            terminal = self.repository.update_simulation_daily_run(
                latest.run_id,
                status=SimulationDailyRunStatus.FAILED_TERMINAL,
                payload_patch={
                    "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                    "broker_called": True,
                    "broker_side_effect_state": "CONFIRMED_RECONCILED",
                    "miniqmt_preplan_unknown_reconciliation": proof,
                },
                payload_unset=(
                    "pre_run_failure",
                    "pre_run_failure_last_observed_at",
                    "submit_failure",
                ),
            )
            terminal = self.repository.clear_simulation_retry_control(
                run_id=terminal.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                expected_claim_token=retry_claim_token,
            )
            return terminal, self._preplan_unknown_reconciliation_result(
                binding=binding,
                run=terminal,
                data_source=data_source,
                proof=proof,
            )

        proof = {
            **proof_base,
            "status": "RECONCILIATION_PENDING",
            "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION_PENDING",
            "auto_retry": True,
        }
        pending = self.repository.update_simulation_daily_run(
            latest.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "broker_side_effect_state": "UNKNOWN",
                "miniqmt_preplan_unknown_reconciliation": proof,
            },
        )
        return pending, self._preplan_unknown_reconciliation_result(
            binding=binding,
            run=pending,
            data_source=data_source,
            proof=proof,
        )

    def _clear_pre_run_failure_after_planning(
        self,
        build_result: SimulationPlanBuildResult,
    ) -> SimulationPlanBuildResult:
        preplan_proof = build_result.run.run_payload_json.get("miniqmt_preplan_unknown_reconciliation")
        close_safe_preplan_proof = (
            isinstance(preplan_proof, dict)
            and preplan_proof.get("status") == "NO_BROKER_SIDE_EFFECT"
            and preplan_proof.get("replacement_plan_created") is not True
        )
        if not close_safe_preplan_proof and not any(
            key in build_result.run.run_payload_json for key in ("pre_run_failure", "selection_inference_pending")
        ):
            return build_result
        payload_patch = (
            {
                "miniqmt_preplan_unknown_reconciliation": {
                    **preplan_proof,
                    "replacement_plan_created": True,
                    "replacement_plan_id": build_result.execution_plan.plan_id,
                    "replacement_plan_hash": build_result.execution_plan.plan_hash,
                }
            }
            if close_safe_preplan_proof
            else None
        )
        cleared = self.repository.update_simulation_daily_run(
            build_result.run.run_id,
            payload_patch=payload_patch,
            payload_unset=(
                "pre_run_failure",
                "pre_run_failure_last_observed_at",
                "pre_run_failure_observed_after_terminal",
                "submit_failure",
                "selection_inference_pending",
            ),
        )
        return replace(build_result, run=cleared)

    def post_close_reconcile_once(
        self,
        *,
        trade_date: date,
        data_source: str,
        limit: int = 100,
        broker_backend: SimulationBrokerBackend | str | None = None,
        strategy_id: str | None = None,
        as_of_time: datetime | None = None,
    ) -> SimulationSchedulerRunOnceResult:
        if limit <= 0:
            raise ValueError("limit must be positive")
        self._ensure_lifecycle_trading_day(trade_date=trade_date)
        if as_of_time is not None:
            as_of_time = self._scheduler_time(as_of_time)
        normalized_backend = self._normalized_backend(broker_backend) if broker_backend is not None else None
        miniqmt_in_scope = normalized_backend in {None, SimulationBrokerBackend.MINIQMT_SIM}
        localsim_in_scope = normalized_backend in {None, SimulationBrokerBackend.LOCAL_SIM}
        kernel_product_tick_failures = (
            self._advance_miniqmt_quote_ingress_lifecycle() if miniqmt_in_scope else []
        )
        terminalized: list[dict[str, Any]] = []
        if miniqmt_in_scope:
            terminalized.extend(
                self._terminalize_post_close_miniqmt_runs(
                    trade_date=trade_date,
                    broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                )
            )
        if localsim_in_scope:
            terminalized.extend(
                self._terminalize_post_close_localsim_runs(
                    trade_date=trade_date,
                    broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                    strategy_id=strategy_id,
                    limit=limit,
                    as_of_time=as_of_time,
                )
            )
        unmatched_failure_result = self._unmatched_kernel_product_failure_result(
            failures=kernel_product_tick_failures,
            data_source=data_source,
        )
        return SimulationSchedulerRunOnceResult(
            trade_date=trade_date,
            data_source=data_source,
            submit=False,
            total_bindings=0,
            results=(unmatched_failure_result,) if unmatched_failure_result is not None else (),
            stale_run_results=tuple(terminalized),
            as_of_time=as_of_time,
            schedule_windows=self._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time),
        )

    def _ensure_lifecycle_trading_day(self, *, trade_date: date) -> None:
        service = self.trading_calendar_service
        if service is None:
            return
        try:
            status = self._lifecycle_trading_day_status(service=service, trade_date=trade_date)
        except DataUnavailableError as exc:
            context = getattr(exc, "context", None)
            if isinstance(context, dict) and context.get("reason_code"):
                raise
            raise DataUnavailableError(
                "simulation lifecycle scheduler trading-day gate could not load authoritative calendar status",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_UNAVAILABLE",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "error_context": context,
                },
            ) from exc
        except Exception as exc:
            raise DataUnavailableError(
                "simulation lifecycle scheduler trading-day gate failed",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_FAILED",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                },
            ) from exc
        if not bool(status.get("is_trading_day")):
            raise DataUnavailableError(
                "simulation lifecycle scheduler skipped non-trading day",
                context={
                    "reason_code": "SIMULATION_LIFECYCLE_NON_TRADING_DAY",
                    "trade_date": trade_date.isoformat(),
                    "next_trading_day": status.get("next_trading_day"),
                    "service": type(service).__name__,
                    "policy": "block_inner_run_once_roll_forward_plan_submit",
                },
            )

    def _lifecycle_trading_day_status(self, *, service: Any, trade_date: date) -> dict[str, Any]:
        status_method = getattr(service, "status", None)
        if callable(status_method):
            raw_status = dict(status_method(as_of_date=trade_date))
            if "is_trading_day" not in raw_status:
                raise DataUnavailableError(
                    "simulation lifecycle scheduler trading calendar status is missing is_trading_day",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "status_keys": sorted(str(key) for key in raw_status),
                    },
                )
            is_trading_day = raw_status.get("is_trading_day")
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation lifecycle scheduler trading calendar status has non-boolean is_trading_day",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            raw_status.setdefault("as_of_date", trade_date.isoformat())
            raw_status["is_trading_day"] = is_trading_day
            return raw_status
        is_trading_day_method = getattr(service, "is_trading_day", None)
        if callable(is_trading_day_method):
            is_trading_day = is_trading_day_method(trade_date)
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation lifecycle scheduler is_trading_day returned non-boolean value",
                    context={
                        "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            return {
                "as_of_date": trade_date.isoformat(),
                "is_trading_day": is_trading_day,
                "next_trading_day": None if is_trading_day else self._next_lifecycle_trading_day_iso(trade_date),
            }
        ensure_method = getattr(service, "ensure_trading_day", None)
        if callable(ensure_method):
            ensure_method(trade_date)
            return {"as_of_date": trade_date.isoformat(), "is_trading_day": True, "next_trading_day": None}
        raise DataUnavailableError(
            "simulation lifecycle scheduler trading calendar service lacks status/is_trading_day/ensure_trading_day",
            context={
                "reason_code": "SIMULATION_LIFECYCLE_TRADING_CALENDAR_METHOD_MISSING",
                "trade_date": trade_date.isoformat(),
                "service": type(service).__name__,
            },
        )

    def _next_lifecycle_trading_day_iso(self, trade_date: date) -> str | None:
        service = self.trading_calendar_service
        if service is None:
            return None
        for method_name in ("next_trading_day", "next_trading_day_after"):
            method = getattr(service, method_name, None)
            if callable(method):
                next_day = method(trade_date)
                return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        return None

    def _post_close_terminalized_binding_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        run_ids: set[str],
    ) -> SimulationSchedulerBindingResult | None:
        if not run_ids:
            return None
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is None or existing.run_id not in run_ids:
            return None
        plan = (
            self.repository.get_execution_plan(existing.execution_plan_id or "") if existing.execution_plan_id else None
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="POST_CLOSE_TERMINALIZED",
            run=existing,
            execution_plan=plan,
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _terminalize_stale_miniqmt_active_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if (
            broker_backend is not None
            and self._normalized_backend(broker_backend) != SimulationBrokerBackend.MINIQMT_SIM
        ):
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _MINIQMT_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date >= trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="STALE_MINIQMT_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._terminalize_stale_miniqmt_run(
                        run=run,
                        scheduler_trade_date=trade_date,
                        as_of_time=as_of_time,
                    ),
                    as_of_time=as_of_time,
                )
                if terminalized_run is not None:
                    terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized[:limit]

    def _terminalize_stale_miniqmt_run(
        self,
        *,
        run: SimulationDailyRun,
        scheduler_trade_date: date,
        as_of_time: datetime | None,
    ) -> dict[str, Any]:
        had_side_effect = bool(run.run_payload_json.get("broker_called") or run.run_payload_json.get("qmt_batch_id"))
        if had_side_effect and self._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json):
            terminalized_run = self._post_close_terminalize_miniqmt_run(run=run, as_of_time=as_of_time)
            if terminalized_run is not None:
                terminalized_run.update(
                    {
                        "cross_day_terminalization": True,
                        "scheduler_trade_date": scheduler_trade_date.isoformat(),
                    }
                )
                return terminalized_run
        next_status = (
            SimulationDailyRunStatus.FAILED_RETRYABLE if had_side_effect else SimulationDailyRunStatus.CANCELLED
        )
        reason_code = (
            "MINIQMT_STALE_ACTIVE_WITH_BROKER_SIDE_EFFECT_UNRESOLVED"
            if had_side_effect
            else "MINIQMT_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
        )
        evidence = {
            "schema_version": "miniqmt_stale_active_run_terminalization_v1",
            "reason": (
                "stale_historical_miniqmt_run_with_broker_side_effect_unresolved"
                if had_side_effect
                else "stale_historical_miniqmt_run_without_broker_side_effect"
            ),
            "reason_code": reason_code,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "stale_trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "had_broker_side_effect": had_side_effect,
            "broker_authoritative_terminalization_attempted": had_side_effect,
            "terminalized_at": (
                self._scheduler_time(as_of_time).isoformat()
                if as_of_time is not None
                else self._scheduler_now().isoformat()
            ),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "stale_active_terminalization": evidence,
                "broker_called": bool(run.run_payload_json.get("broker_called")),
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": evidence["reason"],
            "reason_code": reason_code,
        }

    def _terminalize_stale_localsim_active_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None = None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.LOCAL_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _LOCALSIM_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                trade_date_before=trade_date,
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="STALE_LOCALSIM_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._terminalize_stale_localsim_run(
                        run=run,
                        scheduler_trade_date=trade_date,
                        as_of_time=as_of_time,
                    ),
                    as_of_time=as_of_time,
                )
                if terminalized_run is not None:
                    terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized[:limit]

    def _terminalize_stale_localsim_run(
        self,
        *,
        run: SimulationDailyRun,
        scheduler_trade_date: date,
        as_of_time: datetime | None = None,
    ) -> dict[str, Any]:
        had_side_effect = self._localsim_run_had_side_effect(run.run_payload_json)
        if had_side_effect:
            terminalized = self._terminalize_historical_localsim_durable_run_if_safe(
                run=run,
                scheduler_trade_date=scheduler_trade_date,
                as_of_time=as_of_time,
            )
            if terminalized is not None:
                return terminalized
        next_status = (
            SimulationDailyRunStatus.FAILED_RETRYABLE if had_side_effect else SimulationDailyRunStatus.CANCELLED
        )
        reason_code = (
            "LOCALSIM_STALE_ACTIVE_WITH_BROKER_SIDE_EFFECT"
            if had_side_effect
            else "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
        )
        evidence = {
            "schema_version": "localsim_stale_active_run_terminalization_v1",
            "reason": (
                "stale_historical_localsim_run_with_broker_side_effect"
                if had_side_effect
                else "stale_historical_localsim_run_without_broker_side_effect"
            ),
            "reason_code": reason_code,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "stale_trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "terminal_status": next_status.value,
            "had_broker_side_effect": had_side_effect,
            "terminalized_at": self._scheduler_now().isoformat(),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "last_stage": next_status.value,
                "localsim_stale_active_terminalization": evidence,
                "stale_active_terminalization": evidence,
                "broker_called": bool(run.run_payload_json.get("broker_called")),
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": evidence["reason"],
            "reason_code": reason_code,
        }

    def _terminalize_stale_localsim_failed_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.LOCAL_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        for failed_status in (
            SimulationDailyRunStatus.FAILED_RETRYABLE,
            SimulationDailyRunStatus.FAILED_TERMINAL,
        ):
            for run in self.repository.list_simulation_daily_runs(
                trade_date_before=trade_date,
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                status=failed_status,
                limit=limit,
            ):
                # A run already closed by a terminal recovery carrier is a permanent
                # no-op for this sweep; skip it before claiming a retry attempt so the
                # claim/clear write pair is not paid on every scheduler tick. Carriers
                # present but not dict-shaped fall through so the isolated path keeps
                # failing loud with the typed invalid-carrier error.
                if any(
                    isinstance(run.run_payload_json.get(carrier_field), dict)
                    for carrier_field in _HISTORICAL_LOCALSIM_RECOVERY_TERMINAL_CARRIER_FIELDS
                ):
                    continue
                terminalized_run = self._run_recovery_item_isolated(
                    stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._terminalize_historical_localsim_durable_run_if_safe(
                        run=run,
                        scheduler_trade_date=trade_date,
                        as_of_time=as_of_time,
                    ),
                    as_of_time=as_of_time,
                )
                if terminalized_run is not None:
                    terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized

    def _terminalize_historical_localsim_durable_run_if_safe(
        self,
        *,
        run: SimulationDailyRun,
        scheduler_trade_date: date,
        as_of_time: datetime | None,
    ) -> dict[str, Any] | None:
        raw_outbox = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw_outbox is None:
            return None
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw_outbox)
        except Exception as exc:
            raise DataUnavailableError(
                "Historical LocalSim durable projection outbox is invalid",
                context={
                    "reason_code": "LOCALSIM_HISTORICAL_RECOVERY_OUTBOX_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": run.execution_plan_id,
                },
            ) from exc
        if outbox.status != LocalSimProjectionOutboxStatus.PROJECTED:
            return None
        for carrier_field in _HISTORICAL_LOCALSIM_RECOVERY_TERMINAL_CARRIER_FIELDS:
            if carrier_field not in run.run_payload_json:
                continue
            if not isinstance(run.run_payload_json[carrier_field], dict):
                raise DataUnavailableError(
                    "Historical LocalSim recovery failure carrier is invalid",
                    context={
                        "reason_code": "LOCALSIM_HISTORICAL_RECOVERY_FAILURE_CARRIER_INVALID",
                        "run_id": run.run_id,
                        "binding_id": run.binding_id,
                        "plan_id": run.execution_plan_id,
                        "field": carrier_field,
                        "actual_type": type(run.run_payload_json[carrier_field]).__name__,
                    },
                )
            return None
        if not run.execution_plan_id:
            raise DataUnavailableError(
                "Historical LocalSim durable recovery requires a frozen execution plan",
                context={
                    "reason_code": "LOCALSIM_HISTORICAL_RECOVERY_PLAN_MISSING",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                },
            )
        plan = self.repository.get_execution_plan(run.execution_plan_id)
        binding = self.repository.get_simulation_release_binding(run.binding_id)
        runtime_release = self.repository.get_strategy_runtime_release(run.release_id)
        self._validate_historical_localsim_recovery_identity(
            run=run,
            plan=plan,
            binding=binding,
            runtime_release=runtime_release,
        )
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        self._validate_local_sim_post_close_state_closure(run)
        active_states = tuple(state for state in states if not state.is_terminal)
        if active_states:
            if run.status not in {
                SimulationDailyRunStatus.FAILED_RETRYABLE,
                SimulationDailyRunStatus.FAILED_TERMINAL,
            }:
                return None
            try:
                self._assert_local_sim_plan_uses_twap(binding=binding, plan=plan)
            except RuntimeConfigInvalidError as exc:
                error_context = getattr(exc, "context", None)
                reason_code = error_context.get("reason_code") if isinstance(error_context, Mapping) else None
                if reason_code != "LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED":
                    raise
                return self._terminalize_historical_localsim_legacy_plan_run(
                    run=run,
                    plan=plan,
                    binding=binding,
                    outbox=outbox,
                    states=states,
                    active_states=active_states,
                    policy_error=exc,
                    scheduler_trade_date=scheduler_trade_date,
                    as_of_time=as_of_time,
                )
            return self._recover_historical_failed_localsim_active_generation(
                run=run,
                plan=plan,
                binding=binding,
                runtime_release=runtime_release,
                outbox=outbox,
                states=states,
                scheduler_trade_date=scheduler_trade_date,
                as_of_time=as_of_time,
            )
        if run.status == SimulationDailyRunStatus.FAILED_TERMINAL:
            return None
        self._readback_local_sim_recovery_generation(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=run.trade_date,
            as_of_time=as_of_time,
            outbox=outbox,
            states=states,
        )
        recovery_evidence = {
            "schema_version": "localsim_historical_durable_terminalization_v1",
            "reason_code": "LOCALSIM_HISTORICAL_DURABLE_GENERATION_RECOVERED",
            "run_id": run.run_id,
            "binding_id": run.binding_id,
            "plan_id": plan.plan_id,
            "stale_trade_date": run.trade_date.isoformat(),
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "previous_status": run.status.value,
            "outbox_id": outbox.outbox_id,
            "receipt_id": outbox.receipt_id,
            "generation": outbox.generation,
            "authoritative_state_count": len(states),
            "parent_resubmitted": False,
            "broker_replayed": False,
            "projection_replayed": False,
            "verified_at": (
                self._scheduler_time(as_of_time) if as_of_time is not None else self._scheduler_now()
            ).isoformat(),
        }
        terminalized = self._post_close_terminalize_localsim_run(
            run=run,
            as_of_time=as_of_time,
            historical_recovery_evidence=recovery_evidence,
        )
        if terminalized is None:
            raise DataUnavailableError(
                "Historical LocalSim durable generation passed readback but has no terminal persistence decision",
                context={
                    "reason_code": "LOCALSIM_HISTORICAL_RECOVERY_TERMINAL_STATUS_MISSING",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                },
            )
        terminalized.update(
            {
                "cross_day_terminalization": True,
                "scheduler_trade_date": scheduler_trade_date.isoformat(),
                "durable_generation_readback": True,
            }
        )
        return terminalized

    def _recover_historical_failed_localsim_active_generation(
        self,
        *,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        outbox: LocalSimProjectionOutboxV1,
        states: tuple[LocalSimExecutionStateV1, ...],
        scheduler_trade_date: date,
        as_of_time: datetime | None,
    ) -> dict[str, Any]:
        """Finish an old failed run from its exact active durable generation."""

        is_terminal_failure = run.status == SimulationDailyRunStatus.FAILED_TERMINAL
        evidence_suffix = "terminal" if is_terminal_failure else "retryable"
        reason_prefix = (
            "LOCALSIM_HISTORICAL_FAILED_TERMINAL" if is_terminal_failure else "LOCALSIM_HISTORICAL_FAILED_RETRYABLE"
        )

        persistence = run.run_payload_json.get("local_sim_persistence")
        if not isinstance(persistence, dict) or persistence.get("terminal") is not False:
            raise DataUnavailableError(
                "Historical failed LocalSim active generation has invalid persistence authority",
                context={
                    "reason_code": f"{reason_prefix}_PERSISTENCE_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "persistence_type": type(persistence).__name__,
                    "persistence_terminal": (persistence.get("terminal") if isinstance(persistence, dict) else None),
                },
            )
        recovery_as_of = datetime.combine(
            run.trade_date,
            _POST_CLOSE_RECONCILE_TIME,
            tzinfo=SCHEDULER_TZ,
        )
        context = self._load_existing_plan_context(
            runtime_release=runtime_release,
            binding=binding,
            plan=plan,
            trade_date=run.trade_date,
            as_of_time=recovery_as_of,
        )
        recovery_data_source = str(context.market_data_source or "").strip()
        if not recovery_data_source:
            raise DataUnavailableError(
                "Historical failed LocalSim recovery has no authoritative market-data source",
                context={
                    "reason_code": f"{reason_prefix}_MARKET_DATA_SOURCE_MISSING",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        self._readback_active_local_sim_durable_continuation(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=run.trade_date,
            as_of_time=recovery_as_of,
            context=context,
        )
        driven = self._run_local_sim_binding_single_flight(
            binding=binding,
            trade_date=run.trade_date,
            context={
                "stage": f"STALE_LOCALSIM_FAILED_{evidence_suffix.upper()}_ACTIVE_RECOVERY",
                "run_id": run.run_id,
                "binding_id": binding.binding_id,
                "trade_date": run.trade_date.isoformat(),
                "scheduler_trade_date": scheduler_trade_date.isoformat(),
            },
            func=lambda: self._drive_existing_local_sim(
                binding=binding,
                run=run,
                plan=plan,
                runtime_release=runtime_release,
                trade_date=run.trade_date,
                data_source=recovery_data_source,
                as_of_time=recovery_as_of,
                context=context,
            ),
        )
        latest = self.repository.get_simulation_daily_run(run.run_id)
        latest_states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        self._validate_local_sim_post_close_state_closure(latest)
        latest_persistence = latest.run_payload_json.get("local_sim_persistence")
        remaining_active = tuple(state for state in latest_states if not state.is_terminal)
        if (
            remaining_active
            or not isinstance(latest_persistence, dict)
            or latest_persistence.get("terminal") is not True
        ):
            raise DataUnavailableError(
                "Historical failed LocalSim generation did not reach exact terminal closure",
                context={
                    "reason_code": f"{reason_prefix}_CLOSURE_INCOMPLETE",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "active_state_ids": sorted(state.state_id for state in remaining_active),
                    "persistence_type": type(latest_persistence).__name__,
                    "persistence_terminal": (
                        latest_persistence.get("terminal") if isinstance(latest_persistence, dict) else None
                    ),
                },
            )
        try:
            latest_outbox = LocalSimProjectionOutboxV1.model_validate(
                latest.run_payload_json.get("local_sim_projection_outbox_v1")
            )
        except Exception as exc:
            raise DataUnavailableError(
                "Historical failed LocalSim terminal outbox is invalid",
                context={
                    "reason_code": f"{reason_prefix}_OUTBOX_INVALID",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                },
            ) from exc
        recovery_evidence = {
            "schema_version": f"localsim_historical_failed_{evidence_suffix}_active_recovery_v1",
            "reason_code": f"{reason_prefix}_ACTIVE_GENERATION_RECOVERED",
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "plan_id": plan.plan_id,
            "previous_status": run.status.value,
            "terminal_status": latest.status.value,
            "stale_trade_date": run.trade_date.isoformat(),
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "predecessor_outbox_id": outbox.outbox_id,
            "predecessor_generation": outbox.generation,
            "terminal_outbox_id": latest_outbox.outbox_id,
            "terminal_generation": latest_outbox.generation,
            "predecessor_state_count": len(states),
            "predecessor_state_set_sha256": canonical_json_sha256(
                [
                    {"state_id": state.state_id, "state_hash": state.state_hash}
                    for state in sorted(states, key=lambda item: item.state_id)
                ]
            ),
            "terminal_state_count": len(latest_states),
            "terminal_state_set_sha256": canonical_json_sha256(
                [
                    {"state_id": state.state_id, "state_hash": state.state_hash}
                    for state in sorted(latest_states, key=lambda item: item.state_id)
                ]
            ),
            "parent_resubmitted": False,
            "predecessor_projection_replayed": False,
            "durable_minute_loop_advanced": True,
            "recovery_as_of": recovery_as_of.isoformat(),
            "verified_at": (
                self._scheduler_time(as_of_time) if as_of_time is not None else self._scheduler_now()
            ).isoformat(),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=latest.status,
            payload_patch={
                "last_stage": latest.status.value,
                f"localsim_historical_failed_{evidence_suffix}_active_recovery_v1": recovery_evidence,
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": f"localsim_historical_failed_{evidence_suffix}_active_generation_recovered",
            "reason_code": recovery_evidence["reason_code"],
            f"historical_failed_{evidence_suffix}_active_recovery": True,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "driven_status": driven.status,
        }

    def _terminalize_historical_localsim_legacy_plan_run(
        self,
        *,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        outbox: LocalSimProjectionOutboxV1,
        states: tuple[LocalSimExecutionStateV1, ...],
        active_states: tuple[LocalSimExecutionStateV1, ...],
        policy_error: RuntimeConfigInvalidError,
        scheduler_trade_date: date,
        as_of_time: datetime | None,
    ) -> dict[str, Any]:
        """Terminally close a historical failed run whose frozen plan is retired legacy policy.

        The TWAP-only runtime authority permanently rejects the frozen plan, so the
        durable minute loop can never lawfully advance it again; leaving the run in
        FAILED_RETRYABLE would retry a permanent policy rejection forever. Terminalize
        as FAILED_TERMINAL with loud typed evidence instead. This path performs no
        runtime-context load, no market-data load, no broker call, no parent resubmit,
        no predecessor projection replay and no minute-loop advance; predecessor and
        current durable states stay immutable audit facts.
        """

        error_context = dict(policy_error.context) if isinstance(policy_error.context, Mapping) else {}
        evidence = {
            "schema_version": "localsim_historical_legacy_plan_terminalization_v1",
            "reason_code": "LOCALSIM_HISTORICAL_FAILED_RUN_LEGACY_PLAN_RETIRED",
            "run_id": run.run_id,
            "binding_id": run.binding_id,
            "plan_id": plan.plan_id,
            "plan_execution_policy_version_id": error_context.get("plan_execution_policy_version_id"),
            "plan_algo_code": error_context.get("plan_algo_code"),
            "required_algo_code": error_context.get("required_algo_code") or "TWAP",
            "retired_policy_reason_code": error_context.get("reason_code"),
            "stale_trade_date": run.trade_date.isoformat(),
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "previous_status": run.status.value,
            "terminal_status": SimulationDailyRunStatus.FAILED_TERMINAL.value,
            "outbox_id": outbox.outbox_id,
            "receipt_id": outbox.receipt_id,
            "generation": outbox.generation,
            "authoritative_state_count": len(states),
            "active_state_count": len(active_states),
            "authoritative_state_set_sha256": canonical_json_sha256(
                [
                    {"state_id": state.state_id, "state_hash": state.state_hash}
                    for state in sorted(states, key=lambda item: item.state_id)
                ]
            ),
            "historical_broker_called": bool(run.run_payload_json.get("broker_called")),
            "parent_resubmitted": False,
            "broker_replayed": False,
            "predecessor_projection_replayed": False,
            "durable_minute_loop_advanced": False,
            "legacy_execution_restored": False,
            "fallback_used": False,
            "runtime_context_loaded": False,
            "market_data_loaded": False,
            "verified_at": (
                self._scheduler_time(as_of_time) if as_of_time is not None else self._scheduler_now()
            ).isoformat(),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_TERMINAL,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                "localsim_historical_legacy_plan_terminalization_v1": evidence,
            },
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": "localsim_historical_failed_run_legacy_plan_retired",
            "reason_code": evidence["reason_code"],
            "historical_failed_legacy_plan_terminalization": True,
            "cross_day_terminalization": True,
            "scheduler_trade_date": scheduler_trade_date.isoformat(),
            "durable_minute_loop_advanced": False,
            "legacy_execution_restored": False,
        }

    @staticmethod
    def _validate_historical_localsim_recovery_identity(
        *,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
    ) -> None:
        expected = {
            "strategy_id": run.strategy_id,
            "package_id": run.package_id,
            "manifest_sha256": run.manifest_sha256,
            "release_id": run.release_id,
            "release_hash": run.release_hash,
            "binding_id": run.binding_id,
            "binding_hash": run.binding_hash,
            "account_group_id": run.account_group_id,
            "strategy_slot_id": run.strategy_slot_id,
            "trade_date": run.trade_date.isoformat(),
            "execution_plan_id": run.execution_plan_id,
            "execution_plan_hash": run.execution_plan_hash,
            "broker_backend": SimulationBrokerBackend.LOCAL_SIM.value,
        }
        actual = {
            "strategy_id": plan.strategy_id,
            "package_id": plan.package_id,
            "manifest_sha256": binding.manifest_sha256,
            "release_id": plan.release_id,
            "release_hash": plan.release_hash,
            "binding_id": plan.binding_id,
            "binding_hash": plan.binding_hash,
            "account_group_id": plan.account_group_id,
            "strategy_slot_id": plan.strategy_slot_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "execution_plan_id": plan.plan_id,
            "execution_plan_hash": plan.plan_hash,
            "broker_backend": binding.broker_backend.value,
        }
        drift = {
            field: {"expected": expected[field], "actual": actual[field]}
            for field in expected
            if expected[field] != actual[field]
        }
        release_drift = {
            "package_id": runtime_release.package_id,
            "manifest_sha256": runtime_release.manifest_sha256,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
        }
        binding_drift = {
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "release_id": binding.release_id,
            "release_hash": binding.release_hash,
            "binding_id": binding.binding_id,
            "binding_hash": binding.binding_hash,
            "account_group_id": binding.account_group_id,
            "strategy_slot_id": binding.strategy_slot_id,
        }
        for identity_field, actual_value in release_drift.items():
            if actual_value != expected[identity_field]:
                drift[f"runtime_release.{identity_field}"] = {
                    "expected": expected[identity_field],
                    "actual": actual_value,
                }
        for identity_field, actual_value in binding_drift.items():
            if actual_value != expected[identity_field]:
                drift[f"binding.{identity_field}"] = {
                    "expected": expected[identity_field],
                    "actual": actual_value,
                }
        if drift:
            raise DataUnavailableError(
                "Historical LocalSim durable recovery identity does not close over the frozen run",
                context={
                    "reason_code": "LOCALSIM_HISTORICAL_RECOVERY_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": run.execution_plan_id,
                    "identity_drift": drift,
                },
            )

    def _terminalize_post_close_miniqmt_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if not self._is_post_close_reconcile_time(as_of_time=as_of_time):
            return []
        if (
            broker_backend is not None
            and self._normalized_backend(broker_backend) != SimulationBrokerBackend.MINIQMT_SIM
        ):
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _MINIQMT_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date != trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="POST_CLOSE_MINIQMT_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._post_close_terminalize_miniqmt_run(
                        run=run,
                        as_of_time=as_of_time,
                    ),
                    as_of_time=as_of_time,
                )
                if terminalized_run is None:
                    continue
                terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized

    def _terminalize_post_close_localsim_runs(
        self,
        *,
        trade_date: date,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        limit: int,
        as_of_time: datetime | None,
        raise_on_error: bool = False,
    ) -> list[dict[str, Any]]:
        if not self._is_post_close_reconcile_time(as_of_time=as_of_time):
            return []
        if broker_backend is not None and self._normalized_backend(broker_backend) != SimulationBrokerBackend.LOCAL_SIM:
            return []
        terminalized: list[dict[str, Any]] = []
        seen_run_ids: set[str] = set()
        for status in _LOCALSIM_STALE_ACTIVE_STATUSES:
            for run in self.repository.list_simulation_daily_runs(
                broker_backend=SimulationBrokerBackend.LOCAL_SIM,
                strategy_id=strategy_id,
                status=status,
                limit=limit,
            ):
                if run.run_id in seen_run_ids or run.trade_date != trade_date:
                    continue
                seen_run_ids.add(run.run_id)
                terminalized_run = self._run_recovery_item_isolated(
                    stage="POST_CLOSE_LOCALSIM_TERMINALIZATION",
                    run=run,
                    raise_on_error=raise_on_error,
                    func=lambda run=run: self._post_close_terminalize_localsim_run(
                        run=run,
                        as_of_time=as_of_time,
                    ),
                    as_of_time=as_of_time,
                )
                if terminalized_run is None:
                    continue
                terminalized.append(terminalized_run)
                if len(terminalized) >= limit:
                    return terminalized
        return terminalized

    def _post_close_terminalize_localsim_run(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
        historical_recovery_evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        self._validate_local_sim_post_close_state_closure(run)
        terminal_status, reason, reason_code, audit_state = self._localsim_post_close_terminal_status(run)
        if terminal_status is None:
            return None
        payload = run.run_payload_json
        evidence = {
            "schema_version": "localsim_post_close_terminalization_v1",
            "reason": reason,
            "reason_code": reason_code,
            "previous_status": run.status.value,
            "terminal_status": terminal_status.value,
            "trade_date": run.trade_date.isoformat(),
            "as_of_time": self._scheduler_time(as_of_time).isoformat() if as_of_time is not None else None,
            "broker_called": bool(payload.get("broker_called")),
            "submitted_intents": int(payload.get("submitted_intents") or 0),
            "failed_intents": int(payload.get("failed_intents") or 0),
            "local_sim_persistence_status": (
                payload.get("local_sim_persistence", {}).get("status")
                if isinstance(payload.get("local_sim_persistence"), dict)
                else None
            ),
            "local_sim_cash_fit_status": (
                payload.get("local_sim_cash_fit", {}).get("status")
                if isinstance(payload.get("local_sim_cash_fit"), dict)
                else None
            ),
            "had_broker_side_effect": self._localsim_run_had_side_effect(payload),
            "audit_state": audit_state,
        }
        payload_patch: dict[str, Any] = {
            "last_stage": terminal_status.value,
            "localsim_post_close_terminalization": evidence,
        }
        if historical_recovery_evidence is not None:
            payload_patch["localsim_historical_durable_terminalization_v1"] = {
                **historical_recovery_evidence,
                "terminal_status": terminal_status.value,
                "terminal_reason_code": reason_code,
            }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=terminal_status,
            payload_patch=payload_patch,
            payload_unset=("submit_failure", "local_sim_retry_diagnostics")
            if terminal_status == SimulationDailyRunStatus.SUCCEEDED
            else None,
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "broker_backend": updated.broker_backend.value,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": reason,
            "reason_code": reason_code,
            "post_close_terminalization": True,
        }

    def _validate_local_sim_post_close_state_closure(self, run: SimulationDailyRun) -> None:
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        if not states:
            return
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        expected_intents = {intent.intent_id for intent in plan.intents}
        by_intent = {state.intent_id: state for state in states}
        if (
            len(by_intent) != len(states)
            or set(by_intent) != expected_intents
            or any(
                state.run_id != run.run_id
                or state.binding_id != run.binding_id
                or state.trade_date != run.trade_date
                or state.plan_id != plan.plan_id
                for state in states
            )
        ):
            raise DataUnavailableError(
                "LocalSim post-close durable states do not close over the frozen plan",
                context={
                    "reason_code": "LOCALSIM_POST_CLOSE_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(by_intent),
                    "state_count": len(states),
                },
            )
        persistence = run.run_payload_json.get("local_sim_persistence")
        terminal_flag = persistence.get("terminal") if isinstance(persistence, dict) else None
        active_states = tuple(state for state in states if not state.is_terminal)
        if active_states and terminal_flag is not False:
            raise DataUnavailableError(
                "LocalSim post-close run cannot terminalize while durable execution states remain active",
                context={
                    "reason_code": "LOCALSIM_POST_CLOSE_ACTIVE_STATE_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "persistence_terminal": terminal_flag,
                    "active_state_ids": sorted(state.state_id for state in active_states),
                },
            )
        if not active_states and terminal_flag is False:
            raise DataUnavailableError(
                "LocalSim post-close persistence remains non-terminal after all durable states terminated",
                context={
                    "reason_code": "LOCALSIM_POST_CLOSE_PERSISTENCE_STATE_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "plan_id": plan.plan_id,
                    "state_ids": sorted(state.state_id for state in states),
                },
            )

    @staticmethod
    def _localsim_post_close_terminal_status(
        run: SimulationDailyRun,
    ) -> tuple[SimulationDailyRunStatus | None, str | None, str | None, str | None]:
        payload = run.run_payload_json
        persistence = (
            payload.get("local_sim_persistence") if isinstance(payload.get("local_sim_persistence"), dict) else {}
        )
        persistence_status = str(persistence.get("status") or "").upper()
        if persistence and not isinstance(persistence.get("terminal"), bool):
            raise DataUnavailableError(
                "LocalSim post-close persistence terminal flag must be a boolean",
                context={
                    "reason_code": "LOCALSIM_POST_CLOSE_PERSISTENCE_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "terminal_type": type(persistence.get("terminal")).__name__,
                },
            )
        if persistence and persistence["terminal"] is False:
            # A non-terminal durable generation is the execution authority.
            # The binding loop must restore and drive it with the post-close
            # exchange time before this summary-level terminalizer can decide
            # the run status.
            return None, None, None, None
        if persistence_status == "PERSISTED":
            return (
                SimulationDailyRunStatus.SUCCEEDED,
                "localsim_post_close_persisted_success",
                "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS",
                "succeeded_after_close",
            )
        if persistence_status == "PERSISTED_WITH_CAPACITY_RESIDUAL":
            return (
                SimulationDailyRunStatus.FAILED_TERMINAL,
                "localsim_post_close_capacity_residual_terminal_failed",
                "LOCALSIM_POST_CLOSE_CAPACITY_RESIDUAL_TERMINAL",
                "failed_terminal_after_close",
            )
        if bool(payload.get("no_rebalance_required")) and not bool(payload.get("broker_called")):
            return (
                SimulationDailyRunStatus.SUCCEEDED,
                "localsim_post_close_no_rebalance_success",
                "LOCALSIM_POST_CLOSE_NO_REBALANCE_SUCCESS",
                "succeeded_no_rebalance_after_close",
            )
        if SimulationLifecycleScheduler._localsim_run_had_side_effect(payload):
            return (
                SimulationDailyRunStatus.FAILED_TERMINAL,
                "localsim_post_close_missing_durable_persistence_terminal_failed",
                "LOCALSIM_POST_CLOSE_DURABLE_PERSISTENCE_MISSING",
                "failed_terminal_after_close",
            )
        return (
            SimulationDailyRunStatus.CANCELLED,
            "localsim_post_close_no_broker_side_effect_cancelled",
            "LOCALSIM_POST_CLOSE_NO_BROKER_SIDE_EFFECT",
            "cancelled_after_close",
        )

    @staticmethod
    def _localsim_run_had_side_effect(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return True
        if isinstance(payload.get("local_sim_persistence"), dict):
            return True
        submitted_raw = payload.get("submitted_intents")
        if submitted_raw is None:
            return False
        try:
            submitted = int(submitted_raw)
        except (TypeError, ValueError):
            logger.warning(
                "LocalSim terminalization found invalid submitted_intents; treating run as side-effect-bearing",
                extra={
                    "reason_code": "LOCALSIM_TERMINALIZATION_SUBMITTED_INTENTS_INVALID",
                    "payload_run_id": payload.get("run_id"),
                    "payload_strategy_id": payload.get("strategy_id"),
                    "submitted_intents": submitted_raw,
                    "submitted_intents_type": type(submitted_raw).__name__,
                    "policy": "fail_closed_side_effect_assumed",
                },
            )
            return True
        if submitted < 0:
            logger.warning(
                "LocalSim terminalization found negative submitted_intents; treating run as side-effect-bearing",
                extra={
                    "reason_code": "LOCALSIM_TERMINALIZATION_SUBMITTED_INTENTS_NEGATIVE",
                    "payload_run_id": payload.get("run_id"),
                    "payload_strategy_id": payload.get("strategy_id"),
                    "submitted_intents": submitted,
                    "policy": "fail_closed_side_effect_assumed",
                },
            )
            return True
        return submitted > 0

    def _post_close_terminalize_miniqmt_run(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
    ) -> dict[str, Any] | None:
        payload = run.run_payload_json
        if not self._mini_qmt_batch_has_broker_side_effect_evidence(payload):
            return None
        previous_open_order_evidence = self._latest_miniqmt_payload_evidence(payload, "open_order_evidence")
        previous_submit_result_gate = self._latest_miniqmt_payload_evidence(payload, "submit_result_gate")
        fresh_payload, fresh_reconcile = self._fresh_miniqmt_post_close_payload(
            run=run,
            as_of_time=as_of_time,
        )
        terminal_status, reason = self._miniqmt_post_close_terminal_status(fresh_payload)
        event_loop_pending_after_close = self._miniqmt_event_loop_pending_after_close(fresh_payload)
        if terminal_status is None and event_loop_pending_after_close:
            terminal_status = SimulationDailyRunStatus.FAILED_RETRYABLE
            reason = "miniqmt_post_close_event_loop_pending_algos_untriggered"
        if terminal_status is None:
            return None
        summary = self._mini_qmt_batch_residual_summary(fresh_payload)
        capacity_residual_observability = self._miniqmt_capacity_residual_observability(
            fresh_payload,
            reason=reason,
            source="post_close_terminalization",
        )
        evidence = {
            "schema_version": "miniqmt_post_close_terminalization_v1",
            "reason": reason,
            "previous_status": run.status.value,
            "terminal_status": terminal_status.value,
            "trade_date": run.trade_date.isoformat(),
            "as_of_time": self._scheduler_time(as_of_time).isoformat() if as_of_time is not None else None,
            "qmt_batch_status": fresh_payload.get("qmt_batch_status"),
            "qmt_batch_id": fresh_payload.get("qmt_batch_id"),
            "residual_summary": summary,
            "fresh_reconcile": fresh_reconcile,
            "previous_open_order_evidence": previous_open_order_evidence,
            "previous_submit_result_gate": previous_submit_result_gate,
            "open_order_evidence": self._latest_miniqmt_payload_evidence(fresh_payload, "open_order_evidence"),
            "submit_result_gate": self._latest_miniqmt_payload_evidence(fresh_payload, "submit_result_gate"),
            "audit_state": self._miniqmt_post_close_audit_state(terminal_status, reason),
        }
        if event_loop_pending_after_close:
            evidence["event_loop_pending_after_close"] = event_loop_pending_after_close
        if capacity_residual_observability:
            evidence["miniqmt_capacity_residual_observability"] = capacity_residual_observability
        payload_patch = {
            "last_stage": terminal_status.value,
            "miniqmt_post_close_terminalization": evidence,
        }
        payload_patch.update(self._miniqmt_capacity_residual_payload_patch(capacity_residual_observability))
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=terminal_status,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",) if terminal_status == SimulationDailyRunStatus.SUCCEEDED else None,
        )
        return {
            "run_id": updated.run_id,
            "trade_date": updated.trade_date.isoformat(),
            "strategy_id": updated.strategy_id,
            "previous_status": run.status.value,
            "status": updated.status.value,
            "reason": reason,
            "post_close_terminalization": True,
            **self._miniqmt_capacity_residual_result_fields(updated),
        }

    def _fresh_miniqmt_post_close_payload(
        self,
        *,
        run: SimulationDailyRun,
        as_of_time: datetime | None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            binding = self.repository.get_simulation_release_binding(run.binding_id)
            runtime_release = self.repository.get_strategy_runtime_release(run.release_id)
            context = self.context_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=run.trade_date,
            )
            reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=run, context=context)
        except Exception as exc:
            if (
                isinstance(exc, DataUnavailableError)
                and getattr(exc, "context", {}).get("reason_code") == "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED"
            ):
                raise
            raise DataUnavailableError(
                "MiniQMT post-close terminalization requires a fresh broker reconcile before terminal status",
                context={
                    "reason_code": "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "strategy_id": run.strategy_id,
                    "release_id": run.release_id,
                    "trade_date": run.trade_date.isoformat(),
                    "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "error_context": getattr(exc, "context", None),
                },
            ) from exc
        if reconciliation is None:
            raise DataUnavailableError(
                "MiniQMT post-close terminalization fresh broker reconcile returned no payload",
                context={
                    "reason_code": "MINIQMT_POST_CLOSE_FRESH_RECONCILE_MISSING",
                    "run_id": run.run_id,
                    "binding_id": run.binding_id,
                    "strategy_id": run.strategy_id,
                    "release_id": run.release_id,
                    "trade_date": run.trade_date.isoformat(),
                    "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
                },
            )
        refreshed_run = self.repository.get_simulation_daily_run(run.run_id)
        payload = refreshed_run.run_payload_json
        sync_evidence = payload.get("sync_after_submit") if isinstance(payload.get("sync_after_submit"), dict) else {}
        return payload, {
            "schema_version": "miniqmt_post_close_fresh_reconcile_v1",
            "source": "qmt_broker_snapshot_and_strategy_ledger",
            "as_of_time": as_of_time.isoformat() if as_of_time is not None else None,
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "account_id": binding.broker_account_id,
            "reconcile_payload_key": "reconcile_after_submit",
            "sync_payload_key": "sync_after_submit",
            "sync_evidence": sync_evidence,
            "open_order_evidence": self._latest_miniqmt_payload_evidence(payload, "open_order_evidence"),
            "submit_result_gate": self._latest_miniqmt_payload_evidence(payload, "submit_result_gate"),
        }

    @staticmethod
    def _is_post_close_reconcile_time(*, as_of_time: datetime | None) -> bool:
        if as_of_time is None:
            return False
        local_as_of = SimulationLifecycleScheduler._scheduler_time(as_of_time)
        return local_as_of.time().replace(second=0, microsecond=0) >= _POST_CLOSE_RECONCILE_TIME

    @staticmethod
    def _miniqmt_post_close_terminal_status(
        payload: dict[str, Any],
    ) -> tuple[SimulationDailyRunStatus | None, str | None]:
        open_order_evidence = SimulationLifecycleScheduler._latest_miniqmt_payload_evidence(
            payload,
            "open_order_evidence",
        )
        open_order_count = int(open_order_evidence.get("open_order_count") or 0) if open_order_evidence else 0
        if open_order_count > 0:
            return SimulationDailyRunStatus.FAILED_TERMINAL, "miniqmt_post_close_open_orders_terminal_failed"
        if SimulationLifecycleScheduler._mini_qmt_batch_succeeded(payload):
            return SimulationDailyRunStatus.SUCCEEDED, "miniqmt_post_close_batch_succeeded"
        if SimulationLifecycleScheduler._mini_qmt_batch_has_terminal_capacity_residual(payload):
            return SimulationDailyRunStatus.SUCCEEDED, "miniqmt_post_close_capacity_residual_skipped"
        if SimulationLifecycleScheduler._mini_qmt_batch_has_retryable_buy_residual(payload):
            return SimulationDailyRunStatus.FAILED_RETRYABLE, "miniqmt_post_close_buy_residual_unresolved"
        return None, None

    @staticmethod
    def _miniqmt_post_close_audit_state(status: SimulationDailyRunStatus, reason: str | None) -> str:
        if status == SimulationDailyRunStatus.SUCCEEDED:
            if reason == "miniqmt_post_close_capacity_residual_skipped":
                return "succeeded_with_capacity_residual"
            return "succeeded_after_close"
        if status == SimulationDailyRunStatus.FAILED_RETRYABLE:
            return "failed_retryable_after_close"
        return "failed_terminal_after_close"

    @staticmethod
    def _miniqmt_event_loop_pending_after_close(payload: dict[str, Any]) -> dict[str, Any] | None:
        if not SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos(payload):
            return None
        driver = (
            payload.get("miniqmt_event_loop_tick_driver")
            if isinstance(payload.get("miniqmt_event_loop_tick_driver"), dict)
            else {}
        )
        pending_parent_ids = (
            driver.get("pending_parent_intent_ids") if isinstance(driver.get("pending_parent_intent_ids"), list) else []
        )
        return {
            "schema_version": "miniqmt_event_loop_pending_after_close_v1",
            "reason_code": "MINIQMT_EVENT_LOOP_PENDING_ALGOS_MARKET_CLOSED",
            "stage": "MINIQMT_POST_CLOSE_TERMINALIZATION",
            "reason": "event_loop_algorithms_remained_running_without_child_order_until_market_close",
            "pending_intents": payload.get("pending_intents"),
            "pending_parent_intent_ids": list(pending_parent_ids),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status"),
        }

    @staticmethod
    def _latest_miniqmt_payload_evidence(payload: dict[str, Any], key: str) -> dict[str, Any]:
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            evidence = container.get(key)
            if isinstance(evidence, dict):
                return evidence
        return {}

    def _partition_retired_package_bindings(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        data_source: str,
        package_status_cache: dict[str, PackageStatus],
    ) -> tuple[
        list[SimulationReleaseBinding],
        list[SimulationSchedulerBindingResult],
        set[tuple[str, SimulationBrokerBackend]],
    ]:
        eligible: list[SimulationReleaseBinding] = []
        skipped: list[SimulationSchedulerBindingResult] = []
        blocked_keys: set[tuple[str, SimulationBrokerBackend]] = set()
        for binding in bindings:
            status = self._package_lifecycle_status(
                package_id=binding.package_id,
                package_status_cache=package_status_cache,
            )
            if status != PackageStatus.RETIRED:
                eligible.append(binding)
                continue
            blocked_keys.add((binding.strategy_id, binding.broker_backend))
            skipped.append(self._retired_package_binding_result(binding=binding, data_source=data_source))
        return eligible, skipped, blocked_keys

    def _package_lifecycle_status(
        self,
        *,
        package_id: str,
        package_status_cache: dict[str, PackageStatus],
    ) -> PackageStatus:
        cached = package_status_cache.get(package_id)
        if cached is not None:
            return cached
        package_repository = getattr(self.selection_service, "package_repository", None)
        get_package = getattr(package_repository, "get", None)
        if not callable(get_package):
            raise RuntimeConfigInvalidError(
                "simulation scheduler requires the authoritative StrategyPackage lifecycle reader",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_READER_MISSING",
                    "package_id": package_id,
                },
            )
        record = get_package(package_id)
        record_package_id = str(getattr(record, "package_id", "") or "").strip()
        raw_status = getattr(record, "package_status", None)
        status_value = str(getattr(raw_status, "value", raw_status) or "").strip()
        try:
            status = PackageStatus(status_value)
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "simulation scheduler StrategyPackage lifecycle status is invalid",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_INVALID",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "package_status": status_value or None,
                },
            ) from exc
        if record_package_id != package_id:
            raise RuntimeConfigInvalidError(
                "simulation scheduler StrategyPackage lifecycle identity does not match the binding",
                context={
                    "reason_code": "SIMULATION_SCHEDULER_PACKAGE_LIFECYCLE_IDENTITY_MISMATCH",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "package_status": status.value,
                },
            )
        package_status_cache[package_id] = status
        return status

    @staticmethod
    def _retired_package_binding_result(
        *,
        binding: SimulationReleaseBinding,
        data_source: str,
    ) -> SimulationSchedulerBindingResult:
        diagnostic = {
            "schema_version": "simulation_package_lifecycle_skip_v1",
            "reason_code": "SIMULATION_BINDING_PACKAGE_RETIRED",
            "stage": "BINDING_SELECTION",
            "action": "SKIP",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "package_status": PackageStatus.RETIRED.value,
            "broker_backend": binding.broker_backend.value,
            "broker_called": False,
            "strategy_package_revalidation_performed": False,
        }
        logger.warning("Simulation binding skipped because StrategyPackage is retired: %s", diagnostic)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="SKIPPED_RETIRED_PACKAGE",
            lifecycle_diagnostic=diagnostic,
            data_source=data_source,
        )

    def _with_unattended_roll_forward_bindings(
        self,
        *,
        bindings: list[SimulationReleaseBinding],
        trade_date: date,
        limit: int,
        broker_backend: SimulationBrokerBackend | str | None,
        strategy_id: str | None,
        release_id: str | None,
        approval_states: tuple[SimulationBindingApprovalState, ...] | None,
        data_source: str,
        package_status_cache: dict[str, PackageStatus],
        lifecycle_skips: list[SimulationSchedulerBindingResult],
        blocked_binding_keys: set[tuple[str, SimulationBrokerBackend]],
        created_by: str,
        raise_on_error: bool,
    ) -> list[SimulationReleaseBinding]:
        if release_id is not None:
            return bindings

        bindings = self._without_superseded_unattended_bindings(bindings)
        rebased_bindings: list[SimulationReleaseBinding] = []
        for binding in bindings:
            try:
                rebased_bindings.append(
                    self._rebase_unattended_binding_to_authoritative_manifest(
                        binding=binding,
                        trade_date=trade_date,
                    )
                )
            except RuntimeConfigInvalidError as exc:
                if not self._isolate_invalid_miniqmt_roll_forward_binding(
                    binding=binding,
                    trade_date=trade_date,
                    data_source=data_source,
                    created_by=created_by,
                    exc=exc,
                    phase="MANIFEST_REBASE",
                    lifecycle_skips=lifecycle_skips,
                    blocked_binding_keys=blocked_binding_keys,
                    raise_on_error=raise_on_error,
                ):
                    raise
        bindings = rebased_bindings

        remaining_slots = limit - len(bindings)
        if remaining_slots <= 0:
            return bindings
        existing_keys = {(item.strategy_id, item.broker_backend) for item in bindings}
        existing_keys.update(blocked_binding_keys)
        roll_forwarded: list[SimulationReleaseBinding] = []
        for backend in self._roll_forward_backends_for_filter(broker_backend):
            if len(roll_forwarded) >= remaining_slots:
                break
            source_candidates = self.repository.list_latest_simulation_release_bindings(
                strategy_id=strategy_id,
                broker_backend=backend,
                approval_states=approval_states,
                effective_from_on_or_before=trade_date,
                limit=limit + remaining_slots,
            )
            for source in source_candidates:
                if (source.strategy_id, source.broker_backend) in existing_keys:
                    continue
                package_status = self._package_lifecycle_status(
                    package_id=source.package_id,
                    package_status_cache=package_status_cache,
                )
                if package_status == PackageStatus.RETIRED:
                    lifecycle_skips.append(
                        self._retired_package_binding_result(binding=source, data_source=data_source)
                    )
                    existing_keys.add((source.strategy_id, source.broker_backend))
                    continue
                if not self._binding_can_roll_forward(source=source, trade_date=trade_date):
                    continue
                binding_key = (source.strategy_id, source.broker_backend)
                try:
                    rolled_binding = self._roll_forward_unattended_binding(
                        source=source,
                        trade_date=trade_date,
                    )
                except RuntimeConfigInvalidError as exc:
                    if not self._isolate_invalid_miniqmt_roll_forward_binding(
                        binding=source,
                        trade_date=trade_date,
                        data_source=data_source,
                        created_by=created_by,
                        exc=exc,
                        phase="EXPIRED_BINDING_ROLL_FORWARD",
                        lifecycle_skips=lifecycle_skips,
                        blocked_binding_keys=blocked_binding_keys,
                        raise_on_error=raise_on_error,
                    ):
                        raise
                    existing_keys.add(binding_key)
                    continue
                roll_forwarded.append(rolled_binding)
                existing_keys.add(binding_key)
                if len(roll_forwarded) >= remaining_slots:
                    break

        if not roll_forwarded:
            return bindings
        combined = [*bindings, *roll_forwarded]
        combined.sort(key=lambda item: (item.created_at, item.binding_id), reverse=True)
        return combined[:limit]

    def _isolate_invalid_miniqmt_roll_forward_binding(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        created_by: str,
        exc: RuntimeConfigInvalidError,
        phase: str,
        lifecycle_skips: list[SimulationSchedulerBindingResult],
        blocked_binding_keys: set[tuple[str, SimulationBrokerBackend]],
        raise_on_error: bool,
    ) -> bool:
        context = self._exception_context(exc)
        if context.get("reason_code") != "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED":
            return False
        if raise_on_error:
            raise exc
        lifecycle_skips.append(
            self._record_pre_run_binding_failure_result(
                binding=binding,
                trade_date=trade_date,
                data_source=data_source,
                created_by=created_by,
                exc=exc,
            )
        )
        blocked_binding_keys.add((binding.strategy_id, binding.broker_backend))
        logger.error(
            "Invalid MiniQMT roll-forward binding isolated without starving valid bindings",
            extra={
                "reason_code": "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED",
                "phase": phase,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "broker_backend": binding.broker_backend.value,
                "trade_date": trade_date.isoformat(),
                "broker_called": False,
                "legacy_fallback": False,
            },
        )
        return True

    @staticmethod
    def _without_superseded_unattended_bindings(
        bindings: list[SimulationReleaseBinding],
    ) -> list[SimulationReleaseBinding]:
        """Hide only explicitly superseded same-day roll-forward bindings.

        A manifest rebase creates a new immutable release/binding instead of
        rewriting the already persisted identity. Both rows can be active on
        the rebase day, so the successor's explicit lineage is the only safe
        basis for suppressing the old row. Unrelated overlapping bindings are
        deliberately left visible and continue to fail through their normal
        lifecycle checks.
        """

        superseded_ids: set[str] = set()
        for binding in bindings:
            config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
            metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
            if metadata.get("manifest_identity_source") != "strategy_package_current_manifest":
                continue
            source_binding_id = str(metadata.get("extends_binding_id") or "").strip()
            if source_binding_id:
                superseded_ids.add(source_binding_id)
        return [binding for binding in bindings if binding.binding_id not in superseded_ids]

    def _rebase_unattended_binding_to_authoritative_manifest(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
    ) -> SimulationReleaseBinding:
        config = binding.binding_config_json if isinstance(binding.binding_config_json, dict) else {}
        metadata = config.get("metadata") if isinstance(config.get("metadata"), dict) else {}
        if metadata.get("purpose") not in {
            "localsim_unattended_daily_roll_forward",
            "miniqmt_unattended_daily_roll_forward",
        }:
            return binding

        source_release = self.repository.get_strategy_runtime_release(binding.release_id)
        authoritative_manifest_sha256 = self._authoritative_package_manifest_sha256(
            package_id=binding.package_id,
        )
        if (
            binding.manifest_sha256 == authoritative_manifest_sha256
            and source_release.manifest_sha256 == authoritative_manifest_sha256
        ):
            return binding

        existing_run = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing_run is not None and self._run_has_broker_side_effect_evidence(existing_run):
            # Never move an already submitted run to a different immutable
            # package identity. Returning the old binding preserves the loud
            # downstream identity conflict for operator diagnostics.
            return binding

        return self._roll_forward_unattended_binding(
            source=binding,
            trade_date=trade_date,
            authoritative_manifest_sha256=authoritative_manifest_sha256,
        )

    @staticmethod
    def _run_has_broker_side_effect_evidence(run: SimulationDailyRun) -> bool:
        payload = run.run_payload_json if isinstance(run.run_payload_json, dict) else {}
        if bool(payload.get("broker_called")):
            return True
        if payload.get("broker_side_effect_state") == "UNKNOWN":
            return True
        raw_submitted = payload.get("submitted_intents")
        if raw_submitted is not None:
            try:
                if int(raw_submitted) > 0:
                    return True
            except (TypeError, ValueError):
                return True
        return SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(payload)

    def _authoritative_package_manifest_sha256(self, *, package_id: str) -> str:
        package_repository = getattr(self.selection_service, "package_repository", None)
        get_package = getattr(package_repository, "get", None)
        if not callable(get_package):
            raise RuntimeConfigInvalidError(
                "unattended simulation roll-forward requires the authoritative StrategyPackage repository",
                context={
                    "reason_code": "SIMULATION_ROLL_FORWARD_PACKAGE_IDENTITY_SOURCE_MISSING",
                    "package_id": package_id,
                },
            )
        record = get_package(package_id)
        record_package_id = str(getattr(record, "package_id", "") or "").strip()
        manifest_sha256 = str(getattr(record, "manifest_sha256", "") or "").strip()
        if record_package_id != package_id or not manifest_sha256:
            raise RuntimeConfigInvalidError(
                "authoritative StrategyPackage identity is incomplete for unattended roll-forward",
                context={
                    "reason_code": "SIMULATION_ROLL_FORWARD_PACKAGE_IDENTITY_INVALID",
                    "package_id": package_id,
                    "record_package_id": record_package_id or None,
                    "manifest_sha256": manifest_sha256 or None,
                },
            )
        return manifest_sha256

    @staticmethod
    def _normalized_backend(value: SimulationBrokerBackend | str) -> SimulationBrokerBackend:
        return value if isinstance(value, SimulationBrokerBackend) else SimulationBrokerBackend(str(value))

    @staticmethod
    def _roll_forward_backends_for_filter(
        broker_backend: SimulationBrokerBackend | str | None,
    ) -> tuple[SimulationBrokerBackend, ...]:
        if broker_backend is not None:
            return (SimulationLifecycleScheduler._normalized_backend(broker_backend),)
        return (SimulationBrokerBackend.LOCAL_SIM, SimulationBrokerBackend.MINIQMT_SIM)

    @staticmethod
    def _binding_can_roll_forward(*, source: SimulationReleaseBinding, trade_date: date) -> bool:
        if source.broker_backend not in {SimulationBrokerBackend.LOCAL_SIM, SimulationBrokerBackend.MINIQMT_SIM}:
            return False
        if source.effective_from is not None and source.effective_from > trade_date:
            return False
        if source.effective_to is None or source.effective_to >= trade_date:
            return False
        config = source.binding_config_json or {}
        metadata = config.get("metadata") if isinstance(config, dict) else None
        if isinstance(metadata, dict) and metadata.get("disable_unattended_roll_forward") is True:
            return False
        return True

    def _roll_forward_unattended_binding(
        self,
        *,
        source: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str | None = None,
    ) -> SimulationReleaseBinding:
        miniqmt_quote_control = self._validated_unattended_roll_forward_quote_control(source=source)
        source_release = self.repository.get_strategy_runtime_release(source.release_id)
        resolved_manifest_sha256 = authoritative_manifest_sha256 or self._authoritative_package_manifest_sha256(
            package_id=source_release.package_id,
        )
        release_service = StrategyRuntimeReleaseService(repository=self.repository)
        created_by = self._roll_forward_created_by(source.broker_backend)
        release_metadata = self._roll_forward_release_metadata(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        validation_evidence = self._roll_forward_validation_evidence(
            source_release=source_release,
            source_binding=source,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        execution_policy = self._roll_forward_execution_policy(
            source_release=source_release,
            broker_backend=source.broker_backend,
        )
        new_release = release_service.create_release(
            package_id=source_release.package_id,
            manifest_sha256=resolved_manifest_sha256,
            runtime_profile_id=source_release.runtime_profile_id,
            runtime_profile_version_id=source_release.runtime_profile_version_id,
            runtime_profile_sha256=source_release.runtime_profile_sha256,
            daily_strategy_profile_version_id=source_release.daily_strategy_profile_version_id,
            execution_policy_version_id=execution_policy["policy_version_id"],
            execution_policy_sha256=execution_policy["policy_sha256"],
            tail_policy_version_id=source_release.tail_policy_version_id,
            tail_policy_sha256=source_release.tail_policy_sha256,
            execution_policy_json=execution_policy["policy_json"],
            base_release_id=source_release.release_id,
            validation_state=source_release.validation_state,
            validation_evidence=validation_evidence,
            release_metadata=release_metadata,
            effective_from=trade_date,
            effective_to=trade_date,
            created_by=created_by,
            created_reason=(
                f"Auto roll-forward {source.broker_backend.value} runtime release for unattended daily simulation "
                f"on {trade_date.isoformat()}."
            ),
        )
        binding_metadata = self._roll_forward_binding_metadata(
            source_release=source_release,
            source_binding=source,
            new_release=new_release,
            trade_date=trade_date,
            authoritative_manifest_sha256=resolved_manifest_sha256,
        )
        return release_service.create_binding(
            strategy_id=source.strategy_id,
            release=new_release,
            broker_backend=source.broker_backend,
            capital_allocation=float(source.capital_allocation),
            broker_account_id=source.broker_account_id,
            account_group_id=source.account_group_id,
            strategy_slot_id=source.strategy_slot_id,
            strategy_name=self._strategy_name_for_roll_forward(source=source, trade_date=trade_date),
            order_remark_prefix=source.order_remark_prefix,
            approval_state=source.approval_state,
            binding_metadata=binding_metadata,
            miniqmt_quote_control=miniqmt_quote_control,
            effective_from=trade_date,
            effective_to=trade_date,
            created_by=created_by,
            created_reason=(
                f"Auto roll-forward {source.broker_backend.value} binding for unattended daily simulation "
                f"on {trade_date.isoformat()}."
            ),
        )

    @staticmethod
    def _validated_unattended_roll_forward_quote_control(
        *,
        source: SimulationReleaseBinding,
    ) -> dict[str, Any] | None:
        if source.broker_backend is not SimulationBrokerBackend.MINIQMT_SIM:
            return None

        from backend.execution_algos.adaptive_is.contracts import ControlRevision
        from backend.services.miniqmt_execution_runtime.b0_quote_v2 import QuoteControlBindingV1

        try:
            quote_control = QuoteControlBindingV1.from_binding_config(source.binding_config_json)
        except (TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT unattended roll-forward requires an exact B0_QUOTE_V2 source binding",
                context={
                    "reason_code": "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED",
                    "stage": "SIMULATION_UNATTENDED_ROLL_FORWARD_PREFLIGHT",
                    "binding_id": source.binding_id,
                    "strategy_id": source.strategy_id,
                    "broker_backend": source.broker_backend.value,
                    "legacy_fallback": False,
                },
            ) from exc
        if quote_control.control_revision is not ControlRevision.B0_QUOTE_V2:
            raise RuntimeConfigInvalidError(
                "MiniQMT unattended roll-forward cannot continue a LEGACY_B0 source binding",
                context={
                    "reason_code": "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED",
                    "stage": "SIMULATION_UNATTENDED_ROLL_FORWARD_PREFLIGHT",
                    "binding_id": source.binding_id,
                    "strategy_id": source.strategy_id,
                    "broker_backend": source.broker_backend.value,
                    "control_revision": quote_control.control_revision.value,
                    "required_control_revision": ControlRevision.B0_QUOTE_V2.value,
                    "legacy_fallback": False,
                },
            )
        return quote_control.canonical_payload()

    @staticmethod
    def _roll_forward_created_by(backend: SimulationBrokerBackend) -> str:
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            return _MINIQMT_ROLL_FORWARD_CREATED_BY
        return _LOCALSIM_ROLL_FORWARD_CREATED_BY

    @staticmethod
    def _roll_forward_purpose(backend: SimulationBrokerBackend) -> str:
        if backend == SimulationBrokerBackend.MINIQMT_SIM:
            return "miniqmt_unattended_daily_roll_forward"
        return "localsim_unattended_daily_roll_forward"

    @staticmethod
    def _release_execution_policy_json(release: StrategyRuntimeRelease) -> dict[str, Any] | None:
        config = release.release_config_json or {}
        execution_policy = config.get("execution_policy") if isinstance(config, dict) else None
        policy_json = execution_policy.get("policy_json") if isinstance(execution_policy, dict) else None
        return deepcopy(policy_json) if isinstance(policy_json, dict) and policy_json else None

    @staticmethod
    def _roll_forward_execution_policy(
        *,
        source_release: StrategyRuntimeRelease,
        broker_backend: SimulationBrokerBackend,
    ) -> dict[str, Any]:
        if broker_backend is SimulationBrokerBackend.LOCAL_SIM:
            return local_sim_twap_only_policy_snapshot()
        return {
            "policy_version_id": source_release.execution_policy_version_id,
            "policy_sha256": source_release.execution_policy_sha256,
            "policy_json": SimulationLifecycleScheduler._release_execution_policy_json(source_release),
        }

    @staticmethod
    def _roll_forward_execution_policy_authority(
        *,
        source_release: StrategyRuntimeRelease,
        broker_backend: SimulationBrokerBackend,
    ) -> dict[str, Any]:
        effective = SimulationLifecycleScheduler._roll_forward_execution_policy(
            source_release=source_release,
            broker_backend=broker_backend,
        )
        return {
            "schema_version": "simulation_roll_forward_execution_policy_authority_v1",
            "source_policy_version_id": source_release.execution_policy_version_id,
            "source_policy_sha256": source_release.execution_policy_sha256,
            "effective_policy_version_id": effective["policy_version_id"],
            "effective_policy_sha256": effective["policy_sha256"],
            "authority_source": (
                "localsim_twap_only_runtime_policy"
                if broker_backend is SimulationBrokerBackend.LOCAL_SIM
                else "source_runtime_release"
            ),
            "source_policy_consulted_for_execution": broker_backend is not SimulationBrokerBackend.LOCAL_SIM,
            "fallback_used": False,
        }

    @staticmethod
    def _roll_forward_release_metadata(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        config = source_release.release_config_json or {}
        metadata = deepcopy(config.get("metadata") if isinstance(config, dict) else {}) or {}
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        purpose = SimulationLifecycleScheduler._roll_forward_purpose(source_binding.broker_backend)
        metadata.update(
            {
                "source": created_by,
                "purpose": purpose,
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "manifest_identity_source": "strategy_package_current_manifest",
                "source_release_manifest_sha256": source_release.manifest_sha256,
                "authoritative_manifest_sha256": authoritative_manifest_sha256,
                "manifest_identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                "execution_policy_authority": SimulationLifecycleScheduler._roll_forward_execution_policy_authority(
                    source_release=source_release,
                    broker_backend=source_binding.broker_backend,
                ),
                "roll_forward_policy": {
                    "schema_version": "localsim_roll_forward_policy_v1",
                    "immutable_daily_release": True,
                    "no_manual_db_write_required": True,
                },
            }
        )
        return metadata

    @staticmethod
    def _roll_forward_validation_evidence(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        evidence = deepcopy(source_release.validation_evidence or {})
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        evidence.update(
            {
                "source": created_by,
                "purpose": f"{source_binding.broker_backend.value} unattended daily roll-forward",
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "manifest_identity": {
                    "source": "strategy_package_current_manifest",
                    "source_release_manifest_sha256": source_release.manifest_sha256,
                    "authoritative_manifest_sha256": authoritative_manifest_sha256,
                    "identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                    "strategy_package_revalidation_performed": False,
                },
                "execution_policy_authority": SimulationLifecycleScheduler._roll_forward_execution_policy_authority(
                    source_release=source_release,
                    broker_backend=source_binding.broker_backend,
                ),
            }
        )
        return evidence

    @staticmethod
    def _roll_forward_binding_metadata(
        *,
        source_release: StrategyRuntimeRelease,
        source_binding: SimulationReleaseBinding,
        new_release: StrategyRuntimeRelease,
        trade_date: date,
        authoritative_manifest_sha256: str,
    ) -> dict[str, Any]:
        config = source_binding.binding_config_json or {}
        metadata = deepcopy(config.get("metadata") if isinstance(config, dict) else {}) or {}
        created_by = SimulationLifecycleScheduler._roll_forward_created_by(source_binding.broker_backend)
        purpose = SimulationLifecycleScheduler._roll_forward_purpose(source_binding.broker_backend)
        metadata.update(
            {
                "source": created_by,
                "purpose": purpose,
                "broker_backend": source_binding.broker_backend.value,
                "target_trade_date": trade_date.isoformat(),
                "extends_release_id": source_release.release_id,
                "extends_binding_id": source_binding.binding_id,
                "new_release_id": new_release.release_id,
                "manifest_identity_source": "strategy_package_current_manifest",
                "source_release_manifest_sha256": source_release.manifest_sha256,
                "authoritative_manifest_sha256": authoritative_manifest_sha256,
                "manifest_identity_changed": source_release.manifest_sha256 != authoritative_manifest_sha256,
                "roll_forward_policy": {
                    "schema_version": "localsim_roll_forward_policy_v1",
                    "immutable_daily_binding": True,
                    "new_strategy_package_supported": True,
                },
            }
        )
        return metadata

    @staticmethod
    def _strategy_name_for_roll_forward(
        *,
        source: SimulationReleaseBinding,
        trade_date: date,
    ) -> str | None:
        if source.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            return SimulationLifecycleScheduler._localsim_strategy_name_for_trade_date(source.strategy_name, trade_date)
        return source.strategy_name

    @staticmethod
    def _localsim_strategy_name_for_trade_date(strategy_name: str | None, trade_date: date) -> str | None:
        if not strategy_name:
            return strategy_name
        import re

        target = trade_date.isoformat()
        if re.search(r"\d{4}-\d{2}-\d{2}", strategy_name):
            return re.sub(r"\d{4}-\d{2}-\d{2}", target, strategy_name)
        return strategy_name

    def _load_run_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        as_of_time: datetime | None,
        require_localsim_realtime_quote: bool = False,
    ) -> SimulationRunContext:
        """Load phase-appropriate runtime state without coupling selection to live quotes."""
        phase_loader = getattr(self.context_provider, "load_context_for_phase", None)
        if callable(phase_loader):
            return phase_loader(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
                as_of_time=as_of_time,
                require_localsim_realtime_quote=require_localsim_realtime_quote,
            )
        return self.context_provider.load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )

    def _load_existing_plan_context(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        plan: ExecutionPlan,
        trade_date: date,
        as_of_time: datetime | None,
    ) -> SimulationRunContext:
        self._assert_local_sim_plan_uses_twap(binding=binding, plan=plan)
        loader = getattr(self.context_provider, "load_existing_plan_context", None)
        if callable(loader):
            return loader(
                runtime_release=runtime_release,
                binding=binding,
                plan=plan,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
        return self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
        )

    @staticmethod
    def _assert_local_sim_plan_uses_twap(
        *,
        binding: SimulationReleaseBinding,
        plan: ExecutionPlan,
    ) -> None:
        LocalSimPlanner.assert_plan_uses_twap(binding=binding, plan=plan)

    @staticmethod
    def _run_failure_stage(run: SimulationDailyRun) -> str:
        for key in ("submit_failure", "pre_run_failure"):
            failure = run.run_payload_json.get(key)
            if isinstance(failure, dict):
                stage = str(failure.get("stage") or failure.get("failure_stage") or "").strip()
                if stage:
                    return stage
        if isinstance(run.run_payload_json.get("pre_trade_blocked_order_generation"), dict):
            return "LOCAL_SIM_PRE_TRADE_BLOCKED_REPLAN"
        return "SIMULATION_BINDING_FAILED_RETRYABLE"

    @staticmethod
    def _run_requires_binding_retry_control(run: SimulationDailyRun) -> bool:
        retryable_failure = (
            run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
            and not bool(run.run_payload_json.get("broker_called"))
            and any(isinstance(run.run_payload_json.get(key), dict) for key in ("submit_failure", "pre_run_failure"))
        )
        blocked_replan = (
            run.broker_backend == SimulationBrokerBackend.LOCAL_SIM
            and run.status == SimulationDailyRunStatus.SUCCEEDED
            and not bool(run.run_payload_json.get("broker_called"))
            and isinstance(run.run_payload_json.get("pre_trade_blocked_order_generation"), dict)
        )
        return retryable_failure or blocked_replan

    def _claim_binding_retry_or_defer(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
        data_source: str,
        submit: bool,
        as_of_time: datetime | None,
    ) -> tuple[SimulationDailyRun, SimulationSchedulerBindingResult | None, str | None, str | None]:
        if not submit or not self._run_requires_binding_retry_control(run):
            return run, None, None, None
        source_fingerprint = self._simulation_retry_source_fingerprint(
            run=run,
            retry_key=_SIMULATION_BINDING_RETRY_KEY,
        )
        decision = inspect_simulation_retry_backoff(
            run=run,
            retry_key=_SIMULATION_BINDING_RETRY_KEY,
            source_fingerprint=source_fingerprint,
            as_of_time=self._scheduler_time(as_of_time),
            lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
        )
        if decision is None:
            decision = self.repository.claim_simulation_retry_attempt(
                run_id=run.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                source_fingerprint=source_fingerprint,
                as_of_time=self._scheduler_time(as_of_time),
                lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
            )
        if decision.should_execute:
            return decision.run, None, decision.claim_token, source_fingerprint
        retry_entry = deepcopy(decision.retry_entry)
        error = {
            "schema_version": "simulation_binding_retry_backoff_v1",
            "reason_code": "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE",
            "retry_reason": decision.reason,
            "run_id": decision.run.run_id,
            "binding_id": binding.binding_id,
            "plan_id": plan.plan_id,
            "failure_stage": self._run_failure_stage(decision.run),
            "retry_control": retry_entry,
            "auto_retry": True,
            "next_retry_at": (
                retry_entry.get("next_retry_at") or retry_entry.get("lease_until")
                if isinstance(retry_entry, dict)
                else None
            ),
        }
        return (
            decision.run,
            SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="RETRY_BACKOFF",
                run=decision.run,
                execution_plan=plan,
                lifecycle_diagnostic={
                    **error,
                    "alert": {
                        "severity": "WARNING",
                        "reason_code": "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE",
                        "auto_retry": True,
                        "next_retry_at": error["next_retry_at"],
                    },
                },
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            ),
            None,
            None,
        )

    def _claim_preplan_unknown_retry_or_defer(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
    ) -> tuple[SimulationDailyRun, SimulationSchedulerBindingResult | None, str | None, str]:
        source_fingerprint = self._simulation_retry_source_fingerprint(
            run=run,
            retry_key=_SIMULATION_BINDING_RETRY_KEY,
        )
        decision = inspect_simulation_retry_backoff(
            run=run,
            retry_key=_SIMULATION_BINDING_RETRY_KEY,
            source_fingerprint=source_fingerprint,
            as_of_time=self._scheduler_time(as_of_time),
            lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
        )
        if decision is None:
            decision = self.repository.claim_simulation_retry_attempt(
                run_id=run.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                source_fingerprint=source_fingerprint,
                as_of_time=self._scheduler_time(as_of_time),
                lease_seconds=_SIMULATION_RETRY_ATTEMPT_LEASE_SECONDS,
            )
        if decision.should_execute:
            return decision.run, None, decision.claim_token, source_fingerprint
        retry_entry = deepcopy(decision.retry_entry)
        diagnostic = {
            "schema_version": "miniqmt_preplan_unknown_retry_backoff_v1",
            "reason_code": "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE",
            "retry_reason": decision.reason,
            "run_id": decision.run.run_id,
            "binding_id": binding.binding_id,
            "failure_stage": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION",
            "retry_control": retry_entry,
            "auto_retry": True,
            "next_retry_at": (
                retry_entry.get("next_retry_at") or retry_entry.get("lease_until")
                if isinstance(retry_entry, dict)
                else None
            ),
        }
        return (
            decision.run,
            SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="RETRY_BACKOFF",
                run=decision.run,
                lifecycle_diagnostic={
                    **diagnostic,
                    "alert": {
                        "severity": "WARNING",
                        "reason_code": diagnostic["reason_code"],
                        "auto_retry": True,
                        "next_retry_at": diagnostic["next_retry_at"],
                    },
                },
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            ),
            None,
            source_fingerprint,
        )

    def _finalize_binding_retry_result(
        self,
        *,
        result: SimulationSchedulerBindingResult,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        run = result.run
        if run is None or result.status == "RETRY_BACKOFF":
            return result
        if self._run_requires_binding_retry_control(run):
            failure_stage = self._run_failure_stage(run)
            raw_failure = run.run_payload_json.get("submit_failure")
            if not isinstance(raw_failure, dict):
                raw_failure = run.run_payload_json.get("pre_run_failure")
            if not isinstance(raw_failure, dict):
                blocked = run.run_payload_json.get("pre_trade_blocked_order_generation")
                if isinstance(blocked, dict):
                    raw_failure = {
                        "type": "LocalSimPreTradeBlockedReplan",
                        "message": "LocalSIM frozen plan remains pre-trade blocked",
                        "context": blocked,
                    }
            failure = raw_failure if isinstance(raw_failure, dict) else {}
            context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
            error = {
                "type": str(failure.get("type") or "SimulationRetryableFailure"),
                "message": str(failure.get("message") or "simulation binding failed retryably"),
                "reason_code": str(context.get("reason_code") or failure.get("reason_code") or "") or None,
                "context": deepcopy(context),
                "failure_stage": failure_stage,
            }
            updated = self._record_simulation_retry_failure_evidence(
                run=run,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                failure_stage=failure_stage,
                error=error,
                as_of_time=as_of_time,
                expected_claim_token=result.retry_claim_token,
                source_fingerprint=result.retry_source_fingerprint,
            )
            return replace(result, run=updated)
        if result.retry_claim_token is not None:
            updated = self.repository.clear_simulation_retry_control(
                run_id=run.run_id,
                retry_key=_SIMULATION_BINDING_RETRY_KEY,
                expected_claim_token=result.retry_claim_token,
            )
            return replace(result, run=updated)
        return result

    def _execute_existing_plan_retry_attempt(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        if self._should_rebuild_localsim_plan_after_side_effect_free_failure(
            binding=binding,
            run=run,
            submit=submit,
            trade_date=trade_date,
            as_of_time=as_of_time,
        ):
            return self._rebuild_localsim_plan_after_side_effect_free_failure(
                binding=binding,
                run=run,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
                created_by=created_by,
                selection_cache=selection_cache,
                shared_selection_keys=shared_selection_keys,
                as_of_time=as_of_time,
            )
        if self._should_rebuild_miniqmt_plan_after_side_effect_free_failure(binding=binding, run=run):
            return self._rebuild_miniqmt_plan_after_side_effect_free_failure(
                binding=binding,
                run=run,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                mode=mode,
                created_by=created_by,
                selection_cache=selection_cache,
                shared_selection_keys=shared_selection_keys,
                as_of_time=as_of_time,
            )
        return self._existing_plan_result(
            binding=binding,
            run=run,
            trade_date=trade_date,
            data_source=data_source,
            submit=submit,
            mode=mode,
            as_of_time=as_of_time,
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
        as_of_time: datetime | None = None,
    ) -> SimulationSchedulerBindingResult:
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing = self.repository.get_simulation_daily_run_by_key(
            strategy_id=binding.strategy_id,
            binding_id=binding.binding_id,
            trade_date=trade_date,
        )
        if existing is not None and existing.execution_plan_id is None:
            terminal_preplan_unknown = self._preplan_unknown_terminal_result(
                binding=binding,
                run=existing,
                data_source=data_source,
            )
            if terminal_preplan_unknown is not None:
                return terminal_preplan_unknown
            if (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and existing.run_payload_json.get("broker_side_effect_state") == "UNKNOWN"
            ):
                existing, deferred, retry_claim_token, retry_source_fingerprint = (
                    self._claim_preplan_unknown_retry_or_defer(
                        binding=binding,
                        run=existing,
                        trade_date=trade_date,
                        data_source=data_source,
                        as_of_time=as_of_time,
                    )
                )
                if deferred is not None:
                    return deferred
                try:
                    existing, preplan_unknown_result = self._reconcile_preplan_unknown_miniqmt_run(
                        binding=binding,
                        run=existing,
                        runtime_release=runtime_release,
                        trade_date=trade_date,
                        data_source=data_source,
                        as_of_time=as_of_time,
                        retry_claim_token=retry_claim_token,
                    )
                except Exception as exc:
                    isolated_exc: Exception = exc
                    if not isinstance(exc, (DataUnavailableError, RuntimeConfigInvalidError)):
                        isolated_exc = RuntimeConfigInvalidError(
                            "MiniQMT preplan UNKNOWN reconciliation failed and remains retryable",
                            context={
                                "reason_code": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION_FAILED",
                                "stage": "MINIQMT_PREPLAN_UNKNOWN_RECONCILIATION",
                                "run_id": existing.run_id,
                                "binding_id": binding.binding_id,
                                "trade_date": trade_date.isoformat(),
                                "broker_side_effect_state": "UNKNOWN",
                                "exception": {
                                    "type": type(exc).__name__,
                                    "message": str(exc)[:2048],
                                },
                            },
                        )
                    if retry_claim_token is not None:
                        raise _BindingRetryAttemptError(
                            original=isolated_exc,
                            claim_token=retry_claim_token,
                            source_fingerprint=retry_source_fingerprint,
                        ) from exc
                    if isolated_exc is exc:
                        raise
                    raise isolated_exc from exc
                if preplan_unknown_result is not None:
                    proof = existing.run_payload_json.get("miniqmt_preplan_unknown_reconciliation")
                    if not isinstance(proof, dict) or proof.get("status") != "RECONCILIATION_PENDING":
                        return preplan_unknown_result
                    return replace(
                        preplan_unknown_result,
                        retry_claim_token=retry_claim_token,
                        retry_source_fingerprint=retry_source_fingerprint,
                    )
        if existing is not None and existing.execution_plan_id:
            existing_plan = self.repository.get_execution_plan(existing.execution_plan_id)
            existing, deferred, retry_claim_token, retry_source_fingerprint = self._claim_binding_retry_or_defer(
                binding=binding,
                run=existing,
                plan=existing_plan,
                trade_date=trade_date,
                data_source=data_source,
                submit=submit,
                as_of_time=as_of_time,
            )
            if deferred is not None:
                return deferred
            try:
                result = self._execute_existing_plan_retry_attempt(
                    binding=binding,
                    run=existing,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    submit=submit,
                    mode=mode,
                    created_by=created_by,
                    selection_cache=selection_cache,
                    shared_selection_keys=shared_selection_keys,
                    as_of_time=as_of_time,
                )
            except Exception as exc:
                if retry_claim_token is not None and retry_source_fingerprint is not None:
                    raise _BindingRetryAttemptError(
                        original=exc,
                        claim_token=retry_claim_token,
                        source_fingerprint=retry_source_fingerprint,
                    ) from exc
                raise
            return replace(
                result,
                retry_claim_token=retry_claim_token,
                retry_source_fingerprint=retry_source_fingerprint,
            )

        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
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
            require_realtime_quote=self._localsim_realtime_quote_required(
                binding=binding,
                trade_date=trade_date,
                submit=submit,
                as_of_time=as_of_time,
            )
            if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM
            else None,
            as_of_time=as_of_time,
        )
        build_result = self._clear_pre_run_failure_after_planning(build_result)
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
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
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        try:
            sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
            build_result = self._prepare_localsim_build_result_for_submit(
                binding=binding,
                build_result=build_result,
                context=context,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
            if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                raise
            marked = self._mark_localsim_pre_submit_retry_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                exc=exc,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=marked.status.value,
                run=marked,
                execution_plan=build_result.execution_plan,
                error=self._localsim_pre_submit_error_payload(marked),
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        try:
            execution = self._submit_execution_plan_with_timeout(
                build_result=build_result,
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                context=context,
                mode=mode,
                as_of_time=as_of_time,
                submit_callable=lambda: self.orchestrator.submit_execution_plan(
                    build_result=build_result,
                    local_broker=context.local_broker,
                    managed_order_service=context.managed_order_service,
                    mode=mode,
                    price_by_symbol=context.price_by_symbol or context.current_prices,
                    as_of_time=as_of_time,
                ),
            )
        except BrokerRejectedError as exc:
            terminalized = self._terminalize_deterministic_localsim_submit_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
                exc=exc,
            )
            if terminalized is not None:
                return terminalized
            raise
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=replace(execution, run=latest_run),
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and execution.status == "KERNEL_V2_STARTED":
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="MINIQMT_KERNEL_V2_ACTIVE",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        tail_result = self._handle_tail_after_submit(
            binding=binding, run=execution.run, execution=execution, context=context
        )
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(
            execution.status, tail_result=tail_result, reconciliation=reconciliation
        )
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
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
            data_source=context.market_data_source
            or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _rebuild_miniqmt_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        rebuild_receipt = self._mini_qmt_side_effect_free_rebuild_receipt(run)
        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
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
            as_of_time=as_of_time,
        )
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
        )
        build_result = replace(
            build_result,
            run=self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.PLANNING_EXECUTION,
                selection_evidence=build_result.selection_evidence,
                execution_plan=build_result.execution_plan,
                payload_patch={
                    "last_stage": "PLANNING_EXECUTION",
                    "rebuilt_after_side_effect_free_failure": True,
                    "rebuilt_from_execution_plan_id": run.execution_plan_id,
                    "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    "miniqmt_side_effect_free_rebuild": {
                        **rebuild_receipt,
                        "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    },
                    "miniqmt_context_diagnostics": context.context_diagnostics,
                    "broker_called": False,
                    "submitted_intents": 0,
                    "failed_intents": 0,
                    "qmt_batch_id": None,
                    "qmt_batch_status": None,
                    "qmt_retry_of_batch_id": None,
                    "qmt_batch_result": None,
                    "submit_failure": None,
                },
            ),
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="REBUILT_EXISTING_PLAN",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
        execution = self._submit_execution_plan_with_timeout(
            build_result=build_result,
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
            mode=mode,
            as_of_time=as_of_time,
            submit_callable=lambda: self.orchestrator.submit_execution_plan(
                build_result=build_result,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol or context.current_prices,
                as_of_time=as_of_time,
            ),
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and execution.status == "KERNEL_V2_STARTED":
            self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="MINIQMT_KERNEL_V2_ACTIVE",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=execution,
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        tail_result = self._handle_tail_after_submit(
            binding=binding, run=execution.run, execution=execution, context=context
        )
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(
            execution.status, tail_result=tail_result, reconciliation=reconciliation
        )
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
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
            data_source=context.market_data_source
            or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _rebuild_localsim_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        created_by: str,
        selection_cache: dict[tuple[Any, ...], StrategyPackageSelectionResult | BaseException] | None,
        shared_selection_keys: set[tuple[Any, ...]] | None,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        context = self._load_run_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            as_of_time=as_of_time,
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
            require_realtime_quote=self._localsim_realtime_quote_required(
                binding=binding,
                trade_date=trade_date,
                submit=submit,
                as_of_time=as_of_time,
            ),
            as_of_time=as_of_time,
            preserved_causality_cursor=self._local_sim_plan_causality_cursor(
                self.repository.get_execution_plan(run.execution_plan_id or "")
            ),
        )
        build_result = self._persist_no_rebalance_evidence(
            build_result=build_result,
            current_positions=context.current_positions,
        )
        build_result = replace(
            build_result,
            run=self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.PLANNING_EXECUTION,
                selection_evidence=build_result.selection_evidence,
                execution_plan=build_result.execution_plan,
                payload_patch={
                    "last_stage": "PLANNING_EXECUTION",
                    "rebuilt_after_side_effect_free_failure": True,
                    "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
                    "rebuilt_from_execution_plan_id": run.execution_plan_id,
                    "rebuilt_execution_plan_id": build_result.execution_plan.plan_id,
                    "localsim_context_diagnostics": context.context_diagnostics,
                    "broker_called": False,
                    "submitted_intents": 0,
                },
                payload_unset=("submit_failure",),
            ),
        )
        if not submit:
            self._persist_strategy_performance(binding=binding, run=build_result.run, context=context)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="REBUILT_EXISTING_PLAN",
                run=build_result.run,
                execution_plan=build_result.execution_plan,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )

        try:
            sync_result = self._sync_before_submit(binding=binding, run=build_result.run, context=context)
            build_result = self._prepare_localsim_build_result_for_submit(
                binding=binding,
                build_result=build_result,
                context=context,
            )
        except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
            if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                raise
            marked = self._mark_localsim_pre_submit_retry_failure(
                binding=binding,
                run=build_result.run,
                plan=build_result.execution_plan,
                trade_date=trade_date,
                exc=exc,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=marked.status.value,
                run=marked,
                execution_plan=build_result.execution_plan,
                error=self._localsim_pre_submit_error_payload(marked),
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        execution = self._submit_execution_plan_with_timeout(
            build_result=build_result,
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
            mode=mode,
            as_of_time=as_of_time,
            submit_callable=lambda: self.orchestrator.submit_execution_plan(
                build_result=build_result,
                local_broker=context.local_broker,
                managed_order_service=context.managed_order_service,
                mode=mode,
                price_by_symbol=context.price_by_symbol or context.current_prices,
                as_of_time=as_of_time,
            ),
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding,
            run=execution.run,
            execution=execution,
            context=context,
        )
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run,
                execution_plan=execution.execution_plan,
                execution_result=replace(execution, run=latest_run),
                sync_result=sync_result,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        tail_result = self._handle_tail_after_submit(
            binding=binding, run=execution.run, execution=execution, context=context
        )
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=execution.run, context=context)
        self._persist_strategy_performance(
            binding=binding,
            run=execution.run,
            context=context,
            local_persistence=local_persistence,
        )
        latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
        status = self._result_status_after_post_submit(
            execution.status, tail_result=tail_result, reconciliation=reconciliation
        )
        status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
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
            data_source=context.market_data_source
            or self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def _existing_plan_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        trade_date: date,
        data_source: str,
        submit: bool,
        mode: str,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult:
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
        existing_evidence = self.repository.get_daily_selection_evidence(plan.selection_evidence_id)
        self._validate_fresh_daily_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            evidence=existing_evidence,
            trade_date=trade_date,
            runtime_config=StrategyPackageSelectionService.release_selection_runtime_config(runtime_release),
        )
        status = "REUSED_EXISTING_PLAN"
        if run.status == SimulationDailyRunStatus.SUCCEEDED and not plan.intents:
            status = "NO_REBALANCE"
        try:
            projection_recovery = self._recover_pending_local_sim_projection_if_needed(
                binding=binding,
                run=run,
                plan=plan,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                as_of_time=as_of_time,
            )
        except Exception as exc:
            return self._record_local_sim_durable_runtime_recovery_failure(
                binding=binding,
                run=run,
                plan=plan,
                data_source=data_source,
                recovery_stage="LOCAL_SIM_PROJECTION_RECOVERY",
                exc=exc,
            )
        if projection_recovery is not None:
            return projection_recovery
        try:
            durable_runtime_recovery = self._recover_failed_local_sim_durable_runtime_if_safe(
                binding=binding,
                run=run,
                plan=plan,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                as_of_time=as_of_time,
            )
        except Exception as exc:
            return self._record_local_sim_durable_runtime_recovery_failure(
                binding=binding,
                run=run,
                plan=plan,
                data_source=data_source,
                recovery_stage="LOCAL_SIM_DURABLE_RUNTIME_RECOVERY",
                exc=exc,
            )
        if durable_runtime_recovery is not None:
            # Recovery proves that the persisted execution and projection
            # planes can be restored; it is not itself causal progress.  Keep
            # driving the recovered run in this tick so status cannot become
            # temporarily healthy while every execution state remains stale.
            run = durable_runtime_recovery.run
        local_failure_error = self._local_sim_broker_called_failure_error(binding=binding, run=run)
        if local_failure_error is not None:
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=run.status.value,
                run=run,
                execution_plan=plan,
                error=local_failure_error,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_mark_existing_no_rebalance(run=run, plan=plan, submit=submit):
            updated = self.repository.update_simulation_daily_run(
                run.run_id,
                status=SimulationDailyRunStatus.SUCCEEDED,
                payload_patch={
                    "no_rebalance_required": True,
                    "broker_called": False,
                    "last_stage": "SUCCEEDED",
                    "submit_failure": None,
                },
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="NO_REBALANCE",
                run=updated,
                execution_plan=plan,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if submit and not plan.intents and self._execution_plan_has_pre_trade_blocks(plan):
            execution = self.orchestrator.submit_persisted_execution_plan(
                run=run,
                binding=binding,
                execution_plan=plan,
                mode=mode,
                as_of_time=as_of_time,
            )
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=execution.status,
                run=execution.run,
                execution_plan=plan,
                execution_result=execution,
                data_source=self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_drive_existing_local_sim(binding=binding, run=run, plan=plan, submit=submit):
            return self._drive_existing_local_sim(
                binding=binding,
                run=run,
                plan=plan,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                as_of_time=as_of_time,
            )
        context: SimulationRunContext | None = None
        if self._should_reconcile_existing_miniqmt_run(binding=binding, run=run, submit=submit):
            if context is None:
                context = self._load_existing_plan_context(
                    runtime_release=runtime_release,
                    binding=binding,
                    plan=plan,
                    trade_date=trade_date,
                    as_of_time=as_of_time,
                )
            sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
            reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=run, context=context)
            self._persist_strategy_performance(binding=binding, run=run, context=context)
            latest_run = self.repository.get_simulation_daily_run(run.run_id)
            status = self._result_status_after_post_submit(
                "RECOVERED",
                tail_result=None,
                reconciliation=reconciliation,
            )
            if run.status == SimulationDailyRunStatus.SUCCEEDED and status == "RECONCILED":
                status = "REUSED_EXISTING_PLAN"
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status=status,
                run=latest_run,
                execution_plan=plan,
                sync_result=sync_result,
                reconciliation_result=reconciliation,
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        if self._should_submit_existing_plan(binding=binding, run=run, plan=plan, submit=submit):
            runtime_release = self.repository.get_strategy_runtime_release(binding.release_id)
            try:
                context = self._load_existing_plan_context(
                    runtime_release=runtime_release,
                    binding=binding,
                    plan=plan,
                    trade_date=trade_date,
                    as_of_time=as_of_time,
                )
                sync_result = self._sync_before_submit(binding=binding, run=run, context=context)
                run, plan = self._prepare_localsim_execution_plan_for_submit(
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                )
            except (DataUnavailableError, RuntimeConfigInvalidError) as exc:
                if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
                    raise
                marked = self._mark_localsim_pre_submit_retry_failure(
                    binding=binding,
                    run=run,
                    plan=plan,
                    trade_date=trade_date,
                    exc=exc,
                )
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status=marked.status.value,
                    run=marked,
                    execution_plan=plan,
                    error=self._localsim_pre_submit_error_payload(marked),
                    data_source=self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            try:
                execution = self._submit_execution_plan_with_timeout(
                    build_result=None,
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                    mode=mode,
                    as_of_time=as_of_time,
                    submit_callable=lambda: self.orchestrator.submit_persisted_execution_plan(
                        run=run,
                        binding=binding,
                        execution_plan=plan,
                        local_broker=context.local_broker,
                        managed_order_service=context.managed_order_service,
                        mode=mode,
                        price_by_symbol=context.price_by_symbol or context.current_prices,
                        as_of_time=as_of_time,
                    ),
                )
            except BrokerRejectedError as exc:
                terminalized = self._terminalize_deterministic_localsim_submit_failure(
                    binding=binding,
                    run=run,
                    plan=plan,
                    trade_date=trade_date,
                    data_source=context.market_data_source
                    or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                    exc=exc,
                )
                if terminalized is not None:
                    return terminalized
                raise
            local_persistence = self._persist_local_sim_execution_result(
                binding=binding,
                run=execution.run,
                execution=execution,
                context=context,
            )
            if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status="LOCALSIM_INTRADAY_RUNNING",
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=replace(execution, run=latest_run),
                    sync_result=sync_result,
                    data_source=context.market_data_source
                    or self._effective_market_data_source_for_binding(
                        binding=binding, trade_date=trade_date, default_data_source=data_source
                    ),
                )
            if self._mini_qmt_batch_failed_without_broker_side_effect(execution.run.run_payload_json):
                self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status=execution.status,
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=execution,
                    sync_result=sync_result,
                    data_source=context.market_data_source
                    or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            if (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and execution.status == "KERNEL_V2_STARTED"
            ):
                self._persist_strategy_performance(binding=binding, run=execution.run, context=context)
                latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
                return SimulationSchedulerBindingResult(
                    binding_id=binding.binding_id,
                    strategy_id=binding.strategy_id,
                    broker_backend=binding.broker_backend,
                    status="MINIQMT_KERNEL_V2_ACTIVE",
                    run=latest_run,
                    execution_plan=execution.execution_plan,
                    execution_result=execution,
                    sync_result=sync_result,
                    data_source=context.market_data_source
                    or self._effective_market_data_source_for_binding(
                        binding=binding,
                        trade_date=trade_date,
                        default_data_source=data_source,
                    ),
                )
            tail_result = self._handle_tail_after_submit(
                binding=binding, run=execution.run, execution=execution, context=context
            )
            reconciliation = self._reconcile_after_submit_with_timeout(
                binding=binding, run=execution.run, context=context
            )
            self._persist_strategy_performance(
                binding=binding,
                run=execution.run,
                context=context,
                local_persistence=local_persistence,
            )
            latest_run = self.repository.get_simulation_daily_run(execution.run.run_id)
            status = self._result_status_after_post_submit(
                execution.status, tail_result=tail_result, reconciliation=reconciliation
            )
            status = self._local_sim_terminal_capacity_residual_status(latest_run, fallback=status)
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
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding,
                    trade_date=trade_date,
                    default_data_source=data_source,
                ),
            )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status,
            run=run,
            execution_plan=plan,
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=trade_date,
                default_data_source=data_source,
            ),
        )

    def recover_no_side_effect_reconciling_run_after_operator_cleanup(
        self,
        *,
        run_id: str,
        operator_result: Any,
        source: str = "miniqmt_operator_recovery",
    ) -> SimulationDailyRun:
        run = self.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run_id)
        metadata = dict(getattr(operator_result, "metadata", {}) or {})
        broker_evidence = dict(metadata.get("broker_evidence") or {})
        cleanup = dict(metadata.get("runtime_only_cleanup") or {})
        command_id = str(getattr(operator_result, "command_id", "") or "")
        command_type = str(getattr(operator_result, "command_type", "") or "")
        status = str(getattr(getattr(operator_result, "status", None), "value", getattr(operator_result, "status", "")))
        open_order_count = int(broker_evidence.get("broker_open_order_count") or 0)
        if status != "EXECUTED" or open_order_count != 0 or metadata.get("broker_mutated") is True:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires executed broker-empty operator evidence; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED",
                    "run_id": run.run_id,
                    "operator_status": status,
                    "broker_open_order_count": open_order_count,
                    "broker_mutated": metadata.get("broker_mutated"),
                    "command_id": command_id,
                },
            )
        plan = self.repository.get_execution_plan(run.execution_plan_id or "")
        plan_counts = self._execution_plan_side_counts(plan)
        diagnostic = {
            "schema_version": "miniqmt_no_side_effect_reconciling_recovery_v1",
            "reason_code": "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERED",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": run.strategy_id,
            "binding_id": run.binding_id,
            "trade_date": run.trade_date.isoformat(),
            "previous_status": run.status.value,
            "next_status": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "source": source,
            "operator_command_id": command_id,
            "operator_command_type": command_type,
            "broker_evidence": broker_evidence,
            "runtime_only_cleanup": cleanup,
            "broker_called": False,
            "submitted_intents": 0,
            "order_intent_count": run.run_payload_json.get("order_intent_count", plan_counts["intent_count"]),
            "message": (
                "RECONCILING run had no broker side effect and broker authority was empty; runtime-only stale "
                "state was terminalized so the next scheduler tick can reuse the standard FAILED_RETRYABLE path"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": 0,
                "no_rebalance_required": False,
                "miniqmt_no_side_effect_reconciling_recovery": diagnostic,
                "submit_failure": {
                    "stage": "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY",
                    "type": "OperatorRecovery",
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
        )

    def require_no_side_effect_reconciling_run_for_operator_recovery(self, *, run_id: str) -> SimulationDailyRun:
        run = self.repository.get_simulation_daily_run(run_id)
        if run.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires a MiniQMT SIM run; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_BACKEND_UNSUPPORTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_BACKEND_UNSUPPORTED",
                    "run_id": run.run_id,
                    "broker_backend": run.broker_backend.value,
                },
            )
        if run.status != SimulationDailyRunStatus.RECONCILING:
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery only accepts RECONCILING runs; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED",
                    "run_id": run.run_id,
                    "status": run.status.value,
                },
            )
        if (
            run.run_payload_json.get("broker_called") is not False
            or int(run.run_payload_json.get("submitted_intents") or 0) != 0
        ):
            raise RuntimeConfigInvalidError(
                "MiniQMT stale runtime recovery requires a no-side-effect run; "
                "reason_code=MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE",
                context={
                    "reason_code": "MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE",
                    "run_id": run.run_id,
                    "broker_called": run.run_payload_json.get("broker_called"),
                    "submitted_intents": run.run_payload_json.get("submitted_intents"),
                },
            )
        return run

    def _mark_localsim_pre_submit_retry_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
        exc: BaseException,
    ) -> SimulationDailyRun:
        plan_counts = self._execution_plan_side_counts(plan)
        context = getattr(exc, "context", None)
        if not isinstance(context, dict):
            context = {}
        stage = self._localsim_pre_submit_failure_stage(exc)
        diagnostic = {
            "schema_version": "localsim_pre_submit_retry_diagnostics_v1",
            "stage": stage,
            "reason_code": str(context.get("reason_code") or stage),
            "reason": "local_sim_retry_failed_before_broker_submit",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "trade_date": trade_date.isoformat(),
            "plan_intent_count": plan_counts["intent_count"],
            "buy_intent_count": plan_counts["buy_intent_count"],
            "sell_intent_count": plan_counts["sell_intent_count"],
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "fix the LocalSim context, cash, price, or market-data dependency and rerun the next scheduler "
                "execution tick; no broker order was submitted in this failed retry"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "no_rebalance_required": False,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": plan_counts["intent_count"],
                "local_sim_retry_diagnostics": diagnostic,
                "submit_failure": {
                    "stage": diagnostic["stage"],
                    "type": diagnostic["error_type"],
                    "message": diagnostic["message"],
                    "context": diagnostic,
                },
            },
        )

    def _terminalize_deterministic_localsim_submit_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        trade_date: date,
        data_source: str,
        exc: BrokerRejectedError,
    ) -> SimulationSchedulerBindingResult | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        if not self._is_deterministic_localsim_submit_failure(exc):
            return None
        plan_counts = self._execution_plan_side_counts(plan)
        raw_context = getattr(exc, "context", None)
        context = dict(raw_context) if isinstance(raw_context, dict) else {}
        reason_code = self._deterministic_localsim_submit_reason_code(context)
        diagnostic = {
            "schema_version": "localsim_deterministic_submit_failure_v1",
            "stage": "LOCAL_SIM_SUBMIT_FAILED",
            "reason_code": reason_code,
            "reason": "local_sim_deterministic_submit_failure_terminalized",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "strategy_id": binding.strategy_id,
            "binding_id": binding.binding_id,
            "trade_date": trade_date.isoformat(),
            "plan_intent_count": plan_counts["intent_count"],
            "buy_intent_count": plan_counts["buy_intent_count"],
            "sell_intent_count": plan_counts["sell_intent_count"],
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "fix the deterministic LocalSim order-shape rejection before retrying; "
                "the scheduler will not repeatedly resubmit the same failed plan"
            ),
        }
        updated = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_TERMINAL,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value,
                "no_rebalance_required": False,
                "broker_called": False,
                "submitted_intents": 0,
                "failed_intents": plan_counts["intent_count"],
                "local_sim_retry_diagnostics": diagnostic,
                "local_sim_deterministic_submit_failure": diagnostic,
                "submit_failure": {
                    "stage": "LOCAL_SIM_SUBMIT_FAILED",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=updated.status.value,
            run=updated,
            execution_plan=plan,
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "context": diagnostic,
            },
            data_source=data_source,
        )

    @staticmethod
    def _execution_plan_side_counts(plan: ExecutionPlan) -> dict[str, int]:
        return {
            "intent_count": len(plan.intents),
            "buy_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.BUY),
            "sell_intent_count": sum(1 for intent in plan.intents if intent.side == OrderSide.SELL),
        }

    @staticmethod
    def _localsim_pre_submit_failure_stage(exc: BaseException) -> str:
        context = getattr(exc, "context", None)
        if isinstance(context, dict) and context.get("reason_code") == "LOCALSIM_CASH_CONTEXT_MISSING":
            return "LOCAL_SIM_CASH_CONTEXT_MISSING"
        message = str(exc).lower()
        if "cash" in message:
            return "LOCAL_SIM_CASH_CONTEXT_MISSING"
        if "market data" in message or "minute" in message:
            return "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"
        if "price" in message:
            return "LOCAL_SIM_PRICE_UNAVAILABLE"
        return "LOCAL_SIM_CONTEXT_UNAVAILABLE"

    @staticmethod
    def _localsim_pre_submit_error_payload(run: SimulationDailyRun) -> dict[str, Any]:
        failure = run.run_payload_json.get("submit_failure")
        if not isinstance(failure, dict):
            return {
                "type": run.status.value,
                "message": run.status.value,
                "context": {"run_id": run.run_id},
            }
        return {
            "type": str(failure.get("type") or run.status.value),
            "message": str(failure.get("message") or run.status.value),
            "context": failure.get("context") if isinstance(failure.get("context"), dict) else {"run_id": run.run_id},
        }

    @staticmethod
    def _local_sim_broker_called_failure_error(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return None
        if run.status not in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.FAILED_TERMINAL}:
            return None
        if not bool(run.run_payload_json.get("broker_called")):
            return None
        failure = run.run_payload_json.get("submit_failure")
        return {
            "type": str(failure.get("type") or run.status.value) if isinstance(failure, dict) else run.status.value,
            "message": str(failure.get("message") or "LocalSim broker-called run is failed")
            if isinstance(failure, dict)
            else "LocalSim broker-called run is failed",
            "context": failure.get("context") if isinstance(failure, dict) else {"run_id": run.run_id},
        }

    def _recover_pending_local_sim_projection_if_needed(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult | None:
        return self._local_sim_persistence_coordinator().recover_pending_projection_if_needed(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            as_of_time=as_of_time,
        )

    def _record_local_sim_durable_runtime_recovery_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        data_source: str,
        recovery_stage: str,
        exc: Exception,
    ) -> SimulationSchedulerBindingResult:
        original_context = dict(getattr(exc, "context", None) or {})
        reason_code = str(original_context.get("reason_code") or "LOCALSIM_DURABLE_RUNTIME_RECOVERY_FAILED")
        terminal_reason_codes = {
            "LOCALSIM_PROJECTION_NON_RETRYABLE",
            "LOCALSIM_PROJECTION_RETRY_EXHAUSTED",
            "LOCALSIM_PROJECTION_READBACK_RETRY_EXHAUSTED",
            "LOCALSIM_ECONOMIC_READBACK_RETRY_EXHAUSTED",
        }
        retryable = reason_code not in terminal_reason_codes and self._local_sim_projection_error_is_retryable(exc)
        economic_readback_failure = bool(original_context.get("economic_readback_failure"))
        attempt_count = 0
        if economic_readback_failure:
            attempt_count = 1
            previous_failure = run.run_payload_json.get("local_sim_failed_run_recovery_failure_v1")
            if previous_failure is not None and not isinstance(previous_failure, dict):
                reason_code = "LOCALSIM_ECONOMIC_READBACK_FAILURE_RECEIPT_INVALID"
                retryable = False
            elif isinstance(previous_failure, dict):
                previous_context = previous_failure.get("context")
                if previous_context is not None and not isinstance(previous_context, dict):
                    reason_code = "LOCALSIM_ECONOMIC_READBACK_FAILURE_RECEIPT_INVALID"
                    retryable = False
                elif isinstance(previous_context, dict) and bool(previous_context.get("economic_readback_failure")):
                    raw_attempt_count = previous_context.get("attempt_count")
                    if (
                        isinstance(raw_attempt_count, bool)
                        or not isinstance(raw_attempt_count, int)
                        or raw_attempt_count < 1
                    ):
                        reason_code = "LOCALSIM_ECONOMIC_READBACK_FAILURE_RECEIPT_INVALID"
                        retryable = False
                    else:
                        attempt_count = raw_attempt_count + 1
            if retryable and attempt_count >= _LOCALSIM_PROJECTION_MAX_ATTEMPTS:
                reason_code = "LOCALSIM_ECONOMIC_READBACK_RETRY_EXHAUSTED"
                retryable = False
        status = SimulationDailyRunStatus.FAILED_RETRYABLE if retryable else SimulationDailyRunStatus.FAILED_TERMINAL
        failure_context = {
            **original_context,
            "reason_code": reason_code,
            "stage": recovery_stage,
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "plan_id": plan.plan_id,
            "broker_called": True,
            "submitted_intents": len(plan.intents),
            "failed_intents": 0,
            "parent_resubmitted": False,
            "retryable": retryable,
        }
        if economic_readback_failure:
            failure_context.update(
                {
                    "economic_readback_failure": True,
                    "attempt_count": attempt_count,
                    "max_attempts": _LOCALSIM_PROJECTION_MAX_ATTEMPTS,
                }
            )
        failure_receipt = {
            "schema_version": "local_sim_failed_run_recovery_failure_v1",
            "status": status.value,
            "reason_code": reason_code,
            "run_id": run.run_id,
            "binding_id": binding.binding_id,
            "plan_id": plan.plan_id,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": failure_context,
            "parent_resubmitted": False,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        failed_run = self.repository.update_simulation_daily_run(
            run.run_id,
            status=status,
            payload_patch={
                "last_stage": status.value,
                "broker_called": True,
                "submitted_intents": len(plan.intents),
                "failed_intents": 0,
                "local_sim_failed_run_recovery_failure_v1": failure_receipt,
                "submit_failure": {
                    "stage": recovery_stage,
                    "outer_stage": recovery_stage,
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": failure_context,
                },
            },
            payload_unset=("local_sim_failed_run_recovery_v1", "pre_run_failure"),
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=status.value,
            run=failed_run,
            execution_plan=plan,
            error={
                "type": type(exc).__name__,
                "message": str(exc),
                "context": failure_context,
            },
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=run.trade_date,
                default_data_source=data_source,
            ),
        )

    def _recover_failed_local_sim_durable_runtime_if_safe(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
    ) -> SimulationSchedulerBindingResult | None:
        if (
            binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM
            or run.status != SimulationDailyRunStatus.FAILED_RETRYABLE
        ):
            return None
        raw_outbox = run.run_payload_json.get("local_sim_projection_outbox_v1")
        if raw_outbox is None:
            return None
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw_outbox)
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim failed run durable projection evidence is invalid",
                context={"reason_code": "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID", "run_id": run.run_id},
            ) from exc
        if (
            outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
            or outbox.run_id != run.run_id
            or outbox.plan_id != plan.plan_id
            or run.run_payload_json.get("local_sim_projection_readback_failure")
            or run.run_payload_json.get("local_sim_projection_terminal_failure")
            or run.run_payload_json.get("local_sim_projection_readback_terminal_failure")
        ):
            return None
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        by_intent = {state.intent_id: state for state in states}
        expected_intents = {intent.intent_id for intent in plan.intents}
        if (
            len(by_intent) != len(states)
            or set(by_intent) != expected_intents
            or any(
                state.run_id != run.run_id
                or state.binding_id != binding.binding_id
                or state.trade_date != trade_date
                or state.plan_id != plan.plan_id
                for state in states
            )
        ):
            raise DataUnavailableError(
                "LocalSim failed run durable states do not close over the frozen plan",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(by_intent),
                },
            )
        self._readback_local_sim_recovery_generation(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=trade_date,
            as_of_time=as_of_time,
            outbox=outbox,
            states=states,
        )
        active_states = tuple(state for state in states if not state.is_terminal)
        persistence = run.run_payload_json.get("local_sim_persistence")
        if not active_states or not isinstance(persistence, dict) or persistence.get("terminal") is not False:
            return None
        previous_failure = run.run_payload_json.get("submit_failure")
        recovered = self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.INTRADAY_RUNNING,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.INTRADAY_RUNNING.value,
                "broker_called": True,
                "submitted_intents": len(states),
                "failed_intents": 0,
                "local_sim_failed_run_recovery_v1": {
                    "schema_version": "local_sim_failed_run_recovery_v1",
                    "status": "RECOVERED_TO_DURABLE_MINUTE_LOOP",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "outbox_id": outbox.outbox_id,
                    "generation": outbox.generation,
                    "active_state_ids": sorted(state.state_id for state in active_states),
                    "previous_failure": previous_failure if isinstance(previous_failure, dict) else None,
                    "parent_resubmitted": False,
                    "recovered_at": datetime.now(UTC).isoformat(),
                },
            },
            payload_unset=("submit_failure", "local_sim_retry_diagnostics"),
        )
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="LOCALSIM_DURABLE_RUNTIME_RECOVERED",
            run=recovered,
            execution_plan=plan,
            data_source=self._effective_market_data_source_for_binding(
                binding=binding,
                trade_date=run.trade_date,
                default_data_source=data_source,
            ),
        )

    def _readback_local_sim_recovery_generation(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        as_of_time: datetime | None,
        outbox: LocalSimProjectionOutboxV1,
        states: tuple[LocalSimExecutionStateV1, ...],
        context: SimulationRunContext | None = None,
    ) -> None:
        """Prove both durable planes before reviving a failed minute loop."""
        raw_economic_receipts = run.run_payload_json.get("local_sim_economic_receipts_v1")
        raw_projection_receipts = run.run_payload_json.get("local_sim_projection_receipts_v1")
        if not isinstance(raw_economic_receipts, dict) or not isinstance(raw_projection_receipts, dict):
            raise DataUnavailableError(
                "LocalSim failed run recovery receipts are missing",
                context={"reason_code": "LOCALSIM_RECOVERY_RECEIPT_MISSING", "run_id": run.run_id},
            )
        try:
            economic_receipts = {
                receipt_id: LocalSimEconomicReceiptV1.model_validate(raw)
                for receipt_id, raw in raw_economic_receipts.items()
            }
            projection_receipts = {
                receipt_id: LocalSimProjectionReceiptV1.model_validate(raw)
                for receipt_id, raw in raw_projection_receipts.items()
            }
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim failed run recovery receipt schema is invalid",
                context={"reason_code": "LOCALSIM_RECOVERY_RECEIPT_SCHEMA_INVALID", "run_id": run.run_id},
            ) from exc
        if any(receipt.receipt_id != key for key, receipt in economic_receipts.items()) or any(
            receipt.projection_receipt_id != key for key, receipt in projection_receipts.items()
        ):
            raise DataUnavailableError(
                "LocalSim failed run recovery receipt identity conflicts with its map key",
                context={"reason_code": "LOCALSIM_RECOVERY_RECEIPT_IDENTITY_CONFLICT", "run_id": run.run_id},
            )
        economic_receipt = economic_receipts.get(outbox.receipt_id)
        matching_projection_receipts = tuple(
            receipt for receipt in projection_receipts.values() if receipt.outbox_id == outbox.outbox_id
        )
        if economic_receipt is None or len(matching_projection_receipts) != 1:
            raise DataUnavailableError(
                "LocalSim failed run recovery cannot identify one exact projected generation",
                context={
                    "reason_code": "LOCALSIM_RECOVERY_RECEIPT_MISSING",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )
        projection_receipt = matching_projection_receipts[0]
        if (
            economic_receipt.run_id != run.run_id
            or economic_receipt.binding_id != binding.binding_id
            or economic_receipt.trade_date != trade_date
            or economic_receipt.plan_id != plan.plan_id
            or economic_receipt.generation != outbox.generation
            or economic_receipt.economic_hash != outbox.economic_hash
            or projection_receipt.run_id != run.run_id
            or projection_receipt.generation != outbox.generation
            or projection_receipt.economic_hash != outbox.economic_hash
            or projection_receipt.projection_payload_hash != outbox.projection_payload_hash
        ):
            raise DataUnavailableError(
                "LocalSim failed run recovery generation identities conflict",
                context={
                    "reason_code": "LOCALSIM_RECOVERY_GENERATION_IDENTITY_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                },
            )
        expected_state_hashes = economic_receipt.economic_facts.get("state_hashes")
        actual_state_hashes = {state.state_id: state.state_hash for state in states}
        if not isinstance(expected_state_hashes, dict) or expected_state_hashes != actual_state_hashes:
            raise DataUnavailableError(
                "LocalSim failed run recovery states do not match the committed generation",
                context={
                    "reason_code": "LOCALSIM_RECOVERY_STATE_HASH_CONFLICT",
                    "run_id": run.run_id,
                    "outbox_id": outbox.outbox_id,
                    "expected_state_ids": sorted(expected_state_hashes)
                    if isinstance(expected_state_hashes, dict)
                    else [],
                    "actual_state_ids": sorted(actual_state_hashes),
                },
            )
        self.repository.readback_local_sim_economic_commit(
            run_id=run.run_id,
            receipt=economic_receipt,
            outbox=outbox,
        )
        self.repository.readback_local_sim_projection_commit(
            run_id=run.run_id,
            receipt=projection_receipt,
        )
        if context is None:
            context = self._load_existing_plan_context(
                runtime_release=runtime_release,
                binding=binding,
                plan=plan,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
        paper_repository = self._paper_repository_for_local_sim(binding=binding, run=run, context=context)
        economic_fact_hashes: dict[str, dict[str, Any]] = {}
        for field_name in (
            "order_hashes",
            "fill_hashes",
            "order_event_hashes",
            "cash_entry_hashes",
        ):
            raw_hashes = economic_receipt.economic_facts.get(field_name)
            if not isinstance(raw_hashes, dict):
                raise DataUnavailableError(
                    "LocalSim recovery economic fact identities are invalid",
                    context={
                        "reason_code": "LOCALSIM_RECOVERY_ECONOMIC_FACT_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "field": field_name,
                    },
                )
            economic_fact_hashes[field_name] = raw_hashes
        paper_repository.readback_local_sim_economic_facts(
            run_id=run.run_id,
            order_ids=set(economic_fact_hashes["order_hashes"]),
            fill_ids=set(economic_fact_hashes["fill_hashes"]),
            order_event_ids=set(economic_fact_hashes["order_event_hashes"]),
            cash_fill_ids=set(economic_fact_hashes["cash_entry_hashes"]),
        )
        payload = outbox.projection_payload
        if payload.get("schema_version") == "local_sim_waiting_projection_payload_v1":
            paper_run = paper_repository.get_run(run.run_id)
            order_hashes = payload.get("order_hashes")
            if paper_run.status != RunStatus.RUNNING or not isinstance(order_hashes, dict):
                raise DataUnavailableError(
                    "LocalSim first-bar wait projection cannot be read back for recovery",
                    context={"reason_code": "LOCALSIM_RECOVERY_PAPER_READBACK_FAILED", "run_id": run.run_id},
                )
            persisted_order_hashes = {
                str(order.order_id): canonical_json_sha256(self._local_sim_fact_payload(order, fact_type="order"))
                for order in paper_repository.list_orders_for_run(run.run_id)
            }
            if persisted_order_hashes != order_hashes:
                raise DataUnavailableError(
                    "LocalSim first-bar wait orders do not match the projected generation",
                    context={"reason_code": "LOCALSIM_RECOVERY_PAPER_READBACK_FAILED", "run_id": run.run_id},
                )
            account_reader = getattr(context.local_broker, "query_account", None)
            position_reader = getattr(context.local_broker, "query_positions", None)
            if not callable(account_reader) or not callable(position_reader):
                raise DataUnavailableError(
                    "LocalSim first-bar wait recovery cannot read the current account truth",
                    context={"reason_code": "LOCALSIM_RECOVERY_PAPER_READBACK_FAILED", "run_id": run.run_id},
                )
            try:
                projected_positions = {
                    position.symbol: position
                    for position in (PositionLot.model_validate(raw_position) for raw_position in payload["positions"])
                }
                projected_cash = float(payload["cash_reference"])
            except Exception as exc:
                raise DataUnavailableError(
                    "LocalSim first-bar wait recovery account reference is invalid",
                    context={"reason_code": "LOCALSIM_RECOVERY_PROJECTION_SCHEMA_INVALID", "run_id": run.run_id},
                ) from exc
            self._validate_local_sim_duplicate_account_truth(
                run_id=run.run_id,
                projected_positions=projected_positions,
                projected_cash=projected_cash,
                observed_positions=dict(position_reader()),
                observed_account=account_reader(),
            )
            return
        if payload.get("schema_version") == "local_sim_valuation_pending_projection_payload_v1":
            completion = run.run_payload_json.get("local_sim_valuation_completion_v1")
            if not isinstance(completion, dict):
                raise DataUnavailableError(
                    "LocalSim valuation recovery completion evidence is missing",
                    context={
                        "reason_code": "LOCALSIM_RECOVERY_PROJECTION_SCHEMA_INVALID",
                        "run_id": run.run_id,
                    },
                )
            completion_body = dict(completion)
            completion_hash = str(completion_body.pop("completion_hash", ""))
            try:
                account_snapshot = AccountSnapshot.model_validate(completion_body.get("account_snapshot"))
                expected_position_count = len(payload["positions"])
            except Exception as exc:
                raise DataUnavailableError(
                    "LocalSim valuation recovery completion evidence is invalid",
                    context={
                        "reason_code": "LOCALSIM_RECOVERY_PROJECTION_SCHEMA_INVALID",
                        "run_id": run.run_id,
                    },
                ) from exc
            if (
                completion_body.get("outbox_id") != outbox.outbox_id
                or int(completion_body.get("generation") or 0) != outbox.generation
                or completion_body.get("economic_hash") != outbox.economic_hash
                or completion_hash != canonical_json_sha256(completion_body)
            ):
                raise DataUnavailableError(
                    "LocalSim valuation recovery completion identity conflicts",
                    context={
                        "reason_code": "LOCALSIM_RECOVERY_GENERATION_IDENTITY_CONFLICT",
                        "run_id": run.run_id,
                    },
                )
            paper_repository.readback_local_sim_projection(
                run_id=run.run_id,
                portfolio_id=account_snapshot.portfolio_id,
                trade_date=trade_date,
                outbox_id=outbox.outbox_id,
                generation=outbox.generation,
                expected_position_count=expected_position_count,
            )
            return
        if payload.get("schema_version") != "local_sim_projection_payload_v1":
            raise DataUnavailableError(
                "LocalSim failed run recovery projection kind is unsupported",
                context={"reason_code": "LOCALSIM_RECOVERY_PROJECTION_SCHEMA_INVALID", "run_id": run.run_id},
            )
        try:
            snapshot = AccountSnapshot.model_validate(payload.get("account_snapshot"))
            projection_trade_date = date.fromisoformat(str(payload.get("trade_date")))
            expected_position_count = len(payload["positions"])
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim failed run recovery projection payload is invalid",
                context={"reason_code": "LOCALSIM_RECOVERY_PROJECTION_SCHEMA_INVALID", "run_id": run.run_id},
            ) from exc
        paper_repository.readback_local_sim_projection(
            run_id=run.run_id,
            portfolio_id=snapshot.portfolio_id,
            trade_date=projection_trade_date,
            outbox_id=outbox.outbox_id,
            generation=outbox.generation,
            expected_position_count=expected_position_count,
        )

    def _prepare_localsim_build_result_for_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        build_result: SimulationPlanBuildResult,
        context: SimulationRunContext,
    ) -> SimulationPlanBuildResult:
        run, plan = self._prepare_localsim_execution_plan_for_submit(
            binding=binding,
            run=build_result.run,
            plan=build_result.execution_plan,
            context=context,
        )
        if run is build_result.run and plan is build_result.execution_plan:
            return build_result
        return replace(build_result, run=run, execution_plan=plan)

    def _prepare_localsim_execution_plan_for_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
    ) -> tuple[SimulationDailyRun, ExecutionPlan]:
        return self._local_sim_planner().prepare_execution_plan_for_submit(
            binding=binding,
            run=run,
            plan=plan,
            context=context,
        )

    def _local_sim_planner(self) -> LocalSimPlanner:
        return LocalSimPlanner(
            repository=self.repository,
            normalize_time=scheduler_time,
            schedule_windows=compute_schedule_windows,
        )

    def _cash_fit_localsim_execution_plan(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
    ) -> tuple[ExecutionPlan, dict[str, Any]]:
        return LocalSimPlanner.cash_fit_execution_plan(
            binding=binding,
            run=run,
            plan=plan,
            context=context,
        )

    @staticmethod
    def _copy_localsim_plan_with_intents(
        *,
        plan: ExecutionPlan,
        intents: list[Any],
        cash_fit_payload: dict[str, Any],
    ) -> ExecutionPlan:
        return LocalSimPlanner._copy_plan_with_intents(  # noqa: SLF001 - compatibility seam until PR-D1
            plan=plan,
            intents=intents,
            cash_fit_payload=cash_fit_payload,
        )

    @staticmethod
    def _effective_market_data_source_for_binding(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        default_data_source: str,
    ) -> str:
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            return MinuteDataSource.MINIQMT_REALTIME.value
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM and trade_date == date.today():
            return MinuteDataSource.TDX_REALTIME.value
        return default_data_source

    @staticmethod
    def _should_rebuild_miniqmt_plan_after_side_effect_free_failure(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
    ) -> bool:
        return (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status in {SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.SUCCEEDED}
            and SimulationLifecycleScheduler._b0_manifest_conflict_requires_plan_rebuild(run.run_payload_json)
        )

    def _should_rebuild_localsim_plan_after_side_effect_free_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        submit: bool,
        trade_date: date,
        as_of_time: datetime | None,
    ) -> bool:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return False
        if not submit:
            return False
        if bool(run.run_payload_json.get("broker_called")):
            return False
        if run.status == SimulationDailyRunStatus.SUCCEEDED and isinstance(
            run.run_payload_json.get("pre_trade_blocked_order_generation"), dict
        ):
            return self._localsim_realtime_quote_required(
                binding=binding,
                trade_date=trade_date,
                submit=submit,
                as_of_time=as_of_time,
            )
        if not self._localsim_realtime_quote_required(
            binding=binding,
            trade_date=trade_date,
            submit=submit,
            as_of_time=as_of_time,
        ):
            return False
        if run.status == SimulationDailyRunStatus.FAILED_RETRYABLE:
            failure = run.run_payload_json.get("submit_failure")
            if isinstance(failure, dict) and failure.get("stage") == "LOCAL_SIM_SUBMIT_FAILED":
                context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
                text = " ".join(
                    str(item or "")
                    for item in (
                        failure.get("message"),
                        context.get("cause"),
                        context.get("cause_code"),
                    )
                ).lower()
                if "insufficient cash" in text:
                    return True
        return (
            callable(getattr(self.context_provider, "load_context_for_phase", None))
            and run.status == SimulationDailyRunStatus.PLANNING_EXECUTION
        )

    @staticmethod
    def _localsim_realtime_quote_required(
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        submit: bool,
        as_of_time: datetime | None,
    ) -> bool:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM or not submit:
            return False
        local_as_of = scheduler_time(as_of_time)
        if trade_date != local_as_of.date():
            return False
        return any(
            window.get("state") == "ACTIVE" and window.get("action") == "submit"
            for window in compute_schedule_windows(trade_date=trade_date, as_of_time=local_as_of)
        )

    @staticmethod
    def _is_deterministic_localsim_submit_failure(exc: BaseException) -> bool:
        if not isinstance(exc, BrokerRejectedError):
            return False
        context = getattr(exc, "context", None)
        if not isinstance(context, dict):
            return False
        cause = str(context.get("cause") or "").lower()
        cause_code = str(context.get("cause_code") or "").upper()
        if "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause_code or "LOCAL_SIM_BOARD_LOT_VIOLATION".lower() in cause:
            return True
        return False

    @staticmethod
    def _deterministic_localsim_submit_reason_code(context: dict[str, Any]) -> str:
        cause_code = str(context.get("cause_code") or "").upper()
        cause = str(context.get("cause") or "").upper()
        if "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause_code or "LOCAL_SIM_BOARD_LOT_VIOLATION" in cause:
            return "LOCAL_SIM_BOARD_LOT_VIOLATION"
        return "LOCAL_SIM_DETERMINISTIC_SUBMIT_REJECTED"

    @staticmethod
    def _mini_qmt_batch_failed_without_broker_side_effect(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return False
        if SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(payload):
            return False
        if SimulationLifecycleScheduler._mini_qmt_batch_has_duplicate_order_remark(payload):
            return False
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        if status in {"PREFLIGHT_FAILED", "FAILED", "PARTIAL"}:
            try:
                failed = int(batch.get("failed", payload.get("failed_intents", 0)) or 0)
            except (TypeError, ValueError):
                failed = 0
            if failed > 0:
                return True
        return SimulationLifecycleScheduler._b0_manifest_conflict_requires_plan_rebuild(payload)

    @staticmethod
    def _b0_manifest_conflict_requires_plan_rebuild(payload: dict[str, Any]) -> bool:
        failure = payload.get("submit_failure")
        if not isinstance(failure, dict):
            return False
        context = failure.get("context")
        if not isinstance(context, dict):
            return False
        if str(context.get("reason_code") or "").upper() != "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT":
            return False
        conflicts = context.get("manifest_conflicts")
        if not isinstance(conflicts, dict) or not conflicts:
            return False
        fingerprint = canonical_json_sha256(
            {
                "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "manifest_conflicts": conflicts,
            }
        )
        previous = payload.get("miniqmt_side_effect_free_rebuild")
        if isinstance(previous, dict) and previous.get("failure_fingerprint") == fingerprint:
            # The same immutable conflict after one fresh-plan rebuild means
            # the runtime and compiler roots genuinely disagree. Rebuilding
            # forever would hide that deployment fault, so retain the loud
            # failure until the observed conflict changes.
            return False
        return True

    @staticmethod
    def _mini_qmt_side_effect_free_rebuild_receipt(run: SimulationDailyRun) -> dict[str, Any]:
        payload = run.run_payload_json if isinstance(run.run_payload_json, dict) else {}
        failure = payload.get("submit_failure") if isinstance(payload.get("submit_failure"), dict) else {}
        context = failure.get("context") if isinstance(failure.get("context"), dict) else {}
        reason_code = str(context.get("reason_code") or "MINIQMT_BATCH_SIDE_EFFECT_FREE_FAILURE").upper()
        receipt: dict[str, Any] = {
            "schema_version": "miniqmt_side_effect_free_plan_rebuild_v1",
            "reason_code": reason_code,
            "source_execution_plan_id": run.execution_plan_id,
            "broker_called": False,
        }
        conflicts = context.get("manifest_conflicts")
        if reason_code == "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT" and isinstance(conflicts, dict) and conflicts:
            receipt["manifest_conflicts"] = deepcopy(conflicts)
            receipt["failure_fingerprint"] = canonical_json_sha256(
                {
                    "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
                    "reason_code": reason_code,
                    "manifest_conflicts": conflicts,
                }
            )
        return receipt

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
        runtime_config = StrategyPackageSelectionService.release_selection_runtime_config(runtime_release)
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
            if self._selection_artifact_auto_generation_enabled(runtime_config):
                selection = self._run_auto_generated_selection_nonblocking(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                    created_by=created_by,
                )
            else:
                selection = self._run_selection_sync(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                    created_by=created_by,
                )
        except Exception as exc:
            if selection_cache is not None:
                selection_cache[cache_key] = exc
            raise
        if selection_cache is not None:
            selection_cache[cache_key] = selection
        return selection

    def _run_selection_sync(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        created_by: str,
    ) -> StrategyPackageSelectionResult:
        return self.selection_service.run_selection(
            package_ids=[binding.package_id],
            mode=SelectionMode.SINGLE_PACKAGE,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
            runtime_release=runtime_release,
            created_by=created_by,
        )

    def _run_auto_generated_selection_nonblocking(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
        created_by: str,
    ) -> StrategyPackageSelectionResult:
        if self._authoritative_selection_artifact_exists(
            binding=binding,
            runtime_release=runtime_release,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        ):
            return self._run_selection_sync(
                binding=binding,
                runtime_release=runtime_release,
                trade_date=trade_date,
                data_source=data_source,
                runtime_config=runtime_config,
                created_by=created_by,
            )

        key = self._selection_inference_key(
            binding=binding,
            trade_date=trade_date,
            data_source=data_source,
            runtime_config=runtime_config,
        )
        now = monotonic_time.monotonic()
        with self._selection_inference_lock:
            if self._selection_inference_shutdown:
                raise RuntimeConfigInvalidError(
                    "simulation scheduler selection inference executor is shut down",
                    context={
                        **self._selection_inference_context(
                            binding=binding,
                            runtime_release=runtime_release,
                            trade_date=trade_date,
                            data_source=data_source,
                            runtime_config=runtime_config,
                        ),
                        "reason_code": "SIMULATION_SELECTION_INFERENCE_EXECUTOR_SHUTDOWN",
                        "failure_stage": "SELECTION_INFERENCE",
                    },
                )
            entry = self._selection_inference_inflight.get(key)
            if entry is None:
                context = self._selection_inference_context(
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=runtime_config,
                )
                future = self._selection_inference_executor.submit(
                    self._run_selection_sync,
                    binding=binding,
                    runtime_release=runtime_release,
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config=deepcopy(runtime_config),
                    created_by=created_by,
                )
                entry = _SelectionInferenceInFlight(
                    key=key,
                    future=future,
                    started_monotonic=now,
                    started_at=datetime.now(UTC).isoformat(),
                    context=context,
                )
                self._selection_inference_inflight[key] = entry
                logger.warning(
                    "Simulation scheduler dispatched auto-generated selection inference off tick thread: %s",
                    context,
                )
            if entry.future.done():
                self._selection_inference_inflight.pop(key, None)
                future = entry.future
            else:
                elapsed = now - entry.started_monotonic
                if elapsed >= self._selection_inference_timeout_seconds:
                    entry.timed_out = True
                    raise self._selection_inference_timeout_error(entry, elapsed_seconds=elapsed)
                raise self._selection_inference_pending_error(entry, elapsed_seconds=elapsed)
        return future.result()

    @staticmethod
    def _selection_artifact_auto_generation_enabled(runtime_config: dict[str, Any]) -> bool:
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(runtime_config)
        return bool(artifact_config.get("auto_generate"))

    def _authoritative_selection_artifact_exists(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> bool:
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(runtime_config)
        if bool(artifact_config.get("force_regenerate")):
            return False
        artifact_repository = self._selection_artifact_repository()
        if artifact_repository is None:
            return False
        runtime_hashes = self._selection_artifact_runtime_hashes(
            binding=binding,
            runtime_release=runtime_release,
            runtime_config=runtime_config,
        )
        for runtime_hash in runtime_hashes:
            try:
                artifact = artifact_repository.get(
                    package_id=binding.package_id,
                    manifest_sha256=binding.manifest_sha256 or runtime_release.manifest_sha256 or "",
                    trade_date=trade_date,
                    data_source=data_source,
                    runtime_config_hash=runtime_hash,
                )
            except DataUnavailableError:
                continue
            metadata = artifact.metadata or {}
            if (
                artifact.status.value == "SUCCEEDED"
                and artifact.scores_json
                and metadata.get("source_type") == AUTHORITATIVE_SELECTION_SOURCE_TYPE
                and metadata.get("authority_scope") == AUTHORITATIVE_SELECTION_SCOPE
            ):
                return True
        return False

    def _selection_artifact_runtime_hashes(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        runtime_config: dict[str, Any],
    ) -> list[str]:
        hashes = list(_candidate_selection_artifact_runtime_hashes(runtime_config))
        manifest = self._selection_package_manifest(binding=binding, runtime_release=runtime_release)
        if manifest is not None and manifest.alpha_mode == AlphaMode.MULTI_ALPHA:
            multi_hash = multi_alpha_selection_artifact_runtime_hash(manifest, runtime_config)
            hashes = [multi_hash, *[item for item in hashes if item != multi_hash]]
        return hashes

    def _selection_package_manifest(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
    ) -> StrategyPackageManifest | None:
        package_repository = getattr(self.selection_service, "package_repository", None)
        getter = getattr(package_repository, "get", None)
        if not callable(getter):
            return None
        record = getter(binding.package_id)
        manifest = record.current_manifest()
        if not manifest.manifest_sha256:
            return manifest.model_copy(
                update={"manifest_sha256": binding.manifest_sha256 or runtime_release.manifest_sha256}
            )
        return manifest

    def _selection_artifact_repository(self) -> Any | None:
        candidates = [
            getattr(self.selection_service, "runtime", None),
            getattr(getattr(self.selection_service, "signal_service", None), "runtime", None),
            getattr(self.selection_service, "selection_artifact_service", None),
            getattr(getattr(self.selection_service, "signal_service", None), "selection_artifact_service", None),
        ]
        for candidate in candidates:
            artifact_repository = getattr(candidate, "artifact_repository", None)
            if artifact_repository is not None and callable(getattr(artifact_repository, "get", None)):
                return artifact_repository
        return None

    def _selection_inference_key(
        self,
        *,
        binding: SimulationReleaseBinding,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> tuple[Any, ...]:
        return (
            binding.package_id,
            binding.manifest_sha256,
            binding.release_id,
            binding.release_hash,
            trade_date.isoformat(),
            data_source,
            selection_artifact_runtime_hash(runtime_config),
        )

    def _selection_inference_context(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        runtime_config: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "stage": "SELECTION_INFERENCE",
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "package_id": binding.package_id,
            "manifest_sha256": binding.manifest_sha256 or runtime_release.manifest_sha256,
            "release_id": runtime_release.release_id,
            "release_hash": runtime_release.release_hash,
            "trade_date": trade_date.isoformat(),
            "data_source": data_source,
            "runtime_config_hash": selection_artifact_runtime_hash(runtime_config),
        }

    def _selection_inference_pending_error(
        self,
        entry: _SelectionInferenceInFlight,
        *,
        elapsed_seconds: float,
    ) -> DataUnavailableError:
        return DataUnavailableError(
            "simulation scheduler selection inference is running asynchronously; tick will continue",
            context={
                **entry.context,
                "reason_code": "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS",
                "failure_stage": "SELECTION_INFERENCE",
                "started_at": entry.started_at,
                "elapsed_seconds": round(max(0.0, elapsed_seconds), 3),
                "timeout_seconds": self._selection_inference_timeout_seconds,
            },
        )

    def _selection_inference_timeout_error(
        self,
        entry: _SelectionInferenceInFlight,
        *,
        elapsed_seconds: float,
    ) -> ArtifactGenerationFailedError:
        return ArtifactGenerationFailedError(
            "simulation scheduler selection inference timed out; tick remains non-blocking",
            context={
                **entry.context,
                "reason_code": "SIMULATION_SELECTION_INFERENCE_TIMEOUT",
                "failure_stage": "SELECTION_INFERENCE",
                "started_at": entry.started_at,
                "elapsed_seconds": round(max(0.0, elapsed_seconds), 3),
                "timeout_seconds": self._selection_inference_timeout_seconds,
                "thread_isolated": True,
            },
        )

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
            backend = (
                binding.broker_backend.value
                if isinstance(binding.broker_backend, SimulationBrokerBackend)
                else str(binding.broker_backend)
            )
            backends_by_key.setdefault(key, set()).add(backend)
        return {key for key, backends in backends_by_key.items() if len(backends) > 1}

    def _persist_strategy_performance(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
        local_persistence: LocalSimPersistenceResult | None = None,
    ) -> dict[str, Any]:
        if local_persistence is not None:
            latest = self.repository.get_simulation_daily_run(run.run_id)
            payload = latest.run_payload_json.get("strategy_performance")
            if (
                not isinstance(payload, dict)
                or int(payload.get("local_sim_generation") or 0) != local_persistence.generation
            ):
                raise DataUnavailableError(
                    "LocalSim performance projection generation does not match economic facts",
                    context={
                        "reason_code": "LOCALSIM_PERFORMANCE_GENERATION_CONFLICT",
                        "run_id": run.run_id,
                        "expected_generation": local_persistence.generation,
                    },
                )
            return payload
        marks = self._performance_marks(context)
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and (
            context.qmt_ledger_repository is None or self._has_miniqmt_position_reconciliation_adjustments(context)
        ):
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
        elif (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and context.qmt_ledger_repository is not None
        ):
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
        if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM:
            latest_payload = self.repository.get_simulation_daily_run(run.run_id).run_payload_json
            payload = with_miniqmt_capacity_residual_observability(
                payload,
                self._miniqmt_capacity_residual_observability(latest_payload),
            )
        self.repository.update_simulation_daily_run(
            run.run_id,
            payload_patch={
                "strategy_performance": payload,
                "performance_projection": payload,
            },
        )
        return payload

    @staticmethod
    def _has_miniqmt_position_reconciliation_adjustments(context: SimulationRunContext) -> bool:
        diagnostics = context.context_diagnostics.get("miniqmt_broker_position_reconciliation")
        if not isinstance(diagnostics, dict):
            return False
        return bool(
            int(diagnostics.get("dropped_position_count") or 0) > 0
            or int(diagnostics.get("capped_position_count") or 0) > 0
        )

    def _local_sim_persistence_coordinator(self) -> LocalSimPersistenceCoordinator:
        return LocalSimPersistenceCoordinator(
            runtime_repository=self.repository,
            performance_service=self.performance_service,
            filter_snapshot_by_plan=self._filter_local_sim_snapshot_by_plan,
            validate_execution_states=self._validate_local_sim_execution_states,
            validate_snapshot_for_progress=self._validate_local_sim_snapshot_for_progress,
            validate_snapshot_for_success=self._validate_local_sim_snapshot_for_success,
            paper_repository_for=self._paper_repository_for_local_sim,
            historical_residual_payload=self._local_sim_historical_residual_payload,
            snapshot_time=self._local_sim_snapshot_time,
            cash_after=self._cash_after_local_sim,
            position_marks=self._local_sim_position_marks,
            mark_failure_allows_pending=self._local_sim_mark_failure_allows_valuation_pending,
            ensure_paper_run=self._ensure_local_sim_paper_run,
            mark_submit_failure=self.orchestrator.mark_submit_failure,
            load_existing_plan_context=self._load_existing_plan_context,
            effective_market_data_source=self._effective_market_data_source_for_binding,
            normalize_time=scheduler_time,
            binding_result_factory=SimulationSchedulerBindingResult,
        )

    def _persist_local_sim_execution_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        context: SimulationRunContext,
    ) -> LocalSimPersistenceResult | None:
        return self._local_sim_persistence_coordinator().persist_execution_result(
            binding=binding,
            run=run,
            execution=execution,
            context=context,
        )

    @staticmethod
    def _local_sim_json_value(value: Any, *, path: str = "$") -> Any:
        return canonical_local_sim_json_value(value, path=path)

    @staticmethod
    def _local_sim_fact_payload(item: Any, *, fact_type: str) -> dict[str, Any]:
        return local_sim_fact_payload(item, fact_type=fact_type)

    @staticmethod
    def _local_sim_mark_failure_allows_valuation_pending(exc: BaseException) -> bool:
        """Return true only for explicit, retryable mark-availability gaps."""

        allowed = {
            "LOCALSIM_MARK_PRICE_MISSING",
            "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE",
        }
        current: BaseException | None = exc
        seen: set[int] = set()
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            context = dict(getattr(current, "context", None) or {})
            if str(context.get("reason_code") or "") in allowed:
                return True
            current = current.__cause__ or current.__context__
        return False

    def _local_sim_existing_projection_result(
        self,
        *,
        run_id: str,
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> LocalSimPersistenceResult:
        return LocalSimProjector(
            runtime_repository=self.repository,
        ).existing_projection_result(
            run_id=run_id,
            observed_positions=observed_positions,
            observed_account=observed_account,
        )

    @staticmethod
    def _validate_local_sim_duplicate_account_truth(
        *,
        run_id: str,
        projected_positions: dict[str, PositionLot],
        projected_cash: float,
        observed_positions: dict[str, PositionLot],
        observed_account: Any,
    ) -> None:
        validate_local_sim_duplicate_account_truth(
            run_id=run_id,
            projected_positions=projected_positions,
            projected_cash=projected_cash,
            observed_positions=observed_positions,
            observed_account=observed_account,
        )

    def _replay_pending_local_sim_projection(
        self,
        *,
        run_id: str,
        paper_repository: Any,
        context: SimulationRunContext | None = None,
        execution: Any | None = None,
        observed_positions: dict[str, PositionLot] | None = None,
        observed_account: Any | None = None,
        valuation_as_of_time: datetime | None = None,
    ) -> None:
        return self._local_sim_persistence_coordinator().replay_pending_projection(
            run_id=run_id,
            paper_repository=paper_repository,
            context=context,
            execution=execution,
            observed_positions=observed_positions,
            observed_account=observed_account,
            valuation_as_of_time=valuation_as_of_time,
        )

    def _project_local_sim_first_causal_bar_wait_outbox(
        self,
        *,
        run: SimulationDailyRun,
        outbox: LocalSimProjectionOutboxV1,
        paper_repository: Any,
    ) -> tuple[SimulationDailyRun, dict[str, Any]]:
        return LocalSimProjector(
            runtime_repository=self.repository,
            paper_repository=paper_repository,
        ).project_first_causal_bar_wait_outbox(
            run=run,
            outbox=outbox,
            paper_repository=paper_repository,
        )

    def _project_local_sim_outbox(
        self, *, run_id: str, paper_repository: Any
    ) -> tuple[SimulationDailyRun, dict[str, Any]]:
        return LocalSimProjector(
            runtime_repository=self.repository,
            paper_repository=paper_repository,
            performance_service=self.performance_service,
        ).project_outbox(
            run_id=run_id,
            paper_repository=paper_repository,
        )

    @staticmethod
    def _local_sim_projection_error_is_retryable(exc: BaseException) -> bool:
        return local_sim_projection_error_is_retryable(exc)

    @staticmethod
    def _filter_local_sim_snapshot_by_plan(
        *,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
    ) -> tuple[tuple[Any, ...], tuple[Any, ...], tuple[Any, ...], tuple[Any, ...]]:
        plan_intent_ids = {intent.intent_id for intent in execution.execution_plan.intents}
        unexpected_orders = tuple(order for order in orders if getattr(order, "intent_id", None) not in plan_intent_ids)
        if unexpected_orders:
            raise DataUnavailableError(
                "LocalSim execution snapshot contains orders outside the frozen execution plan",
                context={
                    "reason_code": "LOCALSIM_SNAPSHOT_PLAN_IDENTITY_CONFLICT",
                    "run_id": execution.run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "unexpected_order_ids": sorted(str(getattr(order, "order_id", "")) for order in unexpected_orders),
                    "unexpected_intent_ids": sorted(
                        str(getattr(order, "intent_id", "")) for order in unexpected_orders
                    ),
                },
            )
        selected_orders = tuple(order for order in orders if getattr(order, "intent_id", None) in plan_intent_ids)
        selected_order_ids = {getattr(order, "order_id", None) for order in selected_orders}
        unexpected_fills = tuple(fill for fill in fills if getattr(fill, "order_id", None) not in selected_order_ids)
        unexpected_events = tuple(
            event
            for event in events
            if getattr(event, "order_id", None) not in selected_order_ids
            or (
                getattr(event, "fill", None) is not None
                and getattr(getattr(event, "fill", None), "fill_id", None)
                not in {
                    getattr(fill, "fill_id", None)
                    for fill in fills
                    if getattr(fill, "order_id", None) in selected_order_ids
                }
            )
        )
        selected_fill_ids = {
            getattr(fill, "fill_id", None) for fill in fills if getattr(fill, "order_id", None) in selected_order_ids
        }
        unexpected_cash_entries = tuple(
            entry for entry in cash_entries if getattr(entry, "fill_id", None) not in selected_fill_ids
        )
        if unexpected_fills or unexpected_events or unexpected_cash_entries:
            raise DataUnavailableError(
                "LocalSim execution snapshot contains economic facts outside the frozen execution plan",
                context={
                    "reason_code": "LOCALSIM_SNAPSHOT_PLAN_IDENTITY_CONFLICT",
                    "run_id": execution.run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "unexpected_fill_ids": sorted(str(getattr(fill, "fill_id", "")) for fill in unexpected_fills),
                    "unexpected_event_ids": sorted(str(getattr(event, "event_id", "")) for event in unexpected_events),
                    "unexpected_cash_fill_ids": sorted(
                        str(getattr(entry, "fill_id", "")) for entry in unexpected_cash_entries
                    ),
                },
            )
        selected_fills = tuple(fill for fill in fills if getattr(fill, "order_id", None) in selected_order_ids)
        selected_fill_ids = {getattr(fill, "fill_id", None) for fill in selected_fills}
        selected_events = tuple(
            event
            for event in events
            if getattr(event, "order_id", None) in selected_order_ids
            and (
                getattr(event, "fill", None) is None
                or getattr(getattr(event, "fill", None), "fill_id", None) in selected_fill_ids
            )
        )
        selected_cash_entries = tuple(
            entry for entry in cash_entries if getattr(entry, "fill_id", None) in selected_fill_ids
        )
        return selected_orders, selected_fills, selected_events, selected_cash_entries

    @staticmethod
    def _local_sim_historical_residual_payload(
        *,
        run: SimulationDailyRun,
        orders: tuple[Any, ...],
    ) -> dict[str, Any] | None:
        payload = run.run_payload_json.get("local_sim_cash_fit")
        if isinstance(payload, dict):
            if payload.get("schema_version") != "localsim_capital_dependency_v1":
                raise DataUnavailableError(
                    "LocalSim capital-dependency plan payload has an invalid schema",
                    context={
                        "reason_code": "LOCALSIM_CAPITAL_DEPENDENCY_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "schema_version": payload.get("schema_version"),
                    },
                )
            prepared_raw = payload.get("prepared_intent_count")
            if isinstance(prepared_raw, bool) or not isinstance(prepared_raw, int) or prepared_raw < 0:
                raise DataUnavailableError(
                    "LocalSim capital-dependency prepared intent count is invalid",
                    context={
                        "reason_code": "LOCALSIM_CAPITAL_DEPENDENCY_COUNT_INVALID",
                        "run_id": run.run_id,
                        "prepared_intent_count": prepared_raw,
                    },
                )
            prepared_intent_count = prepared_raw
        else:
            prepared_intent_count = len(orders)
        residual_orders: list[dict[str, Any]] = []
        capital_residual_count = 0
        schedule_residual_count = 0
        for order in orders:
            dependency = (
                order.metadata.get("local_sim_capital_dependency")
                if isinstance(getattr(order, "metadata", None), dict)
                else None
            )
            if int(getattr(order, "remaining_quantity", 0) or 0) <= 0:
                continue
            if isinstance(dependency, dict):
                if (
                    dependency.get("schema_version") != "local_sim_capital_dependency_order_v1"
                    or dependency.get("status") != "CAPITAL_LIMITED"
                ):
                    raise DataUnavailableError(
                        "LocalSim order capital-dependency payload has an invalid schema or status",
                        context={
                            "reason_code": "LOCALSIM_CAPITAL_DEPENDENCY_ORDER_INVALID",
                            "run_id": run.run_id,
                            "order_id": order.order_id,
                            "intent_id": order.intent_id,
                        },
                    )
                counts = {
                    field: dependency.get(field)
                    for field in ("attempted_quantity", "accepted_quantity", "waiting_quantity")
                }
                if (
                    any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in counts.values())
                    or counts["attempted_quantity"] != counts["accepted_quantity"] + counts["waiting_quantity"]
                ):
                    raise DataUnavailableError(
                        "LocalSim order capital-dependency quantities are invalid",
                        context={
                            "reason_code": "LOCALSIM_CAPITAL_DEPENDENCY_ORDER_COUNT_INVALID",
                            "run_id": run.run_id,
                            "order_id": order.order_id,
                            **counts,
                        },
                    )
            classification = (
                "CAPITAL_RESIDUAL" if isinstance(dependency, dict) else "SCHEDULE_RESIDUAL_AT_HISTORICAL_CLOSE"
            )
            if classification == "CAPITAL_RESIDUAL":
                capital_residual_count += 1
            else:
                schedule_residual_count += 1
            residual_orders.append(
                {
                    "order_id": order.order_id,
                    "intent_id": order.intent_id,
                    "symbol": order.symbol,
                    "side": order.side.value,
                    "order_quantity": order.quantity,
                    "filled_quantity": order.filled_quantity,
                    "remaining_quantity": order.remaining_quantity,
                    "classification": classification,
                    "capital_dependency": (dict(dependency) if isinstance(dependency, dict) else None),
                }
            )
        if not residual_orders:
            return None
        return {
            "schema_version": "localsim_historical_residual_v1",
            "status": "HISTORICAL_EXECUTION_RESIDUAL",
            "reason": "historical_broker_execution_ended_with_remaining_quantity",
            "prepared_intent_count": prepared_intent_count,
            "residual_order_count": len(residual_orders),
            "capital_residual_count": capital_residual_count,
            "schedule_residual_count": schedule_residual_count,
            "residual_orders": residual_orders,
        }

    def _paper_repository_for_local_sim(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> Any:
        if context.paper_repository is not None:
            return context.paper_repository
        if isinstance(self.repository, InMemorySimulationRuntimeRepository):
            repository = InMemoryPaperTradingV2Repository()
            object.__setattr__(context, "paper_repository", repository)
            return repository
        return self._build_dependency(
            _default_paper_repository_factory,
            "PaperTradingV2Repository",
            binding=binding,
            trade_date=run.trade_date,
        )

    @staticmethod
    def _validate_local_sim_snapshot_for_progress(
        *,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
    ) -> None:
        if len(orders) != len(execution.execution_plan.intents):
            raise DataUnavailableError(
                "LocalSim execution snapshot order count does not match execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_ORDER_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_order_count": len(execution.execution_plan.intents),
                    "actual_order_count": len(orders),
                },
            )

    @staticmethod
    def _validate_local_sim_execution_states(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
        states: tuple[LocalSimExecutionStateV1, ...],
    ) -> None:
        expected_intents = {intent.intent_id for intent in execution.execution_plan.intents}
        actual_intents = {state.intent_id for state in states}
        if len(states) != len(actual_intents) or actual_intents != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable execution states do not match execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(actual_intents),
                    "state_count": len(states),
                },
            )
        orders_by_intent = {str(order.intent_id): order for order in orders}
        if len(orders_by_intent) != len(orders) or set(orders_by_intent) != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable orders do not form a one-to-one map with execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_ORDER_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(orders_by_intent),
                    "order_count": len(orders),
                },
            )
        intents_by_id = {intent.intent_id: intent for intent in execution.execution_plan.intents}
        for state in states:
            if (
                state.run_id != run.run_id
                or state.binding_id != binding.binding_id
                or state.plan_id != execution.execution_plan.plan_id
                or state.trade_date != run.trade_date
            ):
                raise DataUnavailableError(
                    "LocalSim durable execution state identity conflicts with the active run",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "state_run_id": state.run_id,
                        "run_id": run.run_id,
                        "state_binding_id": state.binding_id,
                        "binding_id": binding.binding_id,
                        "state_plan_id": state.plan_id,
                        "plan_id": execution.execution_plan.plan_id,
                        "state_trade_date": state.trade_date.isoformat(),
                        "trade_date": run.trade_date.isoformat(),
                    },
                )
            order = orders_by_intent[state.intent_id]
            intent = intents_by_id[state.intent_id]
            order_side = getattr(getattr(order, "side", None), "value", getattr(order, "side", None))
            intent_side = getattr(getattr(intent, "side", None), "value", getattr(intent, "side", None))
            state_side = getattr(state.side, "value", state.side)
            if (
                state.order_id != str(getattr(order, "order_id", ""))
                or state.portfolio_id != execution.execution_plan.portfolio_id
                or state.symbol != str(getattr(order, "symbol", ""))
                or state.symbol != intent.symbol
                or state_side != order_side
                or state_side != intent_side
                or state.total_quantity != int(getattr(order, "quantity", -1))
                or state.total_quantity != intent.order_quantity
                or state.filled_quantity != int(getattr(order, "filled_quantity", -1))
                or state.remaining_quantity != int(getattr(order, "remaining_quantity", -1))
                or state.order_status
                != str(getattr(getattr(order, "status", None), "value", getattr(order, "status", "")))
            ):
                raise DataUnavailableError(
                    "LocalSim durable state, order and frozen intent identities do not close",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_ACTION_CHAIN_CONFLICT",
                        "run_id": run.run_id,
                        "plan_id": execution.execution_plan.plan_id,
                        "intent_id": state.intent_id,
                        "state_id": state.state_id,
                        "state_order_id": state.order_id,
                        "order_id": str(getattr(order, "order_id", "")),
                        "state_symbol": state.symbol,
                        "order_symbol": str(getattr(order, "symbol", "")),
                        "intent_symbol": intent.symbol,
                    },
                )

    def _validate_local_sim_snapshot_for_success(
        self,
        *,
        run: SimulationDailyRun,
        execution: SimulationExecutionResult,
        orders: tuple[Any, ...],
        fills: tuple[Any, ...],
        cash_entries: tuple[Any, ...],
    ) -> None:
        if len(orders) != len(execution.execution_plan.intents):
            raise DataUnavailableError(
                "LocalSim execution snapshot order count does not match execution plan intents",
                context={
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "expected_order_count": len(execution.execution_plan.intents),
                    "actual_order_count": len(orders),
                },
            )
        historical_residual = self._local_sim_historical_residual_payload(run=run, orders=orders)
        if fills and not cash_entries:
            raise DataUnavailableError(
                "LocalSim execution fills require matching durable cash ledger entries",
                context={
                    "reason_code": "LOCALSIM_PERSISTENCE_FILL_CASH_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                },
            )
        if not fills and historical_residual is None:
            raise DataUnavailableError(
                "LocalSim execution cannot terminate without durable fills or explicit capital residual evidence",
                context={
                    "reason_code": "LOCALSIM_PERSISTENCE_EMPTY_EFFECTS",
                    "run_id": run.run_id,
                    "plan_id": execution.execution_plan.plan_id,
                    "order_count": len(orders),
                    "fill_count": len(fills),
                    "cash_ledger_count": len(cash_entries),
                },
            )

    @staticmethod
    def _ensure_local_sim_paper_run(
        *,
        repository: Any,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> None:
        portfolio_id = str(context.portfolio_id or run.strategy_id)
        existing = repository.get_run_by_portfolio_date(portfolio_id, run.trade_date)
        if existing is not None and existing.run_id != run.run_id:
            raise RuntimeConfigInvalidError(
                "LocalSim simulation runtime cannot persist into a different Paper v2 run for the same portfolio/date",
                context={
                    "simulation_run_id": run.run_id,
                    "existing_run_id": existing.run_id,
                    "portfolio_id": portfolio_id,
                    "trade_date": run.trade_date.isoformat(),
                },
            )
        if existing is None:
            data_source = MinuteDataSource(str(context.market_data_source or MinuteDataSource.DB_HISTORICAL.value))
            repository.create_run(
                PaperRun(
                    run_id=run.run_id,
                    portfolio_id=portfolio_id,
                    trade_date=run.trade_date,
                    status=RunStatus.RUNNING,
                    data_source=data_source,
                    runtime_config={
                        "source": "simulation_runtime_local_sim",
                        "simulation_run_id": run.run_id,
                        "selection_evidence_id": run.selection_evidence_id,
                        "execution_plan_id": run.execution_plan_id,
                    },
                )
            )

    @staticmethod
    def _local_sim_position_marks(
        *,
        positions: dict[str, PositionLot],
        context: SimulationRunContext,
        execution: SimulationExecutionResult,
        snapshot_time: datetime,
    ) -> tuple[dict[str, float], dict[str, LocalSimMarketMarkV1]]:
        source = str(context.market_data_source or "").strip()
        if positions and source not in {MinuteDataSource.TDX_REALTIME.value, MinuteDataSource.DB_HISTORICAL.value}:
            raise DataUnavailableError(
                "LocalSim market mark source is missing or unsupported",
                context={
                    "reason_code": "LOCALSIM_MARK_SOURCE_INVALID",
                    "source": source or None,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        loader = getattr(context.local_broker, "load_authoritative_position_marks", None)
        if positions and not callable(loader):
            raise DataUnavailableError(
                "LocalSim account generation requires an authoritative market-mark provider",
                context={
                    "reason_code": "LOCALSIM_MARK_PROVIDER_UNAVAILABLE",
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        raw_records = (
            loader(
                symbols=tuple(positions),
                trade_date=execution.run.trade_date,
                as_of_time=snapshot_time,
                pre_trade_tradability=context.pre_trade_tradability,
                previous_marks=SimulationLifecycleScheduler._previous_local_sim_mark_records(execution.run),
            )
            if positions
            else {}
        )
        if not isinstance(raw_records, Mapping):
            raise DataUnavailableError(
                "LocalSim authoritative market-mark provider returned an invalid payload",
                context={
                    "reason_code": "LOCALSIM_MARK_SCHEMA_INVALID",
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        missing = sorted(symbol for symbol in positions if symbol not in raw_records)
        unexpected = sorted(symbol for symbol in raw_records if symbol not in positions)
        if missing or unexpected:
            raise DataUnavailableError(
                "LocalSim authoritative market-mark identities do not match persisted positions",
                context={
                    "reason_code": "LOCALSIM_MARK_IDENTITY_CONFLICT",
                    "missing_symbols": missing,
                    "unexpected_symbols": unexpected,
                    "plan_id": execution.execution_plan.plan_id,
                },
            )
        accepted: dict[str, float] = {}
        records: dict[str, LocalSimMarketMarkV1] = {}
        for symbol in sorted(positions):
            try:
                record = LocalSimMarketMarkV1.model_validate(raw_records[symbol])
            except Exception as exc:
                raise DataUnavailableError(
                    "LocalSim authoritative market mark failed schema or hash validation",
                    context={"reason_code": "LOCALSIM_MARK_SCHEMA_INVALID", "symbol": symbol},
                ) from exc
            if record.symbol != symbol:
                raise DataUnavailableError(
                    "LocalSim authoritative market mark symbol conflicts with the persisted position",
                    context={
                        "reason_code": "LOCALSIM_MARK_IDENTITY_CONFLICT",
                        "symbol": symbol,
                        "mark_symbol": record.symbol,
                    },
                )
            if record.as_of_time.replace(tzinfo=None) > snapshot_time.replace(tzinfo=None):
                raise DataUnavailableError(
                    "LocalSim authoritative market mark is later than the account snapshot",
                    context={
                        "reason_code": "LOCALSIM_MARK_AS_OF_CONFLICT",
                        "symbol": symbol,
                        "mark_as_of_time": record.as_of_time.isoformat(),
                        "snapshot_time": snapshot_time.isoformat(),
                    },
                )
            tradability = context.pre_trade_tradability.get(symbol)
            suspended = pre_trade_tradability_is_suspended(tradability, symbol=symbol)
            if suspended:
                daily_reference = tradability.get("daily_trading_context") if isinstance(tradability, Mapping) else None
                raw_fact = daily_reference.get("symbol_fact") if isinstance(daily_reference, Mapping) else None
                schema_version = daily_reference.get("schema_version") if isinstance(daily_reference, Mapping) else None
                try:
                    if schema_version == "daily_trading_context_reference_v1":
                        fact = DailyTradingSymbolFactV1.model_validate(dict(raw_fact))
                    elif schema_version == "daily_trading_context_reference_v2":
                        fact = DailyTradingSymbolFactV2.model_validate(dict(raw_fact))
                    else:
                        raise ValueError("unsupported frozen daily trading reference schema")
                except Exception as exc:
                    raise DataUnavailableError(
                        "LocalSim suspended market mark requires a valid frozen daily trading fact",
                        context={
                            "reason_code": "LOCALSIM_SUSPENDED_DAILY_FACT_INVALID",
                            "symbol": symbol,
                            "trade_date": execution.run.trade_date.isoformat(),
                        },
                    ) from exc
                expected_source = (
                    f"{fact.pre_close_source}:frozen_daily_trading_context_v1"
                    if isinstance(fact, DailyTradingSymbolFactV1)
                    else f"{fact.limit_authority.value}:frozen_daily_trading_context_v2"
                )
                if (
                    fact.symbol != symbol
                    or fact.trade_date != execution.run.trade_date
                    or fact.pre_close is None
                    or record.provenance != LocalSimMarketMarkProvenance.SUSPENDED_PREV_CLOSE
                    or float(record.price) != float(fact.pre_close)
                    or record.source != expected_source
                    or record.as_of_time.replace(tzinfo=None) != snapshot_time.replace(tzinfo=None)
                ):
                    raise DataUnavailableError(
                        "LocalSim suspended mark is not proven by the previous trading-day close",
                        context={
                            "reason_code": "LOCALSIM_SUSPENDED_PREV_CLOSE_UNPROVEN",
                            "symbol": symbol,
                            "mark_as_of_time": record.as_of_time.isoformat(),
                            "snapshot_time": snapshot_time.isoformat(),
                            "source": record.source,
                            "expected_source": expected_source,
                            "mark_price": float(record.price),
                            "expected_pre_close": float(fact.pre_close) if fact.pre_close is not None else None,
                        },
                    )
            else:
                expected_provenance = (
                    LocalSimMarketMarkProvenance.REALTIME_MINUTE_CLOSE
                    if source == MinuteDataSource.TDX_REALTIME.value
                    else LocalSimMarketMarkProvenance.HISTORICAL_MINUTE_CLOSE
                )
                if (
                    record.provenance != expected_provenance
                    or record.source != source
                    or record.as_of_time.date() != execution.run.trade_date
                ):
                    raise DataUnavailableError(
                        "LocalSim market mark provenance does not match the selected execution source",
                        context={
                            "reason_code": "LOCALSIM_MARK_PROVENANCE_CONFLICT",
                            "symbol": symbol,
                            "expected_source": source,
                            "actual_source": record.source,
                            "expected_provenance": expected_provenance.value,
                            "actual_provenance": record.provenance.value,
                        },
                    )
            accepted[symbol] = float(record.price)
            records[symbol] = record
        return accepted, records

    @staticmethod
    def _previous_local_sim_mark_records(run: SimulationDailyRun) -> dict[str, dict[str, Any]]:
        if "local_sim_projection_outbox_v1" not in run.run_payload_json:
            return {}
        raw_outbox = run.run_payload_json["local_sim_projection_outbox_v1"]
        if not isinstance(raw_outbox, dict):
            raise DataUnavailableError(
                "LocalSim previous projection outbox payload is invalid",
                context={
                    "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "layer": "local_sim_projection_outbox_v1",
                },
            )
        projection_payload = raw_outbox.get("projection_payload")
        if not isinstance(projection_payload, dict):
            raise DataUnavailableError(
                "LocalSim previous projection payload is invalid",
                context={
                    "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "layer": "projection_payload",
                },
            )
        if projection_payload.get("schema_version") == "local_sim_waiting_projection_payload_v1":
            if (
                projection_payload.get("projection_kind") != "FIRST_CAUSAL_BAR_WAIT"
                or "marks" in projection_payload
                or projection_payload.get("final_simulation_status") != SimulationDailyRunStatus.INTRADAY_RUNNING.value
            ):
                raise DataUnavailableError(
                    "LocalSim first-bar wait projection has invalid previous-mark semantics",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "layer": "waiting_projection_payload",
                    },
                )
            return {}
        if projection_payload.get("schema_version") == "local_sim_valuation_pending_projection_payload_v1":
            if projection_payload.get("projection_kind") != "VALUATION_PENDING":
                raise DataUnavailableError(
                    "LocalSim valuation-pending projection has invalid previous-mark semantics",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "layer": "valuation_pending_projection_payload",
                    },
                )
            completion = run.run_payload_json.get("local_sim_valuation_completion_v1")
            if completion is None:
                return {}
            if not isinstance(completion, dict) or not isinstance(completion.get("marks"), list):
                raise DataUnavailableError(
                    "LocalSim valuation completion mark collection is invalid",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "layer": "local_sim_valuation_completion_v1",
                    },
                )
            raw_marks = completion["marks"]
        else:
            raw_marks = projection_payload.get("marks")
        if not isinstance(raw_marks, list):
            raise DataUnavailableError(
                "LocalSim previous market mark collection is invalid",
                context={
                    "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                    "run_id": run.run_id,
                    "layer": "marks",
                },
            )
        records: dict[str, dict[str, Any]] = {}
        for raw in raw_marks:
            if not isinstance(raw, dict):
                raise DataUnavailableError(
                    "LocalSim previous market mark payload is invalid",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                    },
                )
            try:
                mark = LocalSimMarketMarkV1.model_validate(raw)
            except Exception as exc:
                raise DataUnavailableError(
                    "LocalSim previous market mark failed schema or hash validation",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID",
                        "run_id": run.run_id,
                        "symbol": raw.get("symbol"),
                    },
                ) from exc
            if mark.symbol in records:
                raise DataUnavailableError(
                    "LocalSim previous market mark payload contains a duplicate symbol",
                    context={
                        "reason_code": "LOCALSIM_PREVIOUS_MARK_IDENTITY_CONFLICT",
                        "run_id": run.run_id,
                        "symbol": mark.symbol,
                    },
                )
            records[mark.symbol] = mark.model_dump(mode="json")
        return records

    @staticmethod
    def _cash_after_local_sim(cash_entries: tuple[Any, ...], context: SimulationRunContext) -> float:
        if cash_entries:
            return float(getattr(cash_entries[-1], "cash_after"))
        if context.cash is not None:
            return float(context.cash)
        raise DataUnavailableError(
            "LocalSim persistence requires account cash or cash ledger entries",
            context={
                "reason_code": "LOCALSIM_PERSISTENCE_CASH_CONTEXT_MISSING",
                "stage": "LOCAL_SIM_PERSISTENCE",
                "required_action": (
                    "persist LocalSim cash ledger entries or provide explicit account cash; do not infer missing cash"
                ),
            },
        )

    @staticmethod
    def _local_sim_snapshot_time(
        *,
        fills: tuple[Any, ...],
        events: tuple[Any, ...],
        run: SimulationDailyRun,
        local_broker: Any,
        market_data_source: str | None,
    ) -> datetime:
        if fills:
            return max(getattr(fill, "trade_time") for fill in fills)
        if events:
            return max(getattr(event, "event_time") for event in events)
        broker_as_of_time = getattr(local_broker, "scheduler_as_of_time", None)
        if isinstance(broker_as_of_time, datetime):
            if broker_as_of_time.date() != run.trade_date:
                raise DataUnavailableError(
                    "LocalSim broker as-of time does not match the run trade date",
                    context={
                        "reason_code": "LOCALSIM_MARK_AS_OF_DATE_CONFLICT",
                        "run_id": run.run_id,
                        "trade_date": run.trade_date.isoformat(),
                        "as_of_time": broker_as_of_time.isoformat(),
                    },
                )
            return broker_as_of_time
        if market_data_source == MinuteDataSource.TDX_REALTIME.value:
            raise DataUnavailableError(
                "LocalSim realtime account snapshot has no authoritative as-of time",
                context={
                    "reason_code": "LOCALSIM_MARK_AS_OF_TIME_MISSING",
                    "run_id": run.run_id,
                },
            )
        return datetime.combine(run.trade_date, _POST_CLOSE_RECONCILE_TIME, tzinfo=SCHEDULER_TZ)

    @staticmethod
    def _performance_marks(context: SimulationRunContext) -> dict[str, float]:
        marks = dict(context.current_prices or {})
        if context.price_by_symbol:
            marks.update({symbol: float(price) for symbol, price in context.price_by_symbol.items()})
        return marks

    @staticmethod
    def _should_drive_existing_local_sim(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> bool:
        return (
            submit
            and binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM
            and bool(plan.intents)
            and run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
        )

    def _drive_existing_local_sim(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        data_source: str,
        as_of_time: datetime | None,
        context: SimulationRunContext | None = None,
    ) -> SimulationSchedulerBindingResult:
        if context is None:
            context = self._load_existing_plan_context(
                runtime_release=runtime_release,
                binding=binding,
                plan=plan,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
        self._readback_active_local_sim_durable_continuation(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=trade_date,
            as_of_time=as_of_time,
            context=context,
        )
        self._configure_local_sim_runtime_scope(
            binding=binding, run=run, plan=plan, context=context, restore=True, as_of_time=as_of_time
        )
        broker = context.local_broker
        assert broker is not None
        advance = getattr(broker, "advance_realtime_execution", None)
        exporter = getattr(broker, "export_execution_snapshot", None)
        if not callable(advance) or not callable(exporter):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker cannot drive/export the durable minute loop",
                context={
                    "reason_code": "LOCALSIM_DURABLE_ADVANCE_UNSUPPORTED",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        try:
            handles = tuple(advance(as_of_time=scheduler_time(as_of_time)))
        except Exception as exc:
            self.orchestrator.mark_submit_failure(run=run, stage="LOCAL_SIM_INTRADAY_ADVANCE_FAILED", exc=exc)
            raise
        raw_snapshot = exporter(handles=handles)
        snapshot = LocalSimExecutionSnapshot(
            orders=tuple(raw_snapshot.get("orders") or ()),
            fills=tuple(raw_snapshot.get("fills") or ()),
            events=tuple(raw_snapshot.get("events") or ()),
            cash_entries=tuple(raw_snapshot.get("cash_entries") or ()),
            positions=dict(raw_snapshot.get("positions") or {}),
            account=raw_snapshot.get("account"),
            handle_statuses=tuple(raw_snapshot.get("handle_statuses") or ()),
        )
        broker_result = LocalSimPlanSubmitResult(
            order_intents=tuple(LocalSimExecutionBridge().build_order_intents(plan)),
            handles=handles,
            execution_snapshot=snapshot,
        )
        execution = SimulationExecutionResult(
            run=run,
            execution_plan=plan,
            broker_backend=binding.broker_backend,
            status="SUBMITTED",
            intent_count=len(plan.intents),
            broker_result=broker_result,
        )
        local_persistence = self._persist_local_sim_execution_result(
            binding=binding, run=run, execution=execution, context=context
        )
        latest_run = self.repository.get_simulation_daily_run(run.run_id)
        if local_persistence is not None and not bool(local_persistence.payload.get("terminal")):
            return SimulationSchedulerBindingResult(
                binding_id=binding.binding_id,
                strategy_id=binding.strategy_id,
                broker_backend=binding.broker_backend,
                status="LOCALSIM_INTRADAY_RUNNING",
                run=latest_run,
                execution_plan=plan,
                execution_result=replace(execution, run=latest_run),
                data_source=context.market_data_source
                or self._effective_market_data_source_for_binding(
                    binding=binding, trade_date=trade_date, default_data_source=data_source
                ),
            )
        terminal_execution = replace(execution, run=latest_run)
        tail_result = self._handle_tail_after_submit(
            binding=binding, run=latest_run, execution=terminal_execution, context=context
        )
        reconciliation = self._reconcile_after_submit_with_timeout(binding=binding, run=latest_run, context=context)
        self._persist_strategy_performance(
            binding=binding, run=latest_run, context=context, local_persistence=local_persistence
        )
        latest_run = self.repository.get_simulation_daily_run(run.run_id)
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status=self._local_sim_terminal_capacity_residual_status(
                latest_run,
                fallback=self._result_status_after_post_submit(
                    terminal_execution.status, tail_result=tail_result, reconciliation=reconciliation
                ),
            ),
            run=latest_run,
            execution_plan=plan,
            execution_result=terminal_execution,
            reconciliation_result=reconciliation,
            data_source=context.market_data_source
            or self._effective_market_data_source_for_binding(
                binding=binding, trade_date=trade_date, default_data_source=data_source
            ),
        )

    def _readback_active_local_sim_durable_continuation(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        runtime_release: StrategyRuntimeRelease,
        trade_date: date,
        as_of_time: datetime | None,
        context: SimulationRunContext,
    ) -> tuple[LocalSimExecutionStateV1, ...]:
        """Validate the exact committed generation before a LocalSIM continuation.

        ``broker_called`` is an observation of an earlier call, not the owner of
        the durable minute-loop lifecycle.  Continuation authority comes from
        the frozen plan, execution states, persistence marker, outbox and both
        readback receipts.
        """

        raw_outbox = run.run_payload_json.get("local_sim_projection_outbox_v1")
        try:
            outbox = LocalSimProjectionOutboxV1.model_validate(raw_outbox)
        except Exception as exc:
            raise DataUnavailableError(
                "LocalSim active durable continuation outbox is invalid",
                context={
                    "reason_code": "LOCALSIM_ACTIVE_CONTINUATION_OUTBOX_INVALID",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            ) from exc
        if (
            outbox.status != LocalSimProjectionOutboxStatus.PROJECTED
            or outbox.run_id != run.run_id
            or outbox.plan_id != plan.plan_id
        ):
            raise DataUnavailableError(
                "LocalSim active durable continuation outbox does not identify the projected plan generation",
                context={
                    "reason_code": "LOCALSIM_ACTIVE_CONTINUATION_OUTBOX_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                    "outbox_id": outbox.outbox_id,
                    "outbox_status": outbox.status.value,
                    "outbox_run_id": outbox.run_id,
                    "outbox_plan_id": outbox.plan_id,
                },
            )
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        by_intent = {state.intent_id: state for state in states}
        expected_intents = {intent.intent_id for intent in plan.intents}
        if (
            not states
            or len(by_intent) != len(states)
            or set(by_intent) != expected_intents
            or any(
                state.run_id != run.run_id
                or state.binding_id != binding.binding_id
                or state.trade_date != trade_date
                or state.plan_id != plan.plan_id
                for state in states
            )
        ):
            raise DataUnavailableError(
                "LocalSim active durable continuation states do not close over the frozen plan",
                context={
                    "reason_code": "LOCALSIM_ACTIVE_CONTINUATION_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(by_intent),
                    "state_count": len(states),
                },
            )
        persistence = run.run_payload_json.get("local_sim_persistence")
        if not isinstance(persistence, dict) or persistence.get("terminal") is not False:
            raise DataUnavailableError(
                "LocalSim active durable continuation persistence is not explicitly non-terminal",
                context={
                    "reason_code": "LOCALSIM_ACTIVE_CONTINUATION_PERSISTENCE_CONFLICT",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                    "persistence_type": type(persistence).__name__,
                    "persistence_terminal": persistence.get("terminal") if isinstance(persistence, dict) else None,
                },
            )
        active_states = tuple(state for state in states if not state.is_terminal)
        if not active_states:
            raise DataUnavailableError(
                "LocalSim intraday run has no active durable execution state",
                context={
                    "reason_code": "LOCALSIM_ACTIVE_CONTINUATION_STATE_MISSING",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                    "state_ids": sorted(state.state_id for state in states),
                },
            )
        self._readback_local_sim_recovery_generation(
            binding=binding,
            run=run,
            plan=plan,
            runtime_release=runtime_release,
            trade_date=trade_date,
            as_of_time=as_of_time,
            outbox=outbox,
            states=states,
            context=context,
        )
        return active_states

    @staticmethod
    def _should_submit_existing_plan(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> bool:
        if not submit or not plan.intents:
            return False
        if run.run_payload_json.get("broker_called"):
            return (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
                and SimulationLifecycleScheduler._mini_qmt_batch_has_deferred_dependent_buy(run.run_payload_json)
                and not SimulationLifecycleScheduler._mini_qmt_batch_has_open_order_evidence(run.run_payload_json)
            )
        if (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
            and (
                SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_duplicate_order_remark(run.run_payload_json)
            )
        ):
            return False
        statuses = {
            SimulationDailyRunStatus.CREATED,
            SimulationDailyRunStatus.PRECHECKING,
            SimulationDailyRunStatus.SIGNAL_GENERATING,
            SimulationDailyRunStatus.TARGET_GENERATING,
            SimulationDailyRunStatus.PLANNING_EXECUTION,
            SimulationDailyRunStatus.FAILED_RETRYABLE,
        }
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            statuses.add(SimulationDailyRunStatus.SUBMITTING)
        if run.status in statuses:
            return True
        return (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and run.status == SimulationDailyRunStatus.SUCCEEDED
            and SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(run.run_payload_json)
        )

    @staticmethod
    def _should_mark_existing_no_rebalance(
        *,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        submit: bool,
    ) -> bool:
        if not submit or plan.intents or run.run_payload_json.get("broker_called"):
            return False
        if SimulationLifecycleScheduler._execution_plan_has_pre_trade_blocks(plan):
            return False
        return run.status in {
            SimulationDailyRunStatus.CREATED,
            SimulationDailyRunStatus.PRECHECKING,
            SimulationDailyRunStatus.SIGNAL_GENERATING,
            SimulationDailyRunStatus.TARGET_GENERATING,
            SimulationDailyRunStatus.PLANNING_EXECUTION,
            SimulationDailyRunStatus.FAILED_RETRYABLE,
            SimulationDailyRunStatus.SUBMITTING,
        }

    @staticmethod
    def _execution_plan_has_pre_trade_blocks(plan: ExecutionPlan) -> bool:
        for decision in plan.trading_rule_decisions:
            if decision.price_limit_rule.get("pre_trade_tradability") and not bool(
                decision.price_limit_rule["pre_trade_tradability"].get("is_tradable", True)
            ):
                return True
            if str(decision.reason_code).upper() in {
                "SUSPENDED_BY_SUSPEND_D",
                "NO_TRADABLE_REALTIME_QUOTE",
                "REALTIME_QUOTE_MISSING",
                "SUSPENDED_OR_NO_QUOTE_BLOCKED",
            }:
                return True
        return False

    @staticmethod
    def _should_reconcile_existing_miniqmt_run(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        submit: bool,
    ) -> bool:
        return (
            submit
            and binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            and SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(run.run_payload_json)
            and run.status
            in {
                SimulationDailyRunStatus.SUBMITTING,
                SimulationDailyRunStatus.INTRADAY_RUNNING,
                SimulationDailyRunStatus.RECONCILING,
                SimulationDailyRunStatus.SUCCEEDED,
                SimulationDailyRunStatus.FAILED_RETRYABLE,
            }
            and (
                SimulationLifecycleScheduler._mini_qmt_batch_succeeded(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_terminal_capacity_residual(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_batch_has_open_order_evidence(run.run_payload_json)
                or SimulationLifecycleScheduler._mini_qmt_event_loop_has_submitted_children(run.run_payload_json)
                or (
                    run.status != SimulationDailyRunStatus.FAILED_RETRYABLE
                    and SimulationLifecycleScheduler._mini_qmt_batch_has_retryable_buy_residual(run.run_payload_json)
                )
            )
        )

    @staticmethod
    def _miniqmt_optional_nonnegative_counter(
        container: Mapping[str, Any],
        key: str,
        *,
        field_path: str,
    ) -> int | None:
        if key not in container or container.get(key) in (None, ""):
            return None
        value = container.get(key)
        try:
            if isinstance(value, bool):
                raise ValueError("boolean is not an execution counter")
            parsed_decimal = Decimal(str(value))
            if not parsed_decimal.is_finite() or parsed_decimal != parsed_decimal.to_integral_value():
                raise ValueError("counter is not a finite integer")
            parsed = int(parsed_decimal)
        except (ArithmeticError, TypeError, ValueError) as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT event-loop evidence contains an invalid counter",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_COUNTER_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_EVIDENCE_PARSE",
                    "field_path": field_path,
                    "value": value,
                },
            ) from exc
        if parsed < 0:
            raise RuntimeConfigInvalidError(
                "MiniQMT event-loop evidence contains a negative counter",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_COUNTER_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_EVIDENCE_PARSE",
                    "field_path": field_path,
                    "value": value,
                },
            )
        return parsed

    @classmethod
    def _validated_miniqmt_tick_driver_result(
        cls,
        result_payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], int, int, int]:
        if result_payload.get("schema_version") != "miniqmt_event_loop_tick_driver_v1":
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result schema is missing or unsupported",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "schema_version": result_payload.get("schema_version"),
                },
            )
        raw_evidence = result_payload.get("runtime_evidence")
        if not isinstance(raw_evidence, dict):
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result requires typed runtime evidence",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "runtime_evidence",
                },
            )
        evidence = dict(raw_evidence)
        top_source = str(result_payload.get("source") or "").strip()
        evidence_source = str(evidence.get("source") or "").strip()
        if top_source != "simulation_runtime_event_loop_tick_driver" or evidence_source != top_source:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result source identity is invalid",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "result_source": top_source,
                    "evidence_source": evidence_source,
                },
            )
        result_runtime_id = str(result_payload.get("runtime_id") or "").strip()
        evidence_runtime_id = str(evidence.get("runtime_id") or "").strip()
        if not result_runtime_id or evidence_runtime_id != result_runtime_id:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result runtime identity is missing or conflicting",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "result_runtime_id": result_runtime_id or None,
                    "evidence_runtime_id": evidence_runtime_id or None,
                },
            )
        counts: dict[str, int] = {}
        for key in ("submitted_child_count", "rejected_child_count", "pending_algo_count"):
            top_value = cls._miniqmt_optional_nonnegative_counter(
                result_payload,
                key,
                field_path=key,
            )
            evidence_value = cls._miniqmt_optional_nonnegative_counter(
                evidence,
                key,
                field_path=f"runtime_evidence.{key}",
            )
            if top_value is None or evidence_value is None:
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver result is missing a required execution counter",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_MISSING",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field": key,
                        "top_level_present": top_value is not None,
                        "runtime_evidence_present": evidence_value is not None,
                    },
                )
            if top_value != evidence_value:
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver result contains conflicting execution counters",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field": key,
                        "top_level_value": top_value,
                        "runtime_evidence_value": evidence_value,
                    },
                )
            counts[key] = top_value
        trade_event_count = cls._miniqmt_optional_nonnegative_counter(
            evidence,
            "trade_event_count",
            field_path="runtime_evidence.trade_event_count",
        )
        if trade_event_count is None:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver runtime evidence is missing trade_event_count",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_MISSING",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field": "trade_event_count",
                    "runtime_evidence_present": False,
                },
            )
        return (
            evidence,
            counts["submitted_child_count"],
            counts["rejected_child_count"],
            counts["pending_algo_count"],
        )

    @classmethod
    def _validated_miniqmt_tick_driver_batch_results(
        cls,
        result_payload: Mapping[str, Any],
    ) -> dict[str, dict[str, Any]]:
        raw_batch_results = result_payload.get("batch_results")
        if not isinstance(raw_batch_results, dict):
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result batch_results contract is invalid",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "batch_results",
                    "value_type": type(raw_batch_results).__name__,
                },
            )
        validated: dict[str, dict[str, Any]] = {}
        for raw_batch_id, raw_batch in raw_batch_results.items():
            if not isinstance(raw_batch_id, str) or not raw_batch_id.strip():
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result key must be a non-empty string",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": "batch_results.<key>",
                        "value_type": type(raw_batch_id).__name__,
                    },
                )
            batch_id = raw_batch_id.strip()
            field_prefix = f"batch_results.{batch_id}"
            if not isinstance(raw_batch, dict):
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result row must be a mapping",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": field_prefix,
                        "value_type": type(raw_batch).__name__,
                    },
                )
            embedded_batch_id = raw_batch.get("batch_id")
            if not isinstance(embedded_batch_id, str) or embedded_batch_id.strip() != batch_id:
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result identity conflicts with its mapping key",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": f"{field_prefix}.batch_id",
                        "mapping_batch_id": batch_id,
                        "embedded_batch_id": embedded_batch_id,
                    },
                )
            batch_status = cls._validated_miniqmt_tick_driver_batch_status(
                raw_batch.get("batch_status"),
                field_path=f"{field_prefix}.batch_status",
            )
            result_json = raw_batch.get("result_json")
            metadata = raw_batch.get("metadata")
            if not isinstance(result_json, dict) or not isinstance(metadata, dict):
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result carriers must be mappings",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": field_prefix,
                        "result_json_type": type(result_json).__name__,
                        "metadata_type": type(metadata).__name__,
                    },
                )
            cls._validated_miniqmt_tick_driver_result_rows(
                result_json.get("results"),
                field_path=f"{field_prefix}.result_json.results",
            )
            validated[batch_id] = {
                "batch_id": batch_id,
                "batch_status": batch_status,
                "result_json": dict(result_json),
                "metadata": dict(metadata),
            }
        return validated

    @staticmethod
    def _validated_miniqmt_tick_driver_batch_status(value: Any, *, field_path: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver batch status must be a non-empty string",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": field_path,
                    "value_type": type(value).__name__,
                },
            )
        normalized = value.strip().upper()
        try:
            return OrderBatchStatus(normalized).value
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver batch status is unsupported",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": field_path,
                    "value": value,
                },
            ) from exc

    @staticmethod
    def _validated_miniqmt_tick_driver_result_rows(value: Any, *, field_path: str) -> list[dict[str, Any]]:
        if not isinstance(value, list):
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver batch results must be a list",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": field_path,
                    "value_type": type(value).__name__,
                },
            )
        validated: list[dict[str, Any]] = []
        for index, raw_result in enumerate(value):
            if not isinstance(raw_result, dict):
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result item must be a mapping",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": f"{field_path}[{index}]",
                        "value_type": type(raw_result).__name__,
                    },
                )
            for boolean_field in ("success", "broker_called"):
                if not isinstance(raw_result.get(boolean_field), bool):
                    raise RuntimeConfigInvalidError(
                        "MiniQMT tick-driver batch result boolean is invalid",
                        context={
                            "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                            "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                            "field_path": f"{field_path}[{index}].{boolean_field}",
                            "value_type": type(raw_result.get(boolean_field)).__name__,
                        },
                    )
            for identity_field in ("intent_id", "qmt_order_id"):
                identity_value = raw_result.get(identity_field)
                if identity_value is not None and (not isinstance(identity_value, str) or not identity_value.strip()):
                    raise RuntimeConfigInvalidError(
                        "MiniQMT tick-driver batch result identity is invalid",
                        context={
                            "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                            "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                            "field_path": f"{field_path}[{index}].{identity_field}",
                            "value_type": type(identity_value).__name__,
                        },
                    )
            if raw_result["success"] is True and (
                raw_result.get("intent_id") is None
                or raw_result.get("qmt_order_id") is None
                or raw_result["broker_called"] is not True
            ):
                raise RuntimeConfigInvalidError(
                    "MiniQMT successful tick-driver batch result lacks accepted broker identity",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": f"{field_path}[{index}]",
                    },
                )
            if raw_result["broker_called"] is False and raw_result.get("qmt_order_id") is not None:
                raise RuntimeConfigInvalidError(
                    "MiniQMT no-broker tick-driver batch result cannot carry qmt_order_id",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": f"{field_path}[{index}].qmt_order_id",
                    },
                )
            preflight = raw_result.get("preflight")
            if not isinstance(preflight, dict) or not isinstance(preflight.get("allowed"), bool):
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch result preflight is invalid",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": f"{field_path}[{index}].preflight",
                        "value_type": type(preflight).__name__,
                    },
                )
            validated.append(dict(raw_result))
        return validated

    @staticmethod
    def _validated_miniqmt_tick_driver_exact_counter(value: Any, *, field_path: str) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver durable batch cardinality must be a non-negative integer",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": field_path,
                    "value": value,
                },
            )
        return value

    @staticmethod
    def _miniqmt_tick_driver_parent_outcome_counts(
        rows: list[dict[str, Any]],
        *,
        field_path: str,
    ) -> tuple[int, int, int]:
        succeeded = 0
        failed = 0
        pending = 0
        seen_intent_ids: set[str] = set()
        for row in rows:
            intent_id = str(row.get("intent_id") or "").strip()
            if intent_id and intent_id in seen_intent_ids:
                raise RuntimeConfigInvalidError(
                    "MiniQMT tick-driver batch contains duplicate parent intent outcomes",
                    context={
                        "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        "field_path": field_path,
                        "intent_id": intent_id,
                    },
                )
            if intent_id:
                seen_intent_ids.add(intent_id)
            if row["success"] is True:
                succeeded += 1
                continue
            preflight = row["preflight"]
            message = str(row.get("broker_message") or "").lower()
            if (
                row["broker_called"] is False
                and row.get("qmt_order_id") is None
                and preflight["allowed"] is True
                and ("pending tick trigger" in message or "algo dispatched" in message)
            ):
                pending += 1
                continue
            failed += 1
        if succeeded + failed + pending != len(rows):
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver parent outcomes do not close to batch cardinality",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": field_path,
                    "total": len(rows),
                    "succeeded": succeeded,
                    "failed": failed,
                    "pending": pending,
                },
            )
        return succeeded, failed, pending

    @staticmethod
    def _mini_qmt_event_loop_has_submitted_children(payload: dict[str, Any]) -> bool:
        for key in ("submitted_intents", "triggered_child_order_count"):
            parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
                payload,
                key,
                field_path=key,
            )
            if parsed is not None and parsed > 0:
                return True
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        for key in ("succeeded", "submitted_child_count", "triggered_child_order_count"):
            parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
                batch,
                key,
                field_path=f"qmt_batch_result.{key}",
            )
            if parsed is not None and parsed > 0:
                return True
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
            runtime_evidence,
            "submitted_child_count",
            field_path="qmt_batch_result.runtime_evidence.submitted_child_count",
        )
        return parsed is not None and parsed > 0

    @staticmethod
    def _mini_qmt_batch_has_open_order_evidence(payload: dict[str, Any]) -> bool:
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            open_order_evidence = container.get("open_order_evidence")
            if isinstance(open_order_evidence, dict) and int(open_order_evidence.get("open_order_count") or 0) > 0:
                return True
        return False

    @staticmethod
    def _mini_qmt_batch_has_deferred_dependent_buy(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return bool(summary.get("noncompensating_residual")) and int(summary.get("dependent_buy_count") or 0) > 0

    @staticmethod
    def _mini_qmt_batch_has_retryable_buy_residual(
        payload: dict[str, Any],
        *,
        allowed_error_codes: frozenset[str] = _MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES,
    ) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(
            payload,
            allowed_error_codes=allowed_error_codes,
        )
        return bool(summary.get("noncompensating_residual"))

    @staticmethod
    def _mini_qmt_batch_has_noncompensating_residual(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return bool(summary.get("noncompensating_residual"))

    @staticmethod
    def _mini_qmt_batch_has_broker_side_effect_evidence(payload: dict[str, Any]) -> bool:
        if bool(payload.get("broker_called")):
            return True
        if payload.get("miniqmt_side_effect_state") == "UNKNOWN_TIMEOUT":
            return True
        if isinstance(payload.get("miniqmt_submit_timeout"), dict):
            return True
        for container_key in ("reconcile_after_submit", "sync_after_submit", "sync_before_submit"):
            container = payload.get(container_key)
            if not isinstance(container, dict):
                continue
            side_effect_evidence = container.get("side_effect_evidence")
            if (
                isinstance(side_effect_evidence, dict)
                and int(side_effect_evidence.get("broker_side_effect_count") or 0) > 0
            ):
                return True
            open_order_evidence = container.get("open_order_evidence")
            if isinstance(open_order_evidence, dict) and int(open_order_evidence.get("open_order_count") or 0) > 0:
                return True
        return False

    @staticmethod
    def _mini_qmt_batch_has_duplicate_order_remark(payload: dict[str, Any]) -> bool:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        results = batch.get("results")
        if not isinstance(results, list):
            return False
        for result in results:
            if not isinstance(result, dict):
                continue
            preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
            errors = preflight.get("errors")
            if not isinstance(errors, list):
                continue
            for error in errors:
                if isinstance(error, dict) and str(error.get("code") or "").upper() == "DUPLICATE_ORDER_REMARK":
                    return True
        return False

    @staticmethod
    def _miniqmt_failed_run_durable_pending_recovery_evidence(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
    ) -> dict[str, Any]:
        """Prove the one safe FAILED_RETRYABLE -> tick-driver recovery path.

        Parent submission is deliberately not retried here. Recovery is allowed
        only when the immutable plan, runtime id, B0 control revision, durable
        pending algo counts, and side-effect-free child evidence all agree.
        """

        payload = run.run_payload_json
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        route = payload.get("miniqmt_runtime_route") if isinstance(payload.get("miniqmt_runtime_route"), dict) else {}
        quote_control = plan.plan_payload_json.get("quote_control")
        quote_revision = (
            quote_control.get("revision")
            if isinstance(quote_control, dict) and isinstance(quote_control.get("revision"), dict)
            else {}
        )
        conflicts: list[str] = []

        payload_batch_id = str(payload.get("qmt_batch_id") or "").strip()
        result_batch_id = str(batch.get("batch_id") or "").strip()
        if not payload_batch_id or not result_batch_id:
            conflicts.append("batch_id_missing")
        elif payload_batch_id != result_batch_id:
            conflicts.append("batch_id_conflict")

        batch_status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        if batch_status not in {OrderBatchStatus.SUBMITTING.value, OrderBatchStatus.PREFLIGHT_FAILED.value}:
            conflicts.append("batch_status_not_recoverable")

        runtime_id = str(runtime_evidence.get("runtime_id") or "").strip()
        expected_runtime_id = miniqmt_kernel_runtime_id(
            plan_id=plan.plan_id,
            binding_id=binding.binding_id,
            trade_date=plan.target_trade_date,
        )
        if not runtime_id:
            conflicts.append("runtime_id_missing")
        elif runtime_id != expected_runtime_id:
            conflicts.append("runtime_id_conflict")
        source = str(runtime_evidence.get("source") or "").strip()
        if source not in {
            "simulation_runtime_event_loop_submit",
            "simulation_runtime_event_loop_tick_driver",
        }:
            conflicts.append("runtime_evidence_source_invalid")

        runtime_kind = str(payload.get("miniqmt_runtime_kind") or "").strip().lower()
        route_name = str(route.get("route") or "").strip().upper()
        if runtime_kind != MiniQMTExecutionRuntimeKind.EVENT_LOOP.value and route_name != "A_EVENT_LOOP":
            conflicts.append("event_loop_route_identity_missing")
        if route_name and route_name != "A_EVENT_LOOP":
            conflicts.append("event_loop_route_conflict")

        control_revision = str(quote_revision.get("control_revision") or "").strip().upper()
        assignments = quote_control.get("assignments") if isinstance(quote_control, dict) else None
        if control_revision != "B0_QUOTE_V2":
            conflicts.append("quote_control_revision_not_b0_quote_v2")
        if not isinstance(assignments, list) or not assignments:
            conflicts.append("quote_control_assignments_missing")

        def exact_nonnegative_int(raw: Any, field: str) -> int | None:
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                conflicts.append(f"{field}_invalid")
                return None
            return raw

        active_count = exact_nonnegative_int(runtime_evidence.get("active_algo_count"), "active_algo_count")
        pending_count = exact_nonnegative_int(runtime_evidence.get("pending_algo_count"), "pending_algo_count")
        submitted_count = exact_nonnegative_int(
            runtime_evidence.get("submitted_child_count"),
            "submitted_child_count",
        )
        rejected_count = exact_nonnegative_int(
            runtime_evidence.get("rejected_child_count"),
            "rejected_child_count",
        )
        if pending_count is not None and pending_count <= 0:
            conflicts.append("pending_algo_count_zero")
        if active_count is not None and pending_count is not None and active_count != pending_count:
            conflicts.append("active_pending_count_conflict")
        if submitted_count not in {None, 0}:
            conflicts.append("submitted_child_side_effect_present")
        if rejected_count not in {None, 0}:
            conflicts.append("rejected_child_present")

        child_order_ids = runtime_evidence.get("child_order_ids")
        if isinstance(child_order_ids, str):
            child_order_ids = [item for item in child_order_ids.split() if item]
        if child_order_ids:
            conflicts.append("child_order_ids_present")
        if SimulationLifecycleScheduler._mini_qmt_batch_has_broker_side_effect_evidence(payload):
            conflicts.append("broker_side_effect_evidence_present")
        if payload.get("miniqmt_side_effect_state") == "UNKNOWN_TIMEOUT":
            conflicts.append("unknown_timeout_side_effect_state")
        if isinstance(payload.get("miniqmt_submit_timeout"), dict):
            conflicts.append("submit_timeout_present")

        results = batch.get("results")
        if not isinstance(results, list) or not results:
            conflicts.append("batch_results_missing")
        else:
            for index, result in enumerate(results):
                if not isinstance(result, dict):
                    conflicts.append(f"batch_result_{index}_invalid")
                    continue
                if bool(result.get("broker_called")):
                    conflicts.append(f"batch_result_{index}_broker_called")
                    continue
                if bool(result.get("success")):
                    continue
                preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
                errors = preflight.get("errors")
                error_codes = (
                    {
                        str(error.get("code") or "").strip().upper()
                        for error in errors
                        if isinstance(error, dict) and str(error.get("code") or "").strip()
                    }
                    if isinstance(errors, list)
                    else set()
                )
                if error_codes != {"DUPLICATE_ORDER_REMARK"}:
                    conflicts.append(f"batch_result_{index}_failure_not_owned_duplicate")

        return {
            "schema_version": "miniqmt_failed_run_durable_pending_recovery_v1",
            "eligible": not conflicts,
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "batch_id": payload_batch_id or result_batch_id or None,
            "runtime_id": runtime_id or None,
            "expected_runtime_id": expected_runtime_id,
            "runtime_evidence_source": source or None,
            "active_algo_count": active_count,
            "pending_algo_count": pending_count,
            "submitted_child_count": submitted_count,
            "rejected_child_count": rejected_count,
            "control_revision": control_revision or None,
            "conflicts": conflicts,
        }

    @staticmethod
    def _mini_qmt_event_loop_has_pending_algos(payload: dict[str, Any]) -> bool:
        for key in ("pending_intents", "event_loop_pending_count"):
            parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
                payload,
                key,
                field_path=key,
            )
            if parsed is not None and parsed > 0:
                return True
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        for key in ("pending", "pending_child_trigger_count"):
            parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
                batch,
                key,
                field_path=f"qmt_batch_result.{key}",
            )
            if parsed is not None and parsed > 0:
                return True
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        parsed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
            runtime_evidence,
            "pending_algo_count",
            field_path="qmt_batch_result.runtime_evidence.pending_algo_count",
        )
        return parsed is not None and parsed > 0

    def _persist_miniqmt_tick_driver_result(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        result: Any,
    ) -> SimulationDailyRun:
        payload = run.run_payload_json
        raw_result_payload = result.to_dict() if hasattr(result, "to_dict") else result
        if not isinstance(raw_result_payload, Mapping):
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result must be a mapping",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "tick_driver_result",
                    "value_type": type(raw_result_payload).__name__,
                },
            )
        result_payload = dict(raw_result_payload)
        evidence, submitted_child_count, rejected_child_count, _pending_algo_count = (
            self._validated_miniqmt_tick_driver_result(result_payload)
        )
        batch_results = self._validated_miniqmt_tick_driver_batch_results(result_payload)
        raw_qmt_batch_id = payload.get("qmt_batch_id")
        if raw_qmt_batch_id in (None, ""):
            qmt_batch_id = ""
        elif isinstance(raw_qmt_batch_id, str) and raw_qmt_batch_id.strip():
            qmt_batch_id = raw_qmt_batch_id.strip()
        else:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_id must be a non-empty string when present",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_id",
                    "value_type": type(raw_qmt_batch_id).__name__,
                },
            )
        raw_qmt_batch_result = payload.get("qmt_batch_result")
        if raw_qmt_batch_result is None:
            qmt_batch_result: dict[str, Any] = {}
        elif isinstance(raw_qmt_batch_result, dict):
            qmt_batch_result = dict(raw_qmt_batch_result)
        else:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result must be a mapping",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result",
                    "value_type": type(raw_qmt_batch_result).__name__,
                },
            )
        raw_previous_runtime_evidence = qmt_batch_result.get("runtime_evidence")
        if raw_previous_runtime_evidence is None:
            previous_runtime_evidence: dict[str, Any] = {}
        elif isinstance(raw_previous_runtime_evidence, dict):
            previous_runtime_evidence = raw_previous_runtime_evidence
        else:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch runtime_evidence must be a mapping",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.runtime_evidence",
                    "value_type": type(raw_previous_runtime_evidence).__name__,
                },
            )
        previous_runtime_id = str(previous_runtime_evidence.get("runtime_id") or "").strip()
        current_runtime_id = str(evidence.get("runtime_id") or "").strip()
        if previous_runtime_id and current_runtime_id != previous_runtime_id:
            raise RuntimeConfigInvalidError(
                "MiniQMT tick-driver result runtime identity changed from the durable batch",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "qmt_batch_id": qmt_batch_id or None,
                    "durable_runtime_id": previous_runtime_id,
                    "result_runtime_id": current_runtime_id,
                },
            )
        if qmt_batch_result and not qmt_batch_id:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result is missing qmt_batch_id identity",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.batch_id",
                },
            )
        embedded_batch_id = qmt_batch_result.get("batch_id")
        if qmt_batch_result and (not isinstance(embedded_batch_id, str) or embedded_batch_id.strip() != qmt_batch_id):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result identity conflicts with qmt_batch_id",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.batch_id",
                    "qmt_batch_id": qmt_batch_id or None,
                    "result_batch_id": embedded_batch_id,
                },
            )
        raw_payload_batch_status = payload.get("qmt_batch_status")
        qmt_batch_status = (
            self._validated_miniqmt_tick_driver_batch_status(
                raw_payload_batch_status,
                field_path="qmt_batch_status",
            )
            if raw_payload_batch_status not in (None, "")
            else None
        )
        raw_result_batch_status = qmt_batch_result.get("batch_status")
        if qmt_batch_result and raw_result_batch_status in (None, ""):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result is missing batch_status",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.batch_status",
                },
            )
        result_batch_status = (
            self._validated_miniqmt_tick_driver_batch_status(
                raw_result_batch_status,
                field_path="qmt_batch_result.batch_status",
            )
            if raw_result_batch_status not in (None, "")
            else None
        )
        if qmt_batch_status is not None and result_batch_status is not None and qmt_batch_status != result_batch_status:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch status carriers conflict",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "qmt_batch_id": qmt_batch_id or None,
                    "payload_batch_status": qmt_batch_status,
                    "result_batch_status": result_batch_status,
                },
            )
        if qmt_batch_status is None:
            qmt_batch_status = result_batch_status
        existing_result_rows: list[dict[str, Any]] | None = None
        if qmt_batch_result and "results" not in qmt_batch_result:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result is missing results",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.results",
                },
            )
        if "results" in qmt_batch_result:
            existing_result_rows = self._validated_miniqmt_tick_driver_result_rows(
                qmt_batch_result.get("results"),
                field_path="qmt_batch_result.results",
            )
        if qmt_batch_result and "total" not in qmt_batch_result:
            raise RuntimeConfigInvalidError(
                "MiniQMT durable qmt_batch_result is missing total cardinality",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.total",
                },
            )
        for counter_key in (
            "total",
            "succeeded",
            "failed",
            "pending",
            "triggered_child_order_count",
            "pending_child_trigger_count",
        ):
            if counter_key in qmt_batch_result:
                self._validated_miniqmt_tick_driver_exact_counter(
                    qmt_batch_result.get(counter_key),
                    field_path=f"qmt_batch_result.{counter_key}",
                )
        if (
            existing_result_rows is not None
            and "total" in qmt_batch_result
            and qmt_batch_result["total"] != len(existing_result_rows)
        ):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch total conflicts with result cardinality",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "qmt_batch_id": qmt_batch_id or None,
                    "total": qmt_batch_result["total"],
                    "result_count": len(existing_result_rows),
                },
            )
        if qmt_batch_result and not isinstance(qmt_batch_result.get("success"), bool):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable batch success must be a boolean",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "qmt_batch_result.success",
                    "value_type": type(qmt_batch_result.get("success")).__name__,
                },
            )
        if "broker_called" in payload and not isinstance(payload.get("broker_called"), bool):
            raise RuntimeConfigInvalidError(
                "MiniQMT durable broker_called must be a boolean",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID",
                    "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                    "field_path": "broker_called",
                    "value_type": type(payload.get("broker_called")).__name__,
                },
            )
        if qmt_batch_id and qmt_batch_id in batch_results:
            latest_batch = batch_results[qmt_batch_id]
            result_json = latest_batch["result_json"]
            metadata = latest_batch["metadata"]
            qmt_batch_status = latest_batch["batch_status"]
            latest_result_rows = self._validated_miniqmt_tick_driver_result_rows(
                result_json.get("results"),
                field_path=f"batch_results.{qmt_batch_id}.result_json.results",
            )
            parent_succeeded, parent_failed, parent_pending = self._miniqmt_tick_driver_parent_outcome_counts(
                latest_result_rows,
                field_path=f"batch_results.{qmt_batch_id}.result_json.results",
            )
            qmt_batch_result.update(result_json)
            qmt_batch_result.update(
                {
                    "success": parent_failed == 0 and (parent_succeeded > 0 or parent_pending > 0),
                    "batch_id": qmt_batch_id,
                    "batch_status": qmt_batch_status,
                    "total": len(latest_result_rows),
                    "succeeded": parent_succeeded,
                    "failed": parent_failed,
                    "pending": parent_pending,
                    "triggered_child_order_count": submitted_child_count,
                    "pending_child_trigger_count": parent_pending,
                    "runtime_evidence": evidence,
                    "event_loop_batch_metadata": metadata,
                }
            )
        parent_succeeded = 0
        parent_failed = 0
        parent_pending = 0
        if qmt_batch_result:
            durable_result_rows = self._validated_miniqmt_tick_driver_result_rows(
                qmt_batch_result.get("results"),
                field_path="qmt_batch_result.results",
            )
            parent_succeeded, parent_failed, parent_pending = self._miniqmt_tick_driver_parent_outcome_counts(
                durable_result_rows,
                field_path="qmt_batch_result.results",
            )
            qmt_batch_result.update(
                {
                    "success": parent_failed == 0 and (parent_succeeded > 0 or parent_pending > 0),
                    "total": len(durable_result_rows),
                    "succeeded": parent_succeeded,
                    "failed": parent_failed,
                    "pending": parent_pending,
                    "triggered_child_order_count": submitted_child_count,
                    "pending_child_trigger_count": parent_pending,
                    "runtime_evidence": evidence,
                }
            )
        payload_patch = {
            "broker_called": payload.get("broker_called", False) or submitted_child_count + rejected_child_count > 0,
            "submitted_intents": parent_succeeded,
            "failed_intents": parent_failed,
            "pending_intents": parent_pending,
            "miniqmt_event_loop_tick_driver": result_payload,
            "last_stage": SimulationDailyRunStatus.INTRADAY_RUNNING.value,
        }
        if qmt_batch_status is not None:
            payload_patch["qmt_batch_status"] = qmt_batch_status
        if qmt_batch_result:
            payload_patch["qmt_batch_result"] = qmt_batch_result
        if run.status == SimulationDailyRunStatus.FAILED_RETRYABLE:
            recovery_evidence = self._miniqmt_failed_run_durable_pending_recovery_evidence(
                binding=binding,
                run=run,
                plan=plan,
            )
            if not recovery_evidence["eligible"]:
                raise RuntimeConfigInvalidError(
                    "MiniQMT failed-run tick recovery lost its durable evidence contract before persistence",
                    context={
                        "reason_code": "MINIQMT_FAILED_RUN_RECOVERY_EVIDENCE_CHANGED",
                        "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_PERSIST",
                        **recovery_evidence,
                    },
                )
            payload_patch["miniqmt_failed_run_recovery"] = {
                **recovery_evidence,
                "status": "RECOVERED_TO_TICK_DRIVER",
                "recovered_at": datetime.now(UTC).isoformat(),
                "parent_resubmitted": False,
            }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.INTRADAY_RUNNING,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",),
        )

    def _mark_miniqmt_tick_driver_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_event_loop_tick_driver_timeout_v1",
            "stage": "MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT",
            "reason_code": "MINIQMT_EVENT_LOOP_TICK_DRIVER_TIMEOUT",
            "reason": "miniqmt_event_loop_tick_driver_exceeded_stage_timeout",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "timeout_context": context,
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.INTRADAY_RUNNING,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.INTRADAY_RUNNING.value,
                "miniqmt_event_loop_tick_driver_timeout": diagnostic,
                "miniqmt_event_loop_tick_driver": diagnostic,
            },
        )

    @staticmethod
    def _mini_qmt_batch_has_terminal_capacity_residual(payload: dict[str, Any]) -> bool:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        return (
            bool(summary.get("noncompensating_residual"))
            and int(summary.get("capacity_residual_count") or 0) > 0
            and int(summary.get("dependent_buy_count") or 0) == 0
        )

    @staticmethod
    def _miniqmt_capacity_residual_observability(
        payload: dict[str, Any],
        *,
        reason: str | None = None,
        source: str | None = None,
    ) -> dict[str, Any] | None:
        summary = SimulationLifecycleScheduler._mini_qmt_batch_residual_summary(payload)
        capacity_residual_count = int(summary.get("capacity_residual_count") or 0)
        if (
            not bool(summary.get("noncompensating_residual"))
            or capacity_residual_count <= 0
            or int(summary.get("dependent_buy_count") or 0) > 0
        ):
            return None
        submit_gate = SimulationLifecycleScheduler._latest_miniqmt_payload_evidence(payload, "submit_result_gate")
        terminalization = payload.get("miniqmt_post_close_terminalization")
        if not isinstance(terminalization, dict):
            terminalization = {}
        gate_reason = str(submit_gate.get("reason") or "")
        terminal_reason = str(terminalization.get("reason") or "")
        is_succeeded_capacity_residual = (
            bool(submit_gate.get("terminal_capacity_residual"))
            and str(submit_gate.get("status") or "").upper() == "SUCCEEDED"
        ) or terminalization.get("audit_state") == "succeeded_with_capacity_residual"
        if not is_succeeded_capacity_residual and reason not in {
            "miniqmt_capacity_residual_skipped_and_reconciled",
            "miniqmt_post_close_capacity_residual_skipped",
        }:
            return None
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        failed_intents_raw = payload.get("failed_intents", batch.get("failed", 0))
        try:
            failed_intents = max(int(failed_intents_raw or 0), 0)
        except (TypeError, ValueError) as exc:
            raise DataUnavailableError(
                "MiniQMT capacity residual observability found invalid failed_intents",
                context={
                    "reason_code": "MINIQMT_CAPACITY_RESIDUAL_FAILED_INTENTS_INVALID",
                    "failed_intents": failed_intents_raw,
                    "qmt_batch_id": payload.get("qmt_batch_id"),
                    "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
                },
            ) from exc
        alert = {
            "schema_version": "simulation_runtime_alert_v1",
            "severity": "warning",
            "reason_code": "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL",
            "message": "MiniQMT run succeeded with capacity residual; monitoring must not treat it as clean success",
            "capacity_residual_count": capacity_residual_count,
            "failed_intents": failed_intents,
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
        }
        return {
            "schema_version": "miniqmt_capacity_residual_observability_v1",
            "succeeded_with_capacity_residual": True,
            "reason": reason or gate_reason or terminal_reason or "miniqmt_capacity_residual_succeeded",
            "source": source
            or (
                "post_close_terminalization"
                if terminalization.get("audit_state") == "succeeded_with_capacity_residual"
                else "submit_result_gate"
            ),
            "qmt_batch_id": payload.get("qmt_batch_id"),
            "qmt_batch_status": payload.get("qmt_batch_status") or batch.get("batch_status"),
            "failed_intents": failed_intents,
            "failed_result_count": int(summary.get("failed_result_count") or 0),
            "capacity_residual_count": capacity_residual_count,
            "dependent_buy_count": int(summary.get("dependent_buy_count") or 0),
            "unknown_residual_count": int(summary.get("unknown_residual_count") or 0),
            "error_codes": list(summary.get("error_codes") or []),
            "residual_summary": summary,
            "alert": alert,
        }

    @staticmethod
    def _miniqmt_capacity_residual_payload_patch(observability: dict[str, Any] | None) -> dict[str, Any]:
        if not observability:
            return {}
        alert = observability.get("alert") if isinstance(observability.get("alert"), dict) else None
        patch: dict[str, Any] = {
            "succeeded_with_capacity_residual": True,
            "capacity_residual_count": int(observability.get("capacity_residual_count") or 0),
            "capacity_residual_failed_intents": int(observability.get("failed_intents") or 0),
            "miniqmt_capacity_residual_observability": observability,
        }
        if alert is not None:
            patch["simulation_alerts"] = [alert]
        return patch

    @staticmethod
    def _miniqmt_capacity_residual_result_fields(run: SimulationDailyRun | None) -> dict[str, Any]:
        if run is None or run.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return {}
        observability = SimulationLifecycleScheduler._miniqmt_capacity_residual_observability(run.run_payload_json)
        if not observability:
            return {}
        return {
            "succeeded_with_capacity_residual": True,
            "capacity_residual_count": int(observability.get("capacity_residual_count") or 0),
            "capacity_residual_failed_intents": int(observability.get("failed_intents") or 0),
            "miniqmt_capacity_residual_observability": observability,
            "alert": observability.get("alert"),
        }

    @staticmethod
    def _mini_qmt_batch_residual_summary(
        payload: dict[str, Any],
        *,
        allowed_error_codes: frozenset[str] = _MINIQMT_RETRYABLE_BUY_RESIDUAL_ERROR_CODES,
    ) -> dict[str, Any]:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        summary: dict[str, Any] = {
            "schema_version": "miniqmt_batch_residual_summary_v1",
            "qmt_batch_status": status,
            "partial": status == "PARTIAL",
            "compensation_required": bool(batch.get("compensation_required")),
            "failed_result_count": 0,
            "broker_called_failed_count": 0,
            "dependent_buy_count": 0,
            "capacity_residual_count": 0,
            "unknown_residual_count": 0,
            "error_codes": [],
            "noncompensating_residual": False,
        }
        if status != "PARTIAL" or bool(batch.get("compensation_required")):
            return summary
        results = batch.get("results")
        if not isinstance(results, list):
            return summary
        error_codes_seen: set[str] = set()
        for result in results:
            if not isinstance(result, dict) or bool(result.get("success")):
                continue
            summary["failed_result_count"] = int(summary["failed_result_count"]) + 1
            if bool(result.get("broker_called")):
                summary["broker_called_failed_count"] = int(summary["broker_called_failed_count"]) + 1
                continue
            preflight = result.get("preflight") if isinstance(result.get("preflight"), dict) else {}
            errors = preflight.get("errors")
            if not isinstance(errors, list):
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
                continue
            error_codes = {
                str(error.get("code") or "").upper()
                for error in errors
                if isinstance(error, dict) and str(error.get("code") or "").strip()
            }
            error_codes_seen.update(error_codes)
            if not error_codes or not error_codes <= allowed_error_codes:
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
                continue
            if error_codes <= _MINIQMT_DEPENDENT_BUY_RETRY_ERROR_CODES:
                summary["dependent_buy_count"] = int(summary["dependent_buy_count"]) + 1
            elif error_codes <= _MINIQMT_CAPACITY_RESIDUAL_RETRY_ERROR_CODES:
                summary["capacity_residual_count"] = int(summary["capacity_residual_count"]) + 1
            else:
                summary["unknown_residual_count"] = int(summary["unknown_residual_count"]) + 1
        failed_count = int(summary["failed_result_count"])
        known_residual_count = int(summary["dependent_buy_count"]) + int(summary["capacity_residual_count"])
        summary["error_codes"] = sorted(error_codes_seen)
        summary["noncompensating_residual"] = (
            failed_count > 0
            and known_residual_count == failed_count
            and int(summary["broker_called_failed_count"]) == 0
            and int(summary["unknown_residual_count"]) == 0
        )
        return summary

    @staticmethod
    def _mini_qmt_batch_succeeded(payload: dict[str, Any]) -> bool:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        status = str(payload.get("qmt_batch_status") or batch.get("batch_status") or "").upper()
        if status not in {"SUCCEEDED", "PREVIEW_SUCCEEDED"}:
            return False
        if batch.get("success") is False:
            return False
        failed_container = batch if "failed" in batch else payload
        failed_key = "failed" if "failed" in batch else "failed_intents"
        failed = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
            failed_container,
            failed_key,
            field_path="qmt_batch_result.failed" if "failed" in batch else "failed_intents",
        )
        total_container = batch if "total" in batch else payload
        total_key = "total" if "total" in batch else "submitted_intents"
        total = SimulationLifecycleScheduler._miniqmt_optional_nonnegative_counter(
            total_container,
            total_key,
            field_path="qmt_batch_result.total" if "total" in batch else "submitted_intents",
        )
        failed = failed if failed is not None else 0
        total = total if total is not None else 0
        return total > 0 and failed == 0

    def _sync_before_submit(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        synced = self._sync_miniqmt_snapshot(
            binding=binding,
            run=run,
            context=context,
            payload_key="sync_before_submit",
        )
        return synced[0] if synced is not None else None

    def _sync_miniqmt_snapshot(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
        payload_key: str,
    ) -> tuple[dict[str, Any], Any] | None:
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
                payload_key: payload,
            },
        )
        return payload, summary

    def _configure_local_sim_runtime_scope(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
        restore: bool,
        as_of_time: datetime | None,
    ) -> tuple[LocalSimExecutionStateV1, ...]:
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return ()
        if context.market_data_source != MinuteDataSource.TDX_REALTIME.value:
            return ()
        broker = context.local_broker
        if broker is None:
            raise DataUnavailableError(
                "LocalSim realtime execution requires an instantiated broker",
                context={
                    "reason_code": "LOCALSIM_REALTIME_BROKER_MISSING",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        configure = getattr(broker, "configure_execution_runtime", None)
        if not callable(configure):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker does not support durable runtime scope",
                context={
                    "reason_code": "LOCALSIM_DURABLE_RUNTIME_UNSUPPORTED",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        configure(run_id=run.run_id, binding_id=binding.binding_id)
        if not restore:
            return ()
        states = tuple(self.repository.list_local_sim_execution_states(run.run_id, authoritative=True))
        if not states:
            raise DataUnavailableError(
                "LocalSim active run has no durable per-intent execution state",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_MISSING",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        by_intent = {state.intent_id: state for state in states}
        expected_intents = {intent.intent_id for intent in plan.intents}
        if len(by_intent) != len(states) or set(by_intent) != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable states do not close over the execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_STATE_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(by_intent),
                    "state_count": len(states),
                },
            )
        paper_repository = self._paper_repository_for_local_sim(binding=binding, run=run, context=context)
        persisted_orders = tuple(paper_repository.list_orders_for_run(run.run_id))
        orders = {order.intent_id: order for order in persisted_orders}
        if len(orders) != len(persisted_orders) or set(orders) != expected_intents:
            raise DataUnavailableError(
                "LocalSim durable orders do not close over the execution plan intents",
                context={
                    "reason_code": "LOCALSIM_DURABLE_ORDER_PLAN_MISMATCH",
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "expected_intent_ids": sorted(expected_intents),
                    "actual_intent_ids": sorted(orders),
                    "order_count": len(persisted_orders),
                },
            )
        binder = getattr(broker, "bind_execution_plan", None)
        restorer = getattr(broker, "restore_execution_state", None)
        if not callable(binder) or not callable(restorer):
            raise RuntimeConfigInvalidError(
                "LocalSim realtime broker cannot restore the durable minute loop",
                context={
                    "reason_code": "LOCALSIM_DURABLE_RESTORE_UNSUPPORTED",
                    "run_id": run.run_id,
                    "binding_id": binding.binding_id,
                    "plan_id": plan.plan_id,
                },
            )
        binder(plan=plan, as_of_time=scheduler_time(as_of_time))
        for intent in plan.intents:
            intent_id = intent.intent_id
            state = by_intent[intent_id]
            if state.plan_id != plan.plan_id or state.binding_id != binding.binding_id:
                raise DataUnavailableError(
                    "LocalSim durable state identity drifted from the active plan",
                    context={
                        "reason_code": "LOCALSIM_DURABLE_STATE_IDENTITY_CONFLICT",
                        "state_id": state.state_id,
                        "state_plan_id": state.plan_id,
                        "plan_id": plan.plan_id,
                        "state_binding_id": state.binding_id,
                        "binding_id": binding.binding_id,
                    },
                )
            restorer(order=orders[intent_id], state=state)
        return states

    def _submit_execution_plan_with_timeout(
        self,
        *,
        build_result: SimulationPlanBuildResult | None,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        context: SimulationRunContext,
        mode: str,
        as_of_time: datetime | None,
        submit_callable: Callable[[], SimulationExecutionResult],
    ) -> SimulationExecutionResult:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
                self._configure_local_sim_runtime_scope(
                    binding=binding,
                    run=run,
                    plan=plan,
                    context=context,
                    restore=False,
                    as_of_time=as_of_time,
                )
            return submit_callable()
        try:
            self._prepare_miniqmt_quote_context_for_plan(
                binding=binding,
                plan=plan,
                as_of_time=as_of_time,
                recovering_active=False,
            )
        except Exception as exc:
            self._mark_miniqmt_quote_context_prepare_failure(
                binding=binding,
                run=run,
                plan=plan,
                exc=exc,
            )
            raise
        try:
            return self._run_callable_with_timeout(
                stage="MINIQMT_KERNEL_V2_PLAN_START",
                reason_code="MINIQMT_KERNEL_V2_PLAN_START_TIMEOUT",
                timeout_env_var=SIMULATION_MINIQMT_SUBMIT_TIMEOUT_ENV,
                default_timeout_seconds=DEFAULT_MINIQMT_SUBMIT_TIMEOUT_SECONDS,
                context={
                    "run_id": run.run_id,
                    "plan_id": plan.plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": plan.target_trade_date.isoformat(),
                    "mode": str(mode or "SIM").strip().upper(),
                    "as_of_time": as_of_time.isoformat() if isinstance(as_of_time, datetime) else None,
                    "execution_plan_intent_count": len(plan.intents),
                    "runtime_kind": "KERNEL_V2",
                    "build_result_present": build_result is not None,
                },
                func=submit_callable,
            )
        except RuntimeConfigInvalidError as exc:
            if self._exception_context(exc).get("reason_code") == "MINIQMT_KERNEL_V2_PLAN_START_TIMEOUT":
                self._mark_miniqmt_submit_timeout(binding=binding, run=run, plan=plan, exc=exc)
            raise

    def _mark_miniqmt_quote_context_prepare_failure(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: Exception,
    ) -> SimulationDailyRun:
        current = self.repository.get_simulation_daily_run(run.run_id)
        broker_side_effect_evidence = self._run_has_broker_side_effect_evidence(current)
        loud_payload = exc.as_loud_payload() if isinstance(exc, QuoteContractError) else None
        exception_context = (
            loud_payload
            if isinstance(loud_payload, dict)
            else {
                "reason_code": self._exception_context(exc).get("reason_code"),
                "stage": self._exception_context(exc).get("stage"),
                "message": str(exc),
                "context": self._exception_context(exc),
            }
        )
        retryable = not isinstance(exc, QuoteContractError) or exc.retryable
        next_status = (
            SimulationDailyRunStatus.FAILED_RETRYABLE
            if retryable or broker_side_effect_evidence
            else SimulationDailyRunStatus.FAILED_TERMINAL
        )
        diagnostic = {
            "schema_version": "miniqmt_quote_context_prepare_failure_v1",
            "reason_code": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
            "stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
            "run_id": current.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "exception_type": type(exc).__name__,
            "exception": exception_context,
            "retryable": retryable,
            "broker_callable_invoked": False,
            "broker_side_effect_evidence_before_attempt": broker_side_effect_evidence,
            "previous_status": current.status.value,
            "next_status": next_status.value,
        }
        return self.repository.update_simulation_daily_run(
            current.run_id,
            status=next_status,
            payload_patch={
                "last_stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
                "miniqmt_quote_context_prepare_failure": diagnostic,
                "submit_failure": {
                    "stage": _MINIQMT_QUOTE_CONTEXT_PREPARE_FAILURE_STAGE,
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )

    @staticmethod
    def _mini_qmt_run_has_runtime_execution_evidence(payload: dict[str, Any]) -> bool:
        for key in (
            "qmt_batch_id",
            "qmt_batch_result",
            "miniqmt_runtime_id",
            "miniqmt_runtime_route",
            "miniqmt_event_loop_tick_driver",
            "miniqmt_runtime_evidence",
        ):
            if payload.get(key):
                return True
        return False

    def _reconcile_after_submit_with_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any] | None:
        if binding.broker_backend != SimulationBrokerBackend.MINIQMT_SIM:
            return self._reconcile_after_submit(binding=binding, run=run, context=context)
        try:
            return self._run_callable_with_timeout(
                stage="MINIQMT_RECONCILE_AFTER_SUBMIT",
                reason_code="MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
                timeout_env_var=SIMULATION_MINIQMT_RECONCILE_TIMEOUT_ENV,
                default_timeout_seconds=DEFAULT_MINIQMT_RECONCILE_TIMEOUT_SECONDS,
                context={
                    "run_id": run.run_id,
                    "plan_id": run.execution_plan_id,
                    "binding_id": binding.binding_id,
                    "strategy_id": binding.strategy_id,
                    "broker_backend": binding.broker_backend.value,
                    "trade_date": run.trade_date.isoformat(),
                    "broker_called": bool(run.run_payload_json.get("broker_called")),
                    "submitted_intents": run.run_payload_json.get("submitted_intents"),
                    "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
                },
                func=lambda: self._reconcile_after_submit(binding=binding, run=run, context=context),
            )
        except RuntimeConfigInvalidError as exc:
            if self._exception_context(exc).get("reason_code") == "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT":
                self._mark_miniqmt_reconcile_timeout(binding=binding, run=run, exc=exc)
            raise

    def _mark_miniqmt_submit_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        plan: ExecutionPlan,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_submit_timeout_v1",
            "stage": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
            "reason_code": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
            "reason": "miniqmt_event_loop_submit_exceeded_binding_stage_timeout",
            "run_id": run.run_id,
            "plan_id": plan.plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": plan.target_trade_date.isoformat(),
            "execution_plan_intent_count": len(plan.intents),
            "side_effect_state": "UNKNOWN_TIMEOUT",
            "runtime_kind": MiniQMTExecutionRuntimeKind.EVENT_LOOP.value,
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": (
                "treat broker side effects as ambiguous, do not silently retry the same plan, and inspect broker "
                "orders/qmt ledger before any operator retry"
            ),
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "failed_intents": len(plan.intents),
                "miniqmt_side_effect_state": "UNKNOWN_TIMEOUT",
                "miniqmt_submit_timeout": diagnostic,
                "submit_failure": {
                    "stage": "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT",
                    "outer_stage": "MINIQMT_EVENT_LOOP_SUBMIT",
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": diagnostic,
                },
            },
        )

    def _mark_miniqmt_reconcile_timeout(
        self,
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        exc: RuntimeConfigInvalidError,
    ) -> SimulationDailyRun:
        context = self._exception_context(exc)
        diagnostic = {
            "schema_version": "miniqmt_reconcile_timeout_v1",
            "stage": "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
            "reason_code": "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT",
            "reason": "miniqmt_reconcile_after_submit_exceeded_binding_stage_timeout",
            "run_id": run.run_id,
            "plan_id": run.execution_plan_id,
            "binding_id": binding.binding_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "submitted_intents": run.run_payload_json.get("submitted_intents"),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "error_type": type(exc).__name__,
            "message": str(exc),
            "context": context,
            "next_action": "retry reconciliation on a later tick; do not block the scheduler thread",
        }
        return self.repository.update_simulation_daily_run(
            run.run_id,
            status=SimulationDailyRunStatus.FAILED_RETRYABLE,
            payload_patch={
                "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                "reconcile_after_submit": diagnostic,
                "miniqmt_reconcile_timeout": diagnostic,
            },
        )

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
        # MiniQMT fills can appear after submit returns; sync the broker snapshot
        # again before comparing broker positions with strategy lots.
        sync_after_submit = self._sync_miniqmt_snapshot(
            binding=binding,
            run=run,
            context=context,
            payload_key="sync_after_submit",
        )
        if not broker_positions and context.managed_order_service is not None:
            broker = getattr(context.managed_order_service, "_broker", None)
            get_positions = getattr(broker, "get_positions", None)
            if callable(get_positions):
                broker_positions = list(get_positions())
        report = service.reconcile_snapshot(
            account_id=binding.broker_account_id or "",
            trade_date=run.trade_date,
            broker_positions=broker_positions,
            sync_summary=sync_after_submit[1] if sync_after_submit is not None else None,
            broker_authoritative=True,
        )
        payload = report.to_dict() if hasattr(report, "to_dict") else dict(report)
        strategy_scope = self._miniqmt_reconciliation_strategy_scope(
            report=report,
            payload=payload,
            binding=binding,
        )
        run_status_gate = self._miniqmt_reconciliation_run_status_gate(
            payload=payload,
            strategy_scope=strategy_scope,
            context=context,
        )
        batch_residual_summary = self._mini_qmt_batch_residual_summary(run.run_payload_json)
        open_order_evidence = self._miniqmt_open_order_evidence(
            binding=binding,
            run=run,
            context=context,
        )
        side_effect_evidence = self._miniqmt_side_effect_evidence(
            binding=binding,
            run=run,
            context=context,
        )
        submit_result_gate = self._miniqmt_submit_result_gate(
            run=run,
            run_status_gate=run_status_gate,
            batch_residual_summary=batch_residual_summary,
            open_order_evidence=open_order_evidence,
            side_effect_evidence=side_effect_evidence,
        )
        capacity_residual_observability = self._miniqmt_capacity_residual_observability(
            run.run_payload_json,
            reason=submit_result_gate.get("reason"),
            source="submit_result_gate",
        )
        payload = {
            **payload,
            "strategy_scope": strategy_scope,
            "run_status_gate": run_status_gate,
            "submit_result_gate": submit_result_gate,
            "qmt_batch_residual_summary": batch_residual_summary,
            "open_order_evidence": open_order_evidence,
            "side_effect_evidence": side_effect_evidence,
        }
        if capacity_residual_observability:
            payload["miniqmt_capacity_residual_observability"] = capacity_residual_observability
        if submit_result_gate["status"] == "SUCCEEDED":
            next_status = SimulationDailyRunStatus.SUCCEEDED
        elif submit_result_gate["status"] == "PENDING":
            next_status = SimulationDailyRunStatus.INTRADAY_RUNNING
        else:
            next_status = SimulationDailyRunStatus.FAILED_RETRYABLE
        payload_patch = {
            "last_stage": next_status.value,
            "reconcile_after_submit": payload,
        }
        payload_patch.update(self._miniqmt_capacity_residual_payload_patch(capacity_residual_observability))
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch=payload_patch,
            payload_unset=("submit_failure",) if next_status == SimulationDailyRunStatus.SUCCEEDED else None,
        )
        return payload

    @staticmethod
    def _miniqmt_submit_result_gate(
        *,
        run: SimulationDailyRun,
        run_status_gate: dict[str, Any],
        batch_residual_summary: dict[str, Any],
        open_order_evidence: dict[str, Any],
        side_effect_evidence: dict[str, Any],
    ) -> dict[str, Any]:
        batch_succeeded = SimulationLifecycleScheduler._mini_qmt_batch_succeeded(run.run_payload_json)
        pending_event_loop = SimulationLifecycleScheduler._miniqmt_pending_event_loop_evidence(run.run_payload_json)
        terminal_capacity_residual = (
            bool(batch_residual_summary.get("noncompensating_residual"))
            and int(batch_residual_summary.get("capacity_residual_count") or 0) > 0
            and int(batch_residual_summary.get("dependent_buy_count") or 0) == 0
        )
        open_order_count = int(open_order_evidence.get("open_order_count") or 0)
        broker_side_effect_count = int(side_effect_evidence.get("broker_side_effect_count") or 0)
        if open_order_count > 0:
            status = "PENDING"
            reason = "miniqmt_open_orders_pending_after_reconciliation"
        elif pending_event_loop["eligible"]:
            status = "PENDING"
            reason = "miniqmt_event_loop_pending_after_reconciliation_warning"
        elif run_status_gate.get("status") != "SUCCEEDED":
            status = "blocked"
            reason = "miniqmt_reconciliation_run_status_gate_not_succeeded"
        elif batch_succeeded:
            status = "SUCCEEDED"
            reason = "miniqmt_batch_succeeded_and_reconciled"
        elif terminal_capacity_residual:
            status = "SUCCEEDED"
            reason = "miniqmt_capacity_residual_skipped_and_reconciled"
        elif broker_side_effect_count > 0:
            status = "blocked"
            reason = "miniqmt_broker_side_effect_requires_explicit_reconciliation"
        else:
            status = "blocked"
            reason = "MiniQMT reconciliation cannot mark a run successful when submit batch did not succeed"
        return {
            "schema_version": "miniqmt_reconcile_submit_result_gate_v2",
            "status": status,
            "reason": reason,
            "qmt_batch_status": run.run_payload_json.get("qmt_batch_status"),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "batch_succeeded": batch_succeeded,
            "terminal_capacity_residual": terminal_capacity_residual,
            "succeeded_with_capacity_residual": status == "SUCCEEDED" and terminal_capacity_residual,
            "capacity_residual_count": int(batch_residual_summary.get("capacity_residual_count") or 0),
            "failed_intents": int(run.run_payload_json.get("failed_intents") or 0),
            "open_order_count": open_order_count,
            "pending_open_orders": open_order_count > 0,
            "broker_side_effect_count": broker_side_effect_count,
            "pending_event_loop": pending_event_loop,
        }

    @staticmethod
    def _miniqmt_pending_event_loop_evidence(payload: dict[str, Any]) -> dict[str, Any]:
        batch = payload.get("qmt_batch_result") if isinstance(payload.get("qmt_batch_result"), dict) else {}
        runtime_evidence = batch.get("runtime_evidence") if isinstance(batch.get("runtime_evidence"), dict) else {}
        payload_batch_id = str(payload.get("qmt_batch_id") or "").strip()
        result_batch_id = str(batch.get("batch_id") or "").strip()
        runtime_id = str(runtime_evidence.get("runtime_id") or "").strip()
        source = str(runtime_evidence.get("source") or "").strip()
        payload_batch_status = str(payload.get("qmt_batch_status") or "").strip().upper()
        result_batch_status = str(batch.get("batch_status") or "").strip().upper()
        accepted_sources = {
            "simulation_runtime_event_loop_submit",
            "simulation_runtime_event_loop_tick_driver",
        }
        conflicts: list[str] = []

        def required_count(raw: Any, field: str) -> int | None:
            if raw is None:
                conflicts.append(f"{field}_missing")
                return None
            if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                conflicts.append(f"{field}_invalid")
                return None
            return raw

        if not payload_batch_id:
            conflicts.append("payload_batch_id_missing")
        if not result_batch_id:
            conflicts.append("result_batch_id_missing")
        if payload_batch_id and result_batch_id and payload_batch_id != result_batch_id:
            conflicts.append("batch_id_conflict")
        if payload_batch_status != OrderBatchStatus.SUBMITTING.value:
            conflicts.append("payload_batch_not_submitting")
        if result_batch_status != OrderBatchStatus.SUBMITTING.value:
            conflicts.append("result_batch_not_submitting")
        if payload_batch_status and result_batch_status and payload_batch_status != result_batch_status:
            conflicts.append("batch_status_conflict")
        if not runtime_id:
            conflicts.append("runtime_id_missing")
        if source not in accepted_sources:
            conflicts.append("runtime_evidence_source_invalid")

        pending_counts = {
            "payload_pending_intents": required_count(payload.get("pending_intents"), "payload_pending_intents"),
            "result_pending": required_count(batch.get("pending"), "result_pending"),
            "result_pending_child_trigger_count": required_count(
                batch.get("pending_child_trigger_count"),
                "result_pending_child_trigger_count",
            ),
            "runtime_pending_algo_count": required_count(
                runtime_evidence.get("pending_algo_count"),
                "runtime_pending_algo_count",
            ),
        }
        valid_pending_counts = [value for value in pending_counts.values() if value is not None]
        pending_count = pending_counts["runtime_pending_algo_count"]
        if len(valid_pending_counts) == len(pending_counts) and len(set(valid_pending_counts)) != 1:
            conflicts.append("pending_count_conflict")
        if pending_count == 0:
            conflicts.append("no_pending_algos")

        failure_counts = {
            "payload_failed_intents": required_count(payload.get("failed_intents"), "payload_failed_intents"),
            "result_failed": required_count(batch.get("failed"), "result_failed"),
            "runtime_rejected_child_count": required_count(
                runtime_evidence.get("rejected_child_count"),
                "runtime_rejected_child_count",
            ),
        }
        valid_failure_counts = [value for value in failure_counts.values() if value is not None]
        failed_count = max(valid_failure_counts) if valid_failure_counts else None
        if any(value > 0 for value in valid_failure_counts):
            conflicts.append("failed_or_rejected_algos_present")

        active_count = required_count(runtime_evidence.get("active_algo_count"), "runtime_active_algo_count")
        if active_count is not None and pending_count is not None and active_count < pending_count:
            conflicts.append("active_algo_count_below_pending")
        return {
            "schema_version": "miniqmt_pending_event_loop_evidence_v1",
            "eligible": not conflicts,
            "batch_id": payload_batch_id or result_batch_id or None,
            "runtime_id": runtime_id or None,
            "runtime_evidence_source": source or None,
            "batch_status": payload_batch_status or result_batch_status or None,
            "active_algo_count": active_count,
            "pending_algo_count": pending_count,
            "failed_or_rejected_count": failed_count,
            "identity_sources": {
                "payload_batch_id": payload_batch_id or None,
                "result_batch_id": result_batch_id or None,
                "payload_batch_status": payload_batch_status or None,
                "result_batch_status": result_batch_status or None,
            },
            "pending_count_sources": pending_counts,
            "failure_count_sources": failure_counts,
            "conflicts": conflicts,
        }

    @staticmethod
    def _miniqmt_open_order_evidence(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        repository = getattr(context, "qmt_ledger_repository", None)
        list_order_ledger = getattr(repository, "list_order_ledger", None)
        orders = []
        if callable(list_order_ledger):
            orders = list_order_ledger(
                account_id=binding.broker_account_id,
                trade_date=run.trade_date,
                strategy_id=binding.strategy_id,
                batch_id=run.run_payload_json.get("qmt_batch_id"),
            )
        open_orders = [
            order
            for order in orders
            if is_open_like_order_status(getattr(order, "order_status", None))
            and int(getattr(order, "order_volume", 0) or 0) > int(getattr(order, "traded_volume", 0) or 0)
        ]
        return {
            "schema_version": "miniqmt_open_order_evidence_v1",
            "source": "qmt_strategy.order_ledger",
            "account_id": binding.broker_account_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "open_order_count": len(open_orders),
            "open_orders": [
                {
                    "qmt_order_id": order.qmt_order_id,
                    "symbol": order.symbol,
                    "order_type": order.order_type,
                    "order_volume": order.order_volume,
                    "traded_volume": order.traded_volume,
                    "order_status": order.order_status,
                    "status_msg": order.status_msg,
                    "order_remark": order.order_remark,
                }
                for order in open_orders
            ],
        }

    @staticmethod
    def _miniqmt_side_effect_evidence(
        *,
        binding: SimulationReleaseBinding,
        run: SimulationDailyRun,
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        repository = getattr(context, "qmt_ledger_repository", None)
        list_order_ledger = getattr(repository, "list_order_ledger", None)
        orders = []
        if callable(list_order_ledger):
            orders = list_order_ledger(
                account_id=binding.broker_account_id,
                trade_date=run.trade_date,
                strategy_id=binding.strategy_id,
                batch_id=run.run_payload_json.get("qmt_batch_id"),
            )
        order_count = len(orders)
        return {
            "schema_version": "miniqmt_broker_side_effect_evidence_v1",
            "source": "qmt_strategy.order_ledger",
            "account_id": binding.broker_account_id,
            "strategy_id": binding.strategy_id,
            "trade_date": run.trade_date.isoformat(),
            "qmt_batch_id": run.run_payload_json.get("qmt_batch_id"),
            "broker_called": bool(run.run_payload_json.get("broker_called")),
            "order_ledger_count": order_count,
            "broker_side_effect_count": order_count,
        }

    @staticmethod
    def _miniqmt_reconciliation_strategy_scope(
        *,
        report: Any,
        payload: dict[str, Any],
        binding: SimulationReleaseBinding,
    ) -> dict[str, Any]:
        strategy_scope = getattr(report, "strategy_scope", None)
        if callable(strategy_scope):
            return strategy_scope(strategy_id=binding.strategy_id, strategy_name=binding.strategy_name)
        quantities = (
            payload.get("strategy_lot_quantities") if isinstance(payload.get("strategy_lot_quantities"), dict) else {}
        )
        strategy_name = str(binding.strategy_name or "").strip()
        matched = bool(strategy_name and strategy_name in quantities)
        scoped_quantities = quantities.get(strategy_name, {}) if matched else {}
        return {
            "schema_version": "miniqmt_reconciliation_strategy_scope_v1",
            "strategy_id": binding.strategy_id,
            "strategy_name": binding.strategy_name,
            "matched": matched,
            "status": str(payload.get("run", {}).get("status") or "WARNING") if matched else "WARNING",
            "issue_count": int(payload.get("run", {}).get("summary_json", {}).get("issue_count") or 0),
            "issue_types": [],
            "issue_symbols": sorted(scoped_quantities) if isinstance(scoped_quantities, dict) else [],
            "account_level_issue_count": 0,
            "position_count": len(scoped_quantities) if isinstance(scoped_quantities, dict) else 0,
            "symbols": sorted(scoped_quantities) if isinstance(scoped_quantities, dict) else [],
            "strategy_lot_quantities": scoped_quantities if isinstance(scoped_quantities, dict) else {},
        }

    @staticmethod
    def _miniqmt_reconciliation_run_status_gate(
        *,
        payload: dict[str, Any],
        strategy_scope: dict[str, Any],
        context: SimulationRunContext,
    ) -> dict[str, Any]:
        current_issue_count = int(strategy_scope.get("issue_count") or 0)
        diagnostics = context.context_diagnostics.get("miniqmt_broker_position_reconciliation")
        diagnostic_adjustment = _miniqmt_reconciliation_diagnostic_adjustment_symbols(diagnostics)
        issue_symbols = {str(symbol) for symbol in strategy_scope.get("issue_symbols", []) if str(symbol).strip()}
        matched = bool(strategy_scope.get("matched"))
        if not matched:
            status = "WARNING"
            reason = "strategy_scope_not_matched"
        elif current_issue_count <= 0:
            status = "SUCCEEDED"
            reason = "strategy_scope_has_no_blocking_issues"
        elif issue_symbols and issue_symbols <= diagnostic_adjustment:
            status = "SUCCEEDED"
            reason = "strategy_scope_issues_already_reconciled_by_context_provider"
        else:
            status = "WARNING"
            reason = "strategy_scope_has_blocking_issues"
        return {
            "schema_version": "miniqmt_reconciliation_run_status_gate_v1",
            "status": status,
            "reason": reason,
            "strategy_scope_issue_count": current_issue_count,
            "account_level_issue_count": int(strategy_scope.get("account_level_issue_count") or 0),
            "context_reconciled_symbol_count": len(diagnostic_adjustment),
            "strategy_scope_matched": matched,
        }

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
        self._validate_fresh_daily_selection_evidence(
            binding=binding,
            runtime_release=runtime_release,
            evidence=evidence,
            trade_date=trade_date,
            runtime_config=selection.runtime_config,
        )

    def _validate_fresh_daily_selection_evidence(
        self,
        *,
        binding: SimulationReleaseBinding,
        runtime_release: StrategyRuntimeRelease,
        evidence: DailySelectionEvidence,
        trade_date: date,
        runtime_config: dict[str, Any] | None,
    ) -> None:
        stale_reasons: list[str] = []
        if evidence.target_trade_date != trade_date:
            stale_reasons.append("target_trade_date")
        if evidence.package_id != binding.package_id:
            stale_reasons.append("package_id")
        if (
            evidence.manifest_sha256 != binding.manifest_sha256
            or evidence.manifest_sha256 != runtime_release.manifest_sha256
        ):
            stale_reasons.append("manifest_sha256")
        if evidence.release_id != runtime_release.release_id or evidence.release_hash != runtime_release.release_hash:
            stale_reasons.append("runtime_release")
        expected_cutoff = self._expected_rolling_pit_cutoff_date(
            runtime_config=runtime_config,
            trade_date=trade_date,
        )
        if expected_cutoff is not None and evidence.cutoff_date != expected_cutoff:
            stale_reasons.append("cutoff_date")
        if stale_reasons:
            raise DataUnavailableError(
                "simulation scheduler rejected stale daily selection evidence",
                context={
                    "reasons": stale_reasons,
                    "evidence_id": evidence.evidence_id,
                    "target_trade_date": evidence.target_trade_date.isoformat(),
                    "expected_trade_date": trade_date.isoformat(),
                    "cutoff_date": evidence.cutoff_date.isoformat() if evidence.cutoff_date else None,
                    "expected_cutoff_date": expected_cutoff.isoformat() if expected_cutoff else None,
                    "binding_id": binding.binding_id,
                    "release_id": runtime_release.release_id,
                },
            )

    def _expected_rolling_pit_cutoff_date(
        self,
        *,
        runtime_config: dict[str, Any] | None,
        trade_date: date,
    ) -> date | None:
        config = runtime_config if isinstance(runtime_config, dict) else {}
        artifact_config = StrategyPackageSelectionService.selection_artifact_config(config)
        pit_mode = StrategyPackageSelectionService.selection_pit_mode(config, artifact_config=artifact_config)
        if pit_mode == "NONE" or StrategyPackageSelectionService.is_fixed_cutoff_replay_config(
            config,
            artifact_config=artifact_config,
        ):
            return None
        resolver = getattr(self.selection_service, "resolve_point_in_time_context", None)
        if not callable(resolver):
            return None
        context = resolver(trade_date=trade_date, pit_mode=pit_mode, explicit_cutoff_date=None)
        raw_cutoff = context.get("cutoff_date") if isinstance(context, dict) else None
        if raw_cutoff is None:
            return None
        try:
            return date.fromisoformat(str(raw_cutoff))
        except ValueError as exc:
            raise RuntimeConfigInvalidError(
                "point-in-time selection cutoff_date must be YYYY-MM-DD",
                context={"cutoff_date": raw_cutoff, "trade_date": trade_date.isoformat()},
            ) from exc

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
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
        preserved_causality_cursor: datetime | None = None,
    ) -> SimulationPlanBuildResult:
        evidence = selection.evidence_by_package[binding.package_id]
        candidates = selection.package_results.get(binding.package_id, [])
        pre_trade_tradability = self._pre_trade_tradability_for_planning(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
            context=context,
            candidate_symbols=[candidate.symbol for candidate in candidates],
            require_realtime_quote=require_realtime_quote,
            as_of_time=as_of_time,
        )
        snapshot = SignalSnapshot(
            package_id=binding.package_id,
            manifest_sha256=evidence.manifest_sha256,
            trade_date=trade_date,
            data_source=data_source,
            candidates=candidates,
            runtime_config=selection.runtime_config,
            valid_no_candidate=selection.valid_no_candidate,
            no_candidate_reason=selection.no_candidate_reason,
        )
        target_total_equity, target_equity_context = self._target_equity_basis_for_context(
            binding=binding,
            context=context,
        )
        build_result = self.orchestrator.build_execution_plan(
            runtime_release=runtime_release,
            binding=binding,
            selection_evidence=evidence,
            signal_snapshot=snapshot,
            current_positions=context.current_positions,
            current_prices=context.current_prices,
            pre_trade_tradability=pre_trade_tradability,
            manifest=context.manifest,
            portfolio_id=context.portfolio_id or binding.strategy_id,
            top_k=context.top_k,
            execution_policy_payload=context.execution_policy_payload,
            tail_policy_payload=context.tail_policy_payload,
            target_total_equity=target_total_equity,
            target_equity_context=target_equity_context,
            created_by=created_by,
        )
        if binding.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return build_result
        return self._attach_local_sim_causality_cursor(
            build_result=build_result,
            as_of_time=as_of_time,
            preserved_cursor=preserved_causality_cursor,
        )

    def _local_sim_plan_causality_cursor(self, plan: ExecutionPlan | None) -> datetime | None:
        return self._local_sim_planner().causality_cursor(plan)

    def _attach_local_sim_causality_cursor(
        self,
        *,
        build_result: SimulationPlanBuildResult,
        as_of_time: datetime | None,
        preserved_cursor: datetime | None,
    ) -> SimulationPlanBuildResult:
        return self._local_sim_planner().attach_causality_cursor(
            build_result=build_result,
            as_of_time=as_of_time,
            preserved_cursor=preserved_cursor,
        )

    @staticmethod
    def _target_equity_basis_for_context(
        *,
        binding: SimulationReleaseBinding,
        context: SimulationRunContext,
    ) -> tuple[float | None, dict[str, Any] | None]:
        if context.target_total_equity is not None:
            return float(context.target_total_equity), dict(context.target_equity_context or {})
        cash = context.cash
        frozen_cash = context.frozen_cash
        if cash is None:
            if (
                binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                and context.qmt_ledger_repository is not None
            ):
                account = context.qmt_ledger_repository.get_virtual_account(binding.strategy_id)
                cash = float(account.cash)
                frozen_cash = float(account.frozen_cash)
            elif binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM and (
                context.qmt_sync_service is not None
                or context.qmt_reconciliation_service is not None
                or bool(context.broker_positions)
            ):
                raise DataUnavailableError(
                    "MiniQMT target sizing requires strategy-slot dynamic cash/equity context",
                    context={"strategy_id": binding.strategy_id, "binding_id": binding.binding_id},
                )
            else:
                return None, None
        return _build_dynamic_target_equity_basis(
            binding=binding,
            cash=float(cash),
            frozen_cash=float(frozen_cash),
            positions=context.current_positions,
            prices=SimulationLifecycleScheduler._performance_marks(context),
            source=(
                "miniqmt_strategy_slot_dynamic_equity"
                if binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
                else "paper_v2_portfolio_dynamic_equity"
            ),
        )

    def _pre_trade_tradability_for_planning(
        self,
        *,
        runtime_release: StrategyRuntimeRelease,
        binding: SimulationReleaseBinding,
        trade_date: date,
        context: SimulationRunContext,
        candidate_symbols: list[str],
        require_realtime_quote: bool | None = None,
        as_of_time: datetime | None = None,
    ) -> dict[str, dict[str, Any]]:
        symbols = sorted(
            {
                *context.current_positions.keys(),
                *[str(symbol).strip() for symbol in candidate_symbols if str(symbol).strip()],
            }
        )
        if not symbols:
            return {}
        daily_loader = getattr(self.context_provider, "load_daily_trading_context", None)
        frozen_daily_statuses: dict[str, dict[str, Any]] = {}
        planning_as_of = scheduler_time(as_of_time)
        requires_live_daily_context = trade_date == planning_as_of.date() and (
            binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
            or context.market_data_source == MinuteDataSource.TDX_REALTIME.value
        )
        if callable(daily_loader) and requires_live_daily_context:
            service = self.trading_calendar_service
            if service is None:
                raise DataUnavailableError(
                    "simulation daily trading context requires Trading Calendar Service",
                    context={"reason_code": "DAILY_TRADING_CONTEXT_CALENDAR_SERVICE_MISSING"},
                )
            calendar_snapshot = self._lifecycle_trading_day_status(service=service, trade_date=trade_date)
            frozen_daily_statuses = daily_loader(
                symbols=symbols,
                trade_date=trade_date,
                binding=binding,
                runtime_release=runtime_release,
                as_of_time=planning_as_of,
                calendar_service_snapshot=calendar_snapshot,
            )
        if binding.broker_backend == SimulationBrokerBackend.LOCAL_SIM:
            if not frozen_daily_statuses:
                # Static test providers may inject already-frozen evidence. The
                # production provider must never silently bypass this boundary.
                if context.pre_trade_tradability:
                    frozen_statuses = {
                        str(symbol): dict(status)
                        for symbol, status in context.pre_trade_tradability.items()
                        if isinstance(status, dict) and isinstance(status.get("daily_trading_context"), dict)
                    }
                    if frozen_statuses:
                        return frozen_statuses
                if not requires_live_daily_context:
                    return {}
                if getattr(self.context_provider, "provider_mode", None) == "production":
                    raise DataUnavailableError(
                        "LocalSIM production planning requires DailyTradingContextV1",
                        context={"reason_code": "DAILY_TRADING_CONTEXT_PROVIDER_MISSING"},
                    )
                return {}
            return frozen_daily_statuses
        loader = getattr(self.context_provider, "load_pre_trade_tradability", None)
        refresh_from_loader = require_realtime_quote is True and callable(loader)
        statuses = (
            {}
            if refresh_from_loader
            else {str(symbol): dict(status) for symbol, status in (context.pre_trade_tradability or {}).items()}
        )
        missing = list(symbols) if refresh_from_loader else [symbol for symbol in symbols if symbol not in statuses]
        if missing and callable(loader):
            loader_kwargs = {
                "symbols": missing,
                "trade_date": trade_date,
                "binding": binding,
                "market_data_source": context.market_data_source,
                "require_realtime_quote": require_realtime_quote,
                "as_of_time": as_of_time,
                "frozen_daily_statuses": frozen_daily_statuses or None,
            }
            signature = inspect.signature(loader)
            accepts_kwargs = any(
                parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in signature.parameters.values()
            )
            supported_kwargs = {
                key: value for key, value in loader_kwargs.items() if accepts_kwargs or key in signature.parameters
            }
            statuses.update(loader(**supported_kwargs))
        return statuses

    def _persist_no_rebalance_evidence(
        self,
        *,
        build_result: SimulationPlanBuildResult,
        current_positions: dict[str, PositionLot],
    ) -> SimulationPlanBuildResult:
        if build_result.rebalance.order_intents:
            return build_result

        target_by_symbol = {target.symbol: target for target in build_result.target_positions}
        decision_by_symbol = {decision.symbol: decision for decision in build_result.rebalance.trading_rule_decisions}
        rows: list[dict[str, Any]] = []
        for symbol in sorted(set(target_by_symbol) | set(current_positions)):
            target = target_by_symbol.get(symbol)
            position = current_positions.get(symbol)
            current_quantity = int(position.quantity) if position is not None else 0
            target_quantity = int(target.target_quantity) if target is not None else 0
            decision = decision_by_symbol.get(symbol)
            rows.append(
                {
                    "symbol": symbol,
                    "target_quantity": target_quantity,
                    "current_quantity": current_quantity,
                    "delta_quantity": target_quantity - current_quantity,
                    "current_available_quantity": int(position.available_quantity) if position is not None else None,
                    "current_position_trade_date": position.trade_date.isoformat() if position is not None else None,
                    "target_weight": float(target.target_weight)
                    if target is not None and target.target_weight is not None
                    else None,
                    "reference_price": float(target.reference_price)
                    if target is not None and target.reference_price is not None
                    else None,
                    "target_reason": target.reason if target is not None else "DROPPED_FROM_SELECTION",
                    "trading_rule_decision": {
                        "decision_id": decision.decision_id,
                        "decision": decision.decision,
                        "reason_code": decision.reason_code,
                        "requested_quantity": int(decision.requested_quantity),
                        "legal_quantity": int(decision.legal_quantity),
                    }
                    if decision is not None
                    else None,
                }
            )

        reason_code = (
            "TOP_LIST_AND_QUANTITY_MATCH"
            if rows and all(row["delta_quantity"] == 0 for row in rows)
            else "NO_EMITTABLE_REBALANCE_INTENTS"
        )
        evidence_payload = {
            "schema_version": "no_rebalance_evidence_v1",
            "source": "simulation_lifecycle_scheduler",
            "reason_code": reason_code,
            "target_trade_date": build_result.selection_evidence.target_trade_date.isoformat(),
            "package_id": build_result.runtime_release.package_id,
            "manifest_sha256": build_result.runtime_release.manifest_sha256,
            "release_id": build_result.runtime_release.release_id,
            "binding_id": build_result.binding.binding_id,
            "strategy_id": build_result.binding.strategy_id,
            "broker_backend": build_result.binding.broker_backend.value,
            "selection_evidence_id": build_result.selection_evidence.evidence_id,
            "execution_plan_id": build_result.execution_plan.plan_id,
            "selected_symbols": [
                candidate.symbol
                for candidate in sorted(build_result.signal_snapshot.candidates, key=lambda item: item.rank)
            ],
            "target_symbols": sorted(target_by_symbol),
            "current_symbols": sorted(current_positions),
            "target_count": len(build_result.target_positions),
            "current_position_count": len(current_positions),
            "trading_rule_decision_count": len(build_result.rebalance.trading_rule_decisions),
            "rows": rows,
        }
        updated_run = self.repository.update_simulation_daily_run(
            build_result.run.run_id,
            payload_patch={"no_rebalance_evidence": evidence_payload},
        )
        return replace(build_result, run=updated_run)

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
        next_status = (
            SimulationDailyRunStatus.SUCCEEDED if result.success else SimulationDailyRunStatus.FAILED_RETRYABLE
        )
        self.repository.update_simulation_daily_run(
            run.run_id,
            status=next_status,
            payload_patch={
                "tail_handling": payload,
                "last_stage": next_status.value,
            },
            payload_unset=("submit_failure",) if next_status == SimulationDailyRunStatus.SUCCEEDED else None,
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
            submit_result_gate = (
                reconciliation.get("submit_result_gate")
                if isinstance(reconciliation.get("submit_result_gate"), dict)
                else {}
            )
            run_status_gate = (
                reconciliation.get("run_status_gate") if isinstance(reconciliation.get("run_status_gate"), dict) else {}
            )
            status = submit_result_gate.get("status") or run_status_gate.get("status") or run.get("status")
            if status == "PENDING":
                return "RECONCILIATION_PENDING_OPEN_ORDERS"
            if execution_status not in {"SUBMITTED", "RECOVERED"}:
                return "BROKER_SUBMIT_FAILED_RECONCILED" if status == "SUCCEEDED" else "BROKER_SUBMIT_FAILED"
            return "RECONCILED" if status == "SUCCEEDED" else "RECONCILIATION_WARNING"
        return execution_status

    @staticmethod
    def _local_sim_terminal_capacity_residual_status(run: SimulationDailyRun, *, fallback: str) -> str:
        if run.broker_backend != SimulationBrokerBackend.LOCAL_SIM:
            return fallback
        if run.status != SimulationDailyRunStatus.FAILED_TERMINAL:
            return fallback
        terminalization = run.run_payload_json.get("local_sim_capacity_residual_terminalization")
        if isinstance(terminalization, dict):
            if int(terminalization.get("schedule_residual_count") or 0) > 0:
                return "LOCALSIM_EXECUTION_RESIDUAL_TERMINAL"
            return "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
        return fallback

    @staticmethod
    def _compute_schedule_windows(*, trade_date: date, as_of_time: datetime | None) -> tuple[dict[str, Any], ...]:
        return compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)

    @staticmethod
    def _scheduler_now() -> datetime:
        return scheduler_now()

    @staticmethod
    def _scheduler_time(value: datetime | None) -> datetime:
        return scheduler_time(value)


simulation_lifecycle_scheduler = build_simulation_lifecycle_scheduler_from_env()


class SimulationLifecycleBackgroundScheduler:
    """Opt-in unattended scheduler wrapper with trading-window orchestration."""

    def __init__(
        self,
        *,
        lifecycle_scheduler: SimulationLifecycleScheduler | None = None,
        trading_calendar_service: Any | None = None,
        tca_eod_observation_hook: TcaEodObservationHook | None = None,
        tca_observation_metrics_emitter: TcaObservationMetricsEmitter | None = None,
        localsim_replay_lifecycle_owner: Any | None = None,
        miniqmt_enabled: bool = True,
    ) -> None:
        self.lifecycle_scheduler = lifecycle_scheduler or SimulationLifecycleScheduler()
        self._trading_calendar_service = trading_calendar_service or TradingCalendarStatusService()
        self._tca_eod_observation_hook = tca_eod_observation_hook or TcaEodObservationHook()
        self._tca_observation_metrics_emitter = tca_observation_metrics_emitter or TcaObservationMetricsEmitter()
        self._localsim_replay_lifecycle_owner = localsim_replay_lifecycle_owner
        self._miniqmt_enabled = bool(miniqmt_enabled)
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._lock = threading.RLock()
        self._interval_seconds = self._default_interval()
        self._default_submit = self._env_flag("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", default=False)
        self._data_source = (
            os.getenv("SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE") or "DB_HISTORICAL"
        ).strip() or "DB_HISTORICAL"
        self._limit = self._default_limit()
        self._last_run_at: datetime | None = None
        self._last_result: dict[str, Any] | None = None
        self._last_blocking_result: dict[str, Any] | None = None
        self._active_loop_failure: dict[str, Any] | None = None
        self._last_loop_failure: dict[str, Any] | None = None
        self._last_successful_loop_tick_at: datetime | None = None
        self._loop_consecutive_failure_count = 0
        self._loop_total_failure_count = 0
        self._loop_total_success_count = 0

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
        thread_alive = bool(thread and thread.is_alive())
        shutdown_selection = getattr(self.lifecycle_scheduler, "shutdown_selection_inference", None)
        if callable(shutdown_selection):
            graceful = bool(wait and not thread_alive)
            shutdown_selection(wait=graceful)
            logger.info(
                "Simulation runtime scheduler selection inference executor stopped wait=%s thread_alive=%s",
                graceful,
                thread_alive,
            )
        shutdown_binding_ticks = getattr(self.lifecycle_scheduler, "shutdown_binding_ticks", None)
        if callable(shutdown_binding_ticks):
            graceful = bool(wait and not thread_alive)
            binding_shutdown = shutdown_binding_ticks(wait=graceful)
            logger.info(
                "Simulation runtime scheduler LocalSIM binding owner shutdown observed wait=%s "
                "scheduler_thread_alive=%s observation=%s",
                graceful,
                thread_alive,
                binding_shutdown,
            )
        shutdown_quote_ingress = getattr(self.lifecycle_scheduler, "shutdown_miniqmt_quote_ingress", None)
        if callable(shutdown_quote_ingress):
            shutdown_quote_ingress()
            logger.info("Simulation runtime scheduler MiniQMT quote ingress stopped")
        logger.info("Simulation runtime scheduler stopped")
        return self.status()

    def status(self) -> dict[str, Any]:
        thread = self._thread
        base = self.lifecycle_scheduler.status()
        with self._lock:
            thread_alive = bool(thread and thread.is_alive())
            running = bool(thread_alive and not self._stop_event.is_set())
            last_run_at = self._last_run_at
            last_result = deepcopy(self._last_result)
            last_blocking_result = deepcopy(self._last_blocking_result)
            scheduler_loop_health = self._scheduler_loop_health_locked()
        return {
            **base,
            "autostart": running,
            "running": running,
            "thread_alive": thread_alive,
            "scheduler_control_api_enabled": False,
            "manual_tick_endpoint_enabled": False,
            "interval_seconds": self._interval_seconds,
            "default_submit": self._default_submit,
            "data_source": self._data_source,
            "data_source_policy": self._data_source_policy(),
            "miniqmt_enabled": self._miniqmt_enabled,
            "scheduled_broker_backends": (
                [SimulationBrokerBackend.LOCAL_SIM.value, SimulationBrokerBackend.MINIQMT_SIM.value]
                if self._miniqmt_enabled
                else [SimulationBrokerBackend.LOCAL_SIM.value]
            ),
            "limit": self._limit,
            "last_run_at": last_run_at.isoformat() if last_run_at else None,
            "last_result": last_result,
            "last_blocking_result": last_blocking_result,
            "scheduler_loop_health": scheduler_loop_health,
            "trading_calendar_policy": self._trading_calendar_policy(),
            "localsim_replay_lifecycle_owner_enabled": self._localsim_replay_lifecycle_owner is not None,
        }

    def run_once(self, *, as_of_time: datetime | None = None) -> dict[str, Any]:
        now = SimulationLifecycleScheduler._scheduler_time(as_of_time)
        trade_date = self._trade_date(now)
        decision = self._window_decision(as_of_time=now, trade_date=trade_date)
        result: dict[str, Any] = {
            "started_at": now.isoformat(),
            "trade_date": trade_date.isoformat(),
            "timezone": SCHEDULER_TZ_NAME,
            "data_source": self._data_source,
            "data_source_policy": self._data_source_policy(),
            "window": decision["window"],
            "should_run": False,
            "submit": False,
            "reason": None,
            "trading_calendar": None,
            "processed": [],
            "errors": [],
            "alerts": [],
        }
        if self._localsim_replay_lifecycle_owner is not None:
            try:
                result["localsim_replay_lifecycle"] = self._localsim_replay_lifecycle_owner.tick(
                    as_of_time=now
                )
            except Exception as exc:
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                    "stage": "LOCALSIM_REPLAY_LIFECYCLE",
                }
                result["errors"].append(payload)
                logger.warning("LocalSIM replay lifecycle tick failed: %s", payload)
        try:
            calendar_status = self._trading_day_status(trade_date=trade_date)
        except DataUnavailableError as exc:
            payload = {
                "type": type(exc).__name__,
                "message": str(exc),
                "context": getattr(exc, "context", None),
            }
            result["reason"] = "trading_calendar_unavailable"
            result["errors"].append(payload)
            logger.warning("Simulation runtime scheduler trading calendar gate failed: %s", payload)
            return self._record_result(started_at=now, result=result)
        result["trading_calendar"] = calendar_status
        if not bool(calendar_status.get("is_trading_day")):
            result["reason"] = "non_trading_day"
            result["skip_reason"] = "non_trading_day"
            result["next_trading_day"] = calendar_status.get("next_trading_day")
            return self._record_result(started_at=now, result=result)
        result["should_run"] = decision["should_run"]
        result["submit"] = decision["submit"]
        result["reason"] = decision["reason"]
        if decision["should_run"]:
            try:
                if decision["reason"] == "eod_reconcile":
                    tick = self.lifecycle_scheduler.post_close_reconcile_once(
                        trade_date=trade_date,
                        data_source=self._data_source,
                        limit=self._limit,
                        broker_backend=(
                            None if self._miniqmt_enabled else SimulationBrokerBackend.LOCAL_SIM
                        ),
                        as_of_time=now,
                    )
                else:
                    tick = self.lifecycle_scheduler.run_once(
                        trade_date=trade_date,
                        data_source=self._data_source,
                        limit=self._limit,
                        broker_backend=(
                            None if self._miniqmt_enabled else SimulationBrokerBackend.LOCAL_SIM
                        ),
                        submit=bool(decision["submit"]),
                        as_of_time=now,
                    )
                observation_outcomes: list[dict[str, Any]] = []
                if decision["reason"] == "eod_reconcile":
                    observation_outcomes = self._run_tca_eod_observation(
                        terminalized_runs=tuple(tick.stale_run_results),
                        trade_date=trade_date,
                        as_of_time=now,
                    )
                processed = []
                alerts = []
                if observation_outcomes:
                    result["tca_eod_observation"] = observation_outcomes
                    metrics, observation_alerts = self._emit_tca_observation_metrics(
                        outcomes=observation_outcomes,
                        trade_date=trade_date,
                    )
                    result["tca_eod_observation_metrics"] = metrics
                    alerts.extend(observation_alerts)
                for item in tick.results:
                    capacity_fields = SimulationLifecycleScheduler._miniqmt_capacity_residual_result_fields(item.run)
                    alert = capacity_fields.get("alert")
                    if isinstance(alert, dict):
                        alerts.append(alert)
                    lifecycle_alert = (
                        item.lifecycle_diagnostic.get("alert") if isinstance(item.lifecycle_diagnostic, dict) else None
                    )
                    if item.status == "LOCALSIM_BINDING_TICK_IN_PROGRESS" and isinstance(lifecycle_alert, dict):
                        alerts.append(lifecycle_alert)
                    processed.append(
                        {
                            "binding_id": item.binding_id,
                            "strategy_id": item.strategy_id,
                            "broker_backend": item.broker_backend.value,
                            "status": item.status,
                            "run_id": item.run.run_id if item.run else None,
                            "execution_plan_id": item.execution_plan.plan_id if item.execution_plan else None,
                            "data_source": item.data_source or self._data_source,
                            "error": item.error,
                            "lifecycle_diagnostic": item.lifecycle_diagnostic,
                            **capacity_fields,
                        }
                    )
                for terminalized in tick.stale_run_results:
                    alert = terminalized.get("alert")
                    if isinstance(alert, dict):
                        alerts.append(alert)
                    recovery_error = terminalized.get("error")
                    if terminalized.get("terminalization_succeeded") is False and isinstance(recovery_error, dict):
                        result["errors"].append(
                            {
                                **recovery_error,
                                "stage": terminalized.get("stage"),
                                "reason_code": terminalized.get("reason_code"),
                            }
                        )
                result["processed"] = processed
                result["terminalized_runs"] = list(tick.stale_run_results)
                result["alerts"] = alerts
                result["summary"] = {
                    "total_bindings": tick.total_bindings,
                    "planned_count": tick.planned_count,
                    "reused_count": tick.reused_count,
                    "submitted_count": tick.submitted_count,
                    "failed_count": tick.failed_count,
                    "retired_package_skipped_count": sum(
                        1 for item in tick.results if item.status == "SKIPPED_RETIRED_PACKAGE"
                    ),
                    "stale_terminalized_count": tick.stale_terminalized_count,
                    "stale_recovery_failed_count": tick.stale_recovery_failed_count,
                    "recovery_backoff_count": getattr(
                        tick,
                        "recovery_backoff_count",
                        sum(1 for item in tick.stale_run_results if item.get("status") == "RECOVERY_BACKOFF"),
                    ),
                    "succeeded_with_capacity_residual_count": sum(
                        1 for item in processed if item.get("succeeded_with_capacity_residual")
                    )
                    + sum(1 for item in tick.stale_run_results if item.get("succeeded_with_capacity_residual")),
                    "capacity_residual_count": sum(int(item.get("capacity_residual_count") or 0) for item in processed)
                    + sum(int(item.get("capacity_residual_count") or 0) for item in tick.stale_run_results),
                    "capacity_residual_failed_intents": sum(
                        int(item.get("capacity_residual_failed_intents") or 0) for item in processed
                    )
                    + sum(int(item.get("capacity_residual_failed_intents") or 0) for item in tick.stale_run_results),
                }
            except Exception as exc:  # scheduler must expose failure, not crash silently
                payload = {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "context": getattr(exc, "context", None),
                }
                result["errors"].append(payload)
                logger.warning("Simulation runtime scheduler tick failed: %s", payload)
        return self._record_result(started_at=now, result=result)

    def _run_tca_eod_observation(
        self,
        *,
        terminalized_runs: tuple[Mapping[str, Any], ...],
        trade_date: date,
        as_of_time: datetime,
    ) -> list[dict[str, Any]]:
        """Keep observation failures out of scheduler/run/broker outcome handling."""

        try:
            outcomes = self._tca_eod_observation_hook.observe_post_reconciliation(
                lifecycle_scheduler=self.lifecycle_scheduler,
                terminalized_runs=terminalized_runs,
                trade_date=trade_date,
                as_of_time=as_of_time,
            )
            return [dict(item) for item in outcomes]
        except Exception as exc:  # noqa: BLE001 - hook isolation is a Phase 0A safety contract.
            payload = {
                "status": "FAILED",
                "reason_code": "ADAPTIVE_IS_TCA_EOD_HOOK_EXCEPTION",
                "stage": "TCA_EOD_SCHEDULER_SEAM",
                "error_type": type(exc).__name__,
            }
            logger.error("MiniQMT TCA EOD observation hook failed: %s", payload, exc_info=True)
            return [payload]

    def _emit_tca_observation_metrics(
        self, *, outcomes: list[dict[str, Any]], trade_date: date
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Metrics remain fail-isolated facts and cannot block scheduler completion."""

        try:
            emission = self._tca_observation_metrics_emitter.emit(
                outcomes=outcomes,
                trade_date=trade_date,
                source="simulation_runtime_background_scheduler",
            )
            return [dict(item) for item in emission.metrics], [dict(item) for item in emission.alerts]
        except Exception as exc:  # noqa: BLE001 - metric failure must not alter terminal reconciliation.
            payload = {
                "alert_type": "MINIQMT_TCA_OBSERVATION_METRIC_FAILURE",
                "severity": "WARNING",
                "reason_code": "ADAPTIVE_IS_TCA_METRIC_EMIT_FAILED",
                "stage": "TCA_EOD_METRIC",
                "trade_date": trade_date.isoformat(),
                "execution_gate": False,
                "observation_only": True,
                "error_type": type(exc).__name__,
            }
            logger.error("MiniQMT TCA observation metric emission failed: %s", payload, exc_info=True)
            return [], [payload]

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.run_once()
            except Exception as exc:
                failure = self._record_loop_exception(exc)
                logger.exception(
                    "Simulation runtime scheduler run_once crashed reason_code=%s consecutive_failure_count=%s",
                    failure["reason_code"],
                    failure["consecutive_failure_count"],
                )
            else:
                self._record_loop_success()
            if self._stop_event.wait(timeout=self._interval_seconds):
                break

    def _record_loop_exception(self, exc: Exception) -> dict[str, Any]:
        failure_at = datetime.now(UTC)
        trade_date = failure_at.astimezone(SCHEDULER_TZ).date()
        raw_context = getattr(exc, "context", None)
        context = dict(raw_context) if isinstance(raw_context, Mapping) else {}
        message = str(exc)
        bounded_message = message[:2048]
        raw_underlying_reason_code = context.get("reason_code")
        raw_underlying_stage = context.get("stage") or context.get("failure_stage")
        underlying_reason_code = (
            str(raw_underlying_reason_code)[:512] if raw_underlying_reason_code is not None else None
        )
        underlying_stage = str(raw_underlying_stage)[:512] if raw_underlying_stage is not None else None
        with self._lock:
            consecutive_failure_count = self._loop_consecutive_failure_count + 1
            first_failure_at = (
                str(self._active_loop_failure.get("first_failure_at"))
                if isinstance(self._active_loop_failure, dict) and self._active_loop_failure.get("first_failure_at")
                else failure_at.isoformat()
            )
            failure = {
                "schema_version": "simulation_background_scheduler_loop_failure_v1",
                "status": "BLOCKED",
                "reason_code": "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION",
                "stage": "BACKGROUND_SCHEDULER_RUN_LOOP",
                "exception_type": type(exc).__name__,
                "exception_message": bounded_message,
                "exception_message_truncated": len(message) > len(bounded_message),
                "underlying_reason_code": underlying_reason_code,
                "underlying_stage": underlying_stage,
                "context": self._bounded_loop_exception_context(context),
                "trade_date": trade_date.isoformat(),
                "first_failure_at": first_failure_at,
                "failure_at": failure_at.isoformat(),
                "consecutive_failure_count": consecutive_failure_count,
                "total_failure_count": self._loop_total_failure_count + 1,
                "last_successful_tick_at": (
                    self._last_successful_loop_tick_at.isoformat()
                    if self._last_successful_loop_tick_at is not None
                    else None
                ),
                "execution_gate": False,
                "auto_clears_on_success": True,
            }
            failure_result = {
                "schema_version": "simulation_background_scheduler_loop_result_v1",
                "started_at": failure_at.isoformat(),
                "completed_at": failure_at.isoformat(),
                "trade_date": trade_date.isoformat(),
                "timezone": SCHEDULER_TZ_NAME,
                "window": None,
                "should_run": False,
                "submit": False,
                "reason": "background_scheduler_run_loop_exception",
                "processed": [],
                "errors": [
                    {
                        "type": failure["exception_type"],
                        "message": failure["exception_message"],
                        "reason_code": failure["reason_code"],
                        "underlying_reason_code": failure["underlying_reason_code"],
                        "stage": failure["stage"],
                    }
                ],
                "alerts": [],
                "has_blocking_result": True,
                "scheduler_loop_failure": deepcopy(failure),
            }
            self._loop_consecutive_failure_count = consecutive_failure_count
            self._loop_total_failure_count += 1
            self._active_loop_failure = deepcopy(failure)
            self._last_loop_failure = deepcopy(failure)
            self._last_run_at = failure_at
            self._last_result = deepcopy(failure_result)
            self._last_blocking_result = deepcopy(failure_result)
            return deepcopy(failure)

    def _record_loop_success(self) -> None:
        success_at = datetime.now(UTC)
        with self._lock:
            self._loop_consecutive_failure_count = 0
            self._loop_total_success_count += 1
            self._last_successful_loop_tick_at = success_at
            self._active_loop_failure = None

    def _scheduler_loop_health_locked(self) -> dict[str, Any]:
        if self._active_loop_failure is not None:
            status = "BLOCKED"
            reason_code = "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION"
        elif self._last_successful_loop_tick_at is not None:
            status = "HEALTHY"
            reason_code = "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_OK"
        else:
            status = "NOT_YET_RUN"
            reason_code = "SIMULATION_BACKGROUND_SCHEDULER_LOOP_NOT_YET_RUN"
        return {
            "schema_version": "simulation_background_scheduler_loop_health_v1",
            "status": status,
            "reason_code": reason_code,
            "active_failure": deepcopy(self._active_loop_failure),
            "last_failure": deepcopy(self._last_loop_failure),
            "last_successful_tick_at": (
                self._last_successful_loop_tick_at.isoformat()
                if self._last_successful_loop_tick_at is not None
                else None
            ),
            "consecutive_failure_count": self._loop_consecutive_failure_count,
            "total_failure_count": self._loop_total_failure_count,
            "total_success_count": self._loop_total_success_count,
            "execution_gate": False,
            "auto_clears_on_success": True,
        }

    @staticmethod
    def _bounded_loop_exception_context(context: Mapping[str, Any]) -> dict[str, Any]:
        allowed_keys = (
            "reason_code",
            "stage",
            "failure_stage",
            "trade_date",
            "binding_id",
            "run_id",
            "plan_id",
            "strategy_id",
            "broker_backend",
            "timeout_seconds",
            "thread_alive",
        )
        bounded: dict[str, Any] = {}
        for key in allowed_keys:
            if key not in context:
                continue
            value = context[key]
            if value is None or isinstance(value, (bool, int, float)):
                bounded[key] = value
            elif isinstance(value, (date, datetime)):
                bounded[key] = value.isoformat()
            elif isinstance(value, Enum):
                bounded[key] = value.value
            else:
                text = str(value)
                bounded[key] = text[:512]
        return bounded

    def _window_decision(self, *, as_of_time: datetime, trade_date: date) -> dict[str, Any]:
        windows = SimulationLifecycleScheduler._compute_schedule_windows(trade_date=trade_date, as_of_time=as_of_time)
        active = next((item for item in windows if item["state"] == "ACTIVE"), None)
        if active is None:
            return {"window": None, "should_run": False, "submit": False, "reason": "outside_configured_windows"}
        action = str(active.get("action") or "")
        should_run = action in {"selection_evidence", "execution_plan", "submit", "eod_reconcile"}
        submit = bool(self._default_submit and action == "submit")
        return {"window": active, "should_run": should_run, "submit": submit, "reason": action}

    def _trading_day_status(self, *, trade_date: date) -> dict[str, Any]:
        service = self._trading_calendar_service
        status_method = getattr(service, "status", None)
        if callable(status_method):
            raw_status = dict(status_method(as_of_date=trade_date))
            if "is_trading_day" not in raw_status:
                raise DataUnavailableError(
                    "simulation runtime scheduler trading calendar status is missing is_trading_day",
                    context={
                        "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "status_keys": sorted(str(key) for key in raw_status),
                    },
                )
            is_trading_day = raw_status.get("is_trading_day")
            if not isinstance(is_trading_day, bool):
                raise DataUnavailableError(
                    "simulation runtime scheduler trading calendar status has non-boolean is_trading_day",
                    context={
                        "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                        "trade_date": trade_date.isoformat(),
                        "service": type(service).__name__,
                        "is_trading_day_type": type(is_trading_day).__name__,
                        "is_trading_day_value": repr(is_trading_day),
                    },
                )
            raw_status.setdefault("as_of_date", trade_date.isoformat())
            raw_status["is_trading_day"] = is_trading_day
            if not raw_status["is_trading_day"] and not raw_status.get("next_trading_day"):
                raw_status["next_trading_day"] = self._next_trading_day_iso(trade_date)
            return {
                "schema_version": "simulation_runtime_trading_day_gate_v1",
                "service": type(service).__name__,
                "policy": "skip_non_trading_day_before_selection_planning_submit",
                **raw_status,
            }
        is_trading_day_method = getattr(service, "is_trading_day", None)
        if not callable(is_trading_day_method):
            raise DataUnavailableError(
                "simulation runtime scheduler trading calendar service lacks status/is_trading_day",
                context={
                    "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_METHOD_MISSING",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                },
            )
        is_trading_day = is_trading_day_method(trade_date)
        if not isinstance(is_trading_day, bool):
            raise DataUnavailableError(
                "simulation runtime scheduler is_trading_day returned non-boolean value",
                context={
                    "reason_code": "SIMULATION_RUNTIME_TRADING_CALENDAR_STATUS_INVALID",
                    "trade_date": trade_date.isoformat(),
                    "service": type(service).__name__,
                    "is_trading_day_type": type(is_trading_day).__name__,
                    "is_trading_day_value": repr(is_trading_day),
                },
            )
        return {
            "schema_version": "simulation_runtime_trading_day_gate_v1",
            "service": type(service).__name__,
            "policy": "skip_non_trading_day_before_selection_planning_submit",
            "ok": True,
            "as_of_date": trade_date.isoformat(),
            "is_trading_day": is_trading_day,
            "next_trading_day": None if is_trading_day else self._next_trading_day_iso(trade_date),
        }

    def _next_trading_day_iso(self, trade_date: date) -> str | None:
        next_method = getattr(self._trading_calendar_service, "next_trading_day", None)
        if callable(next_method):
            next_day = next_method(trade_date)
            return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        next_after_method = getattr(self._trading_calendar_service, "next_trading_day_after", None)
        if callable(next_after_method):
            next_day = next_after_method(trade_date)
            return next_day.isoformat() if isinstance(next_day, date) else str(next_day)
        return None

    def _record_result(self, *, started_at: datetime, result: dict[str, Any]) -> dict[str, Any]:
        result["completed_at"] = SimulationLifecycleScheduler._scheduler_now().isoformat()
        result["has_blocking_result"] = self._result_has_blocking_evidence(result)
        stored_result = deepcopy(result)
        with self._lock:
            self._last_run_at = started_at
            self._last_result = stored_result
            if result["has_blocking_result"]:
                self._last_blocking_result = deepcopy(stored_result)
        return result

    @staticmethod
    def _result_has_blocking_evidence(result: dict[str, Any]) -> bool:
        if isinstance(result.get("errors"), list) and bool(result["errors"]):
            return True
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        for summary_field in ("failed_count", "stale_recovery_failed_count"):
            try:
                if int(summary.get(summary_field) or 0) > 0:
                    return True
            except (TypeError, ValueError):
                return True
        processed = result.get("processed")
        if isinstance(processed, list):
            for item in processed:
                if not isinstance(item, dict):
                    return True
                if isinstance(item.get("error"), dict):
                    return True
                if str(item.get("status") or "").upper() in {
                    SimulationDailyRunStatus.FAILED_RETRYABLE.value,
                    SimulationDailyRunStatus.FAILED_TERMINAL.value,
                }:
                    return True
        terminalized = result.get("terminalized_runs")
        if isinstance(terminalized, list) and any(
            isinstance(item, dict) and item.get("terminalization_succeeded") is False for item in terminalized
        ):
            return True
        return False

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
            logger.warning(
                "Simulation runtime scheduler invalid interval; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_INTERVAL_INVALID",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC",
                    "raw_value": raw,
                    "fallback_seconds": 30,
                },
            )
            return 30
        if value <= 0:
            logger.warning(
                "Simulation runtime scheduler non-positive interval; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_INTERVAL_NON_POSITIVE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_INTERVAL_SEC",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_seconds": 30,
                },
            )
            return 30
        return value

    @staticmethod
    def _default_limit() -> int:
        raw = (os.getenv("SIMULATION_RUNTIME_SCHEDULER_LIMIT") or "100").strip()
        try:
            value = int(raw)
        except ValueError:
            logger.warning(
                "Simulation runtime scheduler invalid limit; using fail-safe default",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_INVALID",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "fallback_limit": 100,
                },
            )
            return 100
        if value < 1:
            logger.warning(
                "Simulation runtime scheduler non-positive limit; using fail-safe minimum",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_NON_POSITIVE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_limit": 1,
                },
            )
            return 1
        if value > 500:
            logger.warning(
                "Simulation runtime scheduler excessive limit; clamping to fail-safe maximum",
                extra={
                    "reason_code": "SIMULATION_SCHEDULER_LIMIT_TOO_LARGE",
                    "env_var": "SIMULATION_RUNTIME_SCHEDULER_LIMIT",
                    "raw_value": raw,
                    "parsed_value": value,
                    "fallback_limit": 500,
                },
            )
            return 500
        return value

    def _data_source_policy(self) -> dict[str, str]:
        return {
            "default": self._data_source,
            "local_sim_same_day": MinuteDataSource.TDX_REALTIME.value,
            "local_sim_historical": "persisted_portfolio_data_source",
            "miniqmt_sim": MinuteDataSource.MINIQMT_REALTIME.value,
        }

    def _trading_calendar_policy(self) -> dict[str, str]:
        return {
            "service": type(self._trading_calendar_service).__name__,
            "source_of_truth": "TradingCalendarStatusService",
            "timezone": SCHEDULER_TZ_NAME,
            "non_trading_day": "skip_no_selection_plan_submit",
            "calendar_unavailable": "fail_closed_no_submit",
        }

    @staticmethod
    def _env_flag(name: str, *, default: bool) -> bool:
        raw = (os.getenv(name) or "").strip().lower()
        if not raw:
            return default
        return raw in {"1", "true", "yes", "y", "on"}


_background_trading_calendar_service = TradingCalendarStatusService()
simulation_lifecycle_background_scheduler = SimulationLifecycleBackgroundScheduler(
    lifecycle_scheduler=simulation_lifecycle_scheduler,
    trading_calendar_service=_background_trading_calendar_service,
    miniqmt_enabled=SimulationLifecycleBackgroundScheduler._env_flag("MINIQMT_ENABLED", default=False),
    localsim_replay_lifecycle_owner=build_localsim_replay_lifecycle_owner(
        lifecycle_scheduler=simulation_lifecycle_scheduler,
        trading_calendar_service=_background_trading_calendar_service,
    ),
)
