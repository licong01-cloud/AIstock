from __future__ import annotations

import os
import threading
import time as time_module
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
from types import MappingProxyType
from datetime import UTC, date, datetime, time as wall_time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Callable, Mapping
from uuid import uuid4
from zoneinfo import ZoneInfo

import psycopg2
import psycopg2.extras
import pytest
from pydantic import BaseModel, ConfigDict

import backend.services.simulation_runtime.bridges as simulation_bridges
import backend.services.miniqmt_execution_runtime.client as miniqmt_runtime_client

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    DepthQuantityUnit,
    MarketCode,
    PriceBasis,
    QuoteSourceMethod,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
)
from backend.execution_algos.adaptive_is.reasons import (
    QuoteContractError,
    QuoteContractReasonCode,
    quote_contract_error,
)
from backend.miniqmt_quote_contract_config import QuoteContractPolicy, QuoteIngressRuntimeConfig
from backend.services.paper_trading_v2.models import PaperPortfolio
from backend.services.paper_trading_v2.market_data import (
    DailyStStatus,
    MinuteDataSource,
    MinuteExecutionMarketInput,
    PreviousClose,
)
from backend.services.paper_trading_v2.broker.localsim import LocalSimBackend
from backend.services.paper_trading_v2.repository import InMemoryPaperTradingV2Repository
from backend.services.paper_trading_v2.broker.base import OrderHandle
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.models import (
    BUY_ORDER_TYPE,
    IntentSubmitStatus,
    OrderBatchStatus,
    OrderLedgerRecord,
    PositionLotRecord,
    SELL_ORDER_TYPE,
    STATUS_CANCELLED,
    STATUS_FILLED,
    STATUS_OPEN_LIKE,
    STATUS_PART_SUCC,
    STATUS_REJECTED,
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.qmt_strategy_ledger.order_service import ManagedOrderRequest, QmtManagedOrderService
from backend.services.qmt_strategy_ledger.reconciliation import QmtStrategyLedgerReconciliationService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.qmt_strategy_ledger.sync_service import QmtStrategyLedgerSyncService
from backend.services.selection_center.models import SelectionCandidate
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    InMemorySimulationRuntimeRepository,
    SimulationBindingApprovalState,
    SimulationBrokerBackend,
    SimulationLifecycleBackgroundScheduler,
    SimulationDailyRunStatus,
    SimulationDailyRun,
    SimulationLifecycleScheduler as ProductionSimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    SimulationSchedulerBindingResult,
    SimulationSchedulerRunOnceResult,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
    StrategyRuntimeReleaseService,
)
from backend.services.miniqmt_execution_runtime.kernel_product_runtime import (
    K6DProductParentStartResultV1,
    K6DProductPlanStartReceiptV1,
    K6DProductStartStatusV1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.simulation_runtime.lifecycle import (
    MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
    compute_schedule_windows,
)
from backend.services.simulation_runtime.miniqmt_quote_activation import MiniQMTKernelProductSyncError
from backend.services.simulation_runtime.models import (
    ExecutionPlan,
    LocalSimEconomicReceiptV1,
    LocalSimExecutionRuntimeStatus,
    LocalSimExecutionStateV1,
    LocalSimMarketMarkProvenance,
    LocalSimMarketMarkV1,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    SimulationReleaseBinding,
    canonical_json_sha256,
    miniqmt_kernel_runtime_id,
)
from backend.services.simulation_runtime.repository import (
    LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY,
    LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY,
    LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
    LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
    SimulationRuntimeRepository,
    _local_sim_economic_receipt_map,
    _local_sim_projection_outbox,
    _local_sim_projection_receipt_map,
    _merge_local_sim_economic_event,
    _merge_local_sim_projection_retryable,
    _merge_local_sim_projection_success,
)
from backend.services.simulation_runtime.decision import TradingRuleService
from backend.services.strategy_package.manifest import freeze_manifest
from backend.services.strategy_package.models import (
    AlphaCombinationPolicy,
    AlphaComponent,
    AlphaMode,
    BacktestSummary,
    FactorAsset,
    ModelAsset,
    PackageStatus,
    SourceType,
    StrategyPackageManifest,
    StrategyPackageSource,
)
from backend.services.strategy_package.live_inference import (
    PREFLIGHT_CHECK_MODEL_PARAMS,
    PREFLIGHT_CHECK_NAMES,
    PREFLIGHT_STATUS_BLOCKED,
    PREFLIGHT_STATUS_PASS,
    LiveInferencePreflightCheck,
    LiveInferencePreflightError,
    LiveInferencePreflightResult,
)
from backend.services.strategy_package.execution_policy import (
    LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID,
    LOCALSIM_TWAP_ONLY_REASON_CODE,
    compute_execution_policy_sha256,
    local_sim_twap_only_policy_snapshot,
    normalize_execution_policy_json,
)
from backend.services.trading_core.errors import (
    BrokerSubmitError,
    BrokerRejectedError,
    DataUnavailableError,
    InvalidStateTransitionError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderType, PositionLot
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTChildOrder,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTOperatorCommandStatus,
)
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2ControllerFactory,
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    BoundedNormalizedQuoteStore,
    NormalizedQuoteObservation,
    OrderingDisposition,
    QuoteEvaluationContext,
    QuoteEvaluationContextStore,
    QuoteSymbolContext,
    build_execution_clock_event,
    deterministic_market_data_id,
)
from backend.services.miniqmt_execution_runtime.quote_normalizer import (
    capture_raw_quote_frame,
    normalize_raw_quote_frame,
)


TRADE_DATE = date(2026, 5, 21)


class _TestK6DCoordinator:
    def __init__(self, plan: Any, start_calls: list[dict[str, Any]]) -> None:
        self._plan = plan
        self._start_calls = start_calls

    def start_execution_plan_v1(self, **values: Any) -> K6DProductPlanStartReceiptV1:
        self._start_calls.append(dict(values))
        results = tuple(
            K6DProductParentStartResultV1.create(
                plan_intent_ordinal=index,
                parent_intent_id=intent.intent_id,
                algo_instance_id=f"testalgo_{index}_{intent.intent_id}",
                event_id=f"testevent_{index}_{intent.intent_id}",
                ingress_receipt_sha256=hash_hex_v1(
                    "test_k6d_ingress_receipt_v1", {"parent_intent_id": intent.intent_id}
                ),
                start_status=K6DProductStartStatusV1.STARTED,
                terminal_reason_or_null=None,
            )
            for index, intent in enumerate(self._plan.intents, start=1)
        )
        return K6DProductPlanStartReceiptV1.create(
            runtime_id=values["runtime_id"],
            binding_id=values["binding_id"],
            execution_plan_id=values["execution_plan_id"],
            execution_plan_sha256=self._plan.plan_hash,
            product_route_receipt_sha256=hash_hex_v1(
                "test_k6d_product_route_receipt_v1",
                {"runtime_id": values["runtime_id"], "binding_id": values["binding_id"]},
            ),
            ordered_parent_results=results,
        )


class SimulationLifecycleScheduler(ProductionSimulationLifecycleScheduler):
    """Test-only product-root injection; production never has an in-memory fallback."""

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
        self._test_k6d_start_calls: list[dict[str, Any]] = []
        self._test_k6d_activation_calls: list[tuple[str, ...]] = []
        self.orchestrator.miniqmt_product_runtime_factory = lambda **factory_values: SimpleNamespace(
            coordinator=_TestK6DCoordinator(factory_values["plan"], self._test_k6d_start_calls),
            worker_incarnation_id="test_k6d_worker_incarnation",
            activate_hot_market_targets_v1=lambda algo_instance_ids: self._test_k6d_activation_calls.append(
                algo_instance_ids
            ),
        )


MINIQMT_B0_QUOTE_CONTROL = {
    "schema_version": "miniqmt_quote_control_binding_v1",
    "control_revision": "B0_QUOTE_V2",
}


def _with_b0_quote_policy(policy: dict[str, Any]) -> dict[str, Any]:
    benchmark_policy = {
        "benchmark_max_age_ms": 10_000,
        "arrival_forward_window_ms": 2_000,
        "clock_skew_tolerance_ms": 1_000,
        "benchmark_max_transport_latency_ms": 3_000,
        "policy_version": "miniqmt_execution_tca_benchmark_v1",
    }
    algo_config = dict(policy.get("algo_config") or {})
    tca = dict(algo_config.get("tca") or {})
    tca["benchmark_policy"] = benchmark_policy
    algo_config["tca"] = tca
    return {
        **policy,
        "algo_config": algo_config,
        "quote_contract": {
            "schema_version": "miniqmt_quote_contract_policy_v2",
            "control_revision": "B0_QUOTE_V2",
            "required_capabilities": [
                "CALENDAR",
                "DEPTH_UNIT_SHARES",
                "EXCHANGE_TIMESTAMP",
                "FIVE_LEVEL_DEPTH",
                "RAW_PRICE_BASIS",
                "TRADABILITY",
            ],
            "max_receive_age_ms": 20_000,
            "max_source_lag_ms": 20_000,
            "max_exchange_age_ms": 20_000,
            "max_negative_skew_ms": 1_000,
            "max_clock_age_divergence_ms": 1_000,
            "max_dependency_group_skew_ms": 20_000,
            "auction_mode": "OBSERVE_ONLY",
        },
        "quote_evidence": {
            "schema_version": "miniqmt_quote_evidence_policy_v1",
            "benchmark_policy_version": benchmark_policy["policy_version"],
            "mark_policy_version": "miniqmt_execution_tca_mark_selector_v1",
            "markout_max_lag_ms": 10_000,
        },
    }


@pytest.fixture(autouse=True)
def _deterministic_scheduler_now(monkeypatch: pytest.MonkeyPatch) -> None:
    import backend.services.simulation_runtime.lifecycle as lifecycle_module
    import backend.services.simulation_runtime.scheduler as scheduler_module

    def fixed_now() -> datetime:
        return datetime(2026, 5, 21, 10, 0, tzinfo=lifecycle_module.SCHEDULER_TZ)

    monkeypatch.setattr(lifecycle_module, "scheduler_now", fixed_now)
    monkeypatch.setattr(scheduler_module, "scheduler_now", fixed_now)


@pytest.fixture(autouse=True)
def _miniqmt_runtime_repository_test_only(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    """Keep scheduler unit tests off the production Postgres runtime repository."""
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_REPOSITORY", "jsonfile")
    monkeypatch.setenv("AISTOCK_MINIQMT_RUNTIME_JSONFILE_TEST_ONLY", "1")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(tmp_path / "miniqmt-runtime-state.json"))


def _release_and_bindings(
    *,
    qmt_only: bool = False,
    package_id: str = "pkg_scheduler",
    release_metadata: dict | None = None,
    execution_policy_json: dict[str, Any] | None = None,
    approval_state: SimulationBindingApprovalState = SimulationBindingApprovalState.SIM_VALIDATING,
):
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    if qmt_only:
        execution_policy_version_id = "vnpy_asset:SNIPER_MINIQMT"
        execution_policy_json = execution_policy_json or {"algo_code": "SNIPER_MINIQMT", "algo_config": {}}
    else:
        execution_policy_version_id = "exec_policy_v25_1_small_cap"
        execution_policy_json = execution_policy_json or {
            "algo_code": "V25_1_SMALL_CAP",
            "schedule_window": {"mode": "open_to_close"},
        }
    execution_policy_json = _with_b0_quote_policy(execution_policy_json)
    execution_policy_json = normalize_execution_policy_json(execution_policy_json)
    execution_policy_sha256 = compute_execution_policy_sha256(execution_policy_json)
    release = service.create_release(
        package_id=package_id,
        manifest_sha256="manifest_scheduler",
        runtime_profile_id="runtime_profile_scheduler",
        runtime_profile_version_id="runtime_profile_scheduler_v1",
        runtime_profile_sha256="runtime_profile_scheduler_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256,
        execution_policy_json=execution_policy_json,
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        release_metadata=release_metadata,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    qmt_binding = service.create_binding(
        strategy_id="strategy_qmt_scheduler",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="SchedulerQMT",
        order_remark_prefix="sched-qmt",
        miniqmt_quote_control=MINIQMT_B0_QUOTE_CONTROL,
        approval_state=approval_state,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    if qmt_only:
        return release, None, qmt_binding, repo
    local_binding = service.create_binding(
        strategy_id="strategy_local_scheduler",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=approval_state,
        created_by="unit-test",
        created_reason="scheduler test",
    )
    return release, local_binding, qmt_binding, repo


def _create_scheduler_release(
    repo: InMemorySimulationRuntimeRepository,
    *,
    package_id: str,
    manifest_sha256: str,
    release_metadata: dict | None = None,
):
    return StrategyRuntimeReleaseService(repository=repo).create_release(
        package_id=package_id,
        manifest_sha256=manifest_sha256,
        runtime_profile_id=f"runtime_profile_{package_id}",
        runtime_profile_version_id=f"runtime_profile_{package_id}_v1",
        runtime_profile_sha256=f"runtime_profile_{package_id}_hash",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256=f"exec_policy_{package_id}_hash",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256=f"tail_policy_{package_id}_hash",
        release_metadata=release_metadata,
        created_by="unit-test",
        created_reason="scheduler multi-release test",
    )


def _create_extra_binding(
    *,
    release,
    repo: InMemorySimulationRuntimeRepository,
    strategy_id: str,
    broker_backend: SimulationBrokerBackend,
    broker_account_id: str | None = None,
    strategy_name: str | None = None,
    order_remark_prefix: str | None = None,
):
    return StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id=strategy_id,
        release=release,
        broker_backend=broker_backend,
        capital_allocation=100_000,
        broker_account_id=broker_account_id,
        strategy_name=strategy_name,
        order_remark_prefix=order_remark_prefix,
        miniqmt_quote_control=(
            MINIQMT_B0_QUOTE_CONTROL if broker_backend is SimulationBrokerBackend.MINIQMT_SIM else None
        ),
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="multi strategy scheduler test",
    )


def _candidate_rows() -> list[SelectionCandidate]:
    return [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_quantity=1000,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        ),
        SelectionCandidate(
            symbol="688001.SH",
            score=0.98,
            rank=2,
            target_quantity=201,
            target_weight=0.04,
            reference_price=20.0,
            reason="daily_strategy_buy_or_retain",
        ),
    ]


def _evidence(
    release,
    *,
    candidates: list[SelectionCandidate],
    valid_no_candidate: bool = False,
    target_trade_date: date = TRADE_DATE,
    cutoff_date: date = date(2026, 5, 20),
) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": target_trade_date.isoformat(),
        "cutoff_date": cutoff_date.isoformat(),
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile_version_id": release.runtime_profile_version_id,
        "runtime_profile_hash": release.runtime_profile_sha256,
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "selected_candidates": [item.model_dump(mode="json") for item in candidates],
        "excluded_candidates": [],
        "valid_no_candidate": valid_no_candidate,
        "no_candidate_reason": "unit test no candidate day" if valid_no_candidate else None,
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=target_trade_date,
        cutoff_date=cutoff_date,
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=len(candidates),
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


class FakePackageRepository:
    def __init__(
        self,
        *,
        package_id: str,
        manifest_sha256: str,
        package_status: PackageStatus = PackageStatus.SELECTION_ENABLED,
    ) -> None:
        self.package_id = package_id
        self.manifest_sha256 = manifest_sha256
        self.package_status = package_status
        self.calls: list[str] = []

    def get(self, package_id: str) -> Any:
        self.calls.append(package_id)
        if package_id != self.package_id:
            raise DataUnavailableError("fake StrategyPackage does not exist", context={"package_id": package_id})
        return SimpleNamespace(
            package_id=self.package_id,
            manifest_sha256=self.manifest_sha256,
            package_status=self.package_status,
        )


class FakePackageMapRepository:
    def __init__(self, releases_by_package: dict[str, Any]) -> None:
        self.releases_by_package = dict(releases_by_package)
        self.calls: list[str] = []

    def get(self, package_id: str) -> Any:
        self.calls.append(package_id)
        release = self.releases_by_package.get(package_id)
        if release is None:
            raise DataUnavailableError("fake StrategyPackage does not exist", context={"package_id": package_id})
        return SimpleNamespace(
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
            package_status=PackageStatus.SELECTION_ENABLED,
        )


class FakeSelectionService:
    def __init__(
        self,
        release,
        *,
        candidates: list[SelectionCandidate] | None = None,
        valid_no_candidate: bool = False,
        package_status: PackageStatus = PackageStatus.SELECTION_ENABLED,
    ) -> None:
        self.release = release
        self.candidates = list(candidates or [])
        self.valid_no_candidate = valid_no_candidate
        self.calls: list[dict] = []
        self.package_repository = FakePackageRepository(
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
            package_status=package_status,
        )

    def run_selection(self, **kwargs):
        self.calls.append(kwargs)
        runtime_release = kwargs.get("runtime_release") or self.release
        target_trade_date = kwargs.get("trade_date") or TRADE_DATE
        evidence = _evidence(
            runtime_release,
            candidates=self.candidates,
            valid_no_candidate=self.valid_no_candidate,
            target_trade_date=target_trade_date,
        )
        no_candidate_reason = "unit test no candidate day" if self.valid_no_candidate else None
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}
            },
            package_results={self.release.package_id: self.candidates},
            aggregate_results=self.candidates,
            excluded_results={self.release.package_id: []},
            manifest_sha256_by_package={self.release.package_id: self.release.manifest_sha256},
            evidence_by_package={self.release.package_id: evidence},
            valid_no_candidate=self.valid_no_candidate,
            no_candidate_reason=no_candidate_reason,
        )


class PackageRoutingSelectionService:
    def __init__(
        self,
        releases_by_package: dict[str, Any],
        *,
        failing_package_id: str,
        exc: Exception | None = None,
    ) -> None:
        self.releases_by_package = dict(releases_by_package)
        self.failing_package_id = failing_package_id
        self.exc = exc or _live_inference_preflight_error(package_id=failing_package_id)
        self.calls: list[dict[str, Any]] = []
        self.package_repository = FakePackageMapRepository(self.releases_by_package)

    def run_selection(self, **kwargs):
        self.calls.append(kwargs)
        package_id = kwargs["package_ids"][0]
        if package_id == self.failing_package_id:
            raise self.exc
        release = self.releases_by_package[package_id]
        candidates = _candidate_rows()
        evidence = _evidence(release, candidates=candidates, target_trade_date=kwargs.get("trade_date") or TRADE_DATE)
        return StrategyPackageSelectionResult(
            runtime_config={
                "runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}
            },
            package_results={release.package_id: candidates},
            aggregate_results=candidates,
            excluded_results={release.package_id: []},
            manifest_sha256_by_package={release.package_id: release.manifest_sha256},
            evidence_by_package={release.package_id: evidence},
        )


def _live_inference_preflight_error(*, package_id: str) -> LiveInferencePreflightError:
    checks = [
        LiveInferencePreflightCheck(
            name=name,
            status=PREFLIGHT_STATUS_BLOCKED if name == PREFLIGHT_CHECK_MODEL_PARAMS else PREFLIGHT_STATUS_PASS,
            message=(
                "StrategyPackage model params.pkl references local model code that is missing"
                if name == PREFLIGHT_CHECK_MODEL_PARAMS
                else f"{name} passed"
            ),
            context={
                "reason_code": "strategy_package_model_code_missing",
                "package_id": package_id,
                "missing_relative_paths": ["model.py"],
            }
            if name == PREFLIGHT_CHECK_MODEL_PARAMS
            else {},
        )
        for name in PREFLIGHT_CHECK_NAMES
    ]
    preflight = LiveInferencePreflightResult(passed=False, checks=checks)
    return LiveInferencePreflightError(
        "live inference cold-start preflight failed: StrategyPackage model code missing",
        context={
            "source_type": "live_qe_model_inference_v1",
            "source_id": "exp_bad",
            "package_id": package_id,
            "phase": "preflight",
            "blocked_check": PREFLIGHT_CHECK_MODEL_PARAMS,
            "preflight": preflight.to_dict(),
        },
    )


class CountingContextProvider(StaticSimulationRunContextProvider):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.calls: list[str] = []

    def load_context(self, *, runtime_release, binding, trade_date):
        self.calls.append(binding.binding_id)
        return super().load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )


class SelectiveFailingContextProvider(StaticSimulationRunContextProvider):
    def __init__(self, *, failing_binding_id: str, exc_factory, **kwargs) -> None:
        super().__init__(**kwargs)
        self.failing_binding_id = failing_binding_id
        self.exc_factory = exc_factory
        self.calls: list[str] = []

    def load_context(self, *, runtime_release, binding, trade_date):
        self.calls.append(binding.binding_id)
        if binding.binding_id == self.failing_binding_id:
            raise self.exc_factory(binding, trade_date)
        return super().load_context(
            runtime_release=runtime_release,
            binding=binding,
            trade_date=trade_date,
        )


def _position_context(*, portfolio_id: str, local_broker=None, cash: float | None = 100_000) -> SimulationRunContext:
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions={
            "000001.SZ": PositionLot(
                portfolio_id=portfolio_id,
                symbol="000001.SZ",
                quantity=1000,
                available_quantity=1000,
                avg_cost=9.5,
                trade_date=date(2026, 5, 20),
            ),
            "000003.SZ": PositionLot(
                portfolio_id=portfolio_id,
                symbol="000003.SZ",
                quantity=77,
                available_quantity=77,
                avg_cost=8.0,
                trade_date=date(2026, 5, 20),
            ),
        },
        current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0},
        local_broker=local_broker,
        cash=cash,
    )


def _local_sim_execution_policy() -> dict[str, Any]:
    policy_json = normalize_execution_policy_json(
        {
            "algo_code": "TWAP",
            "algo_config": {
                "allow_partial_fill": True,
                "split_count": 1,
            },
        }
    )
    return {
        "policy_id": "exec_policy_twap",
        "policy_sha256": compute_execution_policy_sha256(policy_json),
        "policy_json": policy_json,
    }


def _canonical_local_sim_policy_for_test(policy: dict[str, Any]) -> dict[str, Any]:
    raw_policy_json = policy.get("policy_json")
    assert isinstance(raw_policy_json, dict)
    normalized = normalize_execution_policy_json(raw_policy_json)
    id_field = next(
        field
        for field in (
            "validated_execution_policy_id",
            "policy_version_id",
            "policy_id",
        )
        if field in policy
    )
    return {
        id_field: str(policy[id_field]),
        "policy_sha256": compute_execution_policy_sha256(normalized),
        "policy_json": normalized,
    }


def _local_sim_context_with_real_broker(
    *,
    portfolio_id: str,
    release: Any,
    cash: float = 100_000,
    positions: dict[str, PositionLot] | None = None,
    paper_repository: InMemoryPaperTradingV2Repository | None = None,
    execution_policy: dict[str, Any] | None = None,
) -> SimulationRunContext:
    manifest = _score_weighted_manifest(release)
    current_positions = dict(positions or {})
    policy = _canonical_local_sim_policy_for_test(execution_policy or _local_sim_execution_policy())
    broker = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=cash,
        initial_available_cash=cash,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        package_id=release.package_id,
        market_data_provider=FakeLocalSimMarketDataProvider(),
        execution_policy=policy,
        initial_positions=current_positions,
    )
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions=current_positions,
        current_prices={symbol: 10.0 for symbol in {"000001.SZ", "688001.SH", *current_positions}},
        top_k=1,
        execution_policy_payload=policy,
        local_broker=broker,
        paper_repository=paper_repository,
        cash=cash,
        market_data_source=MinuteDataSource.DB_HISTORICAL.value,
    )


def _local_sim_realtime_context_with_real_broker(
    *,
    portfolio_id: str,
    release: Any,
    paper_repository: InMemoryPaperTradingV2Repository,
    cash: float,
    positions: dict[str, PositionLot],
    market_data_provider: Any | None = None,
) -> SimulationRunContext:
    policy = _canonical_local_sim_policy_for_test(
        {
            "policy_id": "exec_policy_twap_streaming",
            "policy_sha256": "placeholder_replaced_by_test_helper",
            "policy_json": {"algo_code": "TWAP", "algo_config": {"allow_partial_fill": True, "split_count": 6}},
        }
    )
    broker = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=100_000,
        initial_available_cash=cash,
        data_source=MinuteDataSource.TDX_REALTIME,
        manifest=_score_weighted_manifest(release),
        package_id=release.package_id,
        market_data_provider=market_data_provider or FakeLocalSimMarketDataProvider(),
        execution_policy=policy,
        initial_positions=positions,
    )
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions=positions,
        current_prices={"000001.SZ": 10.1, "688001.SH": 10.1, **{symbol: 10.1 for symbol in positions}},
        top_k=1,
        execution_policy_payload=policy,
        local_broker=broker,
        paper_repository=paper_repository,
        cash=cash,
        market_data_source=MinuteDataSource.TDX_REALTIME.value,
    )


class FakeLocalSimBroker:
    def __init__(self) -> None:
        self.submitted = []

    def submit_order_intent(self, intent):
        self.submitted.append(intent)
        return OrderHandle(
            handle_id=f"local_{len(self.submitted)}",
            backend_id="local_sim",
            submitted_at=datetime.now(UTC),
            intent_id=intent.intent_id,
        )


class BoardLotRejectingLocalSimBroker(FakeLocalSimBroker):
    def submit_order_intent(self, intent):
        self.submitted.append(intent)
        raise BrokerRejectedError(
            "LocalSim ledger rejected the order",
            context={
                "intent_id": intent.intent_id,
                "symbol": intent.symbol,
                "quantity": intent.quantity,
                "cause": "fill quantity violates LocalSim board-lot rules; reason_code=LOCAL_SIM_BOARD_LOT_VIOLATION",
                "cause_code": "RISK_RULE_ERROR",
            },
        )


def _default_miniqmt_broker_quotes() -> dict[str, dict[str, Any]]:
    return {
        "000001.SZ": {
            "source": "MINIQMT_REALTIME.broker_quote",
            "price": 10.0,
            "ask_price_1": 10.0,
            "ask_volume_1": 5000,
            "bid_price_1": 10.0,
            "bid_volume_1": 5000,
        },
        "688001.SH": {
            "source": "MINIQMT_REALTIME.broker_quote",
            "price": 20.0,
            "ask_price_1": 20.0,
            "ask_volume_1": 5000,
            "bid_price_1": 20.0,
            "bid_volume_1": 5000,
        },
        "000003.SZ": {
            "source": "MINIQMT_REALTIME.broker_quote",
            "price": 8.0,
            "ask_price_1": 8.0,
            "ask_volume_1": 5000,
            "bid_price_1": 8.0,
            "bid_volume_1": 5000,
        },
    }


class FakeManagedOrderBroker:
    def __init__(
        self,
        order_ids: list[int] | None = None,
        positions: list[dict[str, Any]] | None = None,
        quotes: dict[str, dict[str, Any]] | None = None,
        connected: bool = True,
        connect_ok: bool = True,
        fail_next_place: bool = False,
        fail_order_query: bool = False,
        fail_trade_query: bool = False,
    ) -> None:
        self.order_ids = list(order_ids or [])
        self.positions = (
            list(positions) if positions is not None else [{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
        )
        self.quotes = dict(_default_miniqmt_broker_quotes() if quotes is None else quotes)
        self.connected = connected
        self.connect_ok = connect_ok
        self.fail_next_place = fail_next_place
        self.fail_order_query = fail_order_query
        self.fail_trade_query = fail_trade_query
        self.place_order_payloads = []
        self.full_tick_calls: list[list[str]] = []
        self.order_query_calls = 0
        self.trade_query_calls = 0

    def status(self):
        return {"connected": self.connected, "mode": "SIM", "provider": "fake"}

    def connect(self):
        self.connected = bool(self.connect_ok)
        return self.connected, "connected" if self.connected else "connect failed"

    def get_positions(self):
        return list(self.positions)

    def get_orders(self, cancelable_only: bool = False):
        self.order_query_calls += 1
        if self.fail_order_query:
            raise RuntimeError("simulated broker order snapshot unavailable")
        return []

    def get_trades(self):
        self.trade_query_calls += 1
        if self.fail_trade_query:
            raise RuntimeError("simulated broker trade snapshot unavailable")
        return []

    def get_full_tick(self, symbols):
        self.full_tick_calls.append(list(symbols))
        return {symbol: dict(self.quotes[symbol]) for symbol in symbols if symbol in self.quotes}

    def place_order(self, **kwargs):
        self.place_order_payloads.append(kwargs)
        if self.fail_next_place:
            self.fail_next_place = False
            self.connected = False
            raise RuntimeError("simulated miniQMT disconnect during submit")
        order_id = self.order_ids.pop(0) if self.order_ids else 900000000 + len(self.place_order_payloads)
        return order_id, "accepted" if order_id > 0 else "rejected by fake broker"

    def cancel_order(self, order_id: str):
        return True, f"cancelled {order_id}"


class HangingPlaceOrderBroker(FakeManagedOrderBroker):
    def __init__(self) -> None:
        super().__init__()
        self.place_started = threading.Event()
        self.release_place = threading.Event()

    def place_order(self, **kwargs):
        self.place_order_payloads.append(kwargs)
        self.place_started.set()
        self.release_place.wait(timeout=5.0)
        return 987654321, "accepted after release"


class HangingReconciliationService:
    def __init__(self) -> None:
        self.started = threading.Event()
        self.release = threading.Event()

    def reconcile_snapshot(self, **_kwargs):
        self.started.set()
        self.release.wait(timeout=5.0)
        raise RuntimeError("late reconcile result after watchdog release")


class MissingPlaceOrderQmtClient(FakeManagedOrderBroker):
    place_order = None


class FakeQmtSnapshotClient:
    def __init__(
        self,
        *,
        orders: list[dict[str, Any]] | None = None,
        trades: list[dict[str, Any]] | None = None,
        positions: list[dict[str, Any]] | None = None,
    ) -> None:
        self.calls: list[str] = []
        self._orders = list(orders or [])
        self._trades = list(trades or [])
        self._positions = list(positions or [])

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:
        self.calls.append(f"orders:{cancelable_only}")
        return list(self._orders)

    def get_trades(self) -> list[dict[str, Any]]:
        self.calls.append("trades")
        return list(self._trades)

    def get_positions(self) -> list[dict[str, Any]]:
        self.calls.append("positions")
        return list(self._positions)


class FailingQmtSnapshotClient(FakeQmtSnapshotClient):
    def __init__(self, *, error_message: str = "broker snapshot unavailable", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.error_message = error_message
        self.fail = False

    def get_orders(self, cancelable_only: bool = False) -> list[dict[str, Any]]:
        if self.fail:
            raise RuntimeError(self.error_message)
        return super().get_orders(cancelable_only=cancelable_only)


class _RealB0QuoteSupervisor:
    """In-process WHOLE_QUOTE_CALLBACK harness for scheduler integration tests."""

    def __init__(self, *, config: QuoteIngressRuntimeConfig) -> None:
        self.config = config
        self.normalized_store = BoundedNormalizedQuoteStore(max_symbols=config.max_symbols)
        self.context_store = QuoteEvaluationContextStore()
        self._observations: dict[str, NormalizedQuoteObservation] = {}
        self._sinks: dict[str, Any] = {}
        self._symbols_by_consumer: dict[str, tuple[str, ...]] = {}

    def stage(
        self,
        *,
        context: QuoteEvaluationContext,
        observations: Mapping[str, NormalizedQuoteObservation],
    ) -> None:
        self.context_store.publish(context)
        self._observations = dict(observations)
        admitted = {symbol for consumer_symbols in self._symbols_by_consumer.values() for symbol in consumer_symbols}
        self.normalized_store.replace_admitted(admitted)
        for consumer_id, symbols in self._symbols_by_consumer.items():
            sink = self._sinks.get(consumer_id)
            if sink is None:
                raise AssertionError(f"B0 test consumer lost observation sink: {consumer_id}")
            for symbol in symbols:
                observation = self._observations[symbol]
                self.normalized_store.accept(observation)
                self._deliver_observation(sink=sink, observation=observation)

    def register_observation_sink(self, *, consumer_id: str, sink: Any) -> None:
        if consumer_id in self._sinks:
            raise AssertionError(f"duplicate B0 test observation sink: {consumer_id}")
        self._sinks[consumer_id] = sink

    def unregister_observation_sink(self, *, consumer_id: str) -> None:
        self._sinks.pop(consumer_id, None)

    def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
        sink = self._sinks.get(consumer_id)
        if sink is None:
            raise AssertionError(f"B0 test consumer has no observation sink: {consumer_id}")
        exact_symbols = tuple(sorted({str(symbol).strip().upper() for symbol in symbols}))
        missing = [symbol for symbol in exact_symbols if symbol not in self._observations]
        if missing:
            raise AssertionError(f"B0 test callback observations missing symbols: {missing}")
        self._symbols_by_consumer[consumer_id] = exact_symbols
        admitted = {symbol for consumer_symbols in self._symbols_by_consumer.values() for symbol in consumer_symbols}
        self.normalized_store.replace_admitted(admitted)
        for symbol in exact_symbols:
            observation = self._observations[symbol]
            self.normalized_store.accept(observation)
            self._deliver_observation(sink=sink, observation=observation)

    def _deliver_observation(self, *, sink: Any, observation: NormalizedQuoteObservation) -> None:
        observation_context = self.context_store.snapshot()
        if observation_context is None or observation_context.context_id != observation.context_id:
            raise AssertionError(
                "B0 test callback observation lost its exact projection context: "
                f"observation={observation.context_id} "
                f"projection={getattr(observation_context, 'context_id', None)}"
            )
        sink(observation, observation_context)

    def release_consumer(self, *, consumer_id: str) -> None:
        self._symbols_by_consumer.pop(consumer_id, None)
        admitted = {symbol for consumer_symbols in self._symbols_by_consumer.values() for symbol in consumer_symbols}
        self.normalized_store.replace_admitted(admitted)

    def begin_lifecycle_epoch(self) -> None:
        return None

    def shutdown(self) -> None:
        self._sinks.clear()
        self._symbols_by_consumer.clear()

    def health(self) -> dict[str, object]:
        return {
            "status": "READY",
            "source_method": QuoteSourceMethod.WHOLE_QUOTE_CALLBACK.value,
            "consumer_count": len(self._symbols_by_consumer),
        }


class _TestKernelOutboxRepository:
    def __init__(self, commands: tuple[Mapping[str, Any], ...] = ()) -> None:
        self.commands = tuple(SimpleNamespace(**dict(command)) for command in commands)

    def list_recovery_outbox_commands(
        self,
        *,
        runtime_id: str,
        trade_date: date,
        statuses: tuple[str, ...],
        limit: int,
    ) -> tuple[SimpleNamespace, ...]:
        del trade_date
        accepted = set(statuses)
        return tuple(
            command for command in self.commands if command.runtime_id == runtime_id and str(command.status) in accepted
        )[:limit]

    def read_command_identity_chain(self, command_id: str) -> dict[str, Any]:
        command = next(command for command in self.commands if command.command_id == command_id)
        mapping = SimpleNamespace(
            runtime_id=command.runtime_id,
            command_id=command.command_id,
            mapping_id=command.mapping_id,
            parent_intent_id=command.parent_intent_id,
            deterministic_client_order_ref=command.deterministic_client_order_ref,
            order_remark=command.order_remark,
            broker_order_id=command.broker_order_id,
        )
        return {"outbox": command, "mapping": mapping}


class _RealB0TestActivation:
    """Publish exact quote context, then deliver genuine callback observations."""

    def __init__(self) -> None:
        self.config = QuoteIngressRuntimeConfig.from_mapping(
            {
                "MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": True,
                "MINIQMT_QUOTE_INGRESS_OWNER_MODE": "simulation_scheduler",
            }
        )
        self.supervisor = _RealB0QuoteSupervisor(config=self.config)
        self.controller_factory = B0QuoteV2ControllerFactory(
            supervisor=self.supervisor,
            config=self.config,
            data_session_key="lifecycle-scheduler-real-b0-callback",
        )
        self.quote_context_adapter = None
        self._continuity_generation = 0
        self.released_runtime_ids: list[str] = []
        self._failure_runtimes: dict[str, SimpleNamespace] = {}
        self._failure_attempts: dict[str, dict[str, int]] = {}

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        self.supervisor.begin_lifecycle_epoch()
        return self.health()

    def watchdog_tick(self) -> dict[str, object]:
        return self.health()

    def stage_failure_runtime(
        self,
        *,
        runtime_id: str,
        binding_id: str,
        trade_date: date,
        execution_plan_id: str | None = None,
        lifecycle_generation: int = 1,
        attempt_token: int = 1,
        outbox_commands: tuple[Mapping[str, Any], ...] = (),
    ) -> None:
        self._failure_runtimes[runtime_id] = SimpleNamespace(
            runtime_id=runtime_id,
            binding_id=binding_id,
            trade_date=trade_date,
            execution_plan_id=execution_plan_id,
            lifecycle_generation=lifecycle_generation,
            attempt_token=attempt_token,
            repository=_TestKernelOutboxRepository(outbox_commands),
        )
        self._failure_attempts[runtime_id] = {
            "lifecycle_generation": lifecycle_generation,
            "attempt_token": attempt_token,
        }

    def get_kernel_product_runtime(self, runtime_id: str) -> SimpleNamespace | None:
        return self._failure_runtimes.get(runtime_id)

    def release_kernel_product_runtime(self, runtime_id: str) -> None:
        if self._failure_runtimes.pop(runtime_id, None) is None:
            raise AssertionError(f"unknown staged KERNEL_V2 runtime: {runtime_id}")
        self._failure_attempts.pop(runtime_id, None)
        self.released_runtime_ids.append(runtime_id)

    def prepare_runtime_context(
        self,
        *,
        runtime_id: str,
        plan: Any,
        recovering_active: bool,
        clock_at_utc: datetime,
        clock_monotonic_ns: int,
    ) -> dict[str, object]:
        del recovering_active
        execution_policy = self._execution_policy(plan)
        policy = QuoteContractPolicy.from_execution_policy(execution_policy)
        quote_control = plan.plan_payload_json.get("quote_control")
        if not isinstance(quote_control, Mapping) or not isinstance(quote_control.get("assignments"), list):
            raise AssertionError("B0 scheduler integration plan has no exact quote-control assignments")
        revision_payload = quote_control.get("revision")
        if not isinstance(revision_payload, Mapping):
            raise AssertionError("B0 scheduler integration plan has no frozen quote-control revision")
        revision = B0QuoteV2RevisionV1.from_payload(revision_payload)
        assignments = {
            assignment.parent_intent_id: assignment
            for payload in quote_control["assignments"]
            if isinstance(payload, Mapping)
            for assignment in (ParentQuoteControlAssignmentV1.from_plan_payload(payload, revision=revision),)
        }
        if len(assignments) != len(quote_control["assignments"]):
            raise AssertionError("B0 scheduler integration plan assignments are malformed or duplicated")
        self.controller_factory.prepare_assignment_transition(
            runtime_id=runtime_id,
            assignments=assignments,
        )
        self._continuity_generation += 1
        generation = self._continuity_generation
        exact_symbols = tuple(sorted({intent.symbol for intent in plan.intents}))
        if not exact_symbols:
            raise AssertionError("B0 scheduler integration fixture requires parent symbols")
        segments = (
            SessionSegment(wall_time(9, 15), wall_time(9, 25)),
            SessionSegment(wall_time(9, 30), wall_time(11, 30)),
            SessionSegment(wall_time(13, 0), wall_time(14, 57)),
            SessionSegment(wall_time(14, 57), wall_time(15, 0)),
        )
        calendars = CalendarSnapshotSet(
            snapshot_set_id=f"calendar-set-{runtime_id}-{generation}",
            snapshot_by_market={
                market: CalendarSnapshot(
                    calendar_id=f"calendar-{market.value}-{runtime_id}-{generation}",
                    market=market,
                    trade_date=plan.target_trade_date,
                    timezone="Asia/Shanghai",
                    session_segments=segments,
                    effective_at_utc=clock_at_utc,
                    source_version="scheduler-real-b0-test-calendar-v1",
                )
                for market in MarketCode
            },
        )
        clock_domain_id = f"scheduler-real-b0-clock-{runtime_id}"
        clock = build_execution_clock_event(
            calendar_snapshot_set=calendars,
            clock_at_utc=clock_at_utc,
            clock_monotonic_ns=clock_monotonic_ns,
            clock_domain_id=clock_domain_id,
            source="simulation_lifecycle_scheduler_test_callback",
        )
        symbol_contexts: dict[str, QuoteSymbolContext] = {}
        observation_parts: dict[str, tuple[Any, Any, TradabilitySnapshot]] = {}
        local_clock = clock_at_utc.astimezone(ZoneInfo("Asia/Shanghai"))
        source_time = local_clock.strftime("%H%M%S") + "00"
        for sequence, symbol in enumerate(exact_symbols, start=1):
            market = self._market(symbol)
            price = Decimal(str(self._price(symbol)))
            tradability = TradabilitySnapshot(
                schema_version="adaptive_is_tradability_snapshot_v1",
                tradability_id=f"tradability-{runtime_id}-{generation}-{symbol}",
                symbol=symbol,
                market=market,
                board="MAIN",
                trade_date=plan.target_trade_date,
                price_basis=PriceBasis.RAW_CNY_PER_SHARE,
                pre_close=price,
                limit_up=(price * Decimal("1.10")).quantize(Decimal("0.01")),
                limit_down=(price * Decimal("0.90")).quantize(Decimal("0.01")),
                price_tick=Decimal("0.01"),
                lot_size=100,
                is_suspended=False,
                suspension_source="scheduler-real-b0-test",
                security_status="LISTED",
                openint_status=None,
                observed_at_utc=clock_at_utc,
                source="scheduler.real_b0_test.tradability",
                source_version="scheduler-real-b0-test-authority-v1",
                state=TradabilityState.TRADABLE,
            )
            symbol_contexts[symbol] = QuoteSymbolContext(
                symbol=symbol,
                board="MAIN",
                depth_quantity_unit=DepthQuantityUnit.SHARES,
                unit_evidence_version="xtdata-depth-unit-v1",
                tradability=tradability,
                product_type="EQUITY",
                product_type_proven_equity=True,
                authority_source_version="scheduler-real-b0-test-authority-v1",
            )
            frame = capture_raw_quote_frame(
                {
                    "time": source_time,
                    "lastPrice": str(price),
                    "preClose": str(price),
                    "bidPrice": [str(price - Decimal(index) / 100) for index in range(0, 5)],
                    "bidVol": [1000, 900, 800, 700, 600],
                    "askPrice": [str(price + Decimal(index) / 100) for index in range(0, 5)],
                    "askVol": [1000, 900, 800, 700, 600],
                    "stockStatus": "NORMAL",
                    "openint": "OPEN",
                },
                callback_symbol=symbol,
                source_session_id=f"scheduler-real-b0-session-{generation}",
                ingress_generation=generation,
                ingress_sequence=sequence,
                received_at_utc=clock_at_utc,
                received_monotonic_ns=clock_monotonic_ns + sequence,
                clock_domain_id=clock_domain_id,
                source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
            )
            quote = normalize_raw_quote_frame(
                frame,
                clock_trade_date=plan.target_trade_date,
                board="MAIN",
                depth_quantity_unit=DepthQuantityUnit.SHARES,
                unit_evidence_version="xtdata-depth-unit-v1",
                tradability=tradability,
            )
            observation_parts[symbol] = (frame, quote, tradability)
        context = QuoteEvaluationContext(
            calendar_snapshot_set=calendars,
            clock=clock,
            continuity_generation=generation,
            continuity_valid=True,
            policy=policy,
            symbols=symbol_contexts,
        )
        exact_observations = {
            symbol: NormalizedQuoteObservation(
                frame=frame,
                quote=quote,
                tradability=tradability,
                context_id=context.context_id,
                market_data_id=deterministic_market_data_id(
                    frame=frame,
                    quote=quote,
                    tradability=tradability,
                    calendar_snapshot_set=calendars,
                    policy=policy,
                ),
                ordering_disposition=OrderingDisposition.ACCEPTED,
            )
            for symbol, (frame, quote, tradability) in observation_parts.items()
        }
        self.supervisor.stage(context=context, observations=exact_observations)
        return {
            "runtime_id": runtime_id,
            "context_id": context.context_id,
            "policy_sha256": policy.policy_sha256,
            "symbol_count": len(exact_symbols),
            "source_method": QuoteSourceMethod.WHOLE_QUOTE_CALLBACK.value,
        }

    def health(self) -> dict[str, object]:
        return {
            "status": "READY",
            "production_ddl_gate": "test_only_not_production",
            "ingress": self.supervisor.health(),
            "kernel_product_runtimes": [
                {
                    "runtime_id": runtime_id,
                    "binding_id": runtime.binding_id,
                    "trade_date": runtime.trade_date.isoformat(),
                    "ingress_retry": {
                        "lifecycle_generation": runtime.lifecycle_generation,
                        "active_failure": {
                            "runtime_id": runtime_id,
                            "lifecycle_generation": runtime.lifecycle_generation,
                            "attempt_token": runtime.attempt_token,
                        },
                        "last_failure": {
                            "runtime_id": runtime_id,
                            "lifecycle_generation": runtime.lifecycle_generation,
                            "attempt_token": runtime.attempt_token,
                        },
                        "operations": {},
                    },
                }
                for runtime_id, runtime in sorted(self._failure_runtimes.items())
            ],
        }

    def shutdown(self) -> dict[str, object]:
        self.controller_factory.set_accept_new_assignments(False)
        self.supervisor.shutdown()
        return {"status": "STOPPED"}

    @staticmethod
    def _execution_policy(plan: Any) -> dict[str, Any]:
        container = plan.plan_payload_json.get("execution_policy")
        if not isinstance(container, Mapping):
            raise AssertionError("B0 scheduler integration plan has no execution policy")
        payload = container.get("payload")
        if not isinstance(payload, Mapping):
            raise AssertionError("B0 scheduler integration plan policy payload is missing")
        policy_json = payload.get("policy_json")
        return dict(policy_json) if isinstance(policy_json, Mapping) else dict(payload)

    @staticmethod
    def _market(symbol: str) -> MarketCode:
        suffix = symbol.rsplit(".", 1)[-1]
        try:
            return MarketCode(suffix)
        except ValueError as exc:
            raise AssertionError(f"unsupported B0 scheduler test market: {symbol}") from exc

    @staticmethod
    def _price(symbol: str) -> float:
        return {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}.get(symbol, 10.0)


class _PendingOnlyB0Controller:
    def __init__(self, runtime_id: str) -> None:
        self.runtime_id = runtime_id

    def lifecycle_tick(self, *, now_utc: datetime | None = None) -> None:
        del now_utc


class _PendingOnlyB0ControllerFactory:
    def __init__(self) -> None:
        self.controllers: dict[str, _PendingOnlyB0Controller] = {}
        self.recovering_active_by_runtime: dict[str, bool] = {}

    def assert_accepts_new_assignments(self) -> None:
        return None

    def get(self, runtime_id: str) -> _PendingOnlyB0Controller | None:
        return self.controllers.get(runtime_id)

    def create(
        self,
        *,
        runtime: Any,
        assignments: Any,
        symbols: Any,
        recovering_active: bool = False,
    ) -> _PendingOnlyB0Controller:
        del assignments, symbols
        controller = _PendingOnlyB0Controller(runtime.config.runtime_id)
        self.controllers[runtime.config.runtime_id] = controller
        self.recovering_active_by_runtime[runtime.config.runtime_id] = bool(recovering_active)
        return controller


class _PendingOnlyB0Activation:
    def __init__(self) -> None:
        self.controller_factory = _PendingOnlyB0ControllerFactory()
        self.quote_context_adapter = None

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        return {"status": "READY"}

    def watchdog_tick(self) -> dict[str, object]:
        return self.health()

    def prepare_runtime_context(self, **kwargs: Any) -> dict[str, object]:
        return {
            "runtime_id": kwargs["runtime_id"],
            "recovering_active": bool(kwargs["recovering_active"]),
            "source": "test_pending_real_tick_only",
        }

    def health(self) -> dict[str, object]:
        return {"status": "READY"}

    def shutdown(self) -> dict[str, object]:
        return {"status": "STOPPED"}


def _miniqmt_event_loop_test_scheduler(
    *,
    candidates: list[SelectionCandidate] | None = None,
    execution_policy_json: dict[str, Any] | None = None,
    real_callback: bool = False,
):
    effective_candidates = candidates or [
        *_candidate_rows(),
        SelectionCandidate(
            symbol="000003.SZ",
            score=0.50,
            rank=3,
            target_quantity=100,
            target_weight=0.01,
            reference_price=8.0,
            reason="retain_existing_position_for_tick_route_fixture",
        ),
    ]
    release, _, qmt_binding, repo = _release_and_bindings(
        qmt_only=True,
        execution_policy_json=execution_policy_json,
    )
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_shadow_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_shadow_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=effective_candidates),
        miniqmt_quote_ingress_activation=(_RealB0TestActivation() if real_callback else _PendingOnlyB0Activation()),  # type: ignore[arg-type]
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )
    return scheduler, repo, broker, qmt_binding


def _miniqmt_scheduler_with_ledger_context(
    *,
    cash: Decimal = Decimal("100000"),
    snapshot_client: FakeQmtSnapshotClient | None = None,
):
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=cash,
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_cross_day_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_cross_day_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = snapshot_client or FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )
    return scheduler, repo, broker, qmt_binding, qmt_repo, snapshot_client


def _miniqmt_two_strategy_scheduler():
    release, _, qmt_binding_a, repo = _release_and_bindings(qmt_only=True)
    qmt_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_qmt_scheduler_b",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id=qmt_binding_a.broker_account_id,
        strategy_name="SchedulerQMTB",
        order_remark_prefix="sched-qmt-b",
    )
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for binding in (qmt_binding_a, qmt_binding_b):
        qmt_repo.create_virtual_account(
            VirtualAccount(
                strategy_id=binding.strategy_id,
                strategy_name=binding.strategy_name or binding.strategy_id,
                display_name=binding.strategy_name or binding.strategy_id,
                account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
                mode="SIM",
                initial_cash=Decimal("100000"),
                cash=Decimal("100000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
    for binding, quantity in ((qmt_binding_a, 77), (qmt_binding_b, 123)):
        qmt_repo.create_position_lot(
            PositionLotRecord(
                lot_id=f"lot_scheduler_{binding.strategy_id}_000003",
                strategy_id=binding.strategy_id,
                symbol="000003.SZ",
                open_trade_id=f"trade_scheduler_{binding.strategy_id}_000003",
                open_date=date(2026, 5, 20),
                quantity=quantity,
                available_quantity=quantity,
                remaining_quantity=quantity,
                avg_cost=Decimal("8.00"),
                cost_amount=Decimal(str(quantity * 8)),
                account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
            )
        )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}])
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE])
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}])
    context_by_binding = {}
    for binding, quantity in ((qmt_binding_a, 77), (qmt_binding_b, 123)):
        context_by_binding[binding.binding_id] = SimulationRunContext(
            portfolio_id=f"portfolio_{binding.strategy_id}",
            current_positions={
                "000003.SZ": PositionLot(
                    portfolio_id=f"portfolio_{binding.strategy_id}",
                    symbol="000003.SZ",
                    quantity=quantity,
                    available_quantity=quantity,
                    avg_cost=8.0,
                    trade_date=date(2026, 5, 20),
                )
            },
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            managed_order_service=QmtManagedOrderService(
                repository=qmt_repo,
                broker=broker,  # type: ignore[arg-type]
                calendar_provider=calendar,
            ),
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=snapshot_client,
                account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=calendar,
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}],
        )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(by_binding_id=context_by_binding),
    )
    return scheduler, repo, broker, qmt_binding_a, qmt_binding_b


def _exact_kernel_outbox_command(
    *,
    runtime_id: str,
    suffix: str = "safe",
    status: str = "FAILED_TERMINAL",
    broker_called: bool | None = False,
    broker_order_id: str | None = None,
    order_remark: str | None = None,
) -> dict[str, Any]:
    return {
        "command_id": f"mqcmd_{suffix}",
        "runtime_id": runtime_id,
        "mapping_id": f"mqmap_{suffix}",
        "parent_intent_id": f"mqparent_{suffix}",
        "status": status,
        "broker_called": broker_called,
        "broker_order_id": broker_order_id,
        "deterministic_client_order_ref": f"mqref_{suffix}",
        "order_remark": order_remark or f"mqremark_{suffix}",
    }


def _current_kernel_runtime_identity(
    repo: InMemorySimulationRuntimeRepository,
    binding: SimulationReleaseBinding,
) -> tuple[SimulationDailyRun, ExecutionPlan, str]:
    run = repo.get_simulation_daily_run_by_key(
        strategy_id=binding.strategy_id,
        binding_id=binding.binding_id,
        trade_date=TRADE_DATE,
    )
    assert run is not None and run.execution_plan_id
    plan = repo.get_execution_plan(run.execution_plan_id)
    return (
        run,
        plan,
        miniqmt_kernel_runtime_id(
            plan_id=plan.plan_id,
            binding_id=binding.binding_id,
            trade_date=TRADE_DATE,
        ),
    )


def _stage_exact_kernel_failure(
    *,
    scheduler: SimulationLifecycleScheduler,
    repo: InMemorySimulationRuntimeRepository,
    binding: SimulationReleaseBinding,
    lifecycle_generation: int = 1,
    attempt_token: int = 1,
    outbox_commands: tuple[Mapping[str, Any], ...] | None = None,
    runtime_trade_date: date = TRADE_DATE,
) -> tuple[SimulationDailyRun, ExecutionPlan, str, dict[str, Any]]:
    run, plan, runtime_id = _current_kernel_runtime_identity(repo, binding)
    commands = outbox_commands
    if commands is None:
        commands = (_exact_kernel_outbox_command(runtime_id=runtime_id),)
    scheduler._miniqmt_quote_ingress_activation.stage_failure_runtime(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        trade_date=runtime_trade_date,
        execution_plan_id=plan.plan_id,
        lifecycle_generation=lifecycle_generation,
        attempt_token=attempt_token,
        outbox_commands=commands,
    )
    failure = {
        "runtime_id": runtime_id,
        "binding_id": binding.binding_id,
        "lifecycle_generation": lifecycle_generation,
        "attempt_token": attempt_token,
        "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_SYNC_FAILED",
        "exception_type": "RuntimeError",
        "exception_message": "injected exact current-plan callback failure",
    }
    return run, plan, runtime_id, failure


def _orphan_current_kernel_plan_run(
    *,
    repo: InMemorySimulationRuntimeRepository,
    run: SimulationDailyRun,
) -> SimulationDailyRun:
    identity_keys = {
        "schema_version",
        "strategy_id",
        "binding_id",
        "binding_hash",
        "release_id",
        "release_hash",
        "broker_backend",
        "trade_date",
        "created_by",
    }
    orphan = run.model_copy(
        update={
            "execution_plan_id": None,
            "execution_plan_hash": None,
            "status": SimulationDailyRunStatus.SIGNAL_GENERATING,
            "run_payload_json": {key: value for key, value in run.run_payload_json.items() if key in identity_keys},
        }
    )
    repo.daily_runs[run.run_id] = orphan
    return orphan


def _persist_exact_preplan_unknown(
    *,
    scheduler: SimulationLifecycleScheduler,
    repo: InMemorySimulationRuntimeRepository,
    binding: SimulationReleaseBinding,
    observed: datetime,
    outbox_commands: tuple[Mapping[str, Any], ...] | None = None,
) -> tuple[SimulationDailyRun, str]:
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original = {item.binding_id: item for item in planned.results}[binding.binding_id].run
    assert original is not None
    _run, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding,
        outbox_commands=outbox_commands,
    )
    _orphan_current_kernel_plan_run(repo=repo, run=original)

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = lambda: None
    failed_run = {item.binding_id: item for item in failed.results}[binding.binding_id].run
    assert failed_run is not None
    assert failed_run.execution_plan_id is None
    assert failed_run.run_payload_json["broker_side_effect_state"] == "UNKNOWN"
    return failed_run, runtime_id


def _runtime_store_contains_shadow_marker(path) -> bool:
    paths = [path, path.with_suffix(".jsonl")]
    return any(
        candidate.exists() and "SHADOW_RECONCILIATION" in candidate.read_text(encoding="utf-8") for candidate in paths
    )


def test_scheduler_miniqmt_uses_only_kernel_v2_product_root_and_is_restart_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME", "compiler")

    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "submit_event_loop_plan")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "drive_event_loop_ticks")
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    assert first.results[0].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert second.results[0].status == "REUSED_EXISTING_PLAN"
    assert len(scheduler._test_k6d_start_calls) == 1
    payload = repo.get_simulation_daily_run(first.results[0].run.run_id).run_payload_json
    assert payload["miniqmt_runtime_route"]["route"] == "KERNEL_V2"
    assert payload["miniqmt_runtime_route"]["quote_source"] == "B0_QUOTE_V2"
    assert payload["broker_called"] is False
    assert payload["submitted_intents"] == 0
    assert broker.place_order_payloads == []


def test_scheduler_miniqmt_product_root_failure_uses_durable_backoff_without_plan_rebuild() -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    product_root_calls = 0
    full_context_calls = 0
    existing_context_calls = 0
    retry_claim_calls = 0
    base_provider = scheduler.context_provider
    original_retry_claim = repo.claim_simulation_retry_attempt

    def count_retry_claim(**values: Any) -> Any:
        nonlocal retry_claim_calls
        retry_claim_calls += 1
        return original_retry_claim(**values)

    repo.claim_simulation_retry_attempt = count_retry_claim  # type: ignore[method-assign]

    class PhaseAwareProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            nonlocal full_context_calls
            full_context_calls += 1
            return base_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

        def load_existing_plan_context(self, *, runtime_release, binding, plan, trade_date, as_of_time):
            nonlocal existing_context_calls
            existing_context_calls += 1
            return base_provider.load_context(
                runtime_release=runtime_release,
                binding=binding,
                trade_date=trade_date,
            )

    scheduler.context_provider = PhaseAwareProvider()

    def fail_product_runtime(**_values: Any) -> Any:
        nonlocal product_root_calls
        product_root_calls += 1
        raise RuntimeError('column "release_hash" does not exist')

    scheduler.orchestrator.miniqmt_product_runtime_factory = fail_product_runtime
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(seconds=30),
    )
    third = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    assert first.results[0].status == "FAILED_RETRYABLE"
    assert second.results[0].status == "RETRY_BACKOFF"
    assert second.results[0].error is None
    assert second.results[0].lifecycle_diagnostic["reason_code"] == "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE"
    assert third.results[0].status == "FAILED_RETRYABLE"
    assert third.results[0].run is not None
    run = repo.get_simulation_daily_run(third.results[0].run.run_id)
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert run.run_payload_json["last_stage"] == "FAILED_RETRYABLE"
    assert run.run_payload_json["broker_called"] is False
    assert run.run_payload_json["submitted_intents"] == 0
    assert run.run_payload_json["failed_intents"] == 0
    assert run.run_payload_json["submit_failure"] == {
        "stage": "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        "outer_stage": "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        "type": "RuntimeError",
        "message": 'column "release_hash" does not exist',
        "context": None,
    }
    retry_control = run.run_payload_json["simulation_scheduler_retry_control_v1"]
    retry_entry = retry_control["entries"]["BINDING_FAILED_RETRYABLE"]
    assert retry_entry["failure_stage"] == "MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED"
    assert retry_entry["consecutive_failure_count"] == 2
    assert retry_entry["attempt_count"] == 2
    assert retry_entry["next_retry_at"] == (observed + timedelta(minutes=3)).astimezone(UTC).isoformat()
    assert product_root_calls == 2
    assert full_context_calls == 1
    assert existing_context_calls == 1
    assert retry_claim_calls == 1
    assert first.results[0].run.execution_plan_id == second.results[0].execution_plan.plan_id
    assert second.results[0].execution_plan.plan_id == third.results[0].run.execution_plan_id
    assert broker.place_order_payloads == []


def test_simulation_retry_control_is_hash_closed_bounded_and_single_claimed() -> None:
    scheduler, repo, _broker, _binding = _miniqmt_event_loop_test_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=observed,
    )
    run = planned.results[0].run
    assert run is not None

    first = repo.record_simulation_retry_failure(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="a" * 64,
        failure_fingerprint="b" * 64,
        failure_stage="MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        error={
            "type": "RuntimeConfigInvalidError",
            "message": "exchange-session authority drift",
            "reason_code": "MINIQMT_EXCHANGE_SESSION_AUTHORITY_DRIFT",
            "context": {"plan_id": run.execution_plan_id},
        },
        as_of_time=observed,
        base_delay_seconds=60,
        max_delay_seconds=3600,
    )
    entry = first.run_payload_json["simulation_scheduler_retry_control_v1"]["entries"]["BINDING_FAILED_RETRYABLE"]
    assert entry["consecutive_failure_count"] == 1
    assert entry["attempt_count"] == 1

    early = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="a" * 64,
        as_of_time=observed + timedelta(seconds=30),
        lease_seconds=600,
    )
    assert early.should_execute is False
    assert early.reason == "backoff_not_due"

    due = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="a" * 64,
        as_of_time=observed + timedelta(seconds=60),
        lease_seconds=600,
    )
    assert due.should_execute is True
    assert due.reason == "retry_claimed"
    duplicate = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="a" * 64,
        as_of_time=observed + timedelta(seconds=60),
        lease_seconds=600,
    )
    assert duplicate.should_execute is False
    assert duplicate.reason == "attempt_in_progress"

    changed = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="c" * 64,
        as_of_time=observed + timedelta(seconds=61),
        lease_seconds=600,
    )
    assert changed.should_execute is True
    assert changed.reason == "source_changed"
    assert "simulation_scheduler_retry_control_v1" not in changed.run.run_payload_json

    corrupted = repo.record_simulation_retry_failure(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="c" * 64,
        failure_fingerprint="d" * 64,
        failure_stage="MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        error={
            "type": "RuntimeConfigInvalidError",
            "message": "exchange-session authority drift",
            "reason_code": "MINIQMT_EXCHANGE_SESSION_AUTHORITY_DRIFT",
            "context": {},
        },
        as_of_time=observed + timedelta(seconds=62),
        base_delay_seconds=60,
        max_delay_seconds=3600,
        expected_claim_token=changed.claim_token,
    )
    forged_payload = deepcopy(corrupted.run_payload_json)
    forged_payload["simulation_scheduler_retry_control_v1"]["entries"]["BINDING_FAILED_RETRYABLE"]["next_retry_at"] = (
        (observed + timedelta(days=7)).astimezone(UTC).isoformat()
    )
    repo.daily_runs[run.run_id] = corrupted.model_copy(update={"run_payload_json": forged_payload})
    with pytest.raises(InvalidStateTransitionError, match="retry entry hash drifted"):
        repo.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="BINDING_FAILED_RETRYABLE",
            source_fingerprint="c" * 64,
            as_of_time=observed + timedelta(seconds=63),
            lease_seconds=600,
        )


def test_simulation_retry_first_and_source_changed_attempts_are_durably_single_claimed() -> None:
    scheduler, repo, _broker, _binding = _miniqmt_event_loop_test_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=observed,
    )
    run = planned.results[0].run
    assert run is not None

    first = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_FIRST_CLAIM",
        source_fingerprint="a" * 64,
        as_of_time=observed,
        lease_seconds=600,
    )
    duplicate_first = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_FIRST_CLAIM",
        source_fingerprint="a" * 64,
        as_of_time=observed,
        lease_seconds=600,
    )
    assert first.should_execute is True
    assert first.claim_token is not None
    assert duplicate_first.should_execute is False
    assert duplicate_first.reason == "attempt_in_progress"
    assert duplicate_first.claim_token == first.claim_token
    with pytest.raises(InvalidStateTransitionError) as missing_record_token:
        repo.record_simulation_retry_failure(
            run_id=run.run_id,
            retry_key="RECOVERY:TEST_FIRST_CLAIM",
            source_fingerprint="a" * 64,
            failure_fingerprint="f" * 64,
            failure_stage="TEST_FIRST_CLAIM",
            error={"type": "RuntimeError", "message": "missing token", "reason_code": None, "context": {}},
            as_of_time=observed + timedelta(seconds=1),
            base_delay_seconds=60,
            max_delay_seconds=3600,
        )
    assert missing_record_token.value.context["reason_code"] == ("SIMULATION_SCHEDULER_RETRY_CLAIM_TOKEN_REQUIRED")
    with pytest.raises(InvalidStateTransitionError) as missing_clear_token:
        repo.clear_simulation_retry_control(
            run_id=run.run_id,
            retry_key="RECOVERY:TEST_FIRST_CLAIM",
        )
    assert missing_clear_token.value.context["reason_code"] == "SIMULATION_SCHEDULER_RETRY_CLAIM_TOKEN_REQUIRED"

    failed = repo.record_simulation_retry_failure(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_SOURCE_CHANGE",
        source_fingerprint="b" * 64,
        failure_fingerprint="c" * 64,
        failure_stage="TEST_SOURCE_CHANGE",
        error={"type": "RuntimeError", "message": "old source", "reason_code": None, "context": {}},
        as_of_time=observed,
        base_delay_seconds=60,
        max_delay_seconds=3600,
    )
    assert failed.run_id == run.run_id
    changed = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_SOURCE_CHANGE",
        source_fingerprint="d" * 64,
        as_of_time=observed + timedelta(seconds=1),
        lease_seconds=600,
    )
    duplicate_changed = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_SOURCE_CHANGE",
        source_fingerprint="d" * 64,
        as_of_time=observed + timedelta(seconds=1),
        lease_seconds=600,
    )
    assert changed.should_execute is True
    assert changed.reason == "source_changed"
    assert changed.claim_token is not None
    assert duplicate_changed.should_execute is False
    assert duplicate_changed.reason == "attempt_in_progress"
    assert duplicate_changed.claim_token == changed.claim_token

    successor = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_SOURCE_CHANGE",
        source_fingerprint="d" * 64,
        as_of_time=observed + timedelta(seconds=602),
        lease_seconds=600,
    )
    assert successor.should_execute is True
    assert successor.reason == "initial_claim_recovered"
    assert successor.claim_token is not None
    assert successor.claim_token != changed.claim_token
    with pytest.raises(InvalidStateTransitionError) as stale_exc:
        repo.record_simulation_retry_failure(
            run_id=run.run_id,
            retry_key="RECOVERY:TEST_SOURCE_CHANGE",
            source_fingerprint="d" * 64,
            failure_fingerprint="e" * 64,
            failure_stage="TEST_SOURCE_CHANGE",
            error={"type": "RuntimeError", "message": "stale writer", "reason_code": None, "context": {}},
            as_of_time=observed + timedelta(seconds=603),
            base_delay_seconds=60,
            max_delay_seconds=3600,
            expected_claim_token=changed.claim_token,
        )
    assert stale_exc.value.context["reason_code"] == "SIMULATION_SCHEDULER_RETRY_CLAIM_STALE_WRITER"
    with pytest.raises(InvalidStateTransitionError) as stale_clear:
        repo.clear_simulation_retry_control(
            run_id=run.run_id,
            retry_key="RECOVERY:TEST_SOURCE_CHANGE",
            expected_claim_token=changed.claim_token,
        )
    assert stale_clear.value.context["reason_code"] == "SIMULATION_SCHEDULER_RETRY_CLAIM_STALE_WRITER"
    recorded = repo.record_simulation_retry_failure(
        run_id=run.run_id,
        retry_key="RECOVERY:TEST_SOURCE_CHANGE",
        source_fingerprint="d" * 64,
        failure_fingerprint="e" * 64,
        failure_stage="TEST_SOURCE_CHANGE",
        error={"type": "RuntimeError", "message": "current writer", "reason_code": None, "context": {}},
        as_of_time=observed + timedelta(seconds=604),
        base_delay_seconds=60,
        max_delay_seconds=3600,
        expected_claim_token=successor.claim_token,
    )
    assert (
        "RECOVERY:TEST_SOURCE_CHANGE" not in recorded.run_payload_json["simulation_scheduler_retry_claims_v1"]["claims"]
    )


def test_simulation_retry_initial_claim_strict_readback_rejects_hash_drift() -> None:
    scheduler, repo, _broker, _binding = _miniqmt_event_loop_test_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=observed,
    )
    run = planned.results[0].run
    assert run is not None
    claimed = repo.claim_simulation_retry_attempt(
        run_id=run.run_id,
        retry_key="RECOVERY:CLAIM_HASH",
        source_fingerprint="a" * 64,
        as_of_time=observed,
        lease_seconds=600,
    )
    forged_payload = deepcopy(claimed.run.run_payload_json)
    forged_payload["simulation_scheduler_retry_claims_v1"]["claims"]["RECOVERY:CLAIM_HASH"]["lease_until"] = (
        (observed + timedelta(days=1)).astimezone(UTC).isoformat()
    )
    repo.daily_runs[run.run_id] = claimed.run.model_copy(update={"run_payload_json": forged_payload})

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="RECOVERY:CLAIM_HASH",
            source_fingerprint="a" * 64,
            as_of_time=observed + timedelta(seconds=1),
            lease_seconds=600,
        )
    assert exc_info.value.context["reason_code"] == "SIMULATION_SCHEDULER_RETRY_CLAIM_HASH_DRIFT"


@pytest.mark.parametrize(
    ("corruption", "expected_reason"),
    (
        ("envelope_type", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("schema_version", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("entries_type", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("entry_fields", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("entry_schema", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("entry_identity", "SIMULATION_SCHEDULER_RETRY_CONTROL_IDENTITY_CONFLICT"),
        ("source_hash", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("counter", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("timestamp_format", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("timestamp_naive", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("timeline", "SIMULATION_SCHEDULER_RETRY_CONTROL_TIMELINE_INVALID"),
        ("lease_half", "SIMULATION_SCHEDULER_RETRY_CONTROL_TIMELINE_INVALID"),
        ("last_error", "SIMULATION_SCHEDULER_RETRY_CONTROL_SCHEMA_INVALID"),
        ("control_hash", "SIMULATION_SCHEDULER_RETRY_CONTROL_HASH_DRIFT"),
    ),
)
def test_simulation_retry_control_strict_readback_rejects_malformed_carriers(
    corruption: str,
    expected_reason: str,
) -> None:
    scheduler, repo, _broker, _binding = _miniqmt_event_loop_test_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=observed,
    )
    run = planned.results[0].run
    assert run is not None
    recorded = repo.record_simulation_retry_failure(
        run_id=run.run_id,
        retry_key="BINDING_FAILED_RETRYABLE",
        source_fingerprint="a" * 64,
        failure_fingerprint="b" * 64,
        failure_stage="MINIQMT_KERNEL_V2_PRODUCT_ROOT_BUILD_FAILED",
        error={"type": "RuntimeError", "message": "authority drift", "reason_code": None, "context": {}},
        as_of_time=observed,
        base_delay_seconds=60,
        max_delay_seconds=3600,
    )
    payload = deepcopy(recorded.run_payload_json)
    control = payload["simulation_scheduler_retry_control_v1"]
    entry = control["entries"]["BINDING_FAILED_RETRYABLE"]
    if corruption == "envelope_type":
        payload["simulation_scheduler_retry_control_v1"] = []
    elif corruption == "schema_version":
        control["schema_version"] = "simulation_scheduler_retry_control_v0"
    elif corruption == "entries_type":
        control["entries"] = []
    elif corruption == "entry_fields":
        entry.pop("failure_stage")
    elif corruption == "entry_schema":
        entry["schema_version"] = "simulation_scheduler_retry_entry_v0"
    elif corruption == "entry_identity":
        entry["retry_key"] = "OTHER_RETRY"
    elif corruption == "source_hash":
        entry["source_fingerprint"] = "invalid"
    elif corruption == "counter":
        entry["consecutive_failure_count"] = False
    elif corruption == "timestamp_format":
        entry["first_failed_at"] = "not-a-timestamp"
    elif corruption == "timestamp_naive":
        entry["first_failed_at"] = "2026-05-22T10:00:00"
    elif corruption == "timeline":
        entry["next_retry_at"] = (observed - timedelta(seconds=1)).astimezone(UTC).isoformat()
    elif corruption == "lease_half":
        entry["last_attempt_at"] = observed.astimezone(UTC).isoformat()
    elif corruption == "last_error":
        entry["last_error"] = []
    else:
        control["control_sha256"] = "f" * 64
    repo.daily_runs[run.run_id] = recorded.model_copy(update={"run_payload_json": payload})

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="BINDING_FAILED_RETRYABLE",
            source_fingerprint="a" * 64,
            as_of_time=observed + timedelta(seconds=30),
            lease_seconds=600,
        )
    assert exc_info.value.context["reason_code"] == expected_reason


def test_localsim_pre_trade_blocked_replan_uses_same_durable_retry_contract() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_blocked_retry")}
        ),
    )
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=observed,
    )
    plan = planned.results[0].execution_plan
    run = repo.update_simulation_daily_run(
        planned.results[0].run.run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.SUCCEEDED.value,
            "broker_called": False,
            "pre_trade_blocked_order_generation": {
                "schema_version": "pre_trade_blocked_order_generation_v1",
                "plan_id": plan.plan_id,
                "blocked_intent_ids": [],
                "reason_codes": ["NO_TRADABLE_REALTIME_QUOTE"],
            },
        },
    )
    first = scheduler._finalize_binding_retry_result(  # noqa: SLF001
        result=SimulationSchedulerBindingResult(
            binding_id=local_binding.binding_id,
            strategy_id=local_binding.strategy_id,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            status=SimulationDailyRunStatus.SUCCEEDED.value,
            run=run,
            execution_plan=plan,
            data_source=MinuteDataSource.TDX_REALTIME.value,
        ),
        as_of_time=observed,
    )
    retry_entry = first.run.run_payload_json["simulation_scheduler_retry_control_v1"]["entries"][
        "BINDING_FAILED_RETRYABLE"
    ]
    assert retry_entry["failure_stage"] == "LOCAL_SIM_PRE_TRADE_BLOCKED_REPLAN"
    assert retry_entry["next_retry_at"] == (observed + timedelta(minutes=1)).astimezone(UTC).isoformat()

    _, deferred, claim_token, source_fingerprint = scheduler._claim_binding_retry_or_defer(  # noqa: SLF001
        binding=local_binding,
        run=first.run,
        plan=plan,
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        submit=True,
        as_of_time=observed + timedelta(seconds=30),
    )
    assert deferred is not None
    assert claim_token is None
    assert source_fingerprint is None
    assert deferred.status == "RETRY_BACKOFF"
    assert deferred.error is None
    assert deferred.lifecycle_diagnostic["reason_code"] == "SIMULATION_BINDING_RETRY_BACKOFF_NOT_DUE"


def test_simulation_retry_failure_evidence_normalizes_unsupported_context_before_hashing() -> None:
    scheduler, repo, _broker, _binding = _miniqmt_event_loop_test_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=observed,
    )
    run = planned.results[0].run
    assert run is not None

    class UnsupportedContextValue:
        pass

    error = {
        "type": "RuntimeError",
        "message": "durable authority drift",
        "reason_code": "SIMULATION_TEST_AUTHORITY_DRIFT",
        "context": {"unsupported": UnsupportedContextValue(), "unordered": {3, 1, 2}},
    }
    first = scheduler._record_simulation_retry_failure_evidence(  # noqa: SLF001
        run=run,
        retry_key="BINDING_FAILED_RETRYABLE",
        failure_stage="TEST_FAILURE_STAGE",
        error=error,
        as_of_time=observed,
    )
    second = scheduler._record_simulation_retry_failure_evidence(  # noqa: SLF001
        run=first,
        retry_key="BINDING_FAILED_RETRYABLE",
        failure_stage="TEST_FAILURE_STAGE",
        error=error,
        as_of_time=observed + timedelta(minutes=1),
    )
    entry = second.run_payload_json["simulation_scheduler_retry_control_v1"]["entries"]["BINDING_FAILED_RETRYABLE"]
    assert entry["consecutive_failure_count"] == 2
    assert entry["last_error"]["context"]["unordered"] == [1, 2, 3]
    assert entry["last_error"]["context"]["unsupported"] == {
        "schema_version": "simulation_scheduler_retry_unsupported_evidence_v1",
        "type": (
            "test_lifecycle_scheduler."
            "test_simulation_retry_failure_evidence_normalizes_unsupported_context_before_hashing."
            "<locals>.UnsupportedContextValue"
        ),
    }
    assert repo.get_simulation_daily_run(run.run_id) == second


@pytest.mark.parametrize(
    ("failure_mode", "expected_stage", "expected_type"),
    (
        (
            "coordinator_missing",
            "MINIQMT_KERNEL_V2_PRODUCT_ROOT_UNAVAILABLE",
            "BrokerUnavailableError",
        ),
        (
            "worker_incarnation_missing",
            "MINIQMT_KERNEL_V2_WORKER_INCARNATION_MISSING",
            "RuntimeConfigInvalidError",
        ),
    ),
)
def test_scheduler_miniqmt_invalid_product_root_is_persisted_before_broker(
    failure_mode: str,
    expected_stage: str,
    expected_type: str,
) -> None:
    scheduler, repo, broker, _binding = _miniqmt_event_loop_test_scheduler()
    if failure_mode == "coordinator_missing":
        product_runtime = SimpleNamespace(coordinator=None, worker_incarnation_id="worker_k6d")
    else:
        product_runtime = SimpleNamespace(
            coordinator=SimpleNamespace(start_execution_plan_v1=lambda **_values: None),
            worker_incarnation_id="",
        )
    scheduler.orchestrator.miniqmt_product_runtime_factory = lambda **_values: product_runtime
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )

    assert result.results[0].status == "FAILED_RETRYABLE"
    assert result.results[0].run is not None
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert run.run_payload_json["broker_called"] is False
    assert run.run_payload_json["submitted_intents"] == 0
    assert run.run_payload_json["failed_intents"] == 0
    failure = run.run_payload_json["submit_failure"]
    assert failure["stage"] == expected_stage
    assert failure["outer_stage"] == expected_stage
    assert failure["type"] == expected_type
    assert broker.place_order_payloads == []


def test_scheduler_miniqmt_kernel_v2_keeps_two_bindings_independent() -> None:
    scheduler, _repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )

    assert {item.binding_id for item in result.results} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in result.results} == {"MINIQMT_KERNEL_V2_ACTIVE"}
    assert {item["binding_id"] for item in scheduler._test_k6d_start_calls} == {
        binding_a.binding_id,
        binding_b.binding_id,
    }
    assert broker.place_order_payloads == []


def test_scheduler_kernel_product_tick_failure_isolated_to_owning_binding() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    activation = scheduler._miniqmt_quote_ingress_activation
    original, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    by_binding = {item.binding_id: item for item in result.results}
    assert by_binding[binding_a.binding_id].status == original.status.value
    assert by_binding[binding_a.binding_id].error["context"]["reason_code"] == (
        "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED"
    )
    assert by_binding[binding_a.binding_id].error["context"]["broker_side_effect_state"] == "UNKNOWN"
    assert "broker_called" not in by_binding[binding_a.binding_id].error["context"]
    failed_run = repo.get_simulation_daily_run(by_binding[binding_a.binding_id].run.run_id)
    assert failed_run.run_payload_json["broker_side_effect_state"] == "UNKNOWN"
    assert "broker_called" not in failed_run.run_payload_json
    assert "submitted_intents" not in failed_run.run_payload_json
    assert "failed_intents" not in failed_run.run_payload_json
    assert failed_run.run_payload_json["pre_run_failure"]["broker_side_effect_state"] == "UNKNOWN"
    assert "reconcile broker and durable outbox state" in failed_run.run_payload_json["pre_run_failure"]["next_action"]
    assert scheduler._run_has_broker_side_effect_evidence(failed_run) is True
    assert by_binding[binding_b.binding_id].status == "REUSED_EXISTING_PLAN"
    assert failed_run.execution_plan_id == original.execution_plan_id
    assert runtime_id in failed_run.run_payload_json["pre_run_failure"]["context"]["ordered_failures"][0]["runtime_id"]
    assert first_by_binding[binding_b.binding_id].run.run_id == by_binding[binding_b.binding_id].run.run_id
    assert broker.place_order_payloads == []


def test_scheduler_unmatched_kernel_product_tick_failure_is_not_silently_successful() -> None:
    scheduler, _repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError(
            (
                {
                    "runtime_id": "runtime_not_in_current_binding_page",
                    "binding_id": "simbind_not_in_current_binding_page",
                    "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_SYNC_FAILED",
                    "exception_type": "RuntimeError",
                    "exception_message": "injected failure outside the current binding page",
                },
            )
        )

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    # Every current peer is attempted before the bounded unmatched aggregate is
    # surfaced.  A stale/limit-excluded activation failure must not turn this
    # scheduler tick into failed_count=0.
    assert {item.binding_id for item in result.results[:2]} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in result.results[:2]} == {"MINIQMT_KERNEL_V2_ACTIVE"}
    assert result.failed_count == 1
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    assert unmatched.error["type"] == "MiniQMTKernelProductSyncError"
    assert unmatched.error["context"]["reason_code"] == "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_UNMATCHED"
    assert unmatched.error["context"]["failure_count"] == 1
    assert unmatched.error["context"]["ordered_failures"][0]["runtime_id"] == ("runtime_not_in_current_binding_page")
    assert unmatched.error["context"]["ordered_failures"][0]["binding_id"] == ("simbind_not_in_current_binding_page")
    assert broker.place_order_payloads == []


def test_scheduler_shared_supervisor_failure_attempts_all_peers_before_blocking_receipt() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError(
            (
                {
                    "runtime_id": None,
                    "binding_id": None,
                    "operation": "SUPERVISOR_WATCHDOG",
                    "lifecycle_generation": None,
                    "reason_code": "MINIQMT_SHARED_QUOTE_SUPERVISOR_WATCHDOG_FAILED",
                    "broker_side_effect_state": "UNKNOWN",
                    "exception_type": "RuntimeError",
                    "exception_message": "injected shared-supervisor owner failure",
                },
            )
        )

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert {item.binding_id for item in result.results[:2]} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in result.results[:2]} == {"MINIQMT_KERNEL_V2_ACTIVE"}
    assert result.failed_count == 1
    synthetic = result.results[-1]
    assert synthetic.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    failure = synthetic.error["context"]["ordered_failures"][0]
    assert failure["runtime_id"] is None
    assert failure["binding_id"] is None
    assert failure["scheduler_match_state"] == "GLOBAL_SHARED_OWNER_FAILURE"
    assert len(repo.list_simulation_daily_runs(trade_date=TRADE_DATE, limit=10)) == 2
    assert broker.place_order_payloads == []


def test_scheduler_unmatched_failure_fingerprint_closes_over_omitted_tail() -> None:
    shared = tuple(
        {
            "runtime_id": f"runtime_bounded_{index}",
            "binding_id": f"binding_bounded_{index}",
            "reason_code": "MINIQMT_K6_PRODUCT_CALLBACK_SYNC_FAILED",
            "exception_type": "RuntimeError",
            "exception_message": "bounded failure",
        }
        for index in range(100)
    )
    left = ProductionSimulationLifecycleScheduler._unmatched_kernel_product_failure_result(
        failures=(
            *shared,
            {
                "runtime_id": "runtime_tail_left",
                "binding_id": "binding_tail_left",
                "reason_code": "TAIL_LEFT",
            },
        ),
        data_source="DB_HISTORICAL",
    )
    right = ProductionSimulationLifecycleScheduler._unmatched_kernel_product_failure_result(
        failures=(
            *shared,
            {
                "runtime_id": "runtime_tail_right",
                "binding_id": "binding_tail_right",
                "reason_code": "TAIL_RIGHT",
            },
        ),
        data_source="DB_HISTORICAL",
    )

    assert left is not None and right is not None
    left_context = left.error["context"]
    right_context = right.error["context"]
    assert len(left_context["ordered_failures"]) == 100
    assert left_context["truncated_failure_count"] == 1
    assert left_context["omitted_failures_sha256"] != right_context["omitted_failures_sha256"]
    assert left_context["failure_fingerprint"] != right_context["failure_fingerprint"]


def test_scheduler_expired_kernel_product_failure_is_aggregated_after_current_peers() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    _run, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        runtime_trade_date=TRADE_DATE - timedelta(days=1),
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    current = result.results[:-1]
    assert {item.binding_id for item in current} == {binding_a.binding_id, binding_b.binding_id}
    assert {item.status for item in current} == {"REUSED_EXISTING_PLAN"}
    assert result.failed_count == 1
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    failure = unmatched.error["context"]["ordered_failures"][0]
    assert failure["runtime_id"] == runtime_id
    assert failure["binding_id"] == binding_a.binding_id
    assert failure["scheduler_match_state"] == "RUNTIME_TRADE_DATE_STALE"
    current_run = repo.get_simulation_daily_run_by_key(
        strategy_id=binding_a.strategy_id,
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
    )
    assert current_run.execution_plan_id
    assert "pre_run_failure" not in current_run.run_payload_json
    assert broker.place_order_payloads == []


def test_scheduler_unknown_preplan_failure_reconciles_before_automatic_plan_retry() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original = {item.binding_id: item for item in planned.results}[binding_a.binding_id].run
    _run, _plan, runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
    )
    _orphan_current_kernel_plan_run(repo=repo, run=original)

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    failed_run = repo.get_simulation_daily_run(first_by_binding[binding_a.binding_id].run.run_id)
    assert failed_run.execution_plan_id is None
    assert failed_run.run_payload_json["broker_side_effect_state"] == "UNKNOWN"

    trace: list[str] = []
    original_selection = scheduler.selection_service.run_selection
    activation = scheduler._miniqmt_quote_ingress_activation
    original_release = activation.release_kernel_product_runtime
    delattr(activation._failure_runtimes[runtime_id], "execution_plan_id")

    def traced_release(runtime_to_release: str) -> None:
        trace.append("runtime_release")
        original_release(runtime_to_release)

    activation.release_kernel_product_runtime = traced_release

    def traced_selection(**kwargs: Any):
        trace.append("selection")
        return original_selection(**kwargs)

    scheduler.selection_service.run_selection = traced_selection
    context = scheduler.context_provider._by_binding_id[binding_a.binding_id]
    original_sync = context.qmt_sync_service

    class TracedSync:
        def sync_snapshot(self):
            trace.append("automatic_reconcile_sync")
            return original_sync.sync_snapshot()

    scheduler.context_provider._by_binding_id[binding_a.binding_id] = replace(
        context,
        qmt_sync_service=TracedSync(),
    )
    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = lambda: None

    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    second_by_binding = {item.binding_id: item for item in second.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)

    assert trace[:2] == ["runtime_release", "automatic_reconcile_sync"]
    assert second_by_binding[binding_a.binding_id].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert second_by_binding[binding_b.binding_id].error is None
    assert latest.execution_plan_id
    proof = latest.run_payload_json["miniqmt_preplan_unknown_reconciliation"]
    assert proof["status"] == "NO_BROKER_SIDE_EFFECT"
    assert proof["runtime_id"] == runtime_id
    assert proof["broker_side_effect_count"] == 0
    assert proof["open_order_count"] == 0
    assert proof["runtime_release_status"] == "RELEASED"
    assert proof["automatic"] is True
    assert proof["replacement_plan_created"] is True
    assert proof["replacement_plan_id"] == latest.execution_plan_id
    assert "broker_side_effect_state" not in latest.run_payload_json
    assert "pre_run_failure" not in latest.run_payload_json
    assert broker.place_order_payloads == []


def test_scheduler_unknown_preplan_broker_side_effect_is_terminalized_without_replacement_plan() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original = {item.binding_id: item for item in planned.results}[binding_a.binding_id].run
    run, plan, runtime_id = _current_kernel_runtime_identity(repo, binding_a)
    order_remark = "sched-qmt-a-unknown-preplan"
    outbox_commands = (
        _exact_kernel_outbox_command(
            runtime_id=runtime_id,
            suffix="accepted",
            status="ACKED",
            broker_called=True,
            broker_order_id="900099991",
            order_remark=order_remark,
        ),
    )
    _run, _plan, _runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        outbox_commands=outbox_commands,
    )
    assert run.execution_plan_id == plan.plan_id
    _orphan_current_kernel_plan_run(repo=repo, run=original)

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    failed_run = repo.get_simulation_daily_run(first_by_binding[binding_a.binding_id].run.run_id)
    context = scheduler.context_provider._by_binding_id[binding_a.binding_id]
    context.qmt_ledger_repository.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id="intent_unknown_preplan_broker_fact",
            strategy_id=binding_a.strategy_id,
            strategy_name=binding_a.strategy_name or binding_a.strategy_id,
            qmt_order_id="900099991",
            symbol="000001.SZ",
            order_type=BUY_ORDER_TYPE,
            order_volume=100,
            traded_volume=100,
            order_status=STATUS_FILLED,
            account_id=binding_a.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            price_type=11,
            price=Decimal("10.00"),
            traded_price=Decimal("10.00"),
            status_msg="terminal broker fact from the failed runtime",
            order_remark=order_remark,
        )
    )
    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = lambda: None

    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    second_by_binding = {item.binding_id: item for item in second.results}
    terminal = repo.get_simulation_daily_run(failed_run.run_id)

    assert second_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert second_by_binding[binding_b.binding_id].error is None
    assert terminal.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert terminal.execution_plan_id is None
    proof = terminal.run_payload_json["miniqmt_preplan_unknown_reconciliation"]
    assert proof["status"] == "BROKER_SIDE_EFFECT_RECONCILED_TERMINAL"
    assert proof["runtime_id"] == runtime_id
    assert proof["broker_side_effect_count"] == 1
    assert proof["open_order_count"] == 0
    assert proof["replacement_plan_created"] is False
    assert terminal.run_payload_json["broker_called"] is True
    assert scheduler._miniqmt_quote_ingress_activation.released_runtime_ids == [runtime_id]
    assert broker.place_order_payloads == []

    repeated = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2, seconds=30),
    )
    repeated_by_binding = {item.binding_id: item for item in repeated.results}
    assert repeated_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert repo.get_simulation_daily_run(failed_run.run_id).execution_plan_id is None
    assert scheduler._miniqmt_quote_ingress_activation.released_runtime_ids == [runtime_id]
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_fresh_process_closes_absent_runtime_from_durable_authority() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )
    fresh_activation = _RealB0TestActivation()
    scheduler._miniqmt_quote_ingress_activation = fresh_activation
    scheduler._b0_quote_v2_controller_factory = fresh_activation.controller_factory
    scheduler.orchestrator.b0_quote_v2_controller_factory = fresh_activation.controller_factory
    scheduler._miniqmt_quote_context_adapter = fresh_activation.quote_context_adapter

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    recovered_by_binding = {item.binding_id: item for item in recovered.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)

    assert recovered_by_binding[binding_a.binding_id].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert recovered_by_binding[binding_b.binding_id].error is None
    assert latest.execution_plan_id
    assert latest.run_payload_json["miniqmt_preplan_unknown_runtime_release"] == {
        **latest.run_payload_json["miniqmt_preplan_unknown_runtime_release"],
        "status": "ALREADY_ABSENT",
        "runtime_id": runtime_id,
        "process_local_runtime_present": False,
    }
    assert fresh_activation.released_runtime_ids == []
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_release_persist_crash_retries_as_already_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )
    activation = scheduler._miniqmt_quote_ingress_activation
    original_update = repo.update_simulation_daily_run
    crashed = False

    def crash_after_release(run_id: str, **kwargs: Any):
        nonlocal crashed
        payload_patch = kwargs.get("payload_patch")
        if (
            not crashed
            and isinstance(payload_patch, dict)
            and "miniqmt_preplan_unknown_runtime_release" in payload_patch
        ):
            crashed = True
            raise RuntimeError("injected crash after process-local release before durable receipt")
        return original_update(run_id, **kwargs)

    monkeypatch.setattr(repo, "update_simulation_daily_run", crash_after_release)
    first_retry = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    first_by_binding = {item.binding_id: item for item in first_retry.results}
    assert first_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert first_by_binding[binding_b.binding_id].error is None
    assert activation.released_runtime_ids == [runtime_id]
    assert activation.get_kernel_product_runtime(runtime_id) is None

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=4),
    )
    recovered_by_binding = {item.binding_id: item for item in recovered.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered_by_binding[binding_a.binding_id].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert latest.run_payload_json["miniqmt_preplan_unknown_runtime_release"]["status"] == "ALREADY_ABSENT"
    assert activation.released_runtime_ids == [runtime_id]
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_never_releases_current_successor_attempt() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )
    activation = scheduler._miniqmt_quote_ingress_activation
    failure = failed_run.run_payload_json["miniqmt_preplan_unknown_failure"]
    authority = failure["runtime_authorities"][0]
    activation.stage_failure_runtime(
        runtime_id=runtime_id,
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
        execution_plan_id=authority["execution_plan_id"],
        lifecycle_generation=authority["lifecycle_generation"] + 1,
        attempt_token=authority["attempt_token"] + 1,
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    result_by_binding = {item.binding_id: item for item in result.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)
    assert result_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result_by_binding[binding_b.binding_id].error is None
    assert latest.execution_plan_id is None
    assert activation.get_kernel_product_runtime(runtime_id) is not None
    assert activation.released_runtime_ids == []
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_generic_reconcile_failure_isolated_from_peer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, _runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )

    def fail_reconcile(**_kwargs: Any):
        raise RuntimeError("injected generic reconciliation failure")

    monkeypatch.setattr(scheduler, "_reconcile_after_submit_with_timeout", fail_reconcile)
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    result_by_binding = {item.binding_id: item for item in result.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)
    assert result_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result_by_binding[binding_b.binding_id].error is None
    assert latest.execution_plan_id is None
    assert latest.run_payload_json["broker_side_effect_state"] == "UNKNOWN"
    assert broker.place_order_payloads == []


@pytest.mark.parametrize(
    ("wrong_field", "wrong_value", "expected_conflict"),
    [
        ("account_id", "QMT_WRONG_ACCOUNT", "sync_account_id_conflict"),
        ("trade_date", "2026-05-20", "sync_trade_date_conflict"),
    ],
)
def test_scheduler_preplan_unknown_wrong_sync_identity_never_authorizes_replacement(
    wrong_field: str,
    wrong_value: str,
    expected_conflict: str,
) -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, _runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )
    context = scheduler.context_provider._by_binding_id[binding_a.binding_id]
    original_sync = context.qmt_sync_service

    class WrongIdentitySync:
        def sync_snapshot(self):
            payload = original_sync.sync_snapshot().to_dict()
            payload[wrong_field] = wrong_value

            class MutatedSummary:
                def to_dict(self):
                    return dict(payload)

            return MutatedSummary()

    scheduler.context_provider._by_binding_id[binding_a.binding_id] = replace(
        context,
        qmt_sync_service=WrongIdentitySync(),
    )
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    result_by_binding = {item.binding_id: item for item in result.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)
    proof = latest.run_payload_json["miniqmt_preplan_unknown_reconciliation"]
    assert result_by_binding[binding_a.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result_by_binding[binding_b.binding_id].error is None
    assert latest.execution_plan_id is None
    assert proof["status"] == "RECONCILIATION_PENDING"
    assert expected_conflict in proof["sync_conflicts"]
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_dispatching_outbox_stays_pending_and_honors_retry_backoff() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    _run, _plan, runtime_id = _current_kernel_runtime_identity(repo, binding_a)
    dispatching = (
        _exact_kernel_outbox_command(
            runtime_id=runtime_id,
            suffix="dispatching",
            status="DISPATCHING",
            broker_called=None,
        ),
    )
    run = repo.get_simulation_daily_run_by_key(
        strategy_id=binding_a.strategy_id,
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
    )
    assert run is not None
    scheduler._miniqmt_quote_ingress_activation._failure_runtimes.clear()
    repo.daily_runs.clear()
    repo.daily_run_key_index.clear()
    # Recreate the same exact fixture once, now with an ambiguous durable command.
    failed_run, _runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
        outbox_commands=dispatching,
    )
    context = scheduler.context_provider._by_binding_id[binding_a.binding_id]
    original_sync = context.qmt_sync_service
    sync_calls = 0

    class CountingSync:
        def sync_snapshot(self):
            nonlocal sync_calls
            sync_calls += 1
            return original_sync.sync_snapshot()

    scheduler.context_provider._by_binding_id[binding_a.binding_id] = replace(
        context,
        qmt_sync_service=CountingSync(),
    )
    first_retry = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    assert {item.binding_id: item for item in first_retry.results}[binding_a.binding_id].status == (
        SimulationDailyRunStatus.FAILED_RETRYABLE.value
    )
    proof = repo.get_simulation_daily_run(failed_run.run_id).run_payload_json["miniqmt_preplan_unknown_reconciliation"]
    assert proof["status"] == "RECONCILIATION_PENDING"
    assert "outbox_outcome_ambiguous" in proof["sync_conflicts"]
    assert sync_calls == 2

    early = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2, seconds=30),
    )
    assert {item.binding_id: item for item in early.results}[binding_a.binding_id].status == "RETRY_BACKOFF"
    assert sync_calls == 2
    assert broker.place_order_payloads == []


def test_scheduler_preplan_unknown_foreign_order_does_not_terminalize_exact_runtime() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    failed_run, _runtime_id = _persist_exact_preplan_unknown(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        observed=observed,
    )
    context = scheduler.context_provider._by_binding_id[binding_a.binding_id]
    context.qmt_ledger_repository.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id="intent_foreign_runtime",
            strategy_id=binding_a.strategy_id,
            strategy_name=binding_a.strategy_name or binding_a.strategy_id,
            qmt_order_id="900088888",
            symbol="000001.SZ",
            order_type=BUY_ORDER_TYPE,
            order_volume=100,
            traded_volume=100,
            order_status=STATUS_FILLED,
            account_id=binding_a.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            order_remark="foreign-runtime-order",
        )
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=2),
    )
    recovered_by_binding = {item.binding_id: item for item in recovered.results}
    latest = repo.get_simulation_daily_run(failed_run.run_id)
    proof = latest.run_payload_json["miniqmt_preplan_unknown_reconciliation"]
    assert recovered_by_binding[binding_a.binding_id].status == "MINIQMT_KERNEL_V2_ACTIVE"
    assert recovered_by_binding[binding_b.binding_id].error is None
    assert proof["status"] == "NO_BROKER_SIDE_EFFECT"
    assert proof["exact_broker_authority"]["foreign_order_count"] == 1
    assert proof["exact_broker_authority"]["exact_broker_side_effect_count"] == 0
    assert broker.place_order_payloads == []


def test_scheduler_foreign_same_binding_runtime_does_not_pollute_existing_intraday_plan() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    original = repo.get_simulation_daily_run(first_by_binding[binding_a.binding_id].run.run_id)
    assert original.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert original.execution_plan_id
    assert original.run_payload_json["broker_called"] is False
    scheduler._miniqmt_quote_ingress_activation.stage_failure_runtime(
        runtime_id="runtime_failed_existing_tick",
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError(
            (
                {
                    "runtime_id": "runtime_failed_existing_tick",
                    "binding_id": binding_a.binding_id,
                    "reason_code": "MINIQMT_K6_PRODUCT_SCHEDULER_TICK_FAILED",
                    "broker_side_effect_state": "UNKNOWN",
                    "exception_type": "RuntimeError",
                    "exception_message": "injected failure after plan publication",
                },
            )
        )

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    second_by_binding = {item.binding_id: item for item in second.results}
    latest = repo.get_simulation_daily_run(original.run_id)

    assert second_by_binding[binding_a.binding_id].error is None
    assert latest.status == original.status
    assert latest.execution_plan_id == original.execution_plan_id
    assert latest.execution_plan_hash == original.execution_plan_hash
    assert latest.run_payload_json == original.run_payload_json
    assert second_by_binding[binding_b.binding_id].error is None
    assert repo.get_simulation_daily_run(first_by_binding[binding_b.binding_id].run.run_id).status == (
        SimulationDailyRunStatus.INTRADAY_RUNNING
    )
    unmatched = second.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    failure = unmatched.error["context"]["ordered_failures"][0]
    expected_runtime_id = miniqmt_kernel_runtime_id(
        plan_id=original.execution_plan_id,
        binding_id=binding_a.binding_id,
        trade_date=TRADE_DATE,
    )
    assert failure["runtime_id"] == "runtime_failed_existing_tick"
    assert failure["scheduler_match_state"] == "RUNTIME_NOT_CURRENT_PLAN_OWNER"
    assert failure["scheduler_expected_runtime_id"] == expected_runtime_id
    assert broker.place_order_payloads == []


@pytest.mark.parametrize(
    ("runtime_generation", "runtime_attempt", "failure_generation", "failure_attempt", "expected_state"),
    [
        (2, 1, 1, 1, "RUNTIME_LIFECYCLE_GENERATION_STALE"),
        (1, 2, 1, 1, "RUNTIME_ATTEMPT_TOKEN_STALE"),
    ],
)
def test_scheduler_stale_kernel_attempt_never_pollutes_or_releases_current_plan(
    runtime_generation: int,
    runtime_attempt: int,
    failure_generation: int,
    failure_attempt: int,
    expected_state: str,
) -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original = {item.binding_id: item for item in first.results}[binding_a.binding_id].run
    assert original is not None
    _run, _plan, runtime_id, failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
        lifecycle_generation=runtime_generation,
        attempt_token=runtime_attempt,
    )
    failure["lifecycle_generation"] = failure_generation
    failure["attempt_token"] = failure_attempt

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    latest = repo.get_simulation_daily_run(original.run_id)
    unmatched = result.results[-1]
    assert unmatched.status == "MINIQMT_KERNEL_V2_UNMATCHED_FAILURE"
    assert unmatched.error["context"]["ordered_failures"][0]["scheduler_match_state"] == expected_state
    assert latest == original
    assert scheduler._miniqmt_quote_ingress_activation.released_runtime_ids == []
    assert {item.binding_id: item for item in result.results}[binding_b.binding_id].error is None
    assert runtime_id not in scheduler._miniqmt_quote_ingress_activation.released_runtime_ids
    assert broker.place_order_payloads == []


def test_scheduler_matched_failure_persistence_is_bounded_and_hashes_omitted_tail() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    original, _plan, _runtime_id, base_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
    )
    failures = tuple({**base_failure, "exception_message": f"bounded exact failure {index}"} for index in range(101))

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError(failures)

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    result_by_binding = {item.binding_id: item for item in result.results}
    latest = repo.get_simulation_daily_run(original.run_id)
    evidence = latest.run_payload_json["pre_run_failure"]["context"]
    assert result_by_binding[binding_a.binding_id].error is not None
    assert result_by_binding[binding_b.binding_id].error is None
    assert evidence["failure_count"] == 101
    assert len(evidence["ordered_failures"]) == 100
    assert evidence["truncated_failure_count"] == 1
    assert evidence["omitted_failures_sha256"]
    assert evidence["all_failures_sha256"]
    assert broker.place_order_payloads == []


def test_scheduler_unknown_preserves_existing_positive_broker_side_effect_facts() -> None:
    scheduler, repo, broker, binding_a, binding_b = _miniqmt_two_strategy_scheduler()
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed,
    )
    first_by_binding = {item.binding_id: item for item in first.results}
    original = repo.update_simulation_daily_run(
        first_by_binding[binding_a.binding_id].run.run_id,
        payload_patch={"broker_called": True, "submitted_intents": 3, "failed_intents": 1},
    )
    _run, _plan, _runtime_id, exact_failure = _stage_exact_kernel_failure(
        scheduler=scheduler,
        repo=repo,
        binding=binding_a,
    )

    def failing_watchdog() -> None:
        raise MiniQMTKernelProductSyncError((exact_failure,))

    scheduler._miniqmt_quote_ingress_activation.watchdog_tick = failing_watchdog
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )
    second_by_binding = {item.binding_id: item for item in second.results}
    latest = repo.get_simulation_daily_run(original.run_id)

    assert second_by_binding[binding_a.binding_id].error["context"]["broker_side_effect_state"] == "UNKNOWN"
    assert latest.status == original.status
    assert latest.execution_plan_id == original.execution_plan_id
    assert latest.execution_plan_hash == original.execution_plan_hash
    assert latest.run_payload_json["broker_side_effect_state"] == "UNKNOWN"
    assert latest.run_payload_json["broker_called"] is True
    assert latest.run_payload_json["submitted_intents"] == 3
    assert latest.run_payload_json["failed_intents"] == 1
    assert second_by_binding[binding_b.binding_id].error is None
    assert broker.place_order_payloads == []


def _legacy_scheduler_miniqmt_sim_ignores_retired_env_and_always_routes_to_event_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler()
    runtime_store = tmp_path / "miniqmt-retired-route.json"
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME", "compiler")
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    def _b_submit_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("MiniQMT SIM must not route through retired B compiler submit_plan")

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "submit_plan", _b_submit_must_not_run)

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 0
    assert submitted.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["qmt_batch_result"]["pending"] == len(
        submitted.results[0].execution_plan.intents
    )
    assert "miniqmt_shadow_reconciliation" not in latest_run.run_payload_json
    assert not _runtime_store_contains_shadow_marker(runtime_store)
    assert broker.place_order_payloads == []
    assert qmt_binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM


def _legacy_scheduler_miniqmt_direct_sim_event_loop_routes_to_a_without_shadow_gate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler()
    runtime_store = tmp_path / "miniqmt-direct-event-loop.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))
    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.0,
                "ask_price_1": 20.0,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.0,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 8.0,
                "bid_volume_1": 5000,
            },
        }
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    assert planned.results[0].execution_plan is not None
    assert not _runtime_store_contains_shadow_marker(runtime_store)

    def _b_submit_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("direct SIM event_loop scope must not route through B compiler submit_plan")

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "submit_plan", _b_submit_must_not_run)
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert submitted.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    assert latest_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest_run.run_payload_json["broker_called"] is False
    qmt_result = latest_run.run_payload_json["qmt_batch_result"]
    assert qmt_result["runtime_evidence"]["source"] == "simulation_runtime_event_loop_submit"
    assert qmt_result["pending"] == len(submitted.results[0].execution_plan.intents)
    assert not _runtime_store_contains_shadow_marker(runtime_store)
    assert broker.place_order_payloads == []


def test_miniqmt_shadow_bridge_api_is_removed_from_scheduler_path() -> None:
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliations")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "run_shadow_reconciliation")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "_shadow_scenarios")


def _legacy_scheduler_miniqmt_event_loop_scope_routes_to_a_runtime_with_broker_quote(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler()
    runtime_store = tmp_path / "miniqmt-d4-event-loop.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))
    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.0,
                "ask_price_1": 20.0,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.0,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 8.0,
                "bid_volume_1": 5000,
            },
        }
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    assert planned.results[0].execution_plan is not None

    def _b_submit_must_not_run(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("EVENT_LOOP SIM scope must not route through B compiler submit_plan")

    monkeypatch.setattr(simulation_bridges.MiniQMTExecutionBridge, "submit_plan", _b_submit_must_not_run)
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert submitted.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    assert latest_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest_run.run_payload_json["broker_called"] is False
    qmt_result = latest_run.run_payload_json["qmt_batch_result"]
    assert qmt_result["runtime_evidence"]["source"] == "simulation_runtime_event_loop_submit"
    assert qmt_result["pending"] == len(submitted.results[0].execution_plan.intents)
    assert broker.place_order_payloads == []


def test_miniqmt_event_loop_bridge_product_methods_are_physically_retired() -> None:
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "submit_event_loop_plan")
    assert not hasattr(simulation_bridges.MiniQMTExecutionBridge, "drive_event_loop_ticks")
    assert not hasattr(SimulationLifecycleScheduler, "_drive_miniqmt_event_loop_ticks")


def test_miniqmt_compiler_submit_route_rejects_loudly() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    bridge = simulation_bridges.MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=FakeManagedOrderBroker()),  # type: ignore[arg-type]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=FakeManagedOrderBroker()),  # type: ignore[arg-type]
                    qmt_ledger_repository=qmt_repo,
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        bridge.submit_plan(
            plan=planned.results[0].execution_plan,
            binding=qmt_binding,
            mode="SIM",
            price_by_symbol={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_SIM_COMPILER_ROUTE_RETIRED"
    assert exc_info.value.context["stage"] == "MINIQMT_COMPILER_SUBMIT_REJECTED"
    assert exc_info.value.context["broker_called"] is False


def test_scheduler_local_sim_bindings_do_not_emit_miniqmt_reconciliation_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_shared",
                    release=release,
                    paper_repository=paper_repo,
                )
            }
        ),
    )
    runtime_store = tmp_path / "miniqmt-retired-reconciliation-local.json"
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 1
    assert submitted.results[0].status == "SUBMITTED"
    assert "miniqmt_shadow_reconciliation" not in submitted.results[0].run.run_payload_json
    assert not _runtime_store_contains_shadow_marker(runtime_store)
    assert submitted.results[0].run.run_payload_json["broker_called"] is True


def _qmt_account(repo: InMemoryQmtStrategyLedgerRepository, *, account_id: str, strategy_name: str) -> None:
    repo.create_virtual_account(
        VirtualAccount(
            strategy_id=f"strategy_{strategy_name}",
            strategy_name=strategy_name,
            display_name=strategy_name,
            account_id=account_id,
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )


def _managed_buy_request(*, account_id: str, strategy_name: str, order_remark: str) -> ManagedOrderRequest:
    return ManagedOrderRequest(
        account_id=account_id,
        strategy_name=strategy_name,
        symbol="000001.SZ",
        side="BUY",
        order_type=BUY_ORDER_TYPE,
        quantity=100,
        price_type=5,
        price=Decimal("10"),
        order_remark=order_remark,
        trade_date=TRADE_DATE,
        mode="SIM",
    )


def test_miniqmt_managed_order_disconnect_freezes_until_reconnect_reconcile() -> None:
    account_id = "QMT_DISCONNECT_ACCOUNT"
    strategy_name = "DisconnectFreezeStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=False)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )

    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert len(broker.place_order_payloads) == 1

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    assert frozen.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"
    assert frozen.results[0].preflight.primary_error.context["recovery"]["stage"] == "BROKER_STILL_DISCONNECTED"
    assert len(broker.place_order_payloads) == 1

    broker.connect_ok = True
    recovered = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-recovered")]
    )

    assert recovered.success is True
    assert recovered.results[0].broker_called is True
    assert broker.order_query_calls >= 1
    assert broker.trade_query_calls >= 1
    assert len(broker.place_order_payloads) == 2
    status = service.broker_disconnect_freeze_status()
    assert status["frozen"] is False
    assert status["last_recovery"]["reason_code"] == "MINIQMT_BROKER_RECONNECTED_RECONCILED"


def test_miniqmt_managed_order_reconnect_reconcile_failure_keeps_freeze_without_submit() -> None:
    account_id = "QMT_RECONNECT_FAIL_ACCOUNT"
    strategy_name = "DisconnectFreezeReconcileFailStrategy"
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    _qmt_account(qmt_repo, account_id=account_id, strategy_name=strategy_name)
    broker = FakeManagedOrderBroker(fail_next_place=True, connect_ok=True, fail_order_query=True)
    service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    first = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-first")]
    )
    assert first.success is False
    assert first.results[0].broker_called is True
    assert first.results[0].preflight.primary_error.code == "MINIQMT_BROKER_DISCONNECTED_FREEZE"

    frozen = service.submit_batch(
        [_managed_buy_request(account_id=account_id, strategy_name=strategy_name, order_remark="freeze-blocked")]
    )

    assert frozen.success is False
    assert frozen.results[0].broker_called is False
    error = frozen.results[0].preflight.primary_error
    assert error.code == "MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED"
    assert error.context["recovery"]["stage"] == "RECONNECT_RECONCILE_FAILED"
    assert service.broker_disconnect_freeze_status()["frozen"] is True
    assert broker.order_query_calls == 1
    assert broker.trade_query_calls == 0
    assert len(broker.place_order_payloads) == 1


class FakePaperRepository:
    def __init__(self, portfolio: PaperPortfolio, *, positions: dict[str, PositionLot], cash: float) -> None:
        self.portfolio = portfolio
        self.positions = dict(positions)
        self.cash = cash
        self.calls: list[tuple[str, str, date]] = []

    def get_portfolio(self, portfolio_id: str) -> PaperPortfolio:
        self.calls.append(("get_portfolio", portfolio_id, TRADE_DATE))
        if portfolio_id != self.portfolio.portfolio_id:
            raise DataUnavailableError("paper v2 portfolio does not exist", context={"portfolio_id": portfolio_id})
        return self.portfolio

    def load_latest_positions(self, portfolio_id: str, before_or_on: date) -> dict[str, PositionLot]:
        self.calls.append(("load_latest_positions", portfolio_id, before_or_on))
        return dict(self.positions)

    def load_latest_cash(self, portfolio: PaperPortfolio, before_or_on: date) -> float:
        self.calls.append(("load_latest_cash", portfolio.portfolio_id, before_or_on))
        return self.cash


class FakeLocalSimMarketDataProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.previous_close_provider = SimpleNamespace(
            get_previous_close=lambda symbol, trade_date: PreviousClose(
                symbol=symbol,
                trade_date=trade_date,
                previous_trade_date=trade_date - timedelta(days=1),
                pre_close=10.0,
                source="test.previous_close",
            )
        )

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
                bar_time=start + timedelta(minutes=offset),
                open=10.0,
                high=10.2,
                low=9.9,
                close=10.1,
                volume=100_000,
                amount=1_000_000.0,
                limit_up=11.0,
                limit_down=9.0,
            )
            for offset in range(max(1, min_bars))
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

    def load_observed_intraday(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        until_time: datetime,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        source_input = self.load_symbol_input(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            min_bars=6,
            require_suspend_status=require_suspend_status,
            require_day_features=require_day_features,
        )
        return replace(
            source_input,
            minute_bars=[bar for bar in source_input.minute_bars if bar.bar_time <= until_time.replace(tzinfo=None)],
        )


class TwapSixBarLocalSimMarketDataProvider(FakeLocalSimMarketDataProvider):
    def load_symbol_input(self, **kwargs: Any) -> MinuteExecutionMarketInput:
        return super().load_symbol_input(
            **{
                **kwargs,
                "min_bars": max(6, int(kwargs.get("min_bars") or 0)),
            }
        )


class ToggleMissingLocalSimMarkProvider(FakeLocalSimMarketDataProvider):
    def __init__(self, *, missing_symbol: str) -> None:
        super().__init__()
        self.missing_symbol = missing_symbol
        self.mark_available = False

    def load_observed_intraday(
        self,
        *,
        symbol: str,
        trade_date: date,
        source: MinuteDataSource,
        until_time: datetime,
        require_suspend_status: bool = False,
        require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        observed = super().load_observed_intraday(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            until_time=until_time,
            require_suspend_status=require_suspend_status,
            require_day_features=require_day_features,
        )
        if symbol == self.missing_symbol and not self.mark_available:
            return replace(observed, minute_bars=[])
        return observed


class FakePreTradeTradabilityProvider:
    def __init__(self, statuses: dict[str, dict[str, Any]] | None = None) -> None:
        self.statuses = dict(statuses or {})
        self.calls: list[dict[str, Any]] = []
        self.suspend_status_provider = self
        self.st_status_provider = self

    def get_suspend_status(self, symbol: str, trade_date: date):
        return SimpleNamespace(
            symbol=symbol,
            trade_date=trade_date,
            is_suspended=False,
            suspend_type=None,
            suspend_timing=None,
            source="unit_test.suspend_status",
        )

    def get_st_status(self, symbol: str, trade_date: date):
        return DailyStStatus(
            symbol=symbol,
            trade_date=trade_date,
            is_st=False,
            source="unit_test.stock_st",
        )

    def get_statuses(self, symbols: list[str], trade_date: date, *, require_realtime_quote: bool = False):
        self.calls.append(
            {
                "symbols": list(symbols),
                "trade_date": trade_date,
                "require_realtime_quote": require_realtime_quote,
            }
        )
        return {
            symbol: dict(
                self.statuses.get(
                    symbol,
                    {
                        "schema_version": "pre_trade_tradability_status_v1",
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "is_tradable": True,
                        "reason_code": "OK",
                        "source": "unit_test",
                    },
                )
            )
            for symbol in symbols
        }


def _frozen_manifest(package_id: str = "pkg_scheduler", manifest_sha256: str | None = None) -> StrategyPackageManifest:
    manifest = StrategyPackageManifest(
        manifest_version="alpha_core_v1",
        package_id=package_id,
        package_name="Scheduler test package",
        source=StrategyPackageSource(
            source_type=SourceType.QE_EXPERIMENT,
            source_id="qe_scheduler",
        ),
        alpha_mode=AlphaMode.SINGLE_ALPHA,
        alpha_components=[
            AlphaComponent(
                alpha_id="alpha_scheduler",
                alpha_name="Scheduler Alpha",
                component_weight=1.0,
                factor_ids=["factor_scheduler"],
                model_id="model_scheduler",
                holding_period="1d",
                rebalance_frequency="1d",
                score_direction="higher_better",
            )
        ],
        alpha_combination_policy=AlphaCombinationPolicy(
            method="identity",
            weights={"alpha_scheduler": 1.0},
            conflict_resolution="highest_score",
        ),
        factor_set=[FactorAsset(factor_id="factor_scheduler", factor_name="factor_scheduler")],
        model_asset=ModelAsset(model_id="model_scheduler"),
        backtest_summary=BacktestSummary(ic=0.03, rank_ic=0.02, raw_metrics={"IC": 0.03}),
        package_status=PackageStatus.PAPER_ENABLED,
    )
    frozen = freeze_manifest(manifest)
    if manifest_sha256 is not None:
        frozen = frozen.model_copy(update={"manifest_sha256": manifest_sha256})
    return frozen


def _score_weighted_manifest(release, *, topk: int = 2, n_drop: int = 1) -> StrategyPackageManifest:
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    return manifest.model_copy(
        update={
            "backtest_context": {
                "daily_strategy": {
                    "strategy_id": "score_weighted_topk_v2",
                    "topk": topk,
                    "n_drop": n_drop,
                    "custom_params": {
                        "strategy_class": "score_weighted_topk_v2",
                        "topk": topk,
                        "n_drop": n_drop,
                        "max_n_drop": max(n_drop, 1),
                        "min_n_drop": 0,
                        "weight_method": "equal",
                        "max_position_ratio": 0.95,
                    },
                }
            },
            "manifest_sha256": release.manifest_sha256,
        }
    )


def test_scheduler_plans_active_local_and_miniqmt_bindings_from_same_selection_evidence() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_shared"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert result.failed_count == 0
    plans = [item.execution_plan for item in result.results]
    assert {plan.selection_evidence_hash for plan in plans if plan is not None} == {plans[0].selection_evidence_hash}
    normalized_intents = [
        [(intent.symbol, intent.side.value, intent.order_quantity, intent.rebalance_reason) for intent in plan.intents]
        for plan in plans
        if plan is not None
    ]
    assert normalized_intents[0] == normalized_intents[1]
    assert ("000003.SZ", "SELL", 77, "DROPPED_FROM_SELECTION") in normalized_intents[0]


def test_scheduler_runs_draft_sim_bindings_without_approval_gate() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(
        approval_state=SimulationBindingApprovalState.DRAFT
    )
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_draft"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_draft"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert result.failed_count == 0


def test_scheduler_keeps_retired_sim_bindings_out_of_selection() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(
        approval_state=SimulationBindingApprovalState.RETIRED
    )
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_retired"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_retired"),
            }
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)

    assert result.total_bindings == 0
    assert result.results == ()


def test_scheduler_skips_existing_bindings_when_strategy_package_is_retired() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    selection = FakeSelectionService(
        release,
        candidates=_candidate_rows(),
        package_status=PackageStatus.RETIRED,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=True)

    assert result.total_bindings == 2
    assert result.planned_count == 0
    assert result.submitted_count == 0
    assert result.failed_count == 0
    assert {item.binding_id for item in result.results} == {
        local_binding.binding_id,
        qmt_binding.binding_id,
    }
    assert {item.status for item in result.results} == {"SKIPPED_RETIRED_PACKAGE"}
    assert all(item.run is None and item.execution_plan is None for item in result.results)
    assert all(
        item.lifecycle_diagnostic
        == {
            "schema_version": "simulation_package_lifecycle_skip_v1",
            "reason_code": "SIMULATION_BINDING_PACKAGE_RETIRED",
            "stage": "BINDING_SELECTION",
            "action": "SKIP",
            "binding_id": item.binding_id,
            "strategy_id": item.strategy_id,
            "package_id": release.package_id,
            "package_status": "RETIRED",
            "broker_backend": item.broker_backend.value,
            "broker_called": False,
            "strategy_package_revalidation_performed": False,
        }
        for item in result.results
    )
    assert selection.calls == []
    assert selection.package_repository.calls == [release.package_id]
    assert repo.list_simulation_daily_runs(limit=10) == []


@pytest.mark.parametrize(
    ("exc_cls", "reason_code"),
    (
        (DataUnavailableError, "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE"),
        (RuntimeConfigInvalidError, "UNIT_PRE_RUN_CONFIG_INVALID"),
    ),
)
def test_scheduler_persists_pre_run_binding_failure_without_duplicate_rows(exc_cls, reason_code) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    def exc_factory(binding, trade_date):
        return exc_cls(
            "unit pre-run failure",
            context={
                "reason_code": reason_code,
                "binding_id": binding.binding_id,
                "strategy_id": binding.strategy_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=SelectiveFailingContextProvider(
            failing_binding_id=local_binding.binding_id,
            exc_factory=exc_factory,
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    runs = [
        run
        for run in repo.list_simulation_daily_runs(trade_date=TRADE_DATE, limit=10)
        if run.binding_id == local_binding.binding_id
    ]
    latest = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    assert latest is not None
    pre_run_failure = latest.run_payload_json["pre_run_failure"]
    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(latest.run_id)

    assert first.failed_count == 1
    assert first.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert first.results[0].run is not None
    assert second.failed_count == 1
    assert second.results[0].run is not None
    assert second.results[0].run.run_id == first.results[0].run.run_id
    assert len(runs) == 1
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.execution_plan_id is None
    assert pre_run_failure["stage"] == "PRE_RUN_FAILED"
    assert pre_run_failure["reason_code"] == reason_code
    assert pre_run_failure["binding_id"] == local_binding.binding_id
    assert pre_run_failure["strategy_id"] == local_binding.strategy_id
    assert pre_run_failure["trade_date"] == TRADE_DATE.isoformat()
    assert pre_run_failure["broker_called"] is False
    assert pre_run_failure["observed_count"] == 2
    assert latest.run_payload_json["broker_called"] is False
    assert latest.run_payload_json["submitted_intents"] == 0
    assert latest.run_payload_json["failed_intents"] == 0
    assert latest.run_payload_json["submit_failure"]["stage"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["code"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["context"]["reason_code"] == reason_code


def test_scheduler_pre_run_binding_failure_does_not_block_other_bindings() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    def exc_factory(binding, trade_date):
        return DataUnavailableError(
            "unit LocalSim context unavailable",
            context={
                "reason_code": "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE",
                "binding_id": binding.binding_id,
                "trade_date": trade_date.isoformat(),
            },
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=SelectiveFailingContextProvider(
            failing_binding_id=local_binding.binding_id,
            exc_factory=exc_factory,
            by_binding_id={
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt_still_runs"),
            },
        ),
    )

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)
    by_binding_id = {item.binding_id: item for item in result.results}
    failed_run = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    qmt_run = repo.get_simulation_daily_run_by_key(
        strategy_id=qmt_binding.strategy_id,
        binding_id=qmt_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert by_binding_id[local_binding.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert by_binding_id[local_binding.binding_id].error["context"]["reason_code"] == "UNIT_PRE_RUN_CONTEXT_UNAVAILABLE"
    assert by_binding_id[qmt_binding.binding_id].status == "PLANNED"
    assert failed_run is not None
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert qmt_run is not None
    assert qmt_run.execution_plan_id is not None


def test_scheduler_miniqmt_durable_replay_failure_does_not_starve_later_localsim_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt_replay_failure"),
                local_binding.binding_id: _position_context(portfolio_id="portfolio_local_after_qmt_failure"),
            }
        ),
    )
    original_run_binding = scheduler._run_binding_with_watchdog

    def replay_failure_then_continue(*, binding, **kwargs):
        if binding.binding_id == qmt_binding.binding_id:
            raise BrokerSubmitError(
                "durable result identity conflict",
                context={
                    "reason_code": "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT",
                    "stage": "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY",
                    "qmt_batch_id": "qmtbatch_scheduler_isolation",
                    "binding_id": binding.binding_id,
                },
            )
        return original_run_binding(binding=binding, **kwargs)

    monkeypatch.setattr(scheduler, "_run_binding_with_watchdog", replay_failure_then_continue)

    result = scheduler.run_once(trade_date=TRADE_DATE, data_source="DB_HISTORICAL", submit=False)
    by_binding_id = {item.binding_id: item for item in result.results}
    qmt_failure = by_binding_id[qmt_binding.binding_id]
    local_result = by_binding_id[local_binding.binding_id]
    qmt_run = repo.get_simulation_daily_run_by_key(
        strategy_id=qmt_binding.strategy_id,
        binding_id=qmt_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    local_run = repo.get_simulation_daily_run_by_key(
        strategy_id=local_binding.strategy_id,
        binding_id=local_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert qmt_failure.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert qmt_failure.error["context"]["reason_code"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_IDENTITY_CONFLICT"
    assert (
        qmt_failure.error["context"]["pre_run_failure"]["context"]["stage"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY"
    )
    assert local_result.status == "PLANNED"
    assert qmt_run is not None
    assert qmt_run.run_payload_json["pre_run_failure"]["context"]["stage"] == "MINIQMT_EVENT_LOOP_DURABLE_BATCH_REPLAY"
    assert local_run is not None
    assert local_run.execution_plan_id is not None


def test_scheduler_isolates_live_inference_preflight_failure_and_continues_later_bindings() -> None:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    good_release = _create_scheduler_release(repo, package_id="pkg_good", manifest_sha256="manifest_good")
    bad_release = _create_scheduler_release(repo, package_id="pkg_bad", manifest_sha256="manifest_bad")
    good_binding = service.create_binding(
        strategy_id="strategy_good_after_bad",
        release=good_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="good binding processed after bad preflight",
    )
    bad_binding = service.create_binding(
        strategy_id="strategy_bad_preflight",
        release=bad_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="bad binding with live inference preflight failure",
    )
    selection = PackageRoutingSelectionService(
        {good_release.package_id: good_release, bad_release.package_id: bad_release},
        failing_package_id=bad_release.package_id,
    )
    context_provider = StaticSimulationRunContextProvider(
        by_binding_id={
            bad_binding.binding_id: _position_context(portfolio_id="portfolio_bad"),
            good_binding.binding_id: _position_context(portfolio_id="portfolio_good"),
        }
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=context_provider,
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    by_binding_id = {item.binding_id: item for item in result.results}
    failed_run = repo.get_simulation_daily_run_by_key(
        strategy_id=bad_binding.strategy_id,
        binding_id=bad_binding.binding_id,
        trade_date=TRADE_DATE,
    )
    good_run = repo.get_simulation_daily_run_by_key(
        strategy_id=good_binding.strategy_id,
        binding_id=good_binding.binding_id,
        trade_date=TRADE_DATE,
    )

    assert result.total_bindings == 2
    assert result.failed_count == 1
    assert result.planned_count == 1
    assert [call["package_ids"][0] for call in selection.calls] == ["pkg_bad", "pkg_good"]
    assert by_binding_id[bad_binding.binding_id].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert by_binding_id[good_binding.binding_id].status == "PLANNED"
    assert failed_run is not None
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.execution_plan_id is None
    assert failed_run.run_payload_json["broker_called"] is False
    assert failed_run.run_payload_json["submitted_intents"] == 0
    assert good_run is not None
    assert good_run.execution_plan_id is not None
    pre_run_failure = failed_run.run_payload_json["pre_run_failure"]
    assert pre_run_failure["stage"] == "preflight"
    assert pre_run_failure["failure_stage"] == "preflight"
    assert pre_run_failure["reason_code"] == "strategy_package_model_code_missing"
    assert pre_run_failure["package_id"] == bad_release.package_id
    assert pre_run_failure["manifest_sha256"] == bad_release.manifest_sha256
    assert pre_run_failure["blocked_check"] == PREFLIGHT_CHECK_MODEL_PARAMS
    assert pre_run_failure["missing_relative_paths"] == ["model.py"]
    assert pre_run_failure["broker_called"] is False
    assert pre_run_failure["submitted_intents"] == 0
    assert (
        by_binding_id[bad_binding.binding_id].error["context"]["reason_code"] == "strategy_package_model_code_missing"
    )
    assert by_binding_id[bad_binding.binding_id].error["context"]["blocked_check"] == PREFLIGHT_CHECK_MODEL_PARAMS
    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(failed_run.run_id)
    assert detail["run"]["errors"][0]["code"] == "PRE_RUN_FAILED"
    assert detail["run"]["errors"][0]["context"]["reason_code"] == "strategy_package_model_code_missing"


def test_scheduler_auto_generated_selection_timeout_does_not_freeze_other_bindings() -> None:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    auto_generate_config = {
        "selection_artifact_config": {
            "auto_generate": True,
            "include_reference_price": True,
        },
        "runtime_profile": {
            "selection": {"top_k": 2},
            "tradability": {"exclude_suspended": False},
        },
    }
    slow_release = _create_scheduler_release(
        repo,
        package_id="pkg_slow_autogen",
        manifest_sha256="manifest_slow_autogen",
        release_metadata={"selection_runtime_config": auto_generate_config},
    )
    fast_release = _create_scheduler_release(repo, package_id="pkg_fast_after_slow", manifest_sha256="manifest_fast")
    slow_binding = service.create_binding(
        strategy_id="strategy_slow_autogen",
        release=slow_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="slow autogen binding",
    )
    fast_binding = service.create_binding(
        strategy_id="strategy_fast_after_slow",
        release=fast_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="fast binding still processed",
    )

    class BlockingAutoGenerateSelectionService:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.completed = threading.Event()
            self.calls: list[str] = []
            self.package_repository = SimpleNamespace(
                get=lambda package_id: SimpleNamespace(
                    package_id=package_id,
                    manifest_sha256={
                        slow_release.package_id: slow_release.manifest_sha256,
                        fast_release.package_id: fast_release.manifest_sha256,
                    }[package_id],
                    package_status=PackageStatus.SELECTION_ENABLED,
                )
            )

        def run_selection(self, **kwargs):
            package_id = kwargs["package_ids"][0]
            self.calls.append(package_id)
            if package_id == slow_release.package_id:
                self.started.set()
                self.release.wait(timeout=5.0)
                self.completed.set()
            runtime_release = (
                kwargs.get("runtime_release")
                or {
                    slow_release.package_id: slow_release,
                    fast_release.package_id: fast_release,
                }[package_id]
            )
            candidates = _candidate_rows()
            evidence = _evidence(
                runtime_release, candidates=candidates, target_trade_date=kwargs.get("trade_date") or TRADE_DATE
            )
            return StrategyPackageSelectionResult(
                runtime_config={
                    "runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}
                },
                package_results={runtime_release.package_id: candidates},
                aggregate_results=candidates,
                excluded_results={runtime_release.package_id: []},
                manifest_sha256_by_package={runtime_release.package_id: runtime_release.manifest_sha256},
                evidence_by_package={runtime_release.package_id: evidence},
            )

    selection = BlockingAutoGenerateSelectionService()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                slow_binding.binding_id: _position_context(portfolio_id="portfolio_slow"),
                fast_binding.binding_id: _position_context(portfolio_id="portfolio_fast"),
            }
        ),
        selection_inference_timeout_seconds=0.01,
        selection_inference_max_workers=1,
    )

    try:
        started = time_module.perf_counter()
        first = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )
        elapsed = time_module.perf_counter() - started
        assert selection.started.wait(timeout=1.0)

        by_binding_id = {item.binding_id: item for item in first.results}
        assert elapsed < 0.5
        assert first.total_bindings == 2
        assert first.failed_count == 0
        assert first.planned_count == 1
        pending = by_binding_id[slow_binding.binding_id]
        assert pending.status == "SELECTION_INFERENCE_PENDING"
        assert pending.error is None
        assert pending.lifecycle_diagnostic["reason_code"] == "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS"
        assert pending.run.status == SimulationDailyRunStatus.SIGNAL_GENERATING
        assert "pre_run_failure" not in pending.run.run_payload_json
        assert "submit_failure" not in pending.run.run_payload_json
        assert by_binding_id[fast_binding.binding_id].status == "PLANNED"

        time_module.sleep(0.03)
        second = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )
        second_by_binding_id = {item.binding_id: item for item in second.results}
        inflight_status = scheduler.status()["selection_inference"]

        assert second.total_bindings == 2
        assert second.failed_count == 1
        assert (
            second_by_binding_id[slow_binding.binding_id].error["context"]["reason_code"]
            == "SIMULATION_SELECTION_INFERENCE_TIMEOUT"
        )
        assert second_by_binding_id[slow_binding.binding_id].error["context"]["failure_stage"] == "selection_inference"
        assert second_by_binding_id[fast_binding.binding_id].status == "REUSED_EXISTING_PLAN"
        assert selection.calls.count(slow_release.package_id) == 1
        assert inflight_status["in_flight_count"] == 1
        assert inflight_status["in_flight"][0]["timed_out"] is True

        selection.release.set()
        assert selection.completed.wait(timeout=1.0)
        completed = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )
        completed_by_binding_id = {item.binding_id: item for item in completed.results}
        completed_slow = completed_by_binding_id[slow_binding.binding_id]
        assert completed_slow.status == "PLANNED"
        assert completed_slow.run.status == SimulationDailyRunStatus.PLANNING_EXECUTION
        assert "selection_inference_pending" not in completed_slow.run.run_payload_json
        assert "pre_run_failure" not in completed_slow.run.run_payload_json
        assert "submit_failure" not in completed_slow.run.run_payload_json
    finally:
        selection.release.set()
        scheduler.shutdown_selection_inference(wait=True)


def test_scheduler_auto_generated_selection_inference_isolated_by_runtime_release() -> None:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    auto_generate_config = {
        "selection_artifact_config": {
            "auto_generate": True,
            "include_reference_price": True,
        },
        "runtime_profile": {
            "selection": {"top_k": 2},
            "tradability": {"exclude_suspended": False},
        },
    }
    release_a = _create_scheduler_release(
        repo,
        package_id="pkg_release_isolation",
        manifest_sha256="manifest_release_isolation",
        release_metadata={"selection_runtime_config": auto_generate_config, "release_marker": "a"},
    )
    release_b = _create_scheduler_release(
        repo,
        package_id="pkg_release_isolation",
        manifest_sha256="manifest_release_isolation",
        release_metadata={"selection_runtime_config": auto_generate_config, "release_marker": "b"},
    )
    binding_a = service.create_binding(
        strategy_id="strategy_release_isolation_a",
        release=release_a,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="release-isolated inference a",
    )
    binding_b = service.create_binding(
        strategy_id="strategy_release_isolation_b",
        release=release_b,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="release-isolated inference b",
    )

    class BlockingPerReleaseSelectionService:
        def __init__(self) -> None:
            self.package_repository = FakePackageRepository(
                package_id=release_a.package_id,
                manifest_sha256=release_a.manifest_sha256,
            )
            self.started = {
                release_a.release_id: threading.Event(),
                release_b.release_id: threading.Event(),
            }
            self.release = threading.Event()

        def run_selection(self, **kwargs):
            runtime_release = kwargs["runtime_release"]
            self.started[runtime_release.release_id].set()
            self.release.wait(timeout=5.0)
            candidates = _candidate_rows()
            evidence = _evidence(
                runtime_release,
                candidates=candidates,
                target_trade_date=kwargs.get("trade_date") or TRADE_DATE,
            )
            return StrategyPackageSelectionResult(
                runtime_config={
                    "runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}
                },
                package_results={runtime_release.package_id: candidates},
                aggregate_results=candidates,
                excluded_results={runtime_release.package_id: []},
                manifest_sha256_by_package={runtime_release.package_id: runtime_release.manifest_sha256},
                evidence_by_package={runtime_release.package_id: evidence},
            )

    selection = BlockingPerReleaseSelectionService()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                binding_a.binding_id: _position_context(portfolio_id="portfolio_release_isolation_a"),
                binding_b.binding_id: _position_context(portfolio_id="portfolio_release_isolation_b"),
            }
        ),
        selection_inference_timeout_seconds=5.0,
        selection_inference_max_workers=2,
    )

    try:
        result = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )

        assert result.failed_count == 0
        assert selection.started[release_a.release_id].wait(timeout=1.0)
        assert selection.started[release_b.release_id].wait(timeout=1.0)
        by_binding_id = {item.binding_id: item for item in result.results}
        assert by_binding_id[binding_a.binding_id].status == "SELECTION_INFERENCE_PENDING"
        assert by_binding_id[binding_b.binding_id].status == "SELECTION_INFERENCE_PENDING"
        assert by_binding_id[binding_a.binding_id].lifecycle_diagnostic["context"]["release_id"] == release_a.release_id
        assert by_binding_id[binding_b.binding_id].lifecycle_diagnostic["context"]["release_id"] == release_b.release_id
        status = scheduler.status()["selection_inference"]
        assert status["in_flight_count"] == 2
        assert {item["release_id"] for item in status["in_flight"]} == {
            release_a.release_id,
            release_b.release_id,
        }
    finally:
        selection.release.set()
        scheduler.shutdown_selection_inference(wait=True)


def _legacy_scheduler_miniqmt_submit_timeout_skips_binding_and_continues_later_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, _broker, qmt_binding_a, qmt_binding_b = _miniqmt_two_strategy_scheduler()
    hanging_broker = HangingPlaceOrderBroker()
    first_context = scheduler.context_provider._by_binding_id[qmt_binding_a.binding_id]
    scheduler.context_provider._by_binding_id[qmt_binding_a.binding_id] = replace(
        first_context,
        managed_order_service=QmtManagedOrderService(
            repository=first_context.qmt_ledger_repository,
            broker=hanging_broker,  # type: ignore[arg-type]
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
    )
    monkeypatch.setenv("SIMULATION_RUNTIME_MINIQMT_SUBMIT_TIMEOUT_SEC", "0.2")
    monkeypatch.setenv("SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC", "1.0")

    started = time_module.perf_counter()
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    elapsed = time_module.perf_counter() - started

    try:
        assert elapsed < 1.0
        assert hanging_broker.place_started.wait(timeout=0.5)
        by_binding_id = {item.binding_id: item for item in submitted.results}
        failed = by_binding_id[qmt_binding_a.binding_id]
        continued = by_binding_id[qmt_binding_b.binding_id]
        failed_run = repo.get_simulation_daily_run(failed.run.run_id)
        assert failed.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
        assert failed.error["context"]["reason_code"] == "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT"
        assert failed_run.run_payload_json["submit_failure"]["stage"] == "MINIQMT_EVENT_LOOP_SUBMIT_TIMEOUT"
        assert failed_run.run_payload_json["miniqmt_side_effect_state"] == "UNKNOWN_TIMEOUT"
        assert continued.status == "RECONCILED", continued.error
        assert continued.run.run_payload_json["broker_called"] is True
        assert submitted.total_bindings == 2
        assert submitted.failed_count == 1
        assert submitted.submitted_count == 1
    finally:
        hanging_broker.release_place.set()


def test_scheduler_localsim_watchdog_keeps_one_owned_binding_tick_until_result_is_consumed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_binding_single_flight"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_binding_fast_peer"),
            }
        ),
    )
    started = threading.Event()
    release_worker = threading.Event()
    finished = threading.Event()
    call_count: dict[str, int] = {}
    call_count_lock = threading.Lock()

    def slow_binding(**values: Any) -> SimulationSchedulerBindingResult:
        binding = values["binding"]
        with call_count_lock:
            call_count[binding.binding_id] = call_count.get(binding.binding_id, 0) + 1
        if binding.binding_id == local_binding.binding_id:
            started.set()
            assert release_worker.wait(timeout=2.0)
            finished.set()
        return SimulationSchedulerBindingResult(
            binding_id=binding.binding_id,
            strategy_id=binding.strategy_id,
            broker_backend=binding.broker_backend,
            status="LOCALSIM_TEST_COMPLETED",
        )

    scheduler._run_binding = slow_binding  # type: ignore[method-assign]
    monkeypatch.setenv("SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC", "0.05")
    try:
        first = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="TDX_REALTIME",
            submit=True,
        )
        assert started.wait(timeout=0.5)
        second = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="TDX_REALTIME",
            submit=True,
        )

        assert first.failed_count == 0
        assert second.failed_count == 0
        first_by_binding = {item.binding_id: item for item in first.results}
        second_by_binding = {item.binding_id: item for item in second.results}
        assert first_by_binding[local_binding.binding_id].status == "LOCALSIM_BINDING_TICK_IN_PROGRESS"
        assert second_by_binding[local_binding.binding_id].status == "LOCALSIM_BINDING_TICK_IN_PROGRESS"
        assert first_by_binding[local_binding.binding_id].lifecycle_diagnostic["alert"]["auto_clear"] == (
            "owner_result_consumed"
        )
        assert first_by_binding[qmt_binding.binding_id].status == "LOCALSIM_TEST_COMPLETED"
        assert second_by_binding[qmt_binding.binding_id].status == "LOCALSIM_TEST_COMPLETED"
        assert call_count == {local_binding.binding_id: 1, qmt_binding.binding_id: 2}
        in_flight = scheduler.status()["binding_watchdog"]["in_flight"]
        assert len(in_flight) == 1
        assert in_flight[0]["binding_id"] == local_binding.binding_id
        assert in_flight[0]["thread_alive"] is True
        shutdown_observation = scheduler.shutdown_binding_ticks(wait=False)
        assert shutdown_observation == {
            "schema_version": "localsim_binding_tick_shutdown_observation_v1",
            "wait_requested": False,
            "observed_owner_count": 1,
            "thread_alive_count": 1,
            "all_threads_stopped": False,
            "alive_binding_ids": [local_binding.binding_id],
        }

        release_worker.set()
        assert finished.wait(timeout=1.0)
        completed = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="TDX_REALTIME",
            submit=True,
        )
        completed_by_binding = {item.binding_id: item for item in completed.results}
        assert completed_by_binding[local_binding.binding_id].status == "LOCALSIM_TEST_COMPLETED"
        assert completed_by_binding[qmt_binding.binding_id].status == "LOCALSIM_TEST_COMPLETED"
        assert call_count == {local_binding.binding_id: 1, qmt_binding.binding_id: 3}
        assert scheduler.status()["binding_watchdog"]["in_flight_count"] == 0
    finally:
        release_worker.set()


def test_scheduler_localsim_late_worker_failure_is_consumed_once_and_keeps_original_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_binding_late_failure")}
        ),
    )
    started = threading.Event()
    release_worker = threading.Event()
    call_count = 0

    def fail_late(**_values: Any) -> SimulationSchedulerBindingResult:
        nonlocal call_count
        call_count += 1
        started.set()
        assert release_worker.wait(timeout=2.0)
        raise DataUnavailableError(
            "late LocalSIM market snapshot failure",
            context={
                "reason_code": "LOCALSIM_TEST_LATE_MARKET_FAILURE",
                "failure_stage": "LOCAL_SIM_INTRADAY_ADVANCE",
            },
        )

    scheduler._run_binding = fail_late  # type: ignore[method-assign]
    monkeypatch.setenv("SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC", "0.05")
    try:
        first = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="TDX_REALTIME",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
        )
        assert started.wait(timeout=0.5)
        assert first.results[0].status == "LOCALSIM_BINDING_TICK_IN_PROGRESS"
        assert first.failed_count == 0

        release_worker.set()
        deadline = time_module.monotonic() + 1.0
        while time_module.monotonic() < deadline:
            in_flight = scheduler.status()["binding_watchdog"]["in_flight"]
            if in_flight and in_flight[0]["result_ready"]:
                break
            time_module.sleep(0.01)
        else:
            pytest.fail("late LocalSIM failure was not published by its owning worker")

        failed = scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="TDX_REALTIME",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
        )
        assert call_count == 1
        assert failed.failed_count == 1
        assert failed.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
        assert failed.results[0].error["context"]["reason_code"] == "LOCALSIM_TEST_LATE_MARKET_FAILURE"
        assert scheduler.status()["binding_watchdog"]["in_flight_count"] == 0
    finally:
        release_worker.set()


def _legacy_scheduler_miniqmt_reconcile_timeout_skips_binding_and_continues_later_binding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler, repo, broker, qmt_binding_a, qmt_binding_b = _miniqmt_two_strategy_scheduler()
    hanging_reconcile = HangingReconciliationService()
    first_context = scheduler.context_provider._by_binding_id[qmt_binding_a.binding_id]
    scheduler.context_provider._by_binding_id[qmt_binding_a.binding_id] = replace(
        first_context,
        qmt_reconciliation_service=hanging_reconcile,
    )
    monkeypatch.setenv("SIMULATION_RUNTIME_MINIQMT_RECONCILE_TIMEOUT_SEC", "0.2")
    monkeypatch.setenv("SIMULATION_RUNTIME_BINDING_WATCHDOG_TIMEOUT_SEC", "1.0")

    started = time_module.perf_counter()
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    elapsed = time_module.perf_counter() - started

    try:
        assert elapsed < 1.0
        assert hanging_reconcile.started.wait(timeout=0.5)
        by_binding_id = {item.binding_id: item for item in submitted.results}
        failed = by_binding_id[qmt_binding_a.binding_id]
        continued = by_binding_id[qmt_binding_b.binding_id]
        failed_run = repo.get_simulation_daily_run(failed.run.run_id)
        assert failed.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
        assert failed.error["context"]["reason_code"] == "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT"
        assert (
            failed_run.run_payload_json["miniqmt_reconcile_timeout"]["stage"]
            == "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT"
        )
        assert failed_run.run_payload_json["broker_called"] is True
        assert continued.status == "RECONCILED"
        assert continued.run.run_payload_json["broker_called"] is True
        assert len(broker.place_order_payloads) == 6
        assert submitted.total_bindings == 2
        assert submitted.failed_count == 1
        assert submitted.submitted_count == 1
    finally:
        hanging_reconcile.release.set()


def test_scheduler_raise_on_error_reraises_live_inference_preflight_failure() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    selection = PackageRoutingSelectionService(
        {release.package_id: release},
        failing_package_id=release.package_id,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_raise")}
        ),
    )

    with pytest.raises(LiveInferencePreflightError):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
            raise_on_error=True,
        )

    assert (
        repo.get_simulation_daily_run_by_key(
            strategy_id=local_binding.strategy_id,
            binding_id=local_binding.binding_id,
            trade_date=TRADE_DATE,
        )
        is None
    )


def test_scheduler_does_not_swallow_system_exit_from_binding_boundary() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    class SystemExitSelectionService:
        package_repository = FakePackageRepository(
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
        )

        def run_selection(self, **_kwargs):
            raise SystemExit("fatal scheduler stop signal")

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=SystemExitSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_system_exit")}
        ),
    )

    with pytest.raises(SystemExit):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
        )

    assert (
        repo.get_simulation_daily_run_by_key(
            strategy_id=local_binding.strategy_id,
            binding_id=local_binding.binding_id,
            trade_date=TRADE_DATE,
        )
        is None
    )


def test_scheduler_sizes_miniqmt_targets_from_dynamic_strategy_slot_equity() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            frozen_cash=Decimal("10000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    candidates = [
        SelectionCandidate(
            symbol="000001.SZ",
            score=0.99,
            rank=1,
            target_weight=0.10,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
    ]
    context = SimulationRunContext(
        portfolio_id=qmt_binding.strategy_id,
        current_positions={
            "000003.SZ": PositionLot(
                portfolio_id=qmt_binding.strategy_id,
                symbol="000003.SZ",
                quantity=1000,
                available_quantity=1000,
                avg_cost=9.0,
                trade_date=date(2026, 5, 20),
            )
        },
        current_prices={"000003.SZ": 20.0},
        qmt_ledger_repository=qmt_repo,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    plan = result.results[0].execution_plan
    assert plan is not None
    buy = next(intent for intent in plan.intents if intent.symbol == "000001.SZ")
    assert buy.order_quantity == 1300
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    basis = run.run_payload_json["target_equity_basis"]
    assert basis["source"] == "miniqmt_strategy_slot_dynamic_equity"
    assert basis["cash"] == 100_000.0
    assert basis["frozen_cash"] == 10_000.0
    assert basis["market_value"] == 20_000.0
    assert basis["total_equity"] == 130_000.0
    assert basis["capital_allocation"] == 100_000.0


def test_scheduler_persists_no_rebalance_evidence_when_targets_match_current_positions() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id=qmt_binding.strategy_id,
                    current_positions={
                        "000001.SZ": PositionLot(
                            portfolio_id=qmt_binding.strategy_id,
                            symbol="000001.SZ",
                            quantity=1000,
                            available_quantity=1000,
                            avg_cost=10.0,
                            trade_date=date(2026, 5, 20),
                        ),
                        "688001.SH": PositionLot(
                            portfolio_id=qmt_binding.strategy_id,
                            symbol="688001.SH",
                            quantity=201,
                            available_quantity=201,
                            avg_cost=20.0,
                            trade_date=date(2026, 5, 20),
                        ),
                    },
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    evidence = run.run_payload_json["no_rebalance_evidence"]
    assert evidence["reason_code"] == "TOP_LIST_AND_QUANTITY_MATCH"
    assert evidence["selected_symbols"] == ["000001.SZ", "688001.SH"]
    assert evidence["target_symbols"] == ["000001.SZ", "688001.SH"]
    assert all(row["delta_quantity"] == 0 for row in evidence["rows"])


def test_scheduler_rolls_forward_expired_localsim_binding_for_unattended_daily_runs() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={local_binding.strategy_id: _position_context(portfolio_id="portfolio_roll_forward")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    rolled_binding = result.results[0].run
    assert rolled_binding is not None
    assert rolled_binding.binding_id != local_binding.binding_id
    new_binding = repo.get_simulation_release_binding(rolled_binding.binding_id)
    assert new_binding.strategy_id == local_binding.strategy_id
    assert new_binding.effective_from == next_trade_day
    assert new_binding.effective_to == next_trade_day
    assert new_binding.binding_config_json["metadata"]["purpose"] == "localsim_unattended_daily_roll_forward"
    assert new_binding.binding_config_json["metadata"]["extends_binding_id"] == local_binding.binding_id
    new_release = repo.get_strategy_runtime_release(new_binding.release_id)
    twap_policy = local_sim_twap_only_policy_snapshot()
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
    assert new_release.execution_policy_version_id == twap_policy["policy_version_id"]
    assert new_release.execution_policy_sha256 == twap_policy["policy_sha256"]
    assert new_release.release_config_json["execution_policy"] == twap_policy
    assert new_release.daily_strategy_profile_version_id == release.daily_strategy_profile_version_id
    assert new_release.tail_policy_version_id == release.tail_policy_version_id
    assert new_release.tail_policy_sha256 == release.tail_policy_sha256
    assert new_release.release_config_json["metadata"]["execution_policy_authority"] == {
        "schema_version": "simulation_roll_forward_execution_policy_authority_v1",
        "source_policy_version_id": release.execution_policy_version_id,
        "source_policy_sha256": release.execution_policy_sha256,
        "effective_policy_version_id": twap_policy["policy_version_id"],
        "effective_policy_sha256": twap_policy["policy_sha256"],
        "authority_source": "localsim_twap_only_runtime_policy",
        "source_policy_consulted_for_execution": False,
        "fallback_used": False,
    }
    assert new_release.validation_evidence["execution_policy_authority"] == (
        new_release.release_config_json["metadata"]["execution_policy_authority"]
    )
    assert result.results[0].execution_plan.execution_policy_version_id == new_release.execution_policy_version_id
    assert result.results[0].execution_plan.execution_policy_sha256 == new_release.execution_policy_sha256
    assert result.results[0].execution_plan.plan_payload_json["execution_policy"]["payload"] == twap_policy
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id
    local_bindings = repo.list_simulation_release_bindings(
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        approval_states=[SimulationBindingApprovalState.SIM_VALIDATING],
        limit=10,
    )
    assert len([binding for binding in local_bindings if binding.effective_from == next_trade_day]) == 1


def test_scheduler_does_not_roll_forward_retired_strategy_package_bindings() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    for binding in (local_binding, qmt_binding):
        expired = binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
        repo.bindings[expired.binding_id] = expired
    selection = FakeSelectionService(
        release,
        candidates=_candidate_rows(),
        package_status=PackageStatus.RETIRED,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=True,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 0
    assert result.submitted_count == 0
    assert {item.status for item in result.results} == {"SKIPPED_RETIRED_PACKAGE"}
    assert {item.binding_id for item in result.results} == {
        local_binding.binding_id,
        qmt_binding.binding_id,
    }
    assert all(item.lifecycle_diagnostic["broker_called"] is False for item in result.results)
    assert all(item.lifecycle_diagnostic["strategy_package_revalidation_performed"] is False for item in result.results)
    assert selection.calls == []
    assert selection.package_repository.calls == [release.package_id]
    assert len(repo.bindings) == 2
    assert repo.list_simulation_daily_runs(limit=10) == []


def test_scheduler_roll_forward_uses_current_package_manifest_and_rebases_side_effect_free_same_day_run() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    fake_selection.package_repository.manifest_sha256 = "manifest_current_after_controlled_package_update"
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={local_binding.strategy_id: _position_context(portfolio_id="portfolio_manifest_rebase")}
        ),
    )

    first = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    first_binding = repo.get_simulation_release_binding(first.results[0].run.binding_id)
    first_release = repo.get_strategy_runtime_release(first_binding.release_id)

    assert first.total_bindings == 1
    assert first.planned_count == 1
    assert first_release.manifest_sha256 == "manifest_current_after_controlled_package_update"
    assert first.results[0].run.manifest_sha256 == first_release.manifest_sha256
    assert first_release.validation_evidence["manifest_identity"] == {
        "source": "strategy_package_current_manifest",
        "source_release_manifest_sha256": release.manifest_sha256,
        "authoritative_manifest_sha256": "manifest_current_after_controlled_package_update",
        "identity_changed": True,
        "strategy_package_revalidation_performed": False,
    }

    fake_selection.package_repository.manifest_sha256 = "manifest_current_after_second_controlled_update"
    rebased = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    rebased_binding = repo.get_simulation_release_binding(rebased.results[0].run.binding_id)
    rebased_release = repo.get_strategy_runtime_release(rebased_binding.release_id)
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert rebased.total_bindings == 1
    assert rebased.planned_count == 1
    assert rebased_binding.binding_id != first_binding.binding_id
    assert rebased_binding.binding_config_json["metadata"]["extends_binding_id"] == first_binding.binding_id
    assert rebased_binding.binding_config_json["metadata"]["manifest_identity_source"] == (
        "strategy_package_current_manifest"
    )
    assert rebased_release.manifest_sha256 == "manifest_current_after_second_controlled_update"
    assert rerun.total_bindings == 1
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == rebased_binding.binding_id


def test_scheduler_rolls_forward_expired_miniqmt_binding_for_unattended_daily_runs() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_forward")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    rerun = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    rolled_run = result.results[0].run
    assert rolled_run is not None
    assert rolled_run.binding_id != qmt_binding.binding_id
    assert rolled_run.account_group_id == qmt_binding.account_group_id
    assert rolled_run.strategy_slot_id == qmt_binding.strategy_slot_id
    new_binding = repo.get_simulation_release_binding(rolled_run.binding_id)
    assert new_binding.broker_backend == SimulationBrokerBackend.MINIQMT_SIM
    assert new_binding.account_group_id == qmt_binding.account_group_id
    assert new_binding.strategy_slot_id == qmt_binding.strategy_slot_id
    assert new_binding.strategy_name == qmt_binding.strategy_name
    assert new_binding.order_remark_prefix == qmt_binding.order_remark_prefix
    assert new_binding.effective_from == next_trade_day
    assert new_binding.effective_to == next_trade_day
    assert new_binding.binding_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert new_binding.binding_config_json["metadata"]["extends_binding_id"] == qmt_binding.binding_id
    new_release = repo.get_strategy_runtime_release(new_binding.release_id)
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
    assert new_release.execution_policy_version_id == release.execution_policy_version_id
    assert new_release.execution_policy_sha256 == release.execution_policy_sha256
    assert new_release.release_config_json["execution_policy"] == release.release_config_json["execution_policy"]
    assert new_release.release_config_json["metadata"]["execution_policy_authority"] == {
        "schema_version": "simulation_roll_forward_execution_policy_authority_v1",
        "source_policy_version_id": release.execution_policy_version_id,
        "source_policy_sha256": release.execution_policy_sha256,
        "effective_policy_version_id": release.execution_policy_version_id,
        "effective_policy_sha256": release.execution_policy_sha256,
        "authority_source": "source_runtime_release",
        "source_policy_consulted_for_execution": True,
        "fallback_used": False,
    }
    assert new_release.release_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id


def test_scheduler_isolates_invalid_legacy_miniqmt_roll_forward_without_starving_valid_bindings() -> None:
    release, local_binding, invalid_qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    valid_qmt_binding = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_valid_qmt_after_invalid_roll_forward",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="ValidAfterInvalidRollForward",
        order_remark_prefix="valid-after-invalid-roll-forward",
    )
    invalid_rebase_binding = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_invalid_qmt_manifest_rebase",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="InvalidManifestRebase",
        order_remark_prefix="invalid-manifest-rebase",
    )
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    invalid_config = dict(invalid_qmt_binding.binding_config_json)
    invalid_config.pop("miniqmt_quote_control")
    invalid_qmt_binding = invalid_qmt_binding.model_copy(
        update={
            "binding_config_json": invalid_config,
            "binding_hash": canonical_json_sha256(invalid_config),
            "effective_from": prepared_day,
            "effective_to": prepared_day,
        }
    )
    repo.bindings[invalid_qmt_binding.binding_id] = invalid_qmt_binding
    invalid_rebase_config = dict(invalid_rebase_binding.binding_config_json)
    invalid_rebase_config.pop("miniqmt_quote_control")
    invalid_rebase_config["metadata"] = {
        **dict(invalid_rebase_config.get("metadata") or {}),
        "purpose": "miniqmt_unattended_daily_roll_forward",
        "extends_binding_id": "simbind_previous_day_source",
        "manifest_identity_source": "strategy_package_current_manifest",
    }
    invalid_rebase_binding = invalid_rebase_binding.model_copy(
        update={
            "binding_config_json": invalid_rebase_config,
            "binding_hash": canonical_json_sha256(invalid_rebase_config),
            "effective_from": next_trade_day,
            "effective_to": next_trade_day,
        }
    )
    repo.bindings[invalid_rebase_binding.binding_id] = invalid_rebase_binding
    release_count_before = len(repo.releases)
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    selection.package_repository.manifest_sha256 = "manifest_changed_before_invalid_rebase"
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding.strategy_id: _position_context(portfolio_id="portfolio_valid_local"),
                valid_qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_valid_qmt"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=False,
    )

    by_strategy_id = {item.strategy_id: item for item in result.results}
    invalid_result = by_strategy_id[invalid_qmt_binding.strategy_id]
    invalid_rebase_result = by_strategy_id[invalid_rebase_binding.strategy_id]
    invalid_run = repo.get_simulation_daily_run_by_key(
        strategy_id=invalid_qmt_binding.strategy_id,
        binding_id=invalid_qmt_binding.binding_id,
        trade_date=next_trade_day,
    )
    assert result.total_bindings == 4
    assert result.failed_count == 2
    assert result.planned_count == 2
    assert invalid_result.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert invalid_result.error["context"]["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"
    assert invalid_result.error["context"]["legacy_fallback"] is False
    assert invalid_result.execution_result is None
    assert invalid_run is not None
    assert invalid_run.status is SimulationDailyRunStatus.FAILED_RETRYABLE
    assert invalid_rebase_result.status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert invalid_rebase_result.error["context"]["reason_code"] == "MINIQMT_B0_QUOTE_V2_BINDING_REQUIRED"
    assert invalid_rebase_result.error["context"]["legacy_fallback"] is False
    assert by_strategy_id[local_binding.strategy_id].status == "PLANNED"
    assert by_strategy_id[valid_qmt_binding.strategy_id].status == "PLANNED"
    assert len(repo.releases) == release_count_before
    assert repo.list_simulation_release_bindings(
        strategy_id=invalid_qmt_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        limit=10,
    ) == [invalid_qmt_binding]
    assert repo.list_simulation_release_bindings(
        strategy_id=invalid_rebase_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        limit=10,
    ) == [invalid_rebase_binding]


def test_scheduler_rolls_forward_local_and_miniqmt_when_backend_filter_is_omitted() -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    local_binding = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    qmt_binding = qmt_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    repo.bindings[local_binding.binding_id] = local_binding
    repo.bindings[qmt_binding.binding_id] = qmt_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding.strategy_id: _position_context(portfolio_id="portfolio_local_roll_all"),
                qmt_binding.strategy_id: _position_context(portfolio_id="portfolio_miniqmt_roll_all"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        submit=False,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert {item.broker_backend for item in result.results} == {
        SimulationBrokerBackend.LOCAL_SIM,
        SimulationBrokerBackend.MINIQMT_SIM,
    }
    rolled_bindings = [repo.get_simulation_release_binding(item.run.binding_id) for item in result.results]
    assert {binding.binding_config_json["metadata"]["purpose"] for binding in rolled_bindings} == {
        "localsim_unattended_daily_roll_forward",
        "miniqmt_unattended_daily_roll_forward",
    }


def test_scheduler_rolls_forward_new_localsim_strategy_without_manual_next_day_binding() -> None:
    release, local_binding_a, _, repo = _release_and_bindings()
    assert local_binding_a is not None
    local_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_new_localsim_package",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    for binding in (local_binding_a, local_binding_b):
        expired = binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
        repo.bindings[expired.binding_id] = expired
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={
                local_binding_a.strategy_id: _position_context(portfolio_id="portfolio_roll_a"),
                local_binding_b.strategy_id: _position_context(portfolio_id="portfolio_roll_b"),
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 2
    assert result.planned_count == 2
    assert {item.strategy_id for item in result.results} == {
        local_binding_a.strategy_id,
        local_binding_b.strategy_id,
    }
    assert all(
        item.run.binding_id not in {local_binding_a.binding_id, local_binding_b.binding_id} for item in result.results
    )


def test_scheduler_roll_forward_keeps_active_binding_when_limit_is_full() -> None:
    _, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    next_trade_day = TRADE_DATE + timedelta(days=1)
    active_binding = local_binding.model_copy(update={"effective_from": next_trade_day, "effective_to": next_trade_day})
    repo.bindings[active_binding.binding_id] = active_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(
            repo.get_strategy_runtime_release(active_binding.release_id), candidates=_candidate_rows()
        ),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={active_binding.strategy_id: _position_context(portfolio_id="portfolio_limit_full")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        limit=1,
    )

    assert result.total_bindings == 1
    assert result.results[0].binding_id == active_binding.binding_id
    assert result.planned_count == 1
    assert len(repo.bindings) == 2


def test_repository_latest_binding_ignores_future_manual_binding_for_roll_forward() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    prepared_day = TRADE_DATE
    next_trade_day = TRADE_DATE + timedelta(days=1)
    future_day = next_trade_day + timedelta(days=3)
    expired = local_binding.model_copy(update={"effective_from": prepared_day, "effective_to": prepared_day})
    future = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id=local_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_name="Future LocalSim 2026-05-25",
    ).model_copy(update={"effective_from": future_day, "effective_to": future_day})
    repo.bindings[expired.binding_id] = expired
    repo.bindings[future.binding_id] = future
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_strategy_id={local_binding.strategy_id: _position_context(portfolio_id="portfolio_future_binding")}
        ),
    )

    result = scheduler.run_once(
        trade_date=next_trade_day,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.planned_count == 1
    assert result.results[0].run is not None
    assert result.results[0].run.binding_id not in {expired.binding_id, future.binding_id}
    rolled = repo.get_simulation_release_binding(result.results[0].run.binding_id)
    assert rolled.effective_from == next_trade_day
    assert rolled.binding_config_json["metadata"]["extends_binding_id"] == expired.binding_id


def test_scheduler_passes_release_selection_runtime_config_to_selection_service() -> None:
    release_selection_config = {
        "selection_artifact_config": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2026-05-20",
            "include_reference_price": True,
            "artifact_reuse": "same_trade_date_config_hash",
        },
        "runtime_profile": {
            "selection": {"top_k": 2},
            "tradability": {"exclude_suspended": False},
        },
    }
    release, local_binding, _, repo = _release_and_bindings(
        release_metadata={"selection_runtime_config": release_selection_config}
    )
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_release_config")}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    assert result.planned_count == 1
    assert len(fake_selection.calls) == 1
    assert fake_selection.calls[0]["runtime_config"] == release_selection_config


def test_scheduler_reuses_existing_plans_on_restart_without_reselection_or_resubmit() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    paper_repo = InMemoryPaperTradingV2Repository()
    context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=context_provider,
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    assert first.results[0].status == "SUBMITTED"
    assert first.results[0].run.run_payload_json["broker_called"] is True
    run_id = first.results[0].run.run_id
    persisted_order_count = len(paper_repo.list_orders_for_run(run_id))
    assert persisted_order_count == len(first.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    payload = first.results[0].run.run_payload_json
    assert payload["local_sim_persistence"]["status"] == "PERSISTED"
    assert payload["local_sim_persistence"]["fill_count"] == len(paper_repo.list_fills_for_run(run_id))
    assert payload["strategy_performance"]["cash"] < 100_000
    assert payload["strategy_performance"]["positions"]
    run_events = paper_repo.list_run_events("portfolio_shared", run_id=run_id)
    success_event = next(event for event in run_events if event["event_type"] == "RUN_SUCCEEDED")
    assert success_event["context"]["source"] == "simulation_runtime_local_sim"
    assert success_event["context"]["simulation_run_id"] == run_id
    assert success_event["context"]["fill_count"] == len(paper_repo.list_fills_for_run(run_id))

    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert restarted.reused_count == 1
    assert len(fake_selection.calls) == 1
    assert context_provider.calls == [local_binding.binding_id]
    assert len(paper_repo.list_orders_for_run(run_id)) == persisted_order_count
    assert restarted.results[0].execution_plan.plan_id == first.results[0].execution_plan.plan_id


def test_scheduler_submits_existing_local_plan_after_restart_when_broker_was_not_called() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    plan_only_context = StaticSimulationRunContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
            )
        }
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=plan_only_context,
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    restarted_context = scheduler.context_provider

    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert restarted.results[0].status == "SUBMITTED"
    assert len(fake_selection.calls) == 1
    assert restarted_context.calls == [local_binding.binding_id]
    run_id = restarted.results[0].run.run_id
    assert len(paper_repo.list_orders_for_run(run_id)) == len(planned.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    assert restarted.results[0].run.run_payload_json["broker_called"] is True
    assert restarted.results[0].run.run_payload_json["broker_order_handles"][0]["backend_id"] == "local_sim"


def test_scheduler_recovers_submitting_local_plan_when_broker_was_not_called() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_shared",
                    release=release,
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    interrupted = repo.update_simulation_daily_run(
        planned.results[0].run.run_id,
        status=SimulationDailyRunStatus.SUBMITTING,
        payload_patch={"last_stage": "SUBMITTING"},
    )
    assert interrupted.run_payload_json.get("broker_called") is None

    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = CountingContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_shared",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert recovered.results[0].status == "SUBMITTED"
    assert recovered.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    run_id = recovered.results[0].run.run_id
    assert len(paper_repo.list_orders_for_run(run_id)) == len(planned.results[0].execution_plan.intents)
    assert paper_repo.list_fills_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    assert recovered.results[0].run.run_payload_json["broker_called"] is True
    assert recovered.results[0].run.run_payload_json["last_stage"] == "SUCCEEDED"


def test_scheduler_fails_localsim_submit_without_durable_execution_snapshot() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(
                    portfolio_id="portfolio_shared",
                    local_broker=FakeLocalSimBroker(),
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    runs = repo.list_simulation_daily_runs(
        trade_date=TRADE_DATE,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        limit=10,
    )

    assert result.failed_count == 1
    assert result.results[0].error["context"]["run_id"] == runs[0].run_id
    assert runs[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert runs[0].run_payload_json["broker_called"] is False
    assert runs[0].run_payload_json["submit_failure"]["context"]["economic_commit_staged"] is False
    assert "local_sim_persistence" not in runs[0].run_payload_json
    assert runs[0].run_payload_json["submit_failure"]["stage"] == "LOCAL_SIM_PERSISTENCE_SNAPSHOT_MISSING"


def test_scheduler_fails_closed_when_localsim_cash_context_is_missing() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    broker = FakeLocalSimBroker()
    context = SimulationRunContext(
        portfolio_id="portfolio_missing_cash",
        current_positions={},
        current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
        local_broker=broker,
        top_k=1,
        cash=None,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    diagnostic = latest_run.run_payload_json["local_sim_retry_diagnostics"]

    assert result.failed_count == 1
    assert result.results[0].error["context"]["reason_code"] == "LOCALSIM_CASH_CONTEXT_MISSING"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert diagnostic["stage"] == "LOCAL_SIM_CASH_CONTEXT_MISSING"
    assert diagnostic["context"]["reason_code"] == "LOCALSIM_CASH_CONTEXT_MISSING"
    assert latest_run.run_payload_json["broker_called"] is False
    assert broker.submitted == []


def test_scheduler_localsim_runs_sells_before_buys_and_preserves_dependent_orders() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    positions = {
        "000003.SZ": PositionLot(
            portfolio_id="portfolio_cash_fit",
            symbol="000003.SZ",
            quantity=1200,
            available_quantity=1200,
            avg_cost=10.0,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_cash_fit",
        release=release,
        cash=50.0,
        positions=positions,
        paper_repository=paper_repo,
    )
    context = replace(context, top_k=2)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    payload = latest_run.run_payload_json["local_sim_cash_fit"]
    assert result.failed_count == 0, (
        result.results[0].error,
        [
            (intent.symbol, intent.side.value, intent.order_quantity)
            for intent in (
                repo.get_execution_plan(latest_run.execution_plan_id).intents if latest_run.execution_plan_id else ()
            )
        ],
    )
    submitted = result.results[0].execution_plan.intents
    assert result.results[0].status == "SUBMITTED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.run_payload_json["last_stage"] == "SUCCEEDED"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED"
    assert payload["status"] == "SELL_FIRST_DEPENDENCY_ORDERED"
    assert payload["sell_intent_count"] == 1
    assert payload["buy_intent_count"] == 2
    assert payload["dependent_buy_count"] == 2
    assert "skipped_buy_count" not in payload
    assert "skipped_buy_intents" not in payload
    assert [intent.side for intent in submitted] == [OrderSide.SELL, OrderSide.BUY, OrderSide.BUY]
    assert submitted[0].symbol == "000003.SZ"
    assert sorted(fill["quantity"] for fill in paper_repo.list_fills_for_run(latest_run.run_id)) == [201, 1000, 1200]
    assert "000003.SZ" not in context.local_broker.query_positions()


def test_scheduler_rebuilds_localsim_insufficient_cash_failure_with_fresh_context() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    fake_selection = FakeSelectionService(release, candidates=_candidate_rows())
    initial_context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_rebuild_cash_fit",
        release=release,
        cash=50.0,
        positions={
            "000003.SZ": PositionLot(
                portfolio_id="portfolio_rebuild_cash_fit",
                symbol="000003.SZ",
                quantity=1200,
                available_quantity=0,
                avg_cost=10.0,
                trade_date=TRADE_DATE,
            )
        },
    )
    initial_context = replace(initial_context, top_k=2)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: initial_context}),
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "submit_failure": {
                "stage": "LOCAL_SIM_SUBMIT_FAILED",
                "type": "BrokerRejectedError",
                "message": "LocalSim ledger rejected the order",
                "context": {"cause": "insufficient cash for buy fill", "cause_code": "RISK_RULE_ERROR"},
            },
            "broker_called": False,
        },
    )

    paper_repo = InMemoryPaperTradingV2Repository()
    recovered_context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_rebuild_cash_fit",
        release=release,
        cash=50.0,
        positions={
            "000003.SZ": PositionLot(
                portfolio_id="portfolio_rebuild_cash_fit",
                symbol="000003.SZ",
                quantity=500,
                available_quantity=500,
                avg_cost=10.0,
                trade_date=TRADE_DATE - timedelta(days=1),
            )
        },
        paper_repository=paper_repo,
    )
    recovered_context = replace(recovered_context, top_k=2)
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: recovered_context}
    )

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.execution_plan_id != failed_run.execution_plan_id
    assert latest_run.run_payload_json["rebuilt_failure_backend"] == SimulationBrokerBackend.LOCAL_SIM.value
    assert latest_run.run_payload_json["local_sim_cash_fit"]["status"] == "SELL_FIRST_DEPENDENCY_ORDERED"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_CAPACITY_RESIDUAL"
    terminalization = latest_run.run_payload_json["local_sim_capacity_residual_terminalization"]
    assert terminalization["reason"] == "broker_execution_cash_limited_buy_residual"
    assert terminalization["capital_residual_count"] > 0
    assert terminalization["schedule_residual_count"] == 0
    assert terminalization["residual_orders"]
    assert len(fake_selection.calls) == 2
    assert paper_repo.list_fills_for_run(latest_run.run_id)


def test_scheduler_terminalizes_historical_schedule_residual_instead_of_success() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    policy = {
        "policy_id": "exec_policy_twap_historical_residual",
        "policy_sha256": "exec_policy_hash_twap_historical_residual",
        "policy_json": {
            "algo_code": "TWAP",
            "algo_config": {"allow_partial_fill": True, "split_count": 6},
        },
    }
    context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_historical_schedule_residual",
        release=release,
        cash=100_000,
        positions={},
        paper_repository=paper_repo,
        execution_policy=policy,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    latest = repo.get_simulation_daily_run(result.results[0].run.run_id)
    terminalization = latest.run_payload_json["local_sim_capacity_residual_terminalization"]

    assert result.results[0].status == "LOCALSIM_EXECUTION_RESIDUAL_TERMINAL"
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_RESIDUAL"
    assert terminalization["reason"] == "historical_execution_schedule_residual"
    assert terminalization["capital_residual_count"] == 0
    assert terminalization["schedule_residual_count"] > 0
    assert terminalization["residual_orders"][0]["classification"] == "SCHEDULE_RESIDUAL_AT_HISTORICAL_CLOSE"
    outbox = latest.run_payload_json["local_sim_projection_outbox_v1"]
    assert outbox["projection_payload"]["paper_error"]["code"] == "LOCALSIM_HISTORICAL_EXECUTION_RESIDUAL"


def test_scheduler_marks_localsim_buy_only_retry_failure_with_actionable_context() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_buy_only_retry",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    assert planned_run is not None
    plan = planned.results[0].execution_plan
    assert plan is not None
    assert all(intent.side == OrderSide.BUY for intent in plan.intents)
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "broker_called": False,
            "no_rebalance_required": False,
        },
    )

    class FailingLocalSimContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            raise DataUnavailableError(
                "LocalSim could not load minute market data",
                context={
                    "strategy_id": binding.strategy_id,
                    "binding_id": binding.binding_id,
                    "trade_date": trade_date.isoformat(),
                    "source": "TDX_REALTIME",
                },
            )

    scheduler.context_provider = FailingLocalSimContextProvider()

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert retried.failed_count == 1
    assert retried.results[0].error["context"]["stage"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["failed_intents"] == len(plan.intents)
    diagnostics = latest_run.run_payload_json["local_sim_retry_diagnostics"]
    assert diagnostics["buy_intent_count"] == len(plan.intents)
    assert diagnostics["sell_intent_count"] == 0
    assert diagnostics["next_action"]

    detail = SimulationRuntimeOpsService(repository=repo).get_run_detail(latest_run.run_id)
    assert detail["run"]["broker_context"]["local_sim_retry_diagnostics"]["plan_id"] == plan.plan_id
    assert detail["run"]["errors"][0]["source"] == "local_sim_submit_failure"
    assert detail["run"]["errors"][0]["code"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"


def test_scheduler_clears_localsim_retry_diagnostics_after_successful_retry() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    observed = datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai"))
    candidates = _candidate_rows()[:1]
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_retry_clears_diagnostics",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                    top_k=1,
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    planned_run = planned.results[0].run
    assert planned_run is not None
    plan = planned.results[0].execution_plan
    assert plan is not None
    failed_run = repo.update_simulation_daily_run(
        planned_run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": "FAILED_RETRYABLE",
            "broker_called": False,
            "no_rebalance_required": False,
        },
    )

    class FailingLocalSimContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            raise DataUnavailableError(
                "LocalSim could not load minute market data",
                context={"trade_date": trade_date.isoformat()},
            )

    scheduler.context_provider = FailingLocalSimContextProvider()
    failed_retry = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=observed,
    )
    assert failed_retry.failed_count == 1
    retry_payload = repo.get_simulation_daily_run(failed_run.run_id).run_payload_json
    assert retry_payload["local_sim_retry_diagnostics"]["stage"] == "LOCAL_SIM_MARKET_DATA_UNAVAILABLE"

    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={
            local_binding.binding_id: _local_sim_context_with_real_broker(
                portfolio_id="portfolio_retry_clears_diagnostics",
                release=release,
                paper_repository=paper_repo,
            )
        }
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=observed + timedelta(minutes=1),
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "SUBMITTED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.execution_plan_id == plan.plan_id
    assert "submit_failure" not in latest_run.run_payload_json
    assert "local_sim_retry_diagnostics" not in latest_run.run_payload_json
    assert paper_repo.list_fills_for_run(latest_run.run_id)


def _legacy_scheduler_submits_miniqmt_fake_broker_batch_and_reuses_after_restart() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.results[0].error is None, submitted.results[0].error
    assert submitted.results[0].status == "RECONCILED", (
        submitted.results[0].status,
        submitted.results[0].run.run_payload_json,
        broker.place_order_payloads,
    )
    assert submitted.submitted_count == 1, submitted.results[0]
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert submitted.results[0].sync_result["positions_seen"] == 1
    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run.run_payload_json["strategy_performance"]["broker_backend"] == "minqmt_sim"
    assert "succeeded_with_capacity_residual" not in latest_run.run_payload_json
    assert "miniqmt_capacity_residual_observability" not in latest_run.run_payload_json
    assert "succeeded_with_capacity_residual" not in latest_run.run_payload_json["strategy_performance"]
    assert {position["symbol"] for position in latest_run.run_payload_json["strategy_performance"]["positions"]} >= {
        "000003.SZ"
    }
    assert submitted.results[0].run.run_payload_json["qmt_batch_status"] == "SUCCEEDED"
    assert [call["strategy_name"] for call in broker.place_order_payloads] == [
        qmt_binding.strategy_name,
        qmt_binding.strategy_name,
    ]
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def _legacy_scheduler_miniqmt_restart_syncs_before_submit_and_reconciles_after_submit() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_restart",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_restart",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=managed_order_service,
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert submitted.results[0].status == "RECONCILED"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert submitted.results[0].sync_result["positions_seen"] == 1
    assert submitted.results[0].reconciliation_result["run"]["status"] == "SUCCEEDED"
    assert (
        repo.get_simulation_daily_run(submitted.results[0].run.run_id).run_payload_json["strategy_performance"]["nav"]
        > 0
    )
    assert submitted.results[0].run.run_payload_json["sync_before_submit"]["orders_seen"] == 0
    assert submitted.results[0].run.run_payload_json["reconcile_after_submit"]["broker_quantities"] == {"000003.SZ": 77}
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def _legacy_scheduler_miniqmt_preflight_failure_stays_retryable_and_can_resubmit() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_preflight_retry",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_preflight_retry",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)

    assert failed.results[0].status == "BROKER_SUBMIT_FAILED_RECONCILED"
    assert failed_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert failed_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert failed_run.run_payload_json["broker_called"] is True
    assert failed_run.run_payload_json["submitted_intents"] == 1
    assert failed_run.run_payload_json["failed_intents"] == 1
    assert (
        failed_run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"]
        == "SKIPPED_INSUFFICIENT_CAPITAL"
    )
    reconciliation = failed_run.run_payload_json["reconcile_after_submit"]
    assert reconciliation["submit_result_gate"]["status"] == "SUCCEEDED"
    assert reconciliation["submit_result_gate"]["reason"] == "miniqmt_capacity_residual_skipped_and_reconciled"
    assert reconciliation["submit_result_gate"]["succeeded_with_capacity_residual"] is True
    assert reconciliation["qmt_batch_residual_summary"]["capacity_residual_count"] == 1
    observability = failed_run.run_payload_json["miniqmt_capacity_residual_observability"]
    assert failed_run.run_payload_json["succeeded_with_capacity_residual"] is True
    assert failed_run.run_payload_json["capacity_residual_count"] == 1
    assert failed_run.run_payload_json["capacity_residual_failed_intents"] == 1
    assert observability["succeeded_with_capacity_residual"] is True
    assert observability["failed_intents"] == 1
    assert observability["capacity_residual_count"] == 1
    assert observability["alert"]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    assert (
        failed_run.run_payload_json["simulation_alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    )
    performance = failed_run.run_payload_json["strategy_performance"]
    assert performance["succeeded_with_capacity_residual"] is True
    assert performance["capacity_residual_count"] == 1
    assert performance["capacity_residual_failed_intents"] == 1
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account, cash=Decimal("100000")))
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)

    assert recovered.results[0].status == "REUSED_EXISTING_PLAN"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert recovered_run.run_payload_json["succeeded_with_capacity_residual"] is True
    assert len(broker.place_order_payloads) == 1
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]


def _legacy_scheduler_keeps_miniqmt_capacity_residual_pending_when_open_orders_remain() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_open_order_capacity",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_open_order_capacity",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000001",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=max(int(accepted_intent.quantity) - 20, 0),
            order_status=STATUS_PART_SUCC,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="partially filled but still open at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.SUBMITTING)

    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    reconciliation = latest.run_payload_json["reconcile_after_submit"]

    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert reconciliation["submit_result_gate"]["status"] == "PENDING"
    assert reconciliation["submit_result_gate"]["reason"] == "miniqmt_open_orders_pending_after_reconciliation"
    assert reconciliation["submit_result_gate"]["pending_open_orders"] is True
    assert reconciliation["open_order_evidence"]["open_order_count"] == 1
    assert reconciliation["open_order_evidence"]["open_orders"][0]["order_status"] == STATUS_PART_SUCC
    assert len(broker.place_order_payloads) == 1

    polled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    still_pending = repo.get_simulation_daily_run(run.run_id)

    assert polled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert still_pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert len(broker.place_order_payloads) == 1


def test_scheduler_miniqmt_open_order_evidence_excludes_terminal_xtquant_statuses() -> None:
    _release, _, qmt_binding, _repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for status in (STATUS_CANCELLED, STATUS_FILLED, STATUS_REJECTED):
        qmt_repo.upsert_order_ledger(
            OrderLedgerRecord(
                intent_id=f"intent_terminal_{status}",
                strategy_id=qmt_binding.strategy_id,
                strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
                qmt_order_id=f"terminal_{status}",
                symbol="000003.SZ",
                order_type=SELL_ORDER_TYPE,
                order_volume=100,
                traded_volume=50 if status != STATUS_FILLED else 100,
                order_status=status,
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                status_msg=f"terminal xtquant status {status}",
                order_remark=f"remark_terminal_{status}",
            )
        )

    evidence = SimulationLifecycleScheduler._miniqmt_open_order_evidence(
        binding=qmt_binding,
        run=SimpleNamespace(trade_date=TRADE_DATE, run_payload_json={}),
        context=SimulationRunContext(current_positions={}, qmt_ledger_repository=qmt_repo),
    )

    assert evidence["open_order_count"] == 0
    assert evidence["open_orders"] == []


def _legacy_scheduler_post_close_terminalizes_miniqmt_capacity_residual_without_fake_success() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_capacity",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_capacity",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.total_bindings == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "succeeded_with_capacity_residual"
    assert terminalization["reason"] == "miniqmt_post_close_capacity_residual_skipped"
    assert terminalization["residual_summary"]["capacity_residual_count"] == 1
    assert terminalization["miniqmt_capacity_residual_observability"]["succeeded_with_capacity_residual"] is True
    assert latest.run_payload_json["succeeded_with_capacity_residual"] is True
    assert latest.run_payload_json["capacity_residual_count"] == 1
    assert latest.run_payload_json["capacity_residual_failed_intents"] == 1
    assert post_close.stale_run_results[0]["succeeded_with_capacity_residual"] is True
    assert post_close.stale_run_results[0]["alert"]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
    assert latest.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert len(broker.place_order_payloads) == 1


def _legacy_scheduler_post_close_terminalizes_miniqmt_open_orders_as_failed_terminal() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_open_order",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_open_order",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000002",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted but still open at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    pending = repo.get_simulation_daily_run(run.run_id)
    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING

    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_terminal_after_close"
    assert terminalization["reason"] == "miniqmt_post_close_open_orders_terminal_failed"
    assert terminalization["open_order_evidence"]["open_order_count"] == 1
    assert len(broker.place_order_payloads) == 1


def _legacy_scheduler_post_close_reconciles_fresh_broker_before_terminal_status() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_filled",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_filled",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    open_order_id = "900000003"
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id=open_order_id,
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted at submit and filled later",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    pending = repo.get_simulation_daily_run(run.run_id)

    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert pending.run_payload_json["reconcile_after_submit"]["open_order_evidence"]["open_order_count"] == 1

    snapshot_client._orders = [
        {
            "order_id": open_order_id,
            "order_sysid": "sys_900000003",
            "stock_code": accepted_intent.symbol,
            "order_type": accepted_intent.order_type,
            "order_volume": accepted_intent.quantity,
            "price_type": accepted_intent.price_type,
            "price": "8.0",
            "traded_volume": accepted_intent.quantity,
            "traded_price": "8.0",
            "order_status": STATUS_FILLED,
            "status_msg": "filled by broker before close",
            "strategy_name": accepted_intent.strategy_name,
            "order_remark": accepted_intent.order_remark,
        }
    ]
    snapshot_client._trades = [
        {
            "traded_id": "trade_900000003",
            "stock_code": accepted_intent.symbol,
            "order_type": accepted_intent.order_type,
            "traded_time": "14:30:00",
            "traded_price": "8.0",
            "traded_volume": accepted_intent.quantity,
            "traded_amount": str(Decimal("8.0") * Decimal(accepted_intent.quantity)),
            "order_id": open_order_id,
            "order_sysid": "sys_900000003",
            "commission": "0",
            "strategy_name": accepted_intent.strategy_name,
            "order_remark": accepted_intent.order_remark,
        }
    ]
    before_post_close_calls = len(snapshot_client.calls)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert latest.status != SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert snapshot_client.calls[before_post_close_calls:] == ["orders:False", "trades", "positions"]
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["reason"] == "miniqmt_post_close_batch_succeeded"
    assert terminalization["previous_open_order_evidence"]["open_order_count"] == 1
    assert terminalization["open_order_evidence"]["open_order_count"] == 0
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"
    assert terminalization["fresh_reconcile"]["sync_evidence"]["orders_seen"] == 1
    assert terminalization["fresh_reconcile"]["sync_evidence"]["trades_seen"] == 1
    assert terminalization["fresh_reconcile"]["sync_payload_key"] == "sync_after_submit"
    assert terminalization["fresh_reconcile"]["reconcile_payload_key"] == "reconcile_after_submit"


def _legacy_scheduler_post_close_reconcile_failure_is_loud() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_loud",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_loud",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FailingQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000004",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="accepted but broker query fails at close",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    assert reconciled.results[0].status == "RECONCILIATION_PENDING_OPEN_ORDERS"
    snapshot_client.fail = True

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
            submit=True,
            as_of_time=datetime(2026, 5, 21, 15, 5),
            raise_on_error=True,
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_POST_CLOSE_FRESH_RECONCILE_FAILED"
    assert exc_info.value.context["run_id"] == run.run_id
    assert exc_info.value.context["binding_id"] == qmt_binding.binding_id
    assert exc_info.value.context["error_type"] == "RuntimeError"
    assert exc_info.value.context["error_message"] == "broker snapshot unavailable"
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest.run_payload_json["reconcile_after_submit"]["open_order_evidence"]["open_order_count"] == 1
    assert "miniqmt_post_close_terminalization" not in latest.run_payload_json


def _legacy_scheduler_post_close_terminalizes_dependent_buy_residual_as_retryable_failure() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("3500"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_post_close_dependent",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_post_close_dependent",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    assert (
        run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"]
        == "SELL_PROCEEDS_REQUIRED"
    )
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )
    latest = repo.get_simulation_daily_run(run.run_id)

    assert post_close.stale_terminalized_count == 1
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_retryable_after_close"
    assert terminalization["reason"] == "miniqmt_post_close_buy_residual_unresolved"
    assert terminalization["residual_summary"]["dependent_buy_count"] == 1
    assert len(broker.place_order_payloads) == 1


def _legacy_scheduler_rebuilds_side_effect_free_miniqmt_failed_plan_with_fresh_context() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_stale_000003_rebuild",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_stale_000003_rebuild",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[])
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE])
    manifest = _score_weighted_manifest(release)

    def context_with_positions(positions: dict[str, PositionLot]) -> SimulationRunContext:
        return SimulationRunContext(
            portfolio_id="portfolio_qmt",
            current_positions=positions,
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            manifest=manifest,
            managed_order_service=QmtManagedOrderService(
                repository=qmt_repo,
                broker=broker,  # type: ignore[arg-type]
                calendar_provider=calendar,
            ),
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=FakeQmtSnapshotClient(positions=[]),
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=calendar,
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[],
            cash=100000.0,
        )

    contexts = [
        context_with_positions(
            {
                "000003.SZ": PositionLot(
                    portfolio_id="portfolio_qmt",
                    symbol="000003.SZ",
                    quantity=77,
                    available_quantity=77,
                    avg_cost=8.0,
                    trade_date=date(2026, 5, 20),
                )
            }
        ),
        context_with_positions({}),
    ]

    class RotatingContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            return contexts.pop(0) if contexts else context_with_positions({})

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=RotatingContextProvider(),
    )

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    failed_plan = repo.get_execution_plan(failed_run.execution_plan_id)

    assert failed.results[0].status == "BROKER_PRECHECK_FAILED", failed.results[0].error
    assert failed_run.run_payload_json["qmt_batch_status"] == "PREFLIGHT_FAILED"
    assert failed_run.run_payload_json["broker_called"] is False
    assert {intent.symbol for intent in failed_plan.intents if intent.side == OrderSide.SELL} == {"000003.SZ"}

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    retried_run = repo.get_simulation_daily_run(failed_run.run_id)
    retried_plan = repo.get_execution_plan(retried_run.execution_plan_id)

    assert retried.results[0].execution_plan.plan_id == retried_plan.plan_id
    assert retried_plan.plan_id != failed_plan.plan_id
    assert retried_run.run_payload_json["rebuilt_after_side_effect_free_failure"] is True
    assert retried_run.run_payload_json["rebuilt_from_execution_plan_id"] == failed_plan.plan_id
    rebuild_receipt = retried_run.run_payload_json["miniqmt_side_effect_free_rebuild"]
    assert rebuild_receipt["schema_version"] == "miniqmt_side_effect_free_plan_rebuild_v1"
    assert rebuild_receipt["source_execution_plan_id"] == failed_plan.plan_id
    assert rebuild_receipt["rebuilt_execution_plan_id"] == retried_plan.plan_id
    assert rebuild_receipt["broker_called"] is False
    assert {intent.symbol for intent in retried_plan.intents if intent.side == OrderSide.SELL} == set()
    assert "BATCH_INSUFFICIENT_BROKER_CAN_SELL" not in str(retried_run.run_payload_json["qmt_batch_result"])
    assert broker.place_order_payloads


def test_scheduler_rebuilds_b0_manifest_conflict_once_and_keeps_repeated_runtime_drift_loud() -> None:
    conflicts = {
        "code_sha256": {
            "expected": "53b579e4f5a273dd340354de5876baa8e15070fe52a70f90f7648e3d3dae6996",
            "received": "0f4702d27b3bd873b39fd8d2473f6a9cbd9a98965fe10d7fbf7534fa326b3100",
        }
    }
    payload = {
        "broker_called": False,
        "submitted_intents": 0,
        "failed_intents": 36,
        "submit_failure": {
            "type": "BrokerSubmitError",
            "stage": "ADAPTER",
            "message": "B0_QUOTE_V2 frozen build/schema manifest differs from runtime readback",
            "context": {
                "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
                "manifest_conflicts": conflicts,
                "broker_called": False,
            },
        },
    }

    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is True
    fingerprint = canonical_json_sha256(
        {
            "schema_version": "miniqmt_b0_manifest_conflict_rebuild_v1",
            "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
            "manifest_conflicts": conflicts,
        }
    )
    payload["miniqmt_side_effect_free_rebuild"] = {
        "schema_version": "miniqmt_side_effect_free_plan_rebuild_v1",
        "reason_code": "ADAPTIVE_IS_B0_QUOTE_V2_ASSIGNMENT_CONFLICT",
        "failure_fingerprint": fingerprint,
        "broker_called": False,
    }

    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is False

    payload["submit_failure"]["context"]["manifest_conflicts"] = {
        "code_sha256": {
            "expected": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "received": conflicts["code_sha256"]["received"],
        }
    }
    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is True

    payload["broker_called"] = True
    assert SimulationLifecycleScheduler._mini_qmt_batch_failed_without_broker_side_effect(payload) is False


def _legacy_scheduler_rejects_side_effect_free_failed_retry_outside_shared_window_without_broker_call() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_retry_window_gate",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_retry_window_gate",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[])
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE])
    manifest = _score_weighted_manifest(release)

    def context_with_positions(positions: dict[str, PositionLot]) -> SimulationRunContext:
        return SimulationRunContext(
            portfolio_id="portfolio_qmt",
            current_positions=positions,
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            manifest=manifest,
            managed_order_service=QmtManagedOrderService(
                repository=qmt_repo,
                broker=broker,  # type: ignore[arg-type]
                calendar_provider=calendar,
            ),
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=FakeQmtSnapshotClient(positions=[]),
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=calendar,
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[],
            cash=100000.0,
        )

    contexts = [
        context_with_positions(
            {
                "000003.SZ": PositionLot(
                    portfolio_id="portfolio_qmt",
                    symbol="000003.SZ",
                    quantity=77,
                    available_quantity=77,
                    avg_cost=8.0,
                    trade_date=date(2026, 5, 20),
                )
            }
        ),
        context_with_positions({}),
    ]

    class RotatingContextProvider:
        def load_context(self, *, runtime_release, binding, trade_date):
            return contexts.pop(0) if contexts else context_with_positions({})

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=RotatingContextProvider(),
    )

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    retried_run = repo.get_simulation_daily_run(failed_run.run_id)
    gate = retried_run.run_payload_json["submit_window_gate"]

    assert retried.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert retried_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert retried_run.run_payload_json["rebuilt_after_side_effect_free_failure"] is True
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["broker_called_before_rejection"] is False
    assert retried_run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


def _legacy_scheduler_keeps_deferred_miniqmt_buy_blocked_until_explicit_reconciliation_without_duplicate_sell() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("3500"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_dependent_buy",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_dependent_buy",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    first_run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    first_batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])

    assert first.results[0].status == "BROKER_SUBMIT_FAILED"
    assert first_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert first_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert first_run.run_payload_json["broker_called"] is True
    assert first_batch is not None
    assert first_batch.metadata["dependent_buy_deferred"] is True
    assert first_run.run_payload_json["reconcile_after_submit"]["submit_result_gate"]["status"] == "blocked"
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account_after_sell = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account_after_sell, cash=Decimal("100000")))
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    recovered_run = repo.get_simulation_daily_run(first_run.run_id)
    recovered_batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])

    submit_result_gate = recovered_run.run_payload_json["reconcile_after_submit"]["submit_result_gate"]
    assert second.results[0].status == "RECONCILIATION_WARNING"
    assert recovered_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert recovered_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.PARTIAL.value
    assert recovered_run.run_payload_json["qmt_batch_id"] == first_run.run_payload_json["qmt_batch_id"]
    assert recovered_run.run_payload_json["qmt_batch_result"]["runtime_evidence"]["source"] == (
        "simulation_runtime_event_loop_submit"
    )
    assert submit_result_gate["status"] == "blocked"
    assert submit_result_gate["reason"] == "miniqmt_broker_side_effect_requires_explicit_reconciliation"
    assert recovered_batch is not None
    assert recovered_batch.metadata["dependent_buy_deferred"] is True
    assert recovered_batch.metadata["dependent_buy_retry"] is True
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]


def test_scheduler_rejects_fresh_miniqmt_submit_outside_shared_window_without_broker_call() -> None:
    scheduler, repo, broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    gate = run.run_payload_json["submit_window_gate"]

    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["active_window"] is None
    blocked_plan = repo.get_execution_plan(run.execution_plan_id)
    assert gate["blocked_intent_count"] == len(blocked_plan.intents)
    assert gate["schedule_windows"] == list(
        compute_schedule_windows(trade_date=TRADE_DATE, as_of_time=datetime(2026, 5, 21, 16, 33))
    )
    assert run.run_payload_json["submit_failure"]["stage"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert run.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


def test_scheduler_rejects_fresh_localsim_submit_outside_shared_window_without_broker_call() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    broker = FakeLocalSimBroker()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(
                    portfolio_id="portfolio_local_window_gate",
                    local_broker=broker,
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    gate = run.run_payload_json["submit_window_gate"]

    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert gate["reason_code"] == MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW
    assert gate["broker_backend"] == SimulationBrokerBackend.LOCAL_SIM.value
    assert run.run_payload_json["broker_called"] is False
    assert broker.submitted == []


def _legacy_scheduler_allows_miniqmt_submit_inside_shared_window() -> None:
    scheduler, _repo, broker, _qmt_binding = _miniqmt_event_loop_test_scheduler(real_callback=True)

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )

    assert result.results[0].status == "RECONCILED"
    assert result.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert broker.place_order_payloads


def _legacy_scheduler_rejects_deferred_dependent_buy_replay_after_close_without_duplicate_buy() -> None:
    scheduler, repo, broker, qmt_binding, qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context(
        cash=Decimal("3500")
    )

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    first_run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]

    account_after_sell = qmt_repo.get_virtual_account(qmt_binding.strategy_id)
    qmt_repo.update_virtual_account(replace(account_after_sell, cash=Decimal("100000")))
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 16, 33),
    )
    blocked = repo.get_simulation_daily_run(first_run.run_id)

    assert second.results[0].status == "RECONCILIATION_WARNING"
    assert blocked.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "submit_window_gate" not in blocked.run_payload_json
    assert blocked.run_payload_json["reconcile_after_submit"]["submit_result_gate"]["reason"] == (
        "miniqmt_broker_side_effect_requires_explicit_reconciliation"
    )
    assert blocked.run_payload_json["broker_called"] is True
    assert [payload["order_type"] for payload in broker.place_order_payloads] == [SELL_ORDER_TYPE]
    batch = qmt_repo.get_order_batch(first_run.run_payload_json["qmt_batch_id"])
    assert batch is not None
    assert batch.batch_status == OrderBatchStatus.PARTIAL
    assert batch.metadata["dependent_buy_deferred"] is True


def _legacy_scheduler_polls_succeeded_miniqmt_run_for_late_broker_fill_sync() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    broker = FakeManagedOrderBroker(order_ids=[1082130454])
    broker.quotes["301369.SZ"] = {
        "source": "MINIQMT_REALTIME.broker_quote",
        "price": 180.08,
        "ask_price_1": 180.08,
        "ask_volume_1": 5000,
        "bid_price_1": 180.08,
        "bid_volume_1": 5000,
    }
    broker.positions = []
    snapshot_client = FakeQmtSnapshotClient(orders=[], trades=[], positions=[])
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )
    context = SimulationRunContext(
        portfolio_id="portfolio_qmt",
        current_positions={},
        current_prices={"301369.SZ": 180.08},
        managed_order_service=managed_order_service,
        qmt_ledger_repository=qmt_repo,
        qmt_sync_service=QmtStrategyLedgerSyncService(
            repository=qmt_repo,
            qmt_client=snapshot_client,
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(
            release,
            candidates=[
                SelectionCandidate(
                    symbol="301369.SZ",
                    score=0.99,
                    rank=1,
                    target_quantity=200,
                    target_weight=0.10,
                    reference_price=180.08,
                    reason="daily_strategy_buy_or_retain",
                )
            ],
        ),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    first_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert first_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert first_run.run_payload_json["broker_called"] is True
    assert first_run.run_payload_json["sync_after_submit"]["trades_seen"] == 0
    assert qmt_repo.list_position_lots(qmt_binding.strategy_id, symbol="301369.SZ") == []
    assert len(broker.place_order_payloads) == 1

    order_remark = broker.place_order_payloads[0]["order_remark"]
    snapshot_client._orders = [
        {
            "order_id": "1082130454",
            "order_sysid": "91",
            "stock_code": "301369.SZ",
            "order_type": 23,
            "order_volume": 200,
            "price_type": 5,
            "price": 180.08,
            "traded_volume": 200,
            "traded_price": 186.2,
            "order_status": 56,
            "strategy_name": qmt_binding.strategy_name,
            "order_remark": order_remark,
        }
    ]
    snapshot_client._trades = [
        {
            "traded_id": "1010000032502320",
            "stock_code": "301369.SZ",
            "order_type": 23,
            "traded_time": "092935",
            "traded_price": 186.2,
            "traded_volume": 200,
            "traded_amount": 37240,
            "order_id": "1082130454",
            "order_sysid": "91",
            "commission": 0,
            "strategy_name": qmt_binding.strategy_name,
            "order_remark": order_remark,
        }
    ]
    broker.positions = [{"stock_code": "301369.SZ", "quantity": 200, "can_sell": 0}]

    reconciled = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(first_run.run_id)
    lots = qmt_repo.list_position_lots(qmt_binding.strategy_id, symbol="301369.SZ")
    assert reconciled.reused_count == 1
    assert reconciled.results[0].sync_result["trades_inserted"] == 1
    assert reconciled.results[0].reconciliation_result["run"]["status"] == "SUCCEEDED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.run_payload_json["sync_before_submit"]["trades_inserted"] == 1
    assert (
        latest_run.run_payload_json["reconcile_after_submit"]["run"]["summary_json"]["sync_summary"]["trades_existing"]
        == 1
    )
    assert [(lot.open_trade_id, lot.remaining_quantity) for lot in lots] == [("1010000032502320", 200)]
    assert len(broker.place_order_payloads) == 1


def _legacy_scheduler_recovers_called_miniqmt_retryable_run_by_reconcile_only() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_recover_called",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_recover_called",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    def context_with_positions(quantity: int) -> SimulationRunContext:
        return SimulationRunContext(
            portfolio_id="portfolio_qmt",
            current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
            current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
            managed_order_service=managed_order_service,
            qmt_ledger_repository=qmt_repo,
            qmt_sync_service=QmtStrategyLedgerSyncService(
                repository=qmt_repo,
                qmt_client=snapshot_client,
                account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                trade_date=TRADE_DATE,
                calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
            ),
            qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
            broker_positions=[{"stock_code": "000003.SZ", "quantity": quantity, "can_sell": quantity}],
        )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: context_with_positions(1)}
        ),
    )
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    placed_count = len(broker.place_order_payloads)
    failed_run = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    assert failed.results[0].status == "RECONCILIATION_WARNING"
    assert failed_run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert failed_run.run_payload_json["broker_called"] is True
    assert failed_run.run_payload_json["qmt_batch_status"] == "SUCCEEDED"

    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={qmt_binding.binding_id: context_with_positions(77)}
    )
    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "RECONCILED"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovered_run.run_payload_json["reconcile_after_submit"]["run"]["status"] == "SUCCEEDED"
    assert len(broker.place_order_payloads) == placed_count


def _legacy_scheduler_recovers_miniqmt_retryable_run_with_order_ledger_evidence_by_reconcile_only() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_ledger_side_effect",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_ledger_side_effect",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    managed_order_service = QmtManagedOrderService(
        repository=qmt_repo,
        broker=broker,  # type: ignore[arg-type]
        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )
    context = SimulationRunContext(
        portfolio_id="portfolio_qmt",
        current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
        current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        managed_order_service=managed_order_service,
        qmt_ledger_repository=qmt_repo,
        qmt_sync_service=QmtStrategyLedgerSyncService(
            repository=qmt_repo,
            qmt_client=snapshot_client,
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
        broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    failed_run = repo.update_simulation_daily_run(
        submitted.results[0].run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "broker_called": False,
            "reconcile_after_submit": {
                "side_effect_evidence": {
                    "schema_version": "miniqmt_broker_side_effect_evidence_v1",
                    "broker_side_effect_count": 1,
                }
            },
        },
    )
    placed_count = len(broker.place_order_payloads)

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    recovered_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "RECONCILED"
    assert recovered_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert (
        recovered_run.run_payload_json["reconcile_after_submit"]["side_effect_evidence"]["broker_side_effect_count"] > 0
    )
    assert len(broker.place_order_payloads) == placed_count


def test_scheduler_terminalizes_stale_historical_miniqmt_planning_runs_before_today_tick() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions={},
                    current_prices={"000001.SZ": 10.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=InMemoryQmtStrategyLedgerRepository(),
                        broker=FakeManagedOrderBroker(),  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([TRADE_DATE]),
                    ),
                )
            }
        ),
    )
    stale = scheduler.run_once(
        trade_date=date(2026, 5, 20),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        created_by="codex_final_minqmt_multistrategy_dry_run_20260603",
    )
    stale_run = repo.get_simulation_daily_run(stale.results[0].run.run_id)

    today = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    terminalized = repo.get_simulation_daily_run(stale_run.run_id)

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["run_id"] == stale_run.run_id
    assert terminalized.status == SimulationDailyRunStatus.CANCELLED
    assert terminalized.run_payload_json["stale_active_terminalization"]["previous_status"] == "PLANNING_EXECUTION"
    assert terminalized.run_payload_json["stale_active_terminalization"]["had_broker_side_effect"] is False


def test_scheduler_recovery_failure_is_explicit_without_starving_current_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_recovery_isolation")}
        ),
    )

    def fail_stale_recovery(**_kwargs: Any) -> list[dict[str, Any]]:
        raise DataUnavailableError("stale MiniQMT evidence unavailable", context={"run_id": "stale-run"})

    monkeypatch.setattr(scheduler, "_terminalize_stale_miniqmt_active_runs", fail_stale_recovery)
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert len(result.results) == 1
    assert result.results[0].binding_id == qmt_binding.binding_id
    assert result.stale_terminalized_count == 0
    assert result.stale_recovery_failed_count == 1
    assert result.stale_run_results[0]["status"] == "RECOVERY_FAILED"
    assert result.stale_run_results[0]["error"] == {
        "type": "DataUnavailableError",
        "message": "stale MiniQMT evidence unavailable",
        "context": {"run_id": "stale-run"},
    }


def test_scheduler_publishes_b0_context_before_miniqmt_submit_callable() -> None:
    order: list[str] = []

    class _Activation:
        controller_factory = None
        quote_context_adapter = None

        def prepare_runtime_context(self, **kwargs: Any) -> dict[str, object]:
            order.append("context")
            assert kwargs["recovering_active"] is False
            return {"context_id": "context-before-submit"}

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=_Activation(),  # type: ignore[arg-type]
    )
    binding = SimpleNamespace(
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="binding-context-order",
        strategy_id="strategy-context-order",
    )
    plan = SimpleNamespace(
        plan_id="plan-context-order",
        target_trade_date=TRADE_DATE,
        plan_payload_json={"quote_control": {"binding": {}, "revision": {}, "assignments": []}},
        intents=(SimpleNamespace(symbol="000001.SZ"),),
    )
    run = SimpleNamespace(run_id="run-context-order")
    expected = SimpleNamespace(status="SUBMITTED")

    actual = scheduler._submit_execution_plan_with_timeout(
        build_result=None,
        binding=binding,
        run=run,
        plan=plan,
        context=SimpleNamespace(),
        mode="SIM",
        as_of_time=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
        submit_callable=lambda: order.append("submit") or expected,
    )

    assert actual is expected
    assert order == ["context", "submit"]


def test_scheduler_lifecycle_tick_pumps_kernel_product_callback_watchdog() -> None:
    calls: list[str] = []

    class _Activation:
        controller_factory = None
        quote_context_adapter = None

        def begin_lifecycle_epoch(self) -> None:
            calls.append("begin")

        def watchdog_tick(self) -> None:
            calls.append("watchdog")

    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_ingress_activation=_Activation(),  # type: ignore[arg-type]
    )
    scheduler._advance_miniqmt_quote_ingress_lifecycle()
    assert calls == ["begin", "watchdog"]


def test_scheduler_persists_b0_context_prepare_failure_before_broker_callable() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)

    class _FailingActivation:
        controller_factory = None
        quote_context_adapter = None

        def begin_lifecycle_epoch(self) -> None:
            return None

        def watchdog_tick(self) -> dict[str, object]:
            return {"status": "READY"}

        def prepare_runtime_context(self, **_kwargs: Any) -> dict[str, object]:
            raise quote_contract_error(
                QuoteContractReasonCode.TRADABILITY_DATA_INVALID,
                "authoritative MiniQMT context unavailable",
                context={"reason_code": "ADAPTIVE_IS_TRADABILITY_DATA_INVALID", "stage": "TRADABILITY"},
            )

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_context_failure")}
        ),
        miniqmt_quote_ingress_activation=_FailingActivation(),  # type: ignore[arg-type]
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    plan = plan.model_copy(
        update={
            "plan_payload_json": {
                **plan.plan_payload_json,
                "quote_control": {"binding": {}, "revision": {}, "assignments": []},
            }
        }
    )
    repo.execution_plans[plan.plan_id] = plan
    submit_called = False

    def submit_callable() -> Any:
        nonlocal submit_called
        submit_called = True
        raise AssertionError("broker callable must not run after context preparation failure")

    with pytest.raises(QuoteContractError, match="authoritative MiniQMT context unavailable"):
        scheduler._submit_execution_plan_with_timeout(
            build_result=None,
            binding=qmt_binding,
            run=run,
            plan=plan,
            context=SimpleNamespace(),
            mode="SIM",
            as_of_time=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
            submit_callable=submit_callable,
        )

    latest = repo.get_simulation_daily_run(run.run_id)
    assert submit_called is False
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.run_payload_json["last_stage"] == "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"
    diagnostic = latest.run_payload_json["miniqmt_quote_context_prepare_failure"]
    assert diagnostic["broker_callable_invoked"] is False
    assert diagnostic["broker_side_effect_evidence_before_attempt"] is False
    assert diagnostic["exception"]["reason_code"] == "ADAPTIVE_IS_TRADABILITY_DATA_INVALID"
    assert latest.run_payload_json["submit_failure"]["stage"] == "MINIQMT_QUOTE_CONTEXT_PREPARE_FAILED"


def test_scheduler_recovery_isolates_one_bad_durable_run_and_continues_peer_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scheduler = SimulationLifecycleScheduler(repository=InMemorySimulationRuntimeRepository())
    bad_run = SimpleNamespace(
        run_id="stale-bad",
        trade_date=TRADE_DATE,
        strategy_id="strategy-bad",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="binding-stale-bad",
        binding_hash="binding-hash-bad",
        release_id="release-stale-bad",
        release_hash="release-hash-bad",
        execution_plan_id="plan-stale-bad",
        execution_plan_hash="plan-hash-bad",
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        run_payload_json={},
    )
    good_run = SimpleNamespace(
        run_id="stale-good",
        trade_date=TRADE_DATE,
        strategy_id="strategy-good",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="binding-stale-good",
        binding_hash="binding-hash-good",
        release_id="release-stale-good",
        release_hash="release-hash-good",
        execution_plan_id="plan-stale-good",
        execution_plan_hash="plan-hash-good",
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        run_payload_json={},
    )
    monkeypatch.setattr(scheduler, "_is_post_close_reconcile_time", lambda **_kwargs: True)
    monkeypatch.setattr(
        scheduler.repository,
        "list_simulation_daily_runs",
        lambda **_kwargs: [bad_run, good_run],
    )
    monkeypatch.setattr(
        scheduler.repository,
        "claim_simulation_retry_attempt",
        lambda **kwargs: SimpleNamespace(
            run=bad_run if kwargs["run_id"] == bad_run.run_id else good_run,
            should_execute=True,
            reason="no_previous_failure",
            retry_entry=None,
            claim_token=None,
        ),
    )
    monkeypatch.setattr(
        scheduler.repository,
        "get_simulation_daily_run",
        lambda run_id: bad_run if run_id == bad_run.run_id else good_run,
    )
    monkeypatch.setattr(scheduler, "_record_simulation_retry_failure", lambda **kwargs: kwargs["run"])

    def recover_one(*, run: Any, as_of_time: datetime | None) -> dict[str, Any]:  # noqa: ARG001
        if run.run_id == "stale-bad":
            raise DataUnavailableError("one durable run is unreadable", context={"run_id": run.run_id})
        return {"run_id": run.run_id, "status": "SUCCEEDED"}

    monkeypatch.setattr(scheduler, "_post_close_terminalize_miniqmt_run", recover_one)
    results = scheduler._terminalize_post_close_miniqmt_runs(
        trade_date=TRADE_DATE,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 21, 16, 0, tzinfo=UTC),
    )

    assert [item["run_id"] for item in results] == ["stale-bad", "stale-good"]
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["reason_code"] == "SIMULATION_SCHEDULER_RECOVERY_ITEM_FAILED"
    assert results[1] == {"run_id": "stale-good", "status": "SUCCEEDED"}


def test_post_close_recovery_failure_does_not_masquerade_as_terminalized_current_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_post_close_failure")}
        ),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
    )
    run_id = first.results[0].run.run_id
    failure = {
        "schema_version": "simulation_scheduler_recovery_failure_v1",
        "terminalization_succeeded": False,
        "status": "RECOVERY_FAILED",
        "stage": "POST_CLOSE_MINIQMT_TERMINALIZATION",
        "reason_code": "SIMULATION_SCHEDULER_RECOVERY_ITEM_FAILED",
        "run_id": run_id,
    }
    monkeypatch.setattr(scheduler, "_terminalize_post_close_miniqmt_runs", lambda **_kwargs: [failure])

    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 16, 0, tzinfo=UTC),
    )

    assert second.results[0].status != "POST_CLOSE_TERMINALIZED"
    assert second.stale_recovery_failed_count == 1


def _legacy_scheduler_cross_day_terminalizes_side_effect_miniqmt_open_order_with_fresh_broker_reconcile() -> None:
    scheduler, repo, broker, _qmt_binding, qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context()

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    accepted_intent = next(
        intent
        for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
        if intent.submit_status == IntentSubmitStatus.ACCEPTED
    )
    qmt_repo.upsert_order_ledger(
        OrderLedgerRecord(
            intent_id=accepted_intent.intent_id,
            strategy_id=accepted_intent.strategy_id,
            strategy_name=accepted_intent.strategy_name,
            qmt_order_id="900000565",
            symbol=accepted_intent.symbol,
            order_type=accepted_intent.order_type,
            order_volume=accepted_intent.quantity,
            traded_volume=0,
            order_status=STATUS_OPEN_LIKE,
            account_id=accepted_intent.account_id,
            trade_date=accepted_intent.trade_date,
            price_type=accepted_intent.price_type,
            price=Decimal("8.0"),
            status_msg="cross-day open order remains at broker",
            order_remark=accepted_intent.order_remark,
        )
    )
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    today = scheduler.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["cross_day_terminalization"] is True
    assert today.stale_run_results[0]["status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert terminalization["reason"] == "miniqmt_post_close_open_orders_terminal_failed"
    assert terminalization["open_order_evidence"]["open_order_count"] == 1
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"
    assert len(broker.place_order_payloads) == len(submitted.results[0].execution_plan.intents)


def _legacy_scheduler_cross_day_terminalizes_side_effect_miniqmt_succeeded_batch_after_fresh_reconcile() -> None:
    scheduler, repo, _broker, _qmt_binding, _qmt_repo, _snapshot_client = _miniqmt_scheduler_with_ledger_context()

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    today = scheduler.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["miniqmt_post_close_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["cross_day_terminalization"] is True
    assert today.stale_run_results[0]["status"] == SimulationDailyRunStatus.SUCCEEDED.value
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert terminalization["reason"] == "miniqmt_post_close_batch_succeeded"
    assert terminalization["fresh_reconcile"]["source"] == "qmt_broker_snapshot_and_strategy_ledger"


def test_scheduler_terminalizes_stale_historical_localsim_planning_runs_before_today_tick() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_local_stale")}
        ),
    )
    stale = scheduler.run_once(
        trade_date=date(2026, 5, 20),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        created_by="unit_test_localsim_stale",
    )
    stale_run = repo.get_simulation_daily_run(stale.results[0].run.run_id)

    today = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )
    terminalized = repo.get_simulation_daily_run(stale_run.run_id)
    evidence = terminalized.run_payload_json["localsim_stale_active_terminalization"]

    assert today.stale_terminalized_count == 1
    assert today.stale_run_results[0]["run_id"] == stale_run.run_id
    assert today.stale_run_results[0]["reason_code"] == "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
    assert terminalized.status == SimulationDailyRunStatus.CANCELLED
    assert evidence["reason_code"] == "LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT"
    assert evidence["previous_status"] == "PLANNING_EXECUTION"
    assert evidence["had_broker_side_effect"] is False


def test_scheduler_cross_day_terminalizes_projected_localsim_active_run_without_retryable_intermediate() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_context_with_real_broker(
        portfolio_id="portfolio_local_stale_projected_active",
        release=release,
        paper_repository=paper_repo,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    run_id = submitted.results[0].run.run_id
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    repo.update_simulation_daily_run(run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    next_day = scheduler.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    latest = repo.get_simulation_daily_run(run_id)
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest.run_payload_json["localsim_post_close_terminalization"]["previous_status"] == (
        SimulationDailyRunStatus.INTRADAY_RUNNING.value
    )
    recovered = next(item for item in next_day.stale_run_results if item.get("run_id") == run_id)
    assert recovered["cross_day_terminalization"] is True
    assert recovered["durable_generation_readback"] is True
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == order_ids


def test_scheduler_post_close_terminalizes_localsim_persisted_active_run_with_shanghai_eod() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_post_close",
                    release=release,
                    paper_repository=paper_repo,
                )
            }
        ),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)

    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC),
    )
    latest = repo.get_simulation_daily_run(run.run_id)
    terminalization = latest.run_payload_json["localsim_post_close_terminalization"]

    assert post_close.stale_terminalized_count == 1
    assert post_close.results[0].status == "POST_CLOSE_TERMINALIZED"
    assert post_close.stale_run_results[0]["run_id"] == run.run_id
    assert post_close.stale_run_results[0]["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert terminalization["as_of_time"] == "2026-05-21T15:05:00+08:00"
    assert terminalization["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"
    assert terminalization["local_sim_persistence_status"] == "PERSISTED"


def test_localsim_post_close_uses_committed_generation_authority_over_terminal_history() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="portfolio_localsim_generation_authority",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    for as_of_time in (
        datetime(2026, 5, 21, 9, 32),
        datetime(2026, 5, 21, 9, 34),
        datetime(2026, 5, 21, 9, 36),
    ):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.TDX_REALTIME.value,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
            as_of_time=as_of_time,
        )
        broker = context.local_broker
        assert broker is not None
        if (
            repo.get_simulation_daily_run(planned.results[0].run.run_id).status
            == SimulationDailyRunStatus.INTRADAY_RUNNING
        ):
            context = _local_sim_realtime_context_with_real_broker(
                portfolio_id="portfolio_localsim_generation_authority",
                release=release,
                paper_repository=paper_repo,
                cash=float(broker.query_account().cash),
                positions=broker.query_positions(),
            )
            scheduler.context_provider = StaticSimulationRunContextProvider(
                by_binding_id={local_binding.binding_id: context}
            )

    run_id = planned.results[0].run.run_id
    finished = repo.get_simulation_daily_run(run_id)
    authority_states = tuple(repo.list_local_sim_execution_states(run_id, authoritative=True))
    assert authority_states and all(state.is_terminal for state in authority_states)
    raw_states = dict(finished.run_payload_json["local_sim_execution_states_v1"])
    for state in authority_states:
        historical = LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "algo_instance_id": f"historical_{state.algo_instance_id}",
                "state_hash": "",
                "created_at": (state.created_at - timedelta(seconds=1)).isoformat(),
                "updated_at": (state.updated_at - timedelta(seconds=1)).isoformat(),
            }
        )
        raw_states[historical.state_id] = historical.model_dump(mode="json")
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={"local_sim_execution_states_v1": raw_states},
    )

    post_close = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC),
    )
    latest = repo.get_simulation_daily_run(run_id)
    assert post_close.stale_terminalized_count == 1
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert len(repo.list_local_sim_execution_states(run_id)) == 2 * len(authority_states)
    assert len(repo.list_local_sim_execution_states(run_id, authoritative=True)) == len(authority_states)


def test_scheduler_cross_day_recovers_projected_terminal_localsim_failed_retryable_without_replay() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_historical_failed_retryable"
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    run_id = planned.results[0].run.run_id
    for as_of_time in (
        datetime(2026, 5, 21, 9, 32),
        datetime(2026, 5, 21, 9, 34),
        datetime(2026, 5, 21, 9, 36),
    ):
        scheduler.run_once(
            trade_date=TRADE_DATE,
            data_source=MinuteDataSource.TDX_REALTIME.value,
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
            as_of_time=as_of_time,
        )
        broker = context.local_broker
        assert broker is not None
        if repo.get_simulation_daily_run(run_id).status == SimulationDailyRunStatus.INTRADAY_RUNNING:
            context = _local_sim_realtime_context_with_real_broker(
                portfolio_id=portfolio_id,
                release=release,
                paper_repository=paper_repo,
                cash=float(broker.query_account().cash),
                positions=broker.query_positions(),
            )
            scheduler.context_provider = StaticSimulationRunContextProvider(
                by_binding_id={local_binding.binding_id: context}
            )

    finished = repo.get_simulation_daily_run(run_id)
    authority_states = tuple(repo.list_local_sim_execution_states(run_id, authoritative=True))
    assert finished.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert finished.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED"
    assert finished.run_payload_json["local_sim_persistence"]["terminal"] is True
    assert authority_states and all(state.is_terminal for state in authority_states)
    raw_states = dict(finished.run_payload_json["local_sim_execution_states_v1"])
    for state in authority_states:
        historical = LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "algo_instance_id": f"historical_failed_{state.algo_instance_id}",
                "state_hash": "",
                "created_at": (state.created_at - timedelta(seconds=1)).isoformat(),
                "updated_at": (state.updated_at - timedelta(seconds=1)).isoformat(),
            }
        )
        raw_states[historical.state_id] = historical.model_dump(mode="json")
    failed = repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "local_sim_execution_states_v1": raw_states,
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "submit_failure": {
                "stage": "STALE_LOCALSIM_TERMINALIZATION",
                "type": "DataUnavailableError",
                "message": "old scheduler could not close historical state generations",
                "context": {
                    "reason_code": "LOCALSIM_POST_CLOSE_STATE_PLAN_MISMATCH",
                    "run_id": run_id,
                },
            },
        },
    )
    assert len(repo.list_local_sim_execution_states(run_id)) == 2 * len(authority_states)
    assert len(repo.list_local_sim_execution_states(run_id, authoritative=True)) == len(authority_states)
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    fill_ids = {str(fill["fill_id"]) for fill in paper_repo.list_fills_for_run(run_id)}
    cash_entry_count = len(paper_repo.cash_entries[run_id])

    restarted = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    recovered = restarted.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    latest = repo.get_simulation_daily_run(run_id)
    recovery = latest.run_payload_json["localsim_post_close_terminalization"]
    assert failed.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert recovery["previous_status"] == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert recovery["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS"
    assert recovered.stale_run_results[0]["run_id"] == run_id
    assert recovered.stale_run_results[0]["cross_day_terminalization"] is True
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == order_ids
    assert {str(fill["fill_id"]) for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert len(paper_repo.cash_entries[run_id]) == cash_entry_count

    repeated = restarted.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 1),
    )
    assert all(item.get("run_id") != run_id for item in repeated.stale_run_results)
    assert repo.get_simulation_daily_run(run_id).run_payload_json["localsim_post_close_terminalization"] == recovery
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == order_ids
    assert {str(fill["fill_id"]) for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert len(paper_repo.cash_entries[run_id]) == cash_entry_count


def test_localsim_state_authority_rejects_duplicate_missing_extra_and_hash_conflict() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="portfolio_localsim_duplicate_active_authority",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    assert first.results[0].run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    run = repo.get_simulation_daily_run(planned.results[0].run.run_id)
    authority_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    raw_states = dict(run.run_payload_json["local_sim_execution_states_v1"])
    active = authority_states[0]
    duplicate = LocalSimExecutionStateV1.model_validate(
        {
            **active.model_dump(mode="json"),
            "state_id": "",
            "algo_instance_id": f"duplicate_{active.algo_instance_id}",
            "state_hash": "",
        }
    )
    raw_states[duplicate.state_id] = duplicate.model_dump(mode="json")
    repo.update_simulation_daily_run(run.run_id, payload_patch={"local_sim_execution_states_v1": raw_states})

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_ACTIVE_AUTHORITY_CONFLICT"

    raw_states.pop(duplicate.state_id)
    raw_states.pop(active.state_id)
    repo.update_simulation_daily_run(run.run_id, payload_patch={"local_sim_execution_states_v1": raw_states})
    with pytest.raises(InvalidStateTransitionError) as missing_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert missing_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_AUTHORITY_STATE_MISSING"

    raw_states[active.state_id] = active.model_dump(mode="json")
    extra = LocalSimExecutionStateV1.model_validate(
        {
            **active.model_dump(mode="json"),
            "state_id": "",
            "intent_id": f"extra_{active.intent_id}",
            "algo_instance_id": f"extra_{active.algo_instance_id}",
            "filled_quantity": active.total_quantity,
            "remaining_quantity": 0,
            "runtime_status": LocalSimExecutionRuntimeStatus.FILLED.value,
            "state_hash": "",
        }
    )
    raw_states[extra.state_id] = extra.model_dump(mode="json")
    repo.update_simulation_daily_run(run.run_id, payload_patch={"local_sim_execution_states_v1": raw_states})
    with pytest.raises(InvalidStateTransitionError) as extra_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert extra_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_HISTORY_IDENTITY_CONFLICT"

    raw_states.pop(extra.state_id)
    original_receipt = next(iter(_local_sim_economic_receipt_map(run.run_payload_json).values()))
    tampered_facts = deepcopy(original_receipt.economic_facts)
    tampered_facts["state_hashes"][active.state_id] = "0" * 64
    tampered_receipt = LocalSimEconomicReceiptV1(
        run_id=original_receipt.run_id,
        binding_id=original_receipt.binding_id,
        trade_date=original_receipt.trade_date,
        plan_id=original_receipt.plan_id,
        generation=original_receipt.generation,
        economic_facts=tampered_facts,
        committed_at=original_receipt.committed_at,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            "local_sim_execution_states_v1": raw_states,
            "local_sim_economic_receipts_v1": {tampered_receipt.receipt_id: tampered_receipt.model_dump(mode="json")},
        },
    )
    with pytest.raises(InvalidStateTransitionError) as hash_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert hash_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_AUTHORITY_HASH_CONFLICT"


def test_localsim_state_authority_accepts_hash_closed_superseded_plan_generations() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 100_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    repo.save_execution_plan(successor_plan)
    successor_states = tuple(
        LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "plan_id": successor_plan.plan_id,
                "algo_instance_id": f"successor_{state.algo_instance_id}",
                "order_id": f"successor_{state.order_id}",
                "idempotency_key": f"successor_{state.idempotency_key}",
                "state_hash": "",
            }
        )
        for state in predecessor_states
    )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    all_states = {state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)}
    receipts = {
        predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
        successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
    }
    repo.update_simulation_daily_run(
        run.run_id,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": all_states,
            "local_sim_economic_receipts_v1": receipts,
            "local_sim_economic_generation": successor_receipt.generation,
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": successor_plan.plan_id,
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
        },
    )

    authoritative = repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert {state.state_id for state in authoritative} == {state.state_id for state in successor_states}


def test_localsim_state_authority_accepts_monotonic_successor_generation_progress() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 100_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    repo.save_execution_plan(successor_plan)
    successor_states: list[LocalSimExecutionStateV1] = []
    progressed_intent_id = predecessor_states[0].intent_id
    for state in predecessor_states:
        progressed = state.intent_id == progressed_intent_id
        successor_states.append(
            LocalSimExecutionStateV1.model_validate(
                {
                    **state.model_dump(mode="json"),
                    "state_id": "",
                    "plan_id": successor_plan.plan_id,
                    "algo_instance_id": f"successor_{state.algo_instance_id}",
                    "order_id": f"successor_{state.order_id}",
                    "idempotency_key": f"successor_{state.idempotency_key}",
                    "filled_quantity": state.filled_quantity + (1 if progressed else 0),
                    "remaining_quantity": state.remaining_quantity - (1 if progressed else 0),
                    "runtime_status": (
                        LocalSimExecutionRuntimeStatus.ACTIVE.value if progressed else state.runtime_status.value
                    ),
                    "sequence": state.sequence + (1 if progressed else 0),
                    "latest_fill_sequence": state.latest_fill_sequence + (1 if progressed else 0),
                    "state_hash": "",
                }
            )
        )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": {
                state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)
            },
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
            },
            "local_sim_economic_generation": successor_receipt.generation,
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": successor_plan.plan_id,
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
        },
    )

    authoritative = repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert {state.state_id for state in authoritative} == {state.state_id for state in successor_states}
    progressed = next(state for state in authoritative if state.intent_id == progressed_intent_id)
    assert progressed.filled_quantity == predecessor_states[0].filled_quantity + 1


def test_localsim_state_authority_rejects_missing_durable_rebuild_plan() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 100_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    successor_states = tuple(
        LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "plan_id": successor_plan.plan_id,
                "algo_instance_id": f"successor_{state.algo_instance_id}",
                "order_id": f"successor_{state.order_id}",
                "idempotency_key": f"successor_{state.idempotency_key}",
                "state_hash": "",
            }
        )
        for state in predecessor_states
    )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": {
                state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)
            },
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
            },
            "local_sim_economic_generation": successor_receipt.generation,
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": "plan_missing_rebuild_authority",
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
        },
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert exc_info.value.context["reason_code"] == ("LOCALSIM_DURABLE_STATE_SUPERSEDED_PLAN_AUTHORITY_MISSING")


def test_localsim_state_authority_rejects_unproven_cross_plan_receipt_history() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    forged_facts = deepcopy(predecessor_receipt.economic_facts)
    forged_facts["plan_id"] = "plan_unproven_successor"
    forged = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id="plan_unproven_successor",
        generation=predecessor_receipt.generation + 1,
        economic_facts=forged_facts,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                forged.receipt_id: forged.model_dump(mode="json"),
            },
            "local_sim_economic_generation": forged.generation,
        },
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert exc_info.value.context["reason_code"] == ("LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT")


def test_localsim_state_authority_rejects_forged_superseded_plan_state_hash() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 100_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    repo.save_execution_plan(successor_plan)
    successor_states = tuple(
        LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "plan_id": successor_plan.plan_id,
                "algo_instance_id": f"successor_{state.algo_instance_id}",
                "order_id": f"successor_{state.order_id}",
                "idempotency_key": f"successor_{state.idempotency_key}",
                "state_hash": "",
            }
        )
        for state in predecessor_states
    )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    forged_predecessor = predecessor_states[0].model_copy(
        update={"sequence": predecessor_states[0].sequence + 1, "state_hash": ""}
    )
    raw_states = {state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)}
    raw_states[forged_predecessor.state_id] = forged_predecessor.model_dump(mode="json")
    repo.update_simulation_daily_run(
        run.run_id,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": raw_states,
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
            },
            "local_sim_economic_generation": successor_receipt.generation,
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": successor_plan.plan_id,
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
        },
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert exc_info.value.context["reason_code"] == ("LOCALSIM_DURABLE_STATE_SUPERSEDED_AUTHORITY_CONFLICT")


def test_localsim_state_authority_rejects_superseded_plan_semantic_drift() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 100_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    repo.save_execution_plan(successor_plan)
    successor_states = []
    for index, state in enumerate(predecessor_states):
        successor_states.append(
            LocalSimExecutionStateV1.model_validate(
                {
                    **state.model_dump(mode="json"),
                    "state_id": "",
                    "plan_id": successor_plan.plan_id,
                    "algo_instance_id": f"successor_{state.algo_instance_id}",
                    "order_id": f"successor_{state.order_id}",
                    "idempotency_key": f"successor_{state.idempotency_key}",
                    "total_quantity": state.total_quantity + (1 if index == 0 else 0),
                    "remaining_quantity": state.remaining_quantity + (1 if index == 0 else 0),
                    "state_hash": "",
                }
            )
        )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": {
                state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)
            },
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
            },
            "local_sim_economic_generation": successor_receipt.generation,
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": successor_plan.plan_id,
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
        },
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)

    assert exc_info.value.context["reason_code"] == ("LOCALSIM_DURABLE_STATE_SUPERSEDED_SEMANTIC_CONFLICT")
    evidence = exc_info.value.context["semantic_drift"]
    assert evidence["total_count"] == 1
    assert evidence["retained_count"] == 1
    assert evidence["omitted_count"] == 0
    assert len(evidence["full_set_sha256"]) == 64


def _localsim_authority_review_fixture(
    *,
    package_id: str = "pkg_scheduler",
    release_metadata: dict[str, Any] | None = None,
) -> tuple[
    InMemorySimulationRuntimeRepository,
    SimulationDailyRun,
    SimulationReleaseBinding,
    LocalSimEconomicReceiptV1,
    LocalSimProjectionOutboxV1,
]:
    release, local_binding, _, repo = _release_and_bindings(
        qmt_only=False,
        package_id=package_id,
        release_metadata=release_metadata,
    )
    assert local_binding is not None
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="portfolio_localsim_authority_review",
        release=release,
        paper_repository=InMemoryPaperTradingV2Repository(),
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    assert submitted.results[0].run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    run = repo.get_simulation_daily_run(planned.results[0].run.run_id)
    receipt = max(_local_sim_economic_receipt_map(run.run_payload_json).values(), key=lambda item: item.generation)
    outbox = _local_sim_projection_outbox(run.run_payload_json)
    assert outbox is not None
    return repo, run, receipt, outbox


def _rebuilt_local_sim_receipt(
    receipt: LocalSimEconomicReceiptV1,
    **updates: object,
) -> LocalSimEconomicReceiptV1:
    payload = receipt.model_dump(
        mode="python",
        exclude={"receipt_id", "economic_hash", "idempotency_key", "receipt_hash"},
    )
    payload.update(updates)
    return LocalSimEconomicReceiptV1.model_validate(payload)


def _rebuilt_local_sim_outbox(
    outbox: LocalSimProjectionOutboxV1,
    **updates: object,
) -> LocalSimProjectionOutboxV1:
    payload = outbox.model_dump(
        mode="python",
        exclude={"outbox_id", "projection_payload_hash", "outbox_hash"},
    )
    payload.update(updates)
    return LocalSimProjectionOutboxV1.model_validate(payload)


def test_scheduler_does_not_terminalize_historical_failed_localsim_with_pending_projection() -> None:
    repo, run, _, outbox = _localsim_authority_review_fixture()
    pending = _rebuilt_local_sim_outbox(
        outbox,
        status="PENDING",
        attempt_count=0,
        last_error=None,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "local_sim_projection_outbox_v1": pending.model_dump(mode="json"),
        },
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    latest = repo.get_simulation_daily_run(run.run_id)
    assert results == []
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PENDING"
    assert "localsim_post_close_terminalization" not in latest.run_payload_json


@pytest.mark.parametrize(
    ("candidate_case", "reason_code"),
    [
        ("outbox_missing", None),
        ("outbox_schema", "LOCALSIM_HISTORICAL_RECOVERY_OUTBOX_SCHEMA_INVALID"),
        ("valid_failure_carrier", None),
        ("plan_missing", "LOCALSIM_HISTORICAL_RECOVERY_PLAN_MISSING"),
    ],
)
def test_scheduler_historical_failed_localsim_candidate_classification_is_exact(
    candidate_case: str,
    reason_code: str | None,
) -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    payload_patch: dict[str, object] = {"last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value}
    payload_unset: tuple[str, ...] = ()
    if candidate_case == "outbox_missing":
        payload_unset = ("local_sim_projection_outbox_v1",)
    elif candidate_case == "outbox_schema":
        payload_patch["local_sim_projection_outbox_v1"] = {"schema_version": "malformed"}
    elif candidate_case == "valid_failure_carrier":
        payload_patch["local_sim_projection_readback_failure"] = {
            "schema_version": "local_sim_projection_readback_failure_v1",
            "run_id": run.run_id,
        }
    elif candidate_case == "plan_missing":
        repo.daily_runs[run.run_id] = run.model_copy(update={"execution_plan_id": None})
    elif candidate_case != "active_generation":  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(candidate_case)
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch=payload_patch,
        payload_unset=payload_unset,
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=1,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    if reason_code is None:
        assert results == []
    else:
        assert len(results) == 1
        assert results[0]["status"] == "RECOVERY_FAILED"
        assert results[0]["error"]["context"]["reason_code"] == reason_code
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "localsim_post_close_terminalization" not in latest.run_payload_json


def test_localsim_historical_failed_run_query_filters_trade_date_before_limit() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    historical = repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
    )
    current = historical.model_copy(
        update={
            "run_id": "run_current_failed_retryable_query_limit",
            "trade_date": TRADE_DATE + timedelta(days=1),
            "status": SimulationDailyRunStatus.FAILED_RETRYABLE,
        }
    )
    repo.save_simulation_daily_run(current)

    rows = repo.list_simulation_daily_runs(
        trade_date_before=current.trade_date,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        limit=1,
    )

    assert [item.run_id for item in rows] == [historical.run_id]


def test_scheduler_historical_failed_localsim_authority_corruption_is_backed_off_without_deep_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, run, receipt, _ = _localsim_authority_review_fixture()
    facts = deepcopy(receipt.economic_facts)
    state_id = next(iter(facts["state_hashes"]))
    facts["state_hashes"][state_id] = "0" * 64
    forged = _rebuilt_local_sim_receipt(receipt, economic_facts=facts)
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "local_sim_economic_receipts_v1": {
                forged.receipt_id: forged.model_dump(mode="json"),
            },
        },
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["stage"] == "STALE_LOCALSIM_FAILED_RUN_RECOVERY"
    assert results[0]["error"]["context"]["reason_code"] == ("LOCALSIM_DURABLE_STATE_AUTHORITY_HASH_CONFLICT")
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "localsim_post_close_terminalization" not in latest.run_payload_json
    retry_entry = latest.run_payload_json["simulation_scheduler_retry_control_v1"]["entries"][
        "RECOVERY:STALE_LOCALSIM_FAILED_RUN_RECOVERY"
    ]
    assert retry_entry["consecutive_failure_count"] == 1

    def forbidden_deep_read(_plan_id: str) -> Any:
        raise AssertionError("historical recovery must not reload the frozen plan before retry is due")

    def forbidden_retry_claim(**_values: Any) -> Any:
        raise AssertionError("non-due recovery must use the strict carrier already loaded by the bounded query")

    monkeypatch.setattr(repo, "get_execution_plan", forbidden_deep_read)
    monkeypatch.setattr(repo, "claim_simulation_retry_attempt", forbidden_retry_claim)
    deferred = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0, 30),
    )
    assert len(deferred) == 1
    assert deferred[0]["status"] == "RECOVERY_BACKOFF"
    assert deferred[0]["retry_control"]["next_retry_at"] == retry_entry["next_retry_at"]

    corrupted_run = repo.get_simulation_daily_run(run.run_id)
    corrupted_payload = deepcopy(corrupted_run.run_payload_json)
    corrupted_payload["simulation_scheduler_retry_control_v1"]["entries"][
        "RECOVERY:STALE_LOCALSIM_FAILED_RUN_RECOVERY"
    ]["next_retry_at"] = datetime(2026, 5, 29, 10, 0, tzinfo=UTC).isoformat()
    repo.daily_runs[run.run_id] = corrupted_run.model_copy(update={"run_payload_json": corrupted_payload})
    corrupt_control = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0, 45),
    )
    assert len(corrupt_control) == 1
    assert corrupt_control[0]["status"] == "RECOVERY_FAILED"
    assert corrupt_control[0]["stage"] == ("STALE_LOCALSIM_FAILED_RUN_RECOVERY:RETRY_CONTROL_CLAIM")
    assert corrupt_control[0]["retry_control_claim_failed"] is True
    assert corrupt_control[0]["run_id"] == run.run_id
    assert corrupt_control[0]["error"]["context"]["reason_code"] == ("SIMULATION_SCHEDULER_RETRY_CONTROL_HASH_DRIFT")


def test_scheduler_historical_recovery_backoff_uses_stable_identity_and_failure_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={"last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value},
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)
    started_at = datetime(2026, 5, 22, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
    completed_at = [started_at + timedelta(minutes=3), started_at + timedelta(minutes=7)]
    monkeypatch.setattr(scheduler, "_scheduler_now", lambda: completed_at.pop(0))
    retry_claim_calls = 0
    claim_retry_attempt = repo.claim_simulation_retry_attempt

    def count_retry_claims(**kwargs: Any) -> Any:
        nonlocal retry_claim_calls
        retry_claim_calls += 1
        return claim_retry_attempt(**kwargs)

    monkeypatch.setattr(repo, "claim_simulation_retry_attempt", count_retry_claims)
    deep_recovery_calls = 0

    def fail_after_mutating_attempt_evidence() -> dict[str, Any]:
        nonlocal deep_recovery_calls
        deep_recovery_calls += 1
        repo.update_simulation_daily_run(
            run.run_id,
            payload_patch={
                "economic_commit_staged": {
                    "attempt": deep_recovery_calls,
                    "as_of_time": (started_at + timedelta(minutes=deep_recovery_calls)).isoformat(),
                }
            },
        )
        raise DataUnavailableError(
            "LocalSim mark authority conflicts with the historical trade date",
            context={
                "reason_code": "LOCALSIM_MARK_AS_OF_DATE_CONFLICT",
                "as_of_time": (started_at + timedelta(minutes=deep_recovery_calls)).isoformat(),
            },
        )

    first = scheduler._run_recovery_item_isolated(  # noqa: SLF001
        stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
        run=repo.get_simulation_daily_run(run.run_id),
        raise_on_error=False,
        func=fail_after_mutating_attempt_evidence,
        as_of_time=started_at,
    )
    assert first is not None
    first_entry = first["retry_control"]
    assert first_entry["last_failed_at"] == (started_at + timedelta(minutes=3)).astimezone(UTC).isoformat()
    assert first_entry["next_retry_at"] == (started_at + timedelta(minutes=4)).astimezone(UTC).isoformat()

    deferred = scheduler._run_recovery_item_isolated(  # noqa: SLF001
        stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
        run=repo.get_simulation_daily_run(run.run_id),
        raise_on_error=False,
        func=fail_after_mutating_attempt_evidence,
        as_of_time=started_at + timedelta(minutes=3, seconds=30),
    )
    assert deferred is not None
    assert deferred["status"] == "RECOVERY_BACKOFF"
    assert retry_claim_calls == 1
    assert deep_recovery_calls == 1

    second = scheduler._run_recovery_item_isolated(  # noqa: SLF001
        stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
        run=repo.get_simulation_daily_run(run.run_id),
        raise_on_error=False,
        func=fail_after_mutating_attempt_evidence,
        as_of_time=started_at + timedelta(minutes=4),
    )
    assert second is not None
    second_entry = second["retry_control"]
    assert retry_claim_calls == 2
    assert deep_recovery_calls == 2
    assert second_entry["source_fingerprint"] == first_entry["source_fingerprint"]
    assert second_entry["failure_fingerprint"] == first_entry["failure_fingerprint"]
    assert second_entry["consecutive_failure_count"] == 2
    assert second_entry["attempt_count"] == 2
    assert second_entry["last_failed_at"] == (started_at + timedelta(minutes=7)).astimezone(UTC).isoformat()
    assert second_entry["next_retry_at"] == (started_at + timedelta(minutes=9)).astimezone(UTC).isoformat()
    assert second_entry["last_error"]["context"]["as_of_time"] == (started_at + timedelta(minutes=2)).isoformat()
    changed_authority = repo.get_simulation_daily_run(run.run_id).model_copy(update={"binding_hash": "f" * 64})
    assert (
        scheduler._simulation_retry_source_fingerprint(  # noqa: SLF001
            run=changed_authority,
            retry_key="RECOVERY:STALE_LOCALSIM_FAILED_RUN_RECOVERY",
        )
        != second_entry["source_fingerprint"]
    )


@pytest.mark.parametrize(
    ("corruption", "reason_code"),
    [
        ("run_plan_hash", "LOCALSIM_HISTORICAL_RECOVERY_IDENTITY_CONFLICT"),
        ("runtime_release", "LOCALSIM_HISTORICAL_RECOVERY_IDENTITY_CONFLICT"),
        ("binding", "LOCALSIM_HISTORICAL_RECOVERY_IDENTITY_CONFLICT"),
        ("failure_carrier", "LOCALSIM_HISTORICAL_RECOVERY_FAILURE_CARRIER_INVALID"),
    ],
)
def test_scheduler_historical_failed_localsim_rejects_identity_and_failure_carrier_drift(
    corruption: str,
    reason_code: str,
) -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    failed = run.model_copy(update={"status": SimulationDailyRunStatus.FAILED_RETRYABLE})
    if corruption == "run_plan_hash":
        failed = failed.model_copy(update={"execution_plan_hash": "forged_plan_hash"})
    elif corruption == "runtime_release":
        release = repo.releases[run.release_id]
        repo.releases[run.release_id] = release.model_copy(update={"package_id": "forged_release_package"})
    elif corruption == "binding":
        binding = repo.bindings[run.binding_id]
        repo.bindings[run.binding_id] = binding.model_copy(update={"strategy_id": "forged_binding_strategy"})
    else:
        failed = failed.model_copy(
            update={
                "run_payload_json": {
                    **failed.run_payload_json,
                    "local_sim_projection_readback_failure": "malformed",
                }
            }
        )
    repo.daily_runs[run.run_id] = failed
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["error"]["context"]["reason_code"] == reason_code
    assert repo.get_simulation_daily_run(run.run_id).status == SimulationDailyRunStatus.FAILED_RETRYABLE


def _legacy_v25_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a plan payload whose execution policy is the retired V25_1_SMALL_CAP algorithm."""

    mutated = deepcopy(payload)
    policy_container = dict(mutated["execution_policy"])
    policy_payload = dict(policy_container["payload"])
    policy_json = dict(policy_payload["policy_json"])
    policy_json["algo_code"] = "V25_1_SMALL_CAP"
    policy_payload["policy_json"] = policy_json
    policy_container["payload"] = policy_payload
    mutated["execution_policy"] = policy_container
    return mutated


def _malformed_missing_policy_plan_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a plan payload whose execution policy container is missing entirely."""

    mutated = deepcopy(payload)
    mutated.pop("execution_policy", None)
    return mutated


def _localsim_legacy_plan_failed_run_fixture(
    *,
    failed_status: SimulationDailyRunStatus,
    intent_count: int = 246,
    package_id: str = "pkg_scheduler",
    release_metadata: dict[str, Any] | None = None,
    policy_payload_transform: Callable[[dict[str, Any]], dict[str, Any]] = _legacy_v25_plan_payload,
) -> tuple[
    InMemorySimulationRuntimeRepository,
    SimulationDailyRun,
    SimulationReleaseBinding,
    InMemoryPaperTradingV2Repository,
]:
    """Build the production-equivalent BUG-992 2026-08-06 scenario from real scheduler ticks.

    A hash-closed, generation-continuous, one-way predecessor->current plan replacement
    with 246 frozen intents and 492 durable states (generation 1 predecessor + generation 2
    current), where BOTH durable plans carry the retired V25_1_SMALL_CAP execution policy
    exactly like the production run simrun_7bf1e0c1b6b7d055. ``policy_payload_transform``
    rewrites both lineage plan payloads hash-consistently, so alternate policy shapes
    (for example a missing execution_policy container) can be exercised end to end.
    """

    release, local_binding, _, repo = _release_and_bindings(
        qmt_only=False,
        package_id=package_id,
        release_metadata=release_metadata,
    )
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    candidates = [
        SelectionCandidate(
            symbol=(f"{600000 + index:06d}.SH" if index % 2 == 0 else f"{300001 + index:06d}.SZ"),
            score=0.99 - index * 0.0001,
            rank=index + 1,
            target_quantity=100,
            target_weight=0.001,
            reference_price=10.0,
            reason="daily_strategy_buy_or_retain",
        )
        for index in range(intent_count)
    ]
    context = replace(
        _local_sim_realtime_context_with_real_broker(
            portfolio_id="portfolio_localsim_historical_failed_legacy_plan",
            release=release,
            paper_repository=paper_repo,
            cash=10_000_000,
            positions={},
        ),
        top_k=intent_count,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=candidates),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    assert submitted.results[0].run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    run = repo.get_simulation_daily_run(planned.results[0].run.run_id)
    predecessor_plan = repo.get_execution_plan(run.execution_plan_id)
    assert len(predecessor_plan.intents) == intent_count
    predecessor_states = tuple(repo.list_local_sim_execution_states(run.run_id, authoritative=True))
    assert len(predecessor_states) == intent_count
    predecessor_receipt = max(
        _local_sim_economic_receipt_map(run.run_payload_json).values(),
        key=lambda item: item.generation,
    )

    legacy_payload = policy_payload_transform(predecessor_plan.plan_payload_json)
    legacy_hash = canonical_json_sha256(legacy_payload)
    legacy_plan_id = f"plan_{legacy_hash[:16]}"
    original_predecessor = predecessor_plan
    predecessor_plan = predecessor_plan.model_copy(
        update={
            "plan_id": legacy_plan_id,
            "plan_payload_json": legacy_payload,
            "plan_hash": legacy_hash,
        }
    )
    del repo.execution_plans[original_predecessor.plan_id]
    repo.execution_plan_hash_index.pop(original_predecessor.plan_hash, None)
    repo.execution_plans[predecessor_plan.plan_id] = predecessor_plan
    repo.execution_plan_hash_index[predecessor_plan.plan_hash] = predecessor_plan.plan_id
    predecessor_states = tuple(
        LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "plan_id": predecessor_plan.plan_id,
                "state_hash": "",
            }
        )
        for state in predecessor_states
    )
    predecessor_facts = deepcopy(predecessor_receipt.economic_facts)
    predecessor_facts["plan_id"] = predecessor_plan.plan_id
    predecessor_facts["state_hashes"] = {state.state_id: state.state_hash for state in predecessor_states}
    predecessor_receipt = _rebuilt_local_sim_receipt(
        predecessor_receipt,
        plan_id=predecessor_plan.plan_id,
        economic_facts=predecessor_facts,
    )
    successor_plan = SimulationLifecycleScheduler._copy_localsim_plan_with_intents(  # noqa: SLF001
        plan=predecessor_plan,
        intents=list(reversed(predecessor_plan.intents)),
        cash_fit_payload={
            "schema_version": "localsim_capital_dependency_v1",
            "status": "SELL_FIRST_DEPENDENCY_ORDERED",
            "reason": "localsim_sell_first_durable_dependent_buy",
            "initial_cash": 10_000_000.0,
            "original_intent_count": len(predecessor_plan.intents),
            "prepared_intent_count": len(predecessor_plan.intents),
            "sell_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.SELL),
            "buy_intent_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "dependent_buy_count": sum(1 for item in predecessor_plan.intents if item.side == OrderSide.BUY),
            "capital_waiting_owner": "LocalSimExecutionStateV1",
        },
    )
    transformed_successor_payload = policy_payload_transform(successor_plan.plan_payload_json)
    if transformed_successor_payload != successor_plan.plan_payload_json:
        transformed_successor_hash = canonical_json_sha256(transformed_successor_payload)
        successor_plan = successor_plan.model_copy(
            update={
                "plan_id": f"plan_{transformed_successor_hash[:16]}",
                "plan_payload_json": transformed_successor_payload,
                "plan_hash": transformed_successor_hash,
            }
        )
    repo.save_execution_plan(successor_plan)
    successor_states = tuple(
        LocalSimExecutionStateV1.model_validate(
            {
                **state.model_dump(mode="json"),
                "state_id": "",
                "plan_id": successor_plan.plan_id,
                "algo_instance_id": f"successor_{state.algo_instance_id}",
                "order_id": f"successor_{state.order_id}",
                "idempotency_key": f"successor_{state.idempotency_key}",
                "state_hash": "",
            }
        )
        for state in predecessor_states
    )
    successor_facts = deepcopy(predecessor_receipt.economic_facts)
    successor_facts["plan_id"] = successor_plan.plan_id
    successor_facts["state_hashes"] = {state.state_id: state.state_hash for state in successor_states}
    successor_receipt = LocalSimEconomicReceiptV1(
        run_id=run.run_id,
        binding_id=run.binding_id,
        trade_date=run.trade_date,
        plan_id=successor_plan.plan_id,
        generation=predecessor_receipt.generation + 1,
        economic_facts=successor_facts,
    )
    outbox = _local_sim_projection_outbox(run.run_payload_json)
    successor_outbox = _rebuilt_local_sim_outbox(
        outbox,
        plan_id=successor_plan.plan_id,
        generation=successor_receipt.generation,
    )
    repo.update_simulation_daily_run(
        run.run_id,
        status=failed_status,
        execution_plan=successor_plan,
        payload_patch={
            "local_sim_execution_states_v1": {
                state.state_id: state.model_dump(mode="json") for state in (*predecessor_states, *successor_states)
            },
            "local_sim_economic_receipts_v1": {
                predecessor_receipt.receipt_id: predecessor_receipt.model_dump(mode="json"),
                successor_receipt.receipt_id: successor_receipt.model_dump(mode="json"),
            },
            "local_sim_economic_generation": successor_receipt.generation,
            "local_sim_projection_outbox_v1": successor_outbox.model_dump(mode="json"),
            "rebuilt_after_side_effect_free_failure": True,
            "rebuilt_failure_backend": SimulationBrokerBackend.LOCAL_SIM.value,
            "rebuilt_from_execution_plan_id": predecessor_plan.plan_id,
            "rebuilt_execution_plan_id": successor_plan.plan_id,
            "local_sim_cash_fit": successor_plan.plan_payload_json["local_sim_cash_fit"],
            "last_stage": failed_status.value,
        },
    )
    assert len(repo.list_local_sim_execution_states(run.run_id)) == 2 * intent_count
    assert len(repo.list_local_sim_execution_states(run.run_id, authoritative=True)) == intent_count
    return repo, repo.get_simulation_daily_run(run.run_id), local_binding, paper_repo


@pytest.mark.parametrize(
    "failed_status",
    [SimulationDailyRunStatus.FAILED_RETRYABLE, SimulationDailyRunStatus.FAILED_TERMINAL],
)
def test_scheduler_historical_failed_localsim_legacy_plan_terminalizes_without_v25_execution(
    failed_status: SimulationDailyRunStatus,
) -> None:
    repo, run, _, paper_repo = _localsim_legacy_plan_failed_run_fixture(failed_status=failed_status)
    successor_plan = repo.get_execution_plan(run.execution_plan_id)
    predecessor_plan_id = run.run_payload_json["rebuilt_from_execution_plan_id"]
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)}
    fill_ids = {str(fill["fill_id"]) for fill in paper_repo.list_fills_for_run(run.run_id)}
    cash_entry_count = len(paper_repo.cash_entries.get(run.run_id, []))
    state_snapshot = deepcopy(run.run_payload_json["local_sim_execution_states_v1"])
    scheduler = SimulationLifecycleScheduler(repository=repo)

    # Seed the exact pre-fix production condition: a recorded recovery-backoff failure
    # entry for the stale-run recovery stage, so terminalization must also dispose of it.
    recovery_retry_key = "RECOVERY:STALE_LOCALSIM_FAILED_RUN_RECOVERY"
    scheduler._record_simulation_retry_failure(  # noqa: SLF001
        run=run,
        retry_key=recovery_retry_key,
        failure_stage="STALE_LOCALSIM_FAILED_RUN_RECOVERY",
        exc=RuntimeConfigInvalidError(
            "pre-fix permanent policy rejection",
            context={"reason_code": "LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED"},
        ),
        as_of_time=datetime(2026, 5, 21, 9, 40),
        source_fingerprint=scheduler._simulation_retry_source_fingerprint(  # noqa: SLF001
            run=run,
            retry_key=recovery_retry_key,
        ),
    )
    seeded = repo.get_simulation_daily_run(run.run_id)
    assert recovery_retry_key in seeded.run_payload_json["simulation_scheduler_retry_control_v1"]["entries"]

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    result = results[0]
    assert result["run_id"] == run.run_id
    assert result["previous_status"] == failed_status.value
    assert result["status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert result["reason_code"] == "LOCALSIM_HISTORICAL_FAILED_RUN_LEGACY_PLAN_RETIRED"
    assert result["historical_failed_legacy_plan_terminalization"] is True
    assert result["cross_day_terminalization"] is True
    assert result["durable_minute_loop_advanced"] is False
    assert result["legacy_execution_restored"] is False

    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    evidence = latest.run_payload_json["localsim_historical_legacy_plan_terminalization_v1"]
    assert evidence["schema_version"] == "localsim_historical_legacy_plan_terminalization_v1"
    assert evidence["reason_code"] == "LOCALSIM_HISTORICAL_FAILED_RUN_LEGACY_PLAN_RETIRED"
    assert evidence["plan_id"] == successor_plan.plan_id
    assert evidence["plan_algo_code"] == "V25_1_SMALL_CAP"
    assert evidence["required_algo_code"] == "TWAP"
    assert evidence["retired_policy_reason_code"] == "LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED"
    assert evidence["previous_status"] == failed_status.value
    assert evidence["terminal_status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert evidence["authoritative_state_count"] == 246
    assert evidence["active_state_count"] == 246
    assert len(evidence["authoritative_state_set_sha256"]) == 64
    # The run executed real generations through the local broker before it failed; the
    # carrier records that historical fact under a name that cannot be misread as a
    # side effect of this terminalization.
    assert evidence["historical_broker_called"] is True
    assert evidence["parent_resubmitted"] is False
    assert evidence["broker_replayed"] is False
    assert evidence["predecessor_projection_replayed"] is False
    assert evidence["durable_minute_loop_advanced"] is False
    assert evidence["legacy_execution_restored"] is False
    assert evidence["fallback_used"] is False
    assert evidence["runtime_context_loaded"] is False
    assert evidence["market_data_loaded"] is False

    # The retired plan must never reach a runtime context, market data or the minute loop:
    # the scheduler above was constructed without any context provider, so any attempt to
    # load one would have raised instead of terminalizing.
    assert {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)} == order_ids
    assert {str(fill["fill_id"]) for fill in paper_repo.list_fills_for_run(run.run_id)} == fill_ids
    assert len(paper_repo.cash_entries.get(run.run_id, [])) == cash_entry_count
    assert latest.run_payload_json["local_sim_execution_states_v1"] == state_snapshot
    assert latest.run_payload_json["rebuilt_from_execution_plan_id"] == predecessor_plan_id
    # The seeded pre-fix backoff entry must be disposed of by the successful recovery,
    # exactly like the production run's recorded failure entries. Clearing the last
    # entry drops the control key entirely.
    retry_control = latest.run_payload_json.get("simulation_scheduler_retry_control_v1")
    assert retry_control is None or recovery_retry_key not in retry_control.get("entries", {})

    # The terminal carrier is idempotent: later scheduler cadence skips the run without
    # rewriting evidence or re-driving anything. The hoisted skip must also bypass the
    # retry-attempt claim entirely, so a terminalized run costs no claim/clear writes.
    claim_calls: list[dict[str, Any]] = []
    original_claim = repo.claim_simulation_retry_attempt

    def counting_claim(**kwargs: Any) -> Any:
        claim_calls.append(kwargs)
        return original_claim(**kwargs)

    repo.claim_simulation_retry_attempt = counting_claim  # type: ignore[method-assign]
    repeated = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=2),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 23, 10, 0),
    )
    assert claim_calls == []
    assert all(item.get("run_id") != run.run_id for item in repeated)
    reloaded = repo.get_simulation_daily_run(run.run_id)
    assert reloaded.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert reloaded.run_payload_json["localsim_historical_legacy_plan_terminalization_v1"] == evidence


def test_scheduler_historical_failed_localsim_legacy_plan_malformed_carrier_fails_loud() -> None:
    repo, run, _, paper_repo = _localsim_legacy_plan_failed_run_fixture(
        failed_status=SimulationDailyRunStatus.FAILED_RETRYABLE,
    )
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)}
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={"localsim_historical_legacy_plan_terminalization_v1": "malformed"},
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["error"]["context"]["reason_code"] == ("LOCALSIM_HISTORICAL_RECOVERY_FAILURE_CARRIER_INVALID")
    assert results[0]["error"]["context"]["field"] == ("localsim_historical_legacy_plan_terminalization_v1")
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)} == order_ids


def test_scheduler_historical_failed_localsim_non_legacy_policy_error_stays_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo, run, _, _ = _localsim_legacy_plan_failed_run_fixture(
        failed_status=SimulationDailyRunStatus.FAILED_RETRYABLE,
    )

    def raise_other_policy_error(*, binding: Any, plan: Any) -> None:
        raise RuntimeConfigInvalidError(
            "unit test non-legacy policy rejection",
            context={"reason_code": "UNIT_TEST_OTHER_POLICY_INVALID", "plan_id": plan.plan_id},
        )

    monkeypatch.setattr(
        SimulationLifecycleScheduler,
        "_assert_local_sim_plan_uses_twap",
        staticmethod(raise_other_policy_error),
    )
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["error"]["context"]["reason_code"] == "UNIT_TEST_OTHER_POLICY_INVALID"
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "localsim_historical_legacy_plan_terminalization_v1" not in latest.run_payload_json


def test_scheduler_historical_failed_localsim_malformed_plan_policy_stays_typed_fail_loud() -> None:
    """An unreadable policy payload is an unknown shape, not a verified retired legacy plan.

    The whole lineage (predecessor and current plan) carries no execution_policy
    container at all. The recovery must not terminalize it under the retired-policy
    reason: the typed malformed-policy error propagates, the run keeps its failed
    status, no carrier is written and no durable fact is touched.
    """

    repo, run, _, paper_repo = _localsim_legacy_plan_failed_run_fixture(
        failed_status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        policy_payload_transform=_malformed_missing_policy_plan_payload,
    )
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)}
    state_snapshot = deepcopy(run.run_payload_json["local_sim_execution_states_v1"])
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
        trade_date=TRADE_DATE + timedelta(days=1),
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    assert len(results) == 1
    assert results[0]["run_id"] == run.run_id
    assert results[0]["status"] == "RECOVERY_FAILED"
    assert results[0]["error"]["context"]["reason_code"] == ("LOCALSIM_EXECUTION_PLAN_POLICY_MISSING_OR_MALFORMED")
    latest = repo.get_simulation_daily_run(run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "localsim_historical_legacy_plan_terminalization_v1" not in latest.run_payload_json
    assert latest.run_payload_json["local_sim_execution_states_v1"] == state_snapshot
    assert {order.order_id for order in paper_repo.list_orders_for_run(run.run_id)} == order_ids


@pytest.mark.parametrize(
    ("corruption", "reason_code"),
    [
        ("receipt_key_missing", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_MISSING"),
        ("receipt_map_empty_with_generation", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_MISSING"),
        ("state_hashes_missing", "LOCALSIM_DURABLE_STATE_AUTHORITY_MISSING"),
        ("state_hashes_empty", "LOCALSIM_DURABLE_STATE_AUTHORITY_MISSING"),
        ("receipt_run_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT"),
        ("receipt_binding_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT"),
        ("receipt_trade_date", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT"),
        ("receipt_plan_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT"),
        ("facts_run_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_FACT_IDENTITY_CONFLICT"),
        ("facts_binding_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_FACT_IDENTITY_CONFLICT"),
        ("facts_trade_date", "LOCALSIM_DURABLE_STATE_AUTHORITY_FACT_IDENTITY_CONFLICT"),
        ("facts_plan_id", "LOCALSIM_DURABLE_STATE_AUTHORITY_FACT_IDENTITY_CONFLICT"),
        ("generation_highwater_low", "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_MISMATCH"),
        ("generation_highwater_high", "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_MISMATCH"),
        ("generation_duplicate", "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_CONFLICT"),
        ("generation_gap", "LOCALSIM_DURABLE_STATE_AUTHORITY_GENERATION_GAP"),
        ("authority_state_missing", "LOCALSIM_DURABLE_STATE_AUTHORITY_STATE_MISSING"),
        ("authority_hash_conflict", "LOCALSIM_DURABLE_STATE_AUTHORITY_HASH_CONFLICT"),
    ],
)
def test_localsim_public_authority_rejects_receipt_generation_and_identity_corruption(
    corruption: str,
    reason_code: str,
) -> None:
    repo, run, receipt, _ = _localsim_authority_review_fixture()
    payload_patch: dict[str, object] = {}
    payload_unset: tuple[str, ...] = ()
    receipts = {receipt.receipt_id: receipt}
    state_payload = deepcopy(run.run_payload_json[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY])
    generation = receipt.generation

    if corruption == "receipt_key_missing":
        payload_unset = (LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY, LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY)
    elif corruption == "receipt_map_empty_with_generation":
        receipts = {}
    elif corruption in {"state_hashes_missing", "state_hashes_empty"}:
        facts = deepcopy(receipt.economic_facts)
        if corruption == "state_hashes_missing":
            facts.pop("state_hashes")
        else:
            facts["state_hashes"] = {}
        forged = _rebuilt_local_sim_receipt(receipt, economic_facts=facts)
        receipts = {forged.receipt_id: forged}
    elif corruption.startswith("receipt_"):
        field = corruption.removeprefix("receipt_")
        forged_identity: object
        if field == "trade_date":
            forged_identity = run.trade_date - timedelta(days=1)
        else:
            forged_identity = f"forged_{field}"
        forged = _rebuilt_local_sim_receipt(receipt, **{field: forged_identity})
        receipts = {forged.receipt_id: forged}
    elif corruption.startswith("facts_"):
        field = corruption.removeprefix("facts_")
        facts = deepcopy(receipt.economic_facts)
        facts[field] = (run.trade_date - timedelta(days=1)).isoformat() if field == "trade_date" else f"forged_{field}"
        forged = _rebuilt_local_sim_receipt(receipt, economic_facts=facts)
        receipts = {forged.receipt_id: forged}
    elif corruption in {"generation_highwater_low", "generation_duplicate", "generation_gap"}:
        next_generation = 3 if corruption == "generation_gap" else (1 if corruption == "generation_duplicate" else 2)
        facts = {**deepcopy(receipt.economic_facts), "review_generation_nonce": corruption}
        second = _rebuilt_local_sim_receipt(receipt, generation=next_generation, economic_facts=facts)
        receipts[second.receipt_id] = second
        generation = 3 if corruption == "generation_gap" else 1
    elif corruption == "generation_highwater_high":
        generation = receipt.generation + 1
    elif corruption == "authority_state_missing":
        state_payload.pop(next(iter(receipt.economic_facts["state_hashes"])))
    elif corruption == "authority_hash_conflict":
        facts = deepcopy(receipt.economic_facts)
        state_id = next(iter(facts["state_hashes"]))
        facts["state_hashes"][state_id] = "0" * 64
        forged = _rebuilt_local_sim_receipt(receipt, economic_facts=facts)
        receipts = {forged.receipt_id: forged}
    else:  # pragma: no cover - parametrization is exhaustive
        raise AssertionError(f"unknown authority corruption: {corruption}")

    if receipts or corruption != "receipt_key_missing":
        payload_patch[LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY] = {
            receipt_id: item.model_dump(mode="json") for receipt_id, item in receipts.items()
        }
    if corruption != "receipt_key_missing":
        payload_patch[LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY] = generation
    payload_patch[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY] = state_payload
    repo.update_simulation_daily_run(run.run_id, payload_patch=payload_patch, payload_unset=payload_unset)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == reason_code
    assert exc_info.value.context["run_id"] == run.run_id


@pytest.mark.parametrize("target", ["receipt", "highwater"])
@pytest.mark.parametrize("raw_generation", [True, "1", 1.0])
def test_localsim_public_authority_rejects_non_integer_generation_types(
    target: str,
    raw_generation: object,
) -> None:
    repo, run, receipt, _ = _localsim_authority_review_fixture()
    payload_patch: dict[str, object] = {}
    expected_reason = "LOCALSIM_ECONOMIC_GENERATION_INVALID"
    if target == "receipt":
        raw_receipt = receipt.model_dump(mode="json")
        raw_receipt["generation"] = raw_generation
        payload_patch[LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY] = {receipt.receipt_id: raw_receipt}
        expected_reason = "LOCALSIM_ECONOMIC_RECEIPT_GENERATION_INVALID"
    else:
        payload_patch[LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY] = raw_generation
    repo.update_simulation_daily_run(run.run_id, payload_patch=payload_patch)

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == expected_reason
    assert exc_info.value.context["run_id"] == run.run_id


@pytest.mark.parametrize(
    ("raw_generation", "accepted"),
    [(None, True), (0, True), (False, False), ("0", False), (0.0, False)],
)
def test_localsim_public_authority_empty_path_requires_strict_zero_generation(
    raw_generation: object,
    accepted: bool,
) -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    payload_patch = {}
    payload_unset = [
        LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY,
        LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY,
        LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
        LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
        "local_sim_projection_generation",
        "local_sim_projection_terminal_failure",
        "local_sim_projection_readback_failure",
        "local_sim_projection_readback_terminal_failure",
        "local_sim_valuation_pending_v1",
        "local_sim_valuation_completion_v1",
        "local_sim_persistence",
        "local_sim_durable_minute_loop",
        "strategy_performance",
        "performance_projection",
        "broker_order_handles",
        "broker_called",
        "submitted_intents",
    ]
    if raw_generation is None:
        payload_unset.append(LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY)
    else:
        payload_patch[LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY] = raw_generation
    repo.update_simulation_daily_run(run.run_id, payload_patch=payload_patch, payload_unset=tuple(payload_unset))

    if accepted:
        assert repo.list_local_sim_execution_states(run.run_id, authoritative=True) == []
        return
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_ECONOMIC_GENERATION_INVALID"
    assert exc_info.value.context["run_id"] == run.run_id


@pytest.mark.parametrize(
    ("carrier_case", "carrier_type", "identity_field"),
    [
        ("pending_outbox", LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY, "outbox_id"),
        ("projected_outbox", LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY, "outbox_id"),
        ("projection_receipt", LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY, "receipt_id"),
        ("projection_generation", "local_sim_projection_generation", "outbox_id"),
    ],
)
def test_localsim_public_authority_rejects_orphan_projection_generation_carriers(
    carrier_case: str,
    carrier_type: str,
    identity_field: str,
) -> None:
    repo, run, _, outbox = _localsim_authority_review_fixture()
    projection_receipt = next(iter(_local_sim_projection_receipt_map(run.run_payload_json).values()))
    if carrier_case in {"pending_outbox", "projected_outbox"}:
        carrier_outbox = _rebuilt_local_sim_outbox(
            outbox,
            status="PENDING" if carrier_case == "pending_outbox" else "PROJECTED",
            attempt_count=0,
            last_error=None,
        )
        carrier_value: object = carrier_outbox.model_dump(mode="json")
        expected_identity = carrier_outbox.outbox_id
    elif carrier_case == "projection_receipt":
        carrier_value = {projection_receipt.projection_receipt_id: projection_receipt.model_dump(mode="json")}
        expected_identity = projection_receipt.projection_receipt_id
    else:
        carrier_value = {
            "schema_version": "local_sim_projection_generation_v1",
            "generation": outbox.generation,
            "outbox_id": outbox.outbox_id,
            "economic_hash": outbox.economic_hash,
            "projection_receipt_id": projection_receipt.projection_receipt_id,
        }
        expected_identity = outbox.outbox_id

    carrier_keys = {
        LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
        LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
        "local_sim_projection_generation",
        "local_sim_projection_terminal_failure",
        "local_sim_projection_readback_failure",
        "local_sim_projection_readback_terminal_failure",
        "local_sim_valuation_pending_v1",
        "local_sim_valuation_completion_v1",
        "local_sim_persistence",
        "local_sim_durable_minute_loop",
        "strategy_performance",
        "performance_projection",
        "broker_order_handles",
        "broker_called",
        "submitted_intents",
    }
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: 0,
            carrier_type: carrier_value,
        },
        payload_unset=tuple(sorted(carrier_keys - {carrier_type})),
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_AUTHORITY_ORPHAN_CARRIER"
    assert exc_info.value.context["run_id"] == run.run_id
    assert exc_info.value.context["carrier_type"] == carrier_type
    assert exc_info.value.context[identity_field] == expected_identity
    assert exc_info.value.context["expected_generation"] == 0
    assert exc_info.value.context["actual_generation"] == outbox.generation


@pytest.mark.parametrize(
    ("carrier_type", "carrier_value"),
    [
        ("broker_called", True),
        ("broker_called", 1),
        ("broker_called", "false"),
        ("submitted_intents", 1),
        ("submitted_intents", False),
        ("submitted_intents", 0.0),
    ],
)
def test_localsim_public_authority_rejects_non_initial_broker_carriers(
    carrier_type: str,
    carrier_value: object,
) -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: 0,
            carrier_type: carrier_value,
        },
        payload_unset=tuple(
            key
            for key in (
                LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
                LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
                "local_sim_projection_generation",
                "local_sim_projection_terminal_failure",
                "local_sim_projection_readback_failure",
                "local_sim_projection_readback_terminal_failure",
                "local_sim_valuation_pending_v1",
                "local_sim_valuation_completion_v1",
                "local_sim_persistence",
                "local_sim_durable_minute_loop",
                "strategy_performance",
                "performance_projection",
                "broker_order_handles",
                "broker_called",
                "submitted_intents",
            )
            if key != carrier_type
        ),
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.list_local_sim_execution_states(run.run_id, authoritative=True)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_AUTHORITY_ORPHAN_CARRIER"
    assert exc_info.value.context["carrier_type"] == carrier_type


def test_localsim_public_authority_accepts_exact_initial_broker_facts() -> None:
    repo, run, _, _ = _localsim_authority_review_fixture()
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={
            LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: {},
            LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: 0,
            "broker_called": False,
            "submitted_intents": 0,
        },
        payload_unset=(
            LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY,
            LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY,
            "local_sim_projection_generation",
            "local_sim_projection_terminal_failure",
            "local_sim_projection_readback_failure",
            "local_sim_projection_readback_terminal_failure",
            "local_sim_valuation_pending_v1",
            "local_sim_valuation_completion_v1",
            "local_sim_persistence",
            "local_sim_durable_minute_loop",
            "strategy_performance",
            "performance_projection",
            "broker_order_handles",
        ),
    )

    assert repo.list_local_sim_execution_states(run.run_id, authoritative=True) == []


def test_localsim_post_close_orphan_carrier_fails_run_and_continues_independent_binding() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    peer_binding = StrategyRuntimeReleaseService(repository=repo).create_binding(
        strategy_id="strategy_local_scheduler_peer",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        approval_state=SimulationBindingApprovalState.SIM_VALIDATING,
        created_by="unit-test",
        created_reason="orphan carrier isolation test",
    )
    orphan_outbox = LocalSimProjectionOutboxV1(
        receipt_id="lsec_orphan_post_close",
        run_id="run_orphan_post_close",
        plan_id="plan_orphan_post_close",
        generation=1,
        economic_hash="economic_hash_orphan_post_close",
        projection_payload={"schema_version": "local_sim_projection_payload_v1"},
    )
    bad_run = SimulationDailyRun(
        run_id=orphan_outbox.run_id,
        trade_date=TRADE_DATE,
        strategy_id=local_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash or "",
        binding_id=local_binding.binding_id,
        binding_hash=local_binding.binding_hash or "",
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json={
            LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: 0,
            LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY: orphan_outbox.model_dump(mode="json"),
        },
    )
    good_run = SimulationDailyRun(
        run_id="run_empty_post_close_peer",
        trade_date=TRADE_DATE,
        strategy_id=peer_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash or "",
        binding_id=peer_binding.binding_id,
        binding_hash=peer_binding.binding_hash or "",
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json={"no_rebalance_required": True, "broker_called": False},
    )
    repo.save_simulation_daily_run(bad_run)
    repo.save_simulation_daily_run(good_run)
    scheduler = SimulationLifecycleScheduler(repository=repo)

    results = scheduler._terminalize_post_close_localsim_runs(  # noqa: SLF001
        trade_date=TRADE_DATE,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        strategy_id=None,
        limit=10,
        as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC),
    )

    by_run_id = {item["run_id"]: item for item in results}
    assert bad_run.run_id in by_run_id, results
    failed = by_run_id[bad_run.run_id]
    assert failed["status"] == "RECOVERY_FAILED"
    assert failed["error"]["context"]["reason_code"] == ("LOCALSIM_DURABLE_STATE_AUTHORITY_ORPHAN_CARRIER")
    latest_bad = repo.get_simulation_daily_run(bad_run.run_id)
    assert latest_bad.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert "localsim_post_close_terminalization" not in latest_bad.run_payload_json
    assert by_run_id[good_run.run_id]["reason_code"] == "LOCALSIM_POST_CLOSE_NO_REBALANCE_SUCCESS"
    assert repo.get_simulation_daily_run(good_run.run_id).status == SimulationDailyRunStatus.SUCCEEDED


def test_localsim_economic_readback_rejects_extra_active_state_on_public_repository_seam() -> None:
    repo, run, receipt, outbox = _localsim_authority_review_fixture()
    active = repo.list_local_sim_execution_states(run.run_id, authoritative=True)[0]
    duplicate = LocalSimExecutionStateV1.model_validate(
        {
            **active.model_dump(mode="json"),
            "state_id": "",
            "algo_instance_id": f"review_duplicate_{active.algo_instance_id}",
            "state_hash": "",
        }
    )
    raw_states = deepcopy(run.run_payload_json[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY])
    raw_states[duplicate.state_id] = duplicate.model_dump(mode="json")
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY: raw_states},
    )
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=outbox)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_ACTIVE_AUTHORITY_CONFLICT"
    assert exc_info.value.context["run_id"] == run.run_id


@pytest.mark.parametrize("field", ["receipt_id", "run_id", "plan_id", "generation", "economic_hash"])
def test_localsim_economic_readback_rejects_forged_outbox_identity(
    field: str,
) -> None:
    repo, run, receipt, outbox = _localsim_authority_review_fixture()
    forged_value: object = outbox.generation + 1 if field == "generation" else f"forged_{field}"
    forged = _rebuilt_local_sim_outbox(outbox, **{field: forged_value})
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY: forged.model_dump(mode="json")},
    )
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        repo.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=forged)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_READBACK_IDENTITY_CONFLICT"
    assert exc_info.value.context["run_id"] == run.run_id


def test_localsim_economic_readback_accepts_terminal_history_and_is_repeatable() -> None:
    repo, run, receipt, outbox = _localsim_authority_review_fixture()
    active = repo.list_local_sim_execution_states(run.run_id, authoritative=True)[0]
    historical = LocalSimExecutionStateV1.model_validate(
        {
            **active.model_dump(mode="json"),
            "state_id": "",
            "algo_instance_id": f"review_history_{active.algo_instance_id}",
            "filled_quantity": active.total_quantity,
            "remaining_quantity": 0,
            "runtime_status": LocalSimExecutionRuntimeStatus.FILLED.value,
            "state_hash": "",
            "created_at": (active.created_at - timedelta(seconds=1)).isoformat(),
            "updated_at": (active.updated_at - timedelta(seconds=1)).isoformat(),
        }
    )
    raw_states = deepcopy(run.run_payload_json[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY])
    raw_states[historical.state_id] = historical.model_dump(mode="json")
    repo.update_simulation_daily_run(
        run.run_id,
        payload_patch={LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY: raw_states},
    )
    first = repo.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=outbox)
    second = repo.readback_local_sim_economic_commit(run_id=run.run_id, receipt=receipt, outbox=outbox)
    assert first.run_id == second.run_id == run.run_id
    assert [state.state_id for state in repo.list_local_sim_execution_states(run.run_id, authoritative=True)] == sorted(
        receipt.economic_facts["state_hashes"]
    )


def test_localsim_economic_readback_uses_committed_independent_dev_postgres_connections() -> None:
    if os.getenv("AISTOCK_RUN_SIMULATION_RUNTIME_DEV_DB") != "1":
        pytest.skip("set AISTOCK_RUN_SIMULATION_RUNTIME_DEV_DB=1 for disposable DEV PostgreSQL rows")
    from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn

    nonce = uuid4().hex
    source_repo, run, receipt, outbox = _localsim_authority_review_fixture(
        package_id=f"pkg_scheduler_{nonce}",
        release_metadata={"dev_db_nonce": nonce},
    )
    release = source_repo.releases[run.release_id]
    binding = source_repo.bindings[run.binding_id]
    evidence = source_repo.daily_selection_evidences[run.selection_evidence_id or ""]
    plan = source_repo.execution_plans[run.execution_plan_id or ""]
    dsn = _dev_dsn()
    writer_connections: list[Any] = []
    readback_connections: list[Any] = []
    writer_pids: list[int] = []
    readback_pids: list[int] = []

    def connection_factory(connections: list[Any], pids: list[int]):
        @contextmanager
        def factory():
            connection = psycopg2.connect(**dsn, connect_timeout=5)
            connections.append(connection)
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                pids.append(int(cursor.fetchone()[0]))
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

        return factory

    writer = SimulationRuntimeRepository(conn_factory=connection_factory(writer_connections, writer_pids))
    readback = SimulationRuntimeRepository(conn_factory=connection_factory(readback_connections, readback_pids))

    def replace_payload(payload: dict[str, Any]) -> None:
        current = writer.get_simulation_daily_run(run.run_id)
        writer.update_simulation_daily_run(
            run.run_id,
            payload_patch=payload,
            payload_unset=tuple(sorted(set(current.run_payload_json) - set(payload))),
        )

    try:
        package_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with package_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO strategy_pkg.package (
                        package_id, package_name, package_version, source_type, source_id,
                        package_status, manifest_json, manifest_sha256
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        release.package_id,
                        "BUG-992 disposable DEV package",
                        "1.0.0",
                        "candidate_strategy_package",
                        f"bug992_{nonce}",
                        "ACTIVE",
                        psycopg2.extras.Json({"schema_version": "strategy_package_manifest_v1"}),
                        release.manifest_sha256,
                    ),
                )
            package_connection.commit()
        finally:
            package_connection.close()
        writer.save_strategy_runtime_release(release)
        binding_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with binding_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_v2.simulation_release_binding (
                        binding_id, strategy_id, release_id, release_hash, package_id,
                        manifest_sha256, broker_backend, broker_account_id, capital_allocation,
                        strategy_name, order_remark_prefix, effective_from, effective_to,
                        approval_state, binding_config_json, binding_hash, created_by,
                        created_reason, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        binding.binding_id,
                        binding.strategy_id,
                        binding.release_id,
                        binding.release_hash,
                        binding.package_id,
                        binding.manifest_sha256,
                        binding.broker_backend.value,
                        binding.broker_account_id,
                        binding.capital_allocation,
                        binding.strategy_name,
                        binding.order_remark_prefix,
                        binding.effective_from,
                        binding.effective_to,
                        binding.approval_state.value,
                        psycopg2.extras.Json(binding.binding_config_json),
                        binding.binding_hash,
                        binding.created_by,
                        binding.created_reason,
                        binding.created_at,
                        binding.updated_at,
                    ),
                )
            binding_connection.commit()
        finally:
            binding_connection.close()
        writer.save_daily_selection_evidence(evidence)
        writer.save_execution_plan(plan)
        run_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with run_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_v2.simulation_daily_run (
                        run_id, trade_date, strategy_id, broker_backend, package_id,
                        manifest_sha256, release_id, release_hash, binding_id, binding_hash,
                        selection_evidence_id, selection_artifact_hash, execution_plan_id,
                        execution_plan_hash, status, run_payload_json, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        run.run_id,
                        run.trade_date,
                        run.strategy_id,
                        run.broker_backend.value,
                        run.package_id,
                        run.manifest_sha256,
                        run.release_id,
                        run.release_hash,
                        run.binding_id,
                        run.binding_hash,
                        run.selection_evidence_id,
                        run.selection_artifact_hash,
                        run.execution_plan_id,
                        run.execution_plan_hash,
                        run.status.value,
                        psycopg2.extras.Json(run.run_payload_json),
                        run.created_at,
                        run.updated_at,
                    ),
                )
            run_connection.commit()
        finally:
            run_connection.close()
        assert writer.get_simulation_daily_run(run.run_id).run_id == run.run_id
        assert [
            item.run_id
            for item in readback.list_simulation_daily_runs(
                trade_date_before=run.trade_date + timedelta(days=1),
                strategy_id=run.strategy_id,
                status=run.status,
                limit=1,
            )
        ] == [run.run_id]
        assert [
            item.run_id
            for item in readback.list_simulation_daily_runs(
                strategy_id=run.strategy_id,
                status=run.status,
                limit=1,
            )
        ] == [run.run_id]

        raw_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with raw_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT run_payload_json FROM paper_v2.simulation_daily_run WHERE run_id = %s",
                    (run.run_id,),
                )
                row = cursor.fetchone()
                assert row is not None
                assert isinstance(row[0], dict)
                assert row[0][LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY]["outbox_id"] == outbox.outbox_id
        finally:
            raw_connection.rollback()
            raw_connection.close()

        retry_observed = datetime(2026, 5, 22, 10, 0, tzinfo=UTC)
        initial_claim = writer.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_INITIAL_CLAIM",
            source_fingerprint="f" * 64,
            as_of_time=retry_observed,
            lease_seconds=600,
        )
        duplicate_initial_claim = readback.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_INITIAL_CLAIM",
            source_fingerprint="f" * 64,
            as_of_time=retry_observed,
            lease_seconds=600,
        )
        assert initial_claim.should_execute is True
        assert initial_claim.claim_token is not None
        assert duplicate_initial_claim.should_execute is False
        assert duplicate_initial_claim.claim_token == initial_claim.claim_token
        writer.clear_simulation_retry_control(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_INITIAL_CLAIM",
            expected_claim_token=initial_claim.claim_token,
        )
        writer.record_simulation_retry_failure(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_READBACK",
            source_fingerprint="a" * 64,
            failure_fingerprint="b" * 64,
            failure_stage="DEV_POSTGRES_READBACK",
            error={
                "type": "DataUnavailableError",
                "message": "disposable DEV retry evidence",
                "reason_code": "DEV_POSTGRES_RETRY_EVIDENCE",
                "context": {"run_id": run.run_id},
            },
            as_of_time=retry_observed,
            base_delay_seconds=60,
            max_delay_seconds=3600,
        )
        early_retry = readback.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_READBACK",
            source_fingerprint="a" * 64,
            as_of_time=retry_observed + timedelta(seconds=30),
            lease_seconds=600,
        )
        assert early_retry.should_execute is False
        due_retry = readback.claim_simulation_retry_attempt(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_READBACK",
            source_fingerprint="a" * 64,
            as_of_time=retry_observed + timedelta(seconds=60),
            lease_seconds=600,
        )
        assert due_retry.should_execute is True
        assert due_retry.reason == "retry_claimed"
        cleared_retry = writer.clear_simulation_retry_control(
            run_id=run.run_id,
            retry_key="RECOVERY:DEV_POSTGRES_READBACK",
            expected_claim_token=due_retry.claim_token,
        )
        assert "simulation_scheduler_retry_control_v1" not in cleared_retry.run_payload_json

        active = source_repo.list_local_sim_execution_states(run.run_id, authoritative=True)[0]
        historical = LocalSimExecutionStateV1.model_validate(
            {
                **active.model_dump(mode="json"),
                "state_id": "",
                "algo_instance_id": f"postgres_history_{active.algo_instance_id}",
                "filled_quantity": active.total_quantity,
                "remaining_quantity": 0,
                "runtime_status": LocalSimExecutionRuntimeStatus.FILLED.value,
                "state_hash": "",
                "created_at": (active.created_at - timedelta(seconds=1)).isoformat(),
                "updated_at": (active.updated_at - timedelta(seconds=1)).isoformat(),
            }
        )
        legal_payload = deepcopy(run.run_payload_json)
        legal_payload[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY][historical.state_id] = historical.model_dump(mode="json")
        replace_payload(legal_payload)
        first = readback.readback_local_sim_economic_commit(
            run_id=run.run_id,
            receipt=receipt,
            outbox=outbox,
        )
        second = readback.readback_local_sim_economic_commit(
            run_id=run.run_id,
            receipt=receipt,
            outbox=outbox,
        )
        assert first.run_id == second.run_id == run.run_id

        duplicate = LocalSimExecutionStateV1.model_validate(
            {
                **active.model_dump(mode="json"),
                "state_id": "",
                "algo_instance_id": f"postgres_duplicate_{active.algo_instance_id}",
                "state_hash": "",
            }
        )
        extra_active_payload = deepcopy(run.run_payload_json)
        extra_active_payload[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY][duplicate.state_id] = duplicate.model_dump(
            mode="json"
        )
        replace_payload(extra_active_payload)
        with pytest.raises(InvalidStateTransitionError) as active_info:
            readback.readback_local_sim_economic_commit(
                run_id=run.run_id,
                receipt=receipt,
                outbox=outbox,
            )
        assert active_info.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_ACTIVE_AUTHORITY_CONFLICT"

        orphan_payload = deepcopy(run.run_payload_json)
        orphan_payload[LOCAL_SIM_EXECUTION_STATES_PAYLOAD_KEY] = {}
        orphan_payload[LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY] = {}
        orphan_payload[LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY] = 0
        orphan_payload[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = outbox.model_dump(mode="json")
        orphan_payload.pop(LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY, None)
        orphan_payload.pop("local_sim_projection_generation", None)
        replace_payload(orphan_payload)
        with pytest.raises(InvalidStateTransitionError) as orphan_info:
            readback.readback_local_sim_economic_commit(
                run_id=run.run_id,
                receipt=receipt,
                outbox=outbox,
            )
        assert orphan_info.value.context["reason_code"] == ("LOCALSIM_DURABLE_STATE_AUTHORITY_ORPHAN_CARRIER")

        forged_receipt = _rebuilt_local_sim_receipt(receipt, run_id=f"forged_{run.run_id}")
        forged_receipt_payload = deepcopy(run.run_payload_json)
        forged_receipt_payload[LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY] = {
            forged_receipt.receipt_id: forged_receipt.model_dump(mode="json")
        }
        replace_payload(forged_receipt_payload)
        with pytest.raises(InvalidStateTransitionError) as receipt_info:
            readback.readback_local_sim_economic_commit(
                run_id=run.run_id,
                receipt=receipt,
                outbox=outbox,
            )
        assert receipt_info.value.context["reason_code"] == (
            "LOCALSIM_DURABLE_STATE_AUTHORITY_RECEIPT_IDENTITY_CONFLICT"
        )

        forged_outbox = _rebuilt_local_sim_outbox(outbox, receipt_id="forged_receipt_id")
        forged_outbox_payload = deepcopy(run.run_payload_json)
        forged_outbox_payload[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY] = forged_outbox.model_dump(mode="json")
        replace_payload(forged_outbox_payload)
        with pytest.raises(InvalidStateTransitionError) as outbox_info:
            readback.readback_local_sim_economic_commit(
                run_id=run.run_id,
                receipt=receipt,
                outbox=forged_outbox,
            )
        assert outbox_info.value.context["reason_code"] == ("LOCALSIM_PROJECTION_OUTBOX_READBACK_IDENTITY_CONFLICT")
        assert writer_pids and readback_pids
        assert set(writer_pids).isdisjoint(readback_pids)
    finally:
        cleanup = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with cleanup.cursor() as cursor:
                cursor.execute("DELETE FROM paper_v2.simulation_daily_run WHERE run_id = %s", (run.run_id,))
                cursor.execute("DELETE FROM paper_v2.execution_plan WHERE plan_id = %s", (plan.plan_id,))
                cursor.execute(
                    "DELETE FROM selection.daily_selection_evidence WHERE evidence_id = %s",
                    (evidence.evidence_id,),
                )
                cursor.execute(
                    "DELETE FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                    (binding.binding_id,),
                )
                cursor.execute(
                    "DELETE FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                    (release.release_id,),
                )
                cursor.execute(
                    "DELETE FROM strategy_pkg.package WHERE package_id = %s",
                    (release.package_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()
            for connection in [*writer_connections, *readback_connections]:
                connection.close()

        cleanup_readback = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with cleanup_readback.cursor() as cursor:
                disposable_rows = (
                    (
                        "strategy_pkg.package.package_id",
                        "SELECT count(*) FROM strategy_pkg.package WHERE package_id = %s",
                        release.package_id,
                    ),
                    (
                        "strategy_pkg.strategy_runtime_release.release_id",
                        "SELECT count(*) FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                        release.release_id,
                    ),
                    (
                        "paper_v2.simulation_release_binding.binding_id",
                        "SELECT count(*) FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        binding.binding_id,
                    ),
                    (
                        "selection.daily_selection_evidence.evidence_id",
                        "SELECT count(*) FROM selection.daily_selection_evidence WHERE evidence_id = %s",
                        evidence.evidence_id,
                    ),
                    (
                        "paper_v2.execution_plan.plan_id",
                        "SELECT count(*) FROM paper_v2.execution_plan WHERE plan_id = %s",
                        plan.plan_id,
                    ),
                    (
                        "paper_v2.simulation_daily_run.run_id",
                        "SELECT count(*) FROM paper_v2.simulation_daily_run WHERE run_id = %s",
                        run.run_id,
                    ),
                )
                for identity_label, query, identity_value in disposable_rows:
                    cursor.execute(query, (identity_value,))
                    assert cursor.fetchone()[0] == 0, f"cleanup left disposable DEV row for {identity_label}"
        finally:
            cleanup_readback.rollback()
            cleanup_readback.close()


def test_localsim_legacy_plan_terminalization_commits_across_independent_dev_postgres_connections() -> None:
    if os.getenv("AISTOCK_RUN_SIMULATION_RUNTIME_DEV_DB") != "1":
        pytest.skip("set AISTOCK_RUN_SIMULATION_RUNTIME_DEV_DB=1 for disposable DEV PostgreSQL rows")
    from backend.tests.paper_trading_v2.fixtures_dev_db import _dev_dsn

    nonce = uuid4().hex
    source_repo, run, binding, _ = _localsim_legacy_plan_failed_run_fixture(
        failed_status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        package_id=f"pkg_scheduler_{nonce}",
        release_metadata={"dev_db_nonce": nonce},
    )
    release = source_repo.releases[run.release_id]
    evidence = source_repo.daily_selection_evidences[run.selection_evidence_id or ""]
    successor_plan = source_repo.execution_plans[run.execution_plan_id or ""]
    predecessor_plan = source_repo.execution_plans[run.run_payload_json["rebuilt_from_execution_plan_id"]]
    dsn = _dev_dsn()
    writer_connections: list[Any] = []
    readback_connections: list[Any] = []
    writer_pids: list[int] = []
    readback_pids: list[int] = []

    def connection_factory(connections: list[Any], pids: list[int]):
        @contextmanager
        def factory():
            connection = psycopg2.connect(**dsn, connect_timeout=5)
            connections.append(connection)
            with connection.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                pids.append(int(cursor.fetchone()[0]))
            try:
                yield connection
                connection.commit()
            except Exception:
                connection.rollback()
                raise

        return factory

    writer = SimulationRuntimeRepository(conn_factory=connection_factory(writer_connections, writer_pids))
    readback = SimulationRuntimeRepository(conn_factory=connection_factory(readback_connections, readback_pids))

    try:
        package_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with package_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO strategy_pkg.package (
                        package_id, package_name, package_version, source_type, source_id,
                        package_status, manifest_json, manifest_sha256
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        release.package_id,
                        "BUG-992 disposable DEV package",
                        "1.0.0",
                        "candidate_strategy_package",
                        f"bug992_{nonce}",
                        "ACTIVE",
                        psycopg2.extras.Json({"schema_version": "strategy_package_manifest_v1"}),
                        release.manifest_sha256,
                    ),
                )
            package_connection.commit()
        finally:
            package_connection.close()
        writer.save_strategy_runtime_release(release)
        binding_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with binding_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_v2.simulation_release_binding (
                        binding_id, strategy_id, release_id, release_hash, package_id,
                        manifest_sha256, broker_backend, broker_account_id, capital_allocation,
                        strategy_name, order_remark_prefix, effective_from, effective_to,
                        approval_state, binding_config_json, binding_hash, created_by,
                        created_reason, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        binding.binding_id,
                        binding.strategy_id,
                        binding.release_id,
                        binding.release_hash,
                        binding.package_id,
                        binding.manifest_sha256,
                        binding.broker_backend.value,
                        binding.broker_account_id,
                        binding.capital_allocation,
                        binding.strategy_name,
                        binding.order_remark_prefix,
                        binding.effective_from,
                        binding.effective_to,
                        binding.approval_state.value,
                        psycopg2.extras.Json(binding.binding_config_json),
                        binding.binding_hash,
                        binding.created_by,
                        binding.created_reason,
                        binding.created_at,
                        binding.updated_at,
                    ),
                )
            binding_connection.commit()
        finally:
            binding_connection.close()
        writer.save_daily_selection_evidence(evidence)
        writer.save_execution_plan(predecessor_plan)
        writer.save_execution_plan(successor_plan)
        run_connection = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with run_connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO paper_v2.simulation_daily_run (
                        run_id, trade_date, strategy_id, broker_backend, package_id,
                        manifest_sha256, release_id, release_hash, binding_id, binding_hash,
                        selection_evidence_id, selection_artifact_hash, execution_plan_id,
                        execution_plan_hash, status, run_payload_json, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        run.run_id,
                        run.trade_date,
                        run.strategy_id,
                        run.broker_backend.value,
                        run.package_id,
                        run.manifest_sha256,
                        run.release_id,
                        run.release_hash,
                        run.binding_id,
                        run.binding_hash,
                        run.selection_evidence_id,
                        run.selection_artifact_hash,
                        run.execution_plan_id,
                        run.execution_plan_hash,
                        run.status.value,
                        psycopg2.extras.Json(run.run_payload_json),
                        run.created_at,
                        run.updated_at,
                    ),
                )
            run_connection.commit()
        finally:
            run_connection.close()
        persisted_before = readback.get_simulation_daily_run(run.run_id)
        assert persisted_before.status == SimulationDailyRunStatus.FAILED_RETRYABLE
        assert persisted_before.execution_plan_id == successor_plan.plan_id

        stage = SimulationLifecycleScheduler(repository=writer)
        results = stage._terminalize_stale_localsim_failed_runs(  # noqa: SLF001
            trade_date=run.trade_date + timedelta(days=1),
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            strategy_id=run.strategy_id,
            limit=10,
            as_of_time=datetime(2026, 5, 22, 10, 0, tzinfo=UTC),
        )

        assert [item["run_id"] for item in results] == [run.run_id]
        result = results[0]
        assert result["previous_status"] == SimulationDailyRunStatus.FAILED_RETRYABLE.value
        assert result["status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
        assert result["reason_code"] == "LOCALSIM_HISTORICAL_FAILED_RUN_LEGACY_PLAN_RETIRED"
        assert result["historical_failed_legacy_plan_terminalization"] is True
        assert result["cross_day_terminalization"] is True
        assert result["durable_minute_loop_advanced"] is False
        assert result["legacy_execution_restored"] is False

        persisted = readback.get_simulation_daily_run(run.run_id)
        assert persisted.status == SimulationDailyRunStatus.FAILED_TERMINAL
        terminalization = persisted.run_payload_json["localsim_historical_legacy_plan_terminalization_v1"]
        assert terminalization["schema_version"] == "localsim_historical_legacy_plan_terminalization_v1"
        assert terminalization["reason_code"] == "LOCALSIM_HISTORICAL_FAILED_RUN_LEGACY_PLAN_RETIRED"
        assert terminalization["plan_id"] == successor_plan.plan_id
        assert terminalization["plan_algo_code"] == "V25_1_SMALL_CAP"
        assert terminalization["required_algo_code"] == "TWAP"
        assert terminalization["retired_policy_reason_code"] == ("LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED")
        assert terminalization["terminal_status"] == SimulationDailyRunStatus.FAILED_TERMINAL.value
        assert terminalization["authoritative_state_count"] == 246
        assert terminalization["active_state_count"] == 246
        assert len(terminalization["authoritative_state_set_sha256"]) == 64
        assert terminalization["historical_broker_called"] is True
        assert terminalization["parent_resubmitted"] is False
        assert terminalization["broker_replayed"] is False
        assert terminalization["predecessor_projection_replayed"] is False
        assert terminalization["durable_minute_loop_advanced"] is False
        assert terminalization["legacy_execution_restored"] is False
        assert terminalization["fallback_used"] is False
        assert terminalization["runtime_context_loaded"] is False
        assert terminalization["market_data_loaded"] is False
        assert (
            persisted.run_payload_json["local_sim_execution_states_v1"]
            == (run.run_payload_json["local_sim_execution_states_v1"])
        )
        assert persisted.run_payload_json["rebuilt_from_execution_plan_id"] == predecessor_plan.plan_id
        retry_control = persisted.run_payload_json.get("simulation_scheduler_retry_control_v1")
        assert retry_control in (
            None,
            {"schema_version": "simulation_scheduler_retry_control_v1", "entries": {}},
        )
        assert writer_pids and readback_pids
        assert set(writer_pids).isdisjoint(readback_pids)
    finally:
        cleanup = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with cleanup.cursor() as cursor:
                cursor.execute("DELETE FROM paper_v2.simulation_daily_run WHERE run_id = %s", (run.run_id,))
                cursor.execute(
                    "DELETE FROM paper_v2.execution_plan WHERE plan_id IN (%s, %s)",
                    (predecessor_plan.plan_id, successor_plan.plan_id),
                )
                cursor.execute(
                    "DELETE FROM selection.daily_selection_evidence WHERE evidence_id = %s",
                    (evidence.evidence_id,),
                )
                cursor.execute(
                    "DELETE FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                    (binding.binding_id,),
                )
                cursor.execute(
                    "DELETE FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                    (release.release_id,),
                )
                cursor.execute(
                    "DELETE FROM strategy_pkg.package WHERE package_id = %s",
                    (release.package_id,),
                )
            cleanup.commit()
        finally:
            cleanup.close()
            for connection in [*writer_connections, *readback_connections]:
                connection.close()

        cleanup_readback = psycopg2.connect(**dsn, connect_timeout=5)
        try:
            with cleanup_readback.cursor() as cursor:
                disposable_rows = (
                    (
                        "strategy_pkg.package.package_id",
                        "SELECT count(*) FROM strategy_pkg.package WHERE package_id = %s",
                        release.package_id,
                    ),
                    (
                        "strategy_pkg.strategy_runtime_release.release_id",
                        "SELECT count(*) FROM strategy_pkg.strategy_runtime_release WHERE release_id = %s",
                        release.release_id,
                    ),
                    (
                        "paper_v2.simulation_release_binding.binding_id",
                        "SELECT count(*) FROM paper_v2.simulation_release_binding WHERE binding_id = %s",
                        binding.binding_id,
                    ),
                    (
                        "selection.daily_selection_evidence.evidence_id",
                        "SELECT count(*) FROM selection.daily_selection_evidence WHERE evidence_id = %s",
                        evidence.evidence_id,
                    ),
                    (
                        "paper_v2.execution_plan.plan_id",
                        "SELECT count(*) FROM paper_v2.execution_plan WHERE plan_id IN (%s, %s)",
                        (predecessor_plan.plan_id, successor_plan.plan_id),
                    ),
                    (
                        "paper_v2.simulation_daily_run.run_id",
                        "SELECT count(*) FROM paper_v2.simulation_daily_run WHERE run_id = %s",
                        run.run_id,
                    ),
                )
                for identity_label, query, identity_value in disposable_rows:
                    params = identity_value if isinstance(identity_value, tuple) else (identity_value,)
                    cursor.execute(query, params)
                    assert cursor.fetchone()[0] == 0, f"cleanup left disposable DEV row for {identity_label}"
        finally:
            cleanup_readback.rollback()
            cleanup_readback.close()


def _legacy_scheduler_miniqmt_account_level_reconciliation_warning_does_not_fail_current_slot() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="stale_other_slot",
            strategy_name="StaleOtherSlot",
            display_name="Stale Other Slot",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_current_slot_ok",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_current_slot_ok",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_stale_other_slot",
            strategy_id="stale_other_slot",
            symbol="000004.SZ",
            open_trade_id="trade_scheduler_qmt_stale_other_slot",
            open_date=date(2026, 5, 20),
            quantity=500,
            available_quantity=500,
            remaining_quantity=500,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("4000.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
                )
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    reconciliation = run.run_payload_json["reconcile_after_submit"]
    assert submitted.results[0].status == "RECONCILED"
    assert run.status == SimulationDailyRunStatus.SUCCEEDED
    assert run.run_payload_json["broker_called"] is True
    assert reconciliation["run"]["status"] == "WARNING"
    assert reconciliation["strategy_scope"]["status"] == "SUCCEEDED"
    assert reconciliation["strategy_scope"]["account_level_issue_count"] == 1
    assert reconciliation["run_status_gate"]["status"] == "SUCCEEDED"
    assert reconciliation["run_status_gate"]["reason"] == "strategy_scope_has_no_blocking_issues"


def _legacy_scheduler_miniqmt_reconcile_warning_marks_run_retryable() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_000003_reconcile_warning",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_000003_reconcile_warning",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 1, "can_sell": 1}])
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_qmt",
                    current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
                    current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
                    managed_order_service=QmtManagedOrderService(
                        repository=qmt_repo,
                        broker=broker,  # type: ignore[arg-type]
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_ledger_repository=qmt_repo,
                    qmt_sync_service=QmtStrategyLedgerSyncService(
                        repository=qmt_repo,
                        qmt_client=snapshot_client,
                        account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
                        trade_date=TRADE_DATE,
                        calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
                    ),
                    qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
                    broker_positions=[{"stock_code": "000003.SZ", "quantity": 1, "can_sell": 1}],
                )
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.results[0].status == "RECONCILIATION_WARNING"
    assert submitted.results[0].run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert submitted.results[0].run.run_payload_json["last_stage"] == "FAILED_RETRYABLE"
    reconciliation = submitted.results[0].run.run_payload_json["reconcile_after_submit"]
    assert reconciliation["position_authority"] == "broker_positions"
    assert reconciliation["issues"][0]["issue_type"] == "UNBACKED_STRATEGY_POSITION"
    assert reconciliation["strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 1
    assert reconciliation["raw_strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 77


def test_miniqmt_reconciliation_warning_keeps_durable_event_loop_pending() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "submitted_intents": 13,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628",
                "batch_status": OrderBatchStatus.SUBMITTING.value,
                "succeeded": 13,
                "failed": 0,
                "pending": 16,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "runtime_id": "mqrt_bug628",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "submitted_child_count": 13,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "PENDING"
    assert gate["reason"] == "miniqmt_event_loop_pending_after_reconciliation_warning"
    assert gate["pending_event_loop"] == {
        "schema_version": "miniqmt_pending_event_loop_evidence_v1",
        "eligible": True,
        "batch_id": "qmtbatch_bug628",
        "runtime_id": "mqrt_bug628",
        "runtime_evidence_source": "simulation_runtime_event_loop_submit",
        "batch_status": OrderBatchStatus.SUBMITTING.value,
        "active_algo_count": 29,
        "pending_algo_count": 16,
        "failed_or_rejected_count": 0,
        "identity_sources": {
            "payload_batch_id": "qmtbatch_bug628",
            "result_batch_id": "qmtbatch_bug628",
            "payload_batch_status": OrderBatchStatus.SUBMITTING.value,
            "result_batch_status": OrderBatchStatus.SUBMITTING.value,
        },
        "pending_count_sources": {
            "payload_pending_intents": 16,
            "result_pending": 16,
            "result_pending_child_trigger_count": 16,
            "runtime_pending_algo_count": 16,
        },
        "failure_count_sources": {
            "payload_failed_intents": 0,
            "result_failed": 0,
            "runtime_rejected_child_count": 0,
        },
        "conflicts": [],
    }


def test_miniqmt_post_close_pending_algos_are_retryable_not_fake_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = {
        "qmt_batch_id": "qmtbatch_bug633",
        "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
        "broker_called": True,
        "submitted_intents": 26,
        "failed_intents": 0,
        "pending_intents": 3,
        "qmt_batch_result": {
            "batch_id": "qmtbatch_bug633",
            "batch_status": OrderBatchStatus.SUBMITTING.value,
            "succeeded": 26,
            "failed": 0,
            "pending": 3,
            "pending_child_trigger_count": 3,
            "runtime_evidence": {
                "source": "simulation_runtime_event_loop_tick_driver",
                "runtime_id": "mqrt_bug633",
                "active_algo_count": 29,
                "pending_algo_count": 3,
                "submitted_child_count": 26,
                "rejected_child_count": 0,
            },
        },
        "miniqmt_event_loop_tick_driver": {
            "pending_parent_intent_ids": ["intent_a", "intent_b", "intent_c"],
        },
    }
    run = SimpleNamespace(
        run_id="simrun_bug633",
        trade_date=TRADE_DATE,
        strategy_id="strategy_bug633",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json=payload,
    )

    class _Repository:
        status: SimulationDailyRunStatus | None = None
        payload_patch: dict[str, Any] | None = None

        def update_simulation_daily_run(self, run_id: str, *, status, payload_patch, payload_unset=None):  # noqa: ANN001, ANN201, ARG002
            assert run_id == run.run_id
            self.status = status
            self.payload_patch = payload_patch
            return SimpleNamespace(
                run_id=run.run_id,
                trade_date=run.trade_date,
                strategy_id=run.strategy_id,
                broker_backend=run.broker_backend,
                status=status,
                run_payload_json={**payload, **payload_patch},
            )

    repository = _Repository()
    scheduler = object.__new__(SimulationLifecycleScheduler)
    scheduler.repository = repository
    monkeypatch.setattr(
        scheduler,
        "_fresh_miniqmt_post_close_payload",
        lambda **_kwargs: (payload, {"schema_version": "miniqmt_post_close_fresh_reconcile_v1"}),
    )

    result = scheduler._post_close_terminalize_miniqmt_run(  # noqa: SLF001
        run=run,
        as_of_time=datetime(2026, 5, 21, 15, 1),
    )

    assert result["status"] == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result["reason"] == "miniqmt_post_close_event_loop_pending_algos_untriggered"
    assert repository.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    terminalization = repository.payload_patch["miniqmt_post_close_terminalization"]
    assert terminalization["audit_state"] == "failed_retryable_after_close"
    assert terminalization["event_loop_pending_after_close"] == {
        "schema_version": "miniqmt_event_loop_pending_after_close_v1",
        "reason_code": "MINIQMT_EVENT_LOOP_PENDING_ALGOS_MARKET_CLOSED",
        "stage": "MINIQMT_POST_CLOSE_TERMINALIZATION",
        "reason": "event_loop_algorithms_remained_running_without_child_order_until_market_close",
        "pending_intents": 3,
        "pending_parent_intent_ids": ["intent_a", "intent_b", "intent_c"],
        "qmt_batch_id": "qmtbatch_bug633",
        "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
    }


def test_miniqmt_reconciliation_warning_rejects_unproven_pending_event_loop() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628_invalid",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628_invalid",
                "batch_status": OrderBatchStatus.SUBMITTING.value,
                "failed": 0,
                "pending": 16,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "blocked"
    assert gate["pending_event_loop"]["eligible"] is False
    assert gate["pending_event_loop"]["conflicts"] == ["runtime_id_missing"]


def test_miniqmt_reconciliation_warning_rejects_conflicting_durable_evidence() -> None:
    run = SimpleNamespace(
        run_payload_json={
            "qmt_batch_id": "qmtbatch_bug628_payload",
            "qmt_batch_status": OrderBatchStatus.SUBMITTING.value,
            "broker_called": True,
            "failed_intents": 0,
            "pending_intents": 16,
            "qmt_batch_result": {
                "batch_id": "qmtbatch_bug628_result",
                "batch_status": OrderBatchStatus.FAILED.value,
                "failed": 0,
                "pending": 15,
                "pending_child_trigger_count": 16,
                "runtime_evidence": {
                    "source": "simulation_runtime_event_loop_submit",
                    "runtime_id": "mqrt_bug628_conflict",
                    "active_algo_count": 29,
                    "pending_algo_count": 16,
                    "rejected_child_count": 0,
                },
            },
        }
    )

    gate = SimulationLifecycleScheduler._miniqmt_submit_result_gate(
        run=run,
        run_status_gate={"status": "WARNING", "reason": "strategy_scope_has_blocking_issues"},
        batch_residual_summary={},
        open_order_evidence={"open_order_count": 0},
        side_effect_evidence={"broker_side_effect_count": 13},
    )

    assert gate["status"] == "blocked"
    assert gate["pending_event_loop"]["eligible"] is False
    assert gate["pending_event_loop"]["conflicts"] == [
        "batch_id_conflict",
        "result_batch_not_submitting",
        "batch_status_conflict",
        "pending_count_conflict",
    ]


def _tick_driver_result_payload() -> dict[str, Any]:
    return {
        "schema_version": "miniqmt_event_loop_tick_driver_v1",
        "runtime_id": "mqrt_bug680",
        "source": "simulation_runtime_event_loop_tick_driver",
        "submitted_child_count": 13,
        "rejected_child_count": 0,
        "pending_algo_count": 16,
        "batch_results": {},
        "runtime_evidence": {
            "runtime_id": "mqrt_bug680",
            "source": "simulation_runtime_event_loop_tick_driver",
            "submitted_child_count": 13,
            "rejected_child_count": 0,
            "pending_algo_count": 16,
            "trade_event_count": 2,
        },
    }


def _tick_driver_batch_result_row() -> dict[str, Any]:
    return {
        "success": True,
        "intent_id": "parent_bug683",
        "qmt_order_id": "100001",
        "broker_message": "accepted",
        "broker_called": True,
        "preflight": {"allowed": True},
    }


def _tick_driver_persist_fixture(*, case: str | None = None):
    batch_id = "qmtbatch_bug683"
    row = _tick_driver_batch_result_row()
    result = _tick_driver_result_payload()
    result["batch_results"] = {
        batch_id: {
            "batch_id": batch_id,
            "batch_status": OrderBatchStatus.SUCCEEDED.value,
            "result_json": {"results": [deepcopy(row)]},
            "metadata": {"source": "simulation_runtime_event_loop_tick_driver"},
        }
    }
    run_payload = {
        "qmt_batch_id": batch_id,
        "qmt_batch_status": OrderBatchStatus.SUCCEEDED.value,
        "qmt_batch_result": {
            "success": False,
            "batch_id": batch_id,
            "batch_status": OrderBatchStatus.SUCCEEDED.value,
            "total": 1,
            "results": [deepcopy(row)],
            "runtime_evidence": {"runtime_id": "mqrt_bug680"},
        },
        "broker_called": False,
        "submitted_intents": 0,
        "failed_intents": 0,
        "pending_intents": 1,
    }
    if case == "batch_id_conflict":
        result["batch_results"][batch_id]["batch_id"] = "qmtbatch_other"
    elif case == "invalid_status":
        result["batch_results"][batch_id]["batch_status"] = False
    elif case == "invalid_result_json":
        result["batch_results"][batch_id]["result_json"] = []
    elif case == "invalid_metadata":
        result["batch_results"][batch_id]["metadata"] = []
    elif case == "invalid_result_boolean":
        result["batch_results"][batch_id]["result_json"]["results"][0]["success"] = "false"
    elif case == "invalid_foreign_batch":
        result["batch_results"]["qmtbatch_foreign"] = "malformed"
    elif case == "string_total":
        run_payload["qmt_batch_result"]["total"] = "1"
    elif case == "total_conflict":
        run_payload["qmt_batch_result"]["total"] = 2
    elif case == "durable_status_invalid":
        run_payload["qmt_batch_status"] = "NOT_A_BATCH_STATUS"
    elif case == "broker_called_string":
        run_payload["broker_called"] = "false"
    elif case == "durable_batch_result_list":
        run_payload["qmt_batch_result"] = []
    elif case == "missing_durable_batch_id":
        run_payload["qmt_batch_result"].pop("batch_id")
    elif case == "missing_durable_status":
        run_payload["qmt_batch_result"].pop("batch_status")
    elif case == "missing_durable_results":
        run_payload["qmt_batch_result"].pop("results")
    elif case == "missing_durable_total":
        run_payload["qmt_batch_result"].pop("total")
    elif case == "missing_durable_success":
        run_payload["qmt_batch_result"].pop("success")
    elif case is not None:  # pragma: no cover - parameter table is closed below.
        raise AssertionError(case)
    release, _local_binding, qmt_binding, repository = _release_and_bindings(qmt_only=True)
    run = SimulationDailyRun(
        run_id=f"simrun_bug683_{case or 'valid'}",
        trade_date=TRADE_DATE,
        strategy_id=qmt_binding.strategy_id,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        binding_id=qmt_binding.binding_id,
        binding_hash=qmt_binding.binding_hash,
        account_group_id=qmt_binding.account_group_id,
        strategy_slot_id=qmt_binding.strategy_slot_id,
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        run_payload_json=run_payload,
    )
    repository.save_simulation_daily_run(run)
    scheduler = SimulationLifecycleScheduler(repository=repository)
    return scheduler, repository, run, result


def test_miniqmt_tick_driver_persists_exact_valid_batch_without_defaulting() -> None:
    scheduler, repository, run, result = _tick_driver_persist_fixture()
    try:
        updated = scheduler._persist_miniqmt_tick_driver_result(
            binding=SimpleNamespace(binding_id=run.binding_id),
            run=run,
            plan=SimpleNamespace(plan_id="plan_bug683"),
            result=result,
        )
    finally:
        scheduler.shutdown_selection_inference(wait=True)

    assert updated.run_payload_json["qmt_batch_result"]["total"] == 1
    assert updated.run_payload_json["qmt_batch_result"]["batch_status"] == OrderBatchStatus.SUCCEEDED.value
    assert updated.run_payload_json["qmt_batch_result"]["results"] == [_tick_driver_batch_result_row()]
    assert updated.run_payload_json["qmt_batch_result"]["succeeded"] == 1
    assert updated.run_payload_json["qmt_batch_result"]["triggered_child_order_count"] == 13
    assert updated.run_payload_json["qmt_batch_result"]["runtime_evidence"]["trade_event_count"] == 2
    assert updated.run_payload_json["submitted_intents"] == 1
    assert updated.run_payload_json["failed_intents"] == 0
    assert updated.run_payload_json["pending_intents"] == 0
    assert repository.get_simulation_daily_run(run.run_id) == updated


@pytest.mark.parametrize(
    ("case", "reason_code"),
    [
        ("batch_id_conflict", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT"),
        ("invalid_status", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("invalid_result_json", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_metadata", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_result_boolean", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("invalid_foreign_batch", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("string_total", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID"),
        ("total_conflict", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_CONFLICT"),
        ("durable_status_invalid", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("broker_called_string", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("durable_batch_result_list", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("missing_durable_batch_id", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_IDENTITY_CONFLICT"),
        ("missing_durable_status", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_STATUS_INVALID"),
        ("missing_durable_results", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
        ("missing_durable_total", "MINIQMT_EVENT_LOOP_TICK_DRIVER_BATCH_CARDINALITY_INVALID"),
        ("missing_durable_success", "MINIQMT_EVENT_LOOP_TICK_DRIVER_SCHEMA_INVALID"),
    ],
)
def test_miniqmt_tick_driver_rejects_malformed_batch_before_durable_overwrite(
    case: str,
    reason_code: str,
) -> None:
    scheduler, repository, run, result = _tick_driver_persist_fixture(case=case)
    before = repository.get_simulation_daily_run(run.run_id)
    try:
        with pytest.raises(RuntimeConfigInvalidError) as exc_info:
            scheduler._persist_miniqmt_tick_driver_result(
                binding=SimpleNamespace(binding_id=run.binding_id),
                run=run,
                plan=SimpleNamespace(plan_id="plan_bug683"),
                result=result,
            )
    finally:
        scheduler.shutdown_selection_inference(wait=True)

    assert exc_info.value.context["reason_code"] == reason_code
    assert repository.get_simulation_daily_run(run.run_id) == before


def test_miniqmt_tick_driver_result_requires_exact_schema_identity_and_counts() -> None:
    evidence, submitted, rejected, pending = SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(
        _tick_driver_result_payload()
    )

    assert evidence["runtime_id"] == "mqrt_bug680"
    assert (submitted, rejected, pending) == (13, 0, 16)

    missing = _tick_driver_result_payload()
    missing["runtime_evidence"].pop("submitted_child_count")
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(missing)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_MISSING"

    invalid = _tick_driver_result_payload()
    invalid["pending_algo_count"] = "invalid"
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(invalid)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    conflict = _tick_driver_result_payload()
    conflict["runtime_evidence"]["rejected_child_count"] = 1
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(conflict)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_COUNTER_CONFLICT"

    identity_conflict = _tick_driver_result_payload()
    identity_conflict["runtime_evidence"]["runtime_id"] = "mqrt_other"
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._validated_miniqmt_tick_driver_result(identity_conflict)
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_TICK_DRIVER_IDENTITY_CONFLICT"


def test_miniqmt_pending_and_submitted_evidence_rejects_malformed_counters_instead_of_defaulting() -> None:
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_submitted_children({"submitted_intents": "invalid"})
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos({"pending_intents": float("nan")})
    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_COUNTER_INVALID"

    assert (
        SimulationLifecycleScheduler._mini_qmt_event_loop_has_pending_algos(
            {"qmt_batch_status": OrderBatchStatus.SUBMITTING.value}
        )
        is False
    )


def test_scheduler_broker_backend_filter_limits_tick_scope() -> None:
    release, _, qmt_binding, repo = _release_and_bindings()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={qmt_binding.binding_id: _position_context(portfolio_id="portfolio_qmt")}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )

    assert result.total_bindings == 1
    assert result.results[0].broker_backend == SimulationBrokerBackend.MINIQMT_SIM
    assert result.results[0].binding_id == qmt_binding.binding_id


def test_scheduler_reports_unattended_trading_windows_without_submitting_orders() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_window")}
        ),
    )

    status = scheduler.status()
    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 1, 22, tzinfo=UTC),
    )

    assert status["restart_recovery_mode"] == "persisted_state_only"
    assert [window["window_id"] for window in status["schedule_windows"]] == [
        "pre_open",
        "selection",
        "planning",
        "opening_auction_observe",
        "execution",
        "lunch_recess",
        "execution_afternoon",
        "closing_auction_observe",
        "post_close_reconcile",
    ]
    assert result.planned_count == 1
    assert result.schedule_windows[2]["window_id"] == "planning"
    assert result.schedule_windows[2]["state"] == "ACTIVE"
    assert result.schedule_windows[3]["state"] == "UPCOMING"


@pytest.mark.parametrize(
    ("as_of_time", "window_id", "action"),
    [
        (datetime(2026, 5, 21, 9, 25), "opening_auction_observe", "observe_only"),
        (datetime(2026, 5, 21, 9, 30), "execution", "submit"),
        (datetime(2026, 5, 21, 11, 30), "lunch_recess", "market_wait"),
        (datetime(2026, 5, 21, 13, 0), "execution_afternoon", "submit"),
        (datetime(2026, 5, 21, 14, 57), "closing_auction_observe", "observe_only"),
    ],
)
def test_schedule_windows_segment_non_continuous_trading_phases(
    as_of_time: datetime,
    window_id: str,
    action: str,
) -> None:
    active = next(
        window
        for window in compute_schedule_windows(trade_date=TRADE_DATE, as_of_time=as_of_time)
        if window["state"] == "ACTIVE"
    )

    assert active["window_id"] == window_id
    assert active["action"] == action


def test_localsim_realtime_quote_is_required_only_inside_continuous_submit_windows() -> None:
    _, local_binding, _, _ = _release_and_bindings()
    assert local_binding is not None

    assert (
        SimulationLifecycleScheduler._localsim_realtime_quote_required(
            binding=local_binding,
            trade_date=TRADE_DATE,
            submit=True,
            as_of_time=datetime(2026, 5, 21, 10, 0),
        )
        is True
    )
    assert (
        SimulationLifecycleScheduler._localsim_realtime_quote_required(
            binding=local_binding,
            trade_date=TRADE_DATE,
            submit=True,
            as_of_time=datetime(2026, 5, 21, 12, 0),
        )
        is False
    )
    assert (
        SimulationLifecycleScheduler._localsim_realtime_quote_required(
            binding=local_binding,
            trade_date=TRADE_DATE,
            submit=True,
            as_of_time=datetime(2026, 5, 21, 14, 58),
        )
        is False
    )


def test_trading_rule_applies_limit_evidence_after_actual_side_is_known() -> None:
    service = TradingRuleService()
    status = {
        "is_tradable": True,
        "reason_code": "OK",
        "quote_evidence": {"blocked_sides": ["BUY"]},
    }

    buy = service.decide_order_quantity(
        symbol="000001.SZ",
        side=OrderSide.BUY,
        requested_quantity=100,
        tradability_status=status,
    )
    sell = service.decide_order_quantity(
        symbol="000001.SZ",
        side=OrderSide.SELL,
        requested_quantity=100,
        tplus1_available_quantity=100,
        tradability_status=status,
    )

    assert buy.decision == "REJECT"
    assert buy.reason_code == "LIMIT_UP_BUY_BLOCKED"
    assert sell.decision == "EMIT"
    assert sell.reason_code == "OK"


def test_localsim_selection_and_plan_only_do_not_request_same_day_realtime_quote() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None

    class PhaseAwareContextProvider:
        def __init__(self) -> None:
            self.phase_quote_requirements: list[bool] = []
            self.tradability_quote_requirements: list[bool | None] = []

        def load_context_for_phase(self, **kwargs: Any) -> SimulationRunContext:
            self.phase_quote_requirements.append(bool(kwargs["require_localsim_realtime_quote"]))
            return _position_context(portfolio_id="portfolio_phase_aware")

        def load_pre_trade_tradability(self, **kwargs: Any) -> dict[str, dict[str, Any]]:
            self.tradability_quote_requirements.append(kwargs.get("require_realtime_quote"))
            return {
                symbol: {
                    "schema_version": "pre_trade_tradability_status_v1",
                    "symbol": symbol,
                    "trade_date": kwargs["trade_date"].isoformat(),
                    "is_tradable": True,
                    "reason_code": "OK",
                    "source": "unit_test.daily_status_only",
                }
                for symbol in kwargs["symbols"]
            }

    provider = PhaseAwareContextProvider()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=provider,  # type: ignore[arg-type]
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )

    assert result.planned_count == 1
    assert provider.phase_quote_requirements == [False]
    assert provider.tradability_quote_requirements == []
    causality = result.results[0].execution_plan.plan_payload_json["local_sim_execution_causality"]
    assert causality["eligible_bar_after"] == "2026-05-21T09:29:59.999999+08:00"
    assert result.results[0].run.execution_plan_id == result.results[0].execution_plan.plan_id


def test_localsim_plan_first_created_during_active_window_starts_after_scheduler_time() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_causal_cursor")}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 13, 15),
    )

    causality = result.results[0].execution_plan.plan_payload_json["local_sim_execution_causality"]
    assert causality["eligible_bar_after"] == "2026-05-21T13:15:00+08:00"
    assert causality["cursor_source"] == "first_plan_during_submit_window"


def test_pre_trade_provider_internal_type_error_is_not_retried_or_silenced() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    class BrokenProvider:
        def __init__(self) -> None:
            self.calls = 0

        def get_statuses(self, symbols: list[str], trade_date: date, **kwargs: Any):
            self.calls += 1
            raise TypeError("provider internal contract bug")

    broken = BrokenProvider()
    provider = ProductionSimulationRunContextProvider(pre_trade_tradability_provider=broken)
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    with pytest.raises(TypeError, match="provider internal contract bug"):
        provider.load_pre_trade_tradability(
            symbols=["000001.SZ"],
            trade_date=TRADE_DATE,
            binding=binding,
            require_realtime_quote=False,
        )

    assert broken.calls == 1


def test_localsim_submit_does_not_request_pretrade_quote_after_selection() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    events: list[tuple[str, Any]] = []

    class RecordingSelection(FakeSelectionService):
        def run_selection(self, **kwargs: Any):
            events.append(("selection", None))
            return super().run_selection(**kwargs)

    class RecordingProvider:
        def load_context_for_phase(self, **kwargs: Any) -> SimulationRunContext:
            events.append(("context", kwargs["require_localsim_realtime_quote"]))
            return _position_context(portfolio_id="portfolio_submit_quote", local_broker=FakeLocalSimBroker())

        def load_pre_trade_tradability(self, **kwargs: Any) -> dict[str, dict[str, Any]]:
            events.append(("quote", (kwargs.get("require_realtime_quote"), tuple(kwargs["symbols"]))))
            return {
                symbol: {
                    "schema_version": "pre_trade_tradability_status_v1",
                    "symbol": symbol,
                    "trade_date": kwargs["trade_date"].isoformat(),
                    "is_tradable": True,
                    "reason_code": "OK",
                    "source": "unit_test.realtime_quote",
                }
                for symbol in kwargs["symbols"]
            }

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=RecordingSelection(release, candidates=_candidate_rows()),
        context_provider=RecordingProvider(),  # type: ignore[arg-type]
    )

    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    )

    assert events[0] == ("context", False)
    assert events[1] == ("selection", None)
    assert events == [("context", False), ("selection", None)]


def test_background_scheduler_runs_planning_window_and_keeps_submit_disabled_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: _position_context(portfolio_id="portfolio_background_local"),
                qmt_binding.binding_id: _position_context(portfolio_id="portfolio_background_qmt"),
            }
        ),
    )
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "false")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 1, 22, tzinfo=UTC))

    assert result["should_run"] is True
    assert result["submit"] is False
    assert result["window"]["window_id"] == "planning"
    assert result["trading_calendar"]["is_trading_day"] is True
    assert result["summary"]["planned_count"] == 2
    assert background.status()["default_submit"] is False
    assert background.status()["last_result"]["summary"]["total_bindings"] == 2
    assert background.status()["last_result"]["has_blocking_result"] is False


def test_background_scheduler_noop_window_does_not_erase_last_blocking_result() -> None:
    lifecycle = SimulationLifecycleScheduler(repository=InMemorySimulationRuntimeRepository())
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )
    started_at = datetime(2026, 5, 21, 10, 0, tzinfo=UTC)
    blocking = background._record_result(
        started_at=started_at,
        result={
            "trade_date": TRADE_DATE.isoformat(),
            "reason": "submit",
            "processed": [],
            "errors": [
                {
                    "type": "DataUnavailableError",
                    "message": "required dataset refresh status is missing",
                }
            ],
            "alerts": [],
        },
    )
    noop = background._record_result(
        started_at=started_at + timedelta(hours=5),
        result={
            "trade_date": TRADE_DATE.isoformat(),
            "reason": "outside_configured_windows",
            "processed": [],
            "errors": [],
            "alerts": [],
        },
    )

    status = background.status()
    assert blocking["has_blocking_result"] is True
    assert noop["has_blocking_result"] is False
    assert status["last_result"]["reason"] == "outside_configured_windows"
    assert status["last_blocking_result"]["reason"] == "submit"
    assert status["last_blocking_result"]["errors"][0]["type"] == "DataUnavailableError"


def test_background_scheduler_run_loop_exception_reports_blocked_while_thread_stays_alive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lifecycle = SimulationLifecycleScheduler(repository=InMemorySimulationRuntimeRepository())
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )
    recorded = threading.Event()
    original_record = background._record_loop_exception

    def record_and_signal(exc: Exception) -> dict[str, Any]:
        failure = original_record(exc)
        recorded.set()
        return failure

    def crash() -> dict[str, Any]:
        raise DataUnavailableError(
            "scheduler dependency crashed " + ("x" * 3000),
            context={
                "reason_code": "SIMULATION_TEST_DEPENDENCY_UNAVAILABLE",
                "stage": "TEST_DEPENDENCY_READ",
                "binding_id": "binding-" + ("b" * 600),
                "credential": "must-not-be-projected",
            },
        )

    monkeypatch.setattr(background, "_record_loop_exception", record_and_signal)
    monkeypatch.setattr(background, "run_once", crash)
    try:
        background.start(interval_seconds=60)
        assert recorded.wait(timeout=2.0)

        status = background.status()
        assert status["running"] is True
        assert status["thread_alive"] is True
        assert status["last_result"]["reason"] == "background_scheduler_run_loop_exception"
        assert status["last_result"]["has_blocking_result"] is True
        assert status["last_blocking_result"] == status["last_result"]
        health = status["scheduler_loop_health"]
        assert health["status"] == "BLOCKED"
        assert health["reason_code"] == "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_EXCEPTION"
        assert health["consecutive_failure_count"] == 1
        assert health["total_failure_count"] == 1
        assert health["execution_gate"] is False
        failure = health["active_failure"]
        assert failure["exception_type"] == "DataUnavailableError"
        assert len(failure["exception_message"]) == 2048
        assert failure["exception_message_truncated"] is True
        assert failure["underlying_reason_code"] == "SIMULATION_TEST_DEPENDENCY_UNAVAILABLE"
        assert failure["underlying_stage"] == "TEST_DEPENDENCY_READ"
        assert len(failure["context"]["binding_id"]) == 512
        assert "credential" not in failure["context"]
    finally:
        background.shutdown(wait=True)


def test_background_scheduler_success_clears_active_loop_failure_and_preserves_history() -> None:
    lifecycle = SimulationLifecycleScheduler(repository=InMemorySimulationRuntimeRepository())
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )
    first = background._record_loop_exception(RuntimeError("first loop failure"))
    second = background._record_loop_exception(RuntimeError("second loop failure"))

    blocked = background.status()["scheduler_loop_health"]
    assert blocked["status"] == "BLOCKED"
    assert blocked["consecutive_failure_count"] == 2
    assert blocked["total_failure_count"] == 2
    assert second["first_failure_at"] == first["first_failure_at"]
    assert blocked["last_failure"]["exception_message"] == "second loop failure"

    background._record_result(
        started_at=datetime(2026, 5, 21, 10, 0, tzinfo=UTC),
        result={
            "trade_date": TRADE_DATE.isoformat(),
            "reason": "outside_configured_windows",
            "processed": [],
            "errors": [],
            "alerts": [],
        },
    )
    background._record_loop_success()

    recovered = background.status()
    health = recovered["scheduler_loop_health"]
    assert health["status"] == "HEALTHY"
    assert health["reason_code"] == "SIMULATION_BACKGROUND_SCHEDULER_RUN_LOOP_OK"
    assert health["active_failure"] is None
    assert health["last_failure"]["exception_message"] == "second loop failure"
    assert health["consecutive_failure_count"] == 0
    assert health["total_failure_count"] == 2
    assert health["total_success_count"] == 1
    assert health["last_successful_tick_at"] is not None
    assert recovered["last_result"]["reason"] == "outside_configured_windows"


def test_background_scheduler_exposes_retired_package_skip_without_selection_or_submit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, local_binding, qmt_binding, repo = _release_and_bindings()
    assert local_binding is not None
    selection = FakeSelectionService(
        release,
        candidates=_candidate_rows(),
        package_status=PackageStatus.RETIRED,
    )
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(),
    )
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 1, 22, tzinfo=UTC))

    assert result["should_run"] is True
    assert result["summary"]["retired_package_skipped_count"] == 2
    assert result["summary"]["planned_count"] == 0
    assert result["summary"]["submitted_count"] == 0
    assert result["errors"] == []
    assert {item["status"] for item in result["processed"]} == {"SKIPPED_RETIRED_PACKAGE"}
    assert all(
        item["lifecycle_diagnostic"]["reason_code"] == "SIMULATION_BINDING_PACKAGE_RETIRED"
        for item in result["processed"]
    )
    assert all(item["lifecycle_diagnostic"]["broker_called"] is False for item in result["processed"])
    assert selection.calls == []
    assert repo.list_simulation_daily_runs(limit=10) == []


def test_background_scheduler_stop_shuts_down_selection_inference_executor() -> None:
    release_selection_config = {
        "selection_artifact_config": {
            "auto_generate": True,
            "include_reference_price": True,
        },
        "runtime_profile": {
            "selection": {"top_k": 2},
            "tradability": {"exclude_suspended": False},
        },
    }
    release, local_binding, _, repo = _release_and_bindings(
        release_metadata={"selection_runtime_config": release_selection_config}
    )
    assert local_binding is not None
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id="portfolio_stopped_executor")}
        ),
    )
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    stopped = background.shutdown(wait=True)

    assert stopped["selection_inference"]["shutdown"] is True
    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        lifecycle.run_once(
            trade_date=TRADE_DATE,
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=False,
            raise_on_error=True,
        )
    assert exc_info.value.context["reason_code"] == "SIMULATION_SELECTION_INFERENCE_EXECUTOR_SHUTDOWN"
    assert exc_info.value.context["failure_stage"] == "SELECTION_INFERENCE"
    assert selection.calls == []


def test_background_scheduler_result_surfaces_miniqmt_capacity_residual_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ResidualLifecycleScheduler:
        def status(self) -> dict[str, Any]:
            return {"ok": True, "scheduler": "residual_lifecycle_scheduler"}

        def run_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            run = SimulationDailyRun(
                run_id="simrun_background_capacity_residual",
                trade_date=TRADE_DATE,
                strategy_id="strategy_background_qmt",
                broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
                package_id="pkg_background",
                manifest_sha256="manifest_background",
                release_id="srr_background",
                release_hash="release_hash_background",
                binding_id="simbind_background",
                binding_hash="binding_hash_background",
                account_group_id="ag_background",
                strategy_slot_id="slot_background",
                status=SimulationDailyRunStatus.SUCCEEDED,
                run_payload_json={
                    "last_stage": "SUCCEEDED",
                    "broker_called": True,
                    "failed_intents": 3,
                    "qmt_batch_id": "batch_background_capacity",
                    "qmt_batch_status": "PARTIAL",
                    "qmt_batch_result": {
                        "batch_status": "PARTIAL",
                        "failed": 3,
                        "results": [
                            {
                                "success": False,
                                "broker_called": False,
                                "preflight": {
                                    "primary_error_code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                    "primary_error": {"message": "capacity residual"},
                                    "errors": [
                                        {
                                            "code": "SKIPPED_INSUFFICIENT_CAPITAL",
                                            "message": "capacity residual",
                                            "context": {},
                                        }
                                    ],
                                },
                            }
                        ],
                    },
                    "reconcile_after_submit": {
                        "submit_result_gate": {
                            "schema_version": "miniqmt_reconcile_submit_result_gate_v2",
                            "status": "SUCCEEDED",
                            "reason": "miniqmt_capacity_residual_skipped_and_reconciled",
                            "terminal_capacity_residual": True,
                        }
                    },
                },
            )
            return SimulationSchedulerRunOnceResult(
                trade_date=TRADE_DATE,
                data_source="DB_HISTORICAL",
                submit=True,
                total_bindings=1,
                results=(
                    SimulationSchedulerBindingResult(
                        binding_id=run.binding_id,
                        strategy_id=run.strategy_id,
                        broker_backend=run.broker_backend,
                        status="RECONCILED",
                        run=run,
                        data_source="MINIQMT_REALTIME",
                    ),
                ),
                as_of_time=kwargs.get("as_of_time"),
            )

        def post_close_reconcile_once(self, **kwargs: Any) -> SimulationSchedulerRunOnceResult:
            raise AssertionError("execution window should call run_once")

    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=ResidualLifecycleScheduler(),  # type: ignore[arg-type]
        trading_calendar_service=StaticTradingCalendarProvider([TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 2, 0, tzinfo=UTC))

    assert result["submit"] is True
    assert result["summary"]["succeeded_with_capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_count"] == 1
    assert result["summary"]["capacity_residual_failed_intents"] == 3
    assert result["processed"][0]["succeeded_with_capacity_residual"] is True
    assert result["processed"][0]["capacity_residual_failed_intents"] == 3
    assert result["alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"


def test_background_scheduler_skips_non_trading_day_before_lifecycle_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SpyLifecycleScheduler:
        def __init__(self) -> None:
            self.run_once_calls: list[dict[str, Any]] = []
            self.post_close_calls: list[dict[str, Any]] = []

        def status(self) -> dict[str, Any]:
            return {"ok": True, "scheduler": "spy_lifecycle_scheduler"}

        def run_once(self, **kwargs):
            self.run_once_calls.append(kwargs)
            raise AssertionError("non-trading day must not call lifecycle run_once")

        def post_close_reconcile_once(self, **kwargs):
            self.post_close_calls.append(kwargs)
            raise AssertionError("non-trading day must not call post-close reconcile")

    class StatusCalendar:
        def __init__(self) -> None:
            self.calls: list[date | None] = []

        def status(self, *, as_of_date: date | None = None) -> dict[str, Any]:
            self.calls.append(as_of_date)
            return {
                "ok": True,
                "as_of_date": "2026-06-19",
                "is_trading_day": False,
                "next_trading_day": "2026-06-22",
            }

    next_trading_day = date(2026, 6, 22)
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "true")
    lifecycle = SpyLifecycleScheduler()
    calendar = StatusCalendar()
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,  # type: ignore[arg-type]
        trading_calendar_service=calendar,
    )

    result = background.run_once(as_of_time=datetime(2026, 6, 19, 1, 30, tzinfo=UTC))

    assert result["window"]["window_id"] == "execution"
    assert result["should_run"] is False
    assert result["submit"] is False
    assert result["reason"] == "non_trading_day"
    assert result["skip_reason"] == "non_trading_day"
    assert result["next_trading_day"] == next_trading_day.isoformat()
    assert result["trading_calendar"]["is_trading_day"] is False
    assert calendar.calls == [date(2026, 6, 19)]
    assert result["processed"] == []
    assert result["errors"] == []
    assert lifecycle.run_once_calls == []
    assert lifecycle.post_close_calls == []
    assert background.status()["last_result"]["reason"] == "non_trading_day"


def test_quote_context_health_does_not_change_pending_run_or_non_trading_day_status() -> None:
    class ReadOnlyQuoteContext:
        def __init__(self) -> None:
            self.refresh_calls = 0

        def health(self) -> dict[str, object]:
            return {"status": "INVALID", "reason_code": "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID", "stage": "CLOCK"}

        def refresh_lifecycle(self, **_kwargs: object) -> None:
            self.refresh_calls += 1
            raise RuntimeError("authority unavailable")

    adapter = ReadOnlyQuoteContext()
    scheduler = SimulationLifecycleScheduler(
        repository=InMemorySimulationRuntimeRepository(),
        miniqmt_quote_context_adapter=adapter,
    )

    before = scheduler.status()
    scheduler._refresh_miniqmt_quote_context_lifecycle()
    after = scheduler.status()

    assert before["miniqmt_quote_context"]["status"] == "INVALID"
    assert after["miniqmt_quote_context"] == before["miniqmt_quote_context"]
    assert adapter.refresh_calls == 1
    # No run is created or transitioned merely because quote context health is invalid.
    assert scheduler.repository.list_simulation_daily_runs(limit=10) == []


def test_lifecycle_scheduler_blocks_non_trading_day_before_roll_forward_or_selection() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None

    class StatusCalendar:
        def __init__(self) -> None:
            self.calls: list[date | None] = []

        def status(self, *, as_of_date: date | None = None) -> dict[str, Any]:
            self.calls.append(as_of_date)
            return {
                "ok": True,
                "as_of_date": "2026-06-20",
                "is_trading_day": False,
                "next_trading_day": "2026-06-22",
            }

    class ExplodingContextProvider:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def load_context(self, **kwargs):
            self.calls.append(kwargs)
            raise AssertionError("non-trading day gate must run before LocalSim context loading")

    context_provider = ExplodingContextProvider()
    calendar = StatusCalendar()
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=context_provider,  # type: ignore[arg-type]
        trading_calendar_service=calendar,
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler.run_once(
            trade_date=date(2026, 6, 20),
            data_source="DB_HISTORICAL",
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            submit=True,
        )

    assert exc_info.value.context["reason_code"] == "SIMULATION_LIFECYCLE_NON_TRADING_DAY"
    assert exc_info.value.context["next_trading_day"] == "2026-06-22"
    assert calendar.calls == [date(2026, 6, 20)]
    assert context_provider.calls == []
    assert repo.list_simulation_daily_runs(trade_date=date(2026, 6, 20), limit=10) == []


def test_background_scheduler_fails_closed_when_trading_calendar_is_unavailable() -> None:
    class MissingCalendar:
        def is_trading_day(self, trade_date: date) -> bool:
            raise DataUnavailableError(
                "calendar missing",
                context={"trade_date": trade_date.isoformat()},
            )

    class SpyLifecycleScheduler:
        def __init__(self) -> None:
            self.run_once_calls: list[dict[str, Any]] = []

        def run_once(self, **kwargs):
            self.run_once_calls.append(kwargs)
            raise AssertionError("calendar failure must not call lifecycle run_once")

    lifecycle = SpyLifecycleScheduler()
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,  # type: ignore[arg-type]
        trading_calendar_service=MissingCalendar(),
    )

    result = background.run_once(as_of_time=datetime(2026, 6, 19, 1, 30, tzinfo=UTC))

    assert result["should_run"] is False
    assert result["submit"] is False
    assert result["reason"] == "trading_calendar_unavailable"
    assert result["processed"] == []
    assert result["errors"][0]["type"] == "DataUnavailableError"
    assert result["errors"][0]["context"] == {"trade_date": "2026-06-19"}
    assert lifecycle.run_once_calls == []


def _legacy_background_scheduler_runs_post_close_reconcile_without_submit_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("1"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_scheduler_qmt_background_post_close",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_scheduler_qmt_background_post_close",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker()
    context = SimulationRunContext(
        portfolio_id="portfolio_qmt",
        current_positions=_position_context(portfolio_id="portfolio_qmt").current_positions,
        current_prices={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        managed_order_service=QmtManagedOrderService(
            repository=qmt_repo,
            broker=broker,  # type: ignore[arg-type]
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_ledger_repository=qmt_repo,
        qmt_sync_service=QmtStrategyLedgerSyncService(
            repository=qmt_repo,
            qmt_client=FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service=QmtStrategyLedgerReconciliationService(repository=qmt_repo),
        broker_positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
    )
    lifecycle = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={qmt_binding.binding_id: context}),
    )
    submitted = lifecycle.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    repo.update_simulation_daily_run(run.run_id, status=SimulationDailyRunStatus.INTRADAY_RUNNING)
    monkeypatch.setenv("SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT", "false")
    background = SimulationLifecycleBackgroundScheduler(
        lifecycle_scheduler=lifecycle,
        trading_calendar_service=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
    )

    result = background.run_once(as_of_time=datetime(2026, 5, 21, 7, 5, tzinfo=UTC))
    latest = repo.get_simulation_daily_run(run.run_id)

    assert result["should_run"] is True
    assert result["submit"] is False
    assert result["window"]["window_id"] == "post_close_reconcile"
    assert result["trading_calendar"]["is_trading_day"] is True
    assert result["processed"] == []
    assert result["summary"]["stale_terminalized_count"] == 1
    assert result["terminalized_runs"][0]["run_id"] == run.run_id
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert len(broker.place_order_payloads) == 1


def test_scheduler_no_rebalance_submission_marks_success_without_broker() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_empty",
                    current_positions={},
                )
            }
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert result.submitted_count == 1
    assert result.results[0].status == "NO_REBALANCE"
    assert result.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert result.results[0].run.run_payload_json["broker_called"] is False


def test_scheduler_preserves_localsim_intent_when_transient_quote_is_blocked() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    blocked_position = {
        "688689.SH": PositionLot(
            portfolio_id="portfolio_blocked",
            symbol="688689.SH",
            quantity=878,
            available_quantity=878,
            avg_cost=46.82,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    context = SimulationRunContext(
        portfolio_id="portfolio_blocked",
        current_positions=blocked_position,
        current_prices={"688689.SH": 46.82},
        local_broker=FakeLocalSimBroker(),
        pre_trade_tradability={
            "688689.SH": {
                "schema_version": "pre_trade_tradability_status_v1",
                "symbol": "688689.SH",
                "trade_date": TRADE_DATE.isoformat(),
                "is_tradable": False,
                "reason_code": "NO_TRADABLE_REALTIME_QUOTE",
                "source": "TDX_REALTIME.batch_quote",
                "quote_evidence": {
                    "open": 0,
                    "high": 0,
                    "low": 0,
                    "total_hand": 0,
                    "bid_price_1": 0,
                    "ask_price_1": 0,
                    "no_tradable_market": True,
                },
            }
        },
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert result.results[0].status == "PLANNED"
    assert latest_run.status == SimulationDailyRunStatus.PLANNING_EXECUTION
    assert latest_run.run_payload_json.get("broker_called", False) is False
    assert latest_run.run_payload_json.get("no_rebalance_required", False) is False
    assert len(result.results[0].execution_plan.intents) == 1
    assert result.results[0].execution_plan.intents[0].symbol == "688689.SH"
    assert result.results[0].execution_plan.intents[0].side == OrderSide.SELL
    assert "pre_trade_blocked_order_generation" not in latest_run.run_payload_json
    assert context.local_broker.submitted == []


def test_scheduler_terminalizes_deterministic_localsim_board_lot_rejection_without_replay() -> None:
    release, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    broker = BoardLotRejectingLocalSimBroker()
    position = PositionLot(
        portfolio_id="portfolio_board_lot_terminal",
        symbol="688720.SH",
        quantity=1547,
        available_quantity=1547,
        avg_cost=10.0,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    context = SimulationRunContext(
        portfolio_id="portfolio_board_lot_terminal",
        current_positions={"688720.SH": position},
        current_prices={"688720.SH": 10.0},
        local_broker=broker,
        cash=100_000.0,
        market_data_source=MinuteDataSource.DB_HISTORICAL.value,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=[], valid_no_candidate=True),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["submitted_intents"] == 0
    assert latest_run.run_payload_json["failed_intents"] == 1
    diagnostics = latest_run.run_payload_json["local_sim_deterministic_submit_failure"]
    assert diagnostics["reason_code"] == "LOCAL_SIM_BOARD_LOT_VIOLATION"
    assert diagnostics["stage"] == "LOCAL_SIM_SUBMIT_FAILED"
    assert len(broker.submitted) == 1

    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert second.results[0].status == "REUSED_EXISTING_PLAN"
    assert len(broker.submitted) == 1


def test_scheduler_marks_existing_zero_intent_plan_success_when_submit_window_reuses_it() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    fake_selection = FakeSelectionService(release, candidates=[], valid_no_candidate=True)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=fake_selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                qmt_binding.binding_id: SimulationRunContext(
                    portfolio_id="portfolio_empty_qmt",
                    current_positions={},
                )
            }
        ),
    )

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    resumed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert planned.planned_count == 1
    assert planned.results[0].execution_plan.intents == []
    assert resumed.submitted_count == 1
    assert resumed.reused_count == 0
    assert resumed.results[0].status == "NO_REBALANCE"
    assert len(fake_selection.calls) == 1
    latest = repo.get_simulation_daily_run(planned.results[0].run.run_id)
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest.run_payload_json["no_rebalance_required"] is True
    assert latest.run_payload_json["broker_called"] is False
    assert latest.run_payload_json["last_stage"] == "SUCCEEDED"


def test_scheduler_runs_two_localsim_strategies_with_independent_state_and_restart_idempotency() -> None:
    release, local_binding_a, _, repo = _release_and_bindings()
    assert local_binding_a is not None
    local_binding_b = _create_extra_binding(
        release=release,
        repo=repo,
        strategy_id="strategy_local_scheduler_b",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )
    paper_repo = InMemoryPaperTradingV2Repository()
    selection = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection,
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={
                local_binding_a.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_a",
                    release=release,
                    paper_repository=paper_repo,
                ),
                local_binding_b.binding_id: _local_sim_context_with_real_broker(
                    portfolio_id="portfolio_local_b",
                    release=release,
                    paper_repository=paper_repo,
                    positions={
                        "000001.SZ": PositionLot(
                            portfolio_id="portfolio_local_b",
                            symbol="000001.SZ",
                            quantity=400,
                            available_quantity=400,
                            avg_cost=9.8,
                            trade_date=date(2026, 5, 20),
                        ),
                        "000004.SZ": PositionLot(
                            portfolio_id="portfolio_local_b",
                            symbol="000004.SZ",
                            quantity=200,
                            available_quantity=200,
                            avg_cost=6.0,
                            trade_date=date(2026, 5, 20),
                        ),
                    },
                ),
            }
        ),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )
    submitted_by_strategy = {item.strategy_id: item for item in submitted.results}
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
    )

    assert submitted.total_bindings == 2
    assert submitted.failed_count == 0
    assert set(submitted_by_strategy) == {local_binding_a.strategy_id, local_binding_b.strategy_id}
    assert len({item.execution_plan.plan_id for item in submitted_by_strategy.values()}) == 2
    assert (
        submitted_by_strategy[local_binding_a.strategy_id].run.run_payload_json["strategy_performance"][
            "initial_capital"
        ]
        == 100000.0
    )
    assert (
        submitted_by_strategy[local_binding_b.strategy_id].run.run_payload_json["strategy_performance"]["positions"][0][
            "symbol"
        ]
        == "000001.SZ"
    )
    for item in submitted_by_strategy.values():
        run_id = item.run.run_id
        assert paper_repo.list_orders_for_run(run_id)
        assert paper_repo.list_fills_for_run(run_id)
        assert paper_repo.cash_entries[run_id]
    assert len(selection.calls) == 2
    assert restarted.reused_count == 2


def test_local_sim_economic_models_reject_identity_and_hash_tampering() -> None:
    mark = LocalSimMarketMarkV1(
        symbol="000001.SZ",
        price=10.5,
        as_of_time=datetime(2026, 5, 21, 9, 31),
        source="TDX_REALTIME",
        provenance="REALTIME_MINUTE_CLOSE",
    )
    with pytest.raises(ValueError, match="symbol and source"):
        LocalSimMarketMarkV1(
            symbol=" ",
            price=10.5,
            as_of_time=datetime(2026, 5, 21, 9, 31),
            source="TDX_REALTIME",
            provenance="REALTIME_MINUTE_CLOSE",
        )
    with pytest.raises(ValueError, match="finite and positive"):
        LocalSimMarketMarkV1(
            symbol="000001.SZ",
            price=0,
            as_of_time=datetime(2026, 5, 21, 9, 31),
            source="TDX_REALTIME",
            provenance="REALTIME_MINUTE_CLOSE",
        )

    receipt = LocalSimEconomicReceiptV1(
        run_id="run_schema",
        binding_id="binding_schema",
        trade_date=TRADE_DATE,
        plan_id="plan_schema",
        generation=1,
        economic_facts={"fills": ["fill_1"]},
    )
    outbox = LocalSimProjectionOutboxV1(
        receipt_id=receipt.receipt_id,
        run_id=receipt.run_id,
        plan_id=receipt.plan_id,
        generation=receipt.generation,
        economic_hash=receipt.economic_hash,
        projection_payload={"positions": []},
    )
    projection = LocalSimProjectionReceiptV1(
        outbox_id=outbox.outbox_id,
        run_id=outbox.run_id,
        generation=outbox.generation,
        economic_hash=outbox.economic_hash,
        projection_payload_hash=outbox.projection_payload_hash,
        projection_hash=canonical_json_sha256({"status": "projected"}),
    )

    def assert_tamper_rejected(model: Any, field: str) -> None:
        payload = model.model_dump(mode="json")
        payload[field] = "0" * 64
        with pytest.raises(ValueError):
            type(model).model_validate(payload)

    assert_tamper_rejected(mark, "mark_hash")
    for field in ("economic_hash", "idempotency_key", "receipt_id", "receipt_hash"):
        assert_tamper_rejected(receipt, field)
    for field in ("projection_payload_hash", "outbox_id", "outbox_hash"):
        assert_tamper_rejected(outbox, field)
    for field in ("projection_receipt_id", "receipt_hash"):
        assert_tamper_rejected(projection, field)


def test_local_sim_economic_payload_helpers_fail_loud_on_corruption() -> None:
    receipt = LocalSimEconomicReceiptV1(
        run_id="run_payload",
        binding_id="binding_payload",
        trade_date=TRADE_DATE,
        plan_id="plan_payload",
        generation=1,
        economic_facts={"fills": []},
    )
    outbox = LocalSimProjectionOutboxV1(
        receipt_id=receipt.receipt_id,
        run_id=receipt.run_id,
        plan_id=receipt.plan_id,
        generation=receipt.generation,
        economic_hash=receipt.economic_hash,
        projection_payload={"positions": []},
    )
    projection = LocalSimProjectionReceiptV1(
        outbox_id=outbox.outbox_id,
        run_id=outbox.run_id,
        generation=outbox.generation,
        economic_hash=outbox.economic_hash,
        projection_payload_hash=outbox.projection_payload_hash,
        projection_hash=canonical_json_sha256({"status": "projected"}),
    )

    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_economic_receipt_map({LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: [{}]})
    assert exc_info.value.context["reason_code"] == "LOCALSIM_ECONOMIC_RECEIPT_PAYLOAD_INVALID"
    invalid_receipt = receipt.model_dump(mode="json")
    invalid_receipt["receipt_hash"] = "0" * 64
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_economic_receipt_map(
            {LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: {receipt.receipt_id: invalid_receipt}}
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_ECONOMIC_RECEIPT_SCHEMA_INVALID"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_economic_receipt_map(
            {LOCAL_SIM_ECONOMIC_RECEIPTS_PAYLOAD_KEY: {"wrong_receipt": receipt.model_dump(mode="json")}}
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_ECONOMIC_RECEIPT_IDENTITY_CONFLICT"

    invalid_outbox = outbox.model_dump(mode="json")
    invalid_outbox["outbox_hash"] = "0" * 64
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_projection_outbox({LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY: invalid_outbox})
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_projection_receipt_map({LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY: [{}]})
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RECEIPT_PAYLOAD_INVALID"
    invalid_projection = projection.model_dump(mode="json")
    invalid_projection["receipt_hash"] = "0" * 64
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_projection_receipt_map(
            {LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY: {projection.projection_receipt_id: invalid_projection}}
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RECEIPT_SCHEMA_INVALID"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _local_sim_projection_receipt_map(
            {LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY: {"wrong_projection": projection.model_dump(mode="json")}}
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RECEIPT_IDENTITY_CONFLICT"


def test_local_sim_economic_merge_is_idempotent_and_cas_guarded() -> None:
    event = {
        "run_id": "run_merge",
        "binding_id": "binding_merge",
        "trade_date": TRADE_DATE,
        "plan_id": "plan_merge",
        "states": (),
        "expected_versions": {},
        "economic_facts": {"fills": ["fill_1"]},
        "projection_payload": {"positions": []},
    }
    payload, receipt, outbox, created = _merge_local_sim_economic_event(payload={}, **event)
    assert created is True
    replayed, replay_receipt, replay_outbox, replay_created = _merge_local_sim_economic_event(payload=payload, **event)
    assert replayed == payload
    assert replay_receipt == receipt
    assert replay_outbox == outbox
    assert replay_created is False

    missing_outbox = dict(payload)
    missing_outbox.pop(LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY)
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_economic_event(payload=missing_outbox, **event)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_MISSING"

    next_event = dict(event)
    next_event["economic_facts"] = {"fills": ["fill_2"]}
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_economic_event(payload=payload, **next_event)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_PENDING"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_economic_event(payload={LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: True}, **event)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_ECONOMIC_GENERATION_INVALID"

    retryable = _merge_local_sim_projection_retryable(
        run_id=event["run_id"],
        payload=payload,
        outbox_id=outbox.outbox_id,
        error={"reason_code": "TEST_RETRY"},
    )
    assert retryable[LOCAL_SIM_PROJECTION_OUTBOX_PAYLOAD_KEY]["status"] == "PROJECTION_RETRYABLE"
    projected, projection_receipt = _merge_local_sim_projection_success(
        run_id=event["run_id"],
        payload=retryable,
        outbox_id=outbox.outbox_id,
        generation=outbox.generation,
        projection_result={"status": "projected"},
    )
    projected_replay, replay_projection_receipt = _merge_local_sim_projection_success(
        run_id=event["run_id"],
        payload=projected,
        outbox_id=outbox.outbox_id,
        generation=outbox.generation,
        projection_result={"status": "projected"},
    )
    assert projected_replay == projected
    assert replay_projection_receipt == projection_receipt

    missing_projection_receipt = dict(projected)
    missing_projection_receipt.pop(LOCAL_SIM_PROJECTION_RECEIPTS_PAYLOAD_KEY)
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_projection_success(
            run_id=event["run_id"],
            payload=missing_projection_receipt,
            outbox_id=outbox.outbox_id,
            generation=outbox.generation,
            projection_result={"status": "projected"},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RECEIPT_MISSING"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_projection_success(
            run_id=event["run_id"],
            payload=payload,
            outbox_id="wrong_outbox",
            generation=outbox.generation,
            projection_result={"status": "projected"},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT"
    with pytest.raises(InvalidStateTransitionError) as exc_info:
        _merge_local_sim_projection_retryable(
            run_id=event["run_id"],
            payload=projected,
            outbox_id=outbox.outbox_id,
            error={"reason_code": "TEST_RETRY"},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_CAS_CONFLICT"


class _AtomicSimulationCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        normalized = " ".join(sql.split())
        self.connection.executed.append((normalized, params))
        if normalized.startswith("UPDATE paper_v2.simulation_daily_run"):
            self.connection.status = params[0]
            self.connection.payload = deepcopy(params[1].adapted)

    def fetchone(self):
        return {"run_payload_json": deepcopy(self.connection.payload)}


class _AtomicSimulationConnection:
    def __init__(self):
        self.autocommit = True
        self.payload = {}
        self.status = None
        self.executed = []
        self.commits = 0
        self.rollbacks = 0

    def cursor(self, *args, **kwargs):
        return _AtomicSimulationCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _atomic_simulation_repository(connection):
    @contextmanager
    def factory():
        yield connection

    repository = SimulationRuntimeRepository(conn_factory=factory)

    def get_run(run_id: str) -> SimpleNamespace:
        payload = deepcopy(connection.payload)
        receipts = _local_sim_economic_receipt_map(payload)
        latest = max(receipts.values(), key=lambda item: item.generation) if receipts else None
        return SimpleNamespace(
            run_id=run_id,
            binding_id=latest.binding_id if latest else "binding_atomic_empty",
            trade_date=latest.trade_date if latest else TRADE_DATE,
            execution_plan_id=latest.plan_id if latest else None,
            run_payload_json=payload,
            status=connection.status,
        )

    repository.get_simulation_daily_run = get_run  # type: ignore[method-assign]
    return repository


def test_postgres_simulation_repository_stages_economic_and_projection_receipts_on_owner_connection() -> None:
    connection = _AtomicSimulationConnection()
    repository = _atomic_simulation_repository(connection)
    economic_facts = {"schema_version": "test_economic_v1", "state_hashes": {}}
    receipt, outbox, created = repository.stage_local_sim_economic_commit(
        connection=connection,
        run_id="run_atomic",
        binding_id="binding_atomic",
        trade_date=TRADE_DATE,
        plan_id="plan_atomic",
        states=(),
        expected_versions={},
        economic_facts=economic_facts,
        projection_payload={"schema_version": "test_projection_payload_v1"},
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={"last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED"},
    )
    assert created is True
    repository.readback_local_sim_economic_commit(run_id="run_atomic", receipt=receipt, outbox=outbox)

    projection_receipt = repository.stage_local_sim_projection_commit(
        connection=connection,
        run_id="run_atomic",
        outbox_id=outbox.outbox_id,
        generation=outbox.generation,
        final_status=SimulationDailyRunStatus.SUCCEEDED,
        projection_result={"schema_version": "test_projection_result_v1"},
        payload_patch={"local_sim_projection_generation": {"generation": outbox.generation}},
    )
    repository.readback_local_sim_projection_commit(run_id="run_atomic", receipt=projection_receipt)
    assert connection.payload["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert (
        connection.payload["local_sim_projection_generation"]["projection_receipt_id"]
        == projection_receipt.projection_receipt_id
    )


def test_postgres_simulation_repository_persists_projection_retry_and_readback_recovery_cas() -> None:
    connection = _AtomicSimulationConnection()
    repository = _atomic_simulation_repository(connection)
    _, pending, _ = repository.stage_local_sim_economic_commit(
        connection=connection,
        run_id="run_retry",
        binding_id="binding_retry",
        trade_date=TRADE_DATE,
        plan_id="plan_retry",
        states=(),
        expected_versions={},
        economic_facts={"schema_version": "test_economic_v1", "state_hashes": {}},
        projection_payload={"schema_version": "test_projection_payload_v1"},
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={},
    )
    repository.mark_local_sim_projection_retryable(
        run_id="run_retry",
        outbox_id=pending.outbox_id,
        error={"reason_code": "TEST_RETRY"},
    )
    assert connection.payload["local_sim_projection_outbox_v1"]["status"] == "PROJECTION_RETRYABLE"
    projected = repository.stage_local_sim_projection_commit(
        connection=connection,
        run_id="run_retry",
        outbox_id=pending.outbox_id,
        generation=pending.generation,
        final_status=SimulationDailyRunStatus.SUCCEEDED,
        projection_result={"schema_version": "test_projection_result_v1"},
        payload_patch={"local_sim_projection_generation": {"generation": pending.generation}},
    )
    repository.mark_local_sim_projection_readback_retryable(
        run_id="run_retry",
        outbox_id=pending.outbox_id,
        error={"reason_code": "TEST_READBACK"},
    )
    assert "local_sim_projection_readback_failure" in connection.payload
    repository.clear_local_sim_projection_readback_failure(
        run_id="run_retry",
        outbox_id=pending.outbox_id,
        final_status=SimulationDailyRunStatus.SUCCEEDED,
    )
    assert "local_sim_projection_readback_failure" not in connection.payload
    repository.readback_local_sim_projection_commit(run_id="run_retry", receipt=projected)
    assert connection.commits == 3
    assert connection.rollbacks == 0

    terminal_connection = _AtomicSimulationConnection()
    terminal_repository = _atomic_simulation_repository(terminal_connection)
    _, terminal_outbox, _ = terminal_repository.stage_local_sim_economic_commit(
        connection=terminal_connection,
        run_id="run_terminal",
        binding_id="binding_terminal",
        trade_date=TRADE_DATE,
        plan_id="plan_terminal",
        states=(),
        expected_versions={},
        economic_facts={"schema_version": "test_economic_v1", "state_hashes": {}},
        projection_payload={"schema_version": "test_projection_payload_v1"},
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={},
    )
    terminal_repository.mark_local_sim_projection_terminal(
        run_id="run_terminal",
        outbox_id=terminal_outbox.outbox_id,
        error={"reason_code": "LOCALSIM_PROJECTION_NON_RETRYABLE"},
    )
    assert terminal_connection.status == SimulationDailyRunStatus.FAILED_TERMINAL.value
    assert terminal_connection.payload["local_sim_projection_terminal_failure"]["attempt_count"] == 1


def test_scheduler_localsim_mark_does_not_fall_back_to_plan_prices() -> None:
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="p",
            symbol="000001.SZ",
            quantity=100,
            available_quantity=100,
            avg_cost=9.5,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    context = _local_sim_context_with_real_broker(portfolio_id="p", release=release, positions=positions)
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._local_sim_position_marks(
            positions=positions,
            context=replace(
                context,
                current_prices={"000001.SZ": 99.0},
                price_by_symbol={"000001.SZ": 88.0},
                local_broker=SimpleNamespace(),
            ),
            execution=SimpleNamespace(
                run=planned.results[0].run,
                execution_plan=planned.results[0].execution_plan,
            ),
            snapshot_time=datetime(2026, 5, 21, 9, 31),
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_MARK_PROVIDER_UNAVAILABLE"


@pytest.mark.parametrize(
    ("reason_code", "expected"),
    [
        ("LOCALSIM_MARK_PRICE_MISSING", True),
        ("LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE", True),
        ("LOCALSIM_MARK_SCHEMA_INVALID", False),
        ("LOCALSIM_MARK_IDENTITY_CONFLICT", False),
        ("LOCALSIM_PREVIOUS_MARK_IDENTITY_CONFLICT", False),
        ("LOCALSIM_MARK_SOURCE_INVALID", False),
    ],
)
def test_scheduler_localsim_valuation_pending_accepts_only_transient_mark_availability_gaps(
    reason_code: str,
    expected: bool,
) -> None:
    error = DataUnavailableError(
        "test LocalSim mark failure classification",
        context={"reason_code": reason_code},
    )
    assert SimulationLifecycleScheduler._local_sim_mark_failure_allows_valuation_pending(error) is expected


def test_scheduler_previous_localsim_marks_distinguishes_missing_from_malformed_outbox() -> None:
    missing = SimpleNamespace(run_id="run_marks_missing", run_payload_json={})
    assert SimulationLifecycleScheduler._previous_local_sim_mark_records(missing) == {}

    malformed_payloads = [
        {"local_sim_projection_outbox_v1": "invalid"},
        {"local_sim_projection_outbox_v1": {"projection_payload": "invalid"}},
        {"local_sim_projection_outbox_v1": {"projection_payload": {"marks": "invalid"}}},
    ]
    expected_layers = [
        "local_sim_projection_outbox_v1",
        "projection_payload",
        "marks",
    ]
    for payload, expected_layer in zip(malformed_payloads, expected_layers, strict=True):
        run = SimpleNamespace(run_id=f"run_marks_{expected_layer}", run_payload_json=payload)
        with pytest.raises(DataUnavailableError) as exc_info:
            SimulationLifecycleScheduler._previous_local_sim_mark_records(run)
        assert exc_info.value.context["reason_code"] == "LOCALSIM_PREVIOUS_MARK_SCHEMA_INVALID"
        assert exc_info.value.context["layer"] == expected_layer


def test_localsim_broker_loads_realtime_and_suspended_marks_with_true_provenance() -> None:
    release, _, _, _ = _release_and_bindings(qmt_only=False)
    position = PositionLot(
        portfolio_id="p_marks",
        symbol="000001.SZ",
        quantity=100,
        available_quantity=100,
        avg_cost=9.5,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_marks",
        release=release,
        paper_repository=InMemoryPaperTradingV2Repository(),
        cash=100_000,
        positions={position.symbol: position},
    )
    broker = context.local_broker
    assert isinstance(broker, LocalSimBackend)
    as_of_time = datetime(2026, 5, 21, 9, 33)

    realtime = broker.load_authoritative_position_marks(
        symbols=(position.symbol,),
        trade_date=TRADE_DATE,
        as_of_time=as_of_time,
        pre_trade_tradability={},
    )[position.symbol]
    assert realtime.price == 10.1
    assert realtime.as_of_time == datetime(2026, 5, 21, 9, 33)
    assert realtime.source == MinuteDataSource.TDX_REALTIME.value
    assert realtime.provenance == LocalSimMarketMarkProvenance.REALTIME_MINUTE_CLOSE

    historical_context = _local_sim_context_with_real_broker(
        portfolio_id="p_marks",
        release=release,
        positions={position.symbol: position},
    )
    historical_broker = historical_context.local_broker
    assert isinstance(historical_broker, LocalSimBackend)
    historical = historical_broker.load_authoritative_position_marks(
        symbols=(position.symbol,),
        trade_date=TRADE_DATE,
        as_of_time=as_of_time,
        pre_trade_tradability={},
    )[position.symbol]
    assert historical.price == 10.1
    assert historical.as_of_time == datetime(2026, 5, 21, 9, 31)
    assert historical.source == MinuteDataSource.DB_HISTORICAL.value
    assert historical.provenance == LocalSimMarketMarkProvenance.HISTORICAL_MINUTE_CLOSE

    with pytest.raises(DataUnavailableError) as exc_info:
        broker.load_authoritative_position_marks(
            symbols=(position.symbol,),
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 5, 22, 9, 31),
            pre_trade_tradability={},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_MARK_AS_OF_DATE_CONFLICT"
    with pytest.raises(DataUnavailableError) as exc_info:
        broker.load_authoritative_position_marks(
            symbols=(position.symbol,),
            trade_date=TRADE_DATE,
            as_of_time=datetime(2026, 5, 21, 9, 30),
            pre_trade_tradability={},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_MARK_PRICE_MISSING"

    suspended = broker.load_authoritative_position_marks(
        symbols=(position.symbol,),
        trade_date=TRADE_DATE,
        as_of_time=as_of_time,
        pre_trade_tradability={position.symbol: {"suspend_status": {"is_suspended": True}}},
    )[position.symbol]
    assert suspended.price == 10.0
    assert suspended.as_of_time == datetime(2026, 5, 20, 15, 0)
    assert suspended.source == "test.previous_close"
    assert suspended.provenance == LocalSimMarketMarkProvenance.SUSPENDED_PREV_CLOSE

    with pytest.raises(DataUnavailableError) as exc_info:
        broker.load_authoritative_position_marks(
            symbols=(position.symbol,),
            trade_date=TRADE_DATE,
            as_of_time=as_of_time,
            pre_trade_tradability={position.symbol: {"suspend_status": {"is_suspended": "true"}}},
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PRE_TRADE_SUSPEND_SCHEMA_INVALID"


def test_scheduler_localsim_economic_transaction_rolls_back_both_repositories() -> None:
    class FailingPaperRepository(InMemoryPaperTradingV2Repository):
        def save_fill(self, run_id, fill, **kwargs):
            raise RuntimeError("forced LocalSIM economic write failure")

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = FailingPaperRepository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_rollback", release=release, paper_repository=paper_repo, cash=100_000, positions={}
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    latest = repo.get_simulation_daily_run(failed.results[0].run.run_id)
    assert failed.results[0].status == "FAILED_RETRYABLE"
    assert paper_repo.runs == {} and paper_repo.orders == {} and paper_repo.fills == {}
    assert "local_sim_economic_receipts_v1" not in latest.run_payload_json
    assert "local_sim_projection_outbox_v1" not in latest.run_payload_json


def test_scheduler_localsim_projection_readback_failure_recovers_without_rewrite() -> None:
    class OneShotReadbackFailureRepository(InMemoryPaperTradingV2Repository):
        fail_readback = True

        def readback_local_sim_projection(self, **kwargs):
            if self.fail_readback:
                self.fail_readback = False
                raise InvalidStateTransitionError(
                    "forced projection readback failure", context={"reason_code": "LOCALSIM_PROJECTION_READBACK_FAILED"}
                )
            return super().readback_local_sim_projection(**kwargs)

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = OneShotReadbackFailureRepository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_readback", release=release, paper_repository=paper_repo, cash=100_000, positions={}
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    failed_run = repo.get_simulation_daily_run(run_id)
    event_count = len(paper_repo.run_events)
    assert failed.results[0].status == "FAILED_RETRYABLE"
    assert failed_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert failed_run.run_payload_json["local_sim_projection_readback_failure"]
    assert failed_run.run_payload_json["local_sim_projection_readback_failure"]["attempt_count"] == 1
    recovered_tick = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 33),
    )
    recovered = repo.get_simulation_daily_run(run_id)
    assert recovered_tick.results[0].status == "LOCALSIM_PROJECTION_RECOVERED"
    assert recovered.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert "local_sim_projection_readback_failure" not in recovered.run_payload_json
    assert "submit_failure" not in recovered.run_payload_json
    assert len(paper_repo.run_events) == event_count


def test_scheduler_recovers_failed_localsim_only_from_exact_durable_active_state() -> None:
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = InMemoryPaperTradingV2Repository()
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_durable_runtime_recovery",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: first_context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    assert submitted.results[0].status == "LOCALSIM_INTRADAY_RUNNING"
    order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    states = repo.list_local_sim_execution_states(run_id)
    assert states and any(not state.is_terminal for state in states)
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": len(states),
            "submit_failure": {
                "stage": "LOCAL_SIM_INTRADAY_ADVANCE_FAILED",
                "type": "DataUnavailableError",
                "message": "transient shared provider interruption after the prior durable generation",
                "context": {"reason_code": "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE"},
            },
        },
    )
    first_broker = first_context.local_broker
    assert first_broker is not None
    recovery_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_durable_runtime_recovery",
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={binding.binding_id: recovery_context}
    )

    recovered_tick = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 33),
    )

    recovered = repo.get_simulation_daily_run(run_id)
    assert recovered_tick.results[0].status == "LOCALSIM_DURABLE_RUNTIME_RECOVERED"
    assert recovered.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert recovered.run_payload_json["local_sim_failed_run_recovery_v1"]["parent_resubmitted"] is False
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == order_ids

    # A PROJECTED flag alone is not enough: recovery must independently prove
    # the Paper v2 projection.  Missing orders remain loud and the failed run
    # must not be revived from only the runtime-side state hashes.
    paper_repo.orders[run_id] = paper_repo.orders[run_id][1:]
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "submit_failure": {
                "stage": "LOCAL_SIM_INTRADAY_ADVANCE_FAILED",
                "type": "DataUnavailableError",
                "message": "forced post-projection recovery readback check",
                "context": {"reason_code": "LOCALSIM_REALTIME_MARKET_DATA_UNAVAILABLE"},
            },
        },
        payload_unset=("local_sim_failed_run_recovery_v1",),
    )
    refused = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )
    refused_run = repo.get_simulation_daily_run(run_id)
    assert refused.results[0].status == "FAILED_TERMINAL"
    assert refused.results[0].error["context"]["reason_code"] == "LOCALSIM_ECONOMIC_FACT_READBACK_FAILED"
    assert refused.results[0].error["context"]["stage"] == "LOCAL_SIM_DURABLE_RUNTIME_RECOVERY"
    assert refused.results[0].error["context"]["parent_resubmitted"] is False
    assert refused_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert "local_sim_failed_run_recovery_v1" not in refused_run.run_payload_json
    assert "pre_run_failure" not in refused_run.run_payload_json


def test_scheduler_localsim_projection_business_conflict_is_terminal_not_retryable() -> None:
    projection_attempts: list[bool] = []

    class BusinessConflictPaperRepository(InMemoryPaperTradingV2Repository):
        def save_positions(self, **kwargs):
            projection_attempts.append(True)
            raise InvalidStateTransitionError(
                "forced business conflict",
                context={"reason_code": "TEST_BUSINESS_CONFLICT"},
            )

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = BusinessConflictPaperRepository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_projection_terminal",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    latest = repo.get_simulation_daily_run(run_id)
    terminal = latest.run_payload_json["local_sim_projection_terminal_failure"]
    assert latest.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert terminal["error"]["reason_code"] == "LOCALSIM_PROJECTION_NON_RETRYABLE"
    assert terminal["attempt_count"] == 1

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._replay_pending_local_sim_projection(
            run_id=run_id,
            paper_repository=paper_repo,
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_NON_RETRYABLE"
    assert len(projection_attempts) == 1


def test_scheduler_localsim_projection_connection_retry_is_bounded() -> None:
    projection_attempts: list[bool] = []

    class ConnectionFailurePaperRepository(InMemoryPaperTradingV2Repository):
        def save_positions(self, **kwargs):
            projection_attempts.append(True)
            raise psycopg2.OperationalError("forced connection interruption")

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = ConnectionFailurePaperRepository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_projection_bounded",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    first = repo.get_simulation_daily_run(run_id)
    assert first.run_payload_json["local_sim_projection_outbox_v1"]["attempt_count"] == 1
    assert "local_sim_projection_terminal_failure" not in first.run_payload_json

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._project_local_sim_outbox(run_id=run_id, paper_repository=paper_repo)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RETRYABLE"
    second = repo.get_simulation_daily_run(run_id)
    assert second.run_payload_json["local_sim_projection_outbox_v1"]["attempt_count"] == 2

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._project_local_sim_outbox(run_id=run_id, paper_repository=paper_repo)
    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_RETRY_EXHAUSTED"
    terminal = repo.get_simulation_daily_run(run_id)
    assert terminal.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert terminal.run_payload_json["local_sim_projection_outbox_v1"]["attempt_count"] == 3
    assert terminal.run_payload_json["local_sim_projection_terminal_failure"]["attempt_count"] == 3
    assert len(projection_attempts) == 3


def test_scheduler_localsim_projection_outbox_tamper_fails_loud() -> None:
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = InMemoryPaperTradingV2Repository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="p_outbox_tamper",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    latest = repo.get_simulation_daily_run(run_id)
    outbox = deepcopy(latest.run_payload_json["local_sim_projection_outbox_v1"])
    outbox["projection_payload"]["economic_hash"] = "0" * 64
    repo.update_simulation_daily_run(run_id, payload_patch={"local_sim_projection_outbox_v1": outbox})

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._project_local_sim_outbox(run_id=run_id, paper_repository=paper_repo)

    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID"


def test_scheduler_localsim_mark_gap_commits_economics_and_completes_same_generation_after_restart() -> None:
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    assert binding is not None
    portfolio_id = "portfolio_localsim_valuation_pending"
    held_position = PositionLot(
        portfolio_id=portfolio_id,
        symbol="000003.SZ",
        quantity=200,
        available_quantity=200,
        avg_cost=9.8,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    paper_repo = InMemoryPaperTradingV2Repository()
    missing_provider = ToggleMissingLocalSimMarkProvider(missing_symbol=held_position.symbol)
    initial_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={held_position.symbol: held_position},
        market_data_provider=missing_provider,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: initial_context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )

    run_id = planned.results[0].run.run_id
    pending = repo.get_simulation_daily_run(run_id)
    outbox = deepcopy(pending.run_payload_json["local_sim_projection_outbox_v1"])
    receipt_id = outbox["receipt_id"]
    economic_receipts = pending.run_payload_json["local_sim_economic_receipts_v1"]
    assert list(economic_receipts) == [receipt_id]
    fill_ids = {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)}
    state_facts = {
        state.state_id: (state.sequence, state.state_hash) for state in repo.list_local_sim_execution_states(run_id)
    }

    assert first.results[0].status == "LOCALSIM_INTRADAY_RUNNING", first.results[0].error
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert pending.run_payload_json["local_sim_economic_generation"] == 1
    assert outbox["status"] == "PENDING"
    assert outbox["projection_payload"]["schema_version"] == ("local_sim_valuation_pending_projection_payload_v1")
    assert outbox["projection_payload"]["positions"]
    assert pending.run_payload_json["local_sim_persistence"]["status"] == ("INTRADAY_VALUATION_PENDING")
    assert pending.run_payload_json["local_sim_persistence"]["nav"] is None
    assert pending.run_payload_json["local_sim_persistence"]["missing_mark_symbols"] == [held_position.symbol]
    assert fill_ids
    assert paper_repo.list_orders_for_run(run_id)
    assert paper_repo.cash_entries[run_id]
    assert paper_repo.positions == {}
    assert paper_repo.snapshots == {}

    with pytest.raises(DataUnavailableError) as conflict:
        scheduler._local_sim_existing_projection_result(
            run_id=run_id,
            observed_positions={held_position.symbol: held_position},
            observed_account=SimpleNamespace(cash=100_000.0),
        )
    assert conflict.value.context["reason_code"] == "LOCALSIM_DUPLICATE_ECONOMIC_STATE_CONFLICT"

    restart_provider = ToggleMissingLocalSimMarkProvider(missing_symbol=held_position.symbol)
    restart_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={held_position.symbol: held_position},
        market_data_provider=restart_provider,
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: restart_context})
    still_pending = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 33),
    )
    pending_replay = repo.get_simulation_daily_run(run_id)
    assert still_pending.results[0].status == "LOCALSIM_VALUATION_PENDING"
    assert pending_replay.run_payload_json["local_sim_economic_generation"] == 1
    assert pending_replay.run_payload_json["local_sim_projection_outbox_v1"]["outbox_id"] == outbox["outbox_id"]
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert {
        state.state_id: (state.sequence, state.state_hash) for state in repo.list_local_sim_execution_states(run_id)
    } == state_facts

    restart_provider.mark_available = True
    completed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )
    projected = repo.get_simulation_daily_run(run_id)
    projected_outbox = projected.run_payload_json["local_sim_projection_outbox_v1"]
    assert completed.results[0].status == "LOCALSIM_PROJECTION_RECOVERED"
    assert projected.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert projected.run_payload_json["local_sim_economic_generation"] == 1
    assert projected_outbox["status"] == "PROJECTED"
    assert projected_outbox["outbox_id"] == outbox["outbox_id"]
    assert projected_outbox["receipt_id"] == receipt_id
    assert projected.run_payload_json["local_sim_valuation_completion_v1"]["generation"] == 1
    assert projected.run_payload_json["local_sim_persistence"]["nav"] is not None
    assert "local_sim_valuation_pending_v1" not in projected.run_payload_json
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert len(paper_repo.positions[run_id]) == len(outbox["projection_payload"]["positions"])
    assert paper_repo.snapshots[run_id].snapshot_time.replace(tzinfo=None) == datetime(2026, 5, 21, 9, 34)
    completed_positions = {position.symbol: position for position in paper_repo.positions[run_id]}
    existing = scheduler._local_sim_existing_projection_result(
        run_id=run_id,
        observed_positions=completed_positions,
        observed_account=SimpleNamespace(cash=paper_repo.snapshots[run_id].cash),
    )
    assert existing.outbox_id == outbox["outbox_id"]
    assert existing.generation == 1
    assert existing.payload["nav"] == paper_repo.snapshots[run_id].nav


def test_scheduler_localsim_pending_projection_reproves_economic_readback_before_mark_retry() -> None:
    class FailFirstEconomicReadbackRepository(InMemoryPaperTradingV2Repository):
        def __init__(self) -> None:
            super().__init__()
            self.economic_readback_calls = 0

        def readback_local_sim_economic_facts(self, **kwargs):
            self.economic_readback_calls += 1
            if self.economic_readback_calls == 1:
                raise DataUnavailableError(
                    "forced LocalSim economic readback failure",
                    context={"reason_code": "LOCALSIM_ECONOMIC_FACT_READBACK_FAILED"},
                )
            return super().readback_local_sim_economic_facts(**kwargs)

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    assert binding is not None
    portfolio_id = "portfolio_localsim_pending_economic_readback"
    held_position = PositionLot(
        portfolio_id=portfolio_id,
        symbol="000003.SZ",
        quantity=200,
        available_quantity=200,
        avg_cost=9.8,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    paper_repo = FailFirstEconomicReadbackRepository()
    provider = ToggleMissingLocalSimMarkProvider(missing_symbol=held_position.symbol)
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={held_position.symbol: held_position},
        market_data_provider=provider,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    first_run = repo.get_simulation_daily_run(run_id)

    assert first.results[0].status == "FAILED_RETRYABLE"
    assert first_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PENDING"
    assert paper_repo.economic_readback_calls == 1

    recovered_readback = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 33),
    )
    pending = repo.get_simulation_daily_run(run_id)

    assert recovered_readback.results[0].status == "LOCALSIM_VALUATION_PENDING"
    assert pending.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert paper_repo.economic_readback_calls == 2
    assert pending.run_payload_json["local_sim_economic_generation"] == 1
    assert pending.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PENDING"
    assert "submit_failure" not in pending.run_payload_json
    assert "local_sim_failed_run_recovery_failure_v1" not in pending.run_payload_json
    assert paper_repo.positions == {}
    assert paper_repo.snapshots == {}

    provider.mark_available = True
    completed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )
    projected = repo.get_simulation_daily_run(run_id)

    assert completed.results[0].status == "LOCALSIM_PROJECTION_RECOVERED"
    assert paper_repo.economic_readback_calls == 3
    assert projected.run_payload_json["local_sim_economic_generation"] == 1
    assert projected.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert paper_repo.positions[run_id]
    assert paper_repo.snapshots[run_id]


def test_scheduler_localsim_economic_readback_connection_retry_is_bounded() -> None:
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    assert binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={binding.binding_id: _position_context(portfolio_id="strat1")}
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert plan is not None

    for expected_attempt, expected_status in (
        (1, SimulationDailyRunStatus.FAILED_RETRYABLE),
        (2, SimulationDailyRunStatus.FAILED_RETRYABLE),
        (3, SimulationDailyRunStatus.FAILED_TERMINAL),
    ):
        cause = psycopg2.OperationalError("forced economic readback connection interruption")
        exc = DataUnavailableError(
            "LocalSim valuation-pending economic facts failed independent readback",
            context={
                "reason_code": "LOCALSIM_ECONOMIC_READBACK_RETRYABLE",
                "economic_readback_failure": True,
            },
        )
        exc.__cause__ = cause
        result = scheduler._record_local_sim_durable_runtime_recovery_failure(
            binding=binding,
            run=run,
            plan=plan,
            data_source=MinuteDataSource.TDX_REALTIME.value,
            recovery_stage="LOCAL_SIM_PROJECTION_RECOVERY",
            exc=exc,
        )
        run = result.run
        failure = run.run_payload_json["local_sim_failed_run_recovery_failure_v1"]
        assert run.status == expected_status
        assert failure["context"]["attempt_count"] == expected_attempt
        assert failure["context"]["max_attempts"] == 3

    assert failure["reason_code"] == "LOCALSIM_ECONOMIC_READBACK_RETRY_EXHAUSTED"
    assert failure["context"]["retryable"] is False


def test_scheduler_localsim_valuation_projection_retry_preserves_one_economic_generation() -> None:
    fail_projection = [True]

    class OneShotValuationProjectionFailureRepository(InMemoryPaperTradingV2Repository):
        def save_positions(self, **kwargs):
            if fail_projection[0]:
                fail_projection[0] = False
                raise psycopg2.OperationalError("forced valuation projection connection interruption")
            return super().save_positions(**kwargs)

    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    assert binding is not None
    portfolio_id = "portfolio_localsim_valuation_projection_retry"
    held_position = PositionLot(
        portfolio_id=portfolio_id,
        symbol="000003.SZ",
        quantity=200,
        available_quantity=200,
        avg_cost=9.8,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    paper_repo = OneShotValuationProjectionFailureRepository()
    provider = ToggleMissingLocalSimMarkProvider(missing_symbol=held_position.symbol)
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={held_position.symbol: held_position},
        market_data_provider=provider,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    pending = repo.get_simulation_daily_run(run_id)
    outbox_id = pending.run_payload_json["local_sim_projection_outbox_v1"]["outbox_id"]
    fill_ids = {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)}

    provider.mark_available = True
    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 33),
    )
    retryable = repo.get_simulation_daily_run(run_id)
    assert failed.results[0].status == "FAILED_RETRYABLE"
    assert retryable.run_payload_json["local_sim_economic_generation"] == 1
    assert retryable.run_payload_json["local_sim_projection_outbox_v1"]["status"] == ("PROJECTION_RETRYABLE")
    assert retryable.run_payload_json["local_sim_projection_outbox_v1"]["outbox_id"] == outbox_id
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert paper_repo.positions == {}
    assert paper_repo.snapshots == {}

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )
    projected = repo.get_simulation_daily_run(run_id)
    assert recovered.results[0].status == "LOCALSIM_PROJECTION_RECOVERED", (
        recovered.results[0].error,
        projected.run_payload_json.get("local_sim_projection_outbox_v1", {}).get("last_error"),
        projected.run_payload_json.get("local_sim_projection_terminal_failure"),
    )
    assert projected.run_payload_json["local_sim_economic_generation"] == 1
    assert projected.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert projected.run_payload_json["local_sim_projection_outbox_v1"]["outbox_id"] == outbox_id
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids
    assert paper_repo.positions[run_id]
    assert paper_repo.snapshots[run_id]

    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "local_sim_projection_readback_failure": {
                "reason_code": "LOCALSIM_PROJECTION_READBACK_RETRYABLE",
                "outbox_id": outbox_id,
                "generation": 1,
                "attempt_count": 1,
            },
        },
    )
    readback_recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 35),
    )
    readback_run = repo.get_simulation_daily_run(run_id)
    assert readback_recovered.results[0].status == "LOCALSIM_PROJECTION_RECOVERED"
    assert readback_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert "local_sim_projection_readback_failure" not in readback_run.run_payload_json
    assert readback_run.run_payload_json["local_sim_economic_generation"] == 1
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == fill_ids


def test_scheduler_localsim_persists_first_causal_bar_wait_and_resumes_without_resubmit() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_first_causal_bar_wait"
    position = PositionLot(
        portfolio_id=portfolio_id,
        symbol="000003.SZ",
        quantity=600,
        available_quantity=600,
        avg_cost=10.0,
        trade_date=TRADE_DATE - timedelta(days=1),
    )
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={position.symbol: position},
    )
    selection_service = FakeSelectionService(release, candidates=_candidate_rows())
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=selection_service,
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: first_context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )

    waiting = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 30, 55),
    )

    run_id = planned.results[0].run.run_id
    waiting_run = repo.get_simulation_daily_run(run_id)
    waiting_states = repo.list_local_sim_execution_states(run_id)
    assert waiting.results[0].status == "LOCALSIM_INTRADAY_RUNNING", waiting.results[0].error
    assert waiting_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert waiting_run.run_payload_json["broker_called"] is True
    assert waiting_run.run_payload_json["local_sim_economic_generation"] == 1
    assert waiting_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert waiting_run.run_payload_json["local_sim_persistence"]["status"] == "INTRADAY_WAITING_FOR_CAUSAL_BAR"
    assert waiting_run.run_payload_json["local_sim_persistence"]["valuation_status"] == (
        "WAITING_FOR_FIRST_CAUSAL_MARK"
    )
    assert waiting_run.run_payload_json["local_sim_persistence"]["nav"] is None
    assert "strategy_performance" not in waiting_run.run_payload_json
    assert paper_repo.snapshots == {}
    assert waiting_states
    assert all(
        state.runtime_status == LocalSimExecutionRuntimeStatus.WAITING_FOR_CAUSAL_BAR
        and state.sequence == 0
        and state.last_processed_bar_time is None
        for state in waiting_states
    )
    waiting_execution = waiting.results[0].execution_result
    assert waiting_execution is not None
    waiting_snapshot = waiting_execution.broker_result.execution_snapshot
    tampered_payload = waiting_states[0].model_dump(mode="python")
    tampered_payload.update({"order_id": "order_cross_chain_tamper", "state_hash": ""})
    tampered_state = LocalSimExecutionStateV1.model_validate(tampered_payload)
    with pytest.raises(DataUnavailableError) as chain_exc:
        SimulationLifecycleScheduler._validate_local_sim_execution_states(
            binding=local_binding,
            run=waiting_run,
            execution=waiting_execution,
            orders=tuple(waiting_snapshot.orders),
            states=(tampered_state, *waiting_states[1:]),
        )
    assert chain_exc.value.context["reason_code"] == "LOCALSIM_DURABLE_ACTION_CHAIN_CONFLICT"
    first_order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    assert len(first_order_ids) == len(waiting_states)
    assert paper_repo.list_fills_for_run(run_id) == []
    with pytest.raises(DataUnavailableError) as account_drift_exc:
        scheduler._local_sim_existing_projection_result(
            run_id=run_id,
            observed_positions={position.symbol: position},
            observed_account=SimpleNamespace(cash=99_999.0),
        )
    assert account_drift_exc.value.context["reason_code"] == "LOCALSIM_DUPLICATE_ECONOMIC_STATE_CONFLICT"

    waiting_replay_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={position.symbol: position},
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: waiting_replay_context}
    )
    waiting_replay = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 30, 58),
    )
    assert waiting_replay.results[0].status == "LOCALSIM_INTRADAY_RUNNING"
    assert repo.get_simulation_daily_run(run_id).run_payload_json["local_sim_economic_generation"] == 1
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == first_order_ids

    resumed_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={position.symbol: position},
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: resumed_context}
    )
    resumed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 31, 5),
    )

    resumed_run = repo.get_simulation_daily_run(run_id)
    resumed_states = repo.list_local_sim_execution_states(run_id)
    assert resumed.results[0].status == "LOCALSIM_INTRADAY_RUNNING", resumed.results[0].error
    assert resumed_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert resumed_run.run_payload_json["local_sim_economic_generation"] == 2
    assert any(state.last_processed_bar_time is not None for state in resumed_states)
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == first_order_ids
    assert paper_repo.list_fills_for_run(run_id)


def test_scheduler_localsim_first_causal_bar_wait_projection_recovers_from_outbox_without_resubmit() -> None:
    fail_wait_projection = [True]

    class OneShotWaitProjectionFailureRepository(InMemoryPaperTradingV2Repository):
        def save_run_event(self, *, run_id, event_type, message, context=None):
            if event_type == "RUN_INTRADAY_WAITING_FOR_CAUSAL_BAR" and fail_wait_projection[0]:
                fail_wait_projection[0] = False
                raise psycopg2.OperationalError("forced first-bar wait projection interruption")
            return super().save_run_event(
                run_id=run_id,
                event_type=event_type,
                message=message,
                context=context,
            )

    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = OneShotWaitProjectionFailureRepository()
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="portfolio_localsim_wait_projection_retry",
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )

    failed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 30, 55),
    )

    run_id = planned.results[0].run.run_id
    failed_run = repo.get_simulation_daily_run(run_id)
    first_order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    assert failed.results[0].status == "FAILED_RETRYABLE"
    assert failed_run.run_payload_json["local_sim_economic_generation"] == 1
    assert failed_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTION_RETRYABLE", (
        failed_run.run_payload_json["local_sim_projection_outbox_v1"],
        failed_run.run_payload_json.get("submit_failure"),
    )
    assert first_order_ids

    recovered = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 30, 58),
    )

    recovered_run = repo.get_simulation_daily_run(run_id)
    assert recovered.results[0].status == "LOCALSIM_PROJECTION_RECOVERED", (
        recovered.results[0].error,
        recovered_run.run_payload_json.get("submit_failure"),
    )
    assert recovered_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert recovered_run.run_payload_json["local_sim_economic_generation"] == 1
    assert recovered_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == first_order_ids
    assert sum(event["event_type"] == "RUN_INTRADAY_WAITING_FOR_CAUSAL_BAR" for event in paper_repo.run_events) == 1


def test_scheduler_localsim_realtime_partial_run_resumes_until_all_intents_terminal() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_streaming_restart"
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id, release=release, paper_repository=paper_repo, cash=100_000, positions={}
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: first_context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    assert planned.planned_count == 1
    assert first.results[0].status == "LOCALSIM_INTRADAY_RUNNING", first.results[0].error
    assert first.results[0].run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert "local_sim_synchronous_terminal" not in first.results[0].run.run_payload_json
    assert first.results[0].run.run_payload_json["local_sim_durable_minute_loop"]["terminal"] is False
    run_id = first.results[0].run.run_id
    first_states = repo.list_local_sim_execution_states(run_id)
    assert first_states
    assert all(not state.is_terminal and state.remaining_quantity > 0 for state in first_states)
    first_payload = repo.get_simulation_daily_run(run_id).run_payload_json
    assert first_payload["local_sim_economic_generation"] == 1
    assert first_payload["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    first_fill_ids = {row["fill_id"] for row in paper_repo.list_fills_for_run(run_id)}
    assert first_fill_ids
    first_event_count = len(paper_repo.run_events)
    first_broker = first_context.local_broker
    assert first_broker is not None
    replay_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: replay_context}
    )
    replayed = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    replay_payload = repo.get_simulation_daily_run(run_id).run_payload_json
    assert replayed.results[0].status == "LOCALSIM_INTRADAY_RUNNING"
    assert replay_payload["local_sim_economic_generation"] == 1
    assert {row["fill_id"] for row in paper_repo.list_fills_for_run(run_id)} == first_fill_ids
    assert len(paper_repo.run_events) == first_event_count
    with pytest.raises(InvalidStateTransitionError) as cas_error:
        repo.commit_local_sim_execution_states(
            run_id=run_id,
            states=first_states,
            expected_versions={state.state_id: (state.sequence + 1, "wrong_hash") for state in first_states},
        )
    assert cas_error.value.context["reason_code"] == "LOCALSIM_DURABLE_STATE_CAS_CONFLICT"

    first_broker = replay_context.local_broker
    assert first_broker is not None
    second_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: second_context}
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )
    assert second.results[0].status == "LOCALSIM_INTRADAY_RUNNING"
    assert second.results[0].run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    second_states = repo.list_local_sim_execution_states(run_id)
    assert all(state.sequence == prior.sequence + 1 for state, prior in zip(second_states, first_states))
    assert all(state.filled_quantity > prior.filled_quantity for state, prior in zip(second_states, first_states))
    assert repo.get_simulation_daily_run(run_id).run_payload_json["local_sim_economic_generation"] == 2
    second_fill_ids = {row["fill_id"] for row in paper_repo.list_fills_for_run(run_id)}
    assert first_fill_ids < second_fill_ids

    second_broker = second_context.local_broker
    assert second_broker is not None
    third_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(second_broker.query_account().cash),
        positions=second_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: third_context}
    )
    third = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 36),
    )
    third_states = repo.list_local_sim_execution_states(run_id)
    assert all(state.runtime_status.value == "FILLED" and state.remaining_quantity == 0 for state in third_states)
    assert third.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert repo.get_simulation_daily_run(run_id).run_payload_json["local_sim_economic_generation"] == 3
    terminal_fill_ids = [row["fill_id"] for row in paper_repo.list_fills_for_run(run_id)]
    assert len(terminal_fill_ids) == len(set(terminal_fill_ids))


def test_scheduler_localsim_durable_active_run_continues_when_broker_called_is_false() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_broker_called_false"
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: first_context}),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = first.results[0].run.run_id
    first_states = tuple(repo.list_local_sim_execution_states(run_id))
    first_order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    assert first_states and all(not state.is_terminal for state in first_states)

    repo.update_simulation_daily_run(
        run_id,
        payload_patch={"broker_called": False},
    )
    first_broker = first_context.local_broker
    assert first_broker is not None
    restarted_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    restarted = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: restarted_context}
        ),
    )

    continued = restarted.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 34),
    )

    continued_run = repo.get_simulation_daily_run(run_id)
    continued_states = tuple(repo.list_local_sim_execution_states(run_id))
    assert continued.results[0].status == "LOCALSIM_INTRADAY_RUNNING"
    assert continued_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert continued_run.run_payload_json["local_sim_economic_generation"] == 2
    assert all(state.sequence == previous.sequence + 1 for state, previous in zip(continued_states, first_states))
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == first_order_ids

    repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_projection_outbox_v1": None, "broker_called": False},
    )
    continued_broker = restarted_context.local_broker
    assert continued_broker is not None
    invalid_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(continued_broker.query_account().cash),
        positions=continued_broker.query_positions(),
    )
    invalid_scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: invalid_context}),
    )
    invalid = invalid_scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 35),
    )
    assert invalid.results[0].error is not None
    assert invalid.results[0].error["context"]["reason_code"] == "LOCALSIM_ACTIVE_CONTINUATION_OUTBOX_INVALID"
    assert repo.get_simulation_daily_run(run_id).run_payload_json.get("broker_called") is False


def test_scheduler_localsim_post_close_drives_active_durable_states_before_run_terminalization() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_eod_durable_closure"
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: first_context}),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = first.results[0].run.run_id
    assert any(not state.is_terminal for state in repo.list_local_sim_execution_states(run_id))
    repo.update_simulation_daily_run(run_id, payload_patch={"broker_called": False})

    active_run = repo.get_simulation_daily_run(run_id)
    corrupt_persistence = deepcopy(active_run.run_payload_json["local_sim_persistence"])
    corrupt_persistence["terminal"] = True
    corrupt_run = repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_persistence": corrupt_persistence},
    )
    with pytest.raises(DataUnavailableError) as conflict:
        scheduler._post_close_terminalize_localsim_run(
            run=corrupt_run,
            as_of_time=datetime(2026, 5, 21, 15, 5),
        )
    assert conflict.value.context["reason_code"] == "LOCALSIM_POST_CLOSE_ACTIVE_STATE_CONFLICT"
    correct_persistence = deepcopy(corrupt_persistence)
    correct_persistence["terminal"] = False
    repo.update_simulation_daily_run(run_id, payload_patch={"local_sim_persistence": correct_persistence})

    first_broker = first_context.local_broker
    assert first_broker is not None
    eod_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    restarted = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: eod_context}),
    )

    post_close = restarted.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 15, 5),
    )

    final_run = repo.get_simulation_daily_run(run_id)
    final_states = tuple(repo.list_local_sim_execution_states(run_id))
    assert final_states and all(state.is_terminal for state in final_states)
    assert final_run.status in {
        SimulationDailyRunStatus.SUCCEEDED,
        SimulationDailyRunStatus.FAILED_TERMINAL,
    }
    assert final_run.run_payload_json["local_sim_persistence"]["terminal"] is True
    assert post_close.results[0].status != "POST_CLOSE_TERMINALIZED"
    assert not (
        final_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
        and any(not state.is_terminal for state in final_states)
    )

    nonterminal_persistence = deepcopy(final_run.run_payload_json["local_sim_persistence"])
    nonterminal_persistence["terminal"] = False
    inconsistent_terminal_run = repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_persistence": nonterminal_persistence},
    )
    with pytest.raises(DataUnavailableError) as persistence_conflict:
        restarted._post_close_terminalize_localsim_run(
            run=inconsistent_terminal_run,
            as_of_time=datetime(2026, 5, 21, 15, 6),
        )
    assert persistence_conflict.value.context["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTENCE_STATE_CONFLICT"

    malformed_persistence = deepcopy(nonterminal_persistence)
    malformed_persistence["terminal"] = "false"
    malformed_run = repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_persistence": malformed_persistence},
    )
    with pytest.raises(DataUnavailableError) as schema_conflict:
        restarted._post_close_terminalize_localsim_run(
            run=malformed_run,
            as_of_time=datetime(2026, 5, 21, 15, 7),
        )
    assert schema_conflict.value.context["reason_code"] == "LOCALSIM_POST_CLOSE_PERSISTENCE_SCHEMA_INVALID"


@pytest.mark.parametrize(
    "failed_status",
    [
        SimulationDailyRunStatus.FAILED_RETRYABLE,
        SimulationDailyRunStatus.FAILED_TERMINAL,
    ],
)
def test_scheduler_cross_day_recovers_historical_failed_localsim_active_generation(
    failed_status: SimulationDailyRunStatus,
) -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    portfolio_id = "portfolio_localsim_historical_failed_terminal_active"
    first_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=100_000,
        positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: first_context}),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = first.results[0].run.run_id
    initial_states = tuple(repo.list_local_sim_execution_states(run_id))
    initial_order_ids = {order.order_id for order in paper_repo.list_orders_for_run(run_id)}
    assert initial_states and any(not state.is_terminal for state in initial_states)
    valid_outbox = deepcopy(first.results[0].run.run_payload_json["local_sim_projection_outbox_v1"])

    repo.update_simulation_daily_run(
        run_id,
        status=failed_status,
        payload_patch={
            "last_stage": failed_status.value,
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": len(initial_states),
        },
    )
    first_broker = first_context.local_broker
    assert first_broker is not None
    recovery_context = _local_sim_realtime_context_with_real_broker(
        portfolio_id=portfolio_id,
        release=release,
        paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash),
        positions=first_broker.query_positions(),
    )
    restarted = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: recovery_context}),
    )

    repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_projection_outbox_v1": {"schema_version": "invalid"}},
    )
    invalid = restarted.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )
    invalid_result = next(item for item in invalid.stale_run_results if item.get("run_id") == run_id)
    assert invalid_result["status"] == "RECOVERY_FAILED"
    assert invalid_result["error"]["context"]["reason_code"] == ("LOCALSIM_HISTORICAL_RECOVERY_OUTBOX_SCHEMA_INVALID")
    assert tuple(repo.list_local_sim_execution_states(run_id)) == initial_states
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == initial_order_ids
    repo.update_simulation_daily_run(
        run_id,
        payload_patch={"local_sim_projection_outbox_v1": valid_outbox},
    )

    next_day = restarted.run_once(
        trade_date=TRADE_DATE + timedelta(days=1),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 22, 10, 0),
    )

    recovered = repo.get_simulation_daily_run(run_id)
    recovered_states = tuple(repo.list_local_sim_execution_states(run_id))
    assert recovered_states and all(state.is_terminal for state in recovered_states)
    assert recovered.status in {
        SimulationDailyRunStatus.SUCCEEDED,
        SimulationDailyRunStatus.FAILED_TERMINAL,
    }
    assert recovered.run_payload_json["local_sim_persistence"]["terminal"] is True
    evidence_suffix = "terminal" if failed_status == SimulationDailyRunStatus.FAILED_TERMINAL else "retryable"
    recovery = recovered.run_payload_json[f"localsim_historical_failed_{evidence_suffix}_active_recovery_v1"]
    assert recovery["previous_status"] == failed_status.value
    assert recovery["parent_resubmitted"] is False
    assert recovery["predecessor_projection_replayed"] is False
    assert recovery["durable_minute_loop_advanced"] is True
    assert recovery["predecessor_state_count"] == len(initial_states)
    assert recovery["terminal_state_count"] == len(recovered_states)
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == initial_order_ids
    result = next(item for item in next_day.stale_run_results if item.get("run_id") == run_id)
    assert result[f"historical_failed_{evidence_suffix}_active_recovery"] is True

    terminal_fill_ids = {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)}
    repo.update_simulation_daily_run(
        run_id,
        status=SimulationDailyRunStatus.FAILED_TERMINAL,
        payload_patch={"last_stage": SimulationDailyRunStatus.FAILED_TERMINAL.value},
    )
    repeated = restarted.run_once(
        trade_date=TRADE_DATE + timedelta(days=2),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 23, 10, 0),
    )
    assert not any(item.get("run_id") == run_id for item in repeated.stale_run_results)
    assert repo.get_simulation_daily_run(run_id).status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert {order.order_id for order in paper_repo.list_orders_for_run(run_id)} == initial_order_ids
    assert {fill["fill_id"] for fill in paper_repo.list_fills_for_run(run_id)} == terminal_fill_ids


def test_scheduler_localsim_waiting_for_capital_is_not_terminal_residual() -> None:
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    assert local_binding is not None
    paper_repo = InMemoryPaperTradingV2Repository()
    position = PositionLot(
        portfolio_id="portfolio_localsim_waiting_capital",
        symbol="000003.SZ",
        quantity=1000,
        available_quantity=0,
        avg_cost=10.0,
        trade_date=TRADE_DATE,
    )
    context = _local_sim_realtime_context_with_real_broker(
        portfolio_id="portfolio_localsim_waiting_capital",
        release=release,
        paper_repository=paper_repo,
        cash=0,
        positions={position.symbol: position},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={local_binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    latest = repo.get_simulation_daily_run(run_id)
    states = repo.list_local_sim_execution_states(run_id)

    assert submitted.results[0].status == "LOCALSIM_INTRADAY_RUNNING", submitted.results[0].error
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert any(state.runtime_status.value == "WAITING_FOR_CAPITAL" for state in states)
    assert "local_sim_capacity_residual_terminalization" not in latest.run_payload_json
    outbox = latest.run_payload_json["local_sim_projection_outbox_v1"]
    assert outbox["projection_payload"]["paper_error"] is None


def _legacy_scheduler_miniqmt_two_strategies_same_stock_keep_strategy_lots_and_merged_reconcile() -> None:
    scheduler, _repo, broker, _qmt_binding_a, _qmt_binding_b = _miniqmt_two_strategy_scheduler()

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.total_bindings == 2
    assert submitted.failed_count == 0
    for item in submitted.results:
        payload = item.run.run_payload_json["reconcile_after_submit"]
        assert payload["run"]["status"] == "SUCCEEDED"
        assert payload["broker_quantities"] == {"000003.SZ": 200}
        assert payload["overlap_symbols"] == ["000003.SZ"]
        assert payload["strategy_lot_quantities"]["SchedulerQMT"]["000003.SZ"] == 77
        assert payload["strategy_lot_quantities"]["SchedulerQMTB"]["000003.SZ"] == 123
    assert len(broker.place_order_payloads) == 6
    assert [payload["strategy_name"] for payload in broker.place_order_payloads].count("SchedulerQMT") == 3
    assert [payload["strategy_name"] for payload in broker.place_order_payloads].count("SchedulerQMTB") == 3


def test_production_context_provider_loads_positions():
    """Production provider returns context with positions from the loader."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider
    from backend.services.trading_core.models import PositionLot

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1",
            symbol="000001.XSHE",
            quantity=1000,
            available_quantity=1000,
            avg_cost=12.50,
            trade_date=date.today(),
        ),
    }

    manifest = _frozen_manifest(package_id="pkg", manifest_sha256="aa")
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="Production LocalSim",
        package_id="pkg",
        manifest_sha256="aa",
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=999_000)

    def _pos_loader(strategy_id, trade_date):
        return positions

    provider = ProductionSimulationRunContextProvider(
        position_loader=_pos_loader,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        paper_repository_factory=lambda: paper_repo,
        enable_localsim_broker=False,
    )
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.portfolio_id == "strat1"
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.cash == 999_000


def test_production_context_provider_fails_fast_on_position_failure():
    """Production provider must not turn a loader failure into empty positions."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    def failing_loader(strategy_id, trade_date):
        raise RuntimeError("db unreachable")

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="Production LocalSim",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    provider = ProductionSimulationRunContextProvider(
        position_loader=failing_loader,
        paper_repository_factory=lambda: paper_repo,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    with pytest.raises(DataUnavailableError, match="position_loader failed"):
        provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())


def test_production_context_provider_miniqmt_context():
    """Production provider wires MiniQMT services when backend is MINIQMT_SIM."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1",
            symbol="000001.XSHE",
            quantity=1000,
            available_quantity=1000,
            avg_cost=12.50,
            trade_date=date.today(),
        ),
    }
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="strat1",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=lambda symbols, trade_date: {symbol: 12.6 for symbol in symbols},
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
        package_manifest_loader=lambda package_id: manifest,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
    assert ctx.current_positions == positions
    assert ctx.current_prices == {"000001.XSHE": 12.6}
    assert ctx.manifest == manifest
    assert ctx.managed_order_service == "fake_mos"
    assert ctx.qmt_sync_service == "fake_sync"
    assert ctx.qmt_reconciliation_service == "fake_recon"
    assert ctx.qmt_ledger_repository is qmt_repo


def test_existing_miniqmt_plan_context_uses_broker_marks_without_database_market_reload() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    planning_scheduler, _planning_repo, _planning_broker, _planning_binding = _miniqmt_event_loop_test_scheduler()
    planned = planning_scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(9, 25), tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    plan = planned.results[0].execution_plan
    assert plan is not None

    positions = {
        "000001.XSHE": PositionLot(
            portfolio_id="strat1",
            symbol="000001.XSHE",
            quantity=1000,
            available_quantity=1000,
            avg_cost=12.50,
            trade_date=TRADE_DATE,
        )
    }
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="strat1",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    quote_broker = FakeManagedOrderBroker(
        positions=[],
        quotes={"000001.XSHE": {"lastPrice": 12.7, "time": "20260521100000"}},
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    price_loader_calls: list[tuple[list[str], date]] = []
    tradability = FakePreTradeTradabilityProvider()

    def forbidden_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
        price_loader_calls.append((list(symbols), trade_date))
        raise AssertionError("existing MiniQMT plan must not read database market prices")

    provider = ProductionSimulationRunContextProvider(
        position_loader=lambda strategy_id, trade_date: positions,
        price_loader=forbidden_price_loader,
        qmt_client_factory=lambda: quote_broker,
        managed_order_service_factory=lambda: "fake_mos",
        qmt_sync_service_factory=lambda: "fake_sync",
        qmt_reconciliation_service_factory=lambda: "fake_recon",
        qmt_ledger_repository=qmt_repo,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    context = provider.load_existing_plan_context(
        runtime_release=release,
        binding=binding,
        plan=plan,
        trade_date=TRADE_DATE,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert price_loader_calls == []
    assert tradability.calls == []
    assert context.current_prices == {"000001.XSHE": 12.7}
    assert context.price_by_symbol == {"000001.XSHE": 12.7}
    assert context.target_total_equity is None
    assert context.target_equity_context["planning_market_data_reloaded"] is False
    assert quote_broker.full_tick_calls == [["000001.XSHE"]]


def test_production_context_provider_loads_miniqmt_positions_from_virtual_ledger_without_submit_broker():
    """Default MiniQMT production context reads strategy lots and keeps submission disabled unless explicitly enabled."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            frozen_cash=Decimal("123"),
            realized_pnl=Decimal("45"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_context",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_prod_context",
            open_date=TRADE_DATE,
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    qmt_client = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 1000}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: qmt_client,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].quantity == 1000
    assert ctx.current_prices == {"000001.SZ": 10.5}
    assert ctx.manifest == manifest
    assert ctx.cash == 900000
    assert ctx.frozen_cash == 123
    assert ctx.realized_pnl == 45
    assert ctx.qmt_ledger_repository is qmt_repo
    assert ctx.qmt_sync_service is not None
    assert ctx.qmt_reconciliation_service is not None
    assert getattr(ctx.managed_order_service, "_broker") is qmt_client


def test_production_context_provider_uses_miniqmt_quote_for_pre_trade_gate_today() -> None:
    """MiniQMT same-day gate must use broker quotes and must not call the TDX provider."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_suspended_no_quote",
            strategy_id="strat1",
            symbol="688689.SH",
            open_trade_id="trade_suspended_no_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=878,
            available_quantity=878,
            remaining_quantity=878,
            avg_cost=Decimal("46.82"),
            cost_amount=Decimal("41111.96"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "688689.SH", "quantity": 878, "can_sell": 878}],
        quotes={
            "688689.SH": {
                "bidPrice": [0.0],
                "askPrice": [0.0],
                "bidVol": [0],
                "askVol": [0],
                "lastPrice": 46.82,
                "lastClose": 46.8,
                "openPrice": 0.0,
                "highPrice": 0.0,
                "lowPrice": 0.0,
                "volume": 0,
                "amount": 0,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 46.82 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    assert ctx.pre_trade_tradability["688689.SH"]["reason_code"] == "NO_TRADABLE_REALTIME_QUOTE"
    assert ctx.pre_trade_tradability["688689.SH"]["source"] == "MINIQMT_REALTIME.broker_quote"
    assert ctx.context_diagnostics["pre_trade_tradability"]["blocked_symbols"] == [
        {"symbol": "688689.SH", "reason_code": "NO_TRADABLE_REALTIME_QUOTE", "source": "MINIQMT_REALTIME.broker_quote"}
    ]
    assert tradability.calls == []
    assert broker.full_tick_calls == [["688689.SH"]]


def test_production_context_provider_miniqmt_quote_yuan_limits_do_not_fail_pre_run() -> None:
    """MiniQMT broker quotes are yuan-denominated and must not use TDX raw-li rounding."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_miniqmt_quote",
            strategy_id="strat1",
            symbol="603303.SH",
            open_trade_id="trade_miniqmt_quote",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("30.14"),
            cost_amount=Decimal("3014.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "603303.SH", "quantity": 100, "can_sell": 100}],
        quotes={
            "603303.SH": {
                "bidPrice": [30.23],
                "askPrice": [30.26],
                "bidVol": [108],
                "askVol": [17],
                "lastPrice": 30.23,
                "lastClose": 30.14,
                "openPrice": 30.27,
                "highPrice": 31.8,
                "lowPrice": 29.01,
                "volume": 66974,
                "amount": 203373536,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.23 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["603303.SH"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(33.15)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(27.13)
    assert tradability.calls == []
    assert broker.full_tick_calls == [["603303.SH"]]


def test_production_context_provider_miniqmt_degenerate_raw_limit_metadata_passes_pre_run() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_miniqmt_degenerate_limit_metadata",
            strategy_id="strat1",
            symbol="000048.SZ",
            open_trade_id="trade_miniqmt_degenerate_limit_metadata",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("20.75"),
            cost_amount=Decimal("2075.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000048.SZ", "quantity": 100, "can_sell": 100}],
        quotes={
            "000048.SZ": {
                "price_basis": "raw_li",
                "bidPrice": [20.65],
                "askPrice": [20.66],
                "bidVol": [100],
                "askVol": [100],
                "lastPrice": 20.66,
                "lastClose": 20.75,
                "openPrice": 20.7,
                "highPrice": 20.88,
                "lowPrice": 20.3,
                "limit_up": 20.0,
                "limit_down": 20.0,
                "volume": 12345,
                "amount": 25432100,
                "time": datetime.now().strftime("%Y%m%d%H%M%S"),
            }
        },
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 20.66 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000048.SZ"]
    assert status["is_tradable"] is True
    assert status["quote_evidence"]["quote_source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"]["quote_price_basis"] == "yuan"
    assert status["quote_evidence"]["limit_up"] == pytest.approx(22.83)
    assert status["quote_evidence"]["limit_down"] == pytest.approx(18.68)
    assert broker.full_tick_calls == [["000048.SZ"]]


def test_production_context_provider_miniqmt_quote_missing_is_visible_without_tdx_fallback() -> None:
    """MiniQMT quote outages must surface as MiniQMT evidence, not TDX timestamp failures."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_quote_missing",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_quote_missing",
            open_date=date.today() - timedelta(days=1),
            quantity=100,
            available_quantity=100,
            remaining_quantity=100,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("1000.00"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    tradability = FakePreTradeTradabilityProvider()
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000001.SZ", "quantity": 100, "can_sell": 100}],
        quotes={},
    )
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        pre_trade_tradability_provider=tradability,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())

    status = ctx.pre_trade_tradability["000001.SZ"]
    assert status["is_tradable"] is False
    assert status["reason_code"] == "REALTIME_QUOTE_MISSING"
    assert status["source"] == "MINIQMT_REALTIME.broker_quote"
    assert status["quote_evidence"] == {
        "quote_source": "MINIQMT_REALTIME.broker_quote",
        "quote_present": False,
    }
    assert tradability.calls == []
    assert broker.full_tick_calls == [["000001.SZ"]]


def test_production_context_provider_drops_miniqmt_stale_lots_missing_from_broker():
    """MiniQMT current_positions must not emit impossible sells for lots absent from broker can_sell."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_stale_not_in_broker",
            strategy_id="strat1",
            symbol="000636.SZ",
            open_trade_id="trade_stale_not_in_broker",
            open_date=date(2026, 5, 20),
            quantity=900,
            available_quantity=900,
            remaining_quantity=900,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("9000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    seen_price_symbols: list[tuple[str, ...]] = []

    def price_loader(symbols, trade_date):
        seen_price_symbols.append(tuple(symbols))
        return {symbol: 10.5 for symbol in symbols}

    broker = FakeManagedOrderBroker(positions=[])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=price_loader,
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions == {}
    assert ctx.broker_positions == []
    assert seen_price_symbols == [()]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["dropped_position_count"] == 1
    assert diagnostics["dropped_positions"][0]["symbol"] == "000636.SZ"


def test_production_context_provider_caps_miniqmt_lots_to_broker_quantity_and_can_sell():
    """Strategy-lot context is capped by broker-authoritative quantity/can_sell before planning."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_cap_to_broker",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_cap_to_broker",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    position = ctx.current_positions["000001.SZ"]
    assert position.quantity == 500
    assert position.available_quantity == 300
    assert ctx.broker_positions == [{"stock_code": "000001.SZ", "quantity": 500, "can_sell": 300}]
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["capped_positions"][0]["reconciled_quantity"] == 500
    assert diagnostics["capped_positions"][0]["reconciled_available_quantity"] == 300


def test_production_context_provider_projects_miniqmt_strategy_slot_from_account_broker_authority():
    """One slot's local lots cannot consume another slot's broker-backed attribution."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    for strategy_id, strategy_name, quantity in (
        ("strat1", "StrategyA", 7600),
        ("strat_b", "StrategyB", 6600),
    ):
        qmt_repo.create_virtual_account(
            VirtualAccount(
                strategy_id=strategy_id,
                strategy_name=strategy_name,
                display_name=strategy_name,
                account_id="QMT_SIM_ACCOUNT",
                mode="SIM",
                initial_cash=Decimal("1000000"),
                cash=Decimal("900000"),
                status=VirtualAccountStatus.ENABLED,
            )
        )
        qmt_repo.create_position_lot(
            PositionLotRecord(
                lot_id=f"lot_{strategy_id}",
                strategy_id=strategy_id,
                symbol="001358.SZ",
                open_trade_id=f"trade_{strategy_id}",
                open_date=date(2026, 5, 20),
                quantity=quantity,
                available_quantity=quantity,
                remaining_quantity=quantity,
                avg_cost=Decimal("29.88"),
                cost_amount=Decimal(quantity) * Decimal("29.88"),
                account_id="QMT_SIM_ACCOUNT",
            )
        )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "001358.SZ", "quantity": 13200, "can_sell": 13200}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 30.0 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["001358.SZ"].quantity == 7100
    diagnostics = ctx.context_diagnostics["miniqmt_broker_position_reconciliation"]
    assert diagnostics["position_authority"] == "broker_positions"
    assert diagnostics["account_strategy_count"] == 2
    assert diagnostics["capped_position_count"] == 1
    assert diagnostics["projection_adjustments"][0]["issue_type"] == "UNBACKED_STRATEGY_POSITION"
    assert diagnostics["projection_adjustments"][0]["projected_strategy_quantities"] == {
        "strat1": 7100,
        "strat_b": 6100,
    }


def test_production_context_provider_miniqmt_preview_checks_broker_can_sell_without_submit():
    """Preview-only MiniQMT path can read account sellable quantity but never places orders."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id="strat1",
            strategy_name="StrategyOne",
            display_name="Strategy One",
            account_id="QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("1000000"),
            cash=Decimal("900000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_preview_sell_check",
            strategy_id="strat1",
            symbol="000001.SZ",
            open_trade_id="trade_preview_sell_check",
            open_date=date(2026, 5, 20),
            quantity=1000,
            available_quantity=1000,
            remaining_quantity=1000,
            avg_cost=Decimal("10.00"),
            cost_amount=Decimal("10000"),
            account_id="QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000001.SZ", "quantity": 1000, "can_sell": 100}])
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.MINIQMT_SIM)
    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.manifest == manifest

    result = ctx.managed_order_service.submit_batch(
        [
            ManagedOrderRequest(
                account_id="QMT_SIM_ACCOUNT",
                strategy_name="StrategyOne",
                symbol="000001.SZ",
                side="SELL",
                order_type=24,
                quantity=200,
                price_type=5,
                price=Decimal("0"),
                order_remark="preview-sell-check",
                trade_date=TRADE_DATE,
                mode="SIM",
            )
        ]
    )

    assert result.success is False
    assert result.results[0].broker_called is False
    assert result.results[0].preflight.broker_can_sell == 100
    assert result.results[0].preflight.primary_error.code in {
        "INSUFFICIENT_BROKER_CAN_SELL",
        "BATCH_INSUFFICIENT_BROKER_CAN_SELL",
    }
    assert broker.place_order_payloads == []


def _legacy_production_context_provider_miniqmt_submit_disabled_fails_loud_without_preview_submit():
    """Production MiniQMT A route must fail loud when submit authority is disabled."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_preview_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_prod_preview_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    snapshot_client = FakeQmtSnapshotClient(positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}])
    manifest = _score_weighted_manifest(release)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {
            symbol: {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}[symbol] for symbol in symbols
        },
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        qmt_sync_service_factory=lambda: QmtStrategyLedgerSyncService(
            repository=qmt_repo,
            qmt_client=snapshot_client,
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            calendar_provider=StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        ),
        qmt_reconciliation_service_factory=lambda: QmtStrategyLedgerReconciliationService(repository=qmt_repo),
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=False,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=provider,
    )

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    restarted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    assert submitted.submitted_count == 0
    assert submitted.failed_count == 1
    assert submitted.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert submitted.results[0].run.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    payload = repo.get_simulation_daily_run(submitted.results[0].run.run_id).run_payload_json
    assert payload["broker_called"] is False
    assert payload["submitted_intents"] == 0
    assert payload["failed_intents"] == 3
    assert "qmt_batch_result" not in payload
    assert payload["submit_failure"]["stage"] == "MINIQMT_EVENT_LOOP_SUBMIT_FAILED"
    assert payload["submit_failure"]["context"]["reason_code"] == "MINIQMT_EVENT_LOOP_PREVIEW_ONLY_FORBIDDEN"
    assert payload["target_equity_basis"]["source"] == "miniqmt_strategy_slot_dynamic_equity"
    assert payload["target_equity_basis"]["cash"] == 100_000.0
    assert payload["target_equity_basis"]["market_value"] == 616.0
    assert payload["target_equity_basis"]["total_equity"] == 100_616.0
    assert broker.place_order_payloads == []
    assert restarted.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert restarted.results[0].run.run_payload_json["broker_called"] is False
    assert restarted.results[0].run.run_payload_json["submit_failure"]["context"]["reason_code"] == (
        "MINIQMT_EVENT_LOOP_PREVIEW_ONLY_FORBIDDEN"
    )
    assert broker.place_order_payloads == []


def _legacy_production_context_provider_miniqmt_event_loop_submit_places_broker_orders_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=qmt_binding.strategy_id,
            strategy_name=qmt_binding.strategy_name or qmt_binding.strategy_id,
            display_name="Scheduler QMT Strategy",
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id="lot_prod_event_loop_000003",
            strategy_id=qmt_binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_prod_event_loop_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
        )
    )
    broker = FakeManagedOrderBroker(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}],
        quotes={
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.0,
                "ask_price_1": 20.0,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.0,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 8.0,
                "bid_volume_1": 5000,
            },
        },
    )
    manifest = _score_weighted_manifest(release)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {
            symbol: {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}[symbol] for symbol in symbols
        },
        qmt_ledger_repository=qmt_repo,
        qmt_client_factory=lambda: broker,
        qmt_calendar_provider_factory=lambda: StaticTradingCalendarProvider([date(2026, 5, 20), TRADE_DATE]),
        package_manifest_loader=lambda package_id: manifest,
        enable_miniqmt_submit=True,
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        miniqmt_quote_ingress_activation=_RealB0TestActivation(),
        context_provider=provider,
    )
    runtime_store = tmp_path / "miniqmt-production-event-loop.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    plan = planned.results[0].execution_plan

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    assert latest_run.run_payload_json["miniqmt_runtime_kind"] == "event_loop"
    assert latest_run.run_payload_json["broker_called"] is True
    assert latest_run.run_payload_json["submitted_intents"] == len(plan.intents)
    assert latest_run.run_payload_json["failed_intents"] == 0
    assert len(broker.place_order_payloads) == len(plan.intents)
    assert (
        latest_run.run_payload_json["qmt_batch_result"]["runtime_evidence"]["source"]
        == "simulation_runtime_event_loop_submit"
    )


def _legacy_scheduler_event_loop_no_child_dispatch_stays_pending_not_failed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler(
        execution_policy_json={
            "algo_code": "TWAP_LITE_MINIQMT",
            "algo_config": {"time": 60, "interval": 60},
        }
    )
    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.0,
                "ask_price_1": 10.0,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.0,
                "ask_price_1": 20.0,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.0,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 8.0,
                "bid_volume_1": 5000,
            },
        }
    )
    runtime_store = tmp_path / "miniqmt-event-loop-no-child.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    plan = planned.results[0].execution_plan
    run = planned.results[0].run

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(run.run_id)
    assert submitted.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    assert latest_run.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest_run.run_payload_json["last_stage"] == "INTRADAY_RUNNING"
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["submitted_intents"] == 0
    assert latest_run.run_payload_json["failed_intents"] == 0
    assert latest_run.run_payload_json["pending_intents"] == len(plan.intents)
    assert latest_run.run_payload_json["qmt_batch_status"] == OrderBatchStatus.SUBMITTING.value
    assert latest_run.run_payload_json["qmt_batch_result"]["pending"] == len(plan.intents)
    assert "pre_run_failure" not in latest_run.run_payload_json
    assert "submit_failure" not in latest_run.run_payload_json
    assert broker.place_order_payloads == []


def _legacy_scheduler_restart_recovers_exact_failed_durable_pending_runtime_without_parent_resubmit(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler(
        execution_policy_json={
            "algo_code": "TWAP_LITE_MINIQMT",
            "algo_config": {"time": 60, "interval": 60},
        }
    )
    runtime_store = tmp_path / "miniqmt-failed-durable-pending-restart.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    original = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    original_batch = deepcopy(original.run_payload_json["qmt_batch_result"])
    runtime_evidence = deepcopy(original_batch["runtime_evidence"])
    intent_count = len(submitted.results[0].execution_plan.intents)
    duplicate_results = [
        {
            "success": False,
            "intent_id": None,
            "qmt_order_id": None,
            "broker_called": False,
            "broker_message": "event_loop preflight failed",
            "preflight": {
                "allowed": False,
                "errors": [
                    {
                        "code": "DUPLICATE_ORDER_REMARK",
                        "message": "order_remark already exists in this account",
                        "context": {},
                    }
                ],
            },
        }
        for _ in range(intent_count)
    ]
    failed_batch = {
        **original_batch,
        "success": False,
        "batch_status": OrderBatchStatus.PREFLIGHT_FAILED.value,
        "succeeded": 0,
        "failed": intent_count,
        "pending": 0,
        "pending_child_trigger_count": 0,
        "triggered_child_order_count": 0,
        "results": duplicate_results,
        "runtime_evidence": runtime_evidence,
    }
    failed = repo.update_simulation_daily_run(
        original.run_id,
        status=SimulationDailyRunStatus.FAILED_RETRYABLE,
        payload_patch={
            "last_stage": SimulationDailyRunStatus.FAILED_RETRYABLE.value,
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": intent_count,
            "pending_intents": 0,
            "qmt_batch_status": OrderBatchStatus.PREFLIGHT_FAILED.value,
            "qmt_batch_result": failed_batch,
            "submit_failure": {
                "type": "QuoteContractError",
                "stage": "MINIQMT_EVENT_LOOP_SUBMIT_FAILED",
                "message": "quote-less ACTION_REJECT requires complete raw ingress identity",
            },
        },
    )
    plan = submitted.results[0].execution_plan
    recovery_evidence = scheduler._miniqmt_failed_run_durable_pending_recovery_evidence(
        binding=qmt_binding,
        run=failed,
        plan=plan,
    )
    assert recovery_evidence["eligible"] is True
    assert recovery_evidence["runtime_id"] == runtime_evidence["runtime_id"]
    assert recovery_evidence["pending_algo_count"] == intent_count
    assert broker.place_order_payloads == []

    for field, value, expected_conflict in (
        ("runtime_id", "mqrt_tampered", "runtime_id_conflict"),
        ("submitted_child_count", 1, "submitted_child_side_effect_present"),
    ):
        tampered_payload = deepcopy(failed.run_payload_json)
        tampered_payload["qmt_batch_result"]["runtime_evidence"][field] = value
        tampered_run = failed.model_copy(update={"run_payload_json": tampered_payload})
        tampered_evidence = scheduler._miniqmt_failed_run_durable_pending_recovery_evidence(
            binding=qmt_binding,
            run=tampered_run,
            plan=plan,
        )
        assert tampered_evidence["eligible"] is False
        assert expected_conflict in tampered_evidence["conflicts"]
        assert (
            scheduler._should_drive_existing_miniqmt_event_loop(
                binding=qmt_binding,
                run=tampered_run,
                plan=plan,
                submit=True,
            )
            is False
        )
        assert (
            scheduler._should_submit_existing_plan(
                binding=qmt_binding,
                run=tampered_run,
                plan=plan,
                submit=True,
            )
            is False
        )

    restart_activation = _PendingOnlyB0Activation()
    restarted = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=scheduler.selection_service,
        context_provider=scheduler.context_provider,
        miniqmt_quote_ingress_activation=restart_activation,  # type: ignore[arg-type]
    )
    assert restart_activation.controller_factory.controllers == {}

    recovered = restarted.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        raise_on_error=True,
    )

    latest = repo.get_simulation_daily_run(original.run_id)
    assert recovered.results[0].status == "MINIQMT_EVENT_LOOP_TICK_DRIVEN"
    assert latest.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert latest.run_payload_json["miniqmt_failed_run_recovery"]["status"] == "RECOVERED_TO_TICK_DRIVER"
    assert latest.run_payload_json["miniqmt_failed_run_recovery"]["parent_resubmitted"] is False
    assert latest.run_payload_json["miniqmt_failed_run_recovery"]["runtime_id"] == runtime_evidence["runtime_id"]
    assert "submit_failure" not in latest.run_payload_json
    assert runtime_evidence["runtime_id"] in restart_activation.controller_factory.controllers
    assert restart_activation.controller_factory.recovering_active_by_runtime[runtime_evidence["runtime_id"]] is True
    assert broker.place_order_payloads == []


def _legacy_event_loop_retry_restores_exact_owned_parent_intents_without_self_duplicate_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler(
        candidates=_candidate_rows(),
        execution_policy_json={
            "algo_code": "TWAP_LITE_MINIQMT",
            "algo_config": {"time": 60, "interval": 60},
        },
    )
    runtime_store = tmp_path / "miniqmt-owned-parent-retry.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(submitted.results[0].run.run_id)
    context = scheduler.context_provider._by_binding_id[qmt_binding.binding_id]
    qmt_repo = context.qmt_ledger_repository
    assert isinstance(qmt_repo, InMemoryQmtStrategyLedgerRepository)
    batch_id = run.run_payload_json["qmt_batch_id"]
    batch = qmt_repo.get_order_batch(batch_id)
    assert batch is not None
    requests = miniqmt_runtime_client._event_loop_requests_from_batch(batch)
    assert requests
    sell_request = next(request for request in requests if request.side == "SELL")
    self_duplicate = context.managed_order_service.preview_order(sell_request)
    assert "DUPLICATE_ORDER_REMARK" in {error.code for error in self_duplicate.errors}
    assert self_duplicate.pending_sell_quantity is not None
    assert self_duplicate.pending_sell_quantity >= sell_request.quantity

    qmt_repo.upsert_order_batch(replace(batch, batch_status=OrderBatchStatus.PREFLIGHT_FAILED))
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    client = miniqmt_runtime_client.MiniQMTExecutionRuntimeClient(
        repository=runtime_repo,
        strategy_ledger_repository=qmt_repo,
        runtime_kind="event_loop",
    )
    repriced_requests = [
        replace(
            request,
            price=request.price + Decimal("0.01"),
            metadata={**request.metadata, "qmt_batch_id": "qmtbatch_current_quote_changed"},
        )
        for request in requests
    ]
    restored = client._event_loop_existing_batch_result(
        batch_id="qmtbatch_current_quote_changed",
        requests=repriced_requests,
        request_count=len(requests),
        managed_order_service=context.managed_order_service,
    )

    assert restored is not None
    assert restored.retry_of_batch_id == batch_id
    assert restored.submit_parent_intent_ids == frozenset(
        request.metadata["runtime_parent_intent_id"] for request in requests
    )
    assert all(result.preflight.allowed for result in restored.results)
    assert all(result.broker_called is False for result in restored.results)
    assert all("restored exact runtime-owned parent intent" in result.broker_message for result in restored.results)
    assert broker.place_order_payloads == []

    first_request = requests[0]
    runtime_repo.upsert_child_order(
        MiniQMTChildOrder(
            runtime_id=first_request.metadata["runtime_id"],
            algo_instance_id=first_request.metadata["runtime_algo_instance_id"],
            parent_intent_id=first_request.metadata["runtime_parent_intent_id"],
            strategy_slot_id=qmt_binding.strategy_slot_id or qmt_binding.binding_id,
            symbol=first_request.symbol,
            side=OrderSide(first_request.side),
            quantity=first_request.quantity,
            price=float(first_request.price),
        )
    )
    with pytest.raises(BrokerSubmitError) as side_effect:
        client._event_loop_existing_batch_result(
            batch_id="qmtbatch_current_quote_changed",
            requests=repriced_requests,
            request_count=len(requests),
            managed_order_service=context.managed_order_service,
        )
    assert side_effect.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_OWNED_RETRY_SIDE_EFFECT_PRESENT"
    assert side_effect.value.context["broker_called"] is True
    runtime_repo._child_orders.clear()

    first_parent_id = requests[0].metadata["runtime_parent_intent_id"]
    first_intent = qmt_repo.get_order_intent(first_parent_id)
    qmt_repo._order_intents[first_parent_id] = replace(
        first_intent,
        metadata={**first_intent.metadata, "runtime_id": "mqrt_foreign_owner"},
    )
    with pytest.raises(BrokerSubmitError) as mismatch:
        client._event_loop_existing_batch_result(
            batch_id="qmtbatch_current_quote_changed",
            requests=repriced_requests,
            request_count=len(requests),
            managed_order_service=context.managed_order_service,
        )
    assert mismatch.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_OWNED_RETRY_IDENTITY_MISMATCH"
    assert mismatch.value.context["broker_called"] is False


def _legacy_scheduler_poll_does_not_synthesize_b0_children_from_broker_quote_cache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler()
    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 10.5,
                "ask_price_1": 10.5,
                "ask_volume_1": 5000,
                "bid_price_1": 10.0,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 20.5,
                "ask_price_1": 20.5,
                "ask_volume_1": 5000,
                "bid_price_1": 20.0,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 7.5,
                "ask_price_1": 8.0,
                "ask_volume_1": 5000,
                "bid_price_1": 7.5,
                "bid_volume_1": 5000,
            },
        }
    )
    runtime_store = tmp_path / "miniqmt-event-loop-tick-driver.json"
    monkeypatch.delenv("MINIQMT_EXECUTION_RUNTIME", raising=False)
    monkeypatch.setenv("MINIQMT_EXECUTION_RUNTIME_STORE_PATH", str(runtime_store))

    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    plan = planned.results[0].execution_plan
    run = planned.results[0].run
    limit_by_symbol = {"000001.SZ": 10.0, "688001.SH": 20.0, "000003.SZ": 8.0}
    plan = plan.model_copy(
        update={
            "intents": [
                intent.model_copy(
                    update={
                        "price_policy": {
                            **dict(intent.price_policy),
                            "order_type": OrderType.LIMIT.value,
                            "limit_price": limit_by_symbol[intent.symbol],
                            "reference_price": limit_by_symbol[intent.symbol],
                        }
                    }
                )
                for intent in plan.intents
            ]
        }
    )
    repo.execution_plans[plan.plan_id] = plan

    submitted = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    assert submitted.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    assert broker.place_order_payloads == []
    assert repo.get_simulation_daily_run(run.run_id).status == SimulationDailyRunStatus.INTRADAY_RUNNING
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={"last_stage": "RECONCILING"},
    )

    broker.quotes.update(
        {
            "000001.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 9.9,
                "ask_price_1": 9.9,
                "ask_volume_1": 5000,
                "bid_price_1": 9.8,
                "bid_volume_1": 5000,
            },
            "688001.SH": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 19.9,
                "ask_price_1": 19.9,
                "ask_volume_1": 5000,
                "bid_price_1": 19.8,
                "bid_volume_1": 5000,
            },
            "000003.SZ": {
                "source": "MINIQMT_REALTIME.broker_quote",
                "price": 8.1,
                "ask_price_1": 8.2,
                "ask_volume_1": 5000,
                "bid_price_1": 8.1,
                "bid_volume_1": 5000,
            },
        }
    )

    scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 14, 55),
    )

    latest_run = repo.get_simulation_daily_run(run.run_id)
    assert latest_run.run_payload_json["miniqmt_event_loop_tick_driver"]["errors"] == []
    assert latest_run.run_payload_json["miniqmt_event_loop_tick_driver"]["triggered_child_order_count"] == 0
    assert broker.place_order_payloads == []
    assert latest_run.status in {
        SimulationDailyRunStatus.RECONCILING,
        SimulationDailyRunStatus.INTRADAY_RUNNING,
    }


def _legacy_scheduler_automatically_recovers_exact_legacy_b0_context_failure_without_side_effects() -> None:
    scheduler, repo, broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={
            "last_stage": "RECONCILING",
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": 0,
            "submit_failure": {
                "type": "QuoteContractError",
                "stage": "MINIQMT_EVENT_LOOP_SUBMIT_FAILED",
                "context": None,
                "message": "B0_QUOTE_V2 controller requires scheduler-published context",
                "outer_stage": "MINIQMT_EVENT_LOOP_SUBMIT_FAILED",
            },
        },
    )

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )

    latest = repo.get_simulation_daily_run(run.run_id)
    assert retried.results[0].status != "REUSED_EXISTING_PLAN"
    assert latest.run_payload_json["miniqmt_legacy_b0_context_missing_recovery"]["runtime_execution_evidence"] is False
    assert latest.run_payload_json["miniqmt_legacy_b0_context_missing_recovery"]["broker_side_effect_evidence"] is False
    assert latest.status in {SimulationDailyRunStatus.INTRADAY_RUNNING, SimulationDailyRunStatus.SUCCEEDED}
    assert retried.results[0].status == "MINIQMT_EVENT_LOOP_PENDING"
    assert latest.run_payload_json["broker_called"] is False
    assert broker.place_order_payloads == []


def _legacy_scheduler_does_not_auto_recover_legacy_context_failure_with_runtime_evidence() -> None:
    scheduler, repo, _broker, qmt_binding = _miniqmt_event_loop_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    stuck = repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={
            "last_stage": "RECONCILING",
            "broker_called": False,
            "submitted_intents": 0,
            "qmt_batch_result": {"status": "SUBMITTING", "pending": 0},
            "submit_failure": {
                "type": "QuoteContractError",
                "stage": "MINIQMT_EVENT_LOOP_SUBMIT_FAILED",
                "context": None,
                "message": "B0_QUOTE_V2 controller requires scheduler-published context",
            },
        },
    )

    unchanged = scheduler._recover_legacy_b0_context_missing_run_if_safe(
        binding=qmt_binding,
        run=stuck,
        plan=plan,
        submit=True,
    )

    assert unchanged.status == SimulationDailyRunStatus.RECONCILING
    assert "miniqmt_legacy_b0_context_missing_recovery" not in unchanged.run_payload_json


def _legacy_scheduler_converts_no_side_effect_reconciling_after_runtime_only_cleanup_and_retries() -> None:
    scheduler, repo, broker, qmt_binding = _miniqmt_event_loop_test_scheduler(real_callback=True)
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    plan = planned.results[0].execution_plan
    assert run is not None
    assert plan is not None
    runtime_id = simulation_bridges.MiniQMTExecutionBridge._runtime_id(plan=plan, binding=qmt_binding)
    runtime_repo = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id=runtime_id,
            account_group_id=qmt_binding.account_group_id or qmt_binding.broker_account_id or "QMT_SIM_ACCOUNT",
            trade_date=TRADE_DATE,
            runtime_config_hash=plan.plan_hash,
        ),
        repository=runtime_repo,
        gateway=gateway,
    )
    runtime.start()
    stale_algo = runtime.create_algo_instance(
        parent_intent_id="intent_stale_reconciling",
        strategy_slot_id=qmt_binding.strategy_slot_id or qmt_binding.binding_id,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.submit_child_order(algo_instance_id=stale_algo.algo_instance_id, quantity=100, price=10.0)
    gateway._orders.clear()
    stuck = repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={
            "last_stage": "RECONCILING",
            "broker_called": False,
            "submitted_intents": 0,
            "failed_intents": 0,
            "order_intent_count": len(plan.intents),
        },
    )

    retry_before_recovery = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    assert retry_before_recovery.results[0].status == "REUSED_EXISTING_PLAN"
    assert broker.place_order_payloads == []

    operator_result = runtime.execute_operator_command(
        command_id="opcmd_recover_reconciling_001",
        command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        reason="unit stale runtime recovery",
        payload={"run_id": stuck.run_id},
    )
    assert operator_result.status == MiniQMTOperatorCommandStatus.EXECUTED
    recovered = scheduler.recover_no_side_effect_reconciling_run_after_operator_cleanup(
        run_id=stuck.run_id,
        operator_result=operator_result,
        source="unit_test",
    )
    assert recovered.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert recovered.run_payload_json["miniqmt_no_side_effect_reconciling_recovery"]["broker_called"] is False
    assert recovered.run_payload_json["submit_failure"]["stage"] == "MINIQMT_NO_SIDE_EFFECT_RECONCILING_RECOVERY"

    retried = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    latest = repo.get_simulation_daily_run(stuck.run_id)
    assert retried.results[0].status == "RECONCILED"
    assert latest.run_payload_json["broker_called"] is True
    assert len(broker.place_order_payloads) == len(plan.intents)


def test_scheduler_stale_runtime_recovery_rejects_non_reconciling_run() -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.SUCCEEDED,
        payload_patch={"broker_called": False, "submitted_intents": 0},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_RUN_STATUS_UNSUPPORTED"):
        scheduler.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run.run_id)


@pytest.mark.parametrize(
    ("broker_called", "submitted_intents"),
    [
        (True, 0),
        (False, 1),
    ],
)
def test_scheduler_stale_runtime_recovery_rejects_run_with_side_effect_evidence(
    broker_called: bool,
    submitted_intents: int,
) -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={"broker_called": broker_called, "submitted_intents": submitted_intents},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_RUN_HAS_SIDE_EFFECT_EVIDENCE"):
        scheduler.require_no_side_effect_reconciling_run_for_operator_recovery(run_id=run.run_id)


@pytest.mark.parametrize(
    "operator_result",
    [
        SimpleNamespace(
            command_id="opcmd_recover_rejected",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.REJECTED,
            metadata={"broker_evidence": {"broker_open_order_count": 0}, "broker_mutated": False},
        ),
        SimpleNamespace(
            command_id="opcmd_recover_open_broker",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.EXECUTED,
            metadata={"broker_evidence": {"broker_open_order_count": 1}, "broker_mutated": False},
        ),
        SimpleNamespace(
            command_id="opcmd_recover_mutated_broker",
            command_type="RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
            status=MiniQMTOperatorCommandStatus.EXECUTED,
            metadata={"broker_evidence": {"broker_open_order_count": 0}, "broker_mutated": True},
        ),
    ],
)
def test_scheduler_stale_runtime_recovery_rejects_bad_operator_evidence(operator_result: SimpleNamespace) -> None:
    scheduler, repo, _broker, _qmt_binding = _miniqmt_event_loop_test_scheduler()
    planned = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
    )
    run = planned.results[0].run
    assert run is not None
    repo.update_simulation_daily_run(
        run.run_id,
        status=SimulationDailyRunStatus.RECONCILING,
        payload_patch={"broker_called": False, "submitted_intents": 0},
    )

    with pytest.raises(RuntimeConfigInvalidError, match="MINIQMT_STALE_RUNTIME_RECOVERY_OPERATOR_EVIDENCE_REJECTED"):
        scheduler.recover_no_side_effect_reconciling_run_after_operator_cleanup(
            run_id=run.run_id,
            operator_result=operator_result,
            source="unit_test_bad_operator_evidence",
        )


def test_production_context_provider_builds_localsim_broker_from_persisted_paper_state():
    """LocalSim production context constructs a real broker using persisted Paper v2 cash and lots."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim prod context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=1000,
            avg_cost=10.0,
            trade_date=TRADE_DATE,
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    package_manifest_loads: list[str] = []

    def unexpected_package_manifest_load(package_id: str) -> StrategyPackageManifest:
        package_manifest_loads.append(package_id)
        raise AssertionError("ordinary LocalSIM binding must remain pinned to portfolio frozen manifest")

    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        package_manifest_loader=unexpected_package_manifest_load,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.local_broker is not None
    assert ctx.local_broker.data_source == MinuteDataSource.DB_HISTORICAL
    assert ctx.market_data_source == MinuteDataSource.DB_HISTORICAL.value
    assert ctx.local_broker.query_account().cash == Decimal("980000")
    assert ctx.local_broker.query_positions()["000001.SZ"].quantity == 1000
    assert ctx.local_broker.query_positions()["000001.SZ"].available_quantity == 1000
    assert ctx.manifest is manifest
    assert ctx.context_diagnostics["manifest_identity"] == {
        "schema_version": "localsim_manifest_identity_resolution_v1",
        "source": "paper_v2_portfolio_frozen_manifest",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "strategy_package_revalidation_performed": False,
    }
    assert package_manifest_loads == []
    assert ctx.context_diagnostics["localsim_tplus1_settlement"]["settled_position_count"] == 0


def test_existing_localsim_plan_context_uses_frozen_tradability_and_causal_broker_without_market_db_reads() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    planning_scheduler, _planning_repo, _planning_broker, _planning_binding = _miniqmt_event_loop_test_scheduler()
    planned = planning_scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=False,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(9, 25), tzinfo=ZoneInfo("Asia/Shanghai")),
    )
    plan = planned.results[0].execution_plan
    assert plan is not None

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim existing plan context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.TDX_REALTIME,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_twap",
            "policy_sha256": _local_sim_execution_policy()["policy_sha256"],
            "policy_json": _local_sim_execution_policy()["policy_json"],
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=1000,
            avg_cost=10.0,
            trade_date=TRADE_DATE,
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    tradability = FakePreTradeTradabilityProvider()
    price_loader_calls: list[tuple[list[str], date]] = []

    def forbidden_price_loader(symbols: list[str], trade_date: date) -> dict[str, float]:
        price_loader_calls.append((list(symbols), trade_date))
        raise AssertionError("existing LocalSIM plan must not read database market prices")

    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=forbidden_price_loader,
        pre_trade_tradability_provider=tradability,
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    context = provider.load_existing_plan_context(
        runtime_release=release,
        binding=binding,
        plan=plan,
        trade_date=TRADE_DATE,
        as_of_time=datetime.combine(TRADE_DATE, wall_time(10, 0), tzinfo=ZoneInfo("Asia/Shanghai")),
    )

    assert price_loader_calls == []
    assert tradability.calls == []
    assert context.current_prices == {}
    assert context.local_broker is not None
    assert context.target_total_equity is None
    assert context.target_equity_context["planning_market_data_reloaded"] is False


def test_production_context_provider_loads_authoritative_manifest_for_verified_localsim_successor():
    """A BUG-639 successor uses one current manifest across context and LocalSIM broker."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    source_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    first_successor_manifest_sha256 = "manifest_after_first_controlled_package_update"
    first_successor_release, first_successor_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256=first_successor_manifest_sha256,
    )
    authoritative_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256="manifest_after_second_controlled_package_update",
    )
    successor_release, successor_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=first_successor_release,
        source_binding=first_successor_binding,
        authoritative_manifest_sha256=authoritative_manifest.manifest_sha256 or "",
        repository=runtime_repository,
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=source_manifest)
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    package_manifest_loads: list[str] = []

    def load_package_manifest(package_id: str) -> StrategyPackageManifest:
        package_manifest_loads.append(package_id)
        return authoritative_manifest

    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        package_manifest_loader=load_package_manifest,
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    ctx = provider.load_context(
        runtime_release=successor_release,
        binding=successor_binding,
        trade_date=TRADE_DATE,
    )

    assert package_manifest_loads == [source_release.package_id]
    assert ctx.manifest is authoritative_manifest
    assert ctx.local_broker is not None
    assert ctx.local_broker._manifest is authoritative_manifest
    assert ctx.context_diagnostics["manifest_identity"] == {
        "schema_version": "localsim_manifest_identity_resolution_v1",
        "source": "strategy_package_current_manifest",
        "package_id": source_release.package_id,
        "manifest_sha256": authoritative_manifest.manifest_sha256,
        "source_release_manifest_sha256": first_successor_manifest_sha256,
        "manifest_identity_changed": True,
        "extends_binding_id": first_successor_binding.binding_id,
        "extends_release_id": first_successor_release.release_id,
        "source_binding_readback_id": first_successor_binding.binding_id,
        "source_release_readback_id": first_successor_release.release_id,
        "strategy_package_revalidation_performed": False,
    }


def test_production_context_provider_accepts_verified_localsim_successor_when_manifest_is_unchanged():
    """Daily immutable lineage remains valid without revalidating or changing the admitted package."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    authoritative_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, successor_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256=source_release.manifest_sha256,
    )
    portfolio_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256="older_portfolio_manifest",
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=portfolio_manifest)
    package_manifest_loads: list[str] = []

    def load_package_manifest(package_id: str) -> StrategyPackageManifest:
        package_manifest_loads.append(package_id)
        return authoritative_manifest

    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=load_package_manifest,
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    ctx = provider.load_context(
        runtime_release=successor_release,
        binding=successor_binding,
        trade_date=TRADE_DATE,
    )

    assert package_manifest_loads == [source_release.package_id]
    assert ctx.manifest is authoritative_manifest
    assert ctx.local_broker is not None
    assert ctx.local_broker._manifest is authoritative_manifest
    assert ctx.context_diagnostics["manifest_identity"] == {
        "schema_version": "localsim_manifest_identity_resolution_v1",
        "source": "strategy_package_current_manifest",
        "package_id": source_release.package_id,
        "manifest_sha256": source_release.manifest_sha256,
        "source_release_manifest_sha256": source_release.manifest_sha256,
        "manifest_identity_changed": False,
        "extends_binding_id": source_binding.binding_id,
        "extends_release_id": source_release.release_id,
        "source_binding_readback_id": source_binding.binding_id,
        "source_release_readback_id": source_release.release_id,
        "strategy_package_revalidation_performed": False,
    }


def test_production_context_provider_rejects_false_manifest_change_claim_for_unchanged_successor():
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    authoritative_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, successor_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256=source_release.manifest_sha256,
        binding_metadata_patch={"manifest_identity_changed": True},
    )
    portfolio_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256="older_portfolio_manifest",
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=portfolio_manifest)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=lambda _package_id: authoritative_manifest,
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="successor lineage is invalid") as exc_info:
        provider.load_context(
            runtime_release=successor_release,
            binding=successor_binding,
            trade_date=TRADE_DATE,
        )

    assert "binding.metadata.manifest_identity_changed" in exc_info.value.context["violations"]


def test_production_context_provider_does_not_substitute_package_manifest_for_unmarked_localsim_mismatch():
    """An arbitrary LocalSIM mismatch remains pinned and fails before package lookup."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    source_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, unmarked_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256="manifest_after_unmarked_update",
        binding_metadata_override={},
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=source_manifest)
    package_manifest_loads: list[str] = []
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=lambda package_id: package_manifest_loads.append(package_id),
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    with pytest.raises(DataUnavailableError, match="LocalSim manifest hash does not match runtime release binding"):
        provider.load_context(
            runtime_release=successor_release,
            binding=unmarked_binding,
            trade_date=TRADE_DATE,
        )

    assert package_manifest_loads == []


def test_production_context_provider_rejects_malformed_localsim_successor_lineage_before_package_lookup():
    """The successor marker alone cannot authorize a current-package substitution."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    source_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, malformed_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256="manifest_after_controlled_update",
        binding_metadata_patch={"source_release_manifest_sha256": "unrelated_predecessor_manifest"},
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=source_manifest)
    package_manifest_loads: list[str] = []
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=lambda package_id: package_manifest_loads.append(package_id),
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    with pytest.raises(RuntimeConfigInvalidError, match="successor lineage is invalid") as exc_info:
        provider.load_context(
            runtime_release=successor_release,
            binding=malformed_binding,
            trade_date=TRADE_DATE,
        )

    assert "release.metadata.source_release_manifest_sha256" in exc_info.value.context["violations"]
    assert (
        "validation_evidence.manifest_identity.source_release_manifest_sha256" in exc_info.value.context["violations"]
    )
    assert package_manifest_loads == []


def test_production_context_provider_requires_persisted_localsim_successor_source_records():
    """Successor metadata must read back its exact source release and binding before package lookup."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    source_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, successor_binding, _ = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256="manifest_with_missing_source_readback",
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=source_manifest)
    package_manifest_loads: list[str] = []
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=lambda package_id: package_manifest_loads.append(package_id),
        runtime_repository=InMemorySimulationRuntimeRepository(),
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    with pytest.raises(DataUnavailableError, match="failed to read LocalSim successor source release and binding"):
        provider.load_context(
            runtime_release=successor_release,
            binding=successor_binding,
            trade_date=TRADE_DATE,
        )

    assert package_manifest_loads == []


def test_env_builder_shares_runtime_repository_with_production_context_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Production context source-lineage readback must use the scheduler's repository authority."""
    import backend.services.simulation_runtime.scheduler as scheduler_module

    repository = InMemorySimulationRuntimeRepository()
    monkeypatch.setenv("SIMULATION_RUNTIME_CONTEXT_PROVIDER", "production")
    monkeypatch.setattr(scheduler_module, "build_miniqmt_quote_ingress_activation_from_env", lambda: None)

    scheduler = scheduler_module.build_simulation_lifecycle_scheduler_from_env(repository=repository)

    assert scheduler.repository is repository
    assert isinstance(scheduler.context_provider, scheduler_module.ProductionSimulationRunContextProvider)
    assert scheduler.context_provider._runtime_repository is repository


def test_production_context_provider_rejects_wrong_authoritative_manifest_for_valid_localsim_successor():
    """A verified lineage still fails loud when the package repository returns another hash."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    source_release = _make_test_release()
    source_binding = _make_test_binding(source_release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    source_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256=source_release.manifest_sha256,
    )
    successor_release, successor_binding, runtime_repository = _make_localsim_manifest_successor(
        source_release=source_release,
        source_binding=source_binding,
        authoritative_manifest_sha256="expected_authoritative_manifest",
    )
    wrong_manifest = _frozen_manifest(
        package_id=source_release.package_id,
        manifest_sha256="unexpected_current_manifest",
    )
    portfolio = _make_localsim_portfolio(source_release=source_release, manifest=source_manifest)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        package_manifest_loader=lambda package_id: wrong_manifest,
        runtime_repository=runtime_repository,
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )

    with pytest.raises(DataUnavailableError, match="LocalSim manifest hash does not match runtime release binding"):
        provider.load_context(
            runtime_release=successor_release,
            binding=successor_binding,
            trade_date=TRADE_DATE,
        )


def test_production_context_provider_settles_localsim_tplus1_positions_for_trade_date():
    """LocalSim unattended context must unlock prior-day Paper v2 lots before rebalance planning."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim prod context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=0,
            avg_cost=10.0,
            trade_date=TRADE_DATE - timedelta(days=1),
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert ctx.current_positions["000001.SZ"].available_quantity == 1000
    assert ctx.local_broker.query_positions()["000001.SZ"].available_quantity == 1000
    settlement = ctx.context_diagnostics["localsim_tplus1_settlement"]
    assert settlement["settled_position_count"] == 1
    assert settlement["settled_positions"][0]["previous_available_quantity"] == 0


def test_production_context_provider_uses_tdx_realtime_for_same_day_localsim() -> None:
    """Same-day unattended LocalSim must not depend on post-market DB minute sync."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    trade_date = date.today()
    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim same-day prod context",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=trade_date,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    positions = {
        "000001.SZ": PositionLot(
            portfolio_id="strat1",
            symbol="000001.SZ",
            quantity=1000,
            available_quantity=1000,
            avg_cost=10.0,
            trade_date=trade_date,
        )
    }
    paper_repo = FakePaperRepository(portfolio, positions=positions, cash=980_000)
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.5 for symbol in symbols},
        pre_trade_tradability_provider=FakePreTradeTradabilityProvider(),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=trade_date)

    assert ctx.local_broker is not None
    assert ctx.local_broker.data_source == MinuteDataSource.TDX_REALTIME
    assert ctx.market_data_source == MinuteDataSource.TDX_REALTIME.value


def test_production_context_provider_twap_only_mode_does_not_depend_on_incomplete_source_policy() -> None:
    """LocalSIM selects its explicit TWAP policy without consulting stale portfolio V25."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    policy_id = "vnpy_asset:SNIPER_MINIQMT:final_multistrategy_dry_run_20260603"
    release = _make_test_release(
        execution_policy_version_id=policy_id,
        execution_policy_sha256="sha_vnpy_release",
        execution_policy={"policy_version_id": policy_id, "policy_sha256": "sha_vnpy_release"},
    )
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim stale V25 guard",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_v25_1_small_cap",
            "policy_sha256": "sha_portfolio_v25",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    context = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert context.execution_policy_payload["policy_version_id"] == LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID
    assert context.execution_policy_payload["policy_json"]["algo_code"] == "TWAP"
    assert context.local_broker is not None


@pytest.mark.parametrize(
    ("release_field", "drifted_value"),
    [
        ("execution_policy_version_id", "exec_policy_runtime_identity_drift"),
        ("execution_policy_sha256", "0" * 64),
    ],
)
def test_production_context_provider_twap_only_mode_is_independent_of_source_policy_identity(
    release_field: str,
    drifted_value: str,
) -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    drifted_release = release.model_copy(update={release_field: drifted_value})
    manifest = _frozen_manifest(
        package_id=drifted_release.package_id,
        manifest_sha256=drifted_release.manifest_sha256,
    )
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim release identity conflict",
        package_id=drifted_release.package_id,
        manifest_sha256=drifted_release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "unused_portfolio_policy",
            "policy_sha256": "unused_portfolio_policy_hash",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
    )
    binding = _make_test_binding(
        drifted_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    context = provider.load_context(
        runtime_release=drifted_release,
        binding=binding,
        trade_date=TRADE_DATE,
    )

    assert context.execution_policy_payload["policy_version_id"] == LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID
    assert context.execution_policy_payload["policy_json"]["algo_code"] == "TWAP"


def test_production_context_provider_selects_explicit_twap_only_policy_for_v25_release() -> None:
    """LocalSIM validates the source release, then selects its explicit TWAP runtime policy."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release(
        execution_policy_version_id="exec_policy_runtime_v25",
        execution_policy_sha256="sha_runtime_v25",
        execution_policy={
            "policy_version_id": "exec_policy_runtime_v25",
            "policy_sha256": "sha_runtime_v25",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim release policy authority",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_v25_1_small_cap",
            "policy_sha256": "sha_portfolio_v25",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    market_data = TwapSixBarLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.execution_policy_payload["policy_version_id"] == LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID
    assert ctx.execution_policy_payload["policy_json"]["algo_code"] == "TWAP"
    assert ctx.execution_policy_payload["policy_json"]["fallback_algo_code"] is None
    assert LOCALSIM_TWAP_ONLY_REASON_CODE == "LOCALSIM_TWAP_ONLY_POLICY"
    assert ctx.local_broker is not None
    ctx.local_broker._market_data_provider = market_data
    handle = ctx.local_broker.submit_order_intent(
        OrderIntent(
            package_id=release.package_id,
            portfolio_id="strat1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.MARKET,
            target_trade_date=TRADE_DATE,
        )
    )

    assert ctx.local_broker.query_status(handle).state == "filled"
    assert market_data.calls[-1]["require_day_features"] is False


def test_production_context_provider_policy_authority_never_uses_portfolio_policy_for_recovery():
    """LocalSim recovery must keep the frozen release policy and ignore portfolio/manifest policy."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release()
    manifest = _frozen_manifest(package_id=release.package_id, manifest_sha256=release.manifest_sha256)
    portfolio = PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSim alpha-core recovery",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": "exec_policy_stale_portfolio_v25",
            "policy_sha256": "stale_portfolio_hash_not_consulted",
            "policy_json": {
                "algo_code": "V25_1_SMALL_CAP",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    market_data = TwapSixBarLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.execution_policy_payload["policy_version_id"] == LOCALSIM_TWAP_ONLY_POLICY_VERSION_ID
    assert ctx.execution_policy_payload["policy_json"]["algo_code"] == "TWAP"
    assert ctx.local_broker is not None
    ctx.local_broker._market_data_provider = market_data
    handle = ctx.local_broker.submit_order_intent(
        OrderIntent(
            package_id=release.package_id,
            portfolio_id="strat1",
            symbol="000001.SZ",
            side=OrderSide.BUY,
            quantity=100,
            order_type=OrderType.MARKET,
            target_trade_date=TRADE_DATE,
        )
    )

    assert ctx.local_broker.query_status(handle).state == "filled"
    assert market_data.calls[-1]["require_day_features"] is False


def test_existing_localsim_v25_plan_is_rejected_before_runtime_context_loading() -> None:
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    legacy_plan = SimpleNamespace(
        plan_id="plan_legacy_localsim_v25",
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        plan_payload_json={
            "execution_policy": {
                "payload": {
                    "policy_version_id": "exec_policy_v25_1_small_cap",
                    "policy_sha256": "legacy_v25_sha",
                    "policy_json": {"algo_code": "V25_1_SMALL_CAP", "algo_config": {}},
                }
            }
        },
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        ProductionSimulationLifecycleScheduler._assert_local_sim_plan_uses_twap(
            binding=binding,
            plan=legacy_plan,
        )

    assert exc_info.value.context["reason_code"] == "LOCALSIM_LEGACY_EXECUTION_PLAN_POLICY_RETIRED"
    assert exc_info.value.context["plan_algo_code"] == "V25_1_SMALL_CAP"
    assert exc_info.value.context["broker_call_attempted"] is False
    assert exc_info.value.context["fallback_used"] is False


@pytest.mark.parametrize(
    "plan_payload_json",
    [
        {},
        {"execution_policy": "not-an-object"},
        {"execution_policy": {"payload": "not-an-object"}},
        {"execution_policy": {"payload": {"policy_json": "not-an-object"}}},
        {"execution_policy": {"payload": {"policy_json": {}}}},
        {"execution_policy": {"payload": {"policy_json": {"algo_code": "  "}}}},
        {"execution_policy": {"payload": {}}},
    ],
)
def test_existing_localsim_plan_with_unreadable_policy_is_fail_loud_not_retired(
    plan_payload_json: dict[str, Any],
) -> None:
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    malformed_plan = SimpleNamespace(
        plan_id="plan_malformed_localsim_policy",
        execution_policy_version_id=None,
        plan_payload_json=plan_payload_json,
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        ProductionSimulationLifecycleScheduler._assert_local_sim_plan_uses_twap(
            binding=binding,
            plan=malformed_plan,
        )

    assert exc_info.value.context["reason_code"] == "LOCALSIM_EXECUTION_PLAN_POLICY_MISSING_OR_MALFORMED"
    assert exc_info.value.context["plan_algo_code"] is None
    assert exc_info.value.context["broker_call_attempted"] is False
    assert exc_info.value.context["fallback_used"] is False


def test_existing_localsim_plan_with_inline_twap_policy_payload_is_accepted() -> None:
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    inline_plan = SimpleNamespace(
        plan_id="plan_inline_twap_localsim_policy",
        execution_policy_version_id="exec_policy_twap",
        plan_payload_json={"execution_policy": {"payload": {"algo_code": "TWAP", "algo_config": {}}}},
    )

    ProductionSimulationLifecycleScheduler._assert_local_sim_plan_uses_twap(binding=binding, plan=inline_plan)


def test_scheduler_rejects_stale_selection_evidence_for_new_trade_date():
    release, local_binding, _, repo = _release_and_bindings(qmt_only=False)
    stale_evidence = _evidence(release, candidates=_candidate_rows())
    stale_selection = StrategyPackageSelectionResult(
        runtime_config={},
        package_results={release.package_id: _candidate_rows()},
        aggregate_results=_candidate_rows(),
        excluded_results={release.package_id: []},
        manifest_sha256_by_package={release.package_id: release.manifest_sha256},
        evidence_by_package={release.package_id: stale_evidence},
    )

    class StaleSelectionService:
        package_repository = FakePackageRepository(
            package_id=release.package_id,
            manifest_sha256=release.manifest_sha256,
        )

        def run_selection(self, **kwargs):
            return stale_selection

    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=StaleSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id=local_binding.strategy_id)}
        ),
    )

    result = scheduler.run_once(
        trade_date=date(2026, 5, 22),
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    assert result.failed_count == 1
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result.results[0].error["type"] == "DataUnavailableError"
    assert "stale daily selection evidence" in result.results[0].error["message"]


def test_scheduler_rejects_stale_pit_cutoff_selection_evidence_for_trade_date():
    stale_runtime_config = {
        "selection_artifact_config": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "cutoff_date": "2026-05-19",
        },
        "point_in_time_context": {
            "pit_mode": "PREVIOUS_TRADING_DAY_CLOSE",
            "trade_date": TRADE_DATE.isoformat(),
            "requested_trade_date": TRADE_DATE.isoformat(),
            "effective_trade_date": TRADE_DATE.isoformat(),
            "cutoff_date": "2026-05-19",
            "score_trade_date": "2026-05-19",
            "reference_price_trade_date": "2026-05-19",
        },
    }
    release, local_binding, _, repo = _release_and_bindings(
        qmt_only=False,
        release_metadata={"selection_runtime_config": stale_runtime_config},
    )
    stale_evidence = _evidence(
        release,
        candidates=_candidate_rows(),
        cutoff_date=date(2026, 5, 19),
    )
    stale_selection = StrategyPackageSelectionResult(
        runtime_config=stale_runtime_config,
        package_results={release.package_id: _candidate_rows()},
        aggregate_results=_candidate_rows(),
        excluded_results={release.package_id: []},
        manifest_sha256_by_package={release.package_id: release.manifest_sha256},
        evidence_by_package={release.package_id: stale_evidence},
    )

    class RollingCalendar:
        def ensure_trading_day(self, trade_date: date) -> None:
            if trade_date != TRADE_DATE:
                raise DataUnavailableError("not a trading day", context={"trade_date": trade_date.isoformat()})

        def list_trading_days(self, start_date: date, end_date: date) -> list[date]:
            return [
                item for item in (date(2026, 5, 19), date(2026, 5, 20), TRADE_DATE) if start_date <= item <= end_date
            ]

    class StaleCutoffSelectionService:
        def __init__(self) -> None:
            self.resolver = StrategyPackageSelectionService(calendar_provider=RollingCalendar())
            self.package_repository = FakePackageRepository(
                package_id=release.package_id,
                manifest_sha256=release.manifest_sha256,
            )

        def run_selection(self, **kwargs):
            return stale_selection

        def resolve_point_in_time_context(self, **kwargs):
            return self.resolver.resolve_point_in_time_context(**kwargs)

    assert local_binding is not None
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=StaleCutoffSelectionService(),
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: _position_context(portfolio_id=local_binding.strategy_id)}
        ),
    )

    result = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
    )

    context = result.results[0].error["context"]
    assert result.failed_count == 1
    assert result.results[0].status == SimulationDailyRunStatus.FAILED_RETRYABLE.value
    assert result.results[0].error["type"] == "DataUnavailableError"
    assert "cutoff_date" in context["reasons"]
    assert context["cutoff_date"] == "2026-05-19"
    assert context["expected_cutoff_date"] == "2026-05-20"


def test_scheduler_status_reports_provider_and_controlled_tick_capability():
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    scheduler = SimulationLifecycleScheduler(context_provider=ProductionSimulationRunContextProvider())
    status = scheduler.status()

    assert status["manual_tick_endpoint_enabled"] is True
    assert status["context_provider_mode"] == "production"
    assert status["context_provider"]["miniqmt_preview_enabled"] is True
    assert status["default_submit"] is False
    assert status["sim_binding_selection_policy"] == "all_non_retired"
    assert SimulationBindingApprovalState.DRAFT.value in status["approval_states"]
    assert SimulationBindingApprovalState.RETIRED.value not in status["approval_states"]


def test_fail_fast_provider_still_rejects():
    """FailFastSimulationRunContextProvider still raises DataUnavailableError."""
    from backend.services.simulation_runtime.scheduler import FailFastSimulationRunContextProvider
    from backend.services.trading_core.errors import DataUnavailableError

    provider = FailFastSimulationRunContextProvider()
    release = _make_test_release()
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)
    try:
        provider.load_context(runtime_release=release, binding=binding, trade_date=date.today())
        raise AssertionError("expected DataUnavailableError")
    except DataUnavailableError as exc:
        assert "requires an explicit run context provider" in str(exc)


def _make_test_release(
    *,
    execution_policy_version_id: str = "exec_policy_close_price",
    execution_policy_sha256: str = "policy_sha256",
    execution_policy: dict[str, Any] | None = None,
):
    from backend.services.simulation_runtime.models import StrategyRuntimeRelease

    if execution_policy is None:
        policy_json = normalize_execution_policy_json(
            {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            }
        )
        execution_policy_sha256 = compute_execution_policy_sha256(policy_json)
        policy_payload = {
            "policy_version_id": execution_policy_version_id,
            "policy_sha256": execution_policy_sha256,
            "policy_json": policy_json,
        }
    else:
        policy_payload = dict(execution_policy)
        raw_policy_json = policy_payload.get("policy_json")
        if isinstance(raw_policy_json, dict) and raw_policy_json:
            normalized = normalize_execution_policy_json(raw_policy_json)
            execution_policy_sha256 = compute_execution_policy_sha256(normalized)
            policy_payload["policy_sha256"] = execution_policy_sha256
            policy_payload["policy_json"] = normalized
    return StrategyRuntimeRelease(
        package_id="pkg",
        manifest_sha256="aa",
        runtime_profile_id="rp",
        runtime_profile_version_id="rpv",
        runtime_profile_sha256="rps",
        daily_strategy_profile_version_id="dsp",
        execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256,
        tail_policy_version_id="tpv",
        tail_policy_sha256="tps",
        release_config_json={
            "schema_version": "strategy_runtime_release_v1",
            "package_id": "pkg",
            "manifest_sha256": "aa",
            "runtime_profile": {"profile_id": "rp", "profile_version_id": "rpv", "config_sha256": "rps"},
            "daily_strategy": {"profile_version_id": "dsp"},
            "execution_policy": policy_payload,
            "tail_policy": {"policy_version_id": "tpv", "policy_sha256": "tps"},
            "validation_state": "DRAFT",
            "validation_evidence": {},
            "metadata": {},
        },
    )


def _make_test_binding(release, *, broker_backend):
    from backend.services.simulation_runtime.models import SimulationReleaseBinding

    return SimulationReleaseBinding(
        strategy_id="strat1",
        release_id=release.release_id,
        release_hash=release.release_hash or "",
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        broker_backend=broker_backend,
        capital_allocation=1_000_000.0,
        binding_config_json={
            "schema_version": "simulation_release_binding_v1",
            "strategy_id": "strat1",
            "release_id": release.release_id,
            "release_hash": release.release_hash or "",
            "package_id": release.package_id,
            "manifest_sha256": release.manifest_sha256,
            "broker_backend": broker_backend.value,
            "capital_allocation": 1_000_000.0,
            "approval_state": "DRAFT",
            "metadata": {},
        },
    )


def _make_localsim_manifest_successor(
    *,
    source_release,
    source_binding,
    authoritative_manifest_sha256: str,
    repository: InMemorySimulationRuntimeRepository | None = None,
    binding_metadata_patch: dict[str, Any] | None = None,
    binding_metadata_override: dict[str, Any] | None = None,
):
    repository = repository or InMemorySimulationRuntimeRepository()
    repository.save_strategy_runtime_release(source_release)
    repository.save_simulation_release_binding(source_binding)
    release_service = StrategyRuntimeReleaseService(repository=repository)
    release_metadata = SimulationLifecycleScheduler._roll_forward_release_metadata(
        source_release=source_release,
        source_binding=source_binding,
        trade_date=TRADE_DATE,
        authoritative_manifest_sha256=authoritative_manifest_sha256,
    )
    validation_evidence = SimulationLifecycleScheduler._roll_forward_validation_evidence(
        source_release=source_release,
        source_binding=source_binding,
        trade_date=TRADE_DATE,
        authoritative_manifest_sha256=authoritative_manifest_sha256,
    )
    execution_policy = source_release.release_config_json.get("execution_policy")
    execution_policy_json = (
        execution_policy.get("policy_json")
        if isinstance(execution_policy, dict) and isinstance(execution_policy.get("policy_json"), dict)
        else None
    )
    successor_release = release_service.create_release(
        package_id=source_release.package_id,
        manifest_sha256=authoritative_manifest_sha256,
        base_release_id=source_release.release_id,
        runtime_profile_id=source_release.runtime_profile_id,
        runtime_profile_version_id=source_release.runtime_profile_version_id,
        runtime_profile_sha256=source_release.runtime_profile_sha256,
        daily_strategy_profile_version_id=source_release.daily_strategy_profile_version_id,
        execution_policy_version_id=source_release.execution_policy_version_id,
        execution_policy_sha256=source_release.execution_policy_sha256,
        tail_policy_version_id=source_release.tail_policy_version_id,
        tail_policy_sha256=source_release.tail_policy_sha256,
        execution_policy_json=execution_policy_json,
        validation_state=source_release.validation_state,
        validation_evidence=validation_evidence,
        release_metadata=release_metadata,
        effective_from=TRADE_DATE,
        effective_to=TRADE_DATE,
        created_by="simulation_lifecycle_scheduler.localsim_roll_forward",
        created_reason="unit-test LocalSIM manifest successor",
    )
    generated_binding_metadata = SimulationLifecycleScheduler._roll_forward_binding_metadata(
        source_release=source_release,
        source_binding=source_binding,
        new_release=successor_release,
        trade_date=TRADE_DATE,
        authoritative_manifest_sha256=authoritative_manifest_sha256,
    )
    if binding_metadata_override is not None:
        binding_metadata = dict(binding_metadata_override)
    else:
        binding_metadata = dict(generated_binding_metadata)
        binding_metadata.update(binding_metadata_patch or {})
    successor_binding = release_service.create_binding(
        strategy_id=source_binding.strategy_id,
        release=successor_release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=float(source_binding.capital_allocation),
        broker_account_id=source_binding.broker_account_id,
        strategy_name=source_binding.strategy_name,
        approval_state=source_binding.approval_state,
        binding_metadata=binding_metadata,
        effective_from=TRADE_DATE,
        effective_to=TRADE_DATE,
        created_by="simulation_lifecycle_scheduler.localsim_roll_forward",
        created_reason="unit-test LocalSIM manifest successor binding",
    )
    return successor_release, successor_binding, repository


def _make_localsim_portfolio(
    *,
    source_release,
    manifest: StrategyPackageManifest,
) -> PaperPortfolio:
    return PaperPortfolio(
        portfolio_id="strat1",
        portfolio_name="LocalSIM manifest successor test portfolio",
        package_id=source_release.package_id,
        manifest_sha256=manifest.manifest_sha256 or "",
        frozen_manifest=manifest,
        initial_cash=1_000_000,
        start_date=TRADE_DATE,
        data_source=MinuteDataSource.DB_HISTORICAL,
        execution_policy={
            "validated_execution_policy_id": source_release.execution_policy_version_id,
            "policy_sha256": source_release.execution_policy_sha256,
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )


class _ImmutableLocalSimFact(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fact_id: str
    payload: object


def test_local_sim_fact_payload_normalizes_mappingproxy_before_json_serialization() -> None:
    fact = _ImmutableLocalSimFact(
        fact_id="fact_mappingproxy",
        payload=MappingProxyType(
            {
                "nested": MappingProxyType({"trade_date": date(2026, 7, 20)}),
                "values": (Decimal("1.25"),),
            }
        ),
    )

    payload = SimulationLifecycleScheduler._local_sim_fact_payload(
        fact,
        fact_type="unit_mappingproxy",
    )

    assert payload == {
        "fact_id": "fact_mappingproxy",
        "payload": {
            "nested": {"trade_date": "2026-07-20"},
            "values": ["1.25"],
        },
    }


def test_local_sim_fact_payload_rejects_non_string_key_collision_instead_of_overwriting() -> None:
    fact = _ImmutableLocalSimFact(
        fact_id="fact_mapping_key_collision",
        payload=MappingProxyType({1: "integer-key", "1": "string-key"}),
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._local_sim_fact_payload(
            fact,
            fact_type="unit_mapping_key_collision",
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_FACT_JSON_KEY_INVALID"
    assert exc_info.value.context["key_type"] == "int"


@pytest.mark.parametrize("invalid_value", [float("nan"), float("inf"), Decimal("NaN")])
def test_local_sim_fact_payload_rejects_non_finite_numbers(invalid_value: object) -> None:
    fact = _ImmutableLocalSimFact(
        fact_id="fact_non_finite_number",
        payload=MappingProxyType({"invalid_value": invalid_value}),
    )

    with pytest.raises(RuntimeConfigInvalidError) as exc_info:
        SimulationLifecycleScheduler._local_sim_fact_payload(
            fact,
            fact_type="unit_non_finite_number",
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_FACT_JSON_NUMBER_INVALID"


def test_local_sim_snapshot_rejects_cross_plan_facts_instead_of_filtering_them() -> None:
    execution = SimpleNamespace(
        run=SimpleNamespace(run_id="run_snapshot_identity_conflict"),
        execution_plan=SimpleNamespace(
            plan_id="plan_snapshot_identity_conflict",
            intents=(SimpleNamespace(intent_id="intent_expected"),),
        ),
    )
    with pytest.raises(DataUnavailableError) as exc_info:
        SimulationLifecycleScheduler._filter_local_sim_snapshot_by_plan(
            execution=execution,
            orders=(SimpleNamespace(order_id="order_other", intent_id="intent_other"),),
            fills=(),
            events=(),
            cash_entries=(),
        )
    assert exc_info.value.context["reason_code"] == "LOCALSIM_SNAPSHOT_PLAN_IDENTITY_CONFLICT"


class _CapturingDailyTradingContextProvider:
    def __init__(self) -> None:
        self.kwargs: dict[str, Any] = {}

    def load(self, **kwargs: Any) -> SimpleNamespace:
        self.kwargs = dict(kwargs)
        return SimpleNamespace(context_id="dtc_test")

    @staticmethod
    def to_pre_trade_statuses(context: SimpleNamespace) -> dict[str, dict[str, Any]]:
        return {"context": {"context_id": context.context_id}}


def test_daily_context_wires_tdx_pre_close_authority_for_localsim() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    daily_provider = _CapturingDailyTradingContextProvider()

    def quote_fetcher(symbols: list[str]) -> dict[str, dict[str, Any]]:
        return {symbol: {} for symbol in symbols}

    provider = ProductionSimulationRunContextProvider(
        daily_trading_context_provider=daily_provider,
        localsim_daily_pre_close_quote_fetcher=quote_fetcher,
    )

    result = provider.load_daily_trading_context(
        symbols=["000001.SZ"],
        trade_date=TRADE_DATE,
        binding=SimpleNamespace(
            broker_backend=SimulationBrokerBackend.LOCAL_SIM,
            binding_id="bind-local",
            binding_hash="binding-hash",
        ),
        runtime_release=SimpleNamespace(
            package_id="pkg-local",
            manifest_sha256="manifest-hash",
            release_id="release-local",
            release_hash="release-hash",
        ),
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
    )

    assert result == {"context": {"context_id": "dtc_test"}}
    assert daily_provider.kwargs["pre_close_quote_fetcher"] is quote_fetcher
    assert daily_provider.kwargs["pre_close_quote_source"] == "TDX_REALTIME.batch_quote.pre_close"


def test_daily_context_wires_b0_pre_close_authority_for_miniqmt() -> None:
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    daily_provider = _CapturingDailyTradingContextProvider()
    qmt_client = SimpleNamespace(query_quote=lambda symbol: {"symbol": symbol})
    qmt_factory_calls: list[bool] = []

    def qmt_client_factory() -> SimpleNamespace:
        qmt_factory_calls.append(True)
        return qmt_client

    provider = ProductionSimulationRunContextProvider(
        daily_trading_context_provider=daily_provider,
        qmt_client_factory=qmt_client_factory,
    )
    binding = SimpleNamespace(
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        binding_id="bind-qmt",
        binding_hash="binding-hash",
        strategy_id="strategy-qmt",
        broker_account_id="account-qmt",
    )

    provider.load_daily_trading_context(
        symbols=["000001.SZ"],
        trade_date=TRADE_DATE,
        binding=binding,
        runtime_release=SimpleNamespace(
            package_id="pkg-qmt",
            manifest_sha256="manifest-hash",
            release_id="release-qmt",
            release_hash="release-hash",
        ),
        as_of_time=datetime(2026, 8, 21, 9, 10),
        calendar_service_snapshot={"is_trading_day": True},
    )

    quote_fetcher = daily_provider.kwargs["pre_close_quote_fetcher"]
    assert qmt_factory_calls == []
    assert quote_fetcher(["000001.SZ"]) == {"000001.SZ": {"symbol": "000001.SZ"}}
    assert qmt_factory_calls == [True]
    assert daily_provider.kwargs["pre_close_quote_source"] == "MINIQMT_REALTIME.broker_quote.pre_close"
