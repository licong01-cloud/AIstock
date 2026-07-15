from __future__ import annotations

import threading
import time as time_module
from copy import deepcopy
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime, time as wall_time, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import psycopg2
import pytest

import backend.services.simulation_runtime.bridges as simulation_bridges

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
    SimulationLifecycleScheduler,
    SimulationRunContext,
    SimulationRuntimeOpsService,
    StaticSimulationRunContextProvider,
    SimulationSchedulerBindingResult,
    SimulationSchedulerRunOnceResult,
    StrategyPackageSelectionResult,
    StrategyPackageSelectionService,
    StrategyRuntimeReleaseService,
)
from backend.services.simulation_runtime.lifecycle import (
    MINIQMT_SUBMIT_OUTSIDE_TRADING_WINDOW,
    compute_schedule_windows,
)
from backend.services.simulation_runtime.models import (
    LocalSimEconomicReceiptV1,
    LocalSimMarketMarkProvenance,
    LocalSimMarketMarkV1,
    LocalSimProjectionOutboxV1,
    LocalSimProjectionReceiptV1,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.repository import (
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
from backend.services.trading_core.errors import (
    BrokerUnavailableError,
    BrokerRejectedError,
    DataUnavailableError,
    InvalidStateTransitionError,
    LiveApprovalRequiredError,
    RuntimeConfigInvalidError,
)
from backend.services.trading_core.models import MinuteBar, OrderIntent, OrderSide, OrderType, PositionLot
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
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
    execution_policy_sha256 = canonical_json_sha256(execution_policy_json)
    release = service.create_release(
        package_id="pkg_scheduler",
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
            MINIQMT_B0_QUOTE_CONTROL
            if broker_backend is SimulationBrokerBackend.MINIQMT_SIM
            else None
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
                "runtime_profile": {
                    "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                }
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
                "runtime_profile": {
                    "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                }
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
    return {
        "policy_id": "exec_policy_twap",
        "policy_sha256": "exec_policy_hash_twap",
        "policy_json": {
            "algo_code": "TWAP",
            "algo_config": {
                "allow_partial_fill": True,
                "split_count": 1,
            },
        },
    }


def _local_sim_context_with_real_broker(
    *,
    portfolio_id: str,
    release: Any,
    cash: float = 100_000,
    positions: dict[str, PositionLot] | None = None,
    paper_repository: InMemoryPaperTradingV2Repository | None = None,
) -> SimulationRunContext:
    manifest = _score_weighted_manifest(release)
    current_positions = dict(positions or {})
    broker = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=cash,
        initial_available_cash=cash,
        data_source=MinuteDataSource.DB_HISTORICAL,
        manifest=manifest,
        package_id=release.package_id,
        market_data_provider=FakeLocalSimMarketDataProvider(),
        execution_policy=_local_sim_execution_policy(),
        initial_positions=current_positions,
    )
    return SimulationRunContext(
        portfolio_id=portfolio_id,
        current_positions=current_positions,
        current_prices={
            symbol: 10.0
            for symbol in {"000001.SZ", "688001.SH", *current_positions}
        },
        top_k=1,
        execution_policy_payload=_local_sim_execution_policy(),
        local_broker=broker,
        paper_repository=paper_repository,
        cash=cash,
        market_data_source=MinuteDataSource.DB_HISTORICAL.value,
    )


def _local_sim_realtime_context_with_real_broker(
    *, portfolio_id: str, release: Any, paper_repository: InMemoryPaperTradingV2Repository,
    cash: float, positions: dict[str, PositionLot],
) -> SimulationRunContext:
    policy = {
        "policy_id": "exec_policy_twap_streaming",
        "policy_sha256": "exec_policy_hash_twap_streaming",
        "policy_json": {"algo_code": "TWAP", "algo_config": {"allow_partial_fill": True, "split_count": 6}},
    }
    broker = LocalSimBackend(
        portfolio_id=portfolio_id,
        initial_cash=100_000,
        initial_available_cash=cash,
        data_source=MinuteDataSource.TDX_REALTIME,
        manifest=_score_weighted_manifest(release),
        package_id=release.package_id,
        market_data_provider=FakeLocalSimMarketDataProvider(),
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
            list(positions)
            if positions is not None
            else [{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
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
        admitted = {
            symbol
            for consumer_symbols in self._symbols_by_consumer.values()
            for symbol in consumer_symbols
        }
        self.normalized_store.replace_admitted(admitted)
        for consumer_id, symbols in self._symbols_by_consumer.items():
            sink = self._sinks.get(consumer_id)
            if sink is None:
                raise AssertionError(f"B0 test consumer lost observation sink: {consumer_id}")
            for symbol in symbols:
                observation = self._observations[symbol]
                self.normalized_store.accept(observation)
                sink(observation)

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
        admitted = {
            symbol
            for consumer_symbols in self._symbols_by_consumer.values()
            for symbol in consumer_symbols
        }
        self.normalized_store.replace_admitted(admitted)
        for symbol in exact_symbols:
            observation = self._observations[symbol]
            self.normalized_store.accept(observation)
            sink(observation)

    def release_consumer(self, *, consumer_id: str) -> None:
        self._symbols_by_consumer.pop(consumer_id, None)
        admitted = {
            symbol
            for consumer_symbols in self._symbols_by_consumer.values()
            for symbol in consumer_symbols
        }
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

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        self.supervisor.begin_lifecycle_epoch()
        return self.health()

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

    def assert_accepts_new_assignments(self) -> None:
        return None

    def get(self, runtime_id: str) -> _PendingOnlyB0Controller | None:
        return self.controllers.get(runtime_id)

    def create(self, *, runtime: Any, assignments: Any, symbols: Any) -> _PendingOnlyB0Controller:
        del assignments, symbols
        controller = _PendingOnlyB0Controller(runtime.config.runtime_id)
        self.controllers[runtime.config.runtime_id] = controller
        return controller


class _PendingOnlyB0Activation:
    def __init__(self) -> None:
        self.controller_factory = _PendingOnlyB0ControllerFactory()
        self.quote_context_adapter = None

    def begin_lifecycle_epoch(self) -> dict[str, object]:
        return {"status": "READY"}

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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=effective_candidates),
        miniqmt_quote_ingress_activation=(
            _RealB0TestActivation() if real_callback else _PendingOnlyB0Activation()
        ),  # type: ignore[arg-type]
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 200, "can_sell": 200}]
    )
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


def _runtime_store_contains_shadow_marker(path) -> bool:
    paths = [path, path.with_suffix(".jsonl")]
    return any(
        candidate.exists() and "SHADOW_RECONCILIATION" in candidate.read_text(encoding="utf-8")
        for candidate in paths
    )


def test_scheduler_miniqmt_sim_ignores_retired_env_and_always_routes_to_event_loop(
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


def test_scheduler_miniqmt_direct_sim_event_loop_routes_to_a_without_shadow_gate(
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


def test_scheduler_miniqmt_event_loop_scope_routes_to_a_runtime_with_broker_quote(
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


def test_miniqmt_event_loop_bridge_rejects_live_before_building_submit_payload() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    broker = FakeManagedOrderBroker()
    bridge = simulation_bridges.MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=broker)  # type: ignore[arg-type]
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
                    managed_order_service=QmtManagedOrderService(repository=qmt_repo, broker=broker),  # type: ignore[arg-type]
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

    with pytest.raises(LiveApprovalRequiredError) as exc_info:
        bridge.submit_event_loop_plan(
            plan=planned.results[0].execution_plan,
            binding=qmt_binding,
            mode="LIVE",
            price_by_symbol={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        )

    assert exc_info.value.context["reason_code"] == "MINIQMT_EVENT_LOOP_LIVE_FORBIDDEN"
    assert broker.place_order_payloads == []


def test_miniqmt_event_loop_bridge_requires_real_qmt_callback_gateway() -> None:
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
    bridge = simulation_bridges.MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(
            repository=qmt_repo,
            broker=MissingPlaceOrderQmtClient(),
        )  # type: ignore[arg-type]
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

    with pytest.raises(BrokerUnavailableError) as exc_info:
        bridge.submit_event_loop_plan(
            plan=planned.results[0].execution_plan,
            binding=qmt_binding,
            mode="SIM",
            price_by_symbol={"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0},
        )

    context = exc_info.value.context
    assert context["reason_code"] == "MINIQMT_EVENT_LOOP_REAL_CALLBACKS_MISSING"
    assert context["stage"] == "MINIQMT_EVENT_LOOP_CALLBACK_GATEWAY_UNAVAILABLE"
    assert context["missing_methods"] == ["place_order"]
    assert context["broker_called"] is False
    assert context["submitted_intents"] == 0
    assert context["failed_intents"] == len(planned.results[0].execution_plan.intents)


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
        self, *, symbol: str, trade_date: date, source: MinuteDataSource,
        until_time: datetime, require_day_features: bool = False,
    ) -> MinuteExecutionMarketInput:
        source_input = self.load_symbol_input(
            symbol=symbol,
            trade_date=trade_date,
            source=source,
            min_bars=6,
            require_day_features=require_day_features,
        )
        return replace(
            source_input,
            minute_bars=[
                bar for bar in source_input.minute_bars if bar.bar_time <= until_time.replace(tzinfo=None)
            ],
        )


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
    assert {plan.selection_evidence_hash for plan in plans if plan is not None} == {
        plans[0].selection_evidence_hash
    }
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
    assert by_binding_id[bad_binding.binding_id].error["context"]["reason_code"] == "strategy_package_model_code_missing"
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
            self.calls: list[str] = []

        def run_selection(self, **kwargs):
            package_id = kwargs["package_ids"][0]
            self.calls.append(package_id)
            if package_id == slow_release.package_id:
                self.started.set()
                self.release.wait(timeout=5.0)
            runtime_release = kwargs.get("runtime_release") or {
                slow_release.package_id: slow_release,
                fast_release.package_id: fast_release,
            }[package_id]
            candidates = _candidate_rows()
            evidence = _evidence(runtime_release, candidates=candidates, target_trade_date=kwargs.get("trade_date") or TRADE_DATE)
            return StrategyPackageSelectionResult(
                runtime_config={
                    "runtime_profile": {
                        "selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}
                    }
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
        assert first.failed_count == 1
        assert first.planned_count == 1
        assert by_binding_id[slow_binding.binding_id].error["context"]["reason_code"] == "SIMULATION_SELECTION_INFERENCE_IN_PROGRESS"
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
        assert second_by_binding_id[slow_binding.binding_id].error["context"]["reason_code"] == "SIMULATION_SELECTION_INFERENCE_TIMEOUT"
        assert second_by_binding_id[slow_binding.binding_id].error["context"]["failure_stage"] == "selection_inference"
        assert second_by_binding_id[fast_binding.binding_id].status == "REUSED_EXISTING_PLAN"
        assert selection.calls.count(slow_release.package_id) == 1
        assert inflight_status["in_flight_count"] == 1
        assert inflight_status["in_flight"][0]["timed_out"] is True
    finally:
        selection.release.set()
        scheduler.shutdown_selection_inference(wait=True)


def test_scheduler_miniqmt_submit_timeout_skips_binding_and_continues_later_binding(
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


def test_scheduler_miniqmt_reconcile_timeout_skips_binding_and_continues_later_binding(
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
        assert failed_run.run_payload_json["miniqmt_reconcile_timeout"]["stage"] == "MINIQMT_RECONCILE_AFTER_SUBMIT_TIMEOUT"
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
    assert new_release.base_release_id == release.release_id
    assert new_release.effective_from == next_trade_day
    assert new_release.effective_to == next_trade_day
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
    assert all(
        item.lifecycle_diagnostic["strategy_package_revalidation_performed"] is False
        for item in result.results
    )
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
    assert new_release.release_config_json["metadata"]["purpose"] == "miniqmt_unattended_daily_roll_forward"
    assert rerun.reused_count == 1
    assert rerun.results[0].run.binding_id == new_binding.binding_id


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
    assert all(item.run.binding_id not in {local_binding_a.binding_id, local_binding_b.binding_id} for item in result.results)


def test_scheduler_roll_forward_keeps_active_binding_when_limit_is_full() -> None:
    _, local_binding, _, repo = _release_and_bindings()
    assert local_binding is not None
    next_trade_day = TRADE_DATE + timedelta(days=1)
    active_binding = local_binding.model_copy(update={"effective_from": next_trade_day, "effective_to": next_trade_day})
    repo.bindings[active_binding.binding_id] = active_binding
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(repo.get_strategy_runtime_release(active_binding.release_id), candidates=_candidate_rows()),
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
    assert runs[0].run_payload_json["broker_called"] is True
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


def test_scheduler_localsim_cash_fit_runs_sells_before_buys_and_skips_cash_residual() -> None:
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
    submitted = result.results[0].execution_plan.intents
    assert result.results[0].status == "LOCALSIM_CAPACITY_RESIDUAL_TERMINAL"
    assert latest_run.status == SimulationDailyRunStatus.FAILED_TERMINAL
    assert latest_run.run_payload_json["last_stage"] == "FAILED_TERMINAL"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_CAPACITY_RESIDUAL"
    assert payload["status"] == "CAPACITY_RESIDUAL_SKIPPED"
    assert payload["sell_intent_count"] == 1
    assert payload["submitted_buy_count"] == 1
    assert payload["skipped_buy_count"] == 1
    assert [intent.side for intent in submitted] == [OrderSide.SELL, OrderSide.BUY]
    assert submitted[0].symbol == "000003.SZ"
    assert paper_repo.list_fills_for_run(latest_run.run_id)
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
                quantity=1200,
                available_quantity=1200,
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
    assert latest_run.run_payload_json["local_sim_cash_fit"]["status"] == "CAPACITY_RESIDUAL_SKIPPED"
    assert latest_run.run_payload_json["local_sim_persistence"]["status"] == "PERSISTED_WITH_CAPACITY_RESIDUAL"
    assert len(fake_selection.calls) == 2
    assert paper_repo.list_fills_for_run(latest_run.run_id)


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
    )

    latest_run = repo.get_simulation_daily_run(failed_run.run_id)
    assert recovered.results[0].status == "SUBMITTED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.execution_plan_id == plan.plan_id
    assert "submit_failure" not in latest_run.run_payload_json
    assert "local_sim_retry_diagnostics" not in latest_run.run_payload_json
    assert paper_repo.list_fills_for_run(latest_run.run_id)


def test_scheduler_submits_miniqmt_fake_broker_batch_and_reuses_after_restart() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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
    assert {
        position["symbol"]
        for position in latest_run.run_payload_json["strategy_performance"]["positions"]
    } >= {"000003.SZ"}
    assert submitted.results[0].run.run_payload_json["qmt_batch_status"] == "SUCCEEDED"
    assert [call["strategy_name"] for call in broker.place_order_payloads] == [
        qmt_binding.strategy_name,
        qmt_binding.strategy_name,
    ]
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def test_scheduler_miniqmt_restart_syncs_before_submit_and_reconciles_after_submit() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
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
    assert repo.get_simulation_daily_run(submitted.results[0].run.run_id).run_payload_json["strategy_performance"]["nav"] > 0
    assert submitted.results[0].run.run_payload_json["sync_before_submit"]["orders_seen"] == 0
    assert submitted.results[0].run.run_payload_json["reconcile_after_submit"]["broker_quantities"] == {
        "000003.SZ": 77
    }
    assert restarted.reused_count == 1
    assert len(broker.place_order_payloads) == 2


def test_scheduler_miniqmt_preflight_failure_stays_retryable_and_can_resubmit() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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
    assert failed_run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"] == "SKIPPED_INSUFFICIENT_CAPITAL"
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
    assert failed_run.run_payload_json["simulation_alerts"][0]["reason_code"] == "MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL"
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


def test_scheduler_keeps_miniqmt_capacity_residual_pending_when_open_orders_remain() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
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


def test_scheduler_post_close_terminalizes_miniqmt_capacity_residual_without_fake_success() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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


def test_scheduler_post_close_terminalizes_miniqmt_open_orders_as_failed_terminal() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    accepted_intent = next(
        intent for intent in qmt_repo.list_order_intents_by_batch(run.run_payload_json["qmt_batch_id"])
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


def test_scheduler_post_close_reconciles_fresh_broker_before_terminal_status() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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


def test_scheduler_post_close_reconcile_failure_is_loud() -> None:
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
    snapshot_client = FailingQmtSnapshotClient(
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


def test_scheduler_post_close_terminalizes_dependent_buy_residual_as_retryable_failure() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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

    first = scheduler.run_once(
        trade_date=TRADE_DATE,
        data_source="DB_HISTORICAL",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        submit=True,
    )
    run = repo.get_simulation_daily_run(first.results[0].run.run_id)
    assert run.run_payload_json["qmt_batch_result"]["results"][1]["preflight"]["primary_error_code"] == "SELL_PROCEEDS_REQUIRED"
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


def test_scheduler_rebuilds_side_effect_free_miniqmt_failed_plan_with_fresh_context() -> None:
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


def test_scheduler_rejects_side_effect_free_failed_retry_outside_shared_window_without_broker_call() -> None:
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


def test_scheduler_keeps_deferred_miniqmt_buy_blocked_until_explicit_reconciliation_without_duplicate_sell() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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
    assert gate["schedule_windows"] == list(compute_schedule_windows(trade_date=TRADE_DATE, as_of_time=datetime(2026, 5, 21, 16, 33)))
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


def test_scheduler_allows_miniqmt_submit_inside_shared_window() -> None:
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


def test_scheduler_rejects_deferred_dependent_buy_replay_after_close_without_duplicate_buy() -> None:
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


def test_scheduler_polls_succeeded_miniqmt_run_for_late_broker_fill_sync() -> None:
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
    assert latest_run.run_payload_json["reconcile_after_submit"]["run"]["summary_json"]["sync_summary"]["trades_existing"] == 1
    assert [(lot.open_trade_id, lot.remaining_quantity) for lot in lots] == [("1010000032502320", 200)]
    assert len(broker.place_order_payloads) == 1


def test_scheduler_recovers_called_miniqmt_retryable_run_by_reconcile_only() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
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


def test_scheduler_recovers_miniqmt_retryable_run_with_order_ledger_evidence_by_reconcile_only() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
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
    assert recovered_run.run_payload_json["reconcile_after_submit"]["side_effect_evidence"]["broker_side_effect_count"] > 0
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


def test_scheduler_persists_b0_context_prepare_failure_before_broker_callable() -> None:
    release, _, qmt_binding, repo = _release_and_bindings(qmt_only=True)

    class _FailingActivation:
        controller_factory = None
        quote_context_adapter = None

        def begin_lifecycle_epoch(self) -> None:
            return None

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
    )
    good_run = SimpleNamespace(
        run_id="stale-good",
        trade_date=TRADE_DATE,
        strategy_id="strategy-good",
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
    )
    monkeypatch.setattr(scheduler, "_is_post_close_reconcile_time", lambda **_kwargs: True)
    monkeypatch.setattr(
        scheduler.repository,
        "list_simulation_daily_runs",
        lambda **_kwargs: [bad_run, good_run],
    )

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


def test_scheduler_cross_day_terminalizes_side_effect_miniqmt_open_order_with_fresh_broker_reconcile() -> None:
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


def test_scheduler_cross_day_terminalizes_side_effect_miniqmt_succeeded_batch_after_fresh_reconcile() -> None:
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


def test_scheduler_miniqmt_account_level_reconciliation_warning_does_not_fail_current_slot() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
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


def test_scheduler_miniqmt_reconcile_warning_marks_run_retryable() -> None:
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 1, "can_sell": 1}]
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

    assert SimulationLifecycleScheduler._localsim_realtime_quote_required(
        binding=local_binding,
        trade_date=TRADE_DATE,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 10, 0),
    ) is True
    assert SimulationLifecycleScheduler._localsim_realtime_quote_required(
        binding=local_binding,
        trade_date=TRADE_DATE,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 12, 0),
    ) is False
    assert SimulationLifecycleScheduler._localsim_realtime_quote_required(
        binding=local_binding,
        trade_date=TRADE_DATE,
        submit=True,
        as_of_time=datetime(2026, 5, 21, 14, 58),
    ) is False


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
    assert provider.tradability_quote_requirements
    assert all(requirement is False for requirement in provider.tradability_quote_requirements)
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


def test_localsim_submit_fetches_exact_plan_symbols_only_after_selection() -> None:
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
    assert events[2] == (
        "quote",
        (True, ("000001.SZ", "000003.SZ", "688001.SH")),
    )


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


def test_background_scheduler_runs_post_close_reconcile_without_submit_by_default(
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
            qmt_client=FakeQmtSnapshotClient(
                positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
            ),
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


def test_scheduler_marks_pre_trade_blocked_holding_without_broker_submit() -> None:
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
        submit=True,
    )

    latest_run = repo.get_simulation_daily_run(result.results[0].run.run_id)
    assert result.results[0].status == "PRE_TRADE_BLOCKED"
    assert latest_run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest_run.run_payload_json["broker_called"] is False
    assert latest_run.run_payload_json["no_rebalance_required"] is False
    assert latest_run.run_payload_json["pre_trade_blocked_order_generation"]["blocked_symbols"] == ["688689.SH"]
    assert result.results[0].execution_plan.intents == []
    assert result.results[0].execution_plan.trading_rule_decisions[0].reason_code == "NO_TRADABLE_REALTIME_QUOTE"
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
    assert submitted_by_strategy[local_binding_a.strategy_id].run.run_payload_json["strategy_performance"]["initial_capital"] == 100000.0
    assert submitted_by_strategy[local_binding_b.strategy_id].run.run_payload_json["strategy_performance"]["positions"][0]["symbol"] == "000001.SZ"
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
    replayed, replay_receipt, replay_outbox, replay_created = _merge_local_sim_economic_event(
        payload=payload, **event
    )
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
        _merge_local_sim_economic_event(
            payload={LOCAL_SIM_ECONOMIC_GENERATION_PAYLOAD_KEY: True}, **event
        )
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
    repository.get_simulation_daily_run = lambda run_id: SimpleNamespace(
        run_payload_json=deepcopy(connection.payload), status=connection.status
    )
    repository.list_local_sim_execution_states = lambda run_id: []
    return repository


def test_postgres_simulation_repository_stages_economic_and_projection_receipts_on_owner_connection() -> None:
    connection = _AtomicSimulationConnection()
    repository = _atomic_simulation_repository(connection)
    economic_facts = {"schema_version": "test_economic_v1", "state_hashes": {}}
    receipt, outbox, created = repository.stage_local_sim_economic_commit(
        connection=connection, run_id="run_atomic", binding_id="binding_atomic",
        trade_date=TRADE_DATE, plan_id="plan_atomic", states=(), expected_versions={},
        economic_facts=economic_facts,
        projection_payload={"schema_version": "test_projection_payload_v1"},
        status=SimulationDailyRunStatus.INTRADAY_RUNNING,
        payload_patch={"last_stage": "LOCAL_SIM_ECONOMIC_COMMITTED"},
    )
    assert created is True
    repository.readback_local_sim_economic_commit(
        run_id="run_atomic", receipt=receipt, outbox=outbox
    )

    projection_receipt = repository.stage_local_sim_projection_commit(
        connection=connection, run_id="run_atomic", outbox_id=outbox.outbox_id,
        generation=outbox.generation, final_status=SimulationDailyRunStatus.SUCCEEDED,
        projection_result={"schema_version": "test_projection_result_v1"},
        payload_patch={"local_sim_projection_generation": {"generation": outbox.generation}},
    )
    repository.readback_local_sim_projection_commit(
        run_id="run_atomic", receipt=projection_receipt
    )
    assert connection.payload["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert connection.payload["local_sim_projection_generation"]["projection_receipt_id"] == projection_receipt.projection_receipt_id


def test_postgres_simulation_repository_persists_projection_retry_and_readback_recovery_cas() -> None:
    connection = _AtomicSimulationConnection()
    repository = _atomic_simulation_repository(connection)
    _, pending, _ = repository.stage_local_sim_economic_commit(
        connection=connection, run_id="run_retry", binding_id="binding_retry",
        trade_date=TRADE_DATE, plan_id="plan_retry", states=(), expected_versions={},
        economic_facts={"schema_version": "test_economic_v1", "state_hashes": {}},
        projection_payload={"schema_version": "test_projection_payload_v1"},
        status=SimulationDailyRunStatus.INTRADAY_RUNNING, payload_patch={},
    )
    repository.mark_local_sim_projection_retryable(
        run_id="run_retry", outbox_id=pending.outbox_id,
        error={"reason_code": "TEST_RETRY"},
    )
    assert connection.payload["local_sim_projection_outbox_v1"]["status"] == "PROJECTION_RETRYABLE"
    projected = repository.stage_local_sim_projection_commit(
        connection=connection, run_id="run_retry", outbox_id=pending.outbox_id,
        generation=pending.generation, final_status=SimulationDailyRunStatus.SUCCEEDED,
        projection_result={"schema_version": "test_projection_result_v1"},
        payload_patch={"local_sim_projection_generation": {"generation": pending.generation}},
    )
    repository.mark_local_sim_projection_readback_retryable(
        run_id="run_retry", outbox_id=pending.outbox_id,
        error={"reason_code": "TEST_READBACK"},
    )
    assert "local_sim_projection_readback_failure" in connection.payload
    repository.clear_local_sim_projection_readback_failure(
        run_id="run_retry", outbox_id=pending.outbox_id,
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
    positions = {"000001.SZ": PositionLot(portfolio_id="p", symbol="000001.SZ", quantity=100, available_quantity=100, avg_cost=9.5, trade_date=TRADE_DATE - timedelta(days=1))}
    context = _local_sim_context_with_real_broker(portfolio_id="p", release=release, positions=positions)
    scheduler = SimulationLifecycleScheduler(repository=repo, selection_service=FakeSelectionService(release, candidates=_candidate_rows()), context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}))
    planned = scheduler.run_once(trade_date=TRADE_DATE, data_source=MinuteDataSource.DB_HISTORICAL.value, broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False, as_of_time=datetime(2026, 5, 21, 9, 22))
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
        pre_trade_tradability={
            position.symbol: {"suspend_status": {"is_suspended": True}}
        },
    )[position.symbol]
    assert suspended.price == 10.0
    assert suspended.as_of_time == datetime(2026, 5, 20, 15, 0)
    assert suspended.source == "test.previous_close"
    assert suspended.provenance == LocalSimMarketMarkProvenance.SUSPENDED_PREV_CLOSE


def test_scheduler_localsim_economic_transaction_rolls_back_both_repositories() -> None:
    class FailingPaperRepository(InMemoryPaperTradingV2Repository):
        def save_fill(self, run_id, fill, **kwargs):
            raise RuntimeError("forced LocalSIM economic write failure")
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = FailingPaperRepository()
    context = _local_sim_realtime_context_with_real_broker(portfolio_id="p_rollback", release=release, paper_repository=paper_repo, cash=100_000, positions={})
    scheduler = SimulationLifecycleScheduler(repository=repo, selection_service=FakeSelectionService(release, candidates=_candidate_rows()), context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}))
    scheduler.run_once(trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value, broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False, as_of_time=datetime(2026, 5, 21, 9, 22))
    failed = scheduler.run_once(trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value, broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True, as_of_time=datetime(2026, 5, 21, 9, 32))
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
                raise InvalidStateTransitionError("forced projection readback failure", context={"reason_code": "LOCALSIM_PROJECTION_READBACK_FAILED"})
            return super().readback_local_sim_projection(**kwargs)
    release, binding, _, repo = _release_and_bindings(qmt_only=False)
    paper_repo = OneShotReadbackFailureRepository()
    context = _local_sim_realtime_context_with_real_broker(portfolio_id="p_readback", release=release, paper_repository=paper_repo, cash=100_000, positions={})
    scheduler = SimulationLifecycleScheduler(repository=repo, selection_service=FakeSelectionService(release, candidates=_candidate_rows()), context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}))
    planned = scheduler.run_once(trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value, broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False, as_of_time=datetime(2026, 5, 21, 9, 22))
    failed = scheduler.run_once(trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value, broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True, as_of_time=datetime(2026, 5, 21, 9, 32))
    run_id = planned.results[0].run.run_id
    failed_run = repo.get_simulation_daily_run(run_id)
    event_count = len(paper_repo.run_events)
    assert failed.results[0].status == "FAILED_RETRYABLE"
    assert failed_run.run_payload_json["local_sim_projection_outbox_v1"]["status"] == "PROJECTED"
    assert failed_run.run_payload_json["local_sim_projection_readback_failure"]
    assert failed_run.run_payload_json["local_sim_projection_readback_failure"]["attempt_count"] == 1
    scheduler._replay_pending_local_sim_projection(run_id=run_id, paper_repository=paper_repo)
    recovered = repo.get_simulation_daily_run(run_id)
    assert recovered.status == SimulationDailyRunStatus.INTRADAY_RUNNING
    assert "local_sim_projection_readback_failure" not in recovered.run_payload_json
    assert "submit_failure" not in recovered.run_payload_json
    assert len(paper_repo.run_events) == event_count


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
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={binding.binding_id: context}
        ),
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
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={binding.binding_id: context}
        ),
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
        portfolio_id="p_outbox_tamper", release=release,
        paper_repository=paper_repo, cash=100_000, positions={},
    )
    scheduler = SimulationLifecycleScheduler(
        repository=repo,
        selection_service=FakeSelectionService(release, candidates=_candidate_rows()),
        context_provider=StaticSimulationRunContextProvider(by_binding_id={binding.binding_id: context}),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 32),
    )
    run_id = planned.results[0].run.run_id
    latest = repo.get_simulation_daily_run(run_id)
    outbox = deepcopy(latest.run_payload_json["local_sim_projection_outbox_v1"])
    outbox["projection_payload"]["economic_hash"] = "0" * 64
    repo.update_simulation_daily_run(
        run_id, payload_patch={"local_sim_projection_outbox_v1": outbox}
    )

    with pytest.raises(DataUnavailableError) as exc_info:
        scheduler._project_local_sim_outbox(run_id=run_id, paper_repository=paper_repo)

    assert exc_info.value.context["reason_code"] == "LOCALSIM_PROJECTION_OUTBOX_SCHEMA_INVALID"


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
        context_provider=StaticSimulationRunContextProvider(
            by_binding_id={local_binding.binding_id: first_context}
        ),
    )
    planned = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=False,
        as_of_time=datetime(2026, 5, 21, 9, 22),
    )
    first = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True,
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
        portfolio_id=portfolio_id, release=release, paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash), positions=first_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: replay_context}
    )
    replayed = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True,
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
        portfolio_id=portfolio_id, release=release, paper_repository=paper_repo,
        cash=float(first_broker.query_account().cash), positions=first_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: second_context}
    )
    second = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True,
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
        portfolio_id=portfolio_id, release=release, paper_repository=paper_repo,
        cash=float(second_broker.query_account().cash), positions=second_broker.query_positions(),
    )
    scheduler.context_provider = StaticSimulationRunContextProvider(
        by_binding_id={local_binding.binding_id: third_context}
    )
    third = scheduler.run_once(
        trade_date=TRADE_DATE, data_source=MinuteDataSource.TDX_REALTIME.value,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM, submit=True,
        as_of_time=datetime(2026, 5, 21, 9, 36),
    )
    third_states = repo.list_local_sim_execution_states(run_id)
    assert all(state.runtime_status.value == "FILLED" and state.remaining_quantity == 0 for state in third_states)
    assert third.results[0].run.status == SimulationDailyRunStatus.SUCCEEDED
    assert repo.get_simulation_daily_run(run_id).run_payload_json["local_sim_economic_generation"] == 3
    terminal_fill_ids = [row["fill_id"] for row in paper_repo.list_fills_for_run(run_id)]
    assert len(terminal_fill_ids) == len(set(terminal_fill_ids))


def test_scheduler_miniqmt_two_strategies_same_stock_keep_strategy_lots_and_merged_reconcile() -> None:
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
            portfolio_id="strat1", symbol="000001.XSHE", quantity=1000,
            available_quantity=1000, avg_cost=12.50, trade_date=date.today(),
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
            portfolio_id="strat1", symbol="000001.XSHE", quantity=1000,
            available_quantity=1000, avg_cost=12.50, trade_date=date.today(),
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

    result = ctx.managed_order_service.submit_batch([
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
    ])

    assert result.success is False
    assert result.results[0].broker_called is False
    assert result.results[0].preflight.broker_can_sell == 100
    assert result.results[0].preflight.primary_error.code in {"INSUFFICIENT_BROKER_CAN_SELL", "BATCH_INSUFFICIENT_BROKER_CAN_SELL"}
    assert broker.place_order_payloads == []


def test_production_context_provider_miniqmt_submit_disabled_fails_loud_without_preview_submit():
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
    snapshot_client = FakeQmtSnapshotClient(
        positions=[{"stock_code": "000003.SZ", "quantity": 77, "can_sell": 77}]
    )
    manifest = _score_weighted_manifest(release)
    provider = ProductionSimulationRunContextProvider(
        price_loader=lambda symbols, trade_date: {symbol: {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}[symbol] for symbol in symbols},
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


def test_production_context_provider_miniqmt_event_loop_submit_places_broker_orders_when_enabled(
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
        price_loader=lambda symbols, trade_date: {symbol: {"000001.SZ": 10.0, "000003.SZ": 8.0, "688001.SH": 20.0}[symbol] for symbol in symbols},
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
    assert latest_run.run_payload_json["qmt_batch_result"]["runtime_evidence"]["source"] == "simulation_runtime_event_loop_submit"


def test_scheduler_event_loop_no_child_dispatch_stays_pending_not_failed(
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


def test_scheduler_poll_does_not_synthesize_b0_children_from_broker_quote_cache(
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


def test_scheduler_automatically_recovers_exact_legacy_b0_context_failure_without_side_effects() -> None:
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


def test_scheduler_does_not_auto_recover_legacy_context_failure_with_runtime_evidence() -> None:
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


def test_scheduler_converts_no_side_effect_reconciling_after_runtime_only_cleanup_and_retries() -> None:
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
        "validation_evidence.manifest_identity.source_release_manifest_sha256"
        in exc_info.value.context["violations"]
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


def test_production_context_provider_rejects_stale_portfolio_policy_when_release_policy_is_vnpy_id_only() -> None:
    """LocalSim must not fall back to stale portfolio V25 when the runtime release points to vn.py."""
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

    with pytest.raises(RuntimeConfigInvalidError, match="snapshot is missing full policy_json") as exc_info:
        provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)

    assert exc_info.value.context["release_execution_policy_version_id"] == policy_id
    assert exc_info.value.context["portfolio_policy_algo_code"] == "V25_1_SMALL_CAP"
    assert "LocalSim-compatible execution policy" in exc_info.value.context["required_action"]


def test_production_context_provider_uses_runtime_release_policy_snapshot_over_portfolio_default() -> None:
    """Runtime release policy_json is authoritative when present."""
    from backend.services.simulation_runtime.scheduler import ProductionSimulationRunContextProvider

    release = _make_test_release(
        execution_policy_version_id="exec_policy_runtime_close",
        execution_policy_sha256="sha_runtime_close",
        execution_policy={
            "policy_version_id": "exec_policy_runtime_close",
            "policy_sha256": "sha_runtime_close",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
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
    market_data = FakeLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: FakePaperRepository(portfolio, positions={}, cash=1_000_000),
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
    assert ctx.execution_policy_payload == release.release_config_json["execution_policy"]
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


def test_production_context_provider_uses_portfolio_execution_policy_for_alpha_core_localsim_recovery():
    """Alpha-core LocalSim recovery must use the Paper v2 validated policy snapshot, not manifest.minute_execution_policy."""
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
            "validated_execution_policy_id": "exec_policy_close_price",
            "policy_sha256": "policy_sha256",
            "policy_json": {
                "algo_code": "CLOSE_PRICE",
                "algo_config": {"allow_partial_fill": True},
            },
        },
    )
    paper_repo = FakePaperRepository(portfolio, positions={}, cash=1_000_000)
    market_data = FakeLocalSimMarketDataProvider()
    provider = ProductionSimulationRunContextProvider(
        paper_repository_factory=lambda: paper_repo,
        price_loader=lambda symbols, trade_date: {symbol: 10.0 for symbol in symbols},
    )
    binding = _make_test_binding(release, broker_backend=SimulationBrokerBackend.LOCAL_SIM)

    ctx = provider.load_context(runtime_release=release, binding=binding, trade_date=TRADE_DATE)
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
                item
                for item in (date(2026, 5, 19), date(2026, 5, 20), TRADE_DATE)
                if start_date <= item <= end_date
            ]

    class StaleCutoffSelectionService:
        def __init__(self) -> None:
            self.resolver = StrategyPackageSelectionService(calendar_provider=RollingCalendar())

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
        raise AssertionError('expected DataUnavailableError')
    except DataUnavailableError as exc:
        assert 'requires an explicit run context provider' in str(exc)


def _make_test_release(
    *,
    execution_policy_version_id: str = "exec_policy_close_price",
    execution_policy_sha256: str = "policy_sha256",
    execution_policy: dict[str, Any] | None = None,
):
    from backend.services.simulation_runtime.models import StrategyRuntimeRelease
    policy_payload = execution_policy or {
        "policy_version_id": execution_policy_version_id,
        "policy_sha256": execution_policy_sha256,
    }
    return StrategyRuntimeRelease(
        package_id="pkg", manifest_sha256="aa",
        runtime_profile_id="rp", runtime_profile_version_id="rpv", runtime_profile_sha256="rps",
        daily_strategy_profile_version_id="dsp", execution_policy_version_id=execution_policy_version_id,
        execution_policy_sha256=execution_policy_sha256, tail_policy_version_id="tpv", tail_policy_sha256="tps",
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
        strategy_id="strat1", release_id=release.release_id, release_hash=release.release_hash or "",
        package_id=release.package_id, manifest_sha256=release.manifest_sha256,
        broker_backend=broker_backend, capital_allocation=1_000_000.0,
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
