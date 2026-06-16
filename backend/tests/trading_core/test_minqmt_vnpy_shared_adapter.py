from __future__ import annotations

from datetime import date, timezone
from decimal import Decimal
from typing import Any

import pytest

from backend.execution_algos.vnpy_style import VnpyAction, VnpyStyleConfigError
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.simulation_runtime import (
    ExecutionPathNotCanonicalError,
    ExecutionPlanCompiler,
    LocalSimExecutionBridge,
    MiniQMTExecutionBridge,
    RebalanceIntentService,
    SimulationBrokerBackend,
    TargetPositionService,
)
from backend.services.trading_core.miniqmt_vnpy_execution import (
    MiniQMTCancelResult,
    MiniQMTChildOrderHandle,
    MiniQMTChildOrderRequest,
    MiniQMTChildOrderSubmitResult,
    MiniQMTOrderStatus,
    UnifiedMiniQMTVnpyExecutionAdapter,
)
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderIntent, OrderSide, OrderType
from backend.tests.simulation_runtime.test_target_rebalance_shared import (
    FakeLocalSimBroker,
    _current_positions,
    _evidence,
    _release_binding_repo,
    _snapshot,
)

UTC = timezone.utc
TRADE_DATE = date(2026, 5, 21)


class RecordingChildSubmitter:
    def __init__(self, *, status: MiniQMTOrderStatus | None = None) -> None:
        self.requests: list[MiniQMTChildOrderRequest] = []
        self.cancelled: list[tuple[str, str]] = []
        self.status = status

    def submit_child(self, request: MiniQMTChildOrderRequest) -> MiniQMTChildOrderSubmitResult:
        self.requests.append(request)
        handle = MiniQMTChildOrderHandle(
            handle_id=f"handle_{len(self.requests)}",
            intent_id=request.child_intent.intent_id,
            native_order_id=f"native_{len(self.requests)}",
            native_context={"raw_submit": {"status": 50}},
        )
        status = self.status or MiniQMTOrderStatus(
            handle_id=handle.handle_id,
            state="pending",
            raw_status=50,
            status_msg="reported pending",
            raw={"order_status": 50, "status_msg": "reported pending"},
        )
        return MiniQMTChildOrderSubmitResult(
            handle=handle,
            status=status,
            native_context={"miniqmt_order_id": handle.native_order_id, "raw_submit": {"status": 50}},
        )

    def cancel_child(self, handle: MiniQMTChildOrderHandle, *, action: VnpyAction, reason: str) -> MiniQMTCancelResult:
        self.cancelled.append((handle.handle_id, reason or action.reason))
        return MiniQMTCancelResult(accepted=True, reason=reason, raw={"handle_id": handle.handle_id})

    def query_order(self, handle: MiniQMTChildOrderHandle) -> MiniQMTOrderStatus | None:
        return MiniQMTOrderStatus(handle_id=handle.handle_id, state="cancelled", raw_status="cancelled")

    def query_trades(self, handle: MiniQMTChildOrderHandle) -> list[dict[str, Any]]:  # noqa: ARG002
        return []


def _policy_context(algo_code: str, algo_config: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "validated_execution_policy_id": "execpol_shared_vnpy",
        "policy_sha256": "sha_shared_vnpy",
        "policy_json": {"algo_code": algo_code, "algo_config": dict(algo_config or {})},
    }


def _parent_intent(*, side: OrderSide = OrderSide.BUY, limit_price: float = 10.0, quantity: int = 200) -> OrderIntent:
    return OrderIntent(
        intent_id="parent_intent_shared",
        package_id="pkg_shared",
        portfolio_id="portfolio_shared",
        symbol="000001.SZ",
        side=side,
        quantity=quantity,
        order_type=OrderType.LIMIT,
        limit_price=limit_price,
        target_trade_date=TRADE_DATE,
        metadata={"execution_plan_intent_id": "plan_intent_shared"},
    )


def test_unified_adapter_routes_best_limit_child_action_with_policy_lineage() -> None:
    submitter = RecordingChildSubmitter()
    adapter = UnifiedMiniQMTVnpyExecutionAdapter(
        submitter=submitter,
        policy_context=_policy_context("BEST_LIMIT_MINIQMT", {"min_volume": 100, "max_volume": 100}),
        quote_provider=lambda symbol: {
            "symbol": symbol,
            "bid_price_1": 9.95,
            "bid_volume_1": 500,
            "ask_price_1": 10.05,
            "ask_volume_1": 500,
            "time": "20260521093100",
        },
    )

    result = adapter.execute_intent(_parent_intent(limit_price=10.50), trade_date=TRADE_DATE)

    assert result.terminal_state == "PENDING"
    assert len(submitter.requests) == 1
    child = submitter.requests[0].child_intent
    assert child.limit_price == 9.95
    assert child.metadata["execution_algo_code"] == "BEST_LIMIT_MINIQMT"
    assert child.metadata["execution_policy_id"] == "execpol_shared_vnpy"
    assert child.metadata["source_attribution"]["upstream_source_file"].endswith("best_limit_algo.py")
    assert result.diagnostic["adapter"] == "UnifiedMiniQMTVnpyExecutionAdapter"
    assert result.diagnostic["child_orders"][0]["status"]["raw_status"] == 50


def test_unified_adapter_preserves_rejected_child_raw_status_msg() -> None:
    submitter = RecordingChildSubmitter(
        status=MiniQMTOrderStatus(
            handle_id="handle_1",
            state="rejected",
            raw_status=57,
            status_msg="[COUNTER][260200] insufficient buying power",
            rejection_reason="[COUNTER][260200] insufficient buying power",
            raw={"order_status": 57, "status_msg": "[COUNTER][260200] insufficient buying power"},
        )
    )
    adapter = UnifiedMiniQMTVnpyExecutionAdapter(
        submitter=submitter,
        policy_context=_policy_context("SNIPER_MINIQMT"),
    )

    result = adapter.execute_intent(_parent_intent(), trade_date=TRADE_DATE)

    assert result.terminal_state == "REJECTED"
    status = result.diagnostic["child_orders"][0]["status"]
    assert status["raw_status"] == 57
    assert status["status_msg"].startswith("[COUNTER][260200]")


def _vnpy_plan(algo_code: str, algo_config: dict[str, Any] | None = None):
    release, binding, runtime_repo = _release_binding_repo(backend=SimulationBrokerBackend.MINIQMT_SIM)
    evidence = _evidence(release)
    runtime_repo.save_daily_selection_evidence(evidence)
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=_snapshot(),
        runtime_release=release,
        binding=binding,
        current_positions=_current_positions("portfolio_shared"),
    )
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=TRADE_DATE,
        current_positions=_current_positions("portfolio_shared"),
        target_positions=targets,
    )
    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions,
        portfolio_id="portfolio_shared",
        execution_policy_payload={
            "algo_code": algo_code,
            "algo_config": dict(algo_config or {}),
            "schedule_window": {"mode": "open_to_close"},
        },
        tail_policy_payload={"policy": "cancel_unfilled_at_close"},
    )
    return binding, runtime_repo.save_execution_plan(plan)


def test_miniqmt_bridge_uses_runtime_owned_vnpy_algo_for_child_requests() -> None:
    binding, plan = _vnpy_plan("SNIPER_MINIQMT")
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    requests = bridge.build_managed_order_requests(
        plan=plan,
        binding=binding,
        price_by_symbol={"000001.SZ": Decimal("10.00"), "000003.SZ": Decimal("8.00"), "688001.SH": Decimal("20.00")},
    )

    assert requests
    assert all(request.metadata["source"] == "runtime_owned_vnpy_algo" for request in requests)
    assert all(request.metadata["runtime_owner"] == "MiniQMTExecutionRuntime" for request in requests)
    assert all(request.metadata["runtime_child_order_id"] for request in requests)
    assert all(request.metadata["runtime_algo_instance_id"] for request in requests)
    assert all("vnpy_action" in request.metadata for request in requests)
    assert all("-vn" in request.order_remark for request in requests)
    assert requests[0].metadata["execution_policy_id"] == plan.execution_policy_version_id
    assert requests[0].metadata["execution_policy_sha256"] == plan.execution_policy_sha256
    assert requests[0].metadata["execution_algo_code"] == "SNIPER_MINIQMT"
    assert requests[0].metadata["source_attribution"]["upstream_source_file"].endswith("sniper_algo.py")
    assert "vnpy_execution_diagnostic" not in requests[0].metadata


def test_miniqmt_bridge_rejects_vnpy_id_only_plan_without_policy_snapshot() -> None:
    release, binding, runtime_repo = _release_binding_repo(backend=SimulationBrokerBackend.MINIQMT_SIM)
    policy_id = "vnpy_asset:SNIPER_MINIQMT:final_multistrategy_dry_run_20260603"
    release = release.model_copy(
        update={
            "execution_policy_version_id": policy_id,
            "execution_policy_sha256": "sha_vnpy_id_only",
            "release_config_json": {
                **release.release_config_json,
                "execution_policy": {
                    "policy_version_id": policy_id,
                    "policy_sha256": "sha_vnpy_id_only",
                },
            },
        }
    )
    evidence = _evidence(release)
    runtime_repo.save_daily_selection_evidence(evidence)
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=_snapshot(),
        runtime_release=release,
        binding=binding,
        current_positions=_current_positions("portfolio_shared"),
    )
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=TRADE_DATE,
        current_positions=_current_positions("portfolio_shared"),
        target_positions=targets,
    )
    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions,
        portfolio_id="portfolio_shared",
    )
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    with pytest.raises(RuntimeConfigInvalidError, match="full policy_json snapshot") as exc_info:
        bridge.build_managed_order_requests(
            plan=plan,
            binding=binding,
            price_by_symbol={"000001.SZ": Decimal("10.00"), "000003.SZ": Decimal("8.00"), "688001.SH": Decimal("20.00")},
        )

    assert exc_info.value.context["payload_has_policy_json"] is False
    assert exc_info.value.context["inferred_algo_code"] == "SNIPER_MINIQMT"


def test_localsim_bridge_rejects_vnpy_plan_before_broker_submit() -> None:
    release, binding, runtime_repo = _release_binding_repo(backend=SimulationBrokerBackend.LOCAL_SIM)
    release = release.model_copy(
        update={
            "execution_policy_version_id": "vnpy_asset:SNIPER_MINIQMT",
            "execution_policy_sha256": "sha_vnpy_local",
            "release_config_json": {
                **release.release_config_json,
                "execution_policy": {
                    "policy_version_id": "vnpy_asset:SNIPER_MINIQMT",
                    "policy_sha256": "sha_vnpy_local",
                    "policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}},
                },
            },
        }
    )
    evidence = _evidence(release)
    runtime_repo.save_daily_selection_evidence(evidence)
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=_snapshot(),
        runtime_release=release,
        binding=binding,
        current_positions=_current_positions("portfolio_shared"),
    )
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=TRADE_DATE,
        current_positions=_current_positions("portfolio_shared"),
        target_positions=targets,
    )
    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=rebalance.order_intents,
        trading_rule_decisions=rebalance.trading_rule_decisions,
        portfolio_id="portfolio_shared",
    )
    broker = FakeLocalSimBroker()

    with pytest.raises(RuntimeConfigInvalidError, match="LocalSim cannot execute MiniQMT vn.py-style") as exc_info:
        LocalSimExecutionBridge().submit_plan(plan=plan, broker=broker)  # type: ignore[arg-type]

    assert broker.submitted == []
    assert exc_info.value.context["broker_backend"] == "local_sim"
    assert exc_info.value.context["inferred_algo_code"] == "SNIPER_MINIQMT"


def test_miniqmt_bridge_rejects_non_vnpy_plan_without_managed_order_fallback() -> None:
    binding, plan = _vnpy_plan("SNIPER_MINIQMT")
    plan = plan.model_copy(
        update={
            "execution_policy_version_id": "exec_policy_close_price",
            "plan_payload_json": {
                **plan.plan_payload_json,
                "execution_policy": {
                    **plan.plan_payload_json["execution_policy"],
                    "version_id": "exec_policy_close_price",
                    "payload": {"algo_code": "CLOSE_PRICE", "policy_json": {"algo_code": "CLOSE_PRICE", "algo_config": {}}},
                },
            },
        }
    )
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    with pytest.raises(ExecutionPathNotCanonicalError) as exc_info:
        bridge.build_managed_order_requests(
            plan=plan,
            binding=binding,
            price_by_symbol={"000001.SZ": Decimal("10.00"), "000003.SZ": Decimal("8.00"), "688001.SH": Decimal("20.00")},
        )

    assert exc_info.value.context["inferred_algo_code"] == "CLOSE_PRICE"
    assert exc_info.value.context["required_action"].startswith("activate SNIPER_MINIQMT")


def test_miniqmt_bridge_vnpy_invalid_config_fails_fast_without_direct_order_fallback() -> None:
    binding, plan = _vnpy_plan("BEST_LIMIT_MINIQMT", {"min_volume": 300, "max_volume": 100})
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(repository=InMemoryQmtStrategyLedgerRepository())
    )

    with pytest.raises(VnpyStyleConfigError, match="max_volume >= min_volume"):
        bridge.build_managed_order_requests(
            plan=plan,
            binding=binding,
            price_by_symbol={"000001.SZ": Decimal("10.00"), "000003.SZ": Decimal("8.00"), "688001.SH": Decimal("20.00")},
        )

