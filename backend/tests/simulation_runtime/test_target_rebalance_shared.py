from __future__ import annotations

from datetime import date
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from backend.services.paper_trading_v2.broker.base import OrderHandle
from backend.services.qmt_strategy_ledger.models import (
    PositionLotRecord,
    PositionLotStatus,
    VirtualAccount,
    VirtualAccountStatus,
    new_id,
)
from backend.services.qmt_strategy_ledger.lot_availability import StaticTradingCalendarProvider
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.qmt_strategy_ledger.repository import InMemoryQmtStrategyLedgerRepository
from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    ExecutionPlanCompiler,
    InMemorySimulationRuntimeRepository,
    LocalSimExecutionBridge,
    MiniQMTExecutionBridge,
    RebalanceIntentService,
    SimulationBrokerBackend,
    SimulationDailyRunStatus,
    SimulationLifecycleOrchestrator,
    StrategyRuntimeReleaseService,
    TargetPositionService,
    TradingRuleService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide, PositionLot


def _release_and_binding(*, backend: SimulationBrokerBackend = SimulationBrokerBackend.LOCAL_SIM):
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        runtime_profile_id="runtime_profile_shared",
        runtime_profile_version_id="runtime_profile_shared_v1",
        runtime_profile_sha256="runtime_profile_hash_shared",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="shared target rebalance test",
    )
    binding = service.create_binding(
        strategy_id=f"strategy_{backend.value}",
        release=release,
        broker_backend=backend,
        capital_allocation=100_000,
        broker_account_id=f"acct_{backend.value}",
        strategy_name=f"SharedDecision-{backend.value}",
        order_remark_prefix=f"shared-{backend.value}",
        created_by="unit-test",
        created_reason="shared target rebalance test",
    )
    return release, binding


def _release_binding_repo(*, backend: SimulationBrokerBackend = SimulationBrokerBackend.LOCAL_SIM):
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        runtime_profile_id="runtime_profile_shared",
        runtime_profile_version_id="runtime_profile_shared_v1",
        runtime_profile_sha256="runtime_profile_hash_shared",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="shared target rebalance test",
    )
    binding = service.create_binding(
        strategy_id=f"strategy_{backend.value}",
        release=release,
        broker_backend=backend,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name=f"SharedDecision-{backend.value}",
        order_remark_prefix=f"shared-{backend.value}",
        created_by="unit-test",
        created_reason="shared target rebalance test",
    )
    return release, binding, repo


def _snapshot() -> SignalSnapshot:
    return SignalSnapshot(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        trade_date=date(2026, 5, 21),
        data_source="DB_HISTORICAL",
        candidates=[
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
        ],
        runtime_config={"runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}},
    )


def _evidence(release) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": "2026-05-21",
        "cutoff_date": "2026-05-20",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile": {
            "profile_version_id": release.runtime_profile_version_id,
            "config_sha256": release.runtime_profile_sha256,
        },
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "candidates": [
            {"symbol": "000001.SZ", "score": 0.99, "rank": 1},
            {"symbol": "688001.SH", "score": 0.98, "rank": 2},
        ],
        "exclusions": [],
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=date(2026, 5, 21),
        cutoff_date=date(2026, 5, 20),
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=2,
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


def _empty_snapshot() -> SignalSnapshot:
    return SignalSnapshot(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        trade_date=date(2026, 5, 21),
        data_source="DB_HISTORICAL",
        candidates=[],
        runtime_config={"runtime_profile": {"selection": {"daily_strategy_id": DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID}}},
        valid_no_candidate=True,
        no_candidate_reason="unit test no candidate day",
    )


def _empty_evidence(release) -> DailySelectionEvidence:
    payload = {
        "schema_version": "daily_selection_evidence_v1",
        "target_trade_date": "2026-05-21",
        "cutoff_date": "2026-05-20",
        "package_id": release.package_id,
        "manifest_sha256": release.manifest_sha256,
        "release_id": release.release_id,
        "release_hash": release.release_hash,
        "runtime_profile": {
            "profile_version_id": release.runtime_profile_version_id,
            "config_sha256": release.runtime_profile_sha256,
        },
        "source_type": "live_inference",
        "data_source": "DB_HISTORICAL",
        "candidates": [],
        "exclusions": [],
        "valid_no_candidate": True,
        "no_candidate_reason": "unit test no candidate day",
    }
    digest = canonical_json_sha256(payload)
    return DailySelectionEvidence(
        evidence_id=f"dse_{digest[:16]}",
        target_trade_date=date(2026, 5, 21),
        cutoff_date=date(2026, 5, 20),
        package_id=release.package_id,
        manifest_sha256=release.manifest_sha256,
        release_id=release.release_id,
        release_hash=release.release_hash,
        runtime_profile_version_id=release.runtime_profile_version_id,
        runtime_profile_hash=release.runtime_profile_sha256,
        source_type="live_inference",
        data_source="DB_HISTORICAL",
        candidate_count=0,
        excluded_count=0,
        artifact_hash=digest,
        evidence_payload_json=payload,
        created_by="unit-test",
    )


def _current_positions(portfolio_id: str) -> dict[str, PositionLot]:
    return {
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
    }


def test_target_and_rebalance_services_are_shared_for_localsim_and_miniqmt() -> None:
    release, local_binding = _release_and_binding(backend=SimulationBrokerBackend.LOCAL_SIM)
    _, qmt_binding = _release_and_binding(backend=SimulationBrokerBackend.MINIQMT_SIM)
    snapshot = _snapshot()
    evidence = _evidence(release)
    target_service = TargetPositionService()
    rebalance_service = RebalanceIntentService()
    current_positions = _current_positions("portfolio_shared")

    local_targets = target_service.build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=snapshot,
        runtime_release=release,
        binding=local_binding,
        current_positions=current_positions,
    )
    qmt_targets = target_service.build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=snapshot,
        runtime_release=release,
        binding=qmt_binding,
        current_positions=current_positions,
    )

    assert [target.model_dump(exclude={"metadata"}) for target in local_targets] == [
        target.model_dump(exclude={"metadata"}) for target in qmt_targets
    ]
    local_result = rebalance_service.build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=local_binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions=current_positions,
        target_positions=local_targets,
    )
    qmt_result = rebalance_service.build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=local_binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions=current_positions,
        target_positions=qmt_targets,
    )

    normalized_local = [(item.symbol, item.side.value, item.quantity, item.metadata["rebalance_reason"]) for item in local_result.order_intents]
    normalized_qmt = [(item.symbol, item.side.value, item.quantity, item.metadata["rebalance_reason"]) for item in qmt_result.order_intents]
    assert normalized_local == normalized_qmt
    assert ("000003.SZ", "SELL", 77, "DROPPED_FROM_SELECTION") in normalized_local
    assert ("688001.SH", "BUY", 201, "daily_strategy_buy_or_retain") in normalized_local


def test_trading_rule_service_uses_single_a_share_board_lot_source() -> None:
    service = TradingRuleService()

    main_board_reject = service.decide_order_quantity(symbol="000001.SZ", side=OrderSide.BUY, requested_quantity=99)
    star_emit = service.decide_order_quantity(symbol="688001.SH", side=OrderSide.BUY, requested_quantity=201)
    sell_residual = service.decide_order_quantity(
        symbol="000003.SZ",
        side=OrderSide.SELL,
        requested_quantity=77,
        tplus1_available_quantity=77,
    )

    assert main_board_reject.decision == "REJECT"
    assert main_board_reject.reason_code == "BOARD_LOT_ZERO"
    assert star_emit.decision == "EMIT"
    assert star_emit.legal_quantity == 201
    assert star_emit.lot_rule == {"min_quantity": 200, "increment": 1}
    assert sell_residual.decision == "EMIT"
    assert sell_residual.legal_quantity == 77


def test_execution_plan_compiler_links_release_binding_evidence_and_rule_decisions() -> None:
    release, binding, runtime_repo = _release_binding_repo()
    evidence = _evidence(release)
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
        trade_date=date(2026, 5, 21),
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
        execution_policy_payload={"algo_code": "V25_1_SMALL_CAP", "schedule_window": {"mode": "open_to_close"}},
        tail_policy_payload={"policy": "cancel_unfilled_at_close"},
    )

    assert plan.release_id == release.release_id
    assert plan.binding_id == binding.binding_id
    assert plan.selection_evidence_id == evidence.evidence_id
    assert {intent.trading_rule_decision_id for intent in plan.intents} == {
        decision.decision_id for decision in plan.trading_rule_decisions
    }
    assert [intent.symbol for intent in plan.intents] == ["000003.SZ", "688001.SH"]
    assert plan.plan_id == f"plan_{plan.plan_hash[:16]}"
    runtime_repo.save_daily_selection_evidence(evidence)
    persisted = runtime_repo.save_execution_plan(plan)
    assert runtime_repo.get_execution_plan(persisted.plan_id).plan_hash == plan.plan_hash


def test_execution_plan_compiler_rejects_paper_only_policy() -> None:
    release, binding = _release_and_binding()
    evidence = _evidence(release)
    rebalance = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions=_current_positions("portfolio_shared"),
        target_positions=TargetPositionService().build_target_positions(
            selection_evidence=evidence,
            signal_snapshot=_snapshot(),
            runtime_release=release,
            binding=binding,
            current_positions=_current_positions("portfolio_shared"),
        ),
    )

    with pytest.raises(RuntimeConfigInvalidError):
        ExecutionPlanCompiler().compile_plan(
            runtime_release=release,
            binding=binding,
            selection_evidence=evidence,
            order_intents=rebalance.order_intents,
            trading_rule_decisions=rebalance.trading_rule_decisions,
            portfolio_id="portfolio_shared",
            execution_policy_payload={"paper_only": True, "algo_code": "paper_only"},
        )


def test_empty_daily_signal_sells_dropped_positions_and_no_trade_is_legal() -> None:
    release, binding, runtime_repo = _release_binding_repo()
    evidence = _empty_evidence(release)
    targets = TargetPositionService().build_target_positions(
        selection_evidence=evidence,
        signal_snapshot=_empty_snapshot(),
        runtime_release=release,
        binding=binding,
        current_positions={"000003.SZ": _current_positions("portfolio_shared")["000003.SZ"]},
    )
    assert targets == []

    sell_all = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions={"000003.SZ": _current_positions("portfolio_shared")["000003.SZ"]},
        target_positions=targets,
    )
    assert [(item.symbol, item.side.value, item.quantity) for item in sell_all.order_intents] == [("000003.SZ", "SELL", 77)]
    assert sell_all.order_intents[0].metadata["rebalance_reason"] == "DROPPED_FROM_SELECTION"

    no_trade = RebalanceIntentService().build_order_intents(
        package_id=release.package_id,
        portfolio_id="portfolio_shared",
        strategy_id=binding.strategy_id,
        trade_date=date(2026, 5, 21),
        current_positions={},
        target_positions=targets,
    )
    plan = ExecutionPlanCompiler().compile_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=evidence,
        order_intents=no_trade.order_intents,
        trading_rule_decisions=no_trade.trading_rule_decisions,
        portfolio_id="portfolio_shared",
    )
    runtime_repo.save_daily_selection_evidence(evidence)
    persisted = runtime_repo.save_execution_plan(plan)
    assert persisted.intents == []
    assert persisted.plan_payload_json["intents"] == []


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


class FailingLocalSimBroker:
    def submit_order_intent(self, intent):  # noqa: ANN001
        raise RuntimeError(f"local submit failed for {intent.intent_id}")


class FakeManagedOrderBroker:
    def __init__(self) -> None:
        self.calls = []

    def get_positions(self):
        return [{"stock_code": "000003.SZ", "can_sell": 1000}]

    def place_order(self, **kwargs):
        self.calls.append(kwargs)
        return len(self.calls), "accepted"

    def cancel_order(self, order_id: str):
        return True, f"cancelled {order_id}"


def _compiled_plan_for_bridge(*, backend: SimulationBrokerBackend):
    release, binding, runtime_repo = _release_binding_repo(backend=backend)
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
        trade_date=date(2026, 5, 21),
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
        execution_policy_payload={"algo_code": "V25_1_SMALL_CAP", "schedule_window": {"mode": "open_to_close"}},
        tail_policy_payload={"policy": "cancel_unfilled_at_close"},
    )
    return release, binding, runtime_repo.save_execution_plan(plan)


def test_localsim_execution_bridge_consumes_shared_execution_plan() -> None:
    _, _, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.LOCAL_SIM)
    broker = FakeLocalSimBroker()

    result = LocalSimExecutionBridge().submit_plan(plan=plan, broker=broker)  # type: ignore[arg-type]

    assert [intent.intent_id for intent in result.order_intents] == [intent.intent_id for intent in plan.intents]
    assert [handle.intent_id for handle in result.handles] == [intent.intent_id for intent in plan.intents]
    assert broker.submitted[0].metadata["source_execution_plan_id"] == plan.plan_id


def test_miniqmt_execution_bridge_uses_managed_orders_and_strategy_attribution() -> None:
    _, binding, plan = _compiled_plan_for_bridge(backend=SimulationBrokerBackend.MINIQMT_SIM)
    qmt_repo = InMemoryQmtStrategyLedgerRepository()
    qmt_repo.create_virtual_account(
        VirtualAccount(
            strategy_id=binding.strategy_id,
            strategy_name=binding.strategy_name or binding.strategy_id,
            display_name="Shared MiniQMT Strategy",
            account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
            mode="SIM",
            initial_cash=Decimal("100000"),
            cash=Decimal("100000"),
            status=VirtualAccountStatus.ENABLED,
        )
    )
    qmt_repo.create_position_lot(
        PositionLotRecord(
            lot_id=new_id("lot"),
            strategy_id=binding.strategy_id,
            symbol="000003.SZ",
            open_trade_id="trade_open_000003",
            open_date=date(2026, 5, 20),
            quantity=77,
            available_quantity=77,
            remaining_quantity=77,
            avg_cost=Decimal("8.00"),
            cost_amount=Decimal("616.00"),
            account_id=binding.broker_account_id or "QMT_SIM_ACCOUNT",
            status=PositionLotStatus.OPEN,
        )
    )
    broker = FakeManagedOrderBroker()
    calendar = StaticTradingCalendarProvider([date(2026, 5, 20), date(2026, 5, 21)])
    bridge = MiniQMTExecutionBridge(
        managed_order_service=QmtManagedOrderService(
            repository=qmt_repo,
            broker=broker,  # type: ignore[arg-type]
            calendar_provider=calendar,
        )
    )

    preview = bridge.preview_plan(plan=plan, binding=binding)
    submitted = bridge.submit_plan(plan=plan, binding=binding)

    assert all(item.allowed for item in preview.preflights)
    assert submitted.success is True
    assert [call["strategy_name"] for call in broker.calls] == [binding.strategy_name, binding.strategy_name]
    assert preview.requests[0].metadata["source"] == "shared_execution_plan"
    assert preview.requests[0].order_remark.startswith(binding.order_remark_prefix or "")


def test_lifecycle_orchestrator_builds_dual_backend_plans_from_same_evidence_and_is_idempotent() -> None:
    repo = InMemorySimulationRuntimeRepository()
    service = StrategyRuntimeReleaseService(repository=repo)
    release = service.create_release(
        package_id="pkg_shared_decision",
        manifest_sha256="manifest_shared_decision",
        runtime_profile_id="runtime_profile_shared",
        runtime_profile_version_id="runtime_profile_shared_v1",
        runtime_profile_sha256="runtime_profile_hash_shared",
        daily_strategy_profile_version_id=DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
        execution_policy_version_id="exec_policy_v25_1_small_cap",
        execution_policy_sha256="exec_policy_hash_v25_1_small_cap",
        tail_policy_version_id="tail_policy_close_v1",
        tail_policy_sha256="tail_policy_hash_close_v1",
        created_by="unit-test",
        created_reason="lifecycle dual backend test",
    )
    local_binding = service.create_binding(
        strategy_id="strategy_local_shared",
        release=release,
        broker_backend=SimulationBrokerBackend.LOCAL_SIM,
        capital_allocation=100_000,
        created_by="unit-test",
        created_reason="lifecycle dual backend test",
    )
    qmt_binding = service.create_binding(
        strategy_id="strategy_qmt_shared",
        release=release,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
        capital_allocation=100_000,
        broker_account_id="QMT_SIM_ACCOUNT",
        strategy_name="SharedDecisionQMT",
        order_remark_prefix="shared-qmt",
        created_by="unit-test",
        created_reason="lifecycle dual backend test",
    )
    orchestrator = SimulationLifecycleOrchestrator(repository=repo)
    common = {
        "runtime_release": release,
        "selection_evidence": _evidence(release),
        "signal_snapshot": _snapshot(),
        "current_positions": _current_positions("portfolio_shared"),
        "portfolio_id": "portfolio_shared",
        "execution_policy_payload": {"algo_code": "V25_1_SMALL_CAP", "schedule_window": {"mode": "open_to_close"}},
    }

    local = orchestrator.build_execution_plan(binding=local_binding, **common)
    qmt = orchestrator.build_execution_plan(binding=qmt_binding, **common)
    rerun = orchestrator.build_execution_plan(binding=local_binding, **common)

    assert local.selection_evidence.artifact_hash == qmt.selection_evidence.artifact_hash
    assert [(t.symbol, t.target_quantity) for t in local.target_positions] == [
        (t.symbol, t.target_quantity) for t in qmt.target_positions
    ]
    assert [(i.symbol, i.side.value, i.order_quantity) for i in local.execution_plan.intents] == [
        (i.symbol, i.side.value, i.order_quantity) for i in qmt.execution_plan.intents
    ]
    assert rerun.run.run_id == local.run.run_id
    assert rerun.execution_plan.plan_hash == local.execution_plan.plan_hash


def test_lifecycle_no_rebalance_does_not_call_broker_and_marks_success() -> None:
    release, binding, repo = _release_binding_repo()
    orchestrator = SimulationLifecycleOrchestrator(repository=repo)
    build = orchestrator.build_execution_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=_empty_evidence(release),
        signal_snapshot=_empty_snapshot(),
        current_positions={},
        portfolio_id="portfolio_shared",
    )

    result = orchestrator.submit_execution_plan(build_result=build)

    assert result.status == "NO_REBALANCE"
    assert result.intent_count == 0
    assert result.run.status == SimulationDailyRunStatus.SUCCEEDED
    assert result.run.run_payload_json["no_rebalance_required"] is True


def test_lifecycle_marks_localsim_submit_exception_retryable() -> None:
    release, binding, repo = _release_binding_repo()
    orchestrator = SimulationLifecycleOrchestrator(repository=repo)
    build = orchestrator.build_execution_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=_evidence(release),
        signal_snapshot=_snapshot(),
        current_positions={},
        portfolio_id="portfolio_shared",
    )

    with pytest.raises(RuntimeError, match="local submit failed"):
        orchestrator.submit_execution_plan(
            build_result=build,
            local_broker=FailingLocalSimBroker(),  # type: ignore[arg-type]
        )

    latest = repo.get_simulation_daily_run(build.run.run_id)
    assert latest.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert latest.run_payload_json["submit_failure"]["stage"] == "LOCAL_SIM_SUBMIT_FAILED"


def test_lifecycle_successful_localsim_retry_clears_submit_failure() -> None:
    release, binding, repo = _release_binding_repo()
    orchestrator = SimulationLifecycleOrchestrator(repository=repo)
    build = orchestrator.build_execution_plan(
        runtime_release=release,
        binding=binding,
        selection_evidence=_evidence(release),
        signal_snapshot=_snapshot(),
        current_positions={},
        portfolio_id="portfolio_shared",
    )

    with pytest.raises(RuntimeError, match="local submit failed"):
        orchestrator.submit_execution_plan(
            build_result=build,
            local_broker=FailingLocalSimBroker(),  # type: ignore[arg-type]
        )


    failed = repo.get_simulation_daily_run(build.run.run_id)
    assert failed.status == SimulationDailyRunStatus.FAILED_RETRYABLE
    assert "submit_failure" in failed.run_payload_json

    result = orchestrator.submit_persisted_execution_plan(
        run=failed,
        binding=binding,
        execution_plan=build.execution_plan,
        local_broker=FakeLocalSimBroker(),  # type: ignore[arg-type]
    )

    latest = repo.get_simulation_daily_run(build.run.run_id)
    assert result.run.status == SimulationDailyRunStatus.SUCCEEDED
    assert latest.status == SimulationDailyRunStatus.SUCCEEDED
    assert result.run.run_payload_json["submitted_intents"] == len(build.execution_plan.intents)
    assert result.run.run_payload_json["last_stage"] == "SUCCEEDED"
    assert result.run.run_payload_json["broker_called"] is True
    assert "submit_failure" not in result.run.run_payload_json
    assert "submit_failure" not in latest.run_payload_json
