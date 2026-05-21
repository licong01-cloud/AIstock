from __future__ import annotations

from datetime import date

import pytest

from backend.services.selection_center.models import SelectionCandidate, SignalSnapshot
from backend.services.simulation_runtime import (
    DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID,
    DailySelectionEvidence,
    ExecutionPlanCompiler,
    InMemorySimulationRuntimeRepository,
    RebalanceIntentService,
    SimulationBrokerBackend,
    StrategyRuntimeReleaseService,
    TargetPositionService,
    TradingRuleService,
)
from backend.services.simulation_runtime.models import canonical_json_sha256
from backend.services.trading_core.errors import StrategyPackageValidationError
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
    release, binding = _release_and_binding()
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

    with pytest.raises(StrategyPackageValidationError):
        ExecutionPlanCompiler().compile_plan(
            runtime_release=release,
            binding=binding,
            selection_evidence=evidence,
            order_intents=rebalance.order_intents,
            trading_rule_decisions=rebalance.trading_rule_decisions,
            portfolio_id="portfolio_shared",
            execution_policy_payload={"paper_only": True, "algo_code": "paper_only"},
        )
