from __future__ import annotations

from datetime import date

import pytest

from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeConfig,
    MiniQMTExecutionRuntimeKind,
    MiniQMTExecutionRuntimeMode,
    MiniQMTExecutionRuntime,
    MiniQMTGrayDecisionStatus,
    MiniQMTGraySwitchController,
    MiniQMTShadowCompilerAdapter,
    MiniQMTShadowEventLoopAdapter,
    MiniQMTShadowInputEvent,
    MiniQMTShadowParallelRunner,
    MiniQMTShadowReconciler,
    MiniQMTShadowScenario,
    get_miniqmt_execution_runtime_kind,
)
from backend.services.trading_core.models import OrderSide


TRADE_DATE = date(2026, 6, 23)
RUNTIME_ID = "mqrt_phase6_gray_slot"
PORTFOLIO_ID = "portfolio_phase6_sim"
STRATEGY_SLOT_ID = "slot_phase6_canary"


def _durable_replay_events() -> list[MiniQMTShadowInputEvent]:
    return [
        MiniQMTShadowInputEvent(
            event_type="policy",
            payload={"policy_json": {"algo_code": "SNIPER_MINIQMT", "algo_config": {}}},
        ),
        MiniQMTShadowInputEvent(
            event_type="parent_intent",
            payload={
                "intent_id": "intent_phase6_shadow_001",
                "portfolio_id": PORTFOLIO_ID,
                "strategy_slot_id": STRATEGY_SLOT_ID,
                "symbol": "000001.SZ",
                "side": "BUY",
                "quantity": 100,
                "limit_price": 10.0,
            },
        ),
        MiniQMTShadowInputEvent(
            event_type="tick",
            payload={
                "symbol": "000001.SZ",
                "price": 9.99,
                "bid_price_1": 9.98,
                "bid_volume_1": 1000,
                "ask_price_1": 9.99,
                "ask_volume_1": 1000,
            },
        ),
        MiniQMTShadowInputEvent(
            event_type="trade_fill",
            payload={
                "parent_intent_id": "intent_phase6_shadow_001",
                "quantity": 100,
                "price": 9.99,
                "cumulative_quantity": 100,
            },
        ),
    ]


def _seed_no_fatal_shadow_evidence(
    repo: InMemoryMiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str = RUNTIME_ID,
    portfolio_id: str = PORTFOLIO_ID,
    strategy_slot_id: str = STRATEGY_SLOT_ID,
):
    runner = MiniQMTShadowParallelRunner(reconciler=MiniQMTShadowReconciler(repository=repo))
    report = runner.run(
        runtime_id=runtime_id,
        scenario=MiniQMTShadowScenario.FULL_FILL,
        input_events=_durable_replay_events(),
        event_loop_adapter=MiniQMTShadowEventLoopAdapter(repository=repo),
        compiler_adapter=MiniQMTShadowCompilerAdapter(repository=repo),
        metadata={
            "trade_date": TRADE_DATE.isoformat(),
            "account_group_id": "account_phase6",
            "portfolio_id": portfolio_id,
            "strategy_slot_id": strategy_slot_id,
            "phase": "phase6_shadow_evidence",
            "replay_source": "durable_phase5_shadow_events",
        },
    )
    assert report.fatal_differences == []
    return report


def test_phase6_canary_switch_requires_durable_shadow_report_then_rolls_back() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    report = _seed_no_fatal_shadow_evidence(repo)
    controller = MiniQMTGraySwitchController(repository=repo)

    decision = controller.switch_to_event_loop(
        runtime_id=RUNTIME_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
        mode=MiniQMTExecutionRuntimeMode.SIM,
        trade_date=TRADE_DATE,
        reason="phase6_canary_after_shadow_report",
    )

    assert decision.status == MiniQMTGrayDecisionStatus.APPLIED
    assert decision.runtime_kind == MiniQMTExecutionRuntimeKind.EVENT_LOOP
    assert decision.shadow_event_id == report.durable_event_id
    assert controller.resolve_runtime_kind(
        runtime_id=RUNTIME_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
    ) == MiniQMTExecutionRuntimeKind.EVENT_LOOP
    assert get_miniqmt_execution_runtime_kind({}) == MiniQMTExecutionRuntimeKind.COMPILER
    assert controller.resolve_runtime_kind(
        runtime_id=RUNTIME_ID,
        portfolio_id="portfolio_phase6_unswitched",
        strategy_slot_id="slot_phase6_unswitched",
    ) == MiniQMTExecutionRuntimeKind.COMPILER

    rollback = controller.rollback_to_compiler(
        runtime_id=RUNTIME_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
        mode=MiniQMTExecutionRuntimeMode.SIM,
        trade_date=TRADE_DATE,
        reason="phase6_one_click_rollback_drill",
    )

    assert rollback.status == MiniQMTGrayDecisionStatus.APPLIED
    assert rollback.runtime_kind == MiniQMTExecutionRuntimeKind.COMPILER
    assert controller.resolve_runtime_kind(
        runtime_id=RUNTIME_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
    ) == MiniQMTExecutionRuntimeKind.COMPILER
    assert [event.event_type for event in repo.list_events(RUNTIME_ID)][-2:] == [
        MiniQMTExecutionEventType.GRAY_SWITCH_APPLIED,
        MiniQMTExecutionEventType.GRAY_ROLLBACK_APPLIED,
    ]


def test_phase6_canary_rejects_missing_fatal_or_wrong_scope_shadow_evidence_loudly() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    controller = MiniQMTGraySwitchController(repository=repo)

    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_SHADOW_EVIDENCE_MISSING"):
        controller.switch_to_event_loop(
            runtime_id="mqrt_phase6_missing_evidence",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )
    assert repo.list_events("mqrt_phase6_missing_evidence")[-1].event_type == (
        MiniQMTExecutionEventType.GRAY_SWITCH_REJECTED
    )

    _seed_no_fatal_shadow_evidence(
        repo,
        runtime_id="mqrt_phase6_wrong_scope",
        portfolio_id="other_portfolio",
        strategy_slot_id="other_slot",
    )
    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_SHADOW_SCOPE_MISMATCH"):
        controller.switch_to_event_loop(
            runtime_id="mqrt_phase6_wrong_scope",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )

    reconciler = MiniQMTShadowReconciler(repository=repo)
    with pytest.raises(RuntimeError, match="MINIQMT_SHADOW_RECONCILIATION_FATAL"):
        reconciler.reconcile(
            runtime_id="mqrt_phase6_fatal_shadow",
            scenario=MiniQMTShadowScenario.FULL_FILL,
            a_runtime={
                "runtime_id": "mqrt_phase6_fatal_shadow",
                "runtime_kind": "event_loop",
                "ledger": {"child_orders": [], "trades": [], "cash": {}, "positions": {}},
            },
            b_runtime={
                "runtime_id": "mqrt_phase6_fatal_shadow",
                "runtime_kind": "compiler",
                "ledger": {
                    "child_orders": [
                        {
                            "shadow_key": "child_1",
                            "symbol": "000001.SZ",
                            "side": "BUY",
                            "quantity": 100,
                            "price": 10.0,
                            "status": "FILLED",
                        }
                    ],
                    "trades": [],
                    "cash": {},
                    "positions": {},
                },
            },
            metadata={
                "trade_date": TRADE_DATE.isoformat(),
                "portfolio_id": PORTFOLIO_ID,
                "strategy_slot_id": STRATEGY_SLOT_ID,
            },
        )
    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_SHADOW_EVIDENCE_FATAL"):
        controller.switch_to_event_loop(
            runtime_id="mqrt_phase6_fatal_shadow",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )


@pytest.mark.parametrize(
    "mode,reason_code",
    [
        (MiniQMTExecutionRuntimeMode.LIVE, "MINIQMT_GRAY_LIVE_FORBIDDEN"),
        (MiniQMTExecutionRuntimeMode.LIVE_PENDING_APPROVAL, "MINIQMT_GRAY_LIVE_FORBIDDEN"),
    ],
)
def test_phase6_canary_rejects_live_modes_even_with_clean_shadow_evidence(
    mode: MiniQMTExecutionRuntimeMode,
    reason_code: str,
) -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _seed_no_fatal_shadow_evidence(repo, runtime_id=f"mqrt_phase6_{mode.value.lower()}")
    controller = MiniQMTGraySwitchController(repository=repo)

    with pytest.raises(RuntimeError, match=reason_code):
        controller.switch_to_event_loop(
            runtime_id=f"mqrt_phase6_{mode.value.lower()}",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=mode,
            trade_date=TRADE_DATE,
        )


def test_phase6_switch_and_rollback_reject_in_flight_scope_until_operator_reset() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _seed_no_fatal_shadow_evidence(repo, runtime_id="mqrt_phase6_in_flight")
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="mqrt_phase6_in_flight",
            account_group_id="account_phase6",
            trade_date=TRADE_DATE,
            runtime_config_hash="phase6_in_flight",
        ),
        repository=repo,
        gateway=FakeMiniQMTGateway(),
    )
    runtime.start()
    algo = runtime.create_algo_instance(
        parent_intent_id="intent_phase6_active",
        strategy_slot_id=STRATEGY_SLOT_ID,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.submit_child_order(algo_instance_id=algo.algo_instance_id, quantity=100, price=10.0)
    controller = MiniQMTGraySwitchController(repository=repo)

    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_IN_FLIGHT_AMBIGUOUS"):
        controller.switch_to_event_loop(
            runtime_id="mqrt_phase6_in_flight",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )

    reset_result = runtime.execute_operator_command(
        command_id="op_phase6_reset_before_gray",
        command_type="RESET_STRATEGY_SLOT",
        reason="phase6 in-flight migration drill requires explicit operator reset",
        payload={"strategy_slot_id": STRATEGY_SLOT_ID},
    )
    assert reset_result.errors == []

    decision = controller.switch_to_event_loop(
        runtime_id="mqrt_phase6_in_flight",
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
        mode=MiniQMTExecutionRuntimeMode.SIM,
        trade_date=TRADE_DATE,
    )
    assert decision.status == MiniQMTGrayDecisionStatus.APPLIED

    active_algo = runtime.create_algo_instance(
        parent_intent_id="intent_phase6_active_after_switch",
        strategy_slot_id=STRATEGY_SLOT_ID,
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
    )
    runtime.submit_child_order(algo_instance_id=active_algo.algo_instance_id, quantity=100, price=10.0)
    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_IN_FLIGHT_AMBIGUOUS"):
        controller.rollback_to_compiler(
            runtime_id="mqrt_phase6_in_flight",
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )
