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
_REQUIRED_D3_SCENARIOS = (
    MiniQMTShadowScenario.FULL_FILL,
    MiniQMTShadowScenario.PARTIAL_55_STREAM,
    MiniQMTShadowScenario.REJECT,
    MiniQMTShadowScenario.CANCEL,
    MiniQMTShadowScenario.DISCONNECT,
    MiniQMTShadowScenario.RESTART_RECOVERY,
)


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
    scenario: MiniQMTShadowScenario = MiniQMTShadowScenario.FULL_FILL,
    trade_date: date = TRADE_DATE,
    source: str = "real",
    use_runtime_replay: bool = False,
):
    metadata = {
        "trade_date": trade_date.isoformat(),
        "account_group_id": "account_phase6",
        "portfolio_id": portfolio_id,
        "strategy_slot_id": strategy_slot_id,
        "phase": "phase6_shadow_evidence",
        "source": source,
        "replay_source": "durable_phase5_shadow_events",
    }
    if use_runtime_replay:
        runner = MiniQMTShadowParallelRunner(reconciler=MiniQMTShadowReconciler(repository=repo))
        report = runner.run(
            runtime_id=runtime_id,
            scenario=scenario,
            input_events=_durable_replay_events(),
            event_loop_adapter=MiniQMTShadowEventLoopAdapter(repository=repo),
            compiler_adapter=MiniQMTShadowCompilerAdapter(repository=repo),
            metadata=metadata,
        )
    else:
        report = MiniQMTShadowReconciler(repository=repo).reconcile(
            runtime_id=runtime_id,
            scenario=scenario,
            a_runtime=_empty_shadow_runtime(runtime_id=runtime_id, runtime_kind="event_loop"),
            b_runtime=_empty_shadow_runtime(runtime_id=runtime_id, runtime_kind="compiler"),
            metadata=metadata,
        )
    assert report.fatal_differences == []
    assert report.durable_event_id
    return report


def _empty_shadow_runtime(*, runtime_id: str, runtime_kind: str):
    return {
        "runtime_id": f"{runtime_id}_{runtime_kind}",
        "runtime_kind": runtime_kind,
        "ledger": {"child_orders": [], "trades": [], "cash": {}, "positions": {}},
        "metadata": {"broker_called": False, "broker_mutated": False},
    }


def _seed_required_shadow_coverage(
    repo: InMemoryMiniQMTExecutionRuntimeRepository,
    *,
    runtime_id: str = RUNTIME_ID,
    portfolio_id: str = PORTFOLIO_ID,
    strategy_slot_id: str = STRATEGY_SLOT_ID,
    trade_date: date = TRADE_DATE,
    source_by_scenario: dict[MiniQMTShadowScenario, str] | None = None,
):
    source_by_scenario = dict(source_by_scenario or {})
    return [
        _seed_no_fatal_shadow_evidence(
            repo,
            runtime_id=runtime_id,
            portfolio_id=portfolio_id,
            strategy_slot_id=strategy_slot_id,
            scenario=scenario,
            trade_date=trade_date,
            source=source_by_scenario.get(scenario, "real"),
            use_runtime_replay=scenario == MiniQMTShadowScenario.FULL_FILL
            and source_by_scenario.get(scenario, "real") == "real",
        )
        for scenario in _REQUIRED_D3_SCENARIOS
    ]


def test_phase6_canary_rejects_insufficient_shadow_trading_days_loudly() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _seed_required_shadow_coverage(repo)
    controller = MiniQMTGraySwitchController(repository=repo, shadow_min_trading_days=2)

    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_SHADOW_TRADING_DAYS_INSUFFICIENT") as exc_info:
        controller.switch_to_event_loop(
            runtime_id=RUNTIME_ID,
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )

    assert TRADE_DATE.isoformat() in str(exc_info.value)


def test_phase6_canary_rejects_missing_shadow_scenario_coverage_loudly() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    _seed_no_fatal_shadow_evidence(repo, scenario=MiniQMTShadowScenario.FULL_FILL)
    controller = MiniQMTGraySwitchController(repository=repo)

    with pytest.raises(RuntimeError, match="MINIQMT_GRAY_SHADOW_SCENARIO_COVERAGE_MISSING") as exc_info:
        controller.switch_to_event_loop(
            runtime_id=RUNTIME_ID,
            portfolio_id=PORTFOLIO_ID,
            strategy_slot_id=STRATEGY_SLOT_ID,
            mode=MiniQMTExecutionRuntimeMode.SIM,
            trade_date=TRADE_DATE,
        )

    assert "partial_55_stream" in str(exc_info.value)


def test_phase6_canary_switch_requires_durable_shadow_report_then_rolls_back() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    reports = _seed_required_shadow_coverage(repo)
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
    assert decision.shadow_event_id == reports[-1].durable_event_id
    assert set(decision.metadata["accepted_shadow_event_ids"]) == {report.durable_event_id for report in reports}
    gate_metadata = decision.metadata["shadow_evidence_gate"]
    assert gate_metadata["covered_trade_dates"] == [TRADE_DATE.isoformat()]
    assert gate_metadata["missing_scenarios"] == []
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


def test_phase6_canary_accepts_mixed_real_and_synthetic_durable_shadow_evidence() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    source_by_scenario = {
        MiniQMTShadowScenario.FULL_FILL: "real",
        MiniQMTShadowScenario.PARTIAL_55_STREAM: "synthetic",
        MiniQMTShadowScenario.REJECT: "synthetic",
        MiniQMTShadowScenario.CANCEL: "synthetic",
        MiniQMTShadowScenario.DISCONNECT: "synthetic",
        MiniQMTShadowScenario.RESTART_RECOVERY: "synthetic",
    }
    reports = _seed_required_shadow_coverage(repo, source_by_scenario=source_by_scenario)
    controller = MiniQMTGraySwitchController(repository=repo)

    decision = controller.switch_to_event_loop(
        runtime_id=RUNTIME_ID,
        portfolio_id=PORTFOLIO_ID,
        strategy_slot_id=STRATEGY_SLOT_ID,
        mode=MiniQMTExecutionRuntimeMode.SIM,
        trade_date=TRADE_DATE,
        reason="phase6_canary_after_mixed_shadow_report",
    )

    accepted_reports = decision.metadata["shadow_evidence_gate"]["accepted_reports"]
    sources_by_scenario = {item["scenario"]: item["source"] for item in accepted_reports}
    assert set(decision.metadata["accepted_shadow_event_ids"]) == {report.durable_event_id for report in reports}
    assert sources_by_scenario["full_fill"] == "real"
    assert sources_by_scenario["partial_55_stream"] == "synthetic"
    assert sources_by_scenario["restart_recovery"] == "synthetic"


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
    _seed_required_shadow_coverage(repo, runtime_id="mqrt_phase6_in_flight")
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
