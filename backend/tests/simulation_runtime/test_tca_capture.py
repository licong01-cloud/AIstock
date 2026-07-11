from datetime import UTC, date, datetime

import pytest

from backend.services.simulation_runtime.tca_capture import (
    CaptureMergeOutcome,
    TcaBenchmarkPolicy,
    TcaCaptureConfigurationError,
    build_execution_planning_subjects,
    build_decision_benchmark_capture,
    resolve_execution_deadline,
    resolve_tca_benchmark_policy,
)
from backend.services.simulation_runtime.models import (
    ExecutionPlan,
    ExecutionPlanIntent,
    TradingRuleDecision,
    canonical_json_sha256,
)
from backend.services.trading_core.models import OrderSide
from backend.services.trading_core.tca_sidecar import merge_parent_first_write, new_run_tca_sidecar


def _policy() -> TcaBenchmarkPolicy:
    return TcaBenchmarkPolicy(
        benchmark_max_age_ms=10_000,
        arrival_forward_window_ms=2_000,
        clock_skew_tolerance_ms=1_000,
        benchmark_max_transport_latency_ms=3_000,
        policy_version="phase0a_test_v1",
    )


def test_decision_capture_is_hashed_and_uses_bbo_mid() -> None:
    now = datetime.now(UTC)
    capture = build_decision_benchmark_capture(
        execution_plan_id="plan_tca",
        execution_plan_hash="hash_tca",
        parent_intent_id="parent_tca",
        symbol="000001.SZ",
        side="BUY",
        decision_event_at=now,
        quote_evidence={
            "bid_price_1": 10.00,
            "ask_price_1": 10.02,
            "quote_timestamp": now.isoformat(),
            "received_at": now.isoformat(),
            "quote_source": "MINIQMT_REALTIME.broker_quote",
        },
        policy=_policy(),
        strategy_decision_price=10.01,
        strategy_decision_source="test",
        strategy_decision_time=None,
        strategy_decision_quality="DIAGNOSTIC",
    )

    assert capture.quality == "VALID"
    assert capture.mid_price == 10.01
    assert len(capture.capture_sha256) == 64


def test_tca_policy_never_silently_defaults() -> None:
    with pytest.raises(TcaCaptureConfigurationError) as exc_info:
        resolve_tca_benchmark_policy({"policy_json": {"algo_config": {}}})

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_BENCHMARK_POLICY_MISSING"


def test_run_sidecar_parent_entry_is_first_write_only() -> None:
    sidecar = new_run_tca_sidecar(execution_plan_id="plan", execution_plan_hash="hash")
    first = {"capture_sha256": "a" * 64, "value": 1}
    second = {"capture_sha256": "b" * 64, "value": 2}

    assert merge_parent_first_write(
        sidecar,
        section="decision_capture_by_parent",
        parent_intent_id="parent",
        value=first,
    ) == CaptureMergeOutcome.CREATED
    assert merge_parent_first_write(
        sidecar,
        section="decision_capture_by_parent",
        parent_intent_id="parent",
        value=first,
    ) == CaptureMergeOutcome.IDEMPOTENT
    assert merge_parent_first_write(
        sidecar,
        section="decision_capture_by_parent",
        parent_intent_id="parent",
        value=second,
    ) == CaptureMergeOutcome.CONFLICT
    assert sidecar["decision_capture_by_parent"]["parent"] == first


def test_full_day_deadline_never_silently_defaults_to_close() -> None:
    deadline = resolve_execution_deadline(
        schedule_window={"mode": "full_day"},
        trade_date=date(2026, 7, 13),
    )

    assert deadline["deadline"] is None
    assert deadline["quality"] == "UNRESOLVED"
    assert deadline["reason_code"] == "ADAPTIVE_IS_TCA_DEADLINE_UNRESOLVED"


def test_planning_subject_projection_keeps_rejected_decision_coverage() -> None:
    emitted_payload = {
        "schema_version": "trading_rule_decision_v1",
        "symbol": "000001.SZ",
        "market_board": "MAIN",
        "side": "BUY",
        "requested_quantity": 200,
        "legal_quantity": 100,
        "lot_rule": {"lot_size": 100},
        "price_limit_rule": {},
        "tplus1_available_quantity": None,
        "decision": "ADJUST",
        "reason_code": "BOARD_LOT_ADJUSTED",
        "source_version": "trading_rule_v1",
    }
    rejected_payload = {
        **emitted_payload,
        "symbol": "000002.SZ",
        "requested_quantity": 100,
        "legal_quantity": 0,
        "decision": "REJECT",
        "reason_code": "SUSPENDED_BY_SUSPEND_D",
    }
    emitted_hash = canonical_json_sha256(emitted_payload)
    rejected_hash = canonical_json_sha256(rejected_payload)
    decisions = [
        TradingRuleDecision(
            decision_id=f"trd_{emitted_hash[:16]}",
            decision_hash=emitted_hash,
            **{key: value for key, value in emitted_payload.items() if key != "schema_version"},
        ),
        TradingRuleDecision(
            decision_id=f"trd_{rejected_hash[:16]}",
            decision_hash=rejected_hash,
            **{key: value for key, value in rejected_payload.items() if key != "schema_version"},
        ),
    ]
    plan_payload = {"schema_version": "test_execution_plan_v1"}
    plan_hash = canonical_json_sha256(plan_payload)
    plan_id = f"plan_{plan_hash[:16]}"
    parent = ExecutionPlanIntent(
        intent_id="parent_emitted",
        plan_id=plan_id,
        strategy_id="strategy",
        portfolio_id="portfolio",
        package_id="package",
        release_id="release",
        release_hash="release_hash",
        binding_id="binding",
        binding_hash="binding_hash",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        delta_quantity=100,
        order_quantity=100,
        current_quantity=0,
        rebalance_reason="target_rebalance",
        trading_rule_decision_id=decisions[0].decision_id,
        schedule_window={"mode": "full_day"},
        price_policy={"order_type": "LIMIT"},
    )
    plan = ExecutionPlan(
        plan_id=plan_id,
        strategy_id="strategy",
        portfolio_id="portfolio",
        package_id="package",
        release_id="release",
        release_hash="release_hash",
        binding_id="binding",
        binding_hash="binding_hash",
        selection_evidence_id="selection",
        selection_evidence_hash="selection_hash",
        target_trade_date=date(2026, 7, 13),
        execution_policy_version_id="execution_policy_v1",
        execution_policy_sha256="execution_policy_hash",
        tail_policy_version_id="tail_policy_v1",
        tail_policy_sha256="tail_policy_hash",
        intents=[parent],
        trading_rule_decisions=decisions,
        plan_payload_json=plan_payload,
        plan_hash=plan_hash,
    )

    subjects = build_execution_planning_subjects(plan)
    by_symbol = {subject.symbol: subject for subject in subjects}

    assert len(subjects) == 2
    assert by_symbol["000001.SZ"].emitted_parent_intent_id == "parent_emitted"
    assert by_symbol["000001.SZ"].planning_excluded_quantity == 100
    assert by_symbol["000002.SZ"].planning_decision == "REJECT"
    assert by_symbol["000002.SZ"].emitted_parent_intent_id is None
