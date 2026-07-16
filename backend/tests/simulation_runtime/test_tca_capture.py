from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from backend.services.simulation_runtime.tca_capture import (
    CaptureMergeOutcome,
    TcaBenchmarkPolicy,
    TcaCaptureConfigurationError,
    TcaCaptureDataError,
    build_arrival_benchmark_capture,
    build_capture_error,
    build_execution_planning_subjects,
    build_decision_benchmark_capture,
    build_preflight_eligibility_capture,
    resolve_execution_deadline,
    resolve_tca_benchmark_policy,
)
from backend.services.simulation_runtime.models import (
    ExecutionPlan,
    ExecutionPlanIntent,
    TradingRuleDecision,
    canonical_json_sha256,
)
from backend.services.simulation_runtime.lifecycle import SimulationLifecycleOrchestrator
from backend.services.simulation_runtime.models import SimulationBrokerBackend
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


def _decision_capture(quote_evidence: dict[str, object] | None):
    now = datetime(2026, 7, 16, 1, 30, tzinfo=UTC)
    return build_decision_benchmark_capture(
        execution_plan_id="plan_tca_validation",
        execution_plan_hash="hash_tca_validation",
        parent_intent_id="parent_tca_validation",
        symbol="000001.SZ",
        side="BUY",
        decision_event_at=now,
        quote_evidence=quote_evidence,
        policy=_policy(),
        strategy_decision_price=10.01,
        strategy_decision_source="test",
        strategy_decision_time=None,
        strategy_decision_quality="DIAGNOSTIC",
    )


@pytest.mark.parametrize("invalid_price", ["invalid", True, 0, -1, float("nan"), float("inf")])
def test_quote_capture_rejects_provided_invalid_price(invalid_price: object) -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _decision_capture(
            {
                "bid_price_1": invalid_price,
                "ask_price_1": 10.02,
                "quote_timestamp": "2026-07-16T01:30:00+00:00",
                "received_at": "2026-07-16T01:30:00+00:00",
            }
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_QUOTE_PRICE_INVALID"
    assert exc_info.value.context["field"] == "bid_price_1.bid_price_1"
    assert exc_info.value.context["raw_type"] == type(invalid_price).__name__


def test_quote_capture_rejects_conflicting_price_aliases() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _decision_capture(
            {
                "bid_price_1": 10.0,
                "bidPrice": [10.01],
                "ask_price_1": 10.02,
                "quote_timestamp": "2026-07-16T01:30:00+00:00",
                "received_at": "2026-07-16T01:30:00+00:00",
            }
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_QUOTE_PRICE_ALIAS_CONFLICT"
    assert exc_info.value.context["field"] == "bid_price_1"


@pytest.mark.parametrize("invalid_time", ["not-a-time", "", True])
def test_quote_capture_rejects_provided_invalid_time(invalid_time: object) -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _decision_capture(
            {
                "bid_price_1": 10.0,
                "ask_price_1": 10.02,
                "quote_timestamp": invalid_time,
                "received_at": "2026-07-16T01:30:00+00:00",
            }
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_QUOTE_TIME_INVALID"
    assert exc_info.value.context["field"] == "quote_market_time.quote_timestamp"


def test_quote_capture_rejects_conflicting_time_aliases() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _decision_capture(
            {
                "bid_price_1": 10.0,
                "ask_price_1": 10.02,
                "quote_timestamp": "2026-07-16T01:30:00+00:00",
                "market_time": "2026-07-16T01:30:01+00:00",
                "received_at": "2026-07-16T01:30:00+00:00",
            }
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_QUOTE_TIME_ALIAS_CONFLICT"
    assert exc_info.value.context["field"] == "quote_market_time"


def test_absent_quote_fields_remain_explicit_missing_observation() -> None:
    capture = _decision_capture(None)

    assert capture.quality == "MISSING"
    assert capture.bid_price_1 is None
    assert capture.ask_price_1 is None
    assert capture.quote_market_time is None
    assert capture.raw_quote_sha256 is None


def test_quote_capture_rejects_non_mapping_payload() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _decision_capture([("bid_price_1", 10.0)])  # type: ignore[arg-type]

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_QUOTE_PAYLOAD_INVALID"
    assert exc_info.value.context["field"] == "quote_evidence"


def _preflight_capture(
    *,
    allowed: object = True,
    before_cash: object = 100,
    after_cash: object = 100,
    is_dependent_buy: object = False,
    is_capacity_residual: object = False,
    deadline_context: object = None,
):
    return build_preflight_eligibility_capture(
        parent_intent_id="parent_preflight_validation",
        batch_id="batch_preflight_validation",
        eligibility_as_of=datetime(2026, 7, 16, 1, 31, tzinfo=UTC),
        request_quantity_before_cash=before_cash,  # type: ignore[arg-type]
        request_quantity_after_cash=after_cash,  # type: ignore[arg-type]
        preflight_result={"allowed": allowed, "primary_error_code": None},
        is_dependent_buy=is_dependent_buy,  # type: ignore[arg-type]
        is_capacity_residual=is_capacity_residual,  # type: ignore[arg-type]
        deadline_context=deadline_context,  # type: ignore[arg-type]
    )


@pytest.mark.parametrize("invalid_allowed", ["false", 0, 1, None])
def test_preflight_capture_rejects_non_boolean_allowed(invalid_allowed: object) -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _preflight_capture(allowed=invalid_allowed)

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_PREFLIGHT_ALLOWED_INVALID"
    assert exc_info.value.context["field"] == "preflight_result.allowed"


@pytest.mark.parametrize("invalid_quantity", [-1, True, 1.5, "100"])
def test_preflight_capture_rejects_normalized_quantity(invalid_quantity: object) -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _preflight_capture(before_cash=invalid_quantity)

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_PREFLIGHT_QUANTITY_INVALID"
    assert exc_info.value.context["field"] == "request_quantity_before_cash"


def test_preflight_capture_rejects_quantity_increase_after_cash() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _preflight_capture(before_cash=100, after_cash=200)

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_PREFLIGHT_QUANTITY_CONFLICT"
    assert exc_info.value.context["field"] == "preflight_quantity"


def test_preflight_capture_rejects_non_mapping_payload() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        build_preflight_eligibility_capture(
            parent_intent_id="parent_invalid_payload",
            batch_id="batch_invalid_payload",
            eligibility_as_of=datetime(2026, 7, 16, 1, 31, tzinfo=UTC),
            request_quantity_before_cash=100,
            request_quantity_after_cash=100,
            preflight_result=[("allowed", True)],  # type: ignore[arg-type]
            is_dependent_buy=False,
            is_capacity_residual=False,
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_PREFLIGHT_PAYLOAD_INVALID"
    assert exc_info.value.context["field"] == "preflight_result"


def test_preflight_capture_rejects_conflicting_classification_flags() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _preflight_capture(allowed=False, is_dependent_buy=True, is_capacity_residual=True)

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_PREFLIGHT_CLASSIFICATION_CONFLICT"
    assert exc_info.value.context["field"] == "preflight_classification"


def test_preflight_capture_rejects_invalid_deadline_status() -> None:
    with pytest.raises(TcaCaptureDataError) as exc_info:
        _preflight_capture(
            deadline_context={
                "deadline": None,
                "quality": "UNKNOWN",
                "reason_code": None,
                "schedule_window": {},
            }
        )

    assert exc_info.value.reason_code == "ADAPTIVE_IS_TCA_DEADLINE_STATUS_INVALID"
    assert exc_info.value.context["field"] == "deadline_context.quality"


def test_valid_preflight_capture_preserves_exact_quantity_and_classification() -> None:
    capture = _preflight_capture(
        deadline_context={
            "deadline": datetime(2026, 7, 16, 7, 0, tzinfo=UTC),
            "quality": "RESOLVED",
            "reason_code": None,
            "schedule_window": {"end_time": "15:00:00"},
        }
    )

    assert capture.managed_request_quantity_before_cash == 100
    assert capture.managed_request_quantity_after_cash == 100
    assert capture.eligibility_class == "ELIGIBLE_NOW"
    assert capture.eligible_now_quantity == 100
    assert capture.deadline_quality == "RESOLVED"


def test_arrival_capture_keeps_authoritative_received_at_when_payload_alias_is_invalid() -> None:
    received_at = datetime(2026, 7, 16, 1, 30, 1, tzinfo=UTC)
    capture = build_arrival_benchmark_capture(
        execution_plan_id="plan_arrival",
        execution_plan_hash="hash_arrival",
        parent_intent_id="parent_arrival",
        symbol="000001.SZ",
        side="BUY",
        arrival_time=datetime(2026, 7, 16, 1, 30, tzinfo=UTC),
        arrival_quote_received_at=received_at,
        tick_payload={
            "bid_price_1": 10.0,
            "ask_price_1": 10.02,
            "quote_timestamp": "2026-07-16T01:30:00+00:00",
            "received_at": "invalid-payload-alias",
        },
        policy=_policy(),
    )

    assert capture.quote_received_at == received_at
    assert capture.quality == "VALID"


@pytest.mark.parametrize(
    ("quote_evidence", "expected_reason", "expected_field"),
    [
        (
            [("bid_price_1", 10.0)],
            "ADAPTIVE_IS_TCA_QUOTE_PAYLOAD_INVALID",
            "quote_evidence",
        ),
        (
            {"bid_price_1": "invalid", "ask_price_1": 10.02},
            "ADAPTIVE_IS_TCA_QUOTE_PRICE_INVALID",
            "bid_price_1.bid_price_1",
        ),
    ],
)
def test_lifecycle_decision_capture_preserves_typed_quote_error(
    quote_evidence: object,
    expected_reason: str,
    expected_field: str,
) -> None:
    class _CaptureRepository:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def merge_run_tca_capture_sidecar(self, **kwargs: object) -> CaptureMergeOutcome:
            self.calls.append(dict(kwargs))
            return CaptureMergeOutcome.CREATED

    repository = _CaptureRepository()
    orchestrator = SimulationLifecycleOrchestrator(repository=repository)
    intent = SimpleNamespace(
        intent_id="parent_lifecycle_tca_invalid",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        price_policy={"reference_price": 10.01},
    )
    orchestrator._capture_tca_decision_sidecar(
        run=SimpleNamespace(run_id="run_lifecycle_tca_invalid"),
        binding=SimpleNamespace(broker_backend=SimulationBrokerBackend.MINIQMT_SIM),
        execution_plan=SimpleNamespace(
            plan_id="plan_lifecycle_tca_invalid",
            plan_hash="hash_lifecycle_tca_invalid",
            intents=(intent,),
        ),
        pre_trade_tradability={
            intent.symbol: {"quote_evidence": quote_evidence},  # type: ignore[dict-item]
        },
        execution_policy_payload={
            "policy_json": {
                "algo_config": {
                    "tca": {
                        "benchmark_policy": _policy().model_dump(mode="json"),
                    }
                }
            }
        },
    )

    assert len(repository.calls) == 1
    error = repository.calls[0]["capture_error"]
    assert isinstance(error, dict)
    assert error["reason_code"] == expected_reason
    assert error["context"]["plan_id"] == "plan_lifecycle_tca_invalid"
    assert error["context"]["symbol"] == "000001.SZ"
    assert error["context"]["field"] == expected_field
    assert error["observation_only"] is True
    assert error["execution_gate"] is False
    assert "decision_capture" not in repository.calls[0]


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


def test_invalid_deadline_keeps_explicit_parse_failure_reason() -> None:
    deadline = resolve_execution_deadline(
        schedule_window={"deadline_at": "invalid-deadline"},
        trade_date=date(2026, 7, 13),
    )

    assert deadline["deadline"] is None
    assert deadline["quality"] == "UNRESOLVED"
    assert deadline["reason_code"] == "ADAPTIVE_IS_TCA_DEADLINE_PARSE_FAILED"


def test_capture_error_message_and_context_are_bounded() -> None:
    error = build_capture_error(
        parent_intent_id="parent_bounded_error",
        stage="CAPTURE",
        reason_code="ADAPTIVE_IS_TCA_TEST_INVALID",
        message="m" * 3000,
        context={"k" * 100: "v" * 1000},
        occurred_at=datetime(2026, 7, 16, 1, 32, tzinfo=UTC),
    )

    assert len(error["message"]) == 2048
    assert max(len(key) for key in error["context"]) == 64
    assert len(error["context"]["k" * 64]) == 512
    assert error["retryable"] is False
    assert error["terminal"] is True
    assert error["observation_only"] is True
    assert error["execution_gate"] is False
    assert len(error["error_sha256"]) == 64


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
