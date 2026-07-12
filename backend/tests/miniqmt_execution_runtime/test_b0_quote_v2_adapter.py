from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import Decimal

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    ControlRevision,
    DepthQuantityUnit,
    MarketCode,
    PriceBasis,
    QuoteSourceMethod,
    SessionSegment,
    TradabilitySnapshot,
    TradabilityState,
)
from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.miniqmt_quote_contract_config import QuoteContractPolicy, QuoteIngressRuntimeConfig
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntime,
    MiniQMTExecutionRuntimeConfig,
)
from backend.services.miniqmt_execution_runtime.models import MiniQMTAlgoInstanceStatus
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2Controller,
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
    project_vnpy_tick,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    ActionQuoteEvaluator,
    ActionQuoteRequest,
    BoundedNormalizedQuoteStore,
    NormalizedQuoteObservation,
    OrderingDisposition,
    QuoteEvaluationContext,
    QuoteEvaluationContextStore,
    QuoteSymbolContext,
    build_execution_clock_event,
    deterministic_market_data_id,
)
from backend.services.miniqmt_execution_runtime.quote_evidence import QuoteEvidenceCoordinator
from backend.services.miniqmt_execution_runtime.quote_normalizer import (
    capture_raw_quote_frame,
    normalize_raw_quote_frame,
)
from backend.services.trading_core.models import OrderSide


TRADE_DATE = date(2026, 7, 13)
CLOCK_AT = datetime(2026, 7, 13, 1, 30, tzinfo=UTC)


def _sha(token: str) -> str:
    return token * 64


def _execution_policy() -> dict[str, object]:
    return {
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
            "max_receive_age_ms": 2_000,
            "max_source_lag_ms": 2_000,
            "max_exchange_age_ms": 2_000,
            "max_negative_skew_ms": 20,
            "max_clock_age_divergence_ms": 20,
            "max_dependency_group_skew_ms": 100,
            "auction_mode": "OBSERVE_ONLY",
        }
    }


def _quote_config() -> QuoteIngressRuntimeConfig:
    values = {
        "MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": True,
        "MINIQMT_QUOTE_INGRESS_OWNER_MODE": "simulation_scheduler",
    }
    return QuoteIngressRuntimeConfig.from_mapping(values)


def _context() -> QuoteEvaluationContext:
    segments = (
        SessionSegment(time(9, 15), time(9, 25)),
        SessionSegment(time(9, 30), time(11, 30)),
        SessionSegment(time(13, 0), time(14, 57)),
        SessionSegment(time(14, 57), time(15, 0)),
    )
    calendars = CalendarSnapshotSet(
        snapshot_set_id="calendar-set-p1e",
        snapshot_by_market={
            market: CalendarSnapshot(
                calendar_id=f"calendar-{market.value}-p1e",
                market=market,
                trade_date=TRADE_DATE,
                timezone="Asia/Shanghai",
                session_segments=segments,
                effective_at_utc=CLOCK_AT,
                source_version="calendar-authority-p1e",
            )
            for market in MarketCode
        },
    )
    clock = build_execution_clock_event(
        calendar_snapshot_set=calendars,
        clock_at_utc=CLOCK_AT,
        clock_monotonic_ns=2_000_000_000,
        clock_domain_id="p1e-clock",
        source="simulation_lifecycle_scheduler",
    )
    tradability = TradabilitySnapshot(
        schema_version="adaptive_is_tradability_snapshot_v1",
        tradability_id="tradability-p1e",
        symbol="000001.SZ",
        market=MarketCode.SZ,
        board="MAIN",
        trade_date=TRADE_DATE,
        price_basis=PriceBasis.RAW_CNY_PER_SHARE,
        pre_close=Decimal("10.00"),
        limit_up=Decimal("11.00"),
        limit_down=Decimal("9.00"),
        price_tick=Decimal("0.01"),
        lot_size=100,
        is_suspended=False,
        suspension_source="market.suspend_d",
        security_status="LISTED",
        openint_status=None,
        observed_at_utc=CLOCK_AT,
        source="market.authority.preload",
        source_version="authority-p1e",
        state=TradabilityState.TRADABLE,
    )
    return QuoteEvaluationContext(
        calendar_snapshot_set=calendars,
        clock=clock,
        continuity_generation=1,
        continuity_valid=True,
        policy=QuoteContractPolicy.from_execution_policy(_execution_policy()),
        symbols={
            "000001.SZ": QuoteSymbolContext(
                symbol="000001.SZ",
                board="MAIN",
                depth_quantity_unit=DepthQuantityUnit.SHARES,
                unit_evidence_version="xtdata-depth-unit-v1",
                tradability=tradability,
                product_type="EQUITY",
                product_type_proven_equity=True,
                authority_source_version="authority-p1e",
            )
        },
    )


def _observation(
    context: QuoteEvaluationContext,
    *,
    sequence: int = 1,
    source_time: str = "09300000",
    received_at_utc: datetime = CLOCK_AT,
    received_monotonic_ns: int = 2_000_000_000,
) -> NormalizedQuoteObservation:
    frame = capture_raw_quote_frame(
        {
            "time": source_time,
            "lastPrice": "10.00",
            "preClose": "10.00",
            "bidPrice": ["9.99", "9.98", "9.97", "9.96", "9.95"],
            "bidVol": [1000, 900, 800, 700, 600],
            "askPrice": ["10.01", "10.02", "10.03", "10.04", "10.05"],
            "askVol": [1000, 900, 800, 700, 600],
            "stockStatus": "NORMAL",
            "openint": "OPEN",
        },
        callback_symbol="000001.SZ",
        source_session_id="quote-p1e-session",
        ingress_generation=1,
        ingress_sequence=sequence,
        received_at_utc=received_at_utc,
        received_monotonic_ns=received_monotonic_ns,
        clock_domain_id="p1e-clock",
        source_method=QuoteSourceMethod.WHOLE_QUOTE_CALLBACK,
    )
    tradability = context.symbol_context("000001.SZ").tradability  # type: ignore[union-attr]
    assert tradability is not None
    quote = normalize_raw_quote_frame(
        frame,
        clock_trade_date=TRADE_DATE,
        board="MAIN",
        depth_quantity_unit=DepthQuantityUnit.SHARES,
        unit_evidence_version="xtdata-depth-unit-v1",
        tradability=tradability,
    )
    return NormalizedQuoteObservation(
        frame=frame,
        quote=quote,
        tradability=tradability,
        context_id=context.context_id,
        market_data_id=deterministic_market_data_id(
            frame=frame,
            quote=quote,
            tradability=tradability,
            calendar_snapshot_set=context.calendar_snapshot_set,
            policy=context.policy,
        ),
        ordering_disposition=OrderingDisposition.ACCEPTED,
    )


def _revision(context: QuoteEvaluationContext) -> B0QuoteV2RevisionV1:
    return B0QuoteV2RevisionV1.build(
        execution_policy=_execution_policy(),
        execution_policy_version_id="policy-p1e",
        execution_policy_sha256=_sha("a"),
        adapter_version="adapter-p1e",
        adapter_sha256=_sha("b"),
        code_revision="code-p1e",
        code_sha256=_sha("c"),
        evidence_schema_version="evidence-p1e",
        evidence_schema_sha256=_sha("d"),
        benchmark_policy_version="benchmark-p1e",
        mark_policy_version="mark-p1e",
        markout_max_lag_ms=5_000,
    )


def _runtime_controller() -> tuple[
    B0QuoteV2Controller, MiniQMTExecutionRuntime, FakeMiniQMTGateway, InMemoryMiniQMTExecutionRuntimeRepository
]:
    context = _context()
    observation = _observation(context)
    context_store = QuoteEvaluationContextStore()
    context_store.publish(context)
    normalized_store = BoundedNormalizedQuoteStore(max_symbols=8)
    normalized_store.replace_admitted(("000001.SZ",))
    normalized_store.accept(observation)
    repository = InMemoryMiniQMTExecutionRuntimeRepository()
    gateway = FakeMiniQMTGateway()
    runtime = MiniQMTExecutionRuntime(
        config=MiniQMTExecutionRuntimeConfig(
            runtime_id="runtime-p1e",
            account_group_id="account-p1e",
            trade_date=TRADE_DATE,
            runtime_config_hash=_sha("9"),
        ),
        repository=repository,
        gateway=gateway,
    )
    runtime.start()
    revision = _revision(context)
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-p1e",
        binding_hash=_sha("e"),
        trade_date=TRADE_DATE,
        parent_intent_id="parent-p1e",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    config = _quote_config()
    coordinator = QuoteEvidenceCoordinator(repository=repository, config=config)
    coordinator.observe(observation)
    controller = B0QuoteV2Controller(
        runtime=runtime,
        assignments={assignment.parent_intent_id: assignment},
        normalized_store=normalized_store,
        context_store=context_store,
        evidence_coordinator=coordinator,
        config=config,
        symbols=("000001.SZ",),
    )
    runtime.bind_b0_quote_v2_controller(controller)
    runtime.create_vnpy_algo_instance(
        parent_intent_id="parent-p1e",
        strategy_slot_id="slot-p1e",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=100,
        algo_code="SNIPER_MINIQMT",
        limit_price=10.02,
        metadata={"runtime_child_context": {"price_type": 5}},
    )
    return controller, runtime, gateway, repository


def test_normalized_quote_projects_exact_vnpy_tick_without_time_price_depth_or_unit_fallback() -> None:
    context = _context()
    observation = _observation(context)
    revision = _revision(context)
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-p1e",
        binding_hash=_sha("e"),
        trade_date=TRADE_DATE,
        parent_intent_id="parent-p1e",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    evaluator = ActionQuoteEvaluator()
    eligibility = evaluator.evaluate(
        request=ActionQuoteRequest(
            runtime_id="runtime-p1e",
            parent_intent_id="parent-p1e",
            algo_instance_id="algo-p1e",
            symbol="000001.SZ",
            side="BUY",
            control_revision=ControlRevision.B0_QUOTE_V2,
            policy_sha256=context.policy.policy_sha256,
            config_sha256=_sha("f"),
            adapter_sha256=revision.adapter_sha256,
        ),
        context=context,
        observation=observation,
    ).eligibility

    tick = project_vnpy_tick(observation=observation, eligibility=eligibility, assignment=assignment)

    assert tick.datetime == observation.quote.source_exchange_time_utc
    assert tick.bid_price_1 == 9.99
    assert tick.ask_volume_1 == 1000
    assert tick.raw["market_data_id"] == observation.market_data_id
    assert tick.raw["config_sha256"] == _sha("f")


def test_action_pending_event_and_durable_action_receipt_precede_gateway_submit() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()

    controller.lifecycle_tick(now_utc=CLOCK_AT)

    events = repository.list_events("runtime-p1e", include_archived=True)
    pending_index = next(
        index
        for index, event in enumerate(events)
        if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    evidence_index = next(
        index for index, event in enumerate(events) if event.event_type.value == "QUOTE_ELIGIBILITY_EVALUATED"
    )
    child_index = next(index for index, event in enumerate(events) if event.event_type.value == "CHILD_ORDER_SUBMITTED")
    receipt_index = next(
        index
        for index, event in enumerate(events)
        if event.payload.get("evidence", {}).get("capture_type") == "CHILD_RECEIPT"
    )
    assert pending_index < evidence_index < child_index < receipt_index
    assert len(gateway.submitted_orders) == 1
    child = repository.list_child_orders("runtime-p1e", active_only=False)[0]
    assert child.child_order_id.startswith("mqchild_")


def test_reject_or_persist_failure_keeps_action_pending_and_gateway_call_count_zero() -> None:
    controller, _runtime, gateway, _repository = _runtime_controller()
    context = controller.context_store.snapshot()
    assert context is not None
    stale = _observation(
        context,
        sequence=2,
        source_time="09295500",
        received_at_utc=datetime(2026, 7, 13, 1, 29, 55, tzinfo=UTC),
        received_monotonic_ns=1_995_000_000,
    )
    controller.normalized_store.accept(stale)

    controller.lifecycle_tick(now_utc=CLOCK_AT)

    assert gateway.submitted_orders == []


def test_projection_rejects_cross_observation_market_data_identity() -> None:
    context = _context()
    first = _observation(context, sequence=1)
    second = replace(_observation(context, sequence=2), market_data_id="md_cross_observation")
    revision = _revision(context)
    assignment = ParentQuoteControlAssignmentV1.build(
        binding_id="binding-p1e",
        binding_hash=_sha("e"),
        trade_date=TRADE_DATE,
        parent_intent_id="parent-p1e",
        control_revision=ControlRevision.B0_QUOTE_V2,
        revision=revision,
    )
    eligibility = (
        ActionQuoteEvaluator()
        .evaluate(
            request=ActionQuoteRequest(
                runtime_id="runtime-p1e",
                parent_intent_id="parent-p1e",
                algo_instance_id="algo-p1e",
                symbol="000001.SZ",
                side="BUY",
                control_revision=ControlRevision.B0_QUOTE_V2,
                policy_sha256=context.policy.policy_sha256,
                config_sha256=_sha("f"),
                adapter_sha256=revision.adapter_sha256,
            ),
            context=context,
            observation=first,
        )
        .eligibility
    )

    with pytest.raises(QuoteContractError):
        project_vnpy_tick(observation=second, eligibility=eligibility, assignment=assignment)


def test_authoritative_trade_attaches_markout_anchor_and_schedules_all_horizons() -> None:
    controller, runtime, _gateway, repository = _runtime_controller()
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    child = repository.list_child_orders("runtime-p1e", active_only=False)[0]

    event = runtime.record_trade_event(
        broker_order_id=str(child.broker_order_id),
        quantity=100,
        price=10.01,
        payload={
            "trade_id": "trade-p1e",
            "trade_time_utc": CLOCK_AT.isoformat(),
            "cumulative_quantity": 100,
        },
    )

    assert event.payload["quote_evidence_markout_anchor_v1"]["child_receipt_evidence_id"]
    assert {key[2] for key in controller.evidence_coordinator._pending} == {60, 300, 900}


def test_timer_emitted_submit_is_subject_to_the_same_quote_and_evidence_contract() -> None:
    controller, runtime, gateway, repository = _runtime_controller()
    sniper = repository.list_algo_instances("runtime-p1e", active_only=True)[0]
    repository.upsert_algo_instance(
        sniper.model_copy(update={"status": MiniQMTAlgoInstanceStatus.COMPLETED, "remaining_quantity": 0})
    )
    runtime.create_vnpy_algo_instance(
        parent_intent_id="parent-p1e",
        strategy_slot_id="slot-twap-p1e",
        symbol="000001.SZ",
        side=OrderSide.BUY,
        target_quantity=200,
        algo_code="TWAP_LITE_MINIQMT",
        limit_price=10.02,
        algo_config={"time": 120, "interval": 60},
        metadata={"runtime_child_context": {"price_type": 5}},
    )
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    for ordinal in range(60):
        runtime.on_timer(timer_name="event_loop", payload={"ordinal": ordinal})

    assert gateway.submitted_orders == []
    assert controller.health()["pending_action_count"] == 1

    controller.lifecycle_tick(now_utc=CLOCK_AT)

    assert len(gateway.submitted_orders) == 1
    events = repository.list_events("runtime-p1e", include_archived=True)
    evidence_sequence = next(
        event.sequence
        for event in events
        if event.payload.get("evidence", {}).get("capture_type") == "ACTION_INPUT"
        and event.payload.get("evidence", {}).get("algo_instance_id") != sniper.algo_instance_id
    )
    child_sequence = next(
        event.sequence
        for event in events
        if event.event_type.value == "CHILD_ORDER_SUBMITTED"
        and event.payload.get("algo_instance_id") != sniper.algo_instance_id
    )
    assert evidence_sequence < child_sequence


def test_child_event_and_receipt_rebuild_bidirectional_market_data_chain() -> None:
    controller, _runtime, _gateway, repository = _runtime_controller()
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    events = repository.list_events("runtime-p1e", include_archived=True)
    action_event = next(
        event for event in events if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    action = action_event.payload["b0_quote_v2_action"]
    child_event = next(event for event in events if event.event_type.value == "CHILD_ORDER_SUBMITTED")
    receipt_event = next(
        event for event in events if event.payload.get("evidence", {}).get("capture_type") == "CHILD_RECEIPT"
    )
    receipt = receipt_event.payload["evidence"]

    assert child_event.payload["action_id"] == action["action_id"]
    assert child_event.payload["action_evidence_id"] == action["action_evidence_id"]
    assert child_event.payload["action_market_data_id"] == action["action_market_data_id"]
    assert receipt["source_child_event_id"] == child_event.event_id
    assert receipt["action_evidence_id"] == action["action_evidence_id"]
    assert receipt["anchor_market_data_id"] == action["action_market_data_id"]
    assert receipt["child_receipt_evidence_id"] == receipt["evidence_id"]
