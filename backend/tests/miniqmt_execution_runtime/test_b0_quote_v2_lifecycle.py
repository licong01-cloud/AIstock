from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.miniqmt_execution_runtime import (
    FakeMiniQMTGateway,
    InMemoryMiniQMTExecutionRuntimeRepository,
    MiniQMTExecutionRuntime,
)
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2ControllerFactory,
    ParentQuoteControlAssignmentV1,
)
from backend.services.miniqmt_execution_runtime.quote_eligibility import QuoteEvaluationContext

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import CLOCK_AT, _runtime_controller


class _LifecycleSupervisor:
    def __init__(self, source_controller) -> None:  # type: ignore[no-untyped-def]
        self.normalized_store = source_controller.normalized_store
        self.context_store = source_controller.context_store
        self.sinks: dict[str, object] = {}
        self.leases: dict[str, tuple[str, ...]] = {}
        self.physical_feed_start_count = 0

    def register_observation_sink(self, *, consumer_id: str, sink) -> None:  # type: ignore[no-untyped-def]
        if consumer_id in self.sinks:
            raise AssertionError("duplicate sink")
        self.sinks[consumer_id] = sink

    def unregister_observation_sink(self, *, consumer_id: str) -> None:
        self.sinks.pop(consumer_id, None)

    def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
        if not self.leases:
            self.physical_feed_start_count += 1
        self.leases[consumer_id] = tuple(symbols)

    def release_consumer(self, *, consumer_id: str) -> None:
        self.leases.pop(consumer_id, None)


def test_lifecycle_tick_advances_clock_and_keeps_exact_observation_authority_pairing() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()
    observation = controller.normalized_store.get("000001.SZ")
    original = controller.context_store.snapshot()
    assert observation is not None and original is not None
    controller.observe(observation)
    sampled_at = CLOCK_AT + timedelta(milliseconds=500)
    sampled_monotonic_ns = original.clock.clock_monotonic_ns + 500_000_000

    def advance_clock(*, clock_at_utc, clock_monotonic_ns):  # type: ignore[no-untyped-def]
        advanced = QuoteEvaluationContext(
            calendar_snapshot_set=original.calendar_snapshot_set,
            clock=replace(
                original.clock,
                clock_event_id="clock-p1e-callback-current",
                clock_at_utc=clock_at_utc,
                clock_monotonic_ns=clock_monotonic_ns,
                observed_at_utc=clock_at_utc,
            ),
            continuity_generation=original.continuity_generation,
            continuity_valid=True,
            policy=original.policy,
            symbols=original.symbols,
        )
        controller.context_store.publish(advanced)
        return advanced

    controller._context_advance_callback = advance_clock
    controller._clock_sample_provider = lambda: (sampled_at, sampled_monotonic_ns)

    controller.lifecycle_tick()

    assert controller.context_store.snapshot().clock.clock_at_utc == sampled_at  # type: ignore[union-attr]
    assert len(gateway.submitted_orders) == 1
    assert any(event.event_type.value == "CHILD_ORDER_SUBMITTED" for event in repository.list_events("runtime-p1e"))


def test_lifecycle_without_observation_persists_runtime_wait_not_quote_less_action_reject() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()
    controller.normalized_store._latest_by_symbol.clear()

    first = controller.lifecycle_tick(now_utc=CLOCK_AT)
    second = controller.lifecycle_tick(now_utc=CLOCK_AT)

    events = repository.list_events("runtime-p1e", include_archived=True)
    waiting = [
        event
        for event in events
        if event.payload.get("schema_version") == "b0_quote_v2_quote_waiting_v1"
    ]
    assert len(waiting) == 1
    assert waiting[0].event_type.value == "TIMER"
    assert waiting[0].source == "quote_ingress"
    assert waiting[0].payload["eligibility_state"] == "WAITING_FIRST_QUOTE"
    assert waiting[0].payload["reason_code"] == "ADAPTIVE_IS_QUOTE_BOOTSTRAP_INCOMPLETE"
    assert waiting[0].payload["market_data_id"] is None
    assert waiting[0].payload["raw_ingress_identity_available"] is False
    assert waiting[0].payload["broker_called"] is False
    assert not any(event.event_type.value == "QUOTE_REJECTED" for event in events)
    assert first["b0_quote_v2_waiting_for_quote_algo_count"] == 1
    assert second["b0_quote_v2_quote_wait_event_total"] == 1
    assert gateway.submitted_orders == []


def test_only_scheduler_constructs_controller_and_read_only_paths_never_start_ingress() -> None:
    source, runtime, _gateway, _repository = _runtime_controller()
    supervisor = _LifecycleSupervisor(source)
    released_contexts: list[str] = []
    factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=source.config,
        data_session_key="sim-session-p1e",
        context_release_callback=released_contexts.append,
    )

    controller = factory.create(
        runtime=runtime,
        assignments=source.assignments,
        symbols=tuple(source.symbols),
    )

    assert factory.get(runtime.config.runtime_id) is controller
    assert supervisor.physical_feed_start_count == 1
    assert tuple(supervisor.leases) == ("b0qv2:runtime-p1e",)
    assert factory.health()["controller_count"] == 1
    with pytest.raises(QuoteContractError):
        factory.create(runtime=runtime, assignments=source.assignments, symbols=tuple(source.symbols))

    factory.release(runtime.config.runtime_id)
    assert factory.get(runtime.config.runtime_id) is None
    assert supervisor.leases == {}
    assert supervisor.sinks == {}
    assert released_contexts == [runtime.config.runtime_id]


def test_runtime_leases_share_physical_feed_but_isolate_coordinator_and_symbol_failure() -> None:
    first_source, first_runtime, _first_gateway, _first_repository = _runtime_controller()
    second_source, second_runtime, _second_gateway, _second_repository = _runtime_controller()
    second_runtime.config = second_runtime.config.model_copy(update={"runtime_id": "runtime-p1e-second"})
    supervisor = _LifecycleSupervisor(first_source)
    factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=first_source.config,
        data_session_key="sim-session-p1e",
    )

    first = factory.create(
        runtime=first_runtime,
        assignments=first_source.assignments,
        symbols=tuple(first_source.symbols),
    )
    second = factory.create(
        runtime=second_runtime,
        assignments=second_source.assignments,
        symbols=tuple(second_source.symbols),
    )

    assert supervisor.physical_feed_start_count == 1
    assert len(supervisor.leases) == 2
    assert first.evidence_coordinator is not second.evidence_coordinator
    first.evidence_coordinator._failed_symbols.add("000001.SZ")
    assert "000001.SZ" not in second.evidence_coordinator._failed_symbols


def test_switch_false_drains_active_parent_and_rejects_only_new_assignment() -> None:
    source, runtime, _gateway, _repository = _runtime_controller()
    supervisor = _LifecycleSupervisor(source)
    factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=source.config,
        data_session_key="sim-session-p1e",
    )
    factory.set_accept_new_assignments(False)

    with pytest.raises(QuoteContractError):
        factory.create(runtime=runtime, assignments=source.assignments, symbols=tuple(source.symbols))

    draining = factory.create(
        runtime=runtime,
        assignments=source.assignments,
        symbols=tuple(source.symbols),
        recovering_active=True,
    )

    assert factory.health()["lifecycle_state"] == "DRAINING"
    assert factory.get(runtime.config.runtime_id) is draining
    assert supervisor.leases["b0qv2:runtime-p1e"] == ("000001.SZ",)


def test_assignment_transition_releases_empty_previous_runtime_before_new_context_is_observable() -> None:
    source, source_runtime, _gateway, _repository = _runtime_controller()
    repository = InMemoryMiniQMTExecutionRuntimeRepository()
    runtime = MiniQMTExecutionRuntime(
        config=source_runtime.config.model_copy(update={"runtime_id": "runtime-empty-transition"}),
        repository=repository,
        gateway=FakeMiniQMTGateway(),
    )
    runtime.start()
    supervisor = _LifecycleSupervisor(source)
    released_contexts: list[str] = []
    factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=source.config,
        data_session_key="sim-session-p1e",
        context_release_callback=released_contexts.append,
    )
    controller = factory.create(
        runtime=runtime,
        assignments=source.assignments,
        symbols=tuple(source.symbols),
    )
    prior_assignment = next(iter(source.assignments.values()))
    incoming = ParentQuoteControlAssignmentV1.build(
        binding_id=prior_assignment.binding_id,
        binding_hash=prior_assignment.binding_hash,
        trade_date=prior_assignment.trade_date,
        parent_intent_id="parent-p1e-rebuilt",
        control_revision=prior_assignment.control_revision,
        revision=prior_assignment.revision,
    )

    factory.prepare_assignment_transition(
        runtime_id="runtime-rebuilt-transition",
        assignments={incoming.parent_intent_id: incoming},
    )

    assert factory.get(runtime.config.runtime_id) is None
    assert controller.health()["status"] == "CLOSED"
    assert supervisor.leases == {}
    assert supervisor.sinks == {}
    assert released_contexts == [runtime.config.runtime_id]


def test_assignment_transition_rejects_active_or_child_runtime_without_releasing_lease() -> None:
    source, runtime, gateway, _repository = _runtime_controller()
    supervisor = _LifecycleSupervisor(source)
    factory = B0QuoteV2ControllerFactory(
        supervisor=supervisor,
        config=source.config,
        data_session_key="sim-session-p1e",
    )
    controller = factory.create(
        runtime=runtime,
        assignments=source.assignments,
        symbols=tuple(source.symbols),
    )
    prior_assignment = next(iter(source.assignments.values()))
    incoming = ParentQuoteControlAssignmentV1.build(
        binding_id=prior_assignment.binding_id,
        binding_hash=prior_assignment.binding_hash,
        trade_date=prior_assignment.trade_date,
        parent_intent_id="parent-p1e-conflicting",
        control_revision=prior_assignment.control_revision,
        revision=prior_assignment.revision,
    )

    with pytest.raises(QuoteContractError, match="exact non-empty parent mapping"):
        factory.prepare_assignment_transition(
            runtime_id="runtime-invalid-empty-transition",
            assignments={},
        )
    with pytest.raises(QuoteContractError, match="cannot replace non-empty durable runtime state") as exc_info:
        factory.prepare_assignment_transition(
            runtime_id="runtime-conflicting-transition",
            assignments={incoming.parent_intent_id: incoming},
        )

    assert exc_info.value.context["active_algo_count"] == 1
    assert exc_info.value.context["child_order_count"] == 0
    assert exc_info.value.context["broker_called"] is False
    assert factory.get(runtime.config.runtime_id) is controller
    assert supervisor.leases["b0qv2:runtime-p1e"] == ("000001.SZ",)
    assert gateway.submitted_orders == []
