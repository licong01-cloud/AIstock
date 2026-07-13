from __future__ import annotations

from dataclasses import replace

import pytest

from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import B0QuoteV2Controller, _action_from_payload
from backend.services.miniqmt_execution_runtime.quote_evidence import QuoteEvidenceCoordinator

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import CLOCK_AT, _runtime_controller


def _restart_controller(controller: B0QuoteV2Controller) -> B0QuoteV2Controller:
    return B0QuoteV2Controller(
        runtime=controller.runtime,
        assignments=controller.assignments,
        normalized_store=controller.normalized_store,
        context_store=controller.context_store,
        evidence_coordinator=QuoteEvidenceCoordinator(
            repository=controller.runtime.repository,
            config=controller.config,
        ),
        config=controller.config,
        symbols=tuple(controller.symbols),
    )


def test_restart_submits_durable_action_without_child_once() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()
    original_persist = controller.evidence_coordinator._repository.append_evidence_event_idempotent

    def fail_before_durable_ack(candidate):  # type: ignore[no-untyped-def]
        raise ConnectionError("injected transient evidence persistence failure")

    controller.evidence_coordinator._repository.append_evidence_event_idempotent = fail_before_durable_ack  # type: ignore[method-assign]
    controller.lifecycle_tick(now_utc=CLOCK_AT)

    assert gateway.submitted_orders == []
    assert controller.health()["pending_action_count"] == 1
    pending_event = next(
        event
        for event in repository.list_events("runtime-p1e", include_archived=True)
        if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    assert pending_event.payload["action_evidence_candidate"]["evidence_sha256"]

    controller.evidence_coordinator._repository.append_evidence_event_idempotent = original_persist  # type: ignore[method-assign]
    recovered = _restart_controller(controller)
    recovered.lifecycle_tick(now_utc=CLOCK_AT)

    assert len(gateway.submitted_orders) == 1
    assert len(repository.list_child_orders("runtime-p1e", active_only=False)) == 1


def test_restart_with_child_or_unknown_broker_outcome_reconciles_without_resubmit() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    submitted = tuple(gateway.submitted_orders)

    recovered = _restart_controller(controller)
    recovered.lifecycle_tick(now_utc=CLOCK_AT)

    assert tuple(gateway.submitted_orders) == submitted
    assert len(repository.list_child_orders("runtime-p1e", active_only=False)) == 1
    assert recovered.health()["b0_quote_v2_duplicate_prevented_total"] == 1


def test_same_action_has_deterministic_child_and_conflicting_payload_fails() -> None:
    controller, runtime, gateway, repository = _runtime_controller()
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    child = repository.list_child_orders("runtime-p1e", active_only=False)[0]
    child_event = next(
        event
        for event in repository.list_events("runtime-p1e", include_archived=True)
        if event.event_type.value == "CHILD_ORDER_SUBMITTED"
    )
    action_event = next(
        event
        for event in repository.list_events("runtime-p1e", include_archived=True)
        if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    action = _action_from_payload(action_event.payload["vnpy_action"])
    instance = repository.list_algo_instances("runtime-p1e", active_only=False)[0]

    replay_child, replay_event = runtime.submit_b0_quote_v2_child(
        instance=instance,
        action=action,
        child_order_id=child.child_order_id,
        metadata=dict(child.metadata),
    )

    assert replay_child.child_order_id == child.child_order_id
    assert replay_event.event_id == child_event.event_id
    assert len(gateway.submitted_orders) == 1

    with pytest.raises(QuoteContractError):
        runtime.submit_b0_quote_v2_child(
            instance=instance,
            action=replace(action, price=float(action.price or 0) + 0.01),
            child_order_id=child.child_order_id,
            metadata=dict(child.metadata),
        )
