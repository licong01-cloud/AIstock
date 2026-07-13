from __future__ import annotations

from copy import deepcopy

import pytest

from backend.execution_algos.adaptive_is.reasons import QuoteContractError, QuoteContractReasonCode
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    B0QuoteV2Controller,
    B0QuoteV2ControllerFactory,
    B0QuoteV2RevisionV1,
    ParentQuoteControlAssignmentV1,
    QuoteControlBindingV1,
    _action_from_payload,
    evidence_schema_manifest_payload,
    quote_evidence_policy,
    source_build_manifest,
)
from backend.execution_algos.adaptive_is.contracts import ControlRevision
from backend.services.miniqmt_execution_runtime.client import _b0_quote_v2_assignments
from backend.services.miniqmt_execution_runtime.quote_evidence import QuoteEvidenceCoordinator
from backend.services.trading_core.errors import BrokerSubmitError

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import CLOCK_AT, _runtime_controller
from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_lifecycle import _LifecycleSupervisor


def _restart(controller: B0QuoteV2Controller) -> B0QuoteV2Controller:
    return B0QuoteV2Controller(
        runtime=controller.runtime,
        assignments=controller.assignments,
        normalized_store=controller.normalized_store,
        context_store=controller.context_store,
        evidence_coordinator=QuoteEvidenceCoordinator(
            repository=controller.runtime.repository, config=controller.config
        ),
        config=controller.config,
        symbols=tuple(controller.symbols),
    )


def test_build_and_evidence_manifests_are_content_addressed_and_exact() -> None:
    manifest = source_build_manifest()
    schema = evidence_schema_manifest_payload()

    assert len(manifest.adapter_sha256) == 64
    assert len(manifest.code_sha256) == 64
    assert len(manifest.evidence_schema_sha256) == 64
    assert schema["event_type_by_capture_type"]["ACTION_INPUT"] == "QUOTE_ELIGIBILITY_EVALUATED"
    assert {field["name"] for field in schema["market_data_evidence_fields"]} >= {
        "market_data_id",
        "action_evidence_id",
        "child_receipt_evidence_id",
    }


def test_binding_policy_and_assignment_validation_branches_all_fail_loud() -> None:
    source, _runtime, _gateway, _repository = _runtime_controller()
    assignment = next(iter(source.assignments.values()))
    revision = assignment.revision
    assert revision is not None

    for binding_payload in (
        [],
        {"miniqmt_quote_control": []},
        {"miniqmt_quote_control": {"schema_version": "wrong", "control_revision": "B0_QUOTE_V2"}},
    ):
        with pytest.raises(QuoteContractError):
            QuoteControlBindingV1.from_binding_config(binding_payload)  # type: ignore[arg-type]

    with pytest.raises(QuoteContractError):
        B0QuoteV2RevisionV1.build(
            execution_policy={
                "quote_contract": {
                    "schema_version": "miniqmt_quote_contract_policy_v2",
                    "control_revision": "LEGACY_B0",
                    "required_capabilities": [],
                    "max_receive_age_ms": 1,
                    "max_source_lag_ms": 1,
                    "max_exchange_age_ms": 1,
                    "max_negative_skew_ms": 0,
                    "max_clock_age_divergence_ms": 0,
                    "max_dependency_group_skew_ms": 0,
                    "auction_mode": "OBSERVE_ONLY",
                }
            },
            execution_policy_version_id="legacy",
            execution_policy_sha256="a" * 64,
            adapter_version="adapter",
            adapter_sha256="b" * 64,
            code_revision="code",
            code_sha256="c" * 64,
            evidence_schema_version="schema",
            evidence_schema_sha256="d" * 64,
            benchmark_policy_version="benchmark",
            mark_policy_version="mark",
            markout_max_lag_ms=1,
        )
    with pytest.raises(QuoteContractError):
        B0QuoteV2RevisionV1.build(
            execution_policy={
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
                    "max_receive_age_ms": 1,
                    "max_source_lag_ms": 1,
                    "max_exchange_age_ms": 1,
                    "max_negative_skew_ms": 0,
                    "max_clock_age_divergence_ms": 0,
                    "max_dependency_group_skew_ms": 0,
                    "auction_mode": "OBSERVE_ONLY",
                }
            },
            execution_policy_version_id="b0",
            execution_policy_sha256="a" * 64,
            adapter_version="adapter",
            adapter_sha256="b" * 64,
            code_revision="code",
            code_sha256="c" * 64,
            evidence_schema_version="schema",
            evidence_schema_sha256="d" * 64,
            benchmark_policy_version="benchmark",
            mark_policy_version="mark",
            markout_max_lag_ms=True,  # type: ignore[arg-type]
        )

    for policy in (
        {},
        {"quote_evidence": {"schema_version": "wrong"}},
        {
            "quote_evidence": {
                "schema_version": "miniqmt_quote_evidence_policy_v1",
                "benchmark_policy_version": "benchmark",
                "mark_policy_version": "mark",
                "markout_max_lag_ms": -1,
            }
        },
    ):
        with pytest.raises(QuoteContractError):
            quote_evidence_policy(policy)
    assert quote_evidence_policy(
        {
            "quote_evidence": {
                "schema_version": "miniqmt_quote_evidence_policy_v1",
                "benchmark_policy_version": "benchmark",
                "mark_policy_version": "mark",
                "markout_max_lag_ms": 5000,
            }
        }
    ) == ("benchmark", "mark", 5000)

    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.build(
            binding_id="binding",
            binding_hash="e" * 64,
            trade_date="2026-07-13",  # type: ignore[arg-type]
            parent_intent_id="parent",
            control_revision=ControlRevision.LEGACY_B0,
            revision=None,
        )
    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.build(
            binding_id="binding",
            binding_hash="e" * 64,
            trade_date=assignment.trade_date,
            parent_intent_id="parent",
            control_revision=ControlRevision.LEGACY_B0,
            revision=revision,
        )
    legacy = ParentQuoteControlAssignmentV1.build(
        binding_id="binding",
        binding_hash="e" * 64,
        trade_date=assignment.trade_date,
        parent_intent_id="parent",
        control_revision=ControlRevision.LEGACY_B0,
        revision=None,
    )
    assert ParentQuoteControlAssignmentV1.from_payload(legacy.canonical_payload()) == legacy
    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.from_payload({**legacy.canonical_payload(), "trade_date": "invalid"})
    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.from_payload(assignment.canonical_payload())


def test_revision_and_assignment_readback_reject_unknown_missing_or_changed_identity() -> None:
    source, _runtime, _gateway, _repository = _runtime_controller()
    assignment = next(iter(source.assignments.values()))
    revision = assignment.revision
    assert revision is not None
    revision_payload = revision.canonical_payload()

    assert B0QuoteV2RevisionV1.from_payload(revision_payload) == revision
    for mutation in (
        {**revision_payload, "unknown": True},
        {key: value for key, value in revision_payload.items() if key != "revision_id"},
        {**revision_payload, "adapter_sha256": "0" * 64},
    ):
        with pytest.raises(QuoteContractError):
            B0QuoteV2RevisionV1.from_payload(mutation)

    assignment_payload = assignment.canonical_payload()
    assert ParentQuoteControlAssignmentV1.from_plan_payload(assignment_payload, revision=revision) == assignment
    with pytest.raises(QuoteContractError):
        ParentQuoteControlAssignmentV1.from_plan_payload(
            {**assignment_payload, "binding_hash": "0" * 64},
            revision=revision,
        )


def test_pending_controller_cannot_close_and_closed_controller_cannot_tick() -> None:
    controller, _runtime, _gateway, _repository = _runtime_controller()
    original = controller.evidence_coordinator._repository.append_evidence_event_idempotent
    controller.evidence_coordinator._repository.append_evidence_event_idempotent = (  # type: ignore[method-assign]
        lambda _candidate: (_ for _ in ()).throw(ConnectionError("injected"))
    )
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    with pytest.raises(QuoteContractError):
        controller.close()

    controller.evidence_coordinator._repository.append_evidence_event_idempotent = original  # type: ignore[method-assign]
    recovered = _restart(controller)
    recovered.lifecycle_tick(now_utc=CLOCK_AT)
    recovered.close()
    with pytest.raises(QuoteContractError):
        recovered.lifecycle_tick(now_utc=CLOCK_AT)


def test_factory_rolls_back_sink_when_lease_acquisition_fails_and_rejects_empty_symbol_scope() -> None:
    source, runtime, _gateway, _repository = _runtime_controller()

    class FailingSupervisor(_LifecycleSupervisor):
        def acquire_consumer(self, *, consumer_id: str, symbols: list[str]) -> None:
            super().acquire_consumer(consumer_id=consumer_id, symbols=symbols)
            raise RuntimeError("injected lease failure")

    empty_factory = B0QuoteV2ControllerFactory(
        supervisor=_LifecycleSupervisor(source), config=source.config, data_session_key="empty-symbols"
    )
    with pytest.raises(QuoteContractError):
        empty_factory.create(runtime=runtime, assignments=source.assignments, symbols=())

    supervisor = FailingSupervisor(source)
    factory = B0QuoteV2ControllerFactory(supervisor=supervisor, config=source.config, data_session_key="lease-failure")
    with pytest.raises(RuntimeError, match="lease failure"):
        factory.create(runtime=runtime, assignments=source.assignments, symbols=tuple(source.symbols))
    assert supervisor.sinks == {}
    assert factory.health()["controller_count"] == 0


def test_recovery_rejects_tampered_immutable_evidence_candidate_before_broker() -> None:
    controller, _runtime, gateway, repository = _runtime_controller()
    controller.evidence_coordinator._repository.append_evidence_event_idempotent = (  # type: ignore[method-assign]
        lambda _candidate: (_ for _ in ()).throw(ConnectionError("injected"))
    )
    controller.lifecycle_tick(now_utc=CLOCK_AT)
    events = repository._events["runtime-p1e"]
    index = next(
        index
        for index, event in enumerate(events)
        if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    payload = deepcopy(events[index].payload)
    payload["action_evidence_candidate"]["evidence_sha256"] = "0" * 64
    events[index] = events[index].model_copy(update={"payload": payload})

    with pytest.raises(QuoteContractError) as exc_info:
        _restart(controller)

    assert exc_info.value.reason_code == QuoteContractReasonCode.B0_QUOTE_V2_ACTION_RECOVERY_CONFLICT
    assert gateway.submitted_orders == []


@pytest.mark.parametrize(
    "quote_control",
    (
        [],
        {"binding": {}},
        {"binding": [], "revision": None, "assignments": []},
        {
            "binding": {"schema_version": "miniqmt_quote_control_binding_v1", "control_revision": "B0_QUOTE_V2"},
            "revision": None,
            "assignments": [],
        },
    ),
)
def test_client_assignment_projection_rejects_non_exact_payload_without_broker(quote_control: object) -> None:
    with pytest.raises((BrokerSubmitError, QuoteContractError)):
        _b0_quote_v2_assignments(policy_context={"quote_control": quote_control}, parent_intent_ids={"parent-p1e"})


def test_client_assignment_projection_accepts_frozen_current_build_manifest() -> None:
    source, _runtime, _gateway, _repository = _runtime_controller()
    assignment = next(iter(source.assignments.values()))
    revision = assignment.revision
    assert revision is not None
    manifest = source_build_manifest()
    current_revision = B0QuoteV2RevisionV1.build(
        execution_policy={
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
        },
        execution_policy_version_id=revision.execution_policy_version_id,
        execution_policy_sha256=revision.execution_policy_sha256,
        adapter_version=manifest.adapter_version,
        adapter_sha256=manifest.adapter_sha256,
        code_revision=manifest.code_revision,
        code_sha256=manifest.code_sha256,
        evidence_schema_version=manifest.evidence_schema_version,
        evidence_schema_sha256=manifest.evidence_schema_sha256,
        benchmark_policy_version=revision.benchmark_policy_version,
        mark_policy_version=revision.mark_policy_version,
        markout_max_lag_ms=revision.markout_max_lag_ms,
    )
    current_assignment = ParentQuoteControlAssignmentV1.build(
        binding_id=assignment.binding_id,
        binding_hash=assignment.binding_hash,
        trade_date=assignment.trade_date,
        parent_intent_id=assignment.parent_intent_id,
        control_revision=assignment.control_revision,
        revision=current_revision,
    )
    binding = {"schema_version": "miniqmt_quote_control_binding_v1", "control_revision": "B0_QUOTE_V2"}

    parsed_revision, assignments = _b0_quote_v2_assignments(
        policy_context={
            "quote_control": {
                "binding": binding,
                "revision": current_revision.canonical_payload(),
                "assignments": [current_assignment.canonical_payload()],
            }
        },
        parent_intent_ids={"parent-p1e"},
    )

    assert parsed_revision == current_revision
    assert assignments == {"parent-p1e": current_assignment}

    legacy_context = {
        "quote_control": {
            "binding": {"schema_version": "miniqmt_quote_control_binding_v1", "control_revision": "LEGACY_B0"},
            "revision": None,
            "assignments": [],
        }
    }
    assert _b0_quote_v2_assignments(policy_context=legacy_context, parent_intent_ids={"parent-p1e"}) == (None, {})
    with pytest.raises(BrokerSubmitError):
        _b0_quote_v2_assignments(
            policy_context={
                "quote_control": {
                    **legacy_context["quote_control"],
                    "revision": current_revision.canonical_payload(),
                }
            },
            parent_intent_ids={"parent-p1e"},
        )
    with pytest.raises(BrokerSubmitError):
        _b0_quote_v2_assignments(
            policy_context={
                "quote_control": {
                    "binding": binding,
                    "revision": revision.canonical_payload(),
                    "assignments": [assignment.canonical_payload()],
                }
            },
            parent_intent_ids={"parent-p1e"},
        )
    with pytest.raises(BrokerSubmitError):
        _b0_quote_v2_assignments(
            policy_context={
                "quote_control": {
                    "binding": binding,
                    "revision": current_revision.canonical_payload(),
                    "assignments": [1],
                }
            },
            parent_intent_ids={"parent-p1e"},
        )
    with pytest.raises(BrokerSubmitError):
        _b0_quote_v2_assignments(
            policy_context={
                "quote_control": {
                    "binding": binding,
                    "revision": current_revision.canonical_payload(),
                    "assignments": [current_assignment.canonical_payload(), current_assignment.canonical_payload()],
                }
            },
            parent_intent_ids={"parent-p1e"},
        )
    with pytest.raises(BrokerSubmitError):
        _b0_quote_v2_assignments(
            policy_context={
                "quote_control": {
                    "binding": binding,
                    "revision": current_revision.canonical_payload(),
                    "assignments": [current_assignment.canonical_payload()],
                }
            },
            parent_intent_ids={"different-parent"},
        )


def test_runtime_controller_binding_and_deterministic_child_missing_event_fail_loud() -> None:
    controller, runtime, _gateway, repository = _runtime_controller()

    with pytest.raises(QuoteContractError):
        runtime.bind_b0_quote_v2_controller(controller)

    class WrongController:
        runtime_id = "wrong-runtime"

    _source, unbound_runtime, _other_gateway, _other_repository = _runtime_controller()
    unbound_runtime._b0_quote_v2_controller = None
    with pytest.raises(QuoteContractError):
        unbound_runtime.bind_b0_quote_v2_controller(WrongController())
    instance = unbound_runtime.repository.list_algo_instances("runtime-p1e", active_only=True)[0]
    with pytest.raises(QuoteContractError):
        unbound_runtime.dispatch_b0_quote_v2_tick(instance=instance, tick=object())  # type: ignore[arg-type]

    controller.lifecycle_tick(now_utc=CLOCK_AT)
    child = repository.list_child_orders("runtime-p1e", active_only=False)[0]
    action_event = next(
        event
        for event in repository.list_events("runtime-p1e", include_archived=True)
        if event.payload.get("schema_version") == "b0_quote_v2_action_pending_v1"
    )
    action = _action_from_payload(action_event.payload["vnpy_action"])
    child_events = repository._events["runtime-p1e"]
    repository._events["runtime-p1e"] = [
        event
        for event in child_events
        if not (
            event.event_type.value in {"CHILD_ORDER_SUBMITTED", "CHILD_ORDER_REJECTED"}
            and event.payload.get("child_order_id") == child.child_order_id
        )
    ]
    with pytest.raises(QuoteContractError):
        runtime.submit_b0_quote_v2_child(
            instance=repository.list_algo_instances("runtime-p1e", active_only=False)[0],
            action=action,
            child_order_id=child.child_order_id,
            metadata=dict(child.metadata),
        )
