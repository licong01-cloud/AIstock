from __future__ import annotations

import pytest

from backend.execution_algos.adaptive_is.reasons import QuoteContractError
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import B0QuoteV2ControllerFactory

from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import _runtime_controller


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


def test_only_scheduler_constructs_controller_and_read_only_paths_never_start_ingress() -> None:
    source, runtime, _gateway, _repository = _runtime_controller()
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

    assert factory.get(runtime.config.runtime_id) is controller
    assert supervisor.physical_feed_start_count == 1
    assert tuple(supervisor.leases) == ("b0qv2:runtime-p1e",)
    assert factory.health()["controller_count"] == 1
    with pytest.raises(QuoteContractError):
        factory.create(runtime=runtime, assignments=source.assignments, symbols=tuple(source.symbols))


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
