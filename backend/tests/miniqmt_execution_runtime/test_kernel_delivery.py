from __future__ import annotations

from functools import lru_cache

import pytest

import backend.services.miniqmt_execution_runtime.kernel_delivery as kernel_delivery
import backend.services.miniqmt_execution_runtime.kernel_materializer as kernel_materializer
from backend.execution_algos.vnpy_compat.receipts import build_current_three_compatibility_receipts_v1
from backend.execution_algos.vnpy_style.plugin_manifests import (
    current_three_creation_bindings_v1,
    current_three_descriptors_v2,
    current_three_process_bindings_v2,
)
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelDeliveryExecutionInputV1,
    KernelDeliveryWorkerV1,
    KernelPluginInvocationError,
    KernelRequiredProviderUnavailable,
    ResolvedKernelPluginV1,
    build_command_lifecycle_projection_v1,
    invoke_plugin_initialize_v1,
    invoke_plugin_transition_v1,
    resolve_plugin_for_restore_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_materializer import (
    KernelEffectMaterializationError,
    materialize_applied_transition_v1,
    materialize_failure_transition_v1,
    materialize_skip_transition_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoReadOnlyServicesV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoInitializationV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandAckReceiptV1,
    BrokerAckSourceV1,
    BrokerUnknownOutcomeReceiptV1,
    BrokerUncertainStageV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    ConsumedLineageRefV1,
    ConsumedLineageTypeV1,
    CommandChildMappingStatusV1,
    CurrentThreeActiveOrderStateV3,
    CurrentThreeActiveOrderStatusV3,
    DeliveryStatusV1,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DiagnosticSeverityV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionProjectionSetV1,
    ExecutionProjectionRefV1,
    KernelProjectionTypeV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionCommandChildMappingV1,
    ActiveChildClosureStatusV1,
    KernelErrorEvidenceV1,
    KernelCommandLifecycleProjectionV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    TerminalOutcomeV1,
    _algo_instance_id_v2,
    kernel_lease_fence_token_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import build_plugin_catalog_v2
from backend.tests.miniqmt_execution_runtime.test_current_three_plugin_manifests import _state


@lru_cache(maxsize=1)
def _catalog():
    return build_plugin_catalog_v2(
        descriptors=current_three_descriptors_v2(),
        creation_bindings=current_three_creation_bindings_v1(),
        process_bindings=current_three_process_bindings_v2(),
        pinned_compatibility_receipts=build_current_three_compatibility_receipts_v1(),
    )


def _event() -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k2b",
        sequence=2,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-07-26T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price_decimal": "10.000000"},
        source_identity={"market_data_id": "market_k2b"},
        correlation={},
    )


class _PurePlugin:
    def __init__(self, *, manifest, context: DeterministicExecutionContextV1) -> None:
        self.manifest = manifest
        self._context = context

    def initialize(self, context):  # pragma: no cover - this test exercises transition
        raise AssertionError("initialize is not expected")

    def restore_state(self, snapshot: AlgoStateSnapshotV2) -> AlgoStateSnapshotV2:
        return AlgoStateSnapshotV2.model_validate(snapshot.model_dump(mode="python"), strict=True)

    def transition(self, *, state, event, services):
        assert services.event_id == event.event_id
        next_state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=self._context,
            transition_sequence=state.transition_sequence + 1,
            last_applied_delivery_sequence=self._context.transition_sequence,
            last_applied_delivery_id=self._context.delivery_id,
            last_closed_delivery_sequence=self._context.transition_sequence,
            state=thaw_json_v1(state.state),
            last_applied_event_id=event.event_id,
        )
        effect_payload = {
            "next_state_sha256": next_state.state_sha256,
            "ordered_command_ids": [],
            "ordered_timer_mutation_ids": [],
            "ordered_diagnostic_observation_ids": [],
            "terminal_outcome": None,
        }
        return AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=next_state,
            broker_commands=(),
            timer_mutations=(),
            diagnostic_observations=(),
            terminal_outcome=None,
            effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
        )


class _InitializingPlugin:
    def __init__(self, manifest) -> None:
        self.manifest = manifest

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1:
        state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=context.deterministic_context,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=context.start_delivery_id,
            last_closed_delivery_sequence=1,
            state=_state("SNIPER_MINIQMT"),
            last_applied_event_id=context.start_event_id,
        )
        payload = {
            "next_state_sha256": state.state_sha256,
            "ordered_command_ids": [],
            "ordered_timer_mutation_ids": [],
            "ordered_diagnostic_observation_ids": [],
            "terminal_outcome": None,
        }
        return AlgoInitializationV1(
            schema_version="miniqmt_algo_initialization_v1",
            start_event_id=context.start_event_id,
            start_delivery_id=context.start_delivery_id,
            next_state=state,
            broker_commands=(),
            timer_mutations=(),
            diagnostic_observations=(),
            terminal_outcome=None,
            effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", payload),
        )

    def restore_state(self, snapshot):  # pragma: no cover - initialize-only fixture
        return snapshot

    def transition(self, **_values):  # pragma: no cover - initialize-only fixture
        raise AssertionError("transition is not expected")


def _start_context(manifest) -> AlgoStartContextV1:
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    config_sha = hash_hex_v1("miniqmt_plugin_config_v2", config)
    algo_id = _algo_instance_id_v2(
        runtime_id="runtime_initialize_k2b",
        parent_intent_id="intent_initialize_k2b",
        strategy_slot_id="slot_initialize_k2b",
        algo_code=manifest.algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=config_sha,
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id="runtime_initialize_k2b",
        algo_instance_id=algo_id,
        event_id="event_initialize_k2b",
        delivery_id="delivery_initialize_k2b",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=0,
        logical_time_utc="2026-07-26T01:20:00Z",
        exchange_trade_date="2026-07-26",
        session_epoch="session_initialize_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )
    contract = {"pricetick_decimal": "0.01", "min_volume": 100}
    account = {"account_group_id": "sim_account"}
    capability = {"route_id": "miniqmt_sim_b0", "capabilities": ["L1_ASK"]}
    return AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=deterministic.runtime_id,
        algo_instance_id=algo_id,
        parent_intent_id="intent_initialize_k2b",
        strategy_slot_id="slot_initialize_k2b",
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.01",
        parent_quantity=100,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=manifest,
        plugin_config=config,
        plugin_config_sha256=config_sha,
        start_event_id=deterministic.event_id,
        start_delivery_id=deterministic.delivery_id,
        deterministic_context=deterministic,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        execution_plan_id="plan_initialize_k2b",
        execution_plan_sha256="a" * 64,
        release_id="release_initialize_k2b",
        release_sha256="b" * 64,
        policy_id="policy_initialize_k2b",
        policy_sha256="c" * 64,
    )


def test_current_three_binding_resolves_exact_frozen_plugin_without_legacy_fallback() -> None:
    runtime = _catalog()
    descriptor = next(
        item for item in runtime.snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    resolved = resolve_plugin_for_restore_v1(
        catalog_runtime=runtime,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        canonical_plugin_config=config,
        plugin_config_sha256=hash_hex_v1("miniqmt_plugin_config_v2", config),
    )

    assert resolved.descriptor == descriptor
    assert resolved.plugin.manifest == descriptor.manifest
    assert callable(resolved.state_codec)
    assert config == {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}


def _event_lineage(
    event: RuntimeEventEnvelopeV2,
    projection_set: ExecutionProjectionSetV1,
) -> tuple[ConsumedLineageRefV1, ...]:
    refs = [
        ConsumedLineageRefV1.create(
            lineage_type=ConsumedLineageTypeV1.EVENT,
            identity=event.event_id,
            payload_sha256=event.payload_sha256,
        )
    ]
    market_ref = next(
        (
            item
            for item in projection_set.ordered_projection_refs
            if item.projection_type is KernelProjectionTypeV1.MARKET_DATA
        ),
        None,
    )
    if event.event_type is EventTypeV2.TICK and market_ref is not None:
        refs.append(
            ConsumedLineageRefV1.create(
                lineage_type=ConsumedLineageTypeV1.MARKET_DATA,
                identity=market_ref.projection_id,
                payload_sha256=market_ref.payload_sha256,
            )
        )
    return tuple(refs)


def _market_ref(event: RuntimeEventEnvelopeV2) -> ExecutionProjectionRefV1:
    return ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.MARKET_DATA,
        projection_id="market_k2b",
        projection_version="miniqmt_market_data_projection_v2",
        payload_sha256=hash_hex_v1(
            "miniqmt_market_data_projection_v2",
            {"last_price_decimal": "10.000000"},
        ),
        source_event_id=event.event_id,
        logical_at_utc=event.event_time_utc,
    )


def _command_projection_refs(event: RuntimeEventEnvelopeV2) -> tuple[ExecutionProjectionRefV1, ...]:
    refs = [_market_ref(event)]
    for projection_type, version, projection_id in (
        (KernelProjectionTypeV1.KILL_SWITCH_STATE, "miniqmt_kill_switch_state_v1", "mqkillswitch_k2b"),
        (
            KernelProjectionTypeV1.OMS_PREFLIGHT,
            "miniqmt_oms_preflight_projection_receipt_v1",
            "mqomspreflight_k2b",
        ),
        (KernelProjectionTypeV1.RISK_DECISION, "miniqmt_risk_decision_receipt_v1", "mqriskdecision_k2b"),
        (
            KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
            "plugin_route_compatibility_receipt_v1",
            "mqroutecompat_k2b",
        ),
    ):
        refs.append(
            ExecutionProjectionRefV1.create(
                projection_type=projection_type,
                projection_id=projection_id,
                projection_version=version,
                payload_sha256=hash_hex_v1(version, {"status": "PASS"}),
                source_event_id=event.event_id,
                logical_at_utc=event.event_time_utc,
            )
        )
    return tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id)))


def test_pure_transition_is_rebuilt_from_durable_state_and_immutable_services() -> None:
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    event = _event()
    algo_instance_id = "algo_pure_k2b"
    delivery_id = "delivery_pure_k2b"
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="c" * 64,
    )
    previous_context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id="event_previous_k2b",
        delivery_id="delivery_previous_k2b",
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        transition_sequence=1,
        logical_time_utc="2026-07-26T01:20:00Z",
        exchange_trade_date="2026-07-26",
        session_epoch="session_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="d" * 64,
    )
    state = AlgoStateSnapshotV2.create(
        plugin_manifest=descriptor.manifest,
        deterministic_context=previous_context,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id=previous_context.delivery_id,
        last_closed_delivery_sequence=1,
        state=_state("SNIPER_MINIQMT"),
        last_applied_event_id=previous_context.event_id,
    )
    market_projection = {"last_price_decimal": "10.000000"}
    market_projection_hash = hash_hex_v1("miniqmt_market_data_projection_v2", market_projection)
    market_ref = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.MARKET_DATA,
        projection_id="market_k2b",
        projection_version="miniqmt_market_data_projection_v2",
        payload_sha256=market_projection_hash,
        source_event_id=event.event_id,
        logical_at_utc=event.event_time_utc,
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        projection_refs=(market_ref,),
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id="market_k2b",
        market_data_projection=market_projection,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    transition = invoke_plugin_transition_v1(
        plugin=_PurePlugin(manifest=descriptor.manifest, context=context),
        expected_manifest=descriptor.manifest,
        state_codec=lambda _manifest, payload: payload,
        state=state,
        event=event,
        services=services,
        deterministic_context=context,
    )

    assert transition.next_state.transition_sequence == 2
    assert transition.next_state.last_applied_delivery_id == delivery_id


def test_transition_invocation_strictly_rejects_bad_types_owner_codec_and_result() -> None:
    event, delivery, algo, state = _worker_facts()
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        projection_refs=_command_projection_refs(event),
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id="market_k2b",
        market_data_projection={"last_price_decimal": "10.000000"},
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    plugin = _PurePlugin(manifest=descriptor.manifest, context=context)
    base = dict(
        plugin=plugin,
        expected_manifest=descriptor.manifest,
        state_codec=lambda _manifest, payload: payload,
        state=state,
        event=event,
        services=services,
        deterministic_context=context,
    )
    for field, value, message in (
        ("expected_manifest", object(), "expected_manifest"),
        ("state_codec", object(), "state_codec"),
        ("state", object(), "state"),
        ("event", object(), "event"),
        ("services", object(), "services"),
        ("deterministic_context", object(), "deterministic_context"),
    ):
        values = {**base, field: value}
        with pytest.raises(TypeError, match=message):
            invoke_plugin_transition_v1(**values)

    with pytest.raises(KernelPluginInvocationError) as owner_conflict:
        invoke_plugin_transition_v1(
            **{
                **base,
                "services": services.model_copy(update={"event_id": "event_other_k2b"}),
            }
        )
    assert owner_conflict.value.reason_code == "MINIQMT_ALGO_TRANSITION_OWNER_CONFLICT"

    with pytest.raises(KernelPluginInvocationError) as codec_failure:
        invoke_plugin_transition_v1(
            **{
                **base,
                "state_codec": lambda _manifest, _payload: {"different": True},
            }
        )
    assert codec_failure.value.reason_code == "MINIQMT_ALGO_TRANSITION_PLUGIN_FAILED"

    class _InvalidResult(_PurePlugin):
        def transition(self, **_values):
            return object()

    with pytest.raises(KernelPluginInvocationError) as invalid_result:
        invoke_plugin_transition_v1(
            **{
                **base,
                "plugin": _InvalidResult(manifest=descriptor.manifest, context=context),
            }
        )
    assert invalid_result.value.reason_code == "MINIQMT_ALGO_TRANSITION_RESULT_INVALID"

    valid = plugin.transition(state=state, event=event, services=services)

    class _DriftedResult(_PurePlugin):
        def transition(self, **_values):
            return valid.model_copy(
                update={
                    "next_state": valid.next_state.model_copy(update={"transition_sequence": 99}),
                }
            )

    with pytest.raises(KernelPluginInvocationError) as drifted_result:
        invoke_plugin_transition_v1(
            **{
                **base,
                "plugin": _DriftedResult(manifest=descriptor.manifest, context=context),
            }
        )
    assert drifted_result.value.reason_code == "MINIQMT_ALGO_TRANSITION_RESULT_INVALID"


def test_transition_rejects_manifest_drift_and_fake_success_carrier() -> None:
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    with pytest.raises(KernelPluginInvocationError, match="manifest"):
        invoke_plugin_transition_v1(
            plugin=object(),
            expected_manifest=descriptor.manifest,
            state_codec=lambda _manifest, payload: payload,
            state=object(),
            event=_event(),
            services=object(),
            deterministic_context=object(),
        )


def test_initialize_invocation_uses_exact_manifest_and_rejects_plugin_or_result_failure() -> None:
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    context = _start_context(descriptor.manifest)
    result = invoke_plugin_initialize_v1(
        plugin=_InitializingPlugin(descriptor.manifest),
        expected_manifest=descriptor.manifest,
        start_context=context,
    )
    assert result.next_state.algo_instance_id == context.algo_instance_id

    class _Raising(_InitializingPlugin):
        def initialize(self, _context):
            raise RuntimeError("deterministic initialize failure")

    with pytest.raises(KernelPluginInvocationError) as raised:
        invoke_plugin_initialize_v1(
            plugin=_Raising(descriptor.manifest),
            expected_manifest=descriptor.manifest,
            start_context=context,
        )
    assert raised.value.reason_code == "MINIQMT_ALGO_INITIALIZATION_PLUGIN_FAILED"

    class _UnrenderableError(RuntimeError):
        def __str__(self) -> str:
            raise RuntimeError("renderer exploded")

    class _UnrenderableFailure(_InitializingPlugin):
        def initialize(self, _context):
            raise _UnrenderableError()

    with pytest.raises(KernelPluginInvocationError) as unrenderable:
        invoke_plugin_initialize_v1(
            plugin=_UnrenderableFailure(descriptor.manifest),
            expected_manifest=descriptor.manifest,
            start_context=context,
        )
    assert unrenderable.value.reason_code == "MINIQMT_ALGO_INITIALIZATION_PLUGIN_FAILED"
    assert unrenderable.value.context["exception_type"].endswith("._UnrenderableError")
    assert unrenderable.value.context["exception_message"] == "<_UnrenderableError: unrenderable>"
    assert unrenderable.value.context["renderer_error_type"].endswith("RuntimeError")

    class _InvalidResult(_InitializingPlugin):
        def initialize(self, _context):
            return object()

    with pytest.raises(KernelPluginInvocationError) as raised:
        invoke_plugin_initialize_v1(
            plugin=_InvalidResult(descriptor.manifest),
            expected_manifest=descriptor.manifest,
            start_context=context,
        )
    assert raised.value.reason_code == "MINIQMT_ALGO_INITIALIZATION_RESULT_INVALID"


def test_delivery_worker_applies_exact_lifecycle_projection_and_terminalizes_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    owner = "worker_k2b:incarnation_k2b"
    market_projection = {"last_price_decimal": "10.000000"}
    market_projection_hash = hash_hex_v1("miniqmt_market_data_projection_v2", market_projection)
    market_ref = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.MARKET_DATA,
        projection_id="market_k2b",
        projection_version="miniqmt_market_data_projection_v2",
        payload_sha256=market_projection_hash,
        source_event_id=event.event_id,
        logical_at_utc=event.event_time_utc,
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        projection_refs=(market_ref,),
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id="market_k2b",
        market_data_projection=market_projection,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    monkeypatch.setattr(
        kernel_delivery,
        "resolve_plugin_for_restore_v1",
        lambda **_values: ResolvedKernelPluginV1(
            descriptor=descriptor,
            plugin=_PurePlugin(manifest=descriptor.manifest, context=context),
            state_codec=lambda _manifest, payload: payload,
        ),
    )
    lineage_refs = (
        ConsumedLineageRefV1.create(
            lineage_type=ConsumedLineageTypeV1.EVENT,
            identity=event.event_id,
            payload_sha256=event.payload_sha256,
        ),
        ConsumedLineageRefV1.create(
            lineage_type=ConsumedLineageTypeV1.MARKET_DATA,
            identity="market_k2b",
            payload_sha256=market_projection_hash,
        ),
    )
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )
    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc=event.event_time_utc,
        input_builder=lambda *_args: KernelDeliveryExecutionInputV1(
            services=services,
            deterministic_context=context,
            consumed_lineage_refs=lineage_refs,
            command_lifecycle_projection=_args[-2],
        ),
    )
    assert repository.delivery.lease_owner == owner
    assert result["bundle"].delivery.status is DeliveryStatusV1.APPLIED
    assert repository.retry_calls == []

    drift_repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    drift_result = KernelDeliveryWorkerV1(
        repository=drift_repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    ).process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc=event.event_time_utc,
        input_builder=lambda *_args: KernelDeliveryExecutionInputV1(
            services=services,
            deterministic_context=context,
            consumed_lineage_refs=lineage_refs,
            command_lifecycle_projection=KernelCommandLifecycleProjectionV1.create(
                runtime_id=event.runtime_id,
                algo_instance_id=algo.algo_instance_id,
                event_id=event.event_id,
                delivery_id="delivery_stale_lifecycle_projection",
                ordered_items=(),
            ),
        ),
    )
    assert drift_result["bundle"].delivery.status is DeliveryStatusV1.FAILED_TERMINAL
    assert drift_result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_DELIVERY_LIFECYCLE_PROJECTION_DRIFT"


def test_skip_materializer_rejects_nonfailed_algo_owner() -> None:
    event, delivery, algo, _state_snapshot = _worker_facts()
    claimed_payload = delivery.model_dump(mode="python")
    claimed_payload.update(
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner="worker_k2b:incarnation_k2b",
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="worker_k2b:incarnation_k2b",
        ),
        lease_expires_at="2026-07-26T01:31:00Z",
        row_version=2,
    )
    claimed = AlgoDeliveryPersistenceV1.model_validate(claimed_payload)
    with pytest.raises(KernelEffectMaterializationError) as raised:
        materialize_skip_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            logical_time_utc=event.event_time_utc,
        )
    assert raised.value.reason_code == "MINIQMT_ALGO_SKIP_OWNER_INVALID"


def test_initialization_failure_materializes_typed_terminal_fact_without_fake_state() -> None:
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "SNIPER_MINIQMT"
    )
    config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
    config_hash = hash_hex_v1("miniqmt_plugin_config_v2", config)
    algo_id = _algo_instance_id_v2(
        runtime_id="runtime_init_failure_k2b",
        parent_intent_id="intent_init_failure_k2b",
        strategy_slot_id="slot_init_failure_k2b",
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_sha256=config_hash,
    )
    event = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_init_failure_k2b",
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-26T01:20:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_algo_start_v1",
        payload={"target_quantity": 100},
        source_identity={
            "algo_instance_id": algo_id,
            "runtime_id": "runtime_init_failure_k2b",
            "parent_intent_id": "intent_init_failure_k2b",
            "strategy_slot_id": "slot_init_failure_k2b",
            "algo_code": descriptor.manifest.algo_code,
            "plugin_id": descriptor.manifest.plugin_id,
            "plugin_version": descriptor.manifest.plugin_version,
            "plugin_manifest_sha256": descriptor.manifest.manifest_sha256,
            "plugin_config_sha256": config_hash,
        },
        correlation={"execution_plan_id": "plan_init_failure_k2b"},
    )
    initial_event_delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_id,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        algo_delivery_sequence=1,
        previous_delivery_id=None,
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=event.event_time_utc,
        updated_at_utc=event.event_time_utc,
    )
    initial = AlgoDeliveryPersistenceV1.create(
        delivery=initial_event_delivery,
        lease_epoch=0,
        lease_fence_token=None,
        row_version=1,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )

    bundle = materialize_failure_transition_v1(
        event=event,
        predecessor_delivery=initial,
        previous_algo=None,
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config=config,
        plugin_config_sha256=config_hash,
        compatibility_receipt_sha256="e" * 64,
        parent_intent_id="intent_init_failure_k2b",
        strategy_slot_id="slot_init_failure_k2b",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=100,
        stable_reason_code="MINIQMT_ALGO_INITIALIZATION_PLUGIN_FAILED",
        exception=ValueError("invalid deterministic config"),
        failure_context={"config_sha256": config_hash},
        active_mappings=(),
        active_timer_schedules=(),
        logical_time_utc=event.event_time_utc,
        initialization=True,
    )

    assert bundle.algo_instance.status.value == "FAILED"
    assert bundle.algo_instance.state_json is None
    assert bundle.delivery.status is DeliveryStatusV1.FAILED_TERMINAL
    assert bundle.receipt.failure_receipt_id == bundle.algo_instance.failure_receipt_id


def _worker_facts(*, attempt_count: int = 0, failed_algo: bool = False):
    event = _event()
    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == "TWAP_LITE_MINIQMT"
    )
    config = {"duration_seconds": 300, "slice_interval_seconds": 60}
    config_hash = hash_hex_v1("miniqmt_plugin_config_v2", config)
    algo_id = _algo_instance_id_v2(
        runtime_id=event.runtime_id,
        parent_intent_id="intent_worker_k2b",
        strategy_slot_id="slot_worker_k2b",
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_sha256=config_hash,
    )
    event_delivery = AlgoEventDeliveryV1.create(
        event=event,
        algo_instance_id=algo_id,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        algo_delivery_sequence=2,
        previous_delivery_id="delivery_worker_previous_k2b",
        status=DeliveryStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_expires_at=None,
        transition_id=None,
        last_error_json=None,
        created_at_utc=event.event_time_utc,
        updated_at_utc=event.event_time_utc,
    )
    delivery = AlgoDeliveryPersistenceV1.create(
        delivery=event_delivery,
        lease_epoch=0,
        lease_fence_token=None,
        row_version=1,
        next_attempt_at_utc=None,
        failure_receipt_id=None,
        skip_receipt_id=None,
        closed_at_utc=None,
    )
    if attempt_count:
        retry_error = KernelErrorEvidenceV1.create(
            stage="DELIVERY_REQUIRED_PROVIDER",
            stable_reason_code="MINIQMT_ALGO_DELIVERY_REQUIRED_PROVIDER_UNAVAILABLE",
            exception=RuntimeError("provider remained unavailable"),
            message="provider remained unavailable",
            retryable=True,
            terminal=False,
            broker_called=False,
            primary_context={"attempt_count": attempt_count},
            secondary_errors=(),
        )
        retry_payload = delivery.model_dump(mode="python")
        retry_payload.update(
            status=DeliveryStatusV1.FAILED_RETRYABLE,
            attempt_count=attempt_count,
            lease_epoch=attempt_count,
            last_error_json=retry_error.model_dump(mode="json"),
            next_attempt_at_utc="2026-07-26T01:30:00Z",
            row_version=1 + attempt_count * 2,
        )
        delivery = AlgoDeliveryPersistenceV1.model_validate(retry_payload)
    previous_context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo_id,
        event_id="event_worker_previous_k2b",
        delivery_id="delivery_worker_previous_k2b",
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        transition_sequence=1,
        logical_time_utc="2026-07-26T01:29:00Z",
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="a" * 64,
    )
    state = AlgoStateSnapshotV2.create(
        plugin_manifest=descriptor.manifest,
        deterministic_context=previous_context,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id="delivery_worker_previous_k2b",
        last_closed_delivery_sequence=1,
        state=_state("TWAP_LITE_MINIQMT"),
        last_applied_event_id="event_worker_previous_k2b",
    )
    algo = ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=algo_id,
        runtime_id=event.runtime_id,
        parent_intent_id="intent_worker_k2b",
        strategy_slot_id="slot_worker_k2b",
        symbol="600000.SH",
        side=SideV1.BUY,
        target_quantity=1000,
        traded_quantity=0,
        remaining_quantity=1000,
        algo_code=descriptor.manifest.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_json=config,
        plugin_config_sha256=config_hash,
        compatibility_receipt_sha256="e" * 64,
        state_schema_version=state.state_schema_version,
        state_json=thaw_json_v1(state.state),
        state_sha256=state.state_sha256,
        transition_sequence=1,
        last_applied_delivery_sequence=1,
        last_applied_delivery_id="delivery_worker_previous_k2b",
        last_closed_delivery_sequence=1,
        terminal_delivery_sequence=1 if failed_algo else None,
        status=(ExecutionAlgoPersistenceStatusV2.FAILED if failed_algo else ExecutionAlgoPersistenceStatusV2.ACTIVE),
        failure_receipt_id="failure_worker_k2b" if failed_algo else None,
        active_child_closure_status=(
            ActiveChildClosureStatusV1.CLEAN if failed_algo else ActiveChildClosureStatusV1.NOT_APPLICABLE
        ),
        active_child_count=0,
        row_version=2,
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc="2026-07-26T01:29:00Z",
        terminal_at_utc="2026-07-26T01:29:30Z" if failed_algo else None,
        archived_at_utc=None,
    )
    return event, delivery, algo, state


def _empty_lifecycle_projection(
    event: RuntimeEventEnvelopeV2,
    delivery: AlgoDeliveryPersistenceV1,
    algo: ExecutionAlgoInstancePersistenceV2,
) -> KernelCommandLifecycleProjectionV1:
    return KernelCommandLifecycleProjectionV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        ordered_items=(),
    )


def test_locked_command_lifecycle_projection_closes_empty_v3_state() -> None:
    event, delivery, algo, state = _worker_facts()
    projection = build_command_lifecycle_projection_v1(
        event=event,
        delivery=delivery,
        previous_state=state,
        mappings=(),
        outboxes=(),
    )

    assert projection.runtime_id == event.runtime_id
    assert projection.algo_instance_id == algo.algo_instance_id
    assert projection.event_id == event.event_id
    assert projection.delivery_id == delivery.delivery_id
    assert projection.ordered_items == ()


@pytest.mark.parametrize(
    "lifecycle_status",
    [
        CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
        CurrentThreeActiveOrderStatusV3.SUBMITTED,
        CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
        CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
        CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
    ],
)
def test_lifecycle_projection_validates_every_durable_active_order_state(
    lifecycle_status: CurrentThreeActiveOrderStatusV3,
) -> None:
    event, delivery, algo, previous_state = _worker_facts()
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    submit = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="mqtransition_lifecycle_matrix",
        ordinal=0,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10",
        quantity=300,
        owned_broker_order_id=None,
        reason_code="K3_LIFECYCLE_MATRIX_SUBMIT",
        metadata={},
    )
    broker_id = None if lifecycle_status is CurrentThreeActiveOrderStatusV3.COMMAND_PENDING else "broker_lifecycle"
    mapping_status = {
        CurrentThreeActiveOrderStatusV3.COMMAND_PENDING: CommandChildMappingStatusV1.RESERVED,
        CurrentThreeActiveOrderStatusV3.SUBMITTED: CommandChildMappingStatusV1.BROKER_ACCEPTED,
        CurrentThreeActiveOrderStatusV3.CANCEL_PENDING: CommandChildMappingStatusV1.BROKER_ACCEPTED,
        CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN: CommandChildMappingStatusV1.BROKER_ACCEPTED,
        CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING: CommandChildMappingStatusV1.TERMINAL,
    }[lifecycle_status]
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=mapping_status,
        mapping_version=1 if mapping_status is CommandChildMappingStatusV1.RESERVED else 2,
        broker_order_id=broker_id,
        broker_identity_source_event_id=None if broker_id is None else "event_lifecycle_broker",
        last_order_event_id=(
            "event_lifecycle_terminal"
            if lifecycle_status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
            else None
        ),
        last_trade_event_id=None,
        updated_by_event_id=None if broker_id is None else "event_lifecycle_broker",
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc=(
            "2026-07-26T01:29:00Z" if mapping_status is CommandChildMappingStatusV1.RESERVED else "2026-07-26T01:29:30Z"
        ),
    )
    current_command = submit
    pending_type = BrokerCommandTypeV2.SUBMIT_LIMIT
    pending_id = submit.command_id
    if lifecycle_status in {
        CurrentThreeActiveOrderStatusV3.SUBMITTED,
        CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING,
    }:
        pending_type = None
        pending_id = None
    elif lifecycle_status in {
        CurrentThreeActiveOrderStatusV3.CANCEL_PENDING,
        CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN,
    }:
        current_command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.CANCEL_ORDER,
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            parent_intent_id=algo.parent_intent_id,
            transition_id="mqtransition_lifecycle_cancel",
            ordinal=0,
            local_vt_orderid=submit.local_vt_orderid,
            symbol=algo.symbol,
            side=algo.side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal="10",
            quantity=300,
            owned_broker_order_id=broker_id,
            reason_code="K3_LIFECYCLE_MATRIX_CANCEL",
            metadata={"submit_command_id": submit.command_id},
        )
        pending_type = BrokerCommandTypeV2.CANCEL_ORDER
        pending_id = current_command.command_id
    active_item = CurrentThreeActiveOrderStateV3.create(
        local_vt_orderid=submit.local_vt_orderid,
        submit_command_id=submit.command_id,
        broker_order_id=broker_id,
        symbol=algo.symbol,
        side=algo.side,
        status=lifecycle_status,
        pending_command_type=pending_type,
        pending_command_id=pending_id,
        requested_price_decimal="10",
        requested_quantity=300,
        cumulative_filled_quantity=0,
        remaining_quantity=300,
        last_order_event_id=(
            "event_lifecycle_terminal"
            if lifecycle_status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING
            else None
        ),
        last_trade_event_id=None,
        last_command_outcome_event_id=None,
        last_oms_reconcile_event_id=None,
        terminal_order_status=(
            "CANCELLED" if lifecycle_status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING else None
        ),
        terminal_observed_cumulative_filled_quantity=(
            100 if lifecycle_status is CurrentThreeActiveOrderStatusV3.TERMINAL_TRADE_PENDING else None
        ),
        market_data_lineage=thaw_json_v1(previous_state.state)["last_market_data_lineage"],
    )
    state_payload = thaw_json_v1(previous_state.state)
    state_payload["active_orders"] = [active_item.model_dump(mode="json")]
    state_hash = hash_hex_v1("execution_algo_state_v2", state_payload)
    previous_state_payload = previous_state.model_dump(mode="python")
    previous_state_payload.update(state=state_payload, state_sha256=state_hash)
    previous_state = AlgoStateSnapshotV2.model_validate(previous_state_payload)
    previous_algo_payload = algo.model_dump(mode="python")
    previous_algo_payload.update(
        state_json=state_payload,
        state_sha256=state_hash,
        active_child_count=1,
    )
    previous_algo = ExecutionAlgoInstancePersistenceV2.model_validate(previous_algo_payload)

    if lifecycle_status is CurrentThreeActiveOrderStatusV3.COMMAND_PENDING:
        outbox = BrokerCommandOutboxV1.create(
            command=current_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_epoch=0,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=None,
            callback_watermark_before_call=None,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=1,
            created_at_utc="2026-07-26T01:29:30Z",
            updated_at_utc="2026-07-26T01:29:30Z",
            closed_at_utc=None,
        )
    elif lifecycle_status is CurrentThreeActiveOrderStatusV3.OUTCOME_UNKNOWN:
        unknown = BrokerUnknownOutcomeReceiptV1.create(
            command_id=current_command.command_id,
            dispatch_attempt_id="dispatch_lifecycle_unknown",
            mapping_id=mapping.mapping_id,
            lease_fence_token=kernel_lease_fence_token_v1(
                owner_type="COMMAND",
                owner_id=current_command.command_id,
                lease_epoch=1,
                lease_owner="worker_lifecycle_unknown:incarnation_1",
            ),
            uncertain_stage=BrokerUncertainStageV1.GATEWAY_RETURN,
            callback_watermark=f"{event.runtime_id}:1",
            reason_code="OUTCOME_UNKNOWN",
            observed_at_utc="2026-07-26T01:29:30Z",
        )
        outbox = BrokerCommandOutboxV1.create(
            command=current_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN,
            attempt_count=1,
            lease_owner=None,
            lease_epoch=1,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id="dispatch_lifecycle_unknown",
            callback_watermark_before_call=f"{event.runtime_id}:1",
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=unknown,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=3,
            created_at_utc="2026-07-26T01:29:30Z",
            updated_at_utc="2026-07-26T01:29:30Z",
            closed_at_utc=None,
        )
    elif lifecycle_status is CurrentThreeActiveOrderStatusV3.CANCEL_PENDING:
        outbox = BrokerCommandOutboxV1.create(
            command=current_command,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_epoch=0,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id=None,
            callback_watermark_before_call=None,
            next_attempt_at_utc=None,
            broker_called=None,
            broker_order_id=None,
            ack_receipt_json=None,
            ack_receipt_sha256=None,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=1,
            created_at_utc="2026-07-26T01:29:30Z",
            updated_at_utc="2026-07-26T01:29:30Z",
            closed_at_utc=None,
        )
    else:
        ack = BrokerCommandAckReceiptV1.create(
            command_id=submit.command_id,
            mapping_id=mapping.mapping_id,
            deterministic_client_order_ref=mapping.deterministic_client_order_ref,
            gateway_route_id="gateway_lifecycle",
            gateway_catalog_sha256="a" * 64,
            source=BrokerAckSourceV1.SYNCHRONOUS_RETURN,
            accepted=True,
            broker_order_id=broker_id,
            reason_code="BROKER_ACCEPTED",
            ack_payload_sha256="b" * 64,
            observed_at_utc="2026-07-26T01:29:30Z",
        )
        outbox = BrokerCommandOutboxV1.create(
            command=submit,
            mapping_id=mapping.mapping_id,
            status=BrokerCommandOutboxStatusV1.ACKED,
            attempt_count=1,
            lease_owner=None,
            lease_epoch=1,
            lease_fence_token=None,
            lease_expires_at=None,
            dispatch_attempt_id="dispatch_lifecycle_ack",
            callback_watermark_before_call=f"{event.runtime_id}:1",
            next_attempt_at_utc=None,
            broker_called=True,
            broker_order_id=broker_id,
            ack_receipt_json=ack,
            ack_receipt_sha256=ack.receipt_sha256,
            non_acceptance_receipt=None,
            unknown_outcome_receipt=None,
            reconcile_receipt=None,
            last_error_json=None,
            row_version=3,
            created_at_utc="2026-07-26T01:29:30Z",
            updated_at_utc="2026-07-26T01:29:30Z",
            closed_at_utc="2026-07-26T01:29:30Z",
        )

    projection = build_command_lifecycle_projection_v1(
        event=event,
        delivery=delivery,
        previous_state=previous_state,
        mappings=(mapping,),
        outboxes=(outbox,),
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="c" * 64,
    )
    next_state = AlgoStateSnapshotV2.create(
        plugin_manifest=descriptor.manifest,
        deterministic_context=context,
        transition_sequence=2,
        last_applied_delivery_sequence=2,
        last_applied_delivery_id=delivery.delivery_id,
        last_closed_delivery_sequence=2,
        state=state_payload,
        last_applied_event_id=event.event_id,
    )
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=next_state,
        broker_commands=(),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1(
            "miniqmt_algo_effect_set_v1",
            {
                "next_state_sha256": next_state.state_sha256,
                "ordered_command_ids": [],
                "ordered_timer_mutation_ids": [],
                "ordered_diagnostic_observation_ids": [],
                "terminal_outcome": None,
            },
        ),
    )

    kernel_materializer._validate_command_lifecycle_projection_v1(
        event=event,
        predecessor_delivery=delivery,
        previous_algo=previous_algo,
        transition=transition,
        projection=projection,
        existing_mappings_by_local_vt_orderid={mapping.local_vt_orderid: mapping},
        new_mappings=(),
        new_outboxes=(),
    )


def test_materializer_lifecycle_failures_are_typed_for_malformed_state_and_duplicate_identity() -> None:
    with pytest.raises(KernelEffectMaterializationError, match="active_orders list"):
        kernel_materializer._active_order_items_v3({})
    with pytest.raises(KernelEffectMaterializationError, match="not a strict object"):
        kernel_materializer._active_order_items_v3({"active_orders": ["not-an-object"]})
    with pytest.raises(KernelEffectMaterializationError, match="strict v3 readback"):
        kernel_materializer._active_order_items_v3({"active_orders": [{"local_vt_orderid": "partial"}]})

    event, _delivery, _algo, state = _worker_facts()
    payload = thaw_json_v1(state.state)
    item = CurrentThreeActiveOrderStateV3.create(
        local_vt_orderid="local_duplicate_lifecycle",
        submit_command_id="command_duplicate_lifecycle",
        broker_order_id="broker_duplicate_lifecycle",
        symbol="600000.SH",
        side=SideV1.BUY,
        status=CurrentThreeActiveOrderStatusV3.SUBMITTED,
        pending_command_type=None,
        pending_command_id=None,
        requested_price_decimal="10",
        requested_quantity=100,
        cumulative_filled_quantity=0,
        remaining_quantity=100,
        last_order_event_id=None,
        last_trade_event_id=None,
        last_command_outcome_event_id=None,
        last_oms_reconcile_event_id=None,
        terminal_order_status=None,
        terminal_observed_cumulative_filled_quantity=None,
        market_data_lineage=payload["last_market_data_lineage"],
    ).model_dump(mode="json")
    with pytest.raises(KernelEffectMaterializationError, match="duplicate local identities"):
        kernel_materializer._active_order_items_v3({"active_orders": [item, item]})


def test_materializer_rejects_invalid_terminal_timer_lineage_and_cancel_owner() -> None:
    with pytest.raises(KernelEffectMaterializationError, match="failure receipt"):
        kernel_materializer._terminal_status(
            TerminalOutcomeV1.REJECTED,
            initialization=False,
            previous_status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        )

    event, _delivery, algo, _state = _worker_facts()
    timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.CANCEL,
        algo_instance_id=algo.algo_instance_id,
        transition_id="mqtransition_timer_missing_owner",
        ordinal=0,
        timer_name="timer_missing_owner",
        schedule_epoch="schedule_epoch_missing_owner",
        due_at_exchange_utc=None,
        catch_up_policy="SKIP_MISSED",
        payload={"reason_code": "MISSING_OWNER"},
    )
    timer_state = _worker_facts()[3]
    empty_state = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=timer_state,
        broker_commands=(),
        timer_mutations=(timer,),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1(
            "miniqmt_algo_effect_set_v1",
            {
                "next_state_sha256": timer_state.state_sha256,
                "ordered_command_ids": [],
                "ordered_timer_mutation_ids": [timer.mutation_identity_v1()],
                "ordered_diagnostic_observation_ids": [],
                "terminal_outcome": None,
            },
        ),
    )
    with pytest.raises(KernelEffectMaterializationError, match="no exact durable schedule owner"):
        kernel_materializer._materialize_timers(
            runtime_id=event.runtime_id,
            transition=empty_state,
            logical_time_utc=event.event_time_utc,
            existing_timer_schedules={},
        )

    event_ref = ConsumedLineageRefV1.create(
        lineage_type=ConsumedLineageTypeV1.EVENT,
        identity=event.event_id,
        payload_sha256=event.payload_sha256,
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id="delivery_lineage_duplicate",
        projection_refs=(_market_ref(event),),
    )
    with pytest.raises(KernelEffectMaterializationError, match="duplicate authority identity"):
        kernel_materializer._validate_projection_lineage_v1(
            event=event,
            projection_set=projection_set,
            consumed_lineage_refs=(event_ref, event_ref),
            has_broker_commands=False,
        )
    with pytest.raises(TypeError, match="ConsumedLineageRefV1"):
        kernel_materializer._validate_projection_lineage_v1(
            event=event,
            projection_set=projection_set,
            consumed_lineage_refs=(object(),),
            has_broker_commands=False,
        )

    empty_projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id="delivery_lineage_missing_market",
        projection_refs=(),
    )
    with pytest.raises(KernelEffectMaterializationError, match="market-data projection"):
        kernel_materializer._validate_projection_lineage_v1(
            event=event,
            projection_set=empty_projection_set,
            consumed_lineage_refs=(event_ref,),
            has_broker_commands=False,
        )


def test_lifecycle_projection_rejects_noncarrier_owner_and_hash_drift() -> None:
    event, delivery, algo, state = _worker_facts()
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        projection_refs=_command_projection_refs(event),
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery.delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id="market_k2b",
        market_data_projection={"last_price_decimal": "10.000000"},
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    transition = invoke_plugin_transition_v1(
        plugin=_PurePlugin(manifest=descriptor.manifest, context=context),
        expected_manifest=descriptor.manifest,
        state_codec=lambda _manifest, payload: payload,
        state=state,
        event=event,
        services=services,
        deterministic_context=context,
    )
    valid = _empty_lifecycle_projection(event, delivery, algo)
    common = {
        "event": event,
        "predecessor_delivery": delivery,
        "previous_algo": algo,
        "transition": transition,
        "existing_mappings_by_local_vt_orderid": {},
        "new_mappings": (),
        "new_outboxes": (),
    }
    with pytest.raises(KernelEffectMaterializationError, match="requires one strict"):
        kernel_materializer._validate_command_lifecycle_projection_v1(projection=object(), **common)
    with pytest.raises(KernelEffectMaterializationError, match="owner differs"):
        kernel_materializer._validate_command_lifecycle_projection_v1(
            projection=valid.model_copy(update={"delivery_id": "wrong_delivery"}), **common
        )
    with pytest.raises(KernelEffectMaterializationError, match="hash differs"):
        kernel_materializer._validate_command_lifecycle_projection_v1(
            projection=valid.model_copy(update={"projection_sha256": "0" * 64}), **common
        )


class _WorkerRepository:
    def __init__(self, *, event, delivery, algo, state) -> None:
        self.event = event
        self.delivery = delivery
        self.algo = algo
        self.state = state
        self.retry_calls: list[dict[str, object]] = []
        self.claim_calls = 0
        self.bundle = None

    def read_delivery(self, delivery_id):
        assert delivery_id == self.delivery.delivery_id
        return self.delivery

    def read_algo_instance(self, algo_instance_id):
        assert algo_instance_id == self.algo.algo_instance_id
        return self.algo

    def claim_delivery(self, **values):
        self.claim_calls += 1
        payload = self.delivery.model_dump(mode="python")
        payload.update(
            status=DeliveryStatusV1.CLAIMED,
            attempt_count=self.delivery.attempt_count + 1,
            lease_owner=values["lease_owner"],
            lease_epoch=values["lease_epoch"],
            lease_fence_token=values["lease_fence_token"],
            lease_expires_at=values["lease_expires_at"],
            row_version=self.delivery.row_version + 1,
            updated_at_utc=values["updated_at_utc"],
            next_attempt_at_utc=None,
        )
        self.delivery = AlgoDeliveryPersistenceV1.model_validate(payload)
        return self.delivery

    def apply_claimed_delivery_atomic(self, **values):
        self.bundle = values["bundle_builder"](self.event, self.delivery, self.algo, self.state, (), (), (), None)
        return {"bundle": self.bundle}

    def mark_delivery_retryable(self, **values):
        self.retry_calls.append(values)
        evidence = values["error_evidence"]
        assert isinstance(evidence, KernelErrorEvidenceV1)
        payload = self.delivery.model_dump(mode="python")
        payload.update(
            status=DeliveryStatusV1.FAILED_RETRYABLE,
            lease_owner=None,
            lease_fence_token=None,
            lease_expires_at=None,
            last_error_json=evidence.model_dump(mode="json"),
            next_attempt_at_utc="2026-07-26T01:30:01Z",
            row_version=self.delivery.row_version + 1,
            updated_at_utc=values["failed_at_utc"],
        )
        self.delivery = AlgoDeliveryPersistenceV1.model_validate(payload)
        return self.delivery


def _provider_unavailable(*_args):
    raise KernelRequiredProviderUnavailable(
        "account projection is unavailable",
        context={"provider": "ACCOUNT_PROJECTION", "reason": "READBACK_MISSING"},
    )


def test_delivery_worker_releases_attempt_one_with_durable_retry_evidence() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )

    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc="2026-07-26T01:30:00Z",
        input_builder=_provider_unavailable,
    )

    assert result["retry_scheduled"] is True
    assert result["delivery"].status is DeliveryStatusV1.FAILED_RETRYABLE
    assert len(repository.retry_calls) == 1
    assert repository.bundle is None


def test_delivery_worker_attempt_five_terminally_fails_without_sixth_retry() -> None:
    event, delivery, algo, state = _worker_facts(attempt_count=4)
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )

    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc="2026-07-26T01:30:00Z",
        input_builder=_provider_unavailable,
    )

    assert repository.retry_calls == []
    assert result["bundle"].delivery.status is DeliveryStatusV1.FAILED_TERMINAL
    assert result["bundle"].algo_instance.status is ExecutionAlgoPersistenceStatusV2.FAILED
    assert result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_DELIVERY_RETRY_EXHAUSTED"


def test_delivery_worker_terminalizes_unclassified_pre_broker_input_error_without_retry() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )

    def invalid_input(*_args):
        raise ValueError("projection identity conflicts with readback")

    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc="2026-07-26T01:30:00Z",
        input_builder=invalid_input,
    )

    assert repository.retry_calls == []
    assert result["bundle"].delivery.status is DeliveryStatusV1.FAILED_TERMINAL
    assert result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_DELIVERY_INPUT_INVALID"


def test_delivery_worker_rejects_invalid_identity_status_claim_owner_and_missing_state() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    with pytest.raises(ValueError, match="identities"):
        KernelDeliveryWorkerV1(
            repository=repository,
            catalog_runtime=_catalog(),
            worker_id="",
            process_incarnation_id="incarnation_k2b",
        )

    terminal_payload = delivery.model_dump(mode="python")
    terminal_payload.update(
        status=DeliveryStatusV1.APPLIED,
        attempt_count=1,
        transition_id="transition_terminal_k2b",
        row_version=2,
        closed_at_utc=event.event_time_utc,
    )
    terminal_repository = _WorkerRepository(
        event=event,
        delivery=AlgoDeliveryPersistenceV1.model_validate(terminal_payload),
        algo=algo,
        state=state,
    )
    worker = KernelDeliveryWorkerV1(
        repository=terminal_repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )
    with pytest.raises(KernelPluginInvocationError) as terminal:
        worker.process_once(
            delivery_id=delivery.delivery_id,
            lease_expires_at="2026-07-26T01:31:00Z",
            logical_time_utc=event.event_time_utc,
            input_builder=lambda *_args: object(),
        )
    assert terminal.value.reason_code == "MINIQMT_ALGO_DELIVERY_NOT_CLAIMABLE"

    claimed = repository.claim_delivery(
        lease_owner="other_worker:other_incarnation",
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="other_worker:other_incarnation",
        ),
        lease_expires_at="2026-07-26T01:31:00Z",
        updated_at_utc=event.event_time_utc,
    )
    assert claimed.status is DeliveryStatusV1.CLAIMED
    with pytest.raises(KernelPluginInvocationError) as wrong_owner:
        KernelDeliveryWorkerV1(
            repository=repository,
            catalog_runtime=_catalog(),
            worker_id="worker_k2b",
            process_incarnation_id="incarnation_k2b",
        ).process_once(
            delivery_id=delivery.delivery_id,
            lease_expires_at="2026-07-26T01:31:00Z",
            logical_time_utc=event.event_time_utc,
            input_builder=lambda *_args: object(),
        )
    assert wrong_owner.value.reason_code == "MINIQMT_ALGO_DELIVERY_CLAIM_OWNER_CONFLICT"

    event2, delivery2, algo2, _state2 = _worker_facts()
    missing_state_repository = _WorkerRepository(event=event2, delivery=delivery2, algo=algo2, state=None)
    result = KernelDeliveryWorkerV1(
        repository=missing_state_repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    ).process_once(
        delivery_id=delivery2.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc=event2.event_time_utc,
        input_builder=lambda *_args: pytest.fail("missing state must fail before provider"),
    )
    assert result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_STATE_READBACK_MISSING"


def test_delivery_worker_rejects_noncarrier_input_without_retry() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    result = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    ).process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc=event.event_time_utc,
        input_builder=lambda *_args: object(),
    )
    assert repository.retry_calls == []
    assert result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_DELIVERY_INPUT_INVALID"


def test_delivery_worker_continues_recovered_owned_claim_without_consuming_another_attempt() -> None:
    event, delivery, algo, state = _worker_facts(attempt_count=4)
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    owner = "worker_k2b:incarnation_k2b"
    claimed = repository.claim_delivery(
        lease_owner=owner,
        lease_epoch=5,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=5,
            lease_owner=owner,
        ),
        lease_expires_at="2026-07-26T01:31:00Z",
        updated_at_utc="2026-07-26T01:30:00Z",
    )
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )

    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc="2026-07-26T01:30:00Z",
        input_builder=_provider_unavailable,
    )

    assert repository.claim_calls == 1
    assert claimed.attempt_count == 5
    assert result["bundle"].delivery.status is DeliveryStatusV1.FAILED_TERMINAL
    assert result["bundle"].receipt.stable_reason_code == "MINIQMT_ALGO_DELIVERY_RETRY_EXHAUSTED"


def test_delivery_worker_skips_later_delivery_after_algo_failure_without_provider_or_plugin() -> None:
    event, delivery, algo, state = _worker_facts(failed_algo=True)
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    worker = KernelDeliveryWorkerV1(
        repository=repository,
        catalog_runtime=_catalog(),
        worker_id="worker_k2b",
        process_incarnation_id="incarnation_k2b",
    )

    result = worker.process_once(
        delivery_id=delivery.delivery_id,
        lease_expires_at="2026-07-26T01:31:00Z",
        logical_time_utc="2026-07-26T01:30:00Z",
        input_builder=lambda *_args: pytest.fail("provider must not be called for failed algo"),
    )

    assert result["bundle"].delivery.status is DeliveryStatusV1.SKIPPED_TERMINAL
    assert result["bundle"].algo_instance.failure_receipt_id == algo.failure_receipt_id


def test_applied_materializer_uses_strict_state_quantity_authority_and_rejects_false_filled() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    claimed = repository.claim_delivery(
        lease_owner="worker_k2b:incarnation_k2b",
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=delivery.delivery_id,
            lease_epoch=1,
            lease_owner="worker_k2b:incarnation_k2b",
        ),
        lease_expires_at="2026-07-26T01:31:00Z",
        updated_at_utc=event.event_time_utc,
    )
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        projection_refs=_command_projection_refs(event),
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id="market_k2b",
        market_data_projection={"last_price_decimal": "10.000000"},
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    transition = invoke_plugin_transition_v1(
        plugin=_PurePlugin(manifest=descriptor.manifest, context=context),
        expected_manifest=descriptor.manifest,
        state_codec=lambda _manifest, payload: payload,
        state=state,
        event=event,
        services=services,
        deterministic_context=context,
    )
    materialized = materialize_applied_transition_v1(
        event=event,
        predecessor_delivery=claimed,
        previous_algo=algo,
        transition=transition,
        projection_set=projection_set,
        consumed_lineage_refs=_event_lineage(event, projection_set),
        strategy_slot_id=algo.strategy_slot_id,
        parent_intent_id=algo.parent_intent_id,
        compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
        plugin_config=thaw_json_v1(algo.plugin_config_json),
        plugin_config_sha256=algo.plugin_config_sha256,
        target_quantity=algo.target_quantity,
        algo_code=algo.algo_code,
        symbol=algo.symbol,
        side=algo.side,
        command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
        existing_mappings_by_local_vt_orderid={},
        existing_timer_schedules={},
        initialization=False,
    )
    assert materialized.algo_instance.traded_quantity == thaw_json_v1(transition.next_state.state)["traded_quantity"]

    false_filled_payload = transition.effect_hash_payload_v1()
    false_filled_payload["terminal_outcome"] = "FILLED"
    false_filled = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=transition.next_state,
        broker_commands=transition.broker_commands,
        timer_mutations=transition.timer_mutations,
        diagnostic_observations=transition.diagnostic_observations,
        terminal_outcome=TerminalOutcomeV1.FILLED,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", false_filled_payload),
    )
    with pytest.raises(KernelEffectMaterializationError, match="FILLED terminal outcome"):
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=false_filled,
            projection_set=projection_set,
            consumed_lineage_refs=_event_lineage(event, projection_set),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )


def test_applied_materializer_persists_submit_timer_and_diagnostic_effect_closure() -> None:
    event, delivery, algo, state = _worker_facts()
    repository = _WorkerRepository(event=event, delivery=delivery, algo=algo, state=state)
    owner = "worker_k2b:incarnation_k2b"
    claimed = repository.claim_delivery(
        lease_owner=owner,
        lease_epoch=1,
        lease_fence_token=kernel_lease_fence_token_v1(
            owner_type="DELIVERY", owner_id=delivery.delivery_id, lease_epoch=1, lease_owner=owner
        ),
        lease_expires_at="2026-07-26T01:31:00Z",
        updated_at_utc=event.event_time_utc,
    )
    descriptor = next(
        item
        for item in _catalog().snapshot.registration_descriptors
        if item.manifest.manifest_sha256 == algo.plugin_manifest_sha256
    )
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        projection_refs=_command_projection_refs(event),
    )
    context = DeterministicExecutionContextV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=2,
        logical_time_utc=event.event_time_utc,
        exchange_trade_date="2026-07-26",
        session_epoch="session_worker_k2b",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256=projection_set.projection_set_sha256,
    )
    unchanged_state = AlgoStateSnapshotV2.create(
        plugin_manifest=descriptor.manifest,
        deterministic_context=context,
        transition_sequence=2,
        last_applied_delivery_sequence=2,
        last_applied_delivery_id=claimed.delivery_id,
        last_closed_delivery_sequence=2,
        state=thaw_json_v1(state.state),
        last_applied_event_id=event.event_id,
    )
    transition_id = "mqtransition_" + hash_hex_v1(
        "miniqmt_algo_transition_identity_v1",
        {
            "delivery_id": claimed.delivery_id,
            "event_id": event.event_id,
            "runtime_id": event.runtime_id,
            "algo_instance_id": algo.algo_instance_id,
            "transition_sequence": 2,
        },
    )
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id=transition_id,
        ordinal=0,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.000000",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="MINIQMT_ALGO_SLICE_DUE",
        metadata={"slice": 1},
    )
    next_state_payload = thaw_json_v1(state.state)
    next_state_payload["active_orders"] = [
        CurrentThreeActiveOrderStateV3.create(
            local_vt_orderid=command.local_vt_orderid,
            submit_command_id=command.command_id,
            broker_order_id=None,
            symbol=command.symbol,
            side=command.side,
            status=CurrentThreeActiveOrderStatusV3.COMMAND_PENDING,
            pending_command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            pending_command_id=command.command_id,
            requested_price_decimal=command.price_decimal,
            requested_quantity=command.quantity,
            cumulative_filled_quantity=0,
            remaining_quantity=command.quantity,
            last_order_event_id=None,
            last_trade_event_id=None,
            last_command_outcome_event_id=None,
            last_oms_reconcile_event_id=None,
            terminal_order_status=None,
            terminal_observed_cumulative_filled_quantity=None,
            market_data_lineage=next_state_payload["last_market_data_lineage"],
        ).model_dump(mode="json")
    ]
    next_state = AlgoStateSnapshotV2.create(
        plugin_manifest=descriptor.manifest,
        deterministic_context=context,
        transition_sequence=2,
        last_applied_delivery_sequence=2,
        last_applied_delivery_id=claimed.delivery_id,
        last_closed_delivery_sequence=2,
        state=next_state_payload,
        last_applied_event_id=event.event_id,
    )
    timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=algo.algo_instance_id,
        transition_id=transition_id,
        ordinal=1,
        timer_name="next_slice",
        schedule_epoch="session_worker_k2b",
        due_at_exchange_utc="2026-07-26T01:31:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    diagnostic = DiagnosticObservationV1.create(
        deterministic_context=context,
        transition_id=transition_id,
        ordinal=2,
        severity=DiagnosticSeverityV1.INFO,
        reason_code="MINIQMT_ALGO_SLICE_SCHEDULED",
        message="next slice scheduled",
        context={"schedule_id": timer.schedule_id},
    )
    effect_payload = {
        "next_state_sha256": next_state.state_sha256,
        "ordered_command_ids": [command.command_id],
        "ordered_timer_mutation_ids": [timer.mutation_identity_v1()],
        "ordered_diagnostic_observation_ids": [diagnostic.observation_id],
        "terminal_outcome": None,
    }
    transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=next_state,
        broker_commands=(command,),
        timer_mutations=(timer,),
        diagnostic_observations=(diagnostic,),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
    )
    bundle = materialize_applied_transition_v1(
        event=event,
        predecessor_delivery=claimed,
        previous_algo=algo,
        transition=transition,
        projection_set=projection_set,
        consumed_lineage_refs=_event_lineage(event, projection_set),
        strategy_slot_id=algo.strategy_slot_id,
        parent_intent_id=algo.parent_intent_id,
        compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
        plugin_config=thaw_json_v1(algo.plugin_config_json),
        plugin_config_sha256=algo.plugin_config_sha256,
        target_quantity=algo.target_quantity,
        algo_code=algo.algo_code,
        symbol=algo.symbol,
        side=algo.side,
        command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
        existing_mappings_by_local_vt_orderid={},
        existing_timer_schedules={},
        initialization=False,
    )
    assert len(bundle.new_child_mappings) == 1
    assert bundle.command_outboxes[0].mapping_id == bundle.new_child_mappings[0].mapping_id
    assert bundle.timer_schedules[0].schedule_id == timer.schedule_id
    assert bundle.diagnostic_observations == (diagnostic,)

    with pytest.raises(KernelEffectMaterializationError) as missing_event:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=transition,
            projection_set=projection_set,
            consumed_lineage_refs=(),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert missing_event.value.reason_code == "MINIQMT_ALGO_TRANSITION_EVENT_LINEAGE_INVALID"

    market_only = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        projection_refs=(_market_ref(event),),
    )
    with pytest.raises(KernelEffectMaterializationError) as missing_command_authority:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=transition,
            projection_set=market_only,
            consumed_lineage_refs=_event_lineage(event, market_only),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert missing_command_authority.value.reason_code == "MINIQMT_ALGO_TRANSITION_COMMAND_AUTHORITY_MISSING"

    invalid_refs = list(_command_projection_refs(event))
    risk_index = next(
        index for index, item in enumerate(invalid_refs) if item.projection_type is KernelProjectionTypeV1.RISK_DECISION
    )
    invalid_refs[risk_index] = ExecutionProjectionRefV1.create(
        projection_type=KernelProjectionTypeV1.RISK_DECISION,
        projection_id="risk_not_a_durable_receipt",
        projection_version="miniqmt_risk_decision_receipt_v0",
        payload_sha256="7" * 64,
        source_event_id=event.event_id,
        logical_at_utc=event.event_time_utc,
    )
    invalid_command_authority_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        event_id=event.event_id,
        delivery_id=claimed.delivery_id,
        projection_refs=tuple(sorted(invalid_refs, key=lambda item: (item.projection_type.value, item.projection_id))),
    )
    with pytest.raises(KernelEffectMaterializationError) as invalid_command_authority:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=transition,
            projection_set=invalid_command_authority_set,
            consumed_lineage_refs=_event_lineage(event, invalid_command_authority_set),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert invalid_command_authority.value.reason_code == "MINIQMT_ALGO_TRANSITION_COMMAND_AUTHORITY_INVALID"

    def assert_market_authority_rejected(
        market_projection_ref: ExecutionProjectionRefV1,
        *,
        lineage_payload_sha256: str | None = None,
    ) -> None:
        refs = [
            market_projection_ref if item.projection_type is KernelProjectionTypeV1.MARKET_DATA else item
            for item in _command_projection_refs(event)
        ]
        candidate_set = ExecutionProjectionSetV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            delivery_id=claimed.delivery_id,
            projection_refs=tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id))),
        )
        lineages = list(_event_lineage(event, candidate_set))
        if lineage_payload_sha256 is not None:
            lineages[-1] = ConsumedLineageRefV1.create(
                lineage_type=ConsumedLineageTypeV1.MARKET_DATA,
                identity=market_projection_ref.projection_id,
                payload_sha256=lineage_payload_sha256,
            )
        with pytest.raises(KernelEffectMaterializationError) as raised:
            materialize_applied_transition_v1(
                event=event,
                predecessor_delivery=claimed,
                previous_algo=algo,
                transition=transition,
                projection_set=candidate_set,
                consumed_lineage_refs=tuple(lineages),
                strategy_slot_id=algo.strategy_slot_id,
                parent_intent_id=algo.parent_intent_id,
                compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
                plugin_config=thaw_json_v1(algo.plugin_config_json),
                plugin_config_sha256=algo.plugin_config_sha256,
                target_quantity=algo.target_quantity,
                algo_code=algo.algo_code,
                symbol=algo.symbol,
                side=algo.side,
                command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
                existing_mappings_by_local_vt_orderid={},
                existing_timer_schedules={},
                initialization=False,
            )
        assert raised.value.reason_code == "MINIQMT_ALGO_TRANSITION_MARKET_DATA_LINEAGE_INVALID"

    assert_market_authority_rejected(
        ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.MARKET_DATA,
            projection_id="wrong_market_k2b",
            projection_version="miniqmt_market_data_projection_v2",
            payload_sha256="6" * 64,
            source_event_id=event.event_id,
            logical_at_utc=event.event_time_utc,
        )
    )
    assert_market_authority_rejected(
        ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.MARKET_DATA,
            projection_id="market_k2b",
            projection_version="miniqmt_market_data_projection_v2",
            payload_sha256="6" * 64,
            source_event_id="event_wrong_market_owner",
            logical_at_utc=event.event_time_utc,
        )
    )
    assert_market_authority_rejected(_market_ref(event), lineage_payload_sha256="6" * 64)

    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id=transition_id,
        ordinal=0,
        local_vt_orderid="vord_missing_k2b",
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.000000",
        quantity=100,
        owned_broker_order_id="broker_missing_k2b",
        reason_code="MINIQMT_ALGO_CANCEL_ACTIVE_CHILD",
        metadata={},
    )
    cancel_effect = {
        "next_state_sha256": next_state.state_sha256,
        "ordered_command_ids": [cancel.command_id],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    cancel_transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=next_state,
        broker_commands=(cancel,),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", cancel_effect),
    )
    with pytest.raises(KernelEffectMaterializationError) as cancel_owner:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=cancel_transition,
            projection_set=projection_set,
            consumed_lineage_refs=_event_lineage(event, projection_set),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert cancel_owner.value.reason_code == "MINIQMT_ALGO_TRANSITION_CANCEL_OWNER_INVALID"

    cancel_timer = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.CANCEL,
        algo_instance_id=algo.algo_instance_id,
        transition_id=transition_id,
        ordinal=0,
        timer_name="missing_timer",
        schedule_epoch="session_worker_k2b",
        due_at_exchange_utc=None,
        catch_up_policy="EXPIRE_IF_LATE",
        payload={},
    )
    timer_effect = {
        "next_state_sha256": unchanged_state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [cancel_timer.mutation_identity_v1()],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    timer_transition = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=unchanged_state,
        broker_commands=(),
        timer_mutations=(cancel_timer,),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", timer_effect),
    )
    with pytest.raises(KernelEffectMaterializationError) as timer_owner:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=timer_transition,
            projection_set=projection_set,
            consumed_lineage_refs=_event_lineage(event, projection_set),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert timer_owner.value.reason_code == "MINIQMT_TIMER_CANCEL_OWNER_INVALID"

    rejected_effect = {
        "next_state_sha256": unchanged_state.state_sha256,
        "ordered_command_ids": [],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": TerminalOutcomeV1.REJECTED.value,
    }
    rejected = AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=unchanged_state,
        broker_commands=(),
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=TerminalOutcomeV1.REJECTED,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", rejected_effect),
    )
    with pytest.raises(KernelEffectMaterializationError) as invalid_terminal:
        materialize_applied_transition_v1(
            event=event,
            predecessor_delivery=claimed,
            previous_algo=algo,
            transition=rejected,
            projection_set=projection_set,
            consumed_lineage_refs=_event_lineage(event, projection_set),
            strategy_slot_id=algo.strategy_slot_id,
            parent_intent_id=algo.parent_intent_id,
            compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
            plugin_config=thaw_json_v1(algo.plugin_config_json),
            plugin_config_sha256=algo.plugin_config_sha256,
            target_quantity=algo.target_quantity,
            algo_code=algo.algo_code,
            symbol=algo.symbol,
            side=algo.side,
            command_lifecycle_projection=_empty_lifecycle_projection(event, claimed, algo),
            existing_mappings_by_local_vt_orderid={},
            existing_timer_schedules={},
            initialization=False,
        )
    assert invalid_terminal.value.reason_code == "MINIQMT_ALGO_TRANSITION_TERMINAL_OUTCOME_INVALID"


def test_failure_materializer_cancels_owned_child_and_timer_but_does_not_invent_broker_identity() -> None:
    event, delivery, algo, _state_snapshot = _worker_facts()
    lease_owner = "worker_k2b:incarnation_k2b"
    fence = kernel_lease_fence_token_v1(
        owner_type="DELIVERY",
        owner_id=delivery.delivery_id,
        lease_epoch=1,
        lease_owner=lease_owner,
    )
    delivery_payload = delivery.model_dump(mode="python")
    delivery_payload.update(
        status=DeliveryStatusV1.CLAIMED,
        attempt_count=1,
        lease_owner=lease_owner,
        lease_epoch=1,
        lease_fence_token=fence,
        lease_expires_at="2026-07-26T01:31:00Z",
        row_version=2,
    )
    claimed = AlgoDeliveryPersistenceV1.model_validate(delivery_payload)
    algo_payload = algo.model_dump(mode="python")
    algo_payload["active_child_count"] = 2
    algo = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)

    def mapping(*, ordinal: int, broker_order_id: str | None):
        command = BrokerCommandV2.create(
            command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            parent_intent_id=algo.parent_intent_id,
            transition_id="transition_active_child_k2b",
            ordinal=ordinal,
            local_vt_orderid=None,
            symbol=algo.symbol,
            side=algo.side,
            order_type=OrderTypeV1.LIMIT,
            price_decimal="10.000000",
            quantity=50,
            owned_broker_order_id=None,
            reason_code="MINIQMT_ALGO_SLICE_DUE",
            metadata={"slice": ordinal},
        )
        return ExecutionCommandChildMappingV1.create(
            command=command,
            strategy_slot_id=algo.strategy_slot_id,
            mapping_status=(
                CommandChildMappingStatusV1.BROKER_ACCEPTED
                if broker_order_id is not None
                else CommandChildMappingStatusV1.RESERVED
            ),
            mapping_version=2 if broker_order_id is not None else 1,
            broker_order_id=broker_order_id,
            broker_identity_source_event_id=event.event_id if broker_order_id is not None else None,
            last_order_event_id=event.event_id if broker_order_id is not None else None,
            last_trade_event_id=None,
            updated_by_event_id=event.event_id if broker_order_id is not None else None,
            created_at_utc="2026-07-26T01:29:00Z",
            updated_at_utc=("2026-07-26T01:29:30Z" if broker_order_id is not None else "2026-07-26T01:29:00Z"),
        ), command

    (accepted, _accepted_command) = mapping(ordinal=0, broker_order_id="broker_owned_k2b")
    (unknown, unknown_command) = mapping(ordinal=1, broker_order_id=None)
    unknown_outbox = BrokerCommandOutboxV1.create(
        command=unknown_command,
        mapping_id=unknown.mapping_id,
        status=BrokerCommandOutboxStatusV1.PENDING,
        attempt_count=0,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at=None,
        dispatch_attempt_id=None,
        next_attempt_at_utc=None,
        broker_called=None,
        broker_order_id=None,
        ack_receipt_json=None,
        ack_receipt_sha256=None,
        non_acceptance_receipt=None,
        unknown_outcome_receipt=None,
        reconcile_receipt=None,
        last_error_json=None,
        row_version=1,
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc="2026-07-26T01:29:00Z",
        closed_at_utc=None,
    )
    timer_mutation = TimerMutationV1.create(
        mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
        algo_instance_id=algo.algo_instance_id,
        transition_id="transition_timer_k2b",
        ordinal=0,
        timer_name="next_slice",
        schedule_epoch="session_worker_k2b",
        due_at_exchange_utc="2026-07-26T01:31:00Z",
        catch_up_policy="EXPIRE_IF_LATE",
        payload={"slice": 2},
    )
    timer = ExecutionAlgoTimerScheduleV1.create(
        runtime_id=event.runtime_id,
        mutation=timer_mutation,
        status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
        emitted_event_id=None,
        lease_owner=None,
        lease_epoch=0,
        lease_fence_token=None,
        lease_expires_at_utc=None,
        row_version=1,
        created_at_utc="2026-07-26T01:29:00Z",
        updated_at_utc="2026-07-26T01:29:00Z",
        closed_at_utc=None,
    )

    bundle = materialize_failure_transition_v1(
        event=event,
        predecessor_delivery=claimed,
        previous_algo=algo,
        algo_code=algo.algo_code,
        plugin_id=algo.plugin_id,
        plugin_version=algo.plugin_version,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        plugin_config=thaw_json_v1(algo.plugin_config_json),
        plugin_config_sha256=algo.plugin_config_sha256,
        compatibility_receipt_sha256=algo.compatibility_receipt_sha256,
        parent_intent_id=algo.parent_intent_id,
        strategy_slot_id=algo.strategy_slot_id,
        symbol=algo.symbol,
        side=algo.side,
        target_quantity=algo.target_quantity,
        stable_reason_code="MINIQMT_ALGO_PLUGIN_TRANSITION_FAILED",
        exception=RuntimeError("deterministic plugin failure"),
        failure_context={"stage": "TRANSITION"},
        active_mappings=(accepted, unknown),
        active_command_outboxes=(unknown_outbox,),
        active_timer_schedules=(timer,),
        logical_time_utc=event.event_time_utc,
        initialization=False,
    )

    assert bundle.algo_instance.active_child_closure_status is ActiveChildClosureStatusV1.CANCEL_PENDING
    assert bundle.algo_instance.active_child_count == 1
    assert len(bundle.command_outboxes) == 1
    cancel_payload = thaw_json_v1(bundle.command_outboxes[0].payload_json)
    assert cancel_payload["owned_broker_order_id"] == "broker_owned_k2b"
    assert cancel_payload["local_vt_orderid"] == accepted.local_vt_orderid
    assert bundle.timer_schedules[0].status is ExecutionAlgoTimerScheduleStatusV1.CANCELLED
    assert bundle.timer_schedules[0].schedule_id == timer.schedule_id
    assert bundle.updated_child_mappings[0].mapping_status is CommandChildMappingStatusV1.TERMINAL
    assert bundle.updated_child_mappings[0].mapping_id == unknown.mapping_id
    assert bundle.updated_command_outboxes[0].status is BrokerCommandOutboxStatusV1.FAILED_TERMINAL
    assert bundle.updated_command_outboxes[0].broker_called is False
