from __future__ import annotations

import inspect
import json

import pytest
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

from backend.execution_algos.vnpy_style import (
    hot_best_limit_plugin,
    hot_plugin_manifests,
    hot_sniper_plugin,
    hot_twap_lite_plugin,
)
from backend.execution_algos.vnpy_compat.hot_facade_adapter import IcebergHotTargetV4, StopHotTargetV4
from backend.execution_algos.vnpy_compat.hot_facade_contracts import hot_facade_manifests_v4
from backend.execution_algos.vnpy_style.hot_best_limit_plugin import BestLimitHotTargetV4
from backend.execution_algos.vnpy_style.hot_sniper_plugin import SniperHotTargetV4
from backend.execution_algos.vnpy_style.hot_twap_lite_plugin import TwapLiteHotTargetV4
from backend.services.miniqmt_execution_runtime.kernel_ingress import (
    KernelEventRoutingError,
    KernelIngressCoordinatorV1,
)
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
)
from backend.services.miniqmt_execution_runtime.hot_market_data import (
    HotMarketDataDispositionV1,
    HotMarketDataIngressError,
    HotMarketDataIngressV1,
    _PendingHotMarketEffectV1,
)
from backend.services.miniqmt_execution_runtime.full_five_catalog_authority import (
    build_hot_full_five_catalog_authority_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    DeterministicExecutionContextV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionProjectionSetV1,
    RuntimeEventEnvelopeV2,
    SessionPhaseV1,
    SideV1,
    _algo_instance_id_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import freeze_json_v1, hash_hex_v1, thaw_json_v1
from backend.services.simulation_runtime.miniqmt_kernel_product import SimulationMiniQMTProductRuntimeV1
from backend.services.simulation_runtime.miniqmt_kernel_product import build_k6d_gateway_catalog_v1


def _forged_hot_tick() -> RuntimeEventEnvelopeV2:
    """Construct a historical carrier without invoking the successor validator."""

    return RuntimeEventEnvelopeV2.model_construct(
        schema_version="miniqmt_runtime_event_envelope_v2",
        event_id="mqrtevt_forged_hot_tick",
        event_key_sha256="a" * 64,
        runtime_id="runtime_hot_tick",
        sequence=2,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-08-12T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload=freeze_json_v1({}),
        payload_sha256="b" * 64,
        source_identity=freeze_json_v1({}),
        correlation=freeze_json_v1({}),
    )


def test_process_local_tick_carrier_is_never_a_durable_ingress_grant() -> None:
    event = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_hot_tick",
        sequence=2,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-08-12T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"bid_price_1": "10.00", "ask_price_1": "10.01"},
        source_identity={"market_data_id": "market_hot_tick"},
        correlation={},
    )
    assert event.event_type is EventTypeV2.TICK


def test_durable_ingress_rejects_forged_hot_tick_before_repository_access() -> None:
    class Repository:
        def ingest_routed_event_atomic(self, **_values):
            raise AssertionError("hot TICK must be rejected before repository access")

    coordinator = object.__new__(KernelIngressCoordinatorV1)
    object.__setattr__(coordinator, "repository", Repository())
    object.__setattr__(coordinator, "catalog_runtime", object())
    with pytest.raises(KernelEventRoutingError) as exc_info:
        coordinator.ingest(event=_forged_hot_tick())
    assert exc_info.value.reason_code == "MINIQMT_HOT_MARKET_DATA_DURABLE_INGRESS_FORBIDDEN"


def test_repository_public_seam_rejects_tick_before_connection_access() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_repository import KernelRepositoryConflict
    from backend.services.miniqmt_execution_runtime.kernel_repository_event_delivery import (
        KernelRepositoryEventDeliveryMixin,
    )

    repository = object.__new__(KernelRepositoryEventDeliveryMixin)
    with pytest.raises(KernelRepositoryConflict, match="ordinary TICK"):
        repository.ingest_routed_event_atomic(
            event=_forged_hot_tick(),
            catalog_runtime=object(),
            correlated_algo_instance_ids=(),
        )


def test_product_quote_callback_has_zero_durable_or_scheduler_dependencies() -> None:
    source = inspect.getsource(SimulationMiniQMTProductRuntimeV1.observe_b0_quote_v1)
    for forbidden in (
        "wake_clock_v1",
        "self.repository",
        "_ingest_bounded_v1",
        "ingest_native_event_v1",
        "dispatch_due_outbox_v1",
        "reconcile_due_outbox_v1",
    ):
        assert forbidden not in source
    assert "hot_market_data_ingress" in source


def test_current_three_durable_schemas_and_sources_contain_no_market_lineage() -> None:
    forbidden = {
        "last_tick_lineage",
        "last_market_data_lineage",
        "market_data_lineage",
        "market_data_id",
        "normalized_quote_sha256",
        "payload_sha256",
    }
    manifests = hot_plugin_manifests.current_three_hot_manifests_v4()
    durable_schema_text = json.dumps(
        [
            {
                "state": thaw_json_v1(item.state_schema),
            }
            for item in manifests
        ],
        sort_keys=True,
    )
    for token in forbidden:
        assert token not in durable_schema_text

    emitted_command_sources = "\n".join(
        inspect.getsource(module) for module in (hot_sniper_plugin, hot_best_limit_plugin, hot_twap_lite_plugin)
    )
    assert 'metadata={"market_data_lineage"' not in emitted_command_sources


def test_hot_quote_callback_does_not_own_outbox_or_reconciliation_cadence() -> None:
    source = inspect.getsource(SimulationMiniQMTProductRuntimeV1.observe_b0_quote_v1)
    scheduler_source = inspect.getsource(SimulationMiniQMTProductRuntimeV1.scheduler_tick_v1)
    assert "dispatch_due_outbox_v1" not in source
    assert "reconcile_due_outbox_v1" not in source
    assert "dispatch_due_outbox_v1" in scheduler_source or "wake_clock_v1" in scheduler_source


def _hot_view(*, sequence: int = 1) -> HotMarketDataViewV1:
    return HotMarketDataViewV1(
        runtime_id="runtime_hot_tick",
        symbol="600000.SH",
        generation=1,
        sequence=sequence,
        observed_at_utc=datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
        exchange_time_utc=datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
        exchange_trade_date="2026-08-12",
        session_epoch="session_hot_tick",
        session_phase="CONTINUOUS_AM",
        bid_price_1=Decimal("10.00"),
        ask_price_1=Decimal("10.01"),
        bid_volume_1=1000,
        ask_volume_1=900,
        last_price=Decimal("10.00"),
        pre_close=Decimal("9.90"),
        limit_up=Decimal("10.89"),
        limit_down=Decimal("8.91"),
    )


_HOT_CONFIGS = {
    "BEST_LIMIT_MINIQMT": {"min_volume": 100, "max_volume": 200},
    "ICEBERG": {"display_volume": 100, "interval": 1},
    "SNIPER_MINIQMT": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
    "STOP": {"price_add": "0.01"},
    "TWAP_LITE_MINIQMT": {"time": 2, "interval": 1},
}


def _hot_manifest(algo_code: str):
    return next(
        item
        for item in (*hot_plugin_manifests.current_three_hot_manifests_v4(), *hot_facade_manifests_v4())
        if item.algo_code == algo_code
    )


def _hot_plugin(algo_code: str):
    if algo_code in {"ICEBERG", "STOP"}:
        from backend.execution_algos.vnpy_compat.hot_facade_adapter import (
            create_iceberg_hot_plugin_v4,
            create_stop_hot_plugin_v4,
        )

        factory = create_iceberg_hot_plugin_v4 if algo_code == "ICEBERG" else create_stop_hot_plugin_v4
    else:
        from backend.execution_algos.vnpy_style.hot_plugin_factories import (
            create_best_limit_miniqmt_plugin_v4,
            create_sniper_miniqmt_plugin_v4,
            create_twap_lite_miniqmt_plugin_v4,
        )

        factory = {
            "BEST_LIMIT_MINIQMT": create_best_limit_miniqmt_plugin_v4,
            "SNIPER_MINIQMT": create_sniper_miniqmt_plugin_v4,
            "TWAP_LITE_MINIQMT": create_twap_lite_miniqmt_plugin_v4,
        }[algo_code]
    return factory(_HOT_CONFIGS[algo_code])


def _hot_initialized(algo_code: str):
    manifest = _hot_manifest(algo_code)
    config = _HOT_CONFIGS[algo_code]
    config_sha256 = hash_hex_v1("miniqmt_plugin_config_v2", config)
    parent_intent_id = f"intent_hot_{algo_code.lower()}"
    slot_id = f"slot_hot_{algo_code.lower()}"
    algo_id = _algo_instance_id_v2(
        runtime_id="runtime_hot_tick",
        parent_intent_id=parent_intent_id,
        strategy_slot_id=slot_id,
        algo_code=algo_code,
        plugin_id=manifest.plugin_id,
        plugin_version=manifest.plugin_version,
        plugin_manifest_sha256=manifest.manifest_sha256,
        plugin_config_sha256=config_sha256,
    )
    deterministic = DeterministicExecutionContextV1.create(
        runtime_id="runtime_hot_tick",
        algo_instance_id=algo_id,
        event_id=f"event_start_{algo_code.lower()}",
        delivery_id=f"delivery_start_{algo_code.lower()}",
        plugin_manifest_sha256=manifest.manifest_sha256,
        transition_sequence=0,
        logical_time_utc="2026-08-12T01:29:59Z",
        exchange_trade_date="2026-08-12",
        session_epoch="session_hot_tick",
        session_phase=SessionPhaseV1.CONTINUOUS_AM,
        input_projection_sha256="9" * 64,
    )
    contract = {"pricetick_decimal": "0.01", "min_volume": 100}
    account = {"account_group_id": "sim_account"}
    capability = {"route_id": "miniqmt_sim_b0", "capabilities": ["L1_ASK", "L1_BID"]}
    context = AlgoStartContextV1(
        schema_version="miniqmt_algo_start_context_v1",
        runtime_id=deterministic.runtime_id,
        algo_instance_id=algo_id,
        parent_intent_id=parent_intent_id,
        strategy_slot_id=slot_id,
        symbol="600000.SH",
        side=SideV1.BUY,
        limit_price_decimal="10.01",
        parent_quantity=200,
        min_volume=100,
        volume_increment=100,
        plugin_manifest=manifest,
        plugin_config=config,
        plugin_config_sha256=config_sha256,
        start_event_id=deterministic.event_id,
        start_delivery_id=deterministic.delivery_id,
        deterministic_context=deterministic,
        contract_projection=contract,
        contract_projection_sha256=hash_hex_v1("miniqmt_contract_projection_v1", contract),
        account_projection=account,
        account_projection_sha256=hash_hex_v1("miniqmt_account_projection_v1", account),
        market_capability_projection=capability,
        market_capability_projection_sha256=hash_hex_v1("miniqmt_market_capability_projection_v1", capability),
        execution_plan_id="plan_hot_tick",
        execution_plan_sha256="a" * 64,
        release_id="release_hot_tick",
        release_sha256="b" * 64,
        policy_id="policy_hot_tick",
        policy_sha256="c" * 64,
    )
    plugin = _hot_plugin(algo_code)
    initialization = plugin.initialize(context)
    return plugin, context, initialization.next_state


def _hot_persistence(algo_code: str, context: AlgoStartContextV1, state) -> ExecutionAlgoInstancePersistenceV2:
    return ExecutionAlgoInstancePersistenceV2.create(
        algo_instance_id=context.algo_instance_id,
        runtime_id=context.runtime_id,
        parent_intent_id=context.parent_intent_id,
        strategy_slot_id=context.strategy_slot_id,
        symbol=context.symbol,
        side=context.side,
        target_quantity=context.parent_quantity,
        traded_quantity=0,
        remaining_quantity=context.parent_quantity,
        algo_code=algo_code,
        plugin_id=context.plugin_manifest.plugin_id,
        plugin_version=context.plugin_manifest.plugin_version,
        plugin_manifest_sha256=context.plugin_manifest.manifest_sha256,
        plugin_config_json=_HOT_CONFIGS[algo_code],
        plugin_config_sha256=context.plugin_config_sha256,
        compatibility_receipt_sha256="d" * 64,
        state_schema_version=state.state_schema_version,
        state_json=thaw_json_v1(state.state),
        state_sha256=state.state_sha256,
        transition_sequence=state.transition_sequence,
        last_applied_delivery_sequence=state.last_applied_delivery_sequence,
        last_applied_delivery_id=state.last_applied_delivery_id,
        last_closed_delivery_sequence=state.last_closed_delivery_sequence,
        terminal_delivery_sequence=None,
        status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
        failure_receipt_id=None,
        active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
        active_child_count=0,
        row_version=1,
        created_at_utc=state.updated_at_utc,
        updated_at_utc=state.updated_at_utc,
        terminal_at_utc=None,
        archived_at_utc=None,
    )


def _advance_one_timer(plugin, state, *, timer_name: str):
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=state.algo_instance_id and "runtime_hot_tick",
        sequence=2,
        event_type=EventTypeV2.TIMER,
        event_time_utc="2026-08-12T01:30:00Z",
        monotonic_ns=2,
        source=EventSourceV2.EXCHANGE_SESSION_CLOCK,
        symbol="600000.SH",
        payload_schema_version="miniqmt_timer_due_v1",
        payload={
            "timer_occurrence_id": f"occurrence_{timer_name.lower()}",
            "timer_name": timer_name,
            "schedule_epoch": "session_hot_tick",
        },
        source_identity={"timer_occurrence_id": f"occurrence_{timer_name.lower()}"},
        correlation={
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
        },
    )
    delivery_id = f"delivery_{timer_name.lower()}"
    projection_set = ExecutionProjectionSetV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        projection_refs=(),
    )
    services = AlgoReadOnlyServicesV1.create(
        runtime_id=event.runtime_id,
        algo_instance_id=state.algo_instance_id,
        event_id=event.event_id,
        delivery_id=delivery_id,
        contract_projection_id=None,
        contract_projection=None,
        market_data_projection_id=None,
        market_data_projection=None,
        account_projection_id=None,
        account_projection=None,
        execution_projection_set=projection_set,
    )
    return plugin.transition(state=state, event=event, services=services).next_state


@pytest.mark.parametrize(
    ("algo_code", "target_type", "timer_name"),
    (
        ("BEST_LIMIT_MINIQMT", BestLimitHotTargetV4, None),
        ("ICEBERG", IcebergHotTargetV4, "ICEBERG_ACTIVE_SECOND"),
        ("SNIPER_MINIQMT", SniperHotTargetV4, None),
        ("STOP", StopHotTargetV4, None),
        ("TWAP_LITE_MINIQMT", TwapLiteHotTargetV4, "TWAP_ACTIVE_SECOND"),
    ),
)
def test_all_five_v4_plugins_restore_without_market_state_and_decide_on_next_live_tick(
    algo_code: str,
    target_type,
    timer_name: str | None,
) -> None:
    plugin, context, state = _hot_initialized(algo_code)
    if timer_name is not None:
        state = _advance_one_timer(plugin, state, timer_name=timer_name)
    persisted = _hot_persistence(algo_code, context, state)
    durable_text = json.dumps(thaw_json_v1(persisted.state_json), sort_keys=True)
    for forbidden in ("market_data_lineage", "last_tick_lineage", "market_data_id", "normalized_quote_sha256"):
        assert forbidden not in durable_text
    restarted_target = target_type(algo=persisted)
    view = _hot_view()
    if algo_code == "STOP":
        view = replace(view, last_price=Decimal("10.02"))
    effect = restarted_target.evaluate_hot_market_data_v1(view)
    assert effect is not None
    assert effect.runtime_id == context.runtime_id
    assert effect.algo_instance_id == context.algo_instance_id
    effect_text = json.dumps(thaw_json_v1(effect.economic_payload), sort_keys=True)
    for forbidden in ("market_data_lineage", "market_data_id", "normalized_quote_sha256", "quote_payload"):
        assert forbidden not in effect_text


def test_process_local_no_effect_and_duplicate_paths_never_call_committer() -> None:
    commits: list[object] = []

    class Target:
        runtime_id = "runtime_hot_tick"
        algo_instance_id = "algo_hot_tick"
        symbol = "600000.SH"

        @staticmethod
        def evaluate_hot_market_data_v1(_view):
            return None

        @staticmethod
        def accept_committed_effect_v1(_effect, _readback):
            raise AssertionError("NO_EFFECT must not acknowledge a durable effect")

    ingress = HotMarketDataIngressV1(
        runtime_id="runtime_hot_tick",
        effect_committer=lambda effect: commits.append(effect),
    )
    ingress.replace_targets_v1((Target(),))
    first = ingress.ingest_v1(_hot_view())
    duplicate = ingress.ingest_v1(_hot_view())
    assert first.disposition is HotMarketDataDispositionV1.NO_EFFECT
    assert duplicate.disposition is HotMarketDataDispositionV1.DUPLICATE
    assert commits == []


def test_hot_generation_high_watermark_is_bounded_and_rejects_late_old_generation() -> None:
    ingress = HotMarketDataIngressV1(runtime_id="runtime_hot_tick", effect_committer=lambda _effect: None)
    first = ingress.ingest_v1(_hot_view(sequence=10))
    successor = ingress.ingest_v1(replace(_hot_view(sequence=1), generation=2))
    late = ingress.ingest_v1(replace(_hot_view(sequence=11), generation=1))
    duplicate = ingress.ingest_v1(replace(_hot_view(sequence=1), generation=2))
    assert first.disposition is HotMarketDataDispositionV1.NO_TARGET
    assert successor.disposition is HotMarketDataDispositionV1.NO_TARGET
    assert late.disposition is HotMarketDataDispositionV1.STALE
    assert duplicate.disposition is HotMarketDataDispositionV1.DUPLICATE
    assert ingress._generation_by_symbol == {"600000.SH": 2}
    assert ingress._last_sequence_by_symbol == {"600000.SH": 1}


def test_hot_full_five_authority_publishes_exact_v4_catalog_once() -> None:
    authority = build_hot_full_five_catalog_authority_v1(gateway_catalog=build_k6d_gateway_catalog_v1())
    snapshot = authority.catalog_runtime.snapshot
    assert [item.algo_code for item in snapshot.creation_bindings] == [
        "BEST_LIMIT_MINIQMT",
        "ICEBERG",
        "SNIPER_MINIQMT",
        "STOP",
        "TWAP_LITE_MINIQMT",
    ]
    assert len(snapshot.registration_descriptors) == 5
    assert len(authority.conformance_set.ordered_receipts) == 5
    assert authority.conformance_authority.validation_receipt.status.value == "PASSED"


def test_effect_commit_failure_never_retries_on_tick_and_scheduler_retains_same_identity() -> None:
    effect = HotMarketDataEconomicEffectV1(
        runtime_id="runtime_hot_tick",
        algo_instance_id="algo_hot_tick",
        expected_algo_row_version=3,
        effect_identity="mqhoteffect_economic_1",
        economic_payload={
            "action": "SUBMIT_LIMIT",
            "symbol": "600000.SH",
            "side": "BUY",
            "price_decimal": "10",
            "quantity": 100,
            "reason_code": "sniper_ask_crossed_limit",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
        },
    )
    evaluated: list[int] = []
    accepted: list[object] = []

    class Target:
        runtime_id = "runtime_hot_tick"
        algo_instance_id = "algo_hot_tick"
        symbol = "600000.SH"

        @staticmethod
        def evaluate_hot_market_data_v1(view):
            evaluated.append(view.sequence)
            return effect

        @staticmethod
        def accept_committed_effect_v1(_effect, readback):
            accepted.append(readback)

    attempts: list[str] = []

    def commit(candidate):
        attempts.append(candidate.effect_identity)
        if len(attempts) == 1:
            raise ConnectionError("database unavailable")
        return {"effect_identity": candidate.effect_identity}

    ingress = HotMarketDataIngressV1(runtime_id="runtime_hot_tick", effect_committer=commit)
    ingress.replace_targets_v1((Target(),))
    with pytest.raises(HotMarketDataIngressError) as exc_info:
        ingress.ingest_v1(_hot_view(sequence=1))
    assert exc_info.value.reason_code == "MINIQMT_HOT_MARKET_EFFECT_COMMIT_FAILED"
    assert exc_info.value.context["broker_called"] is False
    assert accepted == []

    pending = ingress.ingest_v1(_hot_view(sequence=2))
    assert pending.disposition is HotMarketDataDispositionV1.EFFECT_PENDING
    assert attempts == [effect.effect_identity]
    recovered = ingress.retry_pending_v1(observed_at_utc=datetime(2026, 8, 12, 1, 30, 1, tzinfo=UTC))
    assert recovered.disposition is HotMarketDataDispositionV1.EFFECT_COMMITTED
    assert evaluated == [1]
    assert attempts == [effect.effect_identity, effect.effect_identity]
    assert accepted == [{"effect_identity": effect.effect_identity}]


def test_committed_effect_with_failed_readback_acceptance_retries_same_identity_only_on_scheduler() -> None:
    effect = HotMarketDataEconomicEffectV1(
        runtime_id="runtime_hot_tick",
        algo_instance_id="algo_hot_tick",
        expected_algo_row_version=3,
        effect_identity="mqhoteffect_commit_unknown",
        economic_payload={
            "action": "CANCEL_ORDER",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "reason_code": "test_cancel",
        },
    )

    class Target:
        runtime_id = "runtime_hot_tick"
        algo_instance_id = "algo_hot_tick"
        symbol = "600000.SH"
        accepted = False

        @staticmethod
        def evaluate_hot_market_data_v1(_view):
            return effect

        @classmethod
        def accept_committed_effect_v1(cls, candidate, readback):
            if readback.get("effect_identity") != candidate.effect_identity:
                raise ValueError("durable readback identity drift")
            cls.accepted = True

    attempts: list[str] = []

    def commit(candidate):
        attempts.append(candidate.effect_identity)
        if len(attempts) == 1:
            return {"effect_identity": "mqhoteffect_wrong_readback"}
        return {"effect_identity": candidate.effect_identity}

    ingress = HotMarketDataIngressV1(runtime_id="runtime_hot_tick", effect_committer=commit)
    ingress.replace_targets_v1((Target(),))
    with pytest.raises(HotMarketDataIngressError) as exc_info:
        ingress.ingest_v1(_hot_view(sequence=1))
    assert exc_info.value.reason_code == "MINIQMT_HOT_MARKET_EFFECT_COMMIT_FAILED"
    assert exc_info.value.context["effect_identity"] == effect.effect_identity
    assert not Target.accepted
    assert ingress.ingest_v1(_hot_view(sequence=2)).disposition is HotMarketDataDispositionV1.EFFECT_PENDING
    assert attempts == [effect.effect_identity]
    receipt = ingress.retry_pending_v1(observed_at_utc=datetime(2026, 8, 12, 1, 30, 1, tzinfo=UTC))
    assert receipt.disposition is HotMarketDataDispositionV1.EFFECT_COMMITTED
    assert attempts == [effect.effect_identity, effect.effect_identity]
    assert Target.accepted


def test_failed_scheduler_retry_uses_bounded_backoff_and_intervening_ticks_never_write() -> None:
    effect = HotMarketDataEconomicEffectV1(
        runtime_id="runtime_hot_tick",
        algo_instance_id="algo_hot_tick",
        expected_algo_row_version=3,
        effect_identity="mqhoteffect_backoff",
        economic_payload={
            "action": "CANCEL_ORDER",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "reason_code": "test_cancel",
        },
    )

    class Target:
        runtime_id = "runtime_hot_tick"
        algo_instance_id = "algo_hot_tick"
        symbol = "600000.SH"

        @staticmethod
        def evaluate_hot_market_data_v1(_view):
            return effect

        @staticmethod
        def accept_committed_effect_v1(_effect, _readback):
            raise AssertionError("failed commit cannot be acknowledged")

    attempts: list[str] = []

    def fail(candidate):
        attempts.append(candidate.effect_identity)
        raise ConnectionError("database unavailable")

    ingress = HotMarketDataIngressV1(runtime_id="runtime_hot_tick", effect_committer=fail)
    ingress.replace_targets_v1((Target(),))
    with pytest.raises(HotMarketDataIngressError):
        ingress.ingest_v1(_hot_view(sequence=1))
    for sequence in range(2, 102):
        assert ingress.ingest_v1(_hot_view(sequence=sequence)).disposition is HotMarketDataDispositionV1.EFFECT_PENDING
    assert attempts == [effect.effect_identity]
    with pytest.raises(HotMarketDataIngressError) as exc_info:
        ingress.retry_pending_v1(observed_at_utc=datetime(2026, 8, 12, 1, 30, 1, tzinfo=UTC))
    assert exc_info.value.reason_code == "MINIQMT_HOT_MARKET_EFFECT_RETRY_FAILED"
    assert attempts == [effect.effect_identity, effect.effect_identity]
    ingress.retry_pending_v1(observed_at_utc=datetime(2026, 8, 12, 1, 30, 2, tzinfo=UTC))
    assert attempts == [effect.effect_identity, effect.effect_identity]


def test_scheduler_retry_failure_evidence_has_bounded_omitted_set_hash_closure() -> None:
    effects: dict[str, HotMarketDataEconomicEffectV1] = {}

    class Target:
        def __init__(self, ordinal: int) -> None:
            self.runtime_id = "runtime_hot_tick"
            self.algo_instance_id = f"algo_hot_tick_{ordinal:03d}"
            self.symbol = "600000.SH"
            effects[self.algo_instance_id] = HotMarketDataEconomicEffectV1(
                runtime_id=self.runtime_id,
                algo_instance_id=self.algo_instance_id,
                expected_algo_row_version=1,
                effect_identity=f"mqhoteffect_{ordinal:03d}",
                economic_payload={
                    "action": "CANCEL_ORDER",
                    "action_time_utc": "2026-08-12T01:30:00Z",
                    "exchange_trade_date": "2026-08-12",
                    "session_epoch": "session_hot_tick",
                    "session_phase": "CONTINUOUS_AM",
                    "reason_code": "price_changed",
                },
            )

        def evaluate_hot_market_data_v1(self, _view):
            return effects[self.algo_instance_id]

        @staticmethod
        def accept_committed_effect_v1(_effect, _readback):
            return None

    def fail(_effect):
        raise ConnectionError("database unavailable")

    ingress = HotMarketDataIngressV1(runtime_id="runtime_hot_tick", effect_committer=fail)
    targets = tuple(Target(ordinal) for ordinal in range(66))
    ingress.replace_targets_v1(targets)
    for target in targets:
        ingress._pending_by_algo[target.algo_instance_id] = _PendingHotMarketEffectV1(
            effect=effects[target.algo_instance_id],
            target=target,
            failure_count=1,
            next_retry_at_utc=datetime(2026, 8, 12, 1, 30, tzinfo=UTC),
        )
    with pytest.raises(HotMarketDataIngressError) as exc_info:
        ingress.retry_pending_v1(observed_at_utc=datetime(2026, 8, 12, 1, 30, 1, tzinfo=UTC))
    evidence = exc_info.value.context
    assert evidence["failure_count"] == 66
    assert len(evidence["failures"]) == 64
    assert evidence["failures_truncated"] is True
    assert evidence["omitted_failure_count"] == 2
    assert len(evidence["omitted_failure_set_sha256"]) == 64


@pytest.mark.parametrize(
    "payload",
    [
        {"action": "SUBMIT_LIMIT", "market_data_id": "market_forbidden"},
        {"action": "SUBMIT_LIMIT", "market_data_lineage": {}},
        {"action": "SUBMIT_LIMIT", "normalized_quote_sha256": "a" * 64},
        {"action": "SUBMIT_LIMIT", "quote_payload": {"bid": "10"}},
    ],
)
def test_economic_effect_rejects_market_data_carriers(payload) -> None:
    with pytest.raises(ValueError, match="prohibited hot market-data evidence"):
        HotMarketDataEconomicEffectV1(
            runtime_id="runtime_hot_tick",
            algo_instance_id="algo_hot_tick",
            expected_algo_row_version=1,
            effect_identity="mqhoteffect_invalid",
            economic_payload=payload,
        )


def test_economic_effect_freezes_caller_payload_before_pending_retry() -> None:
    payload = {
        "action": "CANCEL_ORDER",
        "action_time_utc": "2026-08-12T01:30:00Z",
        "exchange_trade_date": "2026-08-12",
        "session_epoch": "session_hot_tick",
        "session_phase": "CONTINUOUS_AM",
        "reason_code": "price_changed",
    }
    effect = HotMarketDataEconomicEffectV1(
        runtime_id="runtime_hot_tick",
        algo_instance_id="algo_hot_tick",
        expected_algo_row_version=1,
        effect_identity="mqhoteffect_immutable",
        economic_payload=payload,
    )
    payload["action"] = "SUBMIT_LIMIT"
    payload["reason_code"] = "mutated"
    assert thaw_json_v1(effect.economic_payload) == {
        "action": "CANCEL_ORDER",
        "action_time_utc": "2026-08-12T01:30:00Z",
        "exchange_trade_date": "2026-08-12",
        "session_epoch": "session_hot_tick",
        "session_phase": "CONTINUOUS_AM",
        "reason_code": "price_changed",
    }


@pytest.mark.parametrize(
    ("updates", "message"),
    [
        ({"action_time_utc": "2026-08-12T09:30:00+08:00"}, "canonical UTC"),
        ({"action_time_utc": "not-a-time"}, "canonical UTC"),
        ({"exchange_trade_date": "2026-8-12"}, "exchange_trade_date"),
        ({"symbol": "600000"}, "owner fields"),
        ({"symbol": "not-a-symbol.SH"}, "owner fields"),
        ({"price_decimal": "NaN"}, "canonical positive decimal"),
        ({"price_decimal": []}, "price is invalid"),
        ({"unexpected": "field"}, "action schema"),
    ],
)
def test_submit_economic_effect_rejects_noncanonical_or_open_ended_payload(updates, message) -> None:
    payload = {
        "action": "SUBMIT_LIMIT",
        "action_time_utc": "2026-08-12T01:30:00Z",
        "exchange_trade_date": "2026-08-12",
        "session_epoch": "session_hot_tick",
        "session_phase": "CONTINUOUS_AM",
        "symbol": "600000.SH",
        "side": "BUY",
        "price_decimal": "10.01",
        "quantity": 100,
        "reason_code": "best_quote_changed",
    }
    payload.update(updates)
    with pytest.raises((TypeError, ValueError), match=message):
        HotMarketDataEconomicEffectV1(
            runtime_id="runtime_hot_tick",
            algo_instance_id="algo_hot_tick",
            expected_algo_row_version=1,
            effect_identity="mqhoteffect_invalid_submit",
            economic_payload=payload,
        )
