from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import os

import psycopg2
import pytest

from backend.services.miniqmt_execution_runtime.kernel_delivery import KernelTransitionWriteBundleV1
from backend.services.miniqmt_execution_runtime.kernel_product_authority import (
    bind_product_transition_receipt_v3,
    build_product_command_authority_set_v3,
)
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    ProductCommandAuthorityEnvelopeV3,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    ActiveChildClosureStatusV1,
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    AlgoStateSnapshotV2,
    AlgoTransitionReceiptV1,
    AlgoTransitionV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    DeterministicExecutionContextV1,
    DiagnosticObservationV1,
    DiagnosticSeverityV1,
    DeliveryStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    RuntimeEventEnvelopeV2,
    OrderTypeV1,
    SideV1,
    SessionPhaseV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    _algo_instance_id_v2,
    algo_transition_id_v1,
    kernel_lease_fence_token_v1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_k6_migration_postgres import (
    K6C_FORWARD,
    _apply_k2_and_k6,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_migration_postgres import (
    _dev_dsn,
    _fixture_schema,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_repository_postgres import (
    _commit_unknown_factory,
    _conn_factory,
    _seed_event_receipt_deliveries,
)
from backend.tests.miniqmt_execution_runtime.test_vnpy_facade_kernel_invocation import _v2_candidate
from backend.tests.miniqmt_execution_runtime.test_kernel_product_authority import _evidence_for_authority


def test_k6c1_product_repository_public_surface_is_complete() -> None:
    assert {
        "materialize_product_transition_atomic_v3",
        "read_product_materialization_v3",
    } <= set(dir(PostgresMiniQMTKernelRepository))


@pytest.mark.parametrize(
    "mode",
    (
        "zero",
        "materialize",
        "reject",
        "defer",
        "mixed",
        "rollback",
        "commit_unknown",
        "readback_drift",
        "mapping_drift",
        "coordination_drift",
        "effects",
        "transition_drift",
        "timer_drift",
        "diagnostic_drift",
        "outbox_missing",
        "claimed_lifecycle",
        "concurrent",
    ),
    ids=(
        "zero-command",
        "one-materialized-command",
        "one-rejected-command",
        "one-deferred-command",
        "mixed-materialize-reject-defer",
        "late-transaction-failure-rolls-back",
        "commit-unknown-readback-recovery",
        "durable-scalar-drift-fails-loud",
        "mapping-scalar-drift-fails-loud",
        "coordination-scalar-drift-fails-loud",
        "timer-and-diagnostic-effects",
        "transition-scalar-drift-fails-loud",
        "timer-scalar-drift-fails-loud",
        "diagnostic-scalar-drift-fails-loud",
        "outbox-missing-fails-loud",
        "claimed-outbox-lifecycle-readback",
        "same-authority-concurrent-writers",
    ),
)
def test_k6c1_product_transition_is_atomic_and_readback_verified_on_dev_postgres(mode: str) -> None:
    if os.getenv("AISTOCK_RUN_MINIQMT_K2_DEV_DB") != "1":
        pytest.skip("requires explicitly authorized disposable K6-C1 DEV PostgreSQL fixture")
    schema = _fixture_schema().replace("k2a_", "k6c1_", 1)
    raw = psycopg2.connect(**_dev_dsn())
    raw.autocommit = True
    try:
        with raw.cursor() as cur:
            _apply_k2_and_k6(cur, schema)
            cur.execute(K6C_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                ("runtime_k6c1", date(2026, 8, 1)),
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        authority_input = _v2_candidate()[2]
        manifest = authority_input.manifest
        config = {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}
        config_sha256 = hash_hex_v1("miniqmt_plugin_config_v2", config)
        algo_instance_id = _algo_instance_id_v2(
            runtime_id="runtime_k6c1",
            parent_intent_id="intent_k6c1",
            strategy_slot_id="slot_k6c1",
            algo_code=manifest.algo_code,
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            plugin_manifest_sha256=manifest.manifest_sha256,
            plugin_config_sha256=config_sha256,
        )
        initial_state = {"phase": "initial"}
        algo_v1 = ExecutionAlgoInstancePersistenceV2.create(
            algo_instance_id=algo_instance_id,
            runtime_id="runtime_k6c1",
            parent_intent_id="intent_k6c1",
            strategy_slot_id="slot_k6c1",
            symbol="600000.SH",
            side=SideV1.BUY,
            target_quantity=100,
            traded_quantity=0,
            remaining_quantity=100,
            algo_code=manifest.algo_code,
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            plugin_manifest_sha256=manifest.manifest_sha256,
            plugin_config_json=config,
            plugin_config_sha256=config_sha256,
            compatibility_receipt_sha256=authority_input.pinned_compatibility_receipt.receipt_sha256,
            state_schema_version=manifest.state_schema_version,
            state_json=initial_state,
            state_sha256=hash_hex_v1("execution_algo_state_v2", initial_state),
            transition_sequence=0,
            last_applied_delivery_sequence=0,
            last_applied_delivery_id=None,
            last_closed_delivery_sequence=0,
            terminal_delivery_sequence=None,
            status=ExecutionAlgoPersistenceStatusV2.ACTIVE,
            failure_receipt_id=None,
            active_child_closure_status=ActiveChildClosureStatusV1.NOT_APPLICABLE,
            active_child_count=0,
            row_version=1,
            created_at_utc="2026-08-03T01:20:00Z",
            updated_at_utc="2026-08-03T01:20:00Z",
            terminal_at_utc=None,
            archived_at_utc=None,
        )
        repository.compare_and_swap_algo_instance(algo_instance=algo_v1, expected_row_version=0)
        if mode in {"defer", "mixed", "coordination_drift"}:
            with raw.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO {schema}.execution_algo_instance(
                        algo_instance_id,runtime_id,parent_intent_id,strategy_slot_id,symbol,side,target_quantity,
                        remaining_quantity,algo_code,status,kernel_contract_version,traded_quantity,plugin_id,
                        plugin_version,plugin_manifest_sha256,plugin_config_json,plugin_config_sha256,
                        compatibility_receipt_sha256,state_schema_version,state_json,state_sha256,
                        transition_sequence,last_applied_delivery_sequence,last_closed_delivery_sequence,
                        active_child_closure_status,active_child_count,row_version,kernel_carrier_json
                    ) VALUES ('algo_sell','runtime_k6c1','intent_sell','slot_sell','600001.SH','SELL',100,100,
                              'TWAP','ACTIVE','KERNEL_V2',0,'aistock.twap','1.0.0',%s,'{{}}'::jsonb,%s,%s,
                              'twap_state_v1','{{}}'::jsonb,%s,0,0,0,'NOT_APPLICABLE',0,1,'{{}}'::jsonb)
                    """,
                    ("a" * 64, "b" * 64, "c" * 64, "d" * 64),
                )
        worker = repository.start_worker_incarnation(
            worker_id="worker_k6c1",
            process_role="delivery",
            source_revision="k6c1-test",
            started_at_utc="2026-08-03T01:20:00Z",
        )
        event = RuntimeEventEnvelopeV2.create(
            runtime_id="runtime_k6c1",
            sequence=1,
            event_type=EventTypeV2.TICK,
            event_time_utc="2026-08-03T01:30:00Z",
            monotonic_ns=None,
            source=EventSourceV2.B0_QUOTE_V2,
            symbol="600000.SH",
            payload_schema_version="miniqmt_market_data_view_v2",
            payload={"last_price": "10.00"},
            source_identity={"market_data_id": "market_k6c1_zero"},
            correlation={"trace_id": "trace_k6c1_zero"},
        )
        delivery = AlgoEventDeliveryV1.create(
            event=event,
            algo_instance_id=algo_instance_id,
            plugin_manifest_sha256=manifest.manifest_sha256,
            algo_delivery_sequence=1,
            previous_delivery_id=None,
            status=DeliveryStatusV1.PENDING,
            attempt_count=0,
            lease_owner=None,
            lease_expires_at=None,
            transition_id=None,
            last_error_json=None,
            created_at_utc="2026-08-03T01:30:00Z",
            updated_at_utc="2026-08-03T01:30:00Z",
        )
        pending = AlgoDeliveryPersistenceV1.create(
            delivery=delivery,
            lease_epoch=0,
            lease_fence_token=None,
            row_version=1,
            next_attempt_at_utc=None,
            failure_receipt_id=None,
            skip_receipt_id=None,
            closed_at_utc=None,
        )
        _seed_event_receipt_deliveries(repository, event=event, deliveries=(pending,))
        lease_owner = f"worker_k6c1:{worker.process_incarnation_id}"
        fence = kernel_lease_fence_token_v1(
            owner_type="DELIVERY",
            owner_id=pending.delivery_id,
            lease_epoch=1,
            lease_owner=lease_owner,
        )
        claimed = repository.claim_delivery(
            delivery_id=pending.delivery_id,
            lease_owner=lease_owner,
            lease_epoch=1,
            lease_fence_token=fence,
            lease_expires_at="2026-08-03T01:40:00Z",
            updated_at_utc="2026-08-03T01:30:30Z",
            expected_row_version=1,
        )
        commands: tuple[BrokerCommandV2, ...] = ()
        evidences = ()
        if mode != "zero":
            transition_id = algo_transition_id_v1(
                delivery_id=pending.delivery_id,
                event_id=event.event_id,
                runtime_id="runtime_k6c1",
                algo_instance_id=algo_instance_id,
                transition_sequence=1,
            )
            command_modes = (
                ("materialize", "reject", "defer")
                if mode == "mixed"
                else ("defer",)
                if mode == "coordination_drift"
                else ("materialize",)
                if mode
                in {
                    "rollback",
                    "commit_unknown",
                    "readback_drift",
                    "mapping_drift",
                    "effects",
                    "transition_drift",
                    "timer_drift",
                    "diagnostic_drift",
                    "outbox_missing",
                    "claimed_lifecycle",
                    "concurrent",
                }
                else (mode,)
            )
            commands = tuple(
                BrokerCommandV2.create(
                    command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
                    runtime_id="runtime_k6c1",
                    algo_instance_id=algo_instance_id,
                    parent_intent_id="intent_k6c1",
                    transition_id=transition_id,
                    ordinal=ordinal,
                    local_vt_orderid=None,
                    symbol=f"60000{ordinal}.SH",
                    side=SideV1.BUY,
                    order_type=OrderTypeV1.LIMIT,
                    price_decimal=str(10 + ordinal),
                    quantity=100,
                    owned_broker_order_id=None,
                    reason_code="PLUGIN_SUBMIT",
                    metadata={"slice": ordinal},
                )
                for ordinal in range(len(command_modes))
            )
            evidences = tuple(
                _evidence_for_authority(
                    command,
                    dependent=command_mode == "defer",
                    oms_reason="INSUFFICIENT_CASH" if command_mode == "reject" else None,
                    event_id=event.event_id,
                    delivery_id=pending.delivery_id,
                )
                for command, command_mode in zip(commands, command_modes, strict=True)
            )
            projection_set = evidences[0].execution_projection_set
            evidences = tuple(
                type(evidence).create(
                    **evidence.model_dump(
                        mode="python",
                        exclude={"schema_version", "execution_projection_set", "evidence_sha256"},
                    ),
                    execution_projection_set=projection_set,
                )
                for evidence in evidences
            )
        else:
            projection_set = ExecutionProjectionSetV1.create(
                runtime_id="runtime_k6c1",
                algo_instance_id=algo_instance_id,
                event_id=event.event_id,
                delivery_id=pending.delivery_id,
                projection_refs=(),
            )
        next_state_payload = {"phase": "active"}
        after_state = AlgoStateSnapshotV2(
            schema_version="execution_algo_state_snapshot_v2",
            algo_instance_id=algo_instance_id,
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            plugin_manifest_sha256=manifest.manifest_sha256,
            state_schema_version=manifest.state_schema_version,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=pending.delivery_id,
            last_closed_delivery_sequence=1,
            state=next_state_payload,
            state_sha256=hash_hex_v1("execution_algo_state_v2", next_state_payload),
            last_applied_event_id=event.event_id,
            updated_at_utc="2026-08-03T01:31:00Z",
        )
        timer_mutations: tuple[TimerMutationV1, ...] = ()
        timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...] = ()
        diagnostic_observations: tuple[DiagnosticObservationV1, ...] = ()
        if mode in {"effects", "timer_drift", "diagnostic_drift"}:
            deterministic_context = DeterministicExecutionContextV1.create(
                runtime_id="runtime_k6c1",
                algo_instance_id=algo_instance_id,
                event_id=event.event_id,
                delivery_id=pending.delivery_id,
                plugin_manifest_sha256=manifest.manifest_sha256,
                transition_sequence=1,
                logical_time_utc="2026-08-03T01:31:00Z",
                exchange_trade_date="2026-08-01",
                session_epoch="session_k6c1",
                session_phase=SessionPhaseV1.CONTINUOUS_AM,
                input_projection_sha256=projection_set.projection_set_sha256,
            )
            timer = TimerMutationV1.create(
                mutation_type=TimerMutationTypeV1.UPSERT_ONE_SHOT,
                algo_instance_id=algo_instance_id,
                transition_id=transition_id,
                ordinal=1,
                timer_name="next_slice",
                schedule_epoch="session_k6c1",
                due_at_exchange_utc="2026-08-03T01:32:00Z",
                catch_up_policy="EXPIRE_IF_LATE",
                payload={"slice": 2},
            )
            schedule = ExecutionAlgoTimerScheduleV1.create(
                runtime_id="runtime_k6c1",
                mutation=timer,
                status=ExecutionAlgoTimerScheduleStatusV1.SCHEDULED,
                emitted_event_id=None,
                lease_owner=None,
                lease_epoch=0,
                lease_fence_token=None,
                lease_expires_at_utc=None,
                row_version=1,
                created_at_utc="2026-08-03T01:31:00Z",
                updated_at_utc="2026-08-03T01:31:00Z",
                closed_at_utc=None,
            )
            diagnostic = DiagnosticObservationV1.create(
                deterministic_context=deterministic_context,
                transition_id=transition_id,
                ordinal=2,
                severity=DiagnosticSeverityV1.INFO,
                reason_code="MINIQMT_K6C1_TEST_EFFECT",
                message="K6-C1 timer and diagnostic effect",
                context={"schedule_id": timer.schedule_id},
            )
            timer_mutations = (timer,)
            timer_schedules = (schedule,)
            diagnostic_observations = (diagnostic,)
        effect_payload = {
            "next_state_sha256": after_state.state_sha256,
            "ordered_command_ids": [command.command_id for command in commands],
            "ordered_timer_mutation_ids": [item.mutation_identity_v1() for item in timer_mutations],
            "ordered_diagnostic_observation_ids": [item.observation_id for item in diagnostic_observations],
            "terminal_outcome": None,
        }
        transition = AlgoTransitionV1(
            schema_version="miniqmt_algo_transition_v1",
            next_state=after_state,
            broker_commands=commands,
            timer_mutations=timer_mutations,
            diagnostic_observations=diagnostic_observations,
            terminal_outcome=None,
            effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", effect_payload),
        )
        provisional_receipt = AlgoTransitionReceiptV1.create(
            delivery_id=pending.delivery_id,
            event_id=event.event_id,
            runtime_id="runtime_k6c1",
            algo_instance_id=algo_instance_id,
            plugin_id=manifest.plugin_id,
            plugin_version=manifest.plugin_version,
            plugin_manifest_sha256=manifest.manifest_sha256,
            transition_sequence=1,
            before_state_sha256_or_INIT=algo_v1.state_sha256,
            after_state_sha256=after_state.state_sha256,
            ordered_command_ids=tuple(command.command_id for command in commands),
            ordered_timer_mutation_ids=tuple(item.mutation_identity_v1() for item in timer_mutations),
            ordered_diagnostic_observation_ids=tuple(item.observation_id for item in diagnostic_observations),
            ordered_consumed_lineage_refs=(),
            execution_projection_set_sha256=projection_set.projection_set_sha256,
            effect_set_sha256=transition.effect_set_sha256,
            terminal_outcome=None,
            logical_applied_at_utc="2026-08-03T01:31:00Z",
            transaction_commit_identity="mqtx_k6c1_provisional",
        )
        receipt = bind_product_transition_receipt_v3(
            transition=transition,
            transition_receipt=provisional_receipt,
            ordered_evidence=evidences,
            timer_schedules=timer_schedules,
        )
        authority = build_product_command_authority_set_v3(
            transition=transition,
            transition_receipt=receipt,
            projection_set=projection_set,
            ordered_evidence=evidences,
            catalog=authority_input.plugin_catalog_snapshot,
            creation_binding=authority_input,
            timer_schedules=timer_schedules,
        )
        envelope = ProductCommandAuthorityEnvelopeV3.create(
            authority_set=authority,
            creation_authority=authority_input,
            ordered_timer_schedules=timer_schedules,
        )
        algo_payload = algo_v1.model_dump(mode="python")
        algo_payload.update(
            state_json=next_state_payload,
            state_sha256=after_state.state_sha256,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=pending.delivery_id,
            last_closed_delivery_sequence=1,
            row_version=2,
            updated_at_utc="2026-08-03T01:31:00Z",
            active_child_count=len(commands),
        )
        algo_v2 = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)
        delivery_payload = claimed.model_dump(mode="python")
        delivery_payload.update(
            status=DeliveryStatusV1.APPLIED,
            lease_owner=None,
            lease_expires_at=None,
            lease_fence_token=None,
            transition_id=receipt.transition_id,
            row_version=3,
            updated_at_utc="2026-08-03T01:31:00Z",
            closed_at_utc="2026-08-03T01:31:00Z",
        )
        applied_delivery = AlgoDeliveryPersistenceV1.model_validate(delivery_payload)
        new_mappings = tuple(
            ExecutionCommandChildMappingV1.create(
                command=command,
                strategy_slot_id="slot_k6c1",
                mapping_status=CommandChildMappingStatusV1.RESERVED,
                mapping_version=1,
                broker_order_id=None,
                broker_identity_source_event_id=None,
                last_order_event_id=None,
                last_trade_event_id=None,
                updated_by_event_id=None,
                created_at_utc="2026-08-03T01:31:00Z",
                updated_at_utc="2026-08-03T01:31:00Z",
            )
            for command in commands
        )
        command_outboxes = tuple(
            BrokerCommandOutboxV1.create(
                command=command,
                mapping_id=mapping.mapping_id,
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
                created_at_utc="2026-08-03T01:31:00Z",
                updated_at_utc="2026-08-03T01:31:00Z",
                closed_at_utc=None,
            )
            for command, mapping in zip(commands, new_mappings, strict=True)
        )
        bundle = KernelTransitionWriteBundleV1.create(
            algo_instance=algo_v2,
            delivery=applied_delivery,
            receipt=receipt,
            projection_set=projection_set,
            after_state=after_state,
            new_child_mappings=new_mappings,
            command_outboxes=command_outboxes,
            timer_mutations=timer_mutations,
            timer_schedules=timer_schedules,
            diagnostic_observations=diagnostic_observations,
        )

        if mode == "materialize":
            common = {
                "authority_envelope": envelope,
                "transition_bundle": bundle,
                "previous_delivery": claimed,
                "expected_delivery_row_version": 2,
                "expected_algo_row_version": 1,
                "strategy_slot_id": "slot_k6c1",
            }
            with pytest.raises(TypeError, match="authority_envelope"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "authority_envelope": object()}  # type: ignore[arg-type]
                )
            with pytest.raises(TypeError, match="transition_bundle"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "transition_bundle": object()}  # type: ignore[arg-type]
                )
            with pytest.raises(TypeError, match="previous_delivery"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "previous_delivery": object()}  # type: ignore[arg-type]
                )
            with pytest.raises(ValueError, match="strategy_slot_id"):
                repository.materialize_product_transition_atomic_v3(**{**common, "strategy_slot_id": " bad "})
            with pytest.raises(ValueError, match="pre-product command set"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "transition_bundle": replace(bundle, command_outboxes=())}
                )
            non_initial_outbox = bundle.command_outboxes[0].model_copy(
                update={"status": BrokerCommandOutboxStatusV1.CLAIMED}
            )
            with pytest.raises(ValueError, match="initial PENDING"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, command_outboxes=(non_initial_outbox,)),
                    }
                )
            wrong_mapping_outbox = bundle.command_outboxes[0].model_copy(update={"mapping_id": "mqmap_wrong"})
            with pytest.raises(ValueError, match="outbox mapping"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, command_outboxes=(wrong_mapping_outbox,)),
                    }
                )
            wrong_payload_outbox = bundle.command_outboxes[0].model_copy(
                update={"payload_json": {"schema_version": "miniqmt_broker_command_v2", "forged": True}}
            )
            with pytest.raises(ValueError, match="payload differs"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, command_outboxes=(wrong_payload_outbox,)),
                    }
                )
            non_initial_mapping = bundle.new_child_mappings[0].model_copy(
                update={"mapping_status": CommandChildMappingStatusV1.DISPATCHING}
            )
            with pytest.raises(ValueError, match="initial RESERVED"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, new_child_mappings=(non_initial_mapping,)),
                    }
                )
            invalid_algo = bundle.algo_instance.model_copy(update={"active_child_count": 0})
            with pytest.raises(ValueError, match="active-child count"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "transition_bundle": replace(bundle, algo_instance=invalid_algo)}
                )

        if mode == "rollback":
            with raw.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE FUNCTION {schema}.fail_k6c1_authority_insert() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'K6C1_TEST_LATE_FAILURE'; END $$;
                    CREATE TRIGGER fail_k6c1_authority_insert
                    BEFORE INSERT ON {schema}.execution_product_command_authority
                    FOR EACH ROW EXECUTE FUNCTION {schema}.fail_k6c1_authority_insert()
                    """
                )
            with pytest.raises(psycopg2.Error, match="K6C1_TEST_LATE_FAILURE"):
                repository.materialize_product_transition_atomic_v3(
                    authority_envelope=envelope,
                    transition_bundle=bundle,
                    previous_delivery=claimed,
                    expected_delivery_row_version=2,
                    expected_algo_row_version=1,
                    strategy_slot_id="slot_k6c1",
                )
            with raw.cursor() as cur:
                for table in (
                    "execution_algo_transition",
                    "execution_child_order",
                    "execution_algo_command_outbox",
                    "execution_product_command_authority",
                    "execution_product_command_authority_item",
                ):
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    assert cur.fetchone()[0] == 0
            assert repository.read_delivery(pending.delivery_id) == claimed
            assert repository.read_algo_instance(algo_instance_id) == algo_v1
            return

        materializer = (
            PostgresMiniQMTKernelRepository(conn_factory=_commit_unknown_factory(schema))
            if mode == "commit_unknown"
            else repository
        )
        if mode == "concurrent":
            with raw.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE FUNCTION {schema}.delay_k6c1_transition_insert() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.5); RETURN NEW; END $$;
                    CREATE TRIGGER delay_k6c1_transition_insert
                    BEFORE INSERT ON {schema}.execution_algo_transition
                    FOR EACH ROW EXECUTE FUNCTION {schema}.delay_k6c1_transition_insert()
                    """
                )

            def write_same_authority() -> object:
                concurrent_repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
                return concurrent_repository.materialize_product_transition_atomic_v3(
                    authority_envelope=envelope,
                    transition_bundle=bundle,
                    previous_delivery=claimed,
                    expected_delivery_row_version=2,
                    expected_algo_row_version=1,
                    strategy_slot_id="slot_k6c1",
                )

            with ThreadPoolExecutor(max_workers=2) as executor:
                concurrent_results = tuple(executor.map(lambda _: write_same_authority(), range(2)))
            assert concurrent_results[0] == concurrent_results[1]
            with raw.cursor() as cur:
                for table in (
                    "execution_algo_transition",
                    "execution_child_order",
                    "execution_algo_command_outbox",
                    "execution_product_command_authority",
                    "execution_product_command_authority_item",
                ):
                    cur.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
                    assert cur.fetchone()[0] == 1
            materialization = concurrent_results[0]
        elif mode == "commit_unknown":
            with pytest.raises(KernelRepositoryCommitUnknown, match="not observed"):
                materializer.materialize_product_transition_atomic_v3(
                    authority_envelope=envelope,
                    transition_bundle=bundle,
                    previous_delivery=claimed,
                    expected_delivery_row_version=2,
                    expected_algo_row_version=1,
                    strategy_slot_id="slot_k6c1",
                )
            materialization = repository.read_product_materialization_v3(authority.authority_set_sha256)[2]
        else:
            materialization = materializer.materialize_product_transition_atomic_v3(
                authority_envelope=envelope,
                transition_bundle=bundle,
                previous_delivery=claimed,
                expected_delivery_row_version=2,
                expected_algo_row_version=1,
                strategy_slot_id="slot_k6c1",
            )
        if mode in {"readback_drift", "mapping_drift", "coordination_drift"}:
            with raw.cursor() as cur:
                table = {
                    "readback_drift": "execution_product_command_authority_item",
                    "mapping_drift": "execution_child_order",
                    "coordination_drift": "execution_dependent_buy_coordination",
                }[mode]
                cur.execute(f"ALTER TABLE {schema}.{table} DISABLE TRIGGER USER")
                if mode == "readback_drift":
                    cur.execute(
                        f"UPDATE {schema}.{table} SET evaluation_evidence_sha256=%s WHERE authority_set_sha256=%s",
                        ("0" * 64, authority.authority_set_sha256),
                    )
                elif mode == "mapping_drift":
                    cur.execute(
                        f"UPDATE {schema}.{table} SET status='FILLED' WHERE mapping_id=%s",
                        (authority.ordered_items[0].mapping_id,),
                    )
                else:
                    cur.execute(
                        f"UPDATE {schema}.{table} SET required_cash='999' WHERE coordination_id=%s",
                        (authority.ordered_items[0].coordination_id,),
                    )
                cur.execute(f"ALTER TABLE {schema}.{table} ENABLE TRIGGER USER")
            with pytest.raises(KernelRepositoryConflict):
                repository.read_product_materialization_v3(authority.authority_set_sha256)
            return
        if mode in {"transition_drift", "timer_drift", "diagnostic_drift"}:
            with raw.cursor() as cur:
                table = {
                    "transition_drift": "execution_algo_transition",
                    "timer_drift": "execution_algo_timer_schedule",
                    "diagnostic_drift": "execution_algo_diagnostic_observation",
                }[mode]
                cur.execute(f"ALTER TABLE {schema}.{table} DISABLE TRIGGER USER")
                if mode == "transition_drift":
                    cur.execute(
                        f"UPDATE {schema}.{table} SET receipt_sha256=%s WHERE transition_id=%s",
                        ("0" * 64, authority.transition_id),
                    )
                elif mode == "timer_drift":
                    cur.execute(
                        f"UPDATE {schema}.{table} SET payload_sha256=%s WHERE schedule_id=%s",
                        ("0" * 64, timer_schedules[0].schedule_id),
                    )
                else:
                    cur.execute(
                        f"UPDATE {schema}.{table} SET context_sha256=%s WHERE transition_id=%s",
                        ("0" * 64, authority.transition_id),
                    )
                cur.execute(f"ALTER TABLE {schema}.{table} ENABLE TRIGGER USER")
            with pytest.raises(KernelRepositoryConflict):
                repository.read_product_materialization_v3(authority.authority_set_sha256)
            return
        if mode == "outbox_missing":
            with raw.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {schema}.execution_product_command_authority_item "
                    "DROP CONSTRAINT fk_miniqmt_k6_authority_item_outbox"
                )
                cur.execute(f"ALTER TABLE {schema}.execution_algo_command_outbox DISABLE TRIGGER USER")
                cur.execute(
                    f"DELETE FROM {schema}.execution_algo_command_outbox WHERE command_id=%s",
                    (authority.ordered_items[0].command_id,),
                )
                cur.execute(f"ALTER TABLE {schema}.execution_algo_command_outbox ENABLE TRIGGER USER")
            with pytest.raises(KernelRepositoryConflict, match="lacks an outbox"):
                repository.read_product_materialization_v3(authority.authority_set_sha256)
            return
        if mode == "claimed_lifecycle":
            outbox_owner = f"worker_k6c1:{worker.process_incarnation_id}"
            outbox_fence = kernel_lease_fence_token_v1(
                owner_type="OUTBOX_COMMAND",
                owner_id=authority.ordered_items[0].command_id,
                lease_epoch=1,
                lease_owner=outbox_owner,
            )
            repository.claim_outbox_command(
                command_id=authority.ordered_items[0].command_id,
                lease_owner=outbox_owner,
                lease_epoch=1,
                lease_fence_token=outbox_fence,
                lease_expires_at="2026-08-03T01:40:00Z",
                updated_at_utc="2026-08-03T01:32:00Z",
                expected_row_version=1,
            )
        read_authority, lifecycle, read_receipt = repository.read_product_materialization_v3(
            authority.authority_set_sha256
        )
        assert materialization == read_receipt
        assert (
            repository.materialize_product_transition_atomic_v3(
                authority_envelope=envelope,
                transition_bundle=bundle,
                previous_delivery=claimed,
                expected_delivery_row_version=2,
                expected_algo_row_version=1,
                strategy_slot_id="slot_k6c1",
            )
            == read_receipt
        )
        if mode == "materialize":
            with pytest.raises(ValueError, match="pre-product command set"):
                repository.materialize_product_transition_atomic_v3(
                    authority_envelope=envelope,
                    transition_bundle=replace(bundle, command_outboxes=()),
                    previous_delivery=claimed,
                    expected_delivery_row_version=2,
                    expected_algo_row_version=1,
                    strategy_slot_id="slot_k6c1",
                )
        assert read_authority == authority
        assert len(lifecycle.ordered_item_projections) == len(commands)
        assert read_receipt.zero_command is (mode == "zero")
        expected_dispositions = {
            "zero": (),
            "materialize": ("MATERIALIZE",),
            "reject": ("REJECT_SYNCHRONOUS",),
            "defer": ("DEFER_DEPENDENT_BUY",),
            "mixed": ("MATERIALIZE", "REJECT_SYNCHRONOUS", "DEFER_DEPENDENT_BUY"),
            "commit_unknown": ("MATERIALIZE",),
            "effects": ("MATERIALIZE",),
            "claimed_lifecycle": ("MATERIALIZE",),
            "concurrent": ("MATERIALIZE",),
        }[mode]
        assert tuple(item.disposition.value for item in lifecycle.ordered_item_projections) == expected_dispositions
        if mode == "claimed_lifecycle":
            assert lifecycle.ordered_item_projections[0].lifecycle_status.value == "CLAIMED"
        assert repository.read_delivery(pending.delivery_id).status is DeliveryStatusV1.APPLIED
        assert repository.read_algo_instance(algo_instance_id).active_child_count == sum(
            disposition in {"MATERIALIZE", "DEFER_DEPENDENT_BUY"} for disposition in expected_dispositions
        )
    finally:
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()
