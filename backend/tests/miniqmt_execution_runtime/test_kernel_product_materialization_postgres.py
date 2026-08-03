from __future__ import annotations

from dataclasses import replace
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, time
import json
import os

import psycopg2
import psycopg2.extras
import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    MarketCode,
    SessionSegment,
    canonical_json_bytes,
)

from backend.services.miniqmt_execution_runtime.kernel_delivery import KernelTransitionWriteBundleV1
from backend.services.miniqmt_execution_runtime.kernel_clock import build_eod_event_v1
from backend.services.miniqmt_execution_runtime.kernel_product_authority import (
    bind_product_transition_receipt_v3,
    build_product_command_authority_set_v3,
)
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCandidateAuthorityV2,
    DependentBuyDependencyStatusV1,
    DependentBuySellDependencyV2,
    ProductCommandAuthorityEnvelopeV3,
    ProductCommandEvaluationEvidenceV3,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
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
    ExchangeSessionAuthorityV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoTimerScheduleStatusV1,
    ExecutionAlgoTimerScheduleV1,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionSetV1,
    KernelErrorEvidenceV1,
    RuntimeEventEnvelopeV2,
    OrderTypeV1,
    NormalizedOrderStatusV1,
    SideV1,
    SessionPhaseV1,
    TimerMutationTypeV1,
    TimerMutationV1,
    _algo_instance_id_v2,
    algo_transition_id_v1,
    kernel_lease_fence_token_v1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_k6_migration_postgres import (
    K6B_FORWARD,
    K6B_ROLLBACK,
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


_K6B_DEV_MODES = frozenset(
    {
        "released_defer_lifecycle",
        "k6b_concurrent",
        "k6b_wait",
        "k6b_order_block",
        "k6b_trade_release",
        "k6b_eod",
    }
)


def _k6b_session_authority() -> ExchangeSessionAuthorityV1:
    segments = (SessionSegment(time(9, 30), time(11, 30)), SessionSegment(time(13), time(15)))
    effective_at = datetime(2026, 7, 31, 16, tzinfo=UTC)
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar_{market.value}_20260801",
            market=market,
            trade_date=date(2026, 8, 1),
            timezone="Asia/Shanghai",
            session_segments=segments,
            effective_at_utc=effective_at,
            source_version="aistock_calendar_v1",
        )
        for market in MarketCode
    }
    snapshot_set = CalendarSnapshotSet(snapshot_set_id="calendar_set_k6b", snapshot_by_market=snapshots)
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    return ExchangeSessionAuthorityV1.create(
        runtime_id="runtime_k6c1",
        exchange_trade_date="2026-08-01",
        calendar_snapshot_set_id=snapshot_set.snapshot_set_id,
        calendar_snapshot_set_json=snapshot_json,
        calendar_snapshot_set_sha256=snapshot_set.set_sha256,
        ordered_market_calendar_sha256s=tuple(
            snapshot_set.snapshot_by_market[market].calendar_sha256
            for market in (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
        ),
        ordered_session_segments=tuple(segment.canonical_payload() for segment in segments),
        source_effective_at_utc=effective_at,
    )


def test_k6c1_product_repository_public_surface_is_complete() -> None:
    assert {
        "materialize_product_transition_atomic_v3",
        "read_product_materialization_v3",
    } <= set(dir(PostgresMiniQMTKernelRepository))
    repository = PostgresMiniQMTKernelRepository(conn_factory=lambda: None)
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        repository.read_product_materialization_v3("not-a-sha")
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        repository.read_product_materialization_v3("A" * 64)


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
        "terminal_lifecycle",
        "released_defer_lifecycle",
        "k6b_concurrent",
        "k6b_wait",
        "k6b_order_block",
        "k6b_trade_release",
        "k6b_eod",
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
        "terminal-outbox-lifecycle-readback",
        "released-defer-outbox-lifecycle-readback",
        "dependent-buy-concurrent-same-event",
        "dependent-buy-account-wait",
        "dependent-buy-terminal-order-block",
        "dependent-buy-settled-trade-release",
        "dependent-buy-eod-residual",
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
            cur.execute(K6B_FORWARD.read_text(encoding="utf-8").replace("qmt_strategy", schema))
            cur.execute(
                f"INSERT INTO {schema}.execution_runtime(runtime_id,trade_date) VALUES (%s,%s)",
                ("runtime_k6c1", date(2026, 8, 1)),
            )
        repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
        k6b_session = _k6b_session_authority()
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
        if mode in {"defer", "mixed", "coordination_drift", *_K6B_DEV_MODES}:
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
                if mode in {"coordination_drift", *_K6B_DEV_MODES}
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
                    "terminal_lifecycle",
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
            if mode in _K6B_DEV_MODES:
                adjusted_evidences = []
                for evidence in evidences:
                    candidate = evidence.dependent_buy_candidate
                    assert candidate is not None
                    waiting_dependencies = tuple(
                        DependentBuySellDependencyV2.create(
                            runtime_id=dependency.runtime_id,
                            strategy_id=dependency.strategy_id,
                            sell_parent_intent_id=dependency.sell_parent_intent_id,
                            sell_algo_instance_id=dependency.sell_algo_instance_id,
                            latest_order_fact_id=None,
                            latest_order_fact_sha256=None,
                            ordered_settled_proceeds_refs=(),
                            dependency_status=DependentBuyDependencyStatusV1.OPEN,
                        )
                        for dependency in candidate.ordered_sell_dependencies
                    )
                    candidate = DependentBuyCandidateAuthorityV2.create(
                        **candidate.model_dump(
                            mode="python",
                            exclude={
                                "schema_version",
                                "candidate_sha256",
                                "session_authority_sha256",
                                "ordered_sell_dependencies",
                            },
                        ),
                        session_authority_sha256=k6b_session.authority_sha256,
                        ordered_sell_dependencies=waiting_dependencies,
                    )
                    adjusted_evidences.append(
                        ProductCommandEvaluationEvidenceV3.create(
                            **evidence.model_dump(
                                mode="python",
                                exclude={"schema_version", "evidence_sha256", "dependent_buy_candidate"},
                            ),
                            dependent_buy_candidate=candidate,
                        )
                    )
                evidences = tuple(adjusted_evidences)
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
            with pytest.raises(ValueError, match="expected_delivery_row_version"):
                repository.materialize_product_transition_atomic_v3(**{**common, "expected_delivery_row_version": True})
            with pytest.raises(ValueError, match="expected_algo_row_version"):
                repository.materialize_product_transition_atomic_v3(**{**common, "expected_algo_row_version": 0})
            with pytest.raises(ValueError, match="pre-product command set"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "transition_bundle": replace(bundle, command_outboxes=())}
                )
            with pytest.raises(ValueError, match="projection set and after state"):
                repository.materialize_product_transition_atomic_v3(
                    **{**common, "transition_bundle": replace(bundle, projection_set=None)}
                )
            with pytest.raises(ValueError, match="prior terminal updates"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(
                            bundle,
                            updated_child_mappings=bundle.new_child_mappings,
                        ),
                    }
                )
            non_initial_outbox = BrokerCommandOutboxV1.create(
                command=commands[0],
                mapping_id=bundle.command_outboxes[0].mapping_id,
                status=BrokerCommandOutboxStatusV1.CLAIMED,
                attempt_count=1,
                lease_owner="worker_k6c1:preproduct",
                lease_epoch=1,
                lease_fence_token=kernel_lease_fence_token_v1(
                    owner_type="OUTBOX_COMMAND",
                    owner_id=commands[0].command_id,
                    lease_epoch=1,
                    lease_owner="worker_k6c1:preproduct",
                ),
                lease_expires_at="2026-08-03T01:40:00Z",
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
                row_version=2,
                created_at_utc="2026-08-03T01:31:00Z",
                updated_at_utc="2026-08-03T01:32:00Z",
                closed_at_utc=None,
            )
            with pytest.raises(ValueError, match="initial PENDING"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, command_outboxes=(non_initial_outbox,)),
                    }
                )
            wrong_mapping_outbox = BrokerCommandOutboxV1.create(
                command=commands[0],
                mapping_id="mqmap_wrong",
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
            with pytest.raises(ValueError, match="strict durable carrier validation"):
                repository.materialize_product_transition_atomic_v3(
                    **{
                        **common,
                        "transition_bundle": replace(bundle, command_outboxes=(wrong_payload_outbox,)),
                    }
                )
            non_initial_mapping = ExecutionCommandChildMappingV1.create(
                command=commands[0],
                strategy_slot_id="slot_k6c1",
                mapping_status=CommandChildMappingStatusV1.DISPATCHING,
                mapping_version=2,
                broker_order_id=None,
                broker_identity_source_event_id=None,
                last_order_event_id=None,
                last_trade_event_id=None,
                updated_by_event_id=None,
                created_at_utc="2026-08-03T01:31:00Z",
                updated_at_utc="2026-08-03T01:32:00Z",
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
        if mode == "terminal_lifecycle":
            current_chain = repository.read_command_identity_chain(authority.ordered_items[0].command_id)
            current_mapping = current_chain["mapping"]
            current_outbox = current_chain["outbox"]
            terminal_error = KernelErrorEvidenceV1.create(
                stage="OUTBOX_DISPATCH",
                stable_reason_code="K6C1_TERMINAL_READBACK_TEST",
                exception=RuntimeError("terminal before broker call"),
                message="terminal before broker call",
                retryable=False,
                terminal=True,
                broker_called=False,
                primary_context={"command_id": commands[0].command_id},
                secondary_errors=(),
            )
            terminal_mapping = ExecutionCommandChildMappingV1.create(
                command=commands[0],
                strategy_slot_id="slot_k6c1",
                mapping_status=CommandChildMappingStatusV1.TERMINAL,
                mapping_version=current_mapping.mapping_version + 1,
                broker_order_id=None,
                broker_identity_source_event_id=None,
                last_order_event_id=None,
                last_trade_event_id=None,
                updated_by_event_id=event.event_id,
                created_at_utc=current_mapping.created_at_utc,
                updated_at_utc="2026-08-03T01:33:00Z",
            )
            terminal_outbox = BrokerCommandOutboxV1.create(
                command=commands[0],
                mapping_id=current_mapping.mapping_id,
                status=BrokerCommandOutboxStatusV1.FAILED_TERMINAL,
                attempt_count=current_outbox.attempt_count,
                lease_owner=None,
                lease_epoch=current_outbox.lease_epoch,
                lease_fence_token=None,
                lease_expires_at=None,
                dispatch_attempt_id=None,
                next_attempt_at_utc=None,
                broker_called=False,
                broker_order_id=None,
                ack_receipt_json=None,
                ack_receipt_sha256=None,
                non_acceptance_receipt=None,
                unknown_outcome_receipt=None,
                reconcile_receipt=None,
                last_error_json=terminal_error.model_dump(mode="json"),
                row_version=current_outbox.row_version + 1,
                created_at_utc=current_outbox.created_at_utc,
                updated_at_utc="2026-08-03T01:33:00Z",
                closed_at_utc="2026-08-03T01:33:00Z",
            )
            repository.compare_and_swap_mapping_outbox(
                mapping=terminal_mapping,
                outbox=terminal_outbox,
                expected_mapping_version=current_mapping.mapping_version,
                expected_outbox_row_version=current_outbox.row_version,
                expected_lease_owner=None,
                expected_lease_epoch=0,
                expected_lease_fence_token=None,
            )
        if mode in _K6B_DEV_MODES:
            item = authority.ordered_items[0]
            available_cash = "1000" if mode in {"k6b_wait", "k6b_order_block", "k6b_eod"} else "1050"
            with raw.cursor() as cur:
                cur.execute(
                    f"""
                    CREATE TABLE {schema}.virtual_account(
                        strategy_id TEXT PRIMARY KEY, strategy_name TEXT NOT NULL, display_name TEXT NOT NULL,
                        account_id TEXT NOT NULL, mode TEXT NOT NULL, initial_cash NUMERIC(20,6) NOT NULL,
                        cash NUMERIC(20,6) NOT NULL, frozen_cash NUMERIC(20,6) NOT NULL DEFAULT 0,
                        market_value NUMERIC(20,6) NOT NULL DEFAULT 0, realized_pnl NUMERIC(20,6) NOT NULL DEFAULT 0,
                        unrealized_pnl NUMERIC(20,6) NOT NULL DEFAULT 0, status TEXT NOT NULL,
                        risk_config JSONB NOT NULL DEFAULT '{{}}', metadata JSONB NOT NULL DEFAULT '{{}}',
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    CREATE TABLE {schema}.trade_ledger(
                        trade_id TEXT NOT NULL,intent_id TEXT NOT NULL,strategy_id TEXT NOT NULL,qmt_order_id TEXT NOT NULL,
                        qmt_order_sysid TEXT,symbol TEXT NOT NULL,side TEXT NOT NULL,price NUMERIC(20,6) NOT NULL,
                        quantity INTEGER NOT NULL,amount NUMERIC(20,6) NOT NULL,commission NUMERIC(20,6) NOT NULL,
                        trade_date DATE NOT NULL,account_id TEXT NOT NULL,trade_time TIMESTAMPTZ,order_remark TEXT NOT NULL,
                        raw_json JSONB NOT NULL,UNIQUE(account_id,trade_date,trade_id)
                    );
                    CREATE TABLE {schema}.cash_ledger(
                        cash_id TEXT PRIMARY KEY,cash_sequence BIGINT NOT NULL,strategy_id TEXT NOT NULL,account_id TEXT NOT NULL,
                        trade_date DATE NOT NULL,entry_type TEXT NOT NULL,cash_delta NUMERIC(20,6) NOT NULL,
                        cash_after NUMERIC(20,6) NOT NULL,frozen_delta NUMERIC(20,6) NOT NULL,frozen_after NUMERIC(20,6) NOT NULL,
                        intent_id TEXT,trade_id TEXT,symbol TEXT,reason TEXT,metadata JSONB NOT NULL,created_at TIMESTAMPTZ NOT NULL
                    );
                    """
                )
                candidate = item.evaluation_evidence.dependent_buy_candidate
                assert candidate is not None
                cur.execute(
                    f"INSERT INTO {schema}.virtual_account("
                    "strategy_id,strategy_name,display_name,account_id,mode,initial_cash,cash,status,updated_at) "
                    "VALUES (%s,'k6b','K6-B',%s,'SIM',1050,%s,'ENABLED','2026-08-03T01:32:00Z')",
                    (candidate.strategy_id, candidate.virtual_account_id, available_cash),
                )
                if mode == "k6b_trade_release":
                    dependency = candidate.ordered_sell_dependencies[0]
                    cur.execute(
                        f"INSERT INTO {schema}.trade_ledger("
                        "trade_id,intent_id,strategy_id,qmt_order_id,qmt_order_sysid,symbol,side,price,quantity,"
                        "amount,commission,trade_date,account_id,trade_time,order_remark,raw_json) "
                        "VALUES ('trade_k6b',%s,%s,'broker_order_k6b',NULL,'600001.SH','SELL',10.5,100,1050,0,"
                        "%s,%s,'2026-08-03T01:32:30Z','remark_k6b','{}'::jsonb)",
                        (
                            dependency.sell_parent_intent_id,
                            candidate.strategy_id,
                            candidate.trade_date,
                            candidate.virtual_account_id,
                        ),
                    )
                    cur.execute(
                        f"INSERT INTO {schema}.cash_ledger("
                        "cash_id,cash_sequence,strategy_id,account_id,trade_date,entry_type,cash_delta,cash_after,"
                        "frozen_delta,frozen_after,intent_id,trade_id,symbol,reason,metadata,created_at) "
                        "VALUES ('cash_k6b',1,%s,%s,%s,'SELL_FILL',1050,1050,0,0,%s,'trade_k6b',"
                        "'600001.SH','sell-fill','{}'::jsonb,'2026-08-03T01:32:31Z')",
                        (
                            candidate.strategy_id,
                            candidate.virtual_account_id,
                            candidate.trade_date,
                            dependency.sell_parent_intent_id,
                        ),
                    )
                cur.execute(
                    f"INSERT INTO {schema}.execution_kernel_worker_epoch(worker_id,process_role,incarnation_sequence) "
                    "VALUES ('worker_k6b','PRODUCT_COORDINATOR',1)"
                )
                cur.execute(
                    f"INSERT INTO {schema}.execution_kernel_worker_incarnation("
                    "worker_id,process_role,incarnation_sequence,source_revision,process_incarnation_id,started_at_utc,"
                    "startup_transaction_commit_identity,receipt_sha256,startup_receipt_json) "
                    "VALUES ('worker_k6b','PRODUCT_COORDINATOR',1,'k6b-test','process_k6b',now(),'tx_k6b',%s,'{}'::jsonb)",
                    ("a" * 64,),
                )
            repository.write_exchange_session_authority(k6b_session)
            route_receipt = ProductRouteCutoverReceiptV1.create(
                runtime_id="runtime_k6c1",
                binding_id=candidate.binding_id,
                trade_date=candidate.trade_date,
                route_epoch=1,
                route_owner=ProductRouteOwnerKindV1.KERNEL_V2,
                effective_new_instance_sequence=1,
                legacy_active_instance_count=0,
                kernel_active_instance_count=1,
                catalog_sha256="1" * 64,
                gateway_capability_catalog_sha256="2" * 64,
                exchange_session_authority_sha256=k6b_session.authority_sha256,
                migration_readback_sha256="3" * 64,
                product_authority_schema_sha256="4" * 64,
                previous_receipt_sha256=None,
                created_at_utc=datetime(2026, 8, 3, 1, 32, tzinfo=UTC),
            )
            repository.write_product_route_cutover_v1(
                receipt=route_receipt,
                owner=ProductRouteOwnerV1.create(receipt=route_receipt, row_version=1),
            )
            dependency = candidate.ordered_sell_dependencies[0]
            if mode == "k6b_order_block":
                order_payload = {
                    "order_event_id": "order_event_k6b",
                    "runtime_id": "runtime_k6c1",
                    "algo_instance_id": dependency.sell_algo_instance_id,
                    "parent_intent_id": dependency.sell_parent_intent_id,
                    "strategy_slot_id": "slot_sell",
                    "mapping_id": "mapping_sell_k6b",
                    "command_id": "command_sell_k6b",
                    "local_vt_orderid": "local_sell_k6b",
                    "broker_order_id": "broker_order_k6b",
                    "symbol": "600001.SH",
                    "side": SideV1.SELL.value,
                    "normalized_order_status": NormalizedOrderStatusV1.CANCELLED.value,
                    "observed_cumulative_filled_quantity": 0,
                    "observed_remaining_quantity": 100,
                    "terminal": True,
                    "source_payload_sha256": "5" * 64,
                }
                order_payload["fact_sha256"] = hash_hex_v1("miniqmt_kernel_order_event_payload_v1", order_payload)
                trigger_event = RuntimeEventEnvelopeV2.create(
                    runtime_id="runtime_k6c1",
                    sequence=2,
                    event_type=EventTypeV2.ORDER,
                    event_time_utc="2026-08-03T01:33:00Z",
                    monotonic_ns=None,
                    source=EventSourceV2.QMT_GATEWAY_CALLBACK,
                    symbol="600001.SH",
                    payload_schema_version="miniqmt_order_event_v1",
                    payload=order_payload,
                    source_identity={"order_event_id": "order_event_k6b"},
                    correlation={},
                )
            elif mode == "k6b_trade_release":
                trade_payload = {
                    "trade_id": "trade_k6b",
                    "runtime_id": "runtime_k6c1",
                    "algo_instance_id": dependency.sell_algo_instance_id,
                    "parent_intent_id": dependency.sell_parent_intent_id,
                    "strategy_slot_id": "slot_sell",
                    "mapping_id": "mapping_sell_k6b",
                    "command_id": "command_sell_k6b",
                    "local_vt_orderid": "local_sell_k6b",
                    "broker_order_id": "broker_order_k6b",
                    "symbol": "600001.SH",
                    "side": SideV1.SELL.value,
                    "trade_quantity": 100,
                    "trade_price_decimal": "10.5",
                    "source_payload_sha256": "6" * 64,
                }
                trade_payload["fact_sha256"] = hash_hex_v1("miniqmt_kernel_trade_event_payload_v1", trade_payload)
                trigger_event = RuntimeEventEnvelopeV2.create(
                    runtime_id="runtime_k6c1",
                    sequence=2,
                    event_type=EventTypeV2.TRADE,
                    event_time_utc="2026-08-03T01:33:00Z",
                    monotonic_ns=None,
                    source=EventSourceV2.QMT_GATEWAY_CALLBACK,
                    symbol="600001.SH",
                    payload_schema_version="miniqmt_trade_fact_v1",
                    payload=trade_payload,
                    source_identity={"trade_id": "trade_k6b"},
                    correlation={},
                )
            elif mode == "k6b_eod":
                trigger_event = build_eod_event_v1(
                    authority=k6b_session,
                    sequence=2,
                    phase_boundary_at_utc="2026-08-03T07:00:00Z",
                )
            else:
                account_ref = next(
                    ref
                    for ref in item.evaluation_evidence.execution_projection_set.ordered_projection_refs
                    if ref.projection_type.value == "ACCOUNT"
                )
                trigger_event = RuntimeEventEnvelopeV2.create(
                    runtime_id="runtime_k6c1",
                    sequence=2,
                    event_type=EventTypeV2.ACCOUNT,
                    event_time_utc="2026-08-03T01:33:00Z",
                    monotonic_ns=None,
                    source=EventSourceV2.QMT_OMS_PROJECTION,
                    symbol=None,
                    payload_schema_version="miniqmt_account_projection_v1",
                    payload={"cash": available_cash},
                    source_identity={
                        "projection_version": account_ref.projection_version,
                        "projection_sha256": account_ref.payload_sha256,
                    },
                    correlation={},
                )
            _seed_event_receipt_deliveries(repository, event=trigger_event, deliveries=())
            coordinate_args = {
                "event_id": trigger_event.event_id,
                "worker_id": "worker_k6b",
                "process_incarnation_id": "process_k6b",
            }
            if mode == "released_defer_lifecycle":
                unknown_repository = PostgresMiniQMTKernelRepository(conn_factory=_commit_unknown_factory(schema))
                with pytest.raises(KernelRepositoryCommitUnknown, match="not observed"):
                    unknown_repository.coordinate_dependent_buys_for_event_atomic_v2(**coordinate_args)
                release_bundles = repository.coordinate_dependent_buys_for_event_atomic_v2(**coordinate_args)
                restarted_repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
                assert (
                    restarted_repository.coordinate_dependent_buys_for_event_atomic_v2(**coordinate_args)
                    == release_bundles
                )
            else:

                def coordinate_same_event() -> tuple[dict[str, object], ...]:
                    concurrent_repository = PostgresMiniQMTKernelRepository(conn_factory=_conn_factory(schema))
                    return concurrent_repository.coordinate_dependent_buys_for_event_atomic_v2(**coordinate_args)

                with ThreadPoolExecutor(max_workers=2) as executor:
                    concurrent_bundles = tuple(executor.map(lambda _: coordinate_same_event(), range(2)))
                assert concurrent_bundles[0] == concurrent_bundles[1]
                release_bundles = concurrent_bundles[0]
            assert len(release_bundles) == 1
            expected_decision = {
                "released_defer_lifecycle": "RELEASE_TO_K2_OUTBOX",
                "k6b_concurrent": "RELEASE_TO_K2_OUTBOX",
                "k6b_wait": "WAIT",
                "k6b_order_block": "BLOCK",
                "k6b_trade_release": "RELEASE_TO_K2_OUTBOX",
                "k6b_eod": "EOD_RESIDUAL",
            }[mode]
            assert release_bundles[0]["decision"].decision.value == expected_decision
            if expected_decision == "RELEASE_TO_K2_OUTBOX":
                assert release_bundles[0]["outbox"].command_id == item.command_id
            else:
                assert release_bundles[0]["outbox"] is None
            with raw.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {schema}.execution_dependent_buy_decision")
                assert cur.fetchone()[0] == 1
                cur.execute(f"SELECT count(*) FROM {schema}.execution_algo_command_outbox")
                assert cur.fetchone()[0] == (1 if expected_decision == "RELEASE_TO_K2_OUTBOX" else 0)
                with pytest.raises(psycopg2.errors.RaiseException, match="durable rows exist"):
                    cur.execute(K6B_ROLLBACK.read_text(encoding="utf-8").replace("qmt_strategy", schema))
                cur.execute("ROLLBACK")
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
            "terminal_lifecycle": ("MATERIALIZE",),
            "released_defer_lifecycle": ("DEFER_DEPENDENT_BUY",),
            "k6b_concurrent": ("DEFER_DEPENDENT_BUY",),
            "k6b_wait": ("DEFER_DEPENDENT_BUY",),
            "k6b_order_block": ("DEFER_DEPENDENT_BUY",),
            "k6b_trade_release": ("DEFER_DEPENDENT_BUY",),
            "k6b_eod": ("DEFER_DEPENDENT_BUY",),
            "concurrent": ("MATERIALIZE",),
        }[mode]
        assert tuple(item.disposition.value for item in lifecycle.ordered_item_projections) == expected_dispositions
        if mode == "claimed_lifecycle":
            assert lifecycle.ordered_item_projections[0].lifecycle_status.value == "CLAIMED"
        if mode == "terminal_lifecycle":
            assert lifecycle.ordered_item_projections[0].lifecycle_status.value == "FAILED_TERMINAL"
        if mode in _K6B_DEV_MODES:
            expected_lifecycle = {
                "released_defer_lifecycle": "PENDING",
                "k6b_concurrent": "PENDING",
                "k6b_wait": "DEFERRED_DEPENDENT_BUY",
                "k6b_order_block": "FAILED_TERMINAL",
                "k6b_trade_release": "PENDING",
                "k6b_eod": "FAILED_TERMINAL",
            }[mode]
            assert lifecycle.ordered_item_projections[0].lifecycle_status.value == expected_lifecycle
            expected_outbox_id = commands[0].command_id if expected_lifecycle == "PENDING" else None
            assert lifecycle.ordered_item_projections[0].outbox_id == expected_outbox_id
        assert repository.read_delivery(pending.delivery_id).status is DeliveryStatusV1.APPLIED
        expected_active_count = sum(
            disposition in {"MATERIALIZE", "DEFER_DEPENDENT_BUY"} for disposition in expected_dispositions
        )
        if mode in {"terminal_lifecycle", "k6b_order_block", "k6b_eod"}:
            expected_active_count = 0
        assert repository.read_algo_instance(algo_instance_id).active_child_count == expected_active_count
    finally:
        with raw.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        raw.close()
