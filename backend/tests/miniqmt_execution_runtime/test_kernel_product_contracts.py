from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV1,
    DependentBuyDecisionV1,
    DependentBuyDependencyStatusV1,
    DependentBuyLedgerObservationV1,
    DependentBuyReleaseDecisionV1,
    DependentBuySellDependencyV1,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    ProductCommandAggregateDispositionV2,
    ProductCommandAuthorityItemV2,
    ProductCommandAuthoritySetV2,
    ProductCommandDispositionV2,
    ProductCommandLifecycleProjectionItemV2,
    ProductCommandLifecycleProjectionV2,
    ProductLifecycleStatusV2,
    ProductMaterializationReceiptV2,
    ProductRouteCutoverReceiptV1,
    ProductRouteOwnerV1,
    ProductRouteOwnerKindV1,
)


NOW = datetime(2026, 8, 1, 1, 30, tzinfo=timezone.utc)


def _sha(char: str) -> str:
    return char * 64


def _trigger(*, sequence: int = 7, event_id: str = "event_sell_trade") -> DependentBuyTriggerEventRefV1:
    return DependentBuyTriggerEventRefV1.create(
        runtime_id="runtime_k6",
        event_id=event_id,
        event_type=DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
        event_sequence=sequence,
        source_fact_type="qmt_strategy.trade_ledger",
        source_fact_id=f"trade_{sequence}",
        source_fact_sha256=_sha("a"),
        observed_at_utc=NOW,
    )


def _dependency() -> DependentBuySellDependencyV1:
    return DependentBuySellDependencyV1.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_ref=_sha("b"),
        settled_trade_fact_refs=(_sha("c"),),
        settled_cash_ledger_refs=(_sha("d"),),
        dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
    )


def _ledger(*, available_cash: str = "1000", required_cash: str = "800") -> DependentBuyLedgerObservationV1:
    return DependentBuyLedgerObservationV1.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        trade_date="2026-08-01",
        virtual_account_id="account_k6",
        ledger_row_version=9,
        ledger_as_of_utc=NOW,
        available_cash=available_cash,
        required_cash=required_cash,
        ordered_settled_trade_refs=(_sha("c"),),
        ordered_cash_ledger_refs=(_sha("d"),),
        freshness_session_authority_sha256=_sha("e"),
    )


def _coordination(*, status: DependentBuyCoordinationStatusV1) -> DependentBuyCoordinationV1:
    release = status is DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
    return DependentBuyCoordinationV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        strategy_id="strategy_k6",
        buy_algo_instance_id="algo_buy",
        buy_parent_intent_id="intent_buy",
        required_cash="800",
        release_command_payload_sha256=_sha("f"),
        ordered_sell_dependencies=(_dependency(),),
        status=status,
        decision_sequence=1 if release else 0,
        last_decision_sha256=_sha("1") if release else None,
        released_command_id="command_buy" if release else None,
        released_outbox_id="command_buy" if release else None,
        row_version=2 if release else 1,
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


def _authority_item(*, ordinal: int, command_id: str) -> ProductCommandAuthorityItemV2:
    return ProductCommandAuthorityItemV2.create(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        effect_ordinal=ordinal,
        command_id=command_id,
        command_type="SUBMIT_LIMIT",
        command_payload_sha256=_sha("2"),
        plugin_effect_sha256=_sha("3"),
        execution_projection_set_sha256=_sha("4"),
        oms_preflight_receipt_sha256=_sha("5"),
        risk_decision_receipt_sha256=_sha("6"),
        route_compatibility_receipt_sha256=_sha("7"),
        market_data_projection_sha256=_sha("8"),
        account_projection_sha256=_sha("9"),
        contract_projection_sha256=_sha("a"),
        disposition=ProductCommandDispositionV2.MATERIALIZE,
        mapping_id=f"mapping_{command_id}",
        outbox_id=command_id,
        child_order_id=f"child_{command_id}",
    )


def test_dependent_buy_contracts_are_strict_hash_closed_and_round_trip() -> None:
    trigger = _trigger()
    dependency = _dependency()
    ledger = _ledger()
    coordination = _coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS)
    decision = DependentBuyReleaseDecisionV1.create(
        coordination_id=coordination.coordination_id,
        decision_sequence=1,
        previous_decision_sha256=None,
        decision=DependentBuyDecisionV1.WAIT,
        reason_code="MINIQMT_K6_COORDINATION_CASH_STILL_INSUFFICIENT",
        ledger_observation_sha256=ledger.observation_sha256,
        ordered_dependency_sha256s=(dependency.dependency_sha256,),
        trigger_ref_sha256=trigger.trigger_ref_sha256,
        decided_at_utc=NOW,
        worker_id="worker_k6",
        process_incarnation_id="process_k6",
        lease_epoch=1,
    )

    for carrier in (trigger, dependency, ledger, coordination, decision):
        assert type(carrier).model_validate_json(carrier.model_dump_json()) == carrier
    assert ledger.cash_shortfall == "0"
    assert coordination.ordered_sell_dependencies == (dependency,)


def test_dependent_buy_contracts_reject_drift_and_impossible_durable_state() -> None:
    with pytest.raises(ValidationError, match="cash_shortfall"):
        DependentBuyLedgerObservationV1(
            **_ledger().model_dump(exclude={"cash_shortfall", "observation_sha256"}),
            cash_shortfall="1",
            observation_sha256=_sha("0"),
        )
    with pytest.raises(ValidationError, match="released"):
        DependentBuyCoordinationV1(
            **_coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS).model_dump(
                exclude={"released_command_id", "coordination_sha256"}
            ),
            released_command_id="forged",
            coordination_sha256=_sha("0"),
        )
    with pytest.raises(ValueError):
        DependentBuyCoordinationStatusV1("RELEASE_READY")


def test_product_command_authority_supports_zero_one_and_many_without_positional_identity() -> None:
    common = dict(
        runtime_id="runtime_k6",
        algo_instance_id="algo_buy",
        event_id="event_k6",
        delivery_id="delivery_k6",
        transition_id="transition_k6",
        catalog_sha256=_sha("b"),
        creation_binding_sha256=_sha("c"),
        facade_conformance_set_sha256=_sha("d"),
        execution_projection_set_sha256=_sha("4"),
        transition_receipt_sha256=_sha("e"),
    )
    zero = ProductCommandAuthoritySetV2.create(**common, ordered_items=())
    one = ProductCommandAuthoritySetV2.create(**common, ordered_items=(_authority_item(ordinal=0, command_id="cmd_a"),))
    many = ProductCommandAuthoritySetV2.create(
        **common,
        ordered_items=(
            _authority_item(ordinal=0, command_id="cmd_a"),
            _authority_item(ordinal=1, command_id="cmd_b"),
        ),
    )

    assert zero.aggregate_disposition is ProductCommandAggregateDispositionV2.ZERO_COMMAND
    assert one.total_count == 1 and many.total_count == 2
    assert ProductCommandAuthoritySetV2.model_validate_json(many.model_dump_json()) == many
    with pytest.raises(ValidationError, match="ordered"):
        ProductCommandAuthoritySetV2.create(
            **common,
            ordered_items=(
                _authority_item(ordinal=1, command_id="cmd_b"),
                _authority_item(ordinal=0, command_id="cmd_a"),
            ),
        )


def test_product_command_item_rejects_fake_materialization_and_reject_carriers() -> None:
    materialize = _authority_item(ordinal=0, command_id="cmd_a")
    with pytest.raises(ValidationError, match="materialize"):
        ProductCommandAuthorityItemV2(
            **materialize.model_dump(exclude={"mapping_id", "item_sha256"}),
            mapping_id=None,
            item_sha256=_sha("0"),
        )
    with pytest.raises(ValidationError, match="reject"):
        ProductCommandAuthorityItemV2.create(
            **materialize.model_dump(
                exclude={
                    "schema_version",
                    "disposition",
                    "reject_reason_code",
                    "reject_context_sha256",
                    "mapping_id",
                    "outbox_id",
                    "child_order_id",
                    "item_sha256",
                }
            ),
            disposition=ProductCommandDispositionV2.REJECT_SYNCHRONOUS,
            reject_reason_code=None,
            reject_context_sha256=None,
        )


def test_product_lifecycle_and_materialization_receipts_are_factory_hash_closed() -> None:
    item = ProductCommandLifecycleProjectionItemV2.create(
        authority_item_sha256=_sha("a"),
        command_id="command_k6",
        mapping_id="mapping_k6",
        outbox_id="command_k6",
        child_order_id="child_k6",
        lifecycle_status=ProductLifecycleStatusV2.PENDING,
        last_committed_stage="K2_OUTBOX_COMMITTED",
    )
    projection = ProductCommandLifecycleProjectionV2.create(
        authority_set_sha256=_sha("b"),
        ordered_items=(item,),
    )
    receipt = ProductMaterializationReceiptV2.create(
        authority_set_sha256=_sha("b"),
        execution_projection_set_sha256=_sha("c"),
        ordered_mapping_ids=("mapping_k6",),
        ordered_outbox_ids=("command_k6",),
        ordered_child_order_ids=("child_k6",),
        zero_command=False,
        repository_transaction_id="tx_k6",
        independent_readback_sha256=_sha("d"),
    )
    assert ProductCommandLifecycleProjectionV2.model_validate_json(projection.model_dump_json()) == projection
    assert ProductMaterializationReceiptV2.model_validate_json(receipt.model_dump_json()) == receipt
    with pytest.raises(ValidationError, match="unique"):
        ProductMaterializationReceiptV2.create(
            authority_set_sha256=_sha("b"),
            execution_projection_set_sha256=_sha("c"),
            ordered_mapping_ids=("mapping_k6", "mapping_k6"),
            ordered_outbox_ids=("command_a", "command_b"),
            ordered_child_order_ids=("child_a", "child_b"),
            zero_command=False,
            repository_transaction_id="tx_k6",
            independent_readback_sha256=_sha("d"),
        )


def test_route_receipt_and_owner_use_immutable_receipt_plus_cas_pointer() -> None:
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        route_epoch=1,
        route_owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY,
        effective_new_instance_sequence=11,
        legacy_active_instance_count=2,
        kernel_active_instance_count=0,
        catalog_sha256=_sha("a"),
        gateway_capability_catalog_sha256=_sha("b"),
        exchange_session_authority_sha256=_sha("c"),
        migration_readback_sha256=_sha("d"),
        product_authority_schema_sha256=_sha("e"),
        previous_receipt_sha256=None,
        created_at_utc=NOW,
    )
    owner = ProductRouteOwnerV1.create(receipt=receipt, row_version=1)
    assert ProductRouteOwnerV1.model_validate_json(owner.model_dump_json()) == owner
    with pytest.raises(ValidationError, match="owner hash"):
        ProductRouteOwnerV1(
            **owner.model_dump(exclude={"current_receipt_sha256", "owner_sha256"}),
            current_receipt_sha256=_sha("0"),
            owner_sha256=_sha("1"),
        )


def test_k6_negative_state_machine_matrix_is_fail_loud() -> None:
    dependency = _dependency()
    with pytest.raises(ValidationError, match="one-to-one"):
        DependentBuySellDependencyV1.create(
            runtime_id="runtime_k6",
            strategy_id="strategy_k6",
            sell_parent_intent_id="intent_sell",
            sell_algo_instance_id="algo_sell",
            latest_order_fact_ref=_sha("b"),
            settled_trade_fact_refs=(_sha("c"),),
            settled_cash_ledger_refs=(),
            dependency_status=DependentBuyDependencyStatusV1.OPEN,
        )
    with pytest.raises(ValidationError, match="requires trade"):
        DependentBuySellDependencyV1.create(
            runtime_id="runtime_k6",
            strategy_id="strategy_k6",
            sell_parent_intent_id="intent_sell",
            sell_algo_instance_id="algo_sell",
            latest_order_fact_ref=_sha("b"),
            settled_trade_fact_refs=(),
            settled_cash_ledger_refs=(),
            dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
        )
    with pytest.raises(ValidationError, match="dependency hash"):
        DependentBuySellDependencyV1(
            **dependency.model_dump(exclude={"dependency_sha256"}),
            dependency_sha256=_sha("0"),
        )

    coordination = _coordination(status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS)
    decision_values = {
        "coordination_id": coordination.coordination_id,
        "decision_sequence": 1,
        "previous_decision_sha256": None,
        "trigger_ref_sha256": _sha("1"),
        "decision": DependentBuyDecisionV1.WAIT,
        "reason_code": "WAIT",
        "ledger_observation_sha256": _sha("2"),
        "ordered_dependency_sha256s": (dependency.dependency_sha256,),
        "decided_at_utc": NOW,
        "worker_id": "worker_k6",
        "process_incarnation_id": "process_k6",
        "lease_epoch": 1,
    }
    with pytest.raises(ValidationError, match="at least one dependency"):
        DependentBuyReleaseDecisionV1.create(**(decision_values | {"ordered_dependency_sha256s": ()}))
    with pytest.raises(ValidationError, match="lacks K2"):
        DependentBuyReleaseDecisionV1.create(
            **(decision_values | {"decision": DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX})
        )
    with pytest.raises(ValidationError, match="cannot carry release"):
        DependentBuyReleaseDecisionV1.create(**(decision_values | {"release_event_id": "event_forged"}))
    with pytest.raises(ValidationError, match="predecessor"):
        DependentBuyReleaseDecisionV1.create(
            **(decision_values | {"decision_sequence": 2, "previous_decision_sha256": None})
        )

    with pytest.raises(ValidationError, match="cardinality"):
        DependentBuyCoordinationV1.create(
            **coordination.model_dump(
                exclude={"schema_version", "coordination_id", "coordination_sha256", "ordered_sell_dependencies"}
            ),
            ordered_sell_dependencies=(),
        )
    forged_owner = DependentBuySellDependencyV1.create(
        runtime_id="runtime_other",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_ref=_sha("b"),
        settled_trade_fact_refs=(_sha("c"),),
        settled_cash_ledger_refs=(_sha("d"),),
        dependency_status=DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
    )
    with pytest.raises(ValidationError, match="owner differs"):
        DependentBuyCoordinationV1.create(
            **coordination.model_dump(
                exclude={"schema_version", "coordination_id", "coordination_sha256", "ordered_sell_dependencies"}
            ),
            ordered_sell_dependencies=(forged_owner,),
        )
    with pytest.raises(ValueError, match="initial state"):
        _coordination(status=DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX).validate_initial_v1()


def test_k6_product_projection_and_route_negative_matrix_is_fail_loud() -> None:
    materialized = _authority_item(ordinal=0, command_id="cmd_a")
    with pytest.raises(ValidationError, match="outbox identity"):
        ProductCommandAuthorityItemV2.create(
            **materialized.model_dump(exclude={"schema_version", "outbox_id", "item_sha256"}),
            outbox_id="different_outbox",
        )
    with pytest.raises(ValidationError, match="materialized.*identities"):
        ProductCommandLifecycleProjectionItemV2.create(
            authority_item_sha256=_sha("a"),
            command_id="cmd_a",
            lifecycle_status=ProductLifecycleStatusV2.PENDING,
            last_committed_stage="K2_OUTBOX_COMMITTED",
        )
    with pytest.raises(ValidationError, match="ACKED"):
        ProductCommandLifecycleProjectionItemV2.create(
            authority_item_sha256=_sha("a"),
            command_id="cmd_a",
            mapping_id="mapping_a",
            outbox_id="cmd_a",
            child_order_id="child_a",
            lifecycle_status=ProductLifecycleStatusV2.ACKED,
            last_committed_stage="BROKER_CALLBACK",
            broker_called=False,
        )
    with pytest.raises(TypeError, match="repository-owned"):
        ProductMaterializationReceiptV2.create(
            authority_set_sha256=_sha("b"),
            execution_projection_set_sha256=_sha("c"),
            ordered_mapping_ids=(),
            ordered_outbox_ids=(),
            ordered_child_order_ids=(),
            zero_command=True,
            repository_transaction_id="tx_k6",
            independent_readback_sha256=_sha("d"),
            commit_outcome="PENDING",
        )
    receipt = ProductRouteCutoverReceiptV1.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-01",
        route_epoch=1,
        route_owner=ProductRouteOwnerKindV1.LEGACY_DRAIN_ONLY,
        effective_new_instance_sequence=11,
        legacy_active_instance_count=1,
        kernel_active_instance_count=0,
        catalog_sha256=_sha("a"),
        gateway_capability_catalog_sha256=_sha("b"),
        exchange_session_authority_sha256=_sha("c"),
        migration_readback_sha256=_sha("d"),
        product_authority_schema_sha256=_sha("e"),
        previous_receipt_sha256=None,
        created_at_utc=NOW,
    )
    owner = ProductRouteOwnerV1.create(receipt=receipt, row_version=1)
    with pytest.raises(ValueError, match="does not close"):
        owner.validate_receipt_v1(receipt.model_copy(update={"binding_id": "binding_other"}))


@pytest.mark.parametrize("bad", [None, True, 1, [], {}, " "])
def test_k6_identities_never_coerce_malformed_values(bad: object) -> None:
    values = _trigger().model_dump()
    values["event_id"] = bad
    with pytest.raises(ValidationError):
        DependentBuyTriggerEventRefV1.model_validate(values)
