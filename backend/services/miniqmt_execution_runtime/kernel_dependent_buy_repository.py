"""K6-B durable dependent-BUY decision writer and independent readback.

This mixin owns the second K6 writer phase only.  It accepts no broker result
and never calls a broker: the release transaction changes the existing
DEFERRED mapping to RESERVED and first-writes the same command's K2 PENDING
outbox row atomically.  The public trigger reader is deliberately separate so
no caller-supplied source payload can be mistaken for a durable fact.
"""

from __future__ import annotations

from contextlib import nullcontext
from datetime import timedelta
from typing import Any

import psycopg2.extras

from .kernel_clock import session_epoch_v1
from .kernel_dependent_buy import (
    build_dependent_buy_trigger_bundle_v2,
    cash_ledger_fact_sha256_v1,
    qmt_trade_ledger_fact_sha256_v1,
)
from .kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV2,
    DependentBuyDependencyStatusV1,
    DependentBuyLedgerObservationV2,
    DependentBuyReleaseDecisionV2,
    DependentBuySellDependencyV2,
    DependentBuySettledProceedsRefV2,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    ProductCommandAuthorityEnvelopeV3,
    ProductCommandAuthorityItemV3,
    ProductCommandChildMappingV1,
    ProductCommandChildMappingStatusV1,
    KernelProjectionTypeV1,
    ProductRouteOwnerKindV1,
    ProductRouteOwnerV1,
    validate_kernel_product_payload_v1,
)
from .kernel_product_materialization_repository import (
    _authority_item_projection_v3,
    _authority_projection_v3,
    _coordination_projection_v2,
    _dependency_projection_v2,
)
from .kernel_repository_common import KernelRepositoryConflict, _json, _model_from_json, _row_json
from .kernel_repository_projection import (
    _assert_scalar_columns,
    _mapping_scalar_projection,
)
from .plugin_canonical import canonical_decimal_string_v1, thaw_json_v1
from .plugin_contracts import (
    BrokerCommandOutboxV1,
    ExchangeSessionAuthorityV1,
    EventSourceV2,
    EventTypeV2,
    KernelOrderEventPayloadV1,
    ExecutionAlgoInstancePersistenceV2,
    RuntimeEventEnvelopeV2,
    SideV1,
    strict_readback_kernel_event_payload_v1,
)


def _decision_projection_v2(value: DependentBuyReleaseDecisionV2) -> dict[str, Any]:
    return {
        "decision_id": value.decision_id,
        "coordination_id": value.coordination_id,
        "decision_sequence": value.decision_sequence,
        "previous_decision_sha256": value.previous_decision_sha256,
        "trigger_ref_sha256": value.trigger_ref_sha256,
        "decision": value.decision.value,
        "reason_code": value.reason_code,
        "ledger_observation_sha256": value.ledger_observation_sha256,
        "ordered_dependency_sha256s": list(value.ordered_dependency_sha256s),
        "release_event_id": value.release_event_id,
        "release_transition_id": value.release_transition_id,
        "release_command_authority_set_sha256": value.release_command_authority_set_sha256,
        "decided_at_utc": value.decided_at_utc,
        "worker_id": value.worker_id,
        "process_incarnation_id": value.process_incarnation_id,
        "lease_epoch": value.lease_epoch,
        "decision_sha256": value.decision_sha256,
    }


def _decision_evidence_projection_v2(
    trigger: DependentBuyTriggerEventRefV1,
    ledger: DependentBuyLedgerObservationV2,
) -> dict[str, Any]:
    return {
        "trigger_event_id": trigger.event_id,
        "ledger_virtual_account_id": ledger.virtual_account_id,
        "ledger_virtual_account_updated_at_utc": ledger.virtual_account_updated_at_utc,
        "ledger_latest_cash_sequence": ledger.latest_cash_ledger_sequence,
        "ledger_revision_sha256": ledger.ledger_revision_sha256,
    }


def _strict_model(model_type: Any, value: Any, *, stage: str) -> Any:
    if not isinstance(value, model_type):
        raise TypeError(f"{stage} requires {model_type.__name__}")
    return validate_kernel_product_payload_v1(
        model_type,
        value.model_dump(mode="json"),
        stage=stage,
    )


def _strict_sha256(value: object, *, field_name: str) -> str:
    if type(value) is not str or len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
        raise ValueError(f"{field_name} must be one lowercase SHA-256")
    return value


def _event_trigger_type(event: RuntimeEventEnvelopeV2) -> DependentBuyTriggerTypeV1:
    """Classify only one exact K2 event/source contract as a K6-B trigger."""

    if event.event_type is EventTypeV2.TRADE:
        if event.source is not EventSourceV2.QMT_GATEWAY_CALLBACK:
            raise KernelRepositoryConflict("K6-B TRADE trigger source is not QMT gateway callback")
        return DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED
    if event.event_type in {EventTypeV2.ORDER, EventTypeV2.RECONCILE}:
        expected_source = (
            EventSourceV2.QMT_GATEWAY_CALLBACK
            if event.event_type is EventTypeV2.ORDER
            else EventSourceV2.QMT_OMS_RECONCILIATION
        )
        if event.source is not expected_source:
            raise KernelRepositoryConflict("K6-B terminal SELL trigger source differs from registered K2 authority")
        return DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL
    if event.event_type is EventTypeV2.ACCOUNT:
        if event.source is not EventSourceV2.QMT_OMS_PROJECTION:
            raise KernelRepositoryConflict("K6-B ACCOUNT trigger source is not QMT OMS projection")
        return DependentBuyTriggerTypeV1.ACCOUNT_REFRESHED
    if event.event_type is EventTypeV2.EOD:
        if event.source is not EventSourceV2.EXCHANGE_SESSION_CLOCK:
            raise KernelRepositoryConflict("K6-B EOD trigger source is not exchange-session clock")
        return DependentBuyTriggerTypeV1.SESSION_EOD
    raise KernelRepositoryConflict("runtime event is not one legal dependent-BUY trigger")


_TRADE_COLUMNS = (
    "trade_id",
    "intent_id",
    "strategy_id",
    "qmt_order_id",
    "qmt_order_sysid",
    "symbol",
    "side",
    "price",
    "quantity",
    "amount",
    "commission",
    "trade_date",
    "account_id",
    "trade_time",
    "order_remark",
    "raw_json",
)
_CASH_COLUMNS = (
    "cash_id",
    "cash_sequence",
    "strategy_id",
    "account_id",
    "trade_date",
    "entry_type",
    "cash_delta",
    "cash_after",
    "frozen_delta",
    "frozen_after",
    "intent_id",
    "trade_id",
    "symbol",
    "reason",
    "metadata",
    "created_at",
)


def _selected_row(row: Any, columns: tuple[str, ...]) -> dict[str, Any]:
    return {column: row[column] for column in columns}


def _strict_event_payload(event: RuntimeEventEnvelopeV2) -> Any:
    if event.event_type in {EventTypeV2.ORDER, EventTypeV2.TRADE, EventTypeV2.RECONCILE}:
        return strict_readback_kernel_event_payload_v1(event)
    if event.event_type in {EventTypeV2.ACCOUNT, EventTypeV2.EOD}:
        return None
    raise KernelRepositoryConflict("runtime event is not one K6-B trigger")


def _assert_existing_decision_owner_set_v2(
    existing_coordination_ids: tuple[str, ...],
    candidate_coordination_ids: tuple[str, ...],
) -> None:
    """Require replay evidence to close over the exact durable event owners."""

    if existing_coordination_ids != candidate_coordination_ids:
        raise KernelRepositoryConflict("K6-B event durable decision owner set differs from its exact candidates")


class KernelDependentBuyRepositoryMixin:
    """Own the K6-B decision + mapping + outbox all-or-nothing writer."""

    def read_dependent_buy_release_bundle_v2(self, decision_id: str) -> dict[str, Any]:
        decision_id = _strict_sha256(decision_id, field_name="decision_id")
        with self._connection(transaction=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_dependent_buy_decision WHERE decision_id=%s",
                    (decision_id,),
                )
                decision_row = cur.fetchone()
                if decision_row is None:
                    raise KeyError(decision_id)
                decision = validate_kernel_product_payload_v1(
                    DependentBuyReleaseDecisionV2,
                    _row_json(decision_row, "carrier_json"),
                    stage="K6B_DECISION_V2_READBACK",
                )
                trigger = validate_kernel_product_payload_v1(
                    DependentBuyTriggerEventRefV1,
                    _row_json(decision_row, "trigger_ref_json"),
                    stage="K6B_TRIGGER_READBACK",
                )
                ledger = validate_kernel_product_payload_v1(
                    DependentBuyLedgerObservationV2,
                    _row_json(decision_row, "ledger_observation_json"),
                    stage="K6B_LEDGER_READBACK",
                )
                _assert_scalar_columns(decision_row, _decision_projection_v2(decision), carrier_name="k6b_decision")
                _assert_scalar_columns(
                    decision_row,
                    _decision_evidence_projection_v2(trigger, ledger),
                    carrier_name="k6b_decision_evidence",
                )
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_dependent_buy_coordination WHERE coordination_id=%s",
                    (decision.coordination_id,),
                )
                coordination_row = cur.fetchone()
                if coordination_row is None:
                    raise KernelRepositoryConflict("K6-B decision readback lacks its coordination")
                coordination = validate_kernel_product_payload_v1(
                    DependentBuyCoordinationV2,
                    _row_json(coordination_row, "carrier_json"),
                    stage="K6B_COORDINATION_READBACK",
                )
                _assert_scalar_columns(
                    coordination_row,
                    _coordination_projection_v2(coordination),
                    carrier_name="k6b_coordination",
                )
                dependencies = self._read_locked_dependencies_v2(
                    cur,
                    coordination=coordination,
                    lock_clause="",
                )
                if dependencies != coordination.ordered_sell_dependencies:
                    raise KernelRepositoryConflict("K6-B coordination carrier differs from its durable dependency rows")
                # Mapping identity is held by the decision's original authority
                # item, not by a synthetic release command.  Resolve it through
                # the single durable product-item association below.
                cur.execute(
                    "SELECT authority.*,authority.carrier_json AS authority_carrier_json,item.* "
                    "FROM qmt_strategy.execution_product_command_authority_item AS item "
                    "JOIN qmt_strategy.execution_product_command_authority AS authority "
                    "ON authority.authority_set_sha256=item.authority_set_sha256 "
                    "WHERE item.item_sha256=%s",
                    (coordination.release_command_authority_item_sha256,),
                )
                item_row = cur.fetchone()
                if item_row is None:
                    raise KernelRepositoryConflict("K6-B coordination lacks its durable authority item")
                authority_envelope, authority_item = self._strict_authority_item_row_v3(item_row)
                if (
                    authority_item not in authority_envelope.authority_set.ordered_items
                    or authority_item.item_sha256 != coordination.release_command_authority_item_sha256
                    or authority_item.coordination_id != coordination.coordination_id
                    or authority_item.command_id != coordination.release_command_id
                    or authority_item.transition_id != coordination.release_transition_id
                ):
                    raise KernelRepositoryConflict("K6-B durable authority item differs from coordination closure")
                cur.execute(
                    "SELECT * FROM qmt_strategy.execution_child_order WHERE mapping_id=%s",
                    (item_row["mapping_id"],),
                )
                mapping_row = cur.fetchone()
                if mapping_row is None:
                    raise KernelRepositoryConflict("K6-B coordination lacks its durable product mapping")
                mapping = validate_kernel_product_payload_v1(
                    ProductCommandChildMappingV1,
                    _row_json(mapping_row, "mapping_json"),
                    stage="K6B_MAPPING_READBACK",
                )
                _assert_scalar_columns(
                    mapping_row,
                    _mapping_scalar_projection(mapping),
                    carrier_name="k6b_mapping",
                )
                outbox = None
                if decision.decision.value == "RELEASE_TO_K2_OUTBOX":
                    cur.execute(
                        "SELECT carrier_json FROM qmt_strategy.execution_algo_command_outbox WHERE command_id=%s",
                        (coordination.release_command_id,),
                    )
                    outbox_row = cur.fetchone()
                    if outbox_row is None:
                        raise KernelRepositoryConflict("K6-B released coordination lacks same-command outbox")
                    outbox = validate_kernel_product_payload_v1(
                        BrokerCommandOutboxV1,
                        _row_json(outbox_row, "carrier_json"),
                        stage="K6B_OUTBOX_READBACK",
                    )
                    outbox.validate_initial_v1()
        if decision.coordination_id != coordination.coordination_id:
            raise KernelRepositoryConflict("K6-B decision owner differs from durable coordination")
        if decision.decision.value == "RELEASE_TO_K2_OUTBOX":
            if (
                coordination.status is not DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
                or mapping.mapping_status is not ProductCommandChildMappingStatusV1.RESERVED
                or outbox is None
                or outbox.command_id != coordination.release_command_id
                or outbox.broker_called is not None
            ):
                raise KernelRepositoryConflict("K6-B release durable closure is incomplete")
        return {
            "coordination": coordination,
            "dependencies": dependencies,
            "decision": decision,
            "trigger": trigger,
            "ledger": ledger,
            "mapping": mapping,
            "outbox": outbox,
        }

    @staticmethod
    def _strict_authority_item_row_v3(
        row: Any,
    ) -> tuple[ProductCommandAuthorityEnvelopeV3, Any]:
        authority_envelope = validate_kernel_product_payload_v1(
            ProductCommandAuthorityEnvelopeV3,
            _row_json(row, "authority_carrier_json"),
            stage="K6B_AUTHORITY_ENVELOPE_READBACK",
        )
        _assert_scalar_columns(
            row,
            _authority_projection_v3(authority_envelope.authority_set),
            carrier_name="k6b_authority",
        )
        # The aggregate cannot be empty when an item row exists.  Avoid using
        # treating a valid aggregate header as proof for an orphaned item.
        if not authority_envelope.authority_set.ordered_items:
            raise KernelRepositoryConflict("K6-B authority item row has an empty aggregate owner")
        authority_item = validate_kernel_product_payload_v1(
            ProductCommandAuthorityItemV3,
            _row_json(row, "carrier_json"),
            stage="K6B_AUTHORITY_ITEM_READBACK",
        )
        _assert_scalar_columns(
            row,
            _authority_item_projection_v3(authority_item),
            carrier_name="k6b_authority_item",
        )
        return authority_envelope, authority_item

    @staticmethod
    def _read_locked_dependencies_v2(
        cur: Any,
        *,
        coordination: DependentBuyCoordinationV2,
        lock_clause: str,
    ) -> tuple[DependentBuySellDependencyV2, ...]:
        if lock_clause not in {"", " FOR SHARE", " FOR UPDATE"}:
            raise ValueError("K6-B dependency lock clause is not registered")
        cur.execute(
            "SELECT * FROM qmt_strategy.execution_dependent_buy_dependency "
            "WHERE coordination_id=%s ORDER BY sell_parent_intent_id" + lock_clause,
            (coordination.coordination_id,),
        )
        rows = cur.fetchall()
        dependencies: list[DependentBuySellDependencyV2] = []
        for row in rows:
            dependency = validate_kernel_product_payload_v1(
                DependentBuySellDependencyV2,
                _row_json(row, "carrier_json"),
                stage="K6B_DEPENDENCY_READBACK",
            )
            _assert_scalar_columns(
                row,
                _dependency_projection_v2(dependency),
                carrier_name="k6b_dependency",
            )
            if row["coordination_id"] != coordination.coordination_id:
                raise KernelRepositoryConflict("K6-B dependency row owner differs from coordination")
            dependencies.append(dependency)
        result = tuple(dependencies)
        if tuple(item.sell_parent_intent_id for item in result) != tuple(
            item.sell_parent_intent_id for item in coordination.ordered_sell_dependencies
        ):
            raise KernelRepositoryConflict("K6-B durable dependency rows are missing, extra, or noncanonical")
        return result

    @staticmethod
    def _read_k6b_event_cursor(cur: Any, event_id: str) -> RuntimeEventEnvelopeV2:
        cur.execute(
            "SELECT event_id,runtime_id,sequence,payload FROM qmt_strategy.execution_runtime_event "
            "WHERE event_id=%s FOR SHARE",
            (event_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KeyError(event_id)
        event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(row, "payload"))
        if (row["event_id"], row["runtime_id"], int(row["sequence"])) != (
            event.event_id,
            event.runtime_id,
            event.sequence,
        ):
            raise KernelRepositoryConflict("K6-B event scalar columns differ from its strict envelope")
        _event_trigger_type(event)
        _strict_event_payload(event)
        return event

    @staticmethod
    def _event_matches_dependency(event: RuntimeEventEnvelopeV2, dependency: DependentBuySellDependencyV2) -> bool:
        if event.event_type not in {EventTypeV2.ORDER, EventTypeV2.TRADE, EventTypeV2.RECONCILE}:
            return False
        payload = _strict_event_payload(event)
        return (
            payload.runtime_id,
            payload.algo_instance_id,
            payload.parent_intent_id,
            payload.side,
        ) == (
            dependency.runtime_id,
            dependency.sell_algo_instance_id,
            dependency.sell_parent_intent_id,
            SideV1.SELL,
        )

    @staticmethod
    def _account_event_matches_authority(
        event: RuntimeEventEnvelopeV2,
        authority_item: ProductCommandAuthorityItemV3,
    ) -> bool:
        if event.event_type is not EventTypeV2.ACCOUNT:
            return True
        account_refs = tuple(
            ref
            for ref in authority_item.evaluation_evidence.execution_projection_set.ordered_projection_refs
            if ref.projection_type is KernelProjectionTypeV1.ACCOUNT
        )
        if len(account_refs) != 1:
            raise KernelRepositoryConflict("K6-B V3 authority lacks one exact account projection reference")
        return (
            thaw_json_v1(event.source_identity)
            == {
                "projection_version": account_refs[0].projection_version,
                "projection_sha256": authority_item.account_projection_sha256,
            }
            and account_refs[0].payload_sha256 == authority_item.account_projection_sha256
        )

    def _candidate_coordination_ids_cursor(
        self,
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
    ) -> tuple[str, ...]:
        if event.event_type in {EventTypeV2.ORDER, EventTypeV2.TRADE, EventTypeV2.RECONCILE}:
            payload = _strict_event_payload(event)
            if payload.side is not SideV1.SELL:
                raise KernelRepositoryConflict("K6-B order/trade trigger is not a SELL fact")
            cur.execute(
                "SELECT coordination.coordination_id "
                "FROM qmt_strategy.execution_dependent_buy_coordination AS coordination "
                "JOIN qmt_strategy.execution_dependent_buy_dependency AS dependency "
                "ON dependency.coordination_id=coordination.coordination_id "
                "WHERE coordination.runtime_id=%s AND dependency.sell_algo_instance_id=%s "
                "AND dependency.sell_parent_intent_id=%s "
                "ORDER BY coordination.coordination_id",
                (event.runtime_id, payload.algo_instance_id, payload.parent_intent_id),
            )
        else:
            cur.execute(
                "SELECT coordination_id FROM qmt_strategy.execution_dependent_buy_coordination "
                "WHERE runtime_id=%s AND trade_date=("
                "SELECT trade_date FROM qmt_strategy.execution_runtime WHERE runtime_id=%s"
                ") ORDER BY coordination_id",
                (event.runtime_id, event.runtime_id),
            )
        values = tuple(row["coordination_id"] for row in cur.fetchall())
        if values != tuple(sorted(set(values))):
            raise KernelRepositoryConflict("K6-B candidate coordination identities are noncanonical or duplicated")
        return values

    @staticmethod
    def _read_route_owner_cursor(cur: Any, coordination: DependentBuyCoordinationV2) -> ProductRouteOwnerV1:
        cur.execute(
            "SELECT * FROM qmt_strategy.execution_product_route_owner "
            "WHERE runtime_id=%s AND binding_id=%s AND trade_date=%s FOR SHARE",
            (coordination.runtime_id, coordination.binding_id, coordination.trade_date),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("K6-B coordination lacks its product route owner")
        owner = validate_kernel_product_payload_v1(
            ProductRouteOwnerV1,
            _row_json(row, "carrier_json"),
            stage="K6B_ROUTE_OWNER_READBACK",
        )
        if (
            owner.runtime_id,
            owner.binding_id,
            owner.trade_date,
            owner.route_owner,
        ) != (
            coordination.runtime_id,
            coordination.binding_id,
            coordination.trade_date,
            ProductRouteOwnerKindV1.KERNEL_V2,
        ):
            raise KernelRepositoryConflict("K6-B coordinator is not owned by the exact KERNEL_V2 route")
        return owner

    @staticmethod
    def _read_session_authority_cursor(
        cur: Any, coordination: DependentBuyCoordinationV2
    ) -> ExchangeSessionAuthorityV1:
        cur.execute(
            "SELECT authority_json FROM qmt_strategy.execution_exchange_session_authority "
            "WHERE runtime_id=%s AND exchange_trade_date=%s FOR SHARE",
            (coordination.runtime_id, coordination.trade_date),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("K6-B coordination lacks exchange-session authority")
        authority = _model_from_json(ExchangeSessionAuthorityV1, _row_json(row, "authority_json"))
        if authority.authority_sha256 != coordination.session_authority_sha256:
            raise KernelRepositoryConflict("K6-B session authority differs from frozen coordination")
        return authority

    def _read_authority_mapping_cursor(
        self,
        cur: Any,
        *,
        coordination: DependentBuyCoordinationV2,
    ) -> tuple[ProductCommandAuthorityEnvelopeV3, ProductCommandAuthorityItemV3, ProductCommandChildMappingV1]:
        cur.execute(
            "SELECT authority.*,authority.carrier_json AS authority_carrier_json,item.* "
            "FROM qmt_strategy.execution_product_command_authority_item AS item "
            "JOIN qmt_strategy.execution_product_command_authority AS authority "
            "ON authority.authority_set_sha256=item.authority_set_sha256 "
            "WHERE item.item_sha256=%s FOR SHARE OF authority,item",
            (coordination.release_command_authority_item_sha256,),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("K6-B coordination lacks its exact V3 authority item")
        envelope, item = self._strict_authority_item_row_v3(row)
        candidate = item.evaluation_evidence.dependent_buy_candidate
        if (
            item not in envelope.authority_set.ordered_items
            or item.coordination_id != coordination.coordination_id
            or item.command_id != coordination.release_command_id
            or item.transition_id != coordination.release_transition_id
            or candidate is None
            or (
                candidate.runtime_id,
                candidate.binding_id,
                candidate.trade_date,
                candidate.strategy_id,
                candidate.buy_algo_instance_id,
                candidate.buy_parent_intent_id,
                candidate.required_cash,
                candidate.virtual_account_id,
                candidate.session_authority_sha256,
                candidate.ordered_sell_dependencies,
            )
            != (
                coordination.runtime_id,
                coordination.binding_id,
                coordination.trade_date,
                coordination.strategy_id,
                coordination.buy_algo_instance_id,
                coordination.buy_parent_intent_id,
                coordination.required_cash,
                coordination.virtual_account_id,
                coordination.session_authority_sha256,
                coordination.ordered_sell_dependencies,
            )
        ):
            raise KernelRepositoryConflict("K6-B V3 authority item differs from frozen coordination")
        cur.execute(
            "SELECT * FROM qmt_strategy.execution_child_order WHERE mapping_id=%s FOR UPDATE",
            (item.mapping_id,),
        )
        mapping_row = cur.fetchone()
        if mapping_row is None:
            raise KernelRepositoryConflict("K6-B deferred mapping is absent")
        mapping = validate_kernel_product_payload_v1(
            ProductCommandChildMappingV1,
            _row_json(mapping_row, "mapping_json"),
            stage="K6B_DEFERRED_MAPPING_READBACK",
        )
        _assert_scalar_columns(mapping_row, _mapping_scalar_projection(mapping), carrier_name="k6b_mapping")
        if (
            mapping.coordination_id != coordination.coordination_id
            or mapping.authority_item_sha256 != item.item_sha256
            or mapping.mapping_status is not ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY
        ):
            raise KernelRepositoryConflict("K6-B mapping differs from deferred authority closure")
        return envelope, item, mapping

    @staticmethod
    def _read_virtual_account_cursor(cur: Any, coordination: DependentBuyCoordinationV2) -> Any:
        cur.execute(
            "SELECT strategy_id,account_id,cash,updated_at FROM qmt_strategy.virtual_account "
            "WHERE strategy_id=%s FOR UPDATE",
            (coordination.strategy_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelRepositoryConflict("K6-B frozen virtual account is absent")
        if (row["strategy_id"], row["account_id"]) != (
            coordination.strategy_id,
            coordination.virtual_account_id,
        ):
            raise KernelRepositoryConflict("K6-B virtual account differs from frozen owner")
        return row

    @staticmethod
    def _read_settled_refs_cursor(
        cur: Any,
        *,
        coordination: DependentBuyCoordinationV2,
        dependency: DependentBuySellDependencyV2,
    ) -> tuple[DependentBuySettledProceedsRefV2, ...]:
        cur.execute(
            "SELECT * FROM qmt_strategy.trade_ledger WHERE strategy_id=%s AND account_id=%s "
            "AND trade_date=%s AND intent_id=%s AND side='SELL' ORDER BY trade_id FOR SHARE",
            (
                coordination.strategy_id,
                coordination.virtual_account_id,
                coordination.trade_date,
                dependency.sell_parent_intent_id,
            ),
        )
        trade_rows = cur.fetchall()
        refs: list[DependentBuySettledProceedsRefV2] = []
        for trade_row in trade_rows:
            cur.execute(
                "SELECT * FROM qmt_strategy.cash_ledger WHERE strategy_id=%s AND account_id=%s "
                "AND trade_date=%s AND intent_id=%s AND trade_id=%s AND entry_type='SELL_FILL' "
                "ORDER BY cash_sequence FOR SHARE",
                (
                    coordination.strategy_id,
                    coordination.virtual_account_id,
                    coordination.trade_date,
                    dependency.sell_parent_intent_id,
                    trade_row["trade_id"],
                ),
            )
            cash_rows = cur.fetchall()
            if len(cash_rows) != 1:
                raise KernelRepositoryConflict("K6-B settled SELL trade must close to exactly one cash-ledger fact")
            cash_row = cash_rows[0]
            trade_fact = qmt_trade_ledger_fact_sha256_v1(_selected_row(trade_row, _TRADE_COLUMNS))
            cash_fact = cash_ledger_fact_sha256_v1(_selected_row(cash_row, _CASH_COLUMNS))
            refs.append(
                DependentBuySettledProceedsRefV2.create(
                    broker_trade_id=trade_row["trade_id"],
                    qmt_trade_ledger_id=(
                        f"{trade_row['account_id']}:{trade_row['trade_date'].isoformat()}:{trade_row['trade_id']}"
                    ),
                    qmt_trade_account_id=trade_row["account_id"],
                    qmt_trade_fact_sha256=trade_fact,
                    cash_ledger_id=cash_row["cash_id"],
                    cash_ledger_sequence=int(cash_row["cash_sequence"]),
                    cash_ledger_fact_sha256=cash_fact,
                    strategy_id=coordination.strategy_id,
                    runtime_id=coordination.runtime_id,
                    trade_date=coordination.trade_date,
                    sell_parent_intent_id=dependency.sell_parent_intent_id,
                )
            )
        result = tuple(sorted(refs, key=lambda value: value.sort_key_v2()))
        existing = {value.sort_key_v2(): value for value in dependency.ordered_settled_proceeds_refs}
        current = {value.sort_key_v2(): value for value in result}
        if any(current.get(key) != value for key, value in existing.items()):
            raise KernelRepositoryConflict("K6-B durable settled proceeds were removed or changed")
        return result

    def _reconstruct_trigger_and_ledger_cursor(
        self,
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        coordination: DependentBuyCoordinationV2,
        authority_item: ProductCommandAuthorityItemV3,
        dependencies: tuple[DependentBuySellDependencyV2, ...],
        account_row: Any,
        session_authority: ExchangeSessionAuthorityV1,
        decided_at_utc: Any,
    ) -> tuple[
        DependentBuyTriggerEventRefV1,
        DependentBuyLedgerObservationV2,
        tuple[DependentBuySellDependencyV2, ...],
    ]:
        trigger_type = _event_trigger_type(event)
        payload = _strict_event_payload(event)
        source_fact_type: str
        source_fact_id: str
        source_fact_sha256: str
        observed: list[DependentBuySellDependencyV2] = []
        matched = False
        for dependency in dependencies:
            refs = self._read_settled_refs_cursor(cur, coordination=coordination, dependency=dependency)
            latest_id = dependency.latest_order_fact_id
            latest_hash = dependency.latest_order_fact_sha256
            status = dependency.dependency_status
            if event.event_type is EventTypeV2.TRADE and self._event_matches_dependency(event, dependency):
                matched = True
                if not any(value.broker_trade_id == payload.trade_id for value in refs):
                    raise KernelRepositoryConflict("K6-B TRADE event lacks settled trade/cash ledger closure")
                status = DependentBuyDependencyStatusV1.PROCEEDS_SETTLED
            elif event.event_type in {EventTypeV2.ORDER, EventTypeV2.RECONCILE} and self._event_matches_dependency(
                event, dependency
            ):
                matched = True
                if not payload.terminal:
                    raise KernelRepositoryConflict("K6-B SELL order trigger is not terminal")
                latest_id = (
                    payload.order_event_id if isinstance(payload, KernelOrderEventPayloadV1) else payload.receipt_id
                )
                latest_hash = payload.fact_sha256
                status = DependentBuyDependencyStatusV1.TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS
            elif refs and status is DependentBuyDependencyStatusV1.OPEN:
                status = DependentBuyDependencyStatusV1.PROCEEDS_SETTLED
            observed.append(
                DependentBuySellDependencyV2.create(
                    runtime_id=dependency.runtime_id,
                    strategy_id=dependency.strategy_id,
                    sell_parent_intent_id=dependency.sell_parent_intent_id,
                    sell_algo_instance_id=dependency.sell_algo_instance_id,
                    latest_order_fact_id=latest_id,
                    latest_order_fact_sha256=latest_hash,
                    ordered_settled_proceeds_refs=refs,
                    dependency_status=status,
                )
            )
        if event.event_type in {EventTypeV2.ORDER, EventTypeV2.TRADE, EventTypeV2.RECONCILE} and not matched:
            raise KernelRepositoryConflict("K6-B trigger does not own any frozen SELL dependency")
        identity = thaw_json_v1(event.source_identity)
        if event.event_type is EventTypeV2.TRADE:
            matching_refs = [
                ref
                for dependency in observed
                for ref in dependency.ordered_settled_proceeds_refs
                if ref.broker_trade_id == payload.trade_id
            ]
            if len(matching_refs) != 1:
                raise KernelRepositoryConflict("K6-B TRADE event does not resolve one settled proceeds fact")
            source_fact_type = "qmt_strategy.trade_ledger"
            source_fact_id = payload.trade_id
            source_fact_sha256 = matching_refs[0].qmt_trade_fact_sha256
        elif event.event_type is EventTypeV2.ORDER:
            source_fact_type = "miniqmt_kernel_order_event_payload_v1"
            source_fact_id = payload.order_event_id
            source_fact_sha256 = payload.fact_sha256
        elif event.event_type is EventTypeV2.RECONCILE:
            source_fact_type = "miniqmt_kernel_order_reconcile_payload_v1"
            source_fact_id = payload.receipt_id
            source_fact_sha256 = payload.fact_sha256
        elif event.event_type is EventTypeV2.ACCOUNT:
            if not self._account_event_matches_authority(event, authority_item):
                raise KernelRepositoryConflict("K6-B ACCOUNT event differs from frozen V3 account projection")
            source_fact_type = "miniqmt_account_projection_v1"
            source_fact_id = identity["projection_version"]
            source_fact_sha256 = identity["projection_sha256"]
        else:
            eod_payload = thaw_json_v1(event.payload)
            expected_eod = {
                "runtime_id": coordination.runtime_id,
                "trade_date": coordination.trade_date.isoformat(),
                "session_epoch": session_epoch_v1(session_authority),
                "session_phase": "CLOSED",
                "phase_boundary_at_utc": event.event_time_utc,
                "terminal_outcome": "EXPIRED_WITH_RESIDUAL",
                "exchange_session_authority_sha256": session_authority.authority_sha256,
            }
            if eod_payload != expected_eod or identity != {
                "runtime_id": coordination.runtime_id,
                "trade_date": coordination.trade_date.isoformat(),
                "session_epoch": session_epoch_v1(session_authority),
            }:
                raise KernelRepositoryConflict("K6-B EOD event differs from exchange-session authority")
            source_fact_type = "miniqmt_eod_event_v1"
            source_fact_id = event.event_id
            source_fact_sha256 = event.payload_sha256
        trigger = DependentBuyTriggerEventRefV1.create(
            runtime_id=coordination.runtime_id,
            event_id=event.event_id,
            event_type=trigger_type,
            event_sequence=event.sequence,
            source_fact_type=source_fact_type,
            source_fact_id=source_fact_id,
            source_fact_sha256=source_fact_sha256,
            observed_at_utc=event.event_time_utc,
        )
        all_refs = tuple(
            sorted(
                (ref for item in observed for ref in item.ordered_settled_proceeds_refs), key=lambda x: x.sort_key_v2()
            )
        )
        cur.execute(
            "SELECT cash_sequence FROM qmt_strategy.cash_ledger "
            "WHERE strategy_id=%s AND account_id=%s AND trade_date=%s ORDER BY cash_sequence FOR SHARE",
            (coordination.strategy_id, coordination.virtual_account_id, coordination.trade_date),
        )
        cash_sequence_rows = cur.fetchall()
        latest_cash_sequence = max((int(row["cash_sequence"]) for row in cash_sequence_rows), default=0)
        ledger = DependentBuyLedgerObservationV2.create(
            runtime_id=coordination.runtime_id,
            strategy_id=coordination.strategy_id,
            trade_date=coordination.trade_date,
            virtual_account_id=coordination.virtual_account_id,
            virtual_account_updated_at_utc=account_row["updated_at"],
            latest_cash_ledger_sequence=latest_cash_sequence,
            ledger_as_of_utc=decided_at_utc,
            available_cash=canonical_decimal_string_v1(account_row["cash"], field_name="virtual_account.cash"),
            required_cash=coordination.required_cash,
            ordered_settled_proceeds_refs=all_refs,
            freshness_session_authority_sha256=session_authority.authority_sha256,
        )
        return trigger, ledger, tuple(observed)

    def _assert_eod_prior_triggers_consumed_cursor(
        self,
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        coordination: DependentBuyCoordinationV2,
        authority_item: ProductCommandAuthorityItemV3,
        dependencies: tuple[DependentBuySellDependencyV2, ...],
    ) -> None:
        if event.event_type is not EventTypeV2.EOD:
            return
        account_refs = tuple(
            ref
            for ref in authority_item.evaluation_evidence.execution_projection_set.ordered_projection_refs
            if ref.projection_type is KernelProjectionTypeV1.ACCOUNT
        )
        if len(account_refs) != 1:
            raise KernelRepositoryConflict("K6-B EOD closure lacks one frozen account projection")
        cur.execute(
            "SELECT payload FROM qmt_strategy.execution_runtime_event "
            "WHERE runtime_id=%s AND sequence<%s AND event_type=ANY(%s) ORDER BY sequence,event_id FOR SHARE",
            (
                event.runtime_id,
                event.sequence,
                [
                    EventTypeV2.TRADE.value,
                    EventTypeV2.ORDER.value,
                    EventTypeV2.RECONCILE.value,
                    EventTypeV2.ACCOUNT.value,
                    EventTypeV2.EOD.value,
                ],
            ),
        )
        for row in cur.fetchall():
            prior = _model_from_json(RuntimeEventEnvelopeV2, _row_json(row, "payload"))
            relevant = any(self._event_matches_dependency(prior, dependency) for dependency in dependencies)
            if prior.event_type is EventTypeV2.ACCOUNT:
                identity = thaw_json_v1(prior.source_identity)
                relevant = identity == {
                    "projection_version": account_refs[0].projection_version,
                    "projection_sha256": authority_item.account_projection_sha256,
                }
            elif prior.event_type is EventTypeV2.EOD:
                relevant = thaw_json_v1(prior.source_identity).get("trade_date") == coordination.trade_date.isoformat()
            if not relevant:
                continue
            cur.execute(
                "SELECT 1 FROM qmt_strategy.execution_dependent_buy_decision "
                "WHERE coordination_id=%s AND trigger_event_id=%s",
                (coordination.coordination_id, prior.event_id),
            )
            if cur.fetchone() is None:
                raise KernelRepositoryConflict("K6-B EOD has an earlier relevant trigger without terminal disposition")

    def coordinate_dependent_buys_for_event_atomic_v2(
        self,
        *,
        event_id: str,
        worker_id: str,
        process_incarnation_id: str,
    ) -> tuple[dict[str, Any], ...]:
        """Consume one durable K2 event and atomically advance all affected BUY owners."""

        if type(event_id) is not str or not event_id or event_id != event_id.strip():
            raise ValueError("event_id must be one strict durable identity")
        if type(worker_id) is not str or not worker_id or worker_id != worker_id.strip():
            raise ValueError("worker_id must be one strict identity")
        if (
            type(process_incarnation_id) is not str
            or not process_incarnation_id
            or process_incarnation_id != process_incarnation_id.strip()
        ):
            raise ValueError("process_incarnation_id must be one strict identity")
        decision_ids: list[str] = []
        with self._connection(transaction=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                event = self._read_k6b_event_cursor(cur, event_id)
                cur.execute(
                    "SELECT decision_id,coordination_id FROM qmt_strategy.execution_dependent_buy_decision "
                    "WHERE trigger_event_id=%s ORDER BY coordination_id",
                    (event_id,),
                )
                existing_rows = tuple(cur.fetchall())
                existing_ids = tuple(row["decision_id"] for row in existing_rows)
                existing_coordination_ids = tuple(row["coordination_id"] for row in existing_rows)
                if event.event_type is EventTypeV2.ACCOUNT:
                    # ACCOUNT is runtime-wide ingress but V3 account projections
                    # are strategy-specific.  Exact matching therefore happens
                    # under each coordination lock below.
                    existing_ids = ()
                candidate_ids = self._candidate_coordination_ids_cursor(cur, event=event)
                if existing_ids:
                    _assert_existing_decision_owner_set_v2(existing_coordination_ids, candidate_ids)
                    decision_ids.extend(existing_ids)
                else:
                    for coordination_id in candidate_ids:
                        cur.execute(
                            "SELECT * FROM qmt_strategy.execution_dependent_buy_coordination "
                            "WHERE coordination_id=%s FOR UPDATE",
                            (coordination_id,),
                        )
                        coordination_row = cur.fetchone()
                        if coordination_row is None:
                            raise KernelRepositoryConflict("K6-B candidate coordination disappeared under lock")
                        coordination = validate_kernel_product_payload_v1(
                            DependentBuyCoordinationV2,
                            _row_json(coordination_row, "carrier_json"),
                            stage="K6B_COORDINATION_TRIGGER_READBACK",
                        )
                        _assert_scalar_columns(
                            coordination_row,
                            _coordination_projection_v2(coordination),
                            carrier_name="k6b_coordination",
                        )
                        cur.execute(
                            "SELECT decision_id FROM qmt_strategy.execution_dependent_buy_decision "
                            "WHERE coordination_id=%s AND trigger_event_id=%s",
                            (coordination.coordination_id, event.event_id),
                        )
                        existing_decision = cur.fetchone()
                        if existing_decision is not None:
                            decision_ids.append(existing_decision["decision_id"])
                            continue
                        if coordination.status is not DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS:
                            continue
                        self._read_route_owner_cursor(cur, coordination)
                        cur.execute(
                            "SELECT 1 FROM qmt_strategy.execution_runtime WHERE runtime_id=%s FOR SHARE",
                            (coordination.runtime_id,),
                        )
                        if cur.fetchone() is None:
                            raise KernelRepositoryConflict("K6-B runtime owner is absent")
                        cur.execute(
                            "SELECT 1 FROM qmt_strategy.execution_algo_instance "
                            "WHERE runtime_id=%s AND algo_instance_id=%s FOR SHARE",
                            (coordination.runtime_id, coordination.buy_algo_instance_id),
                        )
                        if cur.fetchone() is None:
                            raise KernelRepositoryConflict("K6-B BUY algo owner is absent")
                        dependencies = self._read_locked_dependencies_v2(
                            cur,
                            coordination=coordination,
                            lock_clause=" FOR UPDATE",
                        )
                        account_row = self._read_virtual_account_cursor(cur, coordination)
                        session_authority = self._read_session_authority_cursor(cur, coordination)
                        envelope, authority_item, mapping = self._read_authority_mapping_cursor(
                            cur,
                            coordination=coordination,
                        )
                        if not self._account_event_matches_authority(event, authority_item):
                            continue
                        self._assert_eod_prior_triggers_consumed_cursor(
                            cur,
                            event=event,
                            coordination=coordination,
                            authority_item=authority_item,
                            dependencies=dependencies,
                        )
                        cur.execute("SELECT clock_timestamp() AS decided_at_utc")
                        decided_at_utc = cur.fetchone()["decided_at_utc"]
                        trigger, ledger, observed_dependencies = self._reconstruct_trigger_and_ledger_cursor(
                            cur,
                            event=event,
                            coordination=coordination,
                            authority_item=authority_item,
                            dependencies=dependencies,
                            account_row=account_row,
                            session_authority=session_authority,
                            decided_at_utc=decided_at_utc,
                        )
                        lease_epoch = coordination.lease_epoch + 1
                        self._verify_k6_worker_cursor(
                            cur,
                            worker_id,
                            process_incarnation_id,
                            lease_epoch,
                        )
                        decision, successor, successor_mapping, outbox = build_dependent_buy_trigger_bundle_v2(
                            coordination=coordination,
                            authority_item=authority_item,
                            authority_set=envelope.authority_set,
                            mapping=mapping,
                            observed_dependencies=observed_dependencies,
                            trigger=trigger,
                            ledger_observation=ledger,
                            session_authority_sha256=session_authority.authority_sha256,
                            decided_at_utc=decided_at_utc,
                            worker_id=worker_id,
                            process_incarnation_id=process_incarnation_id,
                            lease_epoch=lease_epoch,
                            lease_expires_at_utc=decided_at_utc + timedelta(seconds=30),
                        )
                        self._persist_dependent_buy_trigger_bundle_v2(
                            previous_coordination=coordination,
                            decision=decision,
                            successor_coordination=successor,
                            trigger=trigger,
                            ledger=ledger,
                            previous_mapping=mapping,
                            successor_mapping=successor_mapping,
                            outbox=outbox,
                            cursor=cur,
                        )
                        decision_ids.append(decision.decision_id)
        return tuple(self.read_dependent_buy_release_bundle_v2(value) for value in decision_ids)

    def _persist_dependent_buy_trigger_bundle_v2(
        self,
        *,
        previous_coordination: DependentBuyCoordinationV2,
        decision: DependentBuyReleaseDecisionV2,
        successor_coordination: DependentBuyCoordinationV2,
        trigger: DependentBuyTriggerEventRefV1,
        ledger: DependentBuyLedgerObservationV2,
        previous_mapping: ProductCommandChildMappingV1,
        successor_mapping: ProductCommandChildMappingV1,
        outbox: BrokerCommandOutboxV1 | None,
        cursor: Any | None = None,
    ) -> None:
        """Persist one repository-built K6-B bundle using the caller's transaction.

        This helper is intentionally private.  The only product seam accepts a
        durable event identity and calls this helper with the same cursor that
        reconstructed every source fact.
        """

        if cursor is None:
            raise RuntimeError("K6-B persistence requires the trigger reader's active transaction cursor")

        previous_coordination = _strict_model(
            DependentBuyCoordinationV2, previous_coordination, stage="K6B_PREDECESSOR"
        )
        decision = _strict_model(DependentBuyReleaseDecisionV2, decision, stage="K6B_DECISION")
        successor_coordination = _strict_model(
            DependentBuyCoordinationV2, successor_coordination, stage="K6B_SUCCESSOR"
        )
        trigger = _strict_model(DependentBuyTriggerEventRefV1, trigger, stage="K6B_TRIGGER")
        ledger = _strict_model(DependentBuyLedgerObservationV2, ledger, stage="K6B_LEDGER")
        previous_mapping = _strict_model(
            ProductCommandChildMappingV1, previous_mapping, stage="K6B_MAPPING_PREDECESSOR"
        )
        successor_mapping = _strict_model(
            ProductCommandChildMappingV1, successor_mapping, stage="K6B_MAPPING_SUCCESSOR"
        )
        if outbox is not None:
            outbox = _strict_model(BrokerCommandOutboxV1, outbox, stage="K6B_OUTBOX")
        successor_coordination.validate_successor_v2(previous_coordination)
        if decision.coordination_id != previous_coordination.coordination_id:
            raise KernelRepositoryConflict("K6-B decision does not close to predecessor coordination")
        if decision.decision_sequence != previous_coordination.decision_sequence + 1:
            raise KernelRepositoryConflict("K6-B decision is not the exact next coordination sequence")
        if decision.previous_decision_sha256 != previous_coordination.last_decision_sha256:
            raise KernelRepositoryConflict("K6-B decision predecessor hash differs from durable coordination")
        if (decision.trigger_ref_sha256, decision.ledger_observation_sha256) != (
            trigger.trigger_ref_sha256,
            ledger.observation_sha256,
        ):
            raise KernelRepositoryConflict("K6-B decision evidence hashes differ from strict trigger or ledger")
        is_release = decision.decision.value == "RELEASE_TO_K2_OUTBOX"
        if is_release != (outbox is not None):
            raise KernelRepositoryConflict("K6-B outbox must exist exactly for a release decision")
        if successor_mapping.mapping_id != previous_mapping.mapping_id:
            raise KernelRepositoryConflict("K6-B successor mapping changes its physical identity")
        if is_release and (
            successor_mapping.mapping_status is not ProductCommandChildMappingStatusV1.RESERVED
            or successor_coordination.status is not DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
            or outbox is None
            or outbox.command_id != successor_coordination.release_command_id
            or outbox.mapping_id != successor_mapping.mapping_id
            or outbox.broker_called is not None
        ):
            raise KernelRepositoryConflict("K6-B release must atomically reserve mapping and create a pre-call outbox")
        if outbox is not None:
            outbox.validate_initial_v1()

        connection_context = nullcontext(None)
        with connection_context as conn:
            cursor_context = (
                conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if cursor is None else nullcontext(cursor)
            )
            with cursor_context as cur:
                cur.execute(
                    "SELECT carrier_json FROM qmt_strategy.execution_dependent_buy_coordination "
                    "WHERE coordination_id=%s FOR UPDATE",
                    (previous_coordination.coordination_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise KeyError(previous_coordination.coordination_id)
                durable_previous = validate_kernel_product_payload_v1(
                    DependentBuyCoordinationV2,
                    _row_json(row, "carrier_json"),
                    stage="K6B_DURABLE_PREDECESSOR",
                )
                if durable_previous != previous_coordination:
                    raise KernelRepositoryConflict("K6-B durable predecessor differs from trigger reader closure")
                durable_dependencies = self._read_locked_dependencies_v2(
                    cur,
                    coordination=durable_previous,
                    lock_clause=" FOR SHARE",
                )
                if durable_dependencies != previous_coordination.ordered_sell_dependencies:
                    raise KernelRepositoryConflict(
                        "K6-B predecessor coordination differs from durable dependency authority"
                    )
                self._verify_k6_worker_cursor(
                    cur,
                    decision.worker_id,
                    decision.process_incarnation_id,
                    decision.lease_epoch,
                )
                cur.execute(
                    "SELECT payload,sequence,runtime_id,event_id FROM qmt_strategy.execution_runtime_event "
                    "WHERE event_id=%s FOR SHARE",
                    (trigger.event_id,),
                )
                event_row = cur.fetchone()
                if event_row is None:
                    raise KernelRepositoryConflict("K6-B trigger event is not durable")
                durable_event = _model_from_json(RuntimeEventEnvelopeV2, _row_json(event_row, "payload"))
                if (
                    event_row["event_id"],
                    event_row["runtime_id"],
                    int(event_row["sequence"]),
                ) != (trigger.event_id, trigger.runtime_id, trigger.event_sequence):
                    raise KernelRepositoryConflict("K6-B trigger does not close to durable runtime event identity")
                if _event_trigger_type(durable_event) is not trigger.event_type:
                    raise KernelRepositoryConflict("K6-B trigger type differs from its durable K2 event")
                cur.execute(
                    "SELECT mapping_json FROM qmt_strategy.execution_child_order WHERE mapping_id=%s FOR UPDATE",
                    (previous_mapping.mapping_id,),
                )
                mapping_row = cur.fetchone()
                if mapping_row is None:
                    raise KernelRepositoryConflict("K6-B deferred mapping is absent")
                durable_mapping = validate_kernel_product_payload_v1(
                    ProductCommandChildMappingV1,
                    _row_json(mapping_row, "mapping_json"),
                    stage="K6B_DURABLE_MAPPING",
                )
                if durable_mapping != previous_mapping:
                    raise KernelRepositoryConflict("K6-B deferred mapping differs from trigger reader closure")
                projection = _decision_projection_v2(decision) | _decision_evidence_projection_v2(trigger, ledger)
                columns = (*projection, "trigger_ref_json", "ledger_observation_json", "carrier_json")
                cur.execute(
                    f"INSERT INTO qmt_strategy.execution_dependent_buy_decision({','.join(columns)}) "
                    f"VALUES ({','.join(['%s'] * len(columns))}) ON CONFLICT (decision_id) DO NOTHING",
                    (
                        *(
                            _json(value) if key == "ordered_dependency_sha256s" else value
                            for key, value in projection.items()
                        ),
                        _json(trigger.model_dump(mode="json")),
                        _json(ledger.model_dump(mode="json")),
                        _json(decision.model_dump(mode="json")),
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("K6-B decision identity already exists before coordination CAS")
                successor_projection = _coordination_projection_v2(successor_coordination)
                assignments = (
                    ",".join(f"{key}=%s" for key in successor_projection if key != "coordination_id")
                    + ",carrier_json=%s"
                )
                cur.execute(
                    f"UPDATE qmt_strategy.execution_dependent_buy_coordination SET {assignments} "
                    "WHERE coordination_id=%s AND row_version=%s",
                    (
                        *[value for key, value in successor_projection.items() if key != "coordination_id"],
                        _json(successor_coordination.model_dump(mode="json")),
                        previous_coordination.coordination_id,
                        previous_coordination.row_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("K6-B coordination CAS failed")
                for dependency in successor_coordination.ordered_sell_dependencies:
                    dependency_projection = _dependency_projection_v2(dependency)
                    cur.execute(
                        "UPDATE qmt_strategy.execution_dependent_buy_dependency SET "
                        "latest_order_fact_ref=%s,settled_trade_fact_refs=%s,settled_cash_ledger_refs=%s,"
                        "dependency_status=%s,carrier_json=%s,dependency_sha256=%s,"
                        "latest_order_fact_id=%s,latest_order_fact_sha256=%s,ordered_settled_proceeds_refs=%s "
                        "WHERE coordination_id=%s AND sell_parent_intent_id=%s AND dependency_sha256=%s",
                        (
                            dependency_projection["latest_order_fact_ref"],
                            _json(dependency_projection["settled_trade_fact_refs"]),
                            _json(dependency_projection["settled_cash_ledger_refs"]),
                            dependency_projection["dependency_status"],
                            _json(dependency.model_dump(mode="json")),
                            dependency_projection["dependency_sha256"],
                            dependency_projection["latest_order_fact_id"],
                            dependency_projection["latest_order_fact_sha256"],
                            _json(dependency_projection["ordered_settled_proceeds_refs"]),
                            previous_coordination.coordination_id,
                            dependency.sell_parent_intent_id,
                            next(
                                item.dependency_sha256
                                for item in previous_coordination.ordered_sell_dependencies
                                if item.sell_parent_intent_id == dependency.sell_parent_intent_id
                            ),
                        ),
                    )
                    if cur.rowcount != 1:
                        raise KernelRepositoryConflict("K6-B dependency CAS failed")
                mapping_projection = _mapping_scalar_projection(successor_mapping)
                mutable_mapping_keys = tuple(
                    key
                    for key in mapping_projection
                    if key
                    not in {
                        "mapping_id",
                        "child_order_id",
                        "runtime_id",
                        "algo_instance_id",
                        "parent_intent_id",
                        "strategy_slot_id",
                        "symbol",
                        "side",
                        "quantity",
                        "price",
                        "price_type",
                        "kernel_contract_version",
                    }
                )
                cur.execute(
                    "UPDATE qmt_strategy.execution_child_order SET "
                    + ",".join(f"{key}=%s" for key in mutable_mapping_keys)
                    + ",mapping_json=%s WHERE mapping_id=%s AND mapping_version=%s",
                    (
                        *(mapping_projection[key] for key in mutable_mapping_keys),
                        _json(successor_mapping.model_dump(mode="json")),
                        previous_mapping.mapping_id,
                        previous_mapping.mapping_version,
                    ),
                )
                if cur.rowcount != 1:
                    raise KernelRepositoryConflict("K6-B mapping CAS failed")
                if successor_mapping.mapping_status is ProductCommandChildMappingStatusV1.TERMINAL:
                    cur.execute(
                        "SELECT kernel_carrier_json FROM qmt_strategy.execution_algo_instance "
                        "WHERE runtime_id=%s AND algo_instance_id=%s FOR UPDATE",
                        (successor_mapping.runtime_id, successor_mapping.algo_instance_id),
                    )
                    algo_row = cur.fetchone()
                    if algo_row is None:
                        raise KernelRepositoryConflict("K6-B terminal mapping lacks its durable algo owner")
                    previous_algo = _model_from_json(
                        ExecutionAlgoInstancePersistenceV2,
                        _row_json(algo_row, "kernel_carrier_json"),
                    )
                    cur.execute(
                        "SELECT COUNT(*) AS active_child_count FROM qmt_strategy.execution_child_order "
                        "WHERE runtime_id=%s AND algo_instance_id=%s AND kernel_contract_version='KERNEL_V2' "
                        "AND mapping_status IN ('DEFERRED_DEPENDENT_BUY','RESERVED','DISPATCHING',"
                        "'BROKER_ACCEPTED','OUTCOME_UNKNOWN')",
                        (successor_mapping.runtime_id, successor_mapping.algo_instance_id),
                    )
                    active_count = int(cur.fetchone()["active_child_count"])
                    algo_payload = previous_algo.model_dump(mode="python")
                    algo_payload.update(
                        active_child_count=active_count,
                        row_version=previous_algo.row_version + 1,
                        updated_at_utc=max(previous_algo.updated_at_utc, successor_mapping.updated_at_utc),
                    )
                    successor_algo = ExecutionAlgoInstancePersistenceV2.model_validate(algo_payload)
                    self._cas_algo_with_cursor(
                        cur,
                        algo_instance=successor_algo,
                        expected_row_version=previous_algo.row_version,
                    )
                if outbox is not None:
                    cur.execute(
                        "INSERT INTO qmt_strategy.execution_algo_command_outbox("
                        "command_id,transition_id,ordinal,runtime_id,algo_instance_id,parent_intent_id,"
                        "mapping_id,command_type,local_vt_orderid,payload_json,payload_sha256,"
                        "status,attempt_count,lease_owner,lease_epoch,lease_fence_token,"
                        "lease_expires_at,dispatch_attempt_id,callback_watermark_before_call,"
                        "deterministic_client_order_ref,next_attempt_at_utc,broker_called,broker_order_id,"
                        "ack_receipt_json,ack_receipt_sha256,non_acceptance_receipt_json,"
                        "unknown_outcome_receipt_json,reconcile_receipt_json,last_error_json,row_version,"
                        "created_at_utc,updated_at_utc,closed_at_utc,carrier_json,outbox_row_sha256"
                        ") VALUES (" + ",".join(["%s"] * 35) + ")",
                        self._outbox_sql_values(outbox),
                    )


__all__ = ["KernelDependentBuyRepositoryMixin"]
