"""Generic constructor-once/restore-without-constructor K4 facade adapter."""

from __future__ import annotations

import inspect
import json
import math
from enum import Enum
from typing import Any

from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_decimal_string_v1,
    hash_hex_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoInitializationV1,
    AlgoReadOnlyServicesV1,
    AlgoStartContextV1,
    AlgoStateSnapshotV2,
    AlgoTransitionV1,
    ActiveChildClosureStatusV1,
    EventTypeV2,
    ExecutionAlgoPluginManifestV2,
    NormalizedOrderStatusV1,
    RuntimeEventEnvelopeV2,
    SideV1,
    TerminalOutcomeV1,
    execution_child_order_id_v1,
)

from .facade import VnpyAlgoEngineFacadeV1, VnpyFacadeEffectCollectorV1
from .facade_contracts import (
    VnpyFacadeActiveOrderV1,
    VnpyFacadeAlgorithmBindingV1,
    VnpyFacadeCommandAuthorityDispositionV1,
    VnpyFacadeContractError,
    VnpyFacadeContractViewV1,
    VnpyFacadeFieldRoleV1,
    VnpyFacadeInitializationInputV1,
    VnpyFacadeRuntimeBindingDispositionV1,
    VnpyFacadeStateEnvelopeV1,
    VnpyFacadeStateFieldMappingV1,
    VnpyFacadeStateValueV1,
    VnpyFacadeTerminalMappingV1,
    VnpyFacadeTransitionInputV1,
)
from .facade_projection import (
    AlgoStatus,
    Direction,
    Offset,
    OrderData,
    Status,
    TradeData,
    project_order_status_v1,
)


def _binding_error(message: str, **context: Any) -> VnpyFacadeContractError:
    return VnpyFacadeContractError(
        "MINIQMT_VNPY_FACADE_BINDING_INVALID",
        message,
        context=context,
    )


def state_mapping_set_sha256_v1(
    mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
) -> str:
    ordered = tuple(
        sorted(
            mappings,
            key=lambda item: (
                item.algo_code,
                item.field_role.value,
                item.attribute_name,
                item.state_path,
            ),
        )
    )
    keys = tuple((item.algo_code, item.field_role.value, item.attribute_name, item.state_path) for item in ordered)
    if mappings != ordered or len(keys) != len(set(keys)):
        raise ValueError("state mappings must be unique and canonically sorted")
    return hash_hex_v1(
        "miniqmt_vnpy_facade_state_mapping_set_v1",
        [item.canonical_payload_v1() for item in ordered],
    )


def terminal_mapping_set_sha256_v1(
    mappings: tuple[VnpyFacadeTerminalMappingV1, ...],
) -> str:
    ordered = tuple(
        sorted(
            mappings,
            key=lambda item: (
                item.algo_code,
                item.algo_status_member,
                item.trigger_event_type,
                item.traded_relation,
                item.required_active_child_closure,
            ),
        )
    )
    keys = tuple(
        (
            item.algo_code,
            item.algo_status_member,
            item.trigger_event_type,
            item.traded_relation,
            item.required_active_child_closure,
        )
        for item in ordered
    )
    if mappings != ordered or len(keys) != len(set(keys)):
        raise ValueError("terminal mappings must be unique and canonically sorted")
    return hash_hex_v1(
        "miniqmt_vnpy_facade_terminal_mapping_set_v1",
        [item.canonical_payload_v1() for item in ordered],
    )


class VnpyFacadeBackedPluginAdapterV1:
    """Sealed process binding for one exact pinned algorithm class."""

    __slots__ = (
        "_algorithm_binding",
        "_algorithm_class",
        "_state_mappings",
        "_terminal_mappings",
        "_sealed",
        "manifest",
    )

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("VnpyFacadeBackedPluginAdapterV1 is immutable")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        *,
        manifest: ExecutionAlgoPluginManifestV2,
        algorithm_class: type[Any],
        algorithm_binding: VnpyFacadeAlgorithmBindingV1,
        state_mappings: tuple[VnpyFacadeStateFieldMappingV1, ...],
        terminal_mappings: tuple[VnpyFacadeTerminalMappingV1, ...],
    ) -> None:
        if not inspect.isclass(algorithm_class):
            raise TypeError("algorithm_class must be a class")
        if not isinstance(manifest, ExecutionAlgoPluginManifestV2):
            raise TypeError("manifest must be ExecutionAlgoPluginManifestV2")
        if not isinstance(algorithm_binding, VnpyFacadeAlgorithmBindingV1):
            raise TypeError("algorithm_binding must be VnpyFacadeAlgorithmBindingV1")
        if algorithm_binding.class_ref != f"{algorithm_class.__module__}:{algorithm_class.__qualname__}":
            raise _binding_error(
                "algorithm class identity conflicts with binding",
                expected=algorithm_binding.class_ref,
                actual=f"{algorithm_class.__module__}:{algorithm_class.__qualname__}",
            )
        if algorithm_binding.algo_code != manifest.algo_code:
            raise _binding_error(
                "algorithm binding algo identity conflicts with manifest",
                expected=manifest.algo_code,
                actual=algorithm_binding.algo_code,
            )
        if state_mapping_set_sha256_v1(state_mappings) != algorithm_binding.state_mapping_set_sha256:
            raise _binding_error("state mapping set conflicts with algorithm binding")
        if terminal_mapping_set_sha256_v1(terminal_mappings) != algorithm_binding.terminal_mapping_set_sha256:
            raise _binding_error("terminal mapping set conflicts with algorithm binding")
        self.manifest = manifest
        self._algorithm_class = algorithm_class
        self._algorithm_binding = algorithm_binding
        self._state_mappings = state_mappings
        self._terminal_mappings = terminal_mappings
        self._sealed = True

    def initialize(self, context: AlgoStartContextV1) -> AlgoInitializationV1:
        raise _binding_error(
            "facade-backed adapter must use initialize_with_facade",
            algo_instance_id=getattr(context, "algo_instance_id", None),
        )

    def restore_state(self, snapshot: AlgoStateSnapshotV2) -> AlgoStateSnapshotV2:
        raise _binding_error(
            "facade-backed adapter state restoration requires an exact transition input",
            algo_instance_id=getattr(snapshot, "algo_instance_id", None),
        )

    def transition(
        self,
        *,
        state: AlgoStateSnapshotV2,
        event: RuntimeEventEnvelopeV2,
        services: AlgoReadOnlyServicesV1,
    ) -> AlgoTransitionV1:
        raise _binding_error(
            "facade-backed adapter must use transition_with_facade",
            algo_instance_id=getattr(state, "algo_instance_id", None),
            event_id=getattr(event, "event_id", None),
            services_sha256=getattr(services, "services_sha256", None),
        )

    def initialize_with_facade(
        self,
        invocation_input: VnpyFacadeInitializationInputV1,
    ) -> AlgoInitializationV1:
        self._validate_invocation_receipt_v1(invocation_input)
        context = invocation_input.start_context
        collector = VnpyFacadeEffectCollectorV1.create(
            context.deterministic_context,
            context.parent_intent_id,
            invocation_input.transition_id,
        )
        facade = VnpyAlgoEngineFacadeV1.create(invocation_input, collector)
        config = thaw_json_v1(context.plugin_config)
        try:
            algorithm = self._algorithm_class(
                facade,
                context.algo_instance_id,
                context.symbol.replace(".SH", ".SSE").replace(".SZ", ".SZSE").replace(".BJ", ".BSE"),
                Direction.LONG if context.side is SideV1.BUY else Direction.SHORT,
                Offset.NONE,
                float(context.limit_price_decimal),
                float(context.parent_quantity),
                config,
            )
            algorithm.start()
        except VnpyFacadeContractError:
            raise
        except Exception as exc:
            raise _binding_error(
                "pinned algorithm initialization failed",
                algo_instance_id=context.algo_instance_id,
                algorithm_class=f"{self._algorithm_class.__module__}:{self._algorithm_class.__qualname__}",
                error_type=f"{type(exc).__module__}.{type(exc).__qualname__}",
            ) from exc
        envelope = self.extract_state_v1(
            algorithm=algorithm,
            invocation_input=invocation_input,
            collector=collector,
            before_envelope=None,
        )
        next_state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=context.deterministic_context,
            transition_sequence=1,
            last_applied_delivery_sequence=1,
            last_applied_delivery_id=context.start_delivery_id,
            last_closed_delivery_sequence=1,
            state=envelope.canonical_payload_v1(),
            last_applied_event_id=context.start_event_id,
        )
        return collector.freeze_initialization(next_state)

    def transition_with_facade(
        self,
        invocation_input: VnpyFacadeTransitionInputV1,
    ) -> AlgoTransitionV1:
        self._validate_invocation_receipt_v1(invocation_input)
        before = VnpyFacadeStateEnvelopeV1.model_validate_json(
            json.dumps(
                thaw_json_v1(invocation_input.before_state.state),
                sort_keys=True,
                separators=(",", ":"),
            ),
            strict=True,
        )
        collector = VnpyFacadeEffectCollectorV1.create(
            invocation_input.deterministic_context,
            invocation_input.algo_instance.parent_intent_id,
            invocation_input.delivery.transition_id,
        )
        try:
            facade = VnpyAlgoEngineFacadeV1.create(invocation_input, collector)
            algorithm = self.restore_algorithm_v1(before, facade=facade)
            self._invoke_callback_once_v1(
                algorithm=algorithm,
                event=invocation_input.runtime_event,
                facade=facade,
                before_envelope=before,
            )
        except VnpyFacadeContractError:
            raise
        except Exception as exc:
            raise _binding_error(
                "pinned algorithm restore or callback failed",
                algo_instance_id=invocation_input.algo_instance.algo_instance_id,
                event_id=invocation_input.runtime_event.event_id,
                event_type=invocation_input.runtime_event.event_type.value,
                error_type=f"{type(exc).__module__}.{type(exc).__qualname__}",
            ) from exc
        if getattr(algorithm, "algo_engine", None) is not facade:
            raise _binding_error(
                "callback replaced its transition-local facade owner",
                algo_instance_id=invocation_input.algo_instance.algo_instance_id,
                event_id=invocation_input.runtime_event.event_id,
            )
        after = self.extract_state_v1(
            algorithm=algorithm,
            invocation_input=invocation_input,
            collector=collector,
            before_envelope=before,
        )
        sequence = invocation_input.delivery.algo_delivery_sequence
        next_state = AlgoStateSnapshotV2.create(
            plugin_manifest=self.manifest,
            deterministic_context=invocation_input.deterministic_context,
            transition_sequence=sequence,
            last_applied_delivery_sequence=sequence,
            last_applied_delivery_id=invocation_input.delivery.delivery_id,
            last_closed_delivery_sequence=sequence,
            state=after.canonical_payload_v1(),
            last_applied_event_id=invocation_input.runtime_event.event_id,
        )
        return collector.freeze(
            next_state,
            self._terminal_outcome_v1(
                after,
                invocation_input.runtime_event,
                invocation_input.algo_instance.active_child_closure_status,
            ),
        )

    def restore_algorithm_v1(
        self,
        envelope: VnpyFacadeStateEnvelopeV1,
        *,
        facade: VnpyAlgoEngineFacadeV1,
    ) -> Any:
        if envelope.algorithm_binding_sha256 != self._algorithm_binding.binding_sha256:
            raise _binding_error("state envelope algorithm binding drifted")
        algorithm = self._algorithm_class.__new__(self._algorithm_class)
        object.__setattr__(algorithm, "algo_engine", facade)
        enum_values: dict[str, Any] = {
            "direction": Direction[envelope.direction_member],
            "offset": Offset[envelope.offset_member],
            "status": AlgoStatus[envelope.status_member],
        }
        base_values = {
            "algo_name": envelope.algo_name,
            "vt_symbol": envelope.symbol.replace(".SH", ".SSE").replace(".SZ", ".SZSE").replace(".BJ", ".BSE"),
            "price": float(envelope.limit_price_decimal),
            "volume": float(envelope.target_volume_decimal),
            "traded": float(envelope.traded_volume_decimal),
            "traded_price": float(envelope.traded_price_decimal),
            "active_orders": {
                item.local_vt_orderid: OrderData(
                    vt_orderid=item.local_vt_orderid,
                    status=(Status.PARTTRADED if item.cumulative_quantity > 0 else Status.NOTTRADED),
                    traded=float(item.cumulative_quantity),
                    price=float(item.price_decimal),
                )
                for item in envelope.ordered_active_orders
                if item.status not in {"COMMAND_PENDING", "OUTCOME_UNKNOWN"}
            },
            **enum_values,
        }
        values = {
            **base_values,
            **{item.name: self._decode_state_value_v1(item) for item in envelope.ordered_parameters},
            **{item.name: self._decode_state_value_v1(item) for item in envelope.ordered_variables},
        }
        for mapping in self._state_mappings:
            if mapping.attribute_name == "algo_engine":
                continue
            if mapping.attribute_name not in values:
                raise _binding_error(
                    "durable state is missing mapped attribute",
                    attribute_name=mapping.attribute_name,
                )
            object.__setattr__(algorithm, mapping.attribute_name, values[mapping.attribute_name])
        return algorithm

    def extract_state_v1(
        self,
        *,
        algorithm: Any,
        invocation_input: VnpyFacadeInitializationInputV1 | VnpyFacadeTransitionInputV1,
        collector: VnpyFacadeEffectCollectorV1,
        before_envelope: VnpyFacadeStateEnvelopeV1 | None,
    ) -> VnpyFacadeStateEnvelopeV1:
        mapped = {item.attribute_name for item in self._state_mappings} | {"algo_engine"}
        actual = set(vars(algorithm))
        if actual != mapped:
            raise _binding_error(
                "algorithm instance attributes drifted from exact state mapping",
                missing=sorted(mapped - actual),
                extra=sorted(actual - mapped),
            )
        if isinstance(invocation_input, VnpyFacadeInitializationInputV1):
            context = invocation_input.start_context
            deterministic = context.deterministic_context
            contract_payload = thaw_json_v1(context.contract_projection)
            volume_increment = str(context.volume_increment)
            contract_hash = context.contract_projection_sha256
        else:
            context = invocation_input.algo_instance
            deterministic = invocation_input.deterministic_context
            contract_payload = (
                {}
                if invocation_input.read_only_services.contract_projection is None
                else thaw_json_v1(invocation_input.read_only_services.contract_projection)
            )
            volume_increment = contract_payload.get("volume_increment", 1)
            contract_hash = invocation_input.read_only_services.contract_projection_sha256
        gateway = invocation_input.authority_input.gateway_capability_catalog
        route = invocation_input.authority_input.route_compatibility_receipt
        contract_view = VnpyFacadeContractViewV1.create(
            runtime_id=deterministic.runtime_id,
            algo_instance_id=deterministic.algo_instance_id,
            symbol=context.symbol,
            exchange_member={"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}[context.symbol[-2:]],
            gateway_name=contract_payload["gateway_name"],
            min_volume=contract_payload["min_volume"],
            volume_increment=volume_increment,
            pricetick_decimal=contract_payload["pricetick_decimal"],
            contract_projection_sha256=contract_hash,
            gateway_catalog_sha256=gateway.catalog_sha256,
            route_receipt_sha256=route.receipt_sha256,
        )
        parameters = self._state_values_v1(algorithm, VnpyFacadeFieldRoleV1.PARAMETER)
        variables = self._state_values_v1(algorithm, VnpyFacadeFieldRoleV1.VARIABLE)
        active_orders = self._active_orders_v1(
            algorithm=algorithm,
            invocation_input=invocation_input,
            collector=collector,
            before_envelope=before_envelope,
        )
        return VnpyFacadeStateEnvelopeV1.create(
            runtime_id=deterministic.runtime_id,
            algo_instance_id=deterministic.algo_instance_id,
            plugin_id=self.manifest.plugin_id,
            plugin_version=self.manifest.plugin_version,
            plugin_manifest_sha256=self.manifest.manifest_sha256,
            algorithm_binding_sha256=self._algorithm_binding.binding_sha256,
            algo_name=algorithm.algo_name,
            symbol=context.symbol,
            direction_member=algorithm.direction.name,
            offset_member=algorithm.offset.name,
            limit_price_decimal=str(algorithm.price),
            target_volume_decimal=str(algorithm.volume),
            status_member=algorithm.status.name,
            traded_volume_decimal=str(algorithm.traded),
            traded_price_decimal=str(algorithm.traded_price),
            contract_view=contract_view,
            ordered_active_orders=active_orders,
            ordered_parameters=parameters,
            ordered_variables=variables,
            state_mapping_set_sha256=self._algorithm_binding.state_mapping_set_sha256,
        )

    def _state_values_v1(
        self,
        algorithm: Any,
        role: VnpyFacadeFieldRoleV1,
    ) -> tuple[VnpyFacadeStateValueV1, ...]:
        entries: list[VnpyFacadeStateValueV1] = []
        for mapping in self._state_mappings:
            if mapping.field_role is not role:
                continue
            value = getattr(algorithm, mapping.attribute_name)
            entries.append(
                VnpyFacadeStateValueV1.create(
                    name=mapping.attribute_name,
                    value=self._strict_state_value_v1(value),
                    value_type=mapping.value_type,
                )
            )
        return tuple(sorted(entries, key=lambda item: item.name))

    @staticmethod
    def _strict_state_value_v1(value: Any) -> Any:
        if isinstance(value, Enum):
            return {"enum_owner": type(value).__name__, "member": value.name, "pinned_value": value.value}
        if type(value) in (str, int, bool) or value is None:
            return value
        if type(value) is float:
            if not math.isfinite(value):
                raise _binding_error("algorithm state contains non-finite float")
            return canonical_decimal_string_v1(str(value), field_name="state_value", allow_zero=True)
        raise _binding_error(
            "algorithm state contains unsupported mutable or callable value",
            value_type=type(value).__name__,
        )

    @staticmethod
    def _decode_state_value_v1(item: VnpyFacadeStateValueV1) -> Any:
        value = thaw_json_v1(item.value)
        if isinstance(value, dict) and set(value) == {
            "enum_owner",
            "member",
            "pinned_value",
        }:
            if value["enum_owner"] == "Status":
                return Status[value["member"]]
            raise _binding_error(
                "durable state enum owner is unsupported",
                enum_owner=value["enum_owner"],
            )
        if "float" in item.value_type:
            return float(value)
        if "int" in item.value_type:
            if type(value) is not int:
                raise _binding_error(
                    "durable integer state value has the wrong carrier type",
                    field=item.name,
                    actual_type=type(value).__name__,
                )
            return value
        return value

    def _active_orders_v1(
        self,
        *,
        algorithm: Any,
        invocation_input: VnpyFacadeInitializationInputV1 | VnpyFacadeTransitionInputV1,
        collector: VnpyFacadeEffectCollectorV1,
        before_envelope: VnpyFacadeStateEnvelopeV1 | None,
    ) -> tuple[VnpyFacadeActiveOrderV1, ...]:
        def replace_order(
            item: VnpyFacadeActiveOrderV1,
            **updates: Any,
        ) -> VnpyFacadeActiveOrderV1:
            return VnpyFacadeActiveOrderV1.create(
                **{
                    **item.canonical_payload_v1(exclude={"schema_version", "active_order_sha256"}),
                    **updates,
                }
            )

        by_local = {
            item.local_vt_orderid: item
            for item in (() if before_envelope is None else before_envelope.ordered_active_orders)
        }
        for command in collector.broker_commands:
            if command.command_type.value == "CANCEL_ORDER":
                existing = by_local.get(command.local_vt_orderid)
                if (
                    existing is None
                    or existing.broker_order_id != command.owned_broker_order_id
                    or existing.command_id == command.command_id
                ):
                    raise _binding_error(
                        "cancel command conflicts with durable active-order identity",
                        local_vt_orderid=command.local_vt_orderid,
                        command_id=command.command_id,
                    )
                by_local[command.local_vt_orderid] = replace_order(existing, status="CANCEL_PENDING")
                continue
            if command.command_type.value != "SUBMIT_LIMIT":
                raise _binding_error(
                    "collector contains an unsupported broker command type",
                    command_type=command.command_type.value,
                    command_id=command.command_id,
                )
            by_local[command.local_vt_orderid] = VnpyFacadeActiveOrderV1.create(
                local_vt_orderid=command.local_vt_orderid,
                broker_order_id=None,
                command_id=command.command_id,
                child_order_id=execution_child_order_id_v1(
                    command_id=command.command_id, local_vt_orderid=command.local_vt_orderid
                ),
                symbol=command.symbol,
                side=command.side.value,
                price_decimal=command.price_decimal,
                requested_quantity=command.quantity,
                cumulative_quantity=0,
                remaining_quantity=command.quantity,
                status="COMMAND_PENDING",
                last_order_event_id=None,
                last_trade_event_id=None,
            )
        source_orders = getattr(algorithm, "active_orders", {})
        if type(source_orders) is not dict or any(
            type(local_id) is not str or not isinstance(order, OrderData) or order.vt_orderid != local_id
            for local_id, order in source_orders.items()
        ):
            raise _binding_error("algorithm active_orders carrier is invalid")
        source_local_ids = set(source_orders)
        if not source_local_ids.issubset(by_local):
            raise _binding_error(
                "algorithm active order has no exact durable/command identity",
                missing_local_vt_orderids=sorted(source_local_ids - set(by_local)),
            )
        new_submit_ids = {
            item.local_vt_orderid for item in collector.broker_commands if item.command_type.value == "SUBMIT_LIMIT"
        }
        current_mapping_ids: set[str] = set()
        callback_event_type: EventTypeV2 | None = None
        if isinstance(invocation_input, VnpyFacadeTransitionInputV1):
            current_mapping_ids = {item.local_vt_orderid for item in invocation_input.ordered_active_mappings}
        retained_ids = current_mapping_ids | new_submit_ids
        if isinstance(invocation_input, VnpyFacadeTransitionInputV1):
            event = invocation_input.runtime_event
            callback_event_type = event.event_type
            if event.event_type is EventTypeV2.ORDER:
                payload = thaw_json_v1(event.payload)
                target = payload.get("local_vt_orderid")
                existing = by_local.get(target)
                if existing is None:
                    raise _binding_error(
                        "ORDER callback target has no durable active-order identity",
                        local_vt_orderid=target,
                    )
                observed = payload["observed_cumulative_filled_quantity"]
                remaining = payload["observed_remaining_quantity"]
                if observed is None:
                    observed = existing.cumulative_quantity
                    remaining = existing.remaining_quantity
                if observed < existing.cumulative_quantity:
                    raise _binding_error(
                        "ORDER callback cumulative quantity regressed",
                        local_vt_orderid=target,
                        previous=existing.cumulative_quantity,
                        observed=observed,
                    )
                if observed + remaining != existing.requested_quantity:
                    raise _binding_error(
                        "ORDER callback quantity closure conflicts with durable request",
                        local_vt_orderid=target,
                        requested=existing.requested_quantity,
                        observed=observed,
                        remaining=remaining,
                    )
                if existing.broker_order_id not in (None, payload["broker_order_id"]):
                    raise _binding_error(
                        "ORDER callback broker identity conflicts with durable active order",
                        local_vt_orderid=target,
                        expected=existing.broker_order_id,
                        actual=payload["broker_order_id"],
                    )
                by_local[target] = replace_order(
                    existing,
                    broker_order_id=payload["broker_order_id"],
                    cumulative_quantity=observed,
                    remaining_quantity=remaining,
                    status=payload["normalized_order_status"],
                    last_order_event_id=event.event_id,
                )
                if target in retained_ids and payload.get("terminal") is True:
                    retained_ids.remove(target)
            elif event.event_type is EventTypeV2.TRADE:
                payload = thaw_json_v1(event.payload)
                target = payload["local_vt_orderid"]
                existing = by_local.get(target)
                if existing is None:
                    raise _binding_error(
                        "TRADE callback target has no durable active-order identity",
                        local_vt_orderid=target,
                    )
                if existing.broker_order_id not in (None, payload["broker_order_id"]):
                    raise _binding_error(
                        "TRADE callback broker identity conflicts with durable active order",
                        local_vt_orderid=target,
                        expected=existing.broker_order_id,
                        actual=payload["broker_order_id"],
                    )
                cumulative = existing.cumulative_quantity + payload["trade_quantity"]
                if cumulative > existing.requested_quantity:
                    raise _binding_error(
                        "TRADE callback overfills durable requested quantity",
                        local_vt_orderid=target,
                        requested=existing.requested_quantity,
                        cumulative=cumulative,
                    )
                remaining = existing.requested_quantity - cumulative
                by_local[target] = replace_order(
                    existing,
                    broker_order_id=payload["broker_order_id"],
                    cumulative_quantity=cumulative,
                    remaining_quantity=remaining,
                    status="FILLED" if remaining == 0 else "PARTIALLY_FILLED",
                    last_trade_event_id=event.event_id,
                )
        retained_ids |= source_local_ids
        for local_id in source_local_ids:
            source_order = source_orders[local_id]
            durable = by_local[local_id]
            if canonical_decimal_string_v1(
                str(source_order.price), field_name="active_order.price", allow_zero=False
            ) != durable.price_decimal or (
                callback_event_type is EventTypeV2.ORDER
                and (not source_order.traded.is_integer() or int(source_order.traded) != durable.cumulative_quantity)
            ):
                raise _binding_error(
                    "post-callback algorithm active order conflicts with durable callback facts",
                    local_vt_orderid=local_id,
                )
        missing = retained_ids - set(by_local)
        if missing:
            raise _binding_error(
                "active mapping has no exact before-state or command identity",
                missing_local_vt_orderids=sorted(missing),
            )
        return tuple(by_local[key] for key in sorted(retained_ids))

    def _invoke_callback_once_v1(
        self,
        *,
        algorithm: Any,
        event: RuntimeEventEnvelopeV2,
        facade: VnpyAlgoEngineFacadeV1,
        before_envelope: VnpyFacadeStateEnvelopeV1,
    ) -> None:
        payload = thaw_json_v1(event.payload)
        if event.event_type is EventTypeV2.TICK:
            tick = facade.get_tick(algorithm)
            if tick is not None:
                algorithm.update_tick(tick)
        elif event.event_type is EventTypeV2.TIMER:
            algorithm.update_timer()
        elif event.event_type is EventTypeV2.ORDER:
            normalized_status = NormalizedOrderStatusV1(payload["normalized_order_status"])
            observed = payload["observed_cumulative_filled_quantity"]
            if observed is None:
                previous = next(
                    (
                        item
                        for item in before_envelope.ordered_active_orders
                        if item.local_vt_orderid == payload["local_vt_orderid"]
                    ),
                    None,
                )
                if previous is None:
                    raise _binding_error(
                        "ORDER callback lacks cumulative quantity authority",
                        local_vt_orderid=payload["local_vt_orderid"],
                    )
                observed = previous.cumulative_quantity
            algorithm.update_order(
                OrderData(
                    vt_orderid=payload["local_vt_orderid"],
                    status=project_order_status_v1(normalized_status),
                    traded=float(observed),
                    price=float(
                        next(
                            item.price_decimal
                            for item in before_envelope.ordered_active_orders
                            if item.local_vt_orderid == payload["local_vt_orderid"]
                        )
                    ),
                )
            )
        elif event.event_type is EventTypeV2.TRADE:
            from datetime import datetime

            trade_datetime = datetime.fromisoformat(event.event_time_utc.replace("Z", "+00:00"))
            algorithm.update_trade(
                TradeData(
                    vt_orderid=payload["local_vt_orderid"],
                    vt_tradeid=payload["trade_id"],
                    price=float(payload["trade_price_decimal"]),
                    volume=float(payload["trade_quantity"]),
                    datetime=trade_datetime,
                )
            )
        else:
            raise _binding_error(
                "event type is not mapped to a pinned callback",
                event_type=event.event_type.value,
            )

    def _terminal_outcome_v1(
        self,
        envelope: VnpyFacadeStateEnvelopeV1,
        event: RuntimeEventEnvelopeV2,
        active_child_closure_status: ActiveChildClosureStatusV1,
    ) -> TerminalOutcomeV1 | None:
        if not isinstance(active_child_closure_status, ActiveChildClosureStatusV1):
            raise TypeError("active_child_closure_status must be ActiveChildClosureStatusV1")
        if envelope.ordered_active_orders:
            return None
        traded = float(envelope.traded_volume_decimal)
        target = float(envelope.target_volume_decimal)
        for mapping in self._terminal_mappings:
            if mapping.algo_code != self._algorithm_binding.algo_code:
                continue
            if mapping.algo_status_member != envelope.status_member:
                continue
            if mapping.trigger_event_type not in {"ANY", event.event_type.value}:
                continue
            if mapping.required_active_child_closure != active_child_closure_status.value:
                continue
            relation = "FULL" if traded >= target else "RESIDUAL"
            if mapping.traded_relation not in {"ANY", relation}:
                continue
            return (
                None
                if mapping.terminal_outcome_or_none is None
                else TerminalOutcomeV1(mapping.terminal_outcome_or_none)
            )
        return None

    def _validate_invocation_receipt_v1(
        self,
        invocation_input: VnpyFacadeInitializationInputV1 | VnpyFacadeTransitionInputV1,
    ) -> None:
        receipt = invocation_input.authority_input.facade_conformance_receipt
        if (
            receipt.runtime_binding_disposition is not VnpyFacadeRuntimeBindingDispositionV1.FACADE_BACKED_ADAPTER
            or receipt.command_authority_disposition is not VnpyFacadeCommandAuthorityDispositionV1.SHADOW_ONLY_K2_V1
        ):
            raise _binding_error(
                "runtime adapter invocation requires exact shadow adapter conformance",
                runtime_binding_disposition=receipt.runtime_binding_disposition.value,
                command_authority_disposition=receipt.command_authority_disposition.value,
            )
        if (
            receipt.algo_code != self.manifest.algo_code
            or receipt.manifest_sha256 != self.manifest.manifest_sha256
            or receipt.algorithm_binding_sha256 != self._algorithm_binding.binding_sha256
            or receipt.algorithm_characterization_receipt_sha256
            != self._algorithm_binding.characterization_receipt_sha256
            or receipt.state_mapping_set_sha256
            != invocation_input.authority_input.facade_conformance_set.state_mapping_set_sha256
            or receipt.terminal_mapping_set_sha256
            != invocation_input.authority_input.facade_conformance_set.terminal_mapping_set_sha256
        ):
            raise _binding_error(
                "runtime adapter invocation authority conflicts with sealed adapter binding",
                algo_code=self.manifest.algo_code,
                manifest_sha256=self.manifest.manifest_sha256,
                algorithm_binding_sha256=self._algorithm_binding.binding_sha256,
                conformance_receipt_sha256=receipt.receipt_sha256,
            )


__all__ = [
    "VnpyFacadeBackedPluginAdapterV1",
    "state_mapping_set_sha256_v1",
    "terminal_mapping_set_sha256_v1",
]
