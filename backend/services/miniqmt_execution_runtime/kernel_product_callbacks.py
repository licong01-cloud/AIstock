"""Production callback ingress for the sole K6-D KERNEL_V2 route."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import json
import math
from typing import Any, Mapping, Protocol

from .kernel_callback_events import (
    build_kernel_order_event_payload_v1,
    build_kernel_trade_event_payload_v1,
    resolve_qmt_trade_identity_alias_v1,
)
from .kernel_product_runtime import MiniQMTKernelV2ProductCoordinator
from .kernel_repository_common import KernelRepositoryCommitUnknown, KernelRepositoryConflict
from .plugin_canonical import hash_hex_v1, thaw_json_v1
from .plugin_contracts import (
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExecutionCommandChildMappingV1,
    KernelCallbackMappingUpdateV1,
    RuntimeEventEnvelopeV2,
)


class KernelProductCallbackRepositoryV1(Protocol):
    def read_callback_identity_chain(self, *, runtime_id: str, broker_order_id: str) -> dict[str, Any]: ...

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int: ...

    def read_event_transaction(self, event_id: str) -> dict[str, Any]: ...


class KernelProductSnapshotGatewayV1(Protocol):
    def reconciliation_snapshot(self, *, runtime_id: str) -> Any: ...


class KernelProductCallbackIngressError(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = {**context, "broker_called": False}
        super().__init__(message)


class KernelProductCallbackIngressV1:
    """Convert real MiniQMT callback facts into atomic K2 callback ingress."""

    def __init__(
        self,
        *,
        repository: KernelProductCallbackRepositoryV1,
        coordinator: MiniQMTKernelV2ProductCoordinator,
    ) -> None:
        if not callable(getattr(repository, "read_callback_identity_chain", None)):
            raise TypeError("repository must expose read_callback_identity_chain")
        if not isinstance(coordinator, MiniQMTKernelV2ProductCoordinator):
            raise TypeError("coordinator must be MiniQMTKernelV2ProductCoordinator")
        self._repository = repository
        self._coordinator = coordinator

    def ingest_order_v1(
        self,
        *,
        runtime_id: str,
        broker_order_id: str,
        raw_payload: Mapping[str, Any],
        observed_at_utc: datetime,
    ) -> RuntimeEventEnvelopeV2:
        raw_payload = _canonical_source_payload_v1(raw_payload)
        chain = self._chain(runtime_id=runtime_id, broker_order_id=broker_order_id)
        mapping = chain["mapping"]
        submit = self._submit_command(chain)
        reference = chain["reference_outbox"].command_id
        callback_id = "mqorderevt_" + hash_hex_v1(
            "miniqmt_k6d_order_callback_identity_v1",
            {
                "runtime_id": runtime_id,
                "broker_order_id": broker_order_id,
                "source_payload": dict(raw_payload),
            },
        )

        def build(
            sequence: int, *, event_time_utc: Any = observed_at_utc
        ) -> tuple[RuntimeEventEnvelopeV2, KernelCallbackMappingUpdateV1]:
            payload = build_kernel_order_event_payload_v1(
                raw_payload=raw_payload,
                order_event_id=callback_id,
                runtime_id=runtime_id,
                algo_instance_id=mapping.algo_instance_id,
                parent_intent_id=mapping.parent_intent_id,
                strategy_slot_id=mapping.strategy_slot_id,
                mapping_id=mapping.mapping_id,
                command_id=reference,
                local_vt_orderid=mapping.local_vt_orderid,
                broker_order_id=broker_order_id,
                symbol=mapping.symbol,
                side=mapping.side,
                requested_quantity=mapping.requested_quantity,
            )
            event = RuntimeEventEnvelopeV2.create(
                runtime_id=runtime_id,
                sequence=sequence,
                event_type=EventTypeV2.ORDER,
                event_time_utc=event_time_utc,
                monotonic_ns=None,
                source=EventSourceV2.QMT_GATEWAY_CALLBACK,
                symbol=mapping.symbol,
                payload_schema_version="miniqmt_order_event_v1",
                payload=payload.model_dump(mode="json"),
                source_identity={"order_event_id": callback_id},
                correlation={
                    "algo_instance_id": mapping.algo_instance_id,
                    "mapping_id": mapping.mapping_id,
                    "reference_command_id": reference,
                },
            )
            successor = self._mapping_successor(
                mapping=mapping,
                submit=submit,
                event=event,
                broker_order_id=broker_order_id,
                status=(
                    CommandChildMappingStatusV1.TERMINAL
                    if payload.terminal
                    else CommandChildMappingStatusV1.BROKER_ACCEPTED
                ),
                order=True,
            )
            update = KernelCallbackMappingUpdateV1.create(
                mapping=successor,
                reference_command_id=reference,
                expected_mapping_version=mapping.mapping_version,
                expected_algo_row_version=chain["algo"].row_version,
            )
            return event, update

        return self._ingest_bounded(callback_id=callback_id, build=build)

    def ingest_trade_v1(
        self,
        *,
        runtime_id: str,
        broker_order_id: str,
        trade_quantity: int,
        trade_price_decimal: Any,
        cumulative_quantity: int,
        raw_payload: Mapping[str, Any],
        observed_at_utc: datetime,
    ) -> RuntimeEventEnvelopeV2:
        raw_payload = _canonical_source_payload_v1(raw_payload)
        if type(cumulative_quantity) is not int or cumulative_quantity <= 0:
            raise TypeError("cumulative_quantity must be a positive strict integer")
        chain = self._chain(runtime_id=runtime_id, broker_order_id=broker_order_id)
        mapping = chain["mapping"]
        if cumulative_quantity < trade_quantity or cumulative_quantity > mapping.requested_quantity:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_TRADE_CUMULATIVE_INVALID",
                "trade cumulative quantity differs from the locked child quantity",
                context={
                    "runtime_id": runtime_id,
                    "mapping_id": mapping.mapping_id,
                    "trade_quantity": trade_quantity,
                    "cumulative_quantity": cumulative_quantity,
                    "requested_quantity": mapping.requested_quantity,
                },
            )
        submit = self._submit_command(chain)
        reference = chain["reference_outbox"].command_id
        callback_id = "mqtradeevt_" + hash_hex_v1(
            "miniqmt_k6d_trade_callback_identity_v1",
            {
                "runtime_id": runtime_id,
                "broker_order_id": broker_order_id,
                "trade_quantity": trade_quantity,
                "trade_price_decimal": str(trade_price_decimal),
                "source_payload": dict(raw_payload),
            },
        )

        def build(
            sequence: int, *, event_time_utc: Any = observed_at_utc
        ) -> tuple[RuntimeEventEnvelopeV2, KernelCallbackMappingUpdateV1]:
            payload = build_kernel_trade_event_payload_v1(
                raw_payload=raw_payload,
                runtime_id=runtime_id,
                algo_instance_id=mapping.algo_instance_id,
                parent_intent_id=mapping.parent_intent_id,
                strategy_slot_id=mapping.strategy_slot_id,
                mapping_id=mapping.mapping_id,
                command_id=reference,
                local_vt_orderid=mapping.local_vt_orderid,
                broker_order_id=broker_order_id,
                symbol=mapping.symbol,
                side=mapping.side,
                trade_quantity=trade_quantity,
                trade_price_decimal=trade_price_decimal,
            )
            event = RuntimeEventEnvelopeV2.create(
                runtime_id=runtime_id,
                sequence=sequence,
                event_type=EventTypeV2.TRADE,
                event_time_utc=event_time_utc,
                monotonic_ns=None,
                source=EventSourceV2.QMT_GATEWAY_CALLBACK,
                symbol=mapping.symbol,
                payload_schema_version="miniqmt_trade_fact_v1",
                payload=payload.model_dump(mode="json"),
                source_identity={"trade_id": payload.trade_id},
                correlation={
                    "algo_instance_id": mapping.algo_instance_id,
                    "mapping_id": mapping.mapping_id,
                    "reference_command_id": reference,
                },
            )
            successor = self._mapping_successor(
                mapping=mapping,
                submit=submit,
                event=event,
                broker_order_id=broker_order_id,
                # TRADE facts advance economic state; the authoritative ORDER
                # or RECONCILE status owns child terminal closure.  Keeping the
                # mapping accepted here allows every distinct fill in one
                # broker snapshot to enter the durable event stream.  A late
                # distinct trade after terminal ORDER evidence preserves the
                # terminal child state while appending its economic lineage.
                status=(
                    CommandChildMappingStatusV1.TERMINAL
                    if mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL
                    else CommandChildMappingStatusV1.BROKER_ACCEPTED
                ),
                order=False,
            )
            update = KernelCallbackMappingUpdateV1.create(
                mapping=successor,
                reference_command_id=reference,
                expected_mapping_version=mapping.mapping_version,
                expected_algo_row_version=chain["algo"].row_version,
            )
            return event, update

        return self._ingest_bounded(callback_id=callback_id, build=build)

    def _ingest_bounded(self, *, callback_id: str, build: Any) -> RuntimeEventEnvelopeV2:
        probe, _ = build(1)
        try:
            existing = self._repository.read_event_transaction(probe.event_id)
        except KeyError:
            existing = None
        if existing is not None:
            return self._strict_existing(existing=existing, build=build)
        last_error: Exception | None = None
        for _ in range(3):
            event, update = build(self._repository.read_runtime_last_event_sequence(probe.runtime_id) + 1)
            try:
                self._coordinator.ingest_callback_event_v1(
                    event=event,
                    callback_mapping_update=update,
                )
                return event
            except (KernelRepositoryConflict, KernelRepositoryCommitUnknown) as exc:
                last_error = exc
                try:
                    existing = self._repository.read_event_transaction(event.event_id)
                except KeyError:
                    continue
                return self._strict_existing(existing=existing, build=build)
        raise KernelProductCallbackIngressError(
            "MINIQMT_K6_PRODUCT_CALLBACK_CONTENTION",
            "callback could not acquire its exact runtime sequence",
            context={"callback_id": callback_id, "error_type": type(last_error).__name__},
        ) from last_error

    def _strict_existing(self, *, existing: dict[str, Any], build: Any) -> RuntimeEventEnvelopeV2:
        event = existing.get("event")
        receipt = existing.get("receipt")
        if not isinstance(event, RuntimeEventEnvelopeV2) or receipt is None:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_CALLBACK_READBACK_INVALID",
                "callback transaction readback is incomplete",
                context={"event_type": type(event).__name__},
            )
        expected, _ = build(event.sequence, event_time_utc=event.event_time_utc)
        if event != expected:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_CALLBACK_IDEMPOTENCY_CONFLICT",
                "callback identity already exists with different facts",
                context={"event_id": event.event_id},
            )
        self._coordinator.process_committed_event_v1(event=event, receipt=receipt)
        return event

    def _chain(self, *, runtime_id: str, broker_order_id: str) -> dict[str, Any]:
        chain = self._repository.read_callback_identity_chain(
            runtime_id=runtime_id,
            broker_order_id=broker_order_id,
        )
        if not isinstance(chain.get("mapping"), ExecutionCommandChildMappingV1):
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_CALLBACK_CHAIN_INVALID",
                "callback durable mapping readback is malformed",
                context={"runtime_id": runtime_id, "broker_order_id": broker_order_id},
            )
        return chain

    @staticmethod
    def _submit_command(chain: dict[str, Any]) -> BrokerCommandV2:
        outbox = chain["submit_outbox"]
        command = BrokerCommandV2.model_validate_json(
            json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
        )
        if command.command_id != chain["mapping"].command_id:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_CALLBACK_SUBMIT_OWNER_INVALID",
                "callback mapping no longer closes to its original SUBMIT command",
                context={"mapping_id": chain["mapping"].mapping_id},
            )
        return command

    @staticmethod
    def _mapping_successor(
        *,
        mapping: ExecutionCommandChildMappingV1,
        submit: BrokerCommandV2,
        event: RuntimeEventEnvelopeV2,
        broker_order_id: str,
        status: CommandChildMappingStatusV1,
        order: bool,
    ) -> ExecutionCommandChildMappingV1:
        if type(broker_order_id) is not str or not broker_order_id or broker_order_id != broker_order_id.strip():
            raise TypeError("broker_order_id must be a canonical identity")
        if mapping.broker_order_id is not None and mapping.broker_order_id != broker_order_id:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_CALLBACK_BROKER_IDENTITY_CONFLICT",
                "callback broker identity differs from the durable child mapping",
                context={
                    "mapping_id": mapping.mapping_id,
                    "durable_broker_order_id": mapping.broker_order_id,
                    "callback_broker_order_id": broker_order_id,
                },
            )
        identity_source = mapping.broker_identity_source_event_id or event.event_id
        return ExecutionCommandChildMappingV1.create(
            command=submit,
            strategy_slot_id=mapping.strategy_slot_id,
            mapping_status=status,
            mapping_version=mapping.mapping_version + 1,
            broker_order_id=broker_order_id,
            broker_identity_source_event_id=identity_source,
            last_order_event_id=event.event_id if order else mapping.last_order_event_id,
            last_trade_event_id=mapping.last_trade_event_id if order else event.event_id,
            updated_by_event_id=event.event_id,
            created_at_utc=mapping.created_at_utc,
            updated_at_utc=event.event_time_utc,
        )


class KernelProductSnapshotIngressV1:
    """Poll the existing MiniQMT query authority and publish owned callback facts.

    The host QMT client does not expose a process callback registrar.  This
    adapter therefore uses its real order/trade snapshot APIs, filters rows by
    the durable K2 mapping/outbox identity chain, and feeds the exact same
    callback writer.  Unowned account rows are expected and ignored; malformed
    or conflicting rows for an owned broker identity fail loudly.
    """

    def __init__(
        self,
        *,
        repository: KernelProductCallbackRepositoryV1,
        gateway: KernelProductSnapshotGatewayV1,
        ingress: KernelProductCallbackIngressV1,
    ) -> None:
        if not callable(getattr(repository, "read_callback_identity_chain", None)):
            raise TypeError("repository must expose read_callback_identity_chain")
        if not callable(getattr(gateway, "reconciliation_snapshot", None)):
            raise TypeError("gateway must expose reconciliation_snapshot")
        if not isinstance(ingress, KernelProductCallbackIngressV1):
            raise TypeError("ingress must be KernelProductCallbackIngressV1")
        self._repository = repository
        self._gateway = gateway
        self._ingress = ingress

    def sync_v1(self, *, runtime_id: str, observed_at_utc: datetime) -> tuple[str, ...]:
        if type(runtime_id) is not str or not runtime_id or runtime_id != runtime_id.strip():
            raise TypeError("runtime_id must be a canonical identity")
        if not isinstance(observed_at_utc, datetime) or observed_at_utc.tzinfo is None:
            raise TypeError("observed_at_utc must be timezone-aware")
        snapshot = self._gateway.reconciliation_snapshot(runtime_id=runtime_id)
        orders = getattr(snapshot, "orders", None)
        trades = getattr(snapshot, "trades", None)
        if type(orders) is not tuple or type(trades) is not tuple:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_SCHEMA_INVALID",
                "gateway reconciliation snapshot must expose strict order/trade tuples",
                context={"runtime_id": runtime_id, "snapshot_type": type(snapshot).__name__},
            )
        owned_trades = self._owned_rows(runtime_id=runtime_id, rows=trades, kind="TRADE")
        owned_orders = self._owned_rows(runtime_id=runtime_id, rows=orders, kind="ORDER")
        event_ids: list[str] = []
        cumulative_by_order: dict[str, int] = {}
        seen_trade_ids: set[tuple[str, str]] = set()
        for broker_order_id, row in sorted(
            owned_trades,
            key=lambda item: (
                item[0],
                str(item[1].get("traded_time") or ""),
                str(resolve_qmt_trade_identity_alias_v1(item[1]) or ""),
            ),
        ):
            trade_id = resolve_qmt_trade_identity_alias_v1(row)
            if trade_id is None:
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_TRADE_IDENTITY_MISSING",
                    "owned MiniQMT trade snapshot lacks a stable broker trade identity",
                    context={"runtime_id": runtime_id, "broker_order_id": broker_order_id},
                )
            key = (broker_order_id, trade_id)
            if key in seen_trade_ids:
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_TRADE_IDENTITY_DUPLICATE",
                    "MiniQMT trade snapshot repeats one owned broker trade identity",
                    context={"runtime_id": runtime_id, "broker_order_id": broker_order_id, "trade_id": trade_id},
                )
            seen_trade_ids.add(key)
            quantity = self._positive_int(row, ("traded_volume", "quantity", "volume"), "trade quantity")
            price = self._positive_number(row, ("traded_price", "price", "avg_price"), "trade price")
            cumulative = cumulative_by_order.get(broker_order_id, 0) + quantity
            cumulative_by_order[broker_order_id] = cumulative
            event = self._ingress.ingest_trade_v1(
                runtime_id=runtime_id,
                broker_order_id=broker_order_id,
                trade_quantity=quantity,
                trade_price_decimal=price,
                cumulative_quantity=cumulative,
                raw_payload=row,
                observed_at_utc=observed_at_utc,
            )
            event_ids.append(event.event_id)
        seen_orders: set[str] = set()
        for broker_order_id, row in sorted(owned_orders, key=lambda item: item[0]):
            if broker_order_id in seen_orders:
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_ORDER_IDENTITY_DUPLICATE",
                    "MiniQMT order snapshot repeats one owned broker order identity",
                    context={"runtime_id": runtime_id, "broker_order_id": broker_order_id},
                )
            seen_orders.add(broker_order_id)
            event = self._ingress.ingest_order_v1(
                runtime_id=runtime_id,
                broker_order_id=broker_order_id,
                raw_payload=row,
                observed_at_utc=observed_at_utc,
            )
            event_ids.append(event.event_id)
        return tuple(event_ids)

    def _owned_rows(
        self,
        *,
        runtime_id: str,
        rows: tuple[Any, ...],
        kind: str,
    ) -> tuple[tuple[str, dict[str, Any]], ...]:
        owned: list[tuple[str, dict[str, Any]]] = []
        for index, raw in enumerate(rows):
            if type(raw) is not dict or any(type(key) is not str for key in raw):
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_SNAPSHOT_ROW_INVALID",
                    "MiniQMT snapshot row must be a strict string-keyed object",
                    context={"runtime_id": runtime_id, "kind": kind, "row_index": index},
                )
            normalized = _canonical_source_payload_v1(raw)
            broker_order_id = self._identity_alias(
                normalized,
                ("broker_order_id", "order_id", "qmt_order_id", "native_order_id"),
                field_name="broker order identity",
            )
            if broker_order_id is None:
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_SNAPSHOT_ORDER_IDENTITY_MISSING",
                    "MiniQMT snapshot row lacks a broker order identity",
                    context={"runtime_id": runtime_id, "kind": kind, "row_index": index},
                )
            try:
                self._repository.read_callback_identity_chain(
                    runtime_id=runtime_id,
                    broker_order_id=broker_order_id,
                )
            except KeyError:
                continue
            owned.append((broker_order_id, normalized))
        return tuple(owned)

    @staticmethod
    def _identity_alias(row: Mapping[str, Any], aliases: tuple[str, ...], *, field_name: str) -> str | None:
        present = [row[name] for name in aliases if name in row]
        if not present:
            return None
        first = present[0]
        if any(value != first or type(value) is not type(first) for value in present[1:]):
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_ALIAS_CONFLICT",
                f"MiniQMT snapshot {field_name} aliases conflict",
                context={"field_name": field_name, "aliases": list(aliases)},
            )
        if type(first) is not str or not first or first != first.strip():
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_IDENTITY_INVALID",
                f"MiniQMT snapshot {field_name} must be a canonical string",
                context={"field_name": field_name, "value_type": type(first).__name__},
            )
        return first

    @staticmethod
    def _positive_int(row: Mapping[str, Any], aliases: tuple[str, ...], field_name: str) -> int:
        value = KernelProductSnapshotIngressV1._one_value(row, aliases, field_name)
        if type(value) is not int or value <= 0:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_QUANTITY_INVALID",
                f"MiniQMT snapshot {field_name} must be a positive strict integer",
                context={"field_name": field_name, "value_type": type(value).__name__},
            )
        return value

    @staticmethod
    def _positive_number(row: Mapping[str, Any], aliases: tuple[str, ...], field_name: str) -> Any:
        value = KernelProductSnapshotIngressV1._one_value(row, aliases, field_name)
        if type(value) not in (int, float, str) or isinstance(value, bool):
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_PRICE_INVALID",
                f"MiniQMT snapshot {field_name} has an unsupported carrier",
                context={"field_name": field_name, "value_type": type(value).__name__},
            )
        return value

    @staticmethod
    def _one_value(row: Mapping[str, Any], aliases: tuple[str, ...], field_name: str) -> Any:
        present = [row[name] for name in aliases if name in row]
        if not present:
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_FIELD_MISSING",
                f"MiniQMT snapshot is missing {field_name}",
                context={"field_name": field_name, "aliases": list(aliases)},
            )
        first = present[0]
        if any(value != first or type(value) is not type(first) for value in present[1:]):
            raise KernelProductCallbackIngressError(
                "MINIQMT_K6_PRODUCT_SNAPSHOT_ALIAS_CONFLICT",
                f"MiniQMT snapshot {field_name} aliases conflict",
                context={"field_name": field_name, "aliases": list(aliases)},
            )
        return first


def _canonical_source_payload_v1(raw_payload: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize real QMT snapshot floats without weakening canonical JSON."""

    if not isinstance(raw_payload, Mapping) or any(type(key) is not str for key in raw_payload):
        raise TypeError("raw callback payload must be one strict string-keyed mapping")

    def normalize(value: Any, path: str) -> Any:
        if value is None or type(value) in (bool, int, str):
            return value
        if type(value) is float:
            if not math.isfinite(value):
                raise KernelProductCallbackIngressError(
                    "MINIQMT_K6_PRODUCT_SNAPSHOT_NONFINITE_NUMBER",
                    "MiniQMT callback payload contains a non-finite binary number",
                    context={"field_path": path},
                )
            return format(Decimal(str(value)), "f")
        if isinstance(value, Mapping):
            if any(type(key) is not str for key in value):
                raise TypeError(f"callback payload object at {path} has a non-string key")
            return {key: normalize(member, f"{path}.{key}") for key, member in value.items()}
        if type(value) is list:
            return [normalize(member, f"{path}[{index}]") for index, member in enumerate(value)]
        raise KernelProductCallbackIngressError(
            "MINIQMT_K6_PRODUCT_SNAPSHOT_VALUE_INVALID",
            "MiniQMT callback payload contains a non-JSON source carrier",
            context={"field_path": path, "value_type": type(value).__name__},
        )

    return {key: normalize(value, key) for key, value in raw_payload.items()}


__all__ = [
    "KernelProductCallbackIngressError",
    "KernelProductCallbackIngressV1",
    "KernelProductSnapshotIngressV1",
]
