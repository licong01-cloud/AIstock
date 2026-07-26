"""Deterministic K2-B event routing for the shadow execution kernel."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, Sequence

from .plugin_canonical import json_safe_evidence_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    DeliveryStatusV1,
    EventTypeV2,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    KernelCallbackMappingUpdateV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
)
from .plugin_registry import PluginCatalogRuntimeV2, PluginCatalogSnapshotV1, PluginKeyV1


class KernelEventRoutingError(RuntimeError):
    """Typed fail-loud routing failure with bounded JSON-safe ownership evidence."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


_CORRELATED_OWNER_EVENTS = frozenset(
    {
        EventTypeV2.TIMER,
        EventTypeV2.ORDER,
        EventTypeV2.TRADE,
        EventTypeV2.RECONCILE,
        EventTypeV2.OPERATOR,
    }
)
_ACTIVE_OR_PAUSED = frozenset(
    {
        ExecutionAlgoPersistenceStatusV2.ACTIVE,
        ExecutionAlgoPersistenceStatusV2.PAUSED,
    }
)


class KernelIngressRepositoryV1(Protocol):
    def read_delivery_tail(self, *, runtime_id: str, algo_instance_id: str) -> AlgoDeliveryPersistenceV1: ...

    def ingest_routed_event_atomic(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        deliveries: Sequence[AlgoDeliveryPersistenceV1],
        callback_mapping_update: KernelCallbackMappingUpdateV1 | None = None,
    ) -> RuntimeEventIngressReceiptV1: ...


@dataclass(frozen=True)
class KernelIngressCoordinatorV1:
    repository: KernelIngressRepositoryV1
    catalog_runtime: PluginCatalogRuntimeV2

    def ingest(
        self,
        *,
        event: RuntimeEventEnvelopeV2,
        algo_instances: Sequence[ExecutionAlgoInstancePersistenceV2],
        correlated_algo_instance_ids: Sequence[str],
        callback_mapping_update: KernelCallbackMappingUpdateV1 | None = None,
    ) -> RuntimeEventIngressReceiptV1:
        callback_types = {EventTypeV2.ORDER, EventTypeV2.TRADE, EventTypeV2.RECONCILE}
        if event.event_type in callback_types:
            if callback_mapping_update is None:
                raise KernelEventRoutingError(
                    "MINIQMT_RUNTIME_EVENT_CALLBACK_MAPPING_UPDATE_MISSING",
                    "callback ingress requires one exact durable mapping successor",
                    context={"event_id": event.event_id, "event_type": event.event_type.value},
                )
            if (
                tuple(correlated_algo_instance_ids) != (callback_mapping_update.mapping.algo_instance_id,)
                or callback_mapping_update.mapping.runtime_id != event.runtime_id
            ):
                raise KernelEventRoutingError(
                    "MINIQMT_RUNTIME_EVENT_CALLBACK_MAPPING_OWNER_CONFLICT",
                    "callback mapping successor conflicts with the exact routed owner",
                    context={
                        "event_id": event.event_id,
                        "mapping_id": callback_mapping_update.mapping.mapping_id,
                        "correlated_algo_instance_ids": list(correlated_algo_instance_ids),
                    },
                )
        elif callback_mapping_update is not None:
            raise KernelEventRoutingError(
                "MINIQMT_RUNTIME_EVENT_CALLBACK_MAPPING_UPDATE_UNEXPECTED",
                "non-callback ingress cannot mutate a durable child mapping",
                context={"event_id": event.event_id, "event_type": event.event_type.value},
            )
        targets = route_event_targets_v1(
            event=event,
            algo_instances=algo_instances,
            catalog_runtime=self.catalog_runtime,
            correlated_algo_instance_ids=correlated_algo_instance_ids,
        )
        by_id = {item.algo_instance_id: item for item in algo_instances}
        deliveries: list[AlgoDeliveryPersistenceV1] = []
        for algo_instance_id in targets:
            algo = by_id[algo_instance_id]
            try:
                predecessor = self.repository.read_delivery_tail(
                    runtime_id=event.runtime_id,
                    algo_instance_id=algo_instance_id,
                )
            except KeyError as exc:
                raise KernelEventRoutingError(
                    "MINIQMT_RUNTIME_EVENT_ROUTING_PREDECESSOR_MISSING",
                    "routed existing algo has no durable delivery predecessor",
                    context={
                        "runtime_id": event.runtime_id,
                        "event_id": event.event_id,
                        "algo_instance_id": algo_instance_id,
                    },
                ) from exc
            delivery = AlgoEventDeliveryV1.create(
                event=event,
                algo_instance_id=algo_instance_id,
                plugin_manifest_sha256=algo.plugin_manifest_sha256,
                algo_delivery_sequence=predecessor.algo_delivery_sequence + 1,
                previous_delivery_id=predecessor.delivery_id,
                status=DeliveryStatusV1.PENDING,
                attempt_count=0,
                lease_owner=None,
                lease_expires_at=None,
                transition_id=None,
                last_error_json=None,
                created_at_utc=event.event_time_utc,
                updated_at_utc=event.event_time_utc,
            )
            deliveries.append(
                AlgoDeliveryPersistenceV1.create(
                    delivery=delivery,
                    lease_epoch=0,
                    lease_fence_token=None,
                    row_version=1,
                    next_attempt_at_utc=None,
                    failure_receipt_id=None,
                    skip_receipt_id=None,
                    closed_at_utc=None,
                )
            )
        return self.repository.ingest_routed_event_atomic(
            event=event,
            deliveries=tuple(deliveries),
            callback_mapping_update=callback_mapping_update,
        )


def _strict_catalog(runtime: PluginCatalogRuntimeV2) -> PluginCatalogSnapshotV1:
    if not isinstance(runtime, PluginCatalogRuntimeV2):
        raise TypeError("catalog_runtime must be PluginCatalogRuntimeV2")
    try:
        return PluginCatalogSnapshotV1.model_validate(runtime.snapshot.model_dump(mode="python"), strict=True)
    except (TypeError, ValueError) as exc:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_CATALOG_INVALID",
            "plugin catalog strict readback failed before routing",
            context={"exception_type": type(exc).__name__, "message": str(exc)},
        ) from exc


def _manifest_for_algo(
    *,
    algo: ExecutionAlgoInstancePersistenceV2,
    catalog: PluginCatalogSnapshotV1,
) -> Any:
    plugin_key = PluginKeyV1(
        plugin_id=algo.plugin_id,
        plugin_version=algo.plugin_version,
        manifest_sha256=algo.plugin_manifest_sha256,
    )
    descriptors = {item.plugin_key: item for item in catalog.registration_descriptors}
    descriptor = descriptors.get(plugin_key)
    if descriptor is None:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_PLUGIN_NOT_REGISTERED",
            "durable algo plugin key is absent from the exact catalog",
            context={
                "runtime_id": algo.runtime_id,
                "algo_instance_id": algo.algo_instance_id,
                "plugin_key": plugin_key.canonical_payload_v1(),
                "catalog_sha256": catalog.catalog_sha256,
            },
        )
    manifest = descriptor.manifest
    if manifest.algo_code != algo.algo_code:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_PLUGIN_IDENTITY_CONFLICT",
            "durable algo code conflicts with the frozen plugin descriptor",
            context={
                "algo_instance_id": algo.algo_instance_id,
                "durable_algo_code": algo.algo_code,
                "manifest_algo_code": manifest.algo_code,
            },
        )
    return manifest


def route_event_targets_v1(
    *,
    event: RuntimeEventEnvelopeV2,
    algo_instances: Sequence[ExecutionAlgoInstancePersistenceV2],
    catalog_runtime: PluginCatalogRuntimeV2,
    correlated_algo_instance_ids: Sequence[str],
) -> tuple[str, ...]:
    """Return the complete stable target set for ``miniqmt_event_routing_v1``."""

    if not isinstance(event, RuntimeEventEnvelopeV2):
        raise TypeError("event must be RuntimeEventEnvelopeV2")
    algos = tuple(algo_instances)
    if any(not isinstance(item, ExecutionAlgoInstancePersistenceV2) for item in algos):
        raise TypeError("algo_instances must contain only ExecutionAlgoInstancePersistenceV2")
    algo_ids = tuple(item.algo_instance_id for item in algos)
    if len(algo_ids) != len(set(algo_ids)):
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_DUPLICATE_OWNER",
            "routing input contains duplicate durable algo identity",
            context={"runtime_id": event.runtime_id, "algo_instance_ids": list(algo_ids)},
        )
    if any(item.runtime_id != event.runtime_id for item in algos):
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_RUNTIME_CONFLICT",
            "routing input contains an algo owned by a different runtime",
            context={"runtime_id": event.runtime_id, "algo_instance_ids": list(algo_ids)},
        )
    correlated = tuple(correlated_algo_instance_ids)
    if any(type(item) is not str or not item.strip() for item in correlated):
        raise TypeError("correlated_algo_instance_ids must contain non-empty strings")
    if len(correlated) != len(set(correlated)):
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_DUPLICATE_CORRELATION",
            "event correlation contains duplicate algo identity",
            context={"event_id": event.event_id, "correlated_algo_instance_ids": list(correlated)},
        )
    if event.event_type in _CORRELATED_OWNER_EVENTS and not correlated:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_OWNER_MISSING",
            "owner-scoped event has no exact durable algo correlation",
            context={
                "runtime_id": event.runtime_id,
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "correlation": thaw_json_v1(event.correlation),
            },
        )
    if event.event_type not in _CORRELATED_OWNER_EVENTS and correlated:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_UNEXPECTED_CORRELATION",
            "broadcast or source-owned event cannot accept caller-selected targets",
            context={
                "event_id": event.event_id,
                "event_type": event.event_type.value,
                "correlated_algo_instance_ids": list(correlated),
            },
        )
    by_id = {item.algo_instance_id: item for item in algos}
    missing = sorted(set(correlated) - set(by_id))
    if missing:
        raise KernelEventRoutingError(
            "MINIQMT_RUNTIME_EVENT_ROUTING_OWNER_UNKNOWN",
            "event correlation does not resolve to a durable algo owner",
            context={"event_id": event.event_id, "missing_algo_instance_ids": missing},
        )
    candidate_algos = tuple(by_id[item] for item in correlated) if correlated else algos
    targets: list[str] = []
    catalog: PluginCatalogSnapshotV1 | None = None
    for algo in candidate_algos:
        if event.event_type is EventTypeV2.ALGO_START:
            source_identity = thaw_json_v1(event.source_identity)
            eligible = source_identity.get("algo_instance_id") == algo.algo_instance_id
        elif event.event_type is EventTypeV2.TICK:
            eligible = algo.status is ExecutionAlgoPersistenceStatusV2.ACTIVE and algo.symbol == event.symbol
        elif event.event_type is EventTypeV2.TIMER:
            eligible = algo.status is ExecutionAlgoPersistenceStatusV2.ACTIVE
        elif event.event_type in {EventTypeV2.ORDER, EventTypeV2.TRADE}:
            eligible = algo.status in _ACTIVE_OR_PAUSED
        elif event.event_type in {EventTypeV2.ACCOUNT, EventTypeV2.SESSION}:
            eligible = algo.status in _ACTIVE_OR_PAUSED
        elif event.event_type is EventTypeV2.RECONCILE:
            eligible = algo.status in _ACTIVE_OR_PAUSED
        elif event.event_type is EventTypeV2.EOD:
            eligible = algo.status in _ACTIVE_OR_PAUSED
        elif event.event_type is EventTypeV2.OPERATOR:
            eligible = algo.status in _ACTIVE_OR_PAUSED
        else:  # pragma: no cover - EventTypeV2 exhaustiveness guard
            raise KernelEventRoutingError(
                "MINIQMT_RUNTIME_EVENT_ROUTING_TYPE_UNSUPPORTED",
                "event type has no registered K2 routing rule",
                context={"event_id": event.event_id, "event_type": event.event_type.value},
            )
        if not eligible:
            continue
        if catalog is None:
            catalog = _strict_catalog(catalog_runtime)
        manifest = _manifest_for_algo(algo=algo, catalog=catalog)
        if event.event_type in manifest.subscribed_event_types:
            targets.append(algo.algo_instance_id)
    return tuple(sorted(targets))


__all__ = [
    "KernelEventRoutingError",
    "KernelIngressCoordinatorV1",
    "KernelIngressRepositoryV1",
    "route_event_targets_v1",
]
