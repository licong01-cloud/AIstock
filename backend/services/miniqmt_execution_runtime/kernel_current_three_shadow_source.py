"""Build K3-B shadow source snapshots from one committed repository view."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import math
from typing import Any

from .kernel_callback_events import (
    normalize_qmt_order_callback_observation_v1,
    resolve_qmt_trade_identity_alias_v1,
)
from .kernel_current_three_contracts import (
    CurrentThreeContractError,
    CurrentThreeShadowEventRefV1,
    CurrentThreeShadowFactRefV1,
    CurrentThreeShadowSourceSnapshotV1,
)
from .models import (
    MiniQMTChildOrder,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeRecord,
)
from .plugin_canonical import canonical_utc_datetime_v1, hash_hex_v1

MAX_SHADOW_EVENTS = 100_000
MAX_SHADOW_ALGOS = 1_000
MAX_SHADOW_CHILDREN = 10_000
_ALLOWED_EVENT_TYPES = frozenset(
    {
        MiniQMTExecutionEventType.TICK,
        MiniQMTExecutionEventType.TIMER,
        MiniQMTExecutionEventType.ORDER_EVENT,
        MiniQMTExecutionEventType.TRADE_EVENT,
        MiniQMTExecutionEventType.RUNTIME_STOPPED,
    }
)


@dataclass(frozen=True)
class CurrentThreeShadowRepositoryReadV1:
    """One immutable repository view plus its strict externally hashable snapshot."""

    snapshot: CurrentThreeShadowSourceSnapshotV1
    runtime: MiniQMTExecutionRuntimeRecord
    events: tuple[MiniQMTExecutionEvent, ...]
    algos: tuple[MiniQMTExecutionAlgoInstance, ...]
    children: tuple[MiniQMTChildOrder, ...]

    def strict_readback_v1(self) -> CurrentThreeShadowSourceSnapshotV1:
        rebuilt = build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha=self.snapshot.repository_commit_sha,
            runtime=self.runtime,
            events=self.events,
            algos=self.algos,
            children=self.children,
            database_snapshot_at_utc=datetime.fromisoformat(
                self.snapshot.database_snapshot_at_utc.replace("Z", "+00:00")
            ),
        )
        if rebuilt != self.snapshot:
            raise _fail(
                "MINIQMT_K3_SHADOW_SOURCE_INVALID",
                "shadow repository material does not reproduce its strict snapshot",
                runtime_id=self.snapshot.runtime_id,
                expected_source_set_sha256=self.snapshot.source_set_sha256,
                actual_source_set_sha256=rebuilt.source_set_sha256,
            )
        return rebuilt


def _fail(reason_code: str, message: str, **context: Any) -> CurrentThreeContractError:
    return CurrentThreeContractError(reason_code, message, context={"stage": "K3_SHADOW_SOURCE", **context})


def _canonical_legacy_fact(value: Any) -> Any:
    """Preserve legacy JSON facts without admitting binary floats to hash domains."""

    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise _fail(
                "MINIQMT_K3_SHADOW_SOURCE_INVALID",
                "legacy source contains a non-finite numeric fact",
                value_type="float",
            )
        return {"__legacy_numeric_type__": "float", "decimal": format(Decimal(str(value)), "f")}
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise _fail(
                "MINIQMT_K3_SHADOW_SOURCE_INVALID",
                "legacy source contains a non-finite decimal fact",
                value_type="Decimal",
            )
        return {"__legacy_numeric_type__": "decimal", "decimal": format(value, "f")}
    if isinstance(value, datetime):
        return {"__legacy_type__": "datetime", "utc": canonical_utc_datetime_v1(value, field_name="legacy_datetime")}
    if isinstance(value, dict):
        if any(type(key) is not str for key in value):
            raise _fail(
                "MINIQMT_K3_SHADOW_SOURCE_INVALID",
                "legacy source object contains a non-string key",
                key_types=sorted({type(key).__name__ for key in value}),
            )
        return {key: _canonical_legacy_fact(member) for key, member in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        return [_canonical_legacy_fact(item) for item in value]
    raise _fail(
        "MINIQMT_K3_SHADOW_SOURCE_INVALID",
        "legacy source contains an unsupported fact type",
        value_type=type(value).__name__,
    )


def _validate_capacity(*, event_count: int, algo_count: int, child_count: int) -> None:
    actual = {"event_count": event_count, "algo_count": algo_count, "child_count": child_count}
    limits = {
        "event_count": MAX_SHADOW_EVENTS,
        "algo_count": MAX_SHADOW_ALGOS,
        "child_count": MAX_SHADOW_CHILDREN,
    }
    exceeded = {name: value for name, value in actual.items() if value > limits[name]}
    if exceeded:
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_CAPACITY_EXCEEDED",
            "current-three shadow snapshot exceeds its production capacity contract",
            **actual,
            limits=limits,
            exceeded=exceeded,
        )


def _event_ref(event: MiniQMTExecutionEvent) -> CurrentThreeShadowEventRefV1:
    payload = {
        "schema_version": "miniqmt_current_three_shadow_event_ref_v1",
        "event_id": event.event_id,
        "sequence": event.sequence,
        "event_type": event.event_type.value,
        "event_source": event.source,
        "payload_sha256": hash_hex_v1(
            "miniqmt_current_three_legacy_event_payload_v1", _canonical_legacy_fact(event.payload)
        ),
        "event_time_utc": canonical_utc_datetime_v1(event.event_time, field_name="event_time"),
    }
    return CurrentThreeShadowEventRefV1(
        **payload,
        ref_sha256=hash_hex_v1("miniqmt_current_three_shadow_event_ref_v1", payload),
    )


def _fact_ref(
    *, identity: str, owner_identity: str, payload: dict[str, Any], logical_time: datetime
) -> CurrentThreeShadowFactRefV1:
    canonical_payload = _canonical_legacy_fact(payload)
    payload_sha256 = hash_hex_v1("miniqmt_current_three_shadow_fact_payload_v1", canonical_payload)
    base = {
        "schema_version": "miniqmt_current_three_shadow_fact_ref_v1",
        "identity": identity,
        "owner_identity": owner_identity,
        "payload": canonical_payload,
        "payload_sha256": payload_sha256,
        "logical_time_utc": canonical_utc_datetime_v1(logical_time, field_name="logical_time"),
    }
    return CurrentThreeShadowFactRefV1(
        **base,
        ref_sha256=hash_hex_v1("miniqmt_current_three_shadow_fact_ref_v1", base),
    )


def _require_event_payload(event: MiniQMTExecutionEvent) -> None:
    if event.event_type not in _ALLOWED_EVENT_TYPES:
        # The committed snapshot retains every runtime event in its source hash.
        # Only the parity adapter selects the five K3-B business event kinds.
        return
    payload = event.payload
    required: tuple[str, ...] = ()
    reason = "MINIQMT_K3_EVENT_PAYLOAD_INVALID"
    if event.event_type is MiniQMTExecutionEventType.TICK:
        required = ("symbol", "bid_price_1", "ask_price_1", "bid_volume_1", "ask_volume_1")
    elif event.event_type is MiniQMTExecutionEventType.TIMER:
        required = ("timer_name", "timer_occurrence_id", "schedule_epoch", "monotonic_ns")
    elif event.event_type is MiniQMTExecutionEventType.ORDER_EVENT:
        required = ("child_order_id", "broker_order_id", "quantity", "price")
        reason = "MINIQMT_K3_ORDER_EVENT_PAYLOAD_INVALID"
    elif event.event_type is MiniQMTExecutionEventType.TRADE_EVENT:
        required = ("child_order_id", "broker_order_id", "quantity", "price")
        reason = "MINIQMT_K3_TRADE_EVENT_PAYLOAD_INVALID"
    elif event.event_type is MiniQMTExecutionEventType.RUNTIME_STOPPED:
        required = ("reason",)
    missing = [name for name in required if payload.get(name) is None or payload.get(name) == ""]
    if missing:
        raise _fail(
            reason,
            "shadow event payload is missing authoritative business facts",
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_type=event.event_type.value,
            missing_fields=missing,
        )
    if event.event_type in {MiniQMTExecutionEventType.ORDER_EVENT, MiniQMTExecutionEventType.TRADE_EVENT}:
        quantity = payload.get("quantity")
        try:
            if type(quantity) is not int or quantity <= 0:
                raise TypeError("quantity must be a positive strict integer")
            price = Decimal(str(payload.get("price")))
            if not price.is_finite() or price <= 0:
                raise ValueError("callback price must be positive and finite")
            if event.event_type is MiniQMTExecutionEventType.ORDER_EVENT:
                _, cumulative = normalize_qmt_order_callback_observation_v1(payload)
                if cumulative is not None and cumulative > quantity:
                    raise ValueError("ORDER cumulative quantity exceeds callback quantity")
            elif resolve_qmt_trade_identity_alias_v1(payload) is None:
                raise ValueError("TRADE requires a raw broker trade identity alias")
        except (TypeError, ValueError) as exc:
            raise _fail(
                reason,
                "shadow callback payload does not satisfy the unique callback authority",
                runtime_id=event.runtime_id,
                event_id=event.event_id,
                event_type=event.event_type.value,
                error_type=type(exc).__name__,
                error_message=str(exc),
            ) from exc


def resolve_current_three_event_owner_v1(
    *,
    event: MiniQMTExecutionEvent,
    algos: Sequence[MiniQMTExecutionAlgoInstance],
    children: Sequence[MiniQMTChildOrder],
) -> str:
    """Resolve one callback/child-lineage event through exact child and broker facts."""

    payload = event.payload
    child_id = payload.get("child_order_id")
    broker_id = payload.get("broker_order_id")
    child_matches = [item for item in children if item.child_order_id == child_id]
    broker_matches = [item for item in children if item.broker_order_id == broker_id]
    if len(child_matches) != 1 or len(broker_matches) != 1 or child_matches[0] != broker_matches[0]:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "callback event does not close to one exact child and broker owner",
            event_id=event.event_id,
            event_type=event.event_type.value,
            child_order_id=child_id,
            broker_order_id=broker_id,
            child_match_count=len(child_matches),
            broker_match_count=len(broker_matches),
        )
    child = child_matches[0]
    algo_matches = [item for item in algos if item.algo_instance_id == child.algo_instance_id]
    if len(algo_matches) != 1:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "callback child does not close to one exact algo owner",
            event_id=event.event_id,
            child_order_id=child.child_order_id,
            algo_instance_id=child.algo_instance_id,
            algo_match_count=len(algo_matches),
        )
    algo = algo_matches[0]
    expected = {
        "algo_instance_id": algo.algo_instance_id,
        "parent_intent_id": algo.parent_intent_id,
        "strategy_slot_id": algo.strategy_slot_id,
    }
    if child.parent_intent_id != algo.parent_intent_id or child.strategy_slot_id != algo.strategy_slot_id:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "callback child owner crosses its algo parent or strategy slot",
            event_id=event.event_id,
            child_order_id=child.child_order_id,
            expected_parent_intent_id=algo.parent_intent_id,
            actual_parent_intent_id=child.parent_intent_id,
            expected_strategy_slot_id=algo.strategy_slot_id,
            actual_strategy_slot_id=child.strategy_slot_id,
        )
    conflicts = {
        field: {"expected": value, "actual": payload[field]}
        for field, value in expected.items()
        if field in payload and (type(payload[field]) is not str or payload[field] != value)
    }
    if conflicts:
        raise _fail(
            "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID",
            "callback event owner aliases cross the exact child owner",
            event_id=event.event_id,
            child_order_id=child.child_order_id,
            conflicts=conflicts,
        )
    return algo.algo_instance_id


def build_current_three_shadow_source_snapshot_v1(
    *,
    repository_commit_sha: str,
    runtime: MiniQMTExecutionRuntimeRecord,
    events: Sequence[MiniQMTExecutionEvent],
    algos: Sequence[MiniQMTExecutionAlgoInstance],
    children: Sequence[MiniQMTChildOrder],
    database_snapshot_at_utc: datetime,
) -> CurrentThreeShadowSourceSnapshotV1:
    """Validate and freeze one repository-owned, same-transaction snapshot."""

    if not isinstance(runtime, MiniQMTExecutionRuntimeRecord):
        raise TypeError("runtime must be MiniQMTExecutionRuntimeRecord")
    _validate_capacity(event_count=len(events), algo_count=len(algos), child_count=len(children))
    runtime_id = runtime.runtime_id
    if any(item.runtime_id != runtime_id for item in (*events, *algos, *children)):
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow snapshot contains a fact owned by a different runtime",
            runtime_id=runtime_id,
        )
    ordered_events = tuple(sorted(events, key=lambda item: (item.sequence, item.event_id)))
    if ordered_events and tuple(item.sequence for item in ordered_events) != tuple(
        range(ordered_events[0].sequence, ordered_events[0].sequence + len(ordered_events))
    ):
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow event sequence is not contiguous",
            runtime_id=runtime_id,
            sequences=[item.sequence for item in ordered_events[:64]],
        )
    if len({item.event_id for item in ordered_events}) != len(ordered_events):
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow event identity is not unique",
            runtime_id=runtime_id,
        )
    for event in ordered_events:
        _require_event_payload(event)
    ordered_algos = tuple(sorted(algos, key=lambda item: item.algo_instance_id))
    if len({item.algo_instance_id for item in ordered_algos}) != len(ordered_algos):
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow algo identity is not unique",
            runtime_id=runtime_id,
        )
    algo_ids = {item.algo_instance_id for item in ordered_algos}
    ordered_children = tuple(sorted(children, key=lambda item: item.child_order_id))
    if len({item.child_order_id for item in ordered_children}) != len(ordered_children):
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow child identity is not unique",
            runtime_id=runtime_id,
        )
    orphan_children = [item.child_order_id for item in ordered_children if item.algo_instance_id not in algo_ids]
    if orphan_children:
        raise _fail(
            "MINIQMT_K3_SHADOW_SOURCE_INVALID",
            "shadow child fact has no exact algo owner",
            runtime_id=runtime_id,
            orphan_child_order_ids=orphan_children[:64],
        )
    for event in ordered_events:
        if event.event_type in {MiniQMTExecutionEventType.ORDER_EVENT, MiniQMTExecutionEventType.TRADE_EVENT}:
            resolve_current_three_event_owner_v1(event=event, algos=ordered_algos, children=ordered_children)

    event_refs = tuple(_event_ref(item) for item in ordered_events)
    child_refs = tuple(
        _fact_ref(
            identity=item.child_order_id,
            owner_identity=item.algo_instance_id,
            payload=item.model_dump(mode="json"),
            logical_time=item.updated_at,
        )
        for item in ordered_children
    )
    algo_refs = tuple(
        _fact_ref(
            identity=item.algo_instance_id,
            owner_identity=item.runtime_id,
            payload=item.model_dump(mode="json"),
            logical_time=item.updated_at,
        )
        for item in ordered_algos
    )
    event_hash = hash_hex_v1(
        "miniqmt_current_three_shadow_event_set_v1", [item.canonical_payload_v1() for item in event_refs]
    )
    child_hash = hash_hex_v1(
        "miniqmt_current_three_shadow_child_set_v1", [item.canonical_payload_v1() for item in child_refs]
    )
    algo_hash = hash_hex_v1(
        "miniqmt_current_three_shadow_algo_set_v1", [item.canonical_payload_v1() for item in algo_refs]
    )
    payload = {
        "schema_version": "miniqmt_current_three_shadow_source_snapshot_v1",
        "repository_commit_sha": repository_commit_sha,
        "runtime_id": runtime_id,
        "trade_date": runtime.trade_date.isoformat(),
        "database_snapshot_at_utc": canonical_utc_datetime_v1(
            database_snapshot_at_utc, field_name="database_snapshot_at_utc"
        ),
        "ordered_legacy_event_refs": event_refs,
        "event_count": len(event_refs),
        "event_set_sha256": event_hash,
        "ordered_child_fact_refs": child_refs,
        "child_count": len(child_refs),
        "child_set_sha256": child_hash,
        "ordered_algo_instance_refs": algo_refs,
        "algo_count": len(algo_refs),
        "algo_set_sha256": algo_hash,
    }
    return CurrentThreeShadowSourceSnapshotV1(
        **payload,
        source_set_sha256=hash_hex_v1(
            "miniqmt_current_three_shadow_source_snapshot_v1",
            CurrentThreeShadowSourceSnapshotV1.model_construct(
                **payload, source_set_sha256="0" * 64
            ).canonical_payload_v1(exclude={"source_set_sha256"}),
        ),
    )


def build_current_three_shadow_repository_read_v1(
    *,
    repository_commit_sha: str,
    runtime: MiniQMTExecutionRuntimeRecord,
    events: Sequence[MiniQMTExecutionEvent],
    algos: Sequence[MiniQMTExecutionAlgoInstance],
    children: Sequence[MiniQMTChildOrder],
    database_snapshot_at_utc: datetime,
) -> CurrentThreeShadowRepositoryReadV1:
    snapshot = build_current_three_shadow_source_snapshot_v1(
        repository_commit_sha=repository_commit_sha,
        runtime=runtime,
        events=events,
        algos=algos,
        children=children,
        database_snapshot_at_utc=database_snapshot_at_utc,
    )
    read = CurrentThreeShadowRepositoryReadV1(
        snapshot=snapshot,
        runtime=runtime.model_copy(deep=True),
        events=tuple(item.model_copy(deep=True) for item in events),
        algos=tuple(item.model_copy(deep=True) for item in algos),
        children=tuple(item.model_copy(deep=True) for item in children),
    )
    read.strict_readback_v1()
    return read


__all__ = [
    "CurrentThreeShadowRepositoryReadV1",
    "build_current_three_shadow_repository_read_v1",
    "build_current_three_shadow_source_snapshot_v1",
    "resolve_current_three_event_owner_v1",
]
