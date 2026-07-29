from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
import threading

import pytest

from backend.services.miniqmt_execution_runtime.kernel_current_three_contracts import (
    CurrentThreeContractError,
    CurrentThreeShadowSourceSnapshotV1,
)
from backend.services.miniqmt_execution_runtime.kernel_current_three_shadow_source import (
    build_current_three_shadow_source_snapshot_v1,
)
from backend.services.miniqmt_execution_runtime.models import (
    MiniQMTAlgoInstanceStatus,
    MiniQMTChildOrder,
    MiniQMTChildOrderStatus,
    MiniQMTExecutionAlgoInstance,
    MiniQMTExecutionEvent,
    MiniQMTExecutionEventType,
    MiniQMTExecutionRuntimeRecord,
)
from backend.services.miniqmt_execution_runtime.repository import InMemoryMiniQMTExecutionRuntimeRepository
from backend.services.miniqmt_execution_runtime.repository import (
    JsonFileMiniQMTExecutionRuntimeRepository,
    QuoteEvidenceEventCandidate,
)
from backend.services.trading_core.models import OrderSide


NOW = datetime(2026, 7, 29, 1, 30, tzinfo=UTC)


def _runtime() -> MiniQMTExecutionRuntimeRecord:
    return MiniQMTExecutionRuntimeRecord(
        runtime_id="runtime_shadow",
        account_group_id="account_shadow",
        trade_date=date(2026, 7, 29),
        runtime_config_hash="runtime-config",
        created_at=NOW,
        updated_at=NOW,
        metadata={"repository_commit_sha": "a" * 40},
    )


def _algo() -> MiniQMTExecutionAlgoInstance:
    return MiniQMTExecutionAlgoInstance(
        algo_instance_id="legacy_algo_1",
        runtime_id="runtime_shadow",
        parent_intent_id="parent_1",
        strategy_slot_id="slot_1",
        symbol="600000.SH",
        side=OrderSide.BUY,
        target_quantity=100,
        remaining_quantity=100,
        algo_code="SNIPER_MINIQMT",
        status=MiniQMTAlgoInstanceStatus.ACTIVE,
        created_at=NOW,
        updated_at=NOW,
        metadata={"config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"}},
    )


def _child() -> MiniQMTChildOrder:
    return MiniQMTChildOrder(
        child_order_id="legacy_child_1",
        runtime_id="runtime_shadow",
        algo_instance_id="legacy_algo_1",
        parent_intent_id="parent_1",
        strategy_slot_id="slot_1",
        symbol="600000.SH",
        side=OrderSide.BUY,
        quantity=100,
        price=10,
        status=MiniQMTChildOrderStatus.SUBMITTED,
        broker_order_id="broker_1",
        submitted_at=NOW,
        updated_at=NOW,
        metadata={"reason_code": "sniper_ask_crossed_limit"},
    )


def _events() -> tuple[MiniQMTExecutionEvent, ...]:
    return (
        MiniQMTExecutionEvent(
            event_id="tick_1",
            runtime_id="runtime_shadow",
            sequence=1,
            event_type=MiniQMTExecutionEventType.TICK,
            event_time=NOW,
            source="gateway",
            payload={
                "symbol": "600000.SH",
                "bid_price_1": 9.99,
                "ask_price_1": 10.0,
                "bid_volume_1": 100,
                "ask_volume_1": 100,
                "market_data_projection_id": "md_1",
                "market_data_projection_sha256": "1" * 64,
            },
        ),
        MiniQMTExecutionEvent(
            event_id="order_1",
            runtime_id="runtime_shadow",
            sequence=2,
            event_type=MiniQMTExecutionEventType.ORDER_EVENT,
            event_time=NOW,
            source="gateway",
            payload={
                "child_order_id": "legacy_child_1",
                "broker_order_id": "broker_1",
                "status": "SUBMITTED",
                "quantity": 100,
                "price": 10,
            },
        ),
    )


def test_inmemory_repository_returns_one_strict_shadow_snapshot() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(_algo())
    repo.upsert_child_order(_child())
    for event in _events():
        repo.append_event(event)

    read = repo.read_current_three_shadow_snapshot("runtime_shadow", include_archived=False)
    snapshot = read.snapshot

    assert snapshot.runtime_id == "runtime_shadow"
    assert snapshot.event_count == 2
    assert snapshot.algo_count == 1
    assert snapshot.child_count == 1
    assert CurrentThreeShadowSourceSnapshotV1.model_validate_json(snapshot.model_dump_json()) == snapshot
    assert read.strict_readback_v1() == snapshot


def test_inmemory_evidence_append_participates_in_shadow_snapshot_lock() -> None:
    class _LockProbe:
        def __init__(self) -> None:
            self.enter_count = 0

        def __enter__(self):
            self.enter_count += 1
            return self

        def __exit__(self, exc_type, exc, traceback) -> None:
            return None

    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    lock = _LockProbe()
    repo._snapshot_lock = lock  # type: ignore[assignment]
    evidence_sha256 = "e" * 64

    repo.append_evidence_event_idempotent(
        QuoteEvidenceEventCandidate(
            event_id="quote_health_1",
            runtime_id="runtime_shadow",
            event_type=MiniQMTExecutionEventType.QUOTE_INGRESS_HEALTH,
            event_time=NOW,
            payload={
                "schema_version": "miniqmt_quote_ingress_health_payload_v1",
                "health_or_aggregate": {"health_sha256": evidence_sha256},
            },
            evidence_sha256=evidence_sha256,
        )
    )

    assert lock.enter_count == 1


def test_json_repository_does_not_publish_snapshot_before_oplog_commit(tmp_path, monkeypatch) -> None:
    repo = JsonFileMiniQMTExecutionRuntimeRepository(tmp_path / "runtime-state.json")
    repo.upsert_runtime(_runtime())
    append_entered = threading.Event()
    allow_append = threading.Event()
    snapshot_finished = threading.Event()
    original_append = repo._append_operation

    def _blocked_append(operation: str, item: dict) -> None:
        append_entered.set()
        assert allow_append.wait(timeout=5)
        original_append(operation, item)

    monkeypatch.setattr(repo, "_append_operation", _blocked_append)
    writer = threading.Thread(target=lambda: repo.append_event(_events()[0]), daemon=True)
    writer.start()
    assert append_entered.wait(timeout=5)

    def _read_snapshot() -> None:
        repo.read_current_three_shadow_snapshot("runtime_shadow")
        snapshot_finished.set()

    reader = threading.Thread(target=_read_snapshot, daemon=True)
    reader.start()
    assert snapshot_finished.wait(timeout=0.1) is False
    allow_append.set()
    writer.join(timeout=5)
    reader.join(timeout=5)
    assert writer.is_alive() is False
    assert reader.is_alive() is False
    assert snapshot_finished.is_set()


def test_shadow_source_rejects_unowned_order_event() -> None:
    orphan = _events()[1].model_copy(
        update={
            "payload": {
                **_events()[1].payload,
                "child_order_id": "missing_child",
                "broker_order_id": "missing_broker",
            }
        }
    )

    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=(_events()[0], orphan),
            algos=(_algo(),),
            children=(_child(),),
            database_snapshot_at_utc=NOW,
        )

    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID"


def test_shadow_source_accepts_registered_trade_identity_alias() -> None:
    trade = MiniQMTExecutionEvent(
        event_id="trade_alias_1",
        runtime_id="runtime_shadow",
        sequence=2,
        event_type=MiniQMTExecutionEventType.TRADE_EVENT,
        event_time=NOW,
        source="gateway",
        payload={
            "child_order_id": "legacy_child_1",
            "broker_order_id": "broker_1",
            "deal_id": "deal_1",
            "quantity": 100,
            "price": 10,
        },
    )

    snapshot = build_current_three_shadow_source_snapshot_v1(
        repository_commit_sha="a" * 40,
        runtime=_runtime(),
        events=(_events()[0], trade),
        algos=(_algo(),),
        children=(_child(),),
        database_snapshot_at_utc=NOW,
    )

    assert snapshot.event_count == 2


@pytest.mark.parametrize(
    ("child_update", "payload_update"),
    [
        ({"parent_intent_id": "cross_parent"}, {}),
        ({"strategy_slot_id": "cross_slot"}, {}),
        ({}, {"algo_instance_id": "conflicting_algo"}),
    ],
)
def test_shadow_source_rejects_cross_owner_callback_lineage(child_update, payload_update) -> None:
    child = _child().model_copy(update=child_update)
    event = _events()[1].model_copy(update={"payload": {**_events()[1].payload, **payload_update}})

    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=(_events()[0], event),
            algos=(_algo(),),
            children=(child,),
            database_snapshot_at_utc=NOW,
        )

    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID"


def test_shadow_source_rejects_duplicate_broker_owner() -> None:
    duplicate = _child().model_copy(update={"child_order_id": "legacy_child_2"})

    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=_events(),
            algos=(_algo(),),
            children=(_child(), duplicate),
            database_snapshot_at_utc=NOW,
        )

    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_ASSOCIATION_INVALID"


def test_shadow_source_rejects_capacity_before_building_any_receipt() -> None:
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=_events(),
            algos=tuple(_algo() for _ in range(1001)),
            children=(_child(),),
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_CAPACITY_EXCEEDED"
    assert exc_info.value.context["algo_count"] == 1001


def test_shadow_source_rejects_sequence_gap_and_owner_drift() -> None:
    gap = _events()[1].model_copy(update={"sequence": 3})
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=(_events()[0], gap),
            algos=(_algo(),),
            children=(_child(),),
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"

    wrong_owner = _child().model_copy(update={"algo_instance_id": "unknown_algo"})
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=_events(),
            algos=(_algo(),),
            children=(wrong_owner,),
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"


def test_shadow_source_hashes_non_parity_event_but_rejects_missing_order_fact() -> None:
    unsupported = _events()[0].model_copy(update={"event_type": MiniQMTExecutionEventType.ACCOUNT_EVENT})
    snapshot = build_current_three_shadow_source_snapshot_v1(
        repository_commit_sha="a" * 40,
        runtime=_runtime(),
        events=(unsupported,),
        algos=(_algo(),),
        children=(_child(),),
        database_snapshot_at_utc=NOW,
    )
    assert snapshot.ordered_legacy_event_refs[0].event_type == "ACCOUNT_EVENT"

    malformed = _events()[1].model_copy(update={"payload": {"child_order_id": "legacy_child_1"}})
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=(_events()[0], malformed),
            algos=(_algo(),),
            children=(_child(),),
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_ORDER_EVENT_PAYLOAD_INVALID"


@pytest.mark.parametrize(
    "bad_value",
    (
        float("nan"),
        Decimal("Infinity"),
        {1: "non-string-key"},
        {"unsupported": object()},
    ),
    ids=("nonfinite_float", "nonfinite_decimal", "non_string_key", "unsupported_object"),
)
def test_shadow_source_rejects_noncanonical_legacy_payload_facts(bad_value: object) -> None:
    event = _events()[0].model_copy(update={"payload": {**_events()[0].payload, "bad": bad_value}})
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=(event,),
            algos=(_algo(),),
            children=(_child(),),
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"


@pytest.mark.parametrize("duplicate_kind", ("event", "algo", "child"))
def test_shadow_source_rejects_duplicate_durable_identity(duplicate_kind: str) -> None:
    events = (_events()[0],)
    algos = (_algo(),)
    children = (_child(),)
    if duplicate_kind == "event":
        events = (_events()[0], _events()[0])
    elif duplicate_kind == "algo":
        algos = (_algo(), _algo())
    else:
        children = (_child(), _child())
    with pytest.raises(CurrentThreeContractError) as exc_info:
        build_current_three_shadow_source_snapshot_v1(
            repository_commit_sha="a" * 40,
            runtime=_runtime(),
            events=events,
            algos=algos,
            children=children,
            database_snapshot_at_utc=NOW,
        )
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"


def test_shadow_repository_readback_rejects_snapshot_material_drift() -> None:
    repo = InMemoryMiniQMTExecutionRuntimeRepository()
    repo.upsert_runtime(_runtime())
    repo.upsert_algo_instance(_algo())
    repo.upsert_child_order(_child())
    repo.append_event(_events()[0])
    read = repo.read_current_three_shadow_snapshot("runtime_shadow")
    drifted = read.snapshot.model_copy(update={"repository_commit_sha": "b" * 40})
    with pytest.raises(CurrentThreeContractError) as exc_info:
        type(read)(
            snapshot=drifted,
            runtime=read.runtime,
            events=read.events,
            algos=read.algos,
            children=read.children,
        ).strict_readback_v1()
    assert exc_info.value.reason_code == "MINIQMT_K3_SHADOW_SOURCE_INVALID"
