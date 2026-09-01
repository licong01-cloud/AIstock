from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("raw_status", "expected"),
    [
        (48, "ACCEPTED"),
        (49, "ACCEPTED"),
        (50, "ACCEPTED"),
        (51, "ACCEPTED"),
        (52, "PARTIALLY_FILLED"),
        (53, "PARTIALLY_FILLED"),
        (55, "PARTIALLY_FILLED"),
        (54, "CANCELLED"),
        (56, "FILLED"),
        (57, "REJECTED"),
        ("SUBMITTED", "ACCEPTED"),
        ("PART_TRADED", "PARTIALLY_FILLED"),
        ("CANCELED", "CANCELLED"),
        ("ALL_TRADED", "FILLED"),
        ("BROKER_REJECTED", "REJECTED"),
    ],
)
def test_qmt_order_status_mapping_is_exact(raw_status: int | str, expected: str) -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import normalize_qmt_order_status_v1

    assert normalize_qmt_order_status_v1(raw_status).value == expected


@pytest.mark.parametrize("raw_status", [47, 58, "UNKNOWN", " submitted ", True, 48.0, None])
def test_qmt_order_status_mapping_fails_loud_for_unknown_or_coerced_values(raw_status: object) -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import normalize_qmt_order_status_v1

    with pytest.raises((TypeError, ValueError)):
        normalize_qmt_order_status_v1(raw_status)


def test_command_outcome_event_composite_is_registered_exactly() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
    )
    from backend.services.miniqmt_execution_runtime.plugin_contracts import (
        EventSourceV2,
        EventTypeV2,
        RuntimeEventEnvelopeV2,
    )

    payload = build_kernel_command_outcome_event_payload_v1(
        receipt_id="mqoutcomercpt_1",
        receipt_sha256="a" * 64,
        runtime_id="runtime_k3a_1",
        algo_instance_id="mqalgo_k3a_1",
        parent_intent_id="parent_k3a_1",
        strategy_slot_id="slot_k3a_1",
        mapping_id="mqmapping_k3a_1",
        command_id="mqcommand_1",
        command_type="SUBMIT_LIMIT",
        local_vt_orderid="mqlocalorder_k3a_1",
        broker_order_id="broker_k3a_1",
        outcome="ACCEPTED",
        outbox_status="ACKED",
        outbox_row_version=3,
        outcome_receipt_sha256="b" * 64,
        outbox_terminal=True,
        order_terminal=False,
    )
    event = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k3a_1",
        sequence=1,
        event_type=EventTypeV2.COMMAND_OUTCOME,
        event_time_utc="2026-07-28T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol="600000.SH",
        payload_schema_version="miniqmt_command_outcome_v1",
        payload=payload.model_dump(mode="json"),
        source_identity={"receipt_id": "mqoutcomercpt_1", "receipt_sha256": "a" * 64},
        correlation={"command_id": "mqcommand_1"},
    )
    assert event.event_type is EventTypeV2.COMMAND_OUTCOME


def test_order_builder_preserves_missing_cumulative_and_rejects_alias_conflict() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_order_event_payload_v1,
    )

    values = {
        "order_event_id": "order_event_k3a_missing_cumulative",
        "runtime_id": "runtime_k3a_1",
        "algo_instance_id": "mqalgo_k3a_1",
        "parent_intent_id": "parent_k3a_1",
        "strategy_slot_id": "slot_k3a_1",
        "mapping_id": "mqmapping_k3a_1",
        "command_id": "mqcommand_1",
        "local_vt_orderid": "mqlocalorder_k3a_1",
        "broker_order_id": "broker_k3a_1",
        "symbol": "600000.SH",
        "side": "BUY",
        "requested_quantity": 100,
    }
    payload = build_kernel_order_event_payload_v1(raw_payload={"order_status": 48}, **values)
    assert payload.observed_cumulative_filled_quantity is None
    assert payload.observed_remaining_quantity is None

    with pytest.raises(ValueError, match="aliases conflict"):
        build_kernel_order_event_payload_v1(
            raw_payload={"order_status": 52, "traded_volume": 10, "filled_quantity": 11},
            **values,
        )
    with pytest.raises(ValueError, match="aliases conflict"):
        build_kernel_order_event_payload_v1(
            raw_payload={"order_status": 52, "status": "FILLED", "traded_volume": 10},
            **values,
        )


def test_trade_builder_requires_one_exact_real_identity() -> None:
    from dataclasses import replace
    from datetime import UTC, date, datetime
    from decimal import Decimal

    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_trade_event_payload_v1,
    )
    from backend.services.qmt_strategy_ledger.models import TradeLedgerRecord
    from backend.services.qmt_strategy_ledger.tca_models import canonical_trade_fact_sha256

    values = {
        "runtime_id": "runtime_k3a_1",
        "algo_instance_id": "mqalgo_k3a_1",
        "parent_intent_id": "parent_k3a_1",
        "strategy_slot_id": "slot_k3a_1",
        "mapping_id": "mqmapping_k3a_1",
        "command_id": "mqcommand_1",
        "local_vt_orderid": "mqlocalorder_k3a_1",
        "broker_order_id": "broker_k3a_1",
        "symbol": "600000.SH",
        "side": "BUY",
        "trade_quantity": 100,
        "trade_price_decimal": "10",
    }
    with pytest.raises(ValueError, match="requires a real broker"):
        build_kernel_trade_event_payload_v1(raw_payload={}, **values)
    qmt_hash = canonical_trade_fact_sha256(
        account_id="account_k3a",
        trade_date=date(2026, 7, 28),
        trade_id="qmt_trade_2",
        qmt_order_id="broker_k3a_1",
        symbol="600000.SH",
        side="BUY",
        price=Decimal("10"),
        quantity=100,
    )
    qmt_trade = TradeLedgerRecord(
        trade_id="qmt_trade_2",
        intent_id="parent_k3a_1",
        strategy_id="strategy_k3a",
        qmt_order_id="broker_k3a_1",
        symbol="600000.SH",
        side="BUY",
        price=Decimal("10"),
        quantity=100,
        amount=Decimal("1000"),
        trade_date=date(2026, 7, 28),
        account_id="account_k3a",
        first_ingest_source="BROKER_CALLBACK",
        first_ingested_at=datetime(2026, 7, 28, 1, 30, tzinfo=UTC),
        canonical_trade_fact_sha256=qmt_hash,
    )
    with pytest.raises(ValueError, match="conflict"):
        build_kernel_trade_event_payload_v1(
            raw_payload={"trade_id": "broker_trade_1"},
            persisted_qmt_strategy_trade=qmt_trade,
            **values,
        )
    payload = build_kernel_trade_event_payload_v1(raw_payload={}, persisted_qmt_strategy_trade=qmt_trade, **values)
    assert payload.trade_id == "qmt_trade_2"
    for forged_trade in (
        replace(qmt_trade, intent_id="other_parent"),
        replace(qmt_trade, amount=Decimal("999")),
        replace(qmt_trade, first_ingest_source="UNAPPROVED"),
        replace(qmt_trade, first_ingested_at=None),
    ):
        with pytest.raises(ValueError):
            build_kernel_trade_event_payload_v1(raw_payload={}, persisted_qmt_strategy_trade=forged_trade, **values)
    with pytest.raises(TypeError, match="TradeLedgerRecord"):
        build_kernel_trade_event_payload_v1(raw_payload={}, persisted_qmt_strategy_trade="arbitrary_trade_id", **values)


@pytest.mark.parametrize(
    "changes",
    [
        {"outbox_status": "PENDING", "outbox_terminal": True},
        {"command_type": "CANCEL_ORDER", "broker_order_id": None},
        {"outcome": "REJECTED", "order_terminal": False},
        {"outcome": "ACCEPTED"},
    ],
)
def test_command_outcome_payload_rejects_status_and_identity_drift(changes: dict[str, object]) -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_command_outcome_event_payload_v1,
    )

    values = {
        "receipt_id": "mqoutcomercpt_invalid_k3a",
        "receipt_sha256": "a" * 64,
        "runtime_id": "runtime_k3a_1",
        "algo_instance_id": "mqalgo_k3a_1",
        "parent_intent_id": "parent_k3a_1",
        "strategy_slot_id": "slot_k3a_1",
        "mapping_id": "mqmapping_k3a_1",
        "command_id": "mqcommand_1",
        "command_type": "SUBMIT_LIMIT",
        "local_vt_orderid": "mqlocalorder_k3a_1",
        "broker_order_id": "broker_k3a_1",
        "outcome": "REJECTED",
        "outbox_status": "ACKED_REJECTED",
        "outbox_row_version": 3,
        "outcome_receipt_sha256": "b" * 64,
        "outbox_terminal": True,
        "order_terminal": True,
    }
    with pytest.raises(ValueError):
        build_kernel_command_outcome_event_payload_v1(**{**values, **changes})


def test_reconcile_writer_rejects_quantity_drift_before_persistence() -> None:
    from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
        build_kernel_order_reconcile_event_payload_v1,
    )

    with pytest.raises(ValueError, match="close to requested quantity"):
        build_kernel_order_reconcile_event_payload_v1(
            ordered_trade_refs=(),
            requested_quantity=100,
            receipt_id="reconcile_quantity_drift",
            receipt_sha256="a" * 64,
            runtime_id="runtime_k3a_1",
            algo_instance_id="mqalgo_k3a_1",
            parent_intent_id="parent_k3a_1",
            strategy_slot_id="slot_k3a_1",
            mapping_id="mqmapping_k3a_1",
            local_vt_orderid="mqlocalorder_k3a_1",
            broker_order_id="broker_k3a_1",
            symbol="600000.SH",
            side="BUY",
            normalized_order_status="FILLED",
            authoritative_cumulative_filled_quantity=100,
            authoritative_remaining_quantity=999,
            callback_watermark="runtime_k3a_1:1",
            snapshot_sha256="b" * 64,
        )
