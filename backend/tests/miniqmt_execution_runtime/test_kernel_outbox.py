from __future__ import annotations

import json
from typing import Any

import pytest

from backend.services.miniqmt_execution_runtime.gateway import (
    FakeMiniQMTGateway,
    MiniQMTGatewayCancelAck,
    MiniQMTGatewayEventSourceError,
    MiniQMTGatewayOrderAck,
    QmtClientMiniQMTGateway,
)
from backend.services.miniqmt_execution_runtime.kernel_outbox import (
    GatewayReconciliationSnapshotV1,
    KernelGatewayPreCallError,
    KernelOutboxDispatchError,
    KernelOutboxDispatcherV1,
    KernelOutboxReconcilerV1,
    MiniQMTKernelGatewayAdapterV1,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandOutboxStatusV1,
    BrokerCommandOutboxV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    BrokerDispatchAttemptV1,
    CommandChildMappingStatusV1,
    ExecutionCommandChildMappingV1,
    GatewayCapabilityCatalogV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    SessionPhaseV1,
    SideV1,
)


def _command() -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k2d",
        algo_instance_id="algo_k2d",
        parent_intent_id="intent_k2d",
        transition_id="transition_k2d",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.2500",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="K2D_TEST_SUBMIT",
        metadata={"test": "k2d"},
    )


def _catalog(
    *,
    idempotent_submit: bool = False,
    quote_source: str = "B0_QUOTE_V2",
    exact_cancel: bool = True,
) -> GatewayCapabilityCatalogV1:
    payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "gateway_route_k2d",
        "quote_source": quote_source,
        "gateway_backend": "minqmt_sim",
        "order_types": (OrderTypeV1.LIMIT,),
        "market_data_capabilities": (MarketDataCapabilityV1.L1_ASK,),
        "session_phases": (SessionPhaseV1.CONTINUOUS_AM, SessionPhaseV1.CONTINUOUS_PM),
        "idempotent_submit_by_client_ref": idempotent_submit,
        "exact_order_id_cancel": exact_cancel,
    }
    return GatewayCapabilityCatalogV1(
        **payload,
        catalog_sha256=hash_hex_v1(
            "miniqmt_gateway_capability_catalog_v1",
            {
                **payload,
                "order_types": [item.value for item in payload["order_types"]],
                "market_data_capabilities": [item.value for item in payload["market_data_capabilities"]],
                "session_phases": [item.value for item in payload["session_phases"]],
            },
        ),
    )


def _initial_chain() -> tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1]:
    command = _command()
    mapping = ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id="slot_k2d",
        mapping_status=CommandChildMappingStatusV1.RESERVED,
        mapping_version=1,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:00Z",
    )
    outbox = BrokerCommandOutboxV1.create(
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
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:00Z",
        closed_at_utc=None,
    )
    return mapping, outbox


def _accepted_mapping() -> ExecutionCommandChildMappingV1:
    command = _command()
    return ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id="slot_k2d",
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=2,
        broker_order_id="broker_k2d_cancel_target",
        broker_identity_source_event_id="event_k2d_cancel_target",
        last_order_event_id="event_k2d_cancel_target",
        last_trade_event_id=None,
        updated_by_event_id="event_k2d_cancel_target",
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:01Z",
    )


def _cancel_command(
    mapping: ExecutionCommandChildMappingV1,
    *,
    owned_broker_order_id: str | None = None,
) -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=mapping.runtime_id,
        algo_instance_id=mapping.algo_instance_id,
        parent_intent_id=mapping.parent_intent_id,
        transition_id="transition_k2d_cancel",
        ordinal=1,
        local_vt_orderid=mapping.local_vt_orderid,
        symbol=mapping.symbol,
        side=mapping.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal=mapping.requested_price_decimal,
        quantity=mapping.requested_quantity,
        owned_broker_order_id=owned_broker_order_id or mapping.broker_order_id,
        reason_code="K2D_TEST_CANCEL",
        metadata={"test": "k2d_cancel"},
    )


class _Repository:
    def __init__(
        self,
        chain: tuple[ExecutionCommandChildMappingV1, BrokerCommandOutboxV1] | None = None,
    ) -> None:
        self.mapping, self.outbox = chain or _initial_chain()
        self.attempts: list[BrokerDispatchAttemptV1] = []
        self.reconciliation_receipts: dict[int, Any] = {}
        self.fail_next_cas = False

    def read_command_identity_chain(self, command_id: str) -> dict[str, Any]:
        assert command_id == self.outbox.command_id
        return {"mapping": self.mapping, "outbox": self.outbox}

    def claim_outbox_command(self, **values: Any) -> BrokerCommandOutboxV1:
        assert values["expected_row_version"] == self.outbox.row_version
        command = _read_command(self.outbox)
        previous = self.outbox
        self.outbox = BrokerCommandOutboxV1.create(
            command=command,
            mapping_id=previous.mapping_id,
            status=BrokerCommandOutboxStatusV1.CLAIMED,
            attempt_count=previous.attempt_count + 1,
            lease_owner=values["lease_owner"],
            lease_epoch=values["lease_epoch"],
            lease_fence_token=values["lease_fence_token"],
            lease_expires_at=values["lease_expires_at"],
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
            row_version=previous.row_version + 1,
            created_at_utc=previous.created_at_utc,
            updated_at_utc=values["updated_at_utc"],
            closed_at_utc=None,
        )
        self.outbox.validate_successor_v1(previous)
        return self.outbox

    def compare_and_swap_mapping_outbox(self, **values: Any) -> dict[str, Any]:
        if self.fail_next_cas:
            self.fail_next_cas = False
            raise RuntimeError("injected reconciliation CAS failure")
        assert values["expected_mapping_version"] == self.mapping.mapping_version
        assert values["expected_outbox_row_version"] == self.outbox.row_version
        assert values["expected_lease_owner"] == self.outbox.lease_owner
        assert values["expected_lease_epoch"] == self.outbox.lease_epoch
        assert values["expected_lease_fence_token"] == self.outbox.lease_fence_token
        mapping = values["mapping"]
        outbox = values["outbox"]
        if mapping != self.mapping:
            mapping.validate_successor_v1(self.mapping)
        outbox.validate_successor_v1(self.outbox)
        self.mapping = mapping
        self.outbox = outbox
        return {"mapping": mapping, "outbox": outbox}

    def append_dispatch_attempt(self, attempt: BrokerDispatchAttemptV1) -> BrokerDispatchAttemptV1:
        duplicate = next(
            (
                item
                for item in self.attempts
                if item.dispatch_attempt_id == attempt.dispatch_attempt_id and item.stage == attempt.stage
            ),
            None,
        )
        if duplicate is not None and duplicate != attempt:
            raise AssertionError("same attempt stage cannot change payload")
        if duplicate is None:
            self.attempts.append(attempt)
        return attempt

    def read_reconciliation_receipt(self, command_id: str, reconcile_attempt: int):  # type: ignore[no-untyped-def]
        assert command_id == self.outbox.command_id
        return self.reconciliation_receipts.get(reconcile_attempt)

    def append_reconciliation_receipt(self, receipt):  # type: ignore[no-untyped-def]
        existing = self.reconciliation_receipts.get(receipt.reconcile_attempt)
        if existing is not None and existing != receipt:
            raise AssertionError("reconciliation attempt identity conflict")
        self.reconciliation_receipts[receipt.reconcile_attempt] = receipt
        return receipt


class _Gateway:
    def __init__(self, *, ack: MiniQMTGatewayOrderAck | None = None, error: BaseException | None = None) -> None:
        self.ack = ack
        self.error = error
        self.calls = 0
        self.pre_call_error: KernelGatewayPreCallError | None = None
        self.snapshots: list[GatewayReconciliationSnapshotV1] = [
            GatewayReconciliationSnapshotV1(callback_watermark="watermark_k2d_1", orders=(), trades=())
        ]
        self.snapshot_error: BaseException | None = None
        self.snapshot_calls = 0

    def validate_pre_call(self, **values: Any) -> None:
        del values
        if self.pre_call_error is not None:
            raise self.pre_call_error

    def dispatch(self, **values: Any) -> MiniQMTGatewayOrderAck:
        del values
        self.calls += 1
        if self.error is not None:
            raise self.error
        assert self.ack is not None
        return self.ack

    def callback_watermark(self, *, runtime_id: str) -> str:
        assert runtime_id == "runtime_k2d"
        return self.snapshots[0].callback_watermark

    def reconciliation_snapshot(self, *, runtime_id: str) -> GatewayReconciliationSnapshotV1:
        assert runtime_id == "runtime_k2d"
        self.snapshot_calls += 1
        if self.snapshot_error is not None:
            raise self.snapshot_error
        if len(self.snapshots) > 1:
            return self.snapshots.pop(0)
        return self.snapshots[0]


def _dispatcher(repository: _Repository, gateway: _Gateway, *, catalog: GatewayCapabilityCatalogV1 | None = None):
    return KernelOutboxDispatcherV1(
        repository=repository,
        gateway=gateway,
        gateway_catalog=catalog or _catalog(),
        lease_owner="worker_k2d:incarnation_k2d",
        process_incarnation_id="incarnation_k2d",
    )


def test_dispatcher_three_phase_submit_calls_gateway_once_and_commits_exact_ack() -> None:
    repository = _Repository()
    gateway = _Gateway(
        ack=MiniQMTGatewayOrderAck(
            accepted=True,
            broker_order_id="broker_k2d_1",
            message="accepted",
            raw={"broker_called": True, "order_remark": repository.mapping.order_remark},
        )
    )
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert gateway.calls == 1
    assert result.status is BrokerCommandOutboxStatusV1.ACKED
    assert result.broker_called is True
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.DISPATCHING
    assert repository.mapping.broker_order_id is None
    assert repository.mapping.broker_identity_source_event_id is None
    assert result.broker_order_id == "broker_k2d_1"
    assert [item.stage.value for item in repository.attempts] == [
        "CLAIMED",
        "PRE_CALL",
        "DISPATCHING_COMMITTED",
        "GATEWAY_RETURNED",
        "COMPLETION_COMMITTED",
    ]


def test_gateway_exception_becomes_unknown_and_never_implicitly_resubmits() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("gateway return lost"))
    unknown = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert gateway.calls == 1
    assert unknown.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN
    assert unknown.broker_called is None
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.OUTCOME_UNKNOWN
    with pytest.raises(KernelOutboxDispatchError, match="not eligible"):
        _dispatcher(repository, gateway).dispatch_one(
            command_id=repository.outbox.command_id,
            observed_at_utc="2026-07-27T01:30:02Z",
            lease_expires_at_utc="2026-07-27T01:31:02Z",
        )
    assert gateway.calls == 1


def test_gateway_exception_persists_unknown_without_live_snapshot_read() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("gateway return lost"))
    gateway.snapshot_error = ConnectionError("OMS snapshot unavailable")
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert result.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN
    assert result.unknown_outcome_receipt is not None
    assert result.unknown_outcome_receipt.callback_watermark == "watermark_k2d_1"
    assert gateway.calls == 1


def test_pre_call_failure_is_bounded_retryable_without_broker_call() -> None:
    repository = _Repository()
    gateway = _Gateway()
    gateway.pre_call_error = KernelGatewayPreCallError(
        "MINIQMT_GATEWAY_UNAVAILABLE",
        "gateway is unavailable before broker call",
        context={"route_id": "gateway_route_k2d"},
    )
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert gateway.calls == 0
    assert result.status is BrokerCommandOutboxStatusV1.FAILED_RETRYABLE
    assert result.broker_called is False
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.RESERVED


def test_callback_watermark_failure_is_pre_call_and_never_calls_broker() -> None:
    repository = _Repository()
    gateway = _Gateway(ack=MiniQMTGatewayOrderAck(True, "broker_unused", "unused", {"broker_called": True}))
    gateway.snapshots = [GatewayReconciliationSnapshotV1(callback_watermark="valid_watermark", orders=(), trades=())]
    gateway.callback_watermark = lambda **values: " "  # type: ignore[method-assign]
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert result.status is BrokerCommandOutboxStatusV1.FAILED_RETRYABLE
    assert result.broker_called is False
    assert gateway.calls == 0


def test_submit_rejection_closes_mapping_without_forged_event_identity() -> None:
    repository = _Repository()
    gateway = _Gateway(
        ack=MiniQMTGatewayOrderAck(
            accepted=False,
            broker_order_id=None,
            message="rejected",
            raw={"broker_called": True},
        )
    )
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert result.status is BrokerCommandOutboxStatusV1.ACKED_REJECTED
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.BROKER_REJECTED
    assert repository.mapping.broker_identity_source_event_id is None


def test_unknown_unique_snapshot_reconciles_without_new_gateway_dispatch() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=(
                {
                    "broker_order_id": "broker_k2d_recovered",
                    "order_remark": repository.mapping.order_remark,
                    "status": "SUBMITTED",
                },
            ),
            trades=(),
        )
    ]
    result = KernelOutboxReconcilerV1(
        repository=repository,
        gateway=gateway,
        gateway_catalog=_catalog(),
    ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:10Z")
    assert gateway.calls == 1
    assert result.status is BrokerCommandOutboxStatusV1.ACKED
    assert result.ack_receipt_json.source.value == "RECONCILIATION"
    assert result.broker_order_id == "broker_k2d_recovered"
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.OUTCOME_UNKNOWN
    assert repository.mapping.broker_order_id is None
    assert repository.mapping.broker_identity_source_event_id is None


def test_reconcile_receipt_survives_cas_failure_and_retry_does_not_reread_gateway() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=(
                {
                    "broker_order_id": "broker_k2d_recovered_once",
                    "order_remark": repository.mapping.order_remark,
                    "status": "SUBMITTED",
                },
            ),
            trades=(),
        )
    ]
    reconciler = KernelOutboxReconcilerV1(repository=repository, gateway=gateway, gateway_catalog=_catalog())
    repository.fail_next_cas = True
    with pytest.raises(RuntimeError, match="injected reconciliation CAS failure"):
        reconciler.reconcile_one(
            command_id=repository.outbox.command_id,
            observed_at_utc="2026-07-27T01:30:02Z",
        )
    assert repository.reconciliation_receipts[1].broker_order_id == "broker_k2d_recovered_once"
    assert gateway.snapshot_calls == 1
    result = reconciler.reconcile_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:02Z",
    )
    assert result.status is BrokerCommandOutboxStatusV1.ACKED
    assert gateway.snapshot_calls == 1


def test_not_found_without_idempotency_stays_reconcile_only_then_fails_terminal() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    reconciler = KernelOutboxReconcilerV1(repository=repository, gateway=gateway, gateway_catalog=_catalog())
    observed_at = "2026-07-27T01:30:02Z"
    for index in range(1, 11):
        result = reconciler.reconcile_one(
            command_id=repository.outbox.command_id,
            observed_at_utc=observed_at,
        )
        if result.next_attempt_at_utc is not None:
            observed_at = result.next_attempt_at_utc
    assert gateway.calls == 1
    assert result.status is BrokerCommandOutboxStatusV1.FAILED_TERMINAL
    assert result.broker_called is None
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.OUTCOME_UNKNOWN
    assert tuple(repository.reconciliation_receipts) == tuple(range(1, 11))


def test_reconcile_cadence_rejects_early_poll_without_new_snapshot() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    reconciler = KernelOutboxReconcilerV1(repository=repository, gateway=gateway, gateway_catalog=_catalog())
    first = reconciler.reconcile_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:02Z",
    )
    assert first.next_attempt_at_utc is not None
    calls = gateway.snapshot_calls
    with pytest.raises(KernelOutboxDispatchError, match="cadence"):
        reconciler.reconcile_one(
            command_id=repository.outbox.command_id,
            observed_at_utc="2026-07-27T01:30:02Z",
        )
    assert gateway.snapshot_calls == calls


def test_exact_non_acceptance_allows_same_command_retry_only_when_catalog_says_idempotent() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway, catalog=_catalog(idempotent_submit=True)).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    gateway.snapshots = [GatewayReconciliationSnapshotV1(callback_watermark="watermark_k2d_2", orders=(), trades=())]
    result = KernelOutboxReconcilerV1(
        repository=repository,
        gateway=gateway,
        gateway_catalog=_catalog(idempotent_submit=True),
    ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:02Z")
    assert result.status is BrokerCommandOutboxStatusV1.FAILED_RETRYABLE
    assert result.broker_called is False
    assert result.non_acceptance_receipt is not None
    assert result.command_id == _command().command_id


def test_malformed_ack_after_dispatch_is_unknown_not_silent_rejection() -> None:
    repository = _Repository()
    gateway = _Gateway(
        ack=MiniQMTGatewayOrderAck(
            accepted=False,
            broker_order_id=None,
            message="missing explicit broker fact",
            raw={},
        )
    )
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert gateway.calls == 1
    assert result.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN
    assert result.broker_called is None


def test_non_boolean_ack_acceptance_is_unknown_not_coerced_to_success() -> None:
    repository = _Repository()
    gateway = _Gateway(
        ack=MiniQMTGatewayOrderAck(  # type: ignore[arg-type]
            accepted=1,
            broker_order_id="broker_wrong_bool",
            message="malformed",
            raw={"broker_called": True},
        )
    )
    result = _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    assert result.status is BrokerCommandOutboxStatusV1.OUTCOME_UNKNOWN
    assert result.broker_called is None


def test_production_gateway_adapter_preserves_exact_child_and_order_remark() -> None:
    mapping, _ = _initial_chain()
    gateway = FakeMiniQMTGateway()
    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=gateway,
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    command = _command()
    adapter.validate_pre_call(command=command, mapping=mapping, gateway_catalog=_catalog())
    ack = adapter.dispatch(command=command, mapping=mapping)
    assert ack.accepted is True
    assert ack.raw["broker_called"] is True
    assert len(gateway.submitted_orders) == 1
    child = gateway.submitted_orders[0]
    assert child.child_order_id == mapping.child_order_id
    assert child.runtime_id == command.runtime_id
    assert child.algo_instance_id == command.algo_instance_id
    assert child.parent_intent_id == command.parent_intent_id
    assert child.symbol == command.symbol
    assert child.quantity == command.quantity
    assert child.price == float(command.price_decimal)
    assert child.metadata["order_remark"] == mapping.order_remark
    assert child.metadata["deterministic_client_order_ref"] == mapping.deterministic_client_order_ref
    snapshot = adapter.reconciliation_snapshot(runtime_id=command.runtime_id)
    assert snapshot.callback_watermark == f"watermark_{command.runtime_id}"
    assert snapshot.orders[0]["broker_order_id"] == ack.broker_order_id
    assert snapshot.orders[0]["price"] == "10.25"


def test_production_gateway_adapter_fails_loud_when_reconciliation_queries_are_missing() -> None:
    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=QmtClientMiniQMTGateway(qmt_client=object()),
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_SYNC_ORDERS_UNAVAILABLE"):
        adapter.reconciliation_snapshot(runtime_id="runtime_k2d")

    class OrdersOnlyClient:
        @staticmethod
        def get_orders(*, cancelable_only: bool) -> list[dict[str, Any]]:
            assert cancelable_only is False
            return []

    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=QmtClientMiniQMTGateway(qmt_client=OrdersOnlyClient()),
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    with pytest.raises(MiniQMTGatewayEventSourceError, match="MINIQMT_EVENT_LOOP_SYNC_TRADES_UNAVAILABLE"):
        adapter.reconciliation_snapshot(runtime_id="runtime_k2d")


def test_production_gateway_adapter_cancel_rejection_does_not_forge_accepted_identity() -> None:
    class RejectCancelGateway(FakeMiniQMTGateway):
        def cancel_child_order(self, order, *, reason):  # type: ignore[no-untyped-def]
            return MiniQMTGatewayCancelAck(
                accepted=False,
                broker_order_id=order.broker_order_id,
                message="cancel rejected",
                raw={"broker_called": True, "reason": reason},
            )

    mapping = _accepted_mapping()
    command = _cancel_command(mapping)
    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=RejectCancelGateway(),
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    adapter.validate_pre_call(command=command, mapping=mapping, gateway_catalog=_catalog())
    ack = adapter.dispatch(command=command, mapping=mapping)
    assert isinstance(ack, MiniQMTGatewayCancelAck)
    assert ack.accepted is False
    assert ack.broker_order_id is None
    assert ack.raw["broker_called"] is True


def test_production_gateway_adapter_rejects_cancel_ownership_drift_before_call() -> None:
    mapping = _accepted_mapping()
    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=FakeMiniQMTGateway(),
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    with pytest.raises(KernelGatewayPreCallError, match="accepted durable broker identity"):
        adapter.validate_pre_call(
            command=_cancel_command(mapping, owned_broker_order_id="broker_not_owned"),
            mapping=mapping,
            gateway_catalog=_catalog(),
        )


def test_gateway_authority_and_exact_cancel_fail_before_broker_call() -> None:
    mapping, _ = _initial_chain()
    adapter = MiniQMTKernelGatewayAdapterV1(
        gateway=FakeMiniQMTGateway(),
        callback_watermark_provider=lambda runtime_id: f"watermark_{runtime_id}",
    )
    with pytest.raises(KernelGatewayPreCallError, match="approved B0"):
        adapter.validate_pre_call(command=_command(), mapping=mapping, gateway_catalog=_catalog(quote_source="OTHER"))
    accepted = _accepted_mapping()
    with pytest.raises(KernelGatewayPreCallError, match="exact-order-id"):
        adapter.validate_pre_call(
            command=_cancel_command(accepted),
            mapping=accepted,
            gateway_catalog=_catalog(exact_cancel=False),
        )


def test_reconciliation_rejects_conflicting_aliases_without_mutating_durable_state() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    before = (repository.mapping, repository.outbox)
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=(
                {
                    "broker_order_id": "broker_a",
                    "order_id": "broker_b",
                    "order_remark": repository.mapping.order_remark,
                },
            ),
            trades=(),
        )
    ]
    with pytest.raises(KernelOutboxDispatchError, match="conflicting identity aliases"):
        KernelOutboxReconcilerV1(
            repository=repository,
            gateway=gateway,
            gateway_catalog=_catalog(),
        ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:02Z")
    assert (repository.mapping, repository.outbox) == before


def test_reconciliation_normalizes_numeric_identity_and_nested_transport_values() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=(
                {
                    "broker_order_id": 123456,
                    "order_remark": repository.mapping.order_remark,
                    "levels": [{"price": 10.25, "volume": 100}],
                },
            ),
            trades=(),
        )
    ]
    result = KernelOutboxReconcilerV1(
        repository=repository,
        gateway=gateway,
        gateway_catalog=_catalog(),
    ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:02Z")
    assert result.status is BrokerCommandOutboxStatusV1.ACKED
    assert result.broker_order_id == "123456"


def test_cancel_reconciliation_matches_numeric_broker_identity_without_secondary_alias() -> None:
    submit = _command()
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id="slot_k2d",
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=2,
        broker_order_id="123456",
        broker_identity_source_event_id="event_k2d_cancel_numeric",
        last_order_event_id="event_k2d_cancel_numeric",
        last_trade_event_id=None,
        updated_by_event_id="event_k2d_cancel_numeric",
        created_at_utc="2026-07-27T01:30:00Z",
        updated_at_utc="2026-07-27T01:30:01Z",
    )
    command = _cancel_command(mapping)
    outbox = BrokerCommandOutboxV1.create(
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
        created_at_utc="2026-07-27T01:30:01Z",
        updated_at_utc="2026-07-27T01:30:01Z",
        closed_at_utc=None,
    )
    repository = _Repository((mapping, outbox))
    gateway = _Gateway(error=TimeoutError("cancel return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:02Z",
        lease_expires_at_utc="2026-07-27T01:31:02Z",
    )
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=({"broker_order_id": 123456, "status": "SUBMITTED"},),
            trades=(),
        )
    ]
    result = KernelOutboxReconcilerV1(
        repository=repository,
        gateway=gateway,
        gateway_catalog=_catalog(),
    ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:03Z")
    assert result.status is BrokerCommandOutboxStatusV1.ACKED
    assert result.broker_order_id == "123456"


def test_reconciliation_rejects_negative_numeric_broker_identity() -> None:
    repository = _Repository()
    gateway = _Gateway(error=TimeoutError("return lost"))
    _dispatcher(repository, gateway).dispatch_one(
        command_id=repository.outbox.command_id,
        observed_at_utc="2026-07-27T01:30:01Z",
        lease_expires_at_utc="2026-07-27T01:31:01Z",
    )
    gateway.snapshots = [
        GatewayReconciliationSnapshotV1(
            callback_watermark="watermark_k2d_2",
            orders=(
                {
                    "broker_order_id": -1,
                    "order_remark": repository.mapping.order_remark,
                },
            ),
            trades=(),
        )
    ]
    with pytest.raises(KernelOutboxDispatchError, match="cannot be negative"):
        KernelOutboxReconcilerV1(
            repository=repository,
            gateway=gateway,
            gateway_catalog=_catalog(),
        ).reconcile_one(command_id=repository.outbox.command_id, observed_at_utc="2026-07-27T01:30:02Z")


def _read_command(outbox: BrokerCommandOutboxV1) -> BrokerCommandV2:
    return BrokerCommandV2.model_validate_json(
        json.dumps(thaw_json_v1(outbox.payload_json), sort_keys=True, separators=(",", ":"))
    )
