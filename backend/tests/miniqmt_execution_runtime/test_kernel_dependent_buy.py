"""Direct K6-B final dependent-BUY state-machine coverage."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from backend.services.miniqmt_execution_runtime.kernel_dependent_buy import (
    build_dependent_buy_release_decision_v2,
    build_dependent_buy_trigger_bundle_v2,
    cash_ledger_fact_sha256_v1,
    evaluate_dependent_buy_decision_v2,
    qmt_trade_ledger_fact_sha256_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_callback_events import (
    build_kernel_order_event_payload_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_dependent_buy_repository import (
    KernelDependentBuyRepositoryMixin,
    _assert_existing_decision_owner_set_v2,
    _event_trigger_type,
    _strict_event_payload,
    _strict_model,
)
from backend.services.miniqmt_execution_runtime.kernel_product_contracts import (
    DependentBuyCoordinationStatusV1,
    DependentBuyCoordinationV2,
    DependentBuyDecisionV1,
    DependentBuyDependencyStatusV1,
    DependentBuyLedgerObservationV2,
    DependentBuySellDependencyV2,
    DependentBuySettledProceedsRefV2,
    DependentBuyTriggerEventRefV1,
    DependentBuyTriggerTypeV1,
    ProductCommandChildMappingV1,
    ProductCommandDispositionV3,
)
from backend.services.miniqmt_execution_runtime.kernel_product_materialization_repository import (
    _coordination_v2 as _materialized_coordination_v2,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import PostgresMiniQMTKernelRepository
from backend.services.miniqmt_execution_runtime.kernel_repository_common import KernelRepositoryConflict
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    EventSourceV2,
    EventTypeV2,
    RuntimeEventEnvelopeV2,
    SideV1,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_product_contracts import (
    _v3_authority,
    _v3_item,
)


NOW = datetime(2026, 8, 3, 1, 30, tzinfo=timezone.utc)
SESSION_SHA = "e" * 64


def _runtime_event(
    event_type: EventTypeV2,
    *,
    source: EventSourceV2 | None = None,
    side: SideV1 = SideV1.SELL,
) -> RuntimeEventEnvelopeV2:
    if event_type is EventTypeV2.ORDER:
        payload = build_kernel_order_event_payload_v1(
            raw_payload={"order_status": 48},
            order_event_id="order_event_k6",
            runtime_id="runtime_k6",
            algo_instance_id="algo_sell",
            parent_intent_id="intent_sell",
            strategy_slot_id="slot_sell",
            mapping_id="mapping_sell",
            command_id="command_sell",
            local_vt_orderid="local_sell",
            broker_order_id="broker_sell",
            symbol="600000.SH",
            side=side,
            requested_quantity=100,
        )
        values = {
            "source": EventSourceV2.QMT_GATEWAY_CALLBACK,
            "symbol": "600000.SH",
            "payload_schema_version": "miniqmt_order_event_v1",
            "payload": payload.model_dump(mode="json"),
            "source_identity": {"order_event_id": payload.order_event_id},
        }
    elif event_type is EventTypeV2.ACCOUNT:
        values = {
            "source": EventSourceV2.QMT_OMS_PROJECTION,
            "symbol": None,
            "payload_schema_version": "miniqmt_account_projection_v1",
            "payload": {"kind": "ACCOUNT"},
            "source_identity": {"projection_version": "account_v1", "projection_sha256": "b" * 64},
        }
    elif event_type is EventTypeV2.EOD:
        values = {
            "source": EventSourceV2.EXCHANGE_SESSION_CLOCK,
            "symbol": None,
            "payload_schema_version": "miniqmt_eod_event_v1",
            "payload": {"kind": "EOD"},
            "source_identity": {
                "runtime_id": "runtime_k6",
                "trade_date": "2026-08-03",
                "session_epoch": "session_k6",
            },
        }
    elif event_type is EventTypeV2.TICK:
        values = {
            "source": EventSourceV2.B0_QUOTE_V2,
            "symbol": "600000.SH",
            "payload_schema_version": "miniqmt_market_data_view_v2",
            "payload": {"kind": "TICK"},
            "source_identity": {"market_data_id": "market_k6"},
        }
    else:
        raise AssertionError(f"test event type is not implemented: {event_type}")
    if source is not None:
        values["source"] = source
    return RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k6",
        sequence=7,
        event_type=event_type,
        event_time_utc=NOW,
        monotonic_ns=None,
        correlation={},
        **values,
    )


def test_k6b_public_repository_seam_accepts_only_durable_event_identity() -> None:
    public_methods = set(dir(PostgresMiniQMTKernelRepository))
    assert "coordinate_dependent_buys_for_event_atomic_v2" in public_methods
    assert "write_dependent_buy_trigger_bundle_atomic_v2" not in public_methods


@pytest.mark.parametrize(
    "values",
    (
        {"event_id": " bad ", "worker_id": "worker", "process_incarnation_id": "process"},
        {"event_id": "", "worker_id": "worker", "process_incarnation_id": "process"},
        {"event_id": "event", "worker_id": " bad ", "process_incarnation_id": "process"},
        {"event_id": "event", "worker_id": "", "process_incarnation_id": "process"},
        {"event_id": "event", "worker_id": "worker", "process_incarnation_id": " bad "},
        {"event_id": "event", "worker_id": "worker", "process_incarnation_id": ""},
    ),
)
def test_k6b_public_repository_seam_rejects_noncanonical_execution_context(values: dict[str, str]) -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=lambda: None)
    with pytest.raises(ValueError, match="strict|identity"):
        repository.coordinate_dependent_buys_for_event_atomic_v2(**values)


@pytest.mark.parametrize("decision_id", ("short", "A" * 64, "g" * 64, None, True))
def test_k6b_readback_rejects_non_sha_decision_identity(decision_id: object) -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=lambda: None)
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        repository.read_dependent_buy_release_bundle_v2(decision_id)  # type: ignore[arg-type]


def test_k6b_event_trigger_reader_preserves_exact_type_source_contract() -> None:
    order = _runtime_event(EventTypeV2.ORDER)
    account = _runtime_event(EventTypeV2.ACCOUNT)
    eod = _runtime_event(EventTypeV2.EOD)
    trade = order.model_copy(update={"event_type": EventTypeV2.TRADE})
    reconcile = order.model_copy(
        update={"event_type": EventTypeV2.RECONCILE, "source": EventSourceV2.QMT_OMS_RECONCILIATION}
    )

    assert _event_trigger_type(trade) is DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED
    assert _event_trigger_type(order) is DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL
    assert _event_trigger_type(reconcile) is DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL
    assert _event_trigger_type(account) is DependentBuyTriggerTypeV1.ACCOUNT_REFRESHED
    assert _event_trigger_type(eod) is DependentBuyTriggerTypeV1.SESSION_EOD

    invalid_sources = (
        trade.model_copy(update={"source": EventSourceV2.QMT_OMS_RECONCILIATION}),
        order.model_copy(update={"source": EventSourceV2.QMT_OMS_RECONCILIATION}),
        reconcile.model_copy(update={"source": EventSourceV2.QMT_GATEWAY_CALLBACK}),
        account.model_copy(update={"source": EventSourceV2.EXCHANGE_SESSION_CLOCK}),
        eod.model_copy(update={"source": EventSourceV2.QMT_OMS_PROJECTION}),
    )
    for event in invalid_sources:
        with pytest.raises(KernelRepositoryConflict, match="source"):
            _event_trigger_type(event)
    with pytest.raises(KernelRepositoryConflict, match="not one legal"):
        _event_trigger_type(_runtime_event(EventTypeV2.TICK))


def test_k6b_strict_payload_and_dependency_reader_reject_unregistered_or_wrong_owner() -> None:
    order = _runtime_event(EventTypeV2.ORDER)
    payload = _strict_event_payload(order)
    dependency = _dependency(DependentBuyDependencyStatusV1.OPEN)

    assert payload.parent_intent_id == dependency.sell_parent_intent_id
    assert KernelDependentBuyRepositoryMixin._event_matches_dependency(order, dependency) is True
    assert (
        KernelDependentBuyRepositoryMixin._event_matches_dependency(_runtime_event(EventTypeV2.ACCOUNT), dependency)
        is False
    )
    assert _strict_event_payload(_runtime_event(EventTypeV2.ACCOUNT)) is None
    assert _strict_event_payload(_runtime_event(EventTypeV2.EOD)) is None
    with pytest.raises(KernelRepositoryConflict, match="not one K6-B trigger"):
        _strict_event_payload(_runtime_event(EventTypeV2.TICK))

    wrong_owner = dependency.model_copy(update={"sell_parent_intent_id": "intent_other"})
    assert KernelDependentBuyRepositoryMixin._event_matches_dependency(order, wrong_owner) is False


def test_k6b_account_projection_match_uses_exact_v3_authority() -> None:
    authority_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    account_refs = tuple(
        ref
        for ref in authority_item.evaluation_evidence.execution_projection_set.ordered_projection_refs
        if ref.projection_type.value == "ACCOUNT"
    )
    assert len(account_refs) == 1
    exact = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k6",
        sequence=7,
        event_type=EventTypeV2.ACCOUNT,
        event_time_utc=NOW,
        monotonic_ns=None,
        source=EventSourceV2.QMT_OMS_PROJECTION,
        symbol=None,
        payload_schema_version="miniqmt_account_projection_v1",
        payload={"kind": "ACCOUNT"},
        source_identity={
            "projection_version": account_refs[0].projection_version,
            "projection_sha256": authority_item.account_projection_sha256,
        },
        correlation={},
    )
    assert KernelDependentBuyRepositoryMixin._account_event_matches_authority(exact, authority_item) is True
    assert (
        KernelDependentBuyRepositoryMixin._account_event_matches_authority(
            _runtime_event(EventTypeV2.ACCOUNT), authority_item
        )
        is False
    )
    assert (
        KernelDependentBuyRepositoryMixin._account_event_matches_authority(
            _runtime_event(EventTypeV2.ORDER), authority_item
        )
        is True
    )

    no_account_authority = authority_item.model_copy(
        update={
            "evaluation_evidence": authority_item.evaluation_evidence.model_copy(
                update={
                    "execution_projection_set": authority_item.evaluation_evidence.execution_projection_set.model_copy(
                        update={"ordered_projection_refs": ()}
                    )
                }
            )
        }
    )
    with pytest.raises(KernelRepositoryConflict, match="one exact account projection"):
        KernelDependentBuyRepositoryMixin._account_event_matches_authority(exact, no_account_authority)


def test_k6b_repository_validation_helpers_fail_loud_before_database_write() -> None:
    with pytest.raises(TypeError, match="requires DependentBuyCoordinationV2"):
        _strict_model(DependentBuyCoordinationV2, {}, stage="TEST")
    assert _strict_model(DependentBuyCoordinationV2, _coordination(), stage="TEST") == _coordination()

    repository = PostgresMiniQMTKernelRepository(conn_factory=lambda: None)
    with pytest.raises(ValueError, match="lock clause"):
        repository._read_locked_dependencies_v2(object(), coordination=_coordination(), lock_clause=" FOR KEY SHARE")

    class DuplicateCursor:
        def execute(self, _query: str, _values: object) -> None:
            return None

        def fetchall(self) -> list[dict[str, str]]:
            return [{"coordination_id": "coordination_b"}, {"coordination_id": "coordination_b"}]

    with pytest.raises(KernelRepositoryConflict, match="noncanonical or duplicated"):
        repository._candidate_coordination_ids_cursor(DuplicateCursor(), event=_runtime_event(EventTypeV2.ORDER))

    _assert_existing_decision_owner_set_v2(("coordination_a",), ("coordination_a",))
    with pytest.raises(KernelRepositoryConflict, match="exact candidates"):
        _assert_existing_decision_owner_set_v2(
            ("coordination_a", "coordination_unowned"),
            ("coordination_a",),
        )
    with pytest.raises(KernelRepositoryConflict, match="exact candidates"):
        _assert_existing_decision_owner_set_v2((), ("coordination_a",))


def test_k6b_repository_authority_readers_reject_missing_or_drifting_durable_rows() -> None:
    repository = PostgresMiniQMTKernelRepository(conn_factory=lambda: None)
    coordination = _coordination(DependentBuyDependencyStatusV1.OPEN)

    class EmptyCursor:
        def execute(self, _query: str, _values: object) -> None:
            return None

        def fetchone(self) -> None:
            return None

        def fetchall(self) -> list[object]:
            return []

    empty = EmptyCursor()
    with pytest.raises(KeyError):
        repository._read_k6b_event_cursor(empty, "event_missing")
    with pytest.raises(KernelRepositoryConflict, match="route owner"):
        repository._read_route_owner_cursor(empty, coordination)
    with pytest.raises(KernelRepositoryConflict, match="exchange-session authority"):
        repository._read_session_authority_cursor(empty, coordination)
    with pytest.raises(KernelRepositoryConflict, match="virtual account"):
        repository._read_virtual_account_cursor(empty, coordination)
    with pytest.raises(KernelRepositoryConflict, match="missing, extra, or noncanonical"):
        repository._read_locked_dependencies_v2(empty, coordination=coordination, lock_clause=" FOR SHARE")
    with pytest.raises(KernelRepositoryConflict, match="removed or changed"):
        repository._read_settled_refs_cursor(
            empty,
            coordination=_coordination(),
            dependency=_coordination().ordered_sell_dependencies[0],
        )

    class ScalarDriftCursor(EmptyCursor):
        def fetchone(self) -> dict[str, object]:
            event = _runtime_event(EventTypeV2.ORDER)
            return {
                "event_id": "different_event_id",
                "runtime_id": event.runtime_id,
                "sequence": event.sequence,
                "payload": event.model_dump(mode="json"),
            }

    with pytest.raises(KernelRepositoryConflict, match="scalar columns"):
        repository._read_k6b_event_cursor(ScalarDriftCursor(), "event_order")

    class MissingCashCursor(EmptyCursor):
        def __init__(self) -> None:
            self._read_count = 0

        def fetchall(self) -> list[object]:
            self._read_count += 1
            return [{"trade_id": "trade_without_cash"}] if self._read_count == 1 else []

    with pytest.raises(KernelRepositoryConflict, match="exactly one cash-ledger fact"):
        repository._read_settled_refs_cursor(
            MissingCashCursor(), coordination=coordination, dependency=coordination.ordered_sell_dependencies[0]
        )

    with pytest.raises(KernelRepositoryConflict, match="not a SELL fact"):
        repository._candidate_coordination_ids_cursor(empty, event=_runtime_event(EventTypeV2.ORDER, side=SideV1.BUY))


def _ref(*, trade_id: str = "trade_sell", sequence: int = 5) -> DependentBuySettledProceedsRefV2:
    return DependentBuySettledProceedsRefV2.create(
        broker_trade_id=trade_id,
        qmt_trade_ledger_id=f"trade_ledger_{trade_id}",
        qmt_trade_account_id="account_k6",
        qmt_trade_fact_sha256="a" * 64,
        cash_ledger_id=f"cash_{sequence}",
        cash_ledger_sequence=sequence,
        cash_ledger_fact_sha256="b" * 64,
        strategy_id="strategy_k6",
        runtime_id="runtime_k6",
        trade_date="2026-08-03",
        sell_parent_intent_id="intent_sell",
    )


def _dependency(
    status: DependentBuyDependencyStatusV1 = DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
) -> DependentBuySellDependencyV2:
    return DependentBuySellDependencyV2.create(
        runtime_id="runtime_k6",
        strategy_id="strategy_k6",
        sell_parent_intent_id="intent_sell",
        sell_algo_instance_id="algo_sell",
        latest_order_fact_id="order_fact_sell",
        latest_order_fact_sha256="c" * 64,
        ordered_settled_proceeds_refs=(_ref(),) if status is DependentBuyDependencyStatusV1.PROCEEDS_SETTLED else (),
        dependency_status=status,
    )


def _coordination(
    status: DependentBuyDependencyStatusV1 = DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
) -> DependentBuyCoordinationV2:
    return DependentBuyCoordinationV2.create(
        runtime_id="runtime_k6",
        binding_id="binding_k6",
        trade_date="2026-08-03",
        strategy_id="strategy_k6",
        buy_algo_instance_id="algo_buy",
        buy_parent_intent_id="intent_buy",
        required_cash="800",
        virtual_account_id="account_k6",
        session_authority_sha256=SESSION_SHA,
        release_command_id="command_buy",
        release_transition_id="transition_buy",
        release_command_authority_item_sha256="d" * 64,
        release_command_payload_sha256="f" * 64,
        ordered_sell_dependencies=(_dependency(status),),
        status=DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS,
        decision_sequence=0,
        last_decision_sha256=None,
        released_command_id=None,
        released_outbox_id=None,
        row_version=1,
        lease_worker_id=None,
        lease_process_incarnation_id=None,
        lease_epoch=0,
        lease_expires_at_utc=None,
        created_at_utc=NOW,
        updated_at_utc=NOW,
    )


def _trigger(kind: DependentBuyTriggerTypeV1) -> DependentBuyTriggerEventRefV1:
    return DependentBuyTriggerEventRefV1.create(
        runtime_id="runtime_k6",
        event_id=f"event_{kind.value.lower()}",
        event_type=kind,
        event_sequence=7,
        source_fact_type="qmt_strategy.test_authority",
        source_fact_id=f"fact_{kind.value.lower()}",
        source_fact_sha256="1" * 64,
        observed_at_utc=NOW,
    )


def _ledger(
    coordination: DependentBuyCoordinationV2,
    *,
    available_cash: str,
) -> DependentBuyLedgerObservationV2:
    refs = tuple(
        ref for dependency in coordination.ordered_sell_dependencies for ref in dependency.ordered_settled_proceeds_refs
    )
    return DependentBuyLedgerObservationV2.create(
        runtime_id=coordination.runtime_id,
        strategy_id=coordination.strategy_id,
        trade_date=coordination.trade_date,
        virtual_account_id="account_k6",
        virtual_account_updated_at_utc=NOW,
        latest_cash_ledger_sequence=max((item.cash_ledger_sequence for item in refs), default=0),
        ledger_as_of_utc=NOW,
        available_cash=available_cash,
        required_cash=coordination.required_cash,
        ordered_settled_proceeds_refs=refs,
        freshness_session_authority_sha256=SESSION_SHA,
    )


@pytest.mark.parametrize(
    ("trigger_type", "available_cash", "dependency_status", "expected"),
    [
        (
            DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
            "800",
            DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
            DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX,
        ),
        (
            DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
            "799.99",
            DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
            DependentBuyDecisionV1.WAIT,
        ),
        (
            DependentBuyTriggerTypeV1.ACCOUNT_REFRESHED,
            "900",
            DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
            DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX,
        ),
        (
            DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL,
            "799",
            DependentBuyDependencyStatusV1.TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS,
            DependentBuyDecisionV1.BLOCK,
        ),
        (
            DependentBuyTriggerTypeV1.SESSION_EOD,
            "0",
            DependentBuyDependencyStatusV1.OPEN,
            DependentBuyDecisionV1.EOD_RESIDUAL,
        ),
    ],
)
def test_final_k6_dependent_buy_state_machine_uses_only_durable_authority(
    trigger_type: DependentBuyTriggerTypeV1,
    available_cash: str,
    dependency_status: DependentBuyDependencyStatusV1,
    expected: DependentBuyDecisionV1,
) -> None:
    coordination = _coordination(dependency_status)
    decision, _ = evaluate_dependent_buy_decision_v2(
        coordination=coordination,
        trigger=_trigger(trigger_type),
        ledger_observation=_ledger(coordination, available_cash=available_cash),
        session_authority_sha256=SESSION_SHA,
    )
    assert decision is expected


def test_release_decision_reuses_original_transition_and_requires_exact_authority() -> None:
    coordination = _coordination()
    trigger = _trigger(DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED)
    ledger = _ledger(coordination, available_cash="900")
    decision, reason = evaluate_dependent_buy_decision_v2(
        coordination=coordination,
        trigger=trigger,
        ledger_observation=ledger,
        session_authority_sha256=SESSION_SHA,
    )
    receipt = build_dependent_buy_release_decision_v2(
        coordination=coordination,
        trigger=trigger,
        ledger_observation=ledger,
        decision=decision,
        reason_code=reason,
        decided_at_utc=NOW,
        worker_id="worker_k6",
        process_incarnation_id="process_k6",
        lease_epoch=1,
        session_authority_sha256=SESSION_SHA,
        release_event_id=trigger.event_id,
        release_command_authority_set_sha256="2" * 64,
    )
    assert receipt.release_transition_id == coordination.release_transition_id
    assert receipt.release_event_id == trigger.event_id
    assert receipt.decision_sequence == 1
    with pytest.raises(ValueError, match="pure state-machine"):
        build_dependent_buy_release_decision_v2(
            coordination=coordination,
            trigger=trigger,
            ledger_observation=ledger,
            decision=DependentBuyDecisionV1.WAIT,
            reason_code="MINIQMT_DEPENDENT_BUY_WAITING_SELL_PROCEEDS",
            decided_at_utc=NOW,
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=1,
            session_authority_sha256=SESSION_SHA,
        )


def test_terminal_order_with_one_open_dependency_remains_waiting() -> None:
    coordination = _coordination(DependentBuyDependencyStatusV1.OPEN)
    decision, reason = evaluate_dependent_buy_decision_v2(
        coordination=coordination,
        trigger=_trigger(DependentBuyTriggerTypeV1.SELL_ORDER_TERMINAL),
        ledger_observation=_ledger(coordination, available_cash="0"),
        session_authority_sha256=SESSION_SHA,
    )
    assert (decision, reason) == (
        DependentBuyDecisionV1.WAIT,
        "MINIQMT_DEPENDENT_BUY_WAITING_SELL_PROCEEDS",
    )


def test_state_machine_rejects_stale_session_or_terminal_reopen() -> None:
    coordination = _coordination()
    ledger = _ledger(coordination, available_cash="900")
    with pytest.raises(ValueError, match="session authority"):
        evaluate_dependent_buy_decision_v2(
            coordination=coordination,
            trigger=_trigger(DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED),
            ledger_observation=ledger,
            session_authority_sha256="3" * 64,
        )
    terminal_payload = coordination.model_dump(
        mode="python",
        exclude={
            "schema_version",
            "coordination_sha256",
            "status",
            "decision_sequence",
            "last_decision_sha256",
            "released_command_id",
            "released_outbox_id",
            "row_version",
            "updated_at_utc",
        },
    )
    terminal = DependentBuyCoordinationV2.create(
        **terminal_payload,
        status=DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
        decision_sequence=1,
        last_decision_sha256="4" * 64,
        released_command_id=None,
        released_outbox_id=None,
        row_version=2,
        updated_at_utc="2026-08-03T01:31:00Z",
    )
    with pytest.raises(ValueError, match="terminal coordination"):
        evaluate_dependent_buy_decision_v2(
            coordination=terminal,
            trigger=_trigger(DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED),
            ledger_observation=_ledger(coordination, available_cash="900"),
            session_authority_sha256=SESSION_SHA,
        )


def test_settled_proceeds_source_hashes_are_full_row_and_canonical() -> None:
    trade = {
        "trade_id": "trade_sell",
        "intent_id": "intent_sell",
        "strategy_id": "strategy_k6",
        "qmt_order_id": "order_sell",
        "qmt_order_sysid": None,
        "symbol": "600000.SH",
        "side": "SELL",
        "price": Decimal("10.500000"),
        "quantity": 100,
        "amount": Decimal("1050.000000"),
        "commission": Decimal("1.000000"),
        "trade_date": date(2026, 8, 3),
        "account_id": "account_k6",
        "trade_time": NOW,
        "order_remark": "order_sell",
        "raw_json": {"broker": "qmt", "fill": 1},
    }
    cash = {
        "cash_id": "cash_5",
        "cash_sequence": 5,
        "strategy_id": "strategy_k6",
        "account_id": "account_k6",
        "trade_date": date(2026, 8, 3),
        "entry_type": "SELL_FILL",
        "cash_delta": Decimal("1049.000000"),
        "cash_after": Decimal("900.000000"),
        "frozen_delta": Decimal("0"),
        "frozen_after": Decimal("0"),
        "intent_id": "intent_sell",
        "trade_id": "trade_sell",
        "symbol": "600000.SH",
        "reason": "sell-fill",
        "metadata": {"source": "qmt"},
        "created_at": NOW,
    }
    baseline = (qmt_trade_ledger_fact_sha256_v1(trade), cash_ledger_fact_sha256_v1(cash))
    assert baseline == (qmt_trade_ledger_fact_sha256_v1(dict(trade)), cash_ledger_fact_sha256_v1(dict(cash)))
    drifted_trade = {**trade, "commission": Decimal("1.100000")}
    drifted_cash = {**cash, "metadata": {"source": "qmt", "revision": 2}}
    assert qmt_trade_ledger_fact_sha256_v1(drifted_trade) != baseline[0]
    assert cash_ledger_fact_sha256_v1(drifted_cash) != baseline[1]


def test_settled_proceeds_source_hash_rejects_partial_or_wrong_cash_fact() -> None:
    with pytest.raises(ValueError, match="columns differ"):
        qmt_trade_ledger_fact_sha256_v1({"trade_id": "trade_only"})
    with pytest.raises(ValueError, match="entry_type"):
        cash_ledger_fact_sha256_v1(
            {
                "cash_id": "cash_5",
                "cash_sequence": 5,
                "strategy_id": "strategy_k6",
                "account_id": "account_k6",
                "trade_date": date(2026, 8, 3),
                "entry_type": "UNKNOWN",
                "cash_delta": "1",
                "cash_after": "1",
                "frozen_delta": "0",
                "frozen_after": "0",
                "intent_id": "intent_sell",
                "trade_id": "trade_sell",
                "symbol": "600000.SH",
                "reason": None,
                "metadata": {},
                "created_at": NOW,
            }
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("side", "BUY", "side"),
        ("quantity", 0, "quantity"),
        ("quantity", True, "quantity"),
        ("qmt_order_sysid", 1, "qmt_order_sysid"),
        ("raw_json", [], "raw_json"),
        ("trade_date", "2026-08-03", "trade_date"),
        ("trade_id", " bad ", "trade_id"),
    ),
)
def test_trade_fact_reader_rejects_non_authoritative_row_types(field: str, value: object, message: str) -> None:
    row = {
        "trade_id": "trade_sell",
        "intent_id": "intent_sell",
        "strategy_id": "strategy_k6",
        "qmt_order_id": "order_sell",
        "qmt_order_sysid": None,
        "symbol": "600000.SH",
        "side": "SELL",
        "price": Decimal("10.5"),
        "quantity": 100,
        "amount": Decimal("1050"),
        "commission": Decimal("0"),
        "trade_date": date(2026, 8, 3),
        "account_id": "account_k6",
        "trade_time": NOW,
        "order_remark": "remark",
        "raw_json": {},
    }
    row[field] = value
    with pytest.raises(ValueError, match=message):
        qmt_trade_ledger_fact_sha256_v1(row)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("cash_sequence", 0, "sequence"),
        ("cash_sequence", True, "sequence"),
        ("entry_type", "BUY_FILL", "SELL_FILL"),
        ("intent_id", " bad ", "intent_id"),
        ("reason", 1, "reason"),
        ("metadata", [], "metadata"),
        ("trade_date", "2026-08-03", "trade_date"),
    ),
)
def test_cash_fact_reader_rejects_non_authoritative_row_types(field: str, value: object, message: str) -> None:
    row = {
        "cash_id": "cash_1",
        "cash_sequence": 1,
        "strategy_id": "strategy_k6",
        "account_id": "account_k6",
        "trade_date": date(2026, 8, 3),
        "entry_type": "SELL_FILL",
        "cash_delta": Decimal("1050"),
        "cash_after": Decimal("1050"),
        "frozen_delta": Decimal("0"),
        "frozen_after": Decimal("0"),
        "intent_id": "intent_sell",
        "trade_id": "trade_sell",
        "symbol": "600000.SH",
        "reason": None,
        "metadata": {},
        "created_at": NOW,
    }
    row[field] = value
    with pytest.raises(ValueError, match=message):
        cash_ledger_fact_sha256_v1(row)


def test_trigger_bundle_requires_full_authority_and_monotonic_dependency_successor() -> None:
    authority_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    authority_set = _v3_authority((authority_item,))
    coordination = _materialized_coordination_v2(authority_item, created_at_utc=NOW)
    mapping = ProductCommandChildMappingV1.create_deferred(
        authority_item=authority_item,
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    observed_dependency = DependentBuySellDependencyV2.create(
        **{
            **coordination.ordered_sell_dependencies[0].model_dump(
                mode="python",
                exclude={"schema_version", "dependency_sha256"},
            ),
            "dependency_status": DependentBuyDependencyStatusV1.PROCEEDS_SETTLED,
        }
    )
    ledger = DependentBuyLedgerObservationV2.create(
        runtime_id=coordination.runtime_id,
        strategy_id=coordination.strategy_id,
        trade_date=coordination.trade_date,
        virtual_account_id=coordination.virtual_account_id,
        virtual_account_updated_at_utc=NOW,
        latest_cash_ledger_sequence=11,
        ledger_as_of_utc=NOW,
        available_cash="1050",
        required_cash=coordination.required_cash,
        ordered_settled_proceeds_refs=observed_dependency.ordered_settled_proceeds_refs,
        freshness_session_authority_sha256=coordination.session_authority_sha256,
    )
    receipt, successor, successor_mapping, outbox = build_dependent_buy_trigger_bundle_v2(
        coordination=coordination,
        authority_item=authority_item,
        authority_set=authority_set,
        mapping=mapping,
        observed_dependencies=(observed_dependency,),
        trigger=DependentBuyTriggerEventRefV1.create(
            runtime_id=coordination.runtime_id,
            event_id="event_trade_release",
            event_type=DependentBuyTriggerTypeV1.SELL_TRADE_SETTLED,
            event_sequence=3,
            source_fact_type="qmt_strategy.trade_ledger",
            source_fact_id="trade_1",
            source_fact_sha256=observed_dependency.ordered_settled_proceeds_refs[0].qmt_trade_fact_sha256,
            observed_at_utc=NOW,
        ),
        ledger_observation=ledger,
        session_authority_sha256=coordination.session_authority_sha256,
        decided_at_utc=NOW + timedelta(seconds=1),
        worker_id="worker_k6",
        process_incarnation_id="process_k6",
        lease_epoch=1,
        lease_expires_at_utc="2026-08-03T01:31:00Z",
    )
    assert receipt.decision is DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX
    assert successor.status is DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
    assert successor_mapping.mapping_status.value == "RESERVED"
    assert outbox is not None and outbox.command_id == coordination.release_command_id


def test_trigger_bundle_rejects_authority_set_omission() -> None:
    authority_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    authority_set = _v3_authority(())
    coordination = _materialized_coordination_v2(authority_item, created_at_utc=NOW)
    mapping = ProductCommandChildMappingV1.create_deferred(
        authority_item=authority_item,
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    with pytest.raises(ValueError, match="absent from the strict authority set"):
        build_dependent_buy_trigger_bundle_v2(
            coordination=coordination,
            authority_item=authority_item,
            authority_set=authority_set,
            mapping=mapping,
            observed_dependencies=coordination.ordered_sell_dependencies,
            trigger=_trigger(DependentBuyTriggerTypeV1.SESSION_EOD),
            ledger_observation=_ledger(coordination, available_cash="0"),
            session_authority_sha256=coordination.session_authority_sha256,
            decided_at_utc="2026-08-03T01:30:01Z",
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=1,
            lease_expires_at_utc=NOW,
        )


def test_trigger_bundle_rejects_expired_lease_after_authority_closure() -> None:
    authority_item = _v3_item(disposition=ProductCommandDispositionV3.DEFER_DEPENDENT_BUY)
    authority_set = _v3_authority((authority_item,))
    coordination = _materialized_coordination_v2(authority_item, created_at_utc=NOW)
    mapping = ProductCommandChildMappingV1.create_deferred(
        authority_item=authority_item,
        strategy_slot_id="slot_k6",
        created_at_utc=NOW,
    )
    with pytest.raises(ValueError, match="lease expiry"):
        build_dependent_buy_trigger_bundle_v2(
            coordination=coordination,
            authority_item=authority_item,
            authority_set=authority_set,
            mapping=mapping,
            observed_dependencies=coordination.ordered_sell_dependencies,
            trigger=_trigger(DependentBuyTriggerTypeV1.SESSION_EOD),
            ledger_observation=_ledger(coordination, available_cash="0"),
            session_authority_sha256=coordination.session_authority_sha256,
            decided_at_utc="2026-08-03T01:30:01Z",
            worker_id="worker_k6",
            process_incarnation_id="process_k6",
            lease_epoch=1,
            lease_expires_at_utc=NOW,
        )
