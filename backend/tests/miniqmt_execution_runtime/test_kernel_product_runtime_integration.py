from __future__ import annotations

import copy
from dataclasses import replace
from datetime import UTC, date, datetime, time
from decimal import Decimal
from hashlib import sha256
import inspect
import json
from types import SimpleNamespace

import pytest

from backend.execution_algos.adaptive_is.contracts import (
    CalendarSnapshot,
    CalendarSnapshotSet,
    MarketCode,
    SessionSegment,
    canonical_json_bytes,
)
from backend.execution_algos.hot_market_contracts import HotMarketDataEconomicEffectV1
from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeAuthorityInputV2
from backend.services.miniqmt_execution_runtime import kernel_delivery, kernel_product_evidence
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV2
from backend.services.miniqmt_execution_runtime.kernel_ingress import KernelIngressCoordinatorV1
from backend.services.miniqmt_execution_runtime.hot_market_data import (
    HotMarketDataEffectRetryableError,
    HotMarketDataEffectTerminalError,
    HotMarketDataIngressV1,
    _PendingHotMarketEffectV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_cutover import KernelProductCutoverCoordinator
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelAlgoCreationRequestV2,
    KernelProductDeliveryWorkerV3,
    KernelTransitionWriteBundleV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_evidence import (
    KernelProductEvidenceError,
    KernelProductEvidenceProviderV3,
    ProductEvidenceBuildResultV3,
    VirtualAccountProjectionError,
    _CursorLedgerReadRepository,
    _CursorTradingCalendar,
    bind_product_transition_bundle_v3,
    virtual_account_projection_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_callbacks import (
    KernelProductCallbackIngressError,
    KernelProductCallbackIngressV1,
    KernelProductSnapshotIngressV1,
    _canonical_source_payload_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_runtime import (
    K6DCommittedSourceEventReadbackV1,
    K6DProductDeliveryAggregateError,
    K6DProductParentStartResultV1,
    K6DProductPlanAuthorityV1,
    K6DProductPlanStartReceiptV1,
    K6DProductStartStatusV1,
    MiniQMTKernelV2ProductCoordinator,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.kernel_repository_event_delivery import (
    KernelRepositoryEventDeliveryMixin,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import freeze_json_v1, hash_hex_v1, thaw_json_v1
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoEventDeliveryV1,
    BrokerCommandOutboxStatusV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    ExecutionCommandChildMappingV1,
    ExecutionAlgoPersistenceStatusV2,
    KernelErrorEvidenceV1,
    AlgoTransitionV1,
    AlgoTransitionReceiptV1,
    EventSourceV2,
    EventTypeV2,
    DeliveryStatusV1,
    OrderTypeV1,
    RuntimeEventIngressReceiptV1,
    RuntimeEventEnvelopeV2,
    SideV1,
    command_child_mapping_id_v1,
    deterministic_client_order_ref_v1,
    execution_child_order_id_v1,
    _algo_instance_id_v2,
)
from backend.services.miniqmt_execution_runtime.plugin_registry import PluginRouteCompatibilityReceiptV1
from backend.services.simulation_runtime import lifecycle, miniqmt_kernel_product as product_module, scheduler
from backend.services.simulation_runtime.miniqmt_kernel_product import (
    MiniQMTKernelProductCompositionError,
    SimulationK6DPlanAuthorityReader,
    SimulationMiniQMTProductRuntimeV1,
    _runtime_id,
    build_k6d_gateway_catalog_v1,
    build_simulation_miniqmt_product_runtime_v1,
)
from backend.services.simulation_runtime.miniqmt_quote_activation import build_miniqmt_quote_ingress_activation_from_env
from backend.services.simulation_runtime.models import SimulationBrokerBackend, miniqmt_kernel_runtime_id
from backend.services.qmt_strategy_ledger.models import (
    VirtualAccount,
    VirtualAccountStatus,
)
from backend.services.strategy_package.execution_policy import compute_execution_policy_sha256
from backend.services.trading_core.models import OrderSide
from backend.tests.miniqmt_execution_runtime.test_kernel_delivery import _worker_facts
from backend.tests.miniqmt_execution_runtime.test_hot_market_data_boundary import (
    _hot_initialized,
    _hot_persistence,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_product_cutover import (
    _catalog,
    _full_five_authority,
    _request,
    _route_owner,
)
from backend.tests.miniqmt_execution_runtime.test_kernel_clock import _authority
from backend.tests.miniqmt_execution_runtime.test_b0_quote_v2_adapter import _context, _observation


def test_product_lifecycle_root_has_no_legacy_bridge_or_event_loop_submit_call() -> None:
    lifecycle_source = inspect.getsource(lifecycle.SimulationLifecycleOrchestrator.submit_persisted_execution_plan)
    scheduler_factory_source = inspect.getsource(
        scheduler.SimulationLifecycleScheduler._build_miniqmt_kernel_product_runtime
    )
    for forbidden in (
        "MiniQMTExecutionBridge",
        "MiniQMTExecutionRuntimeClient",
        "submit_event_loop_vnpy_parent_intents",
        "drive_event_loop_ticks",
        "A_EVENT_LOOP",
    ):
        assert forbidden not in lifecycle_source
        assert forbidden not in scheduler_factory_source
    assert "start_execution_plan_v1" in lifecycle_source
    assert "build_simulation_miniqmt_product_runtime_v1" in scheduler_factory_source


def test_product_repository_exposes_only_v3_initialization_entry() -> None:
    assert not hasattr(PostgresMiniQMTKernelRepository, "initialize_algo_atomic_v2")
    assert callable(PostgresMiniQMTKernelRepository.initialize_product_algo_atomic_v3)
    assert callable(PostgresMiniQMTKernelRepository.apply_claimed_product_delivery_atomic_v3)
    assert callable(PostgresMiniQMTKernelRepository.list_dispatchable_outbox_commands)
    assert callable(PostgresMiniQMTKernelRepository.read_callback_identity_chain)


class _CreationAuthorityCursor:
    def __init__(self, *, request: KernelAlgoCreationRequestV2, payload: dict[str, object]) -> None:
        self.sql = ""
        self.request = request
        self.payload = payload

    def execute(self, sql, _params=()):
        self.sql = " ".join(str(sql).split())

    def fetchone(self):
        request = self.request
        if "paper_v2.execution_plan" in self.sql:
            return {
                "plan_id": request.execution_plan_id,
                "plan_hash": request.execution_plan_sha256,
                "binding_id": request.binding_id,
                "release_id": request.release_id,
                "target_trade_date": date.fromisoformat(request.exchange_trade_date),
                "execution_policy_version_id": request.policy_id,
                "execution_policy_sha256": request.policy_sha256,
                "plan_payload_json": self.payload,
            }
        if "strategy_pkg.strategy_runtime_release" in self.sql:
            return {
                "release_hash": request.release_sha256,
                "execution_policy_version_id": request.policy_id,
                "execution_policy_sha256": request.policy_sha256,
            }
        return None

    def fetchall(self):
        return []


def test_k6d_creation_authority_reads_frozen_execution_plan_without_tca_parent_materialization() -> None:
    base = _request()
    payload = {
        "schema_version": "execution_plan_v1",
        "binding_id": "binding_k6d",
        "release_id": base.release_id,
        "target_trade_date": base.exchange_trade_date,
        "intents": [
            {
                "intent_id": base.parent_intent_id,
                "symbol": base.symbol,
                "side": base.side.value,
                "order_quantity": base.parent_quantity,
            }
        ],
    }
    plan_hash = sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    request = KernelAlgoCreationRequestV2.from_v1(
        base.model_copy(
            update={
                "runtime_id": miniqmt_kernel_runtime_id(
                    plan_id=base.execution_plan_id,
                    binding_id="binding_k6d",
                    trade_date=date.fromisoformat(base.exchange_trade_date),
                ),
                "execution_plan_sha256": plan_hash,
            }
        ),
        binding_id="binding_k6d",
        product_route_cutover_receipt_sha256="d" * 64,
        product_route_owner_sha256="e" * 64,
        product_route_epoch=1,
        effective_new_instance_sequence=7,
    )
    cursor = _CreationAuthorityCursor(request=request, payload=payload)

    repository = object.__new__(PostgresMiniQMTKernelRepository)
    existing = repository._lock_and_validate_creation_authority_with_cursor(cursor, request)

    assert existing == ()
    assert "execution_parent_benchmark" not in cursor.sql

    with pytest.raises(KernelRepositoryConflict, match="execution plan authority conflicts"):
        repository._lock_and_validate_creation_authority_with_cursor(
            _CreationAuthorityCursor(
                request=request.model_copy(update={"runtime_id": "mqrt_sim_wrong_runtime"}),
                payload=payload,
            ),
            request.model_copy(update={"runtime_id": "mqrt_sim_wrong_runtime"}),
        )


def test_k6d_online_product_seams_never_require_offline_tca_parent_rows() -> None:
    seams = (
        PostgresMiniQMTKernelRepository._lock_and_validate_k6d_plan_parent_with_cursor,
        PostgresMiniQMTKernelRepository._lock_product_route_binding_with_cursor,
        KernelProductEvidenceProviderV3._dependent_buy_candidate,
    )

    for seam in seams:
        assert "execution_parent_benchmark" not in inspect.getsource(seam)


def test_product_worker_hard_binds_v3_evidence_path(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def capture_init(_self, **values):
        captured.update(values)

    monkeypatch.setattr(kernel_delivery.KernelDeliveryWorkerV1, "__init__", capture_init)
    evidence = object()
    KernelProductDeliveryWorkerV3(
        repository=object(),
        catalog_runtime=object(),
        worker_id="worker_k6d",
        process_incarnation_id="incarnation_k6d",
        facade_authority=object(),
        gateway_catalog=object(),
        product_evidence_provider=evidence,
    )
    assert captured["product_mode"] is True
    assert captured["product_evidence_provider"] is evidence


def test_product_evidence_provider_requires_exact_code_owned_gateway() -> None:
    catalog = build_k6d_gateway_catalog_v1()
    provider = KernelProductEvidenceProviderV3(gateway_catalog=catalog)
    assert provider._gateway_catalog == catalog
    with pytest.raises(TypeError, match="gateway_catalog"):
        KernelProductEvidenceProviderV3(gateway_catalog=object())  # type: ignore[arg-type]


class _EvidenceCursor:
    def __init__(self, *, dependency_rows=(), mapping: ExecutionCommandChildMappingV1 | None = None) -> None:
        self.sql = ""
        self.dependency_rows = tuple(dependency_rows)
        self.mapping = mapping

    def execute(self, sql, _params=()):
        self.sql = " ".join(str(sql).split())

    def fetchall(self):
        if "execution_algo_instance" in self.sql:
            return list(self.dependency_rows)
        return []

    def fetchone(self):
        if "execution_child_order" in self.sql and self.mapping is not None:
            return {"mapping_json": self.mapping.model_dump(mode="python")}
        return None


def _evidence_account():
    return VirtualAccount(
        strategy_id="strategy_product_evidence",
        strategy_name="strategy-product-evidence",
        display_name="Strategy product evidence",
        account_id="account-product-evidence",
        mode="SIM",
        initial_cash=Decimal("100000"),
        cash=Decimal("100000"),
        frozen_cash=Decimal("0"),
        market_value=Decimal("0"),
        status=VirtualAccountStatus.ENABLED,
        risk_config={"kill_switch": False},
        created_at=datetime(2026, 7, 26, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 26, 1, 29, tzinfo=UTC),
    )


def test_virtual_account_projection_is_exact_json_safe_and_fail_loud() -> None:
    account = replace(
        _evidence_account(),
        risk_config={"nested": [{"ratio": 0.25, "enabled": True, "optional": None}]},
        updated_at=datetime.fromisoformat("2026-07-26T09:29:00+08:00"),
    )
    projection = virtual_account_projection_v1(account)
    assert projection["updated_at_utc"] == "2026-07-26T01:29:00+00:00"
    assert projection["risk_config"] == {"nested": [{"enabled": True, "optional": None, "ratio": 0.25}]}

    malformed = (
        object(),
        replace(account, strategy_id=" "),
        replace(account, strategy_name=1),  # type: ignore[arg-type]
        replace(account, mode="PAPER"),
        replace(account, initial_cash=Decimal("NaN")),
        replace(account, initial_cash=Decimal("0")),
        replace(account, cash=Decimal("-1")),
        replace(account, frozen_cash=Decimal("-1")),
        replace(account, status="ENABLED"),  # type: ignore[arg-type]
        replace(account, risk_config=[]),  # type: ignore[arg-type]
        replace(account, risk_config={1: "bad-key"}),  # type: ignore[dict-item]
        replace(account, risk_config={"ratio": float("nan")}),
        replace(account, risk_config={"opaque": object()}),
        replace(account, updated_at=datetime(2026, 7, 26, 1, 29)),
    )
    for item in malformed:
        with pytest.raises(VirtualAccountProjectionError):
            virtual_account_projection_v1(item)  # type: ignore[arg-type]


def _evidence_plan(algo) -> dict[str, object]:
    return {
        "execution_plan_id": "plan_product_evidence",
        "execution_plan_sha256": "a" * 64,
        "binding_id": "binding_product_evidence",
        "trade_date": date(2026, 7, 26),
        "package_id": "package_product_evidence",
        "strategy_id": "strategy_product_evidence",
        "broker_account_id": "account-product-evidence",
        "strategy_name": "strategy-product-evidence",
        "target_weight": Decimal("0.25"),
        "request_metadata": {"source": "product_evidence_test"},
        "ordered_sell_parent_intent_ids": ("sell_parent",),
        "intent": {
            "intent_id": algo.parent_intent_id,
            "trading_rule_decision_id": "decision_product_evidence",
        },
        "trading_rule_decision": {
            "decision_id": "decision_product_evidence",
            "symbol": algo.symbol,
            "side": algo.side.value,
        },
    }


def _route_receipt_for(algo):
    catalog = _catalog()
    descriptor = next(
        item for item in catalog.snapshot.registration_descriptors if item.manifest.algo_code == algo.algo_code
    )
    return PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=catalog.snapshot,
        plugin_key=descriptor.plugin_key,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
    )


def _transition_with_commands(state, commands: tuple[BrokerCommandV2, ...]) -> AlgoTransitionV1:
    payload = {
        "next_state_sha256": state.state_sha256,
        "ordered_command_ids": [item.command_id for item in commands],
        "ordered_timer_mutation_ids": [],
        "ordered_diagnostic_observation_ids": [],
        "terminal_outcome": None,
    }
    return AlgoTransitionV1(
        schema_version="miniqmt_algo_transition_v1",
        next_state=state,
        broker_commands=commands,
        timer_mutations=(),
        diagnostic_observations=(),
        terminal_outcome=None,
        effect_set_sha256=hash_hex_v1("miniqmt_algo_effect_set_v1", payload),
    )


def _submit_command(event, algo, *, ordinal: int = 0) -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="transition_product_evidence",
        ordinal=ordinal,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.25",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="PRODUCT_EVIDENCE_SUBMIT",
        metadata={},
    )


def _prepare_evidence_provider(monkeypatch, *, preflight) -> KernelProductEvidenceProviderV3:
    provider = KernelProductEvidenceProviderV3(gateway_catalog=build_k6d_gateway_catalog_v1())
    monkeypatch.setattr(provider, "_locked_plan_context", lambda _cur, *, event, algo: _evidence_plan(algo))
    monkeypatch.setattr(
        provider,
        "_locked_market_projection",
        lambda _cur, *, event, algo: (
            "market_product_evidence",
            {"last_price": "10.25", "symbol": algo.symbol},
            event.event_id,
        ),
    )
    monkeypatch.setattr(provider, "_exact_virtual_account", lambda *_args, **_kwargs: _evidence_account())
    monkeypatch.setattr(
        kernel_product_evidence.QmtManagedOrderService,
        "preview_order",
        lambda _self, _request: preflight,
    )
    return provider


def test_product_evidence_builds_exact_same_cursor_submit_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    event, delivery, algo, state = _worker_facts()
    preflight = SimpleNamespace(errors=(), freeze_amount=Decimal("1025"))
    provider = _prepare_evidence_provider(monkeypatch, preflight=preflight)
    cursor = _EvidenceCursor()
    base = provider.build_base_services_with_cursor_v1(cur=cursor, event=event, delivery=delivery, algo=algo)
    transition = _transition_with_commands(state, (_submit_command(event, algo),))

    result = provider.build_with_cursor_v1(
        cur=cursor,
        event=event,
        delivery=delivery,
        algo=algo,
        transition=transition,
        base_services=base,
        route_receipt=_route_receipt_for(algo),
    )

    assert len(result.ordered_evidence) == 1
    assert result.ordered_evidence[0].oms_preflight_receipt.decision.value == "PASS"
    assert {
        item.projection_type.value for item in result.services.execution_projection_set.ordered_projection_refs
    } == {
        "ACCOUNT",
        "CONTRACT",
        "KILL_SWITCH_STATE",
        "MARKET_DATA",
        "MARKET_CAPABILITY",
        "OMS_PREFLIGHT",
        "RISK_DECISION",
        "ROUTE_COMPATIBILITY",
    }


def test_product_evidence_preserves_dependent_buy_rejection_and_sell_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, delivery, algo, state = _worker_facts()

    class Error:
        code = "SELL_PROCEEDS_REQUIRED"

        @staticmethod
        def to_dict():
            return {"code": "SELL_PROCEEDS_REQUIRED", "message": "sell proceeds not settled"}

    preflight = SimpleNamespace(errors=(Error(),), freeze_amount=Decimal("1025"))
    provider = _prepare_evidence_provider(monkeypatch, preflight=preflight)
    cursor = _EvidenceCursor(dependency_rows=({"parent_intent_id": "sell_parent", "algo_instance_id": "sell_algo"},))
    base = provider.build_base_services_with_cursor_v1(cur=cursor, event=event, delivery=delivery, algo=algo)
    result = provider.build_with_cursor_v1(
        cur=cursor,
        event=event,
        delivery=delivery,
        algo=algo,
        transition=_transition_with_commands(state, (_submit_command(event, algo),)),
        base_services=base,
        route_receipt=_route_receipt_for(algo),
    )
    evidence = result.ordered_evidence[0]
    assert evidence.oms_preflight_receipt.decision.value == "REJECT"
    assert evidence.dependent_buy_candidate is not None
    assert evidence.dependent_buy_candidate.ordered_sell_dependencies[0].sell_parent_intent_id == "sell_parent"


def test_product_evidence_dependent_buy_fails_when_frozen_sell_algo_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, delivery, algo, state = _worker_facts()

    class Error:
        code = "SELL_PROCEEDS_REQUIRED"

        @staticmethod
        def to_dict():
            return {"code": "SELL_PROCEEDS_REQUIRED", "message": "sell proceeds not settled"}

    provider = _prepare_evidence_provider(
        monkeypatch,
        preflight=SimpleNamespace(errors=(Error(),), freeze_amount=Decimal("1025")),
    )
    cursor = _EvidenceCursor()
    base = provider.build_base_services_with_cursor_v1(cur=cursor, event=event, delivery=delivery, algo=algo)

    with pytest.raises(KernelProductEvidenceError, match="every frozen SELL parent algo"):
        provider.build_with_cursor_v1(
            cur=cursor,
            event=event,
            delivery=delivery,
            algo=algo,
            transition=_transition_with_commands(state, (_submit_command(event, algo),)),
            base_services=base,
            route_receipt=_route_receipt_for(algo),
        )


def test_product_evidence_zero_command_and_owner_failures_are_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    event, delivery, algo, state = _worker_facts()
    provider = _prepare_evidence_provider(
        monkeypatch,
        preflight=SimpleNamespace(errors=(), freeze_amount=Decimal("0")),
    )
    cursor = _EvidenceCursor()
    base = provider.build_base_services_with_cursor_v1(cur=cursor, event=event, delivery=delivery, algo=algo)
    zero = provider.build_with_cursor_v1(
        cur=cursor,
        event=event,
        delivery=delivery,
        algo=algo,
        transition=_transition_with_commands(state, ()),
        base_services=base,
        route_receipt=_route_receipt_for(algo),
    )
    assert zero == ProductEvidenceBuildResultV3(services=base, ordered_evidence=())
    with pytest.raises(KernelProductEvidenceError, match="product evidence authority"):
        provider.build_with_cursor_v1(
            cur=cursor,
            event=event.model_copy(update={"runtime_id": "runtime_other"}),
            delivery=delivery,
            algo=algo,
            transition=_transition_with_commands(state, ()),
            base_services=base,
            route_receipt=_route_receipt_for(algo),
        )


def test_product_evidence_public_seams_reject_malformed_carriers_before_reading_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, delivery, algo, state = _worker_facts()
    provider = _prepare_evidence_provider(
        monkeypatch,
        preflight=SimpleNamespace(errors=(), freeze_amount=Decimal("0")),
    )
    with pytest.raises(TypeError, match="services"):
        ProductEvidenceBuildResultV3(services=object(), ordered_evidence=())  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="cursor"):
        provider.build_base_services_with_cursor_v1(cur=object(), event=event, delivery=delivery, algo=algo)
    with pytest.raises(TypeError, match="strict kernel carriers"):
        provider.build_base_services_with_cursor_v1(
            cur=_EvidenceCursor(),
            event=object(),  # type: ignore[arg-type]
            delivery=delivery,
            algo=algo,
        )
    with pytest.raises(TypeError, match="algo"):
        provider.build_base_services_with_cursor_v1(
            cur=_EvidenceCursor(),
            event=event,
            delivery=delivery,
            algo=object(),  # type: ignore[arg-type]
        )
    base = provider.build_base_services_with_cursor_v1(
        cur=_EvidenceCursor(),
        event=event,
        delivery=delivery,
        algo=algo,
    )
    with pytest.raises(TypeError, match="ordered_evidence"):
        ProductEvidenceBuildResultV3(services=base, ordered_evidence=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="transition"):
        provider.build_with_cursor_v1(
            cur=_EvidenceCursor(),
            event=event,
            delivery=delivery,
            algo=algo,
            transition=object(),  # type: ignore[arg-type]
            base_services=base,
            route_receipt=_route_receipt_for(algo),
        )


class _PlanCursor:
    def __init__(self, *, start_event, plan_row, binding_row) -> None:
        self.start_event = start_event
        self.plan_row = plan_row
        self.binding_row = binding_row
        self.sql = ""

    def execute(self, sql, _params=()):
        self.sql = " ".join(str(sql).split())

    def fetchall(self):
        if "event_type='ALGO_START'" in self.sql:
            return [{"payload": self.start_event.model_dump(mode="python")}]
        return []

    def fetchone(self):
        if "paper_v2.execution_plan" in self.sql:
            return self.plan_row
        if "paper_v2.simulation_release_binding" in self.sql:
            return self.binding_row
        return None


def test_same_cursor_plan_and_market_authorities_close_exactly() -> None:
    event, _delivery, algo, _state = _worker_facts()
    start = RuntimeEventEnvelopeV2.create(
        runtime_id=event.runtime_id,
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-07-26T01:29:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol=algo.symbol,
        payload_schema_version="miniqmt_algo_start_v1",
        payload={
            "execution_plan_id": "plan_product_evidence",
            "execution_plan_sha256": "a" * 64,
            "binding_id": "binding_product_evidence",
            "release_id": "release_product_evidence",
            "release_sha256": "b" * 64,
        },
        source_identity={
            "algo_instance_id": algo.algo_instance_id,
            "runtime_id": algo.runtime_id,
            "parent_intent_id": algo.parent_intent_id,
            "strategy_slot_id": algo.strategy_slot_id,
            "algo_code": algo.algo_code,
            "plugin_id": algo.plugin_id,
            "plugin_version": algo.plugin_version,
            "plugin_manifest_sha256": algo.plugin_manifest_sha256,
            "plugin_config_sha256": algo.plugin_config_sha256,
        },
        correlation={
            "binding_id": "binding_product_evidence",
            "exchange_trade_date": "2026-07-26",
            "session_epoch": "session_product_evidence",
            "session_phase": "CONTINUOUS_AM",
        },
    )
    intent = {
        "intent_id": algo.parent_intent_id,
        "trading_rule_decision_id": "decision_product_evidence",
        "target_weight": "0.25",
        "risk_context": {"max_notional": "5000"},
        "metadata": {"source": "frozen_plan"},
    }
    cursor = _PlanCursor(
        start_event=start,
        plan_row={
            "plan_payload_json": {
                "intents": [intent],
                "trading_rule_decisions": [
                    {
                        "decision_id": "decision_product_evidence",
                        "symbol": algo.symbol,
                        "side": algo.side.value,
                    }
                ],
            },
            "plan_hash": "a" * 64,
            "package_id": "package_product_evidence",
            "binding_id": "binding_product_evidence",
            "target_trade_date": date(2026, 7, 26),
        },
        binding_row={
            "broker_account_id": "account-product-evidence",
            "strategy_name": "strategy-product-evidence",
            "strategy_id": "strategy_product_evidence",
            "binding_hash": "c" * 64,
            "release_id": "release_product_evidence",
            "release_hash": "b" * 64,
        },
    )
    context = KernelProductEvidenceProviderV3._locked_plan_context(cursor, event=event, algo=algo)
    assert context["execution_plan_id"] == "plan_product_evidence"
    assert context["request_metadata"] == {"source": "frozen_plan", "max_notional": "5000"}

    market_id, market, source_event_id = KernelProductEvidenceProviderV3._locked_market_projection(
        _EvidenceCursor(),
        event=event,
        algo=algo,
    )
    assert (market_id, market, source_event_id) == (
        "market_k2b",
        {"last_price_decimal": "10.000000"},
        event.event_id,
    )
    with pytest.raises(KernelProductEvidenceError, match="symbol differs"):
        KernelProductEvidenceProviderV3._locked_market_projection(
            _EvidenceCursor(),
            event=event.model_copy(update={"symbol": "000001.SZ"}),
            algo=algo,
        )


class _CalendarCursor:
    def __init__(self, rows) -> None:
        self.rows = list(rows)

    def execute(self, _sql, _params=()):
        return None

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


def test_cursor_calendar_authority_is_strict_and_fail_loud() -> None:
    calendar = _CursorTradingCalendar(_CalendarCursor(({"is_trading": True}, {"next_day": date(2026, 7, 27)})))
    assert calendar.is_trading_day(date(2026, 7, 26)) is True
    assert calendar.next_trading_day_after(date(2026, 7, 26)) == date(2026, 7, 27)
    with pytest.raises(KernelProductEvidenceError, match="authority is missing"):
        _CursorTradingCalendar(_CalendarCursor(())).is_trading_day(date(2026, 7, 26))
    with pytest.raises(KernelProductEvidenceError, match="not a strict boolean"):
        _CursorTradingCalendar(_CalendarCursor(({"is_trading": 1},))).is_trading_day(date(2026, 7, 26))
    with pytest.raises(KernelProductEvidenceError, match="next trading day"):
        _CursorTradingCalendar(_CalendarCursor((None,))).next_trading_day_after(date(2026, 7, 26))


class _EmptyLedgerCursor:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, sql, params=()):
        self.executed.append((" ".join(str(sql).split()), tuple(params)))

    @staticmethod
    def fetchall():
        return []

    @staticmethod
    def fetchone():
        return None


def test_same_cursor_ledger_reader_covers_all_bounded_query_shapes() -> None:
    cursor = _EmptyLedgerCursor()
    ledger = _CursorLedgerReadRepository(cursor)
    assert ledger.list_virtual_accounts() == []
    assert ledger.list_virtual_accounts(account_id="account") == []
    assert ledger.get_order_intent_by_remark("account", "remark") is None
    assert ledger.list_position_lots("strategy") == []
    assert ledger.list_position_lots("strategy", "600000.SH") == []
    assert ledger.list_open_sell_intents("strategy") == []
    assert ledger.list_open_sell_intents("strategy", symbol="600000.SH", trade_date=date(2026, 7, 26)) == []
    assert len(cursor.executed) == 7
    assert all("FOR SHARE" in sql for sql, _params in cursor.executed)


def _cancel_commands_and_mapping(event, algo):
    submit = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="transition_cancel_submit",
        ordinal=0,
        local_vt_orderid=None,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.25",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="PRODUCT_EVIDENCE_SUBMIT",
        metadata={},
    )
    child_id = execution_child_order_id_v1(
        command_id=submit.command_id,
        local_vt_orderid=submit.local_vt_orderid,
    )
    mapping_id = command_child_mapping_id_v1(
        command_id=submit.command_id,
        local_vt_orderid=submit.local_vt_orderid,
        child_order_id=child_id,
    )
    mapping = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=3,
        broker_order_id="broker_cancel_owner",
        broker_identity_source_event_id="event_broker_accept",
        last_order_event_id="event_broker_accept",
        last_trade_event_id=None,
        updated_by_event_id="event_broker_accept",
        created_at_utc="2026-07-26T01:30:00Z",
        updated_at_utc="2026-07-26T01:31:00Z",
    )
    cancel = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="transition_cancel_request",
        ordinal=0,
        local_vt_orderid=submit.local_vt_orderid,
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.25",
        quantity=100,
        owned_broker_order_id="broker_cancel_owner",
        reason_code="PRODUCT_EVIDENCE_CANCEL",
        metadata={"submit_command_id": submit.command_id},
    )
    return submit, cancel, mapping, mapping_id, child_id


def test_cancel_lineage_and_durable_owner_are_exact() -> None:
    event, _delivery, algo, _state = _worker_facts()
    submit, cancel, mapping, mapping_id, child_id = _cancel_commands_and_mapping(event, algo)
    assert KernelProductEvidenceProviderV3._command_lineage(cancel) == (
        mapping_id,
        child_id,
        deterministic_client_order_ref_v1(command_id=cancel.command_id, mapping_id=mapping_id),
    )
    KernelProductEvidenceProviderV3._validate_cancel_owner(
        _EvidenceCursor(mapping=mapping),
        command=cancel,
        mapping_id=mapping_id,
    )
    with pytest.raises(KernelProductEvidenceError, match="no exact durable mapping owner"):
        KernelProductEvidenceProviderV3._validate_cancel_owner(
            _EvidenceCursor(),
            command=cancel,
            mapping_id=mapping_id,
        )
    conflicting = ExecutionCommandChildMappingV1.create(
        command=submit,
        strategy_slot_id=algo.strategy_slot_id,
        mapping_status=CommandChildMappingStatusV1.BROKER_ACCEPTED,
        mapping_version=3,
        broker_order_id="broker_other",
        broker_identity_source_event_id="event_broker_other",
        last_order_event_id="event_broker_other",
        last_trade_event_id=None,
        updated_by_event_id="event_broker_other",
        created_at_utc="2026-07-26T01:30:00Z",
        updated_at_utc="2026-07-26T01:31:00Z",
    )
    with pytest.raises(KernelProductEvidenceError, match="differs from its exact durable mapping owner"):
        KernelProductEvidenceProviderV3._validate_cancel_owner(
            _EvidenceCursor(mapping=conflicting),
            command=cancel,
            mapping_id=mapping_id,
        )


@pytest.mark.parametrize(
    "metadata",
    ({}, {"submit_command_id": " ", "mapping_id": None}, {"mapping_id": "mapping", "child_order_id": " "}),
)
def test_cancel_lineage_rejects_ambiguous_or_malformed_authority(metadata) -> None:
    event, _delivery, algo, _state = _worker_facts()
    command = BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.CANCEL_ORDER,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        parent_intent_id=algo.parent_intent_id,
        transition_id="transition_cancel_invalid",
        ordinal=0,
        local_vt_orderid="local_cancel_invalid",
        symbol=algo.symbol,
        side=algo.side,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.25",
        quantity=100,
        owned_broker_order_id="broker_cancel_invalid",
        reason_code="PRODUCT_EVIDENCE_CANCEL",
        metadata=metadata,
    )
    with pytest.raises(KernelProductEvidenceError) as captured:
        KernelProductEvidenceProviderV3._command_lineage(command)
    assert captured.value.reason_code == "MINIQMT_K6_PRODUCT_CANCEL_LINEAGE_INVALID"


def test_product_transition_binding_rebuilds_zero_command_authority_and_rejects_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event, delivery, _legacy_algo, _legacy_state = _worker_facts()
    _plugin, context, state = _hot_initialized("TWAP_LITE_MINIQMT")
    algo = _hot_persistence("TWAP_LITE_MINIQMT", context, state)
    event = event.model_copy(update={"runtime_id": algo.runtime_id, "symbol": algo.symbol})
    delivery = delivery.model_copy(
        update={
            "runtime_id": algo.runtime_id,
            "algo_instance_id": algo.algo_instance_id,
            "plugin_manifest_sha256": algo.plugin_manifest_sha256,
        }
    )
    authority = _full_five_authority()
    descriptor = next(
        item
        for item in authority.catalog_runtime.snapshot.registration_descriptors
        if item.manifest.algo_code == algo.algo_code
    )
    assert algo.plugin_manifest_sha256 == descriptor.manifest.manifest_sha256
    provider = _prepare_evidence_provider(
        monkeypatch,
        preflight=SimpleNamespace(errors=(), freeze_amount=Decimal("0")),
    )
    base = provider.build_base_services_with_cursor_v1(
        cur=_EvidenceCursor(),
        event=event,
        delivery=delivery,
        algo=algo,
    )
    transition = _transition_with_commands(state, ())
    receipt = AlgoTransitionReceiptV1.create(
        delivery_id=delivery.delivery_id,
        event_id=event.event_id,
        runtime_id=event.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        plugin_id=algo.plugin_id,
        plugin_version=algo.plugin_version,
        plugin_manifest_sha256=algo.plugin_manifest_sha256,
        transition_sequence=state.transition_sequence,
        before_state_sha256_or_INIT=algo.state_sha256,
        after_state_sha256=state.state_sha256,
        ordered_command_ids=(),
        ordered_timer_mutation_ids=(),
        ordered_diagnostic_observation_ids=(),
        ordered_consumed_lineage_refs=(),
        execution_projection_set_sha256=base.execution_projection_set.projection_set_sha256,
        effect_set_sha256=transition.effect_set_sha256,
        terminal_outcome=None,
        logical_applied_at_utc=event.event_time_utc,
        transaction_commit_identity="transaction_product_binding",
    )
    bundle = KernelTransitionWriteBundleV1.create(
        algo_instance=algo,
        delivery=delivery,
        receipt=receipt,
        projection_set=base.execution_projection_set,
        after_state=state,
        applied_transition=transition,
    )
    pinned = next(
        item
        for item in authority.catalog_runtime.snapshot.pinned_compatibility_receipts
        if item.plugin_key == descriptor.plugin_key
    )
    route = PluginRouteCompatibilityReceiptV1.create(
        catalog_snapshot=authority.catalog_runtime.snapshot,
        plugin_key=descriptor.plugin_key,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
    )
    creation = VnpyFacadeAuthorityInputV2.create(
        conformance_authority=authority.conformance_authority,
        plugin_catalog_snapshot=authority.catalog_runtime.snapshot,
        gateway_capability_catalog=build_k6d_gateway_catalog_v1(),
        plugin_key=descriptor.plugin_key,
        manifest=descriptor.manifest,
        pinned_compatibility_receipt=pinned,
        route_compatibility_receipt=route,
    )
    evidence = ProductEvidenceBuildResultV3(services=base, ordered_evidence=())
    bound = bind_product_transition_bundle_v3(
        proposal_bundle=bundle,
        replay_bundle=bundle,
        evidence=evidence,
        creation_binding=creation,
    )
    assert bound.transition_bundle.receipt.transaction_commit_identity != receipt.transaction_commit_identity
    assert bound.authority_envelope.authority_set.ordered_items == ()

    missing = KernelTransitionWriteBundleV1.create(
        algo_instance=algo,
        delivery=delivery,
        receipt=receipt,
        projection_set=base.execution_projection_set,
        after_state=state,
        applied_transition=None,
    )
    with pytest.raises(KernelProductEvidenceError, match="replay differs"):
        bind_product_transition_bundle_v3(
            proposal_bundle=missing,
            replay_bundle=bundle,
            evidence=evidence,
            creation_binding=creation,
        )
    with pytest.raises(TypeError, match="proposal_bundle"):
        bind_product_transition_bundle_v3(
            proposal_bundle=object(),  # type: ignore[arg-type]
            replay_bundle=bundle,
            evidence=evidence,
            creation_binding=creation,
        )


def test_product_coordinator_processes_every_durable_target_before_aggregate_failure() -> None:
    event = RuntimeEventEnvelopeV2.create(
        runtime_id="runtime_k6d_aggregate",
        sequence=1,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-08-04T01:30:00Z",
        monotonic_ns=1,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"market_data_id": "market_data_k6d_aggregate", "last_price": "10.00"},
        source_identity={"market_data_id": "market_data_k6d_aggregate"},
        correlation={},
    )
    receipt = RuntimeEventIngressReceiptV1.create(
        runtime_id=event.runtime_id,
        event_id=event.event_id,
        event_key_sha256=event.event_key_sha256,
        runtime_sequence=event.sequence,
        ordered_target_algo_instance_ids=("algo_a", "algo_b", "algo_c"),
        ordered_delivery_ids=("delivery_a", "delivery_b", "delivery_c"),
        transaction_commit_identity="commit_k6d_aggregate",
    )

    class Worker:
        calls: list[str] = []

        def process_committed_delivery_v3(self, *, delivery_id, **_values):
            self.calls.append(delivery_id)
            if delivery_id != "delivery_b":
                raise RuntimeError(f"failure:{delivery_id}")

    coordinator = object.__new__(MiniQMTKernelV2ProductCoordinator)
    coordinator._delivery_worker = Worker()
    with pytest.raises(K6DProductDeliveryAggregateError) as captured:
        coordinator.process_committed_event_v1(event=event, receipt=receipt)
    assert coordinator._delivery_worker.calls == ["delivery_a", "delivery_b", "delivery_c"]
    assert [item["delivery_id"] for item in captured.value.context["ordered_failures"]] == [
        "delivery_a",
        "delivery_c",
    ]
    assert captured.value.context["broker_called"] is False


def _coordinator_event(runtime_id: str) -> RuntimeEventEnvelopeV2:
    return RuntimeEventEnvelopeV2.create(
        runtime_id=runtime_id,
        sequence=1,
        event_type=EventTypeV2.TICK,
        event_time_utc="2026-08-04T01:30:00Z",
        monotonic_ns=1,
        source=EventSourceV2.B0_QUOTE_V2,
        symbol="600000.SH",
        payload_schema_version="miniqmt_market_data_view_v2",
        payload={"last_price": "10.00"},
        source_identity={"market_data_id": "market_coordinator"},
        correlation={},
    )


def _coordinator_receipt(
    event: RuntimeEventEnvelopeV2,
    deliveries=(),
    targets: tuple[str, ...] | None = None,
) -> RuntimeEventIngressReceiptV1:
    return RuntimeEventIngressReceiptV1.create(
        runtime_id=event.runtime_id,
        event_id=event.event_id,
        event_key_sha256=event.event_key_sha256,
        runtime_sequence=event.sequence,
        ordered_target_algo_instance_ids=(
            tuple(f"algo_{index}" for index, _ in enumerate(deliveries)) if targets is None else targets
        ),
        ordered_delivery_ids=tuple(deliveries),
        transaction_commit_identity="transaction_coordinator",
    )


def test_product_start_receipt_and_plan_authority_are_strict_and_hash_closed() -> None:
    started = K6DProductParentStartResultV1.create(
        plan_intent_ordinal=1,
        parent_intent_id="parent_started",
        algo_instance_id="algo_started",
        event_id="event_started",
        ingress_receipt_sha256="a" * 64,
        start_status=K6DProductStartStatusV1.STARTED,
        terminal_reason_or_null=None,
    )
    failed = K6DProductParentStartResultV1.create(
        plan_intent_ordinal=2,
        parent_intent_id="parent_failed",
        algo_instance_id="algo_failed",
        event_id="event_failed",
        ingress_receipt_sha256="b" * 64,
        start_status=K6DProductStartStatusV1.FAILED_TERMINAL,
        terminal_reason_or_null="failure_receipt",
    )
    receipt = K6DProductPlanStartReceiptV1.create(
        runtime_id="runtime_start_receipt",
        binding_id="binding_start_receipt",
        execution_plan_id="plan_start_receipt",
        execution_plan_sha256="c" * 64,
        product_route_receipt_sha256="d" * 64,
        ordered_parent_results=(started, failed),
    )
    assert (receipt.total, receipt.started, receipt.failed, receipt.success) == (2, 1, 1, False)
    with pytest.raises(ValueError, match="canonical plan order"):
        K6DProductPlanStartReceiptV1.create(
            runtime_id=receipt.runtime_id,
            binding_id=receipt.binding_id,
            execution_plan_id=receipt.execution_plan_id,
            execution_plan_sha256=receipt.execution_plan_sha256,
            product_route_receipt_sha256=receipt.product_route_receipt_sha256,
            ordered_parent_results=(failed, started),
        )
    request = _request()
    authority = K6DProductPlanAuthorityV1(
        runtime_id=request.runtime_id,
        binding_id="binding_coordinator",
        execution_plan_id=request.execution_plan_id,
        execution_plan_sha256=request.execution_plan_sha256,
        trade_date=date(2026, 8, 4),
        ordered_creation_requests=(request,),
    )
    assert authority.ordered_creation_requests == (request,)
    with pytest.raises(ValueError, match="duplicate parent"):
        K6DProductPlanAuthorityV1(
            runtime_id=request.runtime_id,
            binding_id="binding_coordinator",
            execution_plan_id=request.execution_plan_id,
            execution_plan_sha256=request.execution_plan_sha256,
            trade_date=date(2026, 8, 4),
            ordered_creation_requests=(request, request),
        )


def test_product_coordinator_start_and_committed_ingress_use_only_strict_durable_readback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    plan = K6DProductPlanAuthorityV1(
        runtime_id=request.runtime_id,
        binding_id="binding_k6d",
        execution_plan_id=request.execution_plan_id,
        execution_plan_sha256=request.execution_plan_sha256,
        trade_date=date(2026, 8, 4),
        ordered_creation_requests=(request,),
    )

    class PlanReader:
        @staticmethod
        def read_plan_authority_v1(**_values):
            return plan

    descriptor = next(
        item for item in _catalog().snapshot.registration_descriptors if item.manifest.algo_code == request.algo_code
    )
    algo_id = _algo_instance_id_v2(
        runtime_id=request.runtime_id,
        parent_intent_id=request.parent_intent_id,
        strategy_slot_id=request.strategy_slot_id,
        algo_code=request.algo_code,
        plugin_id=descriptor.manifest.plugin_id,
        plugin_version=descriptor.manifest.plugin_version,
        plugin_manifest_sha256=descriptor.manifest.manifest_sha256,
        plugin_config_sha256=request.plugin_config_sha256,
    )
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=request.runtime_id,
        sequence=1,
        event_type=EventTypeV2.ALGO_START,
        event_time_utc="2026-08-04T01:20:00Z",
        monotonic_ns=None,
        source=EventSourceV2.MINIQMT_EXECUTION_KERNEL,
        symbol=request.symbol,
        payload_schema_version="miniqmt_algo_start_v1",
        payload={"target_quantity": request.parent_quantity},
        source_identity={
            "algo_instance_id": algo_id,
            "runtime_id": request.runtime_id,
            "parent_intent_id": request.parent_intent_id,
            "strategy_slot_id": request.strategy_slot_id,
            "algo_code": request.algo_code,
            "plugin_id": descriptor.manifest.plugin_id,
            "plugin_version": descriptor.manifest.plugin_version,
            "plugin_manifest_sha256": descriptor.manifest.manifest_sha256,
            "plugin_config_sha256": request.plugin_config_sha256,
        },
        correlation={"execution_plan_id": request.execution_plan_id},
    )
    ingress_receipt = _coordinator_receipt(event, deliveries=("delivery_start",), targets=(algo_id,))
    creator = object.__new__(KernelAlgoCreationCoordinatorV2)
    monkeypatch.setattr(
        KernelAlgoCreationCoordinatorV2,
        "create",
        lambda _self, _request: {
            "event": event,
            "ingress_receipt": ingress_receipt,
            "algo": SimpleNamespace(
                algo_instance_id=algo_id,
                runtime_id=request.runtime_id,
                parent_intent_id=request.parent_intent_id,
                status=SimpleNamespace(value="ACTIVE"),
                failure_receipt_id=None,
            ),
        },
    )
    cutover = object.__new__(KernelProductCutoverCoordinator)
    monkeypatch.setattr(
        KernelProductCutoverCoordinator,
        "activate_kernel_v2_route_v1",
        lambda _self, **_values: _route_owner(),
    )
    ingress = object.__new__(KernelIngressCoordinatorV1)
    monkeypatch.setattr(KernelIngressCoordinatorV1, "ingest", lambda _self, **_values: ingress_receipt)
    worker = object.__new__(KernelProductDeliveryWorkerV3)
    processed: list[str] = []
    monkeypatch.setattr(
        KernelProductDeliveryWorkerV3,
        "process_committed_delivery_v3",
        lambda _self, *, delivery_id, **_values: processed.append(delivery_id),
    )

    class SourceReader:
        @staticmethod
        def read_committed_source_event_v1(**_values):
            return K6DCommittedSourceEventReadbackV1(event=event, ingress_receipt=ingress_receipt)

    coordinator = MiniQMTKernelV2ProductCoordinator(
        plan_authority_reader=PlanReader(),
        source_event_reader=SourceReader(),
        cutover_coordinator=cutover,
        creation_coordinator_factory=lambda _incarnation: creator,
        ingress_coordinator=ingress,
        delivery_worker=worker,
    )
    started = coordinator.start_execution_plan_v1(
        runtime_id=request.runtime_id,
        binding_id="binding_k6d",
        execution_plan_id=request.execution_plan_id,
        worker_incarnation_id="incarnation_coordinator",
    )
    assert started.success is True
    assert started.ordered_parent_results[0].algo_instance_id == algo_id
    assert (
        coordinator.ingest_committed_source_event_v1(
            runtime_id=request.runtime_id,
            binding_id="binding_k6d",
            source_event_ref=event.event_id,
            worker_incarnation_id="incarnation_coordinator",
        )
        == ingress_receipt
    )
    assert coordinator.ingest_native_event_v1(event=event) == ingress_receipt
    coordinator.process_committed_event_v1(event=event, receipt=ingress_receipt)
    assert processed == ["delivery_start", "delivery_start", "delivery_start"]

    with pytest.raises(ValueError, match="differs from its event authority"):
        coordinator.process_committed_event_v1(
            event=event,
            receipt=ingress_receipt.model_copy(update={"runtime_sequence": 2}),
        )
    with pytest.raises(TypeError, match="canonical identity"):
        coordinator.start_execution_plan_v1(
            runtime_id=" ",
            binding_id="binding_k6d",
            execution_plan_id=request.execution_plan_id,
            worker_incarnation_id="incarnation_coordinator",
        )


def test_product_runtime_carriers_and_constructor_reject_every_malformed_owner() -> None:
    started = K6DProductParentStartResultV1.create(
        plan_intent_ordinal=1,
        parent_intent_id="parent_runtime_validation",
        algo_instance_id="algo_runtime_validation",
        event_id="event_runtime_validation",
        ingress_receipt_sha256="a" * 64,
        start_status=K6DProductStartStatusV1.STARTED,
        terminal_reason_or_null=None,
    )
    for updates in (
        {"plan_intent_ordinal": 0},
        {"coordinator_broker_called": True},
        {"terminal_reason_or_null": "unexpected"},
        {"result_sha256": "b" * 64},
    ):
        with pytest.raises(ValueError):
            K6DProductParentStartResultV1.model_validate(started.model_dump(mode="python") | updates)
    receipt = K6DProductPlanStartReceiptV1.create(
        runtime_id="runtime_validation",
        binding_id="binding_validation",
        execution_plan_id="plan_validation",
        execution_plan_sha256="c" * 64,
        product_route_receipt_sha256="d" * 64,
        ordered_parent_results=(started,),
    )
    for updates in ({"started": 0}, {"success": False}, {"receipt_sha256": "e" * 64}):
        with pytest.raises(ValueError):
            K6DProductPlanStartReceiptV1.model_validate(receipt.model_dump(mode="python") | updates)

    event = _coordinator_event("runtime_validation")
    ingress_receipt = _coordinator_receipt(event)
    assert K6DCommittedSourceEventReadbackV1(event=event, ingress_receipt=ingress_receipt).event == event
    with pytest.raises(TypeError, match="event"):
        K6DCommittedSourceEventReadbackV1(event=object(), ingress_receipt=ingress_receipt)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="do not close"):
        K6DCommittedSourceEventReadbackV1(
            event=event,
            ingress_receipt=ingress_receipt.model_copy(update={"runtime_sequence": 2}),
        )

    class PlanReader:
        @staticmethod
        def read_plan_authority_v1(**_values):
            return None

    class SourceReader:
        @staticmethod
        def read_committed_source_event_v1(**_values):
            return None

    valid = {
        "plan_authority_reader": PlanReader(),
        "source_event_reader": SourceReader(),
        "cutover_coordinator": object.__new__(KernelProductCutoverCoordinator),
        "creation_coordinator_factory": lambda _value: None,
        "ingress_coordinator": object.__new__(KernelIngressCoordinatorV1),
        "delivery_worker": object.__new__(KernelProductDeliveryWorkerV3),
    }
    for field_name in valid:
        values = dict(valid)
        values[field_name] = object()
        with pytest.raises(TypeError):
            MiniQMTKernelV2ProductCoordinator(**values)

    coordinator = object.__new__(MiniQMTKernelV2ProductCoordinator)
    coordinator._delivery_worker = object.__new__(KernelProductDeliveryWorkerV3)
    with pytest.raises(TypeError, match="event"):
        coordinator.process_committed_event_v1(event=object(), receipt=ingress_receipt)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="receipt"):
        coordinator.process_committed_event_v1(event=event, receipt=object())  # type: ignore[arg-type]


class _DueCommand:
    def __init__(self, command_id: str) -> None:
        self.command_id = command_id
        self.status = BrokerCommandOutboxStatusV1.PENDING


class _RuntimeRepository:
    def __init__(self) -> None:
        self.reads = 0

    def list_dispatchable_outbox_commands(self, **_values):
        self.reads += 1
        return (_DueCommand("command_k6d"),) if self.reads == 1 else ()

    def list_reconcilable_outbox_commands(self, **_values):
        return ()


class _Dispatcher:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def dispatch_one(self, **values):
        self.calls.append(values)


class _Reconciler:
    def reconcile_one(self, **_values):
        raise AssertionError("empty reconciliation inventory must not call the reconciler")


class _SnapshotIngress:
    def sync_v1(self, **_values):
        return ()


class _Coordinator:
    def start_execution_plan_v1(self, **_values):
        raise AssertionError("runtime construction must not start a plan")


class _Clock:
    def wake(self, **_values):
        raise AssertionError("outbox drain must not wake the exchange clock")


class _CallbackIngress:
    def ingest_order_v1(self, **_values):
        return "order_event"

    def ingest_trade_v1(self, **_values):
        return "trade_event"


def _runtime_session_authority(runtime_id: str, trade_date: date = date(2026, 8, 4)):
    segments = (
        SessionSegment(time(9, 15), time(9, 25)),
        SessionSegment(time(9, 30), time(11, 30)),
        SessionSegment(time(13), time(14, 57)),
        SessionSegment(time(14, 57), time(15)),
    )
    effective_at = datetime.combine(trade_date, time(0), tzinfo=UTC)
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar_{market.value}_{trade_date:%Y%m%d}",
            market=market,
            trade_date=trade_date,
            timezone="Asia/Shanghai",
            session_segments=segments,
            effective_at_utc=effective_at,
            source_version="aistock_calendar_v1",
        )
        for market in MarketCode
    }
    snapshot_set = CalendarSnapshotSet(snapshot_set_id="calendar_set_runtime_k6d", snapshot_by_market=snapshots)
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    ordered = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
    return type(_authority()).create(
        runtime_id=runtime_id,
        exchange_trade_date=trade_date.isoformat(),
        calendar_snapshot_set_id=snapshot_set.snapshot_set_id,
        calendar_snapshot_set_json=snapshot_json,
        calendar_snapshot_set_sha256=snapshot_set.set_sha256,
        ordered_market_calendar_sha256s=tuple(snapshots[market].calendar_sha256 for market in ordered),
        ordered_session_segments=tuple(segment.canonical_payload() for segment in segments),
        source_effective_at_utc=effective_at,
    )


def _runtime(
    repository,
    dispatcher,
    *,
    symbols: tuple[str, ...] = ("600000.SH",),
    trade_date: date = date(2026, 8, 4),
    quote_context_id: str = "context_runtime_k6d",
) -> SimulationMiniQMTProductRuntimeV1:
    reconciler = _Reconciler() if dispatcher is not None else None
    snapshot_ingress = _SnapshotIngress() if dispatcher is not None else None
    return SimulationMiniQMTProductRuntimeV1(
        coordinator=_Coordinator(),  # type: ignore[arg-type]
        worker_incarnation_id="incarnation_k6d",
        runtime_id="runtime_k6d",
        binding_id="binding_k6d",
        trade_date=trade_date,
        symbols=symbols,
        source_capability_sha256="a" * 64,
        quote_context_id=quote_context_id,
        exchange_session_authority=_runtime_session_authority("runtime_k6d", trade_date),
        repository=repository,  # type: ignore[arg-type]
        clock=_Clock(),  # type: ignore[arg-type]
        outbox_dispatcher=dispatcher,  # type: ignore[arg-type]
        outbox_reconciler=reconciler,  # type: ignore[arg-type]
        callback_ingress=_CallbackIngress(),  # type: ignore[arg-type]
        snapshot_ingress=snapshot_ingress,  # type: ignore[arg-type]
        hot_market_data_ingress=HotMarketDataIngressV1(
            runtime_id="runtime_k6d",
            effect_committer=lambda _effect: None,
        ),
    )


def test_product_outcome_publisher_requires_one_bound_coordinator_and_continues_delivery() -> None:
    with pytest.raises(TypeError, match="KernelOutboxOutcomeIngressV1"):
        product_module._ProductOutcomePublisherV1(object())  # type: ignore[arg-type]

    publisher = object.__new__(product_module._ProductOutcomePublisherV1)
    object.__setattr__(publisher, "_ingress", SimpleNamespace())
    object.__setattr__(publisher, "_coordinator", None)
    with pytest.raises(RuntimeError, match="no bound coordinator"):
        publisher.ingest_outbox_outcome_v1(command_id="command_unbound")

    processed: list[tuple[object, object]] = []
    event = _coordinator_event("runtime_outcome_publisher")
    receipt = _coordinator_receipt(event)
    outcome = SimpleNamespace(event=event, ingress_receipt=receipt)
    object.__setattr__(publisher, "_ingress", SimpleNamespace(ingest_outbox_outcome_v1=lambda **_values: outcome))
    coordinator = SimpleNamespace(
        process_committed_event_v1=lambda **values: processed.append((values["event"], values["receipt"]))
    )
    object.__setattr__(publisher, "_coordinator", coordinator)
    assert publisher.ingest_outbox_outcome_v1(command_id="command_outcome") is outcome
    assert processed == [(event, receipt)]


def test_product_runtime_rejects_incomplete_components_hash_and_naive_time() -> None:
    repository = _RuntimeRepository()
    runtime = _runtime(repository, None)
    with pytest.raises(ValueError, match="source_capability_sha256"):
        object.__setattr__(runtime, "source_capability_sha256", "g" * 64)
        runtime.__post_init__()
    object.__setattr__(runtime, "source_capability_sha256", "a" * 64)

    with pytest.raises(TypeError, match="timezone-aware"):
        runtime.wake_clock_v1(observed_at=datetime(2026, 8, 4, 9, 30), monotonic_ns=1)
    with pytest.raises(TypeError, match="positive process-monotonic"):
        runtime.wake_clock_v1(observed_at=datetime(2026, 8, 4, 1, 30, tzinfo=UTC), monotonic_ns=0)

    with pytest.raises(TypeError, match="dispatcher, reconciler and snapshot"):
        SimulationMiniQMTProductRuntimeV1(
            coordinator=_Coordinator(),  # type: ignore[arg-type]
            worker_incarnation_id="incarnation_k6d",
            runtime_id="runtime_k6d",
            binding_id="binding_k6d",
            trade_date=date(2026, 8, 4),
            symbols=("600000.SH",),
            source_capability_sha256="a" * 64,
            quote_context_id="context_runtime_k6d",
            exchange_session_authority=_runtime_session_authority("runtime_k6d"),
            repository=repository,  # type: ignore[arg-type]
            clock=_Clock(),  # type: ignore[arg-type]
            outbox_dispatcher=_Dispatcher(),  # type: ignore[arg-type]
            outbox_reconciler=None,
            callback_ingress=_CallbackIngress(),  # type: ignore[arg-type]
            snapshot_ingress=None,
            hot_market_data_ingress=HotMarketDataIngressV1(
                runtime_id="runtime_k6d",
                effect_committer=lambda _effect: None,
            ),
        )


def _plan_reader_facts():
    policy_json = {
        "algo_code": "SNIPER_MINIQMT",
        "algo_config": {"price_mode": "LIMIT_TRIGGER_BY_BEST_QUOTE"},
    }
    policy_hash = compute_execution_policy_sha256(policy_json)
    binding_hash = "b" * 64
    release_hash = "c" * 64
    binding = SimpleNamespace(
        binding_id="binding_plan_reader",
        binding_hash=binding_hash,
        strategy_id="strategy_plan_reader",
        strategy_slot_id="slot_plan_reader",
        release_id="release_plan_reader",
        release_hash=release_hash,
        broker_backend=SimulationBrokerBackend.MINIQMT_SIM,
    )
    intent = SimpleNamespace(
        intent_id="intent_plan_reader",
        symbol="600000.SH",
        side=OrderSide.BUY,
        trading_rule_decision_id="decision_plan_reader",
        price_policy={"limit_price": "10.25"},
        strategy_slot_id="slot_plan_reader",
        order_quantity=100,
        target_weight=0.25,
    )
    decision = SimpleNamespace(
        decision_id="decision_plan_reader",
        symbol="600000.SH",
        side=OrderSide.BUY,
        market_board="MAIN_BOARD",
        requested_quantity=100,
        legal_quantity=100,
        decision="EMIT",
        lot_rule={"min_quantity": 100, "increment": 100},
        price_limit_rule={"limit_up": "11.00", "limit_down": "9.00"},
        decision_hash="d" * 64,
    )
    policy = {
        "policy_json": policy_json,
        "policy_version_id": "policy_plan_reader",
        "policy_sha256": policy_hash,
    }
    plan = SimpleNamespace(
        plan_id="plan_plan_reader",
        plan_hash="a" * 64,
        binding_id=binding.binding_id,
        binding_hash=binding_hash,
        release_id=binding.release_id,
        release_hash=release_hash,
        package_id="package_plan_reader",
        target_trade_date=date(2026, 7, 27),
        execution_policy_version_id="policy_plan_reader",
        execution_policy_sha256=policy_hash,
        plan_payload_json={
            "execution_policy": {
                "version_id": "policy_plan_reader",
                "sha256": policy_hash,
                "payload": policy,
            }
        },
        intents=[intent],
        trading_rule_decisions=[decision],
    )
    release = SimpleNamespace(release_id=binding.release_id, release_hash=release_hash)
    account = VirtualAccount(
        strategy_id=binding.strategy_id,
        strategy_name="strategy_plan_reader",
        display_name="Strategy plan reader",
        account_id="miniqmt_sim_account",
        mode="SIM",
        initial_cash=Decimal("100000"),
        cash=Decimal("100000"),
        status=VirtualAccountStatus.ENABLED,
        risk_config={"max_position_ratio": "0.25"},
        created_at=datetime(2026, 7, 27, 0, 0, tzinfo=UTC),
        updated_at=datetime(2026, 7, 27, 1, 15, tzinfo=UTC),
    )
    runtime_id = _runtime_id(plan, binding)
    original = _authority()
    session = type(original).create(
        runtime_id=runtime_id,
        exchange_trade_date=original.exchange_trade_date,
        calendar_snapshot_set_id=original.calendar_snapshot_set_id,
        calendar_snapshot_set_json=original.calendar_snapshot_set_json,
        calendar_snapshot_set_sha256=original.calendar_snapshot_set_sha256,
        ordered_market_calendar_sha256s=original.ordered_market_calendar_sha256s,
        ordered_session_segments=original.ordered_session_segments,
        source_effective_at_utc=original.source_effective_at_utc,
    )
    repository = SimpleNamespace(
        get_execution_plan=lambda _plan_id: plan,
        get_simulation_release_binding=lambda _binding_id: binding,
        get_strategy_runtime_release=lambda _release_id: release,
    )
    accounts = SimpleNamespace(get_virtual_account=lambda _strategy_id: account)
    return plan, binding, session, repository, accounts, runtime_id


def _regenerated_session_authority(
    session,
    *,
    effective_at_utc: datetime,
    segments: tuple[SessionSegment, ...] | None = None,
    source_version: str = "aistock_calendar_v1",
):
    exact_segments = segments or tuple(
        SessionSegment(
            start_local=time.fromisoformat(payload["start_local"]),
            end_local=time.fromisoformat(payload["end_local"]),
        )
        for payload in (thaw_json_v1(item) for item in session.ordered_session_segments)
    )
    snapshots = {
        market: CalendarSnapshot(
            calendar_id=f"calendar_{market.value}_20260727",
            market=market,
            trade_date=date(2026, 7, 27),
            timezone="Asia/Shanghai",
            session_segments=exact_segments,
            effective_at_utc=effective_at_utc,
            source_version=source_version,
        )
        for market in MarketCode
    }
    snapshot_set = CalendarSnapshotSet(
        snapshot_set_id=f"calendar_set_restart_{effective_at_utc.timestamp():.0f}",
        snapshot_by_market=snapshots,
    )
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    ordered = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
    return type(session).create(
        runtime_id=session.runtime_id,
        exchange_trade_date=session.exchange_trade_date,
        calendar_snapshot_set_id=snapshot_set.snapshot_set_id,
        calendar_snapshot_set_json=snapshot_json,
        calendar_snapshot_set_sha256=snapshot_set.set_sha256,
        ordered_market_calendar_sha256s=tuple(
            snapshot_set.snapshot_by_market[market].calendar_sha256 for market in ordered
        ),
        ordered_session_segments=tuple(segment.canonical_payload() for segment in exact_segments),
        source_effective_at_utc=effective_at_utc,
    )


def test_product_session_restart_reuses_durable_authority_for_same_economic_session() -> None:
    _plan, _binding, persisted, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 27, 0, 30, tzinfo=UTC),
    )
    assert candidate != persisted
    assert candidate.calendar_snapshot_set_sha256 != persisted.calendar_snapshot_set_sha256

    class Repository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            return persisted

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise AssertionError("an existing durable authority must be resolved before insert")

    assert (
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=Repository(),
            candidate=candidate,
        )
        == persisted
    )


def test_product_session_restart_race_reuses_compatible_durable_authority() -> None:
    _plan, _binding, persisted, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 27, 0, 30, tzinfo=UTC),
    )

    class Repository:
        read_count = 0

        def read_exchange_session_authority(self, **_values):
            self.read_count += 1
            if self.read_count == 1:
                raise KeyError("not committed yet")
            return persisted

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise KernelRepositoryConflict("concurrent exchange-session writer won")

    repository = Repository()
    assert (
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=repository,
            candidate=candidate,
        )
        == persisted
    )
    assert repository.read_count == 2


def test_product_session_commit_unknown_uses_exact_readback_or_preserves_uncertainty() -> None:
    _plan, _binding, persisted, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 27, 0, 30, tzinfo=UTC),
    )

    class CommittedRepository:
        read_count = 0

        def read_exchange_session_authority(self, **_values):
            self.read_count += 1
            if self.read_count == 1:
                raise KeyError("not visible before write")
            return persisted

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise KernelRepositoryCommitUnknown("commit acknowledgement lost")

    committed = CommittedRepository()
    assert (
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=committed,
            candidate=candidate,
        )
        == persisted
    )
    assert committed.read_count == 2

    class UnknownRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KeyError("not visible")

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise KernelRepositoryCommitUnknown("commit remains unknown")

    with pytest.raises(KernelRepositoryCommitUnknown, match="commit remains unknown"):
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=UnknownRepository(),
            candidate=candidate,
        )


def test_product_session_restart_rejects_observation_older_than_durable_generation() -> None:
    _plan, _binding, persisted, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 26, 15, 59, tzinfo=UTC),
    )

    class Repository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            return persisted

    with pytest.raises(MiniQMTKernelProductCompositionError) as caught:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=Repository(),
            candidate=candidate,
        )
    assert caught.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_REBUILD_STALE"
    assert caught.value.context["persisted_source_effective_at_utc"] == persisted.source_effective_at_utc
    assert caught.value.context["candidate_source_effective_at_utc"] == candidate.source_effective_at_utc
    assert caught.value.context["economic_conflict_fields"] == []
    assert caught.value.context["broker_called"] is False


def test_product_session_restart_rejects_true_economic_authority_drift_with_hash_evidence() -> None:
    _plan, _binding, persisted, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 27, 0, 30, tzinfo=UTC),
        source_version="aistock_calendar_v2-conflict",
    )

    class Repository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            return persisted

    with pytest.raises(MiniQMTKernelProductCompositionError) as caught:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=Repository(),
            candidate=candidate,
        )
    assert caught.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_AUTHORITY_DRIFT"
    assert caught.value.context == {
        "persisted_runtime_id": persisted.runtime_id,
        "candidate_runtime_id": candidate.runtime_id,
        "persisted_exchange_trade_date": persisted.exchange_trade_date,
        "candidate_exchange_trade_date": candidate.exchange_trade_date,
        "persisted_authority_sha256": persisted.authority_sha256,
        "candidate_authority_sha256": candidate.authority_sha256,
        "persisted_calendar_snapshot_set_id": persisted.calendar_snapshot_set_id,
        "candidate_calendar_snapshot_set_id": candidate.calendar_snapshot_set_id,
        "persisted_calendar_snapshot_set_sha256": persisted.calendar_snapshot_set_sha256,
        "candidate_calendar_snapshot_set_sha256": candidate.calendar_snapshot_set_sha256,
        "persisted_source_effective_at_utc": persisted.source_effective_at_utc,
        "candidate_source_effective_at_utc": candidate.source_effective_at_utc,
        "persisted_economic_authority_sha256": product_module._exchange_session_economic_authority_sha256_v1(  # type: ignore[attr-defined]
            persisted
        ),
        "candidate_economic_authority_sha256": product_module._exchange_session_economic_authority_sha256_v1(  # type: ignore[attr-defined]
            candidate
        ),
        "economic_conflict_fields": ["ordered_market_snapshots"],
        "broker_called": False,
    }


def test_product_session_resolver_fails_loud_on_invalid_readback_and_orphan_write_conflict() -> None:
    _plan, _binding, candidate, _repository, _accounts, _runtime_id_value = _plan_reader_facts()
    with pytest.raises(TypeError, match="authority must be"):
        product_module._exchange_session_economic_authority_payload_v1(object())  # type: ignore[attr-defined,arg-type]
    with pytest.raises(TypeError, match="candidate must be"):
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined,arg-type]
            repository=object(),
            candidate=object(),
        )

    class InvalidReadbackRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            return {"authority_sha256": candidate.authority_sha256}

    with pytest.raises(MiniQMTKernelProductCompositionError) as invalid:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=InvalidReadbackRepository(),
            candidate=candidate,
        )
    assert invalid.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_INVALID"
    assert invalid.value.context["readback_type"] == "dict"
    assert invalid.value.context["broker_called"] is False

    class ConflictingReadbackRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KernelRepositoryConflict("scalar authority drift")

    with pytest.raises(MiniQMTKernelProductCompositionError) as conflicting:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=ConflictingReadbackRepository(),
            candidate=candidate,
        )
    assert conflicting.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_CONFLICT"
    assert conflicting.value.context["repository_conflict"] == "scalar authority drift"
    assert conflicting.value.context["broker_called"] is False

    class InvalidWriterRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KeyError("missing")

        @staticmethod
        def write_exchange_session_authority(_authority):
            return {"authority_sha256": candidate.authority_sha256}

    with pytest.raises(MiniQMTKernelProductCompositionError) as invalid_writer:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=InvalidWriterRepository(),
            candidate=candidate,
        )
    assert invalid_writer.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_INVALID"
    assert invalid_writer.value.context["readback_type"] == "dict"
    assert invalid_writer.value.context["broker_called"] is False

    class ExactWriterRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KeyError("missing")

        @staticmethod
        def write_exchange_session_authority(_authority):
            return candidate

    assert (
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=ExactWriterRepository(),
            candidate=candidate,
        )
        == candidate
    )

    class OrphanConflictRepository:
        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KeyError("missing")

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise KernelRepositoryConflict("insert conflict without durable row")

    with pytest.raises(MiniQMTKernelProductCompositionError) as orphan:
        product_module._resolve_product_exchange_session_authority_v1(  # type: ignore[attr-defined]
            repository=OrphanConflictRepository(),
            candidate=candidate,
        )
    assert orphan.value.reason_code == "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_WRITE_CONFLICT"
    assert orphan.value.context["candidate_authority_sha256"] == candidate.authority_sha256
    assert orphan.value.context["repository_conflict"] == "insert conflict without durable row"
    assert orphan.value.context["broker_called"] is False


def test_plan_authority_reader_closes_plan_binding_session_account_and_policy() -> None:
    plan, binding, session, repository, accounts, runtime_id = _plan_reader_facts()
    reader = SimulationK6DPlanAuthorityReader(
        simulation_repository=repository,
        account_repository=accounts,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
        session_authority=session,
        logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )
    authority = reader.read_plan_authority_v1(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        execution_plan_id=plan.plan_id,
    )
    request = authority.ordered_creation_requests[0]
    assert request.runtime_id == runtime_id
    assert request.parent_intent_id == "intent_plan_reader"
    assert request.algo_code == "SNIPER_MINIQMT"
    assert request.parent_quantity == 100
    assert thaw_json_v1(request.account_projection) == {
        "strategy_id": "strategy_plan_reader",
        "strategy_name": "strategy_plan_reader",
        "account_id": "miniqmt_sim_account",
        "mode": "SIM",
        "cash": "100000",
        "frozen_cash": "0",
        "market_value": "0",
        "status": "ENABLED",
        "risk_config": {"max_position_ratio": "0.25"},
        "updated_at_utc": "2026-07-27T01:15:00+00:00",
    }
    assert kernel_product_evidence.KernelProductEvidenceProviderV3._account_payload(
        accounts.get_virtual_account(binding.strategy_id)
    ) == thaw_json_v1(request.account_projection)

    mismatched_session = _authority()
    invalid = SimulationK6DPlanAuthorityReader(
        simulation_repository=repository,
        account_repository=accounts,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
        session_authority=mismatched_session,
        logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )
    with pytest.raises(MiniQMTKernelProductCompositionError, match="do not form one owner"):
        invalid.read_plan_authority_v1(
            runtime_id=runtime_id,
            binding_id=binding.binding_id,
            execution_plan_id=plan.plan_id,
        )


def test_plan_authority_reader_rejects_policy_fallback_and_incomplete_plan_sets() -> None:
    plan, binding, session, repository, accounts, runtime_id = _plan_reader_facts()

    def read(mutated_plan, *, account_repository=accounts):
        reader = SimulationK6DPlanAuthorityReader(
            simulation_repository=SimpleNamespace(
                get_execution_plan=lambda _plan_id: mutated_plan,
                get_simulation_release_binding=repository.get_simulation_release_binding,
                get_strategy_runtime_release=repository.get_strategy_runtime_release,
            ),
            account_repository=account_repository,
            gateway_catalog=build_k6d_gateway_catalog_v1(),
            session_authority=session,
            logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
        )
        return reader.read_plan_authority_v1(
            runtime_id=runtime_id,
            binding_id=binding.binding_id,
            execution_plan_id=plan.plan_id,
        )

    for mutate, reason_code in (
        (
            lambda value: value.plan_payload_json["execution_policy"].pop("version_id"),
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
        ),
        (
            lambda value: value.plan_payload_json["execution_policy"]["payload"].__setitem__(
                "validated_execution_policy_id", value.execution_policy_version_id
            ),
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
        ),
        (
            lambda value: value.plan_payload_json["execution_policy"]["payload"]["policy_json"].__setitem__(
                "algo_code", 7
            ),
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
        ),
        (
            lambda value: value.plan_payload_json["execution_policy"]["payload"]["policy_json"].__setitem__(
                "algo_code", "UNKNOWN_MINIQMT"
            ),
            "MINIQMT_K6_PRODUCT_ALGO_UNSUPPORTED",
        ),
        (
            lambda value: value.plan_payload_json["execution_policy"]["payload"]["policy_json"].__setitem__(
                "algo_config", []
            ),
            "MINIQMT_K6_PRODUCT_PLUGIN_CONFIG_INVALID",
        ),
        (
            lambda value: value.plan_payload_json["execution_policy"]["payload"].__setitem__("policy_sha256", "0" * 64),
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_DRIFT",
        ),
    ):
        mutated = copy.deepcopy(plan)
        mutate(mutated)
        with pytest.raises(MiniQMTKernelProductCompositionError) as failure:
            read(mutated)
        assert failure.value.reason_code == reason_code

    empty = copy.deepcopy(plan)
    empty.intents = []
    with pytest.raises(MiniQMTKernelProductCompositionError) as empty_failure:
        read(empty)
    assert empty_failure.value.reason_code == "MINIQMT_K6_PRODUCT_PLAN_EMPTY"

    duplicate = copy.deepcopy(plan)
    duplicate.trading_rule_decisions.append(copy.deepcopy(duplicate.trading_rule_decisions[0]))
    with pytest.raises(MiniQMTKernelProductCompositionError) as duplicate_failure:
        read(duplicate)
    assert duplicate_failure.value.reason_code == "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID"

    orphan_emit = copy.deepcopy(plan)
    orphan_emit.trading_rule_decisions.append(
        SimpleNamespace(
            decision_id="decision_orphan_emit",
            symbol="600001.SH",
            side=OrderSide.BUY,
            market_board="MAIN_BOARD",
            requested_quantity=100,
            legal_quantity=100,
            decision="EMIT",
            lot_rule={"min_quantity": 100, "increment": 100},
            price_limit_rule={},
            decision_hash="e" * 64,
        )
    )
    with pytest.raises(MiniQMTKernelProductCompositionError) as orphan_emit_failure:
        read(orphan_emit)
    assert orphan_emit_failure.value.reason_code == "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID"

    rejected_with_parent = copy.deepcopy(plan)
    rejected_with_parent.trading_rule_decisions[0].decision = "REJECT"
    rejected_with_parent.trading_rule_decisions[0].legal_quantity = 0
    with pytest.raises(MiniQMTKernelProductCompositionError) as rejected_with_parent_failure:
        read(rejected_with_parent)
    assert rejected_with_parent_failure.value.reason_code == "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID"

    quantity_drift = copy.deepcopy(plan)
    quantity_drift.trading_rule_decisions[0].requested_quantity = 200
    quantity_drift.trading_rule_decisions[0].legal_quantity = 200
    with pytest.raises(MiniQMTKernelProductCompositionError) as quantity_drift_failure:
        read(quantity_drift)
    assert quantity_drift_failure.value.reason_code == "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID"

    with pytest.raises(MiniQMTKernelProductCompositionError) as account_failure:
        read(plan, account_repository=SimpleNamespace(get_virtual_account=lambda _strategy_id: object()))
    assert account_failure.value.reason_code == "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID"

    old_parallel_carrier = SimpleNamespace(
        model_dump=lambda **_values: {"strategy_id": binding.strategy_id, "cash": "100000"}
    )
    with pytest.raises(MiniQMTKernelProductCompositionError) as parallel_carrier_failure:
        read(
            plan,
            account_repository=SimpleNamespace(get_virtual_account=lambda _strategy_id: old_parallel_carrier),
        )
    assert parallel_carrier_failure.value.reason_code == "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID"

    wrong_owner = replace(accounts.get_virtual_account(binding.strategy_id), strategy_id="strategy_other")
    with pytest.raises(MiniQMTKernelProductCompositionError) as owner_failure:
        read(plan, account_repository=SimpleNamespace(get_virtual_account=lambda _strategy_id: wrong_owner))
    assert owner_failure.value.reason_code == "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID"
    assert owner_failure.value.context["expected_strategy_id"] == binding.strategy_id
    assert owner_failure.value.context["actual_strategy_id"] == "strategy_other"

    malformed_account = replace(accounts.get_virtual_account(binding.strategy_id), risk_config={"bad": object()})
    with pytest.raises(MiniQMTKernelProductCompositionError) as malformed_failure:
        read(
            plan,
            account_repository=SimpleNamespace(get_virtual_account=lambda _strategy_id: malformed_account),
        )
    assert malformed_failure.value.reason_code == "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID"


def test_plan_authority_reader_keeps_non_emitted_reject_as_planning_subject() -> None:
    plan, binding, session, repository, accounts, runtime_id = _plan_reader_facts()
    plan.trading_rule_decisions.append(
        SimpleNamespace(
            decision_id="decision_rejected_subject",
            symbol="600001.SH",
            side=OrderSide.SELL,
            market_board="MAIN_BOARD",
            requested_quantity=100,
            legal_quantity=0,
            decision="REJECT",
            lot_rule={"min_quantity": 100, "increment": 100},
            price_limit_rule={"pre_trade_tradability": {"is_tradable": False}},
            decision_hash="f" * 64,
        )
    )
    reader = SimulationK6DPlanAuthorityReader(
        simulation_repository=repository,
        account_repository=accounts,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
        session_authority=session,
        logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )

    authority = reader.read_plan_authority_v1(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        execution_plan_id=plan.plan_id,
    )

    assert [request.parent_intent_id for request in authority.ordered_creation_requests] == [
        "intent_plan_reader"
    ]
    assert {decision.decision_id for decision in plan.trading_rule_decisions} == {
        "decision_plan_reader",
        "decision_rejected_subject",
    }


def _production_frozen_price_limit_rule() -> dict:
    """Real-run 2026-08-18 frozen pre-trade quote evidence (binary floats)."""
    return {
        "pre_trade_tradability": {
            "source": "MINIQMT_REALTIME.broker_quote",
            "symbol": "000779.SZ",
            "trade_date": "2026-08-18",
            "is_tradable": True,
            "reason_code": "OK",
            "quote_evidence": {
                "schema_version": "pre_trade_quote_tradability_evidence_v1",
                "symbol": "000779.SZ",
                "quote_source": "MINIQMT_REALTIME.broker_quote",
                "quote_present": True,
                "quote_timestamp": "2026-08-18T09:20:09",
                "quote_age_seconds": 7.560314,
                "quote_max_age_seconds": 300.0,
                "last_price": 10.99,
                "pre_close": 10.63,
                "open": None,
                "high": None,
                "low": None,
                "total_hand": 0.0,
                "amount": 0.0,
                "bid_price_1": 10.99,
                "bid_volume_1": 4997.0,
                "ask_price_1": 10.99,
                "ask_volume_1": 4997.0,
                "ohl_zero": True,
                "turnover_zero": True,
                "book_empty": False,
                "no_tradable_market": False,
                "limit_pct": 0.1,
                "limit_up": 11.69,
                "limit_down": 9.57,
                "quote_price_basis": "yuan",
                "is_st": False,
                "st_status_source": "market.stock_st",
                "at_limit_up": False,
                "at_limit_down": False,
                "blocked_sides": [],
                "requested_side": None,
                "side_block_reason_code": None,
                "limit_state_reason_code": None,
            },
            "suspend_status": {
                "source": "market.suspend_d",
                "is_suspended": False,
                "suspend_type": None,
                "suspend_timing": None,
            },
            "schema_version": "pre_trade_tradability_status_v1",
        }
    }


def _read_authority_with_price_limit_rule(rule: dict):
    plan, binding, session, repository, accounts, runtime_id = _plan_reader_facts()
    plan.trading_rule_decisions[0].price_limit_rule = rule
    reader = SimulationK6DPlanAuthorityReader(
        simulation_repository=repository,
        account_repository=accounts,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
        session_authority=session,
        logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )
    authority = reader.read_plan_authority_v1(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        execution_plan_id=plan.plan_id,
    )
    return authority, plan


def test_plan_authority_reader_canonicalizes_real_run_binary_float_price_limit_rule() -> None:
    authority, _plan = _read_authority_with_price_limit_rule(_production_frozen_price_limit_rule())

    request = authority.ordered_creation_requests[0]
    contract = thaw_json_v1(request.contract_projection)
    evidence = contract["price_limit_rule"]["pre_trade_tradability"]["quote_evidence"]
    assert evidence["limit_up"] == "11.69"
    assert evidence["limit_down"] == "9.57"
    assert evidence["pre_close"] == "10.63"
    assert evidence["last_price"] == "10.99"
    assert evidence["limit_pct"] == "0.1"
    assert evidence["bid_price_1"] == "10.99"
    assert evidence["ask_price_1"] == "10.99"
    assert evidence["bid_volume_1"] == "4997"
    assert evidence["ask_volume_1"] == "4997"
    assert evidence["amount"] == "0"
    assert evidence["total_hand"] == "0"
    assert evidence["quote_age_seconds"] == "7.560314"
    assert evidence["quote_max_age_seconds"] == "300"
    assert evidence["open"] is None
    assert evidence["blocked_sides"] == []
    assert evidence["requested_side"] is None
    assert evidence["quote_present"] is True
    assert contract["price_limit_rule"]["pre_trade_tradability"]["is_tradable"] is True
    assert contract["price_limit_rule"]["pre_trade_tradability"]["suspend_status"] == {
        "source": "market.suspend_d",
        "is_suspended": False,
        "suspend_type": None,
        "suspend_timing": None,
    }
    assert request.contract_projection_sha256 == hash_hex_v1("miniqmt_contract_projection_v1", contract)

    second_authority, _again = _read_authority_with_price_limit_rule(_production_frozen_price_limit_rule())
    second = second_authority.ordered_creation_requests[0]
    assert second.contract_projection_sha256 == request.contract_projection_sha256
    assert thaw_json_v1(second.contract_projection) == contract


def test_plan_authority_reader_normalizes_equivalent_decimal_representations() -> None:
    authority, _plan = _read_authority_with_price_limit_rule(
        {"limit_up": 11.0, "samples": [1.50, -0.0, 10.630, 7, True, None], "note": "plain"}
    )
    contract = thaw_json_v1(authority.ordered_creation_requests[0].contract_projection)
    assert contract["price_limit_rule"]["limit_up"] == "11"
    assert contract["price_limit_rule"]["samples"] == ["1.5", "0", "10.63", 7, True, None]
    assert contract["price_limit_rule"]["note"] == "plain"

    float_authority, _f = _read_authority_with_price_limit_rule({"ratio": 0.1})
    decimal_authority, _d = _read_authority_with_price_limit_rule({"ratio": Decimal("0.10")})
    float_contract = thaw_json_v1(float_authority.ordered_creation_requests[0].contract_projection)
    decimal_contract = thaw_json_v1(decimal_authority.ordered_creation_requests[0].contract_projection)
    assert float_contract["price_limit_rule"] == {"ratio": "0.1"}
    assert decimal_contract["price_limit_rule"] == {"ratio": "0.1"}
    assert (
        float_authority.ordered_creation_requests[0].contract_projection_sha256
        == decimal_authority.ordered_creation_requests[0].contract_projection_sha256
    )

    negative_authority, _n = _read_authority_with_price_limit_rule({"offset": -0.5})
    negative_contract = thaw_json_v1(negative_authority.ordered_creation_requests[0].contract_projection)
    assert negative_contract["price_limit_rule"] == {"offset": "-0.5"}

    lower_authority, _l = _read_authority_with_price_limit_rule({"limit_up": 11.69})
    higher_authority, _h = _read_authority_with_price_limit_rule({"limit_up": 11.691})
    assert (
        lower_authority.ordered_creation_requests[0].contract_projection_sha256
        != higher_authority.ordered_creation_requests[0].contract_projection_sha256
    )


def test_plan_authority_reader_rejects_non_finite_and_foreign_rule_carriers_fail_closed() -> None:
    for bad_value in (
        float("nan"),
        float("inf"),
        float("-inf"),
        {"nested": [float("nan")]},
        {"tuple": (1, 2)},
        {"set": {1, 2}},
        {"opaque": object()},
        {1: "non-string-key"},
    ):
        rule = bad_value if isinstance(bad_value, dict) else {"limit_up": bad_value}
        with pytest.raises(MiniQMTKernelProductCompositionError) as failure:
            _read_authority_with_price_limit_rule(rule)
        assert failure.value.reason_code == "MINIQMT_K6_PRODUCT_PRICE_LIMIT_RULE_AUTHORITY_INVALID"
        assert failure.value.context["broker_called"] is False


def test_plan_authority_reader_keeps_legal_reject_partition_with_float_rules() -> None:
    plan, binding, session, repository, accounts, runtime_id = _plan_reader_facts()
    plan.trading_rule_decisions[0].price_limit_rule = _production_frozen_price_limit_rule()
    plan.trading_rule_decisions.append(
        SimpleNamespace(
            decision_id="decision_rejected_float_subject",
            symbol="600001.SH",
            side=OrderSide.SELL,
            market_board="MAIN_BOARD",
            requested_quantity=100,
            legal_quantity=0,
            decision="REJECT",
            lot_rule={"min_quantity": 100, "increment": 100},
            price_limit_rule={"pre_trade_tradability": {"is_tradable": False, "last_price": 9.99}},
            decision_hash="f" * 64,
        )
    )
    reader = SimulationK6DPlanAuthorityReader(
        simulation_repository=repository,
        account_repository=accounts,
        gateway_catalog=build_k6d_gateway_catalog_v1(),
        session_authority=session,
        logical_time_utc=datetime(2026, 7, 27, 1, 30, tzinfo=UTC),
    )

    authority = reader.read_plan_authority_v1(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        execution_plan_id=plan.plan_id,
    )

    assert [request.parent_intent_id for request in authority.ordered_creation_requests] == [
        "intent_plan_reader"
    ]
    assert len(plan.trading_rule_decisions) == 2


def test_product_committed_source_reader_requires_strict_route_event_and_receipt() -> None:
    event = _coordinator_event("runtime_source_reader")
    receipt = _coordinator_receipt(event)

    class Repository:
        transaction = {"event": event, "receipt": receipt}
        route_value = "KERNEL_V2"

        def read_event_transaction(self, _event_id):
            return self.transaction

        @staticmethod
        def read_runtime_trade_date(_runtime_id):
            return date(2026, 8, 4)

        def read_product_route_owner_v1(self, **_values):
            return SimpleNamespace(route_owner=SimpleNamespace(value=self.route_value))

    repository = Repository()
    reader = product_module._CommittedKernelSourceReader(repository)  # type: ignore[arg-type]
    assert (
        reader.read_committed_source_event_v1(
            runtime_id=event.runtime_id,
            binding_id="binding_source_reader",
            source_event_ref=event.event_id,
        ).ingress_receipt
        == receipt
    )

    repository.route_value = "LEGACY"
    with pytest.raises(MiniQMTKernelProductCompositionError) as route_failure:
        reader.read_committed_source_event_v1(
            runtime_id=event.runtime_id,
            binding_id="binding_source_reader",
            source_event_ref=event.event_id,
        )
    assert route_failure.value.reason_code == "MINIQMT_K6_PRODUCT_ROUTE_OWNER_INVALID"

    repository.route_value = "KERNEL_V2"
    repository.transaction = {"event": event, "receipt": object()}
    with pytest.raises(MiniQMTKernelProductCompositionError) as carrier_failure:
        reader.read_committed_source_event_v1(
            runtime_id=event.runtime_id,
            binding_id="binding_source_reader",
            source_event_ref=event.event_id,
        )
    assert carrier_failure.value.reason_code == "MINIQMT_K6_PRODUCT_SOURCE_READBACK_INVALID"


def test_product_composition_root_builds_preview_runtime_without_parallel_route(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, binding, session, simulation_repository, _accounts, runtime_id = _plan_reader_facts()
    registered: list[tuple[object, tuple[str, ...]]] = []

    class ProductRepository:
        def ensure_product_runtime_v1(self, **_values):
            return None

        @staticmethod
        def read_exchange_session_authority(**_values):
            raise KeyError("not persisted")

        def write_exchange_session_authority(self, authority):
            return authority

        @staticmethod
        def activate_kernel_v2_route_v1(**_values):
            return None

        @staticmethod
        def start_worker_incarnation(**_values):
            return SimpleNamespace(worker_id="worker_product_root", process_incarnation_id="incarnation_product_root")

        @staticmethod
        def list_dispatchable_outbox_commands(**_values):
            return ()

        @staticmethod
        def list_reconcilable_outbox_commands(**_values):
            return ()

        @staticmethod
        def read_runtime_last_event_sequence(_runtime_id):
            return 0

        @staticmethod
        def read_event_transaction(_event_id):
            raise KeyError(_event_id)

    product_repository = ProductRepository()

    monkeypatch.setattr(product_module, "PostgresMiniQMTKernelRepository", lambda **_values: product_repository)
    monkeypatch.setattr(
        product_module,
        "build_hot_full_five_catalog_authority_v1",
        lambda **_values: SimpleNamespace(
            catalog_runtime=SimpleNamespace(snapshot=object()),
            conformance_authority=object(),
        ),
    )
    monkeypatch.setattr(product_module, "_session_authority", lambda _runtime_id, _context: session)
    monkeypatch.setattr(
        product_module,
        "build_k6d_route_source_capability_v1",
        lambda: SimpleNamespace(capability_sha256="e" * 64),
    )
    for owner in (
        product_module.KernelProductDeliveryWorkerV3,
        product_module.KernelOutboxOutcomeIngressV1,
        product_module.MiniQMTKernelV2ProductCoordinator,
        product_module.KernelIngressCoordinatorV1,
        product_module.KernelProductCallbackIngressV1,
        product_module.ExchangeSessionClockV1,
    ):
        monkeypatch.setattr(owner, "__init__", lambda _self, **_values: None)
    activation = SimpleNamespace(
        get_kernel_product_runtime=lambda _runtime_id: None,
        register_kernel_product_runtime=lambda *, runtime, symbols: registered.append((runtime, symbols)),
    )
    context = _context()
    managed = SimpleNamespace(_repository=object(), _broker=None, preview_order=lambda _request: None)
    runtime = build_simulation_miniqmt_product_runtime_v1(
        simulation_repository=simulation_repository,
        execution_plan=plan,
        binding=binding,
        managed_order_service=managed,
        quote_context_adapter=SimpleNamespace(context_store=SimpleNamespace(snapshot=lambda: context)),
        quote_ingress_activation=activation,
        observed_at=datetime(2026, 7, 27, 1, 20, tzinfo=UTC),
        broker_side_effects_enabled=False,
    )
    assert runtime.runtime_id == runtime_id
    assert runtime.outbox_dispatcher is None
    assert registered == [(runtime, ("600000.SH",))]


def test_product_composition_restart_uses_durable_session_without_broker_side_effect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, binding, persisted, simulation_repository, _accounts, runtime_id = _plan_reader_facts()
    candidate = _regenerated_session_authority(
        persisted,
        effective_at_utc=datetime(2026, 7, 27, 0, 30, tzinfo=UTC),
    )
    registered: list[object] = []
    worker_start_calls: list[dict[str, object]] = []

    class ProductRepository:
        @staticmethod
        def ensure_product_runtime_v1(**_values):
            return None

        @staticmethod
        def read_exchange_session_authority(**_values):
            return persisted

        @staticmethod
        def write_exchange_session_authority(_authority):
            raise AssertionError("restart must not overwrite the durable session generation")

        @staticmethod
        def activate_kernel_v2_route_v1(**_values):
            return None

        @staticmethod
        def start_worker_incarnation(**values):
            worker_start_calls.append(values)
            return SimpleNamespace(worker_id="worker_product_restart", process_incarnation_id="incarnation_restart")

        @staticmethod
        def list_dispatchable_outbox_commands(**_values):
            return ()

        @staticmethod
        def list_reconcilable_outbox_commands(**_values):
            return ()

        @staticmethod
        def read_runtime_last_event_sequence(_runtime_id):
            return 0

        @staticmethod
        def read_event_transaction(_event_id):
            raise KeyError(_event_id)

    product_repository = ProductRepository()
    monkeypatch.setattr(product_module, "PostgresMiniQMTKernelRepository", lambda **_values: product_repository)
    monkeypatch.setattr(
        product_module,
        "build_hot_full_five_catalog_authority_v1",
        lambda **_values: SimpleNamespace(
            catalog_runtime=SimpleNamespace(snapshot=object()),
            conformance_authority=object(),
        ),
    )
    monkeypatch.setattr(product_module, "_session_authority", lambda _runtime_id, _context: candidate)
    monkeypatch.setattr(
        product_module,
        "build_k6d_route_source_capability_v1",
        lambda: SimpleNamespace(capability_sha256="e" * 64),
    )
    for owner in (
        product_module.KernelProductDeliveryWorkerV3,
        product_module.KernelOutboxOutcomeIngressV1,
        product_module.MiniQMTKernelV2ProductCoordinator,
        product_module.KernelIngressCoordinatorV1,
        product_module.KernelProductCallbackIngressV1,
        product_module.ExchangeSessionClockV1,
    ):
        monkeypatch.setattr(owner, "__init__", lambda _self, **_values: None)
    activation = SimpleNamespace(
        get_kernel_product_runtime=lambda _runtime_id: None,
        register_kernel_product_runtime=lambda *, runtime, symbols: registered.append((runtime, symbols)),
    )
    managed = SimpleNamespace(_repository=object(), _broker=None, preview_order=lambda _request: None)
    runtime = build_simulation_miniqmt_product_runtime_v1(
        simulation_repository=simulation_repository,
        execution_plan=plan,
        binding=binding,
        managed_order_service=managed,
        quote_context_adapter=SimpleNamespace(context_store=SimpleNamespace(snapshot=_context)),
        quote_ingress_activation=activation,
        observed_at=datetime(2026, 7, 27, 1, 20, tzinfo=UTC),
        broker_side_effects_enabled=False,
    )
    assert runtime.runtime_id == runtime_id
    assert runtime.outbox_dispatcher is None
    assert registered == [(runtime, ("600000.SH",))]
    assert len(worker_start_calls) == 1
    assert worker_start_calls[0]["worker_id"] == "miniqmt_kernel_v2_product"
    assert worker_start_calls[0]["process_role"] == "PRODUCT_COORDINATOR"


def test_product_composition_preflights_registry_and_reuses_only_exact_source_runtime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan, binding, _session, simulation_repository, _accounts, runtime_id = _plan_reader_facts()
    context = _context()
    managed = SimpleNamespace(_repository=object(), _broker=None, preview_order=lambda _request: None)
    with pytest.raises(MiniQMTKernelProductCompositionError) as registry_failure:
        build_simulation_miniqmt_product_runtime_v1(
            simulation_repository=simulation_repository,
            execution_plan=plan,
            binding=binding,
            managed_order_service=managed,
            quote_context_adapter=SimpleNamespace(context_store=SimpleNamespace(snapshot=lambda: context)),
            quote_ingress_activation=SimpleNamespace(),
            observed_at=datetime(2026, 7, 27, 1, 20, tzinfo=UTC),
            broker_side_effects_enabled=False,
        )
    assert registry_failure.value.reason_code == "MINIQMT_K6_PRODUCT_QUOTE_PUBLISHER_UNAVAILABLE"

    monkeypatch.setattr(
        product_module,
        "build_k6d_route_source_capability_v1",
        lambda: SimpleNamespace(capability_sha256="e" * 64),
    )
    existing = _runtime(_RuntimeRepository(), None)
    object.__setattr__(existing, "runtime_id", runtime_id)
    object.__setattr__(existing, "binding_id", binding.binding_id)
    object.__setattr__(existing, "trade_date", plan.target_trade_date)
    object.__setattr__(existing, "symbols", ("600000.SH",))
    object.__setattr__(existing, "source_capability_sha256", "e" * 64)
    activation = SimpleNamespace(
        get_kernel_product_runtime=lambda _runtime_id: existing,
        register_kernel_product_runtime=lambda **_values: (_ for _ in ()).throw(
            AssertionError("existing exact runtime must not be registered twice")
        ),
    )
    assert (
        build_simulation_miniqmt_product_runtime_v1(
            simulation_repository=simulation_repository,
            execution_plan=plan,
            binding=binding,
            managed_order_service=managed,
            quote_context_adapter=SimpleNamespace(context_store=SimpleNamespace(snapshot=lambda: context)),
            quote_ingress_activation=activation,
            observed_at=datetime(2026, 7, 27, 1, 20, tzinfo=UTC),
            broker_side_effects_enabled=False,
        )
        is existing
    )

    object.__setattr__(existing, "source_capability_sha256", "f" * 64)
    with pytest.raises(MiniQMTKernelProductCompositionError) as source_failure:
        build_simulation_miniqmt_product_runtime_v1(
            simulation_repository=simulation_repository,
            execution_plan=plan,
            binding=binding,
            managed_order_service=managed,
            quote_context_adapter=SimpleNamespace(context_store=SimpleNamespace(snapshot=lambda: context)),
            quote_ingress_activation=activation,
            observed_at=datetime(2026, 7, 27, 1, 20, tzinfo=UTC),
            broker_side_effects_enabled=False,
        )
    assert source_failure.value.reason_code == "MINIQMT_K6_PRODUCT_RUNTIME_IDEMPOTENCY_CONFLICT"


def test_product_runtime_drains_only_due_durable_outbox_through_dispatcher() -> None:
    repository = _RuntimeRepository()
    dispatcher = _Dispatcher()
    runtime = _runtime(repository, dispatcher)
    observed = datetime(2026, 8, 4, 1, 30, tzinfo=UTC)
    assert runtime.dispatch_due_outbox_v1(observed_at=observed) == ("command_k6d",)
    assert dispatcher.calls == [
        {
            "command_id": "command_k6d",
            "observed_at_utc": observed,
            "lease_expires_at_utc": datetime(2026, 8, 4, 1, 31, tzinfo=UTC),
        }
    ]


def test_preview_only_product_runtime_keeps_outbox_pending_without_fake_ack() -> None:
    repository = _RuntimeRepository()
    runtime = _runtime(repository, None)
    assert runtime.dispatch_due_outbox_v1(observed_at=datetime(2026, 8, 4, 1, 30, tzinfo=UTC)) == ()
    assert repository.reads == 0


def test_scheduler_tick_ingests_callbacks_before_clock_events_without_quote() -> None:
    order: list[str] = []

    class Snapshot:
        def sync_v1(self, **_values):
            order.append("callbacks")
            return ("event_callback",)

    class Clock:
        def wake(self, **_values):
            order.append("clock")
            return SimpleNamespace(ordered_session_event_ids=(), ordered_timer_event_ids=(), eod_event_id=None)

    repository = _RuntimeRepository()
    repository.reads = 1
    runtime = _runtime(repository, _Dispatcher())
    object.__setattr__(runtime, "snapshot_ingress", Snapshot())
    object.__setattr__(runtime, "clock", Clock())
    callback_ids = runtime.scheduler_tick_v1(
        observed_at=datetime(2026, 8, 4, 7, 1, tzinfo=UTC),
        monotonic_ns=10,
    )
    assert callback_ids == ("event_callback",)
    assert order == ["callbacks", "clock"]


def test_scheduler_tick_refreshes_hot_targets_after_durable_callback_and_removes_terminal() -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    active = _hot_persistence("SNIPER_MINIQMT", context, state)
    successor = active.model_copy(update={"row_version": active.row_version + 1})
    rows = {active.algo_instance_id: successor}

    class Repository(_RuntimeRepository):
        @staticmethod
        def read_algo_instance(algo_instance_id):
            return rows[algo_instance_id]

    class Snapshot:
        @staticmethod
        def sync_v1(**_values):
            return ()

    class Clock:
        @staticmethod
        def wake(**_values):
            return SimpleNamespace(ordered_session_event_ids=(), ordered_timer_event_ids=(), eod_event_id=None)

    repository = Repository()
    repository.reads = 1
    runtime = _runtime(repository, _Dispatcher())
    object.__setattr__(runtime, "runtime_id", active.runtime_id)
    object.__setattr__(
        runtime,
        "hot_market_data_ingress",
        HotMarketDataIngressV1(runtime_id=active.runtime_id, effect_committer=lambda _effect: None),
    )
    object.__setattr__(runtime, "snapshot_ingress", Snapshot())
    object.__setattr__(runtime, "clock", Clock())
    runtime.activate_hot_market_targets_v1((active.algo_instance_id,))
    runtime.scheduler_tick_v1(observed_at=datetime(2026, 8, 4, 7, 1, tzinfo=UTC), monotonic_ns=10)
    refreshed = runtime.hot_market_data_ingress._targets_by_symbol[active.symbol][0]
    assert refreshed.algo.row_version == successor.row_version

    rows[active.algo_instance_id] = successor.model_copy(
        update={
            "status": ExecutionAlgoPersistenceStatusV2.COMPLETED,
            "terminal_delivery_sequence": successor.last_closed_delivery_sequence,
            "terminal_at_utc": successor.updated_at_utc,
            "active_child_closure_status": "CLEAN",
        }
    )
    runtime.scheduler_tick_v1(observed_at=datetime(2026, 8, 4, 7, 1, 1, tzinfo=UTC), monotonic_ns=11)
    assert runtime.hot_market_data_ingress.target_algo_instance_ids_v1() == ()


def test_product_scheduler_refresh_preserves_isolation_until_durable_successor() -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    predecessor = _hot_persistence("SNIPER_MINIQMT", context, state)
    rows = {predecessor.algo_instance_id: predecessor}

    class Repository(_RuntimeRepository):
        @staticmethod
        def read_algo_instance(algo_instance_id):
            return rows[algo_instance_id]

    runtime = _runtime(Repository(), _Dispatcher())
    object.__setattr__(runtime, "runtime_id", predecessor.runtime_id)
    object.__setattr__(
        runtime,
        "hot_market_data_ingress",
        HotMarketDataIngressV1(runtime_id=predecessor.runtime_id, effect_committer=lambda _effect: None),
    )
    runtime.activate_hot_market_targets_v1((predecessor.algo_instance_id,))
    with runtime.hot_market_data_ingress._lock:
        runtime.hot_market_data_ingress._isolate_target_locked_v1(
            symbol=predecessor.symbol,
            algo_instance_id=predecessor.algo_instance_id,
            expected_algo_row_version=predecessor.row_version,
        )

    runtime.refresh_hot_market_targets_v1()
    assert runtime.hot_market_data_ingress.target_algo_instance_ids_v1() == ()
    assert runtime.hot_market_data_ingress.registered_algo_instance_ids_v1() == (predecessor.algo_instance_id,)

    successor = predecessor.model_copy(update={"row_version": predecessor.row_version + 1})
    rows[predecessor.algo_instance_id] = successor
    runtime.refresh_hot_market_targets_v1()
    restored = runtime.hot_market_data_ingress._targets_by_symbol[predecessor.symbol][0]
    assert restored.algo.row_version == successor.row_version


def test_product_scheduler_refresh_skips_repository_while_effect_is_pending() -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    predecessor = _hot_persistence("SNIPER_MINIQMT", context, state)
    reads: list[str] = []

    class Repository(_RuntimeRepository):
        @staticmethod
        def read_algo_instance(algo_instance_id):
            reads.append(algo_instance_id)
            return predecessor

    runtime = _runtime(Repository(), _Dispatcher())
    object.__setattr__(runtime, "runtime_id", predecessor.runtime_id)
    ingress = HotMarketDataIngressV1(runtime_id=predecessor.runtime_id, effect_committer=lambda _effect: None)
    ingress.replace_targets_v1((runtime._build_hot_market_target_v1(predecessor),))
    effect = HotMarketDataEconomicEffectV1(
        runtime_id=predecessor.runtime_id,
        algo_instance_id=predecessor.algo_instance_id,
        expected_algo_row_version=predecessor.row_version,
        effect_identity="mqhoteffect_refresh_pending",
        economic_payload={
            "action": "CANCEL_ORDER",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "reason_code": "refresh_pending",
        },
    )
    ingress._pending_by_algo[predecessor.algo_instance_id] = _PendingHotMarketEffectV1(
        effect=effect,
        target=runtime._build_hot_market_target_v1(predecessor),
        failure_count=1,
        next_retry_at_utc=datetime(2026, 8, 12, 1, 31, tzinfo=UTC),
    )
    object.__setattr__(runtime, "hot_market_data_ingress", ingress)

    runtime.refresh_hot_market_targets_v1()
    assert reads == []
    assert ingress.target_algo_instance_ids_v1() == (predecessor.algo_instance_id,)


def test_product_runtime_quote_clock_callbacks_and_bounded_ingress_use_durable_seams() -> None:
    context = _context()
    observation = _observation(context)
    authority = product_module._session_authority("runtime_k6d", context)
    ingested: list[RuntimeEventEnvelopeV2] = []

    class Repository(_RuntimeRepository):
        @staticmethod
        def read_exchange_session_authority(**_values):
            return authority

        @staticmethod
        def read_event_transaction(_event_id):
            raise KeyError(_event_id)

        @staticmethod
        def read_runtime_last_event_sequence(_runtime_id):
            return len(ingested)

        @staticmethod
        def read_runtime_event(event_id):
            return next(item for item in ingested if item.event_id == event_id)

    class Coordinator(_Coordinator):
        @staticmethod
        def ingest_native_event_v1(*, event):
            ingested.append(event)

    class Clock:
        @staticmethod
        def wake(**_values):
            return SimpleNamespace(ordered_session_event_ids=(), ordered_timer_event_ids=(), eod_event_id=None)

    repository = Repository()
    repository.reads = 1
    runtime = _runtime(
        repository,
        _Dispatcher(),
        symbols=(observation.quote.symbol,),
        trade_date=observation.quote.clock_trade_date,
        quote_context_id=context.context_id,
    )
    object.__setattr__(runtime, "coordinator", Coordinator())
    object.__setattr__(runtime, "clock", Clock())
    runtime.observe_b0_quote_v1(observation, context)
    assert ingested == []
    assert repository.reads == 1

    with pytest.raises(TypeError, match="normalized observation"):
        runtime.observe_b0_quote_v1(object(), context)  # type: ignore[arg-type]
    invalid_depth = replace(
        observation,
        quote=replace(observation.quote, bid_prices=None),
    )
    with pytest.raises(MiniQMTKernelProductCompositionError) as depth_failure:
        runtime.observe_b0_quote_v1(invalid_depth, context)
    assert depth_failure.value.reason_code == "MINIQMT_K6_PRODUCT_B0_DEPTH_INVALID"

    unowned = _runtime(
        repository,
        _Dispatcher(),
        trade_date=observation.quote.clock_trade_date,
        quote_context_id=context.context_id,
    )
    object.__setattr__(unowned, "clock", Clock())
    with pytest.raises(MiniQMTKernelProductCompositionError, match="not owned"):
        unowned.observe_b0_quote_v1(observation, context)

    callback_runtime = _runtime(repository, _Dispatcher())
    order_event = callback_runtime.ingest_order_callback_v1(
        broker_order_id="broker_callback_wrapper",
        raw_payload={"status": 48},
        observed_at=datetime(2026, 8, 4, 1, 30, tzinfo=UTC),
    )
    trade_event = callback_runtime.ingest_trade_callback_v1(
        broker_order_id="broker_callback_wrapper",
        trade_quantity=10,
        trade_price_decimal="10.25",
        cumulative_quantity=10,
        raw_payload={"trade_id": "trade_callback_wrapper"},
        observed_at=datetime(2026, 8, 4, 1, 31, tzinfo=UTC),
    )
    assert (order_event, trade_event) == ("order_event", "trade_event")


def test_product_hot_effect_commit_requires_exact_applied_delivery_and_classifies_terminal_failure() -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    algo = _hot_persistence("SNIPER_MINIQMT", context, state)
    effect = HotMarketDataEconomicEffectV1(
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        expected_algo_row_version=algo.row_version,
        effect_identity="mqhoteffect_product_readback",
        economic_payload={
            "action": "SUBMIT_LIMIT",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "symbol": algo.symbol,
            "side": "BUY",
            "price_decimal": "10.01",
            "quantity": 100,
            "reason_code": "product_readback",
        },
    )
    payload = {
        "schema_version": "miniqmt_hot_market_economic_action_v1",
        "runtime_id": effect.runtime_id,
        "algo_instance_id": effect.algo_instance_id,
        "expected_algo_row_version": effect.expected_algo_row_version,
        "effect_identity": effect.effect_identity,
        "economic_effect": thaw_json_v1(effect.economic_payload),
    }
    event = RuntimeEventEnvelopeV2.create(
        runtime_id=effect.runtime_id,
        sequence=7,
        event_type=EventTypeV2.OPERATOR,
        event_time_utc="2026-08-12T01:30:00Z",
        monotonic_ns=None,
        source=EventSourceV2.SIMULATION_RUNTIME_OPERATOR,
        symbol=algo.symbol,
        payload_schema_version="miniqmt_operator_command_v1",
        payload=payload,
        source_identity={"operator_command_id": effect.effect_identity},
        correlation={
            "algo_instance_id": effect.algo_instance_id,
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
        },
    )
    delivery_id = "mqdelivery_" + hash_hex_v1(
        "miniqmt_algo_event_delivery_identity_v1",
        {
            "event_id": event.event_id,
            "algo_instance_id": algo.algo_instance_id,
            "plugin_manifest_sha256": algo.plugin_manifest_sha256,
        },
    )
    receipt = RuntimeEventIngressReceiptV1.create(
        runtime_id=event.runtime_id,
        event_id=event.event_id,
        event_key_sha256=event.event_key_sha256,
        runtime_sequence=event.sequence,
        ordered_target_algo_instance_ids=(algo.algo_instance_id,),
        ordered_delivery_ids=(delivery_id,),
        transaction_commit_identity="tx_hot_effect_readback",
    )

    def _delivery(status: DeliveryStatusV1) -> AlgoDeliveryPersistenceV1:
        carrier = AlgoEventDeliveryV1.create(
            event=event,
            algo_instance_id=algo.algo_instance_id,
            plugin_manifest_sha256=algo.plugin_manifest_sha256,
            algo_delivery_sequence=algo.last_applied_delivery_sequence + 1,
            previous_delivery_id=algo.last_applied_delivery_id,
            status=DeliveryStatusV1.APPLIED,
            attempt_count=1,
            lease_owner=None,
            lease_expires_at=None,
            transition_id="transition_hot_effect",
            last_error_json=None,
            created_at_utc="2026-08-12T01:30:00Z",
            updated_at_utc="2026-08-12T01:30:00Z",
        )
        persisted = AlgoDeliveryPersistenceV1.create(
            delivery=carrier,
            lease_epoch=1,
            lease_fence_token=None,
            row_version=3,
            next_attempt_at_utc=None,
            failure_receipt_id=None,
            skip_receipt_id=None,
            closed_at_utc="2026-08-12T01:30:00Z",
        )
        if status is DeliveryStatusV1.APPLIED:
            return persisted
        error = KernelErrorEvidenceV1.create(
            stage="DELIVERY_APPLY",
            stable_reason_code="PLUGIN_FAILED",
            exception=RuntimeError("terminal"),
            message="terminal",
            retryable=False,
            terminal=True,
            broker_called=False,
            primary_context={"stage": "DELIVERY"},
            secondary_errors=[],
        )
        return AlgoDeliveryPersistenceV1.model_validate(
            {
                **persisted.model_dump(mode="python"),
                "status": DeliveryStatusV1.FAILED_TERMINAL,
                "transition_id": None,
                "last_error_json": error.model_dump(mode="json"),
                "failure_receipt_id": "failure_hot_effect",
            },
            strict=True,
        )

    applied = _delivery(DeliveryStatusV1.APPLIED)
    successor = algo.model_copy(
        update={
            "row_version": algo.row_version + 1,
            "last_applied_delivery_id": delivery_id,
            "last_applied_delivery_sequence": applied.algo_delivery_sequence,
        }
    )

    class Repository(_RuntimeRepository):
        delivery = applied

        def read_event_transaction(self, _event_id):
            return {"event": event, "receipt": receipt, "deliveries": (self.delivery,)}

        @staticmethod
        def read_algo_instance(_algo_instance_id):
            return successor

    repository = Repository()
    runtime = _runtime(repository, None, symbols=(algo.symbol,), trade_date=date(2026, 8, 12))
    object.__setattr__(runtime, "runtime_id", algo.runtime_id)
    object.__setattr__(runtime, "coordinator", SimpleNamespace(ingest_native_event_v1=lambda **_values: receipt))
    assert runtime.commit_hot_market_effect_v1(effect) == successor

    repository.delivery = _delivery(DeliveryStatusV1.FAILED_TERMINAL)
    with pytest.raises(HotMarketDataEffectTerminalError) as terminal:
        runtime.commit_hot_market_effect_v1(effect)
    assert terminal.value.context["delivery_id"] == delivery_id


@pytest.mark.parametrize(
    "failure",
    (
        KernelRepositoryCommitUnknown("commit acknowledgement lost"),
        product_module.psycopg2.OperationalError("server closed the connection"),
        product_module.psycopg2.errors.SerializationFailure("serialization failure"),
        product_module.PoolError("connection pool exhausted"),
    ),
)
def test_bound_hot_effect_committer_wraps_only_allowlisted_database_transients(failure: Exception) -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    algo = _hot_persistence("SNIPER_MINIQMT", context, state)
    effect = HotMarketDataEconomicEffectV1(
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        expected_algo_row_version=algo.row_version,
        effect_identity="mqhoteffect_transient_classification",
        economic_payload={
            "action": "CANCEL_ORDER",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "reason_code": "transient_classification",
        },
    )

    class Runtime:
        @staticmethod
        def commit_hot_market_effect_v1(_effect):
            raise failure

    committer = product_module._BoundHotMarketEffectCommitterV1()
    committer.bind_v1(Runtime())
    with pytest.raises(HotMarketDataEffectRetryableError) as caught:
        committer(effect)
    assert caught.value.reason_code == "MINIQMT_HOT_MARKET_EFFECT_DATABASE_TRANSIENT"
    assert caught.value.__cause__ is failure


@pytest.mark.parametrize("failure", (ValueError("schema drift"), KernelRepositoryConflict("identity conflict")))
def test_bound_hot_effect_committer_preserves_nonretryable_failures(failure: Exception) -> None:
    _plugin, context, state = _hot_initialized("SNIPER_MINIQMT")
    algo = _hot_persistence("SNIPER_MINIQMT", context, state)
    effect = HotMarketDataEconomicEffectV1(
        runtime_id=algo.runtime_id,
        algo_instance_id=algo.algo_instance_id,
        expected_algo_row_version=algo.row_version,
        effect_identity="mqhoteffect_nonretryable_classification",
        economic_payload={
            "action": "CANCEL_ORDER",
            "action_time_utc": "2026-08-12T01:30:00Z",
            "exchange_trade_date": "2026-08-12",
            "session_epoch": "session_hot_tick",
            "session_phase": "CONTINUOUS_AM",
            "reason_code": "nonretryable_classification",
        },
    )

    class Runtime:
        @staticmethod
        def commit_hot_market_effect_v1(_effect):
            raise failure

    committer = product_module._BoundHotMarketEffectCommitterV1()
    committer.bind_v1(Runtime())
    with pytest.raises(type(failure)) as caught:
        committer(effect)
    assert caught.value is failure


def test_real_product_quote_callback_never_reaches_clock_schema_or_retry_backoff() -> None:
    context = _context()
    observation = _observation(context)
    repository = _RuntimeRepository()
    repository.reads = 1
    runtime = _runtime(
        repository,
        _Dispatcher(),
        symbols=(observation.quote.symbol,),
        trade_date=observation.quote.clock_trade_date,
        quote_context_id=context.context_id,
    )

    class _Diag:
        constraint_name = "ck_miniqmt_event_source"
        schema_name = "qmt_strategy"
        table_name = "execution_runtime_event"

    class _CheckViolation(RuntimeError):
        pgcode = "23514"
        diag = _Diag()

    class _Clock:
        def __init__(self) -> None:
            self.attempts = 0

        def wake(self, **_values: object) -> object:
            self.attempts += 1
            raise _CheckViolation("violates check constraint ck_miniqmt_event_source")

    class _Supervisor:
        sink = None

        def register_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...], sink: object) -> None:
            self.sink = sink

        def get_observation_sink(self, *, consumer_id: str, symbols: tuple[str, ...]) -> object | None:
            return self.sink

        @staticmethod
        def acquire_consumer(**_values: object) -> None:
            return None

        @staticmethod
        def consumer_lease_owner_snapshot(
            *,
            consumer_id: str,
            symbols: tuple[str, ...],
        ) -> dict[str, object]:
            lease = {
                "lease_id": f"lease:{consumer_id}",
                "data_session_key": "SIM:B0_QUOTE_V2:simulation_scheduler",
                "owner": "simulation_scheduler",
                "consumer_id": consumer_id,
                "symbols": list(symbols),
                "generation": 1,
                "status": "ACTIVE",
                "physical_subscription_id": 1001,
            }
            return {
                "schema_version": "miniqmt_quote_consumer_lease_owner_snapshot_v1",
                "readback_current": True,
                "exact_owner": True,
                "state": "ACTIVE",
                "registration_generation": 1,
                "expected_owner_identity_sha256": "a" * 64,
                "actual_owner_identity_sha256": "a" * 64,
                "expected_lease": dict(lease),
                "actual_lease": dict(lease),
            }

        @staticmethod
        def health() -> dict[str, object]:
            return {"status": "READY"}

    clock = _Clock()
    object.__setattr__(runtime, "clock", clock)
    activation = build_miniqmt_quote_ingress_activation_from_env(
        environ={"MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": "false"},
        schema_gate_reader=lambda: "applied_and_verified",
    )
    supervisor = _Supervisor()
    activation.controller_factory = None
    activation.supervisor = supervisor  # type: ignore[assignment]
    activation.register_kernel_product_runtime(runtime=runtime, symbols=runtime.symbols)
    assert callable(supervisor.sink)

    supervisor.sink(observation, context)
    supervisor.sink(observation, context)

    assert clock.attempts == 0
    assert activation.health()["kernel_product_runtimes"][0]["ingress_retry"]["suppressed_callback_count"] == 0


def test_product_runtime_outbox_and_reconcile_are_bounded_and_fail_loud() -> None:
    observed = datetime(2026, 8, 4, 1, 30, tzinfo=UTC)

    class InvalidRepository(_RuntimeRepository):
        def list_dispatchable_outbox_commands(self, **_values):
            return (SimpleNamespace(command_id="invalid", status=BrokerCommandOutboxStatusV1.ACKED),)

    with pytest.raises(MiniQMTKernelProductCompositionError, match="non-dispatchable"):
        _runtime(InvalidRepository(), _Dispatcher()).dispatch_due_outbox_v1(observed_at=observed)

    class EndlessDispatchRepository(_RuntimeRepository):
        @staticmethod
        def list_dispatchable_outbox_commands(**_values):
            return (_DueCommand("command_endless"),)

    with pytest.raises(MiniQMTKernelProductCompositionError, match="drain exceeded"):
        _runtime(EndlessDispatchRepository(), _Dispatcher()).dispatch_due_outbox_v1(observed_at=observed)

    class ReconcileRepository(_RuntimeRepository):
        def __init__(self, *, endless: bool = False) -> None:
            super().__init__()
            self.endless = endless
            self.reconcile_reads = 0

        def list_dispatchable_outbox_commands(self, **_values):
            return ()

        def list_reconcilable_outbox_commands(self, **_values):
            self.reconcile_reads += 1
            if self.endless or self.reconcile_reads == 1:
                return (SimpleNamespace(command_id=f"reconcile_{self.reconcile_reads}"),)
            return ()

    reconciled: list[str] = []
    repository = ReconcileRepository()
    runtime = _runtime(repository, _Dispatcher())
    object.__setattr__(
        runtime,
        "outbox_reconciler",
        SimpleNamespace(reconcile_one=lambda *, command_id, **_values: reconciled.append(command_id)),
    )
    assert runtime.reconcile_due_outbox_v1(observed_at=observed) == ("reconcile_1",)
    assert reconciled == ["reconcile_1"]

    endless = ReconcileRepository(endless=True)
    limited = _runtime(endless, _Dispatcher())
    object.__setattr__(limited, "outbox_reconciler", SimpleNamespace(reconcile_one=lambda **_values: None))
    with pytest.raises(MiniQMTKernelProductCompositionError, match="reconciliation exceeded"):
        limited.reconcile_due_outbox_v1(observed_at=observed)


def test_product_native_ingress_replays_commit_unknown_and_rejects_conflict_or_contention() -> None:
    base_event = _coordinator_event("runtime_k6d")

    def builder(sequence: int):
        return RuntimeEventEnvelopeV2.create(
            runtime_id=base_event.runtime_id,
            sequence=sequence,
            event_type=base_event.event_type,
            event_time_utc=base_event.event_time_utc,
            monotonic_ns=base_event.monotonic_ns,
            source=base_event.source,
            symbol=base_event.symbol,
            payload_schema_version=base_event.payload_schema_version,
            payload={"last_price": "10.00"},
            source_identity={"market_data_id": "market_coordinator"},
            correlation={},
        )

    committed = builder(2)

    class ConflictRepository(_RuntimeRepository):
        def __init__(self, *, expose_commit: bool) -> None:
            super().__init__()
            self.expose_commit = expose_commit
            self.readback_count = 0

        def read_event_transaction(self, _event_id):
            self.readback_count += 1
            if self.expose_commit and self.readback_count > 1:
                return {"event": committed}
            raise KeyError(_event_id)

        @staticmethod
        def read_runtime_last_event_sequence(_runtime_id):
            return 1

    class RecoveringCoordinator(_Coordinator):
        def __init__(self) -> None:
            self.calls = 0

        def ingest_native_event_v1(self, **_values):
            self.calls += 1
            if self.calls == 1:
                raise KernelRepositoryCommitUnknown("unknown")

    class ConflictCoordinator(_Coordinator):
        @staticmethod
        def ingest_native_event_v1(**_values):
            raise KernelRepositoryConflict("event sequence is not the exact runtime successor")

    class CommitUnknownCoordinator(_Coordinator):
        @staticmethod
        def ingest_native_event_v1(**_values):
            raise KernelRepositoryCommitUnknown("unknown")

    recovered_repository = ConflictRepository(expose_commit=True)
    recovered = _runtime(recovered_repository, None)
    object.__setattr__(recovered, "coordinator", RecoveringCoordinator())
    recovered._ingest_bounded_v1(builder=builder)

    contended_repository = ConflictRepository(expose_commit=False)
    contended = _runtime(contended_repository, None)
    object.__setattr__(contended, "coordinator", ConflictCoordinator())
    with pytest.raises(MiniQMTKernelProductCompositionError) as contention:
        contended._ingest_bounded_v1(builder=builder)
    assert contention.value.reason_code == "MINIQMT_K6_PRODUCT_SOURCE_EVENT_CONTENTION"

    commit_unknown = _runtime(ConflictRepository(expose_commit=False), None)
    object.__setattr__(commit_unknown, "coordinator", CommitUnknownCoordinator())
    with pytest.raises(KernelRepositoryCommitUnknown, match="unknown"):
        commit_unknown._ingest_bounded_v1(builder=builder)

    class ExistingRepository(_RuntimeRepository):
        @staticmethod
        def read_event_transaction(_event_id):
            return {"event": builder(1).model_copy(update={"payload_schema_version": "different"})}

    with pytest.raises(MiniQMTKernelProductCompositionError, match="different event facts"):
        _runtime(ExistingRepository(), None)._ingest_bounded_v1(builder=builder)

    class MatchingExistingRepository(_RuntimeRepository):
        @staticmethod
        def read_event_transaction(_event_id):
            return {"event": builder(1)}

    matching = _runtime(MatchingExistingRepository(), None)
    matching_calls: list[RuntimeEventEnvelopeV2] = []
    matching_coordinator = SimpleNamespace(
        start_execution_plan_v1=lambda **_values: None,
        ingest_native_event_v1=lambda *, event: matching_calls.append(event),
    )
    object.__setattr__(matching, "coordinator", matching_coordinator)
    matching._ingest_bounded_v1(builder=builder)
    assert matching_calls == [builder(1)]

    class AuthorityConflictCoordinator(_Coordinator):
        @staticmethod
        def ingest_native_event_v1(**_values):
            raise KernelRepositoryConflict("routing authority conflict")

    authority_conflict = _runtime(ConflictRepository(expose_commit=False), None)
    object.__setattr__(authority_conflict, "coordinator", AuthorityConflictCoordinator())
    with pytest.raises(KernelRepositoryConflict, match="routing authority conflict"):
        authority_conflict._ingest_bounded_v1(builder=builder)


def _callback_command() -> BrokerCommandV2:
    return BrokerCommandV2.create(
        command_type=BrokerCommandTypeV2.SUBMIT_LIMIT,
        runtime_id="runtime_k6d_callback",
        algo_instance_id="algo_k6d_callback",
        parent_intent_id="intent_k6d_callback",
        transition_id="transition_k6d_callback",
        ordinal=0,
        local_vt_orderid=None,
        symbol="600000.SH",
        side=SideV1.BUY,
        order_type=OrderTypeV1.LIMIT,
        price_decimal="10.25",
        quantity=100,
        owned_broker_order_id=None,
        reason_code="K6D_CALLBACK_TEST",
        metadata={"source": "test"},
    )


def _callback_mapping() -> ExecutionCommandChildMappingV1:
    command = _callback_command()
    return ExecutionCommandChildMappingV1.create(
        command=command,
        strategy_slot_id="slot_k6d_callback",
        mapping_status=CommandChildMappingStatusV1.DISPATCHING,
        mapping_version=2,
        broker_order_id=None,
        broker_identity_source_event_id=None,
        last_order_event_id=None,
        last_trade_event_id=None,
        updated_by_event_id=None,
        created_at_utc="2026-08-04T01:30:00Z",
        updated_at_utc="2026-08-04T01:30:01Z",
    )


class _CallbackRepository:
    def __init__(self) -> None:
        self.mapping = _callback_mapping()
        command = _callback_command()
        self.outbox = SimpleNamespace(
            payload_json=freeze_json_v1(command.model_dump(mode="json")),
            command_id=command.command_id,
        )
        self.events: dict[str, dict[str, object]] = {}

    def read_callback_identity_chain(self, *, runtime_id: str, broker_order_id: str):
        assert runtime_id == self.mapping.runtime_id
        if broker_order_id != "broker_k6d_callback":
            raise KeyError((runtime_id, broker_order_id))
        return {
            "mapping": self.mapping,
            "submit_outbox": self.outbox,
            "reference_outbox": self.outbox,
            "algo": SimpleNamespace(row_version=7),
        }

    def read_runtime_last_event_sequence(self, runtime_id: str) -> int:
        assert runtime_id == self.mapping.runtime_id
        return len(self.events)

    def read_event_transaction(self, event_id: str):
        try:
            return self.events[event_id]
        except KeyError as exc:
            raise KeyError(event_id) from exc


class _CallbackCoordinator(MiniQMTKernelV2ProductCoordinator):
    def __init__(self, repository: _CallbackRepository) -> None:
        self.repository = repository
        self.ingested_updates = []
        self.replayed_events = []

    def ingest_callback_event_v1(self, *, event, callback_mapping_update):
        self.ingested_updates.append(callback_mapping_update)
        self.repository.mapping = callback_mapping_update.mapping
        receipt = RuntimeEventIngressReceiptV1.create(
            runtime_id=event.runtime_id,
            event_id=event.event_id,
            event_key_sha256=event.event_key_sha256,
            runtime_sequence=event.sequence,
            ordered_target_algo_instance_ids=(),
            ordered_delivery_ids=(),
            transaction_commit_identity="commit_k6d_callback",
        )
        self.repository.events[event.event_id] = {"event": event, "receipt": receipt}
        return receipt

    def process_committed_event_v1(self, *, event, receipt) -> None:
        self.replayed_events.append((event, receipt))


def test_first_order_callback_closes_synchronous_ack_broker_identity_and_replays_idempotently() -> None:
    repository = _CallbackRepository()
    coordinator = _CallbackCoordinator(repository)
    ingress = KernelProductCallbackIngressV1(repository=repository, coordinator=coordinator)
    raw = {"order_status": 48, "traded_volume": 0}
    first = ingress.ingest_order_v1(
        runtime_id=repository.mapping.runtime_id,
        broker_order_id="broker_k6d_callback",
        raw_payload=raw,
        observed_at_utc=datetime(2026, 8, 4, 1, 30, 2, tzinfo=UTC),
    )
    successor = coordinator.ingested_updates[0].mapping
    assert successor.mapping_status is CommandChildMappingStatusV1.BROKER_ACCEPTED
    assert successor.broker_order_id == "broker_k6d_callback"
    assert successor.broker_identity_source_event_id == first.event_id
    assert successor.last_order_event_id == first.event_id

    replay = ingress.ingest_order_v1(
        runtime_id=repository.mapping.runtime_id,
        broker_order_id="broker_k6d_callback",
        raw_payload=raw,
        observed_at_utc=datetime(2026, 8, 4, 1, 35, tzinfo=UTC),
    )
    assert replay == first
    assert len(coordinator.ingested_updates) == 1
    assert coordinator.replayed_events == [(first, repository.events[first.event_id]["receipt"])]


class _SnapshotGateway:
    def reconciliation_snapshot(self, *, runtime_id: str):
        assert runtime_id == "runtime_k6d_callback"
        return SimpleNamespace(
            trades=(
                {
                    "order_id": "broker_unowned",
                    "traded_id": "trade_unowned",
                    "traded_time": "093001",
                    "traded_volume": 50,
                    "traded_price": 10.0,
                },
                {
                    "order_id": "broker_k6d_callback",
                    "traded_id": "trade_k6d_callback",
                    "traded_time": "093002",
                    "traded_volume": 50,
                    "traded_price": 10.25,
                },
            ),
            orders=(
                {
                    "order_id": "broker_k6d_callback",
                    "order_status": 56,
                    "traded_volume": 100,
                },
            ),
        )


def test_real_gateway_snapshot_ingress_routes_owned_trades_before_terminal_order() -> None:
    repository = _CallbackRepository()
    coordinator = _CallbackCoordinator(repository)
    callback_ingress = KernelProductCallbackIngressV1(repository=repository, coordinator=coordinator)
    pump = KernelProductSnapshotIngressV1(
        repository=repository,
        gateway=_SnapshotGateway(),
        ingress=callback_ingress,
    )
    event_ids = pump.sync_v1(
        runtime_id="runtime_k6d_callback",
        observed_at_utc=datetime(2026, 8, 4, 1, 30, 3, tzinfo=UTC),
    )
    assert len(event_ids) == 2
    assert [item.mapping.mapping_status for item in coordinator.ingested_updates] == [
        CommandChildMappingStatusV1.BROKER_ACCEPTED,
        CommandChildMappingStatusV1.TERMINAL,
    ]
    assert repository.mapping.last_trade_event_id == event_ids[0]
    assert repository.mapping.last_order_event_id == event_ids[1]
    assert repository.mapping.broker_order_id == "broker_k6d_callback"

    terminal_before_late_trade = repository.mapping
    late_trade = callback_ingress.ingest_trade_v1(
        runtime_id="runtime_k6d_callback",
        broker_order_id="broker_k6d_callback",
        trade_quantity=50,
        trade_price_decimal="10.30",
        cumulative_quantity=100,
        raw_payload={
            "order_id": "broker_k6d_callback",
            "traded_id": "trade_k6d_late",
            "traded_volume": 50,
            "traded_price": "10.30",
        },
        observed_at_utc=datetime(2026, 8, 4, 1, 31, tzinfo=UTC),
    )
    assert repository.mapping.mapping_status is CommandChildMappingStatusV1.TERMINAL
    assert repository.mapping.last_trade_event_id == late_trade.event_id
    assert repository.mapping.validate_successor_v1(terminal_before_late_trade) == repository.mapping
    KernelRepositoryEventDeliveryMixin._validate_callback_mapping_update(
        event=late_trade,
        update=coordinator.ingested_updates[-1],
    )


def test_callback_source_payload_normalization_is_recursive_and_fail_loud() -> None:
    assert _canonical_source_payload_v1({"price": 10.25, "nested": {"volume": 100}, "levels": [10.0, None, True]}) == {
        "price": "10.25",
        "nested": {"volume": 100},
        "levels": ["10.0", None, True],
    }
    for payload, error in (
        ({"price": float("inf")}, KernelProductCallbackIngressError),
        ({"value": (1, 2)}, KernelProductCallbackIngressError),
        ({1: "bad"}, TypeError),
        ({"nested": {1: "bad"}}, TypeError),
    ):
        with pytest.raises(error):
            _canonical_source_payload_v1(payload)  # type: ignore[arg-type]


def test_snapshot_alias_and_numeric_helpers_reject_ambiguous_source_facts() -> None:
    assert KernelProductSnapshotIngressV1._identity_alias({}, ("order_id",), field_name="order") is None
    assert (
        KernelProductSnapshotIngressV1._identity_alias(
            {"order_id": "broker", "qmt_order_id": "broker"},
            ("order_id", "qmt_order_id"),
            field_name="order",
        )
        == "broker"
    )
    for row in ({"order_id": "a", "qmt_order_id": "b"}, {"order_id": " "}, {"order_id": 1}):
        with pytest.raises(KernelProductCallbackIngressError):
            KernelProductSnapshotIngressV1._identity_alias(
                row,
                ("order_id", "qmt_order_id"),
                field_name="order",
            )
    assert KernelProductSnapshotIngressV1._positive_int({"quantity": 1}, ("quantity",), "quantity") == 1
    assert KernelProductSnapshotIngressV1._positive_number({"price": "10.25"}, ("price",), "price") == "10.25"
    for row in ({}, {"quantity": 0}, {"quantity": True}, {"quantity": 1, "volume": 2}):
        with pytest.raises(KernelProductCallbackIngressError):
            KernelProductSnapshotIngressV1._positive_int(row, ("quantity", "volume"), "quantity")
    for row in ({"price": True}, {"price": {}}, {"price": 1, "avg_price": "1"}):
        with pytest.raises(KernelProductCallbackIngressError):
            KernelProductSnapshotIngressV1._positive_number(row, ("price", "avg_price"), "price")


def test_callback_ingress_and_snapshot_validate_boundaries_before_side_effects() -> None:
    repository = _CallbackRepository()
    coordinator = _CallbackCoordinator(repository)
    ingress = KernelProductCallbackIngressV1(repository=repository, coordinator=coordinator)
    with pytest.raises(TypeError, match="positive strict integer"):
        ingress.ingest_trade_v1(
            runtime_id=repository.mapping.runtime_id,
            broker_order_id="broker_k6d_callback",
            trade_quantity=1,
            trade_price_decimal="10.25",
            cumulative_quantity=0,
            raw_payload={},
            observed_at_utc=datetime(2026, 8, 4, 1, 30, tzinfo=UTC),
        )
    with pytest.raises(KernelProductCallbackIngressError, match="cumulative quantity"):
        ingress.ingest_trade_v1(
            runtime_id=repository.mapping.runtime_id,
            broker_order_id="broker_k6d_callback",
            trade_quantity=101,
            trade_price_decimal="10.25",
            cumulative_quantity=100,
            raw_payload={},
            observed_at_utc=datetime(2026, 8, 4, 1, 30, tzinfo=UTC),
        )

    class InvalidGateway:
        @staticmethod
        def reconciliation_snapshot(**_values):
            return SimpleNamespace(orders=[], trades=())

    snapshot = KernelProductSnapshotIngressV1(
        repository=repository,
        gateway=InvalidGateway(),
        ingress=ingress,
    )
    with pytest.raises(TypeError, match="runtime_id"):
        snapshot.sync_v1(runtime_id=" ", observed_at_utc=datetime(2026, 8, 4, 1, 30, tzinfo=UTC))
    with pytest.raises(TypeError, match="timezone-aware"):
        snapshot.sync_v1(runtime_id=repository.mapping.runtime_id, observed_at_utc=datetime(2026, 8, 4, 1, 30))
    with pytest.raises(KernelProductCallbackIngressError, match="strict order/trade tuples"):
        snapshot.sync_v1(
            runtime_id=repository.mapping.runtime_id,
            observed_at_utc=datetime(2026, 8, 4, 1, 30, tzinfo=UTC),
        )
    with pytest.raises(KernelProductCallbackIngressError, match="strict string-keyed object"):
        snapshot._owned_rows(runtime_id=repository.mapping.runtime_id, rows=([],), kind="ORDER")
    with pytest.raises(KernelProductCallbackIngressError, match="lacks a broker order identity"):
        snapshot._owned_rows(runtime_id=repository.mapping.runtime_id, rows=({},), kind="ORDER")
