"""Production composition for the sole MiniQMT KERNEL_V2 simulation route."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
from typing import Any

from backend.execution_algos.adaptive_is.contracts import MarketCode, canonical_json_bytes
from backend.execution_algos.vnpy_style.hot_best_limit_plugin import BestLimitHotTargetV4
from backend.execution_algos.vnpy_style.hot_sniper_plugin import SniperHotTargetV4
from backend.execution_algos.vnpy_style.hot_twap_lite_plugin import TwapLiteHotTargetV4
from backend.execution_algos.vnpy_compat.hot_facade_adapter import IcebergHotTargetV4, StopHotTargetV4
from backend.services.miniqmt_execution_runtime.full_five_catalog_authority import (
    FULL_FIVE_ALGO_CODES_V1,
    build_hot_full_five_catalog_authority_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_clock import (
    ExchangeSessionClockV1,
    project_exchange_session_v1,
    session_epoch_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_creation import KernelAlgoCreationCoordinatorV2
from backend.services.miniqmt_execution_runtime.kernel_delivery import (
    KernelAlgoCreationRequestV1,
    KernelProductDeliveryWorkerV3,
)
from backend.services.miniqmt_execution_runtime.kernel_ingress import KernelIngressCoordinatorV1
from backend.execution_algos.hot_market_contracts import (
    HotMarketDataEconomicEffectV1,
    HotMarketDataViewV1,
    validate_hot_market_economic_payload_v1,
)
from backend.services.miniqmt_execution_runtime.hot_market_data import (
    HotMarketDataIngressV1,
)
from backend.services.miniqmt_execution_runtime.kernel_outbox import (
    KernelOutboxDispatcherV1,
    KernelOutboxOutcomeIngressV1,
    KernelOutboxReconcilerV1,
    MiniQMTKernelGatewayAdapterV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_cutover import KernelProductCutoverCoordinator
from backend.services.miniqmt_execution_runtime.kernel_product_callbacks import (
    KernelProductCallbackIngressV1,
    KernelProductSnapshotIngressV1,
)
from backend.services.miniqmt_execution_runtime.kernel_product_evidence import KernelProductEvidenceProviderV3
from backend.services.miniqmt_execution_runtime.kernel_product_runtime import (
    K6DCommittedSourceEventReaderV1,
    K6DCommittedSourceEventReadbackV1,
    K6DProductPlanAuthorityV1,
    MiniQMTKernelV2ProductCoordinator,
)
from backend.services.miniqmt_execution_runtime.kernel_product_source_capability import (
    build_k6d_route_source_capability_v1,
)
from backend.services.miniqmt_execution_runtime.kernel_repository import (
    KernelRepositoryCommitUnknown,
    KernelRepositoryConflict,
    PostgresMiniQMTKernelRepository,
)
from backend.services.miniqmt_execution_runtime.plugin_canonical import (
    canonical_decimal_string_v1,
    hash_hex_v1,
    require_sha256_v1,
    thaw_json_v1,
)
from backend.services.miniqmt_execution_runtime.plugin_contracts import (
    BrokerCommandOutboxStatusV1,
    EventSourceV2,
    EventTypeV2,
    ExchangeSessionAuthorityV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionAlgoPersistenceStatusV2,
    ExecutionProjectionRefV1,
    GatewayCapabilityCatalogV1,
    KernelProjectionTypeV1,
    MarketDataCapabilityV1,
    OrderTypeV1,
    RuntimeEventEnvelopeV2,
    RuntimeEventIngressReceiptV1,
    SessionPhaseV1,
    SideV1,
)
from backend.services.miniqmt_execution_runtime.gateway import QmtClientMiniQMTGateway
from backend.services.miniqmt_execution_runtime.quote_eligibility import (
    NormalizedQuoteObservation,
    QuoteEvaluationContext,
)
from backend.services.qmt_strategy_ledger.order_service import QmtManagedOrderService
from backend.services.strategy_package.execution_policy import compute_execution_policy_sha256

from .models import (
    ExecutionPlan,
    SimulationBrokerBackend,
    SimulationReleaseBinding,
    miniqmt_kernel_runtime_id,
)


class MiniQMTKernelProductCompositionError(RuntimeError):
    """Fail-loud composition failure before any broker side effect."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = {**context, "broker_called": False}
        super().__init__(message)


class _BoundHotMarketEffectCommitterV1:
    """Bind-once bridge from the process-local actor to economic ingress."""

    def __init__(self) -> None:
        self._runtime: SimulationMiniQMTProductRuntimeV1 | None = None

    def bind_v1(self, runtime: "SimulationMiniQMTProductRuntimeV1") -> None:
        if self._runtime is not None and self._runtime is not runtime:
            raise RuntimeError("hot market effect committer is already bound")
        self._runtime = runtime

    def __call__(self, effect: HotMarketDataEconomicEffectV1) -> Any:
        if self._runtime is None:
            raise RuntimeError("hot market effect committer is not bound")
        return self._runtime.commit_hot_market_effect_v1(effect)


class _ProductOutcomePublisherV1:
    """Publish one durable outbox outcome and continue the same K6-D route."""

    def __init__(self, ingress: KernelOutboxOutcomeIngressV1) -> None:
        if not isinstance(ingress, KernelOutboxOutcomeIngressV1):
            raise TypeError("ingress must be KernelOutboxOutcomeIngressV1")
        self._ingress = ingress
        self._coordinator: MiniQMTKernelV2ProductCoordinator | None = None

    def bind_coordinator_v1(self, coordinator: MiniQMTKernelV2ProductCoordinator) -> None:
        if not isinstance(coordinator, MiniQMTKernelV2ProductCoordinator):
            raise TypeError("coordinator must be MiniQMTKernelV2ProductCoordinator")
        if self._coordinator is not None and self._coordinator is not coordinator:
            raise RuntimeError("product outcome publisher is already bound to another coordinator")
        self._coordinator = coordinator

    def ingest_outbox_outcome_v1(self, *, command_id: str) -> Any:
        if self._coordinator is None:
            raise RuntimeError("product outcome publisher has no bound coordinator")
        receipt = self._ingress.ingest_outbox_outcome_v1(command_id=command_id)
        self._coordinator.process_committed_event_v1(
            event=receipt.event,
            receipt=receipt.ingress_receipt,
        )
        return receipt


def build_k6d_gateway_catalog_v1() -> GatewayCapabilityCatalogV1:
    """Code-owned capabilities of the existing K2 gateway adapter."""

    payload = {
        "schema_version": "miniqmt_gateway_capability_catalog_v1",
        "route_id": "route.sim.kernel_v2",
        "quote_source": "B0_QUOTE_V2",
        "gateway_backend": "minqmt_sim",
        "order_types": [OrderTypeV1.LIMIT.value],
        "market_data_capabilities": [item.value for item in sorted(MarketDataCapabilityV1, key=lambda x: x.value)],
        "session_phases": [item.value for item in sorted(SessionPhaseV1, key=lambda x: x.value)],
        "idempotent_submit_by_client_ref": False,
        "exact_order_id_cancel": True,
    }
    return GatewayCapabilityCatalogV1(
        **{
            **payload,
            "order_types": (OrderTypeV1.LIMIT,),
            "market_data_capabilities": tuple(sorted(MarketDataCapabilityV1, key=lambda x: x.value)),
            "session_phases": tuple(sorted(SessionPhaseV1, key=lambda x: x.value)),
        },
        catalog_sha256=hash_hex_v1("miniqmt_gateway_capability_catalog_v1", payload),
    )


def _runtime_id(plan: ExecutionPlan, binding: SimulationReleaseBinding) -> str:
    return miniqmt_kernel_runtime_id(
        plan_id=plan.plan_id,
        binding_id=binding.binding_id,
        trade_date=plan.target_trade_date,
    )


def _policy(plan: ExecutionPlan) -> tuple[str, str, str, dict[str, Any]]:
    container = plan.plan_payload_json.get("execution_policy")
    if not isinstance(container, dict) or set(container) != {"version_id", "sha256", "payload"}:
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
            "frozen execution plan lacks the exact execution-policy envelope",
            context={"plan_id": plan.plan_id},
        )
    payload = container["payload"]
    if not isinstance(payload, dict) or set(payload) != {"policy_version_id", "policy_sha256", "policy_json"}:
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
            "frozen execution plan lacks the exact release policy payload",
            context={"plan_id": plan.plan_id},
        )
    policy_json = payload["policy_json"]
    if not isinstance(policy_json, dict) or not policy_json:
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
            "frozen execution plan policy_json must be a nonempty exact object",
            context={"plan_id": plan.plan_id},
        )
    algo_code_value = policy_json.get("algo_code")
    if type(algo_code_value) is not str or not algo_code_value or algo_code_value != algo_code_value.strip():
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_INVALID",
            "frozen execution policy algo_code must be a canonical identity",
            context={"plan_id": plan.plan_id, "algo_code_type": type(algo_code_value).__name__},
        )
    algo_code = algo_code_value.upper()
    if algo_code not in FULL_FIVE_ALGO_CODES_V1:
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_ALGO_UNSUPPORTED",
            "execution plan does not select one of the exact five product plugins",
            context={"plan_id": plan.plan_id, "algo_code": algo_code},
        )
    config = policy_json.get("algo_config")
    if not isinstance(config, dict):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_PLUGIN_CONFIG_INVALID",
            "execution policy algo_config must be an exact object",
            context={"plan_id": plan.plan_id, "algo_code": algo_code},
        )
    policy_id = payload["policy_version_id"]
    policy_sha256 = payload["policy_sha256"]
    if (
        type(policy_id) is not str
        or not policy_id
        or policy_id != policy_id.strip()
        or policy_id != container["version_id"]
        or policy_id != plan.execution_policy_version_id
        or type(policy_sha256) is not str
        or policy_sha256 != container["sha256"]
        or policy_sha256 != plan.execution_policy_sha256
        or policy_sha256 != compute_execution_policy_sha256(policy_json)
    ):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_POLICY_AUTHORITY_DRIFT",
            "execution policy identity/hash differs from the frozen plan",
            context={"plan_id": plan.plan_id, "policy_id": policy_id, "policy_sha256": policy_sha256},
        )
    return algo_code, policy_id, policy_sha256, dict(config)


class SimulationK6DPlanAuthorityReader:
    def __init__(
        self,
        *,
        simulation_repository: Any,
        account_repository: Any,
        gateway_catalog: GatewayCapabilityCatalogV1,
        session_authority: ExchangeSessionAuthorityV1,
        logical_time_utc: datetime,
    ) -> None:
        self._repository = simulation_repository
        self._accounts = account_repository
        self._gateway = gateway_catalog
        self._session = session_authority
        self._logical_time = logical_time_utc.astimezone(UTC)

    def read_plan_authority_v1(
        self, *, runtime_id: str, binding_id: str, execution_plan_id: str
    ) -> K6DProductPlanAuthorityV1:
        plan = self._repository.get_execution_plan(execution_plan_id)
        binding = self._repository.get_simulation_release_binding(binding_id)
        release = self._repository.get_strategy_runtime_release(binding.release_id)
        if (
            plan.binding_id != binding.binding_id
            or plan.binding_hash != binding.binding_hash
            or plan.release_id != release.release_id
            or plan.release_hash != release.release_hash
            or binding.release_hash != release.release_hash
            or binding.broker_backend is not SimulationBrokerBackend.MINIQMT_SIM
            or self._session.runtime_id != runtime_id
            or plan.target_trade_date.isoformat() != self._session.exchange_trade_date
            or runtime_id != _runtime_id(plan, binding)
        ):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_PLAN_BINDING_CLOSURE_INVALID",
                "plan, binding, release, runtime and exchange session do not form one owner",
                context={"runtime_id": runtime_id, "binding_id": binding_id, "execution_plan_id": execution_plan_id},
            )
        algo_code, policy_id, policy_sha256, plugin_config = _policy(plan)
        account = self._accounts.get_virtual_account(binding.strategy_id)
        account_dump = getattr(account, "model_dump", None)
        if not callable(account_dump) or not isinstance((account_payload := account_dump(mode="json")), dict):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID",
                "virtual account authority does not expose an exact JSON object",
                context={"runtime_id": runtime_id, "strategy_id": binding.strategy_id},
            )
        capability_payload = self._gateway.model_dump(mode="json")
        if not plan.intents:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_PLAN_EMPTY",
                "product execution plan must contain at least one frozen parent",
                context={"runtime_id": runtime_id, "execution_plan_id": execution_plan_id},
            )
        intent_ids = tuple(item.intent_id for item in plan.intents)
        if len(intent_ids) != len(set(intent_ids)):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_PLAN_PARENT_DUPLICATE",
                "product execution plan contains duplicate parent identity",
                context={"runtime_id": runtime_id, "execution_plan_id": execution_plan_id},
            )
        decision_ids = tuple(item.decision_id for item in plan.trading_rule_decisions)
        decisions = {item.decision_id: item for item in plan.trading_rule_decisions}
        referenced_decisions = {item.trading_rule_decision_id for item in plan.intents}
        if len(decision_ids) != len(set(decision_ids)) or referenced_decisions != set(decisions):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID",
                "product plan parent and trading-rule decision sets do not close exactly",
                context={
                    "runtime_id": runtime_id,
                    "execution_plan_id": execution_plan_id,
                    "referenced_decision_ids": sorted(referenced_decisions),
                    "available_decision_ids": sorted(decisions),
                },
            )
        session = project_exchange_session_v1(self._session, self._logical_time)
        requests: list[KernelAlgoCreationRequestV1] = []
        for intent in plan.intents:
            decision = decisions.get(intent.trading_rule_decision_id)
            if decision is None or decision.symbol != intent.symbol or decision.side != intent.side:
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID",
                    "parent intent has no exact frozen trading-rule decision",
                    context={"intent_id": intent.intent_id, "decision_id": intent.trading_rule_decision_id},
                )
            price = intent.price_policy.get("limit_price")
            if price is None:
                price = intent.price_policy.get("reference_price")
            try:
                canonical_price = canonical_decimal_string_v1(Decimal(str(price)))
            except (InvalidOperation, TypeError, ValueError) as exc:
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_PARENT_PRICE_AUTHORITY_INVALID",
                    "parent intent lacks a positive frozen reference/limit price",
                    context={"intent_id": intent.intent_id, "symbol": intent.symbol, "value": price},
                ) from exc
            if Decimal(canonical_price) <= 0:
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_PARENT_PRICE_AUTHORITY_INVALID",
                    "parent intent frozen price must be positive",
                    context={"intent_id": intent.intent_id, "symbol": intent.symbol, "value": canonical_price},
                )
            lot = decision.lot_rule
            if set(lot) != {"min_quantity", "increment"}:
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_LOT_AUTHORITY_INVALID",
                    "trading-rule lot authority must contain exact min/increment fields",
                    context={"intent_id": intent.intent_id, "lot_rule": lot},
                )
            contract = {
                "symbol": intent.symbol,
                "side": intent.side.value,
                "market_board": decision.market_board,
                "min_volume": lot["min_quantity"],
                "volume_increment": lot["increment"],
                "trading_rule_decision_id": decision.decision_id,
                "trading_rule_decision_sha256": decision.decision_hash,
                "price_limit_rule": decision.price_limit_rule,
            }
            hashes = {
                KernelProjectionTypeV1.CONTRACT: hash_hex_v1("miniqmt_contract_projection_v1", contract),
                KernelProjectionTypeV1.ACCOUNT: hash_hex_v1("miniqmt_account_projection_v1", account_payload),
                KernelProjectionTypeV1.MARKET_CAPABILITY: hash_hex_v1(
                    "miniqmt_market_capability_projection_v1", capability_payload
                ),
            }
            refs = tuple(
                sorted(
                    (
                        ExecutionProjectionRefV1.create(
                            projection_type=projection_type,
                            projection_id=f"mq{projection_type.value.lower()}_{payload_hash[:32]}",
                            projection_version={
                                KernelProjectionTypeV1.CONTRACT: "miniqmt_contract_projection_v1",
                                KernelProjectionTypeV1.ACCOUNT: "miniqmt_account_projection_v1",
                                KernelProjectionTypeV1.MARKET_CAPABILITY: self._gateway.schema_version,
                            }[projection_type],
                            payload_sha256=payload_hash,
                            source_event_id=None,
                            logical_at_utc=self._logical_time,
                        )
                        for projection_type, payload_hash in hashes.items()
                    ),
                    key=lambda item: (item.projection_type.value, item.projection_id),
                )
            )
            config_hash = hash_hex_v1("miniqmt_plugin_config_v2", plugin_config)
            requests.append(
                KernelAlgoCreationRequestV1(
                    runtime_id=runtime_id,
                    parent_intent_id=intent.intent_id,
                    strategy_slot_id=str(intent.strategy_slot_id or binding.strategy_slot_id or binding.strategy_id),
                    symbol=intent.symbol,
                    side=SideV1(intent.side.value),
                    limit_price_decimal=canonical_price,
                    parent_quantity=intent.order_quantity,
                    min_volume=int(lot["min_quantity"]),
                    volume_increment=int(lot["increment"]),
                    algo_code=algo_code,
                    plugin_config=plugin_config,
                    plugin_config_sha256=config_hash,
                    contract_projection=contract,
                    contract_projection_sha256=hashes[KernelProjectionTypeV1.CONTRACT],
                    account_projection=account_payload,
                    account_projection_sha256=hashes[KernelProjectionTypeV1.ACCOUNT],
                    market_capability_projection=capability_payload,
                    market_capability_projection_sha256=hashes[KernelProjectionTypeV1.MARKET_CAPABILITY],
                    projection_refs=refs,
                    execution_plan_id=plan.plan_id,
                    execution_plan_sha256=plan.plan_hash,
                    release_id=release.release_id,
                    release_sha256=release.release_hash,
                    policy_id=policy_id,
                    policy_sha256=policy_sha256,
                    logical_time_utc=self._logical_time.isoformat().replace("+00:00", "Z"),
                    exchange_trade_date=plan.target_trade_date.isoformat(),
                    session_epoch=session_epoch_v1(self._session),
                    session_phase=session.session_phase,
                )
            )
        return K6DProductPlanAuthorityV1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            execution_plan_id=execution_plan_id,
            execution_plan_sha256=plan.plan_hash,
            trade_date=plan.target_trade_date,
            ordered_creation_requests=tuple(requests),
        )


class _CommittedKernelSourceReader(K6DCommittedSourceEventReaderV1):
    def __init__(self, repository: PostgresMiniQMTKernelRepository) -> None:
        self._repository = repository

    def read_committed_source_event_v1(
        self, *, runtime_id: str, binding_id: str, source_event_ref: str
    ) -> K6DCommittedSourceEventReadbackV1:
        transaction = self._repository.read_event_transaction(source_event_ref)
        event = transaction.get("event")
        receipt = transaction.get("receipt")
        if not isinstance(event, RuntimeEventEnvelopeV2) or not isinstance(receipt, RuntimeEventIngressReceiptV1):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_SOURCE_READBACK_INVALID",
                "committed source transaction lacks its strict event/receipt pair",
                context={"runtime_id": runtime_id, "source_event_ref": source_event_ref},
            )
        trade_date = self._repository.read_runtime_trade_date(runtime_id)
        owner = self._repository.read_product_route_owner_v1(
            runtime_id=runtime_id,
            binding_id=binding_id,
            trade_date=trade_date,
        )
        if owner.route_owner.value != "KERNEL_V2":
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_ROUTE_OWNER_INVALID",
                "committed source event does not belong to the KERNEL_V2 product owner",
                context={"runtime_id": runtime_id, "binding_id": binding_id, "source_event_ref": source_event_ref},
            )
        if event.runtime_id != runtime_id:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_SOURCE_OWNER_CONFLICT",
                "committed source event crosses runtime owner",
                context={"runtime_id": runtime_id, "source_event_ref": source_event_ref},
            )
        return K6DCommittedSourceEventReadbackV1(event=event, ingress_receipt=receipt)


@dataclass(frozen=True)
class SimulationMiniQMTProductRuntimeV1:
    coordinator: MiniQMTKernelV2ProductCoordinator
    worker_incarnation_id: str
    runtime_id: str
    binding_id: str
    trade_date: date
    symbols: tuple[str, ...]
    source_capability_sha256: str
    quote_context_id: str
    exchange_session_authority: ExchangeSessionAuthorityV1
    repository: PostgresMiniQMTKernelRepository
    clock: ExchangeSessionClockV1
    outbox_dispatcher: KernelOutboxDispatcherV1 | None
    outbox_reconciler: KernelOutboxReconcilerV1 | None
    callback_ingress: KernelProductCallbackIngressV1
    snapshot_ingress: KernelProductSnapshotIngressV1 | None
    hot_market_data_ingress: HotMarketDataIngressV1

    def __post_init__(self) -> None:
        for name in ("worker_incarnation_id", "runtime_id", "binding_id", "quote_context_id"):
            value = getattr(self, name)
            if type(value) is not str or not value or value != value.strip():
                raise TypeError(f"{name} must be a canonical identity")
        if type(self.trade_date) is not date:
            raise TypeError("trade_date must be an exact date")
        if (
            type(self.symbols) is not tuple
            or not self.symbols
            or len(self.symbols) != len(set(self.symbols))
            or any(type(symbol) is not str or not symbol or symbol != symbol.strip() for symbol in self.symbols)
        ):
            raise TypeError("symbols must be one nonempty ordered unique identity tuple")
        require_sha256_v1(self.source_capability_sha256, field_name="source_capability_sha256")
        if not isinstance(self.exchange_session_authority, ExchangeSessionAuthorityV1):
            raise TypeError("exchange_session_authority must be ExchangeSessionAuthorityV1")
        if (
            self.exchange_session_authority.runtime_id != self.runtime_id
            or self.exchange_session_authority.exchange_trade_date != self.trade_date.isoformat()
        ):
            raise ValueError("exchange_session_authority crosses the frozen runtime owner")
        if not callable(getattr(self.coordinator, "start_execution_plan_v1", None)):
            raise TypeError("coordinator must expose start_execution_plan_v1")
        if not callable(getattr(self.repository, "list_dispatchable_outbox_commands", None)):
            raise TypeError("repository must expose durable outbox inventory")
        if not callable(getattr(self.clock, "wake", None)):
            raise TypeError("clock must expose wake")
        broker_components = (
            self.outbox_dispatcher is not None,
            self.outbox_reconciler is not None,
            self.snapshot_ingress is not None,
        )
        if len(set(broker_components)) != 1:
            raise TypeError("broker-enabled product runtime requires dispatcher, reconciler and snapshot ingress")
        if self.snapshot_ingress is not None and not callable(getattr(self.snapshot_ingress, "sync_v1", None)):
            raise TypeError("snapshot_ingress must expose sync_v1")
        if not callable(getattr(self.callback_ingress, "ingest_order_v1", None)) or not callable(
            getattr(self.callback_ingress, "ingest_trade_v1", None)
        ):
            raise TypeError("callback_ingress must expose ORDER and TRADE ingress")
        if not isinstance(self.hot_market_data_ingress, HotMarketDataIngressV1):
            raise TypeError("hot_market_data_ingress must be HotMarketDataIngressV1")
        if self.hot_market_data_ingress.runtime_id != self.runtime_id:
            raise ValueError("hot_market_data_ingress crosses runtime owner")

    def observe_b0_quote_v1(
        self,
        observation: NormalizedQuoteObservation,
        context: QuoteEvaluationContext,
    ) -> None:
        if not isinstance(observation, NormalizedQuoteObservation) or not isinstance(context, QuoteEvaluationContext):
            raise TypeError("B0 product publisher requires normalized observation and paired context")
        if observation.context_id != context.context_id:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_B0_CONTEXT_CONFLICT",
                "normalized B0 observation differs from its context authority",
                context={"runtime_id": self.runtime_id, "market_data_id": observation.market_data_id},
            )
        if context.context_id != self.quote_context_id:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_B0_CONTEXT_OWNER_CONFLICT",
                "normalized B0 observation does not belong to the frozen product quote context",
                context={
                    "runtime_id": self.runtime_id,
                    "expected_context_id": self.quote_context_id,
                    "actual_context_id": context.context_id,
                },
            )
        if observation.quote.symbol not in self.symbols:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_B0_SYMBOL_OWNER_CONFLICT",
                "normalized B0 observation is not owned by this frozen product runtime",
                context={
                    "runtime_id": self.runtime_id,
                    "symbol": observation.quote.symbol,
                    "owned_symbols": list(self.symbols),
                },
            )
        if observation.quote.clock_trade_date != self.trade_date:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_B0_TRADE_DATE_CONFLICT",
                "normalized B0 observation crosses the frozen product trade date",
                context={
                    "runtime_id": self.runtime_id,
                    "expected_trade_date": self.trade_date.isoformat(),
                    "actual_trade_date": observation.quote.clock_trade_date.isoformat(),
                },
            )
        quote = observation.quote
        if (
            quote.bid_prices is None
            or quote.ask_prices is None
            or quote.bid_quantities is None
            or quote.ask_quantities is None
            or quote.bid_prices[0] is None
            or quote.ask_prices[0] is None
        ):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_B0_DEPTH_INVALID",
                "native B0 observation lacks exact L1 bid/ask depth",
                context={"runtime_id": self.runtime_id, "market_data_id": observation.market_data_id},
            )
        projection = project_exchange_session_v1(
            self.exchange_session_authority,
            quote.received_at_utc,
        )
        self.hot_market_data_ingress.ingest_v1(
            HotMarketDataViewV1(
                runtime_id=self.runtime_id,
                symbol=quote.symbol,
                generation=quote.ingress_generation,
                sequence=quote.ingress_sequence,
                observed_at_utc=quote.received_at_utc,
                exchange_time_utc=quote.source_exchange_time_utc or quote.received_at_utc,
                exchange_trade_date=quote.clock_trade_date.isoformat(),
                session_epoch=projection.session_epoch,
                session_phase=projection.session_phase.value,
                bid_price_1=quote.bid_prices[0],
                ask_price_1=quote.ask_prices[0],
                bid_volume_1=quote.bid_quantities[0],
                ask_volume_1=quote.ask_quantities[0],
                last_price=quote.last_price,
                pre_close=quote.pre_close,
                limit_up=None if observation.tradability is None else observation.tradability.limit_up,
                limit_down=None if observation.tradability is None else observation.tradability.limit_down,
            )
        )

    def wake_clock_v1(self, *, observed_at: datetime, monotonic_ns: int) -> None:
        if type(monotonic_ns) is not int or monotonic_ns <= 0:
            raise TypeError("KERNEL_V2 clock wake requires a positive process-monotonic sample")
        observed = self._observed_utc_v1(observed_at)
        receipt = self.clock.wake(
            runtime_id=self.runtime_id,
            exchange_trade_date=self.trade_date,
            observed_at_utc=observed,
            monotonic_ns=monotonic_ns,
            lease_expires_at_utc=observed + timedelta(seconds=60),
        )
        for event_id in (
            *receipt.ordered_session_event_ids,
            *receipt.ordered_timer_event_ids,
            *((receipt.eod_event_id,) if receipt.eod_event_id is not None else ()),
        ):
            event = self.repository.read_runtime_event(event_id)
            self.coordinator.ingest_native_event_v1(event=event)
        self.dispatch_due_outbox_v1(observed_at=observed)

    def commit_hot_market_effect_v1(self, effect: HotMarketDataEconomicEffectV1) -> Any:
        if not isinstance(effect, HotMarketDataEconomicEffectV1):
            raise TypeError("effect must be HotMarketDataEconomicEffectV1")
        if effect.runtime_id != self.runtime_id:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_HOT_MARKET_EFFECT_RUNTIME_CONFLICT",
                "hot economic effect crosses runtime owner",
                context={"runtime_id": self.runtime_id, "effect_runtime_id": effect.runtime_id},
            )
        economic_payload = validate_hot_market_economic_payload_v1(effect.economic_payload)
        payload = {
            "schema_version": "miniqmt_hot_market_economic_action_v1",
            "runtime_id": self.runtime_id,
            "algo_instance_id": effect.algo_instance_id,
            "expected_algo_row_version": effect.expected_algo_row_version,
            "effect_identity": effect.effect_identity,
            "economic_effect": economic_payload,
        }
        action_time = economic_payload.get("action_time_utc")
        if type(action_time) is not str:
            raise ValueError("hot economic effect requires canonical action_time_utc")

        def build(sequence: int) -> RuntimeEventEnvelopeV2:
            return RuntimeEventEnvelopeV2.create(
                runtime_id=self.runtime_id,
                sequence=sequence,
                event_type=EventTypeV2.OPERATOR,
                event_time_utc=action_time,
                monotonic_ns=None,
                source=EventSourceV2.SIMULATION_RUNTIME_OPERATOR,
                symbol=economic_payload.get("symbol"),
                payload_schema_version="miniqmt_operator_command_v1",
                payload=payload,
                source_identity={"operator_command_id": effect.effect_identity},
                correlation={
                    "algo_instance_id": effect.algo_instance_id,
                    "exchange_trade_date": economic_payload["exchange_trade_date"],
                    "session_epoch": economic_payload["session_epoch"],
                    "session_phase": economic_payload["session_phase"],
                },
            )

        self._ingest_bounded_v1(builder=build)
        return self.repository.read_algo_instance(effect.algo_instance_id)

    def activate_hot_market_targets_v1(self, algo_instance_ids: tuple[str, ...]) -> None:
        if type(algo_instance_ids) is not tuple or len(algo_instance_ids) != len(set(algo_instance_ids)):
            raise TypeError("algo_instance_ids must be an exact unique tuple")
        targets = [
            self._build_hot_market_target_v1(self.repository.read_algo_instance(item)) for item in algo_instance_ids
        ]
        self.hot_market_data_ingress.replace_targets_v1(tuple(targets))

    def _build_hot_market_target_v1(self, algo: ExecutionAlgoInstancePersistenceV2) -> Any:
        if not isinstance(algo, ExecutionAlgoInstancePersistenceV2):
            raise TypeError("hot target readback must be ExecutionAlgoInstancePersistenceV2")
        target_types = {
            "BEST_LIMIT_MINIQMT": BestLimitHotTargetV4,
            "ICEBERG": IcebergHotTargetV4,
            "SNIPER_MINIQMT": SniperHotTargetV4,
            "STOP": StopHotTargetV4,
            "TWAP_LITE_MINIQMT": TwapLiteHotTargetV4,
        }
        if algo.runtime_id != self.runtime_id or algo.plugin_version != "4.0.0":
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_HOT_MARKET_TARGET_VERSION_INVALID",
                "hot target requires the exact V4 runtime owner",
                context={
                    "runtime_id": self.runtime_id,
                    "algo_instance_id": algo.algo_instance_id,
                    "actual_runtime_id": algo.runtime_id,
                    "plugin_version": algo.plugin_version,
                },
            )
        try:
            target_type = target_types[algo.algo_code]
        except KeyError as exc:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_HOT_MARKET_TARGET_ALGO_UNREGISTERED",
                "hot target has no registered five-algorithm adapter",
                context={"algo_instance_id": algo.algo_instance_id, "algo_code": algo.algo_code},
            ) from exc
        return target_type(algo=algo)

    def refresh_hot_market_targets_v1(self) -> None:
        """Refresh process-local targets only at scheduler cadence."""

        targets = []
        for algo_instance_id in self.hot_market_data_ingress.target_algo_instance_ids_v1():
            algo = self.repository.read_algo_instance(algo_instance_id)
            if algo.status is ExecutionAlgoPersistenceStatusV2.ACTIVE:
                targets.append(self._build_hot_market_target_v1(algo))
        self.hot_market_data_ingress.replace_targets_v1(tuple(targets))

    def dispatch_due_outbox_v1(self, *, observed_at: datetime) -> tuple[str, ...]:
        """Drain due commands through the sole K2 dispatcher.

        Preview-only scheduler operation deliberately leaves the durable rows
        PENDING.  Product mode never converts that into a fake broker result.
        """

        if self.outbox_dispatcher is None:
            return ()
        observed = self._observed_utc_v1(observed_at)
        dispatched: list[str] = []
        for _ in range(10):
            due = self.repository.list_dispatchable_outbox_commands(
                runtime_id=self.runtime_id,
                observed_at_utc=observed,
                limit=100,
            )
            if not due:
                self.reconcile_due_outbox_v1(observed_at=observed)
                return tuple(dispatched)
            for outbox in due:
                if outbox.status not in {
                    BrokerCommandOutboxStatusV1.PENDING,
                    BrokerCommandOutboxStatusV1.FAILED_RETRYABLE,
                }:
                    raise MiniQMTKernelProductCompositionError(
                        "MINIQMT_K6_PRODUCT_OUTBOX_CANDIDATE_INVALID",
                        "repository returned a non-dispatchable product command",
                        context={"runtime_id": self.runtime_id, "command_id": outbox.command_id},
                    )
                self.outbox_dispatcher.dispatch_one(
                    command_id=outbox.command_id,
                    observed_at_utc=observed,
                    lease_expires_at_utc=observed + timedelta(seconds=60),
                )
                dispatched.append(outbox.command_id)
        remaining = self.repository.list_dispatchable_outbox_commands(
            runtime_id=self.runtime_id,
            observed_at_utc=observed,
            limit=1,
        )
        if remaining:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_OUTBOX_DRAIN_LIMIT_EXCEEDED",
                "product outbox drain exceeded the bounded 1000-command cycle",
                context={"runtime_id": self.runtime_id, "dispatched_count": len(dispatched)},
            )
        self.reconcile_due_outbox_v1(observed_at=observed)
        return tuple(dispatched)

    def reconcile_due_outbox_v1(self, *, observed_at: datetime) -> tuple[str, ...]:
        """Advance due unknown outcomes through the existing bounded K2 reconciler."""

        if self.outbox_reconciler is None:
            return ()
        observed = self._observed_utc_v1(observed_at)
        reconciled: list[str] = []
        for _ in range(10):
            due = self.repository.list_reconcilable_outbox_commands(
                runtime_id=self.runtime_id,
                observed_at_utc=observed,
                limit=100,
            )
            if not due:
                return tuple(reconciled)
            for outbox in due:
                self.outbox_reconciler.reconcile_one(
                    command_id=outbox.command_id,
                    observed_at_utc=observed,
                )
                reconciled.append(outbox.command_id)
        remaining = self.repository.list_reconcilable_outbox_commands(
            runtime_id=self.runtime_id,
            observed_at_utc=observed,
            limit=1,
        )
        if remaining:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_RECONCILE_LIMIT_EXCEEDED",
                "product outbox reconciliation exceeded the bounded 1000-command cycle",
                context={"runtime_id": self.runtime_id, "reconciled_count": len(reconciled)},
            )
        return tuple(reconciled)

    def ingest_order_callback_v1(
        self,
        *,
        broker_order_id: str,
        raw_payload: dict[str, Any],
        observed_at: datetime,
    ) -> RuntimeEventEnvelopeV2:
        event = self.callback_ingress.ingest_order_v1(
            runtime_id=self.runtime_id,
            broker_order_id=broker_order_id,
            raw_payload=raw_payload,
            observed_at_utc=observed_at,
        )
        self.dispatch_due_outbox_v1(observed_at=observed_at)
        return event

    def ingest_trade_callback_v1(
        self,
        *,
        broker_order_id: str,
        trade_quantity: int,
        trade_price_decimal: Any,
        cumulative_quantity: int,
        raw_payload: dict[str, Any],
        observed_at: datetime,
    ) -> RuntimeEventEnvelopeV2:
        event = self.callback_ingress.ingest_trade_v1(
            runtime_id=self.runtime_id,
            broker_order_id=broker_order_id,
            trade_quantity=trade_quantity,
            trade_price_decimal=trade_price_decimal,
            cumulative_quantity=cumulative_quantity,
            raw_payload=raw_payload,
            observed_at_utc=observed_at,
        )
        self.dispatch_due_outbox_v1(observed_at=observed_at)
        return event

    def sync_gateway_callbacks_v1(self, *, observed_at: datetime) -> tuple[str, ...]:
        observed = self._observed_utc_v1(observed_at)
        if self.snapshot_ingress is None:
            return ()
        return self.snapshot_ingress.sync_v1(
            runtime_id=self.runtime_id,
            observed_at_utc=observed,
        )

    def scheduler_tick_v1(self, *, observed_at: datetime, monotonic_ns: int) -> tuple[str, ...]:
        """Advance broker callbacks and exchange time even when no quote arrives.

        Callback facts are ingested before the time-driven SESSION/TIMER/EOD
        events at the same scheduler observation, so a terminal broker fact is
        visible to the algorithm before its next time transition.
        """

        observed = self._observed_utc_v1(observed_at)
        self.hot_market_data_ingress.retry_pending_v1(observed_at_utc=observed)
        callback_ids = self.sync_gateway_callbacks_v1(observed_at=observed)
        self.wake_clock_v1(observed_at=observed, monotonic_ns=monotonic_ns)
        self.refresh_hot_market_targets_v1()
        return callback_ids

    def _ingest_bounded_v1(self, *, builder: Any) -> None:
        probe = builder(1)
        try:
            existing = self.repository.read_event_transaction(probe.event_id)
        except KeyError:
            existing = None
        if existing is not None:
            expected = builder(existing["event"].sequence)
            if expected != existing["event"]:
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_SOURCE_EVENT_DRIFT",
                    "native source identity already exists with different event facts",
                    context={"runtime_id": self.runtime_id, "event_id": probe.event_id},
                )
            self.coordinator.ingest_native_event_v1(event=existing["event"])
            return
        last_error: Exception | None = None
        for _ in range(3):
            event = builder(self.repository.read_runtime_last_event_sequence(self.runtime_id) + 1)
            try:
                self.coordinator.ingest_native_event_v1(event=event)
                return
            except (KernelRepositoryConflict, KernelRepositoryCommitUnknown) as exc:
                last_error = exc
                try:
                    committed = self.repository.read_event_transaction(event.event_id)
                except KeyError:
                    if isinstance(exc, KernelRepositoryCommitUnknown):
                        raise exc
                    if str(exc) not in {
                        "event sequence is not the exact runtime successor",
                        "runtime event sequence CAS failed",
                    }:
                        raise exc
                    continue
                expected = builder(committed["event"].sequence)
                if expected != committed["event"]:
                    raise MiniQMTKernelProductCompositionError(
                        "MINIQMT_K6_PRODUCT_SOURCE_EVENT_DRIFT",
                        "native source event committed with conflicting facts",
                        context={"runtime_id": self.runtime_id, "event_id": event.event_id},
                    ) from exc
                self.coordinator.ingest_native_event_v1(event=committed["event"])
                return
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_SOURCE_EVENT_CONTENTION",
            "native source event could not acquire the exact runtime sequence",
            context={"runtime_id": self.runtime_id, "event_id": probe.event_id, "error": str(last_error)},
        ) from last_error

    @staticmethod
    def _observed_utc_v1(value: datetime) -> datetime:
        if not isinstance(value, datetime) or value.tzinfo is None:
            raise TypeError("observed_at must be timezone-aware")
        return value.astimezone(UTC)


def _session_authority(runtime_id: str, context: QuoteEvaluationContext) -> ExchangeSessionAuthorityV1:
    snapshot_set = context.calendar_snapshot_set
    snapshot_json = json.loads(canonical_json_bytes(snapshot_set.canonical_payload()).decode("utf-8"))
    snapshot_json["set_sha256"] = snapshot_set.set_sha256
    ordered_markets = (MarketCode.SH, MarketCode.SZ, MarketCode.BJ)
    first = snapshot_set.snapshot_by_market[MarketCode.SH]
    return ExchangeSessionAuthorityV1.create(
        runtime_id=runtime_id,
        exchange_trade_date=first.trade_date.isoformat(),
        calendar_snapshot_set_id=snapshot_set.snapshot_set_id,
        calendar_snapshot_set_json=snapshot_json,
        calendar_snapshot_set_sha256=snapshot_set.set_sha256,
        ordered_market_calendar_sha256s=tuple(
            snapshot_set.snapshot_by_market[market].calendar_sha256 for market in ordered_markets
        ),
        ordered_session_segments=tuple(item.canonical_payload() for item in first.session_segments),
        source_effective_at_utc=first.effective_at_utc,
    )


def _exchange_session_economic_authority_payload_v1(
    authority: ExchangeSessionAuthorityV1,
) -> dict[str, Any]:
    """Project restart-stable exchange facts while retaining the durable generation separately."""

    if not isinstance(authority, ExchangeSessionAuthorityV1):
        raise TypeError("authority must be ExchangeSessionAuthorityV1")
    snapshot_set = thaw_json_v1(authority.calendar_snapshot_set_json)
    snapshots = snapshot_set["snapshot_by_market"]
    ordered_snapshots: list[dict[str, Any]] = []
    for market in (MarketCode.SH, MarketCode.SZ, MarketCode.BJ):
        snapshot = dict(snapshots[f"MarketCode.{market.value}"])
        snapshot.pop("effective_at_utc")
        ordered_snapshots.append(snapshot)
    return {
        "runtime_id": authority.runtime_id,
        "exchange_trade_date": authority.exchange_trade_date,
        "timezone": authority.timezone,
        "session_definition_version": authority.session_definition_version,
        "ordered_session_segments": [thaw_json_v1(item) for item in authority.ordered_session_segments],
        "ordered_market_snapshots": ordered_snapshots,
    }


def _exchange_session_economic_authority_sha256_v1(authority: ExchangeSessionAuthorityV1) -> str:
    return hash_hex_v1(
        "miniqmt_exchange_session_economic_authority_v1",
        _exchange_session_economic_authority_payload_v1(authority),
    )


def _exchange_session_drift_context_v1(
    *,
    persisted: ExchangeSessionAuthorityV1,
    candidate: ExchangeSessionAuthorityV1,
) -> dict[str, Any]:
    persisted_economic = _exchange_session_economic_authority_payload_v1(persisted)
    candidate_economic = _exchange_session_economic_authority_payload_v1(candidate)
    return {
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
        "persisted_economic_authority_sha256": _exchange_session_economic_authority_sha256_v1(persisted),
        "candidate_economic_authority_sha256": _exchange_session_economic_authority_sha256_v1(candidate),
        "economic_conflict_fields": sorted(
            key
            for key in set(persisted_economic) | set(candidate_economic)
            if persisted_economic.get(key) != candidate_economic.get(key)
        ),
    }


def _resolve_product_exchange_session_authority_v1(
    *,
    repository: Any,
    candidate: ExchangeSessionAuthorityV1,
) -> ExchangeSessionAuthorityV1:
    """Resolve one insert-once durable session without treating observation identity as economic drift."""

    if not isinstance(candidate, ExchangeSessionAuthorityV1):
        raise TypeError("candidate must be ExchangeSessionAuthorityV1")

    def read_persisted() -> ExchangeSessionAuthorityV1:
        try:
            persisted = repository.read_exchange_session_authority(
                runtime_id=candidate.runtime_id,
                exchange_trade_date=date.fromisoformat(candidate.exchange_trade_date),
            )
        except KeyError:
            raise
        except KernelRepositoryConflict as exc:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_CONFLICT",
                "durable exchange-session authority failed strict repository readback",
                context={
                    "runtime_id": candidate.runtime_id,
                    "exchange_trade_date": candidate.exchange_trade_date,
                    "candidate_authority_sha256": candidate.authority_sha256,
                    "repository_conflict_type": type(exc).__name__,
                    "repository_conflict": str(exc),
                },
            ) from exc
        if not isinstance(persisted, ExchangeSessionAuthorityV1):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_INVALID",
                "durable exchange-session authority readback is not a strict carrier",
                context={
                    "runtime_id": candidate.runtime_id,
                    "exchange_trade_date": candidate.exchange_trade_date,
                    "candidate_authority_sha256": candidate.authority_sha256,
                    "readback_type": type(persisted).__name__,
                },
            )
        return persisted

    try:
        persisted = read_persisted()
    except KeyError:
        try:
            persisted = repository.write_exchange_session_authority(candidate)
        except (KernelRepositoryConflict, KernelRepositoryCommitUnknown) as exc:
            try:
                persisted = read_persisted()
            except KeyError:
                if isinstance(exc, KernelRepositoryCommitUnknown):
                    raise exc
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_WRITE_CONFLICT",
                    "exchange-session write conflicted without an exact durable readback",
                    context={
                        "runtime_id": candidate.runtime_id,
                        "exchange_trade_date": candidate.exchange_trade_date,
                        "candidate_authority_sha256": candidate.authority_sha256,
                        "repository_conflict_type": type(exc).__name__,
                        "repository_conflict": str(exc),
                    },
                ) from exc
        if not isinstance(persisted, ExchangeSessionAuthorityV1):
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_READBACK_INVALID",
                "exchange-session writer did not return a strict durable carrier",
                context={
                    "runtime_id": candidate.runtime_id,
                    "exchange_trade_date": candidate.exchange_trade_date,
                    "candidate_authority_sha256": candidate.authority_sha256,
                    "readback_type": type(persisted).__name__,
                },
            )

    if persisted == candidate:
        return persisted
    persisted_economic = _exchange_session_economic_authority_payload_v1(persisted)
    candidate_economic = _exchange_session_economic_authority_payload_v1(candidate)
    if persisted_economic == candidate_economic:
        persisted_effective = datetime.fromisoformat(persisted.source_effective_at_utc.replace("Z", "+00:00"))
        candidate_effective = datetime.fromisoformat(candidate.source_effective_at_utc.replace("Z", "+00:00"))
        if candidate_effective < persisted_effective:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_REBUILD_STALE",
                "current exchange-session observation predates the durable generation",
                context=_exchange_session_drift_context_v1(persisted=persisted, candidate=candidate),
            )
        return persisted
    raise MiniQMTKernelProductCompositionError(
        "MINIQMT_K6_PRODUCT_EXCHANGE_SESSION_AUTHORITY_DRIFT",
        "current exchange-session facts conflict with the insert-once durable authority",
        context=_exchange_session_drift_context_v1(persisted=persisted, candidate=candidate),
    )


def build_simulation_miniqmt_product_runtime_v1(
    *,
    simulation_repository: Any,
    execution_plan: ExecutionPlan,
    binding: SimulationReleaseBinding,
    managed_order_service: QmtManagedOrderService | None,
    quote_context_adapter: Any,
    quote_ingress_activation: Any,
    observed_at: datetime,
    broker_side_effects_enabled: bool,
) -> SimulationMiniQMTProductRuntimeV1:
    if type(broker_side_effects_enabled) is not bool:
        raise TypeError("broker_side_effects_enabled must be a strict bool")
    register = getattr(quote_ingress_activation, "register_kernel_product_runtime", None)
    if not callable(register):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_QUOTE_PUBLISHER_UNAVAILABLE",
            "quote activation does not expose the KERNEL_V2 publisher registry",
            context={"binding_id": binding.binding_id, "plan_id": execution_plan.plan_id},
        )
    ledger_repository = getattr(managed_order_service, "_repository", None)
    if (
        managed_order_service is None
        or not callable(getattr(managed_order_service, "preview_order", None))
        or ledger_repository is None
    ):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_OMS_AUTHORITY_MISSING",
            "KERNEL_V2 product root requires the existing QmtManagedOrderService authority",
            context={"binding_id": binding.binding_id, "plan_id": execution_plan.plan_id},
        )
    context_store = getattr(quote_context_adapter, "context_store", None)
    if context_store is None or not callable(getattr(context_store, "snapshot", None)):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_QUOTE_CONTEXT_MISSING",
            "KERNEL_V2 product root requires the scheduler-published B0 context",
            context={"binding_id": binding.binding_id, "plan_id": execution_plan.plan_id},
        )
    context = context_store.snapshot()
    if not isinstance(context, QuoteEvaluationContext):
        raise MiniQMTKernelProductCompositionError(
            "MINIQMT_K6_PRODUCT_QUOTE_CONTEXT_INVALID",
            "B0 context strict readback is unavailable",
            context={"binding_id": binding.binding_id, "plan_id": execution_plan.plan_id},
        )
    runtime_id = _runtime_id(execution_plan, binding)
    source_capability = build_k6d_route_source_capability_v1()
    existing_reader = getattr(quote_ingress_activation, "get_kernel_product_runtime", None)
    if callable(existing_reader):
        existing = existing_reader(runtime_id)
        if existing is not None:
            expected_symbols = tuple(dict.fromkeys(intent.symbol for intent in execution_plan.intents))
            if (
                not isinstance(existing, SimulationMiniQMTProductRuntimeV1)
                or getattr(existing, "binding_id", None) != binding.binding_id
                or getattr(existing, "trade_date", None) != execution_plan.target_trade_date
                or tuple(getattr(existing, "symbols", ())) != expected_symbols
                or getattr(existing, "source_capability_sha256", None) != source_capability.capability_sha256
            ):
                raise MiniQMTKernelProductCompositionError(
                    "MINIQMT_K6_PRODUCT_RUNTIME_IDEMPOTENCY_CONFLICT",
                    "existing product runtime differs from the requested frozen plan owner",
                    context={"runtime_id": runtime_id, "binding_id": binding.binding_id},
                )
            return existing
    gateway = build_k6d_gateway_catalog_v1()
    authority = build_hot_full_five_catalog_authority_v1(gateway_catalog=gateway)
    repository = PostgresMiniQMTKernelRepository(
        product_catalog_snapshot=authority.catalog_runtime.snapshot,
        product_gateway_catalog=gateway,
    )
    repository.ensure_product_runtime_v1(
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        execution_plan_id=execution_plan.plan_id,
    )
    session = _session_authority(runtime_id, context)
    persisted_session = _resolve_product_exchange_session_authority_v1(
        repository=repository,
        candidate=session,
    )
    startup = repository.start_worker_incarnation(
        worker_id="miniqmt_kernel_v2_product",
        process_role="K6D_PRODUCT_DELIVERY",
        source_revision=source_capability.capability_sha256,
        started_at_utc=observed_at.astimezone(UTC),
    )
    evidence = KernelProductEvidenceProviderV3(gateway_catalog=gateway)
    worker = KernelProductDeliveryWorkerV3(
        repository=repository,
        catalog_runtime=authority.catalog_runtime,
        worker_id=startup.worker_id,
        process_incarnation_id=startup.process_incarnation_id,
        facade_authority=authority.conformance_authority,
        gateway_catalog=gateway,
        product_evidence_provider=evidence,
    )
    outcome_ingress = KernelOutboxOutcomeIngressV1(
        repository=repository,
        catalog_runtime=authority.catalog_runtime,
    )
    outcome_publisher = _ProductOutcomePublisherV1(outcome_ingress)
    coordinator = MiniQMTKernelV2ProductCoordinator(
        plan_authority_reader=SimulationK6DPlanAuthorityReader(
            simulation_repository=simulation_repository,
            account_repository=ledger_repository,
            gateway_catalog=gateway,
            session_authority=persisted_session,
            logical_time_utc=observed_at,
        ),
        source_event_reader=_CommittedKernelSourceReader(repository),
        cutover_coordinator=KernelProductCutoverCoordinator(repository=repository),
        creation_coordinator_factory=lambda incarnation: KernelAlgoCreationCoordinatorV2(
            repository=repository,
            catalog_runtime=authority.catalog_runtime,
            gateway_catalog=gateway,
            worker_incarnation_id=incarnation,
            facade_authority=authority.conformance_authority,
        ),
        ingress_coordinator=KernelIngressCoordinatorV1(
            repository=repository,
            catalog_runtime=authority.catalog_runtime,
        ),
        delivery_worker=worker,
    )
    callback_ingress = KernelProductCallbackIngressV1(
        repository=repository,
        coordinator=coordinator,
    )
    outcome_publisher.bind_coordinator_v1(coordinator)
    outbox_dispatcher: KernelOutboxDispatcherV1 | None = None
    outbox_reconciler: KernelOutboxReconcilerV1 | None = None
    snapshot_ingress: KernelProductSnapshotIngressV1 | None = None
    if broker_side_effects_enabled:
        qmt_client = getattr(managed_order_service, "_broker", None)
        if qmt_client is None:
            raise MiniQMTKernelProductCompositionError(
                "MINIQMT_K6_PRODUCT_GATEWAY_UNAVAILABLE",
                "broker-enabled KERNEL_V2 product root has no existing MiniQMT client",
                context={"runtime_id": runtime_id, "binding_id": binding.binding_id},
            )
        transport = QmtClientMiniQMTGateway(
            qmt_client=qmt_client,
            strategy_name=binding.strategy_id,
            order_remark_prefix="aistock-kernel-v2",
        )
        gateway_adapter = MiniQMTKernelGatewayAdapterV1(gateway=transport)
        outbox_dispatcher = KernelOutboxDispatcherV1(
            repository=repository,
            gateway=gateway_adapter,
            gateway_catalog=gateway,
            outcome_ingress=outcome_publisher,
            lease_owner=f"{startup.worker_id}:{startup.process_incarnation_id}",
            process_incarnation_id=startup.process_incarnation_id,
        )
        outbox_reconciler = KernelOutboxReconcilerV1(
            repository=repository,
            gateway=gateway_adapter,
            gateway_catalog=gateway,
            outcome_ingress=outcome_publisher,
        )
        snapshot_ingress = KernelProductSnapshotIngressV1(
            repository=repository,
            gateway=gateway_adapter,
            ingress=callback_ingress,
        )
    clock = ExchangeSessionClockV1(
        repository=repository,
        catalog_runtime=authority.catalog_runtime,
        lease_owner=f"{startup.worker_id}:{startup.process_incarnation_id}",
    )
    hot_effect_committer = _BoundHotMarketEffectCommitterV1()
    hot_market_data_ingress = HotMarketDataIngressV1(
        runtime_id=runtime_id,
        effect_committer=hot_effect_committer,
    )
    runtime = SimulationMiniQMTProductRuntimeV1(
        coordinator=coordinator,
        worker_incarnation_id=startup.process_incarnation_id,
        runtime_id=runtime_id,
        binding_id=binding.binding_id,
        trade_date=execution_plan.target_trade_date,
        symbols=tuple(dict.fromkeys(intent.symbol for intent in execution_plan.intents)),
        source_capability_sha256=source_capability.capability_sha256,
        quote_context_id=context.context_id,
        exchange_session_authority=persisted_session,
        repository=repository,
        clock=clock,
        outbox_dispatcher=outbox_dispatcher,
        outbox_reconciler=outbox_reconciler,
        callback_ingress=callback_ingress,
        snapshot_ingress=snapshot_ingress,
        hot_market_data_ingress=hot_market_data_ingress,
    )
    hot_effect_committer.bind_v1(runtime)
    register(runtime=runtime, symbols=tuple(intent.symbol for intent in execution_plan.intents))
    return runtime


__all__ = [
    "MiniQMTKernelProductCompositionError",
    "SimulationK6DPlanAuthorityReader",
    "SimulationMiniQMTProductRuntimeV1",
    "build_k6d_gateway_catalog_v1",
    "build_simulation_miniqmt_product_runtime_v1",
]
