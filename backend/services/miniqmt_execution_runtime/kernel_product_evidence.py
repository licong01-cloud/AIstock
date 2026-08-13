"""Same-transaction product evidence authority for the final MiniQMT route."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any

from backend.services.qmt_strategy_ledger.order_service import (
    BUY_ORDER_TYPE,
    SELL_ORDER_TYPE,
    ManagedOrderRequest,
    QmtManagedOrderService,
)
from backend.services.qmt_strategy_ledger.repository import (
    _row_to_order_intent,
    _row_to_position_lot,
    _row_to_virtual_account,
)
from backend.services.qmt_strategy_ledger.models import VirtualAccount, VirtualAccountStatus
from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeAuthorityInputV2

from .kernel_delivery import KernelTransitionWriteBundleV1
from .kernel_product_authority import (
    bind_product_transition_receipt_v3,
    build_product_command_authority_set_v3,
)
from .kernel_product_contracts import (
    DependentBuyCandidateAuthorityV2,
    DependentBuyDependencyStatusV1,
    DependentBuySellDependencyV2,
    ProductCommandEvaluationEvidenceV3,
    ProductCommandAuthorityEnvelopeV3,
)
from .plugin_canonical import hash_hex_v1, json_safe_evidence_v1, thaw_json_v1
from .plugin_contracts import (
    AlgoDeliveryPersistenceV1,
    AlgoReadOnlyServicesV1,
    AlgoTransitionV1,
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CommandChildMappingStatusV1,
    ExecutionAlgoInstancePersistenceV2,
    ExecutionCommandChildMappingV1,
    ExecutionProjectionRefV1,
    ExecutionProjectionSetV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    OMSPreflightDecisionV1,
    OMSPreflightProjectionReceiptV1,
    RiskDecisionActionV1,
    RiskDecisionStageV1,
    RuntimeEventEnvelopeV2,
    EventTypeV2,
    SideV1,
    command_child_mapping_id_v1,
    deterministic_client_order_ref_v1,
    execution_child_order_id_v1,
)
from .plugin_contracts import GatewayCapabilityCatalogV1
from .plugin_registry import PluginRouteCompatibilityReceiptV1


_DEPENDENT_BUY_CODES = frozenset({"SELL_PROCEEDS_REQUIRED", "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED"})


class KernelProductEvidenceError(RuntimeError):
    """Typed fail-loud product evidence failure."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


class VirtualAccountProjectionError(ValueError):
    """The durable virtual-account carrier cannot form the V1 kernel projection."""


def _virtual_account_json_value_v1(value: Any, *, path: str) -> Any:
    if value is None or type(value) in {str, bool, int}:
        return value
    if type(value) is float:
        if value != value or value in {float("inf"), float("-inf")}:
            raise VirtualAccountProjectionError(f"{path} must contain a finite JSON number")
        return value
    if type(value) is list:
        return [_virtual_account_json_value_v1(item, path=f"{path}[{index}]") for index, item in enumerate(value)]
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise VirtualAccountProjectionError(f"{path} must contain only string object keys")
        return {key: _virtual_account_json_value_v1(value[key], path=f"{path}.{key}") for key in sorted(value)}
    raise VirtualAccountProjectionError(f"{path} contains non-JSON value type {type(value).__name__}")


def virtual_account_projection_v1(account: VirtualAccount) -> dict[str, Any]:
    """Project the repository-owned account into the sole KERNEL_V2 account authority."""

    if type(account) is not VirtualAccount:
        raise VirtualAccountProjectionError(
            f"account must be the exact VirtualAccount carrier, got {type(account).__name__}"
        )
    text_fields = {
        "strategy_id": account.strategy_id,
        "strategy_name": account.strategy_name,
        "account_id": account.account_id,
        "mode": account.mode,
    }
    for field_name, value in text_fields.items():
        if type(value) is not str or not value.strip():
            raise VirtualAccountProjectionError(f"{field_name} must be a non-empty string")
    if account.mode not in {"SIM", "LIVE"}:
        raise VirtualAccountProjectionError("mode must be SIM or LIVE")
    decimal_fields = {
        "initial_cash": account.initial_cash,
        "cash": account.cash,
        "frozen_cash": account.frozen_cash,
        "market_value": account.market_value,
        "realized_pnl": account.realized_pnl,
        "unrealized_pnl": account.unrealized_pnl,
    }
    for field_name, value in decimal_fields.items():
        if type(value) is not Decimal or not value.is_finite():
            raise VirtualAccountProjectionError(f"{field_name} must be a finite Decimal")
    if account.initial_cash <= Decimal("0"):
        raise VirtualAccountProjectionError("initial_cash must be positive")
    if account.cash < Decimal("0") or account.frozen_cash < Decimal("0"):
        raise VirtualAccountProjectionError("cash and frozen_cash must be non-negative")
    if type(account.status) is not VirtualAccountStatus:
        raise VirtualAccountProjectionError("status must be VirtualAccountStatus")
    if type(account.risk_config) is not dict:
        raise VirtualAccountProjectionError("risk_config must be an exact JSON object")
    if type(account.updated_at) is not datetime or account.updated_at.tzinfo is None:
        raise VirtualAccountProjectionError("updated_at must be timezone-aware datetime")
    risk_config = _virtual_account_json_value_v1(account.risk_config, path="risk_config")
    if type(risk_config) is not dict:
        raise VirtualAccountProjectionError("risk_config must project to an exact JSON object")
    return {
        "strategy_id": account.strategy_id,
        "strategy_name": account.strategy_name,
        "account_id": account.account_id,
        "mode": account.mode,
        "cash": str(account.cash),
        "frozen_cash": str(account.frozen_cash),
        "market_value": str(account.market_value),
        "status": account.status.value,
        "risk_config": risk_config,
        "updated_at_utc": account.updated_at.astimezone(UTC).isoformat(),
    }


@dataclass(frozen=True)
class ProductEvidenceBuildResultV3:
    services: AlgoReadOnlyServicesV1
    ordered_evidence: tuple[ProductCommandEvaluationEvidenceV3, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.services, AlgoReadOnlyServicesV1):
            raise TypeError("services must be AlgoReadOnlyServicesV1")
        if type(self.ordered_evidence) is not tuple or any(
            not isinstance(item, ProductCommandEvaluationEvidenceV3) for item in self.ordered_evidence
        ):
            raise TypeError("ordered_evidence must contain strict ProductCommandEvaluationEvidenceV3 items")


@dataclass(frozen=True)
class ProductBoundTransitionV3:
    transition_bundle: KernelTransitionWriteBundleV1
    authority_envelope: ProductCommandAuthorityEnvelopeV3


def bind_product_transition_bundle_v3(
    *,
    proposal_bundle: KernelTransitionWriteBundleV1,
    replay_bundle: KernelTransitionWriteBundleV1,
    evidence: ProductEvidenceBuildResultV3,
    creation_binding: VnpyFacadeAuthorityInputV2,
) -> ProductBoundTransitionV3:
    """Close proposal/replay parity and bind the only durable V3 receipt."""

    if not isinstance(proposal_bundle, KernelTransitionWriteBundleV1) or not isinstance(
        replay_bundle, KernelTransitionWriteBundleV1
    ):
        raise TypeError("proposal_bundle and replay_bundle must be KernelTransitionWriteBundleV1")
    if not isinstance(evidence, ProductEvidenceBuildResultV3):
        raise TypeError("evidence must be ProductEvidenceBuildResultV3")
    if not isinstance(creation_binding, VnpyFacadeAuthorityInputV2):
        raise TypeError("creation_binding must be VnpyFacadeAuthorityInputV2")
    proposal = proposal_bundle.applied_transition
    replay = replay_bundle.applied_transition
    if proposal is None or replay is None or proposal != replay:
        raise KernelProductEvidenceError(
            "MINIQMT_K6_PRODUCT_REPLAY_DRIFT",
            "product plugin replay differs from the proposal transition",
            context={
                "proposal_transition": None if proposal is None else proposal.effect_set_sha256,
                "replay_transition": None if replay is None else replay.effect_set_sha256,
                "broker_called": False,
            },
        )
    if replay_bundle.projection_set != evidence.services.execution_projection_set:
        raise KernelProductEvidenceError(
            "MINIQMT_K6_PRODUCT_REPLAY_PROJECTION_DRIFT",
            "product replay did not consume the exact evidence projection set",
            context={"transition_id": replay_bundle.receipt.transition_id, "broker_called": False},
        )
    if not hasattr(replay_bundle.receipt, "ordered_command_ids"):
        raise KernelProductEvidenceError(
            "MINIQMT_K6_PRODUCT_REPLAY_NOT_APPLIED",
            "product replay did not produce an APPLIED transition",
            context={"receipt_type": type(replay_bundle.receipt).__name__, "broker_called": False},
        )
    receipt = bind_product_transition_receipt_v3(
        transition=replay,
        transition_receipt=replay_bundle.receipt,
        ordered_evidence=evidence.ordered_evidence,
        timer_schedules=replay_bundle.timer_schedules,
    )
    bundle = KernelTransitionWriteBundleV1.create(
        algo_instance=replay_bundle.algo_instance,
        delivery=replay_bundle.delivery,
        receipt=receipt,
        projection_set=replay_bundle.projection_set,
        after_state=replay_bundle.after_state,
        applied_transition=replay,
        new_child_mappings=replay_bundle.new_child_mappings,
        command_outboxes=replay_bundle.command_outboxes,
        updated_child_mappings=replay_bundle.updated_child_mappings,
        updated_command_outboxes=replay_bundle.updated_command_outboxes,
        timer_mutations=replay_bundle.timer_mutations,
        timer_schedules=replay_bundle.timer_schedules,
        diagnostic_observations=replay_bundle.diagnostic_observations,
    )
    authority = build_product_command_authority_set_v3(
        transition=replay,
        transition_receipt=receipt,
        projection_set=evidence.services.execution_projection_set,
        ordered_evidence=evidence.ordered_evidence,
        catalog=creation_binding.plugin_catalog_snapshot,
        creation_binding=creation_binding,
        timer_schedules=bundle.timer_schedules,
    )
    return ProductBoundTransitionV3(
        transition_bundle=bundle,
        authority_envelope=ProductCommandAuthorityEnvelopeV3.create(
            authority_set=authority,
            creation_authority=creation_binding,
            ordered_timer_schedules=bundle.timer_schedules,
        ),
    )


class _CursorTradingCalendar:
    def __init__(self, cur: Any) -> None:
        self._cur = cur

    def is_trading_day(self, trade_date: date) -> bool:
        self._cur.execute(
            "SELECT is_trading FROM market.trading_calendar WHERE cal_date=%s FOR SHARE",
            (trade_date,),
        )
        row = self._cur.fetchone()
        if row is None:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CALENDAR_AUTHORITY_MISSING",
                "trading calendar authority is missing",
                context={"trade_date": trade_date.isoformat()},
            )
        value = row["is_trading"] if isinstance(row, dict) else row[0]
        if type(value) is not bool:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CALENDAR_AUTHORITY_INVALID",
                "trading calendar authority is not a strict boolean",
                context={"trade_date": trade_date.isoformat(), "actual_type": type(value).__name__},
            )
        return value

    def next_trading_day_after(self, trade_date: date) -> date:
        self._cur.execute(
            "SELECT cal_date AS next_day FROM market.trading_calendar "
            "WHERE cal_date>%s AND is_trading=TRUE ORDER BY cal_date LIMIT 1 FOR SHARE",
            (trade_date,),
        )
        row = self._cur.fetchone()
        value = None if row is None else (row["next_day"] if isinstance(row, dict) else row[0])
        if type(value) is not date:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CALENDAR_SUCCESSOR_MISSING",
                "next trading day authority is missing",
                context={"trade_date": trade_date.isoformat()},
            )
        return value


class _CursorLedgerReadRepository:
    """The read subset used by QmtManagedOrderService.preview_order."""

    def __init__(self, cur: Any) -> None:
        self._cur = cur

    def list_virtual_accounts(self, account_id: str | None = None) -> list[Any]:
        if account_id is None:
            self._cur.execute("SELECT * FROM qmt_strategy.virtual_account ORDER BY created_at,strategy_id FOR SHARE")
        else:
            self._cur.execute(
                "SELECT * FROM qmt_strategy.virtual_account WHERE account_id=%s "
                "ORDER BY created_at,strategy_id FOR SHARE",
                (account_id,),
            )
        return [_row_to_virtual_account(row) for row in self._cur.fetchall()]

    def get_order_intent_by_remark(self, account_id: str, order_remark: str) -> Any | None:
        self._cur.execute(
            "SELECT * FROM qmt_strategy.order_intent WHERE account_id=%s AND order_remark=%s FOR SHARE",
            (account_id, order_remark),
        )
        row = self._cur.fetchone()
        return None if row is None else _row_to_order_intent(row)

    def list_position_lots(self, strategy_id: str, symbol: str | None = None) -> list[Any]:
        if symbol is None:
            self._cur.execute(
                "SELECT * FROM qmt_strategy.position_lot WHERE strategy_id=%s ORDER BY open_date,lot_id FOR SHARE",
                (strategy_id,),
            )
        else:
            self._cur.execute(
                "SELECT * FROM qmt_strategy.position_lot WHERE strategy_id=%s AND symbol=%s "
                "ORDER BY open_date,lot_id FOR SHARE",
                (strategy_id, symbol),
            )
        return [_row_to_position_lot(row) for row in self._cur.fetchall()]

    def list_open_sell_intents(
        self,
        strategy_id: str,
        symbol: str | None = None,
        trade_date: date | None = None,
    ) -> list[Any]:
        filters = [
            "strategy_id=%s",
            "side='SELL'",
            "submit_status IN ('CREATED','SUBMITTED','ACCEPTED')",
        ]
        params: list[Any] = [strategy_id]
        if symbol is not None:
            filters.append("symbol=%s")
            params.append(symbol)
        if trade_date is not None:
            filters.append("trade_date=%s")
            params.append(trade_date)
        self._cur.execute(
            "SELECT * FROM qmt_strategy.order_intent WHERE "
            + " AND ".join(filters)
            + " ORDER BY created_at,intent_id FOR SHARE",
            tuple(params),
        )
        return [_row_to_order_intent(row) for row in self._cur.fetchall()]


class KernelProductEvidenceProviderV3:
    """Build V3 evidence only from rows locked by the product transaction."""

    def __init__(self, *, gateway_catalog: GatewayCapabilityCatalogV1) -> None:
        if not isinstance(gateway_catalog, GatewayCapabilityCatalogV1):
            raise TypeError("gateway_catalog must be GatewayCapabilityCatalogV1")
        self._gateway_catalog = GatewayCapabilityCatalogV1.model_validate(
            gateway_catalog.model_dump(mode="python"), strict=True
        )

    def build_base_services_with_cursor_v1(
        self,
        *,
        cur: Any,
        event: RuntimeEventEnvelopeV2,
        delivery: AlgoDeliveryPersistenceV1,
        algo: ExecutionAlgoInstancePersistenceV2,
    ) -> AlgoReadOnlyServicesV1:
        """Read the exact contract, market, account and gateway inputs on one cursor."""

        if not hasattr(cur, "execute"):
            raise TypeError("cur must be a database cursor")
        if not isinstance(event, RuntimeEventEnvelopeV2) or not isinstance(delivery, AlgoDeliveryPersistenceV1):
            raise TypeError("event and delivery must be strict kernel carriers")
        if not isinstance(algo, ExecutionAlgoInstancePersistenceV2):
            raise TypeError("algo must be ExecutionAlgoInstancePersistenceV2")
        plan = self._locked_plan_context(cur, event=event, algo=algo)
        ledger = _CursorLedgerReadRepository(cur)
        account = self._exact_virtual_account(
            ledger,
            account_id=plan["broker_account_id"],
            strategy_name=plan["strategy_name"],
        )
        account_payload = self._account_payload(account)
        intent = plan["intent"]
        decision = plan["trading_rule_decision"]
        contract_payload = {
            "symbol": algo.symbol,
            "side": algo.side.value,
            "parent_intent_id": algo.parent_intent_id,
            "strategy_slot_id": algo.strategy_slot_id,
            "target_quantity": algo.target_quantity,
            "trading_rule_decision_id": intent["trading_rule_decision_id"],
            "trading_rule_decision": decision,
        }
        market_id, market_payload, market_source_event_id = self._locked_market_projection(
            cur,
            event=event,
            algo=algo,
        )
        capability_payload = self._gateway_catalog.model_dump(mode="json")
        payloads = (
            (
                KernelProjectionTypeV1.CONTRACT,
                "mqcontract_" + hash_hex_v1("miniqmt_contract_projection_v1", contract_payload)[:32],
                "miniqmt_contract_projection_v1",
                contract_payload,
                event.event_id,
            ),
            (
                KernelProjectionTypeV1.ACCOUNT,
                "mqaccount_" + hash_hex_v1("miniqmt_account_projection_v1", account_payload)[:32],
                "miniqmt_account_projection_v1",
                account_payload,
                event.event_id,
            ),
            (
                KernelProjectionTypeV1.MARKET_CAPABILITY,
                "mqgateway_" + self._gateway_catalog.catalog_sha256[:32],
                self._gateway_catalog.schema_version,
                capability_payload,
                event.event_id,
            ),
        )
        refs = [
            ExecutionProjectionRefV1.create(
                projection_type=projection_type,
                projection_id=projection_id,
                projection_version=version,
                payload_sha256=hash_hex_v1(
                    {
                        KernelProjectionTypeV1.CONTRACT: "miniqmt_contract_projection_v1",
                        KernelProjectionTypeV1.ACCOUNT: "miniqmt_account_projection_v1",
                        KernelProjectionTypeV1.MARKET_CAPABILITY: "miniqmt_market_capability_projection_v1",
                    }[projection_type],
                    payload,
                ),
                source_event_id=source_event_id,
                logical_at_utc=event.event_time_utc,
            )
            for projection_type, projection_id, version, payload, source_event_id in payloads
        ]
        if market_payload is not None:
            refs.append(
                ExecutionProjectionRefV1.create(
                    projection_type=KernelProjectionTypeV1.MARKET_DATA,
                    projection_id=market_id,
                    projection_version="miniqmt_market_data_projection_v2",
                    payload_sha256=hash_hex_v1("miniqmt_market_data_projection_v2", market_payload),
                    source_event_id=market_source_event_id,
                    logical_at_utc=event.event_time_utc,
                )
            )
        projection_set = ExecutionProjectionSetV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            delivery_id=delivery.delivery_id,
            projection_refs=tuple(sorted(refs, key=lambda item: (item.projection_type.value, item.projection_id))),
        )
        return AlgoReadOnlyServicesV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            delivery_id=delivery.delivery_id,
            contract_projection_id=refs[0].projection_id,
            contract_projection=contract_payload,
            market_data_projection_id=market_id,
            market_data_projection=market_payload,
            account_projection_id=refs[1].projection_id,
            account_projection=account_payload,
            execution_projection_set=projection_set,
        )

    def build_with_cursor_v1(
        self,
        *,
        cur: Any,
        event: RuntimeEventEnvelopeV2,
        delivery: AlgoDeliveryPersistenceV1,
        algo: ExecutionAlgoInstancePersistenceV2,
        transition: AlgoTransitionV1,
        base_services: AlgoReadOnlyServicesV1,
        route_receipt: PluginRouteCompatibilityReceiptV1,
    ) -> ProductEvidenceBuildResultV3:
        if not hasattr(cur, "execute"):
            raise TypeError("cur must be a database cursor")
        strict_types = (
            (event, RuntimeEventEnvelopeV2, "event"),
            (delivery, AlgoDeliveryPersistenceV1, "delivery"),
            (algo, ExecutionAlgoInstancePersistenceV2, "algo"),
            (transition, AlgoTransitionV1, "transition"),
            (base_services, AlgoReadOnlyServicesV1, "base_services"),
            (route_receipt, PluginRouteCompatibilityReceiptV1, "route_receipt"),
        )
        for value, model_type, field_name in strict_types:
            if not isinstance(value, model_type):
                raise TypeError(f"{field_name} must be {model_type.__name__}")
        owner = (event.runtime_id, delivery.runtime_id, algo.runtime_id)
        if (
            len(set(owner)) != 1
            or delivery.event_id != event.event_id
            or delivery.algo_instance_id != algo.algo_instance_id
        ):
            raise self._error("OWNER_CLOSURE", event=event, delivery=delivery, algo=algo)
        if transition.next_state.algo_instance_id != algo.algo_instance_id or any(
            command.runtime_id != algo.runtime_id for command in transition.broker_commands
        ):
            raise self._error("TRANSITION_OWNER", event=event, delivery=delivery, algo=algo)
        commands = transition.broker_commands
        if not commands:
            return ProductEvidenceBuildResultV3(services=base_services, ordered_evidence=())
        same_cursor_base = self.build_base_services_with_cursor_v1(
            cur=cur,
            event=event,
            delivery=delivery,
            algo=algo,
        )
        if same_cursor_base != base_services:
            raise self._error("PRODUCT_BASE_PROJECTION_DRIFT", event=event, delivery=delivery, algo=algo)
        contract = thaw_json_v1(base_services.contract_projection)
        market = (
            None if base_services.market_data_projection is None else thaw_json_v1(base_services.market_data_projection)
        )
        if (
            not isinstance(contract, dict)
            or not contract
            or (market is not None and (not isinstance(market, dict) or not market))
        ):
            raise self._error("PRODUCT_PROJECTION_MISSING", event=event, delivery=delivery, algo=algo)

        plan = self._locked_plan_context(cur, event=event, algo=algo)
        ledger = _CursorLedgerReadRepository(cur)
        calendar = _CursorTradingCalendar(cur)
        service = QmtManagedOrderService(repository=ledger, broker=None, calendar_provider=calendar)
        account = self._exact_virtual_account(
            ledger,
            account_id=plan["broker_account_id"],
            strategy_name=plan["strategy_name"],
        )
        account_payload = self._account_payload(account)
        account_hash = hash_hex_v1("miniqmt_account_projection_v1", account_payload)
        evidence_parts: list[dict[str, Any]] = []
        for command in commands:
            mapping_id, child_order_id, client_ref = self._command_lineage(command)
            if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
                request = ManagedOrderRequest(
                    account_id=plan["broker_account_id"],
                    strategy_name=plan["strategy_name"],
                    symbol=command.symbol,
                    side=command.side.value,
                    order_type=BUY_ORDER_TYPE if command.side is SideV1.BUY else SELL_ORDER_TYPE,
                    quantity=command.quantity,
                    price_type=11,
                    price=Decimal(command.price_decimal),
                    order_remark=client_ref,
                    trade_date=plan["trade_date"],
                    mode="SIM",
                    package_id=plan["package_id"],
                    target_weight=plan["target_weight"],
                    metadata=plan["request_metadata"],
                )
                preflight = service.preview_order(request)
            else:
                preflight = None
                self._validate_cancel_owner(cur, command=command, mapping_id=mapping_id)
            facts = self._fact_payloads(
                cur,
                command=command,
                account=account,
                account_payload=account_payload,
                preflight=preflight,
                child_order_id=child_order_id,
                client_ref=client_ref,
                event_id=event.event_id,
                strategy_slot_id=algo.strategy_slot_id,
            )
            evidence_parts.append(
                {
                    "command": command,
                    "mapping_id": mapping_id,
                    "child_order_id": child_order_id,
                    "client_ref": client_ref,
                    **facts,
                }
            )

        refs = self._projection_refs(
            event=event,
            delivery=delivery,
            base_services=base_services,
            route_receipt=route_receipt,
            account_payload=account_payload,
            evidence_parts=evidence_parts,
        )
        projection_set = ExecutionProjectionSetV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            delivery_id=delivery.delivery_id,
            projection_refs=refs,
        )
        services = AlgoReadOnlyServicesV1.create(
            runtime_id=event.runtime_id,
            algo_instance_id=algo.algo_instance_id,
            event_id=event.event_id,
            delivery_id=delivery.delivery_id,
            contract_projection_id=base_services.contract_projection_id,
            contract_projection=contract,
            market_data_projection_id=base_services.market_data_projection_id,
            market_data_projection=market,
            account_projection_id=f"mqaccount_{account_hash[:32]}",
            account_projection=account_payload,
            execution_projection_set=projection_set,
        )
        ordered_evidence = tuple(
            ProductCommandEvaluationEvidenceV3.create(
                runtime_id=event.runtime_id,
                algo_instance_id=algo.algo_instance_id,
                event_id=event.event_id,
                delivery_id=delivery.delivery_id,
                transition_id=part["command"].transition_id,
                effect_ordinal=part["command"].ordinal,
                command_id=part["command"].command_id,
                oms_preflight_receipt=part["oms"],
                mini_qmt_risk_decision_receipt=part["risk"],
                plugin_route_compatibility_receipt=route_receipt,
                market_data_projection=market,
                account_projection=account_payload,
                contract_projection=contract,
                kill_switch_state=part["kill_switch"],
                execution_projection_set=projection_set,
                dependent_buy_candidate=self._dependent_buy_candidate(
                    cur,
                    command=part["command"],
                    oms=part["oms"],
                    preflight=part["preflight"],
                    plan=plan,
                    account=account,
                    event=event,
                ),
            )
            for part in evidence_parts
        )
        return ProductEvidenceBuildResultV3(services=services, ordered_evidence=ordered_evidence)

    @staticmethod
    def _locked_market_projection(
        cur: Any,
        *,
        event: RuntimeEventEnvelopeV2,
        algo: ExecutionAlgoInstancePersistenceV2,
    ) -> tuple[str | None, dict[str, Any] | None, str | None]:
        if event.event_type is not EventTypeV2.TICK:
            return None, None, None
        selected = event
        if selected.symbol != algo.symbol:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_MARKET_OWNER_CONFLICT",
                "market-data projection symbol differs from the durable algo owner",
                context={"event_id": selected.event_id, "symbol": selected.symbol, "algo_symbol": algo.symbol},
            )
        source_identity = thaw_json_v1(selected.source_identity)
        market_data_id = source_identity.get("market_data_id")
        if type(market_data_id) is not str or not market_data_id.strip():
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_MARKET_IDENTITY_INVALID",
                "native B0 event has no exact market_data_id",
                context={"event_id": selected.event_id},
            )
        return market_data_id, thaw_json_v1(selected.payload), selected.event_id

    @staticmethod
    def _error(stage: str, *, event: Any, delivery: Any, algo: Any) -> KernelProductEvidenceError:
        return KernelProductEvidenceError(
            f"MINIQMT_K6_{stage}",
            "product evidence authority could not be closed",
            context={
                "stage": stage,
                "runtime_id": getattr(event, "runtime_id", None),
                "event_id": getattr(event, "event_id", None),
                "delivery_id": getattr(delivery, "delivery_id", None),
                "algo_instance_id": getattr(algo, "algo_instance_id", None),
                "broker_called": False,
            },
        )

    @staticmethod
    def _locked_plan_context(
        cur: Any, *, event: RuntimeEventEnvelopeV2, algo: ExecutionAlgoInstancePersistenceV2
    ) -> dict[str, Any]:
        cur.execute(
            "SELECT payload FROM qmt_strategy.execution_runtime_event WHERE runtime_id=%s AND event_type='ALGO_START' "
            "AND payload->'source_identity'->>'algo_instance_id'=%s ORDER BY sequence LIMIT 2 FOR SHARE",
            (event.runtime_id, algo.algo_instance_id),
        )
        rows = cur.fetchall()
        if len(rows) != 1:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_ALGO_START_AUTHORITY_INVALID",
                "product evidence requires one exact durable ALGO_START event",
                context={"runtime_id": event.runtime_id, "algo_instance_id": algo.algo_instance_id, "count": len(rows)},
            )
        start = RuntimeEventEnvelopeV2.model_validate(rows[0]["payload"])
        payload = thaw_json_v1(start.payload)
        correlation = thaw_json_v1(start.correlation)
        required = {
            "execution_plan_id",
            "execution_plan_sha256",
            "binding_id",
            "release_id",
            "release_sha256",
        }
        if not required.issubset(payload):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_ALGO_START_AUTHORITY_INVALID",
                "ALGO_START payload lacks product plan lineage",
                context={"missing": sorted(required - set(payload)), "event_id": start.event_id},
            )
        correlation_required = {"binding_id", "exchange_trade_date", "session_epoch", "session_phase"}
        if not isinstance(correlation, dict) or any(
            type(correlation.get(name)) is not str or not correlation[name].strip() for name in correlation_required
        ):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_ALGO_START_AUTHORITY_INVALID",
                "ALGO_START correlation lacks exact exchange-session lineage",
                context={
                    "missing_or_invalid": sorted(
                        name
                        for name in correlation_required
                        if type(correlation.get(name)) is not str or not correlation.get(name, "").strip()
                    ),
                    "event_id": start.event_id,
                },
            )
        cur.execute(
            "SELECT plan_payload_json,plan_hash,package_id,binding_id,target_trade_date FROM paper_v2.execution_plan "
            "WHERE plan_id=%s FOR SHARE",
            (payload["execution_plan_id"],),
        )
        plan_row = cur.fetchone()
        cur.execute(
            "SELECT broker_account_id,strategy_name,strategy_id,binding_hash,release_id,release_hash "
            "FROM paper_v2.simulation_release_binding WHERE binding_id=%s FOR SHARE",
            (payload["binding_id"],),
        )
        binding = cur.fetchone()
        if plan_row is None or binding is None:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_PLAN_AUTHORITY_MISSING",
                "plan or binding authority is missing",
                context={"execution_plan_id": payload["execution_plan_id"], "binding_id": payload["binding_id"]},
            )
        plan_payload = plan_row["plan_payload_json"]
        intents = plan_payload.get("intents") if isinstance(plan_payload, dict) else None
        matches = [
            item for item in intents or [] if isinstance(item, dict) and item.get("intent_id") == algo.parent_intent_id
        ]
        if len(matches) != 1:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_PLAN_PARENT_INVALID",
                "plan does not contain one exact parent intent",
                context={"parent_intent_id": algo.parent_intent_id, "count": len(matches)},
            )
        intent = matches[0]
        decisions = plan_payload.get("trading_rule_decisions") if isinstance(plan_payload, dict) else None
        decision_matches = [
            item
            for item in decisions or []
            if isinstance(item, dict) and item.get("decision_id") == intent.get("trading_rule_decision_id")
        ]
        if len(decision_matches) != 1:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_TRADING_RULE_AUTHORITY_INVALID",
                "plan does not contain one exact trading-rule decision for the parent",
                context={
                    "parent_intent_id": algo.parent_intent_id,
                    "trading_rule_decision_id": intent.get("trading_rule_decision_id"),
                    "count": len(decision_matches),
                },
            )
        if (
            plan_row["plan_hash"] != payload["execution_plan_sha256"]
            or plan_row["binding_id"] != payload["binding_id"]
            or binding["release_id"] != payload["release_id"]
            or binding["release_hash"] != payload["release_sha256"]
            or correlation.get("binding_id") != payload["binding_id"]
            or str(plan_row["target_trade_date"]) != correlation.get("exchange_trade_date")
        ):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_PLAN_AUTHORITY_DRIFT",
                "plan, binding and ALGO_START lineage differ",
                context={"execution_plan_id": payload["execution_plan_id"], "binding_id": payload["binding_id"]},
            )
        account_id = binding["broker_account_id"]
        strategy_name = binding["strategy_name"] or binding["strategy_id"]
        if (
            type(account_id) is not str
            or not account_id.strip()
            or type(strategy_name) is not str
            or not strategy_name.strip()
        ):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_ACCOUNT_AUTHORITY_INVALID",
                "binding lacks broker account or strategy name",
                context={"binding_id": payload["binding_id"]},
            )
        risk_context = intent.get("risk_context") if isinstance(intent.get("risk_context"), dict) else {}
        metadata = intent.get("metadata") if isinstance(intent.get("metadata"), dict) else {}
        return {
            "execution_plan_id": payload["execution_plan_id"],
            "execution_plan_sha256": payload["execution_plan_sha256"],
            "binding_id": payload["binding_id"],
            "trade_date": date.fromisoformat(correlation["exchange_trade_date"]),
            "package_id": plan_row["package_id"],
            "strategy_id": binding["strategy_id"],
            "broker_account_id": account_id,
            "strategy_name": strategy_name,
            "target_weight": None if intent.get("target_weight") is None else Decimal(str(intent["target_weight"])),
            "request_metadata": {**metadata, **risk_context},
            "intent": intent,
            "trading_rule_decision": decision_matches[0],
        }

    @staticmethod
    def _exact_virtual_account(ledger: Any, *, account_id: str, strategy_name: str) -> Any:
        matches = [
            item for item in ledger.list_virtual_accounts(account_id=account_id) if item.strategy_name == strategy_name
        ]
        if len(matches) != 1:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_VIRTUAL_ACCOUNT_INVALID",
                "product evidence requires one exact virtual account",
                context={"account_id": account_id, "strategy_name": strategy_name, "count": len(matches)},
            )
        return matches[0]

    @staticmethod
    def _account_payload(account: Any) -> dict[str, Any]:
        try:
            return virtual_account_projection_v1(account)
        except VirtualAccountProjectionError as exc:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_VIRTUAL_ACCOUNT_INVALID",
                "product evidence virtual account failed strict projection",
                context={"account_type": type(account).__name__, "error": str(exc)},
            ) from exc

    @staticmethod
    def _command_lineage(command: BrokerCommandV2) -> tuple[str, str, str]:
        if command.command_type is BrokerCommandTypeV2.CANCEL_ORDER:
            metadata = thaw_json_v1(command.metadata)
            submit_command_id = metadata.get("submit_command_id")
            mapping_id = metadata.get("mapping_id")
            if (submit_command_id is None) == (mapping_id is None):
                raise KernelProductEvidenceError(
                    "MINIQMT_K6_PRODUCT_CANCEL_LINEAGE_INVALID",
                    "CANCEL command requires exactly one prior mapping authority",
                    context={"command_id": command.command_id},
                )
            if submit_command_id is not None:
                if type(submit_command_id) is not str or not submit_command_id.strip():
                    raise KernelProductEvidenceError(
                        "MINIQMT_K6_PRODUCT_CANCEL_LINEAGE_INVALID",
                        "CANCEL submit command identity is invalid",
                        context={"command_id": command.command_id},
                    )
                child_order_id = execution_child_order_id_v1(
                    command_id=submit_command_id,
                    local_vt_orderid=command.local_vt_orderid,
                )
                mapping_id = command_child_mapping_id_v1(
                    command_id=submit_command_id,
                    local_vt_orderid=command.local_vt_orderid,
                    child_order_id=child_order_id,
                )
            else:
                child_order_id = metadata.get("child_order_id")
                if (
                    type(mapping_id) is not str
                    or not mapping_id.strip()
                    or type(child_order_id) is not str
                    or not child_order_id.strip()
                ):
                    raise KernelProductEvidenceError(
                        "MINIQMT_K6_PRODUCT_CANCEL_LINEAGE_INVALID",
                        "CANCEL mapping/child authority is invalid",
                        context={"command_id": command.command_id},
                    )
            return (
                mapping_id,
                child_order_id,
                deterministic_client_order_ref_v1(
                    command_id=command.command_id,
                    mapping_id=mapping_id,
                ),
            )
        child_order_id = execution_child_order_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
        )
        mapping_id = command_child_mapping_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
            child_order_id=child_order_id,
        )
        return (
            mapping_id,
            child_order_id,
            deterministic_client_order_ref_v1(
                command_id=command.command_id,
                mapping_id=mapping_id,
            ),
        )

    @staticmethod
    def _validate_cancel_owner(cur: Any, *, command: BrokerCommandV2, mapping_id: str) -> None:
        cur.execute(
            "SELECT mapping_json FROM qmt_strategy.execution_child_order WHERE mapping_id=%s FOR SHARE",
            (mapping_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CANCEL_OWNER_MISSING",
                "CANCEL command has no exact durable mapping owner",
                context={"command_id": command.command_id, "mapping_id": mapping_id},
            )
        try:
            raw = row["mapping_json"] if isinstance(row, dict) else row[0]
            mapping = ExecutionCommandChildMappingV1.model_validate(raw, strict=True)
        except (TypeError, ValueError) as exc:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CANCEL_OWNER_INVALID",
                "CANCEL durable mapping failed strict readback",
                context={
                    "command_id": command.command_id,
                    "mapping_id": mapping_id,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
            ) from exc
        if (
            mapping.mapping_id != mapping_id
            or mapping.runtime_id != command.runtime_id
            or mapping.algo_instance_id != command.algo_instance_id
            or mapping.parent_intent_id != command.parent_intent_id
            or mapping.symbol != command.symbol
            or mapping.side is not command.side
            or mapping.local_vt_orderid != command.local_vt_orderid
            or mapping.broker_order_id != command.owned_broker_order_id
            or mapping.mapping_status
            not in {
                CommandChildMappingStatusV1.BROKER_ACCEPTED,
                CommandChildMappingStatusV1.OUTCOME_UNKNOWN,
            }
        ):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_CANCEL_OWNER_CONFLICT",
                "CANCEL command differs from its exact durable mapping owner",
                context={
                    "command_id": command.command_id,
                    "mapping_id": mapping_id,
                    "mapping_status": mapping.mapping_status.value,
                    "expected_broker_order_id": command.owned_broker_order_id,
                    "actual_broker_order_id": mapping.broker_order_id,
                },
            )

    @staticmethod
    def _fact_payloads(
        cur: Any,
        *,
        command: BrokerCommandV2,
        account: Any,
        account_payload: dict[str, Any],
        preflight: Any | None,
        child_order_id: str,
        client_ref: str,
        event_id: str,
        strategy_slot_id: str,
    ) -> dict[str, Any]:
        cur.execute(
            "SELECT lot_id,open_date,quantity,available_quantity,remaining_quantity,avg_cost,status,metadata "
            "FROM qmt_strategy.position_lot WHERE strategy_id=%s AND symbol=%s ORDER BY open_date,lot_id FOR SHARE",
            (account.strategy_id, command.symbol),
        )
        lots = [
            {
                "lot_id": row["lot_id"],
                "open_date": row["open_date"].isoformat(),
                "quantity": int(row["quantity"]),
                "available_quantity": int(row["available_quantity"]),
                "remaining_quantity": int(row["remaining_quantity"]),
                "avg_cost": str(row["avg_cost"]),
                "status": row["status"],
                "metadata": row["metadata"],
            }
            for row in cur.fetchall()
        ]
        cur.execute(
            "SELECT intent_id,quantity,limit_price,submit_status,trade_date,order_remark FROM qmt_strategy.order_intent "
            "WHERE strategy_id=%s AND symbol=%s AND side='SELL' "
            "AND submit_status IN ('CREATED','SUBMITTED','ACCEPTED') ORDER BY created_at,intent_id FOR SHARE",
            (account.strategy_id, command.symbol),
        )
        open_sells = [
            {
                "intent_id": row["intent_id"],
                "quantity": int(row["quantity"]),
                "limit_price": None if row["limit_price"] is None else str(row["limit_price"]),
                "submit_status": row["submit_status"],
                "trade_date": row["trade_date"].isoformat(),
                "order_remark": row["order_remark"],
            }
            for row in cur.fetchall()
        ]
        account_hash = hash_hex_v1("miniqmt_account_projection_v1", account_payload)
        cash_fact = {
            "strategy_id": account.strategy_id,
            "cash": str(account.cash),
            "frozen_cash": str(account.frozen_cash),
            "updated_at_utc": account.updated_at.isoformat(),
        }
        lot_fact = {"strategy_id": account.strategy_id, "symbol": command.symbol, "ordered_lots": lots}
        open_fact = {
            "strategy_id": account.strategy_id,
            "symbol": command.symbol,
            "order_remark": client_ref,
            "ordered_open_sell_intents": open_sells,
        }
        errors = () if preflight is None else tuple(preflight.errors)
        error_codes = tuple(error.code for error in errors)
        oms = OMSPreflightProjectionReceiptV1.create(
            runtime_id=command.runtime_id,
            algo_instance_id=command.algo_instance_id,
            parent_intent_id=command.parent_intent_id,
            child_order_id=child_order_id,
            order_intent_id="mqpreflightintent_"
            + hash_hex_v1(
                "miniqmt_product_preflight_intent_v1", {"command_id": command.command_id, "client_ref": client_ref}
            )[:32],
            strategy_slot_id=strategy_slot_id,
            account_projection_sha256=account_hash,
            cash_fact_sha256=hash_hex_v1("miniqmt_product_cash_fact_v1", cash_fact),
            lot_fact_sha256=hash_hex_v1("miniqmt_product_lot_fact_v1", lot_fact),
            open_order_fact_sha256=hash_hex_v1("miniqmt_product_open_order_fact_v1", open_fact),
            decision=OMSPreflightDecisionV1.PASS if not errors else OMSPreflightDecisionV1.REJECT,
            reason_code="MINIQMT_OMS_PREFLIGHT_PASS" if not errors else errors[0].code,
            logical_at_utc=account.updated_at,
        )
        kill_active = "PRE_TRADE_KILL_SWITCH_ACTIVE" in error_codes
        risk_errors = tuple(error.to_dict() for error in errors if error.code.startswith("PRE_TRADE_"))
        risk = MiniQMTRiskDecisionReceiptV1.create(
            runtime_id=command.runtime_id,
            algo_instance_id=command.algo_instance_id,
            event_id=event_id,
            child_order_id=child_order_id,
            decision_stage=RiskDecisionStageV1.PRE_SUBMIT,
            action=RiskDecisionActionV1.KILL_SWITCH if kill_active else RiskDecisionActionV1.PASS,
            reason_code="PRE_TRADE_KILL_SWITCH_ACTIVE" if kill_active else "MINIQMT_PRE_TRADE_RISK_PASS",
            reason="durable pre-trade risk configuration evaluated by QmtManagedOrderService.preview_order",
            metadata={
                "account_projection_sha256": account_hash,
                "ordered_risk_errors": list(risk_errors),
                "preflight_error_codes": list(error_codes),
            },
            logical_at_utc=account.updated_at,
        )
        return {
            "preflight": preflight,
            "oms": oms,
            "risk": risk,
            "kill_switch": {
                "active": kill_active,
                "source": "qmt_strategy.virtual_account.risk_config",
                "account_projection_sha256": account_hash,
                "risk_config_sha256": hash_hex_v1("miniqmt_product_risk_config_v1", account.risk_config),
            },
        }

    @staticmethod
    def _projection_refs(
        *,
        event: RuntimeEventEnvelopeV2,
        delivery: AlgoDeliveryPersistenceV1,
        base_services: AlgoReadOnlyServicesV1,
        route_receipt: PluginRouteCompatibilityReceiptV1,
        account_payload: dict[str, Any],
        evidence_parts: list[dict[str, Any]],
    ) -> tuple[ExecutionProjectionRefV1, ...]:
        if not evidence_parts:
            return base_services.execution_projection_set.ordered_projection_refs
        first = evidence_parts[0]
        if any(part["kill_switch"] != first["kill_switch"] for part in evidence_parts[1:]):
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_SHARED_PROJECTION_AMBIGUOUS",
                "one transition command set requires different kill-switch authority",
                context={"delivery_id": delivery.delivery_id, "command_count": len(evidence_parts)},
            )
        refs = {item.projection_type: item for item in base_services.execution_projection_set.ordered_projection_refs}
        if base_services.market_data_projection is None:
            refs.pop(KernelProjectionTypeV1.MARKET_DATA, None)
        account_hash = hash_hex_v1("miniqmt_account_projection_v1", account_payload)
        refs[KernelProjectionTypeV1.ACCOUNT] = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.ACCOUNT,
            projection_id=f"mqaccount_{account_hash[:32]}",
            projection_version="miniqmt_account_projection_v1",
            payload_sha256=account_hash,
            source_event_id=event.event_id,
            logical_at_utc=event.event_time_utc,
        )
        kill_hash = hash_hex_v1("miniqmt_kill_switch_state_v1", first["kill_switch"])
        refs[KernelProjectionTypeV1.KILL_SWITCH_STATE] = ExecutionProjectionRefV1.create(
            projection_type=KernelProjectionTypeV1.KILL_SWITCH_STATE,
            projection_id=f"mqkillswitch_{kill_hash[:32]}",
            projection_version="miniqmt_kill_switch_state_v1",
            payload_sha256=kill_hash,
            source_event_id=event.event_id,
            logical_at_utc=event.event_time_utc,
        )
        oms_aggregate_hash = hash_hex_v1(
            "miniqmt_product_oms_preflight_projection_set_v1",
            [part["oms"].receipt_sha256 for part in evidence_parts],
        )
        risk_aggregate_hash = hash_hex_v1(
            "miniqmt_product_risk_decision_projection_set_v1",
            [part["risk"].receipt_sha256 for part in evidence_parts],
        )
        for projection_type, identity, version, payload_hash in (
            (
                KernelProjectionTypeV1.OMS_PREFLIGHT,
                "mqomspreflight_" + oms_aggregate_hash,
                first["oms"].schema_version,
                oms_aggregate_hash,
            ),
            (
                KernelProjectionTypeV1.RISK_DECISION,
                "mqriskdecision_" + risk_aggregate_hash,
                first["risk"].schema_version,
                risk_aggregate_hash,
            ),
            (
                KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
                "mqroutecompat_" + route_receipt.receipt_sha256,
                route_receipt.schema_version,
                route_receipt.receipt_sha256,
            ),
        ):
            refs[projection_type] = ExecutionProjectionRefV1.create(
                projection_type=projection_type,
                projection_id=identity,
                projection_version=version,
                payload_sha256=payload_hash,
                source_event_id=event.event_id,
                logical_at_utc=event.event_time_utc,
            )
        required = {
            KernelProjectionTypeV1.CONTRACT,
            KernelProjectionTypeV1.MARKET_CAPABILITY,
            KernelProjectionTypeV1.ACCOUNT,
            KernelProjectionTypeV1.KILL_SWITCH_STATE,
            KernelProjectionTypeV1.OMS_PREFLIGHT,
            KernelProjectionTypeV1.RISK_DECISION,
            KernelProjectionTypeV1.ROUTE_COMPATIBILITY,
        }
        if base_services.market_data_projection is not None:
            required.add(KernelProjectionTypeV1.MARKET_DATA)
        if set(refs) != required:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_PROJECTION_SET_INVALID",
                "product evidence projection set is not the exact required authority set",
                context={
                    "missing": sorted(item.value for item in required - set(refs)),
                    "extra": sorted(item.value for item in set(refs) - required),
                },
            )
        return tuple(sorted(refs.values(), key=lambda item: (item.projection_type.value, item.projection_id)))

    @staticmethod
    def _dependent_buy_candidate(
        cur: Any,
        *,
        command: BrokerCommandV2,
        oms: OMSPreflightProjectionReceiptV1,
        preflight: Any | None,
        plan: dict[str, Any],
        account: Any,
        event: RuntimeEventEnvelopeV2,
    ) -> DependentBuyCandidateAuthorityV2 | None:
        if preflight is None:
            return None
        dependent_codes = tuple(sorted({item.code for item in preflight.errors} & _DEPENDENT_BUY_CODES))
        if not dependent_codes:
            return None
        cur.execute(
            "SELECT parent.parent_intent_id,algo.algo_instance_id FROM qmt_strategy.execution_parent_benchmark parent "
            "JOIN qmt_strategy.execution_algo_instance algo ON algo.runtime_id=parent.runtime_id "
            "AND algo.parent_intent_id=parent.parent_intent_id "
            "WHERE parent.runtime_id=%s AND parent.execution_plan_id=%s AND parent.side='SELL' "
            "AND algo.kernel_contract_version='KERNEL_V2' ORDER BY parent.parent_intent_id FOR SHARE OF parent,algo",
            (command.runtime_id, plan["execution_plan_id"]),
        )
        rows = cur.fetchall()
        dependencies = tuple(
            DependentBuySellDependencyV2.create(
                runtime_id=command.runtime_id,
                strategy_id=plan["strategy_id"],
                sell_parent_intent_id=row["parent_intent_id"],
                sell_algo_instance_id=row["algo_instance_id"],
                latest_order_fact_id=None,
                latest_order_fact_sha256=None,
                ordered_settled_proceeds_refs=(),
                dependency_status=DependentBuyDependencyStatusV1.OPEN,
            )
            for row in rows
        )
        if not dependencies:
            raise KernelProductEvidenceError(
                "MINIQMT_K6_PRODUCT_DEPENDENT_BUY_SELL_OWNER_MISSING",
                "dependent BUY preflight has no durable SELL parent dependency",
                context={"runtime_id": command.runtime_id, "parent_intent_id": command.parent_intent_id},
            )
        session_payload = {
            "trade_date": plan["trade_date"].isoformat(),
            "source_identity": thaw_json_v1(event.source_identity),
            "correlation": thaw_json_v1(event.correlation),
        }
        return DependentBuyCandidateAuthorityV2.create(
            runtime_id=command.runtime_id,
            binding_id=plan["binding_id"],
            trade_date=plan["trade_date"],
            strategy_id=plan["strategy_id"],
            buy_algo_instance_id=command.algo_instance_id,
            buy_parent_intent_id=command.parent_intent_id,
            command_id=command.command_id,
            execution_plan_id=plan["execution_plan_id"],
            execution_plan_sha256=plan["execution_plan_sha256"],
            plan_parent_relation_sha256=hash_hex_v1(
                "miniqmt_dependent_buy_plan_parent_relation_v2",
                {
                    "buy_parent_intent_id": command.parent_intent_id,
                    "ordered_sell_parent_intent_ids": [item.sell_parent_intent_id for item in dependencies],
                },
            ),
            required_cash=str(preflight.freeze_amount),
            virtual_account_id=account.strategy_id,
            session_authority_sha256=hash_hex_v1("miniqmt_dependent_buy_session_authority_v2", session_payload),
            ordered_sell_dependencies=dependencies,
            oms_preflight_receipt_id=oms.receipt_id,
            oms_preflight_receipt_sha256=oms.receipt_sha256,
            ordered_error_codes=dependent_codes,
        )


__all__ = [
    "ProductBoundTransitionV3",
    "bind_product_transition_bundle_v3",
    "KernelProductEvidenceError",
    "KernelProductEvidenceProviderV3",
    "ProductEvidenceBuildResultV3",
]
