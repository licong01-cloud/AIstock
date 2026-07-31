"""Strict K6 product-cutover contracts with no runtime or persistence side effects."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Literal, Self

from pydantic import StrictBool, model_validator

from .plugin_canonical import (
    canonical_utc_datetime_v1,
    hash_hex_v1 as _raw_hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from .plugin_contracts import (
    CanonicalDecimalV1,
    FrozenStrictModel,
    IdentityV1,
    NonNegativeIntV1,
    PositiveIntV1,
    Sha256V1,
    UtcDateTimeV1,
)


MAX_DEPENDENT_BUY_DEPENDENCIES = 256
MAX_PRODUCT_COMMANDS = 256


def _canonical_hash_input_v1(value: Any) -> Any:
    if isinstance(value, FrozenStrictModel):
        return value.model_dump(mode="json")
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, datetime):
        return canonical_utc_datetime_v1(value, field_name="hash_datetime")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple):
        return [_canonical_hash_input_v1(item) for item in value]
    if isinstance(value, list):
        return [_canonical_hash_input_v1(item) for item in value]
    if isinstance(value, Mapping):
        return {key: _canonical_hash_input_v1(member) for key, member in value.items()}
    try:
        return thaw_json_v1(value)
    except TypeError:
        return value


def hash_hex_v1(domain: str, payload: Any) -> str:
    return _raw_hash_hex_v1(domain, _canonical_hash_input_v1(payload))


def _canonical_decimal_text_v1(value: Decimal) -> str:
    rendered = format(value, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return "0" if rendered in {"", "-0"} else rendered


class KernelProductContractError(ValueError):
    """Typed K6 contract failure with bounded JSON-safe evidence."""

    def __init__(self, reason_code: str, message: str, *, context: dict[str, Any]) -> None:
        self.reason_code = reason_code
        self.context = json_safe_evidence_v1(context)
        super().__init__(message)


class DependentBuyDependencyStatusV1(StrEnum):
    OPEN = "OPEN"
    PROCEEDS_SETTLED = "PROCEEDS_SETTLED"
    TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS = "TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS"


class DependentBuyTriggerTypeV1(StrEnum):
    SELL_TRADE_SETTLED = "SELL_TRADE_SETTLED"
    SELL_ORDER_TERMINAL = "SELL_ORDER_TERMINAL"
    ACCOUNT_REFRESHED = "ACCOUNT_REFRESHED"
    SESSION_EOD = "SESSION_EOD"


class DependentBuyDecisionV1(StrEnum):
    WAIT = "WAIT"
    RELEASE_TO_K2_OUTBOX = "RELEASE_TO_K2_OUTBOX"
    BLOCK = "BLOCK"
    EOD_RESIDUAL = "EOD_RESIDUAL"


class DependentBuyCoordinationStatusV1(StrEnum):
    DEFERRED_WAITING_SELL_PROCEEDS = "DEFERRED_WAITING_SELL_PROCEEDS"
    RELEASED_TO_K2_OUTBOX = "RELEASED_TO_K2_OUTBOX"
    BLOCKED_SELL_PROCEEDS_UNAVAILABLE = "BLOCKED_SELL_PROCEEDS_UNAVAILABLE"
    EOD_RESIDUAL = "EOD_RESIDUAL"


class ProductCommandDispositionV2(StrEnum):
    MATERIALIZE = "MATERIALIZE"
    REJECT_SYNCHRONOUS = "REJECT_SYNCHRONOUS"


class ProductCommandAggregateDispositionV2(StrEnum):
    ZERO_COMMAND = "ZERO_COMMAND"
    ALL_REJECTED = "ALL_REJECTED"
    MATERIALIZE_ALL_ACCEPTED_COMMANDS = "MATERIALIZE_ALL_ACCEPTED_COMMANDS"
    MIXED_PER_COMMAND = "MIXED_PER_COMMAND"


class ProductLifecycleStatusV2(StrEnum):
    SYNCHRONOUS_REJECTED = "SYNCHRONOUS_REJECTED"
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    DISPATCHING = "DISPATCHING"
    ACKED = "ACKED"
    ACKED_REJECTED = "ACKED_REJECTED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    RECONCILING = "RECONCILING"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class ProductRouteOwnerKindV1(StrEnum):
    LEGACY_DRAIN_ONLY = "LEGACY_DRAIN_ONLY"
    KERNEL_V2 = "KERNEL_V2"


class ProductMaterializationCommitOutcomeV2(StrEnum):
    COMMITTED_READBACK_VERIFIED = "COMMITTED_READBACK_VERIFIED"


def _strict_trade_date(value: date | str) -> date:
    if isinstance(value, date) and not isinstance(value, bool):
        return value
    if type(value) is not str:
        raise TypeError("trade_date must be an ISO date")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("trade_date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ValueError("trade_date must use canonical YYYY-MM-DD")
    return parsed


def _ordered_unique_sha(values: tuple[str, ...], *, field_name: str, maximum: int | None = None) -> None:
    if maximum is not None and len(values) > maximum:
        raise ValueError(f"{field_name} exceeds maximum cardinality {maximum}")
    if values != tuple(sorted(values)):
        raise ValueError(f"{field_name} must be stable sorted")
    if any(count != 1 for count in Counter(values).values()):
        raise ValueError(f"{field_name} must be unique")


class DependentBuySellDependencyV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_sell_dependency_v1"]
    runtime_id: IdentityV1
    strategy_id: IdentityV1
    sell_parent_intent_id: IdentityV1
    sell_algo_instance_id: IdentityV1
    required_terminal_policy: Literal["TRADE_SETTLED_OR_ORDER_TERMINAL"]
    latest_order_fact_ref: Sha256V1 | None
    settled_trade_fact_refs: tuple[Sha256V1, ...]
    settled_cash_ledger_refs: tuple[Sha256V1, ...]
    dependency_status: DependentBuyDependencyStatusV1
    dependency_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        runtime_id: str,
        strategy_id: str,
        sell_parent_intent_id: str,
        sell_algo_instance_id: str,
        latest_order_fact_ref: str | None,
        settled_trade_fact_refs: tuple[str, ...],
        settled_cash_ledger_refs: tuple[str, ...],
        dependency_status: DependentBuyDependencyStatusV1,
    ) -> Self:
        payload = {
            "schema_version": "miniqmt_dependent_buy_sell_dependency_v1",
            "runtime_id": runtime_id,
            "strategy_id": strategy_id,
            "sell_parent_intent_id": sell_parent_intent_id,
            "sell_algo_instance_id": sell_algo_instance_id,
            "required_terminal_policy": "TRADE_SETTLED_OR_ORDER_TERMINAL",
            "latest_order_fact_ref": latest_order_fact_ref,
            "settled_trade_fact_refs": settled_trade_fact_refs,
            "settled_cash_ledger_refs": settled_cash_ledger_refs,
            "dependency_status": dependency_status,
        }
        return cls(**payload, dependency_sha256=hash_hex_v1("miniqmt_dependent_buy_sell_dependency_v1", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        _ordered_unique_sha(self.settled_trade_fact_refs, field_name="settled_trade_fact_refs")
        _ordered_unique_sha(self.settled_cash_ledger_refs, field_name="settled_cash_ledger_refs")
        if len(self.settled_trade_fact_refs) != len(self.settled_cash_ledger_refs):
            raise ValueError("settled trade and cash ledger references must close one-to-one")
        if (
            self.dependency_status is DependentBuyDependencyStatusV1.PROCEEDS_SETTLED
            and not self.settled_trade_fact_refs
        ):
            raise ValueError("settled dependency requires trade and cash ledger facts")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_sell_dependency_v1",
            self.canonical_payload_v1(exclude={"dependency_sha256"}),
        )
        if self.dependency_sha256 != expected:
            raise ValueError("dependent-BUY dependency hash mismatch")
        return self


class DependentBuyTriggerEventRefV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_trigger_event_ref_v1"]
    runtime_id: IdentityV1
    event_id: IdentityV1
    event_type: DependentBuyTriggerTypeV1
    event_sequence: PositiveIntV1
    source_fact_type: IdentityV1
    source_fact_id: IdentityV1
    source_fact_sha256: Sha256V1
    observed_at_utc: UtcDateTimeV1
    trigger_ref_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {"schema_version": "miniqmt_dependent_buy_trigger_event_ref_v1", **values}
        return cls(**payload, trigger_ref_sha256=hash_hex_v1("miniqmt_dependent_buy_trigger_event_ref_v1", payload))

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_trigger_event_ref_v1",
            self.canonical_payload_v1(exclude={"trigger_ref_sha256"}),
        )
        if self.trigger_ref_sha256 != expected:
            raise ValueError("dependent-BUY trigger reference hash mismatch")
        return self

    def sort_key_v1(self) -> tuple[int, str]:
        return (self.event_sequence, self.event_id)


class DependentBuyLedgerObservationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_ledger_observation_v1"]
    runtime_id: IdentityV1
    strategy_id: IdentityV1
    trade_date: date
    ledger_authority_source: Literal["qmt_strategy_ledger.virtual_account.cash"]
    virtual_account_id: IdentityV1
    ledger_row_version: PositiveIntV1
    ledger_as_of_utc: UtcDateTimeV1
    available_cash: CanonicalDecimalV1
    required_cash: CanonicalDecimalV1
    cash_shortfall: CanonicalDecimalV1
    ordered_settled_trade_refs: tuple[Sha256V1, ...]
    ordered_cash_ledger_refs: tuple[Sha256V1, ...]
    freshness_session_authority_sha256: Sha256V1
    observation_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        available = Decimal(str(values["available_cash"]))
        required = Decimal(str(values["required_cash"]))
        shortfall = max(required - available, Decimal("0"))
        payload = {
            "schema_version": "miniqmt_dependent_buy_ledger_observation_v1",
            **values,
            "trade_date": _strict_trade_date(values["trade_date"]),
            "ledger_authority_source": "qmt_strategy_ledger.virtual_account.cash",
            "cash_shortfall": _canonical_decimal_text_v1(shortfall),
        }
        return cls(**payload, observation_sha256=hash_hex_v1("miniqmt_dependent_buy_ledger_observation_v1", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        _ordered_unique_sha(self.ordered_settled_trade_refs, field_name="ordered_settled_trade_refs")
        _ordered_unique_sha(self.ordered_cash_ledger_refs, field_name="ordered_cash_ledger_refs")
        if len(self.ordered_settled_trade_refs) != len(self.ordered_cash_ledger_refs):
            raise ValueError("ledger trade and cash references must close one-to-one")
        expected_shortfall = max(Decimal(self.required_cash) - Decimal(self.available_cash), Decimal("0"))
        if Decimal(self.cash_shortfall) != expected_shortfall:
            raise ValueError("cash_shortfall does not equal max(required_cash-available_cash,0)")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_ledger_observation_v1",
            self.canonical_payload_v1(exclude={"observation_sha256"}),
        )
        if self.observation_sha256 != expected:
            raise ValueError("dependent-BUY ledger observation hash mismatch")
        return self


class DependentBuyReleaseDecisionV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_release_decision_v1"]
    decision_id: Sha256V1
    coordination_id: Sha256V1
    decision_sequence: PositiveIntV1
    previous_decision_sha256: Sha256V1 | None
    trigger_ref_sha256: Sha256V1
    decision: DependentBuyDecisionV1
    reason_code: IdentityV1
    ledger_observation_sha256: Sha256V1
    ordered_dependency_sha256s: tuple[Sha256V1, ...]
    release_event_id: IdentityV1 | None
    release_transition_id: IdentityV1 | None
    release_command_authority_set_sha256: Sha256V1 | None
    decided_at_utc: UtcDateTimeV1
    worker_id: IdentityV1
    process_incarnation_id: IdentityV1
    lease_epoch: PositiveIntV1
    decision_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        decision_id = hash_hex_v1(
            "miniqmt_dependent_buy_release_decision_id_v1",
            {
                "coordination_id": values["coordination_id"],
                "decision_sequence": values["decision_sequence"],
                "trigger_ref_sha256": values["trigger_ref_sha256"],
            },
        )
        payload = {
            "schema_version": "miniqmt_dependent_buy_release_decision_v1",
            "decision_id": decision_id,
            **values,
            "release_event_id": values.get("release_event_id"),
            "release_transition_id": values.get("release_transition_id"),
            "release_command_authority_set_sha256": values.get("release_command_authority_set_sha256"),
        }
        return cls(**payload, decision_sha256=hash_hex_v1("miniqmt_dependent_buy_release_decision_v1", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        _ordered_unique_sha(self.ordered_dependency_sha256s, field_name="ordered_dependency_sha256s")
        if not self.ordered_dependency_sha256s:
            raise ValueError("decision requires at least one dependency")
        release_values = (
            self.release_event_id,
            self.release_transition_id,
            self.release_command_authority_set_sha256,
        )
        if self.decision is DependentBuyDecisionV1.RELEASE_TO_K2_OUTBOX:
            if any(value is None for value in release_values):
                raise ValueError("release decision lacks K2 event, transition or command authority")
        elif any(value is not None for value in release_values):
            raise ValueError("non-release decision cannot carry release identities")
        if self.decision_sequence == 1 and self.previous_decision_sha256 is not None:
            raise ValueError("first decision cannot carry predecessor")
        if self.decision_sequence > 1 and self.previous_decision_sha256 is None:
            raise ValueError("successor decision requires predecessor")
        expected_id = hash_hex_v1(
            "miniqmt_dependent_buy_release_decision_id_v1",
            {
                "coordination_id": self.coordination_id,
                "decision_sequence": self.decision_sequence,
                "trigger_ref_sha256": self.trigger_ref_sha256,
            },
        )
        if self.decision_id != expected_id:
            raise ValueError("dependent-BUY decision identity mismatch")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_release_decision_v1",
            self.canonical_payload_v1(exclude={"decision_sha256"}),
        )
        if self.decision_sha256 != expected:
            raise ValueError("dependent-BUY decision hash mismatch")
        return self


class DependentBuyCoordinationV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_coordination_v1"]
    coordination_id: Sha256V1
    runtime_id: IdentityV1
    binding_id: IdentityV1
    trade_date: date
    strategy_id: IdentityV1
    buy_algo_instance_id: IdentityV1
    buy_parent_intent_id: IdentityV1
    required_cash: CanonicalDecimalV1
    release_command_payload_sha256: Sha256V1
    ordered_sell_dependencies: tuple[DependentBuySellDependencyV1, ...]
    status: DependentBuyCoordinationStatusV1
    decision_sequence: NonNegativeIntV1
    last_decision_sha256: Sha256V1 | None
    released_command_id: IdentityV1 | None
    released_outbox_id: IdentityV1 | None
    row_version: PositiveIntV1
    lease_worker_id: IdentityV1 | None
    lease_process_incarnation_id: IdentityV1 | None
    lease_epoch: NonNegativeIntV1
    lease_expires_at_utc: UtcDateTimeV1 | None
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    coordination_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        trade_date = _strict_trade_date(values["trade_date"])
        coordination_id = hash_hex_v1(
            "miniqmt_dependent_buy_coordination_id_v1",
            {
                "runtime_id": values["runtime_id"],
                "buy_algo_instance_id": values["buy_algo_instance_id"],
                "buy_parent_intent_id": values["buy_parent_intent_id"],
                "strategy_id": values["strategy_id"],
                "trade_date": trade_date.isoformat(),
            },
        )
        payload = {
            "schema_version": "miniqmt_dependent_buy_coordination_v1",
            "coordination_id": coordination_id,
            **values,
            "trade_date": trade_date,
            "lease_worker_id": values.get("lease_worker_id"),
            "lease_process_incarnation_id": values.get("lease_process_incarnation_id"),
            "lease_epoch": values.get("lease_epoch", 0),
            "lease_expires_at_utc": values.get("lease_expires_at_utc"),
        }
        hash_payload = {key: value for key, value in payload.items() if key not in _COORDINATION_HASH_EXCLUDES}
        return cls(**payload, coordination_sha256=hash_hex_v1("miniqmt_dependent_buy_coordination_v1", hash_payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if not 1 <= len(self.ordered_sell_dependencies) <= MAX_DEPENDENT_BUY_DEPENDENCIES:
            raise ValueError("ordered_sell_dependencies cardinality must be in [1,256]")
        keys = tuple(item.sell_parent_intent_id for item in self.ordered_sell_dependencies)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("ordered_sell_dependencies must be sorted and unique by sell parent")
        for dependency in self.ordered_sell_dependencies:
            if dependency.runtime_id != self.runtime_id or dependency.strategy_id != self.strategy_id:
                raise ValueError("sell dependency owner differs from coordination")
        released = self.status is DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
        if released and (self.released_command_id is None or self.released_outbox_id is None):
            raise ValueError("released coordination requires command and outbox identities")
        if released and self.released_command_id != self.released_outbox_id:
            raise ValueError("released command and K2 outbox identity must be identical")
        if not released and (self.released_command_id is not None or self.released_outbox_id is not None):
            raise ValueError("non-released coordination cannot carry released identities")
        if self.decision_sequence == 0 and self.last_decision_sha256 is not None:
            raise ValueError("coordination without decisions cannot carry last decision hash")
        if self.decision_sequence > 0 and self.last_decision_sha256 is None:
            raise ValueError("coordination decision sequence requires last decision hash")
        lease_values = (self.lease_worker_id, self.lease_process_incarnation_id, self.lease_expires_at_utc)
        if self.lease_epoch == 0 and any(value is not None for value in lease_values):
            raise ValueError("unleased coordination cannot carry lease fields")
        if self.lease_epoch > 0 and any(value is None for value in lease_values):
            raise ValueError("leased coordination requires complete lease owner and expiry")
        hash_payload = {
            key: value
            for key, value in self.canonical_payload_v1(exclude={"coordination_sha256"}).items()
            if key not in _COORDINATION_HASH_EXCLUDES
        }
        expected = hash_hex_v1("miniqmt_dependent_buy_coordination_v1", hash_payload)
        if self.coordination_sha256 != expected:
            raise ValueError("dependent-BUY coordination hash mismatch")
        return self

    def validate_initial_v1(self) -> None:
        if (
            self.status is not DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS
            or self.decision_sequence != 0
            or self.row_version != 1
            or self.last_decision_sha256 is not None
            or self.released_command_id is not None
            or self.lease_epoch != 0
        ):
            raise ValueError("dependent-BUY first write requires exact waiting initial state")

    def validate_successor_v1(self, previous: Self) -> None:
        immutable = (
            "coordination_id",
            "runtime_id",
            "binding_id",
            "trade_date",
            "strategy_id",
            "buy_algo_instance_id",
            "buy_parent_intent_id",
            "required_cash",
            "release_command_payload_sha256",
            "ordered_sell_dependencies",
            "created_at_utc",
        )
        if any(getattr(self, field) != getattr(previous, field) for field in immutable):
            raise ValueError("dependent-BUY successor changes immutable owner or payload")
        if self.row_version != previous.row_version + 1:
            raise ValueError("dependent-BUY successor row_version must increase by one")
        if self.decision_sequence < previous.decision_sequence:
            raise ValueError("dependent-BUY decision sequence cannot decrease")
        terminal = {
            DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
            DependentBuyCoordinationStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
            DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
        }
        if previous.status in terminal and self != previous:
            raise ValueError("dependent-BUY terminal coordination cannot reopen")


_COORDINATION_HASH_EXCLUDES = {
    "row_version",
    "lease_worker_id",
    "lease_process_incarnation_id",
    "lease_epoch",
    "lease_expires_at_utc",
}


class ProductCommandAuthorityItemV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_authority_item_v2"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    effect_ordinal: NonNegativeIntV1
    command_id: IdentityV1
    command_type: IdentityV1
    command_payload_sha256: Sha256V1
    plugin_effect_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    oms_preflight_receipt_sha256: Sha256V1
    risk_decision_receipt_sha256: Sha256V1
    route_compatibility_receipt_sha256: Sha256V1
    market_data_projection_sha256: Sha256V1
    account_projection_sha256: Sha256V1
    contract_projection_sha256: Sha256V1
    disposition: ProductCommandDispositionV2
    reject_reason_code: IdentityV1 | None
    reject_context_sha256: Sha256V1 | None
    mapping_id: IdentityV1 | None
    outbox_id: IdentityV1 | None
    child_order_id: IdentityV1 | None
    item_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_authority_item_v2",
            **values,
            "reject_reason_code": values.get("reject_reason_code"),
            "reject_context_sha256": values.get("reject_context_sha256"),
            "mapping_id": values.get("mapping_id"),
            "outbox_id": values.get("outbox_id"),
            "child_order_id": values.get("child_order_id"),
        }
        return cls(**payload, item_sha256=hash_hex_v1("miniqmt_product_command_authority_item_v2", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        reject = (self.reject_reason_code, self.reject_context_sha256)
        materialize = (self.mapping_id, self.outbox_id, self.child_order_id)
        if self.disposition is ProductCommandDispositionV2.MATERIALIZE:
            if any(value is not None for value in reject) or any(value is None for value in materialize):
                raise ValueError("materialize item requires mapping/outbox/child and forbids reject facts")
            if self.outbox_id != self.command_id:
                raise ValueError("materialized outbox identity must equal command identity")
        elif any(value is None for value in reject) or any(value is not None for value in materialize):
            raise ValueError("reject item requires reason/context and forbids materialize identities")
        expected = hash_hex_v1(
            "miniqmt_product_command_authority_item_v2",
            self.canonical_payload_v1(exclude={"item_sha256"}),
        )
        if self.item_sha256 != expected:
            raise ValueError("product command authority item hash mismatch")
        return self


class ProductCommandAuthoritySetV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_authority_set_v2"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    catalog_sha256: Sha256V1
    creation_binding_sha256: Sha256V1
    facade_conformance_set_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    transition_receipt_sha256: Sha256V1
    ordered_items: tuple[ProductCommandAuthorityItemV2, ...]
    materialize_count: NonNegativeIntV1
    reject_count: NonNegativeIntV1
    total_count: NonNegativeIntV1
    aggregate_disposition: ProductCommandAggregateDispositionV2
    authority_set_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        items = tuple(values["ordered_items"])
        materialize_count = sum(item.disposition is ProductCommandDispositionV2.MATERIALIZE for item in items)
        reject_count = len(items) - materialize_count
        if not items:
            aggregate = ProductCommandAggregateDispositionV2.ZERO_COMMAND
        elif materialize_count == len(items):
            aggregate = ProductCommandAggregateDispositionV2.MATERIALIZE_ALL_ACCEPTED_COMMANDS
        elif reject_count == len(items):
            aggregate = ProductCommandAggregateDispositionV2.ALL_REJECTED
        else:
            aggregate = ProductCommandAggregateDispositionV2.MIXED_PER_COMMAND
        payload = {
            "schema_version": "miniqmt_product_command_authority_set_v2",
            **values,
            "ordered_items": items,
            "materialize_count": materialize_count,
            "reject_count": reject_count,
            "total_count": len(items),
            "aggregate_disposition": aggregate,
        }
        return cls(**payload, authority_set_sha256=hash_hex_v1("miniqmt_product_command_authority_set_v2", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if len(self.ordered_items) > MAX_PRODUCT_COMMANDS:
            raise ValueError("ordered product commands exceed maximum cardinality 256")
        keys = tuple((item.effect_ordinal, item.command_id) for item in self.ordered_items)
        if keys != tuple(sorted(keys)) or len({item.command_id for item in self.ordered_items}) != len(
            self.ordered_items
        ):
            raise ValueError("ordered product command items must be sorted and unique")
        if tuple(item.effect_ordinal for item in self.ordered_items) != tuple(range(len(self.ordered_items))):
            raise ValueError("ordered product command ordinals must be contiguous from zero")
        owner = (self.runtime_id, self.algo_instance_id, self.event_id, self.delivery_id, self.transition_id)
        for item in self.ordered_items:
            if (item.runtime_id, item.algo_instance_id, item.event_id, item.delivery_id, item.transition_id) != owner:
                raise ValueError("product command item owner differs from authority set")
            if item.execution_projection_set_sha256 != self.execution_projection_set_sha256:
                raise ValueError("product command item projection differs from authority set")
        materialize_count = sum(
            item.disposition is ProductCommandDispositionV2.MATERIALIZE for item in self.ordered_items
        )
        reject_count = len(self.ordered_items) - materialize_count
        if (self.materialize_count, self.reject_count, self.total_count) != (
            materialize_count,
            reject_count,
            len(self.ordered_items),
        ):
            raise ValueError("product authority counts do not close to ordered items")
        expected_aggregate = (
            ProductCommandAggregateDispositionV2.ZERO_COMMAND
            if not self.ordered_items
            else ProductCommandAggregateDispositionV2.MATERIALIZE_ALL_ACCEPTED_COMMANDS
            if reject_count == 0
            else ProductCommandAggregateDispositionV2.ALL_REJECTED
            if materialize_count == 0
            else ProductCommandAggregateDispositionV2.MIXED_PER_COMMAND
        )
        if self.aggregate_disposition is not expected_aggregate:
            raise ValueError("product authority aggregate disposition mismatch")
        expected = hash_hex_v1(
            "miniqmt_product_command_authority_set_v2",
            self.canonical_payload_v1(exclude={"authority_set_sha256"}),
        )
        if self.authority_set_sha256 != expected:
            raise ValueError("product command authority set hash mismatch")
        return self


class ProductCommandLifecycleProjectionItemV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_lifecycle_projection_item_v2"]
    authority_item_sha256: Sha256V1
    command_id: IdentityV1
    mapping_id: IdentityV1 | None
    outbox_id: IdentityV1 | None
    child_order_id: IdentityV1 | None
    lifecycle_status: ProductLifecycleStatusV2
    last_committed_stage: IdentityV1
    broker_called: StrictBool | None
    qmt_order_id: IdentityV1 | None
    callback_watermark: IdentityV1 | None
    reconciliation_receipt_sha256: Sha256V1 | None
    item_projection_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_lifecycle_projection_item_v2",
            **values,
            "mapping_id": values.get("mapping_id"),
            "outbox_id": values.get("outbox_id"),
            "child_order_id": values.get("child_order_id"),
            "broker_called": values.get("broker_called"),
            "qmt_order_id": values.get("qmt_order_id"),
            "callback_watermark": values.get("callback_watermark"),
            "reconciliation_receipt_sha256": values.get("reconciliation_receipt_sha256"),
        }
        return cls(
            **payload,
            item_projection_sha256=hash_hex_v1("miniqmt_product_command_lifecycle_projection_item_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        identity_presence = tuple(value is not None for value in (self.mapping_id, self.outbox_id, self.child_order_id))
        synchronous_reject = self.lifecycle_status is ProductLifecycleStatusV2.SYNCHRONOUS_REJECTED
        if synchronous_reject and any(identity_presence):
            raise ValueError("synchronous reject lifecycle cannot carry materialized identities")
        if not synchronous_reject and not all(identity_presence):
            raise ValueError("materialized lifecycle requires mapping/outbox/child identities")
        if self.lifecycle_status is ProductLifecycleStatusV2.ACKED and (
            self.broker_called is not True or self.qmt_order_id is None
        ):
            raise ValueError("ACKED lifecycle requires broker call and qmt order identity")
        expected = hash_hex_v1(
            "miniqmt_product_command_lifecycle_projection_item_v2",
            self.canonical_payload_v1(exclude={"item_projection_sha256"}),
        )
        if self.item_projection_sha256 != expected:
            raise ValueError("product command lifecycle item hash mismatch")
        return self


class ProductCommandLifecycleProjectionV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_lifecycle_projection_v2"]
    authority_set_sha256: Sha256V1
    ordered_items: tuple[ProductCommandLifecycleProjectionItemV2, ...]
    projection_sha256: Sha256V1

    @classmethod
    def create(
        cls, *, authority_set_sha256: str, ordered_items: tuple[ProductCommandLifecycleProjectionItemV2, ...]
    ) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_lifecycle_projection_v2",
            "authority_set_sha256": authority_set_sha256,
            "ordered_items": ordered_items,
        }
        return cls(
            **payload,
            projection_sha256=hash_hex_v1("miniqmt_product_command_lifecycle_projection_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        if len(self.ordered_items) > MAX_PRODUCT_COMMANDS:
            raise ValueError("product lifecycle projection exceeds maximum cardinality")
        command_ids = tuple(item.command_id for item in self.ordered_items)
        if len(set(command_ids)) != len(command_ids):
            raise ValueError("product lifecycle projection contains duplicate command")
        expected = hash_hex_v1(
            "miniqmt_product_command_lifecycle_projection_v2",
            self.canonical_payload_v1(exclude={"projection_sha256"}),
        )
        if self.projection_sha256 != expected:
            raise ValueError("product command lifecycle projection hash mismatch")
        return self


class ProductMaterializationReceiptV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_materialization_receipt_v2"]
    authority_set_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    ordered_mapping_ids: tuple[IdentityV1, ...]
    ordered_outbox_ids: tuple[IdentityV1, ...]
    ordered_child_order_ids: tuple[IdentityV1, ...]
    zero_command: StrictBool
    repository_transaction_id: IdentityV1
    commit_outcome: Literal[ProductMaterializationCommitOutcomeV2.COMMITTED_READBACK_VERIFIED]
    independent_readback_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        if "commit_outcome" in values:
            raise TypeError("commit_outcome is repository-owned and cannot be supplied")
        payload = {
            "schema_version": "miniqmt_product_materialization_receipt_v2",
            **values,
            "commit_outcome": ProductMaterializationCommitOutcomeV2.COMMITTED_READBACK_VERIFIED,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1("miniqmt_product_materialization_receipt_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        sizes = (len(self.ordered_mapping_ids), len(self.ordered_outbox_ids), len(self.ordered_child_order_ids))
        if self.zero_command != (sizes == (0, 0, 0)) or len(set(sizes)) != 1:
            raise ValueError("materialization receipt identity sets do not close")
        for field_name, identities in (
            ("ordered_mapping_ids", self.ordered_mapping_ids),
            ("ordered_outbox_ids", self.ordered_outbox_ids),
            ("ordered_child_order_ids", self.ordered_child_order_ids),
        ):
            if len(set(identities)) != len(identities):
                raise ValueError(f"{field_name} must be unique")
        expected = hash_hex_v1(
            "miniqmt_product_materialization_receipt_v2",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("product materialization receipt hash mismatch")
        return self


class ProductRouteCutoverReceiptV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_route_cutover_receipt_v1"]
    runtime_id: IdentityV1
    binding_id: IdentityV1
    trade_date: date
    route_epoch: PositiveIntV1
    route_owner: ProductRouteOwnerKindV1
    effective_new_instance_sequence: PositiveIntV1
    legacy_active_instance_count: NonNegativeIntV1
    kernel_active_instance_count: NonNegativeIntV1
    catalog_sha256: Sha256V1
    gateway_capability_catalog_sha256: Sha256V1
    exchange_session_authority_sha256: Sha256V1
    migration_readback_sha256: Sha256V1
    product_authority_schema_sha256: Sha256V1
    previous_receipt_sha256: Sha256V1 | None
    created_at_utc: UtcDateTimeV1
    receipt_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_route_cutover_receipt_v1",
            **values,
            "trade_date": _strict_trade_date(values["trade_date"]),
        }
        return cls(**payload, receipt_sha256=hash_hex_v1("miniqmt_product_route_cutover_receipt_v1", payload))

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        if self.route_epoch == 1 and self.previous_receipt_sha256 is not None:
            raise ValueError("first route receipt cannot carry predecessor")
        if self.route_epoch > 1 and self.previous_receipt_sha256 is None:
            raise ValueError("successor route receipt requires predecessor")
        expected = hash_hex_v1(
            "miniqmt_product_route_cutover_receipt_v1",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("product route cutover receipt hash mismatch")
        return self


class ProductRouteOwnerV1(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_route_owner_v1"]
    runtime_id: IdentityV1
    binding_id: IdentityV1
    trade_date: date
    current_route_epoch: PositiveIntV1
    current_receipt_sha256: Sha256V1
    route_owner: ProductRouteOwnerKindV1
    effective_new_instance_sequence: PositiveIntV1
    row_version: PositiveIntV1
    owner_sha256: Sha256V1

    @classmethod
    def create(cls, *, receipt: ProductRouteCutoverReceiptV1, row_version: int) -> Self:
        payload = {
            "schema_version": "miniqmt_product_route_owner_v1",
            "runtime_id": receipt.runtime_id,
            "binding_id": receipt.binding_id,
            "trade_date": receipt.trade_date,
            "current_route_epoch": receipt.route_epoch,
            "current_receipt_sha256": receipt.receipt_sha256,
            "route_owner": receipt.route_owner,
            "effective_new_instance_sequence": receipt.effective_new_instance_sequence,
            "row_version": row_version,
        }
        return cls(**payload, owner_sha256=hash_hex_v1("miniqmt_product_route_owner_v1", payload))

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_product_route_owner_v1",
            self.canonical_payload_v1(exclude={"owner_sha256"}),
        )
        if self.owner_sha256 != expected:
            raise ValueError("product route owner hash mismatch")
        return self

    def validate_receipt_v1(self, receipt: ProductRouteCutoverReceiptV1) -> None:
        if (
            self.runtime_id,
            self.binding_id,
            self.trade_date,
            self.current_route_epoch,
            self.current_receipt_sha256,
            self.route_owner,
            self.effective_new_instance_sequence,
        ) != (
            receipt.runtime_id,
            receipt.binding_id,
            receipt.trade_date,
            receipt.route_epoch,
            receipt.receipt_sha256,
            receipt.route_owner,
            receipt.effective_new_instance_sequence,
        ):
            raise ValueError("product route owner does not close to receipt")


__all__ = [
    "DependentBuyCoordinationStatusV1",
    "DependentBuyCoordinationV1",
    "DependentBuyDecisionV1",
    "DependentBuyDependencyStatusV1",
    "DependentBuyLedgerObservationV1",
    "DependentBuyReleaseDecisionV1",
    "DependentBuySellDependencyV1",
    "DependentBuyTriggerEventRefV1",
    "DependentBuyTriggerTypeV1",
    "KernelProductContractError",
    "ProductCommandAggregateDispositionV2",
    "ProductCommandAuthorityItemV2",
    "ProductCommandAuthoritySetV2",
    "ProductCommandDispositionV2",
    "ProductCommandLifecycleProjectionItemV2",
    "ProductCommandLifecycleProjectionV2",
    "ProductLifecycleStatusV2",
    "ProductMaterializationCommitOutcomeV2",
    "ProductMaterializationReceiptV2",
    "ProductRouteCutoverReceiptV1",
    "ProductRouteOwnerKindV1",
    "ProductRouteOwnerV1",
]
