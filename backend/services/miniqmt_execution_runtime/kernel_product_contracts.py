"""Strict K6 product-cutover contracts with no runtime or persistence side effects."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
import json
from typing import Any, Literal, Self

from pydantic import StrictBool, ValidationError, model_validator

from backend.execution_algos.vnpy_compat.facade_contracts import VnpyFacadeAuthorityInputV2

from .plugin_canonical import (
    canonical_json_bytes_v1,
    canonical_utc_datetime_v1,
    hash_hex_v1 as _raw_hash_hex_v1,
    json_safe_evidence_v1,
    thaw_json_v1,
)
from .plugin_contracts import (
    BrokerCommandTypeV2,
    BrokerCommandV2,
    CanonicalDecimalV1,
    ExecutionProjectionSetV1,
    ExecutionAlgoTimerScheduleV1,
    FrozenStrictModel,
    FrozenJsonObjectFieldV1,
    IdentityV1,
    KernelProjectionTypeV1,
    MiniQMTRiskDecisionReceiptV1,
    NonNegativeIntV1,
    OMSPreflightProjectionReceiptV1,
    PositiveCanonicalDecimalV1,
    PositiveIntV1,
    SideV1,
    Sha256V1,
    UtcDateTimeV1,
    command_child_mapping_id_v1,
    deterministic_client_order_ref_v1,
    execution_child_order_id_v1,
)
from .plugin_registry import PluginRouteCompatibilityReceiptV1


MAX_DEPENDENT_BUY_DEPENDENCIES = 256
MAX_DEPENDENT_BUY_SETTLED_PROCEEDS_REFS = 4096
MAX_PRODUCT_COMMANDS = 256
MAX_PRODUCT_COMMAND_JSON_BYTES = 16 * 1024
MAX_PRODUCT_EVALUATION_EVIDENCE_JSON_BYTES = 64 * 1024
MAX_CONTRACT_FAILURES = 256


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


def validate_kernel_product_payload_v1(model_type: Any, payload: dict[str, Any], *, stage: str) -> Any:
    """Strictly read a durable K6 carrier and retain bounded typed failure evidence."""

    try:
        return model_type.model_validate_json(json.dumps(payload, sort_keys=True, separators=(",", ":")))
    except ValidationError as exc:
        ordered_failures = sorted(
            (
                {
                    "field_path": "/".join(str(part) for part in error.get("loc", ())),
                    "reason_code": str(error.get("type", "validation_error")),
                    "message": str(error.get("msg", "K6 contract validation failed")),
                }
                for error in exc.errors(include_url=False, include_context=False, include_input=False)
            ),
            key=lambda item: (item["field_path"], item["reason_code"], item["message"]),
        )
        retained = ordered_failures[:MAX_CONTRACT_FAILURES]
        omitted = ordered_failures[MAX_CONTRACT_FAILURES:]
        context: dict[str, Any] = {
            "stage": stage,
            "model": getattr(model_type, "__name__", str(model_type)),
            "failure_count": len(ordered_failures),
            "failures": retained,
            "failures_truncated": bool(omitted),
        }
        if omitted:
            context["omitted_failure_count"] = len(omitted)
            context["omitted_failure_set_sha256"] = hash_hex_v1("miniqmt_k6_omitted_contract_failure_set_v1", omitted)
        raise KernelProductContractError(
            "MINIQMT_K6_CONTRACT_INVALID",
            f"{getattr(model_type, '__name__', 'K6 carrier')} strict readback failed",
            context=context,
        ) from exc


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


class ProductCommandDispositionV3(StrEnum):
    MATERIALIZE = "MATERIALIZE"
    REJECT_SYNCHRONOUS = "REJECT_SYNCHRONOUS"
    DEFER_DEPENDENT_BUY = "DEFER_DEPENDENT_BUY"


class ProductCommandAggregateDispositionV3(StrEnum):
    ZERO_COMMAND = "ZERO_COMMAND"
    ALL_REJECTED = "ALL_REJECTED"
    ALL_DEFERRED = "ALL_DEFERRED"
    MATERIALIZE_ALL_ACCEPTED_COMMANDS = "MATERIALIZE_ALL_ACCEPTED_COMMANDS"
    MIXED_PER_COMMAND = "MIXED_PER_COMMAND"


class ProductLifecycleStatusV3(StrEnum):
    SYNCHRONOUS_REJECTED = "SYNCHRONOUS_REJECTED"
    DEFERRED_DEPENDENT_BUY = "DEFERRED_DEPENDENT_BUY"
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    DISPATCHING = "DISPATCHING"
    ACKED = "ACKED"
    ACKED_REJECTED = "ACKED_REJECTED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN"
    RECONCILING = "RECONCILING"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class ProductMaterializationCommitOutcomeV3(StrEnum):
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


class DependentBuySettledProceedsRefV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_settled_proceeds_ref_v2"]
    broker_trade_id: IdentityV1
    qmt_trade_ledger_id: IdentityV1
    qmt_trade_fact_sha256: Sha256V1
    cash_ledger_id: IdentityV1
    cash_ledger_sequence: PositiveIntV1
    cash_ledger_fact_sha256: Sha256V1
    strategy_id: IdentityV1
    runtime_id: IdentityV1
    trade_date: date
    sell_parent_intent_id: IdentityV1
    proceeds_ref_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_dependent_buy_settled_proceeds_ref_v2",
            **values,
            "trade_date": _strict_trade_date(values["trade_date"]),
        }
        return cls(
            **payload,
            proceeds_ref_sha256=hash_hex_v1("miniqmt_dependent_buy_settled_proceeds_ref_v2", payload),
        )

    def sort_key_v2(self) -> tuple[str, int, str]:
        return (self.broker_trade_id, self.cash_ledger_sequence, self.cash_ledger_id)

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_settled_proceeds_ref_v2",
            self.canonical_payload_v1(exclude={"proceeds_ref_sha256"}),
        )
        if self.proceeds_ref_sha256 != expected:
            raise ValueError("dependent-BUY settled proceeds reference hash mismatch")
        return self


class DependentBuySellDependencyV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_sell_dependency_v2"]
    runtime_id: IdentityV1
    strategy_id: IdentityV1
    sell_parent_intent_id: IdentityV1
    sell_algo_instance_id: IdentityV1
    required_terminal_policy: Literal["TRADE_SETTLED_OR_ORDER_TERMINAL"]
    latest_order_fact_id: IdentityV1 | None
    latest_order_fact_sha256: Sha256V1 | None
    ordered_settled_proceeds_refs: tuple[DependentBuySettledProceedsRefV2, ...]
    dependency_status: DependentBuyDependencyStatusV1
    dependency_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_dependent_buy_sell_dependency_v2",
            **values,
            "required_terminal_policy": "TRADE_SETTLED_OR_ORDER_TERMINAL",
            "latest_order_fact_id": values.get("latest_order_fact_id"),
            "latest_order_fact_sha256": values.get("latest_order_fact_sha256"),
            "ordered_settled_proceeds_refs": tuple(values.get("ordered_settled_proceeds_refs", ())),
        }
        return cls(**payload, dependency_sha256=hash_hex_v1("miniqmt_dependent_buy_sell_dependency_v2", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if (self.latest_order_fact_id is None) != (self.latest_order_fact_sha256 is None):
            raise ValueError("latest order fact identity and hash must be present together")
        refs = self.ordered_settled_proceeds_refs
        if len(refs) > MAX_DEPENDENT_BUY_SETTLED_PROCEEDS_REFS:
            raise ValueError("ordered settled proceeds references exceed maximum cardinality 4096")
        keys = tuple(item.sort_key_v2() for item in refs)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("ordered settled proceeds references must be canonical and unique")
        for item in refs:
            if (
                item.runtime_id != self.runtime_id
                or item.strategy_id != self.strategy_id
                or item.sell_parent_intent_id != self.sell_parent_intent_id
            ):
                raise ValueError("settled proceeds reference owner differs from SELL dependency")
        if self.dependency_status is DependentBuyDependencyStatusV1.PROCEEDS_SETTLED and not refs:
            raise ValueError("settled dependency requires at least one settled proceeds reference")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_sell_dependency_v2",
            self.canonical_payload_v1(exclude={"dependency_sha256"}),
        )
        if self.dependency_sha256 != expected:
            raise ValueError("dependent-BUY V2 dependency hash mismatch")
        return self


class DependentBuyLedgerObservationV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_ledger_observation_v2"]
    runtime_id: IdentityV1
    strategy_id: IdentityV1
    trade_date: date
    ledger_authority_source: Literal["qmt_strategy_ledger.virtual_account.cash"]
    virtual_account_id: IdentityV1
    virtual_account_updated_at_utc: UtcDateTimeV1
    latest_cash_ledger_sequence: NonNegativeIntV1
    ledger_as_of_utc: UtcDateTimeV1
    available_cash: CanonicalDecimalV1
    required_cash: CanonicalDecimalV1
    cash_shortfall: CanonicalDecimalV1
    ordered_settled_proceeds_refs: tuple[DependentBuySettledProceedsRefV2, ...]
    freshness_session_authority_sha256: Sha256V1
    ledger_revision_sha256: Sha256V1
    observation_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        available = Decimal(str(values["available_cash"]))
        required = Decimal(str(values["required_cash"]))
        refs = tuple(values.get("ordered_settled_proceeds_refs", ()))
        payload = {
            "schema_version": "miniqmt_dependent_buy_ledger_observation_v2",
            **values,
            "trade_date": _strict_trade_date(values["trade_date"]),
            "ledger_authority_source": "qmt_strategy_ledger.virtual_account.cash",
            "cash_shortfall": _canonical_decimal_text_v1(max(required - available, Decimal("0"))),
            "ordered_settled_proceeds_refs": refs,
        }
        revision_payload = {
            key: payload[key]
            for key in (
                "runtime_id",
                "strategy_id",
                "trade_date",
                "virtual_account_id",
                "virtual_account_updated_at_utc",
                "latest_cash_ledger_sequence",
                "available_cash",
                "ordered_settled_proceeds_refs",
            )
        }
        payload["ledger_revision_sha256"] = hash_hex_v1("miniqmt_dependent_buy_ledger_revision_v2", revision_payload)
        return cls(**payload, observation_sha256=hash_hex_v1("miniqmt_dependent_buy_ledger_observation_v2", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        refs = self.ordered_settled_proceeds_refs
        if len(refs) > MAX_DEPENDENT_BUY_SETTLED_PROCEEDS_REFS:
            raise ValueError("ledger settled proceeds references exceed maximum cardinality 4096")
        keys = tuple(item.sort_key_v2() for item in refs)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("ledger settled proceeds references must be canonical and unique")
        for item in refs:
            if (
                item.runtime_id != self.runtime_id
                or item.strategy_id != self.strategy_id
                or item.trade_date != self.trade_date
            ):
                raise ValueError("ledger settled proceeds reference owner differs from observation")
        maximum_sequence = max((item.cash_ledger_sequence for item in refs), default=0)
        if self.latest_cash_ledger_sequence < maximum_sequence:
            raise ValueError("latest cash ledger sequence precedes a settled proceeds reference")
        if self.ledger_as_of_utc < self.virtual_account_updated_at_utc:
            raise ValueError("ledger as-of time precedes virtual-account update")
        expected_shortfall = max(Decimal(self.required_cash) - Decimal(self.available_cash), Decimal("0"))
        if Decimal(self.cash_shortfall) != expected_shortfall:
            raise ValueError("cash_shortfall does not equal max(required_cash-available_cash,0)")
        revision_payload = {
            key: self.canonical_payload_v1()[key]
            for key in (
                "runtime_id",
                "strategy_id",
                "trade_date",
                "virtual_account_id",
                "virtual_account_updated_at_utc",
                "latest_cash_ledger_sequence",
                "available_cash",
                "ordered_settled_proceeds_refs",
            )
        }
        if self.ledger_revision_sha256 != hash_hex_v1("miniqmt_dependent_buy_ledger_revision_v2", revision_payload):
            raise ValueError("dependent-BUY ledger revision hash mismatch")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_ledger_observation_v2",
            self.canonical_payload_v1(exclude={"observation_sha256"}),
        )
        if self.observation_sha256 != expected:
            raise ValueError("dependent-BUY ledger observation V2 hash mismatch")
        return self


_DEPENDENT_BUY_EXACT_ERROR_CODES = {
    "SELL_PROCEEDS_REQUIRED",
    "ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED",
}


class DependentBuyCandidateAuthorityV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_candidate_authority_v2"]
    runtime_id: IdentityV1
    binding_id: IdentityV1
    trade_date: date
    strategy_id: IdentityV1
    buy_algo_instance_id: IdentityV1
    buy_parent_intent_id: IdentityV1
    command_id: IdentityV1
    execution_plan_id: IdentityV1
    execution_plan_sha256: Sha256V1
    plan_parent_relation_sha256: Sha256V1
    required_cash: CanonicalDecimalV1
    virtual_account_id: IdentityV1
    session_authority_sha256: Sha256V1
    ordered_sell_dependencies: tuple[DependentBuySellDependencyV2, ...]
    oms_preflight_receipt_id: IdentityV1
    oms_preflight_receipt_sha256: Sha256V1
    ordered_error_codes: tuple[IdentityV1, ...]
    candidate_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_dependent_buy_candidate_authority_v2",
            **values,
            "trade_date": _strict_trade_date(values["trade_date"]),
            "ordered_sell_dependencies": tuple(values["ordered_sell_dependencies"]),
            "ordered_error_codes": tuple(values["ordered_error_codes"]),
        }
        return cls(**payload, candidate_sha256=hash_hex_v1("miniqmt_dependent_buy_candidate_authority_v2", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        dependencies = self.ordered_sell_dependencies
        if not 1 <= len(dependencies) <= MAX_DEPENDENT_BUY_DEPENDENCIES:
            raise ValueError("candidate SELL dependency cardinality must be in [1,256]")
        keys = tuple(item.sell_parent_intent_id for item in dependencies)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("candidate SELL dependencies must be sorted and unique")
        for dependency in dependencies:
            if dependency.runtime_id != self.runtime_id or dependency.strategy_id != self.strategy_id:
                raise ValueError("candidate SELL dependency owner differs from candidate")
            if any(item.trade_date != self.trade_date for item in dependency.ordered_settled_proceeds_refs):
                raise ValueError("candidate SELL dependency trade date differs from candidate")
        if (
            not self.ordered_error_codes
            or self.ordered_error_codes != tuple(sorted(set(self.ordered_error_codes)))
            or not set(self.ordered_error_codes).issubset(_DEPENDENT_BUY_EXACT_ERROR_CODES)
        ):
            raise ValueError("candidate error codes must be a non-empty canonical exact dependent-BUY subset")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_candidate_authority_v2",
            self.canonical_payload_v1(exclude={"candidate_sha256"}),
        )
        if self.candidate_sha256 != expected:
            raise ValueError("dependent-BUY candidate authority hash mismatch")
        return self


class DependentBuyReleaseDecisionV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_release_decision_v2"]
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
            "miniqmt_dependent_buy_release_decision_id_v2",
            {
                "coordination_id": values["coordination_id"],
                "decision_sequence": values["decision_sequence"],
                "trigger_ref_sha256": values["trigger_ref_sha256"],
            },
        )
        payload = {
            "schema_version": "miniqmt_dependent_buy_release_decision_v2",
            "decision_id": decision_id,
            **values,
            "release_event_id": values.get("release_event_id"),
            "release_transition_id": values.get("release_transition_id"),
            "release_command_authority_set_sha256": values.get("release_command_authority_set_sha256"),
        }
        return cls(**payload, decision_sha256=hash_hex_v1("miniqmt_dependent_buy_release_decision_v2", payload))

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
                raise ValueError("release decision lacks trigger event, original transition or command authority")
        elif any(value is not None for value in release_values):
            raise ValueError("non-release decision cannot carry release identities")
        if self.decision_sequence == 1 and self.previous_decision_sha256 is not None:
            raise ValueError("first decision cannot carry predecessor")
        if self.decision_sequence > 1 and self.previous_decision_sha256 is None:
            raise ValueError("successor decision requires predecessor")
        expected_id = hash_hex_v1(
            "miniqmt_dependent_buy_release_decision_id_v2",
            {
                "coordination_id": self.coordination_id,
                "decision_sequence": self.decision_sequence,
                "trigger_ref_sha256": self.trigger_ref_sha256,
            },
        )
        if self.decision_id != expected_id:
            raise ValueError("dependent-BUY decision V2 identity mismatch")
        expected = hash_hex_v1(
            "miniqmt_dependent_buy_release_decision_v2",
            self.canonical_payload_v1(exclude={"decision_sha256"}),
        )
        if self.decision_sha256 != expected:
            raise ValueError("dependent-BUY decision V2 hash mismatch")
        return self


class DependentBuyCoordinationV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_dependent_buy_coordination_v2"]
    coordination_id: Sha256V1
    runtime_id: IdentityV1
    binding_id: IdentityV1
    trade_date: date
    strategy_id: IdentityV1
    buy_algo_instance_id: IdentityV1
    buy_parent_intent_id: IdentityV1
    required_cash: CanonicalDecimalV1
    release_command_id: IdentityV1
    release_transition_id: IdentityV1
    release_command_authority_item_sha256: Sha256V1
    release_command_payload_sha256: Sha256V1
    ordered_sell_dependencies: tuple[DependentBuySellDependencyV2, ...]
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
            "miniqmt_dependent_buy_coordination_id_v2",
            {
                "runtime_id": values["runtime_id"],
                "buy_algo_instance_id": values["buy_algo_instance_id"],
                "buy_parent_intent_id": values["buy_parent_intent_id"],
                "strategy_id": values["strategy_id"],
                "trade_date": trade_date.isoformat(),
            },
        )
        payload = {
            "schema_version": "miniqmt_dependent_buy_coordination_v2",
            "coordination_id": coordination_id,
            **values,
            "trade_date": trade_date,
            "ordered_sell_dependencies": tuple(values["ordered_sell_dependencies"]),
            "lease_worker_id": values.get("lease_worker_id"),
            "lease_process_incarnation_id": values.get("lease_process_incarnation_id"),
            "lease_epoch": values.get("lease_epoch", 0),
            "lease_expires_at_utc": values.get("lease_expires_at_utc"),
        }
        hash_payload = {key: value for key, value in payload.items() if key not in _COORDINATION_HASH_EXCLUDES}
        return cls(**payload, coordination_sha256=hash_hex_v1("miniqmt_dependent_buy_coordination_v2", hash_payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        dependencies = self.ordered_sell_dependencies
        if not 1 <= len(dependencies) <= MAX_DEPENDENT_BUY_DEPENDENCIES:
            raise ValueError("ordered_sell_dependencies cardinality must be in [1,256]")
        keys = tuple(item.sell_parent_intent_id for item in dependencies)
        if keys != tuple(sorted(keys)) or len(set(keys)) != len(keys):
            raise ValueError("ordered_sell_dependencies must be sorted and unique by sell parent")
        for dependency in dependencies:
            if dependency.runtime_id != self.runtime_id or dependency.strategy_id != self.strategy_id:
                raise ValueError("sell dependency owner differs from coordination")
            if any(item.trade_date != self.trade_date for item in dependency.ordered_settled_proceeds_refs):
                raise ValueError("sell dependency trade date differs from coordination")
        released = self.status is DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX
        if released and (
            self.released_command_id != self.release_command_id or self.released_outbox_id != self.release_command_id
        ):
            raise ValueError("released coordination must preserve the original command identity")
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
        if self.coordination_sha256 != hash_hex_v1("miniqmt_dependent_buy_coordination_v2", hash_payload):
            raise ValueError("dependent-BUY coordination V2 hash mismatch")
        return self

    def validate_initial_v2(self) -> None:
        if (
            self.status is not DependentBuyCoordinationStatusV1.DEFERRED_WAITING_SELL_PROCEEDS
            or self.decision_sequence != 0
            or self.row_version != 1
            or self.last_decision_sha256 is not None
            or self.released_command_id is not None
            or self.lease_epoch != 0
        ):
            raise ValueError("dependent-BUY V2 first write requires exact waiting initial state")

    def validate_successor_v2(self, previous: Self) -> None:
        immutable = (
            "coordination_id",
            "runtime_id",
            "binding_id",
            "trade_date",
            "strategy_id",
            "buy_algo_instance_id",
            "buy_parent_intent_id",
            "required_cash",
            "release_command_id",
            "release_transition_id",
            "release_command_authority_item_sha256",
            "release_command_payload_sha256",
            "ordered_sell_dependencies",
            "created_at_utc",
        )
        if any(getattr(self, field) != getattr(previous, field) for field in immutable):
            raise ValueError("dependent-BUY V2 successor changes immutable owner, command, or payload")
        if self.row_version != previous.row_version + 1:
            raise ValueError("dependent-BUY V2 successor row_version must increase by one")
        if self.decision_sequence < previous.decision_sequence:
            raise ValueError("dependent-BUY V2 decision sequence cannot decrease")
        terminal = {
            DependentBuyCoordinationStatusV1.RELEASED_TO_K2_OUTBOX,
            DependentBuyCoordinationStatusV1.BLOCKED_SELL_PROCEEDS_UNAVAILABLE,
            DependentBuyCoordinationStatusV1.EOD_RESIDUAL,
        }
        if previous.status in terminal and self != previous:
            raise ValueError("dependent-BUY V2 terminal coordination cannot reopen")


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
    effect_ordinal: NonNegativeIntV1
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
        item_hashes = tuple(item.authority_item_sha256 for item in self.ordered_items)
        ordinals = tuple(item.effect_ordinal for item in self.ordered_items)
        if len(set(command_ids)) != len(command_ids) or len(set(item_hashes)) != len(item_hashes):
            raise ValueError("product lifecycle projection contains duplicate command or authority item")
        if ordinals != tuple(range(len(self.ordered_items))):
            raise ValueError("product lifecycle projection order must follow contiguous authority ordinals")
        expected = hash_hex_v1(
            "miniqmt_product_command_lifecycle_projection_v2",
            self.canonical_payload_v1(exclude={"projection_sha256"}),
        )
        if self.projection_sha256 != expected:
            raise ValueError("product command lifecycle projection hash mismatch")
        return self

    def validate_against_authority_v2(self, authority: ProductCommandAuthoritySetV2) -> None:
        if not isinstance(authority, ProductCommandAuthoritySetV2):
            raise TypeError("authority must be ProductCommandAuthoritySetV2")
        expected = tuple((item.effect_ordinal, item.command_id, item.item_sha256) for item in authority.ordered_items)
        actual = tuple(
            (item.effect_ordinal, item.command_id, item.authority_item_sha256) for item in self.ordered_items
        )
        if self.authority_set_sha256 != authority.authority_set_sha256 or actual != expected:
            raise KernelProductContractError(
                "MINIQMT_K6_PRODUCT_AUTHORITY_LIFECYCLE_ORDER_DRIFT",
                "product lifecycle projection differs from exact authority order",
                context={"expected": expected, "actual": actual},
            )
        for lifecycle, authority_item in zip(self.ordered_items, authority.ordered_items, strict=True):
            synchronous_reject = authority_item.disposition is ProductCommandDispositionV2.REJECT_SYNCHRONOUS
            if synchronous_reject != (lifecycle.lifecycle_status is ProductLifecycleStatusV2.SYNCHRONOUS_REJECTED):
                raise KernelProductContractError(
                    "MINIQMT_K6_PRODUCT_AUTHORITY_LIFECYCLE_DISPOSITION_DRIFT",
                    "product lifecycle status differs from authority disposition",
                    context={"command_id": authority_item.command_id},
                )


class ProductMaterializationReceiptV2(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_materialization_receipt_v2"]
    authority_set_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    ordered_authority_item_sha256s: tuple[Sha256V1, ...]
    ordered_command_ids: tuple[IdentityV1, ...]
    ordered_mapping_ids: tuple[IdentityV1, ...]
    ordered_outbox_ids: tuple[IdentityV1, ...]
    ordered_child_order_ids: tuple[IdentityV1, ...]
    zero_command: StrictBool
    repository_transaction_id: IdentityV1
    commit_outcome: Literal[ProductMaterializationCommitOutcomeV2.COMMITTED_READBACK_VERIFIED]
    independent_readback_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        authority: ProductCommandAuthoritySetV2,
        repository_transaction_id: str,
        independent_readback_sha256: str,
    ) -> Self:
        if not isinstance(authority, ProductCommandAuthoritySetV2):
            raise TypeError("authority must be ProductCommandAuthoritySetV2")
        materialized = tuple(
            item for item in authority.ordered_items if item.disposition is ProductCommandDispositionV2.MATERIALIZE
        )
        payload = {
            "schema_version": "miniqmt_product_materialization_receipt_v2",
            "authority_set_sha256": authority.authority_set_sha256,
            "execution_projection_set_sha256": authority.execution_projection_set_sha256,
            "ordered_authority_item_sha256s": tuple(item.item_sha256 for item in materialized),
            "ordered_command_ids": tuple(item.command_id for item in materialized),
            "ordered_mapping_ids": tuple(item.mapping_id for item in materialized),
            "ordered_outbox_ids": tuple(item.outbox_id for item in materialized),
            "ordered_child_order_ids": tuple(item.child_order_id for item in materialized),
            "zero_command": authority.total_count == 0,
            "repository_transaction_id": repository_transaction_id,
            "commit_outcome": ProductMaterializationCommitOutcomeV2.COMMITTED_READBACK_VERIFIED,
            "independent_readback_sha256": independent_readback_sha256,
        }
        return cls(
            **payload,
            receipt_sha256=hash_hex_v1("miniqmt_product_materialization_receipt_v2", payload),
        )

    @model_validator(mode="after")
    def _validate_hash(self) -> Self:
        sizes = (
            len(self.ordered_authority_item_sha256s),
            len(self.ordered_command_ids),
            len(self.ordered_mapping_ids),
            len(self.ordered_outbox_ids),
            len(self.ordered_child_order_ids),
        )
        if len(set(sizes)) != 1 or sizes[0] > MAX_PRODUCT_COMMANDS:
            raise ValueError("materialization receipt identity sets do not close")
        if self.zero_command and sizes[0] != 0:
            raise ValueError("zero-command receipt cannot carry materialized identities")
        for field_name, identities in (
            ("ordered_authority_item_sha256s", self.ordered_authority_item_sha256s),
            ("ordered_command_ids", self.ordered_command_ids),
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

    def validate_against_authority_v2(self, authority: ProductCommandAuthoritySetV2) -> None:
        if not isinstance(authority, ProductCommandAuthoritySetV2):
            raise TypeError("authority must be ProductCommandAuthoritySetV2")
        materialized = tuple(
            item for item in authority.ordered_items if item.disposition is ProductCommandDispositionV2.MATERIALIZE
        )
        expected = (
            authority.authority_set_sha256,
            authority.execution_projection_set_sha256,
            tuple(item.item_sha256 for item in materialized),
            tuple(item.command_id for item in materialized),
            tuple(item.mapping_id for item in materialized),
            tuple(item.outbox_id for item in materialized),
            tuple(item.child_order_id for item in materialized),
            authority.total_count == 0,
        )
        actual = (
            self.authority_set_sha256,
            self.execution_projection_set_sha256,
            self.ordered_authority_item_sha256s,
            self.ordered_command_ids,
            self.ordered_mapping_ids,
            self.ordered_outbox_ids,
            self.ordered_child_order_ids,
            self.zero_command,
        )
        if actual != expected:
            raise KernelProductContractError(
                "MINIQMT_K6_PRODUCT_MATERIALIZATION_IDENTITY_DRIFT",
                "materialization receipt differs from exact authority association",
                context={"expected": expected, "actual": actual},
            )


_PRODUCT_PROJECTION_DOMAINS_V3 = {
    KernelProjectionTypeV1.CONTRACT: "miniqmt_contract_projection_v1",
    KernelProjectionTypeV1.MARKET_DATA: "miniqmt_market_data_projection_v2",
    KernelProjectionTypeV1.ACCOUNT: "miniqmt_account_projection_v1",
    KernelProjectionTypeV1.KILL_SWITCH_STATE: "miniqmt_kill_switch_state_v1",
}


class ProductCommandEvaluationEvidenceV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_evaluation_evidence_v3"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    effect_ordinal: NonNegativeIntV1
    command_id: IdentityV1
    oms_preflight_receipt: OMSPreflightProjectionReceiptV1
    mini_qmt_risk_decision_receipt: MiniQMTRiskDecisionReceiptV1
    plugin_route_compatibility_receipt: PluginRouteCompatibilityReceiptV1
    market_data_projection: FrozenJsonObjectFieldV1
    account_projection: FrozenJsonObjectFieldV1
    contract_projection: FrozenJsonObjectFieldV1
    kill_switch_state: FrozenJsonObjectFieldV1
    execution_projection_set: ExecutionProjectionSetV1
    dependent_buy_candidate: DependentBuyCandidateAuthorityV2 | None
    evidence_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_evaluation_evidence_v3",
            **values,
            "dependent_buy_candidate": values.get("dependent_buy_candidate"),
        }
        return cls(**payload, evidence_sha256=hash_hex_v1("miniqmt_product_command_evaluation_evidence_v3", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        projection_set = self.execution_projection_set
        owner = (self.runtime_id, self.algo_instance_id, self.event_id, self.delivery_id)
        if (
            projection_set.runtime_id,
            projection_set.algo_instance_id,
            projection_set.event_id,
            projection_set.delivery_id,
        ) != owner:
            raise ValueError("product evaluation owner differs from execution projection set")
        if (
            self.oms_preflight_receipt.runtime_id,
            self.oms_preflight_receipt.algo_instance_id,
        ) != owner[:2]:
            raise ValueError("OMS preflight owner differs from product evaluation")
        if (
            self.mini_qmt_risk_decision_receipt.runtime_id,
            self.mini_qmt_risk_decision_receipt.algo_instance_id,
            self.mini_qmt_risk_decision_receipt.event_id,
        ) != (self.runtime_id, self.algo_instance_id, self.event_id):
            raise ValueError("risk receipt owner differs from product evaluation")
        candidate = self.dependent_buy_candidate
        if candidate is not None and (
            candidate.runtime_id != self.runtime_id
            or candidate.buy_algo_instance_id != self.algo_instance_id
            or candidate.command_id != self.command_id
        ):
            raise ValueError("dependent-BUY candidate owner differs from product evaluation")
        refs = {item.projection_type: item for item in projection_set.ordered_projection_refs}
        shared_authority_refs = (
            (KernelProjectionTypeV1.OMS_PREFLIGHT, "mqomspreflight_", "miniqmt_oms_preflight_projection_receipt_v1"),
            (KernelProjectionTypeV1.RISK_DECISION, "mqriskdecision_", "miniqmt_risk_decision_receipt_v1"),
            (KernelProjectionTypeV1.ROUTE_COMPATIBILITY, "mqroutecompat_", "plugin_route_compatibility_receipt_v1"),
        )
        for projection_type, identity_prefix, version in shared_authority_refs:
            ref = refs.get(projection_type)
            if (
                ref is None
                or not ref.projection_id.startswith(identity_prefix)
                or ref.projection_version != version
                or ref.source_event_id != self.event_id
            ):
                raise ValueError(f"{projection_type.value} shared projection reference is invalid")
        raw_payloads = {
            KernelProjectionTypeV1.CONTRACT: self.contract_projection,
            KernelProjectionTypeV1.MARKET_DATA: self.market_data_projection,
            KernelProjectionTypeV1.ACCOUNT: self.account_projection,
            KernelProjectionTypeV1.KILL_SWITCH_STATE: self.kill_switch_state,
        }
        for projection_type, payload in raw_payloads.items():
            payload_sha256 = hash_hex_v1(_PRODUCT_PROJECTION_DOMAINS_V3[projection_type], thaw_json_v1(payload))
            ref = refs.get(projection_type)
            if ref is None or ref.payload_sha256 != payload_sha256 or ref.source_event_id != self.event_id:
                raise ValueError(f"{projection_type.value} payload differs from execution projection reference")
        expected = hash_hex_v1(
            "miniqmt_product_command_evaluation_evidence_v3",
            self.canonical_payload_v1(exclude={"evidence_sha256"}),
        )
        if self.evidence_sha256 != expected:
            raise ValueError("product command evaluation evidence hash mismatch")
        if len(canonical_json_bytes_v1(self.canonical_payload_v1())) > MAX_PRODUCT_EVALUATION_EVIDENCE_JSON_BYTES:
            raise ValueError("product command evaluation evidence exceeds 64KiB canonical JSON bound")
        return self


class ProductCommandAuthorityItemV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_authority_item_v3"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    effect_ordinal: NonNegativeIntV1
    command_id: IdentityV1
    command_type: BrokerCommandTypeV2
    command_json: BrokerCommandV2
    evaluation_evidence: ProductCommandEvaluationEvidenceV3
    command_payload_sha256: Sha256V1
    plugin_effect_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    oms_preflight_receipt_sha256: Sha256V1
    risk_decision_receipt_sha256: Sha256V1
    route_compatibility_receipt_sha256: Sha256V1
    market_data_projection_sha256: Sha256V1
    account_projection_sha256: Sha256V1
    contract_projection_sha256: Sha256V1
    disposition: ProductCommandDispositionV3
    reject_reason_code: IdentityV1 | None
    reject_context_sha256: Sha256V1 | None
    coordination_id: Sha256V1 | None
    mapping_id: IdentityV1
    outbox_id: IdentityV1 | None
    child_order_id: IdentityV1
    item_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        command = values["command_json"]
        evidence = values["evaluation_evidence"]
        if not isinstance(command, BrokerCommandV2):
            raise TypeError("command_json must be BrokerCommandV2")
        if not isinstance(evidence, ProductCommandEvaluationEvidenceV3):
            raise TypeError("evaluation_evidence must be ProductCommandEvaluationEvidenceV3")
        payload = {
            "schema_version": "miniqmt_product_command_authority_item_v3",
            **values,
            "command_type": command.command_type,
            "command_id": command.command_id,
            "command_payload_sha256": command.payload_sha256,
            "execution_projection_set_sha256": evidence.execution_projection_set.projection_set_sha256,
            "oms_preflight_receipt_sha256": evidence.oms_preflight_receipt.receipt_sha256,
            "risk_decision_receipt_sha256": evidence.mini_qmt_risk_decision_receipt.receipt_sha256,
            "route_compatibility_receipt_sha256": evidence.plugin_route_compatibility_receipt.receipt_sha256,
            "market_data_projection_sha256": hash_hex_v1(
                "miniqmt_market_data_projection_v2", thaw_json_v1(evidence.market_data_projection)
            ),
            "account_projection_sha256": hash_hex_v1(
                "miniqmt_account_projection_v1", thaw_json_v1(evidence.account_projection)
            ),
            "contract_projection_sha256": hash_hex_v1(
                "miniqmt_contract_projection_v1", thaw_json_v1(evidence.contract_projection)
            ),
            "reject_reason_code": values.get("reject_reason_code"),
            "reject_context_sha256": values.get("reject_context_sha256"),
            "coordination_id": values.get("coordination_id"),
            "outbox_id": values.get("outbox_id"),
        }
        return cls(**payload, item_sha256=hash_hex_v1("miniqmt_product_command_authority_item_v3", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        command = self.command_json
        evidence = self.evaluation_evidence
        owner = (self.runtime_id, self.algo_instance_id, self.event_id, self.delivery_id, self.transition_id)
        if (
            evidence.runtime_id,
            evidence.algo_instance_id,
            evidence.event_id,
            evidence.delivery_id,
            evidence.transition_id,
        ) != owner:
            raise ValueError("evaluation evidence owner differs from authority item")
        if (
            command.runtime_id != self.runtime_id
            or command.algo_instance_id != self.algo_instance_id
            or command.transition_id != self.transition_id
            or command.ordinal != self.effect_ordinal
            or command.command_id != self.command_id
            or command.command_type is not self.command_type
            or command.payload_sha256 != self.command_payload_sha256
            or evidence.effect_ordinal != self.effect_ordinal
            or evidence.command_id != self.command_id
        ):
            raise ValueError("strict command/evidence identity differs from authority item")
        if len(canonical_json_bytes_v1(command.model_dump(mode="json"))) > MAX_PRODUCT_COMMAND_JSON_BYTES:
            raise ValueError("command_json exceeds 16KiB canonical JSON bound")
        evidence_hashes = (
            evidence.execution_projection_set.projection_set_sha256,
            evidence.oms_preflight_receipt.receipt_sha256,
            evidence.mini_qmt_risk_decision_receipt.receipt_sha256,
            evidence.plugin_route_compatibility_receipt.receipt_sha256,
            hash_hex_v1("miniqmt_market_data_projection_v2", thaw_json_v1(evidence.market_data_projection)),
            hash_hex_v1("miniqmt_account_projection_v1", thaw_json_v1(evidence.account_projection)),
            hash_hex_v1("miniqmt_contract_projection_v1", thaw_json_v1(evidence.contract_projection)),
        )
        if evidence_hashes != (
            self.execution_projection_set_sha256,
            self.oms_preflight_receipt_sha256,
            self.risk_decision_receipt_sha256,
            self.route_compatibility_receipt_sha256,
            self.market_data_projection_sha256,
            self.account_projection_sha256,
            self.contract_projection_sha256,
        ):
            raise ValueError("evaluation evidence hashes differ from authority item")
        if command.command_type is BrokerCommandTypeV2.SUBMIT_LIMIT:
            expected_child_order_id = execution_child_order_id_v1(
                command_id=command.command_id,
                local_vt_orderid=command.local_vt_orderid,
            )
            expected_mapping_id = command_child_mapping_id_v1(
                command_id=command.command_id,
                local_vt_orderid=command.local_vt_orderid,
                child_order_id=expected_child_order_id,
            )
            if (self.mapping_id, self.child_order_id) != (expected_mapping_id, expected_child_order_id):
                raise ValueError("SUBMIT authority item mapping identity differs from deterministic command closure")
        else:
            metadata = thaw_json_v1(command.metadata)
            submit_command_id = metadata.get("submit_command_id")
            metadata_mapping_id = metadata.get("mapping_id")
            if (submit_command_id is None) == (metadata_mapping_id is None):
                raise ValueError("CANCEL authority requires exactly one existing mapping lineage reference")
            if submit_command_id is not None:
                if (
                    type(submit_command_id) is not str
                    or not submit_command_id
                    or submit_command_id != submit_command_id.strip()
                ):
                    raise ValueError("CANCEL submit_command_id must be a canonical strict identity")
                expected_child_order_id = execution_child_order_id_v1(
                    command_id=submit_command_id,
                    local_vt_orderid=command.local_vt_orderid,
                )
                expected_mapping_id = command_child_mapping_id_v1(
                    command_id=submit_command_id,
                    local_vt_orderid=command.local_vt_orderid,
                    child_order_id=expected_child_order_id,
                )
                if (self.mapping_id, self.child_order_id) != (expected_mapping_id, expected_child_order_id):
                    raise ValueError("CANCEL authority item mapping identity differs from original SUBMIT closure")
            elif (
                type(metadata_mapping_id) is not str
                or not metadata_mapping_id
                or metadata_mapping_id != metadata_mapping_id.strip()
                or self.mapping_id != metadata_mapping_id
            ):
                raise ValueError("CANCEL authority item mapping identity differs from active mapping reference")
        reject = (self.reject_reason_code, self.reject_context_sha256)
        candidate = evidence.dependent_buy_candidate
        if self.disposition is ProductCommandDispositionV3.MATERIALIZE:
            if self.outbox_id != self.command_id or any(
                value is not None for value in (*reject, self.coordination_id, candidate)
            ):
                raise ValueError("materialize item requires exact outbox and forbids reject/defer facts")
        elif self.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS:
            if self.outbox_id != self.command_id or any(value is None for value in reject):
                raise ValueError("synchronous reject requires terminal outbox and rejection evidence")
            if self.coordination_id is not None or candidate is not None:
                raise ValueError("synchronous reject cannot carry dependent-BUY facts")
        else:
            if (
                command.command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT
                or command.side is not SideV1.BUY
                or self.outbox_id is not None
                or self.coordination_id is None
                or candidate is None
                or any(value is not None for value in reject)
            ):
                raise ValueError(
                    "dependent-BUY defer requires BUY SUBMIT, candidate, coordination and no outbox/reject"
                )
        expected = hash_hex_v1(
            "miniqmt_product_command_authority_item_v3",
            self.canonical_payload_v1(exclude={"item_sha256"}),
        )
        if self.item_sha256 != expected:
            raise ValueError("product command authority item V3 hash mismatch")
        return self


class ProductCommandChildMappingStatusV1(StrEnum):
    DEFERRED_DEPENDENT_BUY = "DEFERRED_DEPENDENT_BUY"
    RESERVED = "RESERVED"
    TERMINAL = "TERMINAL"


class ProductCommandChildMappingV1(FrozenStrictModel):
    """K6-owned mapping carrier for the deferred boundary of one physical K2 row."""

    schema_version: Literal["miniqmt_product_command_child_mapping_v1"]
    mapping_id: IdentityV1
    authority_item_sha256: Sha256V1
    coordination_id: Sha256V1
    command_id: IdentityV1
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    parent_intent_id: IdentityV1
    strategy_slot_id: IdentityV1
    local_vt_orderid: IdentityV1
    child_order_id: IdentityV1
    deterministic_client_order_ref: IdentityV1
    order_remark: IdentityV1
    symbol: IdentityV1
    side: Literal[SideV1.BUY]
    requested_price_decimal: PositiveCanonicalDecimalV1
    requested_quantity: PositiveIntV1
    broker_order_id: None
    broker_identity_source_event_id: None
    mapping_status: ProductCommandChildMappingStatusV1
    mapping_version: PositiveIntV1
    payload_sha256: Sha256V1
    last_order_event_id: None
    last_trade_event_id: None
    created_transition_id: IdentityV1
    updated_by_event_id: IdentityV1 | None
    created_at_utc: UtcDateTimeV1
    updated_at_utc: UtcDateTimeV1
    mapping_receipt_sha256: Sha256V1

    @classmethod
    def create_deferred(
        cls,
        *,
        authority_item: ProductCommandAuthorityItemV3,
        strategy_slot_id: str,
        created_at_utc: Any,
    ) -> Self:
        if not isinstance(authority_item, ProductCommandAuthorityItemV3):
            raise TypeError("authority_item must be ProductCommandAuthorityItemV3")
        ProductCommandAuthorityItemV3.model_validate_json(authority_item.model_dump_json())
        command = authority_item.command_json
        if (
            authority_item.disposition is not ProductCommandDispositionV3.DEFER_DEPENDENT_BUY
            or authority_item.coordination_id is None
            or command.command_type is not BrokerCommandTypeV2.SUBMIT_LIMIT
            or command.side is not SideV1.BUY
        ):
            raise ValueError("deferred mapping requires exact dependent-BUY authority item")
        child_order_id = execution_child_order_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
        )
        mapping_id = command_child_mapping_id_v1(
            command_id=command.command_id,
            local_vt_orderid=command.local_vt_orderid,
            child_order_id=child_order_id,
        )
        if (authority_item.mapping_id, authority_item.child_order_id) != (mapping_id, child_order_id):
            raise ValueError("authority item mapping identity differs from command closure")
        client_ref = deterministic_client_order_ref_v1(command_id=command.command_id, mapping_id=mapping_id)
        canonical_created_at = canonical_utc_datetime_v1(created_at_utc, field_name="created_at_utc")
        payload_fields = {
            "authority_item_sha256": authority_item.item_sha256,
            "coordination_id": authority_item.coordination_id,
            "command_id": command.command_id,
            "runtime_id": command.runtime_id,
            "algo_instance_id": command.algo_instance_id,
            "parent_intent_id": command.parent_intent_id,
            "strategy_slot_id": strategy_slot_id,
            "local_vt_orderid": command.local_vt_orderid,
            "child_order_id": child_order_id,
            "deterministic_client_order_ref": client_ref,
            "order_remark": client_ref,
            "symbol": command.symbol,
            "side": command.side.value,
            "requested_price_decimal": command.price_decimal,
            "requested_quantity": command.quantity,
            "created_transition_id": command.transition_id,
        }
        payload = {
            "schema_version": "miniqmt_product_command_child_mapping_v1",
            "mapping_id": mapping_id,
            **payload_fields,
            "broker_order_id": None,
            "broker_identity_source_event_id": None,
            "mapping_status": ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY.value,
            "mapping_version": 1,
            "payload_sha256": hash_hex_v1("miniqmt_product_command_child_mapping_payload_v1", payload_fields),
            "last_order_event_id": None,
            "last_trade_event_id": None,
            "updated_by_event_id": None,
            "created_at_utc": canonical_created_at,
            "updated_at_utc": canonical_created_at,
        }
        return cls(
            **{
                **payload,
                "side": command.side,
                "mapping_status": ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY,
            },
            mapping_receipt_sha256=hash_hex_v1("miniqmt_product_command_child_mapping_receipt_v1", payload),
        )

    @classmethod
    def create_successor(
        cls,
        *,
        previous: ProductCommandChildMappingV1,
        mapping_status: Literal[
            ProductCommandChildMappingStatusV1.RESERVED,
            ProductCommandChildMappingStatusV1.TERMINAL,
        ],
        updated_by_event_id: str,
        updated_at_utc: Any,
    ) -> Self:
        if not isinstance(previous, ProductCommandChildMappingV1):
            raise TypeError("previous must be ProductCommandChildMappingV1")
        cls.model_validate_json(previous.model_dump_json())
        normalized_status = ProductCommandChildMappingStatusV1(mapping_status)
        if normalized_status is ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY:
            raise ValueError("deferred mapping successor must be RESERVED or TERMINAL")
        payload = previous.canonical_payload_v1(
            exclude={
                "mapping_status",
                "mapping_version",
                "updated_by_event_id",
                "updated_at_utc",
                "mapping_receipt_sha256",
            }
        )
        payload.update(
            {
                "mapping_status": normalized_status.value,
                "mapping_version": previous.mapping_version + 1,
                "updated_by_event_id": updated_by_event_id,
                "updated_at_utc": canonical_utc_datetime_v1(updated_at_utc, field_name="updated_at_utc"),
            }
        )
        successor = cls(
            **{**payload, "mapping_status": normalized_status},
            mapping_receipt_sha256=hash_hex_v1("miniqmt_product_command_child_mapping_receipt_v1", payload),
        )
        return successor.validate_successor_v1(previous)

    def immutable_mapping_payload_v1(self) -> dict[str, Any]:
        return {
            key: self.canonical_payload_v1()[key]
            for key in (
                "authority_item_sha256",
                "coordination_id",
                "command_id",
                "runtime_id",
                "algo_instance_id",
                "parent_intent_id",
                "strategy_slot_id",
                "local_vt_orderid",
                "child_order_id",
                "deterministic_client_order_ref",
                "order_remark",
                "symbol",
                "side",
                "requested_price_decimal",
                "requested_quantity",
                "created_transition_id",
            )
        }

    def validate_successor_v1(self, previous: ProductCommandChildMappingV1) -> Self:
        if not isinstance(previous, ProductCommandChildMappingV1):
            raise TypeError("previous must be ProductCommandChildMappingV1")
        type(self).model_validate_json(self.model_dump_json())
        type(self).model_validate_json(previous.model_dump_json())
        if previous.mapping_status is not ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY:
            raise ValueError("only DEFERRED_DEPENDENT_BUY mapping can advance")
        if self.mapping_status not in {
            ProductCommandChildMappingStatusV1.RESERVED,
            ProductCommandChildMappingStatusV1.TERMINAL,
        }:
            raise ValueError("deferred mapping successor must be RESERVED or TERMINAL")
        if (
            self.mapping_id != previous.mapping_id
            or self.immutable_mapping_payload_v1() != previous.immutable_mapping_payload_v1()
        ):
            raise ValueError("product mapping immutable business payload changed")
        if self.mapping_version != previous.mapping_version + 1:
            raise ValueError("product mapping_version must increment exactly once")
        return self

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        expected_child = execution_child_order_id_v1(
            command_id=self.command_id,
            local_vt_orderid=self.local_vt_orderid,
        )
        expected_mapping = command_child_mapping_id_v1(
            command_id=self.command_id,
            local_vt_orderid=self.local_vt_orderid,
            child_order_id=expected_child,
        )
        expected_ref = deterministic_client_order_ref_v1(command_id=self.command_id, mapping_id=expected_mapping)
        if (self.child_order_id, self.mapping_id) != (expected_child, expected_mapping):
            raise ValueError("product mapping identity does not match command/local closure")
        if self.deterministic_client_order_ref != expected_ref or self.order_remark != expected_ref:
            raise ValueError("product mapping client reference does not match deterministic closure")
        if self.payload_sha256 != hash_hex_v1(
            "miniqmt_product_command_child_mapping_payload_v1", self.immutable_mapping_payload_v1()
        ):
            raise ValueError("product mapping payload hash mismatch")
        initial = self.mapping_status is ProductCommandChildMappingStatusV1.DEFERRED_DEPENDENT_BUY
        if initial:
            if self.mapping_version != 1 or self.updated_by_event_id is not None:
                raise ValueError("initial deferred mapping requires version=1 without update event")
            if self.created_at_utc != self.updated_at_utc:
                raise ValueError("initial deferred mapping timestamps must be equal")
        else:
            if self.mapping_version != 2 or self.updated_by_event_id is None:
                raise ValueError("released or terminal product mapping requires version=2 and update event")
            if self.updated_at_utc <= self.created_at_utc:
                raise ValueError("product mapping successor must advance updated_at_utc")
        expected_receipt = hash_hex_v1(
            "miniqmt_product_command_child_mapping_receipt_v1",
            self.canonical_payload_v1(exclude={"mapping_receipt_sha256"}),
        )
        if self.mapping_receipt_sha256 != expected_receipt:
            raise ValueError("product mapping receipt hash mismatch")
        return self


class ProductCommandAuthoritySetV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_authority_set_v3"]
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
    ordered_items: tuple[ProductCommandAuthorityItemV3, ...]
    materialize_count: NonNegativeIntV1
    reject_count: NonNegativeIntV1
    defer_count: NonNegativeIntV1
    total_count: NonNegativeIntV1
    aggregate_disposition: ProductCommandAggregateDispositionV3
    authority_set_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        items = tuple(values["ordered_items"])
        counts = Counter(item.disposition for item in items)
        materialize_count = counts[ProductCommandDispositionV3.MATERIALIZE]
        reject_count = counts[ProductCommandDispositionV3.REJECT_SYNCHRONOUS]
        defer_count = counts[ProductCommandDispositionV3.DEFER_DEPENDENT_BUY]
        if not items:
            aggregate = ProductCommandAggregateDispositionV3.ZERO_COMMAND
        elif materialize_count == len(items):
            aggregate = ProductCommandAggregateDispositionV3.MATERIALIZE_ALL_ACCEPTED_COMMANDS
        elif reject_count == len(items):
            aggregate = ProductCommandAggregateDispositionV3.ALL_REJECTED
        elif defer_count == len(items):
            aggregate = ProductCommandAggregateDispositionV3.ALL_DEFERRED
        else:
            aggregate = ProductCommandAggregateDispositionV3.MIXED_PER_COMMAND
        payload = {
            "schema_version": "miniqmt_product_command_authority_set_v3",
            **values,
            "ordered_items": items,
            "materialize_count": materialize_count,
            "reject_count": reject_count,
            "defer_count": defer_count,
            "total_count": len(items),
            "aggregate_disposition": aggregate,
        }
        return cls(**payload, authority_set_sha256=hash_hex_v1("miniqmt_product_command_authority_set_v3", payload))

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
        counts = Counter(item.disposition for item in self.ordered_items)
        coordination_ids = tuple(
            item.coordination_id for item in self.ordered_items if item.coordination_id is not None
        )
        if len(coordination_ids) != len(set(coordination_ids)):
            raise ValueError("product authority V3 cannot create duplicate dependent-BUY coordination owners")
        actual_counts = (
            counts[ProductCommandDispositionV3.MATERIALIZE],
            counts[ProductCommandDispositionV3.REJECT_SYNCHRONOUS],
            counts[ProductCommandDispositionV3.DEFER_DEPENDENT_BUY],
            len(self.ordered_items),
        )
        if actual_counts != (self.materialize_count, self.reject_count, self.defer_count, self.total_count):
            raise ValueError("product authority V3 counts do not close to ordered items")
        expected_aggregate = (
            ProductCommandAggregateDispositionV3.ZERO_COMMAND
            if not self.ordered_items
            else ProductCommandAggregateDispositionV3.MATERIALIZE_ALL_ACCEPTED_COMMANDS
            if self.materialize_count == self.total_count
            else ProductCommandAggregateDispositionV3.ALL_REJECTED
            if self.reject_count == self.total_count
            else ProductCommandAggregateDispositionV3.ALL_DEFERRED
            if self.defer_count == self.total_count
            else ProductCommandAggregateDispositionV3.MIXED_PER_COMMAND
        )
        if self.aggregate_disposition is not expected_aggregate:
            raise ValueError("product authority V3 aggregate disposition mismatch")
        expected = hash_hex_v1(
            "miniqmt_product_command_authority_set_v3",
            self.canonical_payload_v1(exclude={"authority_set_sha256"}),
        )
        if self.authority_set_sha256 != expected:
            raise ValueError("product command authority set V3 hash mismatch")
        return self


class ProductCommandAuthorityEnvelopeV3(FrozenStrictModel):
    """Durable fresh-process authority for one product command aggregate."""

    schema_version: Literal["miniqmt_product_command_authority_envelope_v3"]
    authority_set: ProductCommandAuthoritySetV3
    creation_authority: VnpyFacadeAuthorityInputV2
    ordered_timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...]
    envelope_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        authority_set: ProductCommandAuthoritySetV3,
        creation_authority: VnpyFacadeAuthorityInputV2,
        ordered_timer_schedules: tuple[ExecutionAlgoTimerScheduleV1, ...],
    ) -> Self:
        if not isinstance(authority_set, ProductCommandAuthoritySetV3):
            raise TypeError("authority_set must be ProductCommandAuthoritySetV3")
        if not isinstance(creation_authority, VnpyFacadeAuthorityInputV2):
            raise TypeError("creation_authority must be VnpyFacadeAuthorityInputV2")
        if type(ordered_timer_schedules) is not tuple or any(
            not isinstance(item, ExecutionAlgoTimerScheduleV1) for item in ordered_timer_schedules
        ):
            raise TypeError("ordered_timer_schedules must be one strict timer schedule tuple")
        payload = {
            "schema_version": "miniqmt_product_command_authority_envelope_v3",
            "authority_set": authority_set,
            "creation_authority": creation_authority,
            "ordered_timer_schedules": ordered_timer_schedules,
        }
        return cls(
            **payload,
            envelope_sha256=hash_hex_v1("miniqmt_product_command_authority_envelope_v3", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        authority = self.authority_set
        creation = self.creation_authority
        if (
            authority.catalog_sha256 != creation.plugin_catalog_snapshot.catalog_sha256
            or authority.creation_binding_sha256 != creation.authority_input_sha256
            or authority.facade_conformance_set_sha256 != creation.facade_conformance_set_v2.receipt_set_sha256
        ):
            raise ValueError("product authority envelope creation authority differs from aggregate hashes")
        schedule_ids = tuple(item.schedule_id for item in self.ordered_timer_schedules)
        if len(schedule_ids) != len(set(schedule_ids)):
            raise ValueError("product authority envelope timer schedule identities must be unique")
        if any(item.algo_instance_id != authority.algo_instance_id for item in self.ordered_timer_schedules):
            raise ValueError("product authority envelope timer owner differs from aggregate")
        expected = hash_hex_v1(
            "miniqmt_product_command_authority_envelope_v3",
            self.canonical_payload_v1(exclude={"envelope_sha256"}),
        )
        if self.envelope_sha256 != expected:
            raise ValueError("product command authority envelope hash mismatch")
        return self


class ProductCommandLifecycleProjectionItemV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_lifecycle_projection_item_v3"]
    authority_item_sha256: Sha256V1
    effect_ordinal: NonNegativeIntV1
    command_id: IdentityV1
    disposition: ProductCommandDispositionV3
    mapping_id: IdentityV1
    outbox_id: IdentityV1 | None
    child_order_id: IdentityV1
    lifecycle_status: ProductLifecycleStatusV3
    last_committed_stage: IdentityV1
    broker_called: StrictBool | None
    qmt_order_id: IdentityV1 | None
    callback_watermark: IdentityV1 | None
    reconciliation_receipt_sha256: Sha256V1 | None
    item_projection_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_lifecycle_projection_item_v3",
            **values,
            "outbox_id": values.get("outbox_id"),
            "broker_called": values.get("broker_called"),
            "qmt_order_id": values.get("qmt_order_id"),
            "callback_watermark": values.get("callback_watermark"),
            "reconciliation_receipt_sha256": values.get("reconciliation_receipt_sha256"),
        }
        return cls(
            **payload,
            item_projection_sha256=hash_hex_v1("miniqmt_product_command_lifecycle_projection_item_v3", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if self.disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY:
            if (
                self.outbox_id is not None
                or self.lifecycle_status is not ProductLifecycleStatusV3.DEFERRED_DEPENDENT_BUY
            ):
                raise ValueError("deferred lifecycle must have no outbox and exact deferred status")
            if any(
                value is not None
                for value in (
                    self.broker_called,
                    self.qmt_order_id,
                    self.callback_watermark,
                    self.reconciliation_receipt_sha256,
                )
            ):
                raise ValueError("deferred lifecycle cannot carry broker, callback or reconciliation facts")
        elif self.outbox_id != self.command_id:
            raise ValueError("materialized/rejected lifecycle outbox must equal command identity")
        if self.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS:
            if (
                self.lifecycle_status is not ProductLifecycleStatusV3.SYNCHRONOUS_REJECTED
                or self.broker_called is not False
                or any(
                    value is not None
                    for value in (
                        self.qmt_order_id,
                        self.callback_watermark,
                        self.reconciliation_receipt_sha256,
                    )
                )
            ):
                raise ValueError("synchronous reject lifecycle requires terminal no-broker evidence")
        elif self.disposition is ProductCommandDispositionV3.MATERIALIZE:
            if self.lifecycle_status in {
                ProductLifecycleStatusV3.SYNCHRONOUS_REJECTED,
                ProductLifecycleStatusV3.DEFERRED_DEPENDENT_BUY,
            }:
                raise ValueError("materialized lifecycle status cannot use product-only reject/defer states")
            if self.qmt_order_id is not None and self.broker_called is not True:
                raise ValueError("accepted order identity requires broker_called=true")
            if self.lifecycle_status in {
                ProductLifecycleStatusV3.PENDING,
                ProductLifecycleStatusV3.CLAIMED,
                ProductLifecycleStatusV3.DISPATCHING,
            } and (
                self.broker_called is not None
                or self.qmt_order_id is not None
                or self.reconciliation_receipt_sha256 is not None
            ):
                raise ValueError("pre-dispatch lifecycle cannot claim broker outcome")
            if self.lifecycle_status is ProductLifecycleStatusV3.ACKED and (
                self.broker_called is not True or self.qmt_order_id is None
            ):
                raise ValueError("ACKED lifecycle requires broker call and qmt order identity")
            if self.lifecycle_status is ProductLifecycleStatusV3.ACKED_REJECTED and (
                self.broker_called is not True or self.qmt_order_id is not None
            ):
                raise ValueError("ACKED_REJECTED lifecycle requires broker call without accepted order identity")
            if self.lifecycle_status is ProductLifecycleStatusV3.FAILED_RETRYABLE and (
                self.broker_called is not False
                or self.qmt_order_id is not None
                or self.reconciliation_receipt_sha256 is not None
            ):
                raise ValueError("FAILED_RETRYABLE lifecycle requires pre-call failure evidence")
            if self.lifecycle_status in {
                ProductLifecycleStatusV3.OUTCOME_UNKNOWN,
                ProductLifecycleStatusV3.RECONCILING,
            } and (self.broker_called is not None or self.qmt_order_id is not None):
                raise ValueError("unresolved lifecycle cannot claim broker outcome")
        expected = hash_hex_v1(
            "miniqmt_product_command_lifecycle_projection_item_v3",
            self.canonical_payload_v1(exclude={"item_projection_sha256"}),
        )
        if self.item_projection_sha256 != expected:
            raise ValueError("product command lifecycle item V3 hash mismatch")
        return self


class ProductCommandLifecycleProjectionV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_command_lifecycle_projection_v3"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    authority_set_sha256: Sha256V1
    ordered_item_projections: tuple[ProductCommandLifecycleProjectionItemV3, ...]
    lifecycle_projection_sha256: Sha256V1

    @classmethod
    def create(cls, **values: Any) -> Self:
        payload = {
            "schema_version": "miniqmt_product_command_lifecycle_projection_v3",
            **values,
            "ordered_item_projections": tuple(values["ordered_item_projections"]),
        }
        return cls(
            **payload,
            lifecycle_projection_sha256=hash_hex_v1("miniqmt_product_command_lifecycle_projection_v3", payload),
        )

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        items = self.ordered_item_projections
        if len(items) > MAX_PRODUCT_COMMANDS:
            raise ValueError("product lifecycle projection exceeds maximum cardinality")
        if tuple(item.effect_ordinal for item in items) != tuple(range(len(items))):
            raise ValueError("product lifecycle projection order must follow contiguous authority ordinals")
        if len({item.command_id for item in items}) != len(items):
            raise ValueError("product lifecycle projection contains duplicate command")
        expected = hash_hex_v1(
            "miniqmt_product_command_lifecycle_projection_v3",
            self.canonical_payload_v1(exclude={"lifecycle_projection_sha256"}),
        )
        if self.lifecycle_projection_sha256 != expected:
            raise ValueError("product command lifecycle projection V3 hash mismatch")
        return self

    def validate_against_authority_v3(self, authority: ProductCommandAuthoritySetV3) -> None:
        if not isinstance(authority, ProductCommandAuthoritySetV3):
            raise TypeError("authority must be ProductCommandAuthoritySetV3")
        expected_owner = (
            authority.runtime_id,
            authority.algo_instance_id,
            authority.event_id,
            authority.delivery_id,
            authority.transition_id,
            authority.authority_set_sha256,
        )
        actual_owner = (
            self.runtime_id,
            self.algo_instance_id,
            self.event_id,
            self.delivery_id,
            self.transition_id,
            self.authority_set_sha256,
        )
        expected_items = tuple(
            (
                item.effect_ordinal,
                item.command_id,
                item.disposition,
                item.item_sha256,
                item.mapping_id,
                item.outbox_id,
                item.child_order_id,
            )
            for item in authority.ordered_items
        )
        actual_items = tuple(
            (
                item.effect_ordinal,
                item.command_id,
                item.disposition,
                item.authority_item_sha256,
                item.mapping_id,
                item.outbox_id,
                item.child_order_id,
            )
            for item in self.ordered_item_projections
        )
        if actual_owner != expected_owner or actual_items != expected_items:
            raise KernelProductContractError(
                "MINIQMT_K6_PRODUCT_AUTHORITY_LIFECYCLE_ORDER_DRIFT",
                "product lifecycle V3 differs from exact authority order",
                context={
                    "expected_owner": expected_owner,
                    "actual_owner": actual_owner,
                    "expected": expected_items,
                    "actual": actual_items,
                },
            )


class ProductMaterializationReceiptV3(FrozenStrictModel):
    schema_version: Literal["miniqmt_product_materialization_receipt_v3"]
    runtime_id: IdentityV1
    algo_instance_id: IdentityV1
    event_id: IdentityV1
    delivery_id: IdentityV1
    transition_id: IdentityV1
    authority_set_sha256: Sha256V1
    execution_projection_set_sha256: Sha256V1
    ordered_mapping_ids: tuple[IdentityV1, ...]
    ordered_materialized_outbox_ids: tuple[IdentityV1, ...]
    ordered_rejected_outbox_ids: tuple[IdentityV1, ...]
    ordered_deferred_coordination_ids: tuple[Sha256V1, ...]
    ordered_child_order_ids: tuple[IdentityV1, ...]
    zero_command: StrictBool
    repository_transaction_id: IdentityV1
    commit_outcome: Literal[ProductMaterializationCommitOutcomeV3.COMMITTED_READBACK_VERIFIED]
    independent_readback_sha256: Sha256V1
    receipt_sha256: Sha256V1

    @classmethod
    def create(
        cls,
        *,
        authority: ProductCommandAuthoritySetV3,
        repository_transaction_id: str,
        independent_readback_sha256: str,
    ) -> Self:
        if not isinstance(authority, ProductCommandAuthoritySetV3):
            raise TypeError("authority must be ProductCommandAuthoritySetV3")
        payload = {
            "schema_version": "miniqmt_product_materialization_receipt_v3",
            "runtime_id": authority.runtime_id,
            "algo_instance_id": authority.algo_instance_id,
            "event_id": authority.event_id,
            "delivery_id": authority.delivery_id,
            "transition_id": authority.transition_id,
            "authority_set_sha256": authority.authority_set_sha256,
            "execution_projection_set_sha256": authority.execution_projection_set_sha256,
            "ordered_mapping_ids": tuple(item.mapping_id for item in authority.ordered_items),
            "ordered_materialized_outbox_ids": tuple(
                item.outbox_id
                for item in authority.ordered_items
                if item.disposition is ProductCommandDispositionV3.MATERIALIZE
            ),
            "ordered_rejected_outbox_ids": tuple(
                item.outbox_id
                for item in authority.ordered_items
                if item.disposition is ProductCommandDispositionV3.REJECT_SYNCHRONOUS
            ),
            "ordered_deferred_coordination_ids": tuple(
                item.coordination_id
                for item in authority.ordered_items
                if item.disposition is ProductCommandDispositionV3.DEFER_DEPENDENT_BUY
            ),
            "ordered_child_order_ids": tuple(item.child_order_id for item in authority.ordered_items),
            "zero_command": authority.total_count == 0,
            "repository_transaction_id": repository_transaction_id,
            "commit_outcome": ProductMaterializationCommitOutcomeV3.COMMITTED_READBACK_VERIFIED,
            "independent_readback_sha256": independent_readback_sha256,
        }
        return cls(**payload, receipt_sha256=hash_hex_v1("miniqmt_product_materialization_receipt_v3", payload))

    @model_validator(mode="after")
    def _validate_contract(self) -> Self:
        if self.zero_command != (len(self.ordered_mapping_ids) == 0):
            raise ValueError("zero-command receipt does not close to mapping identities")
        for field_name in (
            "ordered_mapping_ids",
            "ordered_materialized_outbox_ids",
            "ordered_rejected_outbox_ids",
            "ordered_deferred_coordination_ids",
            "ordered_child_order_ids",
        ):
            values = getattr(self, field_name)
            if len(values) > MAX_PRODUCT_COMMANDS or len(set(values)) != len(values):
                raise ValueError(f"{field_name} must be unique and bounded")
        expected = hash_hex_v1(
            "miniqmt_product_materialization_receipt_v3",
            self.canonical_payload_v1(exclude={"receipt_sha256"}),
        )
        if self.receipt_sha256 != expected:
            raise ValueError("product materialization receipt V3 hash mismatch")
        return self

    def validate_against_authority_v3(self, authority: ProductCommandAuthoritySetV3) -> None:
        expected = ProductMaterializationReceiptV3.create(
            authority=authority,
            repository_transaction_id=self.repository_transaction_id,
            independent_readback_sha256=self.independent_readback_sha256,
        )
        if canonical_json_bytes_v1(self.model_dump(mode="json")) != canonical_json_bytes_v1(
            expected.model_dump(mode="json")
        ):
            raise KernelProductContractError(
                "MINIQMT_K6_PRODUCT_MATERIALIZATION_IDENTITY_DRIFT",
                "materialization receipt V3 differs from exact authority association",
                context={"authority_set_sha256": authority.authority_set_sha256},
            )


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
    "DependentBuyCandidateAuthorityV2",
    "DependentBuyCoordinationStatusV1",
    "DependentBuyCoordinationV1",
    "DependentBuyCoordinationV2",
    "DependentBuyDecisionV1",
    "DependentBuyDependencyStatusV1",
    "DependentBuyLedgerObservationV1",
    "DependentBuyLedgerObservationV2",
    "DependentBuyReleaseDecisionV1",
    "DependentBuyReleaseDecisionV2",
    "DependentBuySettledProceedsRefV2",
    "DependentBuySellDependencyV1",
    "DependentBuySellDependencyV2",
    "DependentBuyTriggerEventRefV1",
    "DependentBuyTriggerTypeV1",
    "KernelProductContractError",
    "ProductCommandAggregateDispositionV2",
    "ProductCommandAggregateDispositionV3",
    "ProductCommandAuthorityItemV2",
    "ProductCommandAuthorityItemV3",
    "ProductCommandAuthorityEnvelopeV3",
    "ProductCommandAuthoritySetV2",
    "ProductCommandAuthoritySetV3",
    "ProductCommandChildMappingStatusV1",
    "ProductCommandChildMappingV1",
    "ProductCommandDispositionV2",
    "ProductCommandDispositionV3",
    "ProductCommandEvaluationEvidenceV3",
    "ProductCommandLifecycleProjectionItemV2",
    "ProductCommandLifecycleProjectionItemV3",
    "ProductCommandLifecycleProjectionV2",
    "ProductCommandLifecycleProjectionV3",
    "ProductLifecycleStatusV2",
    "ProductLifecycleStatusV3",
    "ProductMaterializationCommitOutcomeV2",
    "ProductMaterializationCommitOutcomeV3",
    "ProductMaterializationReceiptV2",
    "ProductMaterializationReceiptV3",
    "ProductRouteCutoverReceiptV1",
    "ProductRouteOwnerKindV1",
    "ProductRouteOwnerV1",
    "hash_hex_v1",
    "validate_kernel_product_payload_v1",
]
