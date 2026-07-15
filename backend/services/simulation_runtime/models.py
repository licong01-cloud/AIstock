"""Broker-neutral simulation runtime release and binding models."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import RuntimeConfigInvalidError
from backend.services.trading_core.models import OrderSide
from backend.services.miniqmt_execution_runtime.b0_quote_v2 import (
    QUOTE_CONTROL_BINDING_KEY,
    QuoteControlBindingV1,
)


DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID = "platform_default_daily_strategy_profile_v1"
CANONICAL_MINIQMT_RUNTIME_OWNER = "MiniQMTExecutionRuntime"

ALPHA_SIGNAL_FORBIDDEN_KEYS = frozenset(
    {
        "broker_account_id",
        "account_group_id",
        "strategy_name",
        "order_remark",
        "qmt_order_id",
        "order_type",
        "price_type",
        "limit_price",
        "execution_algo_code",
        "execution_policy_id",
        "execution_policy_sha256",
        "tail_policy_id",
        "cash_freeze",
        "position_lot",
        "available_quantity",
        "broker_can_sell",
        "native_status",
        "native_context",
        "native_order_id",
        "raw_status",
        "raw_packet",
        "miniqmt_native_status",
        "miniqmt_raw_packet",
    }
)

FIXED_STRATEGY_COUNT_GATE_KEYS = frozenset(
    {
        "max_concurrent_packages",
        "max_concurrent_strategies",
        "max_strategy_count",
        "strategy_count_limit",
        "package_count_limit",
        "package_count_gate",
    }
)

ALPHA_CORE_RELEASE_FORBIDDEN_KEYS = frozenset(
    {
        "factor_set",
        "feature_schema",
        "model_asset",
        "model_assets",
        "model_weights",
        "alpha_components",
        "alpha_combination_policy",
        "training_assets",
        "training_config",
        "source_lineage",
    }
)

BROKER_BINDING_RELEASE_FORBIDDEN_KEYS = frozenset(
    {
        "broker_backend",
        "target_broker_backend",
        "broker_account_id",
        "account_id",
        "account_group_id",
        "strategy_slot_id",
        "capital_allocation",
        "initial_cash",
        "strategy_name",
        "order_remark",
        "order_remark_prefix",
    }
)

SELECTION_ONLY_FORBIDDEN_KEYS = frozenset(
    {
        "broker_backend",
        "target_broker_backend",
        "broker_account_id",
        "account_id",
        "account_group_id",
        "strategy_slot_id",
        "capital_allocation",
        "capital",
        "initial_cash",
        "cash",
        "total_equity",
        "strategy_name",
        "order_remark",
        "order_remark_prefix",
        "execution_policy",
        "validated_execution_policy",
        "execution_policy_id",
        "execution_policy_version_id",
        "minute_execution_policy",
        "tail_policy",
        "tail_policy_id",
        "tail_policy_version_id",
        "target_position",
        "target_positions",
        "rebalance_intent",
        "rebalance_intents",
        "order_intent",
        "order_intents",
        "execution_plan",
        "broker_order",
        "broker_orders",
        "current_positions",
        "positions",
        "available_quantity",
        "t1_available",
        "t_plus_one",
        "board_lot",
        "order_quantity",
        "quantity",
    }
)

POLICY_BINDING_FORBIDDEN_KEYS = frozenset(
    {
        "runtime_profile_id",
        "runtime_profile_version_id",
        "runtime_profile_sha256",
        "daily_strategy_profile_version_id",
        "execution_policy_version_id",
        "execution_policy_id",
        "execution_policy_sha256",
        "tail_policy_version_id",
        "tail_policy_id",
        "tail_policy_sha256",
    }
)

SIMULATION_RELEASE_BINDING_CONFIG_KEYS = frozenset(
    {
        "schema_version",
        "strategy_id",
        "release_id",
        "release_hash",
        "package_id",
        "manifest_sha256",
        "broker_backend",
        "broker_account_id",
        "account_group_id",
        "strategy_slot_id",
        "capital_allocation",
        "strategy_name",
        "order_remark_prefix",
        "approval_state",
        QUOTE_CONTROL_BINDING_KEY,
        "metadata",
    }
)


class ExecutionPathNotCanonicalError(RuntimeConfigInvalidError):
    error_code = "EXECUTION_PATH_NOT_CANONICAL"


class MiniQMTUnsupportedExecutionAlgoError(RuntimeConfigInvalidError):
    error_code = "MINIQMT_UNSUPPORTED_EXECUTION_ALGO"


class RuntimeReleaseValidationState(str, Enum):
    DRAFT = "DRAFT"
    SIM_VALIDATING = "SIM_VALIDATING"
    SIM_PASSED = "SIM_PASSED"
    LIVE_APPROVAL_PENDING = "LIVE_APPROVAL_PENDING"
    LIVE_APPROVED = "LIVE_APPROVED"
    RETIRED = "RETIRED"


class SimulationBindingApprovalState(str, Enum):
    DRAFT = "DRAFT"
    SIM_VALIDATING = "SIM_VALIDATING"
    SIM_PASSED = "SIM_PASSED"
    LIVE_APPROVAL_PENDING = "LIVE_APPROVAL_PENDING"
    LIVE_APPROVED = "LIVE_APPROVED"
    RETIRED = "RETIRED"


class SimulationBrokerBackend(str, Enum):
    LOCAL_SIM = "local_sim"
    MINIQMT_SIM = "minqmt_sim"


class SimulationDailyRunStatus(str, Enum):
    CREATED = "CREATED"
    PRECHECKING = "PRECHECKING"
    SIGNAL_GENERATING = "SIGNAL_GENERATING"
    TARGET_GENERATING = "TARGET_GENERATING"
    PLANNING_EXECUTION = "PLANNING_EXECUTION"
    SUBMITTING = "SUBMITTING"
    INTRADAY_RUNNING = "INTRADAY_RUNNING"
    TAIL_HANDLING = "TAIL_HANDLING"
    RECONCILING = "RECONCILING"
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"
    CANCELLED = "CANCELLED"


def canonical_json_sha256(payload: dict[str, Any] | list[Any]) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def find_forbidden_key_paths(payload: Any, forbidden_keys: frozenset[str], *, path: str = "") -> list[str]:
    matches: list[str] = []
    if isinstance(payload, dict):
        for key, value in payload.items():
            text_key = str(key)
            key_path = f"{path}.{text_key}" if path else text_key
            if text_key in forbidden_keys:
                matches.append(key_path)
            matches.extend(find_forbidden_key_paths(value, forbidden_keys, path=key_path))
    elif isinstance(payload, list):
        for index, value in enumerate(payload):
            key_path = f"{path}[{index}]" if path else f"[{index}]"
            matches.extend(find_forbidden_key_paths(value, forbidden_keys, path=key_path))
    return matches


def assert_release_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    forbidden = ALPHA_CORE_RELEASE_FORBIDDEN_KEYS | BROKER_BINDING_RELEASE_FORBIDDEN_KEYS
    matches = find_forbidden_key_paths(payload, forbidden)
    if matches:
        raise RuntimeConfigInvalidError(
            "StrategyRuntimeRelease cannot contain alpha-core or broker-binding fields",
            context={**(context or {}), "forbidden_paths": matches, "forbidden_keys": sorted(forbidden)},
        )


def assert_binding_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    unknown = sorted(set(payload).difference(SIMULATION_RELEASE_BINDING_CONFIG_KEYS))
    if unknown:
        raise RuntimeConfigInvalidError(
            "SimulationReleaseBinding contains unsupported top-level fields",
            context={
                **(context or {}),
                "unknown_fields": unknown,
                "allowed_fields": sorted(SIMULATION_RELEASE_BINDING_CONFIG_KEYS),
            },
        )
    forbidden = ALPHA_CORE_RELEASE_FORBIDDEN_KEYS | POLICY_BINDING_FORBIDDEN_KEYS
    matches = find_forbidden_key_paths(payload, forbidden)
    if matches:
        raise RuntimeConfigInvalidError(
            "SimulationReleaseBinding cannot contain alpha-core or runtime-policy fields",
            context={**(context or {}), "forbidden_paths": matches, "forbidden_keys": sorted(forbidden)},
        )
    QuoteControlBindingV1.from_binding_config(payload)


def assert_selection_only_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    matches = find_forbidden_key_paths(payload, SELECTION_ONLY_FORBIDDEN_KEYS)
    if matches:
        raise RuntimeConfigInvalidError(
            "Selection-only signal generation cannot contain broker, capital, target, rebalance or execution fields",
            context={
                **(context or {}),
                "forbidden_paths": matches,
                "forbidden_keys": sorted(SELECTION_ONLY_FORBIDDEN_KEYS),
            },
        )


def assert_alpha_signal_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    matches = find_forbidden_key_paths(payload, ALPHA_SIGNAL_FORBIDDEN_KEYS)
    if matches:
        raise RuntimeConfigInvalidError(
            "AlphaSignalBook cannot contain broker, order, execution, OMS or MiniQMT-native fields",
            context={
                **(context or {}),
                "forbidden_paths": matches,
                "forbidden_keys": sorted(ALPHA_SIGNAL_FORBIDDEN_KEYS),
            },
        )


def assert_no_fixed_strategy_count_gate(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    matches = find_forbidden_key_paths(payload, FIXED_STRATEGY_COUNT_GATE_KEYS)
    if matches:
        raise RuntimeConfigInvalidError(
            "MiniQMT strategy count must be governed by capital capacity and trading rules, not fixed package gates",
            context={
                **(context or {}),
                "forbidden_paths": matches,
                "forbidden_keys": sorted(FIXED_STRATEGY_COUNT_GATE_KEYS),
                "allowed_gate": "funds_and_trading_rules_only",
            },
        )


def assert_canonical_miniqmt_runtime_gate(runtime_owner: str | None, *, context: dict[str, Any] | None = None) -> None:
    owner = str(runtime_owner or "").strip()
    if owner != CANONICAL_MINIQMT_RUNTIME_OWNER:
        raise ExecutionPathNotCanonicalError(
            "MiniQMT product execution must enter the canonical MiniQMTExecutionRuntime",
            context={
                **(context or {}),
                "runtime_owner": owner,
                "required_runtime_owner": CANONICAL_MINIQMT_RUNTIME_OWNER,
            },
        )


class AlphaSignalItem(BaseModel):
    """Broker-neutral alpha candidate generated by a signal source."""

    model_config = ConfigDict(extra="forbid")

    signal_id: str | None = None
    signal_hash: str | None = None
    symbol: str
    side: Literal["BUY", "SELL"]
    rank: int | None = Field(default=None, ge=1)
    score: float | None = None
    target_weight: float | None = Field(default=None, ge=0)
    confidence: float | None = Field(default=None, ge=0, le=1)
    replacement_for: str | None = None
    reason: str | None = None
    exposures: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _reject_broker_execution_fields_before_parse(cls, data: Any) -> Any:
        if isinstance(data, dict):
            assert_alpha_signal_payload_boundary(data, context={"model": cls.__name__})
        return data

    @field_validator("symbol")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("signal_id", "signal_hash", "replacement_for", "reason")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _signal_hash_matches_payload(self) -> "AlphaSignalItem":
        assert_alpha_signal_payload_boundary(
            self.model_dump(mode="json", exclude_none=True),
            context={"model": self.__class__.__name__, "symbol": self.symbol},
        )
        digest = canonical_json_sha256(self.canonical_payload())
        if self.signal_hash is not None and self.signal_hash != digest:
            raise ValueError("signal_hash does not match canonical alpha signal item payload")
        object.__setattr__(self, "signal_hash", digest)
        if self.signal_id is None:
            object.__setattr__(self, "signal_id", f"asig_{digest[:16]}")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "alpha_signal_item_v1",
            "symbol": self.symbol,
            "side": self.side,
            "rank": self.rank,
            "score": self.score,
            "target_weight": self.target_weight,
            "confidence": self.confidence,
            "replacement_for": self.replacement_for,
            "reason": self.reason,
            "exposures": self.exposures,
            "metadata": self.metadata,
        }


class AlphaSignalBook(BaseModel):
    """Canonical broker-neutral signal artifact produced by an alpha source."""

    model_config = ConfigDict(extra="forbid")

    book_id: str | None = None
    signal_hash: str | None = None
    package_id: str
    manifest_sha256: str
    trade_date: date
    cutoff_date: date | None = None
    as_of: datetime | None = None
    release_id: str | None = None
    release_hash: str | None = None
    source_type: str
    data_source: str | None = None
    valid_no_candidate: bool = False
    items: list[AlphaSignalItem] = Field(default_factory=list)
    risk_tags: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="before")
    @classmethod
    def _reject_broker_execution_fields_before_parse(cls, data: Any) -> Any:
        if isinstance(data, dict):
            assert_alpha_signal_payload_boundary(data, context={"model": cls.__name__})
        return data

    @field_validator("package_id", "manifest_sha256", "source_type")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("book_id", "signal_hash", "release_id", "release_hash", "data_source")
    @classmethod
    def _strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        return value

    @model_validator(mode="after")
    def _book_hash_matches_payload(self) -> "AlphaSignalBook":
        if self.package_id is None or self.manifest_sha256 is None or self.source_type is None:
            raise ValueError("package_id, manifest_sha256 and source_type are required")
        assert_alpha_signal_payload_boundary(
            self.model_dump(mode="json", exclude_none=True),
            context={"model": self.__class__.__name__, "package_id": self.package_id},
        )
        digest = canonical_json_sha256(self.canonical_payload())
        if self.signal_hash is not None and self.signal_hash != digest:
            raise ValueError("signal_hash does not match canonical alpha signal book payload")
        expected_id = f"asb_{digest[:16]}"
        if self.book_id is not None and self.book_id != expected_id:
            raise ValueError("book_id does not match signal_hash")
        object.__setattr__(self, "signal_hash", digest)
        object.__setattr__(self, "book_id", expected_id)
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "alpha_signal_book_v1",
            "package_id": self.package_id,
            "manifest_sha256": self.manifest_sha256,
            "trade_date": self.trade_date.isoformat(),
            "cutoff_date": self.cutoff_date.isoformat() if self.cutoff_date else None,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "release_id": self.release_id,
            "release_hash": self.release_hash,
            "source_type": self.source_type,
            "data_source": self.data_source,
            "valid_no_candidate": self.valid_no_candidate,
            "items": [item.canonical_payload() for item in self.items],
            "risk_tags": self.risk_tags,
            "metadata": self.metadata,
        }


class StrategySlotTarget(BaseModel):
    """Execution-layer slot target derived from one broker-neutral signal book."""

    model_config = ConfigDict(extra="forbid")

    target_id: str | None = None
    account_group_id: str
    strategy_slot_id: str
    strategy_id: str
    package_id: str
    alpha_signal_book_id: str
    target_trade_date: date
    capital_allocation: float = Field(gt=0)
    desired_weights: dict[str, float] = Field(default_factory=dict)
    capacity_status: Literal[
        "ACTIVE",
        "SKIPPED_INSUFFICIENT_CAPITAL",
        "PARTIAL_CAPITAL_ALLOCATED",
        "MIN_LOT_NOT_REACHED",
        "SELL_PROCEEDS_REQUIRED",
    ] = "ACTIVE"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="before")
    @classmethod
    def _reject_fixed_count_gate_before_parse(cls, data: Any) -> Any:
        if isinstance(data, dict):
            assert_no_fixed_strategy_count_gate(data, context={"model": cls.__name__})
        return data

    @field_validator("account_group_id", "strategy_slot_id", "strategy_id", "package_id", "alpha_signal_book_id")
    @classmethod
    def _required_text(cls, value: str | None) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("target_id")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _target_id_matches_payload(self) -> "StrategySlotTarget":
        assert_no_fixed_strategy_count_gate(
            self.model_dump(mode="json", exclude_none=True),
            context={"model": self.__class__.__name__, "strategy_slot_id": self.strategy_slot_id},
        )
        digest = canonical_json_sha256(self.canonical_payload())
        expected_id = f"sst_{digest[:16]}"
        if self.target_id is not None and self.target_id != expected_id:
            raise ValueError("target_id does not match canonical strategy slot target payload")
        object.__setattr__(self, "target_id", expected_id)
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "strategy_slot_target_v1",
            "account_group_id": self.account_group_id,
            "strategy_slot_id": self.strategy_slot_id,
            "strategy_id": self.strategy_id,
            "package_id": self.package_id,
            "alpha_signal_book_id": self.alpha_signal_book_id,
            "target_trade_date": self.target_trade_date.isoformat(),
            "capital_allocation": self.capital_allocation,
            "desired_weights": self.desired_weights,
            "capacity_status": self.capacity_status,
            "metadata": self.metadata,
        }


class OperatorCommand(BaseModel):
    """Auditable operator command that must be routed through MiniQMTExecutionRuntime."""

    model_config = ConfigDict(extra="forbid")

    command_id: str = Field(default_factory=lambda: f"opcmd_{uuid4().hex}")
    command_type: Literal[
        "FLATTEN_ALL_POSITIONS",
        "FLATTEN_STRATEGY_SLOT",
        "CANCEL_ALL_OPEN_ORDERS",
        "RESET_STRATEGY_SLOT",
        "RECONCILE_STALE_RUNTIME_NO_BROKER_SIDE_EFFECT",
        "REPLACE_ALPHA_SIGNAL_BOOK",
    ]
    account_group_id: str
    strategy_slot_id: str | None = None
    alpha_signal_book_id: str | None = None
    requested_by: str | None = None
    reason: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("command_id", "account_group_id", "reason")
    @classmethod
    def _required_text(cls, value: str | None) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("strategy_slot_id", "alpha_signal_book_id", "requested_by")
    @classmethod
    def _strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _requires_reason(self) -> "OperatorCommand":
        if not self.account_group_id or not self.reason:
            raise ValueError("account_group_id and reason are required")
        if self.command_type in {"FLATTEN_STRATEGY_SLOT", "RESET_STRATEGY_SLOT"} and not self.strategy_slot_id:
            raise ValueError(f"{self.command_type} requires strategy_slot_id")
        if self.command_type == "REPLACE_ALPHA_SIGNAL_BOOK" and not self.alpha_signal_book_id:
            raise ValueError("REPLACE_ALPHA_SIGNAL_BOOK requires alpha_signal_book_id")
        return self


class MiniQMTExecutionRuntimeRequest(BaseModel):
    """Canonical request envelope for the future single MiniQMT product execution path."""

    model_config = ConfigDict(extra="forbid")

    request_id: str = Field(default_factory=lambda: f"mqrt_{uuid4().hex}")
    schema_version: Literal["miniqmt_execution_runtime_request_v1"] = "miniqmt_execution_runtime_request_v1"
    runtime_owner: Literal["MiniQMTExecutionRuntime"] = CANONICAL_MINIQMT_RUNTIME_OWNER
    account_group_id: str
    trade_date: date
    alpha_signal_books: list[AlphaSignalBook]
    strategy_slot_targets: list[StrategySlotTarget]
    operator_commands: list[OperatorCommand] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="before")
    @classmethod
    def _reject_noncanonical_or_fixed_gate_before_parse(cls, data: Any) -> Any:
        if isinstance(data, dict):
            assert_canonical_miniqmt_runtime_gate(
                data.get("runtime_owner", CANONICAL_MINIQMT_RUNTIME_OWNER),
                context={"model": cls.__name__},
            )
            assert_no_fixed_strategy_count_gate(data, context={"model": cls.__name__})
        return data

    @field_validator("request_id", "account_group_id")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @model_validator(mode="after")
    def _runtime_request_is_consistent(self) -> "MiniQMTExecutionRuntimeRequest":
        assert_canonical_miniqmt_runtime_gate(self.runtime_owner, context={"request_id": self.request_id})
        assert_no_fixed_strategy_count_gate(
            self.model_dump(mode="json", exclude_none=True),
            context={"model": self.__class__.__name__, "request_id": self.request_id},
        )
        if not self.alpha_signal_books:
            raise ValueError("MiniQMTExecutionRuntimeRequest requires at least one AlphaSignalBook")
        if not self.strategy_slot_targets and not self.operator_commands:
            raise ValueError("MiniQMTExecutionRuntimeRequest requires slot targets or operator commands")
        if any(book.trade_date != self.trade_date for book in self.alpha_signal_books):
            raise ValueError("all AlphaSignalBook trade_date values must match request trade_date")
        if any(target.target_trade_date != self.trade_date for target in self.strategy_slot_targets):
            raise ValueError("all StrategySlotTarget target_trade_date values must match request trade_date")
        return self


class StrategyRuntimeRelease(BaseModel):
    """Immutable broker-neutral runtime release for one StrategyPackage alpha core."""

    model_config = ConfigDict(extra="forbid")

    release_id: str = Field(default_factory=lambda: f"srr_{uuid4().hex}")
    package_id: str
    manifest_sha256: str
    base_release_id: str | None = None
    runtime_profile_id: str
    runtime_profile_version_id: str
    runtime_profile_sha256: str
    daily_strategy_profile_version_id: str
    execution_policy_version_id: str
    execution_policy_sha256: str
    tail_policy_version_id: str
    tail_policy_sha256: str
    release_config_json: dict[str, Any]
    release_hash: str | None = None
    validation_state: RuntimeReleaseValidationState = RuntimeReleaseValidationState.DRAFT
    validation_evidence: dict[str, Any] = Field(default_factory=dict)
    effective_from: date | None = None
    effective_to: date | None = None
    created_by: str | None = None
    created_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "release_id",
        "package_id",
        "manifest_sha256",
        "runtime_profile_id",
        "runtime_profile_version_id",
        "runtime_profile_sha256",
        "daily_strategy_profile_version_id",
        "execution_policy_version_id",
        "execution_policy_sha256",
        "tail_policy_version_id",
        "tail_policy_sha256",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("base_release_id", "created_by", "created_reason")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _canonical_hash_matches_config(self) -> "StrategyRuntimeRelease":
        assert_release_payload_boundary(
            self.release_config_json,
            context={"release_id": self.release_id, "package_id": self.package_id},
        )
        digest = canonical_json_sha256(self.release_config_json)
        if self.release_hash is not None and self.release_hash != digest:
            raise ValueError("release_hash does not match release_config_json")
        object.__setattr__(self, "release_hash", digest)
        return self


class SimulationReleaseBinding(BaseModel):
    """Immutable backend/account/capital binding for a StrategyRuntimeRelease."""

    model_config = ConfigDict(extra="forbid")

    binding_id: str = Field(default_factory=lambda: f"simbind_{uuid4().hex}")
    strategy_id: str
    release_id: str
    release_hash: str
    package_id: str
    manifest_sha256: str
    broker_backend: SimulationBrokerBackend
    broker_account_id: str | None = None
    account_group_id: str | None = None
    strategy_slot_id: str | None = None
    capital_allocation: float = Field(gt=0)
    strategy_name: str | None = None
    order_remark_prefix: str | None = None
    effective_from: date | None = None
    effective_to: date | None = None
    approval_state: SimulationBindingApprovalState = SimulationBindingApprovalState.DRAFT
    binding_config_json: dict[str, Any]
    binding_hash: str | None = None
    created_by: str | None = None
    created_reason: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("binding_id", "strategy_id", "release_id", "release_hash", "package_id", "manifest_sha256")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator(
        "broker_account_id",
        "account_group_id",
        "strategy_slot_id",
        "strategy_name",
        "order_remark_prefix",
        "created_by",
        "created_reason",
    )
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _canonical_hash_matches_config(self) -> "SimulationReleaseBinding":
        assert_binding_payload_boundary(
            self.binding_config_json,
            context={"binding_id": self.binding_id, "strategy_id": self.strategy_id},
        )
        digest = canonical_json_sha256(self.binding_config_json)
        if self.binding_hash is not None and self.binding_hash != digest:
            raise ValueError("binding_hash does not match binding_config_json")
        object.__setattr__(self, "binding_hash", digest)
        return self


class DailySelectionEvidence(BaseModel):
    """Auditable broker-neutral evidence for one daily StrategyPackage selection signal."""

    model_config = ConfigDict(extra="forbid")

    evidence_id: str
    target_trade_date: date
    cutoff_date: date | None = None
    package_id: str
    manifest_sha256: str
    release_id: str | None = None
    release_hash: str | None = None
    runtime_profile_version_id: str
    runtime_profile_hash: str
    source_type: str
    data_source: str
    candidate_count: int = Field(ge=0)
    excluded_count: int = Field(ge=0)
    artifact_hash: str
    evidence_payload_json: dict[str, Any]
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    created_by: str | None = None

    @field_validator(
        "evidence_id",
        "package_id",
        "manifest_sha256",
        "runtime_profile_version_id",
        "runtime_profile_hash",
        "source_type",
        "data_source",
        "artifact_hash",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator("release_id", "release_hash", "created_by")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @model_validator(mode="after")
    def _artifact_hash_matches_payload(self) -> "DailySelectionEvidence":
        assert_selection_only_payload_boundary(
            self.evidence_payload_json,
            context={"evidence_id": self.evidence_id, "package_id": self.package_id},
        )
        digest = canonical_json_sha256(self.evidence_payload_json)
        if self.artifact_hash != digest:
            raise ValueError("artifact_hash does not match evidence_payload_json")
        expected_id = f"dse_{digest[:16]}"
        if self.evidence_id != expected_id:
            raise ValueError("evidence_id does not match artifact_hash")
        return self


class TradingRuleDecision(BaseModel):
    """Single authoritative trading-rule decision for one intended order."""

    model_config = ConfigDict(extra="forbid")

    decision_id: str
    symbol: str
    market_board: str
    side: OrderSide
    requested_quantity: int = Field(ge=0)
    legal_quantity: int = Field(ge=0)
    lot_rule: dict[str, Any]
    price_limit_rule: dict[str, Any] = Field(default_factory=dict)
    tplus1_available_quantity: int | None = Field(default=None, ge=0)
    decision: Literal["EMIT", "ADJUST", "REJECT"]
    reason_code: str
    source_version: str
    decision_hash: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("decision_id", "symbol", "market_board", "reason_code", "source_version", "decision_hash")
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @model_validator(mode="after")
    def _decision_id_matches_payload(self) -> "TradingRuleDecision":
        payload = self.canonical_payload()
        digest = canonical_json_sha256(payload)
        if self.decision_hash != digest:
            raise ValueError("decision_hash does not match canonical payload")
        expected_id = f"trd_{digest[:16]}"
        if self.decision_id != expected_id:
            raise ValueError("decision_id does not match decision_hash")
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": "trading_rule_decision_v1",
            "symbol": self.symbol,
            "market_board": self.market_board,
            "side": self.side.value,
            "requested_quantity": self.requested_quantity,
            "legal_quantity": self.legal_quantity,
            "lot_rule": self.lot_rule,
            "price_limit_rule": self.price_limit_rule,
            "tplus1_available_quantity": self.tplus1_available_quantity,
            "decision": self.decision,
            "reason_code": self.reason_code,
            "source_version": self.source_version,
        }


class ExecutionPlanIntent(BaseModel):
    """Broker-neutral order instruction compiled from a shared rebalance intent."""

    model_config = ConfigDict(extra="forbid")

    intent_id: str
    plan_id: str
    strategy_id: str
    portfolio_id: str
    package_id: str
    release_id: str
    release_hash: str
    binding_id: str
    binding_hash: str
    account_group_id: str | None = None
    strategy_slot_id: str | None = None
    symbol: str
    side: OrderSide
    target_quantity: int = Field(ge=0)
    delta_quantity: int
    order_quantity: int = Field(gt=0)
    target_weight: float | None = Field(default=None, gt=0)
    current_quantity: int = Field(ge=0)
    current_available_quantity: int | None = Field(default=None, ge=0)
    rebalance_reason: str
    trading_rule_decision_id: str
    schedule_window: dict[str, Any]
    price_policy: dict[str, Any]
    risk_context: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator(
        "intent_id",
        "plan_id",
        "strategy_id",
        "portfolio_id",
        "package_id",
        "release_id",
        "release_hash",
        "binding_id",
        "binding_hash",
        "symbol",
        "rebalance_reason",
        "trading_rule_decision_id",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value


class ExecutionPlan(BaseModel):
    """Shared execution plan consumed by LocalSim and MiniQMT broker bridges."""

    model_config = ConfigDict(extra="forbid")

    plan_id: str
    strategy_id: str
    portfolio_id: str
    package_id: str
    release_id: str
    release_hash: str
    binding_id: str
    binding_hash: str
    account_group_id: str | None = None
    strategy_slot_id: str | None = None
    selection_evidence_id: str
    selection_evidence_hash: str
    target_trade_date: date
    execution_policy_version_id: str
    execution_policy_sha256: str
    tail_policy_version_id: str
    tail_policy_sha256: str
    intents: list[ExecutionPlanIntent]
    trading_rule_decisions: list[TradingRuleDecision]
    plan_payload_json: dict[str, Any]
    plan_hash: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "plan_id",
        "strategy_id",
        "portfolio_id",
        "package_id",
        "release_id",
        "release_hash",
        "binding_id",
        "binding_hash",
        "selection_evidence_id",
        "selection_evidence_hash",
        "execution_policy_version_id",
        "execution_policy_sha256",
        "tail_policy_version_id",
        "tail_policy_sha256",
        "plan_hash",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @model_validator(mode="after")
    def _plan_hash_matches_payload(self) -> "ExecutionPlan":
        digest = canonical_json_sha256(self.plan_payload_json)
        if self.plan_hash != digest:
            raise ValueError("plan_hash does not match plan_payload_json")
        expected_id = f"plan_{digest[:16]}"
        if self.plan_id != expected_id:
            raise ValueError("plan_id does not match plan_hash")
        for intent in self.intents:
            if intent.plan_id != self.plan_id:
                raise ValueError("execution plan intent plan_id does not match plan_id")
        return self


class SimulationDailyRun(BaseModel):
    """Unified daily simulation lifecycle row for LocalSim and MiniQMT paths."""

    model_config = ConfigDict(extra="forbid")

    run_id: str
    trade_date: date
    strategy_id: str
    broker_backend: SimulationBrokerBackend
    package_id: str
    manifest_sha256: str
    release_id: str
    release_hash: str
    binding_id: str
    binding_hash: str
    account_group_id: str | None = None
    strategy_slot_id: str | None = None
    selection_evidence_id: str | None = None
    selection_artifact_hash: str | None = None
    execution_plan_id: str | None = None
    execution_plan_hash: str | None = None
    status: SimulationDailyRunStatus = SimulationDailyRunStatus.CREATED
    run_payload_json: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator(
        "run_id",
        "strategy_id",
        "package_id",
        "manifest_sha256",
        "release_id",
        "release_hash",
        "binding_id",
        "binding_hash",
    )
    @classmethod
    def _required_text(cls, value: str) -> str:
        value = str(value or "").strip()
        if not value:
            raise ValueError("field is required")
        return value

    @field_validator(
        "account_group_id",
        "strategy_slot_id",
        "selection_evidence_id",
        "selection_artifact_hash",
        "execution_plan_id",
        "execution_plan_hash",
    )
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None


class LocalSimExecutionRuntimeStatus(str, Enum):
    WAITING_FOR_CAUSAL_BAR = "WAITING_FOR_CAUSAL_BAR"
    ACTIVE = "ACTIVE"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    EXPIRED_WITH_RESIDUAL = "EXPIRED_WITH_RESIDUAL"


LOCAL_SIM_TERMINAL_RUNTIME_STATUSES = frozenset(
    {
        LocalSimExecutionRuntimeStatus.FILLED,
        LocalSimExecutionRuntimeStatus.CANCELLED,
        LocalSimExecutionRuntimeStatus.REJECTED,
        LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL,
    }
)


class LocalSimExecutionStateV1(BaseModel):
    """Durable per-intent state for the scheduler-owned LocalSIM minute loop."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["local_sim_execution_state_v1"] = "local_sim_execution_state_v1"
    state_id: str = ""
    run_id: str
    binding_id: str
    trade_date: date
    plan_id: str
    intent_id: str
    algo_instance_id: str
    portfolio_id: str
    order_id: str
    symbol: str
    side: OrderSide
    total_quantity: int = Field(gt=0)
    filled_quantity: int = Field(ge=0)
    remaining_quantity: int = Field(ge=0)
    algo_code: str
    order_status: str
    runtime_status: LocalSimExecutionRuntimeStatus
    algo_state: dict[str, Any] = Field(default_factory=dict)
    plan: dict[str, Any] | None = None
    plan_sha256: str | None = None
    schedule_version: str
    next_slice_index: int = Field(default=0, ge=0)
    causality_cursor: datetime
    last_processed_bar_time: datetime | None = None
    last_applied_bar_identity: str | None = None
    market_session: str | None = None
    latest_order_sequence: int = Field(default=0, ge=0)
    latest_fill_sequence: int = Field(default=0, ge=0)
    latest_cash_sequence: int = Field(default=0, ge=0)
    latest_position_sequence: int = Field(default=0, ge=0)
    terminal_reason: str | None = None
    residual_classification: str | None = None
    sequence: int = Field(default=0, ge=0)
    idempotency_key: str
    state_hash: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def _identity_quantity_and_hash_are_canonical(self) -> "LocalSimExecutionStateV1":
        if self.filled_quantity + self.remaining_quantity != self.total_quantity:
            raise ValueError("filled_quantity + remaining_quantity must equal total_quantity")
        expected_state_id = local_sim_execution_state_id(
            binding_id=self.binding_id,
            trade_date=self.trade_date,
            plan_id=self.plan_id,
            intent_id=self.intent_id,
            algo_instance_id=self.algo_instance_id,
        )
        if self.state_id and self.state_id != expected_state_id:
            raise ValueError("state_id does not match LocalSimExecutionStateV1 identity")
        object.__setattr__(self, "state_id", expected_state_id)
        if self.plan is None:
            if self.plan_sha256 is not None:
                raise ValueError("plan_sha256 requires plan")
        else:
            expected_plan_sha256 = canonical_json_sha256(self.plan)
            if self.plan_sha256 is not None and self.plan_sha256 != expected_plan_sha256:
                raise ValueError("plan_sha256 does not match plan")
            object.__setattr__(self, "plan_sha256", expected_plan_sha256)
        if self.runtime_status == LocalSimExecutionRuntimeStatus.FILLED and self.remaining_quantity != 0:
            raise ValueError("FILLED LocalSIM state cannot retain remaining quantity")
        if self.runtime_status == LocalSimExecutionRuntimeStatus.EXPIRED_WITH_RESIDUAL:
            if self.remaining_quantity <= 0:
                raise ValueError("EXPIRED_WITH_RESIDUAL requires remaining quantity")
            if not self.terminal_reason or not self.residual_classification:
                raise ValueError("EXPIRED_WITH_RESIDUAL requires terminal reason and residual classification")
        expected_hash = local_sim_execution_state_hash(self)
        if self.state_hash and self.state_hash != expected_hash:
            raise ValueError("state_hash does not match LocalSimExecutionStateV1 payload")
        object.__setattr__(self, "state_hash", expected_hash)
        return self

    @property
    def is_terminal(self) -> bool:
        return self.runtime_status in LOCAL_SIM_TERMINAL_RUNTIME_STATUSES


def local_sim_execution_state_id(
    *, binding_id: str, trade_date: date, plan_id: str, intent_id: str, algo_instance_id: str,
) -> str:
    digest = canonical_json_sha256(
        ["localsim_execution_state_v1", binding_id, trade_date.isoformat(), plan_id, intent_id, algo_instance_id]
    )
    return f"lsstate_{digest}"


def local_sim_execution_state_hash(state: LocalSimExecutionStateV1) -> str:
    payload = state.model_dump(mode="json", exclude={"state_hash", "updated_at"})
    return canonical_json_sha256(payload)
