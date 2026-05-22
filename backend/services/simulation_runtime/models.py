"""Broker-neutral simulation runtime release and binding models."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, date, datetime
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from backend.services.trading_core.errors import StrategyPackageValidationError
from backend.services.trading_core.models import OrderSide


DEFAULT_DAILY_STRATEGY_PROFILE_VERSION_ID = "platform_default_daily_strategy_profile_v1"

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
        "capital_allocation",
        "strategy_name",
        "order_remark_prefix",
        "approval_state",
        "metadata",
    }
)


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
        raise StrategyPackageValidationError(
            "StrategyRuntimeRelease cannot contain alpha-core or broker-binding fields",
            context={**(context or {}), "forbidden_paths": matches, "forbidden_keys": sorted(forbidden)},
        )


def assert_binding_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    unknown = sorted(set(payload).difference(SIMULATION_RELEASE_BINDING_CONFIG_KEYS))
    if unknown:
        raise StrategyPackageValidationError(
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
        raise StrategyPackageValidationError(
            "SimulationReleaseBinding cannot contain alpha-core or runtime-policy fields",
            context={**(context or {}), "forbidden_paths": matches, "forbidden_keys": sorted(forbidden)},
        )


def assert_selection_only_payload_boundary(payload: dict[str, Any], *, context: dict[str, Any] | None = None) -> None:
    matches = find_forbidden_key_paths(payload, SELECTION_ONLY_FORBIDDEN_KEYS)
    if matches:
        raise StrategyPackageValidationError(
            "Selection-only signal generation cannot contain broker, capital, target, rebalance or execution fields",
            context={
                **(context or {}),
                "forbidden_paths": matches,
                "forbidden_keys": sorted(SELECTION_ONLY_FORBIDDEN_KEYS),
            },
        )


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

    @field_validator("broker_account_id", "strategy_name", "order_remark_prefix", "created_by", "created_reason")
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

    @field_validator("selection_evidence_id", "selection_artifact_hash", "execution_plan_id", "execution_plan_hash")
    @classmethod
    def _optional_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = str(value).strip()
        return value or None
