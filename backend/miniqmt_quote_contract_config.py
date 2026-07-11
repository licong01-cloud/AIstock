"""Strict, dependency-free configuration schema for Phase 1 quote ingress.

The process configuration has documented defaults because it controls worker
capacity and lifecycle.  The immutable per-binding policy deliberately has no
implicit freshness or skew defaults: a B0_QUOTE_V2 binding must carry every
threshold explicitly.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from backend.execution_algos.adaptive_is.contracts import canonical_sha256
from backend.execution_algos.adaptive_is.reasons import QuoteContractReasonCode, quote_contract_error
from backend.miniqmt_quote_contract_env import (
    QUOTE_INGRESS_ENV_DEFAULTS,
    QUOTE_INGRESS_ENV_METADATA,
    QuoteIngressEnvValidationError,
    parse_quote_ingress_env_values,
)


QUOTE_CONTRACT_POLICY_SCHEMA_VERSION = "miniqmt_quote_contract_policy_v2"
QUOTE_CONTRACT_CONTROL_REVISION = "B0_QUOTE_V2"
QUOTE_AUCTION_MODE_OBSERVE_ONLY = "OBSERVE_ONLY"
REQUIRED_B0_QUOTE_V2_CAPABILITIES = frozenset(
    {
        "FIVE_LEVEL_DEPTH",
        "EXCHANGE_TIMESTAMP",
        "RAW_PRICE_BASIS",
        "DEPTH_UNIT_SHARES",
        "TRADABILITY",
        "CALENDAR",
    }
)


def _configuration_error(message: str, *, context: Mapping[str, Any] | None = None) -> ValueError:
    return quote_contract_error(QuoteContractReasonCode.POLICY_SCHEMA_INVALID, message, context=context)


def _positive_int(value: Any, *, key: str) -> int:
    if isinstance(value, bool):
        raise _configuration_error("integer process config cannot be boolean", context={"key": key, "value": value})
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise _configuration_error("process config must be an integer", context={"key": key, "value": value}) from exc
    if parsed <= 0:
        raise _configuration_error("process config must be positive", context={"key": key, "value": value})
    return parsed


@dataclass(frozen=True)
class QuoteIngressRuntimeConfig:
    enabled: bool
    owner_mode: str
    max_symbols: int
    drain_budget: int
    heartbeat_timeout_ms: int
    restart_backoff_ms: int
    restart_max_backoff_ms: int
    loud_interval_seconds: int
    evidence_outbox_max_events: int
    evidence_flush_batch_size: int

    def __post_init__(self) -> None:
        values = {
            "MINIQMT_ADAPTIVE_IS_QUOTE_INGRESS_ENABLED": self.enabled,
            "MINIQMT_QUOTE_INGRESS_OWNER_MODE": self.owner_mode,
            "MINIQMT_QUOTE_INGRESS_MAX_SYMBOLS": self.max_symbols,
            "MINIQMT_QUOTE_INGRESS_DRAIN_BUDGET": self.drain_budget,
            "MINIQMT_QUOTE_INGRESS_HEARTBEAT_TIMEOUT_MS": self.heartbeat_timeout_ms,
            "MINIQMT_QUOTE_INGRESS_RESTART_BACKOFF_MS": self.restart_backoff_ms,
            "MINIQMT_QUOTE_INGRESS_RESTART_MAX_BACKOFF_MS": self.restart_max_backoff_ms,
            "MINIQMT_QUOTE_INGRESS_LOUD_INTERVAL_SECONDS": self.loud_interval_seconds,
            "MINIQMT_QUOTE_EVIDENCE_OUTBOX_MAX_EVENTS": self.evidence_outbox_max_events,
            "MINIQMT_QUOTE_EVIDENCE_FLUSH_BATCH_SIZE": self.evidence_flush_batch_size,
        }
        parsed = _parse_runtime_config(values)
        for field_name, value in parsed.items():
            object.__setattr__(self, field_name, value)

    @classmethod
    def from_mapping(cls, values: Mapping[str, Any]) -> "QuoteIngressRuntimeConfig":
        return cls(**_parse_runtime_config(values))


@dataclass(frozen=True)
class QuoteContractPolicy:
    """Canonical immutable policy section carried inside execution_policy."""

    schema_version: str
    control_revision: str
    required_capabilities: frozenset[str]
    max_receive_age_ms: int
    max_source_lag_ms: int
    max_exchange_age_ms: int
    max_negative_skew_ms: int
    max_dependency_group_skew_ms: int
    auction_mode: str
    policy_sha256: str = field(init=False)

    @classmethod
    def from_execution_policy(cls, execution_policy: Mapping[str, Any]) -> "QuoteContractPolicy":
        raw = execution_policy.get("quote_contract")
        if not isinstance(raw, Mapping):
            raise _configuration_error("execution_policy.quote_contract must be an explicit object")
        allowed = {
            "schema_version",
            "control_revision",
            "required_capabilities",
            "max_receive_age_ms",
            "max_source_lag_ms",
            "max_exchange_age_ms",
            "max_negative_skew_ms",
            "max_dependency_group_skew_ms",
            "auction_mode",
        }
        received = set(raw)
        unknown = received - allowed
        missing = allowed - received
        if unknown or missing:
            raise _configuration_error(
                "quote_contract schema has unknown or missing fields; implicit defaults are forbidden",
                context={"unknown": sorted(unknown), "missing": sorted(missing)},
            )
        capabilities_value = raw["required_capabilities"]
        if not isinstance(capabilities_value, (list, tuple, set, frozenset)) or isinstance(capabilities_value, (str, bytes)):
            raise _configuration_error("required_capabilities must be an explicit list of capabilities")
        capabilities = frozenset(str(value) for value in capabilities_value)
        if len(capabilities) != len(capabilities_value):
            raise _configuration_error("required_capabilities cannot contain duplicates")
        if capabilities != REQUIRED_B0_QUOTE_V2_CAPABILITIES:
            raise _configuration_error(
                "B0_QUOTE_V2 required capabilities must match the registered immutable set",
                context={"received": sorted(capabilities), "required": sorted(REQUIRED_B0_QUOTE_V2_CAPABILITIES)},
            )
        if raw["schema_version"] != QUOTE_CONTRACT_POLICY_SCHEMA_VERSION:
            raise _configuration_error("unsupported quote contract policy schema version", context={"value": raw["schema_version"]})
        if raw["control_revision"] != QUOTE_CONTRACT_CONTROL_REVISION:
            raise _configuration_error("quote contract policy must explicitly select B0_QUOTE_V2", context={"value": raw["control_revision"]})
        if raw["auction_mode"] != QUOTE_AUCTION_MODE_OBSERVE_ONLY:
            raise _configuration_error("Phase 1 quote policy only permits OBSERVE_ONLY auction mode", context={"value": raw["auction_mode"]})
        return cls(
            schema_version=str(raw["schema_version"]),
            control_revision=str(raw["control_revision"]),
            required_capabilities=capabilities,
            max_receive_age_ms=_positive_int(raw["max_receive_age_ms"], key="max_receive_age_ms"),
            max_source_lag_ms=_positive_int(raw["max_source_lag_ms"], key="max_source_lag_ms"),
            max_exchange_age_ms=_positive_int(raw["max_exchange_age_ms"], key="max_exchange_age_ms"),
            max_negative_skew_ms=_non_negative_int(raw["max_negative_skew_ms"], key="max_negative_skew_ms"),
            max_dependency_group_skew_ms=_positive_int(raw["max_dependency_group_skew_ms"], key="max_dependency_group_skew_ms"),
            auction_mode=str(raw["auction_mode"]),
        )

    def __post_init__(self) -> None:
        if self.schema_version != QUOTE_CONTRACT_POLICY_SCHEMA_VERSION:
            raise _configuration_error("unsupported quote contract policy schema version", context={"value": self.schema_version})
        if self.control_revision != QUOTE_CONTRACT_CONTROL_REVISION:
            raise _configuration_error("quote contract policy must explicitly select B0_QUOTE_V2", context={"value": self.control_revision})
        if self.auction_mode != QUOTE_AUCTION_MODE_OBSERVE_ONLY:
            raise _configuration_error("Phase 1 quote policy only permits OBSERVE_ONLY auction mode", context={"value": self.auction_mode})
        if not isinstance(self.required_capabilities, (list, tuple, set, frozenset)) or isinstance(
            self.required_capabilities, (str, bytes)
        ):
            raise _configuration_error("required_capabilities must be an explicit collection")
        capabilities = frozenset(str(value) for value in self.required_capabilities)
        if capabilities != REQUIRED_B0_QUOTE_V2_CAPABILITIES:
            raise _configuration_error(
                "B0_QUOTE_V2 required capabilities must match the registered immutable set",
                context={"received": sorted(capabilities), "required": sorted(REQUIRED_B0_QUOTE_V2_CAPABILITIES)},
            )
        object.__setattr__(self, "required_capabilities", capabilities)
        object.__setattr__(self, "max_receive_age_ms", _positive_int(self.max_receive_age_ms, key="max_receive_age_ms"))
        object.__setattr__(self, "max_source_lag_ms", _positive_int(self.max_source_lag_ms, key="max_source_lag_ms"))
        object.__setattr__(self, "max_exchange_age_ms", _positive_int(self.max_exchange_age_ms, key="max_exchange_age_ms"))
        object.__setattr__(self, "max_negative_skew_ms", _non_negative_int(self.max_negative_skew_ms, key="max_negative_skew_ms"))
        object.__setattr__(
            self,
            "max_dependency_group_skew_ms",
            _positive_int(self.max_dependency_group_skew_ms, key="max_dependency_group_skew_ms"),
        )
        object.__setattr__(self, "policy_sha256", canonical_sha256(self.canonical_payload()))

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "control_revision": self.control_revision,
            "required_capabilities": sorted(self.required_capabilities),
            "max_receive_age_ms": self.max_receive_age_ms,
            "max_source_lag_ms": self.max_source_lag_ms,
            "max_exchange_age_ms": self.max_exchange_age_ms,
            "max_negative_skew_ms": self.max_negative_skew_ms,
            "max_dependency_group_skew_ms": self.max_dependency_group_skew_ms,
            "auction_mode": self.auction_mode,
        }

    def assert_policy_sha256(self, expected: str) -> None:
        if not isinstance(expected, str) or expected != self.policy_sha256:
            raise _configuration_error(
                "policy_sha256 does not match the canonical quote contract policy",
                context={"expected": expected, "actual": self.policy_sha256},
            )


def _non_negative_int(value: Any, *, key: str) -> int:
    if isinstance(value, bool):
        raise _configuration_error("integer policy threshold cannot be boolean", context={"key": key, "value": value})
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise _configuration_error("policy threshold must be an integer", context={"key": key, "value": value}) from exc
    if parsed < 0:
        raise _configuration_error("policy threshold must be non-negative", context={"key": key, "value": value})
    return parsed


def _parse_runtime_config(values: Mapping[str, Any]) -> dict[str, Any]:
    try:
        return parse_quote_ingress_env_values(values)
    except QuoteIngressEnvValidationError as exc:
        raise _configuration_error(str(exc)) from exc


__all__ = [
    "QUOTE_AUCTION_MODE_OBSERVE_ONLY",
    "QUOTE_CONTRACT_CONTROL_REVISION",
    "QUOTE_CONTRACT_POLICY_SCHEMA_VERSION",
    "QUOTE_INGRESS_ENV_DEFAULTS",
    "QUOTE_INGRESS_ENV_METADATA",
    "REQUIRED_B0_QUOTE_V2_CAPABILITIES",
    "QuoteContractPolicy",
    "QuoteIngressRuntimeConfig",
]
