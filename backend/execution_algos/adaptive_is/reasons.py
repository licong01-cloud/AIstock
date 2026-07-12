"""One authoritative loud-reason registry for Phase 1 quote handling.

The registry intentionally lives in the algorithm-neutral package so that a
normalizer, a future ingress worker, a runtime adapter, and their tests cannot
quietly drift to incompatible strings or stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Mapping


class QuoteContractStage(str, Enum):
    """Stable Phase 1 failure stages defined by the approved design."""

    OWNER = "OWNER"
    SUBSCRIBE = "SUBSCRIBE"
    BOOTSTRAP = "BOOTSTRAP"
    INGRESS = "INGRESS"
    NORMALIZE = "NORMALIZE"
    UNIT = "UNIT"
    CLOCK = "CLOCK"
    CALENDAR = "CALENDAR"
    TRADABILITY = "TRADABILITY"
    ELIGIBILITY = "ELIGIBILITY"
    PERSIST = "PERSIST"
    MARKOUT = "MARKOUT"
    ADAPTER = "ADAPTER"


class QuoteFailureSeverity(str, Enum):
    """Stable severity carried by every loud quote-contract failure."""

    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class QuoteFailureRetryClass(str, Enum):
    """Deterministic recovery behavior; no value requires manual approval."""

    NEXT_EVIDENCE = "NEXT_EVIDENCE"
    AUTOMATIC_RETRY = "AUTOMATIC_RETRY"
    NON_RETRYABLE = "NON_RETRYABLE"


class QuoteContractReasonCode(str, Enum):
    """Stable reason codes, including every code in the Phase 1 failure matrix."""

    OWNER_CONFLICT = "ADAPTIVE_IS_QUOTE_OWNER_CONFLICT"
    SUBSCRIPTION_UNAVAILABLE = "ADAPTIVE_IS_QUOTE_SUBSCRIPTION_UNAVAILABLE"
    LEASE_REBUILD_FAILED = "ADAPTIVE_IS_QUOTE_LEASE_REBUILD_FAILED"
    BOOTSTRAP_INCOMPLETE = "ADAPTIVE_IS_QUOTE_BOOTSTRAP_INCOMPLETE"
    CAPACITY_EXCEEDED = "ADAPTIVE_IS_QUOTE_CAPACITY_EXCEEDED"
    UNEXPECTED_SYMBOL = "ADAPTIVE_IS_QUOTE_UNEXPECTED_SYMBOL"
    PAYLOAD_INVALID = "ADAPTIVE_IS_QUOTE_PAYLOAD_INVALID"
    SYMBOL_INVALID = "ADAPTIVE_IS_QUOTE_SYMBOL_INVALID"
    ALIAS_CONFLICT = "ADAPTIVE_IS_QUOTE_ALIAS_CONFLICT"
    TIMESTAMP_INVALID = "ADAPTIVE_IS_QUOTE_TIMESTAMP_INVALID"
    DEPTH_CAPABILITY_MISSING = "ADAPTIVE_IS_QUOTE_DEPTH_CAPABILITY_MISSING"
    DEPTH_SCHEMA_INVALID = "ADAPTIVE_IS_QUOTE_DEPTH_SCHEMA_INVALID"
    UNIT_UNPROVEN = "ADAPTIVE_IS_QUOTE_UNIT_UNPROVEN"
    ORDERING_REJECTED = "ADAPTIVE_IS_QUOTE_ORDERING_REJECTED"
    CLOCK_CALENDAR_INVALID = "ADAPTIVE_IS_QUOTE_CLOCK_CALENDAR_INVALID"
    MARKET_NOT_TRADABLE = "ADAPTIVE_IS_QUOTE_MARKET_NOT_TRADABLE"
    TRADABILITY_DATA_INVALID = "ADAPTIVE_IS_TRADABILITY_DATA_INVALID"
    ACTION_QUOTE_INELIGIBLE = "ADAPTIVE_IS_ACTION_QUOTE_INELIGIBLE"
    POLICY_SCHEMA_INVALID = "ADAPTIVE_IS_QUOTE_POLICY_SCHEMA_INVALID"
    CLOSING_AUCTION_CAPABILITY_UNAVAILABLE = "ADAPTIVE_IS_CLOSING_AUCTION_CAPABILITY_UNAVAILABLE"
    EVIDENCE_OUTBOX_FULL = "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_OUTBOX_FULL"
    EVIDENCE_PERSIST_FAILED = "ADAPTIVE_IS_MARKET_DATA_EVIDENCE_PERSIST_FAILED"
    MARKOUT_QUOTE_UNAVAILABLE = "ADAPTIVE_IS_MARKOUT_QUOTE_UNAVAILABLE"
    PARITY_VIOLATION = "ADAPTIVE_IS_B0_QUOTE_V2_PARITY_VIOLATION"
    CONSUMER_FAILURE = "ADAPTIVE_IS_QUOTE_CONSUMER_FAILURE"


@dataclass(frozen=True)
class QuoteFailureDefinition:
    """Reason-to-stage mapping consumed by all future Phase 1 boundaries."""

    reason_code: QuoteContractReasonCode
    stage: QuoteContractStage
    severity: QuoteFailureSeverity
    retry_class: QuoteFailureRetryClass
    allowed_stages: frozenset[QuoteContractStage] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        allowed = self.allowed_stages or frozenset({self.stage})
        if self.stage not in allowed:
            raise ValueError("primary failure stage must be included in allowed_stages")
        object.__setattr__(self, "allowed_stages", frozenset(allowed))

    @property
    def retryable(self) -> bool:
        return self.retry_class != QuoteFailureRetryClass.NON_RETRYABLE


def _definition(
    reason_code: QuoteContractReasonCode,
    stage: QuoteContractStage,
    *,
    severity: QuoteFailureSeverity = QuoteFailureSeverity.ERROR,
    retry_class: QuoteFailureRetryClass = QuoteFailureRetryClass.NEXT_EVIDENCE,
    allowed_stages: frozenset[QuoteContractStage] | None = None,
) -> QuoteFailureDefinition:
    return QuoteFailureDefinition(
        reason_code=reason_code,
        stage=stage,
        severity=severity,
        retry_class=retry_class,
        allowed_stages=allowed_stages or frozenset({stage}),
    )


QUOTE_FAILURE_REGISTRY: Mapping[QuoteContractReasonCode, QuoteFailureDefinition] = MappingProxyType(
    {
        QuoteContractReasonCode.OWNER_CONFLICT: _definition(
            QuoteContractReasonCode.OWNER_CONFLICT,
            QuoteContractStage.OWNER,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
        QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE: _definition(
            QuoteContractReasonCode.SUBSCRIPTION_UNAVAILABLE,
            QuoteContractStage.SUBSCRIBE,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
        QuoteContractReasonCode.LEASE_REBUILD_FAILED: _definition(
            QuoteContractReasonCode.LEASE_REBUILD_FAILED,
            QuoteContractStage.SUBSCRIBE,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
        QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE: _definition(
            QuoteContractReasonCode.BOOTSTRAP_INCOMPLETE,
            QuoteContractStage.BOOTSTRAP,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.CAPACITY_EXCEEDED: _definition(QuoteContractReasonCode.CAPACITY_EXCEEDED, QuoteContractStage.INGRESS),
        QuoteContractReasonCode.UNEXPECTED_SYMBOL: _definition(
            QuoteContractReasonCode.UNEXPECTED_SYMBOL,
            QuoteContractStage.INGRESS,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.PAYLOAD_INVALID: _definition(QuoteContractReasonCode.PAYLOAD_INVALID, QuoteContractStage.INGRESS),
        QuoteContractReasonCode.SYMBOL_INVALID: _definition(QuoteContractReasonCode.SYMBOL_INVALID, QuoteContractStage.NORMALIZE),
        QuoteContractReasonCode.ALIAS_CONFLICT: _definition(QuoteContractReasonCode.ALIAS_CONFLICT, QuoteContractStage.NORMALIZE),
        QuoteContractReasonCode.TIMESTAMP_INVALID: _definition(QuoteContractReasonCode.TIMESTAMP_INVALID, QuoteContractStage.NORMALIZE),
        QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING: _definition(
            QuoteContractReasonCode.DEPTH_CAPABILITY_MISSING,
            QuoteContractStage.NORMALIZE,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.DEPTH_SCHEMA_INVALID: _definition(QuoteContractReasonCode.DEPTH_SCHEMA_INVALID, QuoteContractStage.NORMALIZE),
        QuoteContractReasonCode.UNIT_UNPROVEN: _definition(QuoteContractReasonCode.UNIT_UNPROVEN, QuoteContractStage.UNIT),
        QuoteContractReasonCode.ORDERING_REJECTED: _definition(
            QuoteContractReasonCode.ORDERING_REJECTED,
            QuoteContractStage.NORMALIZE,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.CLOCK_CALENDAR_INVALID: _definition(
            QuoteContractReasonCode.CLOCK_CALENDAR_INVALID,
            QuoteContractStage.CLOCK,
            allowed_stages=frozenset({QuoteContractStage.CLOCK, QuoteContractStage.CALENDAR}),
        ),
        QuoteContractReasonCode.MARKET_NOT_TRADABLE: _definition(
            QuoteContractReasonCode.MARKET_NOT_TRADABLE,
            QuoteContractStage.TRADABILITY,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.TRADABILITY_DATA_INVALID: _definition(QuoteContractReasonCode.TRADABILITY_DATA_INVALID, QuoteContractStage.TRADABILITY),
        QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE: _definition(
            QuoteContractReasonCode.ACTION_QUOTE_INELIGIBLE,
            QuoteContractStage.ELIGIBILITY,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.POLICY_SCHEMA_INVALID: _definition(
            QuoteContractReasonCode.POLICY_SCHEMA_INVALID,
            QuoteContractStage.ELIGIBILITY,
            retry_class=QuoteFailureRetryClass.NON_RETRYABLE,
        ),
        QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE: _definition(
            QuoteContractReasonCode.CLOSING_AUCTION_CAPABILITY_UNAVAILABLE,
            QuoteContractStage.ELIGIBILITY,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.EVIDENCE_OUTBOX_FULL: _definition(
            QuoteContractReasonCode.EVIDENCE_OUTBOX_FULL,
            QuoteContractStage.PERSIST,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
        QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED: _definition(
            QuoteContractReasonCode.EVIDENCE_PERSIST_FAILED,
            QuoteContractStage.PERSIST,
            severity=QuoteFailureSeverity.CRITICAL,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
        QuoteContractReasonCode.MARKOUT_QUOTE_UNAVAILABLE: _definition(
            QuoteContractReasonCode.MARKOUT_QUOTE_UNAVAILABLE,
            QuoteContractStage.MARKOUT,
            severity=QuoteFailureSeverity.WARNING,
        ),
        QuoteContractReasonCode.PARITY_VIOLATION: _definition(
            QuoteContractReasonCode.PARITY_VIOLATION,
            QuoteContractStage.ADAPTER,
            severity=QuoteFailureSeverity.CRITICAL,
            retry_class=QuoteFailureRetryClass.NON_RETRYABLE,
        ),
        QuoteContractReasonCode.CONSUMER_FAILURE: _definition(
            QuoteContractReasonCode.CONSUMER_FAILURE,
            QuoteContractStage.INGRESS,
            severity=QuoteFailureSeverity.CRITICAL,
            retry_class=QuoteFailureRetryClass.AUTOMATIC_RETRY,
        ),
    }
)


def failure_definition(reason_code: QuoteContractReasonCode | str) -> QuoteFailureDefinition:
    """Return the registry entry or fail loudly for an unregistered reason."""

    try:
        code = reason_code if isinstance(reason_code, QuoteContractReasonCode) else QuoteContractReasonCode(reason_code)
    except ValueError as exc:
        raise ValueError(f"unregistered Adaptive IS quote reason code: {reason_code!r}") from exc
    return QUOTE_FAILURE_REGISTRY[code]


@dataclass(frozen=True)
class QuoteContractError(ValueError):
    """Typed, structured failure; callers must surface it as a loud event."""

    reason_code: QuoteContractReasonCode
    stage: QuoteContractStage
    message: str
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        expected = failure_definition(self.reason_code)
        if self.stage not in expected.allowed_stages:
            raise ValueError(
                f"reason {self.reason_code.value} is not registered for stage {self.stage.value}"
            )
        object.__setattr__(self, "context", _freeze_context(self.context))
        ValueError.__init__(self, self.message)

    @property
    def retryable(self) -> bool:
        return failure_definition(self.reason_code).retryable

    def as_loud_payload(self) -> dict[str, Any]:
        """Return only structured context; runtime logging adds identities later."""

        definition = failure_definition(self.reason_code)
        return {
            "reason_code": self.reason_code.value,
            "stage": self.stage.value,
            "severity": definition.severity.value,
            "retry_class": definition.retry_class.value,
            "retryable": self.retryable,
            "message": self.message,
            "context": _thaw_context(self.context),
        }


def quote_contract_error(
    reason_code: QuoteContractReasonCode,
    message: str,
    *,
    stage: QuoteContractStage | None = None,
    context: Mapping[str, Any] | None = None,
) -> QuoteContractError:
    """Construct a registry-consistent failure without ad-hoc stage strings."""

    definition = failure_definition(reason_code)
    return QuoteContractError(
        reason_code=reason_code,
        stage=stage or definition.stage,
        message=message,
        context=dict(context or {}),
    )


def _freeze_context(value: Mapping[str, Any]) -> Mapping[str, Any]:
    def freeze(item: Any) -> Any:
        if isinstance(item, Mapping):
            return MappingProxyType({str(key): freeze(nested) for key, nested in item.items()})
        if isinstance(item, (list, tuple, set, frozenset)):
            return tuple(freeze(nested) for nested in item)
        return item

    return MappingProxyType({str(key): freeze(item) for key, item in value.items()})


def _thaw_context(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _thaw_context(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_thaw_context(item) for item in value]
    return value


__all__ = [
    "QUOTE_FAILURE_REGISTRY",
    "QuoteContractError",
    "QuoteContractReasonCode",
    "QuoteContractStage",
    "QuoteFailureRetryClass",
    "QuoteFailureSeverity",
    "QuoteFailureDefinition",
    "failure_definition",
    "quote_contract_error",
]
