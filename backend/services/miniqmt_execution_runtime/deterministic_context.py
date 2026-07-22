"""Pure deterministic ID and draw helpers for MiniQMT K1-A.

No helper in this module reads wall clock, UUID state, process-global random,
repository state, or broker/runtime objects.
"""

from __future__ import annotations

from enum import StrEnum

from .plugin_canonical import digest_bytes_v1, hash_hex_v1, require_sha256_v1
from .plugin_contracts import DeterministicExecutionContextV1


class DeterministicIdKindV1(StrEnum):
    ACTION = "ACTION"
    BROKER_COMMAND = "BROKER_COMMAND"
    LOCAL_ORDER = "LOCAL_ORDER"
    TIMER_MUTATION = "TIMER_MUTATION"
    TIMER_SCHEDULE = "TIMER_SCHEDULE"
    TIMER_OCCURRENCE = "TIMER_OCCURRENCE"
    DIAGNOSTIC = "DIAGNOSTIC"
    TRANSITION = "TRANSITION"


_KIND_PREFIX: dict[DeterministicIdKindV1, str] = {
    DeterministicIdKindV1.ACTION: "mqaction_",
    DeterministicIdKindV1.BROKER_COMMAND: "mqcommand_",
    DeterministicIdKindV1.LOCAL_ORDER: "mqlocalorder_",
    DeterministicIdKindV1.TIMER_MUTATION: "mqtimermut_",
    DeterministicIdKindV1.TIMER_SCHEDULE: "mqtimersched_",
    DeterministicIdKindV1.TIMER_OCCURRENCE: "mqtimerocc_",
    DeterministicIdKindV1.DIAGNOSTIC: "mqdiag_",
    DeterministicIdKindV1.TRANSITION: "mqtransition_",
}


def _require_nonnegative_int(value: object, *, field_name: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be a strict integer")
    if value < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return value


def derive_id_v1(
    *,
    context: DeterministicExecutionContextV1,
    kind: DeterministicIdKindV1,
    ordinal: int,
    business_payload_sha256: str,
) -> str:
    """Derive one exact, domain-separated effect identity."""

    if not isinstance(context, DeterministicExecutionContextV1):
        raise TypeError("context must be DeterministicExecutionContextV1")
    if not isinstance(kind, DeterministicIdKindV1):
        raise TypeError("kind must be DeterministicIdKindV1")
    normalized_ordinal = _require_nonnegative_int(ordinal, field_name="ordinal")
    payload_hash = require_sha256_v1(business_payload_sha256, field_name="business_payload_sha256")
    digest_hex = hash_hex_v1(
        "miniqmt_deterministic_id_v1",
        {
            "context_sha256": context.context_sha256,
            "kind": kind.value,
            "ordinal": normalized_ordinal,
            "business_payload_sha256": payload_hash,
        },
    )
    return _KIND_PREFIX[kind] + digest_hex


def draw_u53_v1(*, context: DeterministicExecutionContextV1, draw_ordinal: int) -> float:
    """Return a replay-stable ``[0, 1)`` draw from the first 53 raw digest bits."""

    if not isinstance(context, DeterministicExecutionContextV1):
        raise TypeError("context must be DeterministicExecutionContextV1")
    normalized_ordinal = _require_nonnegative_int(draw_ordinal, field_name="draw_ordinal")
    raw_digest = digest_bytes_v1(
        "miniqmt_plugin_draw_v1",
        {
            "context_sha256": context.context_sha256,
            "draw_ordinal": normalized_ordinal,
        },
    )
    return (int.from_bytes(raw_digest[0:7], "big") >> 3) / (2**53)


def best_limit_quantity_v1(
    *,
    context: DeterministicExecutionContextV1,
    min_volume: int,
    max_volume: int,
    draw_ordinal: int,
) -> int:
    """Apply the approved BestLimit integer-share draw formula exactly."""

    normalized_min = _require_nonnegative_int(min_volume, field_name="min_volume")
    normalized_max = _require_nonnegative_int(max_volume, field_name="max_volume")
    if normalized_min <= 0:
        raise ValueError("min_volume must be positive")
    if normalized_max < normalized_min:
        raise ValueError("max_volume must be greater than or equal to min_volume")
    return int(
        normalized_min + (normalized_max - normalized_min) * draw_u53_v1(context=context, draw_ordinal=draw_ordinal)
    )


def validate_contiguous_ordinals_v1(ordinals: tuple[int, ...]) -> None:
    """Fail loud on duplicate, skipped, negative, or coerced effect ordinals."""

    if type(ordinals) is not tuple:
        raise TypeError("ordinals must be a tuple")
    normalized = tuple(_require_nonnegative_int(item, field_name="ordinal") for item in ordinals)
    if len(normalized) != len(set(normalized)):
        raise ValueError("effect ordinal sequence contains duplicate values")
    if normalized != tuple(range(len(normalized))):
        raise ValueError("effect ordinal sequence contains a gap or is out of order")


__all__ = [
    "DeterministicIdKindV1",
    "best_limit_quantity_v1",
    "derive_id_v1",
    "draw_u53_v1",
    "validate_contiguous_ordinals_v1",
]
