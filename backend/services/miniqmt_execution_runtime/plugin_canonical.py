"""Canonical, deeply immutable carriers for the MiniQMT K1 plugin contracts.

This module is deliberately a dependency leaf.  It does not import the runtime,
repository, gateway, broker SDK, or any plugin implementation.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_EVIDENCE_DEPTH = 12
_MAX_EVIDENCE_ITEMS = 128
_MAX_EVIDENCE_TEXT = 2_048


class FrozenJsonArrayV1(tuple):
    """Tuple marker that distinguishes a frozen JSON array from an object."""

    __slots__ = ()

    def __new__(cls, values: Sequence["FrozenJsonValueV1"] = ()) -> "FrozenJsonArrayV1":
        return tuple.__new__(cls, values)


@dataclass(frozen=True, slots=True)
class FrozenJsonMemberV1:
    """One Unicode-key-sorted member in a frozen JSON object."""

    key: str
    value: "FrozenJsonValueV1"


class FrozenJsonObjectV1(tuple):
    """Tuple marker containing :class:`FrozenJsonMemberV1` values."""

    __slots__ = ()

    def __new__(cls, values: Sequence[FrozenJsonMemberV1] = ()) -> "FrozenJsonObjectV1":
        return tuple.__new__(cls, values)


FrozenJsonScalarV1 = None | bool | int | str
FrozenJsonValueV1 = FrozenJsonScalarV1 | FrozenJsonArrayV1 | FrozenJsonObjectV1


def freeze_json_v1(value: Any) -> FrozenJsonValueV1:
    """Deep-copy and freeze one strict JSON value.

    External tuples are rejected because an empty tuple cannot state whether it
    represents an array or object.  Only the marker types created here are
    accepted as already-frozen internal values.
    """

    if isinstance(value, (FrozenJsonArrayV1, FrozenJsonObjectV1)):
        return value
    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, Mapping):
        members: list[FrozenJsonMemberV1] = []
        for key, member_value in value.items():
            if type(key) is not str:
                raise TypeError(f"frozen JSON object key must be str, got {type(key).__name__}")
            members.append(FrozenJsonMemberV1(key=key, value=freeze_json_v1(member_value)))
        members.sort(key=lambda member: member.key)
        return FrozenJsonObjectV1(members)
    if isinstance(value, list):
        return FrozenJsonArrayV1(freeze_json_v1(item) for item in value)
    if isinstance(value, tuple):
        raise TypeError("external tuple is not a valid JSON carrier; use list or object")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float is not valid JSON")
        raise TypeError("binary float is forbidden in K1 canonical JSON")
    raise TypeError(f"unsupported frozen JSON carrier: {type(value).__name__}")


def thaw_json_v1(value: FrozenJsonValueV1) -> Any:
    """Return a new mutable JSON view without exposing frozen internals."""

    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, FrozenJsonArrayV1):
        return [thaw_json_v1(item) for item in value]
    if isinstance(value, FrozenJsonObjectV1):
        return {member.key: thaw_json_v1(member.value) for member in value}
    raise TypeError(f"value is not FrozenJsonValueV1: {type(value).__name__}")


def _strict_json_value_v1(value: Any) -> Any:
    if isinstance(value, (FrozenJsonArrayV1, FrozenJsonObjectV1)):
        return thaw_json_v1(value)
    if isinstance(value, BaseModel):
        return _strict_json_value_v1(value.model_dump(mode="json"))
    if value is None or type(value) in (bool, int, str):
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, member_value in value.items():
            if type(key) is not str:
                raise TypeError(f"canonical JSON object key must be str, got {type(key).__name__}")
            normalized[key] = _strict_json_value_v1(member_value)
        return normalized
    if isinstance(value, list):
        return [_strict_json_value_v1(item) for item in value]
    if isinstance(value, tuple):
        raise TypeError("external tuple is not a canonical JSON carrier")
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float is not canonical JSON")
        raise TypeError("binary float is forbidden in canonical JSON")
    raise TypeError(f"unsupported canonical JSON value: {type(value).__name__}")


def canonical_json_bytes_v1(value: Any) -> bytes:
    """Encode strict canonical JSON V1 as UTF-8 bytes."""

    normalized = _strict_json_value_v1(value)
    return json.dumps(
        normalized,
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def validate_json_text_v1(value: str | bytes | bytearray) -> None:
    """Reject duplicate keys, BOMs, invalid UTF-8 and non-standard constants.

    Pydantic's JSON decoder is still used for model construction after this
    preflight.  This pass exists because a normal ``dict`` cannot retain the
    evidence that the source JSON repeated a key.
    """

    if isinstance(value, (bytes, bytearray)):
        try:
            text = bytes(value).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("JSON readback must be valid UTF-8") from exc
    elif type(value) is str:
        text = value
    else:
        raise TypeError(f"JSON readback must be str or bytes, got {type(value).__name__}")
    if text.startswith("\ufeff"):
        raise ValueError("JSON readback must not contain a BOM")

    def _object_without_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, member_value in pairs:
            if key in result:
                raise ValueError(f"JSON readback contains duplicate key: {key}")
            result[key] = member_value
        return result

    def _reject_constant(constant: str) -> None:
        raise ValueError(f"JSON readback contains non-standard constant: {constant}")

    try:
        json.loads(text, object_pairs_hook=_object_without_duplicates, parse_constant=_reject_constant)
    except json.JSONDecodeError as exc:
        raise ValueError("JSON readback is not valid strict JSON") from exc


def _require_domain_v1(domain: Any) -> str:
    if type(domain) is not str or not domain or domain != domain.strip():
        raise ValueError("hash domain must be a non-empty, trim-stable string")
    return domain


def digest_bytes_v1(domain: str, payload: Any) -> bytes:
    """Return the exact raw SHA-256 digest for a domain-separated payload."""

    normalized_domain = _require_domain_v1(domain)
    return hashlib.sha256(normalized_domain.encode("utf-8") + b"\x00" + canonical_json_bytes_v1(payload)).digest()


def hash_hex_v1(domain: str, payload: Any) -> str:
    """Return lowercase hex for :func:`digest_bytes_v1`."""

    return digest_bytes_v1(domain, payload).hex()


def require_identity_v1(value: Any, *, field_name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise ValueError(f"{field_name} must be a non-empty, trim-stable string")
    return value


def require_sha256_v1(value: Any, *, field_name: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a lowercase sha256")
    return value


def canonical_decimal_string_v1(
    value: Any,
    *,
    field_name: str = "decimal",
    allow_zero: bool = True,
    max_scale: int | None = None,
) -> str:
    """Validate a decimal carrier and return its finite canonical string.

    Only strings and ``Decimal`` instances are accepted.  Integers and floats
    are rejected so no caller can silently change an upstream unit or inject a
    binary-float rounding artifact.
    """

    if type(value) is str:
        if not value or value != value.strip():
            raise ValueError(f"{field_name} must be a non-empty, trim-stable decimal string")
        raw = value
    elif isinstance(value, Decimal):
        raw = str(value)
    else:
        raise TypeError(f"{field_name} must be a decimal string or Decimal, got {type(value).__name__}")
    try:
        decimal_value = Decimal(raw)
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} is not a valid decimal") from exc
    if not decimal_value.is_finite():
        raise ValueError(f"{field_name} must be finite")
    if decimal_value < 0 or (decimal_value == 0 and not allow_zero):
        comparator = "positive" if not allow_zero else "non-negative"
        raise ValueError(f"{field_name} must be {comparator}")
    normalized = format(decimal_value, "f")
    if "." in normalized:
        normalized = normalized.rstrip("0").rstrip(".")
    if normalized in ("-0", ""):
        normalized = "0"
    if max_scale is not None:
        scale = max(0, -Decimal(normalized).as_tuple().exponent)
        if scale > max_scale:
            raise ValueError(f"{field_name} exceeds maximum scale {max_scale}")
    return normalized


def canonical_utc_datetime_v1(value: Any, *, field_name: str = "datetime") -> str:
    """Normalize an aware datetime/string to fixed microsecond UTC ``Z`` form."""

    if isinstance(value, datetime):
        parsed = value
    elif type(value) is str:
        if not value or value != value.strip():
            raise ValueError(f"{field_name} must be a non-empty, trim-stable timestamp")
        candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be ISO-8601") from exc
    else:
        raise TypeError(f"{field_name} must be datetime or ISO-8601 string, got {type(value).__name__}")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include an unambiguous timezone")
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _bounded_text(value: str) -> str:
    return value if len(value) <= _MAX_EVIDENCE_TEXT else value[:_MAX_EVIDENCE_TEXT] + "…"


def json_safe_evidence_v1(value: Any, *, _depth: int = 0) -> Any:
    """Convert malformed input into bounded JSON-safe diagnostic evidence.

    This codec never feeds malformed values into ``set``, ``Counter`` or a
    heterogeneous sort.  Unknown objects are represented by type only, avoiding
    process-specific memory addresses and a second exception from ``repr``.
    """

    if _depth >= _MAX_EVIDENCE_DEPTH:
        return {"__truncated__": "max_depth", "type": type(value).__name__}
    if value is None or type(value) in (bool, int, str):
        return _bounded_text(value) if type(value) is str else value
    if isinstance(value, float):
        if math.isfinite(value):
            return value
        return {"__type__": "float", "value": str(value)}
    if isinstance(value, Decimal):
        return {"__type__": "Decimal", "value": str(value)}
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return {"__type__": "datetime", "value": value.isoformat(), "timezone": "missing"}
        return {"__type__": "datetime", "value": canonical_utc_datetime_v1(value)}
    if isinstance(value, bytes):
        return {
            "__type__": "bytes",
            "length": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
        }
    if isinstance(value, BaseModel):
        return json_safe_evidence_v1(value.model_dump(mode="json"), _depth=_depth + 1)
    if isinstance(value, Mapping):
        items = list(value.items())
        truncated = len(items) > _MAX_EVIDENCE_ITEMS
        items = items[:_MAX_EVIDENCE_ITEMS]
        if all(type(key) is str for key, _ in items):
            result = {str(key): json_safe_evidence_v1(member_value, _depth=_depth + 1) for key, member_value in items}
            if truncated:
                result["__truncated_items__"] = len(value) - _MAX_EVIDENCE_ITEMS
            return result
        return {
            "__type__": type(value).__name__,
            "items": [
                {
                    "key": json_safe_evidence_v1(key, _depth=_depth + 1),
                    "value": json_safe_evidence_v1(member_value, _depth=_depth + 1),
                }
                for key, member_value in items
            ],
            "truncated": truncated,
        }
    if isinstance(value, (list, tuple)):
        items = list(value)
        result = [json_safe_evidence_v1(item, _depth=_depth + 1) for item in items[:_MAX_EVIDENCE_ITEMS]]
        if len(items) > _MAX_EVIDENCE_ITEMS:
            result.append({"__truncated_items__": len(items) - _MAX_EVIDENCE_ITEMS})
        return result
    if isinstance(value, (set, frozenset)):
        return {"__type__": type(value).__name__, "item_count": len(value)}
    if isinstance(value, BaseException):
        return {
            "__type__": f"{type(value).__module__}.{type(value).__qualname__}",
            "message": _bounded_text(str(value)),
        }
    return {"__type__": f"{type(value).__module__}.{type(value).__qualname__}"}


__all__ = [
    "FrozenJsonArrayV1",
    "FrozenJsonMemberV1",
    "FrozenJsonObjectV1",
    "FrozenJsonScalarV1",
    "FrozenJsonValueV1",
    "canonical_decimal_string_v1",
    "canonical_json_bytes_v1",
    "canonical_utc_datetime_v1",
    "digest_bytes_v1",
    "freeze_json_v1",
    "hash_hex_v1",
    "json_safe_evidence_v1",
    "require_identity_v1",
    "require_sha256_v1",
    "thaw_json_v1",
    "validate_json_text_v1",
]
