from __future__ import annotations

import hashlib
import json
import math
from datetime import date, datetime, time, timezone
from decimal import ROUND_HALF_EVEN, Decimal
from enum import Enum
from typing import Any

from pydantic import BaseModel


class HistoricalRangeCanonicalJSONError(ValueError):
    """Phase 1R canonical serializer rejected a non-deterministic value."""


def _decimal_scale_for_key(key: str | None) -> int:
    normalized = (key or "").lower()
    if "price" in normalized or normalized.endswith("_px"):
        return 6
    return 12


def _format_decimal(value: Decimal, *, key: str | None) -> str:
    if not value.is_finite():
        raise HistoricalRangeCanonicalJSONError("NaN and infinity are not allowed in canonical payloads")
    quantized = value.quantize(Decimal(1).scaleb(-_decimal_scale_for_key(key)), rounding=ROUND_HALF_EVEN)
    if quantized == 0:
        quantized = Decimal(0)
    return format(quantized, "f")


def canonicalize(value: Any, *, key: str | None = None) -> Any:
    if isinstance(value, BaseModel):
        return canonicalize(value.model_dump(mode="python", exclude_none=False), key=key)
    if isinstance(value, Enum):
        return canonicalize(value.value, key=key)
    if isinstance(value, Decimal):
        return _format_decimal(value, key=key)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise HistoricalRangeCanonicalJSONError("NaN and infinity are not allowed in canonical payloads")
        return _format_decimal(Decimal(str(value)), key=key)
    if isinstance(value, datetime):
        normalized = value.astimezone(timezone.utc) if value.tzinfo is not None else value
        return normalized.isoformat(timespec="microseconds")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(item_key): canonicalize(item_value, key=str(item_key))
            for item_key, item_value in sorted(value.items(), key=lambda item: str(item[0]))
        }
    if isinstance(value, (list, tuple)):
        return [canonicalize(item, key=key) for item in value]
    if isinstance(value, set):
        normalized = [canonicalize(item, key=key) for item in value]
        return sorted(normalized, key=canonical_json_text)
    if value is None or isinstance(value, (str, int, bool)):
        return value
    raise HistoricalRangeCanonicalJSONError(f"unsupported canonical payload type: {type(value).__name__}")


def canonical_json_text(payload: Any) -> str:
    return json.dumps(canonicalize(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def canonical_json_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json_text(payload).encode("utf-8")).hexdigest()
