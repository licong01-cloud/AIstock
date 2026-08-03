from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal, ROUND_HALF_EVEN
from typing import Any

from pydantic import BaseModel, ConfigDict

from backend.services.advisory_historical_range.canonical import canonical_json_sha256
from backend.services.advisory_historical_range.models import require_sha256


class FrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def canonical_model_hash(model: BaseModel, *, exclude: set[str]) -> str:
    return canonical_json_sha256(model.model_dump(mode="python", exclude=exclude))


def validated_hash(value: str | None, *, field_name: str) -> str | None:
    return require_sha256(value, field_name=field_name) if value is not None else None


def strict_identifier(value: str, *, field_name: str) -> str:
    if value != value.strip() or not value:
        raise ValueError(f"{field_name} must be non-empty without surrounding whitespace")
    return value


def utc_datetime(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def quantize_12(value: Decimal | str | int | float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_EVEN)


def set_computed_hash(
    instance: BaseModel,
    *,
    field_name: str,
    exclude: set[str],
) -> None:
    supplied = getattr(instance, field_name)
    digest = canonical_model_hash(instance, exclude=exclude)
    if supplied is not None and supplied != digest:
        raise ValueError(f"{field_name} differs from canonical payload")
    object.__setattr__(instance, field_name, digest)


def ensure_unique(values: tuple[Any, ...], *, field_name: str) -> tuple[Any, ...]:
    if len(set(values)) != len(values):
        raise ValueError(f"{field_name} must not contain duplicates")
    return values
