"""Pure, versioned integrity contracts used by workers and read-only consumers."""
from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from enum import Enum
import hashlib
import json
import math
from typing import Any, Mapping, Sequence

CONTRACT_VERSION = "aistock_daily_integrity_v1"
CANONICAL_SOURCE = "canonical_db"


class PartitionState(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    NOT_DUE = "NOT_DUE"
    WAIT_UPSTREAM = "WAIT_UPSTREAM"
    VERIFYING = "VERIFYING"
    VALID = "VALID"
    INCOMPLETE = "INCOMPLETE"
    ERROR = "ERROR"
    NOT_APPLICABLE = "NOT_APPLICABLE"

    def __str__(self):
        return self.value


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                    ensure_ascii=False, allow_nan=False, default=str).encode()).hexdigest()


def finite_number(value: Any, *, positive: bool = False) -> bool:
    """Zero is a real fact unless the existing field contract requires positive."""
    try:
        return (value is not None and not isinstance(value, (bool, str))
                and math.isfinite(value) and (not positive or value > 0))
    except (ValueError, TypeError, OverflowError):
        return False


@dataclass(frozen=True)
class CheckCounts:
    """One unit and disjoint classes. None means unknown, never implicit zero."""
    unit: str
    expected: int | None
    valid: int | None
    legal_na: int | None
    missing: int | None
    invalid: int | None
    unverified: int | None

    def __post_init__(self):
        if not self.unit:
            raise ValueError("integrity count unit is required")
        values = [self.expected, self.valid, self.legal_na, self.missing, self.invalid, self.unverified]
        if any(value is not None and (isinstance(value, bool) or not isinstance(value, int) or value < 0)
               for value in values):
            raise ValueError("integrity counts must be non-negative integers or unknown")
        if self.expected is not None and all(value is not None for value in values[1:]):
            if sum(values[1:]) != self.expected:
                raise ValueError("integrity count classes must partition the expected set")

    @property
    def complete(self) -> bool:
        return (self.expected is not None and self.valid is not None and self.legal_na is not None
                and self.missing == self.invalid == self.unverified == 0
                and self.valid + self.legal_na == self.expected)


@dataclass(frozen=True)
class ValidationResult:
    state: PartitionState
    counts: tuple[CheckCounts, ...]
    checks: Mapping[str, Any]
    gaps: tuple[Mapping[str, Any], ...] = ()
    reason: str | None = None

    def __post_init__(self):
        if self.state == PartitionState.VALID and (not self.counts or not all(c.complete for c in self.counts)):
            raise ValueError("VALID requires complete, non-vacuous checks")
        if self.state == PartitionState.NOT_APPLICABLE and not self.reason:
            raise ValueError("NOT_APPLICABLE requires a formal applicability reason")

    def as_dict(self) -> dict[str, Any]:
        return {"state": self.state.value, "counts": [asdict(c) for c in self.counts],
                "checks": dict(self.checks), "gaps": [dict(g) for g in self.gaps], "reason": self.reason}


def effective_partition(row: Mapping[str, Any] | None, *, scope_version: str, rule_version: str) -> dict[str, Any]:
    """A delayed materialized daily summary must never conceal committed dirtiness."""
    result = dict(row or {})
    current = (bool(row) and type(row.get("data_revision")) is int and row["data_revision"] >= 0
               and type(row.get("validated_revision")) is int
               and row.get("validated_revision") == row.get("data_revision")
               and row.get("scope_version") == scope_version and row.get("rule_version") == rule_version
               and isinstance(row.get("validation_id"), str) and bool(row["validation_id"].strip()))
    state = row.get("validation_status", "UNVERIFIED") if row else "UNVERIFIED"
    if not current:
        state = "UNVERIFIED"
    result.update(state=state, current=bool(current), source_complete=current and state in {"VALID", "NOT_APPLICABLE"})
    return result


def aggregate_day(partitions: Sequence[Mapping[str, Any]], *, trading_day: bool,
                  catalogue_known: bool = True, future: bool = False) -> dict[str, Any]:
    if not trading_day:
        return {"state": "NOT_TRADING", "all_sync_complete": False}
    if future or not catalogue_known or not partitions:
        return {"state": "UNVERIFIED", "all_sync_complete": False}
    states = {str(p["state"]) for p in partitions}
    complete = all(p.get("source_complete") is True for p in partitions)
    if "ERROR" in states:
        state = "ERROR"
    elif "INCOMPLETE" in states:
        state = "PARTIAL"
    elif complete:
        state = "COMPLETE"
    elif "UNVERIFIED" in states:
        state = "UNVERIFIED"
    elif any(p.get("overdue") and p.get("state") in {"WAIT_UPSTREAM", "NOT_DUE"} for p in partitions):
        state = "PARTIAL"
    else:
        state = "IN_PROGRESS"
    return {"state": state, "all_sync_complete": complete}


def completion_kind(previous: Mapping[str, Any] | None, *, as_of: datetime,
                    due_at: datetime | None, timing_known: bool) -> tuple[str, bool | None]:
    if previous and previous.get("gap_since_complete"):
        return "RECOVERED", None
    if previous and previous.get("ever_complete"):
        return "SOURCE_REVISED", None
    if not timing_known or due_at is None:
        return "HISTORICAL_VERIFIED", None
    on_time = as_of <= due_at
    return ("ON_TIME" if on_time else "HISTORICAL_VERIFIED"), on_time


@dataclass(frozen=True)
class PartitionRef:
    dataset: str
    fact_date: date
    data_revision: int
    validation_id: str
    scope_version: str
    rule_version: str

    def as_dict(self):
        return {**asdict(self), "fact_date": self.fact_date.isoformat()}


def release_contains_revision(ref: Mapping[str, Any], pins: Sequence[Mapping[str, Any]] | None,
                              *, cutoff: date, fact_date: date) -> bool | None:
    if fact_date > cutoff:
        return False
    if pins is None:
        return None
    return any(all(pin.get(key) == ref.get(key) for key in
                   ("dataset", "fact_date", "data_revision", "validation_id", "scope_version", "rule_version"))
               for pin in pins)
