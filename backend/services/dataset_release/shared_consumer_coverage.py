"""Shared source semantics; never constructs features, fills values or activates data.

Causal market-cap facts are independent of executable PIT intervals.  Only the
latest *observed* source row strictly before the consuming date is considered;
an invalid latest value does not authorize falling back to an older valid one.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
import itertools
import math
from numbers import Real
from typing import Any, Iterable, Mapping, Sequence


CAUSAL_COVERAGE_SCHEMA = "aistock_shared_causal_source_history_coverage_v1"


def _day(value: Any) -> date:
    if isinstance(value, datetime):
        raise ValueError("source fact dates must not include timestamps")
    return value if type(value) is date else date.fromisoformat(str(value))


def _positive(value: Any) -> bool:
    return isinstance(value, (Real, Decimal)) and not isinstance(value, bool) and math.isfinite(float(value)) and value > 0


def audit_causal_source_history(
    rows: Iterable[Mapping[str, Any]], *, calendar: Sequence[date],
    spans: Sequence[tuple[str, date, date]], history_start: date,
    approved_warmup_keys: frozenset[tuple[str, date]] = frozenset(),
    not_applicable_keys: frozenset[tuple[str, date]] = frozenset(),
    source_identity: Any = None,
    required_dates: frozenset[date] | None = None,
    emit: Any = None,
    on_required_key: Any = None,
) -> dict[str, Any]:
    """Stream bounded ordered source rows with O(securities) causal state.

    No facts are synthesized and no latest finite fallback is permitted. The
    exact source-window first day can be a no-prior warmup; arbitrary missing
    dates require an explicit consumer-approved exact key, never a ratio rule.
    """
    if not calendar or tuple(calendar) != tuple(sorted(set(calendar))) or history_start > calendar[0]:
        raise ValueError("causal source calendar/window is invalid")
    if required_dates is not None and not required_dates.issubset(calendar):
        raise ValueError("required causal dates escape the source calendar")
    by_symbol: dict[str, list[tuple[date, date]]] = defaultdict(list)
    for symbol, left, right in spans:
        if not symbol or left > right:
            raise ValueError("PIT interval identity is invalid")
        by_symbol[symbol].append((left, right))
    for values in by_symbol.values():
        ordered = sorted(values)
        if any(right >= next_left for (_, right), (next_left, _) in zip(ordered, ordered[1:])):
            raise ValueError("PIT intervals overlap")
    previous = None

    def ordered_rows():
        nonlocal previous
        for row in rows:
            day, symbol = _day(row["trade_date"]), str(row["ts_code"])
            key = (day, symbol)
            if previous is not None and key <= previous:
                raise ValueError("source rows are not ordered unique date/security facts")
            previous = key
            if history_start <= day <= calendar[-1]:
                yield day, symbol, row.get("circ_mv")

    grouped = iter(itertools.groupby(ordered_rows(), key=lambda row: row[0]))
    pending = next(grouped, None)
    latest: dict[str, tuple[date, Any]] = {}
    result: dict[str, Any] = {"schema_version": CAUSAL_COVERAGE_SCHEMA, "history_start": history_start.isoformat(),
        "required_start": calendar[0].isoformat(), "required_end": calendar[-1].isoformat(),
        "expected": 0, "resolved": 0, "explained_warmup": 0, "not_applicable": 0,
        "unexplained": 0, "pre_entry_facts_used": 0, "issues": []}
    for position, day in enumerate(calendar):
        while pending is not None and pending[0] < day:
            for source_day, source, cap in pending[1]:
                latest[source] = (source_day, cap)
            pending = next(grouped, None)
        if required_dates is not None and day not in required_dates:
            continue
        for symbol, values in by_symbol.items():
            active = [left for left, right in values if left <= day <= right]
            if not active:
                continue
            result["expected"] += 1
            if (symbol, day) in not_applicable_keys:
                result["not_applicable"] += 1
                continue
            prior_day = calendar[position - 1] if position else None
            source = (
                source_identity.resolve(symbol, prior_day, "market.daily_basic").source_ts_code
                if source_identity is not None and prior_day is not None else symbol
            )
            prior = latest.get(source)
            if prior is None and (day == history_start or (symbol, day) in approved_warmup_keys):
                result["explained_warmup"] += 1
            elif prior is not None and _positive(prior[1]):
                result["resolved"] += 1
                result["pre_entry_facts_used"] += int(prior[0] < active[0])
            else:
                result["unexplained"] += 1
                issue = {"symbol": symbol, "trade_date": day.isoformat(),
                    "source_date": None if prior is None else prior[0].isoformat(),
                    "reason": "NO_STRICT_PRIOR_SOURCE_FACT" if prior is None else "LATEST_CAUSAL_VALUE_INVALID"}
                if emit is None:
                    result["issues"].append(issue)
                else:
                    emit(issue)
            if on_required_key is not None:
                is_warmup = prior is None and (day == history_start or (symbol, day) in approved_warmup_keys)
                if not is_warmup:
                    on_required_key(symbol, day, prior)
    # Consume the complete bounded stream so duplicates/ordering errors on the
    # last day cannot evade validation simply because no later weight is read.
    while pending is not None:
        for _ in pending[1]:
            pass
        pending = next(grouped, None)
    if result["expected"] != sum(result[key] for key in ("resolved", "explained_warmup", "not_applicable", "unexplained")):
        raise ValueError("causal coverage denominator does not close")
    result["status"] = "BLOCKED" if result["unexplained"] else "PASS"
    return result


def validate_live_pit_lease_identity(
    binding: Mapping[str, Any], *, native_leases: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Inspect native leases only; a hypothetical lease is not business proof."""
    if binding.get("authority_status") != "ACTIVE_CANONICAL" or type(binding.get("activation_generation")) is not int or binding["activation_generation"] < 1:
        return {"status": "BLOCKED", "reason": "CANONICAL_AUTHORITY_ACTIVATION_REQUIRED"}
    if not native_leases:
        return {"status": "BLOCKED", "reason": "NATIVE_SELECTION_LEASE_NOT_OBSERVED"}
    from backend.services.selection_center.canonical_pit_runtime import SelectionPitRuntimeLease
    fields = ("authority_id", "authority_status", "universe_key", "rule_version", "rule_parameters_digest", "activation_generation",
              "activation_envelope_digest", "expected_source_commit", "state_source_digest", "coverage_start", "coverage_end")
    try:
        for raw in native_leases:
            lease = SelectionPitRuntimeLease.from_mapping(raw).as_dict()
            if any(lease.get(key) != binding.get(key) for key in fields):
                return {"status": "BLOCKED", "reason": "NATIVE_SELECTION_LEASE_IDENTITY_DRIFT"}
    except (TypeError, ValueError, RuntimeError) as exc:
        return {"status": "BLOCKED", "reason": "NATIVE_SELECTION_LEASE_INVALID", "error_type": type(exc).__name__}
    return {"status": "PASS", "native_lease_count": len(native_leases), "generation": binding["activation_generation"]}
