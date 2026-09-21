"""Deterministic source checks and impact classification for monthly releases.

The helpers operate on rows read by data-owned adapters inside one database
snapshot.  They never query a runtime provider, infer PASS from an exit code,
or repair production data.  Adapters must persist the snapshot identity and
the exact exceptions used by each full-scope check.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import hashlib
import math
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .canonical import canonical_json_bytes, ensure_sha256
from .monthly_unified import SOURCE_GATES, SourceChange


class MonthlySourceAuditError(RuntimeError):
    pass


TYPED_EXCEPTION_REASONS = frozenset(
    {
        "LISTING_PREHISTORY",
        "DELIST_POSTHISTORY",
        "SUSPEND_FULL_DAY",
        "SUSPEND_INTRADAY",
        "SOURCE_NOT_APPLICABLE",
        "OFFICIAL_QUOTE_UNAVAILABLE",
    }
)


@dataclass(frozen=True, slots=True)
class TypedGap:
    dataset: str
    symbol: str
    start: str
    end: str
    field: str
    reason_code: str
    authority_sha256: str | None = None

    def __post_init__(self) -> None:
        if not self.dataset or not self.symbol or not self.start or not self.end or not self.field:
            raise ValueError("typed gap identity is incomplete")
        if self.reason_code not in TYPED_EXCEPTION_REASONS:
            raise ValueError("typed gap reason is not registered")
        if self.authority_sha256 is None:
            raise ValueError("typed gap requires an authority hash")
        ensure_sha256(self.authority_sha256, field="authority_sha256")

    def payload(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "symbol": self.symbol,
            "start": self.start,
            "end": self.end,
            "field": self.field,
            "reason_code": self.reason_code,
            "authority_sha256": self.authority_sha256,
        }


@dataclass(frozen=True, slots=True)
class SourceGateEvidence:
    gate: str
    snapshot_group_id: str
    expectation_contract_ref: str
    readback_ref: str
    expected_count: int
    observed_count: int
    explained_missing_count: int = 0
    unexplained_missing_count: int = 0
    duplicate_count: int = 0
    invalid_value_count: int = 0
    exception_refs: tuple[TypedGap, ...] = ()

    def __post_init__(self) -> None:
        if self.gate not in SOURCE_GATES:
            raise ValueError("source gate is not registered")
        if not self.snapshot_group_id.strip():
            raise ValueError("source snapshot identity is empty")
        if not self.expectation_contract_ref.strip() or not self.readback_ref.strip():
            raise ValueError("source gate contract/readback reference is empty")
        for reference in (self.expectation_contract_ref, self.readback_ref):
            path = Path(reference)
            if path.is_absolute() or ".." in path.parts or "\\" in reference:
                raise ValueError("source gate contract/readback reference is not portable")
        for value in (
            self.expected_count,
            self.observed_count,
            self.explained_missing_count,
            self.unexplained_missing_count,
            self.duplicate_count,
            self.invalid_value_count,
        ):
            if type(value) is not int or value < 0:
                raise ValueError("source gate counts must be non-negative integers")
        if self.observed_count + self.explained_missing_count + self.unexplained_missing_count != self.expected_count:
            raise ValueError("source gate denominator does not close")
        if self.explained_missing_count and not self.exception_refs:
            raise ValueError("explained source gaps require typed exception references")
        if not self.explained_missing_count and self.exception_refs:
            raise ValueError("source exception references require explained gaps")

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": "aistock_monthly_source_gate_v2",
            "gate_id": self.gate,
            "snapshot_group_id": self.snapshot_group_id,
            "expectation_contract_ref": self.expectation_contract_ref,
            "readback_ref": self.readback_ref,
            "expected_count": self.expected_count,
            "observed_count": self.observed_count,
            "explained_missing_count": self.explained_missing_count,
            "unexplained_missing_count": self.unexplained_missing_count,
            "duplicate_count": self.duplicate_count,
            "invalid_value_count": self.invalid_value_count,
            "exception_refs": [item.payload() for item in self.exception_refs],
            "status": "PASS"
            if not (self.unexplained_missing_count or self.duplicate_count or self.invalid_value_count)
            else "BLOCKED",
        }


def close_source_audit(
    *,
    cutoff: date,
    predecessor_cutoff: date,
    gates: Sequence[SourceGateEvidence],
    changes: Sequence[SourceChange],
) -> dict[str, Any]:
    """Close all gates only when they came from exactly one consistent view."""

    by_name = {gate.gate: gate for gate in gates}
    if len(by_name) != len(gates) or set(by_name) != set(SOURCE_GATES):
        raise MonthlySourceAuditError("source audit must cover every registered gate exactly once")
    snapshot_ids = {gate.snapshot_group_id for gate in gates}
    if len(snapshot_ids) != 1:
        raise MonthlySourceAuditError("source audit gates came from different snapshots")
    blocking = {
        gate.gate: {
            "unexplained_missing_count": gate.unexplained_missing_count,
            "duplicate_count": gate.duplicate_count,
            "invalid_value_count": gate.invalid_value_count,
        }
        for gate in gates
        if gate.unexplained_missing_count or gate.duplicate_count or gate.invalid_value_count
    }
    if blocking:
        raise MonthlySourceAuditError(f"source audit contains blocking counts: {blocking}")
    if cutoff <= predecessor_cutoff:
        raise MonthlySourceAuditError("source audit cutoff must advance the predecessor")
    payload = {
        "schema_version": "aistock_monthly_source_audit_v2",
        "cutoff": cutoff.isoformat(),
        "predecessor_cutoff": predecessor_cutoff.isoformat(),
        "snapshot_group_id": snapshot_ids.pop(),
        "gates": {name: by_name[name].payload() for name in SOURCE_GATES},
        "changes": [change.payload() for change in changes],
        "unexplained_gap_count": 0,
        "database_write_performed": False,
        "runtime_fallback": False,
    }
    payload["canonical_sha256"] = hashlib.sha256(canonical_json_bytes(payload)).hexdigest()
    return payload


def cn_a_share_minute_labels(trade_date: date) -> tuple[datetime, ...]:
    """Return the canonical 240 close-labelled bars (09:31..11:30, 13:01..15:00)."""

    labels: list[datetime] = []
    for start, count in ((time(9, 31), 120), (time(13, 1), 120)):
        current = datetime.combine(trade_date, start)
        labels.extend(current + timedelta(minutes=offset) for offset in range(count))
    return tuple(labels)


def validate_trading_calendar(*, sessions: Sequence[date], cutoff: date) -> dict[str, Any]:
    if not sessions or list(sessions) != sorted(set(sessions)):
        raise MonthlySourceAuditError("trading calendar must be non-empty, unique and ordered")
    if sessions[-1] != cutoff:
        raise MonthlySourceAuditError("trading calendar does not end at the requested cutoff")
    return {
        "status": "PASS",
        "start": sessions[0].isoformat(),
        "end": sessions[-1].isoformat(),
        "session_count": len(sessions),
    }


def validate_daily_rows(
    *,
    expected_keys: Iterable[tuple[str, date]],
    rows: Sequence[Mapping[str, Any]],
    explained_absences: Mapping[tuple[str, date], TypedGap] | None = None,
) -> dict[str, Any]:
    expected = set(expected_keys)
    exceptions = dict(explained_absences or {})
    indexed: dict[tuple[str, date], Mapping[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("ts_code") or "")
        raw_date = row.get("trade_date")
        trade_date = raw_date if isinstance(raw_date, date) else date.fromisoformat(str(raw_date))
        key = (symbol, trade_date)
        if key in indexed:
            raise MonthlySourceAuditError(f"duplicate daily row: {symbol} {trade_date}")
        indexed[key] = row
    extras = sorted(set(indexed).difference(expected))
    if extras:
        raise MonthlySourceAuditError(f"daily readback contains unexpected keys: {extras[:3]}")
    missing = sorted(expected.difference(indexed))
    unresolved: list[tuple[str, date]] = []
    for key in missing:
        exception = exceptions.get(key)
        if (
            exception is None
            or exception.symbol != key[0]
            or exception.start != key[1].isoformat()
            or exception.end != key[1].isoformat()
            or exception.reason_code not in {"LISTING_PREHISTORY", "DELIST_POSTHISTORY", "SUSPEND_FULL_DAY"}
        ):
            unresolved.append(key)
    if unresolved:
        raise MonthlySourceAuditError(f"daily coverage contains unexplained gaps: {unresolved[:3]}")
    required_fields = ("open", "high", "low", "close", "pre_close", "vol", "amount")
    invalid: list[tuple[str, date, str]] = []
    for key, row in indexed.items():
        values: dict[str, float] = {}
        for field in required_fields:
            raw = row.get(field)
            if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(float(raw)):
                invalid.append((*key, field))
                continue
            values[field] = float(raw)
        if len(values) != len(required_fields):
            continue
        if (
            min(values[field] for field in ("open", "high", "low", "close", "pre_close")) <= 0
            or values["high"] < max(values["open"], values["low"], values["close"])
            or values["low"] > min(values["open"], values["high"], values["close"])
            or values["vol"] < 0
            or values["amount"] < 0
        ):
            invalid.append((*key, "ohlcv_domain"))
    if invalid:
        raise MonthlySourceAuditError(f"daily rows contain invalid values: {invalid[:3]}")
    return {
        "status": "PASS",
        "expected_count": len(expected),
        "observed_count": len(indexed),
        "explained_missing_count": len(missing),
        "unexplained_missing_count": 0,
        "duplicate_count": 0,
        "invalid_value_count": 0,
    }


def validate_six_pool_market_coverage(
    *,
    pools: Mapping[str, Sequence[tuple[str, date, date]]],
    sessions: Sequence[date],
    daily_keys: Iterable[tuple[str, date]],
    minute_keys: Iterable[tuple[str, date]],
    suspended_keys: Iterable[tuple[str, date]],
    minute_start: date,
) -> dict[str, Any]:
    required_pools = {
        "stock_universe",
        "csi300",
        "csi500",
        "csi1000",
        "star50",
        "star100",
    }
    if set(pools) != required_pools:
        raise MonthlySourceAuditError("PIT pool set differs from the six-pool contract")
    calendar = set(sessions)
    daily = set(daily_keys)
    minute = set(minute_keys)
    suspended = set(suspended_keys)
    result: dict[str, Any] = {}
    for pool_name in sorted(required_pools):
        by_symbol: dict[str, list[tuple[date, date]]] = {}
        for symbol, start, end in pools[pool_name]:
            if end < start:
                raise MonthlySourceAuditError(f"PIT pool span is inverted: {pool_name} {symbol}")
            by_symbol.setdefault(symbol, []).append((start, end))
        for symbol, spans in by_symbol.items():
            ordered = sorted(spans)
            if any(ordered[index][0] <= ordered[index - 1][1] for index in range(1, len(ordered))):
                raise MonthlySourceAuditError(f"PIT pool spans overlap: {pool_name} {symbol}")
        daily_gaps: list[tuple[str, date]] = []
        minute_gaps: list[tuple[str, date]] = []
        expected_count = 0
        for symbol, spans in by_symbol.items():
            for start, end in spans:
                for trade_date in sessions:
                    if trade_date not in calendar or trade_date < start or trade_date > end:
                        continue
                    expected_count += 1
                    key = (symbol, trade_date)
                    if key not in daily and key not in suspended:
                        daily_gaps.append(key)
                    if trade_date >= minute_start and key not in minute and key not in suspended:
                        minute_gaps.append(key)
        if daily_gaps or minute_gaps:
            raise MonthlySourceAuditError(
                f"PIT pool market coverage is incomplete: {pool_name} daily={daily_gaps[:3]} minute={minute_gaps[:3]}"
            )
        result[pool_name] = {
            "symbol_count": len(by_symbol),
            "expected_symbol_date_count": expected_count,
            "day_gap_count": 0,
            "minute_gap_count": 0,
        }
    return result


def validate_daily_minute_parity(
    *,
    symbol: str,
    trade_date: date,
    daily: Mapping[str, float],
    minute_rows: Sequence[Mapping[str, Any]],
    suspended: bool,
    suspend_authority_sha256: str | None = None,
    typed_exception: TypedGap | None = None,
    price_tolerance: float = 1e-4,
    volume_tolerance: float = 1e-4,
    amount_tolerance: float = 1e-4,
) -> dict[str, Any]:
    expected = cn_a_share_minute_labels(trade_date)
    indexed: dict[datetime, Mapping[str, Any]] = {}
    forbidden: list[str] = []
    for row in minute_rows:
        raw = row.get("datetime")
        timestamp = raw if isinstance(raw, datetime) else datetime.fromisoformat(str(raw))
        if timestamp.time() in {time(9, 0), time(13, 0)}:
            forbidden.append(timestamp.isoformat(sep=" "))
        if timestamp in indexed:
            raise MonthlySourceAuditError(f"duplicate minute bar: {symbol} {timestamp}")
        indexed[timestamp] = row
    if forbidden:
        raise MonthlySourceAuditError(f"non-session minute labels exist: {symbol} {forbidden[:3]}")
    if suspended:
        if suspend_authority_sha256 is None:
            raise MonthlySourceAuditError("suspended day lacks an authority hash")
        ensure_sha256(suspend_authority_sha256, field="suspend_authority_sha256")
        if minute_rows:
            raise MonthlySourceAuditError("suspended day unexpectedly contains minute bars")
        return {"status": "EXPLAINED_SUSPEND", "bar_count": 0, "gap_count": 0}
    missing = [stamp for stamp in expected if stamp not in indexed]
    extras = [stamp for stamp in indexed if stamp not in set(expected)]
    if missing or extras:
        if typed_exception is None:
            raise MonthlySourceAuditError(
                f"minute session coverage differs: {symbol} missing={len(missing)} extras={len(extras)}"
            )
        if typed_exception.reason_code != "SUSPEND_INTRADAY":
            raise MonthlySourceAuditError("minute session exception is not an intraday suspend")
        if (
            typed_exception.symbol != symbol
            or typed_exception.start != trade_date.isoformat()
            or typed_exception.end != trade_date.isoformat()
            or typed_exception.dataset != "kline_minute_raw"
        ):
            raise MonthlySourceAuditError("minute session exception identity differs")
        return {
            "status": "EXPLAINED_TYPED_EXCEPTION",
            "bar_count": len(indexed),
            "gap_count": 0,
            "exception": typed_exception.payload(),
        }
    rows = [indexed[stamp] for stamp in expected]
    required_fields = ("open", "high", "low", "close", "vol", "amount")
    for row in rows:
        values: dict[str, float] = {}
        for field in required_fields:
            raw = row.get(field)
            if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(float(raw)):
                raise MonthlySourceAuditError(f"minute row contains a non-finite field: {symbol} {field}")
            values[field] = float(raw)
        if (
            min(values[field] for field in ("open", "high", "low", "close")) <= 0
            or values["high"] < max(values["open"], values["low"], values["close"])
            or values["low"] > min(values["open"], values["high"], values["close"])
            or values["vol"] < 0
            or values["amount"] < 0
        ):
            raise MonthlySourceAuditError(f"minute row violates OHLCV domain: {symbol}")
    aggregates = {
        "open": float(rows[0]["open"]),
        "high": max(float(row["high"]) for row in rows),
        "low": min(float(row["low"]) for row in rows),
        "close": float(rows[-1]["close"]),
        "vol": sum(float(row["vol"]) for row in rows),
        "amount": sum(float(row["amount"]) for row in rows),
    }
    tolerances = {
        "open": price_tolerance,
        "high": price_tolerance,
        "low": price_tolerance,
        "close": price_tolerance,
        "vol": volume_tolerance,
        "amount": amount_tolerance,
    }
    mismatches = {
        key: {"daily": float(daily[key]), "minute": value}
        for key, value in aggregates.items()
        if not math.isclose(float(daily[key]), value, rel_tol=tolerances[key], abs_tol=tolerances[key])
    }
    if mismatches:
        raise MonthlySourceAuditError(f"daily/minute aggregate mismatch: {symbol} {mismatches}")
    return {"status": "PASS", "bar_count": 240, "gap_count": 0, "aggregates": aggregates}


def validate_daily_basic_rows(
    *,
    expected_symbols: Iterable[str],
    rows: Sequence[Mapping[str, Any]],
    required_finite_fields: Sequence[str] = ("turnover_rate", "turnover_rate_f", "volume_ratio"),
    nullable: Mapping[tuple[str, str], TypedGap] | None = None,
) -> dict[str, Any]:
    expected = set(expected_symbols)
    exceptions = dict(nullable or {})
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("ts_code") or "")
        if symbol in indexed:
            raise MonthlySourceAuditError(f"duplicate daily_basic row: {symbol}")
        indexed[symbol] = row
    missing_rows = sorted(expected.difference(indexed))
    missing_fields: list[dict[str, str]] = []
    for symbol in sorted(expected.intersection(indexed)):
        for field in required_finite_fields:
            raw = indexed[symbol].get(field)
            finite = isinstance(raw, (int, float)) and not isinstance(raw, bool) and math.isfinite(float(raw))
            if not finite:
                exception = exceptions.get((symbol, field))
                if (
                    exception is None
                    or exception.dataset != "daily_basic"
                    or exception.symbol != symbol
                    or exception.field != field
                    or exception.reason_code != "SOURCE_NOT_APPLICABLE"
                ):
                    missing_fields.append({"symbol": symbol, "field": field})
    if missing_rows or missing_fields:
        raise MonthlySourceAuditError(
            f"daily_basic coverage incomplete rows={len(missing_rows)} fields={len(missing_fields)}"
        )
    return {
        "status": "PASS",
        "expected_symbol_count": len(expected),
        "row_count": len(indexed),
        "typed_nullable_count": len(exceptions),
        "gap_count": 0,
    }


def classify_adj_factor_change(
    *,
    symbol: str,
    old: Mapping[date, float],
    new: Mapping[date, float],
    receipt_sha256: str,
    tolerance: float = 1e-10,
) -> SourceChange | None:
    """Classify factor drift without assuming every upstream revision is an action."""

    ensure_sha256(receipt_sha256, field="receipt_sha256")
    if not old or not new:
        raise MonthlySourceAuditError("adj factor comparison requires old and new history")
    overlap = sorted(set(old).intersection(new))
    if not overlap or min(new) > min(old) or max(new) < max(old):
        raise MonthlySourceAuditError("adj factor comparison lacks the complete predecessor history")
    ratios: list[float] = []
    changed_dates: list[date] = []
    for trade_date in overlap:
        old_value = float(old[trade_date])
        new_value = float(new[trade_date])
        if not math.isfinite(old_value) or not math.isfinite(new_value) or old_value <= 0 or new_value <= 0:
            raise MonthlySourceAuditError("adj factor values must be finite and positive")
        ratio = new_value / old_value
        ratios.append(ratio)
        if not math.isclose(old_value, new_value, rel_tol=tolerance, abs_tol=tolerance):
            changed_dates.append(trade_date)
    appended = sorted(set(new).difference(old))
    if not changed_dates:
        if not appended:
            return None
        kind = "TAIL_APPEND" if min(appended) > max(old) else "HISTORICAL_REPAIR"
        affected = appended
    elif max(ratios) - min(ratios) <= tolerance * max(1.0, abs(max(ratios))):
        kind = "ADJ_DENOMINATOR_CHANGE"
        affected = overlap + appended
    else:
        kind = "ADJ_HISTORY_RESTATEMENT"
        affected = changed_dates + appended
    return SourceChange(
        dataset="adj_factor",
        fields=("adj_factor",),
        instruments=(symbol,),
        start=min(affected),
        end=max(affected),
        kind=kind,
        source_receipt_sha256=receipt_sha256,
    )


__all__ = (
    "MonthlySourceAuditError",
    "SourceGateEvidence",
    "TypedGap",
    "classify_adj_factor_change",
    "close_source_audit",
    "cn_a_share_minute_labels",
    "validate_daily_basic_rows",
    "validate_daily_rows",
    "validate_daily_minute_parity",
    "validate_six_pool_market_coverage",
    "validate_trading_calendar",
)
