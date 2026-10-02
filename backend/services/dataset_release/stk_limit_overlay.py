"""Bounded missing-or-incomplete ``stk_limit`` overlay for candidates."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from itertools import groupby
from typing import Any, Callable, Iterator

from .a_share_limit_rule import PRICE_LIMIT_RULE_VERSION, AShareLimitRuleError, derive_limit_prices
from .errors import DatasetReleaseError
from .pit import FrozenPitSnapshot


STK_LIMIT_RULE_OVERLAY_SCHEMA = "dataset_release_stk_limit_rule_overlay_v2"
MAX_STK_LIMIT_RULE_OVERLAY_ROWS = 50_000
PRICE_UNIT_DIVISOR = Decimal("1000")


class StkLimitRuleOverlayError(DatasetReleaseError):
    code = "BLOCKED_STK_LIMIT_RULE_OVERLAY_INVALID"


@dataclass(frozen=True, slots=True)
class LimitReferencePoint:
    close: Decimal
    close_adj_factor: Decimal
    close_date: date
    latest_adj_factor: Decimal
    latest_adj_date: date


@dataclass(frozen=True, slots=True)
class StkLimitRuleOverlayResult:
    overlay_rows: tuple[Mapping[str, str], ...]
    reference_state: Mapping[str, LimitReferencePoint]
    expected_pit_keys: int
    database_rows: int
    rule_derived_rows: int
    database_completion_rows: int
    database_override_rows: int
    reference_seed_rows: int
    unresolved_keys: int
    peak_code_partition_rows: int
    rule_version: str = PRICE_LIMIT_RULE_VERSION
    schema_version: str = STK_LIMIT_RULE_OVERLAY_SCHEMA


def build_stk_limit_rule_overlay(
    *,
    pit_snapshot: FrozenPitSnapshot,
    trading_dates: Sequence[date],
    partition_start: date,
    partition_end: date,
    database_limit_rows: Iterable[Mapping[str, Any]],
    daily_rows: Iterable[Mapping[str, Any]],
    adj_factor_rows: Iterable[Mapping[str, Any]],
    reference_state: Mapping[str, LimitReferencePoint] | None = None,
    reference_resolver: Callable[[str, date], LimitReferencePoint | None] | None = None,
    max_overlay_rows: int = MAX_STK_LIMIT_RULE_OVERLAY_ROWS,
) -> StkLimitRuleOverlayResult:
    """Derive exact missing/incomplete PIT keys one code partition at a time.

    A database row is authoritative only when all three price fields are
    present and valid.  A partial row may be completed, but every existing
    non-null value must match the deterministic rule result exactly.
    """

    if (
        pit_snapshot.cutoff < partition_end
        or partition_end < partition_start
        or tuple(sorted(set(trading_dates))) != tuple(trading_dates)
        or type(max_overlay_rows) is not int
        or not 0 < max_overlay_rows <= MAX_STK_LIMIT_RULE_OVERLAY_ROWS
    ):
        raise StkLimitRuleOverlayError("stk_limit overlay partition contract is invalid")
    partition_days = tuple(day for day in trading_dates if partition_start <= day <= partition_end)
    if not partition_days:
        raise StkLimitRuleOverlayError("stk_limit overlay partition has no trading dates")
    pit_codes = {span.ts_code for span in pit_snapshot.spans}
    spans_by_code: dict[str, list[Any]] = {}
    for span in pit_snapshot.spans:
        if span.ts_code not in spans_by_code:
            spans_by_code[span.ts_code] = []
        spans_by_code[span.ts_code].append(span)
    state = dict(reference_state or {})
    if set(state).difference(pit_codes):
        raise StkLimitRuleOverlayError("reference state contains a non-PIT instrument")
    database_count = 0
    completion_count = 0
    reference_seed_count = 0
    expected_count = 0
    limit_groups = _code_groups(
        database_limit_rows,
        source="stk_limit",
        date_field="trade_date",
    )
    daily_groups = _code_groups(
        daily_rows,
        source="kline_daily_raw",
        date_field="trade_date",
    )
    adj_groups = _code_groups(
        adj_factor_rows,
        source="adj_factor",
        date_field="trade_date",
    )
    limit_current = next(limit_groups, None)
    daily_current = next(daily_groups, None)
    adj_current = next(adj_groups, None)
    codes = sorted(pit_codes)
    overlay_rows: list[Mapping[str, str]] = []
    unresolved: list[tuple[str, date, str]] = []
    peak_code_rows = 0
    for code in codes:
        code_limits: tuple[Mapping[str, Any], ...] = ()
        code_daily: tuple[Mapping[str, Any], ...] = ()
        code_adj: tuple[Mapping[str, Any], ...] = ()
        if limit_current is not None and limit_current[0] == code:
            code_limits = limit_current[1]
            limit_current = next(limit_groups, None)
        elif limit_current is not None and limit_current[0] < code:
            raise StkLimitRuleOverlayError("stk_limit source code ordering differs")
        if daily_current is not None and daily_current[0] == code:
            code_daily = daily_current[1]
            daily_current = next(daily_groups, None)
        elif daily_current is not None and daily_current[0] < code:
            raise StkLimitRuleOverlayError("daily source code ordering differs")
        if adj_current is not None and adj_current[0] == code:
            code_adj = adj_current[1]
            adj_current = next(adj_groups, None)
        elif adj_current is not None and adj_current[0] < code:
            raise StkLimitRuleOverlayError("adj_factor source code ordering differs")
        peak_code_rows = max(peak_code_rows, len(code_limits) + len(code_daily) + len(code_adj))
        database_by_date = {
            _row_date(row, "trade_date", "stk_limit"): row for row in code_limits
        }
        database_count += len(code_limits)
        derivation_dates: set[date] = set()
        for span in spans_by_code.get(code, ()):
            left = bisect_left(partition_days, max(partition_start, span.eligible_start))
            right = bisect_right(partition_days, min(partition_end, span.eligible_end), left)
            for day in partition_days[left:right]:
                expected_count += 1
                row = database_by_date.get(day)
                if row is None or not _complete_limit_row(row):
                    derivation_dates.add(day)
        daily_by_date = {_row_date(row, "trade_date", "kline_daily_raw"): row for row in code_daily}
        adj_by_date = {_row_date(row, "trade_date", "adj_factor"): row for row in code_adj}
        point = state.get(code)
        latest_adj = point.latest_adj_factor if point is not None else None
        latest_adj_date = point.latest_adj_date if point is not None else None
        for day in partition_days:
            adj_row = adj_by_date.get(day)
            if adj_row is not None:
                latest_adj = _positive_decimal(adj_row.get("adj_factor"), field="adj_factor")
                latest_adj_date = day
            if day in derivation_dates:
                if point is None and reference_resolver is not None:
                    resolved_point = reference_resolver(code, day)
                    if resolved_point is not None:
                        _validate_reference_point(resolved_point, code=code, required_day=day)
                        source_adj_row = adj_by_date.get(resolved_point.close_date)
                        if source_adj_row is not None:
                            source_adj = _positive_decimal(
                                source_adj_row.get("adj_factor"),
                                field="adj_factor",
                            )
                            if source_adj != resolved_point.close_adj_factor:
                                raise StkLimitRuleOverlayError(
                                    "stk_limit reference provider/database adj_factor differs",
                                    context={
                                        "ts_code": code,
                                        "close_date": resolved_point.close_date.isoformat(),
                                        "database_value": format(source_adj, "f"),
                                        "provider_value": format(
                                            resolved_point.close_adj_factor,
                                            "f",
                                        ),
                                    },
                                )
                        point = resolved_point
                        reference_seed_count += 1
                if point is None or latest_adj is None or latest_adj_date is None:
                    unresolved.append((code, day, "missing_reference_price_or_adj_factor"))
                else:
                    try:
                        derived = derive_limit_prices(
                            ts_code=code,
                            trade_date=day,
                            previous_close=point.close,
                            previous_adj_factor=point.close_adj_factor,
                            current_adj_factor=latest_adj,
                            is_st=False,
                        )
                    except AShareLimitRuleError as exc:
                        unresolved.append((code, day, exc.code))
                    else:
                        derived_row = derived.as_source_row()
                        database_row = database_by_date.get(day)
                        if database_row is not None:
                            _require_partial_values_match(database_row, derived_row)
                            completion_count += 1
                        overlay_rows.append(derived_row)
                        if len(overlay_rows) > max_overlay_rows:
                            raise StkLimitRuleOverlayError("stk_limit rule overlay exceeds row hard limit")
            daily_row = daily_by_date.get(day)
            if daily_row is not None:
                if latest_adj is None or latest_adj_date is None:
                    raise StkLimitRuleOverlayError("daily close has no current adjustment factor")
                point = LimitReferencePoint(
                    close=_positive_decimal(daily_row.get("close_li"), field="close_li") / PRICE_UNIT_DIVISOR,
                    close_adj_factor=latest_adj,
                    close_date=day,
                    latest_adj_factor=latest_adj,
                    latest_adj_date=latest_adj_date,
                )
            elif point is not None and latest_adj is not None and latest_adj_date is not None:
                point = LimitReferencePoint(
                    close=point.close,
                    close_adj_factor=point.close_adj_factor,
                    close_date=point.close_date,
                    latest_adj_factor=latest_adj,
                    latest_adj_date=latest_adj_date,
                )
        if point is not None:
            state[code] = point
    if limit_current is not None or daily_current is not None or adj_current is not None:
        raise StkLimitRuleOverlayError("source contains a non-PIT instrument")
    if unresolved:
        sample = [
            {"ts_code": code, "trade_date": day.isoformat(), "reason": reason}
            for code, day, reason in unresolved[:20]
        ]
        raise StkLimitRuleOverlayError(
            "stk_limit rule overlay has unresolved PIT keys",
            context={"unresolved_count": len(unresolved), "sample": sample},
        )
    overlay_rows.sort(key=lambda row: (str(row["ts_code"]), str(row["trade_date"])))
    return StkLimitRuleOverlayResult(
        overlay_rows=tuple(overlay_rows),
        reference_state=state,
        expected_pit_keys=expected_count,
        database_rows=database_count,
        rule_derived_rows=len(overlay_rows),
        database_completion_rows=completion_count,
        database_override_rows=0,
        reference_seed_rows=reference_seed_count,
        unresolved_keys=0,
        peak_code_partition_rows=peak_code_rows,
    )


def _code_groups(
    rows: Iterable[Mapping[str, Any]],
    *,
    source: str,
    date_field: str,
) -> Iterator[tuple[str, tuple[Mapping[str, Any], ...]]]:
    previous: tuple[str, date] | None = None

    def checked() -> Iterator[Mapping[str, Any]]:
        nonlocal previous
        for row in rows:
            key = _stock_day_key(row, source=source, date_field=date_field)
            if previous is not None and key <= previous:
                raise StkLimitRuleOverlayError(f"{source} rows must be globally ordered and unique")
            previous = key
            yield row

    for code, group in groupby(checked(), key=lambda row: str(row.get("ts_code", "")).strip().upper()):
        yield code, tuple(group)


def _stock_day_key(
    row: Mapping[str, Any],
    *,
    source: str,
    date_field: str = "trade_date",
) -> tuple[str, date]:
    code = str(row.get("ts_code", "")).strip().upper()
    if not code:
        raise StkLimitRuleOverlayError(f"{source} stock code is empty")
    return code, _row_date(row, date_field, source)


def _row_date(row: Mapping[str, Any], field: str, source: str) -> date:
    value = row.get(field)
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise StkLimitRuleOverlayError(f"{source} date is invalid") from exc


def _positive_decimal(value: Any, *, field: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise StkLimitRuleOverlayError(f"{field} is invalid") from exc
    if not number.is_finite() or number <= 0:
        raise StkLimitRuleOverlayError(f"{field} must be positive and finite")
    return number


def _optional_price(value: Any, *, field: str) -> Decimal | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        number = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise StkLimitRuleOverlayError(f"{field} is invalid") from exc
    if not number.is_finite() or number < 0:
        raise StkLimitRuleOverlayError(f"{field} must be non-negative and finite")
    if number == 0:
        return None
    quantized = number.quantize(Decimal("0.01"))
    if number != quantized:
        raise StkLimitRuleOverlayError(f"{field} is not aligned to the price tick")
    return quantized


def _validate_reference_point(
    point: LimitReferencePoint,
    *,
    code: str,
    required_day: date,
) -> None:
    if not isinstance(point, LimitReferencePoint):
        raise StkLimitRuleOverlayError("stk_limit reference resolver returned an invalid point")
    for field, value in (
        ("reference close", point.close),
        ("reference close adj_factor", point.close_adj_factor),
        ("reference latest adj_factor", point.latest_adj_factor),
    ):
        _positive_decimal(value, field=field)
    if (
        point.close_date >= required_day
        or point.latest_adj_date < point.close_date
        or point.latest_adj_date > required_day
    ):
        raise StkLimitRuleOverlayError(
            "stk_limit reference resolver returned a future point",
            context={"ts_code": code, "required_date": required_day.isoformat()},
        )


def _complete_limit_row(row: Mapping[str, Any]) -> bool:
    return all(
        _optional_price(row.get(field), field=field) is not None
        for field in ("pre_close", "up_limit", "down_limit")
    )


def _require_partial_values_match(
    database_row: Mapping[str, Any],
    derived_row: Mapping[str, Any],
) -> None:
    for field in ("pre_close", "up_limit", "down_limit"):
        observed = _optional_price(database_row.get(field), field=field)
        if observed is None:
            continue
        expected = _positive_decimal(derived_row[field], field=field).quantize(Decimal("0.01"))
        if observed != expected:
            raise StkLimitRuleOverlayError(
                "incomplete stk_limit database value conflicts with rule derivation",
                context={
                    "ts_code": str(derived_row["ts_code"]),
                    "trade_date": str(derived_row["trade_date"]),
                    "field": field,
                    "database_value": format(observed, "f"),
                    "derived_value": format(expected, "f"),
                },
            )


__all__ = [
    "MAX_STK_LIMIT_RULE_OVERLAY_ROWS",
    "STK_LIMIT_RULE_OVERLAY_SCHEMA",
    "LimitReferencePoint",
    "StkLimitRuleOverlayError",
    "StkLimitRuleOverlayResult",
    "build_stk_limit_rule_overlay",
]
