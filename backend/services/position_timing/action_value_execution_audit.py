"""Bounded minute verification of the daily action-value fill proxy.

This is an offline diagnostic only. It replays already-frozen daily plans over
an immutable minute candidate; it neither creates a minute direction nor adds
an intraday model.
"""

from __future__ import annotations

from datetime import date
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd

from .action_value import ActionPlan, ActionValueError, daily_fill
from .action_value_data import file_reference
from .contracts import canonical_sha256


AUDIT_FIELDS = (
    "open",
    "high",
    "low",
    "close",
    "factor",
    "up_limit_price",
    "down_limit_price",
)
DEFAULT_AUDIT_LIMIT = 256


def audit_minute_execution(
    sleeve_days: pd.DataFrame,
    *,
    minute_root: Path,
    sample_limit: int = DEFAULT_AUDIT_LIMIT,
) -> dict[str, Any]:
    """Check trigger order/fill/price on a fixed source-independent sample."""

    required = {
        "sleeve_id",
        "symbol",
        "target_trade_date",
        "baseline",
        "planned_delta_qty",
        "plan_reference_raw",
        "plan_risk_exit",
        "pre_quantity",
        "pre_sellable_qty",
        "fill_status",
        "fill_price_raw",
    }
    if not required.issubset(sleeve_days) or sample_limit <= 0:
        raise ActionValueError("EXECUTION_AUDIT_INPUT_INVALID")
    minute_root = minute_root.resolve()
    meta_path = minute_root / "meta_export.json"
    calendar_path = minute_root / "calendars" / "1min.txt"
    instruments_path = minute_root / "instruments" / "all.txt"
    base_refs = {
        "minute_meta": file_reference(meta_path),
        "minute_calendar": file_reference(calendar_path),
        "minute_instruments": file_reference(instruments_path),
    }
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        coverage_start = date.fromisoformat(str(meta["start"]))
        coverage_end = date.fromisoformat(str(meta["end"]))
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise ActionValueError("EXECUTION_AUDIT_MINUTE_META_INVALID") from exc
    advertised = set(meta.get("required_minute_fields") or ())
    if not set(AUDIT_FIELDS).issubset(advertised):
        raise ActionValueError("EXECUTION_AUDIT_MINUTE_FIELDS_MISSING")
    calendar = pd.DatetimeIndex(pd.to_datetime(calendar_path.read_text(encoding="utf-8").splitlines()))
    if not len(calendar) or not calendar.is_unique or not calendar.is_monotonic_increasing:
        raise ActionValueError("EXECUTION_AUDIT_MINUTE_CALENDAR_INVALID")
    mutable_indexes: dict[date, list[int]] = {}
    for index, timestamp in enumerate(calendar):
        mutable_indexes.setdefault(timestamp.date(), []).append(index)
    indexes_by_date = {
        day: np.asarray(indexes, dtype=int) for day, indexes in mutable_indexes.items()
    }
    spans = _instrument_spans(instruments_path)
    source_population = (
        sleeve_days.loc[
            sleeve_days["baseline"].eq("BUY_AND_HOLD")
            & sleeve_days["planned_delta_qty"].ne(0)
        ]
        .drop_duplicates(["sleeve_id", "target_trade_date"])
        .copy()
    )
    source_population["target_trade_date"] = pd.to_datetime(
        source_population["target_trade_date"]
    ).dt.date
    inside_date_range = source_population["target_trade_date"].map(
        lambda target: coverage_start <= target <= coverage_end
    )
    inside_instrument_span = pd.Series(
        (
            any(
                start <= row.target_trade_date <= end
                for start, end in spans.get(str(row.symbol), ())
            )
            for row in source_population.itertuples()
        ),
        index=source_population.index,
        dtype=bool,
    )
    population = source_population.loc[inside_date_range & inside_instrument_span].copy()
    population["sample_key"] = population.apply(_sample_key, axis=1)
    population = population.sort_values("sample_key").head(sample_limit)

    source_refs: dict[str, Any] = dict(base_refs)
    feature_cache: dict[tuple[str, str], tuple[Path, dict[str, Any]]] = {}
    results: list[dict[str, Any]] = []
    for row in population.to_dict(orient="records"):
        symbol = str(row["symbol"])
        target = row["target_trade_date"]
        base = {
            "sleeve_id": row["sleeve_id"],
            "canonical_symbol": symbol,
            "target_trade_date": target.isoformat(),
            "planned_delta_qty": int(row["planned_delta_qty"]),
            "daily_proxy_status": row["fill_status"],
            "daily_proxy_price_raw": _optional_finite_float(row["fill_price_raw"]),
        }
        indexes = indexes_by_date.get(target)
        if indexes is None or not len(indexes):
            results.append({**base, "status": "DATA_ERROR_MINUTE_DAY_MISSING"})
            continue
        arrays: dict[str, np.ndarray] = {}
        failed_field = None
        for field in AUDIT_FIELDS:
            cache_key = (symbol, field)
            if cache_key not in feature_cache:
                path = minute_root / "features" / symbol.lower() / f"{field}.1min.bin"
                try:
                    reference = file_reference(path)
                except ActionValueError:
                    failed_field = field
                    break
                feature_cache[cache_key] = (path, reference)
                source_refs[f"minute_feature:{symbol}:{field}"] = reference
            path, _ = feature_cache[cache_key]
            try:
                from .minute_execution_pipeline import _read_bin_values

                arrays[field] = _read_bin_values(path, indexes)
            except (OSError, ValueError):
                failed_field = field
                break
        if failed_field is not None:
            results.append({**base, "status": "DATA_ERROR_REQUIRED_FIELD", "field": failed_field})
            continue
        result = _evaluate_plan(row, arrays)
        results.append({**base, **result})

    for _, (path, reference) in feature_cache.items():
        if file_reference(path) != reference:
            raise ActionValueError("EXECUTION_AUDIT_SOURCE_CHANGED_WHILE_READING")
    if any(file_reference(path) != base_refs[role] for role, path in (
        ("minute_meta", meta_path),
        ("minute_calendar", calendar_path),
        ("minute_instruments", instruments_path),
    )):
        raise ActionValueError("EXECUTION_AUDIT_SOURCE_CHANGED_WHILE_READING")
    counts = pd.Series([item["status"] for item in results], dtype="object").value_counts().to_dict()
    paired = [item for item in results if item["status"] == "PAIRED"]
    agreement = [item for item in paired if item["fill_status_agrees"]]
    price_differences = [item["minute_minus_daily_price_bps"] for item in paired if item["minute_minus_daily_price_bps"] is not None]
    receipt = {
        "schema_version": "position_timing_action_value_execution_audit_v3",
        "audit_role": "DIAGNOSTIC_ONLY_NOT_MINUTE_SIGNAL",
        "sample_selection": "SHA256_PLAN_IDENTITY_WITHIN_ADVERTISED_MINUTE_COVERAGE",
        "sample_limit": sample_limit,
        "source_population_count": int(len(source_population)),
        "eligible_population_count": int((inside_date_range & inside_instrument_span).sum()),
        "excluded_before_sampling_counts": {
            "MINUTE_COVERAGE_OUTSIDE_RANGE": int((~inside_date_range).sum()),
            "MINUTE_PIT_UNIVERSE_EXCLUDED": int((inside_date_range & ~inside_instrument_span).sum()),
        },
        "population_count": int(len(population)),
        "result_counts": {str(key): int(value) for key, value in sorted(counts.items())},
        "paired_count": len(paired),
        "fill_status_agreement_count": len(agreement),
        "fill_status_agreement_ratio": len(agreement) / len(paired) if paired else None,
        "mean_minute_minus_daily_price_bps": float(np.mean(price_differences)) if price_differences else None,
        "execution_realism_status": (
            "PARTIAL_MINUTE_VERIFICATION_AVAILABLE"
            if paired and not any(item["status"].startswith("DATA_ERROR") for item in results)
            else "EXECUTION_REALISM_UNVERIFIED"
        ),
        "minute_coverage_start": coverage_start.isoformat(),
        "minute_coverage_end": coverage_end.isoformat(),
        "source_refs": source_refs,
        "results": results,
    }
    receipt["execution_audit_sha256"] = canonical_sha256(receipt)
    return receipt


def _evaluate_plan(row: Mapping[str, Any], arrays: Mapping[str, np.ndarray]) -> dict[str, Any]:
    raw = {name: np.asarray(arrays[name], dtype=float) for name in AUDIT_FIELDS}
    lengths = {len(values) for values in raw.values()}
    if len(lengths) != 1 or not lengths or next(iter(lengths)) == 0:
        return {"status": "DATA_ERROR_MINUTE_BAR_INVALID", "reason_code": "ARRAY_SHAPE_INVALID"}
    # The exported global minute calendar may contain a structural timestamp
    # (for example 13:00) at which every field is absent for this instrument.
    # Drop only all-field-empty padding; partial required-field absence remains
    # a source error rather than being silently forward-filled.
    observed = np.logical_or.reduce([np.isfinite(values) for values in raw.values()])
    if not observed.any():
        return {"status": "DATA_ERROR_MINUTE_DAY_EMPTY"}
    raw = {name: values[observed] for name, values in raw.items()}
    factor = raw["factor"]
    if not np.isfinite(factor).all() or (factor <= 0).any():
        return {"status": "DATA_ERROR_FACTOR_INVALID"}
    converted = {
        field: raw[field] / factor
        for field in ("open", "high", "low", "close")
    }
    converted["up_limit"] = raw["up_limit_price"]
    converted["down_limit"] = raw["down_limit_price"]
    if any(not np.isfinite(values).all() for values in converted.values()):
        return {"status": "DATA_ERROR_REQUIRED_FIELD_NONFINITE"}
    plan = ActionPlan(
        str(row["symbol"]),
        int(row["planned_delta_qty"]),
        _decimal(row["plan_reference_raw"]),
        bool(row["plan_risk_exit"]),
    )
    minute_fill = None
    for index in range(len(factor)):
        bar = {name: _decimal(values[index]) for name, values in converted.items()}
        bar["is_suspended"] = False
        fill = daily_fill(
            plan,
            bar,
            sellable=int(row["pre_sellable_qty"]),
            full_exit=(-plan.delta == int(row["pre_quantity"])),
        )
        if fill.status == "UNKNOWN":
            return {"status": "DATA_ERROR_MINUTE_BAR_INVALID", "reason_code": fill.reason}
        if fill.status == "FILLED":
            minute_fill = fill
            break
    minute_status = "FILLED" if minute_fill is not None else "NO_FILL"
    daily_status = str(row["fill_status"])
    daily_price = _optional_finite_float(row.get("fill_price_raw"))
    minute_price = float(minute_fill.price) if minute_fill is not None else None
    price_difference = None
    if minute_price is not None and daily_price is not None and daily_price > 0:
        price_difference = (minute_price / daily_price - 1) * 10000
    return {
        "status": "PAIRED",
        "minute_replay_status": minute_status,
        "minute_replay_price_raw": minute_price,
        "fill_status_agrees": minute_status == daily_status,
        "minute_minus_daily_price_bps": price_difference,
    }


def _sample_key(row: pd.Series) -> str:
    payload = ":".join(
        str(row[name])
        for name in ("sleeve_id", "symbol", "target_trade_date", "planned_delta_qty", "plan_reference_raw")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _instrument_spans(path: Path) -> dict[str, tuple[tuple[date, date], ...]]:
    result: dict[str, list[tuple[date, date]]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split("\t")
        if len(parts) != 3:
            raise ActionValueError("EXECUTION_AUDIT_MINUTE_INSTRUMENTS_INVALID")
        try:
            result.setdefault(parts[0].upper(), []).append(
                (date.fromisoformat(parts[1][:10]), date.fromisoformat(parts[2][:10]))
            )
        except ValueError as exc:
            raise ActionValueError("EXECUTION_AUDIT_MINUTE_INSTRUMENTS_INVALID") from exc
    return {symbol: tuple(spans) for symbol, spans in result.items()}


def _decimal(value: Any):
    from decimal import Decimal

    parsed = Decimal(str(value))
    if not parsed.is_finite():
        raise ActionValueError("EXECUTION_AUDIT_NONFINITE_VALUE")
    return parsed


def _optional_finite_float(value: Any) -> float | None:
    if value is None or pd.isna(value):
        return None
    parsed = float(value)
    return parsed if np.isfinite(parsed) else None


__all__ = ["audit_minute_execution"]
