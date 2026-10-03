"""Bounded D-only feature computation, never a source or model qualification."""

from datetime import date
from numbers import Real
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


FEATURES = ("ret_10", "relative_ret_5_vs_csi300", "close_location_in_day", "volume_ratio_5_to_20")
SEMANTICS = {
    "schema_version": "economic_daily_information_v1", "window_sessions": 20,
    "price_basis": "D_ADJUSTED", "volume_basis": "RAW_SAME_UNIT",
    "formulas": ["C_D/C_D_minus_10-1", "C_D/C_D_minus_5-1-benchmark_ret_5",
                 "(C_D-L_D)/(H_D-L_D)", "mean_V_last_5/mean_V_last_20"],
    "units": ["fraction", "fraction", "dimensionless", "dimensionless"],
    "missing_policy": "per_feature_unknown_no_fill_no_drop",
}


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ECONOMIC_INFORMATION_INVALID")


def _number(value):
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, Real):
        _fail("economic information input contains a non-numeric value")
    value = float(value)
    if np.isnan(value):
        return None
    if not np.isfinite(value):
        _fail("economic information input contains a nonfinite value")
    return value


def build_economic_daily_information_v1(*, decision_date, candidates, calendar, panel,
                                        benchmark_return_5d, benchmark_as_of,
                                        price_basis, volume_basis):
    """Read caller-owned normalized inputs; no I/O, clocks, fills or fitting."""
    if (type(decision_date) is not date or type(benchmark_as_of) is not date or benchmark_as_of != decision_date
            or price_basis != "D_ADJUSTED" or volume_basis != "RAW_SAME_UNIT"
            or not isinstance(calendar, (list, tuple)) or len(calendar) != 20
            or any(type(value) is not date for value in calendar)
            or list(calendar) != sorted(set(calendar)) or calendar[-1] != decision_date):
        _fail("economic information needs exact D bases and 20 unique chronological sessions")
    if not isinstance(candidates, (list, tuple)) or len(candidates) > 20:
        _fail("economic information candidate budget exceeds Top20")
    roster = []
    for row in candidates:
        if (not isinstance(row, dict) or set(row) != {"instrument", "selection_rank"}
                or not isinstance(row["instrument"], str) or re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", row["instrument"]) is None
                or type(row["selection_rank"]) is not int or not 1 <= row["selection_rank"] <= 20):
            _fail("economic information original candidate identity is malformed")
        roster.append(dict(row))
    symbols = [row["instrument"] for row in roster]
    ranks = [row["selection_rank"] for row in roster]
    if len(set(symbols)) != len(symbols) or len(set(ranks)) != len(ranks) or ranks != sorted(ranks):
        _fail("economic information candidate uniqueness or original order differs")
    if (not isinstance(panel, pd.DataFrame) or len(panel) > 400 or not isinstance(panel.index, pd.MultiIndex)
            or panel.index.names != ["datetime", "instrument"] or panel.index.has_duplicates or not panel.columns.is_unique):
        _fail("economic information normalized panel exceeds its unique-key budget")
    try:
        dates = pd.DatetimeIndex(panel.index.get_level_values("datetime"))
    except (TypeError, ValueError):
        _fail("economic information panel date is malformed")
    if (dates.tz is not None or dates.hasnans or not dates.equals(dates.normalize())
            or not dates.isin(pd.DatetimeIndex(calendar)).all()
            or set(panel.index.get_level_values("instrument")) - set(symbols)):
        _fail("economic information panel contains foreign candidates or time")
    normalized = panel.copy(deep=False)
    normalized.index = pd.MultiIndex.from_arrays([dates, panel.index.get_level_values("instrument")], names=panel.index.names)
    if normalized.index.has_duplicates:
        _fail("economic information normalized panel contains aliased duplicate keys")
    benchmark = _number(benchmark_return_5d)
    if benchmark is not None and benchmark <= -1:
        _fail("economic information benchmark return contradicts positive prices")
    fields = ("close", "high", "low", "volume")
    source_rows, lookup = [], {}
    for (day, instrument), row in normalized.sort_index().iterrows():
        day = pd.Timestamp(day).date()
        values = {field: _number(row[field]) if field in panel.columns else None for field in fields}
        if (any(values[field] is not None and values[field] <= 0 for field in fields[:-1])
                or values["volume"] is not None and values["volume"] < 0
                or (values["high"] is not None and values["low"] is not None and values["high"] < values["low"])
                or (values["close"] is not None and values["high"] is not None and values["close"] > values["high"])
                or (values["close"] is not None and values["low"] is not None and values["close"] < values["low"])):
            _fail("economic information normalized OHLC or volume is contradictory")
        lookup[(day, instrument)] = values
        source_rows.append({"date": day.isoformat(), "instrument": instrument, **values})
    rows = []
    for candidate in roster:
        symbol = candidate["instrument"]
        history = [lookup.get((day, symbol), {}) for day in calendar]
        values, reasons = dict.fromkeys(FEATURES), {}
        for feature, periods in ((FEATURES[0], 10), (FEATURES[1], 5)):
            closes = [row.get("close") for row in history[-(periods + 1):]]
            if any(value is None for value in closes):
                reasons[feature] = "CLOSE_WINDOW_INCOMPLETE"
            elif feature == FEATURES[1] and benchmark is None:
                reasons[feature] = "BENCHMARK_UNAVAILABLE"
            else:
                values[feature] = closes[-1] / closes[0] - 1 - (benchmark if feature == FEATURES[1] else 0)
        close, high, low = (history[-1].get(field) for field in fields[:3])
        if any(value is None for value in (close, high, low)):
            reasons[FEATURES[2]] = "D_OHLC_INCOMPLETE"
        elif high == low:
            reasons[FEATURES[2]] = "D_PRICE_RANGE_ZERO"
        else:
            values[FEATURES[2]] = (close - low) / (high - low)
        volumes = [row.get("volume") for row in history]
        if any(value is None for value in volumes):
            reasons[FEATURES[3]] = "VOLUME_WINDOW_INCOMPLETE"
        elif not any(volumes):
            reasons[FEATURES[3]] = "VOLUME_MEAN_ZERO"
        else:
            # Scale before summation to avoid overflow for valid finite inputs.
            scaled = np.asarray(volumes) / max(volumes)
            values[FEATURES[3]] = float(scaled[-5:].mean() / scaled.mean())
        if any(value is not None and not np.isfinite(value) for value in values.values()):
            _fail("economic information derived value is nonfinite")
        rows.append({**candidate, "values": values, "unknown_reasons": reasons})
    payload = {
        "schema_version": SEMANTICS["schema_version"], "decision_date": decision_date.isoformat(),
        "feature_names": list(FEATURES), "feature_semantics_sha256": sha(SEMANTICS),
        "candidate_roster_sha256": sha(roster), "calendar_sha256": sha([value.isoformat() for value in calendar]),
        "source_sha256": sha({"panel": source_rows, "present_fields": [field for field in fields if field in panel.columns],
                             "benchmark_return_5d": benchmark,
                             "benchmark_as_of": benchmark_as_of.isoformat(), "price_basis": price_basis, "volume_basis": volume_basis}),
        "source_evidence": "COMPUTATION_ONLY", "decision_use": "NAVIGATION_ONLY", "deployable": False,
        "outcomes_read": False, "rows": rows,
    }
    return {**payload, "information_sha256": sha(payload)}
