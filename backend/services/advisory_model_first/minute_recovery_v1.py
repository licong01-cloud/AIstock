"""Frozen D-close recovery measurements; no labels, fitting or ranking."""
from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.multiperiod_top10_5td_inputs_v1 import INPUT_FEATURES
from backend.services.advisory_model_first.serial_price_pressure_top10_5td_inputs_v1 import OLD_MINUTE
from backend.services.advisory_model_first.volume_conditioned_pressure_top10_5td_v1 import (
    NEW as CONDITIONAL,
    minute_features as previous_features,
)

OLD = (*OLD_MINUTE, "serial_correlation", "serial_closing_pressure", "serial_coverage", "nonzero_return_fraction")
CONTROLS = ("event_drawdown_bps", "trough_clock_fraction")
NEW = ("recovery_delay_fraction", "recovery_hold_fraction", "unrecovered_fraction",
       "is_no_drawdown", "is_unrecovered")
MATCHED = (*INPUT_FEATURES, *OLD, *CONDITIONAL, *CONTROLS)
FEATURES = (*MATCHED, *NEW)
VERSION = "minute_recovery_v1_core240_first_relative_mdd"


def core_clock(day) -> pd.DatetimeIndex:
    d = pd.Timestamp(day)
    if pd.isna(d) or d.tz is not None or d != d.normalize():
        raise ValueError("recovery decision day must be a naive date")
    return pd.date_range(d + pd.Timedelta(hours=9, minutes=31), periods=120, freq="min").append(
        pd.date_range(d + pd.Timedelta(hours=13, minutes=1), periods=120, freq="min"))


def recovery_features(slots, close, *, day) -> dict:
    """Earliest maximum relative drawdown, measured on 240 complete core closes.

    An unrecovered delay is a censored waiting lower bound, not a recovery time.
    Missing core observations retain UNKNOWN; optional auction anchors never fill them.
    """
    expected = core_clock(day)
    slots, values = pd.DatetimeIndex(slots), np.asarray(close, dtype=float)
    anchors = pd.DatetimeIndex([expected[0] - pd.Timedelta(minutes=1),
                                expected[120] - pd.Timedelta(minutes=1)])
    if (slots.tz is not None or slots.hasnans or slots.has_duplicates
            or not slots.is_monotonic_increasing or len(slots) > 300
            or not slots.isin(expected.union(anchors)).all() or values.shape != (len(slots),)):
        raise ValueError("recovery D calendar/clock/shape differs")
    if np.isinf(values).any() or (values[np.isfinite(values)] <= 0).any():
        raise ValueError("invalid known recovery close")
    positions = slots.get_indexer(expected)
    present = positions >= 0
    path = np.full(240, np.nan)
    path[present] = values[positions[present]]
    known = np.isfinite(path)
    result = dict(recovery_status="UNKNOWN_INCOMPLETE_CLOSE_PATH",
                  recovery_core_slots=int(present.sum()), recovery_core_known=int(known.sum()),
                  **{name: np.nan for name in (*CONTROLS, *NEW)})
    if not known.all():
        return result
    peaks = np.maximum.accumulate(path)
    draws = 1 - path / peaks
    q = int(np.argmax(draws))
    if draws[q] == 0:
        result.update(recovery_status="NO_DRAWDOWN", event_drawdown_bps=0., trough_clock_fraction=0.,
                      recovery_delay_fraction=0., recovery_hold_fraction=1., unrecovered_fraction=0.,
                      is_no_drawdown=1., is_unrecovered=0.)
        return result
    peak, trough = peaks[q], path[q]
    after = path[q+1:] >= peak
    recovered = bool(after.any())
    delay = int(np.flatnonzero(after)[0]) + 1 if recovered else 239 - q
    result.update(recovery_status="RECOVERED" if recovered else "UNRECOVERED",
                  event_drawdown_bps=float(draws[q] * 10000), trough_clock_fraction=q / 239,
                  recovery_delay_fraction=delay / 239,
                  recovery_hold_fraction=float(after.mean()) if len(after) else 0.,
                  unrecovered_fraction=float(max(0., (peak - path[-1]) / (peak - trough))),
                  is_no_drawdown=0., is_unrecovered=float(not recovered))
    if not all(np.isfinite(result[name]) for name in (*CONTROLS, *NEW)):
        raise ValueError("nonfinite recovery arithmetic")
    return result


def minute_features(slots, arrays, *, day) -> dict:
    recovery = recovery_features(slots, arrays["close"], day=day)
    prior = previous_features(slots, arrays, day=day)
    result = {name: prior[name] for name in (*OLD, *CONDITIONAL)}
    if np.isinf(list(result.values())).any():
        raise ValueError("nonfinite old minute control")
    return {**result, **recovery}
