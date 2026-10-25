"""H-TIMING-1 D-only descriptors; no labels, I/O, fitting or qualification."""
from datetime import date
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import (
    QUOTE_FIELDS, RAW_FIELDS, _frame, _number, _records,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

TIMING_FEATURES = ("overnight_intraday_contrast_19", "overnight_volatility_19")
ROSTER_FIELDS = (*KEY, "selection_effective_rank", "candidate_group_size")
SEMANTICS = {
    "schema_version": "economic_entry_timing_features_v1", "features": TIMING_FEATURES,
    "sessions": 20, "pairs": 19, "unit": "log_return", "volatility_ddof": 1,
    "price_basis": "RAW_LI_TO_CNY_ADJ_OVER_D_ANCHOR",
    "formula": "ON=log(Oj/Cprev)+log(adj_j/adj_prev);ID=log(Cj/Oj);mean(ON-ID),std(ON,ddof=1)",
    "bar_policy": "positive_raw_volume_non_synthetic_full_or_endpoint_ambiguous_suspend_unknown",
    "qualification": "COMPUTATION_ONLY",
}


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ECONOMIC_TIMING_INPUT_INVALID")


def _bar_reason(row, events):
    if pd.isna(row.volume_hand) or row.volume_hand <= 0:
        return "REAL_VOLUME_UNKNOWN_OR_ZERO"
    if pd.isna(row.synthetic_bar) or row.synthetic_bar:
        return "BAR_NOT_PROVEN_REAL"
    stopping = events.loc[events.suspend_type.eq("S")]
    if stopping.empty:
        return None  # A real R-only day is not a full-session suspension.
    if events.suspend_type.eq("R").any():
        return "S_R_ENDPOINT_IDENTITY_UNKNOWN"
    timing = stopping.suspend_timing.iloc[0]
    if pd.isna(timing) or not timing.strip() or timing.strip() == "09:30-09:30":
        return "FULL_SESSION_SUSPENSION"
    match = re.fullmatch(r"(\d{2}:\d{2})-(\d{2}:\d{2})", timing.strip())
    if match is None:
        return "SUSPENSION_ENDPOINT_IDENTITY_UNKNOWN"
    start, end = match.groups()
    if (any(int(value[3:]) >= 60 for value in (start, end))
            or not "09:30" < start < end < "15:00"):
        return "SUSPENSION_ENDPOINT_IDENTITY_UNKNOWN"
    if pd.isna(row.open_li) or pd.isna(row.close_li):
        return "PARTIAL_SESSION_ENDPOINT_UNKNOWN"
    return None


def build_economic_entry_timing_features_v1(*, candidates, raw_daily, suspend_rows, calendar):
    """Retain every exact original candidate and independently propagate unknowns.

    Calendar contains twenty real sessions through D and next T (clock only).
    H-TIMING-1 approved design §5 defines the nineteen fixed pairs. Algebra in
    log coordinates avoids overflow, but still requires the explicit D anchor.
    """
    if (not isinstance(calendar, (tuple, list)) or len(calendar) != 21
            or any(type(day) is not date for day in calendar) or list(calendar) != sorted(set(calendar))):
        _fail("timing requires twenty D sessions and next T")
    if (not isinstance(candidates, pd.DataFrame) or len(candidates) > 20
            or not candidates.columns.is_unique or set(candidates.columns) != set(ROSTER_FIELDS)):
        _fail("timing needs the exact original roster projection")
    roster = candidates.loc[:, ROSTER_FIELDS].copy()
    for name, day in zip(KEY[:2], calendar[-2:], strict=True):
        values = pd.to_datetime(roster[name], errors="coerce")
        if values.dt.tz is not None or not values.eq(pd.Timestamp(day)).all():
            _fail("timing original candidate D/T differs")
        roster[name] = values
    def integer(value):
        return isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_))
    if (not roster.selection_effective_rank.map(integer).all()
            or roster.selection_effective_rank.tolist() != list(range(1, len(roster) + 1))
            or not roster.candidate_group_size.map(lambda value: integer(value) and value == len(roster)).all()
            or roster.instrument.duplicated().any()
            or not roster.instrument.map(lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is not None).all()):
        _fail("timing must preserve original unique candidates and ranks")
    sessions, symbols = pd.DatetimeIndex(calendar[:-1]), roster.instrument.tolist()
    raw = _frame(raw_daily, columns=("trade_date", "instrument", *RAW_FIELDS),
        optional=(*QUOTE_FIELDS, "synthetic_bar"), maximum=400, dates=sessions, symbols=symbols)
    if "synthetic_bar" not in raw_daily:
        raw["synthetic_bar"] = False
    if not raw.synthetic_bar.map(lambda value: isinstance(value, (bool, np.bool_)) or pd.isna(value)).all():
        _fail("timing synthetic marker must be boolean or unknown")
    for name in RAW_FIELDS + QUOTE_FIELDS:
        raw[name] = raw[name].map(lambda value, name=name: _number(value,
            positive=name not in ("volume_hand", "amount_li"), nonnegative=name in ("volume_hand", "amount_li")))
    if (raw.high_li.lt(raw.low_li).any() or raw.open_li.gt(raw.high_li).any()
            or raw.open_li.lt(raw.low_li).any() or raw.close_li.gt(raw.high_li).any()
            or raw.close_li.lt(raw.low_li).any()):
        _fail("timing OHLC range contradicts original values")
    suspends = _frame(suspend_rows, columns=("trade_date", "instrument", "suspend_type"),
        optional=("suspend_timing",), maximum=800, dates=sessions, symbols=symbols,
        keys=("trade_date", "instrument", "suspend_type"))
    if (not suspends.suspend_type.isin(("S", "R")).all()
            or not suspends.suspend_timing.map(lambda value: isinstance(value, str) or pd.isna(value)).all()):
        _fail("timing suspension schema differs")
    output = roster.loc[:, KEY].copy()
    for name in TIMING_FEATURES:
        output[name] = np.nan
    unknown = []
    for index, symbol in zip(output.index, symbols, strict=True):
        bars = raw.loc[raw.instrument.eq(symbol)].set_index("trade_date").reindex(sessions)
        reasons = {}
        missing = bars.instrument.isna().any()
        real_reason = "CANDIDATE_SESSION_MISSING" if missing else next((reason for day, row in bars.iterrows()
            if (reason := _bar_reason(row, suspends.loc[suspends.trade_date.eq(day) & suspends.instrument.eq(symbol)]))), None)
        if real_reason:
            reasons = dict.fromkeys(TIMING_FEATURES, real_reason)
        elif bars.adj_factor.isna().any():
            reasons = dict.fromkeys(TIMING_FEATURES, "D_ANCHOR_OR_SESSION_FACTOR_UNKNOWN")
        elif bars.open_li.iloc[1:].isna().any() or bars.close_li.iloc[:-1].isna().any():
            reasons = dict.fromkeys(TIMING_FEATURES, "PAIR_OPEN_OR_PREVIOUS_CLOSE_UNKNOWN")
        else:
            on = (np.log(bars.open_li.iloc[1:].to_numpy()) - np.log(bars.close_li.iloc[:-1].to_numpy())
                + np.diff(np.log(bars.adj_factor.to_numpy())))
            output.loc[index, TIMING_FEATURES[1]] = float(np.std(on, ddof=1))
            if pd.isna(bars.close_li.iloc[-1]):
                reasons[TIMING_FEATURES[0]] = "PAIR_CURRENT_CLOSE_UNKNOWN"
            else:
                intraday = np.log(bars.close_li.iloc[1:].to_numpy()) - np.log(bars.open_li.iloc[1:].to_numpy())
                output.loc[index, TIMING_FEATURES[0]] = float(np.mean(on - intraday))
        unknown.append({"instrument": symbol, "fields": reasons})
    if np.isinf(output.loc[:, TIMING_FEATURES].to_numpy(dtype=float)).any():
        _fail("timing derived value is nonfinite")
    receipt = {
        "schema_version": SEMANTICS["schema_version"], "semantics_sha256": sha(SEMANTICS),
        "input_sha256": sha({"roster": _records(roster), "raw": _records(raw), "suspends": _records(suspends),
            "calendar": [day.isoformat() for day in calendar], "raw_present_fields": sorted(raw_daily.columns),
            "suspend_present_fields": sorted(suspend_rows.columns)}),
        "feature_sha256": sha(_records(output)), "unknown_fields": unknown,
        "decision_date": calendar[-2].isoformat(), "target_date": calendar[-1].isoformat(),
        "candidate_count": len(output), "status": "NO_CANDIDATES" if output.empty else "COMPUTED",
        "source_evidence": "COMPUTATION_ONLY", "deployable": False, "outcomes_read": False,
        "new_native_receipt": False,
    }
    return output, receipt
