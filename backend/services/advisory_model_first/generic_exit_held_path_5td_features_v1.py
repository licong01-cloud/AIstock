"""Original held-episode paths visible by S. No fit, future factors or new Selection."""
import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_exit_held_path_5td_contracts_v1 import (
    FEATURE_KEY, FEATURE_SHA256, NEW_FEATURES, PRICE_FIELDS,
)
from backend.services.advisory_model_first.generic_remaining_value_exit_5td_contracts_v1 import POLICY, ordered_calendar


def build_exit_held_path_features_v1(*, rows, prices, calendar, development_cutoff):
    days = ordered_calendar(calendar)
    positions = {day: i for i, day in enumerate(days)}
    cutoff = _day(development_cutoff)
    required = {*FEATURE_KEY, "entry_date", "instrument", "remaining_sessions"}
    if (not isinstance(rows, pd.DataFrame) or not required.issubset(rows.columns)
            or not rows.columns.is_unique or len(rows) > 15000 or rows.duplicated(list(FEATURE_KEY)).any()
            or not isinstance(prices, pd.DataFrame) or set(prices.columns) != set(PRICE_FIELDS)
            or not prices.columns.is_unique or len(prices) > 500000):
        raise ValueError("held-path original keys/price schema or budget differs")
    prices = prices.copy(deep=True)
    prices["trade_date"] = prices.trade_date.map(_day)
    if prices.trade_date.gt(cutoff).any() or prices.duplicated(["trade_date", "instrument"]).any():
        raise ValueError("held-path cannot decode test/future-cutoff or duplicate quotes")
    bars = prices.set_index(["trade_date", "instrument"]).to_dict("index")
    output, unknown = [], []
    for row in rows.to_dict("records"):
        t, s = _day(row["entry_date"]), _day(row["s_date"])
        if t not in positions or s not in positions or not 0 <= positions[s]-positions[t] <= 3 or row["remaining_sessions"] != 4-(positions[s]-positions[t]):
            raise ValueError("held-path original T/S/remaining clock differs")
        values = dict.fromkeys(NEW_FEATURES)
        if s <= cutoff:
            sessions = days[positions[t]:positions[s]+1]
            history = [bars.get((day, row["instrument"])) for day in sessions]
            first, last = history[0], history[-1]
            f_s = None if last is None else _number(last["adj_factor"], positive=True)
            def price(bar, name):
                if bar is None or f_s is None:
                    return None
                value, factor = _number(bar["raw_"+name+"_cny"], positive=True), _number(bar["adj_factor"], positive=True)
                return None if value is None or factor is None else value*factor/f_s
            anchor, close = price(first, "open"), price(last, "close")
            highs, lows = [price(bar, "high") for bar in history], [price(bar, "low") for bar in history]
            if any(high is not None and low is not None and high < low for high, low in zip(highs, lows, strict=True)):
                raise ValueError("held-path original high/low range contradicts")
            if (close is not None and ((highs[-1] is not None and close > highs[-1]) or (lows[-1] is not None and close < lows[-1]))
                    or anchor is not None and ((highs[0] is not None and anchor > highs[0]) or (lows[0] is not None and anchor < lows[0]))):
                raise ValueError("held-path original OHLC range contradicts")
            if anchor is not None and close is not None:
                values[NEW_FEATURES[0]] = 10000*(close*(1-POLICY["sell_bps"]/10000)/(anchor*(1+POLICY["buy_bps"]/10000))-1)
            if close is not None and all(value is not None for value in highs):
                values[NEW_FEATURES[1]] = close/max(highs)-1
            if all(value is not None for value in (*highs, *lows)):
                values[NEW_FEATURES[2]] = max(highs)/min(lows)-1
        if any(value is not None and not np.isfinite(value) for value in values.values()):
            raise ValueError("held-path derived value is nonfinite")
        output.append({**{key: row[key] for key in FEATURE_KEY}, **values})
        unknown.append({**{key: str(row[key]) for key in FEATURE_KEY},
            "unknown_fields": [key for key, value in values.items() if value is None],
            "reason": "ORIGINAL_T_TO_S_INPUT_UNKNOWN" if s <= cutoff else "S_AFTER_DEVELOPMENT_CUTOFF"})
    features = pd.DataFrame(output, columns=(*FEATURE_KEY, *NEW_FEATURES))
    receipt = dict(feature_sha256=FEATURE_SHA256, original_decisions=len(rows), new_fields=list(NEW_FEATURES),
        known_counts={name: int(features[name].notna().sum()) for name in NEW_FEATURES}, unknown=unknown,
        future_features_read=False, test_values_decoded=False, original_episode_reselected=False, physical_fits=0)
    return features, receipt
