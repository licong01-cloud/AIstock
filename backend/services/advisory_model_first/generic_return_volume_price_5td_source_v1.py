"""D-only 19 returns/volumes: project actual requests before parsing values."""
import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_return_volume_price_5td_contracts_v1 import KEY, LAG_FEATURES


def _metrics(closes, factors, volumes):
    if np.isnan([*closes, *factors, *volumes]).any():
        return [np.nan]*3, "UNKNOWN_SOURCE", 0
    r = np.diff(np.log(closes)+np.log(factors))
    if not np.isfinite(r).all():
        raise ValueError("return-volume historical price arithmetic is nonfinite")
    positive = volumes > 0
    if not positive.any():
        return [np.nan]*3, "UNKNOWN_ZERO_VOLUME_MASS", 0
    logs = np.log(volumes[positive])-np.log(factors[1:][positive])
    q = np.zeros(19)
    q[positive] = np.exp(logs-logs.max())
    underflow = int(np.sum(positive & (q == 0)))
    scale = float(np.max(np.abs(r)))
    scaled = r/scale if scale else r
    prior, now, future = q[:-1], q[1:], scaled[1:]
    centred = future-future.mean()
    pressure = prior*np.maximum(-scaled[:-1], 0)
    a, b, c = prior.sum(), np.sum(now+prior), pressure.sum()
    values = [float(prior@future/a-future.mean())*scale if a else np.nan,
              float((now-prior)@centred/b)*scale if b else np.nan,
              float(pressure@future/c)*scale if c else np.nan]
    if any(not np.isfinite(v) for v in values if not np.isnan(v)):
        raise ValueError("return-volume lag calculation is nonfinite")
    reasons = ([] if a else ["UNKNOWN_NO_LAG_VOLUME_MASS"])+([] if c else ["UNKNOWN_NO_OBSERVED_DOWNSIDE_PRESSURE"])
    return values, ";".join(reasons) or "AVAILABLE", underflow


def build_return_volume_lag_features_v1(*, roster, calendar, prices, volumes):
    """Normal absence preserves every KEY; bad requested values are errors."""
    if (not isinstance(roster, pd.DataFrame) or not roster.columns.is_unique or not set(KEY).issubset(roster.columns)
            or len(roster) > 7720 or roster.duplicated(list(KEY)).any()):
        raise ValueError("return-volume original candidate keys differ")
    original = roster.loc[:, KEY].copy()
    for name in KEY[:2]:
        original[name] = original[name].map(_day).map(pd.Timestamp)
    if (original.duplicated([KEY[0], KEY[2]]).any()
            or not original.instrument.map(lambda v: isinstance(v, str) and bool(v.strip())).all()):
        raise ValueError("return-volume candidate symbols/clock differ")
    days = pd.DatetimeIndex([pd.Timestamp(_day(v)) for v in calendar])
    if len(days) > 5000 or days.has_duplicates or not days.is_monotonic_increasing:
        raise ValueError("return-volume original calendar differs")
    windows, requests = [], set()
    for d, t, symbol in original.itertuples(index=False, name=None):
        position = days.get_indexer([d])[0]
        if position < 0 or position+1 >= len(days) or days[position+1] != t:
            raise ValueError("return-volume T is not immediate original next session")
        window = days[max(0, position-19):position+1]
        windows.append(window)
        requests.update((day, symbol) for day in window)
    key_columns = ["trade_date", "instrument"]
    selected = []
    for frame, fields, maximum in ((prices, [*key_columns, "raw_close_cny", "adj_factor"], 500000),
                                    (volumes, [*key_columns, "volume_hand"], 154400)):
        if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique
                or not set(fields).issubset(frame.columns) or len(frame) > maximum):
            raise ValueError("return-volume source schema/budget differs")
        keys = frame.loc[:, key_columns].copy()
        keys["trade_date"] = keys.trade_date.map(_day).map(pd.Timestamp)
        mask = [key in requests for key in keys.itertuples(index=False, name=None)]
        block = frame.loc[mask, fields].copy()
        block["trade_date"] = keys.loc[mask, "trade_date"].to_numpy()
        if block.duplicated(key_columns).any():
            raise ValueError("return-volume requested source has duplicate keys")
        for field in fields[2:]:
            block[field] = block[field].map(lambda v: _number(v, nonnegative=True) if field == "volume_hand"
                                           else _number(v, positive=True)).astype(float)
        selected.append(block.set_index(key_columns))
    stock, quantity = selected
    output, underflow_count = [], 0
    for item, window in zip(original.to_dict("records"), windows, strict=True):
        fields, reason, underflow = [np.nan]*3, "UNKNOWN_20D_WARMUP", 0
        if len(window) == 20:
            keys = pd.MultiIndex.from_tuples([(day, item["instrument"]) for day in window], names=key_columns)
            p, v = stock.reindex(keys), quantity.reindex(keys[1:])
            fields, reason, underflow = _metrics(p.raw_close_cny.to_numpy(), p.adj_factor.to_numpy(), v.volume_hand.to_numpy())
        underflow_count += underflow
        output.append({**item, **dict(zip(LAG_FEATURES, fields, strict=True)), "return_volume_reason": reason,
                       "quantity_encoding_underflow_count": underflow})
    result = pd.DataFrame(output, columns=[*KEY, *LAG_FEATURES, "return_volume_reason", "quantity_encoding_underflow_count"])
    receipt = dict(original_keys=len(original), requested_D_past_keys=len(requests),
        rows_all_lag_features_known=int(result.loc[:, LAG_FEATURES].notna().all(axis=1).sum()),
        reason_counts={str(k): int(v) for k, v in result.return_volume_reason.value_counts().items()},
        quantity_encoding_underflow_count=underflow_count, future_feature_values_parsed=0,
        source_evidence="CURRENT_FROZEN_NON_VINTAGE", database_written=False, selection_regenerated=False)
    return result, receipt
