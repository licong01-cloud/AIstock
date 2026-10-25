"""Cost-once value algebra and supported price sets; no model or market IO."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_entry_labels import _day
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import (
    COST, RISK_REFERENCE_BPS, ValueAnchorGapSupportV1, ValueAnchorPointV1,
    ValueAnchorPriceSetV1, finite_number,
)


def build_value_anchor_gap_support_v1(observations):
    """Unlabelled train-only actual gaps; never choose support using returns."""
    fields = ["decision_as_of_trade_date", "target_trade_date", "instrument", "split", "actual_gap_bps", *D_FEATURES]
    if not set(fields).issubset(observations.columns):
        raise ValueError("gap observations omit D inputs/clock/split")
    data = observations.loc[:, fields].copy()
    if not data.split.eq("train").all() or data.duplicated(fields[:3]).any():
        raise ValueError("support accepts unique train observations only")
    for name in fields[:2]:
        data[name] = data[name].map(_day)
    if not (data[fields[0]] < data[fields[1]]).all():
        raise ValueError("support has an invalid decision clock")
    if not data.instrument.map(lambda value: isinstance(value, str) and bool(value.strip())).all():
        raise ValueError("support instruments must have an explicit identity")
    if data.loc[:, ["actual_gap_bps", *D_FEATURES]].map(lambda value: isinstance(value, (bool, np.bool_))).any().any():
        raise ValueError("boolean model/gap data are not numeric observations")
    values = data.loc[:, ["actual_gap_bps", *D_FEATURES]].apply(pd.to_numeric, errors="coerce")
    data = data.loc[np.isfinite(values).all(axis=1)].copy()
    data["actual_gap_bps"] = values.loc[data.index, "actual_gap_bps"]
    if data.empty:
        return ValueAnchorGapSupportV1(())
    if (data.actual_gap_bps <= -10000).any():
        raise ValueError("observed gap does not represent a positive price")
    low, high = data.actual_gap_bps.quantile([.025, .975], interpolation="linear")
    buckets = np.floor(data.actual_gap_bps/100).astype(int)
    intervals = []
    for _, group in data.groupby(buckets, sort=True):
        if len(group) < 30 or group.decision_as_of_trade_date.nunique() < 5:
            continue
        left, right = max(float(group.actual_gap_bps.min()), low), min(float(group.actual_gap_bps.max()), high)
        if left <= right:
            intervals.append((float(left), float(right)))
    return ValueAnchorGapSupportV1(tuple(intervals))


def evaluate_value_anchor_price_v1(*, estimate, price_cny, reference_cny, support):
    price, reference = (finite_number(value, positive=True) for value in (price_cny, reference_cny))
    if not isinstance(support, ValueAnchorGapSupportV1):
        raise ValueError("price support contract differs")
    if estimate is not None:
        mean = finite_number(estimate.mean_gross_value_ratio, positive=True)
        lower = finite_number(estimate.path_min_ratio_q10, positive=True)
    gap = float((Decimal(str(price))/Decimal(str(reference))-1)*10000)
    if not support.contains(gap):
        return ValueAnchorPointV1("UNKNOWN_OUT_OF_SUPPORT", None, None)
    if estimate is None:
        return ValueAnchorPointV1("UNKNOWN_MODEL_VALUE", None, None)
    # Revalidate caller estimates; never downgrade malformed predictions to control.
    after_cost = (1-Decimal(str(COST.sell_cost_bps))/10000)/(
        (Decimal(str(price))/Decimal(str(reference)))*(1+Decimal(str(COST.buy_cost_bps))/10000))
    expected_exact = (Decimal(str(mean))*after_cost-1)*10000
    downside_exact = max(Decimal(0), (1-Decimal(str(lower))*after_cost)*10000)
    expected, downside = float(expected_exact), float(downside_exact)
    if not np.isfinite([expected, downside]).all():
        raise ValueError("value calculation overflowed")
    return ValueAnchorPointV1("ACCEPTABLE" if expected_exact > 0 and downside_exact <= Decimal(str(RISK_REFERENCE_BPS)) else "AVOID", expected, downside)


def build_value_anchor_price_set_v1(*, estimate, reference_cny, support, legal_low_cny, legal_high_cny, tick_cny=.01):
    """Tick inward by testing the strict value predicate; never bridge support holes."""
    reference, low, high, tick = (finite_number(value, positive=True) for value in (reference_cny, legal_low_cny, legal_high_cny, tick_cny))
    if low > high:
        raise ValueError("legal price bounds are reversed")
    step = Decimal(str(tick))
    first = int((Decimal(str(low))/step).to_integral_value(rounding=ROUND_CEILING))
    last = int((Decimal(str(high))/step).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("price grid exceeds the explicit pure-kernel budget")
    segments, start, end, has_support = [], None, None, False
    for node in range(first, last+1):
        price = float(step*node)
        point = evaluate_value_anchor_price_v1(estimate=estimate, price_cny=price, reference_cny=reference, support=support)
        has_support |= point.status != "UNKNOWN_OUT_OF_SUPPORT"
        if point.status == "ACCEPTABLE":
            if start is None:
                start = price
            end = price
        elif start is not None:
            segments.append((start, end))
            start = end = None
    if start is not None:
        segments.append((start, end))
    status = "ACCEPTABLE" if segments else "UNKNOWN_OUT_OF_SUPPORT" if not has_support else "UNKNOWN_MODEL_VALUE" if estimate is None else "AVOID"
    return ValueAnchorPriceSetV1(status, tuple(segments))
