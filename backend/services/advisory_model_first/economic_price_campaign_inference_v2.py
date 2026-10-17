"""Pure conditional price nodes/sets, without labels or future market input."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_price_campaign_contracts_v2 import ARMS
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import fit_identity_v2, predict_campaign_v2
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorPriceSetV1, finite_number
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import evaluate_value_anchor_price_v1


def campaign_nodes_v2(*, fitted, rows, arm):
    if arm not in ARMS or not rows.index.is_unique or len(rows) > 500000:
        raise ValueError('campaign query arm/index differs')
    if fit_identity_v2(fitted.model_id, fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('campaign fitted identity changed')
    result = pd.DataFrame(dict(status=['UNKNOWN_INPUT_OR_SUPPORT']*len(rows),
        expected_net_bps=[None]*len(rows), downside_q90_bps=[None]*len(rows)), index=rows.index)
    supported = []
    for index, row in rows.iterrows():
        raw = [row[name] for name in (*D_FEATURES, 'actual_gap_bps')]
        if any(isinstance(value, (bool, np.bool_)) for value in raw):
            raise ValueError('campaign query contains boolean numeric input')
        values = np.asarray([np.nan if pd.isna(value) else float(value) for value in raw])
        if np.isfinite(values).all() and fitted.support.contains(float(values[-1])):
            supported.append(index)
    if supported:
        estimates = predict_campaign_v2(fitted=fitted, rows=rows.loc[supported], arm=arm)
        for index, estimate in zip(supported, estimates, strict=True):
            if estimate is None:
                result.loc[index, 'status'] = 'UNKNOWN_LOCAL_SUPPORT'
                continue
            point = evaluate_value_anchor_price_v1(estimate=estimate,
                price_cny=1+float(rows.loc[index, 'actual_gap_bps'])/10000,
                reference_cny=1., support=fitted.support)
            result.loc[index] = [point.status, point.expected_net_bps, point.downside_q90_bps]
    return result


def campaign_price_set_v2(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    reference, low, high, tick = (finite_number(value, positive=True) for value in
        (reference_cny, legal_low_cny, legal_high_cny, tick_cny))
    if low > high or set(d_features) != set(D_FEATURES):
        raise ValueError('campaign legal bounds/D schema differs')
    step = Decimal(str(tick))
    first, last = (int((Decimal(str(value))/step).to_integral_value(rounding=mode))
        for value, mode in ((low, ROUND_CEILING), (high, ROUND_FLOOR)))
    if last-first+1 > 100000:
        raise ValueError('campaign pure price grid exceeds budget')
    prices = [float(step*number) for number in range(first, last+1)]
    rows = pd.DataFrame([{**d_features, 'actual_gap_bps': float((Decimal(str(price))/Decimal(str(reference))-1)*10000)}
        for price in prices], columns=[*D_FEATURES, 'actual_gap_bps'])
    nodes = campaign_nodes_v2(fitted=fitted, rows=rows, arm=arm)
    intervals, start, end = [], None, None
    for price, status in zip(prices, nodes.status, strict=True):
        if status == 'ACCEPTABLE':
            if start is None:
                start = price
            end = price
        elif start is not None:
            intervals.append((start, end))
            start = end = None
    if start is not None:
        intervals.append((start, end))
    status = 'ACCEPTABLE' if intervals else 'AVOID' if nodes.status.eq('AVOID').any() else 'UNKNOWN_INPUT_OR_SUPPORT'
    return ValueAnchorPriceSetV1(status, tuple(intervals), valuation_semantics='OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL')
