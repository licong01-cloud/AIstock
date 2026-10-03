"""Conditional price grid; no actual T prices, labels or outcome access."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_context_value_contracts_v1 import ARMS
from backend.services.advisory_model_first.economic_context_value_training_v1 import context_fit_identity_v1, predict_context_value_v1
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import (
    ValueAnchorPriceSetV1, finite_number,
)
from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import evaluate_value_anchor_price_v1


def context_value_nodes_v1(*, fitted, rows, arm):
    """UNKNOWN common support is not a default sector or a malformed-model mask."""
    if arm not in ARMS or not rows.index.is_unique:
        raise ValueError('context query family or row index differs')
    if context_fit_identity_v1(fitted.recipe, fitted.models, fitted.support) != fitted.model_sha256:
        raise ValueError('context fitted identity changed')
    result = pd.DataFrame({'status':['UNKNOWN_CONTEXT_OR_SUPPORT']*len(rows),
        'expected_net_bps':[None]*len(rows), 'downside_q90_bps':[None]*len(rows)}, index=rows.index)
    indices = []
    for index, row in rows.iterrows():
        support = fitted.support.get(row['classification_l2_code'])
        values = [row[name] for name in (*D_FEATURES, 'actual_gap_bps')]
        if any(isinstance(v, (bool, np.bool_)) for v in values):
            raise ValueError('context numerical query contains a boolean')
        values = [np.nan if pd.isna(value) else float(value) for value in values]
        if support is not None and np.isfinite(values).all() and support.contains(values[-1]):
            indices.append(index)
    if indices:
        estimates = predict_context_value_v1(fitted=fitted, rows=rows.loc[indices], arm=arm)
        for index, estimate in zip(indices, estimates, strict=True):
            row = rows.loc[index]
            point = evaluate_value_anchor_price_v1(estimate=estimate, price_cny=1+float(row.actual_gap_bps)/10000,
                reference_cny=1., support=fitted.support[row.classification_l2_code])
            result.loc[index] = [point.status, point.expected_net_bps, point.downside_q90_bps]
    return result


def context_value_price_set_v1(*, fitted, d_features, category, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny=.01):
    reference, low, high, tick = (finite_number(value, positive=True) for value in (reference_cny, legal_low_cny, legal_high_cny, tick_cny))
    if low > high or set(d_features) != set(D_FEATURES):
        raise ValueError('context grid legal bounds or D schema differ')
    step = Decimal(str(tick))
    first, last = (int((Decimal(str(value))/step).to_integral_value(rounding=mode)) for value, mode in ((low, ROUND_CEILING),(high,ROUND_FLOOR)))
    if last-first+1 > 100000:
        raise ValueError('context grid exceeds pure-kernel budget')
    prices = [float(step*node) for node in range(first,last+1)]
    rows = pd.DataFrame([{**d_features,'actual_gap_bps':float((Decimal(str(price))/Decimal(str(reference))-1)*10000),
        'classification_l2_code':category} for price in prices], columns=[*D_FEATURES,'actual_gap_bps','classification_l2_code'])
    nodes = context_value_nodes_v1(fitted=fitted, rows=rows, arm=arm)
    intervals, start, end = [], None, None
    for price, status in zip(prices,nodes.status,strict=True):
        if status == 'ACCEPTABLE':
            if start is None:
                start = price
            end = price
        elif start is not None:
            intervals.append((start,end))
            start = end = None
    if start is not None:
        intervals.append((start,end))
    status = 'ACCEPTABLE' if intervals else 'AVOID' if nodes.status.eq('AVOID').any() else 'UNKNOWN_CONTEXT_OR_SUPPORT'
    return ValueAnchorPriceSetV1(status,tuple(intervals),valuation_semantics='OBSERVED_PRICE_CONDITIONAL_NOT_CAUSAL_LIMIT_FILL')
