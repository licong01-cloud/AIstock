"""Pure D information plus hypothetical price; not an opening-price forecast or minute order."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_price_campaign_models_v2 import predict_json_v2
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_minute_price_5td_contracts_v1 import (
    ARMS, FEATURES, MINUTE_FEATURES, POLICY, POLICY_SHA256, SCHEMA_SHA256,
)
from backend.services.advisory_model_first.generic_minute_price_5td_models_v1 import (
    matrix_v1, validate_fit_v1, values_v1,
)


def query_minute_price_nodes_v1(*, fitted, features, scenario_gap_bps, arm):
    support = validate_fit_v1(fitted)
    if arm not in ARMS or not features.index.is_unique:
        raise ValueError("minute price query arm/index differs")
    raw, available = values_v1(features)
    gaps = np.asarray([_number(value) for value in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("minute price hypothetical coordinates differ")
    selected = available & np.array([False if np.isnan(g) else support.contains(float(g)) for g in gaps], dtype=bool)
    result = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(features),
        expected_net_bps=np.full(len(features), np.nan), downside_q90_bps=np.full(len(features), np.nan)), index=features.index)
    for name in ("decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank",
                 "candidate_group_size", "package_id", "run_id", "list_version_id", "universe_identity", "source_evidence",
                 "minute_reason", "minute_coverage_fraction", "minute_activity_coverage", "minute_partial"):
        if name in features:
            result[name] = features[name].copy()
    names = (*FEATURES, *MINUTE_FEATURES) if arm == "candidate" else FEATURES
    result["input_unknown_fields"] = [tuple(name for name, value in zip(names, row[:len(names)], strict=True) if np.isnan(value))
                                      for row in raw]
    result["model_sha256"], result["policy_sha256"] = fitted.model_sha256, POLICY_SHA256
    result["schema_sha256"], result["label_contract"] = SCHEMA_SHA256, POLICY["label_contract"]
    result["holding_period"] = "T_OPEN_THROUGH_T_PLUS_4_CLOSE"
    if selected.any():
        x = matrix_v1(features.iloc[np.flatnonzero(selected)], fitted.recipe["medians"], gaps=gaps[selected], arm=arm)
        mean, lower = (predict_json_v2(fitted.models[arm+"_"+head], x) for head in ("mean", "path"))
        if (mean <= 0).any() or (lower <= 0).any():
            raise ValueError("minute price model predicted a nonpositive ratio")
        entry = 1+gaps[selected]/10000
        net = 10000*(mean*(1-POLICY["sell_bps"]/10000)/(entry*(1+POLICY["buy_bps"]/10000))-1)
        risk = 10000*np.maximum(0., 1-lower/entry)
        if not np.isfinite([net, risk]).all():
            raise ValueError("minute price arithmetic is nonfinite")
        result.loc[selected, "status"] = np.where((net > 0) & (risk <= POLICY["risk_bps"]), "ACCEPTABLE", "AVOID")
        result.loc[selected, "expected_net_bps"], result.loc[selected, "downside_q90_bps"] = net, risk
    return result


def minute_price_set_5td_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    numbers = [_number(value, positive=True) for value in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    if any(value is None for value in numbers) or set(d_features) != {*FEATURES, *MINUTE_FEATURES}:
        raise ValueError("minute price legal coordinates or D feature schema differ")
    reference, low, high, tick = numbers
    if low > high:
        raise ValueError("minute price legal interval contradicts itself")
    step = Decimal(str(tick))
    first = int((Decimal(str(low))/step).to_integral_value(rounding=ROUND_CEILING))
    last = int((Decimal(str(high))/step).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("minute price full legal tick grid exceeds budget")
    prices = [float(step*number) for number in range(first, last+1)]
    gaps = [float((Decimal(str(price))/Decimal(str(reference))-1)*10000) for price in prices]
    frame = pd.DataFrame([d_features]*len(prices), columns=[*FEATURES, *MINUTE_FEATURES])
    nodes = query_minute_price_nodes_v1(fitted=fitted, features=frame, scenario_gap_bps=gaps, arm=arm)
    bands, start, end = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            start = price if start is None else start
            end = price
        elif start is not None:
            bands.append((start, end))
            start = end = None
    if start is not None:
        bands.append((start, end))
    unknown = int(nodes.status.eq("UNKNOWN_INPUT_OR_SUPPORT").sum())
    status = ("EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands
              else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if unknown and unknown < len(prices)
              else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
        model_sha256=fitted.model_sha256, schema_sha256=SCHEMA_SHA256, policy_sha256=POLICY_SHA256,
        label_contract=POLICY["label_contract"], holding_sessions=5, price_basis="D_ANCHORED_CNY",
        evidence_use="NAVIGATION_ONLY", deployable=False, valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
