"""D-only price scenarios: no future data, observed-open prediction or orders."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import ARMS, FEATURES, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_models_v1 import feature_values, matrix, validate_fit
from backend.services.advisory_model_first.economic_price_campaign_models_v2 import predict_json_v2


def query_price_nodes_v1(*, fitted, features, scenario_gap_bps, arm):
    support = validate_fit(fitted)
    if arm not in ARMS or not features.index.is_unique:
        raise ValueError("GP5 arm or query index differs")
    raw_values, available = feature_values(features)
    gaps = np.asarray([_number(v) for v in scenario_gap_bps], dtype=float)
    if gaps.shape != (len(features),) or (gaps[~np.isnan(gaps)] <= -10000).any():
        raise ValueError("GP5 query price coordinates differ")
    selected = available & np.array([False if np.isnan(g) else support.contains(float(g)) for g in gaps], dtype=bool)
    result = pd.DataFrame({"status": ["UNKNOWN_INPUT_OR_SUPPORT"]*len(features),
                           "expected_net_bps": np.full(len(features), np.nan),
                           "downside_q90_bps": np.full(len(features), np.nan)}, index=features.index)
    metadata = ("decision_as_of_trade_date", "target_trade_date", "instrument", "selection_effective_rank",
                "candidate_group_size", "package_id", "run_id", "list_version_id", "universe_identity", "source_evidence")
    for name in metadata:
        if name in features:
            result[name] = features[name].copy()
    result["model_sha256"], result["policy_sha256"] = fitted.model_sha256, POLICY_SHA256
    result["label_contract"] = POLICY["label_contract"]
    result["holding_period"] = "T_OPEN_THROUGH_T_PLUS_4_CLOSE"
    result["input_unknown_fields"] = [tuple(name for name, value in zip(FEATURES, row, strict=True) if np.isnan(value))
                                      for row in raw_values]
    if selected.any():
        x = matrix(features.iloc[np.flatnonzero(selected)], fitted.recipe["medians"],
                   gaps=gaps[selected] if arm == "candidate" else None)
        means, lower = (predict_json_v2(fitted.models[arm+"_"+head], x) for head in ("mean", "path"))
        if (means <= 0).any() or (lower <= 0).any():
            raise ValueError("GP5 model predicted nonpositive price ratio")
        gross_entry = 1+gaps[selected]/10000
        net = 10000*(means*(1-POLICY["sell_bps"]/10000)/(gross_entry*(1+POLICY["buy_bps"]/10000))-1)
        risk = 10000*np.maximum(0, 1-lower/gross_entry)
        if not np.isfinite([net, risk]).all():
            raise ValueError("GP5 net/risk arithmetic is nonfinite")
        result.loc[selected, "status"] = np.where((net > 0) & (risk <= POLICY["risk_bps"]), "ACCEPTABLE", "AVOID")
        result.loc[selected, "expected_net_bps"] = net
        result.loc[selected, "downside_q90_bps"] = risk
    return result


def generic_price_set_5td_v1(*, fitted, d_features, arm, reference_cny, legal_low_cny, legal_high_cny, tick_cny):
    numbers = [_number(v, positive=True) for v in (reference_cny, legal_low_cny, legal_high_cny, tick_cny)]
    if any(v is None for v in numbers) or set(d_features) != set(FEATURES):
        raise ValueError("GP5 price set schema or known legal coordinates differ")
    reference, low, high, tick = numbers
    if low > high:
        raise ValueError("GP5 legal price bounds differ")
    step = Decimal(str(tick))
    first = int((Decimal(str(low))/step).to_integral_value(rounding=ROUND_CEILING))
    last = int((Decimal(str(high))/step).to_integral_value(rounding=ROUND_FLOOR))
    if last-first+1 > 100000:
        raise ValueError("GP5 full tick grid exceeds budget")
    prices = [float(step*number) for number in range(first, last+1)]
    gaps = [float((Decimal(str(price))/Decimal(str(reference))-1)*10000) for price in prices]
    features = pd.DataFrame([d_features]*len(prices), columns=FEATURES)
    nodes = query_price_nodes_v1(fitted=fitted, features=features, scenario_gap_bps=gaps, arm=arm)
    bands, a, b = [], None, None
    for price, state in zip(prices, nodes.status, strict=True):
        if state == "ACCEPTABLE":
            a = price if a is None else a
            b = price
        elif a is not None:
            bands.append((a, b))
            a = b = None
    if a is not None:
        bands.append((a, b))
    unknown = int(nodes.status.eq("UNKNOWN_INPUT_OR_SUPPORT").sum())
    status = ("EMPTY_LEGAL_GRID" if not prices else "ACCEPTABLE_PRICE_SET" if bands
              else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if unknown and unknown < len(prices)
              else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
    return dict(status=status, intervals_cny=tuple(bands), legal_node_count=len(prices), unknown_node_count=unknown,
                model_sha256=fitted.model_sha256, policy_sha256=POLICY_SHA256,
                label_contract=POLICY["label_contract"], holding_sessions=5, price_basis="D_ANCHORED_CNY",
                evidence_use="NAVIGATION_ONLY", deployable=False,
                valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
