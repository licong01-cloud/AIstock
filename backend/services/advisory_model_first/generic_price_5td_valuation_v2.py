"""Fixed-session valuation is not a fill: retain the original V1 execution labels."""
from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_5td_labels_v1 import (
    PRICE_FIELDS, REFERENCE_FIELDS, _frame, build_generic_price_5td_labels_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

VALUATION_POLICY = {
    "contract": "GENERIC_FIXED5_VALUATION_EXECUTION_V2",
    "legacy_execution_policy_sha256": POLICY_SHA256,
    "sessions_including_entry": 5,
    "entry": "ORIGINAL_T_OPEN_NOMINAL_NOT_A_FILL",
    "endpoint": "ORIGINAL_T_PLUS_4_CLOSE_MARK_NOT_A_FILL",
    "suspension": "EXPLICIT_VERIFIED_ONLY_CARRY_LAST_D_ANCHORED_CLOSE_MARK",
    "unexplained_missing": "UNKNOWN_NO_FILL_NO_DATE_REMOVAL",
    "limit_down_exit": "VALUATION_KNOWN_EXECUTION_UNPROVEN_NO_DELAY",
    "buy_bps": POLICY["buy_bps"],
    "hypothetical_sell_bps": POLICY["sell_bps"],
}
VALUATION_POLICY_SHA256 = sha(VALUATION_POLICY)
VALUATION_FIELDS = (
    "valuation_status", "valuation_reason", "valuation_gross_terminal_ratio",
    "valuation_path_min_ratio", "mark_to_market_net_bps", "hypothetical_liquidation_net_bps",
    "exit_execution_status", "verified_suspension_sessions", "carried_valuation_sessions",
    "valuation_policy_sha256", "actual_fill_proven", "realized_return_bps",
)


def build_generic_price_5td_valuation_v2(**packet):
    """A separate post-H measurement; never relabel V1 or alter D-time predictions."""
    legacy, legacy_receipt = build_generic_price_5td_labels_v1(**packet)
    prices = _frame(packet["prices"], PRICE_FIELDS, 500000, (KEY[0], "trade_date"))
    references = _frame(packet["references"], REFERENCE_FIELDS, 7720, KEY[:2])
    for field in ("open", "high", "low", "close", "d_anchor_factor", "up_limit", "down_limit"):
        prices[field] = prices[field].map(lambda v: _number(v, positive=True)).astype(float)
    references["reference_cny"] = references.reference_cny.map(
        lambda v: _number(v, positive=True)).astype(float)
    quotes = prices.set_index([KEY[0], "trade_date", "instrument"]).to_dict("index")
    refs = references.set_index(list(KEY)).to_dict("index")
    calendar = pd.to_datetime(packet["calendar"]).tolist()
    calendar_positions = {day: index for index, day in enumerate(calendar)}
    additions = []
    for original in legacy.to_dict("records"):
        d, t, symbol = (original[name] for name in KEY)
        start = calendar_positions[t]
        horizon = calendar[start:start+5]
        values = dict(valuation_status="UNKNOWN", valuation_reason=original["label_reason"],
            valuation_gross_terminal_ratio=np.nan, valuation_path_min_ratio=np.nan,
            mark_to_market_net_bps=np.nan, hypothetical_liquidation_net_bps=np.nan,
            exit_execution_status="UNKNOWN", verified_suspension_sessions=0,
            carried_valuation_sessions=0, valuation_policy_sha256=VALUATION_POLICY_SHA256,
            actual_fill_proven=False, realized_return_bps=np.nan)
        if original["label_status"] == "IMMATURE":
            values.update(valuation_status="IMMATURE", exit_execution_status="IMMATURE")
            additions.append(values)
            continue
        if original["label_status"] == "ENTRY_NOT_EXECUTABLE":
            values.update(valuation_status="CASH_ENTRY_NOT_EXECUTABLE",
                mark_to_market_net_bps=0., hypothetical_liquidation_net_bps=0.,
                exit_execution_status="NOT_HELD")
            additions.append(values)
            continue
        ref = refs[(d, t, symbol)]
        anchor = ref["reference_cny"]
        entry = quotes.get((d, t, symbol))
        if (pd.isna(anchor) or pd.isna(ref["reference_visible_through"]) or entry is None
                or entry["suspended"] or entry["tradability_unknown"]
                or any(pd.isna(entry[name]) for name in ("open", "d_anchor_factor", "up_limit"))):
            additions.append(values)
            continue
        entry_price = entry["open"]*entry["d_anchor_factor"]
        marks, minima = [], []
        last_close, reason, endpoint = None, None, None
        for day in horizon:
            bar = quotes.get((d, day, symbol))
            if bar is None:
                reason = "UNEXPLAINED_MISSING_BAR"
                break
            if bar["tradability_unknown"]:
                reason = "UNKNOWN_TRADABILITY"
                break
            endpoint = bar
            if bar["suspended"]:
                values["verified_suspension_sessions"] += 1
                if last_close is None:
                    reason = "SUSPENSION_LAST_KNOWN_CLOSE_UNAVAILABLE"
                    break
                # A valuation observation, never a fabricated OHLC or executable quote.
                marks.append(last_close)
                minima.append(last_close)
                values["carried_valuation_sessions"] += 1
                continue
            if any(pd.isna(bar[name]) for name in ("open", "high", "low", "close", "d_anchor_factor")):
                reason = "UNKNOWN_TRADED_PRICE_OR_COORDINATE"
                break
            last_close = bar["close"]*bar["d_anchor_factor"]
            marks.append(last_close)
            minima.append(bar["low"]*bar["d_anchor_factor"])
        if reason is not None or len(marks) != 5:
            values["valuation_reason"] = reason or "INCOMPLETE_FIXED_SESSION_PATH"
            additions.append(values)
            continue
        gross, lower = marks[-1]/anchor, min(minima)/anchor
        net_mark = 10000*(marks[-1]/entry_price/(1+POLICY["buy_bps"]/10000)-1)
        net_estimate = 10000*((marks[-1]/entry_price)*(1-POLICY["sell_bps"]/10000)
                             /(1+POLICY["buy_bps"]/10000)-1)
        if not np.isfinite([gross, lower, net_mark, net_estimate]).all() or min(gross, lower) <= 0:
            raise ValueError("fixed5 valuation has invalid D-anchored price arithmetic")
        if endpoint["suspended"]:
            execution = "PENDING_EXIT_SUSPENDED"
        elif pd.isna(endpoint["down_limit"]):
            execution = "UNKNOWN_EXIT_LIMIT"
        elif endpoint["close"] <= endpoint["down_limit"]:
            execution = "EXIT_UNPROVEN_LIMIT_DOWN"
        else:
            execution = "NOMINAL_EXIT_ELIGIBLE_NOT_FILL_PROOF"
        values.update(valuation_status="AVAILABLE", valuation_reason=None,
            valuation_gross_terminal_ratio=gross, valuation_path_min_ratio=lower,
            mark_to_market_net_bps=net_mark, hypothetical_liquidation_net_bps=net_estimate,
            exit_execution_status=execution)
        additions.append(values)
    result = pd.concat([legacy.reset_index(drop=True),
                        pd.DataFrame(additions, columns=VALUATION_FIELDS)], axis=1)
    receipt = dict(contract=VALUATION_POLICY["contract"], valuation_policy_sha256=VALUATION_POLICY_SHA256,
        legacy_receipt=legacy_receipt, legacy_execution_labels_unchanged=True,
        valuation_counts={str(k): int(v) for k, v in result.valuation_status.value_counts().items()},
        exit_execution_counts={str(k): int(v) for k, v in result.exit_execution_status.value_counts().items()},
        fit_count=0, actual_fill_proven=False, sell_cost_is_hypothetical=True,
        realized_profit_claimed=False, independent_oos_evidence=False, activation_evidence=False)
    return result, receipt
