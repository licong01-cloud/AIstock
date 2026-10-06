"""One authentic five-session observation per original candidate, never per price grid."""
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number, _records
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY, POLICY_SHA256, ROSTER
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

PRICE_FIELDS = (KEY[0], "trade_date", "instrument", "open", "high", "low", "close",
                "d_anchor_factor", "suspended", "tradability_unknown", "up_limit", "down_limit")
REFERENCE_FIELDS = (*KEY, "reference_cny", "reference_visible_through")
LABEL_FIELDS = ("label_information_end", "observed_gap_bps", "gross_terminal_ratio", "path_min_ratio",
                "label_status", "label_reason", "label_contract", "policy_sha256")


def _frame(frame, columns, maximum, date_columns):
    if (not isinstance(frame, pd.DataFrame) or not frame.columns.is_unique
            or set(frame.columns) != set(columns) or len(frame) > maximum):
        raise ValueError("GP5 original frame schema or row budget differs")
    out = frame.loc[:, columns].copy(deep=True)
    for name in date_columns:
        out[name] = out[name].map(_day).map(pd.Timestamp)
    return out


def validate_roster(candidates, decision_dates, calendar):
    days = [_day(value) for value in calendar]
    decisions = [_day(value) for value in decision_dates]
    if (not days or len(days) > 5000 or days != sorted(set(days))
            or decisions != sorted(set(decisions)) or not set(decisions).issubset(days)):
        raise ValueError("GP5 original calendar or full decision schedule differs")
    roster = _frame(candidates, ROSTER, 7720, KEY[:2])
    if (roster.duplicated(list(KEY)).any() or roster.duplicated([KEY[0], KEY[2]]).any()
            or not roster.instrument.map(lambda v: isinstance(v, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", v) is not None).all()):
        raise ValueError("GP5 candidate keys must be original and unique")
    def integer(v):
        return isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_))
    for decision, group in roster.groupby(KEY[0], sort=False):
        d = decision.date()
        position = days.index(d) if d in decisions else -1
        if (position < 0 or position+1 >= len(days) or len(group) > 50
                or not group[KEY[1]].eq(pd.Timestamp(days[position+1])).all()
                or not group.selection_effective_rank.map(integer).all()
                or not group.candidate_group_size.map(integer).all()
                or group.selection_effective_rank.tolist() != list(range(1, len(group)+1))
                or not group.candidate_group_size.eq(len(group)).all()):
            raise ValueError("GP5 complete original ranks, D/T or group size differs")
    return roster, decisions, days


def build_generic_price_5td_labels_v1(*, candidates, decision_dates, calendar, prices, references, source_context):
    roster, decisions, days = validate_roster(candidates, decision_dates, calendar)
    required = {"calendar_sha256", "prices_sha256", "references_sha256", "label_price_basis", "source_evidence"}
    if (not isinstance(source_context, dict) or set(source_context) != required
            or source_context["label_price_basis"] != "D_REFERENCE_POLICY_RATIO"
            or not isinstance(source_context["source_evidence"], str) or not source_context["source_evidence"].strip()
            or any(not isinstance(source_context[name], str) or not re.fullmatch("[a-f0-9]{64}", source_context[name])
                   for name in required if name.endswith("_sha256"))):
        raise ValueError("GP5 source identity/coordinate declarations differ")
    price = _frame(prices, PRICE_FIELDS, 500000, (KEY[0], "trade_date"))
    reference = _frame(references, REFERENCE_FIELDS, 7720, KEY[:2])
    if price.duplicated([KEY[0], "trade_date", "instrument"]).any() or reference.duplicated(list(KEY)).any():
        raise ValueError("GP5 source duplicate keys")
    keys = set(roster.loc[:, KEY].itertuples(index=False, name=None))
    if set(reference.loc[:, KEY].itertuples(index=False, name=None)) != keys:
        raise ValueError("GP5 references differ from complete original candidate keys")
    allowed = {}
    for row in roster.itertuples(index=False):
        d, t = row.decision_as_of_trade_date, row.target_trade_date
        position = days.index(t.date())
        allowed[(d, row.instrument)] = {pd.Timestamp(day) for day in days[position:position+5]}
    if any(row.trade_date not in allowed.get((row.decision_as_of_trade_date, row.instrument), set())
           for row in price.itertuples(index=False)):
        raise ValueError("GP5 label source contains foreign candidate or outside-horizon quotes")
    for field in ("open", "high", "low", "close", "d_anchor_factor", "up_limit", "down_limit"):
        price[field] = price[field].map(lambda v: _number(v, positive=True)).astype(float)
    if (price.high.lt(price.low).any() or price.open.lt(price.low).any() or price.open.gt(price.high).any()
            or price.close.lt(price.low).any() or price.close.gt(price.high).any()
            or price.up_limit.le(price.down_limit).any()):
        raise ValueError("GP5 known OHLC/limit inputs contradict their units")
    for flag in ("suspended", "tradability_unknown"):
        if not price[flag].map(lambda v: type(v) is bool).all():
            raise ValueError("GP5 trading flags must be explicit bools; unknown uses tradability_unknown")
    reference["reference_cny"] = reference.reference_cny.map(lambda v: _number(v, positive=True)).astype(float)
    clocks = reference.reference_visible_through.map(lambda v: _day(v, nullable=True))
    if any(clock is not None and clock > row.decision_as_of_trade_date.date()
           for clock, row in zip(clocks, reference.itertuples(index=False), strict=True)):
        raise ValueError("GP5 reference sees after D")
    reference["reference_visible_through"] = clocks
    refs = reference.set_index(list(KEY)).to_dict("index")
    quotes = price.set_index([KEY[0], "trade_date", "instrument"]).to_dict("index")
    output = []
    for row in roster.to_dict("records"):
        key = tuple(row[name] for name in KEY)
        d, t, symbol = key
        position = days.index(t.date())
        horizon = days[position:position+5]
        h = pd.Timestamp(horizon[-1]) if len(horizon) == 5 else pd.NaT
        ref = refs[key]
        anchor = ref["reference_cny"]
        values = dict(label_information_end=h, observed_gap_bps=np.nan, gross_terminal_ratio=np.nan,
                      path_min_ratio=np.nan, label_status="UNKNOWN", label_reason="UNKNOWN_REFERENCE",
                      label_contract=POLICY["label_contract"], policy_sha256=POLICY_SHA256)
        bars = [quotes.get((d, pd.Timestamp(day), symbol)) for day in horizon]
        entry = bars[0]
        known_ref = pd.notna(anchor) and ref["reference_visible_through"] is not None
        if known_ref and entry is not None and not entry["suspended"] and not entry["tradability_unknown"]:
            if pd.notna(entry["open"]) and pd.notna(entry["d_anchor_factor"]):
                values["observed_gap_bps"] = 10000*(entry["open"]*entry["d_anchor_factor"]/anchor-1)
        reason = None
        if len(horizon) != 5:
            values["label_status"], reason = "IMMATURE", "HORIZON_BEYOND_SOURCE_CALENDAR"
        elif not known_ref:
            reason = "UNKNOWN_REFERENCE"
        elif entry is None:
            reason = "UNKNOWN_ENTRY_BAR"
        elif entry["suspended"]:
            values["label_status"], reason = "ENTRY_NOT_EXECUTABLE", "ENTRY_SUSPENDED"
        elif entry["tradability_unknown"] or pd.isna(entry["up_limit"]):
            reason = "UNKNOWN_TRADABILITY"
        elif pd.notna(entry["open"]) and entry["open"] >= entry["up_limit"]:
            values["label_status"], reason = "ENTRY_NOT_EXECUTABLE", "ENTRY_AT_UP_LIMIT"
        elif any(bar is None or bar["suspended"] or bar["tradability_unknown"] or any(pd.isna(bar[name])
                    for name in ("open", "high", "low", "close", "d_anchor_factor")) for bar in bars):
            reason = "UNKNOWN_PATH"
        elif pd.isna(bars[-1]["down_limit"]) or bars[-1]["close"] <= bars[-1]["down_limit"]:
            reason = "UNKNOWN_ENDPOINT_EXECUTION"
        else:
            gross = bars[-1]["close"]*bars[-1]["d_anchor_factor"]/anchor
            lower = min(bar["low"]*bar["d_anchor_factor"]/anchor for bar in bars)
            if not np.isfinite([gross, lower, values["observed_gap_bps"]]).all() or min(gross, lower) <= 0:
                raise ValueError("GP5 price-coordinate calculation is nonfinite or nonpositive")
            values.update(gross_terminal_ratio=gross, path_min_ratio=lower, label_status="AVAILABLE")
        values["label_reason"] = reason
        output.append({**row, **values})
    labels = pd.DataFrame(output, columns=[*ROSTER, *LABEL_FIELDS])
    receipt = dict(label_contract=POLICY["label_contract"], policy_sha256=POLICY_SHA256,
                   decision_dates=[day.isoformat() for day in decisions], candidate_count=len(roster),
                   label_counts={str(k): int(v) for k, v in labels.label_status.value_counts().items()},
                   source_context=dict(source_context), roster_sha256=sha(_records(roster)),
                   calendar_sha256=sha([day.isoformat() for day in days]), new_native_receipt=False,
                   source_evidence=source_context["source_evidence"], fit_count=0, deployable=False)
    return labels, receipt
