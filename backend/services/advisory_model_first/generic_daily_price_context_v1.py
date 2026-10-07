"""Raw-tick projection for the explicit DAILY_5TD family; no DB or activation."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
import time

import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, _day, _number
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import query_price_nodes_v1
from backend.services.advisory_model_first.generic_price_set_consumer_v1 import (
    LoadedGenericPriceSetModelV1, _fail, _fitted, project_generic_price_sets_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

NUMBERS = ("reference_cny", "raw_legal_low_cny", "raw_legal_high_cny", "raw_tick_cny", "target_raw_price_multiplier")
CONTEXT_KEYS = {*NUMBERS, "visible_through", "coordinate_source"}


def _coordinate(value, decision):
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != CONTEXT_KEYS:
        _fail("generic raw price coordinate schema differs")
    day = _day(value["visible_through"], nullable=True)
    if day is not None and day > _day(decision):
        _fail("generic raw price coordinate is later than D")
    source = value["coordinate_source"]
    if source is not None and (not isinstance(source, str) or not source.strip() or len(source) > 1024):
        _fail("generic raw price coordinate source differs")
    known = {name: _number(value[name], positive=True) for name in NUMBERS}
    low, high = known["raw_legal_low_cny"], known["raw_legal_high_cny"]
    if low is not None and high is not None and low > high:
        _fail("generic raw legal bounds contradict each other")
    return {**known, "visible_through": day.isoformat() if day else None, "coordinate_source": source}


def _groups(prices, states):
    bands, start, end = [], None, None
    for price, state in zip(prices, states, strict=True):
        if state == "ACCEPTABLE":
            start, end = price if start is None else start, price
        elif start is not None:
            bands.append((start, end))
            start = end = None
    if start is not None:
        bands.append((start, end))
    return tuple(bands)


def project_generic_raw_price_sets_v1(*, loaded, candidate_rows, raw_price_contexts, source_context,
                                      budget_seconds=30., monotonic=time.monotonic):
    """Same original model nodes, but never round an ex-right adjusted tick."""
    if (not isinstance(loaded, LoadedGenericPriceSetModelV1) or loaded.model_family != "DAILY_5TD"
            or not isinstance(raw_price_contexts, dict)):
        _fail("generic raw adapter only supplies the explicit nine-field daily family")
    # Existing consumer validates immutable model, original roster and feature clocks.
    started = monotonic()
    result = project_generic_price_sets_v1(loaded=loaded, candidate_rows=candidate_rows,
        price_contexts={}, source_context=source_context, budget_seconds=budget_seconds, monotonic=monotonic)
    deadline = started + budget_seconds
    if set(raw_price_contexts) - {row["instrument"] for row in result["advice"]}:
        _fail("generic raw coordinates contain a foreign original candidate")
    coordinates = [_coordinate(raw_price_contexts.get(row["instrument"]), row["decision_as_of_trade_date"])
                   for row in result["advice"]]
    fitted = _fitted(loaded.model_family, loaded.model_bytes)
    advice = []
    for original, features, coordinate in zip(result["advice"], candidate_rows.to_dict("records"), coordinates, strict=True):
        if monotonic() > deadline:
            _fail("generic raw projection exceeded its deadline without publishing a partial batch")
        row = {**original, "intervals_raw_cny": (), "raw_tick_cny": None, "target_raw_price_multiplier": None,
               "raw_price_basis": "T_RAW_CNY", "coordinate_source": None}
        if original["status"] == "UNKNOWN_FEATURE_CLOCK" or coordinate is None:
            advice.append(row)
            continue
        row.update({name: coordinate[name] for name in ("raw_tick_cny", "target_raw_price_multiplier", "coordinate_source")})
        if (coordinate["visible_through"] != row["decision_as_of_trade_date"]
                or coordinate["coordinate_source"] is None or any(coordinate[name] is None for name in NUMBERS)):
            advice.append(row)
            continue
        with localcontext() as context:
            context.prec = 34
            reference, low, high, tick, multiplier = (Decimal(str(coordinate[name])) for name in NUMBERS)
            first = int((low / tick).to_integral_value(rounding=ROUND_CEILING))
            last = int((high / tick).to_integral_value(rounding=ROUND_FLOOR))
            count = max(0, last - first + 1)
            if count > 100000:
                _fail("generic raw complete grid exceeds its upfront node budget")
            prices = [tick * index for index in range(first, last + 1)]
            gaps = [float((price / multiplier / reference - 1) * 10000) for price in prices]
            nodes = query_price_nodes_v1(fitted=fitted, features=pd.DataFrame(
                [{name: features[name] for name in FEATURES}] * count, columns=FEATURES),
                scenario_gap_bps=gaps, arm="candidate")
            if not set(nodes.status).issubset({"ACCEPTABLE", "AVOID", "UNKNOWN_INPUT_OR_SUPPORT"}):
                _fail("generic raw model returned an unknown node state")
            raw_bands = _groups(prices, nodes.status)
            anchored_bands = tuple((float(low / multiplier), float(high / multiplier)) for low, high in raw_bands)
            unknown = int(nodes.status.eq("UNKNOWN_INPUT_OR_SUPPORT").sum())
            status = ("EMPTY_LEGAL_GRID" if not count else "ACCEPTABLE_PRICE_SET" if raw_bands
                else "UNKNOWN_PARTIAL_OR_NO_ACCEPTABLE" if unknown and unknown < count
                else "UNKNOWN_INPUT_OR_SUPPORT" if unknown else "NO_ACCEPTABLE_PRICE")
            row.update(status=status, intervals_cny=anchored_bands,
                intervals_raw_cny=tuple((float(low), float(high)) for low, high in raw_bands),
                legal_node_count=count, unknown_node_count=unknown,
                valuation_semantics="OBSERVED_OPEN_SCENARIO_ASSOCIATION_NOT_LIMIT_FILL")
        advice.append(row)
        if monotonic() > deadline:
            _fail("generic raw projection exceeded its deadline without publishing a partial batch")
    if monotonic() > deadline:
        _fail("generic raw projection exceeded its deadline without publishing a partial batch")
    result.update(schema_version="generic_daily_raw_price_projection_v1", advice=advice,
        input_sha256=sha(dict(generic_input_sha256=result["input_sha256"], raw_coordinates=coordinates)),
        raw_price_basis="T_RAW_CNY", coordinate_mapping="RAW_DECIMAL_TICK_TO_D_ANCHOR_NO_REGRID")
    return result
