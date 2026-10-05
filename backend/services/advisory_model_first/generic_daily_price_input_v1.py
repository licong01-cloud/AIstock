"""Pure package-independent D inputs; not a model, source or eligibility gate."""
from datetime import date
from decimal import Decimal
import json
import math
from numbers import Real
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

KEY = ("decision_as_of_trade_date", "target_trade_date", "instrument")
ROSTER = (*KEY, "selection_effective_rank", "candidate_group_size")
FEATURES = ("ret_1", "ret_5", "ret_10", "atr14_close", "csi300_ret_5",
            "relative_ret_5_vs_csi300", "close_location_in_day", "volume_ratio_5_to_20", "market_up_ratio")
CONTEXT = ("package_id", "run_id", "list_version_id", "universe_identity", "source_evidence",
           "price_basis", "volume_basis", "source_visible_through", "benchmark_visible_through")
SEMANTICS = {"schema_version": "generic_daily_price_input_v1", "features": FEATURES,
             "price_basis": "D_ADJUSTED_CNY", "volume_basis": "RAW_SHARES", "history_sessions": 20,
             "return_windows": [1, 5, 10], "returns": "complete_original_session_closes_no_fill",
             "atr": "mean_last_14_true_ranges_over_D_close_requires_15_sessions",
             "volume": "scaled_mean_last_5_over_mean_last_20", "market": "declared_D_definition",
             "missing": "per_field_unknown_no_fill_no_drop", "max_candidates": 50}


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_GENERIC_PRICE_INPUT_INVALID")


def _day(value, *, nullable=False, timestamp=True):
    if value is None or value is pd.NA or value is pd.NaT:
        if nullable:
            return None
        _fail("generic price input date is unknown")
    if type(value) is date:
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        try:
            return date.fromisoformat(value)
        except ValueError:
            pass
    if timestamp and isinstance(value, pd.Timestamp) and value.tz is None and value == value.normalize():
        return value.date()
    _fail("generic price input needs original session dates without timezone or intraday time")


def _number(value, *, positive=False, nonnegative=False):
    if value is None or value is pd.NA:
        return None
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (Real, Decimal)):
        _fail("generic price input needs numeric values, not booleans or strings")
    try:
        result = float(value)
    except (ValueError, OverflowError):
        _fail("generic price input numeric value cannot be represented")
    if math.isnan(result):
        return None
    if not math.isfinite(result) or positive and result <= 0 or nonnegative and result < 0:
        _fail("generic price input numeric value contradicts its units")
    return result


def _text(value):
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        _fail("generic price source identity must be text or explicitly unknown")
    return value


def _integer(value):
    return isinstance(value, (int, np.integer)) and not isinstance(value, (bool, np.bool_))


def _frame(frame, columns, maximum, sessions):
    if (not isinstance(frame, pd.DataFrame) or len(frame) > maximum or not frame.columns.is_unique
            or set(frame.columns) != set(columns)):
        _fail("generic price frame schema or bounded row count differs")
    result = frame.loc[:, columns].copy(deep=True)
    dates = result.trade_date.map(_day)
    if not dates.isin(sessions).all():
        _fail("generic price frame contains a foreign or future session")
    result["trade_date"] = dates.map(pd.Timestamp)
    if result.duplicated(["trade_date", "instrument"]).any():
        _fail("generic price frame contains duplicate or aliased keys")
    return result


def _records(frame):
    def encode(value):
        if isinstance(value, pd.Timestamp):
            return value.date().isoformat()
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, Real):
            return None if math.isnan(float(value)) else float(value)
        return value
    return [{name: encode(value) for name, value in row.items()} for row in frame.to_dict("records")]


def build_generic_daily_price_input_v1(*, candidates, calendar, panel, benchmark_daily, market_state, source_context):
    """Preserve the original roster and compute nine D-only values without I/O."""
    if (not isinstance(calendar, (tuple, list)) or len(calendar) != 21 or any(type(day) is not date for day in calendar)
            or list(calendar) != sorted(set(calendar))):
        _fail("generic price input requires twenty original D sessions and immediate next T")
    sessions, decision, target = tuple(calendar[:-1]), calendar[-2], calendar[-1]
    if (not isinstance(candidates, pd.DataFrame) or len(candidates) > 50 or not candidates.columns.is_unique
            or set(candidates.columns) != set(ROSTER)):
        _fail("generic price input requires the complete original candidate projection")
    roster = candidates.loc[:, ROSTER].copy(deep=True)
    for column, day in ((KEY[0], decision), (KEY[1], target)):
        if not roster[column].map(_day).eq(day).all():
            _fail("generic price roster D/T differs from its calendar")
        roster[column] = pd.Timestamp(day)
    ranks, groups = roster.selection_effective_rank, roster.candidate_group_size
    if (not ranks.map(_integer).all() or not groups.map(_integer).all()
            or ranks.tolist() != list(range(1, len(roster) + 1)) or not groups.eq(len(roster)).all()
            or roster.instrument.duplicated().any()
            or not roster.instrument.map(lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is not None).all()):
        _fail("generic price roster uniqueness, complete rank or group size differs")
    if not isinstance(source_context, dict) or set(source_context) != set(CONTEXT):
        _fail("generic price source context schema differs")
    context = {name: _text(source_context[name]) for name in CONTEXT[:3] + ("source_evidence", "price_basis", "volume_basis")}
    if context["price_basis"] != "D_ADJUSTED_CNY" or context["volume_basis"] != "RAW_SHARES":
        _fail("generic price coordinates must use the explicit D and volume bases")
    universe = source_context["universe_identity"]
    if universe is not None and not isinstance(universe, (str, dict)):
        _fail("generic price universe identity must be a finite JSON identity or unknown")
    if isinstance(universe, str):
        _text(universe)
    try:
        encoded = json.dumps(universe, ensure_ascii=False, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError, OverflowError):
        _fail("generic price universe identity is not finite JSON")
    if len(encoded.encode("utf-8")) > 65536:
        _fail("generic price universe identity exceeds its metadata budget")
    context["universe_identity"] = json.loads(encoded)
    clocks = {name: _day(source_context[name], nullable=True, timestamp=False)
              for name in ("source_visible_through", "benchmark_visible_through")}
    if any(value is not None and value > decision for value in clocks.values()):
        _fail("generic price source sees after D")
    context.update({name: value.isoformat() if value is not None else None for name, value in clocks.items()})
    raw = _frame(panel, ("trade_date", "instrument", "open", "high", "low", "close", "volume"), 1000, sessions)
    benchmark = _frame(benchmark_daily, ("trade_date", "instrument", "close"), 20, sessions)
    if set(raw.instrument) - set(roster.instrument) or not benchmark.instrument.eq("000300.SH").all():
        _fail("generic price input contains foreign candidate or benchmark instruments")
    for frame, clock in ((raw, clocks["source_visible_through"]), (benchmark, clocks["benchmark_visible_through"])):
        if clock is not None and frame.trade_date.gt(pd.Timestamp(clock)).any():
            _fail("generic price consumed bar is newer than its source clock")
    for column in ("open", "high", "low", "close", "volume"):
        raw[column] = raw[column].map(lambda value, field=column: _number(value, positive=field != "volume", nonnegative=field == "volume"))
    if (raw.high.lt(raw.low).any() or raw.open.lt(raw.low).any() or raw.open.gt(raw.high).any()
            or raw.close.lt(raw.low).any() or raw.close.gt(raw.high).any()):
        _fail("generic price OHLC values contradict their range")
    benchmark["close"] = benchmark.close.map(lambda value: _number(value, positive=True))
    if not isinstance(market_state, dict) or set(market_state) != {"trade_date", "market_up_ratio", "market_definition_id", "visible_through"}:
        _fail("generic price market state schema differs")
    if _day(market_state["trade_date"]) != decision:
        _fail("generic price market state belongs to another D")
    breadth = _number(market_state["market_up_ratio"])
    definition = _text(market_state["market_definition_id"])
    market_clock = _day(market_state["visible_through"], nullable=True, timestamp=False)
    if (breadth is not None and not 0 <= breadth <= 1 or market_clock is not None and market_clock > decision
            or breadth is not None and market_clock is not None and market_clock != decision):
        _fail("generic price market value or clock contradicts D")
    market = {"trade_date": decision.isoformat(), "market_up_ratio": breadth, "market_definition_id": definition,
              "visible_through": market_clock.isoformat() if market_clock is not None else None}
    quotes = {(row.trade_date.date(), row.instrument): row for row in raw.itertuples(index=False)}
    indices = {row.trade_date.date(): _number(row.close) for row in benchmark.itertuples(index=False)}
    base = [indices.get(day) for day in sessions[-6:]]
    breturn = base[-1] / base[0] - 1 if clocks["benchmark_visible_through"] is not None and all(value is not None for value in base) else None
    output = roster.copy(deep=True)
    for field in FEATURES:
        output[field] = np.nan
    unknown = []
    for position, symbol in enumerate(roster.instrument):
        history = [quotes.get((day, symbol)) for day in sessions]
        values, reasons = dict.fromkeys(FEATURES), {}
        price_known = clocks["source_visible_through"] is not None
        for field, periods in (("ret_1", 1), ("ret_5", 5), ("ret_10", 10)):
            closes = [None if row is None else _number(row.close) for row in history[-(periods + 1):]]
            if price_known and all(value is not None for value in closes):
                values[field] = closes[-1] / closes[0] - 1
        recent = history[-15:]
        if (price_known and all(row is not None and _number(row.close) is not None for row in recent)
                and all(all(_number(getattr(row, field)) is not None for field in ("high", "low")) for row in recent[1:])):
            tr = [max(now.high - now.low, abs(now.high - previous.close), abs(now.low - previous.close))
                  for previous, now in zip(recent[:-1], recent[1:], strict=True)]
            values["atr14_close"] = sum(value / 14 for value in tr) / recent[-1].close
        last = history[-1]
        if price_known and last is not None and all(_number(getattr(last, field)) is not None for field in ("high", "low", "close")) and last.high != last.low:
            values["close_location_in_day"] = (last.close - last.low) / (last.high - last.low)
        volumes = [None if row is None else _number(row.volume) for row in history]
        if price_known and all(value is not None for value in volumes) and max(volumes) > 0:
            scaled = np.asarray(volumes) / max(volumes)
            values["volume_ratio_5_to_20"] = float(scaled[-5:].mean() / scaled.mean())
        values["csi300_ret_5"] = breturn
        if values["ret_5"] is not None and breturn is not None:
            values["relative_ret_5_vs_csi300"] = values["ret_5"] - breturn
        if market_clock is not None and definition is not None:
            values["market_up_ratio"] = breadth
        for field, value in values.items():
            if value is not None and not math.isfinite(value):
                _fail("generic price derived value is nonfinite")
            output.iloc[position, output.columns.get_loc(field)] = np.nan if value is None else value
            if value is None:
                reasons[field] = ("UNKNOWN_SOURCE_CLOCK" if not price_known and field not in ("csi300_ret_5", "market_up_ratio")
                                  else "BENCHMARK_INPUT_OR_CLOCK_UNKNOWN" if field == "csi300_ret_5"
                                  else "MARKET_INPUT_DEFINITION_OR_CLOCK_UNKNOWN" if field == "market_up_ratio"
                                  else "D_INPUT_WINDOW_OR_DENOMINATOR_UNKNOWN")
        unknown.append({"instrument": symbol, "fields": reasons})
    receipt = {"schema_version": SEMANTICS["schema_version"], "semantics_sha256": sha(SEMANTICS),
               "candidate_roster_sha256": sha(_records(roster)), "context": context,
               "source_sha256": sha({"panel": _records(raw), "benchmark": _records(benchmark), "market": market,
                                     "context": context, "calendar": [day.isoformat() for day in calendar]}),
               "feature_sha256": sha(_records(output.loc[:, (*KEY, *FEATURES)])),
               "decision_date": decision.isoformat(), "target_date": target.isoformat(), "candidate_count": len(roster),
               "known_counts": {field: int(output[field].notna().sum()) for field in FEATURES},
               "unknown_counts": {field: int(output[field].isna().sum()) for field in FEATURES}, "unknown_fields": unknown,
               "status": "NO_CANDIDATES" if output.empty else "COMPUTED", "source_evidence": context["source_evidence"],
               "computation_only": True, "source_identity_rechecked": False, "qualification_rechecked": False,
               "outcomes_read": False, "fit_count": 0, "new_native_receipt": False}
    return output, receipt
