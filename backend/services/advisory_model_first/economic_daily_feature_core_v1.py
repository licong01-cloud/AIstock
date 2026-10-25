"""One bounded D-information recipe for future fitting and daily consumption.

Pure calculation only: no source/native qualification, I/O or old-study replay.
"""
from datetime import date
from decimal import Decimal
import math
from numbers import Real
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_information_v1 import FEATURES, build_economic_daily_information_v1
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_sources import _suspension_states
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.realtime_feature_source import _market_frame
from backend.services.advisory_model_first.shared_feature_builder import _build_benchmark_features, _build_instrument_features, _build_market_features
from backend.services.advisory_model_first.suspension_aware_bar_policy import BAR_POLICY_HASH, build_suspension_aware_bar_panel
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

D_FEATURES = (*ECONOMIC_FEATURE_NAMES[:-1], *FEATURES)
RAW_FIELDS = ("open_li", "high_li", "low_li", "close_li", "volume_hand", "amount_li", "adj_factor")
QUOTE_FIELDS = ("pre_close", "up_limit", "down_limit")
SEMANTICS = {"schema_version": "economic_daily_feature_core_v1", "feature_names": D_FEATURES,
    "candidate_sessions": 20, "benchmark_sessions": 20, "breadth_sessions": 2,
    "benchmark_return_5": "all_six_session_closes_required_no_fill",
    "suspension_states": "economic_source_full_session_suspension_v1_resume_partial_no_synthetic_bar",
    "rank_denominator": "complete_original_candidate_count_minus_one_clipped_at_one",
    "candidate_price_basis": "RAW_LI_TO_CNY_ADJ_OVER_LAST_D_VISIBLE_ANCHOR",
    "information_volume_basis": "RAW_HAND_TO_SHARES_NOT_ADJUSTED",
    "bar_policy_sha256": BAR_POLICY_HASH, "qualification": "COMPUTATION_ONLY"}


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ECONOMIC_DAILY_CORE_INVALID")


def _number(value, *, positive=False, nonnegative=False, required=False):
    if value is None or value is pd.NA or isinstance(value, (Real, Decimal)) and not isinstance(value, (bool, np.bool_)) and np.isnan(float(value)):
        if required:
            _fail("daily core required numeric value is unknown")
        return np.nan
    if isinstance(value, (bool, np.bool_)) or not isinstance(value, (Real, Decimal)):
        _fail("daily core needs real numeric values, not booleans or strings")
    number = float(value)
    if not math.isfinite(number) or positive and number <= 0 or nonnegative and number < 0:
        _fail("daily core numeric value is invalid")
    return number


def _frame(frame, *, columns, optional=(), maximum, dates, symbols=None, keys=("trade_date", "instrument")):
    allowed = (*columns, *optional)
    if (not isinstance(frame, pd.DataFrame) or len(frame) > maximum or not frame.columns.is_unique
            or not set(columns).issubset(frame.columns) or set(frame.columns) - set(allowed)):
        _fail("daily core frame schema or row budget differs")
    result = frame.copy()
    for column in optional:
        if column not in result:
            result[column] = np.nan
    result = result.loc[:, allowed]
    days = pd.to_datetime(result.trade_date, errors="coerce")
    if (days.isna().any() or days.dt.tz is not None or not days.eq(days.dt.normalize()).all()
            or not days.isin(pd.DatetimeIndex(dates)).all()):
        _fail("daily core frame contains foreign or non-session time")
    result["trade_date"] = days
    if (not result.instrument.map(lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is not None).all()
            or symbols is not None and set(result.instrument) - set(symbols)
            or result.duplicated(list(keys)).any()):
        _fail("daily core frame contains foreign or duplicate instruments")
    return result.sort_values(list(keys)).reset_index(drop=True)


def _records(frame):
    return [{key: value.isoformat() if isinstance(value, (pd.Timestamp, date)) else None if pd.isna(value) else value
             for key, value in row.items()} for row in frame.to_dict("records")]


def build_economic_daily_feature_core_v1(*, candidates, raw_daily, market_daily, benchmark_daily,
                                         suspend_rows, calendar, component_roles, terminal_weights):
    """Return all original candidates and twelve D-only values; T is a clock.

    Call this same function per day during training or batch serving. Upstream
    dataset/PIT identity remains caller-owned, and no old scope is upgraded.
    """
    if (not isinstance(calendar, (tuple, list)) or len(calendar) != 21 or any(type(day) is not date for day in calendar)
            or list(calendar) != sorted(set(calendar))):
        _fail("daily core requires twenty D sessions and immediate next T")
    sessions, decision, target = tuple(calendar[:-1]), calendar[-2], calendar[-1]
    if (not isinstance(component_roles, dict) or set(component_roles) != {"lstm", "fund"}
            or any(not isinstance(value, str) or not value.strip() for value in component_roles.values())
            or len(set(component_roles.values())) != 2 or not isinstance(terminal_weights, dict)
            or set(terminal_weights) != set(component_roles.values())):
        _fail("daily core requires explicit two-leg roles and weights")
    weights = {key: _number(value, positive=True, required=True) for key, value in terminal_weights.items()}
    if not math.isclose(sum(weights.values()), 1., rel_tol=0., abs_tol=1e-10):
        _fail("daily core weights must sum to one")
    candidate_columns = (*KEY, "selection_effective_rank", "candidate_group_size", "combined_score", *(f"norm__{value}" for value in component_roles.values()))
    if (not isinstance(candidates, pd.DataFrame) or len(candidates) > 20 or not candidates.columns.is_unique
            or set(candidates.columns) != set(candidate_columns)):
        _fail("daily core needs the exact original candidate projection")
    roster = candidates.loc[:, candidate_columns].copy()
    for column, day in ((KEY[0], decision), (KEY[1], target)):
        values = pd.to_datetime(roster[column], errors="coerce")
        if values.dt.tz is not None or not values.eq(pd.Timestamp(day)).all():
            _fail("daily core candidate D/T clock differs")
        roster[column] = values
    ranks = roster.selection_effective_rank
    groups = roster.candidate_group_size
    if (not ranks.map(lambda v: isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_))).all()
            or not groups.map(lambda v: isinstance(v, (int, np.integer)) and not isinstance(v, (bool, np.bool_)) and v == len(roster)).all()
            or ranks.tolist() != list(range(1, len(roster) + 1)) or roster.instrument.duplicated().any()
            or not roster.instrument.map(lambda v: isinstance(v, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", v) is not None).all()):
        _fail("daily core must preserve the complete original Top20 order")
    for column in ("combined_score", *(f"norm__{value}" for value in component_roles.values())):
        # Empty projections otherwise keep object dtype after map, which breaks
        # np.isclose. Coercion follows the same strict finite-number validation.
        roster[column] = roster[column].map(lambda value: _number(value, required=True)).astype(float)
    expected = sum(roster[f"norm__{leg}"] * weight for leg, weight in weights.items())
    if not np.isclose(roster.combined_score, expected, atol=1e-8, rtol=0).all():
        _fail("daily core combined score disagrees with frozen legs")
    symbols = roster.instrument.tolist()
    raw = _frame(raw_daily, columns=("trade_date", "instrument", *RAW_FIELDS), optional=QUOTE_FIELDS,
                 maximum=400, dates=sessions, symbols=symbols)
    for column in RAW_FIELDS + QUOTE_FIELDS:
        raw[column] = raw[column].map(lambda value, name=column: _number(value,
            positive=name not in ("volume_hand", "amount_li"), nonnegative=name in ("volume_hand", "amount_li")))
    if (raw.high_li.lt(raw.low_li).any() or raw.open_li.gt(raw.high_li).any()
            or raw.open_li.lt(raw.low_li).any() or raw.close_li.gt(raw.high_li).any()
            or raw.close_li.lt(raw.low_li).any()):
        _fail("daily core raw OHLC values contradict their range")
    market = _frame(market_daily, columns=("trade_date", "instrument", "close"), maximum=20000, dates=sessions[-2:])
    market["close"] = market.close.map(lambda value: _number(value, positive=True))
    benchmark = _frame(benchmark_daily, columns=("trade_date", "instrument", "close"), maximum=20, dates=sessions, symbols=("000300.SH",))
    benchmark["close"] = benchmark.close.map(lambda value: _number(value, positive=True))
    suspends = _frame(suspend_rows, columns=("trade_date", "instrument", "suspend_type"), optional=("suspend_timing",),
        maximum=800, dates=sessions, symbols=symbols, keys=("trade_date", "instrument", "suspend_type"))
    if not suspends.suspend_type.isin(("S", "R")).all():
        _fail("daily core suspension state is invalid")
    if not suspends.suspend_timing.map(lambda value: isinstance(value, str) or pd.isna(value)).all():
        _fail("daily core suspension timing is invalid")
    states = _suspension_states(suspends)
    full_suspends = states.loc[states.suspended.eq(True), ["trade_date", "instrument"]].assign(suspend_type="S")
    input_hash = sha({"roster": _records(roster), "raw": _records(raw), "raw_present_fields": sorted(raw_daily.columns),
        "market": _records(market), "benchmark": _records(benchmark), "suspends": _records(suspends),
        "suspend_present_fields": sorted(suspend_rows.columns),
        "calendar": [day.isoformat() for day in calendar], "roles": component_roles, "weights": weights})
    output = roster.loc[:, KEY].copy()
    for name in D_FEATURES:
        output[name] = np.nan
    output["parent_combined_score"] = roster.combined_score
    output["parent_rank_pct"] = 1. - (ranks - 1) / max(len(roster) - 1, 1)
    output["leg_norm_score_gap"] = roster[f"norm__{component_roles['lstm']}"] - roster[f"norm__{component_roles['fund']}"]
    info_panel = pd.DataFrame(columns=["high", "low", "close", "volume"],
        index=pd.MultiIndex.from_tuples([], names=["datetime", "instrument"]))
    missing_bars = {}
    if not raw.empty:
        projected = raw.rename(columns={"instrument": "ts_code"}).copy()
        anchors = raw.dropna(subset=["adj_factor"]).groupby("instrument", sort=False).adj_factor.last()
        projected["base_adj_factor"] = projected.ts_code.map(anchors)
        panel = _market_frame(projected, context="economic_daily_feature_core_v1")
        info_panel = panel.loc[:, ["high", "low", "close"]].copy()
        info_panel["volume"] = raw.set_index(["trade_date", "instrument"]).volume_hand.rename_axis(["datetime", "instrument"]) * 100
        for index, symbol in zip(output.index, symbols, strict=True):
            one = panel.loc[panel.index.get_level_values("instrument") == symbol]
            if one.empty:
                missing_bars[symbol] = "CANDIDATE_HISTORY_UNKNOWN"
                continue
            resume_days = suspends.loc[suspends.instrument.eq(symbol) & suspends.suspend_type.eq("R"), "trade_date"]
            partial_days = states.loc[states.instrument.eq(symbol) & states.tradability_unknown.eq(True), "trade_date"]
            barrier_days = pd.DatetimeIndex(pd.concat([resume_days, partial_days])).unique()
            if len(barrier_days):
                barriers = one.reindex(pd.MultiIndex.from_product([barrier_days, [symbol]], names=one.index.names))
                visible = np.isfinite(barriers.loc[:, ["open", "high", "low", "close", "factor", "volume", "amount"]]).all(axis=1)
                if not (visible & (barriers.volume.gt(0) | barriers.amount.gt(0))).all():
                    missing_bars[symbol] = "RESUME_OR_PARTIAL_SESSION_BAR_UNKNOWN"
                    continue
            try:
                normalized = build_suspension_aware_bar_panel(daily=one,
                    suspend_rows=full_suspends.loc[full_suspends.instrument.eq(symbol)], trading_calendar=pd.DatetimeIndex(sessions)).panel
            except AdvisoryModelFirstError as exc:
                if exc.reason_code not in {"ADVISORY_SUSPENSION_UNEXPLAINED_MISSING", "ADVISORY_SUSPENSION_LAST_CLOSE_UNAVAILABLE"}:
                    raise
                missing_bars[symbol] = exc.reason_code
                continue
            # These static fields are not consumed by this API. They only keep
            # the shared full calculator's optional-column contract explicit.
            for name in ("db_turnover_rate", "db_volume_ratio", "mf_lg_buy_amt", "mf_elg_buy_amt", "mf_lg_sell_amt", "mf_elg_sell_amt",
                         "db_pe_ttm", "db_pb", "db_circ_mv", "bb_rev_yoy", "bb_profit_yoy", "bb_gpr", "bb_npr", "cp_winner_rate",
                         "cp_cost_95pct", "cp_cost_5pct", "cp_cost_50pct", "md_rzye", "l2_code_id"):
                normalized[name] = np.nan
            calculated = _build_instrument_features(normalized)
            if (pd.Timestamp(decision), symbol) in calculated.index:
                for name in ("ret_1", "ret_5", "atr14_close"):
                    output.loc[index, name] = calculated.loc[(pd.Timestamp(decision), symbol), name]
    elif symbols:
        missing_bars = dict.fromkeys(symbols, "CANDIDATE_HISTORY_UNKNOWN")
    benchmark_values = benchmark.set_index(["trade_date", "instrument"]).rename_axis(["datetime", "instrument"])
    if not benchmark.empty:
        # Missing sessions stay NaN; pct_change cannot jump missing benchmark dates.
        benchmark_values = benchmark_values.reindex(pd.MultiIndex.from_product([pd.DatetimeIndex(sessions), ["000300.SH"]], names=["datetime", "instrument"]))
        if benchmark_values.close.iloc[-6:].notna().all():
            output["csi300_ret_5"] = _build_benchmark_features(benchmark_values).loc[pd.Timestamp(decision), "csi300_ret_5"]
    if not market.empty:
        market_values = market.set_index(["trade_date", "instrument"]).rename_axis(["datetime", "instrument"]).assign(limit_up=np.nan)
        breadth = _build_market_features(market_values)
        if pd.Timestamp(decision) in breadth.index:
            output["market_up_ratio"] = breadth.loc[pd.Timestamp(decision), "market_up_ratio"]
    breturn = output.csi300_ret_5.iloc[0] if len(output) else np.nan
    information = build_economic_daily_information_v1(decision_date=decision,
        candidates=[{"instrument": symbol, "selection_rank": int(rank)} for symbol, rank in zip(symbols, ranks, strict=True)],
        calendar=sessions, panel=info_panel, benchmark_return_5d=None if pd.isna(breturn) else float(breturn), benchmark_as_of=decision,
        price_basis="D_ADJUSTED", volume_basis="RAW_SAME_UNIT")
    unknown = []
    for index, row in zip(output.index, information["rows"], strict=True):
        for name, value in row["values"].items():
            output.loc[index, name] = np.nan if value is None else value
        unknown.append({"instrument": row["instrument"], "fields": {name: row["unknown_reasons"].get(name,
            missing_bars.get(row["instrument"], "D_INPUT_OR_WINDOW_UNKNOWN")) for name in D_FEATURES if pd.isna(output.loc[index, name])}})
    if np.isinf(output.loc[:, D_FEATURES].to_numpy(dtype=float)).any():
        _fail("daily core derived values are nonfinite")
    receipt = {"schema_version": SEMANTICS["schema_version"], "semantics_sha256": sha(SEMANTICS),
        "input_sha256": input_hash, "feature_sha256": sha(_records(output)), "information_sha256": information["information_sha256"],
        "decision_date": decision.isoformat(), "target_date": target.isoformat(), "candidate_count": len(output),
        "unknown_fields": unknown, "status": "NO_CANDIDATES" if output.empty else "COMPUTED",
        "source_evidence": "COMPUTATION_ONLY", "old_training_parity": "UNPROVEN",
        "deployable": False, "outcomes_read": False, "new_native_receipt": False}
    return output, receipt
