"""D-bounded information rows over original rosters; no labels or native claims."""
from datetime import date, timedelta
from decimal import Decimal
import hashlib
from numbers import Real
import re

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.economic_daily_information_v1 import FEATURES, build_economic_daily_information_v1
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingConfigurationV1
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail, _frame
from backend.services.advisory_model_first.economic_entry_sources import EconomicEntryReadonlyDailySource
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _numeric(series, *, positive=False, nonnegative=False):
    def convert(value):
        if value is None or value is pd.NA:
            return np.nan
        if isinstance(value, (bool, np.bool_)) or not isinstance(value, (Real, Decimal)):
            _fail("information raw source has a malformed numeric scalar")
        value = float(value)
        if np.isnan(value):
            return value
        if not np.isfinite(value) or (positive and value <= 0) or (nonnegative and value < 0):
            _fail("information raw source has an invalid finite value")
        return value
    return series.map(convert)


def _roster(candidates):
    if len(candidates) > 100000:
        _fail("information roster exceeds its row budget")
    roster = _frame(candidates, KEY, set(KEY) | {"selection_effective_rank"})
    ranks = roster.selection_effective_rank
    if (not ranks.map(lambda x: isinstance(x, (int, np.integer)) and not isinstance(x, (bool, np.bool_))).all()
            or not ranks.between(1, 20).all() or roster.duplicated([KEY[0], "selection_effective_rank"]).any()
            or roster.duplicated([KEY[0], "instrument"]).any()
            or not roster.instrument.map(lambda x: re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", x) is not None).all()
            or not (roster[KEY[0]] < roster[KEY[1]]).all()):
        _fail("information candidate identity/rank or D/T clock differs")
    return roster


def build_information_feature_rows_v4(*, candidates, raw_daily, calendar, benchmark_daily):
    if len(raw_daily) > 500000 or len(benchmark_daily) > 10000:
        _fail("information source exceeds explicit row budgets")
    roster = _roster(candidates)
    if not isinstance(calendar, (list, tuple)) or any(type(x) is not date for x in calendar) or list(calendar) != sorted(set(calendar)):
        _fail("information calendar must be exact ordered date objects")
    required = {"trade_date", "instrument", "high_li", "low_li", "close_li", "volume_hand", "adj_factor"}
    raw = _frame(raw_daily, ["trade_date", "instrument"], required)
    benchmark = _frame(benchmark_daily, ["trade_date", "instrument"], {"trade_date", "instrument", "close"})
    raw["trade_date"] = pd.to_datetime(raw.trade_date)
    benchmark["trade_date"] = pd.to_datetime(benchmark.trade_date)
    if (not set(raw.instrument).issubset(set(roster.instrument)) or not benchmark.instrument.eq("000300.SH").all()
            or not set(raw.trade_date.dt.date).issubset(set(calendar)) or not set(benchmark.trade_date.dt.date).issubset(set(calendar))):
        _fail("information raw inputs leave their exact source roster/calendar")
    if not roster.empty and (not calendar or calendar[-1] > roster[KEY[0]].max().date()):
        _fail("information source includes future sessions")
    raw = raw.sort_values(["trade_date", "instrument"])
    benchmark = benchmark.sort_values(["trade_date", "instrument"])
    def records(frame):
        return [{name: value.isoformat() if isinstance(value, pd.Timestamp) else None if pd.isna(value) else value
                 for name,value in row.items()} for row in frame.to_dict("records")]
    def stream_hash(frame):
        # Bounded memory: never materialize 500k dicts plus a second giant JSON.
        digest = hashlib.sha256()
        digest.update(sha(list(frame.columns)).encode())
        for values in frame.itertuples(index=False,name=None):
            row={name:value.isoformat() if isinstance(value,pd.Timestamp) else None if pd.isna(value) else str(value) if isinstance(value,Decimal) else value
                 for name,value in zip(frame.columns,values,strict=True)}
            digest.update(sha(row).encode())
            digest.update(b"\n")
        return digest.hexdigest()
    raw_identity = sha({"algorithm":"ROW_CANONICAL_SHA256_STREAM_V1", "raw":stream_hash(raw),
        "benchmark":stream_hash(benchmark),"calendar":[day.isoformat() for day in calendar]})
    for name in ("high_li", "low_li", "close_li", "adj_factor"):
        raw[name] = _numeric(raw[name], positive=True)
    raw["volume_hand"] = _numeric(raw.volume_hand, nonnegative=True)
    benchmark["close"] = _numeric(benchmark.close, positive=True)
    raw = raw.rename(columns={"trade_date": "datetime"}).set_index(["datetime", "instrument"])
    benchmark = benchmark.set_index("trade_date").close
    rows, packs = [], []
    for decision, group in roster.groupby(KEY[0], sort=False):
        if group.selection_effective_rank.tolist() != sorted(group.selection_effective_rank):
            _fail("information source cannot reorder original candidates")
        day = decision.date()
        if day not in calendar or calendar.index(day) < 19:
            _fail("information source lacks authoritative twenty-session warmup")
        position = calendar.index(day)
        sessions = calendar[position-19:position+1]
        symbols = group.instrument.tolist()
        # Slice the indexed 20-day window before symbol filtering, rather than
        # scanning up to 500k raw rows for every one of the 386 first-study Ds.
        window = raw.loc[(slice(pd.Timestamp(sessions[0]), decision), slice(None)), :]
        part = window.loc[window.index.get_level_values("instrument").isin(symbols)].copy()
        base = part.loc[part.index.get_level_values("datetime") == decision, "adj_factor"].droplevel("datetime")
        factor = part.adj_factor / part.index.get_level_values("instrument").map(base)
        panel = pd.DataFrame(index=part.index)
        for name in ("high", "low", "close"):
            panel[name] = part[name + "_li"] / 1000 * factor
        panel["volume"] = part.volume_hand * 100  # RAW shares, never adjustment-scaled volume.
        bvalues = benchmark.reindex(pd.DatetimeIndex(sessions[-6:]))
        breturn = float(bvalues.iloc[-1]/bvalues.iloc[0]-1) if bvalues.notna().all() else None
        packed = build_economic_daily_information_v1(decision_date=day,
            candidates=[{"instrument": row.instrument, "selection_rank": int(row.selection_effective_rank)} for row in group.itertuples()],
            calendar=tuple(sessions), panel=panel, benchmark_return_5d=breturn, benchmark_as_of=day,
            price_basis="D_ADJUSTED", volume_basis="RAW_SAME_UNIT")
        packs.append({"D": day.isoformat(), "information_sha256": packed["information_sha256"]})
        for original, result in zip(group.to_dict("records"), packed["rows"], strict=True):
            rows.append({**{name: original[name] for name in KEY}, **result["values"],
                "information_visible_through": decision, "information_day_sha256": packed["information_sha256"],
                "unknown_reasons": result["unknown_reasons"]})
    frame = pd.DataFrame(rows, columns=KEY + list(FEATURES) + ["information_visible_through", "information_day_sha256", "unknown_reasons"])
    receipt = {"schema_version": "economic_entry_information_source_v4", "day_hashes": packs,
        "original_roster_sha256": sha(records(roster.loc[:, KEY+["selection_effective_rank"]])),
        "raw_source_sha256": raw_identity,
        "candidate_count": len(roster), "source_evidence": "CURRENT_DB_HISTORICAL_NON_VINTAGE",
        "decision_use": "NAVIGATION_ONLY", "deployable": False, "outcomes_read": False,
        "information_semantics": "D_ADJUSTED_RAW_VOLUME_SAME_UNIT", "new_native_receipt": False}
    return frame, {**receipt, "information_content_sha256": sha(receipt)}


class EconomicEntryReadonlyInformationSourceV4(EconomicEntryReadonlyDailySource):
    """Training-only, explicitly configured consumed window. No global QE dates."""
    def load(self, *, candidates, configuration):
        configuration = EconomicEntryTrainingConfigurationV1.model_validate(configuration.model_dump())
        roster = _roster(candidates)
        if roster.empty:
            _fail("training information needs a nonempty original candidate roster")
        dates = roster[KEY[0]].dt.date
        if len(roster) > configuration.resource_max_rows or dates.min() < configuration.train_start or dates.max() > configuration.test_end:
            _fail("information read leaves explicitly registered development decisions")
        start, end = dates.min(), dates.max()
        symbols = sorted(set(roster.instrument))
        with self._connection() as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SET LOCAL statement_timeout = %s", (15000,))
                    cursor.execute("""SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE
                        AND cal_date BETWEEN %s AND %s ORDER BY cal_date LIMIT 10001""", (start-timedelta(days=60), end))
                    calendar = [row[0] for row in cursor.fetchall()]
                    if len(calendar)>10000 or start not in calendar or calendar.index(start)<19:
                        _fail("information DB calendar lacks bounded warmup")
                    calendar = calendar[calendar.index(start)-19:]
                    cursor.execute("""SELECT p.trade_date,p.ts_code AS instrument,p.high_li,p.low_li,p.close_li,p.volume_hand,a.adj_factor
                        FROM market.kline_daily_raw p LEFT JOIN market.adj_factor a
                          ON a.ts_code=p.ts_code AND a.trade_date=p.trade_date AND a.trade_date BETWEEN %s AND %s
                        WHERE p.ts_code=ANY(%s) AND p.trade_date BETWEEN %s AND %s
                        ORDER BY p.trade_date,p.ts_code LIMIT 500001""", (calendar[0],end,symbols,calendar[0],end))
                    raw = pd.DataFrame(cursor.fetchall(), columns=[item.name for item in cursor.description])
                    cursor.execute("""SELECT trade_date,ts_code AS instrument,close FROM market.index_daily
                        WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date LIMIT 10001""", (calendar[0],end))
                    benchmark = pd.DataFrame(cursor.fetchall(), columns=[item.name for item in cursor.description])
            finally:
                conn.rollback()
        return build_information_feature_rows_v4(candidates=roster,raw_daily=raw,calendar=calendar,benchmark_daily=benchmark)
