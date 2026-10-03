"""Bounded D input reads, shared by one-day and batch consumers, not native capture."""
from datetime import date, datetime, timezone
import math
from numbers import Real
import re
import time

import pandas as pd

from backend.db.pg_pool import get_conn
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import RAW_FIELDS, build_economic_daily_feature_core_v1
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError


def _invalid(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ECONOMIC_DAILY_SOURCE_INVALID")


def _keys(frame, allowed, *, key=("trade_date", "instrument"), market=False):
    if frame.columns.is_unique is False:
        _invalid("daily source contains duplicate columns")
    frame = frame.copy()
    days = pd.to_datetime(frame.trade_date, errors="coerce")
    if days.isna().any() or days.dt.tz is not None or not days.eq(days.dt.normalize()).all():
        _invalid("daily source dates are malformed")
    frame["trade_date"] = days
    if frame.duplicated(list(key)).any():
        _invalid("daily source contains duplicate keys")
    if market:
        valid = days.isin(pd.DatetimeIndex(allowed)) & frame.instrument.map(
            lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ)", value) is not None)
        if not valid.all():
            _invalid("daily source market contains foreign dates or instruments")
    elif not set(map(tuple, frame.loc[:, ["trade_date", "instrument"]].to_numpy())).issubset(allowed):
        _invalid("daily source contains foreign dates or instruments")
    return frame


def _packets(packets):
    if not isinstance(packets, (tuple, list)) or not 1 <= len(packets) <= 20:
        _invalid("daily source needs one to twenty explicit day packets")
    for packet in packets:
        if not isinstance(packet, dict) or set(packet) != {"candidates", "calendar", "component_roles", "terminal_weights"}:
            _invalid("daily source packet schema differs")
        calendar, roster = packet["calendar"], packet["candidates"]
        if (not isinstance(calendar, (list, tuple)) or len(calendar) != 21
                or any(type(day) is not date for day in calendar) or list(calendar) != sorted(set(calendar))
                or not isinstance(roster, pd.DataFrame) or len(roster) > 20 or not roster.columns.is_unique
                or not set(KEY).issubset(roster.columns)
                or not roster.instrument.map(lambda value: isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", value) is not None).all()
                or roster.instrument.duplicated().any()):
            _invalid("daily source roster or calendar exceeds its explicit contract")
        for column, day in ((KEY[0], calendar[-2]), (KEY[1], calendar[-1])):
            values = pd.to_datetime(roster[column], errors="coerce")
            if values.dt.tz is not None or not values.eq(pd.Timestamp(day)).all():
                _invalid("daily source D/T differs from its original candidates")
    return packets


class EconomicCommonCoreReadonlyDailySourceV1:
    """No model, file, publication, selection or outcome operations."""

    def __init__(self, *, connection_context_factory=None, budget_seconds=30., monotonic=None):
        if (isinstance(budget_seconds, bool) or not isinstance(budget_seconds, Real)
                or not math.isfinite(budget_seconds) or not 0 < budget_seconds <= 30):
            _invalid("daily source time budget must be finite and at most thirty seconds")
        self._connection = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))
        self._budget, self._clock = float(budget_seconds), monotonic or time.monotonic

    def load_day(self, **packet):
        return self.load_batch(packets=[packet])[0]

    def load_batch(self, *, packets):
        packets = _packets(packets)
        deadline = self._clock() + self._budget
        pair_set = {(pd.Timestamp(day), symbol) for packet in packets for day in packet["calendar"][:-1]
                    for symbol in packet["candidates"].instrument}
        pairs = sorted(pair_set)
        benchmarks = sorted({pd.Timestamp(day) for packet in packets for day in packet["calendar"][:-1]})
        markets = sorted({pd.Timestamp(day) for packet in packets for day in packet["calendar"][-3:-1]})
        expected_calendar = {(index, pd.Timestamp(day)) for index, packet in enumerate(packets) for day in packet["calendar"]}
        queries = []
        with self._connection() as conn:
            try:
                conn.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                with conn.cursor() as cursor:
                    def read(phase, sql, parameters, columns, maximum):
                        if phase != "calendar" and not pairs:
                            return pd.DataFrame(columns=columns)
                        began = self._clock()
                        remaining = deadline - began
                        if remaining <= 0:
                            _invalid("daily source total time budget exceeded")
                        cursor.execute("SET LOCAL statement_timeout = %s", (max(1, min(15000, int(remaining*1000))),))
                        cursor.execute(sql, (*parameters, maximum+1))
                        rows = cursor.fetchall()
                        names = [item.name for item in cursor.description]
                        if len(rows) > maximum or names != list(columns) or self._clock() > deadline:
                            _invalid("daily source row/schema/time budget differs")
                        queries.append({"phase": phase, "rows": len(rows), "seconds": round(self._clock()-began, 6)})
                        return pd.DataFrame(rows, columns=names)

                    calendar = read("calendar", """SELECT request.packet, cal.cal_date AS trade_date
                        FROM unnest(%s::integer[], %s::date[], %s::date[]) AS request(packet, first_day, target)
                        JOIN market.trading_calendar cal ON cal.cal_date BETWEEN request.first_day AND request.target
                        WHERE cal.is_trading=TRUE ORDER BY request.packet,cal.cal_date LIMIT %s""",
                        (list(range(len(packets))), [p["calendar"][0] for p in packets], [p["calendar"][-1] for p in packets]),
                        ("packet", "trade_date"), len(expected_calendar))
                    calendar["trade_date"] = pd.to_datetime(calendar.trade_date, errors="coerce")
                    if (not calendar.packet.map(lambda value: type(value) is int).all()
                            or calendar.duplicated(["packet", "trade_date"]).any()
                            or set(map(tuple, calendar.to_numpy())) != expected_calendar):
                        _invalid("daily source authoritative twenty sessions or next T differs")
                    request_dates, request_symbols = [day.date() for day, _ in pairs], [symbol for _, symbol in pairs]
                    first, last = benchmarks[0].date(), benchmarks[-1].date()
                    raw = read("raw", """SELECT price.trade_date,price.ts_code AS instrument,
                        price.open_li,price.high_li,price.low_li,price.close_li,price.volume_hand,price.amount_li,adj.adj_factor
                        FROM market.kline_daily_raw price
                        JOIN unnest(%s::date[],%s::text[]) request(day,instrument)
                          ON request.day=price.trade_date AND request.instrument=price.ts_code
                        LEFT JOIN market.adj_factor adj ON adj.trade_date=price.trade_date AND adj.ts_code=price.ts_code
                          AND adj.trade_date BETWEEN %s AND %s
                        WHERE price.trade_date BETWEEN %s AND %s ORDER BY price.trade_date,price.ts_code LIMIT %s""",
                        (request_dates, request_symbols, first, last, first, last), ("trade_date", "instrument", *RAW_FIELDS), len(pairs))
                    benchmark = read("benchmark", """SELECT trade_date,ts_code AS instrument,close FROM market.index_daily
                        WHERE ts_code='000300.SH' AND trade_date=ANY(%s::date[]) AND trade_date BETWEEN %s AND %s
                        ORDER BY trade_date LIMIT %s""", ([day.date() for day in benchmarks],first,last),
                        ("trade_date", "instrument", "close"), len(benchmarks))
                    market = read("market", """SELECT price.trade_date,price.ts_code AS instrument,
                        price.close_li/1000.0*adj.adj_factor AS close
                        FROM market.kline_daily_raw price JOIN market.stock_basic stock ON stock.ts_code=price.ts_code
                        JOIN market.sector_data eligible ON eligible.trade_date=price.trade_date AND eligible.ts_code=price.ts_code
                        LEFT JOIN market.adj_factor adj ON adj.trade_date=price.trade_date AND adj.ts_code=price.ts_code
                          AND adj.trade_date BETWEEN %s AND %s
                        WHERE price.trade_date=ANY(%s::date[]) AND price.trade_date BETWEEN %s AND %s
                          AND stock.list_date<=price.trade_date AND (stock.delist_date IS NULL OR stock.delist_date>price.trade_date)
                          AND (price.ts_code LIKE '%%.SH' OR price.ts_code LIKE '%%.SZ')
                        ORDER BY price.trade_date,price.ts_code LIMIT %s""",
                        (markets[0].date(),markets[-1].date(),[day.date() for day in markets],markets[0].date(),markets[-1].date()),
                        ("trade_date", "instrument", "close"), len(markets)*10000)
                    suspend = read("suspend", """SELECT state.trade_date,state.ts_code AS instrument,state.suspend_type,state.suspend_timing
                        FROM market.suspend_d state JOIN unnest(%s::date[],%s::text[]) request(day,instrument)
                          ON request.day=state.trade_date AND request.instrument=state.ts_code
                        WHERE state.trade_date BETWEEN %s AND %s ORDER BY state.trade_date,state.ts_code,state.suspend_type LIMIT %s""",
                        (request_dates,request_symbols,first,last), ("trade_date", "instrument", "suspend_type", "suspend_timing"), len(pairs)*2)
            except AdvisoryModelFirstError:
                raise
            except Exception as exc:
                raise AdvisoryModelFirstError("daily source read-only query failed", reason_code="ADVISORY_ECONOMIC_DAILY_SOURCE_QUERY_FAILED",
                    context={"error_type": type(exc).__name__, "completed_queries": queries}) from exc
            finally:
                try:
                    conn.rollback()
                except Exception as exc:
                    raise AdvisoryModelFirstError("daily source rollback failed", reason_code="ADVISORY_ECONOMIC_DAILY_SOURCE_QUERY_FAILED",
                        context={"phase": "rollback", "error_type": type(exc).__name__}) from exc
        raw = _keys(raw, pair_set)
        benchmark = _keys(benchmark, {(day,"000300.SH") for day in benchmarks})
        market = _keys(market, markets, market=True)
        suspend = _keys(suspend, pair_set, key=("trade_date", "instrument", "suspend_type"))
        results = []
        read_at = datetime.now(timezone.utc).isoformat()
        for packet in packets:
            if self._clock() > deadline:
                _invalid("daily source total time budget exceeded")
            sessions = pd.DatetimeIndex(packet["calendar"][:-1])
            symbols = packet["candidates"].instrument.tolist()
            frame, receipt = build_economic_daily_feature_core_v1(**packet,
                raw_daily=raw.loc[raw.trade_date.isin(sessions) & raw.instrument.isin(symbols)].copy(),
                benchmark_daily=benchmark.loc[benchmark.trade_date.isin(sessions)].copy(),
                market_daily=market.loc[market.trade_date.isin(sessions[-2:])].copy(),
                suspend_rows=suspend.loc[suspend.trade_date.isin(sessions) & suspend.instrument.isin(symbols)].copy())
            receipt["db_source"] = {"read_at": read_at, "isolation": "REPEATABLE READ", "readonly": True,
                "database_written": False, "native_capture": False, "evidence": "CURRENT_DB_READONLY_NOT_NATIVE_CAPTURE",
                "select_count": len(queries), "queries": queries, "batch_packet_count": len(packets)}
            results.append((frame,receipt))
        if self._clock() > deadline:
            _invalid("daily source total time budget exceeded")
        return results
