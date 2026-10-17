"""D-only nine-field DB adapter; three SELECTs share one readonly batch snapshot."""
from datetime import datetime, time, timezone
import json
import re
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import (
    FEATURES, KEY, ROSTER, SEMANTICS, _day, _integer, _number, _records, _text, build_generic_daily_price_input_v1,
)
from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

META = {"package_id", "run_id", "list_version_id", "universe_identity"}
RAW_COLUMNS = ("anchor_date", "trade_date", "instrument", "open_li", "high_li", "low_li", "close_li",
               "volume_hand", "adj_factor", "anchor_factor")
PRICES = ("open_li", "high_li", "low_li", "close_li")
SOURCE = "CURRENT_DATABASE_NON_VINTAGE"


def _fail(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_GENERIC_DAILY_DB_INPUT_INVALID")


def _packet(value):
    if not isinstance(value, dict) or set(value) != {"decision_date", "target_date", "candidates", "metadata"}:
        _fail("generic DB packet schema differs")
    d, t = _day(value["decision_date"]), _day(value["target_date"])
    roster, metadata = value["candidates"], value["metadata"]
    if (d >= t or not isinstance(roster, pd.DataFrame) or not roster.columns.is_unique
            or set(roster.columns) != set(ROSTER) or len(roster) > 50
            or not isinstance(metadata, dict) or set(metadata) != META):
        _fail("generic DB original packet dates, population or metadata differs")
    for key in ("package_id", "run_id", "list_version_id"):
        _text(metadata[key])
    if metadata["universe_identity"] is not None and not isinstance(metadata["universe_identity"], (str, dict)):
        _fail("generic DB pool metadata is not a declared identity or unknown")
    if isinstance(metadata["universe_identity"], str):
        _text(metadata["universe_identity"])
    try:
        encoded = json.dumps(metadata, ensure_ascii=False, allow_nan=False)
    except (ValueError, TypeError):
        _fail("generic DB metadata is not finite JSON")
    if len(encoded.encode("utf-8")) > 65536:
        _fail("generic DB metadata exceeds its budget")
    roster = roster.copy(deep=True)
    for column, day in zip(KEY[:2], (d, t), strict=True):
        if not roster[column].map(_day).eq(day).all():
            _fail("generic DB roster D/T differs")
        roster[column] = pd.Timestamp(day)
    if (not roster.selection_effective_rank.map(_integer).all()
            or not roster.candidate_group_size.map(_integer).all()
            or roster.selection_effective_rank.tolist() != list(range(1, len(roster)+1))
            or not roster.candidate_group_size.eq(len(roster)).all()
            or roster.instrument.duplicated().any()
            or not roster.instrument.map(lambda v: isinstance(v, str) and re.fullmatch(r"\d{6}\.(SH|SZ|BJ)", v) is not None).all()):
        _fail("generic DB original keys, rank/order or group differs")
    return dict(decision_date=d, target_date=t, candidates=roster, metadata=json.loads(encoded))


def _frame(cursor, sql, params, columns, maximum):
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    if len(rows) > maximum:
        _fail("generic DB query exceeds its exact requested-row budget")
    try:
        return pd.DataFrame(rows, columns=columns)
    except ValueError:
        _fail("generic DB query columns differ from their contract")


def _keys(frame, columns, expected):
    if frame.duplicated(list(columns)).any() or not set(map(tuple, frame.loc[:, columns].to_numpy())).issubset(expected):
        _fail("generic DB returned duplicate, future or foreign keys")


def _context(packet, *, clock):
    return {**packet["metadata"], "source_evidence": SOURCE, "price_basis": "D_ADJUSTED_CNY", "volume_basis": "RAW_SHARES",
            "source_visible_through": clock, "benchmark_visible_through": clock}


def _unread(packet, status):
    features = packet["candidates"].copy(deep=True)
    for name in FEATURES:
        features[name] = np.nan
    return features, dict(schema_version=SEMANTICS["schema_version"], semantics_sha256=sha(SEMANTICS), status=status,
        decision_date=packet["decision_date"].isoformat(), target_date=packet["target_date"].isoformat(),
        candidate_count=len(features), candidate_roster_sha256=sha(_records(packet["candidates"])),
        context=_context(packet, clock=None), query_count=0, calendar_verified=False, source_evidence=SOURCE,
        known_counts=dict.fromkeys(FEATURES, 0), unknown_counts=dict.fromkeys(FEATURES, len(features)),
        fit_count=0, outcomes_read=False, database_write=False, new_native_receipt=False, qualification_rechecked=False)


class GenericDailyReadonlyDBInputV1:
    def __init__(self, *, session_factory=None, now=None):
        self._session = session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30.)))
        self._now = now or (lambda: datetime.now(timezone.utc))

    def load_day(self, **packet):
        return self.load_batch(packets=[packet])[0]

    def load_batch(self, *, packets):
        if not isinstance(packets, (list, tuple)) or len(packets) > 20:
            _fail("generic DB batch needs zero to twenty bounded original packets")
        values = [_packet(packet) for packet in packets]
        started = self._now()
        if not isinstance(started, datetime) or started.utcoffset() is None:
            _fail("generic DB reader requires a real aware consumption clock")
        local = started.astimezone(ZoneInfo("Asia/Shanghai"))
        if any(packet["decision_date"] > local.date() for packet in values):
            _fail("generic DB reader cannot consume a future decision date")
        if len({(p["decision_date"], p["target_date"]) for p in values}) != len(values):
            _fail("generic DB batch contains duplicate original dates")
        active = [index for index, p in enumerate(values) if len(p["candidates"])
                  and not (p["decision_date"] == local.date() and local.time() < time(15))]
        results = [_unread(p, "NO_CANDIDATES" if p["candidates"].empty else "DEFERRED_D_NOT_CLOSED") for p in values]
        if not active:
            return results
        session = self._session()
        try:
            with session.connection() as connection:
                with connection.cursor() as cursor:
                    calendar = _frame(cursor, """/* advisory_generic_calendar */
                        SELECT request.packet, history.cal_date AS trade_date, upcoming.cal_date AS target_date
                        FROM unnest(%s::integer[],%s::date[]) request(packet,decision)
                        CROSS JOIN LATERAL (SELECT cal_date FROM market.trading_calendar
                            WHERE is_trading=TRUE AND cal_date<=request.decision ORDER BY cal_date DESC LIMIT 20) history
                        LEFT JOIN LATERAL (SELECT cal_date FROM market.trading_calendar
                            WHERE is_trading=TRUE AND cal_date>request.decision ORDER BY cal_date LIMIT 1) upcoming ON TRUE
                        ORDER BY request.packet,history.cal_date LIMIT %s""",
                        (active, [values[i]["decision_date"] for i in active], 20*len(active)+1),
                        ("packet", "trade_date", "target_date"), 20*len(active))
                    if not calendar.packet.map(_integer).all() or not set(calendar.packet).issubset(active):
                        _fail("generic DB calendar has foreign packets")
                    for column in ("trade_date", "target_date"):
                        calendar[column] = calendar[column].map(_day)
                    if calendar.duplicated(["packet", "trade_date"]).any():
                        _fail("generic DB calendar repeats original sessions")
                    calendars = {}
                    for index in active:
                        d, t = values[index]["decision_date"], values[index]["target_date"]
                        rows = calendar.loc[calendar.packet.eq(index)]
                        dates = tuple(rows.trade_date)
                        if (len(dates) != 20 or list(dates) != sorted(set(dates)) or dates[-1] != d
                                or not rows.target_date.eq(t).all()):
                            _fail("generic DB authoritative twenty D sessions or immediate T differs")
                        calendars[index] = (*dates, t)
                    requests = sorted({(values[i]["decision_date"], day, symbol)
                        for i in active for day in calendars[i][:-1] for symbol in values[i]["candidates"].instrument})
                    raw = _frame(cursor, """/* advisory_generic_raw */
                        SELECT request.anchor AS anchor_date,price.trade_date,price.ts_code AS instrument,
                            price.open_li,price.high_li,price.low_li,price.close_li,price.volume_hand,
                            adjustment.adj_factor,anchor.adj_factor AS anchor_factor
                        FROM unnest(%s::date[],%s::date[],%s::text[]) request(anchor,day,instrument)
                        JOIN market.kline_daily_raw price ON price.trade_date=request.day AND price.ts_code=request.instrument
                        LEFT JOIN market.adj_factor adjustment ON adjustment.trade_date=request.day AND adjustment.ts_code=request.instrument
                        LEFT JOIN market.adj_factor anchor ON anchor.trade_date=request.anchor AND anchor.ts_code=request.instrument
                        ORDER BY request.anchor,price.trade_date,price.ts_code LIMIT %s""",
                        ([r[0] for r in requests], [r[1] for r in requests], [r[2] for r in requests], len(requests)+1),
                        RAW_COLUMNS, len(requests))
                    for column in ("anchor_date", "trade_date"):
                        raw[column] = raw[column].map(_day)
                    _keys(raw, ("anchor_date", "trade_date", "instrument"), set(requests))
                    for column in (*PRICES, "adj_factor", "anchor_factor", "volume_hand"):
                        raw[column] = raw[column].map(lambda v, c=column: _number(v, positive=c != "volume_hand", nonnegative=c == "volume_hand")).astype(float)
                    if (raw.high_li.lt(raw.low_li).any() or raw.open_li.lt(raw.low_li).any() or raw.open_li.gt(raw.high_li).any()
                            or raw.close_li.lt(raw.low_li).any() or raw.close_li.gt(raw.high_li).any()):
                        _fail("generic DB raw OHLC values contradict their range")
                    days = sorted({day for i in active for day in calendars[i][:-1]})
                    benchmark = _frame(cursor, """/* advisory_generic_benchmark */
                        SELECT trade_date,ts_code AS instrument,close FROM market.index_daily
                        WHERE ts_code='000300.SH' AND trade_date=ANY(%s::date[])
                        ORDER BY trade_date LIMIT %s""", (days, len(days)+1), ("trade_date", "instrument", "close"), len(days))
                    benchmark["trade_date"] = benchmark.trade_date.map(_day)
                    _keys(benchmark, ("trade_date", "instrument"), {(day, "000300.SH") for day in days})
                    benchmark["close"] = benchmark.close.map(lambda v: _number(v, positive=True)).astype(float)
                    session.budget.check()
            completed = self._now()
            if not isinstance(completed, datetime) or completed.utcoffset() is None or completed < started:
                _fail("generic DB read completion clock is invalid")
            for index in active:
                p = values[index]
                subset = raw.loc[raw.anchor_date.eq(p["decision_date"])].copy()
                panel = subset.loc[:, ["trade_date", "instrument"]].copy()
                for column in PRICES:
                    normalized = subset[column]/1000.*subset.adj_factor/subset.anchor_factor
                    panel[column.removesuffix("_li")] = normalized
                panel["volume"] = subset.volume_hand*100.
                base = benchmark.loc[benchmark.trade_date.isin(calendars[index][:-1])].copy()
                features, receipt = build_generic_daily_price_input_v1(candidates=p["candidates"],
                    calendar=calendars[index], panel=panel, benchmark_daily=base,
                    market_state=dict(trade_date=p["decision_date"], market_up_ratio=None,
                        market_definition_id=None, visible_through=None), source_context=_context(p, clock=p["decision_date"]))
                raw_hash_input = subset.copy()
                for column in ("anchor_date", "trade_date"):
                    raw_hash_input[column] = pd.to_datetime(raw_hash_input[column])
                receipt.update(source=SOURCE, source_started_at=started.isoformat(), source_completed_at=completed.isoformat(),
                    source_tables=("market.kline_daily_raw", "market.adj_factor", "market.index_daily", "market.trading_calendar"),
                    raw_price_unit_divisor=1000., raw_volume_unit_multiplier=100., price_adjustment="EXACT_SESSION_FACTOR_OVER_EXACT_D_FACTOR",
                    raw_slice_sha256=sha(_records(raw_hash_input)), query_count=3, calendar_verified=True,
                    shared_batch_query_count=3, market_reason="MARKET_DEFINITION_NOT_REQUESTED",
                    fit_count=0, database_write=False, historical_vintage_proven=False, new_native_receipt=False,
                    eod_ingestion_completeness_attested=False, qualification_rechecked=False)
                results[index] = features, receipt
                session.budget.check()
            return results
        except AdvisoryModelFirstError:
            raise
        except Exception as error:
            raise AdvisoryModelFirstError(f"generic readonly DB input unavailable: {type(error).__name__}",
                reason_code="ADVISORY_GENERIC_DAILY_DB_INPUT_UNAVAILABLE") from error
        finally:
            session.close()
