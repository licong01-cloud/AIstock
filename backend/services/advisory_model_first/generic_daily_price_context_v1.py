"""Raw-tick projection and D-only readonly legal coordinates; no activation."""
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, localcontext
from datetime import date, datetime, time as wall_time, timezone
import time
from zoneinfo import ZoneInfo

import pandas as pd

from backend.services.advisory_model_first.generic_daily_price_input_v1 import FEATURES, _day, _number
from backend.services.advisory_model_first.generic_price_5td_inference_v1 import query_price_nodes_v1
from backend.services.advisory_model_first.generic_price_set_consumer_v1 import (
    LoadedGenericPriceSetModelV1, _fail, _fitted, project_generic_price_sets_v1,
)
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.services.advisory_model_first.price_range_regulatory import resolve_regulatory_price_range
from backend.services.advisory_model_first.realtime_feature_source import (
    PriceRangeRealtimeContext, _board_type, _project_target_st, _target_raw_price_multiplier,
)
from backend.services.canonical_equity_pit import CANONICAL_PIT_UNIVERSE_KEY

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
            _fail("generic raw projection exceeded its deadline without publishing a partial batch", "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
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
                _fail("generic raw complete grid exceeds its upfront node budget", "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
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
            _fail("generic raw projection exceeded its deadline without publishing a partial batch", "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
    if monotonic() > deadline:
        _fail("generic raw projection exceeded its deadline without publishing a partial batch", "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
    result.update(schema_version="generic_daily_raw_price_projection_v1", advice=advice,
        input_sha256=sha(dict(generic_input_sha256=result["input_sha256"], raw_coordinates=coordinates)),
        raw_price_basis="T_RAW_CNY", coordinate_mapping="RAW_DECIMAL_TICK_TO_D_ANCHOR_NO_REGRID")
    return result


def _bounded_rows(cursor, sql, params, maximum, width):
    cursor.execute(sql, params)
    rows = cursor.fetchall()
    if len(rows) > maximum or any(len(row) != width for row in rows):
        _fail("generic legal coordinate rows exceed their bounded declared domain")
    return rows


def _row_identity(rows):
    return [[value.isoformat() if isinstance(value, date) else str(value) if isinstance(value, Decimal) else value
             for value in row] for row in rows]


class GenericDailyPriceContextV1:
    """Read only D-visible legal coordinates in the caller's pinned snapshot.

    F-794/F-795: absence is interpreted only under ready current PIT coverage;
    this is not a claim about the original historical capture. No T quote/audit.
    """

    def __init__(self, *, read_session, now=None):
        self._session = read_session
        self._now = now or (lambda: datetime.now(timezone.utc))

    def load_batch(self, *, packets):
        if not isinstance(packets, (list, tuple)) or len(packets) > 20:
            _fail("generic legal coordinates need at most twenty original packets")
        now = self._now()
        if not isinstance(now, datetime) or now.utcoffset() is None:
            _fail("generic legal coordinate clock must be aware")
        local = now.astimezone(ZoneInfo("Asia/Shanghai"))
        requests, outputs = [], []
        for index, packet in enumerate(packets):
            d, t = _day(packet["decision_date"]), _day(packet["target_date"])
            if d >= t or d > local.date():
                _fail("generic legal coordinate original D/T is invalid")
            symbols = list(packet["candidates"].instrument)
            if len(symbols) > 50 or len(set(symbols)) != len(symbols):
                _fail("generic legal coordinate roster is duplicate or oversized")
            deferred = d == local.date() and local.time() < wall_time(15)
            outputs.append(dict(contexts={}, receipt=dict(source_evidence="CURRENT_DATABASE_NON_VINTAGE",
                decision_date=d.isoformat(), target_date=t.isoformat(), candidate_count=len(symbols),
                status="DEFERRED_D_NOT_CLOSED" if deferred else "NO_CANDIDATES" if not symbols else "READ",
                query_count=0, unavailable=[], historical_vintage_proven=False, outcomes_read=False,
                database_write=False, new_native_receipt=False)))
            if not deferred:
                requests.extend((index, d, t, symbol) for symbol in symbols)
        if not requests:
            return outputs
        indexes, days, targets, symbols = map(list, zip(*requests, strict=True))
        expected = {(index, symbol) for index, _d, _t, symbol in requests}
        with self._session.connection() as connection, connection.cursor() as cursor:
            raw = _bounded_rows(cursor, """/* advisory_generic_legal_base */
                SELECT request.packet,request.instrument,price.close_li,basic.list_date,
                    CASE WHEN basic.list_date IS NULL THEN NULL
                         WHEN basic.list_date < request.target-INTERVAL '14 days' THEN 99
                         ELSE (SELECT COUNT(*) FROM market.trading_calendar cal
                             WHERE cal.is_trading=TRUE AND cal.cal_date BETWEEN basic.list_date AND request.target) END,
                    adjustment.adj_factor
                FROM unnest(%s::integer[],%s::date[],%s::date[],%s::text[])
                    request(packet,decision,target,instrument)
                LEFT JOIN market.kline_daily_raw price ON price.trade_date=request.decision AND price.ts_code=request.instrument
                LEFT JOIN market.stock_basic basic ON basic.ts_code=request.instrument
                LEFT JOIN market.adj_factor adjustment ON adjustment.trade_date=request.decision AND adjustment.ts_code=request.instrument
                ORDER BY request.packet,request.instrument LIMIT %s""",
                (indexes, days, targets, symbols, len(requests)+1), len(requests), 6)
            st = _bounded_rows(cursor, """/* advisory_generic_legal_st */
                SELECT request.packet,request.instrument,event.event_kind,event.action_date,event.visible_date,
                    state.status,state.dirty,state.start_date,state.end_date
                FROM unnest(%s::integer[],%s::date[],%s::date[],%s::text[])
                    request(packet,decision,target,instrument)
                LEFT JOIN market.stock_universe_pit_state state ON state.universe_key=%s
                LEFT JOIN LATERAL (SELECT event_kind,action_date,COALESCE(source_pub_date,source_imp_date) visible_date
                    FROM market.stock_universe_pit_events
                    WHERE universe_key=%s AND ts_code=request.instrument AND event_kind IN ('st_negative','st_restore')
                      AND action_date<=request.target AND COALESCE(source_pub_date,source_imp_date)<=request.decision
                    ORDER BY action_date DESC,event_id DESC LIMIT 1) event ON TRUE
                ORDER BY request.packet,request.instrument LIMIT %s""",
                (indexes, days, targets, symbols, CANONICAL_PIT_UNIVERSE_KEY, CANONICAL_PIT_UNIVERSE_KEY,
                 len(requests)+1), len(requests), 9)
            actions = _bounded_rows(cursor, """/* advisory_generic_legal_actions */
                SELECT request.packet,request.instrument,action.end_date,action.ann_date,action.div_proc,
                    CASE WHEN action.div_proc='实施' AND action.imp_ann_date<=request.decision THEN action.stk_div END,
                    CASE WHEN action.div_proc='实施' AND action.imp_ann_date<=request.decision THEN action.stk_bo_rate END,
                    CASE WHEN action.div_proc='实施' AND action.imp_ann_date<=request.decision THEN action.stk_co_rate END,
                    CASE WHEN action.div_proc='实施' AND action.imp_ann_date<=request.decision THEN action.cash_div END,
                    CASE WHEN action.div_proc='实施' AND action.imp_ann_date<=request.decision THEN action.cash_div_tax END,
                    CASE WHEN action.imp_ann_date<=request.decision THEN action.imp_ann_date END
                FROM unnest(%s::integer[],%s::date[],%s::date[],%s::text[])
                    request(packet,decision,target,instrument)
                JOIN market.dividend action ON action.ts_code=request.instrument AND action.ex_date=request.target
                  AND (action.ann_date<=request.decision OR action.imp_ann_date<=request.decision)
                ORDER BY request.packet,request.instrument,action.end_date,action.ann_date LIMIT %s""",
                (indexes, days, targets, symbols, 64*len(requests)+1), 64*len(requests), 11)
            self._session.budget.check()
        tables = []
        for rows in (raw, st):
            mapping = {}
            for row in rows:
                key = tuple(row[:2])
                if type(row[0]) is not int or key not in expected or key in mapping:
                    _fail("generic legal coordinate query returned duplicate or foreign keys")
                mapping[key] = row
            if set(mapping) != expected:
                _fail("generic legal coordinate LEFT JOIN lost an original request key")
            tables.append(mapping)
        action_map, action_seen = {}, set()
        for row in actions:
            key = tuple(row[:2])
            if type(row[0]) is not int or key not in expected or tuple(row) in action_seen:
                _fail("generic D-visible action contains foreign or duplicate rows")
            action_seen.add(tuple(row))
            action_map.setdefault(key, []).append(tuple(row[1:]))
            if len(action_map[key]) > 64:
                _fail("generic D-visible actions exceed the per-original-stock budget")
        for index, d, t, symbol in requests:
            key = index, symbol
            base, event, action_rows = tables[0].get(key), tables[1].get(key), action_map.get(key, [])
            unavailable = None
            if base is None or base[2] is None or base[3] is None or base[4] is None:
                unavailable = "D_CLOSE_OR_LISTING_UNKNOWN"
            elif event is None or event[5] != "ready" or event[6] is not False or event[7] is None or event[8] is None:
                unavailable = "D_VISIBLE_ST_STATE_UNKNOWN"
            elif _day(event[7]) > d or _day(event[8]) < d:
                unavailable = "D_VISIBLE_ST_STATE_UNKNOWN"
            if unavailable is None:
                close = _number(base[2], positive=True)/1000.
                listed = _day(base[3])
                factor = _number(base[5], positive=True)
                if type(base[4]) is not int or base[4] < 1 or listed > t:
                    _fail("generic original listing or listed-session count contradicts T")
                if event[2] is not None and (event[2] not in {"st_negative", "st_restore"}
                        or _day(event[3]) > t or _day(event[4]) > d):
                    _fail("generic ST query returned an invalid or future event")
                if any((_day(row[9], nullable=True) or _day(row[2], nullable=True)) is None
                       or (_day(row[9], nullable=True) or _day(row[2], nullable=True)) > d for row in action_rows):
                    _fail("generic action query returned a future or unclocked action")
                implemented = [row for row in action_rows if row[3] == "实施" and row[9] is not None]
                pending = [row for row in action_rows if row not in implemented
                           and row[1] not in {value[1] for value in implemented}]
                if pending or implemented and factor is None:
                    unavailable = "D_VISIBLE_ACTION_IMPLEMENTATION_UNKNOWN"
                else:
                    if any(all(value is None for value in row[4:9])
                           or row[8] is None and _number(row[7], nonnegative=True) not in (None, 0.) for row in implemented):
                        unavailable = "D_VISIBLE_ACTION_IMPLEMENTATION_UNKNOWN"
                    if unavailable is None:
                        multiplier, source = _target_raw_price_multiplier(symbol=symbol, decision_raw_close=close,
                            decision_adjustment_factor=factor, decision_adjustment_ready=factor is not None,
                            rows=implemented, decision_as_of_trade_date=d)
                    if unavailable is None:
                        state = _project_target_st((symbol, event[2])) if event[2] is not None else False
                        context = PriceRangeRealtimeContext(symbol=symbol, decision_raw_close=close,
                            decision_price_trade_date=d, decision_price_source="market.kline_daily_raw:D",
                            price_unit_divisor=1000., target_raw_price_multiplier=multiplier, corporate_action_source=source,
                            board_type=_board_type(symbol), list_date=listed, listed_trading_days=base[4],
                            target_is_st=state, tick_size=.01)
                        legal = resolve_regulatory_price_range(context, target_trade_date=t)
                        if legal.status == "NO_DAILY_LIMIT":
                            unavailable = "NO_FINITE_LEGAL_DOMAIN"
                        else:
                            outputs[index]["contexts"][symbol] = dict(reference_cny=close,
                                raw_legal_low_cny=legal.low, raw_legal_high_cny=legal.high, raw_tick_cny=.01,
                                target_raw_price_multiplier=multiplier, visible_through=d.isoformat(),
                                coordinate_source=f"{source}|{legal.rule_id}|CURRENT_D_VISIBLE_ST:{CANONICAL_PIT_UNIVERSE_KEY}")
            receipt = outputs[index]["receipt"]
            receipt["query_count"] = 3
            if unavailable:
                receipt["unavailable"].append(dict(instrument=symbol, reason_code=unavailable))
        for index, output in enumerate(outputs):
            receipt = output["receipt"]
            receipt["known_context_count"] = len(output["contexts"])
            receipt["input_sha256"] = sha(dict(contexts=output["contexts"], receipt=receipt,
                d_visible_rows=_row_identity([row for row in raw if row[0] == index]),
                st_rows=_row_identity([row for row in st if row[0] == index]),
                action_rows=_row_identity([row for row in actions if row[0] == index])))
        return outputs
