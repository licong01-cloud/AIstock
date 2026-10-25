"""Separate D-close/T-open projections: never decode T-close or sealed outcomes."""
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.entry_price_daily_service import BoundedEntryReadSession, EntryWorkBudget
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _number
from backend.services.advisory_model_first.market_open_hurdle_price_5td_contracts_v1 import (
    CLOCK, KEY, MARKET_COLUMN, sha,
)
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_inputs_v1 import (
    EVAL_FINANCE as EVAL_FINANCE, META as META, checked_source as checked_source,
    clocks_v1 as clocks_v1, prepare_hurdle_inputs_v1 as prepare_core,
    read_projection_v1 as read_projection_v1, sessions_v1 as sessions_v1,
    validate_finance_v1 as validate_finance_v1, validate_roster_v1 as validate_roster_v1,
)


def read_market_open_v1(*, plan, dates, calendar, session_factory=None):
    """One shared snapshot, bounded exact D.close and T.open only, no retries."""
    sessions, clocks = sessions_v1(calendar), clocks_v1(plan, calendar)
    dates = [pd.Timestamp(d) for d in dates]
    allowed = set(pd.to_datetime(clocks["train_dates"]+clocks["gap_evaluation_dates"]))
    if (len(dates) > 305 or len(set(dates)) != len(dates) or not set(dates).issubset(allowed)
            or any(pd.isna(d) or d.tz is not None or d != d.normalize() for d in dates)):
        raise ValueError("market observation cannot read sealed/immature or foreign dates")
    pairs = sorted((d, sessions[sessions.get_loc(d)+1]) for d in dates)
    receipt = dict(source="market.index_daily", index_id="000300.SH", requested_pairs=len(pairs),
        information_clock=CLOCK, source_evidence="CURRENT_DATABASE_NON_VINTAGE", native_identity="UNPROVEN",
        requested_keys_sha256=sha([[d.date().isoformat(), t.date().isoformat()] for d, t in pairs]),
        database_written=False, selects=0, fields={"D": ["close"], "T": ["open"]},
        query_at=datetime.now(timezone.utc).isoformat(), sealed_consumed=False)
    names = [*KEY[:2], MARKET_COLUMN, "market_source_status"]
    if not pairs:
        return pd.DataFrame(columns=names), dict(receipt, returned_close=0, returned_open=0)
    session = (session_factory or (lambda: BoundedEntryReadSession(EntryWorkBudget(30))))()
    try:
        with session.connection() as connection:
            with connection.cursor() as cursor:
                observed = []
                for field, days in (("close", [d for d, _ in pairs]), ("open", [t for _, t in pairs])):
                    # Field is a closed internal enum. Never SELECT an entire T market row.
                    cursor.execute("SELECT trade_date, "+field+" FROM market.index_daily "
                        "WHERE ts_code=%s AND trade_date=ANY(%s::date[]) ORDER BY trade_date LIMIT %s",
                        ("000300.SH", [d.date() for d in days], len(days)+1))
                    values = cursor.fetchall()
                    if [c.name for c in cursor.description] != ["trade_date", field] or len(values) > len(days):
                        raise ValueError("market field projection/schema/row budget differs")
                    mapping = {}
                    for day, raw in values:
                        day = pd.Timestamp(day)
                        if day not in days or day in mapping:
                            raise ValueError("market source returned duplicate or foreign keys")
                        number = _number(raw)
                        if number is not None and number <= 0:
                            raise ValueError("known market prices must be positive")
                        mapping[day] = number
                    observed.append(mapping)
    finally:
        session.close()
    records = []
    for d, t in pairs:
        close, opened = observed[0].get(d), observed[1].get(t)
        gap = None if close is None or opened is None else 10000*(opened/close-1)
        if gap is not None and (not np.isfinite(gap) or gap <= -10000):
            raise ValueError("computed market gap is not a positive price coordinate")
        records.append({KEY[0]: d, KEY[1]: t, MARKET_COLUMN: gap,
            "market_source_status": "KNOWN_POST_OPEN_OBSERVATION" if gap is not None else "UNKNOWN_MARKET_OPEN"})
    frame = pd.DataFrame(records, columns=names)
    return frame, dict(receipt, selects=2, returned_close=len(observed[0]), returned_open=len(observed[1]),
        projected_values_sha256=sha(frame.astype(str).to_dict("records")),
        market_known_pairs=int(frame[MARKET_COLUMN].notna().sum()))


def join_market_open_v1(rows, market):
    names = [*KEY[:2], MARKET_COLUMN, "market_source_status"]
    if (not set(names).issubset(market) or market.duplicated(list(KEY[:2])).any()
            or MARKET_COLUMN in rows or "market_source_status" in rows):
        raise ValueError("market projection identity/duplicate columns differ")
    return rows.merge(market[names], on=list(KEY[:2]), how="left", validate="many_to_one")


def prepare_hurdle_inputs_v1(*, plan, session_factory=None):
    from backend.services.advisory_model_first.market_open_hurdle_price_5td_model_v1 import fit_encoding_v1
    prepared = prepare_core(plan=plan)
    old = prepared["encoding"]
    market, receipt = read_market_open_v1(plan=plan, dates=old["clocks"]["train_dates"],
        calendar=prepared["calendar"], session_factory=session_factory)
    domain = join_market_open_v1(prepared["domain"], market)
    if domain.market_source_status.isna().any():
        raise ValueError("an original training date was lost in the market projection")
    encoding = fit_encoding_v1(domain, old["intervals_bps"])
    encoding.update({n: old[n] for n in ("clocks", "original_roster_sha256", "source_stage_sha256", "source_ref")})
    encoding["market_train_source_receipt_sha256"] = sha(receipt)
    return dict(prepared, domain=domain, encoding=encoding, market_train=market, market_receipt=receipt)
