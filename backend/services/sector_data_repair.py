"""Durable, date-scoped invalidation of derived SW facts (no schema creation).

Source writes and invalidation share a transaction. Completed dates are cleared
only inside the sector rebuild transaction. Ordinary unchanged refreshes do not
schedule historical scans. Source observation time is never knowledge time.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta

import psycopg2.extras as pgx

REPAIR_SOURCE = "sector_dependency_revision"
# The compatibility producer repeats SW2021 quotes, not SW2014 backcasts.
SW2021_QUOTE_START = date(2021, 7, 30)


def _day(value):
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    return date.fromisoformat(text) if "-" in text else datetime.strptime(text, "%Y%m%d").date()


def membership_revision_spans(existing, incoming):
    """Return exactly changed membership coverage; provider ends are inclusive."""
    old = {(r["l2_code"], r["ts_code"], _day(r["in_date"])): r for r in existing}
    spans = set()
    for row in incoming:
        start = _day(row["in_date"])
        end = _day(row["out_date"]) if row.get("out_date") else None
        previous = old.get((row["l2_code"], row["ts_code"], start))
        if previous is None:
            spans.add((start, end))
            continue
        previous_end = _day(previous["out_date"]) if previous.get("out_date") else None
        if previous_end == end:
            continue
        low, high = sorted((previous_end or date.max, end or date.max))
        if low < date.max:
            spans.add((low + timedelta(days=1), None if high == date.max else high))
    return sorted(spans, key=lambda item: (item[0], item[1] or date.max))


def eligibility_revision_spans(existing, incoming):
    """Small PIT interval geometry diff; no price scan, label-only false alarms."""
    symbols = {}
    for side, rows in enumerate((existing, incoming)):
        for row in rows:
            start, end = _day(row["eligible_start"]), _day(row["eligible_end"])
            if end < start:
                raise ValueError("PIT eligibility interval is inverted")
            events = symbols.setdefault(row["ts_code"], {})
            events.setdefault(start, [0, 0])[side] += 1
            if end != date.max:
                events.setdefault(end + timedelta(days=1), [0, 0])[side] -= 1
    result = set()
    for events in symbols.values():
        state = [0, 0]
        days = sorted(events)
        for i, day in enumerate(days):
            state = [state[j] + events[day][j] for j in (0, 1)]
            if bool(state[0]) != bool(state[1]):
                end = days[i + 1] - timedelta(days=1) if i + 1 < len(days) else date.max
                if end >= SW2021_QUOTE_START:
                    result.add((max(day, SW2021_QUOTE_START), end))
    return sorted(result)


def capture_sector_pit_spans(conn, universe_key):
    from .stock_universe_pit_service import DEFAULT_ST_PIT_UNIVERSE_KEY, IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX

    if universe_key != DEFAULT_ST_PIT_UNIVERSE_KEY and not universe_key.startswith(IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX):
        return None
    with conn.cursor() as cur:
        cur.execute("SELECT ts_code,eligible_start,eligible_end FROM market.stock_universe_pit_spans WHERE universe_key=%s FOR UPDATE", (universe_key,))
        return [dict(zip(("ts_code", "eligible_start", "eligible_end"), row)) for row in cur.fetchall()]


def mark_pit_revision(conn, previous, spans):
    if previous is None:
        return
    changed = eligibility_revision_spans(previous, spans)
    if not changed:
        return
    # Ready upstream quote dates also cover a racing first-day sector build.
    with conn.cursor() as cur:
        dates = pgx.execute_values(cur, "SELECT DISTINCT trade_date FROM market.sw_daily JOIN (VALUES %s) AS spans(a,b) ON trade_date BETWEEN a AND b ORDER BY trade_date", changed, template="(%s::date,%s::date)", fetch=True)
    mark_revision_dates(conn, [r[0] for r in dates], "stock_universe_pit_spans")


_MARK_SQL = """
INSERT INTO market.dataset_date_refresh_audit AS existing
    (dataset, trade_date, data_source, status, row_count, refreshed_at,
     error_message, metadata, quality_status, failure_category)
SELECT 'sector_data', d, 'sector_dependency_revision', 'failed', 0, NOW(),
       'sector source facts changed; targeted rebuild pending',
       jsonb_build_object('repair_pending', true, 'dependency', %s),
       'error', 'sector_source_revision'
FROM unnest(%s::date[]) AS dates(d)
ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
    status='failed', refreshed_at=NOW(), quality_status='error',
    failure_category='sector_source_revision', error_message=EXCLUDED.error_message,
    metadata=EXCLUDED.metadata
"""


def mark_revision_dates(conn, dates, dependency):
    if conn.autocommit:
        raise ValueError("sector invalidation must share the source transaction")
    dates = sorted({_day(value) for value in dates if _day(value) >= SW2021_QUOTE_START})
    if not dates:
        return
    with conn.cursor() as cur:
        # Retain even an in-progress first build's date: filtering by MAX would
        # lose a revision racing that build. The normal append clears it once.
        cur.execute(_MARK_SQL, (dependency, dates))


def mark_membership_revision(conn, existing, incoming):
    spans = [(max(start, SW2021_QUOTE_START), end)
             for start, end in membership_revision_spans(existing, incoming)
             if end is None or end >= SW2021_QUOTE_START]
    if not spans:
        return
    if conn.autocommit:
        raise ValueError("sector invalidation must share the source transaction")
    with conn.cursor() as cur:
        # Only already materialized dates whose dependency coverage really changed.
        sql = """SELECT DISTINCT sd.trade_date FROM market.sector_data sd
                 JOIN (VALUES %s) AS spans(start_date,end_date)
                 ON sd.trade_date >= spans.start_date
                 AND (spans.end_date IS NULL OR sd.trade_date <= spans.end_date)
                 ORDER BY sd.trade_date"""
        dates = pgx.execute_values(cur, sql, spans, template="(%s::date,%s::date)", fetch=True)
    mark_revision_dates(conn, [row[0] for row in dates], "sw_index_member")


_CLEAR_SQL = """UPDATE market.dataset_date_refresh_audit SET status='success',
    row_count=(SELECT COUNT(*) FROM market.sector_data WHERE trade_date=%s),
    written_rows=%s, refreshed_at=NOW(), quality_status='ok',
    error_message=NULL, failure_category=NULL,
    metadata=metadata || '{"repair_pending":false}'::jsonb
    WHERE dataset='sector_data' AND data_source='sector_dependency_revision'
      AND trade_date=%s AND status='failed'"""


def clear_revision_date(conn, trade_date, rows):
    if conn.autocommit:
        raise ValueError("sector completion must share the rebuild transaction")
    with conn.cursor() as cur:
        cur.execute(_CLEAR_SQL, (trade_date, rows, trade_date))


def pending_revision_dates(conn, through):
    with conn.cursor() as cur:
        cur.execute(
            """SELECT trade_date FROM market.dataset_date_refresh_audit
               WHERE dataset='sector_data' AND data_source='sector_dependency_revision'
                 AND status='failed' AND metadata @> '{"repair_pending":true}'::jsonb
                 AND trade_date <= %s ORDER BY trade_date""",
            (through,),
        )
        return [row[0] for row in cur.fetchall()]


def projection_revision_plan(existing, source_facts, start, end, *, approved_history=()):
    """Restore only dated, independently hashed provider identities, not snapshots.

    Source rows outside the requested window are only considered when they close
    a currently open local interval intersecting that window. Missing source rows
    never imply deletion or an invented end date.
    """
    from .sw_industry_history_store import digest

    start, end = _day(start), _day(end)
    if start > end:
        raise ValueError("repair window is inverted")
    def key(row):
        return row["l2_code"], row["ts_code"], _day(row["in_date"])
    old = {key(row): row for row in existing}
    observed = {}
    fields = ("l1_code", "l2_code", "l3_code", "out_date", "is_new")
    for fact in source_facts:
        payload = fact["payload"]
        row = payload["row"]
        if payload.get("endpoint") != "index_member_all" or digest(payload) != fact["fact_sha256"]:
            raise ValueError("dated member source identity differs")
        if digest(row) != payload.get("source_row_sha256"):
            raise ValueError("dated member source row pin differs")
        item = dict(row, in_date=_day(row["in_date"]),
                    out_date=_day(row["out_date"]) if row.get("out_date") else None)
        if item["is_new"] not in {"Y", "N"} or (item["is_new"] == "Y") != (item["out_date"] is None):
            raise ValueError("dated member source current flag differs")
        if item["out_date"] and item["out_date"] < item["in_date"]:
            raise ValueError("dated member source interval is inverted")
        identity = key(item)
        if identity in observed and any(observed[identity][0].get(f) != item.get(f) for f in fields):
            raise ValueError("dated source revisions need explicit lineage")
        observed[identity] = (item, fact["fact_sha256"])
    changes = []
    for identity, (item, pin) in sorted(observed.items()):
        previous = old.get(identity)
        local_intersects = previous and _day(previous["in_date"]) <= end and (
            not previous.get("out_date") or _day(previous["out_date"]) >= start)
        source_intersects = item["in_date"] <= end and (item["out_date"] is None or item["out_date"] >= start)
        if not (local_intersects or source_intersects):
            continue
        normalized = dict(previous or {})
        if normalized.get("out_date"):
            normalized["out_date"] = _day(normalized["out_date"])
        if previous is None or any(normalized.get(f) != item.get(f) for f in fields):
            if previous and previous.get("out_date") and item["out_date"] is None:
                raise ValueError("ended local history cannot be silently reopened")
            changes.append(dict(row=item, previous=previous, source_fact_sha256=pin))
    unknown = []
    history = []
    for fact in approved_history:
        if digest({"bundle_sha256": fact["bundle_sha256"], "row": fact["payload"]}) != fact["fact_sha256"]:
            raise ValueError("approved classification history pin differs")
        history.append(fact)
    for identity, row in old.items():
        if _day(row["in_date"]) <= end and (not row.get("out_date") or _day(row["out_date"]) >= start):
            if identity not in observed:
                # A provider omission alone never closes membership. One already
                # approved dated classification interval AND its real succeeding
                # provider membership may prove the exact boundary instead.
                matches = []
                for fact in history:
                    item = fact["payload"]
                    if item.get("canonical_symbol") != row["ts_code"] or item.get("published_l2_code") != row["l2_code"]:
                        continue
                    finish = _day(item["effective_to"])
                    if _day(item["effective_from"]) > end or finish < start:
                        continue
                    successors = [v[0] for v in observed.values() if v[0]["ts_code"] == row["ts_code"]
                                  and v[0]["in_date"] == finish + timedelta(days=1)
                                  and v[0]["l2_code"] != row["l2_code"]]
                    if len(successors) == 1:
                        matches.append((fact, finish))
                if len(matches) == 1:
                    fact, finish = matches[0]
                    if not row.get("out_date") or _day(row["out_date"]) != finish:
                        changes.append(dict(row=dict(row, out_date=finish, is_new="N"), previous=row,
                                            source_fact_sha256=fact["fact_sha256"],
                                            source_bundle_sha256=fact["bundle_sha256"],
                                            resolution="CLOSE_BY_APPROVED_DATED_CLASSIFICATION_AND_SUCCESSOR"))
                else:
                    unknown.append(dict(ts_code=row["ts_code"], l2_code=row["l2_code"], in_date=row["in_date"]))
    return dict(changes=changes, unverified_existing_identities=unknown,
                source_rows_sha256=digest(sorted(f["fact_sha256"] for f in source_facts)),
                start_date=start, end_date=end)
