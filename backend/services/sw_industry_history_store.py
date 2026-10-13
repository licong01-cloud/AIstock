"""Append-only SW source facts; the legacy table is a non-destructive projection.

Observation time is NOT historical knowledge time. SW2014 and SW2021 recovery
intervals live in version-qualified tables, never in the SW2021 compatibility
projection. Provider omissions are not removal events.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime

import psycopg2.extras as pgx


def _json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"Unsupported source value: {type(value).__name__}")


def canonical_bytes(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_json_default
    ).encode("utf-8")


def digest(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def source_fact(source_id, taxonomy_version, row, request):
    payload = json.loads(
        canonical_bytes({"source_id": source_id, "taxonomy_version": taxonomy_version, "row": row, "request": request})
    )
    return {
        "fact_sha256": digest(payload),
        "source_id": source_id,
        "taxonomy_version": taxonomy_version,
        "source_sha256": digest(payload["row"]),
        "payload": payload,
    }


def _day(value):
    if isinstance(value, datetime):
        raise ValueError("Industry dates must not include a timestamp")
    if isinstance(value, date):
        return value
    value = str(value)
    return date.fromisoformat(value) if "-" in value else datetime.strptime(value, "%Y%m%d").date()


def _key(row):
    return row["l2_code"], row["ts_code"], _day(row["in_date"])


def prepare_member_refresh(existing, incoming, code):
    """Validate a full Y+N refresh without inferring an end from absent rows.

    Dated membership out_date is inclusive, as in the shared PIT SQL readers.
    No returned/retained interval may overlap, including a same-day handoff.
    """
    selected = {}
    for row in incoming:
        if row.get("l2_code") != code or not re.fullmatch(r"\d{6}[.](SH|SZ|BJ)", row.get("ts_code") or ""):
            raise ValueError("SW membership request/code identity differs")
        for field in ("l1_code", "l2_code", "l3_code"):
            if not re.fullmatch(r"\d{6}[.]SI", row.get(field) or ""):
                raise ValueError("SW membership hierarchy identity is incomplete")
        start = _day(row.get("in_date"))
        end = _day(row["out_date"]) if row.get("out_date") else None
        flag = row.get("is_new")
        if flag not in {"Y", "N"} or (flag == "Y") != (end is None) or (end and end < start):
            raise ValueError("SW membership dates/current flag are inconsistent")
        key = _key(row)
        if key in selected and canonical_bytes(selected[key]) != canonical_bytes(row):
            raise ValueError("SW provider returned conflicting revisions for one identity")
        selected[key] = dict(row)
    for old in existing:
        if not old.get("out_date") and _key(old) not in selected:
            raise ValueError(f"SW unexplained active member omission: {old['ts_code']}/{code}")
        candidate = selected.get(_key(old))
        if old.get("out_date") and candidate is not None and not candidate.get("out_date"):
            raise ValueError("SW ended history cannot be reopened by a daily refresh")
    merged = {_key(row): row for row in existing}
    merged.update(selected)
    by_symbol = {}
    for row in merged.values():
        by_symbol.setdefault(row["ts_code"], []).append(row)
    for rows in by_symbol.values():
        rows.sort(key=lambda r: _day(r["in_date"]))
        for previous, current in zip(rows, rows[1:]):
            if not previous.get("out_date") or _day(previous["out_date"]) >= _day(current["in_date"]):
                raise ValueError("SW membership interval overlap after refresh")
    return list(selected.values())


def append_source_facts(conn, facts):
    if not facts:
        return
    with conn.cursor() as cur:
        pgx.execute_values(
            cur,
            """INSERT INTO market.sw_industry_source_fact_v1
            (fact_sha256,source_id,taxonomy_version,source_sha256,payload)
            VALUES %s ON CONFLICT (fact_sha256) DO NOTHING""",
            [
                (
                    r["fact_sha256"],
                    r["source_id"],
                    r["taxonomy_version"],
                    r["source_sha256"],
                    pgx.Json(r["payload"], dumps=lambda v: canonical_bytes(v).decode("utf-8")),
                )
                for r in facts
            ],
            page_size=1000,
        )


def store_member_refresh(conn, rows, write_projection):
    """One local atomic transaction: source append then non-destructive upsert.

    A table lock serializes writers across industries: a stock can move between
    different L2s. Network fetches have already completed outside this transaction.
    No implicit schema creation or fallback if the operator migration is absent.
    """
    if not rows:
        return 0
    codes = {row.get("l2_code") for row in rows}
    if len(codes) != 1:
        raise ValueError("SW refresh requires exactly one requested L2")
    code = next(iter(codes))
    if not conn.autocommit:
        raise ValueError("SW refresh requires a standalone transaction")
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("LOCK TABLE market.sw_index_member IN SHARE ROW EXCLUSIVE MODE")
            cur.execute("SELECT * FROM market.sw_index_member WHERE l2_code=%s FOR UPDATE", (code,))
            columns = [col.name for col in cur.description]
            old = [dict(zip(columns, row)) for row in cur.fetchall()]
        accepted = prepare_member_refresh(old, rows, code)
        append_source_facts(
            conn,
            [
                source_fact("tushare:index_member_all", "SW2021", r, {"l2_code": code, "is_new": r["is_new"]})
                for r in accepted
            ],
        )
        count = write_projection(accepted)
        from .sector_data_repair import mark_membership_revision

        mark_membership_revision(conn, old, accepted)
        conn.commit()
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True
