"""Small transaction adapter for sanctioned legacy batch import/repair writers."""
from __future__ import annotations

from datetime import date, datetime
import re

from psycopg2 import extras

from .catalog import CN
from .repository import mark_changed


def sql_revision_marker(dataset, dates_variable="changed_dates"):
    """Fragment inside a managed repair transaction, not a new DML authority."""
    if any(not re.fullmatch(r"[a-z][a-z0-9_]*", value) for value in (dataset, dates_variable)):
        raise ValueError("invalid managed repair identity")
    derived = ""
    if dataset in {"moneyflow_ts", "sw_daily"}:
        from backend.services.sector_data_repair import _MARK_SQL
        dates = (f"(SELECT array_agg(d) FROM unnest({dates_variable}) AS t(d) "
                 "WHERE d >= DATE '2021-07-30')")
        derived = _MARK_SQL.replace("%s", f"'{dataset}'", 1).replace("%s", dates, 1) + ";"
        derived += f"""PERFORM market.mark_data_integrity_changed('sector_data',d,'dependency_changed','{dataset}')
            FROM (SELECT DISTINCT unnest({dates_variable}) AS d) AS dates WHERE d >= DATE '2021-07-30';"""
    return f"""IF to_regprocedure('market.mark_data_integrity_changed(text,date,text,text)') IS NOT NULL THEN
        PERFORM market.mark_data_integrity_changed('{dataset}',d,'managed_repair','controlled_sql')
        FROM (SELECT DISTINCT unnest({dates_variable}) AS d) AS dates WHERE d IS NOT NULL;
        {derived}
        END IF;"""


def tracked_repair_sql(query, dataset):
    """Caller supplies its already validated exact VALUES SQL with RETURNING."""
    return ("DO $integrity_write$ DECLARE changed_dates date[]; BEGIN WITH changed AS ("
        + query.rstrip("; ") + ") SELECT array_agg(DISTINCT trade_date) INTO changed_dates FROM changed; "
        + sql_revision_marker(dataset) + " END $integrity_write$;")


def fact_day(value):
    if isinstance(value, datetime):
        return value.astimezone(CN).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if "T" in text or " " in text:
        return fact_day(datetime.fromisoformat(text.replace("Z", "+00:00")))
    return date.fromisoformat(text) if "-" in text else datetime.strptime(text, "%Y%m%d").date()


def changed_values(conn, query, values, *, dataset, source, **options):
    """query RETURNING fact-date only for real insert/update/delete effects.

    Existing callers keep their row-count meaning and own their transaction if
    already non-autocommit. Failure of the marker rolls back owned fact writes.
    """
    if not values:
        return 0
    owns = getattr(conn, "autocommit", False)
    if owns:
        conn.autocommit = False
    try:
        with conn.cursor() as cur:
            changed = extras.execute_values(cur, query, values, fetch=True, **options)
        days = sorted({fact_day(row[0]) for row in changed})
        mark_changed(conn, dataset, days, reason="batch_fact_changed", source=source)
        if days and dataset in {"moneyflow_ts", "sw_daily"}:
            from backend.services.sector_data_repair import mark_revision_dates
            mark_revision_dates(conn, days, dataset)
        if owns:
            conn.commit()
    except BaseException:
        if owns:
            conn.rollback()
        raise
    finally:
        if owns:
            conn.autocommit = True
    return len(values)
