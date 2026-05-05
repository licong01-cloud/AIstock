from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
from pathlib import Path
from typing import Any, Optional

import psycopg2.extras as pgx
from dotenv import load_dotenv

from ..db.pg_pool import get_conn


PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env", override=False)

DEFAULT_ST_PIT_UNIVERSE_KEY = "shsz_st_pit_active_v1"
DEFAULT_ST_PIT_RULE_VERSION = "st_pub_next_trade_restore_active_l_v1"
DEFAULT_ST_PIT_SCOPE = "st_only_active"
DEFAULT_ST_PIT_START_DATE = dt.date(2018, 8, 1)


class StockUniversePitError(RuntimeError):
    """Raised when the ST PIT derived universe cannot be prepared."""


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str, sort_keys=True)


def _fingerprint_sha256(data: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dumps(data).encode("utf-8")).hexdigest()


class StockUniversePitService:
    """Build and validate the derived ST-only PIT stock universe cache."""

    def __init__(self, reports_dir: Optional[Path] = None) -> None:
        self.reports_dir = reports_dir or Path("reports") / "stock_universe_pit"

    def ensure_tables(self) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS market;")
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS market.stock_universe_pit_state (
                        universe_key TEXT PRIMARY KEY,
                        rule_version TEXT NOT NULL,
                        scope TEXT NOT NULL,
                        start_date DATE NOT NULL,
                        end_date DATE NOT NULL,
                        status TEXT NOT NULL,
                        dirty BOOLEAN NOT NULL DEFAULT FALSE,
                        source_fingerprint JSONB NOT NULL DEFAULT '{}'::jsonb,
                        source_fingerprint_sha256 TEXT NOT NULL DEFAULT '',
                        last_build_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
                        last_error TEXT,
                        generated_at TIMESTAMPTZ,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
                comments = {
                    "stock_universe_pit_state": (
                        "Build state for derived ST PIT stock-universe cache; "
                        "source tables remain stock_basic/stock_st/stock_st_events"
                    ),
                }
                for table, comment in comments.items():
                    cur.execute(f"COMMENT ON TABLE market.{table} IS %s;", (comment,))
                column_comments = {
                    "universe_key": "Logical universe id, e.g. shsz_st_pit_active_v1",
                    "rule_version": "ST PIT rule version used for current derived rows",
                    "scope": "Universe scope; st_only_active excludes current D/P stocks and does not implement delisting PIT",
                    "start_date": "Inclusive earliest date covered by derived PIT spans",
                    "end_date": "Inclusive latest date covered by derived PIT spans",
                    "status": "Build status: ready, dirty, building, or failed",
                    "dirty": "True when upstream source tables changed after the last successful build",
                    "source_fingerprint": "JSON fingerprint of source table counts/max dates used to detect stale derived rows",
                    "source_fingerprint_sha256": "SHA256 of source_fingerprint for compact comparisons and metadata",
                    "last_build_summary": "Last builder summary including counts, validation, and report path",
                    "last_error": "Last build or ensure error message, if any",
                    "generated_at": "Timestamp of the last successful derived universe generation",
                    "updated_at": "Timestamp of the last state row update",
                }
                for col, comment in column_comments.items():
                    cur.execute(f"COMMENT ON COLUMN market.stock_universe_pit_state.{col} IS %s;", (comment,))
                cur.execute(
                    """
                    INSERT INTO market.data_stats_config
                        (data_kind, table_name, date_column, updated_column, enabled, extra_info)
                    VALUES (
                        'stock_universe_pit_state',
                        'market.stock_universe_pit_state',
                        'end_date',
                        'updated_at',
                        TRUE,
                        jsonb_build_object(
                            'desc', 'Derived ST PIT stock-universe build state',
                            'rule_version', %s,
                            'date_sequence', 'trading',
                            'source', 'stock_basic+stock_st+stock_st_events',
                            'is_timeseries', false
                        )
                    )
                    ON CONFLICT (data_kind) DO UPDATE
                        SET table_name = EXCLUDED.table_name,
                            date_column = EXCLUDED.date_column,
                            updated_column = EXCLUDED.updated_column,
                            enabled = EXCLUDED.enabled,
                            extra_info = EXCLUDED.extra_info;
                    """,
                    (DEFAULT_ST_PIT_RULE_VERSION,),
                )

    def resolve_default_end_date(self) -> dt.date:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT MAX(cal_date)::date
                      FROM market.trading_calendar
                     WHERE is_trading = TRUE
                       AND cal_date <= CURRENT_DATE
                    """
                )
                row = cur.fetchone()
        if row and row[0]:
            return row[0]
        return dt.date.today()

    def compute_source_fingerprint(self) -> dict[str, Any]:
        with get_conn() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE list_status = 'L') AS l_count,
                        COUNT(*) FILTER (WHERE list_status = 'D') AS d_count,
                        COUNT(*) FILTER (WHERE list_status = 'P') AS p_count,
                        COUNT(*) AS total_count,
                        MAX(list_date)::date AS max_list_date,
                        MAX(delist_date)::date AS max_delist_date
                      FROM market.stock_basic
                     WHERE exchange IN ('SSE', 'SZSE')
                       AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
                    """
                )
                stock_basic = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT COUNT(*) AS row_count, MAX(ann_date)::date AS max_ann_date
                      FROM market.stock_st
                    """
                )
                stock_st = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS row_count,
                        MAX(pub_date)::date AS max_pub_date,
                        MAX(imp_date)::date AS max_imp_date
                      FROM market.stock_st_events
                    """
                )
                stock_st_events = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT MAX(cal_date)::date AS max_trading_day
                      FROM market.trading_calendar
                     WHERE is_trading = TRUE
                       AND cal_date <= CURRENT_DATE
                    """
                )
                trading_calendar = dict(cur.fetchone() or {})
        return {
            "stock_basic": stock_basic,
            "stock_st": stock_st,
            "stock_st_events": stock_st_events,
            "trading_calendar": trading_calendar,
        }

    def get_status(self, *, universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY) -> dict[str, Any]:
        self.ensure_tables()
        with get_conn() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT universe_key, rule_version, scope, start_date, end_date,
                           status, dirty, source_fingerprint,
                           source_fingerprint_sha256, last_build_summary,
                           last_error, generated_at, updated_at
                      FROM market.stock_universe_pit_state
                     WHERE universe_key = %s
                    """,
                    (universe_key,),
                )
                row = cur.fetchone()
        if not row:
            return {"universe_key": universe_key, "status": "missing", "dirty": True}
        return dict(row)

    def mark_dirty(
        self,
        *,
        reason: str,
        source_dataset: str | None = None,
        universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY,
    ) -> dict[str, Any]:
        self.ensure_tables()
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO market.stock_universe_pit_state (
                        universe_key, rule_version, scope, start_date, end_date,
                        status, dirty, last_build_summary, last_error, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, 'dirty', TRUE,
                        jsonb_build_object('dirty_reason', %s, 'source_dataset', %s),
                        NULL, NOW()
                    )
                    ON CONFLICT (universe_key) DO UPDATE
                        SET status = CASE
                                WHEN market.stock_universe_pit_state.status = 'building' THEN 'building'
                                ELSE 'dirty'
                            END,
                            dirty = TRUE,
                            last_build_summary = COALESCE(market.stock_universe_pit_state.last_build_summary, '{}'::jsonb)
                                || jsonb_build_object('dirty_reason', %s, 'source_dataset', %s),
                            updated_at = NOW();
                    """,
                    (
                        universe_key,
                        DEFAULT_ST_PIT_RULE_VERSION,
                        DEFAULT_ST_PIT_SCOPE,
                        DEFAULT_ST_PIT_START_DATE,
                        DEFAULT_ST_PIT_START_DATE,
                        reason,
                        source_dataset,
                        reason,
                        source_dataset,
                    ),
                )
        return {"universe_key": universe_key, "dirty": True, "reason": reason, "source_dataset": source_dataset}

    def _needs_rebuild(
        self,
        *,
        state: dict[str, Any],
        start_date: dt.date,
        end_date: dt.date,
        rule_version: str,
        source_sha: str,
    ) -> tuple[bool, str]:
        if state.get("status") == "missing":
            return True, "missing_state"
        if state.get("status") != "ready":
            return True, f"status_{state.get('status')}"
        if bool(state.get("dirty")):
            return True, "dirty"
        if state.get("rule_version") != rule_version:
            return True, "rule_version_changed"
        if state.get("scope") != DEFAULT_ST_PIT_SCOPE:
            return True, "scope_changed"
        if state.get("start_date") and state["start_date"] > start_date:
            return True, "start_coverage_insufficient"
        if state.get("end_date") and state["end_date"] < end_date:
            return True, "end_coverage_insufficient"
        if state.get("source_fingerprint_sha256") != source_sha:
            return True, "source_fingerprint_changed"
        summary = state.get("last_build_summary") or {}
        validation = summary.get("validation") if isinstance(summary, dict) else {}
        if isinstance(validation, dict) and any(
            int(validation.get(key, 0) or 0) > 0
            for key in ["invalid_span_count", "overlap_error_count", "event_action_violation_count"]
        ):
            return True, "last_validation_failed"
        return False, "ready"

    def ensure_st_pit_universe(
        self,
        *,
        universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY,
        start_date: dt.date = DEFAULT_ST_PIT_START_DATE,
        end_date: dt.date | None = None,
        rule_version: str = DEFAULT_ST_PIT_RULE_VERSION,
        force: bool = False,
        strict: bool = True,
        rebuild_if_stale: bool = True,
    ) -> dict[str, Any]:
        self.ensure_tables()
        end = end_date or self.resolve_default_end_date()
        source = self.compute_source_fingerprint()
        source_sha = _fingerprint_sha256(source)
        state = self.get_status(universe_key=universe_key)
        needs_rebuild, reason = self._needs_rebuild(
            state=state,
            start_date=start_date,
            end_date=end,
            rule_version=rule_version,
            source_sha=source_sha,
        )
        if force:
            needs_rebuild, reason = True, "force"
        if not needs_rebuild:
            return {
                "universe_key": universe_key,
                "status": "ready",
                "rebuilt": False,
                "reason": reason,
                "source_fingerprint_sha256": source_sha,
                "state": state,
            }
        if not rebuild_if_stale:
            message = f"ST PIT universe {universe_key} is stale: {reason}"
            if strict:
                raise StockUniversePitError(message)
            return {"universe_key": universe_key, "status": "stale", "rebuilt": False, "reason": reason, "error": message}
        try:
            rebuilt = self.rebuild_st_pit_universe(
                universe_key=universe_key,
                start_date=start_date,
                end_date=end,
                rule_version=rule_version,
                source_fingerprint=source,
                source_fingerprint_sha256=source_sha,
            )
            rebuilt["reason"] = reason
            return rebuilt
        except Exception as exc:
            if strict:
                raise
            return {
                "universe_key": universe_key,
                "status": "failed",
                "rebuilt": False,
                "reason": reason,
                "error": str(exc),
            }

    def rebuild_st_pit_universe(
        self,
        *,
        universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY,
        start_date: dt.date = DEFAULT_ST_PIT_START_DATE,
        end_date: dt.date | None = None,
        rule_version: str = DEFAULT_ST_PIT_RULE_VERSION,
        source_fingerprint: Optional[dict[str, Any]] = None,
        source_fingerprint_sha256: Optional[str] = None,
    ) -> dict[str, Any]:
        self.ensure_tables()
        end = end_date or self.resolve_default_end_date()
        source = source_fingerprint or self.compute_source_fingerprint()
        source_sha = source_fingerprint_sha256 or _fingerprint_sha256(source)
        lock_key = f"stock_universe_pit:{universe_key}"
        with get_conn() as lock_conn:
            with lock_conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_lock(hashtext(%s))", (lock_key,))
            try:
                self._set_building(universe_key, start_date, end, rule_version, source, source_sha)
                from scripts import build_stock_universe_pit_spans as pit_builder

                args = argparse.Namespace(
                    universe_key=universe_key,
                    rule_version=rule_version,
                    scope=DEFAULT_ST_PIT_SCOPE,
                    start_date=start_date.isoformat(),
                    end_date=end.isoformat(),
                    ipo_filter_days=365,
                    reports_dir=str(self.reports_dir),
                    dry_run=False,
                    write_all_txt=False,
                )
                summary = pit_builder.build(args)
                validation = summary.get("validation") or {}
                failed_counts = [
                    int(validation.get("invalid_span_count", 0) or 0),
                    int(validation.get("overlap_error_count", 0) or 0),
                    int(validation.get("event_action_violation_count", 0) or 0),
                    int(validation.get("terminal_reentry_violation_count", 0) or 0),
                ]
                if any(count > 0 for count in failed_counts):
                    raise StockUniversePitError(f"ST PIT validation failed: {validation}")
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            """
                            UPDATE market.stock_universe_pit_state
                               SET status = 'ready',
                                   dirty = FALSE,
                                   source_fingerprint = %s::jsonb,
                                   source_fingerprint_sha256 = %s,
                                   last_build_summary = %s::jsonb,
                                   last_error = NULL,
                                   generated_at = NOW(),
                                   updated_at = NOW(),
                                   start_date = %s,
                                   end_date = %s,
                                   rule_version = %s,
                                   scope = %s
                             WHERE universe_key = %s
                            """,
                            (
                                _json_dumps(source),
                                source_sha,
                                _json_dumps(summary),
                                start_date,
                                end,
                                rule_version,
                                DEFAULT_ST_PIT_SCOPE,
                                universe_key,
                            ),
                        )
                        cur.execute("SELECT market.refresh_data_stats();")
                return {
                    "universe_key": universe_key,
                    "status": "ready",
                    "rebuilt": True,
                    "source_fingerprint_sha256": source_sha,
                    "summary": summary,
                }
            except Exception as exc:
                self._set_failed(universe_key, start_date, end, rule_version, source, source_sha, str(exc))
                raise
            finally:
                with lock_conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_unlock(hashtext(%s))", (lock_key,))

    def _set_building(
        self,
        universe_key: str,
        start_date: dt.date,
        end_date: dt.date,
        rule_version: str,
        source: dict[str, Any],
        source_sha: str,
    ) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO market.stock_universe_pit_state (
                        universe_key, rule_version, scope, start_date, end_date,
                        status, dirty, source_fingerprint,
                        source_fingerprint_sha256, last_error, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'building', TRUE, %s::jsonb, %s, NULL, NOW())
                    ON CONFLICT (universe_key) DO UPDATE
                        SET rule_version = EXCLUDED.rule_version,
                            scope = EXCLUDED.scope,
                            start_date = EXCLUDED.start_date,
                            end_date = EXCLUDED.end_date,
                            status = 'building',
                            dirty = TRUE,
                            source_fingerprint = EXCLUDED.source_fingerprint,
                            source_fingerprint_sha256 = EXCLUDED.source_fingerprint_sha256,
                            last_error = NULL,
                            updated_at = NOW();
                    """,
                    (
                        universe_key,
                        rule_version,
                        DEFAULT_ST_PIT_SCOPE,
                        start_date,
                        end_date,
                        _json_dumps(source),
                        source_sha,
                    ),
                )

    def _set_failed(
        self,
        universe_key: str,
        start_date: dt.date,
        end_date: dt.date,
        rule_version: str,
        source: dict[str, Any],
        source_sha: str,
        error: str,
    ) -> None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO market.stock_universe_pit_state (
                        universe_key, rule_version, scope, start_date, end_date,
                        status, dirty, source_fingerprint,
                        source_fingerprint_sha256, last_error, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, 'failed', TRUE, %s::jsonb, %s, %s, NOW())
                    ON CONFLICT (universe_key) DO UPDATE
                        SET status = 'failed',
                            dirty = TRUE,
                            source_fingerprint = EXCLUDED.source_fingerprint,
                            source_fingerprint_sha256 = EXCLUDED.source_fingerprint_sha256,
                            last_error = EXCLUDED.last_error,
                            updated_at = NOW();
                    """,
                    (
                        universe_key,
                        rule_version,
                        DEFAULT_ST_PIT_SCOPE,
                        start_date,
                        end_date,
                        _json_dumps(source),
                        source_sha,
                        error[:4000],
                    ),
                )

    def get_eligible_codes(
        self,
        *,
        trade_date: dt.date,
        universe_key: str = DEFAULT_ST_PIT_UNIVERSE_KEY,
        ensure: bool = True,
    ) -> list[str]:
        if ensure:
            self.ensure_st_pit_universe(
                universe_key=universe_key,
                start_date=DEFAULT_ST_PIT_START_DATE,
                end_date=trade_date,
                strict=True,
            )
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT ts_code
                      FROM market.stock_universe_pit_spans
                     WHERE universe_key = %s
                       AND eligible_start <= %s
                       AND eligible_end >= %s
                     ORDER BY ts_code
                    """,
                    (universe_key, trade_date, trade_date),
                )
                return [str(row[0]) for row in cur.fetchall()]
