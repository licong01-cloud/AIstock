from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import time
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
DEFAULT_ST_PIT_REFRESH_POLICY = "coverage"
SUPPORTED_ST_PIT_REFRESH_POLICIES = {"coverage", "source_fingerprint"}
DEFAULT_ST_PIT_LOCK_WAIT_SECONDS = 180.0
DEFAULT_ST_PIT_LOCK_POLL_SECONDS = 1.0
IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX = "shsz_st_pit_qe_dataset_"


class StockUniversePitError(RuntimeError):
    """Raised when the ST PIT derived universe cannot be prepared."""


def _json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str, sort_keys=True)


def _fingerprint_sha256(data: dict[str, Any]) -> str:
    return hashlib.sha256(_json_dumps(data).encode("utf-8")).hexdigest()


def _normalize_refresh_policy(refresh_policy: str | None) -> str:
    policy = (refresh_policy or DEFAULT_ST_PIT_REFRESH_POLICY).strip().lower()
    if policy not in SUPPORTED_ST_PIT_REFRESH_POLICIES:
        raise ValueError(f"unsupported ST PIT refresh_policy: {refresh_policy}")
    return policy


class StockUniversePitService:
    """Build and validate the derived ST-only PIT stock universe cache."""

    def __init__(self, reports_dir: Optional[Path] = None) -> None:
        self.reports_dir = reports_dir or Path("reports") / "stock_universe_pit"

    @staticmethod
    def _preserve_existing_end_date(requested_end: dt.date, state: dict[str, Any]) -> dt.date:
        """Keep a shared PIT universe's coverage monotonic across consumers."""

        existing_end = state.get("end_date")
        if isinstance(existing_end, dt.datetime):
            existing_end = existing_end.date()
        elif isinstance(existing_end, str):
            try:
                existing_end = dt.date.fromisoformat(existing_end)
            except ValueError:
                existing_end = None
        if isinstance(existing_end, dt.date) and existing_end > requested_end:
            return existing_end
        return requested_end

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
                    "scope": "Universe scope; st_only_active uses requested-end active stocks and does not implement full delisting PIT",
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

    def compute_source_fingerprint(self, *, end_date: dt.date | None = None) -> dict[str, Any]:
        fingerprint_end = end_date or self.resolve_default_end_date()
        with get_conn() as conn:
            with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (
                            WHERE (list_date IS NULL OR list_date::date <= %(end_date)s)
                              AND (delist_date IS NULL OR delist_date::date > %(end_date)s)
                        ) AS active_asof_count,
                        COUNT(*) FILTER (WHERE delist_date IS NOT NULL AND delist_date::date <= %(end_date)s)
                            AS delisted_asof_count,
                        COUNT(*) FILTER (WHERE list_date IS NULL OR list_date::date <= %(end_date)s)
                            AS listed_asof_count,
                        COUNT(*) AS total_known_count,
                        MAX(list_date::date) FILTER (WHERE list_date IS NOT NULL AND list_date::date <= %(end_date)s)
                            AS max_list_date_asof,
                        MAX(delist_date::date) FILTER (
                            WHERE delist_date IS NOT NULL AND delist_date::date <= %(end_date)s
                        ) AS max_delist_date_asof
                      FROM market.stock_basic
                     WHERE exchange IN ('SSE', 'SZSE')
                       AND (ts_code LIKE '%%.SH' OR ts_code LIKE '%%.SZ')
                    """,
                    {"end_date": fingerprint_end},
                )
                stock_basic = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT COUNT(*) AS row_count, MAX(ann_date)::date AS max_ann_date
                      FROM market.stock_st
                     WHERE ann_date::date <= %s
                    """,
                    (fingerprint_end,),
                )
                stock_st = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT
                        COUNT(*) AS row_count,
                        MAX(pub_date)::date AS max_pub_date,
                        MAX(imp_date)::date AS max_imp_date
                      FROM market.stock_st_events
                     WHERE pub_date::date <= %s
                    """,
                    (fingerprint_end,),
                )
                stock_st_events = dict(cur.fetchone() or {})
                cur.execute(
                    """
                    SELECT MAX(cal_date)::date AS max_trading_day
                      FROM market.trading_calendar
                     WHERE is_trading = TRUE
                       AND cal_date <= %s
                    """,
                    (fingerprint_end,),
                )
                trading_calendar = dict(cur.fetchone() or {})
        return {
            "fingerprint_end_date": fingerprint_end.isoformat(),
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

    def ensure_immutable_dataset_snapshot(
        self,
        *,
        universe_key: str,
        start_date: dt.date,
        end_date: dt.date,
        rule_version: str = DEFAULT_ST_PIT_RULE_VERSION,
        bootstrap_if_missing: bool = True,
        lock_wait_seconds: float = DEFAULT_ST_PIT_LOCK_WAIT_SECONDS,
    ) -> dict[str, Any]:
        """Return one exact ST PIT snapshot without refreshing it from live data.

        A dataset contract receives a new ``universe_key`` whenever its source
        dataset changes.  The only write permitted here is first-time bootstrap
        of a missing key.  Existing snapshots are validated byte-contract style:
        they are never extended, rebuilt because an upstream source changed, or
        repaired in place.  A bad snapshot therefore fails fast and requires a
        new dataset contract id.
        """

        if not universe_key.startswith(IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX):
            raise ValueError(
                "immutable QE ST PIT universe key must start with "
                f"{IMMUTABLE_QE_ST_PIT_UNIVERSE_PREFIX!r}: {universe_key!r}"
            )
        if end_date < start_date:
            raise ValueError(f"ST PIT snapshot end date {end_date} is earlier than {start_date}")

        self.ensure_tables()
        state = self.get_status(universe_key=universe_key)
        bootstrapped = False
        if state.get("status") == "missing":
            if not bootstrap_if_missing:
                raise StockUniversePitError(
                    f"immutable QE ST PIT snapshot is missing: {universe_key}"
                )
            source = self.compute_source_fingerprint(end_date=end_date)
            source_sha = _fingerprint_sha256(source)
            self.rebuild_st_pit_universe(
                universe_key=universe_key,
                start_date=start_date,
                end_date=end_date,
                rule_version=rule_version,
                source_fingerprint=source,
                source_fingerprint_sha256=source_sha,
                write_mode="replace",
                incremental_from=None,
                skip_if_ready=True,
                refresh_policy="coverage",
                lock_wait_seconds=lock_wait_seconds,
            )
            state = self.get_status(universe_key=universe_key)
            bootstrapped = True
        elif state.get("status") == "building":
            state, wait_reason = self._wait_for_ready_state(
                universe_key=universe_key,
                start_date=start_date,
                end_date=end_date,
                rule_version=rule_version,
                source_sha=str(state.get("source_fingerprint_sha256") or ""),
                refresh_policy="coverage",
                timeout_seconds=lock_wait_seconds,
            )
            if state is None:
                raise StockUniversePitError(
                    f"immutable QE ST PIT snapshot did not become ready: {universe_key} ({wait_reason})"
                )

        def _state_date(name: str) -> dt.date | None:
            value = state.get(name)
            if isinstance(value, dt.datetime):
                return value.date()
            if isinstance(value, dt.date):
                return value
            if isinstance(value, str):
                try:
                    return dt.date.fromisoformat(value)
                except ValueError:
                    return None
            return None

        violations: list[str] = []
        if state.get("status") != "ready":
            violations.append(f"status={state.get('status')!r}")
        if bool(state.get("dirty")):
            violations.append("dirty=true")
        if state.get("rule_version") != rule_version:
            violations.append(f"rule_version={state.get('rule_version')!r}")
        if state.get("scope") != DEFAULT_ST_PIT_SCOPE:
            violations.append(f"scope={state.get('scope')!r}")
        if _state_date("start_date") != start_date:
            violations.append(f"start_date={state.get('start_date')!r}")
        if _state_date("end_date") != end_date:
            violations.append(f"end_date={state.get('end_date')!r}")
        if not str(state.get("source_fingerprint_sha256") or ""):
            violations.append("source_fingerprint_sha256=empty")
        summary = state.get("last_build_summary") or {}
        validation = summary.get("validation") if isinstance(summary, dict) else None
        if isinstance(validation, dict):
            for key in (
                "invalid_span_count",
                "overlap_error_count",
                "event_action_violation_count",
                "terminal_reentry_violation_count",
            ):
                if int(validation.get(key, 0) or 0) > 0:
                    violations.append(f"{key}={validation.get(key)!r}")
        if violations:
            raise StockUniversePitError(
                "immutable QE ST PIT snapshot contract violation; publish a new dataset contract instead "
                f"of mutating this key: {universe_key}: {', '.join(violations)}"
            )

        return {
            "universe_key": universe_key,
            "status": "ready",
            "rebuilt": bootstrapped,
            "reason": "immutable_dataset_snapshot_bootstrapped" if bootstrapped else "immutable_dataset_snapshot_ready",
            "source_fingerprint_sha256": state["source_fingerprint_sha256"],
            "state": state,
        }

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
                                ELSE market.stock_universe_pit_state.status
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
        refresh_policy: str = DEFAULT_ST_PIT_REFRESH_POLICY,
    ) -> tuple[bool, str]:
        refresh_policy = _normalize_refresh_policy(refresh_policy)
        if state.get("status") == "missing":
            return True, "missing_state"
        if state.get("status") == "building":
            return True, "status_building"
        if state.get("status") not in {"ready", "dirty"}:
            return True, f"status_{state.get('status')}"
        if state.get("rule_version") != rule_version:
            return True, "rule_version_changed"
        if state.get("scope") != DEFAULT_ST_PIT_SCOPE:
            return True, "scope_changed"
        if state.get("start_date") and state["start_date"] > start_date:
            return True, "start_coverage_insufficient"
        if state.get("end_date") and state["end_date"] < end_date:
            return True, "end_coverage_insufficient"
        summary = state.get("last_build_summary") or {}
        validation = summary.get("validation") if isinstance(summary, dict) else {}
        if isinstance(validation, dict) and any(
            int(validation.get(key, 0) or 0) > 0
            for key in ["invalid_span_count", "overlap_error_count", "event_action_violation_count"]
        ):
            return True, "last_validation_failed"
        if refresh_policy == "source_fingerprint":
            if bool(state.get("dirty")):
                return True, "dirty"
            if state.get("source_fingerprint_sha256") != source_sha:
                return True, "source_fingerprint_changed"
            return False, "ready"
        if bool(state.get("dirty")) or state.get("source_fingerprint_sha256") != source_sha:
            return False, "coverage_ready_source_changed_ignored"
        return False, "ready"

    def _wait_for_ready_state(
        self,
        *,
        universe_key: str,
        start_date: dt.date,
        end_date: dt.date,
        rule_version: str,
        source_sha: str,
        refresh_policy: str,
        timeout_seconds: float,
        poll_seconds: float = DEFAULT_ST_PIT_LOCK_POLL_SECONDS,
        retryable_reasons: frozenset[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        retryable = frozenset({"status_building"}) if retryable_reasons is None else retryable_reasons
        deadline = time.monotonic() + max(timeout_seconds, 0.0)
        last_reason = "not_checked"
        while time.monotonic() <= deadline:
            state = self.get_status(universe_key=universe_key)
            needs_rebuild, reason = self._needs_rebuild(
                state=state,
                start_date=start_date,
                end_date=end_date,
                rule_version=rule_version,
                source_sha=source_sha,
                refresh_policy=refresh_policy,
            )
            last_reason = reason
            if not needs_rebuild:
                return state, reason
            if reason not in retryable:
                return None, reason
            time.sleep(max(poll_seconds, 0.1))
        return None, f"lock_wait_timeout:{last_reason}"

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
        refresh_policy: str = DEFAULT_ST_PIT_REFRESH_POLICY,
        lock_wait_seconds: float = DEFAULT_ST_PIT_LOCK_WAIT_SECONDS,
    ) -> dict[str, Any]:
        refresh_policy = _normalize_refresh_policy(refresh_policy)
        self.ensure_tables()
        requested_end = end_date or self.resolve_default_end_date()
        state = self.get_status(universe_key=universe_key)
        end = self._preserve_existing_end_date(requested_end, state)
        source = self.compute_source_fingerprint(end_date=end)
        source_sha = _fingerprint_sha256(source)
        needs_rebuild, reason = self._needs_rebuild(
            state=state,
            start_date=start_date,
            end_date=end,
            rule_version=rule_version,
            source_sha=source_sha,
            refresh_policy=refresh_policy,
        )
        if force:
            needs_rebuild, reason = True, "force"
        elif reason == "status_building":
            peer_state, peer_reason = self._wait_for_ready_state(
                universe_key=universe_key,
                start_date=start_date,
                end_date=end,
                rule_version=rule_version,
                source_sha=source_sha,
                refresh_policy=refresh_policy,
                timeout_seconds=lock_wait_seconds,
            )
            if peer_state is not None:
                return {
                    "universe_key": universe_key,
                    "status": "ready",
                    "rebuilt": False,
                    "reason": f"built_by_peer:{peer_reason}",
                    "source_fingerprint_sha256": source_sha,
                    "state": peer_state,
                }
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
        write_mode = "replace"
        incremental_from: dt.date | None = None
        if (
            reason == "end_coverage_insufficient"
            and not force
            and state.get("status") == "ready"
            and state.get("start_date")
            and state.get("end_date")
            and state["start_date"] <= start_date
        ):
            write_mode = "incremental"
            incremental_from = state["end_date"] + dt.timedelta(days=1)
        try:
            rebuilt = self.rebuild_st_pit_universe(
                universe_key=universe_key,
                start_date=start_date,
                end_date=end,
                rule_version=rule_version,
                source_fingerprint=source,
                source_fingerprint_sha256=source_sha,
                write_mode=write_mode,
                incremental_from=incremental_from,
                skip_if_ready=not force,
                refresh_policy=refresh_policy,
                lock_wait_seconds=lock_wait_seconds,
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
        write_mode: str = "replace",
        incremental_from: dt.date | None = None,
        skip_if_ready: bool = False,
        refresh_policy: str = DEFAULT_ST_PIT_REFRESH_POLICY,
        lock_wait_seconds: float = DEFAULT_ST_PIT_LOCK_WAIT_SECONDS,
    ) -> dict[str, Any]:
        refresh_policy = _normalize_refresh_policy(refresh_policy)
        self.ensure_tables()
        requested_end = end_date or self.resolve_default_end_date()
        initial_state = self.get_status(universe_key=universe_key)
        end = self._preserve_existing_end_date(requested_end, initial_state)
        source_matches_effective_end = end == requested_end
        source = (
            source_fingerprint
            if source_fingerprint is not None and source_matches_effective_end
            else self.compute_source_fingerprint(end_date=end)
        )
        source_sha = (
            source_fingerprint_sha256
            if source_fingerprint_sha256 is not None and source_matches_effective_end
            else _fingerprint_sha256(source)
        )
        lock_key = f"stock_universe_pit:{universe_key}"
        with get_conn() as lock_conn:
            with lock_conn.cursor() as cur:
                cur.execute("SELECT pg_try_advisory_lock(hashtext(%s))", (lock_key,))
                locked = bool(cur.fetchone()[0])
            if not locked:
                peer_state, peer_reason = self._wait_for_ready_state(
                    universe_key=universe_key,
                    start_date=start_date,
                    end_date=end,
                    rule_version=rule_version,
                    source_sha=source_sha,
                    refresh_policy=refresh_policy,
                    timeout_seconds=lock_wait_seconds,
                    # The advisory-lock owner may not have inserted its first
                    # ``building`` row yet.  Only this lock-loser path may
                    # treat that initial missing state as transient.
                    retryable_reasons=frozenset({"missing_state", "status_building"}),
                )
                if peer_state is not None:
                    return {
                        "universe_key": universe_key,
                        "status": "ready",
                        "rebuilt": False,
                        "reason": f"built_by_peer:{peer_reason}",
                        "source_fingerprint_sha256": source_sha,
                        "state": peer_state,
                    }
                raise StockUniversePitError(
                    "ST PIT rebuild is already running and did not become ready "
                    f"within {lock_wait_seconds:.0f}s ({peer_reason})"
                )
            try:
                locked_state = self.get_status(universe_key=universe_key)
                locked_end = self._preserve_existing_end_date(end, locked_state)
                if locked_end != end:
                    end = locked_end
                    source = self.compute_source_fingerprint(end_date=end)
                    source_sha = _fingerprint_sha256(source)
                if skip_if_ready:
                    needs_rebuild, reason = self._needs_rebuild(
                        state=locked_state,
                        start_date=start_date,
                        end_date=end,
                        rule_version=rule_version,
                        source_sha=source_sha,
                        refresh_policy=refresh_policy,
                    )
                    if not needs_rebuild:
                        return {
                            "universe_key": universe_key,
                            "status": "ready",
                            "rebuilt": False,
                            "reason": reason,
                            "source_fingerprint_sha256": source_sha,
                            "state": locked_state,
                        }
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
                    write_mode=write_mode,
                    incremental_from=incremental_from.isoformat() if incremental_from else None,
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
                        # Global data statistics are maintained by ingestion. A PIT
                        # rebuild must not synchronously scan every registered market
                        # table on the latency-sensitive selection path.
                return {
                    "universe_key": universe_key,
                    "status": "ready",
                    "rebuilt": True,
                    "write_mode": write_mode,
                    "incremental_from": incremental_from.isoformat() if incremental_from else None,
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
