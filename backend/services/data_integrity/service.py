"""Read-only calendar/detail facade. No GET initiates validation or market scans."""
from __future__ import annotations

import base64
from contextlib import contextmanager
from dataclasses import asdict
import calendar as month_calendar
from datetime import date, datetime, timedelta
import json
import re
from itertools import islice

from psycopg2.extras import RealDictCursor

from .catalog import CN, obligations
from .contracts import CONTRACT_VERSION, aggregate_day, digest, effective_partition
from .repository import IntegrityRepository, IntegrityRevisionChanged, IntegritySchemaUnavailable, schema_installed


def parse_day(value: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError("date must use YYYY-MM-DD")
    return date.fromisoformat(value)


class DailyIntegrityService:
    def __init__(self, repository: IntegrityRepository | None = None):
        self.repository = repository or IntegrityRepository()

    @staticmethod
    def _release_view(days):
        from .release import active_release_pins, release_day
        try:
            cutoff, pins, identity = active_release_pins()
        except (OSError, ValueError, RuntimeError, KeyError) as exc:
            # Source integrity remains readable when an optional release is
            # unavailable; never turn that into fake source gaps/green pins.
            return [{**day, "release_contains_revision": None, "release_coverage_state": "READ_ERROR",
                     "release_read_error": type(exc).__name__} for day in days]
        return [release_day(day, cutoff=cutoff, pins=pins, identity=identity) for day in days]

    def _read(self, conn, start: date, end: date):
        if not schema_installed(conn):
            raise IntegritySchemaUnavailable("daily integrity migration is pending")
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Include real preceding/following trading dates for publication clocks.
            cur.execute("SELECT cal_date,is_trading FROM market.trading_calendar WHERE cal_date BETWEEN %s AND %s ORDER BY cal_date",
                        (date.fromordinal(max(1, start.toordinal()-15)),
                         date.fromordinal(min(date.max.toordinal(), end.toordinal()+30))))
            calendar = {r["cal_date"]: r["is_trading"] for r in cur.fetchall()}
            cur.execute("SELECT dataset,enabled,options FROM market.ingestion_schedules WHERE enabled=TRUE")
            schedules = [dict(r) for r in cur.fetchall()]
            cur.execute("""SELECT DISTINCT ON (fact_date) * FROM market.data_daily_integrity_revisions
                WHERE fact_date BETWEEN %s AND %s AND contract_version=%s
                ORDER BY fact_date,revision DESC""", (start, end, CONTRACT_VERSION))
            summaries = {r["fact_date"]: dict(r) for r in cur.fetchall()}
        rows = self.repository.partitions(conn, start, end)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""SELECT DISTINCT ON (dataset,target_date) dataset,target_date,target_status,next_retry_at,
                target_scope->>'operation' AS operation,
                (metadata->>'lease_until')::timestamptz>clock_timestamp() AS lease_active
                FROM market.data_sync_targets WHERE target_date BETWEEN %s AND %s
                ORDER BY dataset,target_date,CASE WHEN target_scope->>'operation'='VALIDATE_ONLY' THEN 0 ELSE 1 END,updated_at DESC""", (start, end))
            targets = {(r["target_date"], r["dataset"]): dict(r) for r in cur.fetchall()}
        # Display metadata never becomes an alternative completeness authority.
        for row in rows:
            target = targets.get((row["trade_date"], row["dataset"]))
            if target:
                row["validation_queue"] = target
        partitions = {(r["trade_date"], r["dataset"]): r for r in rows}
        return calendar, schedules, summaries, partitions

    @staticmethod
    def _day(day, calendar, schedules, summaries, partitions, now):
        if day not in calendar:
            # Unknown calendar is not a weekend and not a complete empty day.
            trading = None
        else:
            trading = bool(calendar[day])
        summary = summaries.get(day) or {}
        historic = (summary.get("summary") or {}).get("obligations", [])
        required, catalogue_id = obligations(schedules, historic)
        items = []
        for obligation in required:
            row = partitions.get((day, obligation.dataset))
            item = effective_partition(row, scope_version=obligation.scope_version, rule_version=obligation.rule_version)
            queue = item.get("validation_queue") or {}
            if not item["current"] and queue.get("lease_active") and item["state"] == "UNVERIFIED":
                item["state"] = "VERIFYING"
            item["next_retry_at"] = queue.get("next_retry_at")
            due = obligation.due_at(day, calendar)
            # Only known absence of a verdict can be waiting; never hide a confirmed gap.
            if item["state"] == "UNVERIFIED" and not row and due and now < due and day <= now.date():
                item["state"] = "NOT_DUE"
            item.update(dataset=obligation.dataset, category=obligation.category, due_at=due,
                        overdue=bool(due and now > due), fact_date=day.isoformat(),
                        scope_version=obligation.scope_version, rule_version=obligation.rule_version)
            items.append(item)
        result = aggregate_day(items, trading_day=trading is not False,
                               catalogue_known=trading is not None, future=day > now.date())
        watermark = digest([{key: p.get(key) for key in
            ("dataset", "data_revision", "validated_revision", "validation_id", "scope_version", "rule_version", "state")}
            for p in items])
        materialized = summary.get("summary") or {}
        current = materialized.get("watermark") == watermark and materialized.get("catalogue_id") == catalogue_id
        return {"schema_version": CONTRACT_VERSION, "as_of": now.isoformat(), "fact_date": day.isoformat(),
                "contract_version": CONTRACT_VERSION,
                "daily_integrity_id": summary.get("daily_integrity_id") if current else None,
                "last_materialized_integrity_id": summary.get("daily_integrity_id"),
                "revision": summary.get("revision") if current else None, **result,
                "future": day > now.date(),
                "is_trading": trading,
                "first_completed_at": materialized.get("first_completed_at"),
                "last_completed_at": materialized.get("last_completed_at"),
                "completion_kind": summary.get("completion_kind") if current and result["all_sync_complete"] else None,
                "observed_on_time": summary.get("observed_on_time") if current else None,
                "watermark": watermark, "summary_current": current, "catalogue_id": catalogue_id,
                "view_watermark": digest([watermark, [p.get("validation_queue") for p in items]]),
                "obligations": [o.as_dict() for o in required], "partitions": items,
                "source_complete": result["all_sync_complete"],
                "release_contains_revision": None,
                "release_coverage_state": "NOT_ESTABLISHED"}

    def calendar(self, month: str):
        if not re.fullmatch(r"\d{4}-\d{2}", month):
            raise ValueError("month must use YYYY-MM")
        start = date.fromisoformat(month + "-01")
        end = start.replace(day=month_calendar.monthrange(start.year, start.month)[1])
        with self.repository.transaction(readonly=True) as conn:
            inputs = self._read(conn, start, end)
            now = datetime.now(CN)
            days = [self._day(start + timedelta(days=i), *inputs, now) for i in range((end-start).days+1)]
        days = self._release_view(days)
        slim = [{key: value for key, value in day.items() if key not in {"partitions", "obligations"}} for day in days]
        return {"schema_version": CONTRACT_VERSION, "month": month, "as_of": now.isoformat(), "days": slim,
                "trading_day_count": sum(d["is_trading"] is True for d in slim),
                "recovered_day_count": sum(d["completion_kind"] == "RECOVERED" for d in slim),
                "counts": {state: sum(d["state"] == state for d in slim) for state in
                           ("NOT_TRADING", "UNVERIFIED", "IN_PROGRESS", "PARTIAL", "ERROR", "COMPLETE")},
                "etag": digest([(d["fact_date"], d["view_watermark"], d["state"], d.get("release_identity"),
                    d.get("release_contains_revision"), d.get("release_read_error"), d["daily_integrity_id"],
                    d["revision"], d["summary_current"], d["completion_kind"]) for d in slim])}

    def day(self, day: str):
        fact_date = parse_day(day)
        with self.repository.transaction(readonly=True) as conn:
            result = self._day(fact_date, *self._read(conn, fact_date, fact_date), datetime.now(CN))
        return self._release_view([result])[0]

    def readiness(self, *, data_date: str, consumer: str, binding_id: str):
        from backend.services.canonical_equity_pit import (
            CanonicalPitAuthorityResolver, CanonicalPitContractError, require_canonical_consumer_binding,
        )
        from backend.services.strategy_package.models import StrategyPackageManifest
        from backend.services.strategy_package.package_asset_store import LocalPackageAssetStore
        from backend.services.trading_core.errors import PackageAssetInvalidError
        from .consumers import DependencyUnknown, package_dependencies, project_readiness

        day = parse_day(data_date)
        if consumer.upper() not in {"SELECTION", "ADVISORY", "LOCALSIM", "HMM", "QE", "FACTOR"}:
            raise ValueError("unknown consumer")
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,160}", binding_id):
            raise ValueError("invalid formal binding identity")
        now = datetime.now(CN)
        if day > now.date():
            raise ValueError("future source date is not available")
        base = {"schema_version": CONTRACT_VERSION, "fact_date": data_date, "data_date": data_date,
                "decision_at": now.isoformat(), "as_of": now.isoformat(), "consumer": consumer.upper(),
                "binding_id": binding_id, "source_kind": "DATABASE_CURRENT"}
        with self.repository.transaction(readonly=True) as conn:
            if not schema_installed(conn):
                raise IntegritySchemaUnavailable("daily integrity migration is pending")
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT manifest_json,manifest_sha256,package_status FROM strategy_pkg.package WHERE package_id=%s", (binding_id,))
                record = cur.fetchone()
            if not record or record["package_status"] in {"DRAFT", "RETIRED"}:
                return {**base, **project_readiness(None, [], {}, pit_binding=None, reason="admitted_binding_not_resolved")}
            try:
                manifest = StrategyPackageManifest.model_validate(record["manifest_json"])
                if manifest.manifest_sha256 != record["manifest_sha256"]:
                    raise DependencyUnknown("frozen package manifest identity differs")
                dependencies = package_dependencies(manifest, LocalPackageAssetStore())
            except (ValueError, PackageAssetInvalidError) as exc:
                return {**base, **project_readiness(None, [], {}, pit_binding=None, reason=str(exc))}

            @contextmanager
            def borrowed_connection():
                yield conn

            try:
                pit = CanonicalPitAuthorityResolver(borrowed_connection).resolve_live_binding()
                require_canonical_consumer_binding(pit, consumer="daily_integrity")
                if pit.coverage_start is None or pit.coverage_end is None or not pit.coverage_start <= day <= pit.coverage_end:
                    raise DependencyUnknown("global PIT does not cover the requested source date")
            except (CanonicalPitContractError, DependencyUnknown) as exc:
                return {**base, **project_readiness(None, [], {}, pit_binding=None, reason=str(exc))}
            with conn.cursor() as cur:
                cur.execute("""SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date<=%s
                    ORDER BY cal_date DESC LIMIT %s""", (day, dependencies.window_trading_days))
                dates = sorted(r[0] for r in cur.fetchall())
            if len(dates) != dependencies.window_trading_days or dates[-1] != day:
                return {**base, **project_readiness(None, [], {}, pit_binding=asdict(pit), reason="official_input_window_unresolved")}
            rows = self.repository.partitions(conn, dates[0], dates[-1])
            result = project_readiness(dependencies, dates, {(r["trade_date"], r["dataset"]): r for r in rows}, pit_binding=asdict(pit))
            daily = self._day(day, *self._read(conn, day, day), now)
            return {**base, **result, "all_sync_complete": daily["all_sync_complete"],
                    "daily_integrity_id": daily["daily_integrity_id"], "revision": daily["revision"]}

    def gaps(self, day: str, dataset: str, *, cursor: str | None = None, limit: int = 100):
        if not 1 <= limit <= 500 or not re.fullmatch(r"[a-z][a-z0-9_]*", dataset):
            raise ValueError("invalid dataset or page size")
        result = self.day(day)
        item = next((p for p in result["partitions"] if p["dataset"] == dataset), None)
        if item is None:
            raise ValueError("dataset is outside this day's obligation catalogue")
        offset = 0
        identity = digest([result["watermark"], dataset, item.get("validation_id")])
        if cursor:
            try:
                decoded = json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
                if not isinstance(decoded, dict) or set(decoded) != {"identity", "offset"}:
                    raise ValueError
                offset = decoded["offset"]
                if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
                    raise ValueError
            except (ValueError, UnicodeError, TypeError) as exc:
                raise ValueError("invalid gap cursor") from exc
            if decoded["identity"] != identity:
                raise IntegrityRevisionChanged("gap version changed; restart at the first page")
        validation = (item.get("metadata") or {}).get("validation") or {}
        groups = validation.get("gaps") or []
        def entries():
            for group in groups:
                field = "symbols" if "symbols" in group else "keys" if "keys" in group else None
                if field:
                    base = {k: v for k, v in group.items() if k != field}
                    for value in group[field]:
                        yield {**base, "symbol" if field == "symbols" else "key": value}
                else:
                    yield group
        total = sum(len(g["symbols"]) if "symbols" in g else len(g["keys"]) if "keys" in g else 1 for g in groups)
        more = offset + limit < total
        next_cursor = base64.urlsafe_b64encode(json.dumps({"identity": identity, "offset": offset+limit}).encode()).decode() if more else None
        return {"fact_date": day, "dataset": dataset, "watermark": result["watermark"],
                "validation_id": item.get("validation_id"), "counts": validation.get("counts"),
                "groups": list(islice(entries(), offset, offset+limit)), "group_count": total, "next_cursor": next_cursor,
                "validation_current": item.get("current", False),
                "state": item["state"]}
