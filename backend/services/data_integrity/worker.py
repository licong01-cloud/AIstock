"""Bounded maintenance work on the existing scheduler/executor and target table."""
from __future__ import annotations

from datetime import date, datetime, timedelta
import logging
import time

from psycopg2.extras import RealDictCursor

from .catalog import CN, RULE_VERSION, SCOPE_VERSION, obligations
from .contracts import PartitionState, ValidationResult, completion_kind
from .repository import IntegrityRepository, IntegrityRevisionChanged, enqueue_validation, schema_installed
from .service import DailyIntegrityService
from .validation import PhysicalValidator

logger = logging.getLogger(__name__)


class DailyIntegrityWorker:
    def __init__(self, repository=None, validator=None):
        self.repository = repository or IntegrityRepository()
        self.validator = validator or PhysicalValidator()
        self.service = DailyIntegrityService(self.repository)

    def run_once(self, *, limit=10, budget_seconds=45):
        """No history bootstrap: current day plus exact durable changed targets."""
        if not 1 <= limit <= 50 or not 1 <= budget_seconds <= 60:
            raise ValueError("integrity maintenance batch must be bounded")
        start = time.monotonic()
        today = datetime.now(CN).date()
        with self.repository.transaction() as conn:
            if not schema_installed(conn):
                return {"status": "MIGRATION_PENDING", "validated": 0}
            calendar, schedules, summaries, _partitions = self.service._read(conn, today, today)
            required, _identity = obligations(schedules, (summaries.get(today, {}).get("summary") or {}).get("obligations", []))
            # Don't create successful or future-day verdicts. Startup does not
            # trigger historical price scans or a replacement scheduler.
            seed_days = sorted(d for d, trading in calendar.items() if trading and d < today)[-1:]
            if calendar.get(today):
                seed_days.append(today)
            for seed_day in seed_days:
                for obligation in required:
                    due = obligation.due_at(seed_day, calendar)
                    if due is not None and due <= datetime.now(CN):
                        enqueue_validation(conn, obligation.dataset, seed_day)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""SELECT dataset,target_date FROM market.data_sync_targets
                    WHERE data_source='canonical_db' AND target_scope->>'operation'='VALIDATE_ONLY'
                      AND target_status IN ('pending','retry') AND target_date<=%s
                      AND (next_retry_at IS NULL OR next_retry_at<=clock_timestamp())
                    ORDER BY (dataset=ANY(%s::text[])) DESC,priority,COALESCE(next_retry_at,created_at),target_date LIMIT %s""",
                    (today, [o.dataset for o in required], limit))
                candidates = [dict(r) for r in cur.fetchall()]
        validated = stale = 0
        for candidate in candidates:
            if time.monotonic()-start >= budget_seconds:
                break
            dataset, day = candidate["dataset"], candidate["target_date"]
            claim = self.repository.claim(dataset, day)
            if claim is None:
                continue
            revision = claim["metadata"]["desired_revision"]
            obligation = None
            try:
                with self.repository.transaction(readonly=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute("SET LOCAL statement_timeout='30s'")
                    calendar, schedules, summaries, partitions = self.service._read(conn, day, day)
                    historical = (summaries.get(day, {}).get("summary") or {}).get("obligations", [])
                    catalogue = {o.dataset: o for o in obligations(schedules, historical)[0]}
                    obligation = catalogue.get(dataset)
                    if obligation is None:
                        # Change tracking also covers on-demand facts. They do
                        # not silently become a new daily obligation.
                        result = ValidationResult(PartitionState.UNVERIFIED, (), {}, reason="non_daily_dataset_contract")
                        self.repository.publish(dataset=dataset, day=day, revision=revision,
                            scope_version=SCOPE_VERSION, rule_version=RULE_VERSION,
                            target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result)
                        continue
                    partition = partitions.get((day, dataset))
                    if not partition or partition["data_revision"] != revision:
                        raise IntegrityRevisionChanged("partition changed before validation snapshot")
                    if calendar.get(day) is None or (calendar.get(day) is False and obligation.category not in {"event", "reference"}):
                        result = ValidationResult(PartitionState.UNVERIFIED, (), {}, reason="fact_date_calendar_not_trading")
                    else:
                        result = self.validator.validate(conn, obligation, day, revision)
                self.repository.publish(dataset=dataset, day=day, revision=revision,
                    scope_version=obligation.scope_version, rule_version=obligation.rule_version,
                    target_id=claim["target_id"], token=claim["metadata"]["lease_token"], result=result)
                self._repair_target(obligation, day, result, revision=revision)
                self.rebuild_day(day)
                validated += 1
            except IntegrityRevisionChanged:
                stale += 1  # newer persistent target remains pending
            except Exception:
                # The lease expires and the exact target remains durable. Never
                # kill a process, manufacture success or crash the scheduler.
                logger.exception("daily integrity validation failed dataset=%s fact_date=%s", dataset, day)
                if obligation is not None:
                    try:
                        self.repository.publish(dataset=dataset, day=day, revision=revision,
                            scope_version=obligation.scope_version, rule_version=obligation.rule_version,
                            target_id=claim["target_id"], token=claim["metadata"]["lease_token"],
                            result=ValidationResult(PartitionState.ERROR, (), {}, reason="VALIDATION_EXECUTION_ERROR"))
                        self.rebuild_day(day)
                    except IntegrityRevisionChanged:
                        stale += 1
                    except Exception:
                        logger.exception("daily integrity error verdict could not be persisted dataset=%s fact_date=%s", dataset, day)
        # Projection-only recovery is cheap even if no facts changed; no market
        # scan or repeated validation is initiated by this rebuild.
        self.rebuild_day(today)
        return {"status": "PROCESSED", "validated": validated, "stale_discarded": stale}

    def _repair_target(self, obligation, day, result, *, revision=None):
        """One existing fetch/derived target per day, not a retry per field/bar."""
        dependencies = result.checks.get("required_dependencies") or []
        if dependencies:
            with self.repository.transaction() as conn:
                for dataset in dependencies:
                    enqueue_validation(conn, dataset, day)
            return
        if (result.state not in {PartitionState.INCOMPLETE, PartitionState.WAIT_UPSTREAM}
                and result.reason != "complete_source_scan_not_bound_to_revision"):
            return
        from backend.services.data_sync_targets import DataSyncTargetRecord, DataSyncTargetRepository
        dataset = "stock_moneyflow_ts" if obligation.dataset == "moneyflow_ts" else obligation.dataset
        if dataset in {"sw_index_classify", "sw_index_member"}:
            dataset = "sw_sector"
        record = DataSyncTargetRecord(dataset=dataset, data_source="integrity_repair", target_date=day,
            target_scope={"operation": "FETCH", "scope": "exact_fact_date"}, target_status="retry",
            next_retry_at=datetime.now(CN)+timedelta(minutes=30),
            metadata={"schedule_dataset": dataset, "health_dataset": dataset,
                      "integrity_dataset": obligation.dataset, "failure_category": result.reason or "physical_gap"})
        with self.repository.transaction() as conn:
            if revision is not None:
                with conn.cursor() as cur:
                    cur.execute("""SELECT data_revision,validation_status FROM market.dataset_date_refresh_audit
                        WHERE dataset=%s AND trade_date=%s AND data_source='canonical_db' FOR UPDATE""",
                        (obligation.dataset, day))
                    row = cur.fetchone()
                if not row or row[0] != revision or row[1] not in {"INCOMPLETE", "WAIT_UPSTREAM", "UNVERIFIED"}:
                    return  # a later writer/verifier already moved this partition
            DataSyncTargetRepository().upsert_target(record, conn=conn)

    def rebuild_day(self, day: date):
        with self.repository.transaction() as conn:
            inputs = self.service._read(conn, day, day)
            now = datetime.now(CN)
            current = self.service._day(day, *inputs, now)
            if current["state"] == "NOT_TRADING":
                return None
            previous = (inputs[2].get(day, {}).get("summary") or {})
            confirmed = any((c.get("missing") or 0) + (c.get("invalid") or 0) > 0
                for p in current["partitions"] for c in ((p.get("metadata") or {}).get("validation") or {}).get("counts", []))
            content = {key: current[key] for key in ("state", "all_sync_complete", "watermark", "catalogue_id", "obligations")}
            content.update(ever_incomplete=bool(confirmed or previous.get("ever_incomplete")),
                           ever_complete=bool(current["all_sync_complete"] or previous.get("ever_complete")))
            content["first_completed_at"] = previous.get("first_completed_at")
            content["last_completed_at"] = previous.get("last_completed_at")
            content["gap_since_complete"] = False if current["all_sync_complete"] else bool(
                confirmed or previous.get("gap_since_complete"))
            content["partitions"] = [{key: p.get(key) for key in
                ("dataset", "data_revision", "validated_revision", "validation_id", "scope_version", "rule_version", "state")}
                for p in current["partitions"]]
            if current["all_sync_complete"]:
                # Retain completion semantics on an idempotent projection.
                if previous.get("watermark") == current["watermark"] and previous.get("all_sync_complete"):
                    content.update({key: previous.get(key) for key in ("completion_kind", "observed_on_time")})
                else:
                    content["first_completed_at"] = previous.get("first_completed_at") or now.isoformat()
                    content["last_completed_at"] = now.isoformat()
                    due = max((p["due_at"] for p in current["partitions"] if p["due_at"]), default=None)
                    kind, on_time = completion_kind(previous, as_of=now, due_at=due,
                        timing_known=bool(due and now<=due))
                    content.update(completion_kind=kind, observed_on_time=on_time)
            return self.repository.append_day(conn, day, content)
