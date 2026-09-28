from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, time as wall_time, timezone
import json
import math
import os
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from .errors import AdvisoryModelFirstError


class EntryWorkBudget:
    """One consumer operation budget, propagated to SQL and cooperative CPU blocks."""

    def __init__(self, seconds=30.0, *, monotonic=None):
        if not 0 < seconds <= 30:
            raise ValueError("entry work budget must be in (0, 30] seconds")
        self._clock = monotonic or time.monotonic
        self._deadline = self._clock() + seconds

    def remaining(self):
        remaining = self._deadline - self._clock()
        if remaining <= 0:
            raise AdvisoryModelFirstError("entry work deferred after its bounded budget", reason_code="ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
        return remaining

    def check(self):
        self.remaining()


class _DeadlineCursor:
    def __init__(self, cursor, budget):
        self._cursor, self._budget = cursor, budget

    def execute(self, sql, params=None):
        from psycopg2.errors import QueryCanceled
        milliseconds = max(1, math.floor(self._budget.remaining() * 1000))
        if " ".join(str(sql).upper().split()).startswith("SET LOCAL STATEMENT_TIMEOUT"):
            return self._cursor.execute("SET LOCAL statement_timeout = %s", (min(milliseconds, int(params[0])),))
        self._cursor.execute("SET LOCAL statement_timeout = %s", (milliseconds,))
        try:
            result = self._cursor.execute(sql, params)
        except QueryCanceled as exc:
            raise AdvisoryModelFirstError("entry read query exceeded its remaining budget", reason_code="ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET") from exc
        self._budget.check()
        return result

    def fetchall(self):
        self._budget.check()
        value = self._cursor.fetchall()
        self._budget.check()
        return value

    def fetchone(self):
        self._budget.check()
        value = self._cursor.fetchone()
        self._budget.check()
        return value

    def close(self):
        self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _DeadlineConnection:
    def __init__(self, connection, budget):
        self._connection, self._budget = connection, budget

    def cursor(self, *args, **kwargs):
        self._budget.check()
        return _DeadlineCursor(self._connection.cursor(*args, **kwargs), self._budget)

    def set_session(self, **kwargs):
        if kwargs.get("readonly") is False or kwargs.get("autocommit") is True:
            raise ValueError("entry input connections must remain read-only transactional")
        self._connection.set_session(**dict(kwargs, readonly=True, autocommit=False))

    def rollback(self):
        self._connection.rollback()


class BoundedEntryReadSession:
    """Reuse one explicitly bounded readonly connection; never change the public pool or env."""

    def __init__(self, budget, *, connector=None):
        self.budget, self._connector, self._connection = budget, connector, None
        self._in_transaction = False

    @contextmanager
    def connection(self, **_kwargs):
        self.budget.check()
        if self._in_transaction:
            raise ValueError("entry read snapshots cannot nest transactions")
        if self._connection is None:
            import psycopg2
            from backend.db.pg_pool import _db_cfg
            config = _db_cfg()
            if self.budget.remaining() < 2:
                raise AdvisoryModelFirstError("entry connection deferred before budget exhaustion", reason_code="ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET")
            config.update(connect_timeout=math.floor(min(5, self.budget.remaining())),
                          application_name="AIstock-advisory-entry-readonly")
            connection = (self._connector or psycopg2.connect)(**config)
            try:
                connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
                self.budget.check()
            except Exception:
                connection.close()
                raise
            self._connection = connection
        try:
            self._in_transaction = True
            yield _DeadlineConnection(self._connection, self.budget)
        finally:
            self._in_transaction = False
            self._connection.rollback()

    def close(self):
        if self._connection is not None:
            self._connection.close()
            self._connection = None


class EntryReadOnlyCalendar:
    """Use the authority's DB reader without its optional filesystem-cache refresh side effect."""

    def __init__(self, connection_factory):
        self._connection = connection_factory

    def list_trading_days(self, start_date, end_date, *, allow_empty=False):
        from backend.services.trading_calendar_status import TradingCalendarStatusService
        with self._connection() as connection:
            return TradingCalendarStatusService.list_trading_days_from_conn(connection, start_date, end_date, allow_empty=allow_empty)

    def next_trading_day(self, anchor_date, *, inclusive=False):
        from datetime import timedelta
        start = anchor_date if inclusive else anchor_date + timedelta(days=1)
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT MIN(cal_date) FROM market.trading_calendar WHERE cal_date >= %s AND is_trading = TRUE", (start,))
                row = cursor.fetchone()
        if not row or row[0] is None:
            raise AdvisoryModelFirstError("authoritative next trading day is unavailable", reason_code="ADVISORY_ENTRY_PRICE_CALENDAR_UNAVAILABLE")
        target = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0]))
        # The authority validates every intervening date; no weekday inference or gap skipping.
        self.list_trading_days(start, target)
        return target


def build_bounded_entry_services(session: BoundedEntryReadSession):
    from functools import lru_cache
    from backend.services.advisory_program import AdvisoryProgramPGRepository, AdvisoryProgramService, list_item_to_dict, list_version_to_dict
    from backend.services.selection_center.repository import SelectionCenterRepository
    from backend.services.canonical_equity_pit import CanonicalPitAuthorityResolver
    from .entry_price_service import AdvisoryEntryPriceService
    from .model_bundle import load_frozen_research_bundle
    from .price_range_runtime_bundle import load_frozen_price_range_bundle
    from .realtime_feature_source import PostgresAdvisoryReviewSource, PostgresRealtimeFeatureSource

    class PersistedSelectionReader:
        def __init__(self):
            self._repository = SelectionCenterRepository(conn_factory=session.connection)

        def get_run(self, run_id):
            return self._repository.get_run(run_id)

    class EntryProgramReader(AdvisoryProgramService):
        def validate_entry_assets(self, role):
            from backend.services.advisory_delivery_preflight import AdvisoryDeliveryPreflightService
            from backend.services.strategy_package.service import StrategyPackageService
            from backend.services.strategy_package.repository import StrategyPackageRepository
            from backend.services.strategy_package.asset_eligibility import StrategyPackageAssetEligibilityService
            from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository
            assets = StrategyPackageAssetEligibilityService(
                selection_artifact_reader=StrategyPackageSelectionArtifactRepository(conn_factory=session.connection))
            packages = StrategyPackageService(repository=StrategyPackageRepository(conn_factory=session.connection), asset_eligibility=assets)
            result = AdvisoryDeliveryPreflightService(package_service=packages, program_service=self).preflight(
                package_id=role.scope.package_id, universe_selection=role.scope.universe_selection.model_dump(mode="json"),
                target_count=role.scope.target_count, program_id=role.program_id)
            if (result["blockers"] or result["overall_status"] not in {"READY_WITH_MODEL", "READY_BASELINE_ONLY"}
                    or result["package"]["manifest_sha256"] != role.scope.package_manifest_sha256):
                _daily_error("daily entry assets are retired or incompatible")
            session.budget.check()

        def recommendation_list_version_detail(self, list_version_id):
            # Scoring consumes persisted identities, not optional display-name enrichment lookups.
            version = self.repository.get_list_version(list_version_id)
            return {"list_version": list_version_to_dict(version),
                    "items": [list_item_to_dict(row) for row in self.repository.list_version_items(list_version_id)]}

    selection = PersistedSelectionReader()
    programs = EntryProgramReader(repository=AdvisoryProgramPGRepository(conn_factory=session.connection),
                                  selection_service=selection, calendar_provider=EntryReadOnlyCalendar(session.connection))
    entry = AdvisoryEntryPriceService(
        program_service=programs, selection_service=selection,
        review_source=PostgresAdvisoryReviewSource(connection_context_factory=session.connection),
        feature_source=PostgresRealtimeFeatureSource(connection_context_factory=session.connection,
                                                   statement_timeout_ms=30_000, deadline_check=session.budget.check),
        parent_loader=lru_cache(maxsize=1)(load_frozen_research_bundle),
        price_loader=lru_cache(maxsize=1)(load_frozen_price_range_bundle),
        pit_authority_resolver=CanonicalPitAuthorityResolver(connection_factory=session.connection),
    )
    return programs, entry


class QEEntryResourceGuard:
    """Consumer-side complete snapshot check, deliberately not an exclusive cross-module lease."""

    TERMINAL = frozenset({"completed", "succeeded", "success", "failed", "cancelled", "canceled", "stopped", "aborted"})
    ACTIVE = frozenset({"running", "pending", "queued", "registered", "starting", "submitted", "preparing", "stopping", "paused", "waiting"})

    def __init__(self, *, api_base_url=None, get_json=None):
        self._base = (api_base_url or os.getenv("AISTOCK_ADVISORY_QE_READ_API_BASE_URL", "http://127.0.0.1:8001/api/v1")).rstrip("/")
        self._reader = get_json

    def check(self, budget):
        import httpx
        try:
            with httpx.Client(timeout=min(5, budget.remaining()), follow_redirects=False, trust_env=False) as client:
                def get(path, params):
                    budget.check()
                    if self._reader is not None:
                        return self._reader(path, params)
                    response = client.get(self._base + path, params=params, timeout=min(5, budget.remaining()))
                    response.raise_for_status()
                    result = response.json()
                    budget.check()
                    return result

                offset, total, seen, task_states = 0, None, set(), {}
                while True:
                    response = get("/quantevolver/experiments", {"limit": 200, "offset": offset, "include_children": "true", "detail": "summary"})
                    if (response.get("ok") is not True or not isinstance(response.get("items"), list)
                            or not isinstance(response.get("total"), int) or response["total"] < 0
                            or response.get("offset") != offset or not isinstance(response.get("has_more"), bool)):
                        return _resource_wait("QE_PAGE_IDENTITY_UNKNOWN")
                    if total is not None and total != response["total"]:
                        return _resource_wait("QE_PAGE_SET_CHANGED")
                    total = response["total"]
                    items = response["items"]
                    for item in items:
                        identity = item.get("experiment_id")
                        if not isinstance(identity, str) or not identity or identity in seen:
                            return _resource_wait("QE_PAGE_IDENTITY_UNKNOWN")
                        seen.add(identity)
                        if not self._terminal_or_unsubmitted(item, get=get, task_states=task_states):
                            return _resource_wait("QE_TASK_NONTERMINAL_OR_UNKNOWN")
                    offset += len(items)
                    if not response["has_more"]:
                        if offset != total:
                            return _resource_wait("QE_PAGE_SET_INCOMPLETE")
                        return {"status": "READ_SNAPSHOT_IDLE", "checked_experiments": offset, "exclusive_lease": False}
                    if not items or offset >= total:
                        return _resource_wait("QE_PAGE_SET_INCOMPLETE")
        except Exception as exc:
            if isinstance(exc, AdvisoryModelFirstError) and exc.reason_code == "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET":
                raise
            # Unreachable/invalid QE control-plane responses cannot become an idle assertion.
            return {**_resource_wait("QE_RESOURCE_READ_UNAVAILABLE"), "error_type": type(exc).__name__}

    def _terminal_or_unsubmitted(self, item, *, get, task_states):
        from urllib.parse import quote
        canonical = str(item.get("canonical_status") or "").lower()
        raw = str(item.get("status") or "").lower()
        progress = item.get("progress_summary") or {}
        if not isinstance(progress, dict):
            return False
        progress_status = str(progress.get("status") or "").lower()
        if canonical in self.ACTIVE or raw in self.ACTIVE or progress_status in self.ACTIVE:
            return False
        if canonical in self.TERMINAL and (not progress_status or progress_status in self.TERMINAL):
            return True
        task_id = item.get("qe_task_id")
        if (canonical == "planned" and not task_id and not progress
                and item.get("startable") is True and item.get("editable") is True):
            return True
        # A completed custom-evolution template can remain 'created/planned' in the experiment list.
        if canonical != "planned" or not isinstance(task_id, str) or not task_id or progress.get("kind") != "evolution":
            return False
        if progress.get("task_id") != task_id or progress_status not in self.TERMINAL:
            return False
        if task_id not in task_states:
            response = get(f"/quantevolver/evolution/tasks/{quote(task_id, safe='')}", {"detail": "summary"})
            data = response.get("data") or {}
            if response.get("status") != "success" or data.get("task_id") != task_id:
                return False
            task_states[task_id] = str(data.get("status") or "").lower()
        return task_states[task_id] in self.TERMINAL


def _resource_wait(reason):
    return {"status": "WAITING_RESOURCE", "reason_code": reason, "exclusive_lease": False}


class AdvisoryEntryPriceDailyService:
    """Independent immutable entry-price snapshots. Construction and reads have no writes."""

    def __init__(self, *, model_root=None, program_service=None, day_service=None, outcome_source=None,
                 role_store=None, resource_guard=None, confirmation_reader=None, now_provider=None,
                 budget_factory=None, provider_factory=None):
        from .entry_price_role_binding import EntryPriceRoleStore
        from .entry_price_confirmation import read_confirmed_entry_price_artifact
        self._root = str(model_root or os.getenv("AISTOCK_ADVISORY_MODEL_ROOT", "")).strip()
        self._programs, self._entry, self._outcomes = program_service, day_service, outcome_source
        self._roles = role_store or EntryPriceRoleStore()
        self._resource = resource_guard or QEEntryResourceGuard()
        self._confirmation = confirmation_reader or read_confirmed_entry_price_artifact
        self._now = now_provider or (lambda: datetime.now(timezone.utc))
        self._budget = budget_factory or EntryWorkBudget
        self._providers = provider_factory or build_bounded_entry_services
        self._last_attempt = {"capture": {}, "settlement": {}}

    @contextmanager
    def _context(self, budget):
        from .entry_price_confirmation import PostgresEntryPriceConfirmationOutcomeSource
        session = BoundedEntryReadSession(budget)
        try:
            if self._programs is None or self._entry is None:
                programs, entry = self._providers(session)
            else:
                programs, entry = self._programs, self._entry
            outcomes = self._outcomes or PostgresEntryPriceConfirmationOutcomeSource(connection_context_factory=session.connection)
            yield programs, entry, outcomes
        finally:
            session.close()

    def _role(self, programs, program_id):
        if not self._root:
            return None
        binding = programs.active_binding(program_id)
        value = self._roles.read(model_root=self._root, program_id=program_id,
                                 binding_version_id=binding["binding_version_id"])
        if not value or not value[1]["enabled"]:
            return None
        role = value[0]
        program = programs.get_program(program_id)
        if (tuple(binding.get("package_ids") or ()) != (role.scope.package_id,)
                or tuple(program.package_ids) != (role.scope.package_id,)
                or program.review_policy_sha256 != role.scope.review_policy_sha256
                or program.target_count != role.scope.target_count
                or binding.get("universe_selection") != role.scope.universe_selection.model_dump(mode="json")):
            _daily_error("current Program differs from the independent entry role")
        if hasattr(programs, "validate_entry_assets"):
            programs.validate_entry_assets(role)
        return value

    def _path(self, role, target, stage):
        from .entry_price_role_binding import EntryPriceRoleStore
        EntryPriceRoleStore.directory(model_root=self._root, program_id=role.program_id,
                                      binding_version_id=role.binding_version_id)
        if stage not in {"predictions", "settlements"} or not isinstance(target, date):
            _daily_error("invalid daily artifact identity")
        root = Path(self._root).resolve()
        path = root / f"entry_price_daily_{stage}" / role.role_sha256 / target.isoformat() / "artifact.json"
        if path.resolve() != path.absolute() or not path.resolve().is_relative_to(root):
            _daily_error("daily artifact path cannot be redirected")
        return path

    def _read(self, role, target, stage):
        path = self._path(role, target, stage)
        if not path.exists():
            return None
        from .price_range_contracts import canonical_json_sha256
        from .entry_price_confirmation_contracts import EntryPriceConfirmationPredictionDay, EntryPriceConfirmationSettlementDay, validate_entry_projection
        payload = json.loads(path.read_text(encoding="utf-8"))
        digest = payload.pop("artifact_sha256", None)
        if (canonical_json_sha256(payload) != digest or payload.get("role_sha256") != role.role_sha256
                or payload.get("program_id") != role.program_id or payload.get("binding_version_id") != role.binding_version_id
                or payload.get("target_trade_date") != target.isoformat()
                or payload.get("schema_version") != f"advisory_entry_daily_{stage}_v1"):
            _daily_error("daily artifact hash or scope mismatch")
        published = datetime.fromisoformat(payload["published_at"])
        if published.utcoffset() is None or payload.get("evidence_level") != "PROSPECTIVE_OOS":
            _daily_error("daily artifact must have a real publication clock")
        if stage == "predictions":
            predicted = EntryPriceConfirmationPredictionDay.model_validate(payload["prediction"])
            envelope = predicted.envelope
            if (published >= _open(target) or published < role.created_at or target < role.effective_from_target_date
                    or envelope.role_binding_sha256 != role.role_sha256 or envelope.program_id != role.program_id
                    or envelope.binding_version_id != role.binding_version_id or envelope.target_trade_date != target
                    or envelope.evidence_state != "CONFIRMED_PRICE_DISTRIBUTION"):
                _daily_error("daily prediction differs from role or capture clock")
            expected = {name: getattr(role.scope, name) for name in (
                "package_id", "package_manifest_sha256", "style_profile_hash", "review_policy_sha256",
                "universe_identity_sha256", "candidate_projection_sha256", "feature_schema_sha256",
                "price_range_bundle_id", "price_range_bundle_manifest_sha256",
            )}
            if (any(getattr(envelope, name) != value for name, value in expected.items())
                    or envelope.training_lineage is None
                    or envelope.training_lineage.parent_bundle_id != role.scope.parent_bundle_id
                    or envelope.training_lineage.outcome_bundle_id != role.scope.outcome_bundle_id):
                _daily_error("daily prediction identity differs from the confirmed role scope")
            for candidate, row in zip(envelope.candidates, predicted.rows):
                if candidate.entry_price.status == "AVAILABLE":
                    validate_entry_projection(candidate, payload["control_gaps"], row.control_range)
        else:
            settled = EntryPriceConfirmationSettlementDay.model_validate(payload["settlement"])
            if settled.target_trade_date != target or published < _mature(target):
                _daily_error("daily settlement clock or target mismatch")
            prediction = self._read(role, target, "predictions")
            if prediction is None or prediction["artifact_sha256"] != payload.get("prediction_sha256"):
                _daily_error("daily settlement does not bind the original prediction")
            if tuple(row.symbol for row in settled.outcomes) != tuple(row["symbol"] for row in prediction["prediction"]["rows"]):
                _daily_error("daily settlement roster differs from prediction")
        return {**payload, "artifact_sha256": digest}

    def _publish(self, role, target, stage, values, budget):
        from .entry_price_confirmation import _immutable_json
        from .price_range_contracts import canonical_json_sha256
        payload = dict(values, schema_version=f"advisory_entry_daily_{stage}_v1", role_sha256=role.role_sha256,
                       program_id=role.program_id, binding_version_id=role.binding_version_id,
                       target_trade_date=target.isoformat(), evidence_level="PROSPECTIVE_OOS",
                       published_at=self._now().isoformat())
        budget.check()
        if stage == "predictions" and self._now() >= _open(target):
            return _daily_state("MISSED_CAPTURE", role, target)
        payload["artifact_sha256"] = canonical_json_sha256(payload)
        _immutable_json(self._path(role, target, stage), payload)
        return self._read(role, target, stage)

    def _confirmed_request(self, role, budget):
        from .research_control import evidence_reference_for_file
        budget.check()
        if evidence_reference_for_file(role.confirmation.artifact_uri, role=role.confirmation.role) != role.confirmation:
            _daily_error("confirmation reference changed")
        request, _ = self._confirmation(role.confirmation.artifact_uri, recompute=False)
        if request.request_sha256 != role.confirmation_request_sha256 or request.scope != role.scope:
            _daily_error("confirmation no longer matches the role")
        budget.check()
        return request

    def capture(self, *, program_id, target_trade_date, budget=None):
        budget = budget or self._budget()
        with self._context(budget) as (programs, entry, _outcomes):
            resolved = self._role(programs, program_id)
            if not resolved:
                return _daily_state("NOT_CONFIGURED", program_id=program_id)
            role, pointer = resolved
            return self._capture(programs, entry, role, pointer, target_trade_date, budget)

    def _capture(self, programs, entry, role, pointer, target, budget):
        from .entry_price_confirmation import _prediction_day
        existing = self._read(role, target, "predictions")
        if existing:
            return existing
        if self._now() >= _open(target):
            return _daily_state("MISSED_CAPTURE", role, target)
        if target < role.effective_from_target_date or self._now() < role.created_at:
            return _daily_state("NOT_EFFECTIVE", role, target)
        resource = self._resource.check(budget)
        if resource["status"] != "READ_SNAPSHOT_IDLE":
            return {**_daily_state("WAITING_RESOURCE", role, target), **resource}
        request = self._confirmed_request(role, budget)
        result = entry.evaluate_day(model_root=self._root, program_id=role.program_id,
            binding_version_id=role.binding_version_id, target_trade_date=target, scope=role.scope,
            role_binding_sha256=role.role_sha256, evidence_state="CONFIRMED_PRICE_DISTRIBUTION", capture_as_of=self._now())
        budget.check()
        prediction = _prediction_day(request, result)
        # Recheck the public snapshot after the bounded computation; this is not a mutual-exclusion claim.
        resource = self._resource.check(budget)
        if resource["status"] != "READ_SNAPSHOT_IDLE":
            return {**_daily_state("WAITING_RESOURCE", role, target), **resource}
        path = self._path(role, target, "predictions")
        active = self._roles.directory(model_root=self._root, program_id=role.program_id,
                                       binding_version_id=role.binding_version_id) / "active.json"
        with _daily_lock(active, budget), _daily_lock(path, budget):
            existing = self._read(role, target, "predictions")
            if existing:
                return existing
            current = self._role(programs, role.program_id)
            if current is None or current[1] != pointer:
                _daily_error("entry pointer changed during capture")
            return self._publish(role, target, "predictions", {
                "status": "NO_CANDIDATES" if prediction.envelope.candidate_count == 0 else "PUBLISHED", "prediction": prediction.model_dump(mode="json"),
                "decision_as_of_trade_date": prediction.envelope.decision_as_of_trade_date.isoformat(),
                "input_identity": result.input_identity,
                "control_gaps": [request.control.q10, request.control.q50, request.control.q90],
            }, budget)

    def settle(self, *, program_id, target_trade_date, budget=None, role=None):
        budget = budget or self._budget()
        with self._context(budget) as (programs, _entry, outcomes):
            if role is None:
                resolved = self._role(programs, program_id)
                if not resolved:
                    return _daily_state("NOT_CONFIGURED", program_id=program_id)
                role = resolved[0]
            if role.program_id != program_id:
                _daily_error("settlement role belongs to another Program")
            return self._settle(outcomes, role, target_trade_date, budget)

    def _settle(self, outcomes, role, target, budget):
        from .entry_price_confirmation_contracts import EntryPriceConfirmationPredictionDay
        from .entry_price_confirmation import entry_price_observation_metrics, _means
        existing = self._read(role, target, "settlements")
        if existing:
            return existing
        prediction = self._read(role, target, "predictions")
        if prediction is None:
            return _daily_state("NOT_CAPTURED", role, target)
        if self._now() < _mature(target):
            return _daily_state("WAITING_MATURITY", role, target)
        resource = self._resource.check(budget)
        if resource["status"] != "READ_SNAPSHOT_IDLE":
            return {**_daily_state("WAITING_RESOURCE", role, target), **resource}
        frozen = EntryPriceConfirmationPredictionDay.model_validate(prediction["prediction"])
        symbols = tuple(row.symbol for row in frozen.rows)
        settlement = outcomes.load(symbols=symbols, target_trade_date=target)
        if settlement.target_trade_date != target or tuple(row.symbol for row in settlement.outcomes) != symbols:
            _daily_error("outcomes differ from the frozen daily candidate roster")
        metrics = [entry_price_observation_metrics(candidate, continuous, outcome, prediction["control_gaps"])
                   for candidate, continuous, outcome in zip(frozen.envelope.candidates, frozen.rows, settlement.outcomes)
                   if candidate.entry_price.status == "AVAILABLE" and outcome.market_status == "AVAILABLE"]
        unknown = sum(row.market_status == "UNAVAILABLE" for row in settlement.outcomes)
        values = dict(status="FAILED_INPUT" if unknown else "SETTLED", prediction_sha256=prediction["artifact_sha256"],
                      decision_as_of_trade_date=prediction["decision_as_of_trade_date"],
                      settlement=settlement.model_dump(mode="json"), valid_rows=len(metrics),
                      market_unknown=unknown, suspended=sum(row.market_status == "NOT_APPLICABLE" for row in settlement.outcomes),
                      market_available=sum(row.market_status == "AVAILABLE" for row in settlement.outcomes),
                      metrics=_means(metrics))
        with _daily_lock(self._path(role, target, "settlements"), budget):
            existing = self._read(role, target, "settlements")
            return existing or self._publish(role, target, "settlements", values, budget)

    def read_price(self, *, program_id, target_trade_date=None, list_version_id=None):
        """GET path: persisted snapshot or explicitly uncaptured, never late recomputation."""
        budget = self._budget()
        with self._context(budget) as (programs, _entry, _outcomes):
            if self._root and list_version_id and target_trade_date and self._now() >= _open(target_trade_date):
                return self._read_historical_list(programs, program_id, target_trade_date, list_version_id, budget)
            resolved = self._role(programs, program_id)
            if not resolved:
                return _empty_envelope(program_id, target_trade_date, "ENTRY_PRICE_NOT_CONFIGURED")
            role = resolved[0]
            self._confirmed_request(role, budget)
            target_trade_date = target_trade_date or self._next_target(programs)
            frozen = self._read(role, target_trade_date, "predictions")
            if frozen:
                if list_version_id and frozen["input_identity"]["list_version_id"] != list_version_id:
                    _daily_error("entry snapshot belongs to another recommendation list")
                return frozen["prediction"]["envelope"]
            reason = "ENTRY_PRICE_WAITING_CAPTURE" if self._now() < _open(target_trade_date) else "ENTRY_PRICE_NOT_CAPTURED"
            return _empty_envelope(program_id, target_trade_date, reason, role.binding_version_id)

    def _read_historical_list(self, programs, program_id, target, list_version_id, budget):
        version = programs.recommendation_list_version_detail(list_version_id)["list_version"]
        if (version.get("program_id") != program_id or version.get("list_version_id") != list_version_id
                or str(version.get("target_trade_date") or version.get("trade_date"))[:10] != target.isoformat()):
            _daily_error("historical entry list differs from requested Program/date")
        binding = version["binding_version_id"]
        directory = self._roles.directory(model_root=self._root, program_id=program_id, binding_version_id=binding)
        active = self._roles.read(model_root=self._root, program_id=program_id, binding_version_id=binding)
        matches = []
        versions = directory / "versions"
        if versions.exists():
            for path in versions.iterdir():
                budget.check()
                if path.suffix != ".json":
                    continue
                role = self._roles._read_version(directory, path.stem, program_id, binding)
                frozen = self._read(role, target, "predictions")
                if frozen and frozen["input_identity"].get("list_version_id") == list_version_id:
                    if active and active[0].role_sha256 == role.role_sha256:
                        self._confirmed_request(role, budget)
                        return frozen["prediction"]["envelope"]
                    matches.append((role, frozen))
        if len(matches) > 1:
            _daily_error("historical entry list has ambiguous frozen role versions")
        if matches:
            self._confirmed_request(matches[0][0], budget)
            return matches[0][1]["prediction"]["envelope"]
        return _empty_envelope(program_id, target, "ENTRY_PRICE_NOT_CAPTURED", binding)

    def _next_target(self, programs):
        now = self._now().astimezone(ZoneInfo("Asia/Shanghai"))
        return programs.calendar_provider.next_trading_day(now.date(), inclusive=now < _open(now.date()))

    def _targets(self, role, stage, budget):
        parent = self._path(role, role.effective_from_target_date, stage).parent.parent
        if not parent.exists():
            return []
        dates = []
        for path in parent.iterdir():
            budget.check()
            if path.is_dir():
                try:
                    target = date.fromisoformat(path.name)
                except ValueError:
                    _daily_error("unrecognized daily artifact directory")
                if self._path(role, target, stage).is_file():
                    dates.append(target)
        return sorted(dates)

    def status(self, *, program_id):
        budget = self._budget()
        with self._context(budget) as (programs, _entry, _outcomes):
            resolved = self._role(programs, program_id)
            if not resolved:
                return dict(schema_version="advisory_entry_price_status_v1", configured=False,
                            program_id=program_id, status="NOT_CONFIGURED", database_written=False)
            role, _pointer = resolved
            self._confirmed_request(role, budget)
            captures = self._targets(role, "predictions", budget)
            settlements = self._targets(role, "settlements", budget)
            prediction = self._read(role, captures[-1], "predictions") if captures else None
            settled = [self._read(role, day, "settlements") for day in settlements]
            quality = _quality_readback(settled)
            return dict(schema_version="advisory_entry_price_status_v1", configured=True, program_id=program_id,
                        status="QUALITY_REVIEW_REQUIRED" if quality["review_required"] else "CONFIGURED",
                        role_sha256=role.role_sha256, binding_version_id=role.binding_version_id,
                        bundle_id=role.scope.price_range_bundle_id, scope=role.scope.model_dump(mode="json"),
                        confirmation_state="CONFIRMED_PRICE_DISTRIBUTION", effective_from_target_date=role.effective_from_target_date.isoformat(),
                        latest_prediction=_artifact_summary(prediction), latest_settlement=_artifact_summary(settled[-1] if settled else None),
                        unsettled_targets=[day.isoformat() for day in captures if day not in set(settlements)],
                        quality=quality, database_written=False, binding_activated=False)

    def run_once(self):
        """At most one capture and one settlement after baseline work; all shared reads are bounded."""
        if not self._root or not (Path(self._root) / "entry_price_roles").is_dir():
            return {"status": "NOT_CONFIGURED", "results": []}
        budget, results = self._budget(), []
        try:
            with self._context(budget) as (programs, entry, outcomes):
                captures, settlements = [], self._pending_settlements(budget, results)
                for program in programs.list_programs(include_archived=False):
                    budget.check()
                    if program.status != "ENABLED" or (program.review_schedule or {}).get("frequency") != "daily_after_close":
                        continue
                    try:
                        resolved = self._role(programs, program.program_id)
                        if not resolved:
                            continue
                        role, pointer = resolved
                        target = self._next_target(programs)
                        if target >= role.effective_from_target_date and self._read(role, target, "predictions") is None:
                            captures.append((role, pointer, target))
                    except Exception as exc:
                        budget.check()
                        results.append({"program_id": program.program_id, **_failure(exc)})
                jobs = []
                if captures:
                    role, pointer, target = min(captures, key=lambda row: (self._last_attempt["capture"].get(row[0].program_id, 0), row[0].program_id))
                    jobs.append(("capture", role.program_id, role, target,
                                 lambda r=role, p=pointer, t=target: self._capture(programs, entry, r, p, t, budget)))
                if settlements:
                    role, target = min(settlements, key=lambda row: (self._last_attempt["settlement"].get((row[0].role_sha256, row[1]), 0), row[1]))
                    jobs.append(("settlement", (role.role_sha256, target), role, target,
                                 lambda r=role, t=target: self._settle(outcomes, r, t, budget)))
                for stage, key, role, target, action in sorted(jobs, key=lambda job: self._last_attempt[job[0]].get(job[1], 0)):
                    budget.check()  # An unstarted job keeps its old priority when the preceding one exhausted the cycle.
                    self._last_attempt[stage][key] = time.monotonic()
                    results.append(self._attempt(stage, role, target, action, budget))
        except Exception as exc:
            results.append(_failure(exc, budget))
        return {"status": "CHECKED", "results": results}

    def _pending_settlements(self, budget, failures):
        # A disabled/rolled/rebound Program must not orphan a prediction already published under an old role.
        pending = []
        for version in (Path(self._root).resolve() / "entry_price_roles").glob("*/*/versions/*.json"):
            budget.check()
            program_id, binding_id = version.parent.parent.parent.name, version.parent.parent.name
            try:
                directory = self._roles.directory(model_root=self._root, program_id=program_id, binding_version_id=binding_id)
                role = self._roles._read_version(directory, version.stem, program_id, binding_id)
                for target in self._targets(role, "predictions", budget):
                    if self._now() >= _mature(target) and self._read(role, target, "settlements") is None:
                        pending.append((role, target))
            except Exception as exc:
                budget.check()
                failures.append({"program_id": program_id, "stage": "settlement_scan", **_failure(exc)})
        return pending

    @staticmethod
    def _attempt(stage, role, target, action, budget):
        try:
            value = action()
            return {"stage": stage, **(_artifact_summary(value) or value)}
        except Exception as exc:
            return {**_daily_state("FAILED", role, target), "stage": stage, **_failure(exc, budget)}


def _failure(exc, budget=None):
    if budget is not None:
        try:
            budget.check()
        except AdvisoryModelFirstError as expired:
            exc = expired
    reason = getattr(exc, "reason_code", "ADVISORY_ENTRY_DAILY_INPUT_UNAVAILABLE")
    state = "DEFERRED" if reason in {"ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET", "ADVISORY_ENTRY_DAILY_LOCK_BUSY"} else "FAILED_INPUT"
    if reason == "ADVISORY_MODEL_SELECTION_INPUT_UNAVAILABLE":
        state = "WAITING_INPUT"
    if "AUDIT" in reason or "NOT_READY" in reason or "WAITING" in reason or "NOT_MATURE" in reason:
        state = "WAITING_DATA"
    return {"status": state, "reason_code": reason, "error_type": type(exc).__name__}


def _artifact_summary(value):
    if not value:
        return None
    return {key: value[key] for key in ("program_id", "binding_version_id", "role_sha256", "decision_as_of_trade_date", "target_trade_date", "status", "artifact_sha256", "published_at") if key in value}


def _quality_readback(settled):
    from .entry_price_confirmation import _means
    usable = [row for row in settled if row["status"] == "SETTLED" and row["valid_rows"] > 0]
    metrics = _means([row["metrics"] for row in usable])
    rows = sum(row["valid_rows"] for row in usable)
    sufficient = len(usable) >= 20 and rows >= 300
    market_available = sum(row["market_available"] for row in settled)
    model_availability = sum(row["valid_rows"] for row in settled) / market_available if market_available else None
    width_ratio = metrics["tick_width"] / metrics["control_tick_width"] if metrics and metrics["control_tick_width"] > 0 else None
    review = sufficient and (not 0.75 <= metrics["continuous_coverage"] <= 0.85 or metrics["tick_coverage"] < 0.75
                             or width_ratio is None or width_ratio > 1.25 or model_availability is None or model_availability < 0.95)
    return dict(valid_dates=len(usable), valid_rows=rows, sufficient_for_description=sufficient,
                date_weighted=metrics, failed_input_dates=sum(row["status"] == "FAILED_INPUT" for row in settled),
                business_width_ratio=width_ratio, model_availability=model_availability,
                review_required=review, auto_activation=False, profitability_confirmed=False)


@contextmanager
def _daily_lock(target, budget):
    """Use the descriptor lock's exact OS/file identity but defer immediately if another writer holds it."""
    budget.check()
    lock = target.parent / f".{target.name}.lock"
    if lock.resolve() != lock.absolute():
        _daily_error("daily lock path cannot be redirected")
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open("a+b") as handle:
        if lock.stat().st_size == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            def acquire():
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            def release():
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            def acquire():
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            def release():
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        try:
            acquire()
        except OSError as exc:
            raise AdvisoryModelFirstError("daily entry publication lock is busy", reason_code="ADVISORY_ENTRY_DAILY_LOCK_BUSY") from exc
        try:
            budget.check()
            yield
        finally:
            handle.seek(0)
            release()


def _daily_error(message):
    raise AdvisoryModelFirstError(message, reason_code="ADVISORY_ENTRY_DAILY_IDENTITY_MISMATCH")


def _daily_state(status, role=None, target=None, *, program_id=None):
    return {"status": status, "program_id": role.program_id if role else program_id,
            "role_sha256": role.role_sha256 if role else None, "target_trade_date": target.isoformat() if target else None}


def _open(target):
    return datetime.combine(target, wall_time(9, 30), ZoneInfo("Asia/Shanghai"))


def _mature(target):
    return datetime.combine(target, wall_time(18), ZoneInfo("Asia/Shanghai"))


def _empty_envelope(program_id, target, reason, binding=None):
    from .entry_price_contracts import AdvisoryEntryPriceEnvelopeV2
    return AdvisoryEntryPriceEnvelopeV2(program_id=program_id, binding_version_id=binding, target_trade_date=target,
        availability_status="UNAVAILABLE", auxiliary_availability="UNAVAILABLE", candidate_count=0, available_count=0,
        unavailable_count=0, reason_code=reason, message="independent entry price is not captured for this target").as_payload()
