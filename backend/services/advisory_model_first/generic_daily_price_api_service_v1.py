"""F-791..797: original daily DB inputs to one explicit fixed-5TD price model."""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import os
import re
import time
from zoneinfo import ZoneInfo

from backend.services.advisory_model_first.entry_price_daily_service import EntryWorkBudget, BoundedEntryReadSession
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_daily_price_context_v1 import (
    GenericDailyPriceContextV1, project_generic_raw_price_sets_v1,
)
from backend.services.advisory_model_first.generic_daily_published_list_v1 import GenericDailyPublishedListSourceV1
from backend.services.advisory_model_first.generic_daily_price_input_v1 import ROSTER, _day, _records
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY, POLICY_SHA256
from backend.services.advisory_model_first.generic_price_set_consumer_v1 import _fail, _object, _read, load_generic_price_set_model_v1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def _envelope(status, **values):
    return dict(schema_version="generic_daily_price_api_v1", status=status, days=[], complete=False,
        holding_sessions=5, label_contract=POLICY["label_contract"], policy_sha256=POLICY_SHA256,
        evidence_use="NAVIGATION_ONLY", economic_confirmation=False, deployable=False,
        hypothetical_price_not_order=True, fit_count=0, outcomes_read=False, database_write=False,
        model_activation=False, qualification_rechecked=False, **values)


class _CountingCursor:
    def __init__(self, cursor, count):
        self._cursor, self._count = cursor, count

    def execute(self, sql, params=None):
        normalized = re.sub(r"^\s*(/\*.*?\*/\s*)*", "", str(sql), flags=re.S).upper()
        if normalized.startswith("SELECT"):
            self._count["selects"] += 1
        return self._cursor.execute(sql, params)

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self._cursor.close()

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _CountingConnection:
    def __init__(self, connection, count):
        self._connection, self._count = connection, count

    def cursor(self, *args, **kwargs):
        return _CountingCursor(self._connection.cursor(*args, **kwargs), self._count)

    def __getattr__(self, name):
        return getattr(self._connection, name)


class _PinnedReadSession:
    """Borrow the existing snapshot; only the outer owner may close or rollback."""
    def __init__(self, connection, budget):
        self._connection, self.budget = connection, budget

    @contextmanager
    def connection(self, **_kwargs):
        self.budget.check()
        yield self._connection
        self.budget.check()

    def close(self):
        pass  # Deliberately not the owner; F-793.


def _db_input(pinned, now):
    # The separately delivered input source is never copied into this API PR.
    from backend.services.advisory_model_first.generic_daily_db_input_v1 import GenericDailyReadonlyDBInputV1
    return GenericDailyReadonlyDBInputV1(session_factory=lambda: pinned, now=lambda: now)


class GenericDailyPriceAPIServiceV1:
    def __init__(self, *, config_path=None, model_loader=None, session_factory=None,
                 list_source_factory=None, input_factory=None, context_factory=None, now=None, monotonic=None):
        self._config_path = config_path
        self._model_loader = model_loader or load_generic_price_set_model_v1
        self._session_factory = session_factory or (lambda budget: BoundedEntryReadSession(budget))
        self._list_source = list_source_factory or (lambda pinned: GenericDailyPublishedListSourceV1(read_session=pinned))
        self._input = input_factory or _db_input
        self._contexts = context_factory or (lambda pinned, now: GenericDailyPriceContextV1(read_session=pinned, now=lambda: now))
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._clock = monotonic or time.monotonic

    def read_price(self, *, program_id, target_date=None, list_version_id=None):
        request = dict(target_trade_date=target_date, list_version_id=list_version_id)
        result = self.read_batch(program_id=program_id, requests=[request])
        return result

    def read_batch(self, *, program_id, requests):
        budget = EntryWorkBudget(30., monotonic=self._clock)
        now = self._now()
        if not isinstance(now, datetime) or now.utcoffset() is None:
            _fail("generic daily API consumption clock is not aware")
        today = now.astimezone(ZoneInfo("Asia/Shanghai")).date()
        if not isinstance(program_id, str) or not program_id.strip() or len(program_id) > 128:
            _fail("generic daily API program identity is malformed")
        if not isinstance(requests, (list, tuple)) or not 1 <= len(requests) <= 20:
            _fail("generic daily API needs one to twenty original list queries")
        queries = []
        for request in requests:
            if not isinstance(request, dict) or set(request)-{"target_trade_date", "list_version_id"}:
                _fail("generic daily API original list query schema differs")
            target = _day(request.get("target_trade_date"), nullable=True)
            version = request.get("list_version_id")
            if target is not None and target > today or version is not None and (
                    not isinstance(version, str) or not version.strip() or len(version) > 128):
                _fail("generic daily API target is future or list identity malformed")
            queries.append(dict(target_date=target, list_version_id=version))
        if len({(q["target_date"], q["list_version_id"]) for q in queries}) != len(queries):
            _fail("generic daily API contains duplicate original list requests")
        path = self._config_path if self._config_path is not None else os.environ.get("AISTOCK_ADVISORY_GENERIC_PRICE_CONFIG")
        if not path:
            return _envelope("NOT_CONFIGURED", program_id=program_id, database_read=False)
        try:
            body = _read(path, 1024**2)
            config = _object(body)
            if (set(config) != {"schema_version", "model_family", "trained_manifest_ref", "policy_sha256"}
                    or config["schema_version"] != "generic_daily_price_api_config_v1"
                    or config["model_family"] != "DAILY_5TD" or config["policy_sha256"] != POLICY_SHA256):
                _fail("generic daily API configuration family/policy/schema differs")
            loaded = self._model_loader(model_family=config["model_family"], trained_manifest_ref=config["trained_manifest_ref"])
        except (AdvisoryModelFirstError, OSError, TypeError, ValueError) as error:
            raise AdvisoryModelFirstError("generic daily API explicit configuration/model is invalid",
                reason_code="ADVISORY_GENERIC_DAILY_INVALID_CONFIGURATION") from error
        originals, count, session = [], {"selects": 0}, None
        try:
            budget.check()
            session = self._session_factory(budget)
            with session.connection() as connection:
                pinned = _PinnedReadSession(_CountingConnection(connection, count), budget)
                source = self._list_source(pinned)
                for query in queries:
                    originals.append(source.load_day(program_id=program_id, **query))
                    budget.check()
                packets = [value["packet"] for value in originals]
                days = [(_day(p["decision_date"]), _day(p["target_date"])) for p in packets]
                if len(set(days)) != len(days) or any(t > today or d >= t for d, t in days):
                    _fail("generic daily API resolved duplicate/future original dates")
                features = self._input(pinned, now).load_batch(packets=packets)
                contexts = self._contexts(pinned, now).load_batch(packets=packets)
                if len(features) != len(packets) or len(contexts) != len(packets):
                    _fail("generic daily API dependency returned an incomplete batch")
                budget.check()
            session.close()
            session = None
            results = []
            for original, (frame, input_receipt), price in zip(originals, features, contexts, strict=True):
                receipt = original["candidate_receipt"]
                packet = original["packet"]
                if (sha(_records(frame.loc[:, ROSTER])) != receipt["candidate_roster_sha256"]
                        or input_receipt["candidate_roster_sha256"] != receipt["candidate_roster_sha256"]):
                    _fail("generic daily API input changed the original published candidate roster")
                context = dict(**packet["metadata"], source_evidence="CURRENT_DATABASE_NON_VINTAGE",
                    feature_visible_through=input_receipt["context"]["source_visible_through"])
                projection = project_generic_raw_price_sets_v1(loaded=loaded, candidate_rows=frame,
                    raw_price_contexts=price["contexts"], source_context=context,
                    budget_seconds=min(30., budget.remaining()), monotonic=self._clock)
                if input_receipt["status"] == "DEFERRED_D_NOT_CLOSED":
                    projection["status"] = "DEFERRED_D_NOT_CLOSED"
                projection.update(decision_date=receipt["decision_date"], target_date=receipt["target_date"],
                    candidate_receipt=receipt, input_receipt=input_receipt, price_context_receipt=price["receipt"],
                    original_items=receipt["original_items"], unmodeled_items=receipt["unmodeled_items"], database_read=True,
                    target_calendar_verified=input_receipt["calendar_verified"])
                results.append(projection)
                budget.check()
            data_selects = max((v[1]["query_count"] for v in features), default=0)
            legal_selects = max((v["receipt"]["query_count"] for v in contexts), default=0)
            result = _envelope("COMPUTED", program_id=program_id, database_read=True,
                configuration_sha256=hashlib.sha256(body).hexdigest(), original_lists=[v["candidate_receipt"] for v in originals],
                query_counts=dict(total_selects=count["selects"], shared_nine_field_selects=data_selects,
                    shared_legal_context_selects=legal_selects,
                    original_list_and_identity_selects=count["selects"]-data_selects-legal_selects))
            result.update(days=results, complete=True)
            states = {value["status"] for value in results}
            if states in ({"NO_CANDIDATES"}, {"DEFERRED_D_NOT_CLOSED"}):
                result["status"] = next(iter(states))
            budget.check()
            return result
        except AdvisoryModelFirstError as error:
            if error.reason_code != "ADVISORY_ENTRY_PRICE_DEFERRED_BUDGET":
                raise
            return _envelope("DEFERRED_BUDGET", program_id=program_id, database_read=session is not None or bool(originals),
                original_lists=[v["candidate_receipt"] for v in originals], reason_code=error.reason_code)
        finally:
            if session is not None:
                session.close()
