"""Read-only database-close input for the original frozen 20D L2 risk model.

No monthly release, fallback, model fit, legacy coefficient or business policy.
Historical current-revision input is never labelled historical-as-known.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import datetime
import heapq
import itertools
import math
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import pandas as pd

from backend.services.data_refresh_audit import DataRefreshAuditRepository
from backend.services.hmm_risk import risk_l2_date_inference as core
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_input import SOURCE_START, _collect_domains, _fact_identity
from backend.services.hmm_risk.provider_absence import load_provider_absence_manifest
from backend.services.hmm_risk.security_identity import load_security_source_identity_manifest
from backend.services.hmm_risk.stock_fact_repository import PostgresStockFactReader, StockFactSourceSpec

VERSION = "hmm_risk_l2_database_inference_v1"
REQUIRED_DATASETS = ("kline_daily_raw", "daily_basic", "stock_moneyflow_ts", "stk_limit", "index_daily", "suspend_d")
MODES = {"historical_current_revision": "historical_replay", "forward_current_read_snapshot": "forward"}
SOURCE_FIELDS = {
    "database_target",
    "knowledge_mode",
    "industry_authority",
    "security_identity_manifest",
    "security_identity_sha256",
    "provider_absence_manifest",
    "provider_absence_sha256",
    "universe_key",
    "universe_rule_version",
}
FEATURE_FIELDS = {
    "schema_version",
    "request_sha256",
    "source_commit",
    "model_identity",
    "catalog",
    "feature_names",
    "parameters",
    "rows",
    "as_of",
    "inference_identity",
    "receipt_sha256",
}


def require(condition, message, reason="database_input_invalid"):
    core.require(condition, message, reason)


def validate_request(request):
    core.verify_receipt(request)
    require(
        set(request)
        == {"schema_version", "model_assets", "source", "decision_dates", "display_limit", "receipt_sha256"}
        and request["schema_version"] == VERSION + "_request",
        "explicit database inference request schema differs",
    )
    source, assets, days = request["source"], request["model_assets"], request["decision_dates"]
    require(
        isinstance(source, dict) and set(source) == SOURCE_FIELDS, "database source fields differ; no release fallback"
    )
    require(
        isinstance(assets, dict) and set(assets) == {"source_request_path", "sealed_path", "acceptance_path"},
        "model locators differ",
    )
    target = source["database_target"]
    require(
        isinstance(target, dict)
        and set(target) == {"host", "port", "dbname"}
        and all(isinstance(target[k], str) and target[k] == target[k].strip() and target[k] for k in ("host", "dbname"))
        and type(target["port"]) is int
        and 1 <= target["port"] <= 65535,
        "explicit non-secret database target required",
    )
    require(
        isinstance(source["knowledge_mode"], str) and source["knowledge_mode"] in MODES,
        "explicit knowledge mode required",
    )
    authority = source["industry_authority"]
    require(
        isinstance(authority, dict)
        and isinstance(authority.get("research_basis"), dict)
        and authority["research_basis"].get("active_mode") == MODES[source["knowledge_mode"]],
        "knowledge mode differs from PIT research basis",
        "identity_mismatch",
    )
    require(
        all(
            isinstance(source[k], str) and source[k].strip() == source[k] and source[k]
            for k in ("universe_key", "universe_rule_version")
        ),
        "explicit PIT universe identity required",
    )
    require(
        isinstance(days, list) and days and all(isinstance(d, str) for d in days) and days == sorted(set(days)),
        "dates must be nonempty, sorted, unique",
    )
    require(
        all(core._day(day) > core._day(core.risk.TRAIN_END) for day in days), "decision precedes frozen training end"
    )
    require(
        type(request["display_limit"]) is int and 1 <= request["display_limit"] <= 30, "display limit must be 1..30"
    )
    for key in ("security_identity_sha256", "provider_absence_sha256"):
        core.reader._require_sha256(source[key], key)
    for value in [*assets.values(), source["security_identity_manifest"], source["provider_absence_manifest"]]:
        require(isinstance(value, str) and Path(value).is_absolute(), "explicit absolute authority locator required")
        require(
            not any(core.inputs.reader._is_link(p) for p in (Path(value), *Path(value).parents)),
            "indirect input forbidden",
        )


@contextmanager
def connect(request):
    """Use existing credential configuration; never store credentials in a request."""
    import psycopg2
    from backend.db.pg_pool import _db_cfg

    validate_request(request)
    cfg = _db_cfg()
    target = request["source"]["database_target"]
    require(
        {k: cfg[k] for k in target} == target, "configured database differs from explicit target", "identity_mismatch"
    )
    connection = psycopg2.connect(**cfg)
    try:
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)
        yield connection
    finally:
        try:
            connection.rollback()
        finally:
            connection.close()


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise core.risk.fail("invalid transaction timestamp", "database_input_invalid") from exc
    require(parsed.tzinfo is not None, "timezone-aware transaction timestamp required")
    return parsed


def _plan(request, calendar, cutoff):
    require(
        isinstance(calendar, list)
        and calendar
        and all(isinstance(d, str) for d in calendar)
        and calendar == sorted(set(calendar)),
        "canonical database calendar is empty/duplicated",
    )
    require(
        all(SOURCE_START <= core._day(d) <= core._day(request["decision_dates"][-1]) for d in calendar),
        "database calendar escapes explicit window",
    )
    positions = {day: i for i, day in enumerate(calendar)}
    require(all(day in positions for day in request["decision_dates"]), "decision date is not an open session")
    first = positions[request["decision_dates"][0]]
    require(
        first >= core.CONTEXT_OPEN_DAYS + 1, "full composed feature context unavailable", "source_context_unavailable"
    )
    as_of = {day: calendar[positions[day] - 1] for day in request["decision_dates"]}
    require(
        core._day(max(as_of.values())) <= _timestamp(cutoff).astimezone(ZoneInfo("Asia/Shanghai")).date(),
        "future database as-of date",
    )
    return {
        "as_of": as_of,
        "start": calendar[first - core.CONTEXT_OPEN_DAYS],
        "benchmark_start": calendar[first - core.CONTEXT_OPEN_DAYS - 1],
        "end": max(as_of.values()),
        "calendar": calendar,
        "read_cutoff_at": cutoff,
    }


def _read_plan(request, connection):
    target = request["source"]["database_target"]
    dsn = connection.get_dsn_parameters()
    require(
        {"host": dsn.get("host"), "port": int(dsn.get("port", 0)), "dbname": dsn.get("dbname")} == target,
        "connection database differs from explicit target",
        "identity_mismatch",
    )
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT current_setting('transaction_read_only'), current_setting('transaction_isolation'), transaction_timestamp()"
        )
        readonly, isolation, cutoff = cursor.fetchone()
        require(readonly == "on" and isolation == "repeatable read", "read-only repeatable-read transaction required")
        cursor.execute(
            "SELECT cal_date::date FROM market.trading_calendar WHERE is_trading=true AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
            (SOURCE_START, core._day(request["decision_dates"][-1])),
        )
        calendar = [row[0].isoformat() for row in cursor.fetchall()]
    return _plan(request, calendar, cutoff.isoformat())


def _completion(request, connection, plan):
    if request["source"]["knowledge_mode"] == "historical_current_revision":
        return {"authority": "bounded_fact_validation", "historical_as_known_verified": False, "statuses": []}
    repository = DataRefreshAuditRepository(conn_factory=lambda: nullcontext(connection))
    statuses = []
    for day in sorted(set(plan["as_of"].values())):
        for dataset in REQUIRED_DATASETS:
            try:
                status = repository.require_success(dataset=dataset, trade_date=core._day(day))
            except RuntimeError as exc:
                raise core.risk.fail(f"completion not ready: {dataset}/{day}", "database_data_waiting") from exc
            require(
                status.row_count >= (0 if dataset == "suspend_d" else 1)
                and _timestamp(status.refreshed_at.isoformat()) <= _timestamp(plan["read_cutoff_at"]),
                f"completion invalid: {dataset}/{day}",
                "database_data_waiting",
            )
            statuses.append(
                {
                    "dataset": dataset,
                    "trade_date": day,
                    "status": status.status,
                    "quality_status": status.quality_status,
                    "row_count": status.row_count,
                    "refreshed_at": status.refreshed_at.isoformat(),
                    "job_id": None if status.job_id is None else str(status.job_id),
                }
            )
    return {
        "authority": "market.dataset_date_refresh_audit",
        "historical_as_known_verified": False,
        "statuses": statuses,
    }


def _benchmark(connection, plan, window):
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT trade_date,close FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date",
            (core._day(plan["benchmark_start"]), core._day(plan["end"])),
        )
        rows = cursor.fetchall()
    expected = [d for d in plan["calendar"] if plan["benchmark_start"] <= d <= plan["end"]]
    require([r[0].isoformat() for r in rows] == expected, "CSI300 has missing/duplicate dates")
    require(
        all(v is not None and math.isfinite(float(v)) and float(v) > 0 for _, v in rows),
        "CSI300 close must be finite positive",
    )
    # Same close-to-close formula as the file reader; no pct_chg/label substitute.
    changes = pd.Series([float(v) for _, v in rows], index=[d for d, _ in rows]).pct_change(fill_method=None)
    return {day: float(changes.loc[day]) for day in window}


class _BoundedIndustryProjection:
    """Reuse the file consumer's authority-interval index, not per-row revalidation."""

    def __init__(self, adapter, calendar):
        self.adapter = adapter
        self.index = core.reader._IndustryProjectionIndex(adapter, calendar=calendar)

    @property
    def classification_lookup(self):
        return self.adapter.classification_lookup

    def preflight(self, *args, **kwargs):
        return self.adapter.preflight(*args, **kwargs)

    def resolve(self, symbol, trade_date):
        # The interval's identity is shared; the observation date is not.
        return replace(self.index.resolve(symbol, trade_date), trade_date=trade_date)


def _align_provider_previous_close(connection, day, rows):
    """The training Bin uses provider pre_close, not a corporate-action-blind lag."""
    symbols = sorted(r["symbol"] for r in rows if r.get("prev_close_yuan") is not None)
    if not symbols:
        return rows
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT ts_code,pre_close FROM market.stk_limit WHERE trade_date=%s AND ts_code=ANY(%s) ORDER BY ts_code",
            (day, symbols),
        )
        facts = cursor.fetchall()
    require([r[0] for r in facts] == symbols, "provider pre_close missing/duplicated; lag fallback forbidden")
    require(
        all(value is not None and math.isfinite(float(value)) and float(value) > 0 for _, value in facts),
        "provider pre_close must be finite positive",
    )
    previous = {symbol: float(value) for symbol, value in facts}
    # Preserve original PIT-entry/prewarm NA; do not manufacture a first observation.
    return [{**row, "prev_close_yuan": previous[row["symbol"]]} if row["symbol"] in previous else row for row in rows]


def _aggregates(request, connection, frozen, plan):
    source = request["source"]
    adapter = core.reader._industry_adapter(
        source["industry_authority"], forbidden_roots=(Path(__file__).resolve().parents[3],)
    )
    require(
        list(core.reader._canonical_sector_codes(adapter)[1]) == frozen["catalog"],
        "official L2 text catalog differs",
        "identity_mismatch",
    )
    security = load_security_source_identity_manifest(
        Path(source["security_identity_manifest"]), expected_sha256=source["security_identity_sha256"]
    )
    provider = load_provider_absence_manifest(
        Path(source["provider_absence_manifest"]), expected_sha256=source["provider_absence_sha256"]
    )
    spec = StockFactSourceSpec(
        source["universe_key"],
        source["universe_rule_version"],
        core._day(plan["start"]),
        core._day(plan["end"]),
        SOURCE_START,
    )
    window = tuple(core._day(d) for d in plan["calendar"] if plan["start"] <= d <= plan["end"])
    reader = PostgresStockFactReader(
        connection,
        spec,
        security_identity_manifest=security,
        provider_absence_manifest=provider,
        industry_pit_adapter=_BoundedIndustryProjection(adapter, window),
    )
    source_identity = reader.validate_source()
    pit_preflight = reader.run_industry_pit_preflight(
        window_start=window[0], window_end=window[-1], expected_trading_days=len(window)
    )
    aggregates = {"L2": []}
    evidence = {"l2_domain_receipts": [], "l2_invalid_price_domain": []}
    day_hashes = []

    # Both sources are date ordered; C-013 projection need not preserve the old
    # SQL L1 ordering. Canonicalize within the day before arithmetic and hashing.
    rows = heapq.merge(reader.iter_stock_fact_rows(), reader.iter_missing_price_rows(), key=lambda r: r["trade_date"])
    for day, group in itertools.groupby(rows, key=lambda row: row["trade_date"]):
        values = sorted(group, key=lambda r: (r["l1_code"], r["symbol"], r["l2_code"]))
        require(day in window, "stock facts outside bounded context")
        require(len({r["symbol"] for r in values}) == len(values), "duplicate stock/date input")
        require(
            all(r["l2_code"] in frozen["catalog"] for r in values), "stock industry identity outside official catalog"
        )
        values = _align_provider_previous_close(connection, day, values)
        _collect_domains(
            day, values, eligibility=frozen["eligibility"], aggregates=aggregates, evidence=evidence, levels=("L2",)
        )
        day_hashes.append([day.isoformat(), canonical_sha256(_fact_identity(values))])
    require([d for d, _ in day_hashes] == [d.isoformat() for d in window], "stock-fact calendar has an internal gap")
    reasons = {
        (r["trade_date"], r["sector_code"]): r["price_domain_reason_code"] for r in evidence["l2_invalid_price_domain"]
    }
    return (
        window,
        aggregates["L2"],
        reasons,
        {
            "universe": source_identity,
            "pit_preflight_sha256": canonical_sha256(pit_preflight),
            "source_facts_sha256": canonical_sha256(day_hashes),
            "domain_lineage_sha256": canonical_sha256(evidence),
        },
    )


def prepare(request, source_commit, *, connection=None):
    validate_request(request)
    require(isinstance(source_commit, str) and re.fullmatch(r"[0-9a-f]{40}", source_commit), "executor commit invalid")
    frozen, parameters, model_identity = core.model_source(request)
    context = connect(request) if connection is None else nullcontext(connection)
    # No process-global monkeypatch belongs in this reusable service: it may be
    # called by a multithreaded consumer. The isolated CLI applies action poison.
    with context as active:
        plan = _read_plan(request, active)
        completion = _completion(request, active, plan)
        window, aggregates, reasons, facts_identity = _aggregates(request, active, frozen, plan)
        benchmark = _benchmark(active, plan, window)
    source = request["source"]
    identity = {
        "source_mode": "database_daily",
        "database_target": source["database_target"],
        "knowledge_mode": source["knowledge_mode"],
        "historical_as_known_verified": False,
        "read_cutoff_at": plan["read_cutoff_at"],
        "source_dates": [d.isoformat() for d in window],
        "calendar": plan["calendar"],
        "calendar_sha256": canonical_sha256(plan["calendar"]),
        "completion": completion,
        "industry_authority_sha256": canonical_sha256(source["industry_authority"]),
        "security_identity_sha256": source["security_identity_sha256"],
        "provider_absence_sha256": source["provider_absence_sha256"],
        "benchmark_sha256": canonical_sha256([[d.isoformat(), benchmark[d]] for d in window]),
        **facts_identity,
    }
    return core.features_from_aggregates(
        request,
        source_commit,
        frozen,
        parameters,
        model_identity,
        window,
        aggregates,
        benchmark,
        plan["as_of"],
        identity,
        reasons,
        version=VERSION,
    )


def validate_bundle(request, bundle, expected_sha256):
    validate_request(request)
    core.reader._require_sha256(expected_sha256, "parent database feature bundle SHA256")
    core.verify_receipt(bundle, expected_sha256)
    require(set(bundle) == FEATURE_FIELDS, "feature carrier differs; labels/outcomes forbidden")
    frozen, parameters, model_identity = core.model_source(request)
    source, identity = request["source"], bundle["inference_identity"]
    require(isinstance(identity, dict), "database source identity missing", "identity_mismatch")
    require(
        bundle["schema_version"] == VERSION + "_features"
        and bundle["request_sha256"] == request["receipt_sha256"]
        and bundle["model_identity"] == model_identity
        and bundle["parameters"] == parameters
        and bundle["catalog"] == frozen["catalog"]
        and len(bundle["catalog"]) == 131
        and bundle["catalog"] == sorted(set(bundle["catalog"]))
        and bundle["feature_names"] == list(core.ALL_CORE_FEATURES)
        and isinstance(bundle["source_commit"], str)
        and re.fullmatch(r"[0-9a-f]{40}", bundle["source_commit"])
        and identity["source_mode"] == "database_daily"
        and identity["database_target"] == source["database_target"]
        and identity["knowledge_mode"] == source["knowledge_mode"]
        and identity["historical_as_known_verified"] is False
        and identity["industry_authority_sha256"] == canonical_sha256(source["industry_authority"])
        and identity["security_identity_sha256"] == source["security_identity_sha256"]
        and identity["provider_absence_sha256"] == source["provider_absence_sha256"]
        and identity["universe"]["universe_key"] == source["universe_key"]
        and identity["universe"]["universe_rule_version"] == source["universe_rule_version"],
        "database feature/model/source identity differs",
        "identity_mismatch",
    )
    plan = _plan(request, identity["calendar"], identity["read_cutoff_at"])
    require(
        identity["calendar_sha256"] == canonical_sha256(plan["calendar"])
        and bundle["as_of"] == plan["as_of"]
        and identity["source_dates"] == [d for d in plan["calendar"] if plan["start"] <= d <= plan["end"]],
        "as-of/context/calendar differs",
        "identity_mismatch",
    )
    for field in (
        "source_facts_sha256",
        "pit_preflight_sha256",
        "domain_lineage_sha256",
        "benchmark_sha256",
        "cross_section_sha256",
    ):
        core.reader._require_sha256(identity[field], field)
    completion = identity["completion"]
    require(completion["historical_as_known_verified"] is False, "historical knowledge claim forbidden")
    if source["knowledge_mode"] == "historical_current_revision":
        require(
            completion
            == {"authority": "bounded_fact_validation", "historical_as_known_verified": False, "statuses": []},
            "historical source cannot claim forward completion",
        )
    else:
        expected = [(day, dataset) for day in sorted(set(plan["as_of"].values())) for dataset in REQUIRED_DATASETS]
        statuses = completion["statuses"]
        require(
            completion["authority"] == "market.dataset_date_refresh_audit"
            and [(s["trade_date"], s["dataset"]) for s in statuses] == expected
            and all(
                s["status"] == "success"
                and s["quality_status"] not in {"error", "empty_invalid", "low_coverage"}
                and type(s["row_count"]) is int
                and s["row_count"] >= (0 if s["dataset"] == "suspend_d" else 1)
                and _timestamp(s["refreshed_at"]) <= _timestamp(plan["read_cutoff_at"])
                for s in statuses
            ),
            "database completion carrier differs",
            "database_data_waiting",
        )
    core.validate_feature_rows(request, bundle, plan["as_of"])


def infer(request, bundle, *, expected_bundle_sha256):
    validate_bundle(request, bundle, expected_bundle_sha256)
    return core.predict_prepared(request, bundle, version=VERSION)


def validate_prediction(request, bundle, result):
    validate_bundle(request, bundle, bundle["receipt_sha256"])
    core.validate_prediction(request, bundle, result, version=VERSION)
