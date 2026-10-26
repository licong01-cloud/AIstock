"""Dual-source contracts; no fit, service, real DB write or dataset mutation."""

from copy import deepcopy
from contextlib import nullcontext
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.hmm_risk import risk_l2_database_inference as db
from backend.services.hmm_risk import risk_l2_date_inference as core
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_executor import read_json, write_once
from backend.services.hmm_risk.formal_state_model import FormalStateError
from backend.services.hmm_risk.frozen_l2_history import no_training_or_external_actions
from backend.services.hmm_risk.industry_pit_adapter import HMMIndustryProjection
from backend.tests.hmm_risk.test_risk_l2_date_inference import case as file_case, reseal  # noqa: F401
from scripts.hmm_risk import run_risk_l2_date_inference as cli


def test_database_accepts_only_explicit_pinned_quarter_model(case, tmp_path):
    request, *_ = case
    assets = {
        **request["model_assets"],
        "model_bundle_path": str(tmp_path / "model.json"),
        "model_bundle_sha256": "a" * 64,
    }
    db.validate_request(reseal({**request, "model_assets": assets}))
    with pytest.raises(FormalStateError):
        db.validate_request(reseal({**request, "model_assets": {**assets, "latest": True}}))


class Connection:
    def __init__(self, calendar):
        self.calendar = calendar
        self.readonly, self.isolation = "on", "repeatable read"
        self.cutoff = datetime(2026, 10, 21, 10, tzinfo=timezone.utc)
        self.target = {"host": "127.0.0.1", "port": "5432", "dbname": "aistock"}
        self.queries = []
        self.rollback_count = self.close_count = 0

    def get_dsn_parameters(self):
        return self.target

    def cursor(self):
        connection = self

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def execute(self, sql, args=None):
                connection.queries.append((sql, args))
                self.sql = sql

            def fetchone(self):
                return connection.readonly, connection.isolation, connection.cutoff

            def fetchall(self):
                if "market.trading_calendar" in self.sql:
                    return [(date.fromisoformat(day),) for day in connection.calendar]
                raise AssertionError(self.sql)

        return Cursor()

    def set_session(self, **kwargs):
        assert kwargs == dict(readonly=True, isolation_level="REPEATABLE READ", autocommit=False)

    def rollback(self):
        self.rollback_count += 1

    def close(self):
        self.close_count += 1


@pytest.fixture
def case(file_case):  # noqa: F811 - explicitly reuse the existing file fixture, not its implementation
    file_request, file_bundle = file_case
    source = file_request["source"]
    request = reseal(
        {
            **file_request,
            "schema_version": db.VERSION + "_request",
            "source": {
                "database_target": {"host": "127.0.0.1", "port": 5432, "dbname": "aistock"},
                "knowledge_mode": "historical_current_revision",
                "industry_authority": {"official": "C-013", "research_basis": {"active_mode": "historical_replay"}},
                **{
                    k: source[k]
                    for k in (
                        "security_identity_manifest",
                        "security_identity_sha256",
                        "provider_absence_manifest",
                        "provider_absence_sha256",
                    )
                },
                "universe_key": "aistock_equity_pit_canonical_v2",
                "universe_rule_version": "shsz_a_252td_st_delist_asof_v2",
            },
        }
    )
    calendar = (
        (Path(source["candidate_root"]) / "components/daily_bin_candidate/calendars/day.txt").read_text().splitlines()
    )
    connection = Connection(calendar)
    plan = db._read_plan(request, connection)
    identity = {
        "source_mode": "database_daily",
        "database_target": request["source"]["database_target"],
        "knowledge_mode": "historical_current_revision",
        "historical_as_known_verified": False,
        "read_cutoff_at": plan["read_cutoff_at"],
        "calendar": calendar,
        "calendar_sha256": canonical_sha256(calendar),
        "source_dates": [day for day in calendar if plan["start"] <= day <= plan["end"]],
        "industry_authority_sha256": canonical_sha256(request["source"]["industry_authority"]),
        "security_identity_sha256": source["security_identity_sha256"],
        "provider_absence_sha256": source["provider_absence_sha256"],
        "completion": db._completion(request, connection, plan),
        "universe": {k: request["source"][k] for k in ("universe_key", "universe_rule_version")},
        **{
            k: "a" * 64
            for k in (
                "source_facts_sha256",
                "pit_preflight_sha256",
                "domain_lineage_sha256",
                "benchmark_sha256",
                "cross_section_sha256",
            )
        },
    }
    bundle = reseal(
        {
            **file_bundle,
            "schema_version": db.VERSION + "_features",
            "request_sha256": request["receipt_sha256"],
            "inference_identity": identity,
        }
    )
    return request, bundle, connection, file_request, file_bundle


def test_same_twenty_features_and_frozen_model_produce_identical_predictions(case):
    request, bundle, _, file_request, file_bundle = case
    code = bundle["catalog"][0]
    for carrier in (bundle, file_bundle):
        carrier["rows"]["2026-08-28"][code].update(features=None, reason_code="hmm_risk_stock_fact_suspended")
    bundle, file_bundle = reseal(bundle), reseal(file_bundle)
    with no_training_or_external_actions():
        actual = db.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
        expected = core.infer(file_request, file_bundle, expected_bundle_sha256=file_bundle["receipt_sha256"])
    assert actual["predictions"] == expected["predictions"]
    assert actual["model_identity"] == expected["model_identity"]
    assert actual["inference_identity"] != expected["inference_identity"]
    assert len(actual["predictions"]) == 262 and actual["fits"] == 0
    assert actual["dates"][0]["available_count"] == 130
    db.validate_prediction(request, bundle, actual)


def test_parent_rejects_consistent_but_wrong_probability_from_both_children(case):
    request, bundle, *_ = case
    result = db.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
    row = result["predictions"][0]
    row["probability"] = (row["probability"] + 1.0) / 2.0
    row["warning"] = row["probability"] >= 0.20
    with pytest.raises(FormalStateError, match="frozen parameter readback"):
        db.validate_prediction(request, bundle, reseal(result))


@pytest.mark.parametrize("fault", ["readonly", "isolation", "target", "future", "calendar"])
def test_database_target_transaction_calendar_and_causality_are_mandatory(case, fault):
    request, _, connection, *_ = case
    if fault == "readonly":
        connection.readonly = "off"
    elif fault == "isolation":
        connection.isolation = "read committed"
    elif fault == "target":
        connection.target["dbname"] = "other"
    elif fault == "future":
        connection.cutoff = datetime(2026, 8, 26, tzinfo=timezone.utc)
    else:
        connection.calendar.append(connection.calendar[-1])
    with pytest.raises(FormalStateError):
        db._read_plan(request, connection)


@pytest.mark.parametrize("fault", ["mode", "pit", "secret", "release", "dates", "training", "display"])
def test_request_rejects_fallback_ambiguous_mode_or_changed_business_boundary(case, fault):
    request = deepcopy(case[0])
    if fault == "mode":
        request["source"]["knowledge_mode"] = {}
    elif fault == "pit":
        request["source"]["industry_authority"]["research_basis"]["active_mode"] = "forward"
    elif fault == "secret":
        request["source"]["database_target"]["password"] = "not-a-real-secret"
    elif fault == "release":
        request["source"]["candidate_root"] = "old-release"
    elif fault == "dates":
        request["decision_dates"] = ["2026-08-28", "2026-08-28"]
    elif fault == "training":
        request["decision_dates"] = ["2024-06-28"]
    else:
        request["display_limit"] = 31
    with pytest.raises(FormalStateError):
        db.validate_request(reseal(request))


@pytest.mark.parametrize(
    "fault", ["asof", "context", "calendar_hash", "model", "features", "universe", "forward_claim"]
)
def test_self_rehashed_feature_drift_is_not_authority(case, fault):
    request, original, *_ = case
    bundle = deepcopy(original)
    identity = bundle["inference_identity"]
    if fault == "asof":
        bundle["as_of"]["2026-08-28"] = "2026-08-26"
    elif fault == "context":
        identity["source_dates"].pop(0)
    elif fault == "calendar_hash":
        identity["calendar_sha256"] = "0" * 64
    elif fault == "model":
        bundle["parameters"]["intercept"][0] += 1
    elif fault == "features":
        bundle["rows"]["2026-08-28"][bundle["catalog"][0]]["features"] = [0.0] * 19
    elif fault == "universe":
        identity["universe"]["universe_key"] = "current-listed-stocks"
    else:
        identity["completion"]["historical_as_known_verified"] = True
    bundle = reseal(bundle)
    with pytest.raises(FormalStateError):
        db.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])


def forward_request(request):
    changed = deepcopy(request)
    changed["source"]["knowledge_mode"] = "forward_current_read_snapshot"
    changed["source"]["industry_authority"]["research_basis"]["active_mode"] = "forward"
    return reseal(changed)


@pytest.mark.parametrize("fault", [None, "missing", "future", "empty_price"])
def test_forward_completion_reuses_real_audit_and_accepts_zero_suspensions(case, monkeypatch, fault):
    request, _, connection, *_ = case
    request = forward_request(request)
    plan = db._read_plan(request, connection)
    calls = []

    class Repository:
        def __init__(self, conn_factory):
            with conn_factory() as active:
                assert active is connection

        def require_success(self, *, dataset, trade_date):
            calls.append((trade_date, dataset))
            if fault == "missing":
                raise RuntimeError("not completed")
            return SimpleNamespace(
                row_count=0 if dataset == "suspend_d" or fault == "empty_price" else 5000,
                refreshed_at=datetime(2027 if fault == "future" else 2026, 9, 1, tzinfo=timezone.utc),
                status="success",
                quality_status="ok",
                job_id=None,
            )

    monkeypatch.setattr(db, "DataRefreshAuditRepository", Repository)
    if fault:
        with pytest.raises(FormalStateError, match="completion") as exc:
            db._completion(request, connection, plan)
        assert exc.value.reason_code.endswith("database_data_waiting")
    else:
        completion = db._completion(request, connection, plan)
        assert len(calls) == 12 and all(d < date(2026, 8, 31) for d, _ in calls)
        assert all(s["row_count"] == 0 for s in completion["statuses"] if s["dataset"] == "suspend_d")


def test_owned_connection_is_readonly_rolled_back_and_closed_on_failure(case, monkeypatch):
    import psycopg2
    from backend.db import pg_pool

    request, _, connection, *_ = case
    monkeypatch.setattr(pg_pool, "_db_cfg", lambda: request["source"]["database_target"])
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: connection)
    with pytest.raises(RuntimeError, match="injected"):
        with db.connect(request) as active:
            assert active is connection
            raise RuntimeError("injected")
    assert connection.rollback_count == connection.close_count == 1


@pytest.mark.parametrize("fault", [None, "missing", "duplicate", "nonfinite"])
def test_provider_previous_close_preserves_ex_rights_semantics_and_pit_entry_na(fault):
    day = date(2026, 8, 28)
    rows = [
        {"symbol": "000001.SZ", "prev_close_yuan": 10.0},
        {"symbol": "000002.SZ", "prev_close_yuan": None},
    ]

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, args):
            assert "market.stk_limit" in sql and args == (day, ["000001.SZ"])

        def fetchall(self):
            facts = [("000001.SZ", float("nan") if fault == "nonfinite" else 9.0)]
            return [] if fault == "missing" else facts * 2 if fault == "duplicate" else facts

    connection = SimpleNamespace(cursor=Cursor)
    if fault:
        with pytest.raises(FormalStateError, match="provider pre_close"):
            db._align_provider_previous_close(connection, day, rows)
    else:
        aligned = db._align_provider_previous_close(connection, day, rows)
        assert aligned[0]["prev_close_yuan"] == 9.0  # Not the unadjusted previous transaction close.
        assert aligned[1]["prev_close_yuan"] is None
        assert rows[0]["prev_close_yuan"] == 10.0


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "duplicate",
        "unknown_industry",
        "date_gap",
        "classification_na",
        "partial_na",
        "invalid_na",
        "duplicate_na",
    ],
)
def test_database_stock_facts_use_official_projection_and_canonical_l2_collection(case, monkeypatch, fault):
    request, bundle, connection, *_ = case
    frozen, _, _ = core.model_source(request)
    plan = db._read_plan(request, connection)
    plan["start"] = "2026-08-27"
    days = [date(2026, 8, 27), date(2026, 8, 28)]
    data = [
        {"trade_date": day, "symbol": symbol, "l1_code": l1, "l2_code": frozen["catalog"][i]}
        for day in days
        for i, symbol, l1 in [(0, "000001.SZ", "b"), (1, "000002.SZ", "a")]
    ]
    if fault == "duplicate":
        data.append(data[-1])
    elif fault == "unknown_industry":
        data[-1] = {**data[-1], "l2_code": "unregistered"}
    elif fault == "date_gap":
        data = data[:2]
    elif fault in {"classification_na", "partial_na", "invalid_na", "duplicate_na"}:
        for row in data:
            if row["trade_date"] == days[-1] and (fault != "partial_na" or row["symbol"] == "000001.SZ"):
                row.update(
                    l1_code=None,
                    l2_code=None,
                    industry_pit_resolution={
                        "status": "unavailable",
                        "canonical_symbol": row["symbol"],
                        "trade_date": row["trade_date"].isoformat(),
                        "l1_code": None,
                        "l2_code": None,
                        "reason_code": "classification_authority_unavailable",
                    },
                )
        if fault == "invalid_na":
            data[-1]["l2_code"] = frozen["catalog"][0]
        elif fault == "duplicate_na":
            data.append(dict(data[-1]))
    adapter = object()
    monkeypatch.setattr(core.reader, "_industry_adapter", lambda *a, **k: adapter)
    monkeypatch.setattr(core.reader, "_canonical_sector_codes", lambda a: ([], frozen["catalog"]))
    monkeypatch.setattr(db, "load_security_source_identity_manifest", lambda *a, **k: "security")
    monkeypatch.setattr(db, "load_provider_absence_manifest", lambda *a, **k: "provider")

    class Reader:
        def __init__(self, active, spec, **kwargs):
            assert active is connection and kwargs["industry_pit_adapter"].adapter is adapter
            assert kwargs["preserve_unavailable_classification"] is True
            assert spec.universe_key == request["source"]["universe_key"]
            assert spec.circ_mv_history_start == db.SOURCE_START

        def validate_source(self):
            return bundle["inference_identity"]["universe"]

        def run_industry_pit_preflight(self, **kwargs):
            assert kwargs == dict(window_start=days[0], window_end=days[1], expected_trading_days=2)
            return {"passed": True}

        def iter_stock_fact_rows(self):
            return iter(sorted(data, key=lambda r: (r["trade_date"], r["symbol"])))

        def iter_missing_price_rows(self):
            return iter(())

    seen = []

    def collect(day, rows, **kw):
        assert kw["levels"] == ("L2",) and kw["eligibility"] is frozen["eligibility"]
        expected = (
            []
            if day == days[-1] and fault == "classification_na"
            else ["a"]
            if day == days[-1] and fault == "partial_na"
            else ["a", "b"]
        )
        assert [r["l1_code"] for r in rows] == expected
        seen.append(day)

    monkeypatch.setattr(db, "PostgresStockFactReader", Reader)
    monkeypatch.setattr(db, "_collect_domains", collect)
    if fault in {"duplicate", "unknown_industry", "date_gap", "invalid_na", "duplicate_na"}:
        with pytest.raises(FormalStateError):
            db._aggregates(request, connection, frozen, plan)
    else:
        window, _, reasons, identity = db._aggregates(request, connection, frozen, plan)
        assert list(window) == seen == days
        assert len(identity["source_facts_sha256"]) == 64
        if fault in {"classification_na", "partial_na"}:
            audit = identity["classification_unavailable"]
            assert len(audit) == 1 and audit[0]["trade_date"] == days[-1].isoformat()
            assert audit[0]["count"] == (2 if fault == "classification_na" else 1)
            assert audit[0]["reason_counts"] == {"classification_authority_unavailable": audit[0]["count"]}
            if fault == "classification_na":
                assert all(
                    reasons[(days[-1].isoformat(), code)] == "hmm_risk_industry_pit_classification_unavailable"
                    for code in frozen["catalog"]
                )


def test_bounded_projection_preserves_dates_identity_and_real_transition():
    days = [date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)]
    base = HMMIndustryProjection(
        "resolved",
        "000001.SZ",
        days[0],
        "801010.SI",
        "L1",
        "801011.SI",
        "L2",
        None,
        "a" * 64,
        "b" * 64,
        ("c" * 64,),
        ("d" * 64,),
        "aligned",
        "as_published_pit",
        False,
    )
    calls = []

    class Adapter:
        classification_lookup = {"official": "catalog"}
        classification_resolver = index_membership_resolver = SimpleNamespace(
            transition_dates=lambda symbol: (days[1],)
        )

        def resolve(self, symbol, day):
            calls.append(day)
            return replace(base, trade_date=day, l2_code="801012.SI" if day >= days[1] else base.l2_code)

        def preflight(self, *args, **kwargs):
            return {"original": True}

    adapter = Adapter()
    bounded = db._BoundedIndustryProjection(adapter, days)
    actual = [bounded.resolve(base.canonical_symbol, day) for day in days]
    assert calls == days[:2]  # Once per real interval, never once per stock-day.
    assert actual == [adapter.resolve(base.canonical_symbol, day) for day in days]
    assert bounded.classification_lookup is adapter.classification_lookup
    assert bounded.preflight() == {"original": True}
    with pytest.raises(core.reader.RotationL1InputBundleError):
        bounded.resolve(base.canonical_symbol, date(2026, 8, 31))


@pytest.mark.parametrize("fault", [None, "gap", "nonfinite"])
def test_benchmark_uses_same_causal_close_return_not_reported_pct_change(case, fault):
    request, _, connection, *_ = case
    plan = db._read_plan(request, connection)
    plan.update(benchmark_start="2026-08-26", end="2026-08-28")
    dates = [date(2026, 8, 26), date(2026, 8, 27), date(2026, 8, 28)]

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, args):
            assert "000300.SH" in sql and args == (dates[0], dates[-1])

        def fetchall(self):
            rows = list(zip(dates, [100.0, 110.0, 99.0], strict=True))
            return rows[1:] if fault == "gap" else [*rows[:2], (dates[-1], float("nan"))] if fault else rows

    connection.cursor = Cursor
    if fault:
        with pytest.raises(FormalStateError):
            db._benchmark(connection, plan, dates[1:])
    else:
        assert db._benchmark(connection, plan, dates[1:]) == pytest.approx({dates[1]: 0.10, dates[2]: -0.10})


def test_prepare_uses_one_original_feature_kernel_and_never_queries_target_day(case, monkeypatch):
    request, bundle, connection, *_ = case
    frozen, _, _ = core.model_source(request)
    plan = db._read_plan(request, connection)
    window = tuple(date.fromisoformat(d) for d in plan["calendar"] if plan["start"] <= d <= plan["end"])
    calls = []

    def aggregate(req, active, original, source_plan):
        assert active is connection and original is frozen and source_plan == plan
        return window, ["original-L2-aggregates"], {}, bundle["inference_identity"]

    def panel(aggregates, **kw):
        calls.append(kw)
        assert aggregates == ["original-L2-aggregates"] and kw["direct_sector_level"] == "L2"
        assert kw["trading_dates"] == window and kw["expected_sector_count"] == 131
        assert max(window).isoformat() == "2026-08-28"
        index = pd.MultiIndex.from_product([pd.to_datetime(window), frozen["catalog"]], names=["trade_date", "l1_code"])
        return (
            pd.DataFrame(np.ones((len(index), 20)), index=index, columns=core.ALL_CORE_FEATURES),
            frozen["feature_definition"],
            {},
        )

    monkeypatch.setattr(db, "_aggregates", aggregate)
    monkeypatch.setattr(db, "_benchmark", lambda c, p, w: {day: 0.0 for day in w})
    monkeypatch.setattr(core, "build_c010_feature_domain_panel", panel)
    actual = db.prepare(request, "f" * 40, connection=connection)
    assert len(calls) == 1
    assert actual["model_identity"] == bundle["model_identity"]
    assert actual["rows"]["2026-08-28"][frozen["catalog"][0]]["features"] == [1.0] * 20
    assert connection.rollback_count == connection.close_count == 0  # Caller owns its transaction.


def test_real_feature_kernel_preserves_full_na_catalog_without_filling_classification(case, monkeypatch):
    request, bundle, connection, *_ = case
    frozen, _, _ = core.model_source(request)
    plan = db._read_plan(request, connection)
    window = tuple(date.fromisoformat(d) for d in plan["calendar"] if plan["start"] <= d <= plan["end"])
    benchmark = {day: 0.001 for day in window}
    _, definition, _ = core.build_c010_feature_domain_panel(
        [],
        trading_dates=window,
        csi300_returns=benchmark,
        expected_sector_count=131,
        direct_sector_level="L2",
        canonical_sector_codes=frozen["catalog"],
    )
    frozen["feature_definition"] = definition
    reasons = {
        (observed, code): "hmm_risk_industry_pit_classification_unavailable"
        for observed in plan["as_of"].values()
        for code in frozen["catalog"]
    }
    monkeypatch.setattr(db, "_aggregates", lambda *a: (window, [], reasons, bundle["inference_identity"]))
    monkeypatch.setattr(db, "_benchmark", lambda *a: benchmark)
    with no_training_or_external_actions():
        actual = db.prepare(request, "f" * 40, connection=connection)
        db.validate_bundle(request, actual, actual["receipt_sha256"])
    assert actual["inference_identity"]["source_dates"] == [d.isoformat() for d in window]
    for rows in actual["rows"].values():
        assert list(rows) == frozen["catalog"] and len(rows) == 131
        assert all(
            row["features"] is None and row["reason_code"] == "hmm_risk_industry_pit_classification_unavailable"
            for row in rows.values()
        )


def test_cli_database_dispatch_preserves_file_default_and_children_cannot_read_db(case, tmp_path, monkeypatch):
    request, bundle, *_ = case
    path, output = tmp_path / "request.json", tmp_path / "run"
    write_once(path, request)
    monkeypatch.setattr(cli, "_head", lambda: "f" * 40)
    monkeypatch.setattr(cli, "_clean", lambda head: None)
    monkeypatch.setattr(cli, "_numeric", lambda kind: {"fixed": True})
    monkeypatch.setattr(core, "prepare", lambda *a: pytest.fail("database mode fell back to files"))
    monkeypatch.setattr(db, "connect", lambda *a: nullcontext(case[2]))
    monkeypatch.setattr(db, "prepare", lambda *a, **kw: bundle)

    class Process:
        def __init__(self, command, **kw):
            assert command[command.index("--source-mode") + 1] == "database_daily"
            with no_training_or_external_actions():
                result = db.infer(request, bundle, expected_bundle_sha256=bundle["receipt_sha256"])
            write_once(
                Path(command[command.index("--output") + 1]), reseal({**result, "numeric_environment": {"fixed": True}})
            )

        def wait(self):
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", Process)
    args = [
        "run",
        "--source-mode",
        "database_daily",
        "--request",
        str(path),
        "--request-sha256",
        request["receipt_sha256"],
        "--output",
        str(output),
    ]
    assert cli.main(args) == 0
    acceptance = read_json(output / "acceptance.json")
    assert acceptance["schema_version"] == db.VERSION + "_acceptance"
    assert acceptance["two_process_bitwise_equal"] and acceptance["database_write"] is False
    assert acceptance["fits"] == 0 and acceptance["forward_confirmed"] is False
