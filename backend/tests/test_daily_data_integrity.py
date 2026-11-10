"""Direct contracts for the shared daily ledger (no market/network access)."""
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest
from backend.services.data_integrity import stock_scope

from backend.services.data_integrity.contracts import (
    CheckCounts, PartitionState, ValidationResult, aggregate_day, effective_partition,
    completion_kind, finite_number, release_contains_revision,
)
from backend.services.data_integrity.consumers import ConsumerDependencies, project_readiness
from backend.services.data_integrity.provider import provider_scan, verify_scan
from backend.services.tushare_dataset_specs import DAILY_BASIC, DIVIDEND


def test_newly_supported_contract_wakes_validation_not_facts_or_a_valid_verdict():
    from contextlib import nullcontext
    from types import SimpleNamespace
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    queries = []
    cursor = SimpleNamespace(execute=lambda query, params: queries.append((query, params)),
        fetchall=lambda: [{"dataset": "idx_anns", "target_date": date(2026, 10, 30)}])
    conn = SimpleNamespace(cursor=lambda **_: nullcontext(cursor))
    result = DailyIntegrityWorker._wake_supported_contracts(conn, ["idx_anns"], today=date(2026, 11, 6), limit=10)
    assert result == [{"dataset": "idx_anns", "target_date": date(2026, 10, 30)}]
    query, params = queries[0]
    assert "non_daily_dataset_contract" in query and "UNVERIFIED" in query
    assert "SKIP LOCKED" in query and "lease_until" in query and "LIMIT %s" in query
    assert "UPDATE market.data_sync_targets" in query
    assert "UPDATE market.dataset_date_refresh_audit" not in query
    assert "VALID'" not in query and "market.kline" not in query
    assert params[0:2] == (["idx_anns"], date(2026, 11, 6))
    assert set(params[2]) == {"stk_shock", "stk_high_shock", "stk_alert", "idx_anns", "stk_seasoned"}
    assert params[3] == 10
    assert "complete_current_event_query_required" in query and "NOT EXISTS" in query


def test_rearmed_contract_validation_gets_a_slot_without_increasing_worker_budget():
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    import inspect
    source = inspect.getsource(DailyIntegrityWorker.run_once)
    assert "_wake_supported_contracts" in source and "optional_input_contract" in source
    assert "woken" in source and "limit - len(woken)" in source
    assert inspect.signature(DailyIntegrityWorker.run_once).parameters["limit"].default == 10


@pytest.mark.parametrize("missing", [False, True])
@pytest.mark.parametrize("bad_price", [None, float("nan"), 0, -1, float("inf"), 10])
def test_stk_limit_validation_retains_physical_count_without_hiding_price_gaps(monkeypatch, bad_price, missing):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation, RULE_VERSION, SCOPE_VERSION
    day = date(2026, 11, 5)
    def read(_conn, query, _params=()):
        if isinstance(query, str) and "dataset='kline_daily_raw'" in query:
            return [{"data_revision": 1, "validated_revision": 1, "validation_status": "VALID",
                     "validation_id": "daily-valid", "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION}]
        return [{"ts_code": "000001.SZ", "pre_close": 10, "up_limit": 11, "down_limit": 9},
                {"ts_code": "000002.SZ", "pre_close": bad_price, "up_limit": 11, "down_limit": 9}]
    monkeypatch.setattr(validation, "rows", read)
    validator = validation.PhysicalValidator()
    monkeypatch.setattr(validator, "_traded", lambda *_: ["000001.SZ", "000002.SZ"] + (["000003.SZ"] if missing else []))
    result = validator.validate(None, Obligation("stk_limit", "daily_series", "09:10"), day, 1)
    assert result.checks["physical_rows"] == 2
    invalid = bad_price != 10
    assert result.state == (PartitionState.INCOMPLETE if missing or invalid else PartitionState.VALID)
    assert result.counts[0].missing == int(missing) and result.counts[0].invalid == int(invalid)
    assert {gap["classification"] for gap in result.gaps} == (
        ({"MISSING"} if missing else set()) | ({"INVALID"} if invalid else set()))


def test_history_validation_backoff_is_bounded_and_recent_data_stays_fast():
    from backend.services.data_integrity.repository import validation_retry_minutes
    today = date(2026, 11, 5)
    assert validation_retry_minutes(date(2026, 11, 4), today=today, unchanged=True, attempts=80) == 30
    assert validation_retry_minutes(date(2026, 5, 20), today=today, unchanged=False, attempts=80) == 30
    assert validation_retry_minutes(date(2026, 5, 20), today=today, unchanged=True, attempts=80) == 1440


@pytest.mark.parametrize("complete", [True, False])
def test_completed_provider_scan_rearms_validation_without_fact_changes(monkeypatch, complete):
    from backend.tests.test_dataset_refresh_audit import _FakeConn
    from backend.services.data_refresh_audit import DataRefreshAuditRepository
    from backend.services.data_integrity import repository
    calls = []
    monkeypatch.setattr(repository, "schema_installed", lambda _: True)
    monkeypatch.setattr(repository, "enqueue_validation", lambda *a, **kw: calls.append((a, kw)))
    conn = _FakeConn(fetchone_result=(3,))
    DataRefreshAuditRepository(conn_factory=lambda: conn).record_success(
        dataset="index_daily", trade_date=date(2026, 10, 28), row_count=1005,
        metadata={"provider_scan": {"entries": ["fact"]}, "provider_query_scope_complete": complete})
    assert len(calls) == 1 and calls[0][1] == {"source_completed": complete}


@pytest.mark.parametrize("reason", ["complete_source_scan_not_bound_to_revision",
    "row_count_seed_is_not_a_complete_source_scan", "index_quote_query_scope_incomplete"])
def test_unproven_source_scan_creates_exact_fetch_instead_of_endless_validation(monkeypatch, reason):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import Mock
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    from backend.services.data_integrity.catalog import Obligation
    import backend.services.data_sync_targets as targets
    repo = Mock()
    monkeypatch.setattr(targets, "DataSyncTargetRepository", lambda: repo)
    worker = DailyIntegrityWorker(repository=SimpleNamespace(transaction=lambda: nullcontext(object())))
    worker._repair_target(Obligation("index_daily", "daily_series", "16:50"), date(2026, 10, 28),
        ValidationResult(PartitionState.UNVERIFIED, (), {}, reason=reason))
    record = repo.upsert_target.call_args.args[0]
    assert record.dataset == "index_daily" and record.target_date == date(2026, 10, 28)
    assert record.target_scope == {"operation": "FETCH", "scope": "exact_fact_date"}


@pytest.mark.parametrize("dataset,expected", [("stk_alert", True), ("idx_anns", True), ("daily_basic", False)])
def test_missing_event_query_proof_requeues_source_not_a_false_valid(monkeypatch, dataset, expected):
    from contextlib import nullcontext
    from types import SimpleNamespace
    from unittest.mock import Mock
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    from backend.services.data_integrity.catalog import Obligation
    import backend.services.data_sync_targets as targets
    repo = Mock()
    monkeypatch.setattr(targets, "DataSyncTargetRepository", lambda: repo)
    worker = DailyIntegrityWorker(repository=SimpleNamespace(transaction=lambda: nullcontext(object())))
    worker._repair_target(Obligation(dataset, "event", "23:59"), date(2024, 4, 1),
        ValidationResult(PartitionState.UNVERIFIED, (), {}, reason="complete_current_event_query_required"))
    assert repo.upsert_target.called is expected
    if expected:
        record = repo.upsert_target.call_args.args[0]
        assert record.data_source == "integrity_repair" and record.target_status == "retry"
        assert record.target_date == date(2024, 4, 1)
        assert record.target_scope == {"operation": "FETCH", "scope": "exact_fact_date"}


def test_fair_validation_query_uses_only_the_small_target_ledger():
    import ast
    import inspect
    import textwrap
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    tree = ast.parse(textwrap.dedent(inspect.getsource(DailyIntegrityWorker.run_once)))
    query = next(n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
                 and isinstance(n.value, str) and n.value.startswith("WITH due AS"))
    assert "ROW_NUMBER() OVER (PARTITION BY dataset" in query
    assert "dataset_rank" in query and "LIMIT %s" in query
    assert "market.kline" not in query and "market.sector_data" not in query
    assert inspect.signature(DailyIntegrityWorker.run_once).parameters["limit"].default == 10
    assert inspect.signature(DailyIntegrityWorker.run_once).parameters["budget_seconds"].default == 45


@pytest.mark.skipif(__import__("os").environ.get("AISTOCK_INTEGRITY_DEV_TEST") != "1", reason="explicit existing DEV opt-in")
def test_fair_validation_native_dev_plan_does_not_starve_recent_partitions():
    import ast
    import inspect
    import json
    import os
    from pathlib import Path
    import textwrap
    from datetime import timedelta
    import psycopg2
    from psycopg2.extras import execute_values
    from scripts.prepare_canonical_pit_monthly import _load_database_config
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    c = _load_database_config("dev", Path(os.environ["AISTOCK_INTEGRITY_ENV_FILE"]))
    assert c.port == 5433 and c.dbname == "aistock_dev" and c.host in {"localhost", "127.0.0.1"}
    tree = ast.parse(textwrap.dedent(inspect.getsource(DailyIntegrityWorker.run_once)))
    query = next(n.value for n in ast.walk(tree) if isinstance(n, ast.Constant)
                 and isinstance(n.value, str) and n.value.startswith("WITH due AS"))
    query = query.replace("market.data_sync_targets", "pg_temp.integrity_worker_queue")
    conn = psycopg2.connect(host=c.host, port=c.port, dbname=c.dbname, user=c.user, password=c.password,
        options="-c statement_timeout=3000 -c lock_timeout=1000", application_name="BUG-2176-DEV-queue-plan")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE integrity_worker_queue (LIKE market.data_sync_targets INCLUDING DEFAULTS) ON COMMIT DROP")
            values = [(f"old_{i}", "adj_factor", date(2010, 1, 1)+timedelta(days=i), "retry", f"old_key_{i}") for i in range(2000)]
            values += [("new_announcement", "anns_metadata", date(2026, 11, 4), "pending", "new_ann"),
                       ("new_basic", "bak_basic", date(2026, 11, 4), "pending", "new_basic")]
            execute_values(cur, """INSERT INTO pg_temp.integrity_worker_queue
                (target_id,dataset,target_date,target_status,target_key_sha256,data_source,target_scope,priority,next_retry_at)
                VALUES %s""", [(a,b,c,d,e,"canonical_db",'{"operation":"VALIDATE_ONLY"}',50,"2026-01-01") for a,b,c,d,e in values])
            params = (date(2026, 11, 5), ["adj_factor", "anns_metadata", "bak_basic"], 10)
            cur.execute("""SELECT dataset FROM pg_temp.integrity_worker_queue WHERE target_date<=%s
                ORDER BY (dataset=ANY(%s::text[])) DESC,priority,COALESCE(next_retry_at,created_at),target_date LIMIT %s""", params)
            assert {r[0] for r in cur.fetchall()} == {"adj_factor"}
            cur.execute(query, params)
            selected = cur.fetchall()
            assert {r[0] for r in selected[:3]} == {"adj_factor", "anns_metadata", "bak_basic"}
            cur.execute("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query, params)
            plan = cur.fetchone()[0][0]
            assert plan["Execution Time"] < 1000
            print(json.dumps({"queued": len(values), "selected": len(selected), "execution_ms": plan["Execution Time"]}))
    finally:
        conn.rollback()
        conn.close()


@pytest.mark.parametrize("dataset", ["tushare_forecast_raw", "tushare_express_raw", "tushare_fina_indicator_raw"])
def test_raw_financial_scans_share_supported_stock_scope_without_rewriting_legacy_hash(dataset):
    from backend.services.tushare_dataset_specs import DATASET_REGISTRY
    from backend.services.data_integrity.contracts import digest
    from backend.services.data_integrity.provider import scoped_scan_entries
    spec = DATASET_REGISTRY[dataset]
    supported = {"source_record_key": f"{dataset}:000001.SZ:20261028:20260930", "source_row_hash": "a"*64}
    excluded = {"source_record_key": f"{dataset}:920306.BJ:20261028:20260930", "source_row_hash": "b"*64}
    scan = provider_scan(spec, [supported, excluded], parameters={"period": "20260930"})
    assert len(scan["entries"]) == 1
    scan["entries"].append({"key": [excluded[k] for k in spec.primary_keys]})
    scan["scan_sha256"] = digest({k: v for k, v in scan.items() if k != "scan_sha256"})
    original = digest(scan)
    assert len(scoped_scan_entries(spec, scan)) == 1
    assert verify_scan(spec, scan, [supported]) == ([], [])
    missing, invalid = verify_scan(spec, scan, [])
    assert missing == [[supported[k] for k in spec.primary_keys]] and invalid == []
    assert digest(scan) == original
    unknown = {**excluded, "source_record_key": "opaque:" + excluded["source_record_key"]}
    unknown_scan = provider_scan(spec, [unknown], parameters={})
    assert verify_scan(spec, unknown_scan, [])[0] == [[unknown[k] for k in spec.primary_keys]]


@pytest.mark.parametrize("value,expected", [(0, True), (Decimal("0"), True), (None, False),
    (float("nan"), False), (Decimal("NaN"), False), (float("inf"), False), (True, False), ("1", False)])
def test_field_finite_contract_distinguishes_real_zero(value, expected):
    assert finite_number(value) == expected
    assert not finite_number(value, positive=True) if value == 0 else True


def test_disjoint_counts_and_no_unknown_to_zero():
    assert CheckCounts("stock_date", 3, 2, 1, 0, 0, 0).complete
    assert not CheckCounts("stock_date", None, 2, None, None, None, None).complete
    with pytest.raises(ValueError, match="partition"):
        CheckCounts("stock_date", 3, 3, 1, 0, 0, 0)
    with pytest.raises(ValueError, match="non-vacuous"):
        ValidationResult(PartitionState.VALID, (), {})


@pytest.mark.parametrize("state,failed,expected", [
    ("COMPLETE", [], PartitionState.VALID),
    ("PARTIAL", ["300506.SZ"], PartitionState.INCOMPLETE),
    ("PARTIAL", ["688109.SH"], PartitionState.VALID),
    ("RUNNING", [], PartitionState.UNVERIFIED),
    ("INTERRUPTED", [], PartitionState.UNVERIFIED),
])
def test_adj_factor_baseline_conflict_is_not_hidden_by_positive_latest_value(state, failed, expected):
    from backend.services.data_integrity.validation import adj_factor_result
    result = adj_factor_result({"300506.SZ"}, {"300506.SZ": {"adj_factor": 1}},
                               {"status": state, "failed_symbols": failed})
    assert result.state == expected


def test_dirty_revision_or_rule_never_reuses_old_green():
    row = {"data_revision": 8, "validated_revision": 8, "scope_version": "s1", "rule_version": "r1",
           "validation_status": "VALID", "validation_id": "v1"}
    assert effective_partition(row, scope_version="s1", rule_version="r1")["source_complete"]
    for changed in ({"data_revision": 9}, {"rule_version": "r2"}, {"validation_id": None},
                    {"data_revision": -1, "validated_revision": -1}, {"validation_id": ""}):
        result = effective_partition({**row, **changed}, scope_version="s1", rule_version="r1")
        assert result["state"] == "UNVERIFIED" and not result["source_complete"]


@pytest.mark.parametrize("dataset", ["bak_basic", "cyq_perf"])
def test_dense_panel_legacy_scan_green_is_not_current_traded_scope(dataset):
    from backend.services.data_integrity.catalog import SCOPE_VERSION, RULE_VERSION
    row = {"dataset": dataset, "data_revision": 1, "validated_revision": 1,
           "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION,
           "validation_status": "VALID", "validation_id": "old-provider-scan",
           "metadata": {"validation": {"checks": {"provider": "tushare"}}}}
    old = effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)
    assert old["state"] == "UNVERIFIED" and not old["source_complete"]
    row["metadata"]["validation"]["checks"]["coverage_basis"] = "independent_dated_traded_stock_keys"
    assert effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]
    row["validation_status"] = "INCOMPLETE"
    assert not effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]
    row.update(dataset="index_daily", validation_status="VALID", metadata={})
    assert effective_partition(row, scope_version=SCOPE_VERSION, rule_version=RULE_VERSION)["source_complete"]


@pytest.mark.parametrize("metadata", [None, "invalid", {"validation": []}, {"validation": {"checks": "invalid"}}])
def test_malformed_dense_validation_metadata_is_unknown_not_green_or_api_error(metadata):
    row = {"dataset": "bak_basic", "data_revision": 1, "validated_revision": 1,
           "scope_version": "s1", "rule_version": "r1", "validation_status": "VALID",
           "validation_id": "old", "metadata": metadata}
    assert effective_partition(row, scope_version="s1", rule_version="r1")["state"] == "UNVERIFIED"


@pytest.mark.parametrize("parts,expected", [([], "UNVERIFIED"),
    ([{"state": "VALID", "source_complete": True}], "COMPLETE"),
    ([{"state": "WAIT_UPSTREAM"}], "IN_PROGRESS"),
    ([{"state": "INCOMPLETE"}, {"state": "VERIFYING"}], "PARTIAL"),
    ([{"state": "WAIT_UPSTREAM", "overdue": True}], "PARTIAL"),
    ([{"state": "UNVERIFIED", "overdue": True}], "UNVERIFIED"),
    ([{"state": "ERROR"}, {"state": "INCOMPLETE"}], "ERROR")])
def test_day_state_precedence(parts, expected):
    assert aggregate_day(parts, trading_day=True)["state"] == expected
    assert aggregate_day(parts, trading_day=False)["state"] == "NOT_TRADING"
    assert aggregate_day(parts, trading_day=True, future=True)["state"] == "UNVERIFIED"


def test_legacy_release_without_pins_is_unknown_not_missing():
    d = date(2026, 10, 1)
    assert release_contains_revision({}, None, cutoff=d, fact_date=d) is None
    assert release_contains_revision({}, [], cutoff=d, fact_date=d) is False


def test_independent_provider_keys_detect_missing_row_not_only_count():
    facts = [{"ts_code": "000001.SZ", "trade_date": "20260930", "circ_mv": 10},
             {"ts_code": "000002.SZ", "trade_date": "20260930", "circ_mv": None}]
    scan = provider_scan(DAILY_BASIC, facts, parameters={"trade_date": "20260930"})
    missing, invalid = verify_scan(DAILY_BASIC, scan, [facts[0], {**facts[1], "ts_code": "000003.SZ"}])
    assert len(missing) == 1 and not invalid
    # No provider-value equality gate over an independently corrected DB fact.
    assert verify_scan(DAILY_BASIC, scan, [{**r, "circ_mv": 25} for r in facts]) == ([], [])
    with pytest.raises(ValueError, match="identity differs"):
        verify_scan(DAILY_BASIC, {**scan, "complete": False}, facts)


def test_formally_nullable_source_identity_not_fabricated():
    value = {"ts_code": "600000.SH", "end_date": "20251231", "ann_date": None, "div_proc": "实施"}
    scan = provider_scan(DIVIDEND, [value], parameters={"ex_date": "20260630"})
    assert None in scan["entries"][0]["key"]
    assert verify_scan(DIVIDEND, scan, [value]) == ([], [])


def test_daily_basic_admission_ratio_is_not_zero_gap_certification():
    from backend.services.data_integrity.validation import daily_basic_result
    from backend.services.tushare_sync_engine import DAILY_BASIC_REQUIRED_FIELDS
    facts = {f"{index:06}.SZ": dict.fromkeys(DAILY_BASIC_REQUIRED_FIELDS, 1) for index in range(20)}
    facts["000001.SZ"]["volume_ratio"] = None
    result = daily_basic_result(facts, facts)
    assert result.checks["coverage"]["required_fields_ratio"] == .95
    assert result.state == PartitionState.INCOMPLETE
    count = next(c for c in result.counts if c.unit == "field_cell:volume_ratio")
    assert count.missing == 1 and count.valid == 19
    facts["000001.SZ"]["volume_ratio"] = 0  # Real zero is valid, not a fabricated fill.
    facts["000001.SZ"]["dv_ratio"] = None  # Formally nullable valuation.
    assert daily_basic_result(facts, facts).state == PartitionState.VALID
    facts["000001.SZ"]["circ_mv"] = float("nan")
    assert daily_basic_result(facts, facts).state == PartitionState.INCOMPLETE


def test_traded_scope_reuses_shared_legacy_code_normalization():
    from backend.services.data_integrity.validation import PhysicalValidator
    from backend.data_service.security_source_identity import load_default_security_source_identity_manifest
    authority = load_default_security_source_identity_manifest()
    codes = PhysicalValidator._canonical_raw_codes(authority,
        ["SZ000001 ", "000001.SZ", "sh600000", "SZ200002", "SH900901"], date(2026, 2, 9))
    assert codes == {"000001.SZ", "600000.SH"}
    assert {authority.resolve(code, date(2026, 2, 9), "market.daily_basic").source_ts_code for code in codes} == codes
    with pytest.raises(ValueError, match="canonical_ts_code is invalid"):
        for code in PhysicalValidator._canonical_raw_codes(authority, ["bad-symbol"], date(2026, 2, 9)):
            authority.resolve(code, date(2026, 2, 9), "market.daily_basic")


@pytest.mark.parametrize("dataset", ["cyq_perf", "bak_basic"])
@pytest.mark.parametrize("mutation", ["valid", "zero", "missing", "nan", "missing_feature", "dependency"])
def test_dense_historical_facts_use_traded_keys_not_sparse_scan_receipts(monkeypatch, dataset, mutation):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation, SCOPE_VERSION, RULE_VERSION
    validator = validation.PhysicalValidator()
    partition = {"data_revision": 1, "validated_revision": 1, "scope_version": SCOPE_VERSION,
                 "rule_version": RULE_VERSION, "validation_status": "VALID", "validation_id": "raw-valid"}
    if mutation == "dependency":
        partition["data_revision"] = 2
    fact = {"ts_code": "000001.SZ", "weight_avg": 2, "cost_50pct": 2, "winner_rate": 0,
            "his_low": 1, "his_high": 3, "cost_15pct": 1, "cost_85pct": 3}
    if mutation == "nan":
        fact["weight_avg"] = float("nan")
    if mutation == "zero":
        fact["cost_50pct"] = 0  # A finite provider zero is not a missing cell.
    if mutation == "missing_feature":
        fact["his_high"] = None
    facts = [] if mutation == "missing" else [fact]
    monkeypatch.setattr(validation, "rows", lambda _c, q, _p=(): [partition]
        if "dataset_date_refresh_audit" in str(q) else facts)
    monkeypatch.setattr(validator, "_traded", lambda *_a: {"000001.SZ"})
    monkeypatch.setattr("backend.services.data_integrity.stock_scope.delisting_panel_exclusions", lambda *_a: {})
    monkeypatch.setattr(validator, "_scan_contract", lambda *_a: pytest.fail("dense facts must not require a source scan"))
    result = validator.validate(None, Obligation(dataset, "daily_series", "21:00"), date(2025, 8, 22), 0)
    expected = (PartitionState.UNVERIFIED if mutation == "dependency" else PartitionState.INCOMPLETE
                if mutation == "missing" or mutation in {"nan", "missing_feature"} and dataset == "cyq_perf" else PartitionState.VALID)
    assert result.state == expected


def test_daily_basic_only_proven_new_listing_ratio_na_is_legal():
    from backend.services.data_integrity.validation import daily_basic_result
    from backend.services.tushare_sync_engine import DAILY_BASIC_REQUIRED_FIELDS
    facts = {"301718.SZ": dict.fromkeys(DAILY_BASIC_REQUIRED_FIELDS, 1)}
    facts["301718.SZ"]["volume_ratio"] = None
    proof = {"301718.SZ": {"list_date": "2026-10-22", "listed_trading_days": 5}}
    result = daily_basic_result(facts, facts, volume_ratio_warmup=proof)
    assert result.state == PartitionState.VALID
    count = next(c for c in result.counts if c.unit == "field_cell:volume_ratio")
    assert count.legal_na == 1 and count.missing == 0 and count.valid == 0
    assert facts["301718.SZ"]["volume_ratio"] is None
    assert daily_basic_result(facts, facts).state == PartitionState.INCOMPLETE
    facts["301718.SZ"]["circ_mv"] = None
    assert daily_basic_result(facts, facts, volume_ratio_warmup=proof).state == PartitionState.INCOMPLETE
    facts["301718.SZ"]["circ_mv"] = 1
    facts["301718.SZ"]["volume_ratio"] = float("nan")
    assert daily_basic_result(facts, facts, volume_ratio_warmup=proof).state == PartitionState.VALID
    assert daily_basic_result(facts, facts).state == PartitionState.INCOMPLETE
    from decimal import Decimal
    facts["301718.SZ"]["volume_ratio"] = Decimal("NaN")
    assert daily_basic_result(facts, facts, volume_ratio_warmup=proof).state == PartitionState.VALID
    facts["301718.SZ"]["volume_ratio"] = float("inf")
    assert daily_basic_result(facts, facts, volume_ratio_warmup=proof).state == PartitionState.INCOMPLETE
    assert daily_basic_result(facts, {}, volume_ratio_warmup=proof).state == PartitionState.INCOMPLETE


@pytest.mark.parametrize("events,expected", [
    ([], 240), ([("S", None)], 0), ([("R", None)], 240),
    ([("S", "13:00-15:00")], 120), ([("S", "10:00-10:30")], 210),
    ([("S", "13:00-9:30")], None), ([("S", "09:30-09:30")], None),
    ([("S", None), ("R", None)], None),
])
def test_shared_suspension_minutes_keep_real_unknown_and_regular_boundaries(events, expected):
    from backend.data_service.suspension_day_authority import required_minute_end_times
    day = date(2026, 9, 1)
    rows = [{"ts_code": "000001.SZ", "trade_date": day, "suspend_type": kind, "suspend_timing": timing}
            for kind, timing in events]
    authority = {"schema_version": "aistock_suspension_day_authority_v1",
                 "policy": "provider_events_with_disclosure_resolution_v1", "entries": []}
    result = required_minute_end_times("000001.SZ", day, rows, authority=authority)
    assert (len(result) if result is not None else None) == expected
    if events == [("S", "10:00-10:30")]:
        assert time(10, 0) in result and time(10, 30) not in result and time(10, 31) in result


def test_raw_counts_are_disjoint_and_full_day_suspension_is_legal_na(monkeypatch):
    from backend.services.data_integrity.catalog import Obligation
    from backend.services.data_integrity.validation import PhysicalValidator
    validator = PhysicalValidator()
    class Raw:
        def _raw_date_observation(self, *_a, **_kw):
            return 2, None, {"expected_symbols": 2, "incomplete_symbols": 1,
                "expected_symbol_keys": ["000001.SZ", "000002.SZ"], "legal_na_symbols": ["000003.SZ"],
                "missing_symbols": ["000001.SZ"], "invalid_symbols": ["000001.SZ"]}
    monkeypatch.setattr(validator, "_raw_scheduler", lambda *_a, **_kw: Raw())
    result = validator.validate(None, Obligation("kline_daily_raw", "daily_series", "16:10"), date(2026, 9, 1), 1)
    assert result.counts[0] == CheckCounts("security_date", 3, 1, 1, 0, 1, 0)
    assert result.state == PartitionState.INCOMPLETE


def test_consumer_not_blocked_by_unused_delayed_dataset_and_unknown_not_ready():
    day = date(2026, 10, 23)
    dependency = ConsumerDependencies(("adj_factor",), ("factor",), 1, "frozen-input")
    row = {"data_revision": 1, "validated_revision": 1, "scope_version": "full_provider_fact_scope_v1",
           "rule_version": "existing_dataset_quality_contracts_v1", "validation_id": "v1", "validation_status": "VALID"}
    partitions = {(day, "adj_factor"): row, (day, "margin_detail"): {"validation_status": "WAIT_UPSTREAM"}}
    result = project_readiness(dependency, [day], partitions, pit_binding={"activation_generation": 3})
    assert result["consumer_ready"] and result["prediction_ready"]
    assert not result["prediction_started"]
    partitions[(day, "adj_factor")] = {**row, "data_revision": 2}
    assert not project_readiness(dependency, [day], partitions, pit_binding={"activation_generation": 3})["consumer_ready"]
    assert project_readiness(None, [], {}, pit_binding=None)["state"] == "UNKNOWN"


@pytest.mark.parametrize("previous,expected", [({}, "HISTORICAL_VERIFIED"),
    ({"ever_complete": True, "ever_incomplete": True, "gap_since_complete": False}, "SOURCE_REVISED"),
    ({"ever_complete": True, "gap_since_complete": True}, "RECOVERED")])
def test_completion_distinguishes_historical_verification_revision_and_actual_recovery(previous, expected):
    kind, on_time = completion_kind(previous, as_of=datetime.now(timezone.utc), due_at=None, timing_known=False)
    assert kind == expected and on_time is None


@pytest.mark.parametrize("state", [PartitionState.UNVERIFIED, PartitionState.VALID])
def test_index_request_catalogue_is_not_a_required_quote_universe(monkeypatch, state):
    from backend.services.data_integrity.catalog import Obligation
    from backend.services.data_integrity.validation import PhysicalValidator
    validator = PhysicalValidator()
    result = ValidationResult(state, (CheckCounts("contract_check", 1, 1 if state == PartitionState.VALID else 0,
        0, 0, 0, 1 if state == PartitionState.UNVERIFIED else 0),), {})
    monkeypatch.setattr(validator, "_scan_contract", lambda *_a: result)
    assert validator.validate(None, Obligation("index_daily", "daily_series", "16:50"), date(2026, 10, 27), 1) is result


@pytest.mark.parametrize("mutation,expected", [("valid", PartitionState.VALID),
    ("close_only", PartitionState.VALID), ("nullable_volume", PartitionState.VALID),
    ("nan_open", PartitionState.INCOMPLETE), ("zero_open", PartitionState.INCOMPLETE),
    ("negative_volume", PartitionState.INCOMPLETE), ("null_close", PartitionState.INCOMPLETE),
    ("missing", PartitionState.INCOMPLETE), ("nan", PartitionState.INCOMPLETE),
    ("partial_query", PartitionState.UNVERIFIED), ("old_revision", PartitionState.UNVERIFIED)])
def test_index_complete_query_keys_and_finite_quotes_are_both_required(monkeypatch, mutation, expected):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation
    from backend.services.tushare_dataset_specs import INDEX_DAILY
    day = date(2026, 10, 27)
    facts = [{"ts_code": code, "trade_date": day, "open": 1, "high": 2, "low": 1,
              "close": 2, "vol": 0, "amount": 0} for code in ["000300.SH", "000985.CSI"]]
    scan = provider_scan(INDEX_DAILY, facts, parameters={"index_markets": None})
    source = {"status": "success", "quality_status": "ok", "data_source": "tushare",
        "metadata": {"integrity_observed_revision": 1, "provider_query_scope_complete": mutation != "partial_query",
                     "provider_scan": scan}}
    actual = [dict(r) for r in facts]
    if mutation == "missing":
        actual.pop()
    if mutation == "nan":
        actual[0]["close"] = float("nan")
    if mutation == "close_only":
        actual[0].update(open=None, high=None, low=None, vol=None, amount=None)
    if mutation == "nullable_volume":
        actual[0].update(vol=None, amount=None)
    if mutation == "nan_open":
        actual[0]["open"] = float("nan")
    if mutation == "zero_open":
        actual[0]["open"] = 0
    if mutation == "negative_volume":
        actual[0]["vol"] = -1
    if mutation == "null_close":
        actual[0]["close"] = None
    if mutation == "old_revision":
        source["metadata"]["integrity_observed_revision"] = 0
    monkeypatch.setattr(validation, "rows", lambda _c, query, _p=(): [source]
        if "dataset_date_refresh_audit" in str(query) else actual)
    result = validation.PhysicalValidator()._scan_contract(None, Obligation("index_daily", "daily_series", "16:50"), day, 1)
    assert result.state == expected
    if mutation == "valid":
        assert result.checks["coverage_basis"] == "complete_query_returned_keys_not_catalogue_availability"
    if mutation in {"close_only", "nullable_volume"}:
        assert result.checks["quote_field_contract"]["required_fields"] == ["close"]
        assert result.counts[-1].legal_na == (5 if mutation == "close_only" else 2)
def test_stock_fact_scope_is_shsz_not_pit_buy_eligibility():
    from backend.services.data_integrity.stock_scope import (
        filter_stock_rows, is_supported_stock_code, stock_exchange_selection, stock_scope_sql,
    )
    assert all(is_supported_stock_code(c) for c in ['000001.SZ', 'SH600000', '300001.SZ', '688001.SH'])
    assert not any(is_supported_stock_code(c) for c in ['920296.BJ', 'BJ430047', '200001.SZ', '900901.SH'])
    # No ST/IPO/exit-period admission check is allowed in a quote fact scope.
    rows = [{'ts_code': '000001.SZ', 'name': '*ST sample'}, {'ts_code': '920296.BJ'},
            {'ts_code': 'BJ430047'}, {'ts_code': '200001.SZ'}, {'ts_code': 'bad-source-key'}]
    assert filter_stock_rows(rows) == [rows[0], rows[-1]]  # malformed facts still reach existing errors
    assert stock_exchange_selection(['all']) == ['sh', 'sz']
    assert stock_exchange_selection(['bj']) == []
    assert 'BJ' not in stock_scope_sql('k.ts_code')
    from backend.services.sector_data_builder import sector_fact_comparison_sql
    assert stock_scope_sql() in sector_fact_comparison_sql()


def test_legacy_provider_scan_bse_keys_do_not_become_required_stock_facts():
    from backend.services.data_integrity.contracts import digest
    from backend.services.data_integrity.provider import provider_scan, verify_scan
    from backend.services.tushare_dataset_specs import DAILY_BASIC, INDEX_DAILY
    facts = [{'ts_code': '000001.SZ', 'trade_date': '20261028'},
             {'ts_code': '920296.BJ', 'trade_date': '20261028'}]
    scan = provider_scan(DAILY_BASIC, facts, parameters={})
    assert len(scan['entries']) == 1
    # A genuine old completed scan may contain BSE. Verify its original digest,
    # then apply supported scope; never rewrite the old signed source identity.
    scan['entries'].append({'key': ['2026-10-28', '920296.BJ']})
    scan['scan_sha256'] = digest({k: v for k, v in scan.items() if k != 'scan_sha256'})
    assert verify_scan(DAILY_BASIC, scan, facts[:1]) == ([], [])
    assert verify_scan(DAILY_BASIC, scan, []) == ([['2026-10-28', '000001.SZ']], [])
    index = provider_scan(INDEX_DAILY, facts, parameters={})
    assert len(index['entries']) == 2  # never apply an equity scope to index catalogues


class Cursor:
    def __init__(self, records):
        self.records = records
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, statement, parameters):
        self.calls.append((statement, parameters))

    def fetchall(self):
        return self.records


class Connection:
    def __init__(self, records=()):
        self.query = Cursor(records)

    def cursor(self):
        return self.query


def _legacy_completion(day, dataset="stock_st", api="stock_st", mode="by_date", count=1):
    source = {"dataset": dataset, "trade_date": day, "data_source": "tushare", "status": "success",
        "quality_status": "ok" if count else "empty_valid", "row_count": count,
        "job_id": "job-legacy", "refreshed_at": datetime(2026, 10, 19, 12, tzinfo=timezone.utc),
        "metadata": {"mode": mode, "tushare_api": api}}
    job = {"job_id": "job-legacy", "status": "success", "summary": {"dataset": dataset,
        "start_date": str(day), "end_date": str(day), "stats": {"dataset": dataset,
            "job_id": "job-legacy", "total_batches": 1, "success_batches": 1, "failed_batches": 0,
            "error": None}}}
    return source, job


def test_legacy_partition_requires_real_complete_job_and_physical_validation():
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_ST
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day)
    facts = [{"ts_code": "000001.SZ", "ann_date": day}]
    result = legacy_completion_result(STOCK_ST, day, 0, source, job, facts)
    assert result.state == PartitionState.VALID
    assert result.checks['coverage_basis'] == 'completed_legacy_job_current_database_partition'
    assert result.checks['historical_vintage_proven'] is False
    assert 'provider_scan' not in source['metadata']
    missing = legacy_completion_result(STOCK_ST, day, 0, source, job, [])
    assert missing.state == PartitionState.INCOMPLETE and missing.counts[0].missing == 1
    bad = legacy_completion_result(STOCK_ST, day, 0, source, job, [{"ts_code": None, "ann_date": day}])
    assert bad.state == PartitionState.INCOMPLETE and bad.counts[-1].invalid == 1


@pytest.mark.parametrize('mutation', ['failed_job', 'incomplete_batches', 'wrong_dataset', 'wrong_day',
    'wrong_job_id', 'missing_job', 'seed', 'partial_query', 'query_error', 'new_revision', 'bound_other_revision'])
def test_legacy_completion_never_relaxes_unknown_or_changed_scope(mutation):
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_ST
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day)
    revision = 0
    if mutation == 'failed_job':
        job['status'] = 'failed'
    elif mutation == 'incomplete_batches':
        job['summary']['stats']['success_batches'] = 0
    elif mutation == 'wrong_dataset':
        job['summary']['dataset'] = 'dividend'
    elif mutation == 'wrong_day':
        job['summary']['end_date'] = '2026-10-18'
    elif mutation == 'wrong_job_id':
        job['summary']['stats']['job_id'] = 'different-job'
    elif mutation == 'missing_job':
        job = None
    elif mutation == 'seed':
        source['metadata']['mode'] = 'physical_audit_seed'
    elif mutation == 'partial_query':
        source['metadata']['pagination_complete'] = False
    elif mutation == 'query_error':
        job['summary']['stats']['error'] = 'query failed'
    elif mutation == 'new_revision':
        revision = 1
    elif mutation == 'bound_other_revision':
        source['metadata']['integrity_observed_revision'] = 1
    result = legacy_completion_result(STOCK_ST, day, revision, source, job,
        [{"ts_code": '000001.SZ', "ann_date": day}])
    assert result.state == PartitionState.UNVERIFIED


def test_legacy_zero_events_require_complete_period_scope_not_row_count_alone():
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import TUSHARE_EXPRESS_RAW
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day, 'tushare_express_raw', 'express_vip', 'by_period', 0)
    result = legacy_completion_result(TUSHARE_EXPRESS_RAW, day, 0, source, job, [])
    assert result.state == PartitionState.UNVERIFIED
    source['metadata'].update(periods=['20260930'], date_column='ann_date', sparse_event_dataset=True)
    job['summary']['stats']['periods'] = ['20260930']
    result = legacy_completion_result(TUSHARE_EXPRESS_RAW, day, 0, source, job, [])
    assert result.state == PartitionState.VALID
    assert result.checks['query_periods'] == ['20260930']
    job['summary']['stats']['periods'] = ['20260630']
    assert legacy_completion_result(TUSHARE_EXPRESS_RAW, day, 0, source, job, []).state == PartitionState.UNVERIFIED


def test_legacy_reference_current_table_cannot_stand_in_for_historical_snapshot():
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_BASIC
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day, 'stock_basic', 'stock_basic', 'single_call')
    result = legacy_completion_result(STOCK_BASIC, day, 0, source, job,
        [{"ts_code": '000001.SZ', 'list_date': date(1991, 4, 3)}])
    assert result.state == PartitionState.UNVERIFIED
    assert result.reason == 'historical_reference_scope_unproven'


def test_legacy_etf_share_validation_reuses_publication_quality():
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import ETF_SHARE_SIZE
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day, 'etf_share_size', 'etf_share_size')
    facts = [{'ts_code': '510300.SH', 'trade_date': day, 'total_share': 10, 'total_size': None}]
    assert legacy_completion_result(ETF_SHARE_SIZE, day, 0, source, job, facts).state == PartitionState.INCOMPLETE
    facts[0]['total_size'] = 20
    assert legacy_completion_result(ETF_SHARE_SIZE, day, 0, source, job, facts).state == PartitionState.VALID


@pytest.mark.parametrize('mutation', ['duplicate', 'wrong_date', 'missing_date', 'blank_identity'])
def test_legacy_physical_identity_defects_remain_explicit(mutation):
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_ST
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day)
    facts = [{'ts_code': '000001.SZ', 'ann_date': day}]
    if mutation == 'duplicate':
        facts.append(dict(facts[0]))
    elif mutation == 'wrong_date':
        facts[0]['ann_date'] = date(2026, 10, 20)
    elif mutation == 'missing_date':
        facts[0]['ann_date'] = None
    else:
        facts[0]['ts_code'] = ''
    assert legacy_completion_result(STOCK_ST, day, 0, source, job, facts).state == PartitionState.INCOMPLETE


def test_legacy_supported_scope_retains_old_raw_count_without_requiring_bse():
    from copy import deepcopy
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_ST
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day, count=2)
    facts = [{'ts_code': '000001.SZ', 'ann_date': day}, {'ts_code': '920296.BJ', 'ann_date': day}]
    original = deepcopy((source, job, facts))
    result = legacy_completion_result(STOCK_ST, day, 0, source, job, facts)
    assert result.state == PartitionState.VALID
    assert result.checks['physical_rows'] == 1 and result.counts[0].expected == 2
    assert (source, job, facts) == original


def test_dated_legacy_etf_snapshot_requires_all_official_param_sets():
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import ETF_BASIC_SNAPSHOTS
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day, 'etf_basic_snapshots', 'etf_basic', 'single_call')
    facts = [{'snapshot_date': day, 'ts_code': '510300.SH'}]
    assert legacy_completion_result(ETF_BASIC_SNAPSHOTS, day, 0, source, job, facts).state == PartitionState.UNVERIFIED
    job['summary']['stats'].update(total_batches=3, success_batches=3)
    assert legacy_completion_result(ETF_BASIC_SNAPSHOTS, day, 0, source, job, facts).state == PartitionState.VALID
    facts[0]['snapshot_date'] = date(2026, 10, 20)
    assert legacy_completion_result(ETF_BASIC_SNAPSHOTS, day, 0, source, job, facts).state == PartitionState.INCOMPLETE


@pytest.mark.parametrize('variant', ['new_revision', 'old_bound_revision', 'latest_failure', 'partial_quality', 'good'])
def test_legacy_native_scan_fallback_is_bounded_and_preserves_new_contracts(monkeypatch, variant):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day)
    if variant == 'old_bound_revision':
        source['metadata']['integrity_observed_revision'] = 1
    if variant == 'latest_failure':
        source['status'] = 'failed'
    if variant == 'partial_quality':
        source['quality_status'] = 'low_coverage'
    queries = []
    def read(_conn, query, params=()):
        queries.append((query, params))
        if 'dataset_date_refresh_audit' in str(query):
            return [source]
        if 'ingestion_jobs' in str(query):
            return [job]
        return [{'ts_code': '000001.SZ', 'ann_date': day}]
    monkeypatch.setattr(validation, 'rows', read)
    result = validation.PhysicalValidator().validate(None,
        Obligation('stock_st', 'event', '17:00'), day, 1 if variant == 'new_revision' else 0)
    expected = {'new_revision': PartitionState.UNVERIFIED, 'old_bound_revision': PartitionState.UNVERIFIED,
        'latest_failure': PartitionState.WAIT_UPSTREAM, 'partial_quality': PartitionState.INCOMPLETE,
        'good': PartitionState.VALID}
    assert result.state == expected[variant]
    assert len(queries) == (3 if variant == 'good' else 1)


def test_legacy_source_only_validation_cannot_trigger_fetches_or_false_green_without_scope():
    from backend.services.data_integrity.legacy_validation import validate_legacy_partition
    from backend.services.data_integrity.catalog import Obligation
    result = validate_legacy_partition(None, Obligation('anns_metadata', 'event', '23:59'),
        date(2026, 10, 19), 0, [], read=lambda *_: [
            {'ts_code': '000001.SZ', 'ann_date': date(2026, 10, 19), 'title': 'real title'}])
    assert result.state == PartitionState.UNVERIFIED
    assert result.checks['physical_rows'] == 1
    assert result.reason == 'historical_source_query_unproven'


def test_legacy_unproven_history_does_not_create_repeat_fetch_target(monkeypatch):
    from backend.services.data_integrity.worker import DailyIntegrityWorker
    from backend.services.data_integrity.catalog import Obligation
    from backend.services.data_integrity.legacy_validation import legacy_completion_result
    from backend.services.tushare_dataset_specs import STOCK_ST
    day = date(2026, 10, 19)
    source, job = _legacy_completion(day)
    job['status'] = 'failed'
    result = legacy_completion_result(STOCK_ST, day, 0, source, job,
        [{'ts_code': '000001.SZ', 'ann_date': day}])
    worker = DailyIntegrityWorker(repository=object())
    worker._repair_target(Obligation('stock_st', 'event', '17:00'), day, result, revision=0)


@pytest.mark.parametrize("dataset", ["bak_basic", "cyq_perf", "market.bak_basic"])
def test_dated_consolidation_exclusion_is_one_bounded_shared_query(dataset):
    day = date(2026, 10, 23)
    conn = Connection([("002731.SZ", day)])
    excluded = stock_scope.delisting_panel_exclusions(conn, dataset, day, {"002731.SZ", "000001.SZ"})
    assert excluded == {"002731.SZ": day}
    assert len(conn.query.calls) == 1
    statement, parameters = conn.query.calls[0]
    assert "market.stock_st_events" in statement and "ANY(" in statement
    assert "pub_date" in statement and "imp_date" in statement
    assert "stock_basic" in statement and "list_date" in statement
    assert "stock_universe_pit" not in statement
    assert parameters[0] == ["000001.SZ", "002731.SZ"]
    assert parameters[1] == "退市整理期"
    assert parameters[2:] == (day, day, day)


@pytest.mark.parametrize("dataset", ["kline_daily_raw", "market.kline_minute_raw", "daily_basic", "stk_limit", "moneyflow_ts"])
def test_consolidation_scope_never_hides_holdings_quotes_or_other_facts(dataset):
    conn = Connection([("002731.SZ", date(2026, 10, 23))])
    assert stock_scope.delisting_panel_exclusions(conn, dataset, date(2026, 10, 29), {"002731.SZ"}) == {}
    assert not conn.query.calls


def test_no_codes_means_no_extra_query():
    conn = Connection()
    assert stock_scope.delisting_panel_exclusions(conn, "bak_basic", date(2026, 10, 23), set()) == {}
    assert not conn.query.calls


def test_only_dated_rows_are_filtered_and_existing_rows_are_not_deleted(monkeypatch):
    from backend.services.tushare_dataset_specs import BAK_BASIC

    rows = [{"ts_code": "002731.SZ", "trade_date": "20261022"},
            {"ts_code": "002731.SZ", "trade_date": "20261023"},
            {"ts_code": "000001.SZ", "trade_date": "20261023"}]
    monkeypatch.setattr(stock_scope, "delisting_panel_exclusions", lambda _c, _ds, d, _codes:
                        {"002731.SZ": date(2026, 10, 23)} if d >= date(2026, 10, 23) else {})
    original = [dict(row) for row in rows]
    assert stock_scope.filter_delisting_panel_rows(None, BAK_BASIC, rows) == [rows[0], rows[2]]
    assert rows == original


def test_unknown_event_remains_required_instead_of_using_current_delisted_name():
    conn = Connection()
    assert stock_scope.delisting_panel_exclusions(conn, "bak_basic", date(2026, 1, 1), {"000004.SZ"}) == {}
    assert "delist_date -" not in conn.query.calls[0][0]


@pytest.mark.parametrize("dataset", ["bak_basic", "cyq_perf"])
@pytest.mark.parametrize("ordinary_missing", [False, True])
def test_validator_uses_same_scope_and_keeps_real_ordinary_gaps(monkeypatch, dataset, ordinary_missing):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation, RULE_VERSION, SCOPE_VERSION
    from backend.services.data_integrity.contracts import PartitionState

    raw = {"data_revision": 1, "validated_revision": 1, "validation_id": "raw-valid",
           "scope_version": SCOPE_VERSION, "rule_version": RULE_VERSION, "validation_status": "VALID"}
    fact = dict(ts_code="000001.SZ", his_low=1, his_high=2, cost_15pct=1, cost_50pct=1,
                cost_85pct=2, weight_avg=1, winner_rate=0)
    validator = validation.PhysicalValidator()
    monkeypatch.setattr(validator, "_traded", lambda *_: {"000001.SZ", "002731.SZ"})
    monkeypatch.setattr(stock_scope, "delisting_panel_exclusions", lambda *_: {"002731.SZ": date(2026, 10, 23)})
    monkeypatch.setattr(validation, "rows", lambda _c, statement, *_: [raw]
                        if "dataset_date_refresh_audit" in str(statement) else [] if ordinary_missing else [fact])
    result = validator.validate(None, Obligation(dataset, "daily_series", "21:00"), date(2026, 10, 29), 1)
    assert result.state == (PartitionState.INCOMPLETE if ordinary_missing else PartitionState.VALID)
    assert result.counts[0].expected == 1
    assert result.checks["excluded_delisting_symbols"] == ["002731.SZ"]
    assert result.checks["applicability_policy"] == stock_scope.DELISTING_PANEL_POLICY
    assert all("002731.SZ" not in gap.get("symbols", []) for gap in result.gaps)


def test_dense_supplement_does_not_request_excluded_security(monkeypatch):
    from backend.services import tushare_sync_engine as module
    from backend.services.tushare_dataset_specs import BAK_BASIC

    engine = module.TushareSyncEngine()
    monkeypatch.setattr("backend.services.data_integrity.validation.PhysicalValidator._traded",
                        lambda *_: {"000001.SZ", "002731.SZ"})
    monkeypatch.setattr(stock_scope, "delisting_panel_exclusions", lambda *_: {"002731.SZ": date(2026, 10, 23)})
    monkeypatch.setattr("backend.services.data_integrity.validation.rows", lambda *_: [{"ts_code": "000001.SZ"}])
    monkeypatch.setattr(engine, "_fetch_from_tushare", lambda *_: pytest.fail("excluded key must not be fetched"))
    receipt, written = engine._complete_dense_panel(None, BAK_BASIC, date(2026, 10, 29))
    assert written == 0 and receipt["missing_symbols"] == [] and receipt["expected_rows"] == 1
    assert receipt["excluded_delisting_symbols"] == ["002731.SZ"]


def test_real_upsert_filters_only_panels_before_writing(monkeypatch):
    from backend.services.tushare_sync_engine import TushareSyncEngine
    from backend.services.tushare_dataset_specs import BAK_BASIC

    engine = TushareSyncEngine()
    monkeypatch.setattr(stock_scope, "delisting_panel_exclusions", lambda *_: {"002731.SZ": date(2026, 10, 23)})
    written = []
    monkeypatch.setattr(engine, "_upsert_plain_batch", lambda _c, _s, rows: written.extend(rows) or len(rows))
    rows = [{"ts_code": "002731.SZ", "trade_date": "20261029"},
            {"ts_code": "000001.SZ", "trade_date": "20261029"}]
    assert engine._upsert_batch(None, BAK_BASIC, rows) == 1
    assert written == [rows[1]]


def test_late_lifecycle_event_wakes_only_affected_old_validation_targets(monkeypatch):
    from backend.services.data_integrity import repository

    day = date(2026, 10, 29)
    conn = Connection([('bak_basic', day)])
    queued = []
    monkeypatch.setattr(repository, 'schema_installed', lambda _: True)
    monkeypatch.setattr(repository, 'enqueue_validation', lambda _c, ds, d, **kw: queued.append((ds, d, kw)))
    assert stock_scope.wake_delisting_panel_validations(conn, [{'ts_code': '002731.SZ', 'st_type': '退市整理期'}]) == 1
    assert queued == [('bak_basic', day, {'source_completed': True})]
    statement, _ = conn.query.calls[0]
    assert "p.validation_status='INCOMPLETE'" in statement
    assert "excluded_delisting_symbols" in statement and "'{validation,gaps}'" in statement


def test_ordinary_st_event_does_not_wake_or_exempt_panels():
    conn = Connection()
    assert stock_scope.wake_delisting_panel_validations(conn, [{'ts_code': '002731.SZ', 'st_type': '*ST'}]) == 0
    assert not conn.query.calls


def _hk_session(day=date(2026, 10, 19), is_open=0):
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    facts = {day: [{'ts_code': 'H11100.CSI', 'close': None}]}
    return capture_index_sessions(facts, lambda _a, _b:
        [{'cal_date': day.strftime('%Y%m%d'), 'is_open': is_open}])[day]


def test_index_hk_holiday_placeholder_is_legal_without_manufacturing_prices():
    from backend.services.data_integrity.validation import index_quote_result
    row = dict(close=None, open=None, high=None, low=None, vol=0, amount=0, pre_close=2125.5917)
    original = dict(row)
    result = index_quote_result({'H11100.CSI'}, {'H11100.CSI': row},
        session=_hk_session(), trade_date=date(2026, 10, 19))
    assert result.state == PartitionState.VALID
    assert result.counts[0].expected == result.counts[0].legal_na == 1
    assert result.counts[0].valid == 0 and not result.gaps
    assert row == original and row['close'] is None


@pytest.mark.parametrize('code', ['000300.SH', '930999.CSI', 'UNKNOWN.CSI'])
def test_index_hk_calendar_does_not_exempt_domestic_mixed_or_unknown_indices(code):
    from backend.services.data_integrity.validation import index_quote_result
    result = index_quote_result({code}, {code: dict(close=None, vol=0, amount=0)},
        session=_hk_session(), trade_date=date(2026, 10, 19))
    assert result.state == PartitionState.INCOMPLETE
    assert result.counts[0].invalid == 1 and result.counts[0].legal_na == 0


@pytest.mark.parametrize('session_kind', ['open', 'absent', 'wrong_date', 'tampered', 'unknown'])
def test_index_hk_empty_quote_requires_correct_confirmed_closed_session(session_kind):
    from backend.services.data_integrity.validation import index_quote_result
    session = _hk_session(is_open=1) if session_kind == 'open' else _hk_session()
    if session_kind == 'absent':
        session = None
    elif session_kind == 'wrong_date':
        session = _hk_session(day=date(2026, 10, 20))
    elif session_kind == 'tampered':
        session['exchange'] = 'SSE'
    elif session_kind == 'unknown':
        session = {'status': 'UNAVAILABLE'}
    result = index_quote_result({'H11100.CSI'}, {'H11100.CSI': dict(close=None, vol=0, amount=0)},
        session=session, trade_date=date(2026, 10, 19))
    assert result.state != PartitionState.VALID
    assert result.counts[0].legal_na == 0


@pytest.mark.parametrize('change', [dict(close=0), dict(close=float('nan')),
    dict(close=-1), dict(open=1), dict(vol=1), dict(amount=1), dict(vol=float('nan'))])
def test_index_hk_closed_day_does_not_hide_bad_or_nonplaceholder_values(change):
    from backend.services.data_integrity.validation import index_quote_result
    row = dict(close=None, open=None, high=None, low=None, vol=0, amount=0)
    row.update(change)
    result = index_quote_result({'H11100.CSI'}, {'H11100.CSI': row},
        session=_hk_session(), trade_date=date(2026, 10, 19))
    assert result.state == PartitionState.INCOMPLETE
    assert result.counts[0].legal_na == 0


def test_index_hk_calendar_never_invents_absent_rows_or_rejects_valid_published_quotes():
    from backend.services.data_integrity.validation import index_quote_result
    missing = index_quote_result({'H11100.CSI'}, {}, session=_hk_session(), trade_date=date(2026, 10, 19))
    assert missing.state != PartitionState.VALID and missing.counts[0].missing == 1
    present = index_quote_result({'H11100.CSI'}, {'H11100.CSI': dict(close=2125.5)},
        session=_hk_session(), trade_date=date(2026, 10, 19))
    assert present.state == PartitionState.VALID and present.counts[0].valid == 1


def test_index_calendar_capture_is_bounded_and_not_needed_for_normal_quotes():
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    calls = []
    def query(a, b):
        calls.append((a, b))
        return [{'cal_date': '20261019', 'is_open': 0}]
    days = {date(2026, 10, 19): [{'ts_code': 'H11100.CSI', 'close': None},
                                {'ts_code': '930709.CSI', 'close': None}]}
    assert capture_index_sessions(days, query)[date(2026, 10, 19)]['is_open'] is False
    assert calls == [('20261019', '20261019')]
    assert capture_index_sessions({date(2026, 10, 19): [{'ts_code':'000300.SH','close':None}]},
        lambda *_: pytest.fail('do not fetch HK calendar for domestic missing quote')) == {}
    assert capture_index_sessions({date(2026, 10, 20): [{'ts_code':'H11100.CSI','close':2096}]},
        lambda *_: pytest.fail('positive published quotes need no extra calendar query')) == {}


@pytest.mark.parametrize('returned', [[], [{'cal_date': '20261019', 'is_open': None}],
    [{'cal_date': '20261019', 'is_open': float('nan')}],
    [{'cal_date': '20261019', 'is_open': 0}, {'cal_date': '20261019', 'is_open': 1}]])
def test_index_calendar_missing_invalid_or_conflicting_response_is_not_closed(returned):
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    result = capture_index_sessions({date(2026, 10, 19): [{'ts_code':'H11100.CSI','close':None}]},
        lambda *_: returned)
    assert result[date(2026, 10, 19)]['status'] == 'UNAVAILABLE'


def test_index_calendar_unrelated_malformed_row_does_not_block_confirmed_neighbor():
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    result = capture_index_sessions({date(2026, 10, 19): [{'ts_code':'H11100.CSI','close':None}],
                                    date(2026, 10, 20): [{'ts_code':'H11100.CSI','close':None}]},
        lambda *_: [{'cal_date':'20261019','is_open':0}, {'cal_date':'bad','is_open':0}])
    assert result[date(2026, 10, 19)]['status'] == 'CONFIRMED'
    assert result[date(2026, 10, 20)]['status'] == 'UNAVAILABLE'


def test_index_calendar_transport_failure_stays_explicit_without_discarding_quotes():
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    def failed(*_):
        raise RuntimeError('upstream calendar unavailable')
    result = capture_index_sessions({date(2026, 10, 19): [{'ts_code':'H11100.CSI','close':None}]}, failed)
    assert result[date(2026, 10, 19)]['status'] == 'UNAVAILABLE'
    assert result[date(2026, 10, 19)]['reason'] == 'RuntimeError'


def test_index_native_provider_scan_receives_dated_session_contract(monkeypatch):
    from backend.services.data_integrity import validation
    from backend.services.data_integrity.catalog import Obligation
    from backend.services.data_integrity.provider import provider_scan
    from backend.services.tushare_dataset_specs import INDEX_DAILY
    day = date(2026, 10, 19)
    row = dict(ts_code='H11100.CSI', trade_date=day, close=None, vol=0, amount=0)
    metadata = dict(integrity_observed_revision=7, provider_query_scope_complete=True,
        provider_scan=provider_scan(INDEX_DAILY, [row], parameters={'trade_date': str(day)}),
        index_market_session=_hk_session())
    source = dict(status='success', quality_status='ok', data_source='tushare', metadata=metadata)
    monkeypatch.setattr(validation, 'rows', lambda _c, query, *_:
        [source] if "data_source<>" in str(query) else [row])
    result = validation.PhysicalValidator().validate(None, Obligation('index_daily','daily_series','16:50'), day, 7)
    assert result.state == PartitionState.VALID and result.counts[2].legal_na == 1
    assert result.counts[0].valid == 1 and not result.gaps
    metadata['index_market_session'] = _hk_session(is_open=1)
    assert validation.PhysicalValidator().validate(None,
        Obligation('index_daily','daily_series','16:50'), day, 7).state == PartitionState.INCOMPLETE


def test_index_calendar_backfill_queries_are_year_bounded_and_reuse_each_response():
    from backend.services.data_integrity.index_quote_calendar import capture_index_sessions
    days = [date(2024, 1, 2), date(2024, 12, 30), date(2025, 1, 2)]
    facts = {day: [{'ts_code':'H11100.CSI','close':None}] for day in days}
    calls = []
    def query(start, end):
        calls.append((start, end))
        return [{'cal_date': day.strftime('%Y%m%d'), 'is_open':0} for day in days
                if start <= day.strftime('%Y%m%d') <= end]
    sessions = capture_index_sessions(facts, query)
    assert calls == [('20240102','20241230'), ('20250102','20250102')]
    assert all(row['status'] == 'CONFIRMED' for row in sessions.values())
