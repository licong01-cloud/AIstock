"""Direct contracts for the shared daily ledger (no market/network access)."""
from datetime import date, datetime, time, timezone
from decimal import Decimal

import pytest

from backend.services.data_integrity.contracts import (
    CheckCounts, PartitionState, ValidationResult, aggregate_day, effective_partition,
    completion_kind, finite_number, release_contains_revision,
)
from backend.services.data_integrity.consumers import ConsumerDependencies, project_readiness
from backend.services.data_integrity.provider import provider_scan, verify_scan
from backend.services.tushare_dataset_specs import DAILY_BASIC, DIVIDEND


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
    if mutation == "old_revision":
        source["metadata"]["integrity_observed_revision"] = 0
    monkeypatch.setattr(validation, "rows", lambda _c, query, _p=(): [source]
        if "dataset_date_refresh_audit" in str(query) else actual)
    result = validation.PhysicalValidator()._scan_contract(None, Obligation("index_daily", "daily_series", "16:50"), day, 1)
    assert result.state == expected
    if mutation == "valid":
        assert result.checks["coverage_basis"] == "complete_query_returned_keys_not_catalogue_availability"
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
