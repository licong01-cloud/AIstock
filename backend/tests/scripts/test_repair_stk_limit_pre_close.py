from datetime import date
from decimal import Decimal
import hashlib

import pytest

from scripts.repair_stk_limit_pre_close import applicable_classification, plan_repairs, repair_sql


DAY = date(2026, 5, 8)
ROW = {"ts_code": "001205.SZ", "trade_date": "20260508", "pre_close": 15.30,
       "up_limit": 16.83, "down_limit": 13.77}


def test_repair_uses_same_day_source_and_only_null_pre_close():
    patches, unresolved = plan_repairs(DAY, [ROW], [{**ROW, "pre_close": None}])
    assert len(patches) == 1 and not unresolved
    sql = repair_sql(DAY, patches, database="aistock")
    assert "UPDATE market.stk_limit t SET pre_close=src.pre_close" in sql
    assert "AND t.pre_close IS NULL" in sql
    assert "current_database() <> 'aistock'" in sql
    assert "t.up_limit=src.up_limit AND t.down_limit=src.down_limit" in sql
    assert "repair readback differs" in sql
    assert "15.3::numeric" in sql
    assert "INSERT INTO" not in sql
    assert "simulation" not in sql
    assert plan_repairs(DAY, [ROW], [ROW]) == ([], [])
    assert plan_repairs(DAY, [ROW], [{**ROW, "pre_close": 14.5}]) == ([], [])


@pytest.mark.parametrize("change,reason", [
    ({"pre_close": None}, "provider_missing_or_invalid"),
    ({"pre_close": 20}, "provider_missing_or_invalid"),
    ({"up_limit": 17}, "provider_limit_identity_differs"),
])
def test_unproven_or_conflicting_source_is_not_repaired(change, reason):
    patches, unresolved = plan_repairs(DAY, [{**ROW, **change}], [{**ROW, "pre_close": None}])
    assert patches == []
    assert unresolved == [{"ts_code": "001205.SZ", "reason": reason}]


@pytest.mark.parametrize("rows", [[ROW, ROW], [{**ROW, "trade_date": "20260507"}], [{**ROW, "ts_code": "invalid'"}]])
def test_source_duplicate_date_or_code_drift_fails_closed(rows):
    with pytest.raises(ValueError):
        plan_repairs(DAY, rows, [{**ROW, "pre_close": None}])


@pytest.mark.parametrize("invalid", [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), 0, -1])
def test_invalid_existing_reference_is_planned_with_exact_compare_and_swap(invalid):
    before = [{**ROW, "pre_close": invalid}]
    patches, unresolved = plan_repairs(DAY, [ROW], before)
    assert patches == [{**ROW, "pre_close": "15.3", "up_limit": "16.83", "down_limit": "13.77"}]
    assert not unresolved
    sql = repair_sql(DAY, patches, database="aistock", before=before)
    assert "t.pre_close IS NOT DISTINCT FROM src.old_pre_close" in sql
    assert "SET pre_close=src.pre_close" in sql
    assert "repair identity drift" in sql
    assert "SET up_limit" not in sql


def test_suspended_source_null_is_unresolved_not_previous_close_or_limit_estimate():
    source = {**ROW, "ts_code": "603616.SH", "trade_date": "20260121", "pre_close": None,
              "up_limit": 6.85, "down_limit": 5.61}
    before = [{**source, "pre_close": Decimal("NaN")}]
    patches, unresolved = plan_repairs(date(2026, 1, 21), [source], before)
    assert not patches
    assert unresolved == [{"ts_code": "603616.SH", "reason": "provider_missing_or_invalid"}]


def test_exact_repair_rejects_finite_existing_value_and_duplicate_before_keys():
    with pytest.raises(ValueError):
        repair_sql(DAY, [ROW], database="aistock", before=[ROW])
    old = {**ROW, "pre_close": None}
    with pytest.raises(ValueError):
        plan_repairs(DAY, [ROW], [old, old])


def test_quote_requirement_does_not_disappear_outside_pit_and_suspension_never_supplies_price():
    row = {**ROW, "pre_close": None, "has_trade": True, "in_pit": False,
           "limit_row_exists": True, "suspend_s": False, "suspend_r": False}
    assert applicable_classification(row) == "TRADED_PRICE_INVALID"
    assert applicable_classification({**row, "limit_row_exists": False}) == "TRADED_LIMIT_ROW_MISSING"
    row.update(has_trade=False, in_pit=True, suspend_s=True)
    assert applicable_classification(row) == "DOCUMENTED_SUSPENSION_NO_TRADE"
    assert applicable_classification({**row, "suspend_r": True}) == "MIXED_SUSPENSION_NO_TRADE_UNPROVEN"
    assert applicable_classification({**row, "suspend_s": False}) == "PIT_NO_TRADE_UNPROVEN"


def test_finite_reference_with_no_limit_sentinel_is_not_a_missing_pre_close():
    row = {**ROW, "ts_code": "920086.BJ", "has_trade": True, "in_pit": False,
           "limit_row_exists": True, "suspend_s": False, "suspend_r": False,
           "up_limit": 99999.99, "down_limit": 0}
    assert applicable_classification(row) == "TRADED_LIMIT_APPLICABILITY_UNPROVEN"
    assert plan_repairs(DAY, [], [row]) == ([], [])


def test_nonfinite_source_is_rejected_and_sql_database_target_is_allowlisted():
    with pytest.raises(ValueError):
        plan_repairs(DAY, [{**ROW, "pre_close": Decimal("NaN")}], [{**ROW, "pre_close": None}])
    for database in ("foreign_database", "aistock';DROP TABLE x;--"):
        with pytest.raises(ValueError):
            repair_sql(DAY, [ROW], database=database)


def test_sql_rejects_before_date_or_limit_drift():
    canonical, _ = plan_repairs(DAY, [ROW], [{**ROW, "pre_close": None}])
    for delta in ({"trade_date": "20260507"}, {"up_limit": 17}):
        with pytest.raises(ValueError):
            repair_sql(DAY, canonical, database="aistock", before=[{**ROW, "pre_close": None, **delta}])


def test_first_day_reference_requires_independent_listing_fact_and_preserves_no_limit_sentinel():
    row = {**ROW, "ts_code": "920178.BJ", "pre_close": "29.47", "up_limit": "99999.99", "down_limit": "0"}
    before = [{**row, "pre_close": None}]
    assert plan_repairs(DAY, [row], before)[0] == []
    facts = [{"ts_code": "920178.BJ", "list_date": DAY.isoformat()}]
    patches, unresolved = plan_repairs(DAY, [row], before, no_limit_listing_facts=facts)
    assert not unresolved and patches[0]["pre_close"] == "29.47"
    sql = repair_sql(DAY, patches, database="aistock_dev", before=before, no_limit_listing_facts=facts)
    assert "29.47::numeric,99999.99::numeric,0::numeric" in sql
    assert "SET down_limit" not in sql and "SET up_limit" not in sql
    for delta in ({"list_date": "2026-05-07"}, {"ts_code": "001205.SZ"}):
        with pytest.raises(ValueError, match="listing identity"):
            plan_repairs(DAY, [row], before, no_limit_listing_facts=[{**facts[0], **delta}])
    assert not plan_repairs(DAY, [{**row, "pre_close": None}], before, no_limit_listing_facts=facts)[0]


def test_delisting_first_day_requires_frozen_exchange_evidence_not_just_sentinel(tmp_path):
    document = tmp_path / "exchange.pdf"
    document.write_bytes(b"test fixture for digest binding only")
    row = {**ROW, "ts_code": "920305.BJ", "pre_close": "3.57", "up_limit": "99999.99", "down_limit": "0"}
    before = [{**row, "pre_close": None}]
    fact = {"ts_code": row["ts_code"], "event_date": DAY.isoformat(), "event_type": "DELISTING_FIRST_DAY",
            "source_url": "https://www.bse.cn/disclosure/test.pdf", "source_path": str(document),
            "source_content_sha256": hashlib.sha256(document.read_bytes()).hexdigest()}
    patches, unresolved = plan_repairs(DAY, [row], before, no_limit_event_facts=[fact])
    assert not unresolved and patches[0]["pre_close"] == "3.57"
    assert "SET pre_close=src.pre_close" in repair_sql(DAY, patches, database="aistock_dev", before=before,
                                                       no_limit_event_facts=[fact])
    for delta in ({"event_date": "2026-05-07"}, {"source_content_sha256": "0" * 64},
                  {"event_type": "UNKNOWN"}, {"source_url": "https://untrusted.example/evidence"}):
        with pytest.raises(ValueError, match="event authority"):
            plan_repairs(DAY, [row], before, no_limit_event_facts=[{**fact, **delta}])
