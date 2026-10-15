from datetime import date

import pytest

from scripts.repair_stk_limit_pre_close import plan_repairs, repair_sql


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
