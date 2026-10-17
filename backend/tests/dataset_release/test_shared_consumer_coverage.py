from datetime import date

import pytest

from backend.services.dataset_release.shared_consumer_coverage import (
    audit_causal_source_history,
    validate_live_pit_lease_identity,
)


def test_causal_history_is_not_clipped_at_pit_entry():
    days = (date(2024, 7, 1), date(2024, 7, 2))
    rows = [{"trade_date": days[0], "ts_code": "000001.SZ", "circ_mv": 10.0}]
    result = audit_causal_source_history(
        rows, calendar=days, spans=(("000001.SZ", days[1], days[1]),), history_start=days[0],
    )
    assert result["resolved"] == 1
    assert result["unexplained"] == 0
    assert result["pre_entry_facts_used"] == 1


@pytest.mark.parametrize("value", [None, float("nan"), 0, -1, True])
def test_latest_invalid_fact_cannot_fall_back_to_older_finite_cap(value):
    days = tuple(date(2024, 7, day) for day in (1, 2, 3))
    rows = [{"trade_date": days[0], "ts_code": "000001.SZ", "circ_mv": 10.0},
            {"trade_date": days[1], "ts_code": "000001.SZ", "circ_mv": value}]
    result = audit_causal_source_history(rows, calendar=days,
        spans=(("000001.SZ", days[2], days[2]),), history_start=days[0])
    assert result["unexplained"] == 1
    assert result["issues"][0]["reason"] == "LATEST_CAUSAL_VALUE_INVALID"


def test_current_value_is_not_a_causal_source_and_warmup_is_exact():
    days = (date(2021, 8, 9), date(2021, 8, 10), date(2021, 8, 11))
    result = audit_causal_source_history(
        [{"trade_date": days[1], "ts_code": "000792.SZ", "circ_mv": 2}],
        calendar=days, spans=(("000792.SZ", days[1], days[2]),), history_start=days[0],
        approved_warmup_keys=frozenset({("000792.SZ", days[1])}),
    )
    assert result["explained_warmup"] == result["resolved"] == 1
    assert result["unexplained"] == 0


def test_warmup_does_not_hide_invalid_latest_fact():
    days = (date(2021, 8, 9), date(2021, 8, 10))
    result = audit_causal_source_history(
        [{"trade_date": days[0], "ts_code": "000792.SZ", "circ_mv": None}],
        calendar=days, spans=(("000792.SZ", days[1], days[1]),), history_start=days[0],
        approved_warmup_keys=frozenset({("000792.SZ", days[1])}),
    )
    assert result["explained_warmup"] == 0
    assert result["unexplained"] == 1


@pytest.mark.parametrize("rows", [
    [{"trade_date": date(2024,7,1), "ts_code":"000001.SZ", "circ_mv":1}]*2,
    [{"trade_date": date(2024,7,2), "ts_code":"000001.SZ", "circ_mv":1},
     {"trade_date": date(2024,7,1), "ts_code":"000001.SZ", "circ_mv":1}],
])
def test_duplicate_or_unordered_source_fails_closed(rows):
    with pytest.raises(ValueError):
        audit_causal_source_history(rows, calendar=(date(2024,7,1),date(2024,7,2)),
            spans=(("000001.SZ",date(2024,7,2),date(2024,7,2)),), history_start=date(2024,7,1))


def test_live_legacy_is_not_equivalent_to_ready_canonical_component():
    result = validate_live_pit_lease_identity(
        {"authority_status":"DEPLOYED_LEGACY_PENDING_MIGRATION", "activation_generation":0,
         "universe_key":"shsz_st_pit_active_v1"}, native_leases=(),
    )
    assert result["status"] == "BLOCKED"
    assert result["reason"] == "CANONICAL_AUTHORITY_ACTIVATION_REQUIRED"


def test_no_native_lease_cannot_be_claimed_consumer_readback():
    from backend.services.canonical_equity_pit import canonical_rule_parameters_digest

    active = {"authority_status":"ACTIVE_CANONICAL", "activation_generation":1,
        "authority_id":"aistock_equity_pit_canonical", "universe_key":"aistock_equity_pit_canonical_v2",
        "rule_version":"shsz_a_252td_st_delist_asof_v2", "rule_parameters_digest":canonical_rule_parameters_digest(),
        "activation_envelope_digest":"b"*64, "expected_source_commit":"c"*40,
        "state_source_digest":"d"*64, "coverage_start":"2018-08-01", "coverage_end":"2026-09-30"}
    assert validate_live_pit_lease_identity(active, native_leases=())["reason"] == "NATIVE_SELECTION_LEASE_NOT_OBSERVED"
    lease={**active,"schema_version":"selection_canonical_pit_runtime_lease_v1"}
    assert validate_live_pit_lease_identity(active,native_leases=(lease,))["status"] == "PASS"
    lease["activation_generation"]=0
    assert validate_live_pit_lease_identity(active,native_leases=(lease,))["status"] == "BLOCKED"


def test_incremental_required_dates_still_use_previous_full_market_day_alias():
    from types import SimpleNamespace
    seen=[]
    class Identity:
        def resolve(self, symbol, day, dataset):
            seen.append((symbol,day,dataset))
            return SimpleNamespace(source_ts_code="000002.SZ")
    days=tuple(date(2024,7,day) for day in (1,2,3))
    result=audit_causal_source_history(
        [{"trade_date":days[0],"ts_code":"000002.SZ","circ_mv":5}],
        calendar=days,spans=(("000001.SZ",days[2],days[2]),),history_start=days[0],
        required_dates=frozenset({days[2]}),source_identity=Identity(),
    )
    assert result["resolved"]==result["expected"]==1
    assert seen==[("000001.SZ",days[1],"market.daily_basic")]


def test_required_key_callback_counts_only_applicable_causal_obligations():
    days=tuple(date(2024,7,day) for day in (1,2,3))
    checked=[]
    result=audit_causal_source_history(
        [{"trade_date":days[0],"ts_code":"000001.SZ","circ_mv":10}],
        calendar=days,spans=(("000001.SZ",days[0],days[-1]),),history_start=days[0],
        not_applicable_keys=frozenset({("000001.SZ",days[1])}),
        on_required_key=lambda symbol,day,prior: checked.append((symbol,day,prior)),
    )
    assert result["expected"]==3
    assert result["explained_warmup"]==result["not_applicable"]==result["resolved"]==1
    assert checked==[("000001.SZ",days[2],(days[0],10))]


def test_live_operator_uses_native_success_status_and_readonly_transaction(monkeypatch):
    from dataclasses import dataclass
    from types import SimpleNamespace
    import psycopg2
    from scripts import audit_shared_dataset_consumer_coverage as cli
    executed=[]
    transactions=[]
    @dataclass
    class Binding:
        authority_status: str="DEPLOYED_LEGACY_PENDING_MIGRATION"
        activation_generation: int=0
    class Cursor:
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def execute(self,sql,parameters=None):executed.append((sql,parameters))
        def fetchone(self):
            return ("selection.run",) if len(executed)==1 else ("sel_native",date(2026,9,30),None)
    class Connection:
        def set_session(self,**kwargs):transactions.append(kwargs)
        def cursor(self):return Cursor()
        def rollback(self):pass
        def close(self):pass
    class Resolver:
        def __init__(self,**kwargs):pass
        def resolve_live_binding(self):return Binding()
    monkeypatch.setattr(cli,"_load_database_config",lambda *args:SimpleNamespace(host="dev",port=5433,user="reader",password="",dbname="dev"))
    monkeypatch.setattr(psycopg2,"connect",lambda **kwargs:Connection())
    monkeypatch.setattr(cli,"CanonicalPitAuthorityResolver",Resolver)
    result=cli.read_live_pit_consumer("dev",None)
    assert transactions==[{"readonly":True,"isolation_level":"REPEATABLE READ"}]
    assert executed[-1][1]==("SUCCEEDED",)
    assert "canonical_pit_authority' IS NOT NULL" not in executed[-1][0]
    assert result["native_selection_run_id"]=="sel_native"
    assert result["consumer_gate"]["reason"]=="CANONICAL_AUTHORITY_ACTIVATION_REQUIRED"
    assert not result["database_write"] and not result["outcomes_read"]


@pytest.mark.parametrize("cap", [None, 0, -1, float("inf"), 3])
def test_monthly_gate_checks_required_current_caps(cap):
    from backend.services.dataset_release.monthly_frozen_source_audit import GateCounter, audit_month_rows
    from backend.services.dataset_release.monthly_unified import SOURCE_GATES
    day=date(2026,9,30)
    gates={key:GateCounter(key) for key in SOURCE_GATES}
    audit_month_rows({"daily_basic":[{"trade_date":day,"ts_code":"000001.SZ",
         "turnover_rate":1,"turnover_rate_f":1,"volume_ratio":1,"total_mv":10,"circ_mv":cap}]},
         sessions=(day,),pools={"stock_universe":{day:{"000001.SZ"}}},gates=gates,
         authority_sha256="a"*64,minute_start=date(2026,10,1))
    assert gates["daily_basic_required_fields"].invalid_count == (0 if cap==3 else 1)
