from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from backend.services.simulation_runtime.localsim_workbench import LocalSimWorkbenchService, money
from backend.services.simulation_runtime.localsim_workbench_presentation import readable
from backend.services.trading_core.errors import RuntimeConfigInvalidError, DataUnavailableError


def facts():
    t = datetime(2026, 1, 5, 7, tzinfo=UTC)
    return {
        "account": {"account_id": "a", "account_name": "测试账户", "package_id": "p", "status": "ACTIVE", "initial_capital": "10000", "created_at": t},
        "package": {"display_name": "沪深300增强"}, "scope": "a", "native": True, "version": "generation-one",
        "snapshots": [
            {"run_id": "r1", "trade_date": "2026-01-05", "snapshot_time": t, "cash": "8995", "market_value": "1100", "nav": "10095", "position_count": 1},
            {"run_id": "r2", "trade_date": "2026-01-06", "snapshot_time": t.replace(day=6), "cash": "10190", "market_value": "0", "nav": "10190", "position_count": 0}],
        "positions": [{"run_id": "r1", "trade_date": "2026-01-05", "symbol": "600000.SH", "stock_name": "测试证券", "quantity": 100, "available_quantity": 0, "avg_cost": "10", "market_price": "11", "market_value": "1100"}],
        "cash": [
            {"cash_id": 1, "run_id": "r1", "fill_id": "f1", "trade_date": "2026-01-05", "symbol": "600000.SH", "side": "BUY", "notional": "1000", "fee": "5", "cash_delta": "-1005", "cash_after": "8995", "created_at": t},
            {"cash_id": 2, "run_id": "r2", "fill_id": "f2", "trade_date": "2026-01-06", "symbol": "600000.SH", "side": "SELL", "notional": "1200", "fee": "5", "cash_delta": "1195", "cash_after": "10190", "created_at": t.replace(day=6)}],
        "fills": [
            {"fill_id": "f1", "run_id": "r1", "order_id": "o1", "symbol": "600000.SH", "stock_name": "测试证券", "side": "BUY", "quantity": 100, "price": "10", "trade_time": t},
            {"fill_id": "f2", "run_id": "r2", "order_id": "o2", "symbol": "600000.SH", "stock_name": "测试证券", "side": "SELL", "quantity": 100, "price": "12", "trade_time": t.replace(day=6)}],
        "orders": [], "events": [], "replays": [],
        "runs": [{"run_id": "r1", "trade_date": "2026-01-05", "status": "SUCCEEDED", "started_at": t}, {"run_id": "r2", "trade_date": "2026-01-06", "status": "SUCCEEDED", "started_at": t.replace(day=6)}],
        "calendar": [{"cal_date": "2026-01-05"}, {"cal_date": "2026-01-06"}],
    }


class Service(LocalSimWorkbenchService):
    def __init__(self, data=None):
        self.data = data or facts()

    def _load(self, account_id, version=None):
        assert account_id == "a"
        if version and version != self.data["version"]:
            raise RuntimeConfigInvalidError("集合已更新")
        return deepcopy(self.data)


def test_closed_stock_and_actual_five_yuan_fees_reconcile_without_repricing():
    result = Service().view("a", "pnl", group_by="security")
    assert result.status == "complete"
    assert money(result.summary.pnl) == Decimal("190")
    assert money(result.summary.fees) == Decimal("10")
    assert money(result.summary.realized) == Decimal("200")
    assert money(result.summary.unrealized_change) == 0
    assert result.items[0].closing_quantity == 0
    assert money(result.items[0].pnl) == 190


def test_period_start_uses_previous_close_and_keeps_cleared_stock():
    result = Service().view("a", "pnl", start=date(2026, 1, 6), end=date(2026, 1, 6), group_by="security")
    assert money(result.summary.opening_nav) == 10095
    assert money(result.summary.pnl) == 95
    assert money(result.summary.unrealized_change) == -100
    assert result.items[0].opening_quantity == 100


def test_historical_holdings_are_run_scoped_and_empty_latest_is_real_zero():
    service = Service()
    assert service.view("a", "positions").total_count == 0
    old = service.view("a", "positions", as_of=date(2026, 1, 5))
    assert old.total_count == 1 and old.items[0].available == 0
    assert old.items[0].frozen is None
    assert service.view("a", "positions", as_of=date(2026, 1, 4)).status == "partial"


@pytest.mark.parametrize("fault", ["missing_start", "missing_day", "unknown_cash", "unbalanced_cash", "partial_positions"])
def test_incomplete_evidence_never_becomes_zero_or_fake_complete(fault):
    data = facts()
    if fault == "missing_start": data["native"] = False
    if fault == "missing_day": data["calendar"].append({"cal_date": "2026-01-04"})
    if fault == "unknown_cash": data["cash"][0]["symbol"] = None
    if fault == "unbalanced_cash": data["snapshots"][-1]["cash"] = "10200"
    if fault == "partial_positions": data["snapshots"][-1]["position_count"] = 1
    result = Service(data).view("a", "pnl", start=date(2026, 1, 4))
    assert result.status == "partial"
    assert result.warnings


def test_intraday_snapshot_cannot_become_close_to_close_return():
    data = facts()
    data["snapshots"][-1]["snapshot_time"] = datetime(2026, 1, 6, 1, 34, tzinfo=UTC)
    result = Service(data).view("a", "pnl", start=date(2026, 1, 6))
    assert result.summary.pnl is None
    assert result.status == "partial"


def test_cursor_scope_filters_and_generation_and_no_equal_visible_field_dedup():
    service = Service()
    result = service.view("a", "trades", page_size=1)
    second = service.view("a", "trades", page_size=1, cursor=result.next_cursor)
    assert result.items[0].key != second.items[0].key
    assert second.next_cursor is None and second.total_count == 2
    with pytest.raises(RuntimeConfigInvalidError):
        service.view("a", "trades", side="BUY", page_size=1, cursor=result.next_cursor)
    with pytest.raises(RuntimeConfigInvalidError):
        service.view("a", "trades", page_size=1, cursor=result.next_cursor+"x")
    service.data["version"] = "generation-two"
    with pytest.raises(RuntimeConfigInvalidError):
        service.view("a", "trades", page_size=1, cursor=result.next_cursor)


@pytest.mark.parametrize("name", ["qe_20261007_141342_0221", "精选 qe_20261007_141342_0221", "123456789abcdef123456789abcdef", "精选 C:/secret/file", "", "LSTM"])
def test_names_never_fall_back_to_machine_strings(name):
    assert readable(name, "名称待完善") == "名称待完善"
    assert readable("沪深300增强", "名称待完善") == "沪深300增强"


def test_export_is_complete_without_identity_columns_and_formula_safe():
    service = Service()
    output = service.export("a", "trades")
    assert "600000.SH" not in output and "generation-one" not in output
    assert "测试证券" in output and "交易费用合计" in output
    assert output.count("测试证券") == 2
    assert service._csv_cell("=危险公式") == "'=危险公式"
    assert service._csv_cell("-5.00") == "-5.00"


@pytest.mark.parametrize("fault", ["missing_fill", "missing_cash", "wrong_security", "wrong_notional", "wrong_fee_delta", "wrong_inventory", "wrong_nav"])
def test_independent_trade_cash_inventory_and_asset_oracles_reject_missing_or_conflicting_facts(fault):
    data = facts()
    if fault == "missing_fill": data["fills"].pop()
    if fault == "missing_cash": data["cash"].pop()
    if fault == "wrong_security": data["cash"][0]["symbol"] = "000001.SZ"
    if fault == "wrong_notional": data["cash"][0]["notional"] = "999"
    if fault == "wrong_fee_delta": data["cash"][0]["fee"] = "10"
    if fault == "wrong_inventory": data["fills"][0]["quantity"] = 200
    if fault == "wrong_nav": data["snapshots"][-1]["nav"] = "20000"
    result = Service(data).view("a", "pnl", group_by="security")
    assert result.summary.pnl is None and result.status == "partial"


def test_legacy_cost_inclusion_unknown_does_not_invent_realized_decomposition():
    data = facts()
    data["native"] = False
    result = Service(data).view("a", "pnl", start=date(2026, 1, 6), group_by="security")
    assert result.summary.pnl == "95"
    assert result.summary.realized is None and result.items[0].realized is None
    assert "尚待核验" in result.summary.cost_basis


def test_zero_mark_keeps_quantity_and_cost_but_never_invents_a_loss():
    data = facts()
    data["positions"][0].update(market_price="0", market_value="0")
    data["snapshots"][0].update(market_value="0", nav="8995")
    r = Service(data).view("a", "positions", as_of=date(2026, 1, 5))
    assert r.status == "partial" and r.items[0].quantity == 100
    assert r.items[0].cost == "10" and r.items[0].price is None and r.items[0].unrealized is None


def test_unknown_cash_retained_and_not_misclassified_as_external_flow_or_zero_fee():
    data = facts()
    data["cash"].append({**data["cash"][0], "cash_id": 3, "fill_id": None, "symbol": None, "cash_delta": "25", "notional": "25"})
    result = Service(data).view("a", "cash-flows")
    assert result.total_count == 3 and result.status == "partial"
    pnl = Service(data).view("a", "pnl")
    assert pnl.summary.net_flows is None and pnl.summary.return_pct is None


def test_order_association_contains_all_partial_fills_without_cross_order_rows():
    data = facts()
    data["orders"] = [{"order_id": "o1", "run_id": "r1", "symbol": "600000.SH", "stock_name": "测试证券", "side": "BUY", "quantity": 300, "filled_quantity": 100, "status": "PARTIALLY_FILLED", "created_at": data["fills"][0]["trade_time"]}]
    data["events"] = [{"event_id": "e1", "order_id": "o1", "event_type": "FILLED", "event_time": data["fills"][0]["trade_time"]}]
    service = Service(data)
    assert service.view("a", "orders", order_key="o1").items[0].remaining == 200
    assert {r.order_key for r in service.view("a", "trades", order_key="o1").items} == {"o1"}
    assert service.view("a", "events", order_key="o1").total_count == 1
    assert service.view("a", "trades", order_key="another-account-order").total_count == 0


def test_full_csv_has_more_than_two_pages_and_safe_account_metadata():
    data = facts()
    data["account"]["account_name"] = "=测试账户"
    original = data["fills"][0]
    original_cash = data["cash"][0]
    data["fills"] = [{**original, "fill_id": f"fill_{n}"} for n in range(451)]
    data["cash"] = [{**original_cash, "cash_id": n, "fill_id": f"fill_{n}"} for n in range(451)]
    output = Service(data).export("a", "trades")
    assert output.count("测试证券") == 451
    assert "'=测试账户" in output
    assert "600000.SH" not in output and "fill_" not in output


def test_missing_estimate_date_is_an_explicit_curve_gap_not_interpolation():
    data = facts()
    data["calendar"].insert(1, {"cal_date": "2026-01-04"})
    result = Service(data).view("a", "pnl", start=date(2026, 1, 4))
    gap = next(r for r in result.items if r.date == "2026-01-04")
    assert not gap.known and gap.value is None and gap.pnl is None


def test_sort_and_search_are_full_collection_filters_bound_to_cursor():
    service = Service()
    r = service.view("a", "trades", sort="price", descending=False, search="测试", page_size=1)
    assert r.items[0].price == "10"
    with pytest.raises(RuntimeConfigInvalidError):
        service.view("a", "trades", sort="price", descending=True, search="测试", page_size=1, cursor=r.next_cursor)
    assert service.view("a", "trades", search="不存在的简称").total_count == 0


def test_resource_cap_refuses_truncation(monkeypatch):
    from backend.services.simulation_runtime import localsim_workbench as module
    monkeypatch.setattr(module, "MAX_ROWS", 1)
    class Cursor:
        def execute(self, sql, args):
            assert args[-1] == 2
        def fetchall(self): return [{"x": 1}, {"x": 2}]
    with pytest.raises(DataUnavailableError):
        LocalSimWorkbenchService._read(Cursor(), "SELECT 1")


def test_realtime_aggregate_excludes_historical_and_misaligned_economic_clocks():
    from backend.services.simulation_runtime.localsim_workbench_models import WorkbenchAccount
    s = Service().view("a", "summary").summary
    s.comparable_live = True
    s.pnl, s.daily_pnl, s.daily_date = "190", "95", "2026-01-06"
    curve = Service().view("a", "pnl").items
    a = WorkbenchAccount(key="a", summary=s, curve=sorted(curve, key=lambda r:r.date))
    b = a.model_copy(deep=True); b.key="b"; b.summary.comparable_live=False
    c = a.model_copy(deep=True); c.key="c"; c.summary.economic_as_of="2026-01-05T15:00:00+08:00"
    r = LocalSimWorkbenchService._aggregate_overview([a,b,c])
    assert r.included_count == 1 and r.excluded_count == 2
    assert r.nav == "10190" and r.pnl == "190" and r.return_pct == "1.900"


def test_repeatable_read_cache_and_cursor_pin_old_facts_but_revalidate_account_scope(monkeypatch):
    from types import SimpleNamespace
    from backend.services.simulation_runtime import localsim_workbench as module
    data=facts(); queries=[]; scope=SimpleNamespace(ledger_scope_id="read-only-test-scope",scope_kind="SUCCESSOR_NATIVE")
    class Repository:
        calls=0
        def get_account(self, account_id):
            assert account_id=="a"; self.calls+=1
            return SimpleNamespace(package_id="p",model_dump=lambda **_:deepcopy(data["account"]))
        def resolve_ledger_scope_for_account(self, account_id): return scope
    class Cursor:
        rows=[]
        def __enter__(self): return self
        def __exit__(self,*_): pass
        def execute(self,sql,args=()):
            queries.append((sql,args));self.rows=[]
            if sql.startswith("SET"): return
            assert sql.lstrip().startswith("SELECT")
            if "strategy_pkg.package" in sql: self.rows=[deepcopy(data["package"])];return
            if "simulation_daily_run" in sql: return
            mapping={"daily_snapshots":"snapshots","positions":"positions","orders":"orders","cash_ledger":"cash","run":"runs","fills":"fills","order_events":"events","localsim_replay_job_v1":"replays"}
            for table,key in mapping.items():
                if "FROM paper_v2." in sql and sql.split("FROM paper_v2.",1)[1].split()[0] == table:
                    assert args[0] in {scope.ledger_scope_id,"a"};self.rows=deepcopy(data[key]);return
            if "market.trading_calendar" in sql: self.rows=deepcopy(data["calendar"])
        def fetchall(self): return self.rows
        def fetchone(self): return self.rows[0] if self.rows else None
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*_): pass
        def cursor(self,**_): return Cursor()
    repo=Repository();service=LocalSimWorkbenchService(repository=repo,conn_factory=Connection)
    first=service.view("a","trades",page_size=1);old=first.snapshot_set
    data["positions"][0]["avg_cost"]="9.99"  # old-day revaluation, latest snapshot clock unchanged
    second=service.view("a","summary")
    assert second.snapshot_set!=old
    frozen=service.view("a","trades",page_size=1,cursor=first.next_cursor)
    assert frozen.snapshot_set==old and len(frozen.items)==1
    assert repo.calls==3
    assert sum("REPEATABLE READ, READ ONLY" in sql for sql,_ in queries)==2
    scope.ledger_scope_id="changed-authority-scope"
    with pytest.raises(RuntimeConfigInvalidError):
        service.view("a","trades",page_size=1,cursor=first.next_cursor)
    module._CACHE.clear()


def test_typed_http_contract_and_complete_csv_without_shadowing_native_controls():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from backend.routers.simulation_runtime import router, get_localsim_workbench_service
    app=FastAPI();app.include_router(router)
    app.dependency_overrides[get_localsim_workbench_service]=lambda:Service()
    with TestClient(app) as client:
        response=client.get("/simulation-runtime/localsim/accounts/a/positions?as_of=2026-01-05")
        assert response.status_code==200
        assert response.json()["items"][0]["quantity"]==100
        assert "metadata" not in response.json()["items"][0]
        csv_response=client.get("/simulation-runtime/localsim/accounts/a/export?kind=trades")
        assert csv_response.status_code==200 and "600000.SH" not in csv_response.text
        assert "filename*=UTF-8''" in csv_response.headers["content-disposition"]
        assert client.get("/simulation-runtime/localsim/accounts/a/trades?page_size=201").status_code==422
    paths=[route.path for route in router.routes]
    assert paths.index("/simulation-runtime/localsim/accounts/{account_id}/ledger")<paths.index("/simulation-runtime/localsim/accounts/{account_id}/{view_kind}")
