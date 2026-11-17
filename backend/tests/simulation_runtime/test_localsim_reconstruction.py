"""Correction replay protects original facts and the native cash-time boundary."""
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from types import SimpleNamespace

import pytest
from backend.tests.paper_trading_v2.test_day_runner import make_paper_enabled_manifest

from backend.services.simulation_data.contracts import MinuteDataSource
from backend.services.simulation_data.daily_context import canonical_json_sha256, DailyTradingContextV1
from backend.services.simulation_execution.localsim.runtime import LocalSimBackend
from backend.services.strategy_package.execution_policy import local_sim_twap_only_policy_snapshot
from backend.services.trading_core.ledger import FeeModel
from backend.services.trading_core.models import OrderIntent, OrderSide, PositionLot
from backend.services.simulation_runtime import localsim_reconstruction as replay
from backend.services.simulation_runtime.localsim_reconstruction_inputs import (
    FrozenHistoricalMarketProvider, TZ, file_ref, money, publish, read, trusted_opening,
)


def day_input():
    day = date(2026, 11, 6)
    stamp = datetime.combine(day, time(9, 30), TZ)
    symbols = ["000001.SZ", "000002.SZ"]
    facts = {s: {"symbol": s, "trade_date": str(day), "pre_close": 10.0,
        "pre_close_source": "market.stk_limit.pre_close", "pre_close_evidence_hash": None,
        "up_limit": 11.0, "down_limit": 9.0, "price_basis": "raw", "stk_limit_row_hash": "a" * 64,
        "is_st": False, "st_source": "market.stock_st", "st_evidence_hash": "b" * 64,
        "is_suspended": False, "suspend_type": None, "suspend_timing": None,
        "suspend_source": "market.suspend_d", "board": "MAIN", "lot_rule": {"min_quantity": 100, "increment": 100}}
        for s in symbols}
    for s in symbols:
        facts[s]["stk_limit_row_hash"] = canonical_json_sha256({
            "source": "market.stk_limit", "symbol": s, "trade_date": str(day), "pre_close": 10.0,
            "up_limit": 11.0, "down_limit": 9.0, "price_basis": "raw"})
        facts[s].pop("pre_close_source")
        facts[s].pop("pre_close_evidence_hash")
    seed = {"schema_version": "daily_trading_context_v1", "trade_date": str(day), "timezone": "Asia/Shanghai",
        "plan_identity": "p", "binding_identity": "b", "package_identity": "pkg", "symbol_set": symbols,
        "symbol_set_hash": canonical_json_sha256(symbols), "calendar_service_snapshot_id": "calendar",
        "captured_at": stamp.isoformat(), "sources": {
            "stk_limit": {"source": "market.stk_limit"}, "stock_st": {"source": "market.stock_st"},
            "suspend_d": {"source": "market.suspend_d"}}, "symbols": facts}
    digest = canonical_json_sha256(seed)
    context = {**seed, "context_hash": digest, "context_id": "dtc_" + digest[:16]}
    DailyTradingContextV1.model_validate(context)
    markets = {}
    for s in symbols:
        bars = [{"symbol": s, "bar_time": (stamp + timedelta(minutes=i)).isoformat(),
                 "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
                 "volume": 0 if s == "000001.SZ" and i == 1 else 100000,
                 "is_suspended": False, "limit_up": 11.0, "limit_down": 9.0}
                 for i in [*range(1, 121), *range(211, 331)]]
        markets[s] = {"bars": bars, "market_context": {"data_source": "DB_HISTORICAL",
            "prev_close": 10.0, "limit_up": 11.0, "limit_down": 9.0,
            "full_day_high": 99.0, "full_day_volume": 999999,
            "suspend_status": {"is_suspended": False, "source": "market.suspend_d"}}}
    return {"trade_date": str(day), "daily_context": context, "markets": markets}


def test_historical_feed_is_causal_and_preserves_real_source():
    value = day_input()
    provider = FrozenHistoricalMarketProvider(value)
    context = provider.context
    reference = {"context_hash": context.context_hash, "symbol_fact": context.symbols["000001.SZ"].canonical_payload()}
    observed = provider.load_observed_intraday(symbol="000001.SZ", trade_date=context.trade_date,
        source=MinuteDataSource.DB_HISTORICAL, until_time=datetime(2026, 11, 6, 9, 31, tzinfo=TZ),
        frozen_daily_fact=reference)
    assert len(observed.minute_bars) == 1
    assert observed.source == MinuteDataSource.DB_HISTORICAL
    assert not any(k.startswith("full_day_") for k in observed.market_context)
    with pytest.raises(ValueError, match="OBSERVED_SOURCE_SCOPE"):
        provider.load_observed_intraday(symbol="000001.SZ", trade_date=context.trade_date,
            source=MinuteDataSource.TDX_REALTIME, until_time=datetime(2026, 11, 6, 9, 31, tzinfo=TZ),
            frozen_daily_fact=reference)


def test_causal_native_loop_cannot_spend_later_sell_proceeds():
    value = day_input()
    provider = FrozenHistoricalMarketProvider(value)
    day = provider.context.trade_date
    start = datetime.combine(day, time(9, 30), TZ)
    backend = LocalSimBackend(portfolio_id="recon", package_id="pkg", initial_cash=100,
        manifest=make_paper_enabled_manifest(), data_source=MinuteDataSource.DB_HISTORICAL, market_data_provider=provider,
        execution_policy=local_sim_twap_only_policy_snapshot(), causal_historical_replay=True,
        fee_model=FeeModel(open_cost=0, close_cost=0, min_cost=0),
        initial_positions={"000001.SZ": PositionLot(portfolio_id="recon", symbol="000001.SZ",
            quantity=300, available_quantity=300, avg_cost=10, trade_date=date(2026, 11, 5))})
    intents = [OrderIntent(intent_id=side.value, package_id="pkg", portfolio_id="recon", symbol=symbol,
        side=side, quantity=300 if side == OrderSide.SELL else 100, target_trade_date=day)
        for side, symbol in [(OrderSide.SELL, "000001.SZ"), (OrderSide.BUY, "000002.SZ")]]
    plan = SimpleNamespace(plan_id="p", target_trade_date=day, intents=intents, plan_payload_json={
        "daily_trading_context": value["daily_context"], "local_sim_execution_causality": {
            "eligible_bar_after": (start - timedelta(microseconds=1)).isoformat()}})
    backend.configure_execution_runtime(run_id="r", binding_id="b")
    backend.bind_execution_plan(plan=plan, as_of_time=start)
    for intent in intents:
        backend.submit_order_intent(intent)
    backend.advance_realtime_execution(as_of_time=start + timedelta(minutes=1))
    assert not backend.export_execution_snapshot()["fills"]
    backend.advance_realtime_execution(as_of_time=start + timedelta(minutes=2))
    fills = backend.export_execution_snapshot()["fills"]
    assert any(f.side == OrderSide.SELL for f in fills), backend.export_execution_snapshot()["execution_states"]
    assert all(f.trade_time >= start + timedelta(minutes=2) for f in fills)
    assert backend._ledger.cash >= 0


def test_opt_in_rejects_unsealed_provider():
    with pytest.raises(ValueError, match="REQUIRES_SEALED_PROVIDER"):
        LocalSimBackend(portfolio_id="p", package_id="pkg", initial_cash=100,
            manifest=None, data_source=MinuteDataSource.DB_HISTORICAL, causal_historical_replay=True,
            market_data_provider=object(), execution_policy=local_sim_twap_only_policy_snapshot())


def facts():
    return {"account": {"account_id": "a", "initial_capital": 1000}, "scope": "a",
        "runs": [{"run_id": "r", "trade_date": "2026-11-05"}],
        "snapshots": [{"run_id": "r", "trade_date": "2026-11-05", "snapshot_time": "2026-11-05T15:00:00+08:00",
            "position_count": 1, "cash": 500, "market_value": 500, "nav": 1000}],
        "positions": [{"run_id": "r", "trade_date": "2026-11-05", "symbol": "000001.SZ", "quantity": 100,
            "available_quantity": 100, "avg_cost": 5, "market_price": 5}],
        "fills": [{"fill_id": "f", "run_id": "r", "side": "BUY", "symbol": "000001.SZ", "quantity": 100}],
        "cash": [{"fill_id": "f", "trade_date": "2026-11-05", "cash_delta": -500}]}


@pytest.mark.parametrize("change,error", [
    (lambda d: d["snapshots"][0].update(cash=501, nav=1001), "OPENING_CASH_BRIDGE"),
    (lambda d: d["fills"][0].update(quantity=200), "OPENING_INVENTORY_BRIDGE"),
    (lambda d: d["snapshots"][0].update(snapshot_time="2026-11-05T09:32:00+08:00"), "OPENING_NOT_CLOSE"),
])
def test_bad_anchor_cannot_seed_replay(change, error):
    value = facts()
    change(value)
    with pytest.raises(ValueError, match=error):
        trusted_opening(value, "2026-11-05")


def test_valid_opening_keeps_cost_and_original_capital():
    value = facts()
    before = deepcopy(value)
    opening = trusted_opening(value, "2026-11-05")
    assert opening["origin_initial_cash"] == "1000"
    assert opening["positions"][0]["avg_cost"] == 5
    assert opening["new_capital"] == "0"
    assert value == before


def test_immutable_publication_and_hash_validation(tmp_path):
    path = tmp_path / "source.json"
    pin = publish(path, {"value": 1})
    assert publish(path, {"value": 1}) == pin
    with pytest.raises(ValueError, match="ARTIFACT_CONFLICT"):
        publish(path, {"value": 2})
    path.write_text('{"value": 2}', encoding="utf-8")
    with pytest.raises(ValueError, match="SOURCE_CHANGED"):
        read(pin)


def test_source_inspection_precedes_any_economic_read(tmp_path, monkeypatch):
    request = {"schema_version": "localsim_reconstruction_request_v1",
        "new_capital": "0", "activate_account": False, "production_write": False, "selection_inference": False,
        "implementation_identity": replay.implementation_identity(), "source_refs": [
            {"path": str(tmp_path / "missing-source"), "sha256": "a" * 64, "bytes": 3}]}
    pin = publish(tmp_path / "request.json", request)
    monkeypatch.setattr(replay, "_inspect", lambda *a: pytest.fail("economic facts read before source preflight"))
    with pytest.raises(FileNotFoundError):
        replay.run(pin)


def test_retry_does_not_invoke_economic_engine(tmp_path, monkeypatch):
    root = tmp_path / "replay"
    root.mkdir()
    opening = trusted_opening(facts(), "2026-11-05")
    request = {"output_root": str(root), "request_hash": "r", "account_version_id": "v",
               "opening": publish(tmp_path / "opening.json", opening), "source_account_id": "a"}
    pin = publish(tmp_path / "request.json", request)
    inputs = [{"trade_date": d, "input_hash": d} for d in ["2026-11-06", "2026-11-09"]]
    previous = replay._initial(opening, "v")
    for value in inputs:
        result = {**previous, "request_hash": "r", "input_hash": value["input_hash"],
            "trade_date": value["trade_date"], "account_version_id": "v", "marks": {"000001.SZ": 5},
            "opening_state_hash": canonical_json_sha256(previous), "fills": [], "cash_entries": [],
            "origin_initial_cash": "1000", "valuation_day_pnl": "0", "cumulative_pnl": "0"}
        publish(root / "days" / (value["trade_date"] + ".json"), result)
    monkeypatch.setattr(replay, "_sources", lambda *a: (opening, inputs))
    monkeypatch.setattr(replay, "replay_day", lambda *a: pytest.fail("retry recomputed economics"))
    replay.run(pin)
    before = file_ref(root / "facts.json")
    receipt = replay.exact_retry(pin)
    assert receipt["recomputed_days"] == receipt["new_database_economic_commits"] == 0
    assert receipt["facts"] == before == file_ref(root / "facts.json")
    assert money("0.01") > 0


def test_valuation_only_reuses_book_and_keeps_no_trade_day(tmp_path, monkeypatch):
    data = facts()
    data.update(native=True, calendar=[{"cal_date": "2026-11-05"}, {"cal_date": "2026-11-06"}])
    data["snapshots"][0]["metadata"] = {"terminal": True}
    source = publish(tmp_path / "source.json", data)
    states = replay.valuation_states(data)
    assert states["days"][1]["state_carry"]["fills"] == 0
    states["source_facts"] = source
    state_pin = publish(tmp_path / "states.json", states)
    from backend.services.simulation_execution.localsim.models import LocalSimMarketMarkV1
    days = [{"trade_date": d, "marks": [LocalSimMarketMarkV1(symbol="000001.SZ", price=p,
            as_of_time=datetime.fromisoformat(d + "T15:00:00+08:00"), source="DB_HISTORICAL",
            provenance="HISTORICAL_MINUTE_CLOSE").model_dump(mode="json")]}
            for d, p in [("2026-11-05", 5), ("2026-11-06", 6)]]
    mark_pin = publish(tmp_path / "marks.json", {"days": days})
    monkeypatch.setattr(replay, "LocalSimBackend", lambda **k: pytest.fail("valuation replayed account"))
    kwargs = dict(states_ref=state_pin, marks_ref=mark_pin, source_refs=[source], output_root=tmp_path / "valuation")
    first = replay.revalue_existing(**kwargs)
    result = read(first["facts"])
    assert result["days"][1]["valuation_day_pnl"] == "100.0"
    assert result["days"][1]["cumulative_nav_pnl"] == "100.0"
    assert replay.revalue_existing(**kwargs)["recomputed_days"] == 0
    assert file_ref(source["path"]) == source
