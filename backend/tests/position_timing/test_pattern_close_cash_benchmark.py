"""Small behavioral matrix: capital, causality, limits, population and identity."""
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import ActionValueError
from backend.services.position_timing.pattern_close_cash_benchmark import inspect, publish_json, seal
from backend.services.position_timing.pattern_close_cash_replay import (
    Account, CAPITAL, affordable_quantity, execute, execution_status, fee, replay,
)
from backend.services.position_timing.pattern_close_cash_report import build_report, comparison, index_returns
from backend.services.position_timing.pattern_universe_benchmark import CandidatePoolMemberships, POOL_IDS


def bar(price=100., factor=1., **changes):
    return {"open": price, "high": price, "low": price, "close": price,
            "factor": factor, "up_limit": price*1.1, "down_limit": price*.9,
            "is_suspended": False, "pit_active": True, "volume": 100000., **changes}


@pytest.mark.parametrize("symbol,price,cash,minimum", [
    ("600001.SH", "10", "10000000", 100), ("688001.SH", "600", "100000", 200),
    ("688001.SH", "600", "10000000", 200), ("300001.SZ", "10", "999", 100),
])
def test_affordable_lots_include_fee_floor(symbol, price, cash, minimum):
    p, c = Decimal(price), Decimal(cash)
    q = affordable_quantity(symbol, c, p)
    assert not q or q >= minimum
    if q:
        assert p*q + fee(p*q, "BUY") <= c
    step = 1 if symbol.startswith("688") else 100
    nxt = max(minimum, q + step)
    assert p*nxt + fee(p*nxt, "BUY") > c


@pytest.mark.parametrize("side,overrides,status", [
    ("BUY", {"close": 110.}, "DIRECTIONAL_LIMIT_BLOCKED"),
    ("SELL", {"close": 110.}, "EXECUTABLE"),
    ("SELL", {"close": 90.}, "DIRECTIONAL_LIMIT_BLOCKED"),
    ("BUY", {"close": 90.}, "EXECUTABLE"),
    ("SELL", {"is_suspended": True}, "SUSPENDED"),
    ("BUY", {"up_limit": np.nan}, "LIMIT_DATA_UNKNOWN"),
    ("BUY", {"factor": np.nan}, "MARKET_DATA_UNKNOWN"),
])
def test_directional_execution(side, overrides, status):
    source = bar()
    source.update(overrides)
    assert execution_status(source, side) == status


def test_virtual_basis_invariance_t1_and_natural_reinvestment():
    nav = []
    for scale in (1., 3.):
        account = Account()
        first = execute(account, symbol="600001.SH", bar=bar(factor=scale), ordinal=1,
                        side="BUY", reference=Decimal(100*scale), guarded=False)
        assert first["status"] == "FILLED"
        t1 = execute(account, symbol="600001.SH", bar=bar(factor=scale), ordinal=1,
                     side="SELL", reference=Decimal(100*scale), guarded=False)
        assert t1["status"] == "T1_NOT_SELLABLE"
        sold = execute(account, symbol="600001.SH", bar=bar(200, scale), ordinal=2,
                       side="SELL", reference=Decimal(200*scale), guarded=False)
        assert sold["status"] == "FILLED" and account.cash > CAPITAL
        second = execute(account, symbol="600001.SH", bar=bar(100, scale), ordinal=3,
                         side="BUY", reference=Decimal(100*scale), guarded=False)
        assert second["raw_quantity"] > first["raw_quantity"]
        nav.append(account.wealth(Decimal(100*scale)))
    assert abs(nav[0] - nav[1]) < Decimal("0.000001")


def test_signal_causality_and_terminal_unknown_not_silent_fill():
    n = 70
    prices = 100 + np.sin(np.arange(n)/3) * 5 + np.arange(n) * .1
    data = pd.DataFrame([bar(p, high=p+1, low=p-1) for p in prices], index=pd.bdate_range("2020-01-01", periods=n))
    first, fills, _ = replay("600001.SH", data)
    changed = data.copy()
    changed.loc[changed.index[-5:], ["open", "high", "low", "close"]] *= 1.5
    second, _, _ = replay("600001.SH", changed)
    pd.testing.assert_frame_equal(first.loc[first.ordinal < n-5], second.loc[second.ordinal < n-5])
    assert all(x["execution_ordinal"] == x["decision_ordinal"] + 1 for x in fills)
    missing = data.copy()
    missing.loc[missing.index[-1], ["open", "high", "low", "close"]] = np.nan
    unknown, _, _ = replay("600001.SH", missing)
    assert np.isnan(unknown.hold_nav.iloc[-1])
    missing.loc[missing.index[-1], "is_suspended"] = True
    suspended, _, _ = replay("600001.SH", missing)
    assert suspended.hold_nav.iloc[-1] == suspended.hold_nav.iloc[-2]
    assert suspended.stale_mark.iloc[-1]


def test_index_missing_does_not_fallback_and_uses_percentage_compounding():
    dates = pd.bdate_range("2020-01-01", periods=3)
    index = pd.DataFrame({"ts_code": ["000300.SH"]*3, "trade_date": dates, "close": [100, 110, 99]})
    ret = index_returns(index, dates)
    assert np.isnan(ret["000698.SH"]).all()
    output = comparison(np.array([.1, -.1]), np.array([0., 0.]), ret["000300.SH"][1:])
    assert output["timing"]["total_return"] == pytest.approx(-.01)
    assert output["timing_minus_index"] == pytest.approx(0)


def test_immutable_manifest_retry_and_tamper(tmp_path: Path):
    path = tmp_path / "evidence.json"
    publish_json(path, {"status": "complete"})
    first = seal(tmp_path, ["evidence.json"], request_hash="a"*64)
    assert seal(tmp_path, ["evidence.json"], request_hash="a"*64) == first
    assert inspect(tmp_path, request_hash="a"*64)["status"] == "VERIFIED"
    with pytest.raises(Exception):
        publish_json(path, {"status": "changed"})
    with pytest.raises(ActionValueError):
        inspect(tmp_path, request_hash="b"*64)


def test_pool_membership_is_lagged_and_accounts_not_spliced(tmp_path: Path):
    dates = pd.bdate_range("2020-01-01", periods=4)
    span = pd.DataFrame({"symbol": ["600001.SH", "600002.SH"], "start": [dates[0]]*2, "end": [dates[-1]]*2})
    intervals = {p: span.copy() for p in POOL_IDS}
    intervals["csi300"] = pd.DataFrame({"symbol": ["600001.SH", "600002.SH"],
                                       "start": [dates[0], dates[2]], "end": [dates[1], dates[-1]]})
    pools = CandidatePoolMemberships(tmp_path, {}, "a"*64, intervals, {})
    days = pd.DataFrame({"symbol": ["600001.SH"]*4 + ["600002.SH"]*4,
                         "ordinal": list(range(4))*2,
                         "timing_nav": [100, 110, 121, 133.1, 100, 90, 81, 72.9],
                         "hold_nav": [100]*8, "timing_exposure": [1.]*8, "stale_mark": [False]*8})
    detail = [{"symbol": s, "status": "REPLAYED", "start_ordinal": 0} for s in span.symbol]
    index = pd.DataFrame({"ts_code": ["000300.SH"]*4, "trade_date": dates, "close": [100]*4})
    report, stocks, daily = build_report([(days, detail)], memberships=pools, calendar=dates, index_frame=index)
    actual = daily.loc[(daily.pool == "csi300") & (daily["mode"] == "dynamic"), "timing_return_observed"]
    assert list(actual) == pytest.approx([.1, .1, -.1])
    assert len(stocks) == 2
    result = next(p for p in report["pools"] if p["pool"] == "csi300" and p["mode"] == "dynamic")
    assert result["full_population"]["timing"]["total_return"] == pytest.approx(1.1*1.1*.9-1)


def test_empty_feature_population_remains_visible(tmp_path: Path):
    dates = pd.bdate_range("2020-01-01", periods=3)
    span = pd.DataFrame({"symbol": ["600001.SH"], "start": [dates[0]], "end": [dates[-1]]})
    pools = CandidatePoolMemberships(tmp_path, {}, "a"*64, {p: span for p in POOL_IDS}, {})
    index = pd.DataFrame({"ts_code": ["000300.SH"]*3, "trade_date": dates, "close": [100]*3})
    report, stocks, _ = build_report([(pd.DataFrame(), [{"symbol": "600001.SH", "status": "NO_FEATURE_READY_PIT_SESSION"}])], memberships=pools, calendar=dates, index_frame=index)
    assert len(stocks) == 1 and len(report["symbol_diagnostics"]) == 1
