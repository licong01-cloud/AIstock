from __future__ import annotations

from datetime import date
from decimal import Decimal
import json

import numpy as np
import pandas as pd

from backend.services.position_timing.causal_timing_contracts import BH, E0, FEATURE_ORDER
from backend.services.position_timing.causal_timing_execution import (
    MINUTE_FIELDS, ExecutionQuote, MinuteExecutionSource, execute_quote,
)
from backend.services.position_timing.causal_timing_oracle import (
    _standardized_label, labels_from_legacy_fills, restricted_account_oracle,
)
from backend.services.position_timing.causal_timing_replay import replay_policy
from backend.services.position_timing.pattern_close_cash_replay import Account, CAPITAL


def quote(*, price: str = "10", factor: str = "1", limit_up: bool = False) -> ExecutionQuote:
    return ExecutionQuote(
        E0, date(2024, 7, 1), Decimal(price), Decimal(factor), None,
        Decimal("11"), Decimal("9"), limit_up, False, False, "DAILY_CLOSE_PROXY",
    )


def test_execution_obeys_cash_lot_t1_and_direction_limit() -> None:
    account = Account()
    bought = execute_quote(account, symbol="000001.SZ", quote=quote(), ordinal=1, side="BUY")
    assert bought["status"] == "FILLED"
    assert account.cash >= 0
    assert bought["raw_quantity"] % 100 == 0
    assert execute_quote(account, symbol="000001.SZ", quote=quote(), ordinal=1, side="SELL")["status"] == "T1_NOT_SELLABLE"
    blocked = Account()
    assert execute_quote(blocked, symbol="000001.SZ", quote=quote(price="11", limit_up=True), ordinal=1, side="BUY")["status"] == "DIRECTIONAL_LIMIT_BLOCKED"


def test_star_minimum_lot_and_factor_virtual_units() -> None:
    account = Account(cash=Decimal("2500"))
    fill = execute_quote(account, symbol="688001.SH", quote=quote(price="10", factor="2"), ordinal=1, side="BUY")
    assert fill["status"] == "FILLED"
    assert fill["raw_quantity"] >= 200
    assert Decimal(str(fill["virtual_units"])) == Decimal(fill["raw_quantity"]) / 2


def test_buy_hold_replay_uses_same_view_and_terminal_liquidation() -> None:
    dates = pd.date_range("2024-06-28", periods=4, freq="B")
    bars = pd.DataFrame({
        "open": [10.0] * 4, "high": [10.0] * 4, "low": [10.0] * 4,
        "close": [10.0] * 4, "volume": [1_000_000.0] * 4, "factor": [1.0] * 4,
        "up_limit": [11.0] * 4, "down_limit": [9.0] * 4,
        "is_suspended": [False] * 4, "pit_active": [True] * 4,
    }, index=dates)
    adjusted = bars.copy()
    pattern = pd.DataFrame(index=dates)
    trend = pd.DataFrame(index=dates)
    features = pd.DataFrame(0.0, index=dates, columns=FEATURE_ORDER)
    days, fills, detail = replay_policy(
        symbol="000001.SZ", bars=bars, adjusted=adjusted, pattern=pattern, trend=trend,
        ready=np.zeros(4, dtype=bool), model_features=features, policy_id=BH,
        execution_view=E0, enrollment_ordinal=0, terminal_ordinal=3,
        minute_source=None, models={},
    )
    assert fills.authority.tolist() == ["COMMON_INITIAL_ENTRY", "TERMINAL_LIQUIDATION"]
    assert detail["terminal_status"] == "CASH"
    assert detail["terminal_cash_cny"] < float(CAPITAL)
    assert not days.nav.isna().any()


def test_minute_reader_uses_calendar_seconds_and_restores_raw_basis(tmp_path) -> None:
    (tmp_path / "calendars").mkdir()
    (tmp_path / "calendars" / "1min.txt").write_text("2024-07-01 10:00:00\n", encoding="utf-8")
    (tmp_path / "meta_export.json").write_text(json.dumps({
        "start": "2024-07-01", "end": "2024-07-01", "required_minute_fields": list(MINUTE_FIELDS),
    }), encoding="utf-8")
    folder = tmp_path / "features" / "000001.sz"
    folder.mkdir(parents=True)
    values = {
        "open": 20.0, "close": 20.0, "volume": 500.0, "factor": 2.0,
        "up_limit_price": 11.0, "down_limit_price": 9.0, "limit_up": 0.0, "limit_down": 0.0,
    }
    for field, value in values.items():
        np.asarray([0.0, value], dtype="<f4").tofile(folder / f"{field}.1min.bin")
    source = MinuteExecutionSource(tmp_path)
    assert source.identity() == json.loads(json.dumps(source.identity()))
    observed = source.quote(
        symbol="000001.SZ", trade_date=date(2024, 7, 1), view_id="SCHEDULED_1000_PROXY",
    )
    assert observed.minute_label == "2024-07-01 10:00:00"
    assert observed.raw_price == Decimal("10.0")
    assert observed.raw_volume == Decimal("1000.0")


def test_mature_label_uses_frozen_independent_cash_account() -> None:
    dates = pd.date_range("2022-01-03", periods=30, freq="B")
    bars = pd.DataFrame({
        "open": [10.0] * 30, "high": [10.0] * 30, "low": [10.0] * 30,
        "close": [10.0] * 30, "volume": [1_000_000.0] * 30, "factor": [1.0] * 30,
        "up_limit": [11.0] * 30, "down_limit": [9.0] * 30,
        "is_suspended": [False] * 30, "pit_active": [True] * 30,
    }, index=dates)
    result = _standardized_label(
        symbol="000001.SZ", bars=bars, adjusted=bars,
        decision_ordinal=1, sell_execution_ordinal=2,
    )
    assert result["status"] == "MATURED"
    assert result["label_available_at"] == str(dates[22].date())
    assert result["standardized_initial_quantity"] % 100 == 0
    assert result["standardized_initial_buy_fee"] > 0
    assert result["standardized_initial_residual_cash"] >= 0
    assert (
        result["standardized_initial_residual_cash"]
        + result["standardized_initial_quantity"] * 10
        + result["standardized_initial_buy_fee"]
    ) == float(CAPITAL)


def test_restricted_oracle_keeps_exact_grid_and_initial_cash_anchor() -> None:
    dates = pd.date_range("2024-07-01", periods=50, freq="B")
    bars = pd.DataFrame({
        "open": [10.0] * 50, "high": [10.0] * 50, "low": [10.0] * 50,
        "close": [10.0] * 50, "volume": [1_000_000.0] * 50, "factor": [1.0] * 50,
        "up_limit": [11.0] * 50, "down_limit": [9.0] * 50,
        "is_suspended": [False] * 50, "pit_active": [True] * 50,
    }, index=dates)
    result = restricted_account_oracle(
        symbol="000001.SZ", bars=bars, adjusted=bars,
        enrollment_ordinal=0, terminal_ordinal=49,
    )
    assert result["status"] == "COMPLETE"
    assert result["grid_ordinals"] == [21, 41]
    assert result["path_count"] == 4
    assert result["best_terminal"]["path_id"] == "BH"
    assert result["bh"]["max_drawdown"] < 0


def test_failed_reference_sell_is_preserved_as_zero_action_label() -> None:
    dates = pd.date_range("2022-01-03", periods=30, freq="B")
    bars = pd.DataFrame({
        "open": [10.0] * 30, "high": [10.0] * 30, "low": [10.0] * 30,
        "close": [10.0] * 30, "volume": [1_000_000.0] * 30, "factor": [1.0] * 30,
        "up_limit": [11.0] * 30, "down_limit": [9.0] * 30,
        "is_suspended": [False] * 30, "pit_active": [True] * 30,
    }, index=dates)
    bars.loc[dates[2], "down_limit"] = 10.0
    fills = pd.DataFrame([{
        "role": "timing", "side": "SELL", "status": "DIRECTIONAL_LIMIT_BLOCKED",
        "authority": "R0_TACTICAL_20_TRIM", "policy_id": "S1",
        "decision_ordinal": 1, "execution_ordinal": 2,
    }])
    features = pd.DataFrame(0.0, index=dates, columns=FEATURE_ORDER)
    labels = labels_from_legacy_fills(
        symbol="000001.SZ", bars=bars, adjusted=bars,
        market_features=features, fills=fills,
    )
    assert len(labels) == 1
    assert labels.iloc[0].reference_fill_status == "DIRECTIONAL_LIMIT_BLOCKED"
    assert labels.iloc[0].sell_status == "DIRECTIONAL_LIMIT_BLOCKED"
    assert labels.iloc[0].label_net_action_value_bps == 0.0
