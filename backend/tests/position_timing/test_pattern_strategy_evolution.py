"""Behavioral contracts for the frozen PT-NEXT-021 strategy set."""
from decimal import Decimal
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.pattern_close_cash_replay import (
    Account,
    execute,
    replay,
)
from backend.services.position_timing.pattern_strategy_evolution import (
    NO_OPEN_GAP_POLICY,
    STRATEGIES,
    STRATEGY_SET_SHA256,
    _trade_profitable,
    _trend_features,
    replay_strategy_set,
)
from backend.services.position_timing.pattern_strategy_evolution_benchmark import (
    _baseline_equivalence,
    _bootstrap_family,
    _merge_partials,
    _partial_frame,
)
from backend.services.position_timing.pattern_close_cash_benchmark import (
    publish_frame,
    publish_json,
)
from backend.services.position_timing.action_value_data import file_reference
from backend.services.position_timing.action_value import ActionValueError


def _bar(price=100.0, factor=1.0, **changes):
    return {
        "open": price,
        "high": price + 1,
        "low": price - 1,
        "close": price,
        "factor": factor,
        "up_limit": price * 1.1,
        "down_limit": price * 0.9,
        "is_suspended": False,
        "pit_active": True,
        "volume": 100_000.0,
        **changes,
    }


def _bars(periods=150):
    ordinal = np.arange(periods)
    price = 100 + ordinal * 0.2 + np.sin(ordinal / 4) * 4
    return pd.DataFrame(
        [_bar(value, volume=100_000 + (index % 9) * 2_000) for index, value in enumerate(price)],
        index=pd.bdate_range("2020-01-01", periods=periods),
    )


def test_strategy_family_is_frozen_and_complete():
    assert [item.strategy_id for item in STRATEGIES] == [
        "A0", "A1", "A2", "A3", "B0", "B1", "B2", "C0", "C1", "C2"
    ]
    assert len(STRATEGY_SET_SHA256) == 64
    assert sum(item.omit_open_gap_veto for item in STRATEGIES) == 8


def test_open_gap_policy_keeps_observed_gap_but_only_removes_that_veto():
    source = _bar(100.0, open=104.0, close=100.0, up_limit=110.0, down_limit=90.0)
    blocked = execute(
        Account(), symbol="600001.SH", bar=source, ordinal=1,
        side="BUY", reference=Decimal("100"), guarded=True,
    )
    accepted = execute(
        Account(), symbol="600001.SH", bar=source, ordinal=1,
        side="BUY", reference=Decimal("100"), guarded=True,
        price_guard_policy=NO_OPEN_GAP_POLICY,
    )
    assert blocked["status"] == "PRICE_GUARD_BLOCKED"
    assert accepted["status"] == "FILLED"


def test_partial_sell_preserves_cost_basis_and_never_uses_residual_exception():
    account = Account()
    bought = execute(
        account, symbol="600001.SH", bar=_bar(10.0), ordinal=1,
        side="BUY", reference=Decimal("10"), guarded=False,
    )
    entry = account.entry
    sold = execute(
        account, symbol="600001.SH", bar=_bar(12.0), ordinal=2,
        side="SELL", reference=Decimal("12"), guarded=False,
        sell_fraction=Decimal("0.5"),
    )
    assert bought["raw_quantity"] % 100 == 0
    assert sold["status"] == "FILLED"
    assert Decimal(str(sold["remaining_virtual_units"])) > 0
    assert account.entry == entry
    assert account.cash > 0

    residual = Account(cash=Decimal(0), units=Decimal(100), entry=Decimal(10), bought_on=0)
    unavailable = execute(
        residual, symbol="600001.SH", bar=_bar(12.0), ordinal=2,
        side="SELL", reference=Decimal("12"), guarded=False,
        sell_fraction=Decimal("0.5"),
    )
    assert unavailable["status"] == "PARTIAL_QUANTITY_UNAVAILABLE"
    assert residual.units == 100


def test_trade_profit_uses_remaining_position_not_account_recovery():
    account = Account(cash=Decimal("100"), units=Decimal("1000"), entry=Decimal("10"), bought_on=1)
    assert _trade_profitable(account, Decimal("11"))
    assert not _trade_profitable(account, Decimal("9"))


def test_trend_windows_do_not_compress_a_missing_global_session():
    index = pd.bdate_range("2020-01-01", periods=80)
    values = np.arange(80, dtype=float) + 100
    pattern = pd.DataFrame(
        {
            "adjusted_close": values,
            "adjusted_high": values + 1,
            "adjusted_low": values - 1,
        },
        index=index,
    )
    pattern.loc[index[30], ["adjusted_close", "adjusted_high", "adjusted_low"]] = np.nan
    trend = _trend_features(pattern)
    assert np.isnan(trend.loc[index[60], "sma60"])
    assert np.isfinite(trend.loc[index[-1], "sma20"])


def test_a0_own_start_reproduces_existing_replay_and_future_changes_are_causal():
    bars = _bars()
    baseline, _, _ = replay("600001.SH", bars)
    days, fills, details = replay_strategy_set("600001.SH", bars)
    a0 = days.loc[(days.strategy_id == "A0") & (days.start_mode == "OWN_START")]
    pd.testing.assert_frame_equal(
        baseline.reset_index(drop=True),
        a0.drop(columns=["symbol", "strategy_id", "start_mode"]).reset_index(drop=True),
    )
    assert len(details) == 20
    assert set(days.strategy_id) == {item.strategy_id for item in STRATEGIES}
    assert set(days.start_mode) == {"OWN_START", "COMMON_START"}
    assert all(item["execution_ordinal"] == item["decision_ordinal"] + 1 for item in fills.to_dict("records"))

    changed = bars.copy()
    changed.loc[changed.index[-5:], ["open", "high", "low", "close"]] *= 1.5
    later, _, _ = replay_strategy_set("600001.SH", changed)
    left = days.loc[days.ordinal < len(bars) - 5].reset_index(drop=True)
    right = later.loc[later.ordinal < len(bars) - 5].reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right)


def test_chunk_partials_merge_exactly_and_family_keeps_all_54_hypotheses():
    first = {
        ("A1", "COMMON_START", "csi300", "dynamic", 0): {
            "timing_sum": np.array([0.0, 0.1, 0.2]),
            "hold_sum": np.array([0.0, 0.0, 0.1]),
            "expected": np.array([0, 1, 1]),
            "paired": np.array([0, 1, 1]),
            "unknown": np.array([0, 0, 0]),
        }
    }
    encoded = _partial_frame(first)
    merged = _merge_partials([encoded, encoded], 3)
    actual = merged[("A1", "COMMON_START", "csi300", "dynamic", 0)]
    assert actual["timing_sum"].tolist() == pytest.approx([0.0, 0.2, 0.4])
    assert actual["paired"].tolist() == [0, 2, 2]

    values = np.linspace(-0.01, 0.01, 80)
    family = {
        (strategy, pool): values
        for strategy in ("A1", "A2", "A3", "B0", "B1", "B2", "C0", "C1", "C2")
        for pool in ("stock_universe", "csi300", "csi500", "csi1000", "star50", "star100")
    }
    output = _bootstrap_family(series=family, sessions=len(values))
    assert len(output) == 54
    assert all(item["family_size"] == 54 for item in output)
    assert all(item["status"] == "ESTIMATED" for item in output)


def test_a0_full_population_equivalence_is_fail_closed(tmp_path: Path):
    fields = {
        "start": "2020-01-01",
        "end": "2020-12-31",
        "paired_unknown_sessions": 0,
        "invested_session_fraction": 0.5,
        "conditional_exposure": 0.9,
        "average_exposure": 0.45,
        "stale_mark_sessions": 0,
        "timing_minus_hold": 0.1,
        "timing_status": "COMPLETE",
        "timing_sessions": 250,
        "timing_total_return": 0.2,
        "timing_annualized_return": 0.201,
        "timing_max_drawdown": -0.1,
        "hold_status": "COMPLETE",
        "hold_sessions": 250,
        "hold_total_return": 0.1,
        "hold_annualized_return": 0.101,
        "hold_max_drawdown": -0.2,
    }
    baseline = pd.DataFrame([{"symbol": "600001.SH", **fields}])
    publish_frame(tmp_path / "stocks.parquet", baseline)
    publish_json(
        tmp_path / "manifest.json",
        {"files": {"stocks.parquet": file_reference(tmp_path / "stocks.parquet")}},
    )
    current = baseline.assign(strategy_id="A0", start_mode="OWN_START")
    audit = _baseline_equivalence(
        stocks=current,
        baseline_manifest_path=tmp_path / "manifest.json",
    )
    assert audit["symbol_count"] == 1
    changed = current.copy()
    changed.loc[0, "timing_total_return"] += 1e-8
    with pytest.raises(ActionValueError, match="STRATEGY_EVOLUTION_A0_BASELINE_MISMATCH"):
        _baseline_equivalence(
            stocks=changed,
            baseline_manifest_path=tmp_path / "manifest.json",
        )
