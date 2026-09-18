"""Economic and deterministic contracts for PT-NEXT-022."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing import fundamental_timing as timing
from backend.services.position_timing.fundamental_timing import (
    POLICY_CONTRACT_SHA256,
    T1,
    T2,
    replay_fundamental_policies,
)
from backend.services.position_timing.fundamental_timing_benchmark import (
    FAMILY_SIZE,
    _bootstrap,
    _merge_partials,
    _ordered_replays,
    _partial_frame,
    _replay_symbol_task,
)


def _bar(price: float, **changes):
    return {
        "open": price,
        "high": price + 1,
        "low": price - 1,
        "close": price,
        "factor": 1.0,
        "up_limit": price * 1.1,
        "down_limit": price * 0.9,
        "is_suspended": False,
        "pit_active": True,
        "volume": 100_000.0,
        **changes,
    }


def _bars(periods: int = 150) -> pd.DataFrame:
    price = 30 + np.arange(periods, dtype=float) * .15
    return pd.DataFrame(
        [_bar(value, volume=100_000 + (ordinal % 7) * 1_000) for ordinal, value in enumerate(price)],
        index=pd.bdate_range("2020-01-01", periods=periods),
    )


def test_both_policies_share_enrollment_anchor_and_hold_comparator():
    days, fills, details = replay_fundamental_policies("600001.SH", _bars(), enrollment_ordinal=70)
    assert set(days.policy_id) == {T1, T2}
    assert {detail["enrollment_ordinal"] for detail in details} == {70}
    holds = {
        policy: days.loc[days.policy_id == policy, ["ordinal", "hold_nav"]].reset_index(drop=True)
        for policy in (T1, T2)
    }
    pd.testing.assert_frame_equal(holds[T1], holds[T2])
    assert all(event.execution_ordinal == event.decision_ordinal + 1 for event in fills.itertuples())
    assert len(POLICY_CONTRACT_SHA256) == 64


def test_t2_trims_once_on_edge_then_recovers_from_its_own_cash(monkeypatch):
    def acceleration(_frame, ordinal, _regime):
        return ordinal == 90, {"ordinal": ordinal}

    monkeypatch.setattr(timing, "acceleration_volume_exit", acceleration)
    _, fills, details = replay_fundamental_policies("600001.SH", _bars(), enrollment_ordinal=70)
    t2 = fills.loc[(fills.policy_id == T2) & (fills.role == "timing")]
    trims = t2.loc[(t2.authority == "R0_ACCELERATION_TACTICAL_TRIM") & (t2.status == "FILLED")]
    recoveries = t2.loc[(t2.authority == "TACTICAL_RECOVERY") & (t2.status == "FILLED")]
    assert len(trims) == 1
    assert len(recoveries) == 1
    assert trims.iloc[0].remaining_virtual_units > 0
    assert recoveries.iloc[0].execution_ordinal > trims.iloc[0].execution_ordinal
    detail = next(item for item in details if item["policy_id"] == T2)
    assert detail["terminal"]["timing"]["fees_cny"] > 0


def test_partial_totals_round_trip_and_six_hypothesis_family_is_fixed():
    values = {
        (T1, "stock_universe"): {
            "timing_sum": np.array([0.0, .01]),
            "hold_sum": np.array([0.0, .02]),
            "expected": np.array([0, 1]),
            "paired": np.array([0, 1]),
            "unknown": np.array([0, 0]),
        }
    }
    encoded = _partial_frame(values)
    actual = _merge_partials([encoded, encoded], 2)[(T1, "stock_universe")]
    assert actual["paired"].tolist() == [0, 2]
    assert actual["timing_sum"].tolist() == pytest.approx([0.0, .02])
    assert FAMILY_SIZE == 6
    result = _bootstrap(np.linspace(-.001, .001, 80))
    assert result["family_size"] == 6
    assert result["economic_threshold_bps"] == 0.0
    assert result["evidence_state"] in {"SUPPORTED", "NEGATIVE", "INCONCLUSIVE"}


def test_single_and_spawn_workers_are_result_identical():
    bars = _bars()
    market_cap = pd.Series(600_000.0, index=bars.index)
    inputs = [("600001.SH", bars, market_cap), ("600002.SH", bars, market_cap)]

    def normalized(results):
        return [
            (
                symbol,
                days.to_json(orient="table", index=False, double_precision=15),
                fills.to_json(orient="table", index=False, double_precision=15),
                details,
                audit,
            )
            for symbol, days, fills, details, audit in results
        ]

    sequential = normalized(_ordered_replays(inputs, worker_count=1, max_in_flight=1))
    parallel = normalized(_ordered_replays(inputs, worker_count=2, max_in_flight=2))
    assert sequential == parallel


def test_unknown_market_cap_before_first_possible_pass_fails_enrollment_closed():
    bars = _bars()
    market_cap = pd.Series(600_000.0, index=bars.index)
    market_cap.iloc[:100] = np.nan
    _, days, fills, details, audit = _replay_symbol_task("600001.SH", bars, market_cap)
    assert audit["status"] == "ENROLLMENT_UNKNOWN"
    assert audit["enrollment_ordinal"] is None
    assert days.empty and fills.empty
    assert {value["status"] for value in details} == {"ENROLLMENT_UNKNOWN"}
