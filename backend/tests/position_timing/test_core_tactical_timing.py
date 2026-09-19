"""Account invariants for PT-NEXT-023 core/tactical policies."""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing import core_tactical_timing as timing
from backend.services.position_timing import core_tactical_benchmark as benchmark
from backend.services.position_timing.core_tactical_timing import S1, S2, replay_core_tactical_policies
from backend.services.position_timing.r8_proxy_screen import BAK_COLUMNS, SCREEN_IDS


def _bar(price: float, **changes):
    return {
        "open": price,
        "high": price + .1,
        "low": price - .1,
        "close": price,
        "factor": 1.0,
        "up_limit": price * 1.1,
        "down_limit": price * .9,
        "is_suspended": False,
        "pit_active": True,
        "volume": 100_000.0,
        **changes,
    }


def _bars(periods: int = 150) -> pd.DataFrame:
    prices = 30 + np.arange(periods, dtype=float) * .5
    return pd.DataFrame(
        [_bar(value, volume=100_000 + (ordinal % 7) * 1_000) for ordinal, value in enumerate(prices)],
        index=pd.bdate_range("2020-01-01", periods=periods),
    )


def test_common_initial_entry_matches_hold_and_s1_recovers_its_own_cash(monkeypatch):
    def acceleration(_frame, ordinal, template):
        matched = template == "R0" and ordinal == 90
        return matched, {"available": True}

    monkeypatch.setattr(timing, "acceleration_volume_exit", acceleration)
    days, fills, details = replay_core_tactical_policies("600001.SH", _bars(), enrollment_ordinal=70)
    first = fills.loc[fills.authority.isin(["COMMON_INITIAL_ENTRY", "BUY_AND_HOLD"]) & (fills.status == "FILLED")]
    for policy, group in first.groupby("policy_id"):
        timing_buy = group.loc[group.role == "timing"].iloc[0]
        hold_buy = group.loc[group.role == "hold"].iloc[0]
        assert timing_buy.notional == pytest.approx(hold_buy.notional)
        assert timing_buy.fee == pytest.approx(hold_buy.fee)
    s1 = fills.loc[(fills.policy_id == S1) & (fills.role == "timing")]
    trim = s1.loc[(s1.authority == "R0_TACTICAL_20_TRIM") & (s1.status == "FILLED")]
    recovery = s1.loc[(s1.authority == "TACTICAL_RECOVERY") & (s1.status == "FILLED")]
    assert len(trim) == 1 and len(recovery) == 1
    assert trim.iloc[0].remaining_virtual_units >= trim.iloc[0].core_floor_units
    assert recovery.iloc[0].execution_ordinal > trim.iloc[0].execution_ordinal
    detail = next(item for item in details if item["policy_id"] == S1)
    assert detail["floor_violation_count"] == 0
    assert days.loc[days.policy_id == S1, "timing_exposure"].max() <= 1.000001


def test_s2_executes_two_distinct_fifteen_percent_stages_without_crossing_floor(monkeypatch):
    def acceleration(_frame, ordinal, template):
        matched = (template == "R0" and ordinal in {90, 91, 92}) or (template == "R6" and ordinal >= 92)
        return matched, {"available": True}

    monkeypatch.setattr(timing, "acceleration_volume_exit", acceleration)
    _, fills, details = replay_core_tactical_policies("600001.SH", _bars(), enrollment_ordinal=70)
    s2 = fills.loc[(fills.policy_id == S2) & (fills.role == "timing") & (fills.status == "FILLED")]
    stage1 = s2.loc[s2.authority == "R0_STAGE1_15_TRIM"]
    stage2 = s2.loc[s2.authority == "R6_STAGE2_15_TRIM"]
    assert len(stage1) == 1 and len(stage2) == 1
    assert stage2.iloc[0].execution_ordinal > stage1.iloc[0].execution_ordinal
    assert stage2.iloc[0].remaining_virtual_units >= stage2.iloc[0].core_floor_units
    assert stage1.iloc[0].virtual_units / stage1.iloc[0].full_units_anchor == pytest.approx(.15, abs=.001)
    assert stage2.iloc[0].virtual_units / stage2.iloc[0].full_units_anchor == pytest.approx(.15, abs=.001)
    detail = next(item for item in details if item["policy_id"] == S2)
    assert detail["floor_violation_count"] == 0


def test_risk_signal_reduces_only_to_frozen_core_floor(monkeypatch):
    monkeypatch.setattr(
        timing,
        "evaluate_exit",
        lambda context, _policy: SimpleNamespace(should_exit=context.days_since_entry >= 20),
    )
    monkeypatch.setattr(
        timing,
        "acceleration_volume_exit",
        lambda _frame, _ordinal, _template: (False, {"available": True}),
    )
    _, fills, details = replay_core_tactical_policies("600001.SH", _bars(), enrollment_ordinal=70)
    assert not fills.status.eq("PARTIAL_QUANTITY_UNAVAILABLE").any()
    for policy, floor in ((S1, .80), (S2, .70)):
        event = fills.loc[
            (fills.policy_id == policy)
            & (fills.authority == "FROZEN_RISK_TACTICAL_REDUCTION")
            & (fills.status == "FILLED")
        ]
        assert len(event) == 1
        row = event.iloc[0]
        assert row.remaining_virtual_units >= row.core_floor_units
        assert row.remaining_virtual_units / row.full_units_anchor == pytest.approx(floor, abs=.001)
        detail = next(item for item in details if item["policy_id"] == policy)
        assert detail["terminal"]["timing"]["status"] == "HELD_AT_END"


def test_blocked_acceleration_does_not_consume_edge(monkeypatch):
    bars = _bars()
    trigger_target = 91
    bars.iloc[trigger_target, bars.columns.get_loc("close")] = bars.iloc[trigger_target].down_limit

    monkeypatch.setattr(
        timing,
        "acceleration_volume_exit",
        lambda _frame, ordinal, template: (template == "R0" and ordinal in {90, 91}, {"available": True}),
    )
    _, fills, _ = replay_core_tactical_policies("600001.SH", bars, enrollment_ordinal=70)
    s1 = fills.loc[(fills.policy_id == S1) & (fills.authority == "R0_TACTICAL_20_TRIM")]
    assert s1.iloc[0].status == "DIRECTIONAL_LIMIT_BLOCKED"
    assert (s1.status == "FILLED").sum() == 1


def test_offline_side_effect_contract_is_false():
    assert timing.POLICY_CONTRACT["ordinary_full_exit"] is False
    assert timing.POLICY_CONTRACT["account"] == "INDEPENDENT_10M_CASH_NATURAL_REINVESTMENT"
    assert len(timing.POLICY_CONTRACT_SHA256) == 64


def test_prepare_freezes_contract_before_outcomes():
    assert benchmark.FAMILY_SIZE == 6
    assert len(benchmark.CONTRACT["hypothesis_family"]) == 6
    assert benchmark.CONTRACT["selected_trial_count"] == 0
    assert benchmark.CONTRACT["strict_financial_pit_claimed"] is False
    for key in (
        "database_read", "database_write", "network_accessed",
        "runtime_action_performed", "service_process_control_performed",
    ):
        assert benchmark.CONTRACT[key] is False


def test_symbol_replay_covers_three_screens_and_is_spawn_deterministic(monkeypatch):
    monkeypatch.setattr(
        timing,
        "acceleration_volume_exit",
        lambda _frame, _ordinal, _template: (False, {"available": True}),
    )
    bars = _bars()
    daily = pd.DataFrame({
        "db_total_mv": 600_000.0,
        "db_turnover_rate_f": 1.0,
        "db_pe_ttm": 20.0,
        "db_pb": 2.0,
    }, index=bars.index)
    bak = pd.DataFrame({column: 1.0 for column in BAK_COLUMNS}, index=bars.index)
    result = benchmark._replay_symbol_task("600001.SH", bars, daily, bak)
    assert result[0] == "600001.SH"
    assert set(result[1].screen_id) == set(SCREEN_IDS)
    assert len(result[3]) == 6
    assert {item["status"] for item in result[4]} == {"ENROLLED"}
    execution = benchmark._execution_attribution(result[2])
    assert len(execution) == 6
    assert all(item["timing_turnover_cny"] > 0 for item in execution)
    assert all(item["hold_turnover_cny"] > 0 for item in execution)

    # The worker process cannot inherit monkeypatch state, so use the real pure
    # signal function for the cross-process identity check.
    monkeypatch.undo()
    inputs = [("600001.SH", bars, daily, bak), ("600002.SH", bars, daily, bak)]

    def normalized(values):
        return [
            (
                item[0],
                item[1].to_json(orient="table", index=False, double_precision=15),
                item[2].to_json(orient="table", index=False, double_precision=15),
                item[3],
                item[4],
            )
            for item in values
        ]

    sequential = normalized(benchmark._ordered_replays(inputs, worker_count=1, max_in_flight=1))
    parallel = normalized(benchmark._ordered_replays(inputs, worker_count=2, max_in_flight=2))
    assert sequential == parallel


def test_partial_totals_round_trip_keep_screen_identity():
    values = {
        (SCREEN_IDS[0], S1, "stock_universe"): {
            "timing_sum": np.array([0.0, .01]),
            "hold_sum": np.array([0.0, .02]),
            "expected": np.array([0, 1]),
            "paired": np.array([0, 1]),
            "unknown": np.array([0, 0]),
        }
    }
    encoded = benchmark._partial_frame(values)
    actual = benchmark._merge_partials([encoded, encoded], 2)[(SCREEN_IDS[0], S1, "stock_universe")]
    assert actual["paired"].tolist() == [0, 2]
    assert actual["timing_sum"].tolist() == pytest.approx([0.0, .02])
