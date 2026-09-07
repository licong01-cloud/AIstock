from __future__ import annotations

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value_research import (
    ActionValuePopulationSpec,
    build_action_value_rows,
    replay_continuous_cohorts,
    deterministic_symbols,
    walk_forward_action_values,
)
from backend.services.position_timing.contracts import canonical_sha256


class FakeCandidate:
    def __init__(self, sessions: int = 820) -> None:
        self.calendar = pd.bdate_range("2022-01-03", periods=sessions)
        self.symbols = ("000001.SZ", "600000.SH")
        self._frames = {
            symbol: self._frame(index, symbol)
            for index, symbol in enumerate((*self.symbols, "000300.SH"))
        }

    def _frame(self, offset: int, symbol: str) -> pd.DataFrame:
        wave = np.sin(np.arange(len(self.calendar)) / (17 + offset)) * 0.01
        trend = 10 + offset + np.arange(len(self.calendar)) * (0.003 + offset * 0.0002)
        close = trend * (1 + wave)
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = close * 0.998
        frame["high"] = close * 1.012
        frame["low"] = close * 0.988
        frame["close"] = close
        frame["volume"] = 1_000_000 + np.arange(len(frame)) * 100
        frame["factor"] = 1.0
        frame["up_limit"] = close * 1.1
        frame["down_limit"] = close * 0.9
        frame["is_suspended"] = False
        frame["pit_active"] = symbol != "000300.SH"
        frame["available_at"] = [day.to_pydatetime().astimezone().isoformat() for day in self.calendar]
        return frame

    def bars(self, symbol: str) -> pd.DataFrame:
        return self._frames[symbol].copy()


def test_source_only_sampling_is_stable_and_outcome_independent() -> None:
    symbols = ["600000.SH", "000001.SZ", "300001.SZ"]
    assert deterministic_symbols(symbols, limit=2, seed=7) == deterministic_symbols(
        reversed(symbols), limit=2, seed=7
    )


def test_population_builds_both_heads_with_shared_features_and_costs() -> None:
    candidate = FakeCandidate(180)
    spec = ActionValuePopulationSpec(
        start=candidate.calendar[35].date(),
        end=candidate.calendar[-25].date(),
        symbol_limit=2,
        review_stride=20,
    )
    result = build_action_value_rows(candidate, spec)
    assert set(result.rows.objective) == {"ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2"}
    assert set(result.rows["objective"]) == {"ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2"}
    assert result.coverage["counts"]["rows"] == len(result.rows)
    assert result.coverage["coverage_sha256"] == canonical_sha256(
        {key: value for key, value in result.coverage.items() if key != "coverage_sha256"}
    )
    assert result.rows["estimated_leg_cost_bps"].gt(0).all()
    assert result.rows["label_available_at"].gt(result.rows["decision_as_of"]).all()


def test_walk_forward_never_scores_before_model_is_available() -> None:
    candidate = FakeCandidate(820)
    spec = ActionValuePopulationSpec(
        start=candidate.calendar[35].date(),
        end=candidate.calendar[-25].date(),
        symbol_limit=2,
        review_stride=10,
    )
    population = build_action_value_rows(candidate, spec)
    identity = canonical_sha256(population.coverage)
    result = walk_forward_action_values(
        population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=identity,
        request_sha256="b" * 64,
        source_commit="a" * 40,
    )
    assert result.models
    assert result.diagnostics["prediction_rows"] == len(result.predictions)
    assert result.diagnostics["warning"].endswith("NOT_CONTINUOUS_POLICY_RETURN")
    for model in result.models:
        assert model.metadata["temporal_mode"] == "HISTORICAL_REPLAY"

    replay = replay_continuous_cohorts(
        candidate,
        models=result.models,
        symbols=candidate.symbols,
        bootstrap_samples=200,
    )
    assert replay.receipt["sleeve_count"] > 0
    assert replay.receipt["capital_injection_policy"] == "ONCE_PER_SYMBOL_INITIAL_STATE_FOR_FULL_OOT_PATH"
    assert replay.receipt["monthly_retrain_resets_position"] is False
    assert replay.sleeve_days["sleeve_id"].nunique() <= len(candidate.symbols) * 2
    assert set(replay.receipt["comparisons"]) == {"BUY_AND_HOLD", "FROZEN_L1_V1"}
    assert set(replay.daily_comparisons["baseline"]) == {"BUY_AND_HOLD", "FROZEN_L1_V1"}
    assert replay.receipt["receipt_sha256"] == canonical_sha256(
        {key: value for key, value in replay.receipt.items() if key != "receipt_sha256"}
    )


def test_factor_change_is_reported_not_approximated() -> None:
    candidate = FakeCandidate(180)
    candidate._frames["000001.SZ"].iloc[80:, candidate._frames["000001.SZ"].columns.get_loc("factor")] = 1.1
    spec = ActionValuePopulationSpec(
        start=candidate.calendar[35].date(),
        end=candidate.calendar[-25].date(),
        symbol_limit=2,
        review_stride=5,
    )
    result = build_action_value_rows(candidate, spec)
    assert result.coverage["counts"]["corporate_action_unavailable"] > 0
