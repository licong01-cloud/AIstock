from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import numpy as np
import pandas as pd

from backend.services.position_timing.action_value import TZ
from backend.services.position_timing.action_value_corporate_actions import (
    CorporateAction,
    CorporateActionBook,
)
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


def test_continuous_replay_rejects_unbound_material_factor_change_and_cannot_support() -> None:
    candidate = FakeCandidate(820)
    population = build_action_value_rows(
        candidate,
        ActionValuePopulationSpec(
            start=candidate.calendar[35].date(),
            end=candidate.calendar[-25].date(),
            symbol_limit=2,
            review_stride=10,
        ),
    )
    forward = walk_forward_action_values(
        population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=canonical_sha256(population.coverage),
        request_sha256="b" * 64,
        source_commit="a" * 40,
    )
    first_model_day = pd.Timestamp(forward.models[0].metadata["available_at"]).date()
    first = candidate.calendar.get_loc(str(first_model_day))
    candidate._frames["000001.SZ"].iloc[
        first + 5 :, candidate._frames["000001.SZ"].columns.get_loc("factor")
    ] = 1.1
    replay = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=candidate.symbols,
        bootstrap_samples=40,
    )
    assert replay.receipt["excluded"]["unbound_material_factor_change_sleeves"] == 2
    assert replay.receipt["coverage_can_support_policy"] is False
    assert replay.receipt["study_effect_evidence"] == "INCONCLUSIVE"


def test_continuous_replay_carries_suspension_without_dropping_whole_sleeve() -> None:
    candidate = FakeCandidate(820)
    population = build_action_value_rows(
        candidate,
        ActionValuePopulationSpec(
            start=candidate.calendar[35].date(),
            end=candidate.calendar[-25].date(),
            symbol_limit=2,
            review_stride=10,
        ),
    )
    forward = walk_forward_action_values(
        population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=canonical_sha256(population.coverage),
        request_sha256="b" * 64,
        source_commit="a" * 40,
    )
    first_model_day = pd.Timestamp(forward.models[0].metadata["available_at"]).date()
    first = candidate.calendar.get_loc(str(first_model_day))
    frame = candidate._frames["000001.SZ"]
    suspension = frame.index[first + 5 : first + 8]
    frame.loc[suspension, ["open", "high", "low", "close", "volume", "factor"]] = np.nan
    frame.loc[suspension, "is_suspended"] = True

    replay = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=candidate.symbols,
        bootstrap_samples=40,
    )

    assert replay.receipt["excluded"]["source_factor_invalid_sleeves"] == 0
    assert replay.receipt["excluded"]["path_unknown"] == 0
    assert replay.receipt["excluded"]["decision_input_unavailable_sleeve_days"] > 0
    assert replay.receipt["sleeve_count"] == len(candidate.symbols) * 2
    unavailable = replay.sleeve_days.loc[
        replay.sleeve_days["decision_input_status"].eq("UNAVAILABLE")
    ]
    assert not unavailable.empty
    assert unavailable["planned_delta_qty"].eq(0).all()


def test_continuous_replay_applies_target_action_after_unavailable_decision() -> None:
    candidate = FakeCandidate(820)
    population = build_action_value_rows(
        candidate,
        ActionValuePopulationSpec(
            start=candidate.calendar[35].date(),
            end=candidate.calendar[-25].date(),
            symbol_limit=2,
            review_stride=10,
        ),
    )
    forward = walk_forward_action_values(
        population.rows,
        calendar=[day.date() for day in candidate.calendar],
        source_sha256=canonical_sha256(population.coverage),
        request_sha256="b" * 64,
        source_commit="a" * 40,
    )
    first_model_day = pd.Timestamp(forward.models[0].metadata["available_at"]).date()
    decision_ordinal = candidate.calendar.get_loc(str(first_model_day)) + 5
    frame = candidate._frames["000001.SZ"]
    decision_day = frame.index[decision_ordinal]
    target_day = frame.index[decision_ordinal + 1]
    listing_day = frame.index[decision_ordinal + 2]
    frame.loc[decision_day, ["open", "high", "low", "close", "volume", "factor"]] = np.nan
    frame.loc[decision_day, "is_suspended"] = True
    frame.loc[target_day:, "factor"] = 1.2
    action = CorporateAction(
        symbol="000001.SZ",
        effective_trade_date=target_day.date(),
        quantity_multiplier=Decimal("1.2"),
        cashflow_yuan_per_share=Decimal("0"),
        reference_price_cash_yuan_per_share=Decimal("0"),
        cash_pay_date=None,
        share_listing_date=listing_day.date(),
        source_available_at=datetime.combine(
            decision_day.date(),
            datetime.min.time(),
            tzinfo=TZ,
        ),
        source_row_count=1,
        source_rows_sha256="c" * 64,
    )
    actions = CorporateActionBook(
        (action,),
        canonical_sha256({"test": "suspension-before-action"}),
    )

    replay = replay_continuous_cohorts(
        candidate,
        models=forward.models,
        symbols=candidate.symbols,
        corporate_actions=actions,
        bootstrap_samples=40,
    )

    assert replay.receipt["excluded"]["path_unknown"] == 0
    affected = replay.sleeve_days.loc[
        replay.sleeve_days["corporate_action_applied"]
        & replay.sleeve_days["symbol"].eq("000001.SZ")
    ]
    assert not affected.empty
    assert affected["decision_input_status"].eq("UNAVAILABLE").all()
    assert affected["planned_delta_qty"].eq(0).all()
