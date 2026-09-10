from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from backend.services.position_timing.action_value import (
    CORE_INFORMATION_BLOCK,
    FEATURE_ORDER,
    ActionValueError,
    cutoff_on,
)
from backend.services.position_timing.action_value_deferred_entry import (
    ALWAYS_OPEN_COMPARATOR,
    LABEL_CONTRACT,
    STUDY_CONTRACT,
)
from backend.services.position_timing.action_value_entry_timing_model import (
    ENTRY_TIMING_ACTION_AUTHORITY,
    ENTRY_TIMING_MODEL_SCHEMA,
    ENTRY_TIMING_OBJECTIVE,
    ENTRY_TIMING_POLICY_SHA256,
    entry_timing_training_rows_asof,
    fit_entry_timing_model,
)
from backend.services.position_timing.action_value_model import HEADS, MODEL_SCHEMA
from backend.services.position_timing.action_value_research import (
    ActionValuePopulationSpec,
    build_deferred_entry_timing_rows,
    replay_continuous_cohorts,
)
from backend.services.position_timing.contracts import canonical_sha256


class FakeCandidate:
    def __init__(self, sessions: int = 820) -> None:
        self.calendar = pd.bdate_range("2022-01-03", periods=sessions)
        self.symbols = ("000001.SZ", "600000.SH")
        self._frames = {
            symbol: self._frame(offset, symbol)
            for offset, symbol in enumerate((*self.symbols, "000300.SH"))
        }

    def _frame(self, offset: int, symbol: str) -> pd.DataFrame:
        steps = np.arange(len(self.calendar))
        wave = np.sin(steps / (13 + offset)) * 0.015
        close = (10 + offset + steps * (0.002 + offset * 0.0001)) * (1 + wave)
        if symbol == "000300.SH":
            close = close * 300
        frame = pd.DataFrame(index=self.calendar)
        frame["open"] = close * (0.996 + np.cos(steps / 9) * 0.002)
        frame["high"] = np.maximum(frame["open"], close) * 1.01
        frame["low"] = np.minimum(frame["open"], close) * 0.99
        frame["close"] = close
        frame["volume"] = 1_000_000 + steps * 100
        frame["factor"] = 1.0
        frame["up_limit"] = close * 1.1
        frame["down_limit"] = close * 0.9
        frame["is_suspended"] = False
        frame["pit_active"] = symbol != "000300.SH"
        return frame

    def bars(self, symbol: str) -> pd.DataFrame:
        return self._frames[symbol].copy()


def _training_fixture(rows: int = 500) -> pd.DataFrame:
    rng = np.random.default_rng(47)
    frame = pd.DataFrame(
        rng.normal(size=(rows, len(FEATURE_ORDER))), columns=FEATURE_ORDER
    )
    frame["holding_exposure"] = 0.0
    frame["cash_fraction"] = 1.0
    frame["action_fraction"] = 0.25
    frame["holding_age"] = np.nan
    frame["holding_age_missing"] = 1.0
    frame["unrealized_return_bps"] = np.nan
    frame["entry_cost_missing"] = 1.0
    frame["objective"] = ENTRY_TIMING_OBJECTIVE
    frame["planned_delta_qty"] = 100
    frame["immediate_fill_status"] = "FILLED"
    frame["deferred_fill_status"] = "FILLED"
    frame["net_entry_timing_value_bps"] = frame["return_1d_bps"] * 0.4
    frame["decision_as_of"] = cutoff_on(date(2024, 1, 2))
    frame["label_available_at"] = cutoff_on(date(2024, 2, 2))
    return frame


def test_deferred_entry_contract_is_one_new_head_without_legacy_drift() -> None:
    assert MODEL_SCHEMA == "position_timing_local_model_v2"
    assert HEADS == ("ENTRY_ACTION_VALUE_V2", "EXIT_ACTION_VALUE_V2")
    assert ENTRY_TIMING_OBJECTIVE not in HEADS
    assert ENTRY_TIMING_ACTION_AUTHORITY == STUDY_CONTRACT["candidate_policy"]["policy_id"]
    assert STUDY_CONTRACT["comparators"][-1] == ALWAYS_OPEN_COMPARATOR
    assert STUDY_CONTRACT["economic_threshold_bps"] == 0.0
    assert STUDY_CONTRACT["familywise_hypothesis_count"] == 3
    assert LABEL_CONTRACT["candidate"].endswith("OPEN_T_PLUS_1")
    assert LABEL_CONTRACT["baseline"].endswith("OPEN_T_PLUS_2")
    assert len(ENTRY_TIMING_POLICY_SHA256) == 64


def test_deferred_entry_labels_are_same_stock_same_quantity_and_causal() -> None:
    candidate = FakeCandidate(180)
    result = build_deferred_entry_timing_rows(
        candidate,
        ActionValuePopulationSpec(
            start=candidate.calendar[35].date(),
            end=candidate.calendar[-25].date(),
            symbol_limit=2,
            review_stride=20,
        ),
        symbols=candidate.symbols,
    )

    assert result.rows["objective"].eq(ENTRY_TIMING_OBJECTIVE).all()
    assert result.rows["planned_delta_qty"].gt(0).all()
    assert result.rows["immediate_fill_status"].eq("FILLED").all()
    assert result.rows["deferred_fill_status"].isin(("FILLED", "NO_FILL")).all()
    assert result.rows["label_available_at"].gt(result.rows["decision_as_of"]).all()
    assert result.rows["holding_exposure"].eq(0).all()
    assert result.coverage["counts"]["rows"] == len(result.rows)
    assert result.coverage["coverage_sha256"] == canonical_sha256(
        {key: value for key, value in result.coverage.items() if key != "coverage_sha256"}
    )


def test_entry_timing_training_excludes_unmatured_and_rejects_wrong_support() -> None:
    rows = _training_fixture()
    rows.loc[0, "label_available_at"] = cutoff_on(date(2025, 1, 1))
    eligible = entry_timing_training_rows_asof(
        rows,
        cutoff_on(date(2024, 3, 29)),
        feature_order=FEATURE_ORDER,
    )
    assert len(eligible) == len(rows) - 1

    broken = rows.copy()
    broken.loc[1, "immediate_fill_status"] = "NO_FILL"
    with pytest.raises(ActionValueError, match="STATE_SUPPORT_INVALID"):
        entry_timing_training_rows_asof(
            broken,
            cutoff_on(date(2024, 3, 29)),
            feature_order=FEATURE_ORDER,
        )


def test_entry_timing_model_has_separate_identity_and_frozen_estimator() -> None:
    rows = _training_fixture()
    model = fit_entry_timing_model(
        rows,
        cutoff=cutoff_on(date(2024, 3, 29)),
        available_at=cutoff_on(date(2024, 4, 1)),
        source_sha256="1" * 64,
        request_sha256="2" * 64,
        source_commit="a" * 40,
    )
    assert model.metadata["schema_version"] == ENTRY_TIMING_MODEL_SCHEMA
    assert model.metadata["objective"] == ENTRY_TIMING_OBJECTIVE
    assert model.metadata["package_version"] == "4.6.0"
    assert model.metadata["entry_timing_policy_sha256"] == ENTRY_TIMING_POLICY_SHA256
    prediction = model.predict(
        rows.loc[:4, FEATURE_ORDER],
        decision_as_of=cutoff_on(date(2024, 4, 2)),
    )
    assert prediction.shape == (5,)
    assert np.isfinite(prediction).all()


class RecordingTimingModel:
    metadata = {
        "model_sha256": "4" * 64,
        "available_at": cutoff_on(date(2022, 3, 1)).isoformat(),
        "information_block": CORE_INFORMATION_BLOCK,
    }

    def __init__(self, value: float) -> None:
        self.value = value
        self.calls = 0

    def predict(self, frame, *, decision_as_of):
        self.calls += len(frame)
        return np.full(len(frame), self.value)


@pytest.mark.parametrize(("value", "cash_action"), [(10.0, "OPEN"), (-10.0, "WAIT")])
def test_continuous_deferred_entry_policy_only_models_cash_open_wait(
    value: float, cash_action: str
) -> None:
    candidate = FakeCandidate(180)
    model = RecordingTimingModel(value)
    replay = replay_continuous_cohorts(
        candidate,
        models=(),
        entry_timing_models=(model,),
        symbols=candidate.symbols,
        bootstrap_samples=20,
        model_action_authority=ENTRY_TIMING_ACTION_AUTHORITY,
    )
    cash = replay.sleeve_days.loc[replay.sleeve_days["initial_state"].eq("CASH_START")]
    held = replay.sleeve_days.loc[replay.sleeve_days["initial_state"].eq("HOLDING_START")]
    assert cash_action in set(cash["action"])
    assert not set(held["action"]).intersection({"OPEN", "ADD"})
    assert replay.receipt["policy_sha256"] == ENTRY_TIMING_POLICY_SHA256
    assert replay.receipt["entry_timing_force_open"] is False
    assert model.calls > 0


def test_force_open_comparator_is_explicit_and_not_a_serving_authority() -> None:
    candidate = FakeCandidate(180)
    model = RecordingTimingModel(-10.0)
    replay = replay_continuous_cohorts(
        candidate,
        models=(),
        entry_timing_models=(model,),
        symbols=candidate.symbols,
        bootstrap_samples=20,
        model_action_authority=ENTRY_TIMING_ACTION_AUTHORITY,
        entry_timing_force_open=True,
    )
    cash = replay.sleeve_days.loc[replay.sleeve_days["initial_state"].eq("CASH_START")]
    assert "OPEN" in set(cash["action"])
    assert replay.receipt["entry_timing_force_open"] is True
