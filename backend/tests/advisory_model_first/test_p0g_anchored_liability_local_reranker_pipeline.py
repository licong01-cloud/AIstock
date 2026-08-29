from __future__ import annotations

from types import SimpleNamespace
import time

import pandas as pd
import pytest

from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first import (
    p0g_anchored_liability_local_reranker_pipeline as pipeline,
)


def _label_rows(dates: pd.DatetimeIndex) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "decision_as_of_trade_date": decision,
                "target_trade_date": decision + pd.offsets.BDay(1),
                "instrument": f"S{rank:02d}",
                "label_status": "MATURED",
            }
            for decision in dates
            for rank in range(1, 21)
        ]
    )


def test_fixed_anchor_oof_reselects_price_inside_each_inner_train_fold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dates = pd.bdate_range("2026-01-05", periods=4)
    folds = (
        SimpleNamespace(
            block_id=0,
            train_dates=(dates[0], dates[1]),
            validation_dates=(dates[2],),
            score_dates=(dates[2],),
            purged_dates=(),
            embargo_dates=(),
        ),
        SimpleNamespace(
            block_id=1,
            train_dates=(dates[1], dates[2]),
            validation_dates=(dates[3],),
            score_dates=(dates[3],),
            purged_dates=(),
            embargo_dates=(),
        ),
    )
    observed: list[tuple[pd.Timestamp, ...]] = []

    def fake_select(**kwargs):
        observed.append(tuple(pd.DatetimeIndex(kwargs["calibration_dates"])))
        return {"shadow_price": 0.0, "p0d_turnover_budget": 0.2}

    def fake_train(**kwargs):
        score_dates = pd.DatetimeIndex(kwargs["validation_dates"])
        predictions = pd.DataFrame(
            [
                {
                    "decision_as_of_trade_date": decision,
                    "target_trade_date": decision + pd.offsets.BDay(1),
                    "instrument": f"S{rank:02d}",
                    "entry_priority_rank": rank,
                }
                for decision in score_dates
                for rank in range(1, 21)
            ]
        )
        return SimpleNamespace(validation_predictions=predictions, best_iteration=7)

    monkeypatch.setattr(pipeline, "_select_anchor_price", fake_select)
    monkeypatch.setattr(pipeline, "train_turnover_constrained_utility_trial", fake_train)
    request = SimpleNamespace(
        expected_candidates_per_date=20,
        exact_p0g_anchor_reference=SimpleNamespace(winner_seed=20260817),
        target_count=5,
    )
    result, rounds, receipts = pipeline._train_anchor_oof(
        features=pd.DataFrame(),
        labels=_label_rows(dates),
        folds=folds,
        rankings=pd.DataFrame(),
        block_by_date={},
        candidate_daily=pd.DataFrame(),
        benchmark=pd.DataFrame(),
        suspend=pd.DataFrame(),
        calendar=(),
        policy=object(),
        cost=object(),
        request=request,
        p0d_family=object(),
        p0g_family=object(),
        suffix="path",
    )
    assert observed == [(dates[0], dates[1]), (dates[1], dates[2])]
    assert set(pd.to_datetime(result["decision_as_of_trade_date"])) == {dates[2], dates[3]}
    assert rounds == (7, 7)
    assert len(receipts) == 2


def test_pbo_identity_vectors_are_diagnostic_only_without_fake_number() -> None:
    frame = pd.DataFrame(
        [
            {"trial_id": trial, "block_id": block, "metric": float(block)}
            for trial in ("trial_a", "trial_b")
            for block in range(8)
        ]
    )
    receipt = pipeline._pbo_with_identity_diagnostic(
        frame, metric="metric", group_count=8
    )
    assert receipt["status"] == "DEGENERATE_NOT_INTERPRETABLE"
    assert receipt["pbo"] is None
    assert receipt["unique_block_score_vector_count"] == 1
    assert receipt["pbo_is_gate"] is False


def test_complete_roster_requires_six_trials_on_same_twenty_eight_paths() -> None:
    request = SimpleNamespace(
        expected_outer_trial_path_count=168,
        expected_outer_path_count=28,
        family_specs=(SimpleNamespace(), SimpleNamespace()),
        seed_roster=(1, 2, 3),
    )
    complete = pd.DataFrame(
        [
            {"family_id": family, "seed": seed, "path_id": f"p{path:02d}"}
            for family in ("core", "hmm")
            for seed in (1, 2, 3)
            for path in range(28)
        ]
    )
    pipeline._verify_complete_roster(request, complete)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        pipeline._verify_complete_roster(request, complete.iloc[:-1])
    assert exc_info.value.reason_code == "ADVISORY_P0L_INCOMPLETE_CPCV"


def test_p0l_resource_limit_uses_stage_specific_typed_failure() -> None:
    progress = pipeline.P0LProgress(-1)
    with pytest.raises(AdvisoryModelFirstError) as exc_info:
        progress.add("test", time.monotonic())
    assert exc_info.value.reason_code == "ADVISORY_P0L_RESOURCE_LIMIT_EXCEEDED"
