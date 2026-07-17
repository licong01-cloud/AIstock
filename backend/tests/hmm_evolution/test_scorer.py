from __future__ import annotations

import pytest

from backend.services.hmm_evolution.scorer import (
    RecommendationCandidate,
    score_batch,
)


def _candidate(candidate_id: str, **metrics):
    return RecommendationCandidate(candidate_id=candidate_id, metrics=metrics)


def test_scorer_uses_batch_percentiles_and_stable_top3() -> None:
    recommendations = score_batch(
        [
            _candidate(
                "c_low",
                net_label_return=0.01,
                net_db_10d=0.01,
                positive_net_label_day_ratio=0.4,
                primary_coverage_ratio=0.8,
            ),
            _candidate(
                "c_high",
                net_label_return=0.03,
                net_db_10d=0.04,
                positive_net_label_day_ratio=0.8,
                primary_coverage_ratio=1.0,
            ),
            _candidate(
                "c_mid",
                net_label_return=0.02,
                net_db_10d=0.02,
                positive_net_label_day_ratio=0.6,
                primary_coverage_ratio=0.9,
            ),
            _candidate(
                "c_fourth",
                net_label_return=0.015,
                net_db_10d=0.015,
                positive_net_label_day_ratio=0.5,
                primary_coverage_ratio=0.85,
            ),
        ]
    )
    by_id = {item.candidate_id: item for item in recommendations}
    assert by_id["c_high"].rank == 1
    assert by_id["c_mid"].rank == 2
    assert by_id["c_fourth"].rank == 3
    assert by_id["c_low"].rank == 4
    assert {item.candidate_id for item in recommendations if item.is_top3} == {
        "c_high",
        "c_mid",
        "c_fourth",
    }


def test_singleton_percentiles_are_half() -> None:
    recommendation = score_batch(
        [
            _candidate(
                "only",
                net_label_return=0.02,
                net_db_10d=0.03,
                positive_net_label_day_ratio=0.7,
                primary_coverage_ratio=0.9,
            )
        ]
    )[0]
    assert recommendation.score == pytest.approx(50.0)
    assert recommendation.confidence == pytest.approx(1.0)
    assert set(recommendation.components["percentiles"].values()) == {0.5}


def test_tied_candidates_use_db_then_candidate_id_tiebreak() -> None:
    recommendations = score_batch(
        [
            _candidate("b", net_label_return=0.1, net_db_10d=0.2),
            _candidate("a", net_label_return=0.1, net_db_10d=0.2),
        ]
    )
    by_id = {item.candidate_id: item for item in recommendations}
    assert by_id["a"].rank == 1
    assert by_id["b"].rank == 2


def test_missing_metrics_renormalize_and_degrade_without_zero_fill() -> None:
    recommendation = score_batch(
        [_candidate("partial", net_label_return=0.1, primary_coverage_ratio=0.8)]
    )[0]
    assert recommendation.score == pytest.approx(50.0)
    assert recommendation.confidence == pytest.approx(0.55)
    assert recommendation.components["evidence_quality"] == "degraded"
    assert recommendation.components["missing_metrics"] == [
        "net_db_10d",
        "positive_net_label_day_ratio",
    ]


def test_coverage_only_candidate_remains_unranked() -> None:
    recommendation = score_batch(
        [_candidate("coverage", primary_coverage_ratio=1.0)]
    )[0]
    assert recommendation.score is None
    assert recommendation.confidence == pytest.approx(0.10)
    assert recommendation.rank is None
    assert recommendation.is_top3 is False


def test_scorer_does_not_mutate_input_metrics() -> None:
    metrics = {"net_label_return": 0.1, "net_db_10d": 0.2}
    candidate = RecommendationCandidate(candidate_id="immutable", metrics=metrics)
    score_batch([candidate])
    assert metrics == {"net_label_return": 0.1, "net_db_10d": 0.2}


def test_non_finite_metric_fails_loudly() -> None:
    with pytest.raises(ValueError, match="finite"):
        score_batch([_candidate("bad", net_label_return=float("nan"))])
