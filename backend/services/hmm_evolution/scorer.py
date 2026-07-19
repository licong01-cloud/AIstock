"""Batch-relative HMM recommendation scorer."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

RECOMMENDATION_VERSION = "hmm_recommendation_v1"
RECOMMENDATION_WEIGHTS: Mapping[str, float] = {
    "net_label_return": 0.45,
    "net_db_10d": 0.30,
    "positive_net_label_day_ratio": 0.15,
    "primary_coverage_ratio": 0.10,
}
EFFICACY_METRICS = frozenset({"net_label_return", "net_db_10d"})


@dataclass(frozen=True)
class RecommendationCandidate:
    candidate_id: str
    metrics: Mapping[str, float | None]


@dataclass(frozen=True)
class Recommendation:
    candidate_id: str
    score: float | None
    metric_availability_ratio: float | None
    rank: int | None
    is_top3: bool
    components: Mapping[str, Any]


def score_batch(
    candidates: Sequence[RecommendationCandidate],
) -> tuple[Recommendation, ...]:
    """Score all successful candidates without thresholds or elimination."""

    ids = [candidate.candidate_id for candidate in candidates]
    if len(ids) != len(set(ids)):
        raise ValueError("recommendation candidates must have unique candidate_id values")
    normalized = [_normalize_candidate(candidate) for candidate in candidates]
    percentiles = {
        metric: _metric_percentiles(normalized, metric)
        for metric in RECOMMENDATION_WEIGHTS
    }
    provisional: list[dict[str, Any]] = []
    for candidate in normalized:
        available_metrics = [
            metric for metric in RECOMMENDATION_WEIGHTS if candidate.metrics.get(metric) is not None
        ]
        efficacy_available = any(candidate.metrics.get(metric) is not None for metric in EFFICACY_METRICS)
        available_weight = sum(RECOMMENDATION_WEIGHTS[metric] for metric in available_metrics)
        score = None
        if efficacy_available and available_weight > 0:
            score = 100.0 * sum(
                RECOMMENDATION_WEIGHTS[metric] * percentiles[metric][candidate.candidate_id]
                for metric in available_metrics
            ) / available_weight
        missing_metrics = [metric for metric in RECOMMENDATION_WEIGHTS if metric not in available_metrics]
        warnings: list[dict[str, Any]] = []
        if missing_metrics:
            warnings.append(
                {
                    "code": "hmm_evolution_recommendation_weights_renormalized",
                    "message": "recommendation weights were renormalized over available metrics",
                    "context": {
                        "missing_metrics": missing_metrics,
                        "available_weight": available_weight,
                    },
                }
            )
        if not efficacy_available:
            warnings.append(
                {
                    "code": "hmm_evolution_recommendation_unranked_no_efficacy_metric",
                    "message": "candidate has no label or DB efficacy metric and remains unranked",
                    "context": {},
                }
            )
        provisional.append(
            {
                "candidate": candidate,
                "score": score,
                "metric_availability_ratio": available_weight if available_weight > 0 else None,
                "components": {
                    "schema_version": "hmm_recommendation_components_v1",
                    "recommendation_version": RECOMMENDATION_VERSION,
                    "formula": "100 * sum(weight * percentile) / available_weight",
                    "weights": dict(RECOMMENDATION_WEIGHTS),
                    "values": dict(candidate.metrics),
                    "percentiles": {
                        metric: percentiles[metric].get(candidate.candidate_id)
                        for metric in RECOMMENDATION_WEIGHTS
                    },
                    "available_weight": available_weight,
                    "missing_metrics": missing_metrics,
                    "evidence_quality": "degraded" if missing_metrics else "complete",
                    "warnings": warnings,
                },
            }
        )

    ranked = sorted(
        (item for item in provisional if item["score"] is not None),
        key=lambda item: (
            -float(item["score"]),
            -float(item["metric_availability_ratio"]),
            _descending_nullable(item["candidate"].metrics.get("net_db_10d")),
            item["candidate"].candidate_id,
        ),
    )
    ranks = {item["candidate"].candidate_id: index for index, item in enumerate(ranked, start=1)}
    top3 = set(list(ranks)[:3])
    recommendations = [
        Recommendation(
            candidate_id=item["candidate"].candidate_id,
            score=item["score"],
            metric_availability_ratio=item["metric_availability_ratio"],
            rank=ranks.get(item["candidate"].candidate_id),
            is_top3=item["candidate"].candidate_id in top3,
            components=item["components"],
        )
        for item in provisional
    ]
    return tuple(sorted(recommendations, key=lambda item: ids.index(item.candidate_id)))


def _normalize_candidate(candidate: RecommendationCandidate) -> RecommendationCandidate:
    candidate_id = str(candidate.candidate_id or "").strip()
    if not candidate_id:
        raise ValueError("candidate_id is required")
    metrics: dict[str, float | None] = {}
    for metric in RECOMMENDATION_WEIGHTS:
        value = candidate.metrics.get(metric)
        if value is None:
            metrics[metric] = None
            continue
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(f"recommendation metric {metric} must be finite or null")
        metrics[metric] = numeric
    return RecommendationCandidate(candidate_id=candidate_id, metrics=metrics)


def _metric_percentiles(
    candidates: Sequence[RecommendationCandidate],
    metric: str,
) -> dict[str, float]:
    values = sorted(
        (
            (candidate.candidate_id, float(candidate.metrics[metric]))
            for candidate in candidates
            if candidate.metrics.get(metric) is not None
        ),
        key=lambda item: (item[1], item[0]),
    )
    if not values:
        return {}
    if len(values) == 1:
        return {values[0][0]: 0.5}
    output: dict[str, float] = {}
    index = 0
    while index < len(values):
        end = index + 1
        while end < len(values) and values[end][1] == values[index][1]:
            end += 1
        average_rank = ((index + 1) + end) / 2.0
        percentile = (average_rank - 1.0) / (len(values) - 1.0)
        for candidate_id, _value in values[index:end]:
            output[candidate_id] = percentile
        index = end
    return output


def _descending_nullable(value: float | None) -> tuple[int, float]:
    if value is None:
        return (1, 0.0)
    return (0, -float(value))
