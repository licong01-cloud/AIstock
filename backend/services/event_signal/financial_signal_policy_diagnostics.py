"""Diagnostics for first-stage structured financial event signal policy.

The module reads event-study aggregate metrics and proposes conservative
research-only policy recommendations.  It does not mutate event_signal rows and
does not connect the recommendations to QE, Selection, Paper, or live trading.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional


ROOT = Path(__file__).resolve().parents[3]

NEGATIVE_FINANCIAL_EVENTS: frozenset[str] = frozenset(
    {
        "financial_forecast_loss",
        "financial_forecast_large_decline",
        "financial_express_loss",
        "financial_express_large_decline",
        "financial_indicator_loss",
        "financial_indicator_large_decline",
    }
)

POSITIVE_OR_RESEARCH_EVENTS: frozenset[str] = frozenset(
    {
        "financial_forecast_large_growth",
        "financial_forecast_turnaround",
        "financial_express_large_growth",
        "financial_indicator_large_growth",
    }
)

RELATION_EVENTS: frozenset[str] = frozenset(
    {
        "financial_positive_but_miss_expectation",
    }
)

PREFERRED_WINDOWS: tuple[str, ...] = ("T0", "T0_T2", "T0_T20", "T0_T10", "T+1")


@dataclass(frozen=True)
class FinancialPolicyRecommendation:
    event_type: str
    recommended_action: str
    recommended_risk_level: str
    enable_alpha: bool
    hard_block_candidate: bool
    confidence: str
    reason_codes: tuple[str, ...]
    evidence: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _metric_by_event_window(aggregates: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    mapping: dict[tuple[str, str], dict[str, Any]] = {}
    for row in aggregates:
        event_type = str(row.get("event_type"))
        window_name = str(row.get("window_name"))
        mapping[(event_type, window_name)] = dict(row)
    return mapping


def _select_evidence_metric(
    aggregates_by_key: Mapping[tuple[str, str], Mapping[str, Any]],
    event_type: str,
) -> Optional[dict[str, Any]]:
    for window in PREFERRED_WINDOWS:
        row = aggregates_by_key.get((event_type, window))
        if row:
            return dict(row)
    candidates = [dict(row) for (row_event_type, _), row in aggregates_by_key.items() if row_event_type == event_type]
    return candidates[0] if candidates else None


def _negative_signal_is_empirically_supported(metric: Mapping[str, Any]) -> bool:
    mean_raw = _safe_float(metric.get("mean_raw_return"))
    negative_rate = _safe_float(metric.get("negative_return_rate"))
    down_limit_rate = _safe_float(metric.get("down_limit_rate"))
    return bool(
        (mean_raw is not None and mean_raw <= -0.005)
        or (negative_rate is not None and negative_rate >= 0.58)
        or (down_limit_rate is not None and down_limit_rate >= 0.08)
    )


def _relation_signal_is_not_hard_block(metric: Mapping[str, Any]) -> bool:
    mean_raw = _safe_float(metric.get("mean_raw_return"))
    negative_rate = _safe_float(metric.get("negative_return_rate"))
    return bool(
        (mean_raw is None or mean_raw > -0.01)
        and (negative_rate is None or negative_rate < 0.58)
    )


def recommend_financial_policy(
    event_type: str,
    aggregates_by_key: Mapping[tuple[str, str], Mapping[str, Any]],
) -> FinancialPolicyRecommendation:
    """Recommend conservative first-stage handling for one event type."""

    metric = _select_evidence_metric(aggregates_by_key, event_type) or {}
    evidence = {
        "selected_window": metric.get("window_name"),
        "rows": metric.get("rows"),
        "valid_raw_returns": metric.get("valid_raw_returns"),
        "mean_raw_return": metric.get("mean_raw_return"),
        "negative_return_rate": metric.get("negative_return_rate"),
        "down_limit_rate": metric.get("down_limit_rate"),
    }

    if event_type in NEGATIVE_FINANCIAL_EVENTS:
        if metric and _negative_signal_is_empirically_supported(metric):
            return FinancialPolicyRecommendation(
                event_type=event_type,
                recommended_action="warn_review",
                recommended_risk_level="P2_REVIEW",
                enable_alpha=False,
                hard_block_candidate=False,
                confidence="medium",
                reason_codes=("negative_financial_event_supported_by_event_study", "risk_warning_only_first_stage"),
                evidence=evidence,
            )
        return FinancialPolicyRecommendation(
            event_type=event_type,
            recommended_action="warn_review",
            recommended_risk_level="P2_REVIEW",
            enable_alpha=False,
            hard_block_candidate=False,
            confidence="low",
            reason_codes=("negative_financial_event_needs_more_samples_or_threshold_refinement",),
            evidence=evidence,
        )

    if event_type in RELATION_EVENTS:
        if metric and _relation_signal_is_not_hard_block(metric):
            return FinancialPolicyRecommendation(
                event_type=event_type,
                recommended_action="warn_review",
                recommended_risk_level="P2_REVIEW",
                enable_alpha=False,
                hard_block_candidate=False,
                confidence="medium",
                reason_codes=("expectation_miss_signal_is_review_not_block", "needs_threshold_refinement"),
                evidence=evidence,
            )
        return FinancialPolicyRecommendation(
            event_type=event_type,
            recommended_action="warn_high_candidate",
            recommended_risk_level="P1_HIGH_CANDIDATE",
            enable_alpha=False,
            hard_block_candidate=False,
            confidence="low",
            reason_codes=("expectation_miss_tail_risk_candidate", "manual_review_before_policy_change"),
            evidence=evidence,
        )

    if event_type in POSITIVE_OR_RESEARCH_EVENTS:
        return FinancialPolicyRecommendation(
            event_type=event_type,
            recommended_action="record_only",
            recommended_risk_level="P3_POSITIVE_CANDIDATE",
            enable_alpha=False,
            hard_block_candidate=False,
            confidence="medium" if metric else "low",
            reason_codes=("positive_alpha_disabled_until_model_validation",),
            evidence=evidence,
        )

    return FinancialPolicyRecommendation(
        event_type=event_type,
        recommended_action="record_only",
        recommended_risk_level="P4_NEUTRAL",
        enable_alpha=False,
        hard_block_candidate=False,
        confidence="low",
        reason_codes=("unrecognized_financial_event_type",),
        evidence=evidence,
    )


def build_financial_policy_diagnostics(aggregates: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    aggregates_by_key = _metric_by_event_window(aggregates)
    event_types = sorted({event_type for event_type, _ in aggregates_by_key})
    recommendations = [
        recommend_financial_policy(event_type, aggregates_by_key).to_dict()
        for event_type in event_types
    ]
    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "event_types": event_types,
        "recommendations": recommendations,
        "summary": {
            "rows": len(recommendations),
            "warn_review": sum(1 for row in recommendations if row["recommended_action"] == "warn_review"),
            "record_only": sum(1 for row in recommendations if row["recommended_action"] == "record_only"),
            "alpha_enabled": sum(1 for row in recommendations if row["enable_alpha"]),
            "hard_block_candidates": sum(1 for row in recommendations if row["hard_block_candidate"]),
        },
        "stage_boundary": {
            "trading_consumption_enabled": False,
            "alpha_overlay_enabled": False,
            "hard_block_enabled": False,
        },
    }


def load_aggregates_from_event_study_json(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    aggregates = payload.get("aggregates")
    if not isinstance(aggregates, list):
        raise ValueError(f"{path} does not contain an aggregates list")
    return [dict(row) for row in aggregates]


def write_policy_diagnostics(payload: dict[str, Any], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str, indent=2), encoding="utf-8")
    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build research-only financial signal policy diagnostics")
    parser.add_argument("event_study_json", help="Path to financial_event_study_*.json")
    parser.add_argument("--output", default=None, help="Optional output JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = Path(args.event_study_json)
    payload = build_financial_policy_diagnostics(load_aggregates_from_event_study_json(input_path))
    output_path = Path(args.output) if args.output else input_path.with_name(input_path.stem + "_policy_diagnostics.json")
    write_policy_diagnostics(payload, output_path)
    print(json.dumps({"output": str(output_path), "summary": payload["summary"]}, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
