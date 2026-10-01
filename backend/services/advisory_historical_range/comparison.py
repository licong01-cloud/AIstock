"""Compact, read-only comparison projection for one historical-range batch."""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from .query_repository import HistoricalRangeQueryError


COMPARISON_SCHEMA_VERSION = "advisory_historical_range_comparison_v1"
_AVAILABLE = "AVAILABLE"
_UNAVAILABLE = "UNAVAILABLE"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def build_historical_range_comparison(
    *,
    batch_id: str,
    baseline: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare two latest immutable summaries without creating new evidence."""

    baseline_id = _required_text(baseline, "range_run_id")
    candidate_id = _required_text(candidate, "range_run_id")
    if baseline_id == candidate_id:
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_RUNS_NOT_DISTINCT",
            "baseline and candidate historical-range runs must be different",
            context={"range_run_id": baseline_id},
        )
    mismatched = [
        run_id
        for run_id, fact in ((baseline_id, baseline), (candidate_id, candidate))
        if fact.get("batch_id") != batch_id
    ]
    if mismatched:
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_BATCH_MISMATCH",
            "historical-range comparison runs must belong to the requested batch",
            context={"batch_id": batch_id, "range_run_ids": mismatched},
        )

    blockers: list[str] = []
    warnings: list[str] = []
    baseline_has_summary = bool(baseline.get("summary_id"))
    candidate_has_summary = bool(candidate.get("summary_id"))
    if not baseline_has_summary:
        blockers.append("BASELINE_SUMMARY_UNAVAILABLE")
    if not candidate_has_summary:
        blockers.append("CANDIDATE_SUMMARY_UNAVAILABLE")
    if baseline_has_summary and not _complete_summary_identity(baseline):
        blockers.append("BASELINE_SUMMARY_IDENTITY_INCOMPLETE")
    if candidate_has_summary and not _complete_summary_identity(candidate):
        blockers.append("CANDIDATE_SUMMARY_IDENTITY_INCOMPLETE")

    if blockers:
        comparability = "INCOMPLETE_EVIDENCE"
    else:
        if baseline.get("summary_policy_hash") != candidate.get("summary_policy_hash"):
            blockers.append("SUMMARY_POLICY_HASH_MISMATCH")
        if baseline.get("producer_code_hash") != candidate.get("producer_code_hash"):
            blockers.append("PRODUCER_CODE_HASH_MISMATCH")
        comparability = "INCOMPATIBLE" if blockers else "COMPARABLE"

    baseline_metrics = _metric_index(baseline)
    candidate_metrics = _metric_index(candidate)
    if set(baseline_metrics) != set(candidate_metrics):
        warnings.append("METRIC_KEY_SET_DIFFERS")
    metric_rows = []
    for metric_key, group_key in sorted(set(baseline_metrics) | set(candidate_metrics)):
        left = baseline_metrics.get((metric_key, group_key), _not_reported())
        right = candidate_metrics.get((metric_key, group_key), _not_reported())
        delta = None
        if comparability == "COMPARABLE" and left["status"] == right["status"] == _AVAILABLE:
            baseline_value = _decimal(left["value"], metric_key=metric_key, side="baseline")
            candidate_value = _decimal(right["value"], metric_key=metric_key, side="candidate")
            delta = _format_decimal(candidate_value - baseline_value)
        metric_rows.append(
            {
                "metric_key": metric_key,
                "group_key": group_key or None,
                "baseline": left,
                "candidate": right,
                "delta": delta,
                "delta_semantics": "CANDIDATE_MINUS_BASELINE",
            }
        )

    return {
        "schema_version": COMPARISON_SCHEMA_VERSION,
        "batch_id": batch_id,
        "comparability": {
            "status": comparability,
            "blockers": blockers,
            "warnings": warnings,
            "summary_policy_hash": (
                baseline.get("summary_policy_hash") if comparability == "COMPARABLE" else None
            ),
            "producer_code_hash": (
                baseline.get("producer_code_hash") if comparability == "COMPARABLE" else None
            ),
            "decision_use": "BUSINESS_VALIDATION_ONLY",
        },
        "baseline": _run_identity(baseline),
        "candidate": _run_identity(candidate),
        "day_support": {
            "baseline": _day_support(baseline),
            "candidate": _day_support(candidate),
        },
        "omitted_diagnostics": {
            "reason": "HIGH_CARDINALITY_PER_DATE_RECALL_NOT_A_BUSINESS_AGGREGATE",
            "baseline": _omitted_metric_counts(baseline),
            "candidate": _omitted_metric_counts(candidate),
        },
        "metrics": metric_rows,
        "interpretation": {
            "delta_semantics": "CANDIDATE_MINUS_BASELINE",
            "winner_declared": False,
            "significance_claimed": False,
        },
    }


def _metric_index(fact: Mapping[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for status, field in ((_AVAILABLE, "metrics"), (_UNAVAILABLE, "unavailable_metrics")):
        raw_items = fact.get(field) or []
        if fact.get("summary_id") and fact.get(field) is None:
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                "historical-range summary is missing a required metric collection",
                context={"range_run_id": fact.get("range_run_id"), "field": field},
            )
        if not isinstance(raw_items, Sequence) or isinstance(raw_items, (str, bytes)):
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                "historical-range summary metric collection is invalid",
                context={"range_run_id": fact.get("range_run_id"), "field": field},
            )
        for raw_item in raw_items:
            if not isinstance(raw_item, Mapping):
                raise HistoricalRangeQueryError(
                    "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                    "historical-range summary metric item is invalid",
                    context={"range_run_id": fact.get("range_run_id"), "field": field},
                )
            metric_key = _required_text(raw_item, "metric_key")
            raw_group_key = raw_item.get("group_key")
            if raw_group_key is not None and not isinstance(raw_group_key, str):
                raise HistoricalRangeQueryError(
                    "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                    "historical-range summary metric group identity is invalid",
                    context={"range_run_id": fact.get("range_run_id"), "metric_key": metric_key},
                )
            group_key = raw_group_key or ""
            identity = (metric_key, group_key)
            if identity in result:
                raise HistoricalRangeQueryError(
                    "ADVISORY_HR_COMPARISON_METRIC_DUPLICATE",
                    "historical-range summary contains a duplicate metric identity",
                    context={"range_run_id": fact.get("range_run_id"), "metric_key": metric_key},
                )
            raw_coverage = raw_item.get("coverage") or {}
            reason_code = raw_item.get("reason_code")
            if (
                not isinstance(raw_coverage, Mapping)
                or raw_item.get("status") != status
                or (status == _AVAILABLE and reason_code is not None)
                or (status == _UNAVAILABLE and (not isinstance(reason_code, str) or not reason_code))
                or (status == _UNAVAILABLE and raw_item.get("value") is not None)
            ):
                raise HistoricalRangeQueryError(
                    "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                    "historical-range summary metric payload is invalid",
                    context={"range_run_id": fact.get("range_run_id"), "metric_key": metric_key},
                )
            item = {
                "status": status,
                "value": raw_item.get("value") if status == _AVAILABLE else None,
                "reason_code": reason_code if status == _UNAVAILABLE else None,
                "coverage": dict(raw_coverage),
            }
            if status == _AVAILABLE:
                item["value"] = _format_decimal(
                    _decimal(item["value"], metric_key=metric_key, side=str(fact.get("range_run_id")))
                )
            result[identity] = item
    return result


def _run_identity(fact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: fact.get(key)
        for key in (
            "range_run_id",
            "research_program_id",
            "package_id",
            "package_version",
            "manifest_sha256",
            "status",
            "summary_id",
            "summary_version",
            "summary_artifact_hash",
            "summary_policy_hash",
            "producer_code_hash",
        )
    }


def _day_support(fact: Mapping[str, Any]) -> dict[str, Any]:
    raw_counts = fact.get("day_status_counts") or {}
    if not isinstance(raw_counts, Mapping):
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_DAY_SUPPORT_INVALID",
            "historical-range day support counts are invalid",
            context={"range_run_id": fact.get("range_run_id")},
        )
    counts: dict[str, int] = {}
    for key, value in raw_counts.items():
        if (
            not isinstance(key, str)
            or not key
            or isinstance(value, bool)
            or not isinstance(value, int)
            or value < 0
        ):
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_COMPARISON_DAY_SUPPORT_INVALID",
                "historical-range day support counts are invalid",
                context={"range_run_id": fact.get("range_run_id")},
            )
        counts[key] = value
    valid_no_candidate = counts.get("VALID_NO_CANDIDATE", 0)
    return {
        "status_counts": counts,
        "total_day_count": sum(counts.values()),
        "successful_day_count": counts.get("COMPLETE", 0) + valid_no_candidate,
        "valid_no_candidate_day_count": valid_no_candidate,
    }


def _not_reported() -> dict[str, Any]:
    return {
        "status": "NOT_REPORTED",
        "value": None,
        "reason_code": "METRIC_NOT_REPORTED",
        "coverage": {},
    }


def _complete_summary_identity(fact: Mapping[str, Any]) -> bool:
    version = fact.get("summary_version")
    return (
        isinstance(version, int)
        and not isinstance(version, bool)
        and version >= 1
        and all(
            isinstance(fact.get(field), str) and bool(_SHA256.fullmatch(str(fact[field])))
            for field in ("summary_artifact_hash", "summary_policy_hash", "producer_code_hash")
        )
    )


def _omitted_metric_counts(fact: Mapping[str, Any]) -> dict[str, int]:
    raw_counts = fact.get("omitted_metric_counts")
    if raw_counts is None and not fact.get("summary_id"):
        return {"available_daily_recall": 0, "unavailable_daily_recall": 0}
    required_keys = ("available_daily_recall", "unavailable_daily_recall")
    if not isinstance(raw_counts, Mapping):
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
            "historical-range omitted diagnostic counts are invalid",
            context={"range_run_id": fact.get("range_run_id")},
        )
    result: dict[str, int] = {}
    for key in required_keys:
        value = raw_counts.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise HistoricalRangeQueryError(
                "ADVISORY_HR_COMPARISON_SUMMARY_INVALID",
                "historical-range omitted diagnostic counts are invalid",
                context={"range_run_id": fact.get("range_run_id"), "field": key},
            )
        result[key] = value
    return result


def _required_text(value: Mapping[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_FACT_INVALID",
            "historical-range comparison fact is missing a required identity",
            context={"field": field},
        )
    return result


def _decimal(value: Any, *, metric_key: str, side: str) -> Decimal:
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_METRIC_INVALID",
            "historical-range comparison metric is not numeric",
            context={"metric_key": metric_key, "side": side},
        ) from exc
    if not result.is_finite():
        raise HistoricalRangeQueryError(
            "ADVISORY_HR_COMPARISON_METRIC_INVALID",
            "historical-range comparison metric must be finite",
            context={"metric_key": metric_key, "side": side},
        )
    return result


def _format_decimal(value: Decimal) -> str:
    return format(value, "f")
