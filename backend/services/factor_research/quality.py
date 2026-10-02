"""Descriptive factor-quality evidence. No thresholds, ranking policy or production writes."""
from __future__ import annotations

import calendar
from collections import Counter, defaultdict
from datetime import date
import math

from .models import ResearchError

BASIS = ("calc_batch_id", "snapshot_date", "calc_engine", "return_horizon", "universe",
         "universe_rule_version", "universe_fingerprint_sha256", "index_policy", "direction",
         "coverage_semantics")
MEASURES = ("ic_mean", "rank_ic_mean", "icir", "coverage", "h20_ic_mean", "h20_rank_ic_mean")
PAIR_BASIS = ("method", "as_of_date", "data_window_days", "universe", "universe_rule_version",
              "universe_fingerprint_sha256", "index_policy")


def day(value):
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def finite(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def window(as_of, history_start, recent_start):
    values = tuple(day(v) for v in (as_of, history_start, recent_start))
    end, start, recent = values
    if any(v is None for v in values) or not start < recent <= end:
        raise ResearchError("invalid_request", "Require history_start < recent_start <= as_of (ISO dates)")
    return values


def clean(value):
    """Preserve missing/nonfinite diagnostics separately; emit finite JSON, never substitute zero."""
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean(v) for v in value]
    if isinstance(value, date):
        return value.isoformat()
    return value


def metric_evidence(rows, end, start, recent):
    reasons, eligible, observations = set(), [], []
    for row in sorted(rows, key=lambda r: r["id"]):
        lo, hi, snapshot = (day(row.get(k)) for k in ("data_start", "data_end", "snapshot_date"))
        issues = []
        if lo is None or hi is None or snapshot is None or lo > hi:
            issues.append("metric_date_invalid")
        elif hi > end or snapshot > end or hi > snapshot:
            issues.append("metric_after_as_of")
        elif hi < start:
            continue
        if type(row.get("n_trading_days")) is not int or row["n_trading_days"] <= 0:
            issues.append("metric_sample_unavailable")
        for field in MEASURES:
            if row.get(field) is not None and not finite(row[field]):
                issues.append(f"nonfinite_{field}")
        if not finite(row.get("ic_mean")):
            issues.append("ic_unavailable")
        for field in ("ic_mean", "rank_ic_mean", "h20_ic_mean", "h20_rank_ic_mean"):
            if finite(row.get(field)) and abs(row[field]) > 1:
                issues.append(f"invalid_range_{field}")
        if finite(row.get("coverage")) and not 0 <= row["coverage"] <= 1:
            issues.append("coverage_invalid_range")
        if row.get("direction") not in (-1, 1):
            issues.append("direction_unavailable")
        observations.append({**clean(row), "issues": sorted(set(issues))})
        reasons.update(issues)
        if not issues:
            eligible.append(row)
    groups = defaultdict(list)
    for row in eligible:
        missing = [key for key in BASIS if row.get(key) in (None, "")]
        if missing:
            reasons.add("metric_basis_incomplete")
            continue
        groups[tuple(str(row[k]) for k in BASIS)].append(row)
    comparisons = []
    for basis, group in sorted(groups.items()):
        boundaries = Counter((str(r["data_start"]), str(r["data_end"]), r["eval_window"]) for r in group)
        if any(n > 1 for n in boundaries.values()):
            reasons.add("metric_window_conflict")
            continue
        old = [r for r in group if start <= day(r["data_start"]) < recent]
        new = [r for r in group if recent <= day(r["data_start"]) <= day(r["data_end"]) <= end]
        for baseline in old:
            for current in new:
                overlap = day(baseline["data_end"]) >= day(current["data_start"])
                comparisons.append({"baseline_id": baseline["id"], "recent_id": current["id"],
                                    "basis": dict(zip(BASIS, basis)), "overlapping": overlap,
                                    "interpretation": "descriptive_not_significance_or_alpha_validity",
                                    "raw_ic_delta": current["ic_mean"] - baseline["ic_mean"],
                                    "direction_adjusted_ic_delta": (current["ic_mean"] - baseline["ic_mean"]) * current["direction"],
                                    "baseline_days": baseline["n_trading_days"], "recent_days": current["n_trading_days"]})
    if not rows:
        reasons.add("metrics_missing")
    if not comparisons:
        reasons.add("comparable_windows_unavailable")
    return observations, comparisons, reasons


def monthly_evidence(rows, end, start, recent):
    groups, reasons = defaultdict(list), set()
    for row in sorted(rows, key=lambda r: r["id"]):
        raw = str(row.get("month_end", ""))
        try:
            first = date.fromisoformat(raw + "-01" if len(raw) == 7 else raw).replace(day=1)
            last = first.replace(day=calendar.monthrange(first.year, first.month)[1])
        except ValueError:
            reasons.add("month_invalid")
            continue
        snapshot = day(row.get("snapshot_date"))
        if snapshot is None:
            reasons.add("monthly_snapshot_invalid")
            continue
        if snapshot > end or last > end or last > snapshot or first < start:
            continue
        if (not finite(row.get("ic_mean")) or abs(row["ic_mean"]) > 1
                or type(row.get("n_days")) is not int or not 0 < row["n_days"] <= last.day):
            reasons.add("monthly_sample_or_value_unavailable")
            continue
        if first < recent <= last:
            reasons.add("partial_month_excluded")
            continue
        groups[str(snapshot)].append({**clean(row), "month": first.strftime("%Y-%m"),
                                      "period": "recent" if first >= recent else "history"})
    result = []
    for snapshot, rows in sorted(groups.items()):
        if len({r["month"] for r in rows}) != len(rows):
            reasons.add("monthly_boundary_conflict")
            result.append({"snapshot_date": snapshot, "status": "conflict", "row_ids": [r["id"] for r in rows]})
            continue
        periods = {}
        for period in ("history", "recent"):
            selected = [r for r in rows if r["period"] == period]
            days = sum(r["n_days"] for r in selected)
            periods[period] = {"months": len(selected), "days": days,
                               "weighted_ic": sum(r["ic_mean"] * r["n_days"] for r in selected) / days if days else None}
        old, new = (periods[p]["weighted_ic"] for p in ("history", "recent"))
        result.append({"snapshot_date": snapshot, "status": "descriptive_only",
                       "basis": "monthly_batch_horizon_universe_and_label_maturity_unverified",
                       "periods": periods, "raw_ic_delta": new - old if old is not None and new is not None else None,
                       "months": rows})
    return result, reasons | {"monthly_basis_unverified" if result else "monthly_unavailable"}


def diagnose(catalog, metrics, monthly, correlations, *, as_of, history_start, recent_start):
    end, start, recent = window(as_of, history_start, recent_start)
    names = Counter(row["factor_name"] for row in catalog)
    identities = {row["id"]: row["factor_name"] for row in catalog}
    unassigned = []
    by_name, by_id, months, pairs, expressions = (defaultdict(list) for _ in range(5))
    for row in metrics:
        if row.get("factor_catalog_id") is None:
            by_name[row["factor_name"]].append(row)
            if names[row["factor_name"]] != 1:
                unassigned.append(row["id"])
        else:
            by_id[row["factor_catalog_id"]].append(row)
            if identities.get(row["factor_catalog_id"]) != row["factor_name"]:
                unassigned.append(row["id"])
    for row in monthly:
        months[row["factor_name"]].append(row)
    for row in correlations:
        stamp = day(row.get("as_of_date"))
        if stamp is None or stamp > end or stamp < start:
            continue
        pairs[row["factor_a_id"]].append(row)
        if row["factor_b_id"] != row["factor_a_id"]:
            pairs[row["factor_b_id"]].append(row)
    for row in catalog:
        if row.get("expression") and row["expression"].strip():
            expressions[row["expression"]].append(row["id"])
    factors = []
    for item in sorted(catalog, key=lambda r: r["id"]):
        ident, name = item["id"], item["factor_name"]
        reasons = set()
        rows = by_id[ident]
        if names[name] == 1:
            rows = rows + by_name[name]
        elif by_name[name] or months[name]:
            reasons.add("name_only_source_ambiguous")
        rows = [r for r in rows if r["factor_name"] == name]
        evidence, comparisons, issues = metric_evidence(rows, end, start, recent)
        reasons.update(issues)
        month, issues = monthly_evidence(months[name] if names[name] == 1 else [], end, start, recent)
        reasons.update(issues)
        linked = []
        pair_keys = Counter((tuple(sorted((p["factor_a_id"], p["factor_b_id"]))),
                             tuple(str(p.get(k)) for k in PAIR_BASIS)) for p in pairs[ident])
        for pair in pairs[ident]:
            corr = pair.get("correlation")
            invalid = not finite(corr) or abs(corr) > 1 or pair["factor_a_id"] == pair["factor_b_id"]
            key = (tuple(sorted((pair["factor_a_id"], pair["factor_b_id"]))),
                   tuple(str(pair.get(k)) for k in PAIR_BASIS))
            conflict = pair_keys[key] > 1
            if conflict:
                reasons.add("correlation_basis_conflict")
            if any(pair.get(k) in (None, "") for k in PAIR_BASIS):
                reasons.add("correlation_basis_incomplete")
            reasons.add("correlation_invalid" if invalid else "correlation_sample_support_unavailable")
            linked.append({**clean(pair), "absolute_correlation": abs(corr) if not invalid and not conflict else None,
                           "status": "invalid" if invalid else "conflict" if conflict else "redundancy_lead_not_disposal_evidence"})
        linked.sort(key=lambda p: (p["absolute_correlation"] is None, -(p["absolute_correlation"] or 0), p["id"]))
        if not linked:
            reasons.add("correlations_missing")
        duplicate_ids = sorted(i for i in expressions.get(item.get("expression"), []) if i != ident)
        observations_available = any(not r["issues"] for r in evidence)
        suggestions = ["review_purpose_and_consumer_dependencies"]
        if not observations_available:
            suggestions.insert(0, "resolve_metric_evidence_gap")
        if any(c["direction_adjusted_ic_delta"] < 0 for c in comparisons):
            suggestions.append("investigate_observed_decline_not_automatic_retirement")
        if duplicate_ids:
            suggestions.append("compare_same_expression_versions")
        factors.append({"catalog_id": ident, "factor_name": name, "source": item.get("source"),
                        "is_available_unchanged": item.get("is_available"),
                        "purpose_description": item.get("description_cn"),
                        "status": "reviewed" if observations_available else "pending",
                        "metrics": evidence, "comparisons": comparisons, "monthly": month,
                        "correlations": linked, "same_expression_catalog_ids": duplicate_ids,
                        "source_references": {k: item.get(k) for k in ("experiment_id", "best_loop_task_run_id", "source_task_id")},
                        "consumer_dependencies": "unverified_not_unused", "suggestions": suggestions,
                        "reasons": sorted(reasons)})
    status = Counter(r["status"] for r in factors)
    reason_counts = Counter(reason for r in factors for reason in r["reasons"])
    return clean({"schema_version": "factor_quality_observation_v1", "scope": "selected_database_catalog",
                  "as_of": as_of, "history_start": history_start, "recent_start": recent_start,
                  "summary": {"total": len(catalog), "reviewed": status["reviewed"], "pending": status["pending"],
                              "status": "empty" if not catalog else "inventory_complete",
                              "reasons": dict(sorted(reason_counts.items()))},
                  "meaning": "reviewed_is_observation_not_factor_validity_or_disposal_approval",
                  "unassigned_metric_ids": sorted(unassigned),
                  "factors": factors, "official_writes": False, "recomputation": False})
