"""Deterministic SW L2 rotation scoring and development evaluation.

This module is intentionally independent from persistence and dataset I/O.  It
accepts one frozen, fully validated input bundle and implements the approved
L2-P0-D2..D4 contract without model fitting, parameter search, or fallback.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from datetime import date
from typing import Any, Mapping, Sequence

import numpy as np

from backend.services.hmm_risk.contracts import canonical_sha256


CONTRACT_VERSION = "hmm_risk_rotation_l2_moneyflow_delta_v1"
INPUT_SCHEMA = "hmm_risk_rotation_l2_input_bundle_v1"
PROCESS_SCHEMA = "hmm_risk_rotation_l2_process_report_v1"
ACCEPTANCE_SCHEMA = "hmm_risk_rotation_l2_acceptance_v1"
CONSUMER_SCHEMA = "hmm_rotation_l2_consumer_v1"
CATALOG_COUNT = 131
FEATURE_DAYS = 25
INTENSITY_DAYS = 20
DELTA_LAG = 5
HORIZON = 10
BINDING_MBE_RANK_IC = 0.02
COVERAGE_THRESHOLD = 0.90
HAC_LAG = 9
DEVELOPMENT_START = date(2024, 9, 19)
DEVELOPMENT_END = date(2026, 3, 31)
REPORT_BLOCKS = (
    ("block_1", date(2024, 9, 19), date(2025, 3, 31)),
    ("block_2", date(2025, 4, 1), date(2025, 9, 30)),
    ("block_3", date(2025, 10, 1), date(2026, 3, 31)),
)


class RotationL2Error(RuntimeError):
    def __init__(self, reason_code: str, message: str, *, context: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.reason_code = reason_code
        self.context = dict(context or {})


def _fail(suffix: str, message: str, **context: Any) -> RotationL2Error:
    return RotationL2Error(f"hmm_risk_rotation_l2_{suffix}", message, context=context)


def model_contract() -> dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "feature": {
            "source": "pit_stock_moneyflow_and_amount_cny",
            "source_days": FEATURE_DAYS,
            "intensity_days": INTENSITY_DAYS,
            "delta_lag_open_days": DELTA_LAG,
            "minimum_daily_contributors": 1,
            "minimum_daily_coverage": COVERAGE_THRESHOLD,
        },
        "score": "(average_rank(delta)-1)/(N-1)-0.5",
        "state_fraction": 0.20,
        "state_tie_policy": "boundary_tie_group_is_neutral",
        "fit_count": 0,
        "seed": "not_applicable",
    }


def evaluation_contract() -> dict[str, Any]:
    return {
        "schema_version": "hmm_risk_rotation_l2_evaluation_contract_v1",
        "development_start": DEVELOPMENT_START.isoformat(),
        "development_end": DEVELOPMENT_END.isoformat(),
        "horizon_open_days": HORIZON,
        "report_blocks": [
            {"name": name, "start": start.isoformat(), "end": end.isoformat()} for name, start, end in REPORT_BLOCKS
        ],
        "binding_mean_daily_rank_ic": BINDING_MBE_RANK_IC,
        "coverage_threshold": COVERAGE_THRESHOLD,
        "hac_lag": HAC_LAG,
        "tail_forbidden_from": "2026-04-01",
    }


MODEL_HASH = canonical_sha256(model_contract())
EVALUATION_CONTRACT_HASH = canonical_sha256(evaluation_contract())


def _parse_day(value: Any, field: str) -> date:
    try:
        parsed = date.fromisoformat(str(value))
    except ValueError as exc:
        raise _fail("input_identity_invalid", f"{field} is not an ISO date", field=field, value=value) from exc
    return parsed


def _finite(value: Any, field: str) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise _fail("source_invalid", f"{field} is not numeric", field=field, value=value) from exc
    if not math.isfinite(parsed):
        raise _fail("source_invalid", f"{field} is not finite", field=field, value=value)
    return parsed


def validate_input_bundle(bundle: Mapping[str, Any]) -> dict[str, Any]:
    if bundle.get("schema_version") != INPUT_SCHEMA:
        raise _fail("input_identity_invalid", "input bundle schema differs")
    catalog = bundle.get("catalog")
    calendar_raw = bundle.get("calendar")
    daily = bundle.get("daily_aggregates")
    sector_returns = bundle.get("sector_returns")
    benchmark = bundle.get("benchmark_close")
    identity = bundle.get("identity")
    if not isinstance(catalog, list) or len(catalog) != CATALOG_COUNT:
        raise _fail("input_identity_invalid", "canonical L2 catalog must contain 131 rows")
    if not isinstance(calendar_raw, list) or not calendar_raw:
        raise _fail("input_identity_invalid", "calendar is empty")
    if not all(isinstance(value, list) for value in (daily, sector_returns, benchmark)) or not isinstance(
        identity, Mapping
    ):
        raise _fail("input_identity_invalid", "input bundle sections are incomplete")
    codes: list[str] = []
    names: dict[str, str] = {}
    for raw in catalog:
        if not isinstance(raw, Mapping):
            raise _fail("input_identity_invalid", "catalog row is not an object")
        code = str(raw.get("sector_code") or "").strip().upper()
        name = str(raw.get("sector_name") or "").strip()
        if not code or not name or code in names:
            raise _fail("input_identity_invalid", "catalog is blank or duplicated", sector_code=code)
        codes.append(code)
        names[code] = name
    if codes != sorted(codes):
        raise _fail("input_identity_invalid", "catalog is not canonical ordered")
    calendar = tuple(_parse_day(value, "calendar") for value in calendar_raw)
    if tuple(sorted(set(calendar))) != calendar:
        raise _fail("input_identity_invalid", "calendar is not sorted and unique")
    if DEVELOPMENT_START not in calendar or DEVELOPMENT_END not in calendar:
        raise _fail("history_unavailable", "development boundaries are absent from calendar")
    start_index = calendar.index(DEVELOPMENT_START)
    if start_index < FEATURE_DAYS:
        raise _fail("history_unavailable", "25 source sessions before development are unavailable")
    sha_fields = (
        "source_commit",
        "profile_sha256",
        "manifest_sha256",
        "mapping_hash",
        "quote_authority_hash",
        "calendar_hash",
    )
    for field in sha_fields:
        value = str(identity.get(field) or "")
        if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
            raise _fail("input_identity_invalid", f"identity {field} is not a SHA-256")
    if identity.get("cutoff") != "2026-08-31" or not str(identity.get("release_id") or "").strip():
        raise _fail("input_identity_invalid", "release identity or cutoff differs")
    source_git_commit = str(identity.get("source_git_commit") or "")
    if (
        len(source_git_commit) != 40
        or any(character not in "0123456789abcdef" for character in source_git_commit)
        or identity.get("source_commit") != hashlib.sha256(source_git_commit.encode("ascii")).hexdigest()
    ):
        raise _fail("input_identity_invalid", "source Git commit identity differs")
    if identity.get("sector_display_name_authority") != "canonical_sw_l2_code_only":
        raise _fail("input_identity_invalid", "sector display-name authority differs")
    source_file_hashes = identity.get("source_file_hashes")
    if not isinstance(source_file_hashes, Mapping) or not source_file_hashes:
        raise _fail("input_identity_invalid", "source file hashes are absent")
    for field, value in source_file_hashes.items():
        if (
            not field
            or not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise _fail("input_identity_invalid", "source file hash is invalid", field=field)
    catalog_set = set(codes)
    for section, lower, upper in (
        (daily, calendar[0], DEVELOPMENT_END),
        (sector_returns, DEVELOPMENT_START, DEVELOPMENT_END),
    ):
        for row in section:
            if not isinstance(row, Mapping):
                raise _fail("source_invalid", "input row is not an object")
            row_day = _parse_day(row.get("trade_date"), "source.trade_date")
            if not lower <= row_day <= upper or row_day > DEVELOPMENT_END:
                raise _fail("source_invalid", "source row is outside the development contract")
            if str(row.get("sector_code") or "") not in catalog_set:
                raise _fail("source_invalid", "source row references an unknown L2 sector")
    for row in benchmark:
        if not isinstance(row, Mapping):
            raise _fail("source_invalid", "benchmark row is not an object")
        row_day = _parse_day(row.get("trade_date"), "benchmark.trade_date")
        if not DEVELOPMENT_START <= row_day <= DEVELOPMENT_END:
            raise _fail("source_invalid", "benchmark row is outside the development contract")
    body = {key: value for key, value in bundle.items() if key != "input_hash"}
    expected_hash = canonical_sha256(body)
    if bundle.get("input_hash") != expected_hash:
        raise _fail("input_identity_invalid", "input bundle canonical hash differs")
    return {
        "catalog_codes": tuple(codes),
        "sector_names": names,
        "calendar": calendar,
        "input_hash": expected_hash,
        "identity": dict(identity),
    }


def _average_ranks(values: Mapping[str, float]) -> dict[str, float]:
    ordered = sorted(values.items(), key=lambda item: (item[1], item[0]))
    ranks: dict[str, float] = {}
    index = 0
    while index < len(ordered):
        stop = index + 1
        while stop < len(ordered) and ordered[stop][1] == ordered[index][1]:
            stop += 1
        rank = (index + 1 + stop) / 2.0
        for code, _ in ordered[index:stop]:
            ranks[code] = rank
        index = stop
    return ranks


def _score_and_states(deltas: Mapping[str, float]) -> tuple[dict[str, float], dict[str, str]]:
    count = len(deltas)
    if count < 2:
        raise _fail("cross_section_insufficient", "fewer than two L2 sectors are feature eligible", count=count)
    ranks = _average_ranks(deltas)
    scores = {code: (rank - 1.0) / (count - 1.0) - 0.5 for code, rank in ranks.items()}
    q = min(count // 2, math.ceil(0.20 * count))
    ordered = sorted(scores, key=lambda code: (scores[code], code))
    states = {code: "neutral" for code in ordered}
    for code in ordered[:q]:
        states[code] = "fading"
    for code in ordered[count - q :]:
        states[code] = "trending"
    for score in sorted(set(scores.values())):
        group = [code for code in ordered if scores[code] == score]
        group_states = {states[code] for code in group}
        if len(group_states) > 1:
            for code in group:
                states[code] = "neutral"
    return scores, states


def _daily_index(rows: Sequence[Mapping[str, Any]]) -> dict[tuple[date, str], dict[str, Any]]:
    output: dict[tuple[date, str], dict[str, Any]] = {}
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise _fail("source_invalid", "daily aggregate row is not an object")
        key = (_parse_day(raw.get("trade_date"), "daily_aggregates.trade_date"), str(raw.get("sector_code") or ""))
        if not key[1] or key in output:
            raise _fail("source_invalid", "daily aggregate identity is blank or duplicated", identity=str(key))
        output[key] = dict(raw)
    return output


def build_predictions(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    validated = validate_input_bundle(bundle)
    calendar = validated["calendar"]
    catalog = validated["catalog_codes"]
    names = validated["sector_names"]
    daily = _daily_index(bundle["daily_aggregates"])
    development_days = [value for value in calendar if DEVELOPMENT_START <= value <= DEVELOPMENT_END]
    rows: list[dict[str, Any]] = []
    for trade_date in development_days:
        offset = calendar.index(trade_date)
        as_of_date = calendar[offset - 1]
        source_days = calendar[offset - FEATURE_DAYS : offset]
        deltas: dict[str, float] = {}
        reasons: dict[str, str] = {}
        diagnostics: dict[str, dict[str, Any]] = {}
        for code in catalog:
            source_rows = [daily.get((source_day, code)) for source_day in source_days]
            if any(value is None for value in source_rows):
                raise _fail(
                    "source_invalid",
                    "daily aggregate grid is incomplete",
                    trade_date=trade_date.isoformat(),
                    sector_code=code,
                )
            assert all(value is not None for value in source_rows)
            reason = next((value.get("reason_code") for value in source_rows if not value.get("eligible")), None)
            if reason is not None and (not isinstance(reason, str) or not reason.strip()):
                raise _fail(
                    "source_invalid",
                    "ineligible L2 source row does not contain a typed reason",
                    trade_date=trade_date.isoformat(),
                    sector_code=code,
                )
            if reason:
                reasons[code] = reason
                continue
            net = [_finite(value.get("net_mf_amount_cny"), "net_mf_amount_cny") for value in source_rows]
            amount = [_finite(value.get("amount_cny"), "amount_cny") for value in source_rows]
            recent_amount = math.fsum(amount[5:25])
            lagged_amount = math.fsum(amount[0:20])
            if recent_amount <= 0 or lagged_amount <= 0:
                reasons[code] = "hmm_risk_rotation_l2_source_invalid"
                continue
            deltas[code] = math.fsum(net[5:25]) / recent_amount - math.fsum(net[0:20]) / lagged_amount
            diagnostics[code] = {
                "minimum_expected_contributors": min(int(value["expected_contributors"]) for value in source_rows),
                "minimum_valid_contributors": min(int(value["valid_contributors"]) for value in source_rows),
                "maximum_member_amount_share": max(
                    float(value["maximum_member_amount_share"]) for value in source_rows
                ),
            }
        try:
            scores, states = _score_and_states(deltas)
        except RotationL2Error as exc:
            if exc.reason_code != "hmm_risk_rotation_l2_cross_section_insufficient":
                raise
            scores, states = {}, {}
            reasons.update({code: exc.reason_code for code in catalog if code not in reasons})
        for code in catalog:
            available = code in scores
            rows.append(
                {
                    "trade_date": trade_date.isoformat(),
                    "as_of_date": as_of_date.isoformat(),
                    "sector_level": "L2",
                    "sector_code": code,
                    "sector_name": names[code],
                    "rotation_score": scores.get(code),
                    "forecast_state": states.get(code),
                    "feature_contributions": (
                        {"moneyflow_intensity_delta_5d_rank": scores[code]} if available else None
                    ),
                    "availability": "available" if available else "unavailable",
                    "reason_code": None if available else reasons.get(code, "hmm_risk_rotation_l2_history_unavailable"),
                    "structural_eligible": daily[(as_of_date, code)]["structural_eligible"],
                    "feature_eligible": available,
                    "outcome_status": "PENDING_EVALUATION",
                    "feature_diagnostics": diagnostics.get(code),
                }
            )
    return rows


def _rank_ic(scores: Mapping[str, float], outcomes: Mapping[str, float]) -> float | None:
    codes = sorted(set(scores) & set(outcomes))
    if len(codes) < 2:
        return None
    left = np.asarray([_average_ranks(scores)[code] for code in codes], dtype=np.float64)
    right = np.asarray([_average_ranks(outcomes)[code] for code in codes], dtype=np.float64)
    if float(np.std(left)) == 0.0 or float(np.std(right)) == 0.0:
        return None
    value = float(np.corrcoef(left, right)[0, 1])
    return value if math.isfinite(value) else None


def _newey_west(calendar: Sequence[date], values: Mapping[date, float]) -> dict[str, Any]:
    ordered = [day for day in calendar if day in values]
    if len(ordered) < 2:
        return {"status": "HAC_UNAVAILABLE", "mean": None, "lower": None, "upper": None, "n": len(ordered)}
    mean = math.fsum(values[day] for day in ordered) / len(ordered)
    centered = {day: values[day] - mean for day in ordered}
    positions = {day: index for index, day in enumerate(calendar)}
    variance_numerator = math.fsum(value * value for value in centered.values())
    for lag in range(1, HAC_LAG + 1):
        covariance = math.fsum(
            centered[left] * centered[right]
            for left in ordered
            for right in ordered
            if positions[left] - positions[right] == lag
        )
        variance_numerator += 2.0 * (1.0 - lag / (HAC_LAG + 1.0)) * covariance
    variance = variance_numerator / (len(ordered) ** 2)
    if not math.isfinite(variance) or variance < 0:
        return {"status": "HAC_UNAVAILABLE", "mean": mean, "lower": None, "upper": None, "n": len(ordered)}
    standard_error = math.sqrt(variance)
    return {
        "status": "AVAILABLE",
        "mean": mean,
        "lower": mean - 1.96 * standard_error,
        "upper": mean + 1.96 * standard_error,
        "n": len(ordered),
        "standard_error": standard_error,
    }


def evaluate_predictions(bundle: Mapping[str, Any], predictions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    validated = validate_input_bundle(bundle)
    calendar = validated["calendar"]
    returns: dict[tuple[date, str], dict[str, Any]] = {}
    for raw in bundle["sector_returns"]:
        key = (_parse_day(raw.get("trade_date"), "sector_returns.trade_date"), str(raw.get("sector_code") or ""))
        if not key[1] or key in returns:
            raise _fail("source_invalid", "sector return identity is blank or duplicated", identity=str(key))
        returns[key] = dict(raw)
    benchmark: dict[date, float] = {}
    for raw in bundle["benchmark_close"]:
        day = _parse_day(raw.get("trade_date"), "benchmark_close.trade_date")
        if day in benchmark:
            raise _fail("source_invalid", "benchmark date is duplicated", trade_date=day.isoformat())
        close = _finite(raw.get("close"), "benchmark_close.close")
        if close <= 0:
            raise _fail("source_invalid", "benchmark close is not positive", trade_date=day.isoformat())
        benchmark[day] = close
    by_day: dict[date, list[dict[str, Any]]] = defaultdict(list)
    for raw in predictions:
        row = dict(raw)
        by_day[_parse_day(row["trade_date"], "prediction.trade_date")].append(row)
    daily_ic: dict[date, float] = {}
    daily_spread: dict[date, float] = {}
    metric_days = 0
    mature_days = 0
    outcome_counts: dict[str, int] = defaultdict(int)
    evaluated_rows: list[dict[str, Any]] = []
    for trade_date in sorted(by_day):
        offset = calendar.index(trade_date)
        source_rows = by_day[trade_date]
        eligible = [row for row in source_rows if row["availability"] == "available"]
        scores = {row["sector_code"]: float(row["rotation_score"]) for row in eligible}
        if offset + HORIZON >= len(calendar) or calendar[offset + HORIZON] > DEVELOPMENT_END:
            for row in source_rows:
                row["outcome_status"] = "outcome_not_mature"
                evaluated_rows.append(row)
                outcome_counts[row["outcome_status"]] += 1
            continue
        mature_days += 1
        outcome_days = calendar[offset + 1 : offset + HORIZON + 1]
        market_end = benchmark.get(outcome_days[-1])
        market_start = benchmark.get(trade_date)
        if market_end is None or market_start is None:
            raise _fail("source_invalid", "benchmark outcome path is incomplete", trade_date=trade_date.isoformat())
        market_return = market_end / market_start - 1.0
        outcomes: dict[str, float] = {}
        for row in source_rows:
            if row["availability"] != "available":
                row["outcome_status"] = "prediction_unavailable"
            else:
                daily_returns: list[float] = []
                unavailable = False
                for outcome_day in outcome_days:
                    source = returns.get((outcome_day, row["sector_code"]))
                    if source is None:
                        raise _fail(
                            "source_invalid",
                            "sector outcome grid is incomplete",
                            trade_date=trade_date.isoformat(),
                            outcome_date=outcome_day.isoformat(),
                            sector_code=row["sector_code"],
                        )
                    if source.get("quote_available") is False:
                        unavailable = True
                        break
                    value = _finite(source.get("pct_change"), "sector_returns.pct_change") / 100.0
                    if 1.0 + value <= 0:
                        raise _fail("source_invalid", "sector gross return is not positive")
                    daily_returns.append(value)
                if unavailable:
                    row["outcome_status"] = "outcome_unavailable_quote_discontinued"
                else:
                    sector_return = math.prod(1.0 + value for value in daily_returns) - 1.0
                    relative = sector_return - market_return
                    row["outcome_status"] = "available"
                    row["relative_return_10d"] = relative
                    outcomes[row["sector_code"]] = relative
            outcome_counts[row["outcome_status"]] += 1
            evaluated_rows.append(row)
        required = max(2, math.ceil(COVERAGE_THRESHOLD * len(eligible)))
        if len(outcomes) >= required:
            metric_days += 1
            ic = _rank_ic(scores, outcomes)
            if ic is not None:
                daily_ic[trade_date] = ic
            trending = [
                outcomes[row["sector_code"]]
                for row in eligible
                if row["forecast_state"] == "trending" and row["sector_code"] in outcomes
            ]
            fading = [
                outcomes[row["sector_code"]]
                for row in eligible
                if row["forecast_state"] == "fading" and row["sector_code"] in outcomes
            ]
            if trending and fading:
                daily_spread[trade_date] = math.fsum(trending) / len(trending) - math.fsum(fading) / len(fading)
    coverage_by_day: dict[date, float | None] = {}
    for trade_date, rows in by_day.items():
        structural = sum(bool(row["structural_eligible"]) for row in rows)
        feature = sum(bool(row["feature_eligible"]) for row in rows)
        coverage_by_day[trade_date] = feature / structural if structural else None

    def block_summary(name: str, start: date, end: date) -> dict[str, Any]:
        days = [day for day in by_day if start <= day <= end]
        structural_days = [day for day in days if coverage_by_day[day] is not None]
        coverage_pass_days = [day for day in structural_days if float(coverage_by_day[day]) >= COVERAGE_THRESHOLD]
        ic_values = [daily_ic[day] for day in days if day in daily_ic]
        spread_values = [daily_spread[day] for day in days if day in daily_spread]
        return {
            "name": name,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "decision_day_count": len(days),
            "structural_day_count": len(structural_days),
            "coverage_pass_day_count": len(coverage_pass_days),
            "coverage_pass_day_share": len(coverage_pass_days) / len(structural_days) if structural_days else None,
            "valid_ic_day_count": len(ic_values),
            "mean_daily_rank_ic": math.fsum(ic_values) / len(ic_values) if ic_values else None,
            "mean_daily_spread": math.fsum(spread_values) / len(spread_values) if spread_values else None,
        }

    blocks = [block_summary(name, start, end) for name, start, end in REPORT_BLOCKS]
    overall = block_summary("overall", DEVELOPMENT_START, DEVELOPMENT_END)
    metric_share = len(daily_ic) / mature_days if mature_days else None
    coverage_sufficient = bool(
        overall["coverage_pass_day_share"] is not None
        and overall["coverage_pass_day_share"] >= COVERAGE_THRESHOLD
        and all(
            block["coverage_pass_day_share"] is not None and block["coverage_pass_day_share"] >= COVERAGE_THRESHOLD
            for block in blocks
        )
    )
    evidence_sufficient = bool(metric_share is not None and metric_share >= COVERAGE_THRESHOLD and coverage_sufficient)
    mean_ic = overall["mean_daily_rank_ic"]
    if not any(row["availability"] == "available" for row in evaluated_rows):
        effect = "NO_USABLE_PREDICTIONS"
    elif not evidence_sufficient or mean_ic is None:
        effect = "EVIDENCE_INSUFFICIENT"
    elif mean_ic < BINDING_MBE_RANK_IC:
        effect = "BELOW_BINDING_MBE"
    else:
        effect = "DEVELOPMENT_EFFECT_QUALIFIED"
    return {
        "evaluated_rows": evaluated_rows,
        "metrics": {
            "blocks": blocks,
            "overall": overall,
            "mature_day_count": mature_days,
            "metric_eligible_day_count": metric_days,
            "valid_ic_day_count": len(daily_ic),
            "valid_ic_day_share": metric_share,
            "hac": _newey_west([day for day in calendar if DEVELOPMENT_START <= day <= DEVELOPMENT_END], daily_ic),
            "daily_rank_ic": {day.isoformat(): daily_ic[day] for day in sorted(daily_ic)},
            "daily_spread": {day.isoformat(): daily_spread[day] for day in sorted(daily_spread)},
            "outcome_status_counts": dict(sorted(outcome_counts.items())),
            "coverage_sufficient": coverage_sufficient,
            "evidence_sufficient": evidence_sufficient,
        },
        "effect_status": effect,
    }


def run_process(bundle: Mapping[str, Any], *, process_index: int) -> dict[str, Any]:
    if process_index not in (1, 2):
        raise _fail("score_authority_failed", "fresh process index must be 1 or 2")
    validated = validate_input_bundle(bundle)
    predictions = build_predictions(bundle)
    evaluation = evaluate_predictions(bundle, predictions)
    capability = (
        "RESEARCH_PREDICTION_AVAILABLE_FORWARD_UNCONFIRMED"
        if evaluation["effect_status"] == "DEVELOPMENT_EFFECT_QUALIFIED"
        else "NOT_AVAILABLE"
    )
    payload = {
        "contract_version": CONTRACT_VERSION,
        "model_hash": MODEL_HASH,
        "evaluation_contract_hash": EVALUATION_CONTRACT_HASH,
        "input_hash": validated["input_hash"],
        "mapping_hash": validated["identity"]["mapping_hash"],
        "quote_authority_hash": validated["identity"]["quote_authority_hash"],
        "planned_fits": 0,
        "started_fits": 0,
        "completed_fits": 0,
        "failed_fits": 0,
        "seed": "not_applicable",
        "selection_basis": "RETROSPECTIVE_DEVELOPMENT_SELECTED",
        "predictions": evaluation["evaluated_rows"],
        "metrics": evaluation["metrics"],
        "execution_status": "COMPLETED",
        "effect_status": evaluation["effect_status"],
        "research_surface_status": "NOT_AVAILABLE",
        "rotation_l2_capability_status": capability,
        "forward_power_status": "UNAVAILABLE",
        "forward_confirmation": "NOT_STARTED",
        "advisory_status": "NOT_AVAILABLE",
        "validation_basis": "HISTORICAL_CAUSAL_REPLAY_ZERO_FIT",
        "tail_accessed": False,
    }
    report = {
        "schema_version": PROCESS_SCHEMA,
        "process_index": process_index,
        "reproducibility_payload": payload,
        "reproducibility_payload_sha256": canonical_sha256(payload),
    }
    report["report_sha256"] = canonical_sha256(report)
    return report


def close_processes(
    first: Mapping[str, Any], second: Mapping[str, Any], *, input_bundle: Mapping[str, Any]
) -> dict[str, Any]:
    for expected_index, child in ((1, first), (2, second)):
        body = {key: value for key, value in child.items() if key != "report_sha256"}
        payload = child.get("reproducibility_payload")
        if (
            child.get("schema_version") != PROCESS_SCHEMA
            or child.get("process_index") != expected_index
            or not isinstance(payload, Mapping)
            or child.get("reproducibility_payload_sha256") != canonical_sha256(payload)
            or child.get("report_sha256") != canonical_sha256(body)
        ):
            raise _fail("score_authority_failed", "fresh process receipt is invalid", process_index=expected_index)
    if first["reproducibility_payload_sha256"] != second["reproducibility_payload_sha256"]:
        raise _fail("score_authority_failed", "fresh process payloads differ")
    parent = run_process(input_bundle, process_index=1)["reproducibility_payload"]
    if canonical_sha256(parent) != first["reproducibility_payload_sha256"]:
        raise _fail("score_authority_failed", "parent recomputation differs from child authority")
    run_id = canonical_sha256(
        {
            "model_hash": MODEL_HASH,
            "evaluation_contract_hash": EVALUATION_CONTRACT_HASH,
            "input_hash": parent["input_hash"],
        }
    )
    acceptance = {
        "schema_version": ACCEPTANCE_SCHEMA,
        "run_id": run_id,
        **parent,
        "child_report_sha256": [first["report_sha256"], second["report_sha256"]],
    }
    acceptance["acceptance_sha256"] = canonical_sha256(acceptance)
    return acceptance


def build_consumer_artifact(acceptance: Mapping[str, Any]) -> dict[str, Any]:
    body = {key: value for key, value in acceptance.items() if key != "acceptance_sha256"}
    if acceptance.get("schema_version") != ACCEPTANCE_SCHEMA or acceptance.get("acceptance_sha256") != canonical_sha256(
        body
    ):
        raise _fail("score_authority_failed", "acceptance receipt is invalid")
    rows = [
        {
            "trade_date": row["trade_date"],
            "as_of_date": row["as_of_date"],
            "sector_level": "L2",
            "sector_code": row["sector_code"],
            "score": row["rotation_score"],
            "state": row["forecast_state"],
            "availability": row["availability"],
            "reason_code": row["reason_code"],
            "overlay_applicable": row["availability"] == "available",
        }
        for row in acceptance["predictions"]
    ]
    artifact = {
        "schema_version": CONSUMER_SCHEMA,
        "run_id": acceptance["run_id"],
        "model_hash": acceptance["model_hash"],
        "evaluation_contract_hash": acceptance["evaluation_contract_hash"],
        "input_hash": acceptance["input_hash"],
        "mapping_hash": acceptance["mapping_hash"],
        "quote_authority_hash": acceptance["quote_authority_hash"],
        "horizon_open_days": HORIZON,
        "causal_statement": "decision t uses only source observations through previous open session",
        "rows": rows,
    }
    artifact["content_sha256"] = canonical_sha256(artifact)
    return artifact


__all__ = [
    "ACCEPTANCE_SCHEMA",
    "BINDING_MBE_RANK_IC",
    "CONSUMER_SCHEMA",
    "CONTRACT_VERSION",
    "EVALUATION_CONTRACT_HASH",
    "INPUT_SCHEMA",
    "MODEL_HASH",
    "PROCESS_SCHEMA",
    "RotationL2Error",
    "build_consumer_artifact",
    "build_predictions",
    "close_processes",
    "evaluate_predictions",
    "evaluation_contract",
    "model_contract",
    "run_process",
    "validate_input_bundle",
]
