"""One approved two-day warning consumption comparison; never produces model signals."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Mapping

from backend.services.hmm_risk import risk_l2_value_replay as old
from backend.services.hmm_risk.contracts import canonical_sha256
from backend.services.hmm_risk.formal_state_effect import verify_receipt
from backend.services.hmm_risk.formal_state_model import FormalStateError, receipt

VERSION = "hmm_risk_l2_warning_persistence_value_v1"
APPROVED_PINS = {
    **old.APPROVED_PINS,
    "value_hash": "97dacd409b8847c3a3b3d47c45e76b66d5c1f1539f659e2787928d1ae5b54043",
}
CONTRACT = {
    "schema_version": VERSION,
    "confirmation_days": 2,
    "initial_cash_latch": 0,
    "initial_warning_streak": 0,
    "initial_clear_streak": 0,
    "unavailable_policy": "INPUT_UNAVAILABLE_CASH_RESET_STREAKS_KEEP_LATCH",
    "warning_threshold": 0.20,
    "budget_denominator": 131,
    "one_sided_cost_bps": list(old.COST_BPS),
    "cost_basis": "ILLUSTRATIVE_BUDGET_COST_NOT_EXECUTION_NET",
    "net_value_status": "UNASSESSED",
    "forward_confirmed": False,
}
CONTRACT_SHA256 = canonical_sha256(CONTRACT)
NEW_ARMS = ("C", "X_C")
ARMS = (*old.ARMS, *NEW_ARMS)
SOURCES = ("acceptance", "sealed", "features", "facts", "value")
METRICS = ("cumulative_return", "maximum_drawdown", "worst_daily_return", "downside_squared_loss")
require = old.require


def actions(catalog: list[str], days: list[str], signals: Mapping) -> dict:
    """Advance only on the current raw signal, including the final non-return decision."""
    require(len(catalog) == 131 and catalog == sorted(set(catalog)), "131 canonical sectors required")
    require(
        len(days) >= 2 and days == sorted(set(days)) and all(old.original._iso(d) == d for d in days),
        "decision calendar differs",
    )
    require(set(signals) == set(days), "signal dates differ")
    memory = {c: {"cash_latch": 0, "warning_streak": 0, "clear_streak": 0} for c in catalog}
    result = {}
    for index, day in enumerate(days):
        rows = signals[day]
        require(set(rows) == set(catalog), "signal catalog differs")
        old._budgets(catalog, rows)  # Reuse raw signal validation, not the old replay.
        as_of = {r["as_of_date"] for r in rows.values()}
        require(len(as_of) == 1 and old.original._iso(next(iter(as_of))) < day, "signal as-of is not prior")
        if index:
            require(as_of == {days[index - 1]}, "signal as-of differs from prior frozen open date")
        result[day] = {}
        for code in catalog:
            state = memory[code]
            row = rows[code]
            if row["availability"] == "unavailable":
                state["warning_streak"] = state["clear_streak"] = 0
                status = "INPUT_UNAVAILABLE_CASH"
            elif row["warning"]:
                state["warning_streak"] = min(2, state["warning_streak"] + 1)
                state["clear_streak"] = 0
                if state["warning_streak"] == 2:
                    state["cash_latch"] = 1
                status = "CONFIRMED_CASH" if state["cash_latch"] else "ENTER_CONFIRMATION_PENDING"
            else:
                state["clear_streak"] = min(2, state["clear_streak"] + 1)
                state["warning_streak"] = 0
                if state["clear_streak"] == 2:
                    state["cash_latch"] = 0
                status = "EXIT_CONFIRMATION_PENDING" if state["cash_latch"] else "BASE_EXPOSED"
            result[day][code] = {**state, "status": status}
    return result


def validate_baseline(baseline: dict, catalog, days, signals, returns) -> None:
    """Close the pinned old daily readback against original inputs, without replaying it."""
    require(
        baseline["schema_version"] == old.VERSION + "_result"
        and baseline["sector_count"] == 131
        and baseline["planned_return_dates"] == len(days) - 1
        and baseline["return_start"] == days[1]
        and baseline["return_end"] == days[-1]
        and len(baseline["daily"]) == len(days) - 1,
        "old value population differs",
        "identity_mismatch",
    )
    for s, t, row in zip(days[:-1], days[1:], baseline["daily"], strict=True):
        _, eligible, warnings = old._budgets(catalog, signals[s])
        missing = [c for c in eligible if returns[t][c] is None]
        require(
            row["return_date"] == t
            and row["signal_date"] == s
            and row["signal_as_of"] == signals[s][catalog[0]]["as_of_date"]
            and row["catalog_count"] == 131
            and row["eligible_count"] == len(eligible)
            and row["warning_count"] == len(warnings)
            and row["input_unavailable_cash_count"] == 131 - len(eligible)
            and row["missing_held_return_sectors"] == missing
            and set(row["arms"]) == set(old.ARMS),
            "old value dates, availability or arms differ",
            "identity_mismatch",
        )
        require(type(row["paired_return_available"]) is bool, "old paired status invalid")
        for a in old.ARMS:
            arm = row["arms"][a]
            require(
                all(old.finite(arm[k]) for k in ("risk_budget", "cash_budget"))
                and 0 <= arm["risk_budget"] <= 1 + 1e-12
                and math.isclose(arm["risk_budget"] + arm["cash_budget"], 1, abs_tol=1e-12)
                and all(arm[k] is None or old.finite(arm[k]) for k in ("gross_return", "one_sided_risk_turnover"))
                and set(arm["cost_sensitivity_return"]) == {str(v) for v in old.COST_BPS}
                and all(v is None or old.finite(v) for v in arm["cost_sensitivity_return"].values()),
                "old value numeric readback invalid",
                "numeric_invalid",
            )
        require(
            row["paired_return_available"] == all(row["arms"][a]["gross_return"] is not None for a in old.ARMS)
            and (not missing or not row["paired_return_available"]),
            "old paired availability inconsistent",
            "identity_mismatch",
        )
    dates = days[1:]
    for a in old.ARMS:
        readback = old._blocks(
            dates, [r["arms"][a]["gross_return"] if r["paired_return_available"] else None for r in baseline["daily"]]
        )
        require(readback == baseline["gross_blocks"][a], "old block readback differs", "identity_mismatch")


def load_inputs(request_path: Path, expected_hash: str) -> tuple:
    try:
        request, catalog, days, signals, returns = old.load_inputs(
            request_path, expected_hash, request_version=VERSION, approved_pins=APPROVED_PINS
        )
        require(
            canonical_sha256(request.get("contract")) == CONTRACT_SHA256
            and request.get("contract_sha256") == CONTRACT_SHA256,
            "approved persistence policy differs",
            "identity_mismatch",
        )
        value_path = Path(request["value_path"])
        acceptance, stamp = old.original._read(value_path)
        verify_receipt(acceptance, APPROVED_PINS["value_hash"])
        require(
            acceptance["schema_version"] == old.VERSION + "_acceptance"
            and acceptance["execution_status"] == "COMPLETED"
            and acceptance["fresh_process_bitwise_equal"] is True
            and acceptance["numeric_tolerance_used"] is False
            and type(acceptance["planned_fits"]) is int
            and acceptance["planned_fits"] == 0
            and type(acceptance["completed_fits"]) is int
            and acceptance["completed_fits"] == 0
            and all(acceptance[k] is False for k in ("database_access", "tail_accessed", "runtime_action")),
            "old acceptance is not a completed zero-fit result",
            "identity_mismatch",
        )
        baseline = acceptance["result"]
        verify_receipt(baseline)
        require(
            baseline["source_pins"] == old.APPROVED_PINS
            and baseline["source_paths"] == {k: request[k + "_path"] for k in SOURCES[:-1]}
            and all(
                type(baseline[k]) is int and baseline[k] == 0
                for k in ("new_fits", "new_filter_calls", "new_predict_calls")
            )
            and all(
                baseline[k] is False for k in ("database_access", "tail_accessed", "dataset_write", "runtime_action")
            ),
            "old source identity or zero-compute markers differ",
            "identity_mismatch",
        )
        require(set(returns) == set(days[1:]), "return dates differ")
        for values in returns.values():
            require(
                set(values) == set(catalog) and all(v is None or (old.finite(v) and v >= -1) for v in values.values()),
                "return values invalid",
            )
        validate_baseline(baseline, catalog, days, signals, returns)
        risk_path = Path(request["acceptance_path"])
        risk_acceptance, risk_stamp = old.original._read(risk_path)
        verify_receipt(risk_acceptance, APPROVED_PINS["acceptance_hash"])
        labels = risk_acceptance["result"]["predictions"]
        require(
            len(labels) == 131 * len(days)
            and {(r["trade_date"], r["sector_code"]) for r in labels} == {(d, c) for d in days for c in catalog},
            "original label population differs",
            "identity_mismatch",
        )
        old.original._stable(value_path, stamp)
        old.original._stable(risk_path, risk_stamp)
        return request, catalog, days, signals, returns, baseline, labels
    except old.ReplayError:
        raise
    except (
        old.original.RiskL2PredictionError,
        FormalStateError,
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        TypeError,
    ) as exc:
        raise old.ReplayError("input_invalid", "persistence five-source validation failed") from exc


def _risk_tradeoff(action, labels):
    groups = {k: [] for k in ("delayed_event", "delayed_exit_non_event")}
    counts = {
        "mature_available_sector_days": 0,
        "events": 0,
        "raw_event_covered": 0,
        "confirmed_event_covered": 0,
        "raw_non_event_cash_sector_days": 0,
        "confirmed_non_event_cash_sector_days": 0,
    }
    statuses = {}
    for row in labels:
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
        if row["status"] != "AVAILABLE" or row["availability"] != "available":
            continue
        require(
            type(row["event"]) is int
            and row["event"] in (0, 1)
            and old.finite(row["return"])
            and old.finite(row["drawdown"]),
            "mature label invalid",
        )
        q = action[row["trade_date"]][row["sector_code"]]["cash_latch"]
        w, event = row["warning"], row["event"]
        counts["mature_available_sector_days"] += 1
        counts["events"] += event
        counts["raw_event_covered"] += int(w and event)
        counts["confirmed_event_covered"] += int(q and event)
        counts["raw_non_event_cash_sector_days"] += int(w and not event)
        counts["confirmed_non_event_cash_sector_days"] += int(q and not event)
        if w and not q and event:
            groups["delayed_event"].append(row)
        if not w and q and not event:
            groups["delayed_exit_non_event"].append(row)
    return {
        **counts,
        "original_outcome_status_counts": statuses,
        "basis": "OVERLAPPING_10D_SECTOR_DAYS_NOT_INDEPENDENT_INCIDENTS_OR_AVOIDED_LOSS",
        "groups": {
            k: {
                "sector_days": len(rows),
                "return_10d": old._distribution([r["return"] for r in rows]),
                "drawdown_10d": old._distribution([r["drawdown"] for r in rows]),
            }
            for k, rows in groups.items()
        },
        "raw_event_coverage": counts["raw_event_covered"] / counts["events"] if counts["events"] else None,
        "confirmed_event_coverage": counts["confirmed_event_covered"] / counts["events"] if counts["events"] else None,
    }


def compare(catalog, days, signals, returns, baseline, labels):
    action = actions(catalog, days, signals)
    require(set(returns) == set(days[1:]), "return dates differ")
    for values in returns.values():
        require(
            set(values) == set(catalog) and all(v is None or (old.finite(v) and v >= -1) for v in values.values()),
            "return values invalid",
        )
    validate_baseline(baseline, catalog, days, signals, returns)
    previous = {a: {c: 0.0 for c in catalog} for a in NEW_ARMS}
    depleted = {a: False for a in NEW_ARMS}
    daily = []
    for s, t, prior in zip(days[:-1], days[1:], baseline["daily"], strict=True):
        eligible = [c for c in catalog if signals[s][c]["availability"] == "available"]
        held = {c for c in eligible if not action[s][c]["cash_latch"]}
        matched = len(held) / (131 * len(eligible)) if eligible else 0.0
        weights = {
            "C": {c: 1 / 131 if c in held else 0.0 for c in catalog},
            "X_C": {c: matched if c in eligible else 0.0 for c in catalog},
        }
        require(
            math.isclose(math.fsum(weights["C"].values()), math.fsum(weights["X_C"].values()), abs_tol=1e-12),
            "new matched exposure differs",
        )
        arms = dict(prior["arms"])
        for a in NEW_ARMS:
            arms[a], previous[a], depleted[a] = old.arm_day(
                catalog, weights[a], returns[t], previous[a], depleted[a], prior["paired_return_available"]
            )
        paired = prior["paired_return_available"] and all(arms[a]["gross_return"] is not None for a in NEW_ARMS)
        daily.append(
            {
                **{k: v for k, v in prior.items() if k != "arms"},
                "paired_return_available": paired,
                "arms": arms,
                "reason_code": prior["reason_code"]
                or ("REFERENCE_CAPITAL_DEPLETED" if any(depleted.values()) else None),
                "confirmed_cash_count": len(eligible) - len(held),
                "action_status_counts": {
                    status: sum(action[s][c]["status"] == status for c in catalog)
                    for status in sorted({action[s][c]["status"] for c in catalog})
                },
            }
        )
    dates = days[1:]
    blocks = {
        a: old._blocks(dates, [r["arms"][a]["gross_return"] if r["paired_return_available"] else None for r in daily])
        for a in ARMS
    }
    complete = (
        all(r["paired_return_available"] for r in daily)
        and not any(depleted.values())
        and not baseline["reference_capital_depleted"]
    )
    exposure_exists = any(r["eligible_count"] for r in daily)
    full = {a: blocks[a][0] if complete and exposure_exists else None for a in ARMS}
    comparisons = []
    for index in range(len(blocks["C"])):
        group = {a: blocks[a][index] for a in ARMS}
        key = tuple(group["C"][k] for k in ("start", "end", "date_count"))
        require(
            all(tuple(v[k] for k in ("start", "end", "date_count")) == key for v in group.values()),
            "five-arm blocks differ",
        )
        comparisons.append(
            {
                "start": key[0],
                "end": key[1],
                "date_count": key[2],
                "diagnostic_only_not_full_path": not complete,
                **{
                    left + "_minus_" + right: {k: group[left][k] - group[right][k] for k in METRICS}
                    for left, right in (("C", "R"), ("C", "B"), ("C", "X_C"), ("R", "X"))
                },
                "mean_risk_budget": {
                    a: math.fsum(r["arms"][a]["risk_budget"] for r in daily if key[0] <= r["return_date"] <= key[1])
                    / key[2]
                    for a in ARMS
                },
            }
        )

    def known(a, row):
        return row["arms"][a]["one_sided_risk_turnover"] is not None

    paired_turnover = [r for r in daily if known("C", r) and known("R", r)]
    four_turnover = [r for r in daily if all(known(a, r) for a in ("C", "X_C", "R", "X"))]
    difference = (
        math.fsum(
            r["arms"]["C"]["one_sided_risk_turnover"] - r["arms"]["R"]["one_sided_risk_turnover"]
            for r in paired_turnover
        )
        if paired_turnover
        else None
    )
    adjusted = (
        math.fsum(
            r["arms"]["C"]["one_sided_risk_turnover"]
            - r["arms"]["X_C"]["one_sided_risk_turnover"]
            - r["arms"]["R"]["one_sided_risk_turnover"]
            + r["arms"]["X"]["one_sided_risk_turnover"]
            for r in four_turnover
        )
        if four_turnover
        else None
    )
    return receipt(
        {
            "schema_version": VERSION + "_result",
            "execution_status": "COMPLETED",
            "status": "INSUFFICIENT_REFERENCE_PATH"
            if not complete or not exposure_exists
            else "REFERENCE_PATH_COMPLETE",
            "reference_status": "INSUFFICIENT_REFERENCE_PATH"
            if not complete or not exposure_exists
            else "REFERENCE_PATH_COMPLETE",
            "turnover_assessment": "UNASSESSED"
            if difference is None
            else "KNOWN_PAIRED_TURNOVER_LOWER"
            if difference < 0
            else "KNOWN_PAIRED_TURNOVER_NOT_LOWER",
            "contract": CONTRACT,
            "contract_sha256": CONTRACT_SHA256,
            "reference_basis": baseline["reference_basis"],
            "validation_basis": baseline["validation_basis"],
            "arm_roles": {
                "B": "NO_OVERLAY",
                "R": "ORIGINAL_IMMEDIATE_CASH",
                "X": "ORIGINAL_X_R_MATCHED_EXPOSURE",
                "C": "TWO_DAY_CONFIRMED_CASH",
                "X_C": "CONFIRMED_MATCHED_EXPOSURE",
            },
            "net_value_status": "UNASSESSED",
            "forward_confirmed": False,
            "sector_count": 131,
            "decision_dates": len(days),
            "planned_return_dates": len(dates),
            "return_start": dates[0],
            "return_end": dates[-1],
            "paired_return_dates": sum(r["paired_return_available"] for r in daily),
            "action_sha256": canonical_sha256(action),
            "action_count": 131 * len(days),
            "population_sha256": canonical_sha256({"catalog": catalog, "decision_dates": days}),
            "complete_reference_path": complete,
            "reference_capital_depleted": any(depleted.values()) or baseline["reference_capital_depleted"],
            "legal_na_dates": [r["return_date"] for r in daily if r["missing_held_return_sectors"]],
            "gross_full_path": full,
            "gross_blocks": blocks,
            "paired_block_comparisons": comparisons,
            "turnover_comparison": {
                "common_dates": [r["return_date"] for r in paired_turnover],
                "date_count": len(paired_turnover),
                "C_minus_R": difference,
                "exposure_adjusted_common_dates": [r["return_date"] for r in four_turnover],
                "exposure_adjusted_difference": adjusted,
            },
            "exposure": {
                a: {
                    "mean_risk_budget": math.fsum(r["arms"][a]["risk_budget"] for r in daily) / len(daily),
                    "final_risk_budget": daily[-1]["arms"][a]["risk_budget"],
                    "initial_turnover": daily[0]["arms"][a]["one_sided_risk_turnover"],
                    "known_turnover_sum": math.fsum(
                        r["arms"][a]["one_sided_risk_turnover"] for r in daily if known(a, r)
                    ),
                    "unknown_turnover_dates": [r["return_date"] for r in daily if not known(a, r)],
                }
                for a in ARMS
            },
            "opportunity_cost": {
                "basis": "SIGNED_PAIRED_BLOCK_C_MINUS_B_AND_C_MINUS_R_NOT_EXECUTION_PNL",
                "positive_return_budget_foregone_C": math.fsum(
                    (1 / 131) * max(0, returns[t][c])
                    for s, t in zip(days[:-1], days[1:], strict=True)
                    for c in catalog
                    if signals[s][c]["availability"] == "available"
                    and action[s][c]["cash_latch"]
                    and returns[t][c] is not None
                ),
            },
            "risk_opportunity_tradeoff": _risk_tradeoff(action, labels),
            "cost_sensitivity": {
                "basis": CONTRACT["cost_basis"],
                "one_sided_cost_bps": list(old.COST_BPS),
                "paths": {
                    a: {
                        str(bp): old._blocks(
                            dates,
                            [
                                r["arms"][a]["cost_sensitivity_return"][str(bp)]
                                if r["paired_return_available"]
                                else None
                                for r in daily
                            ],
                        )
                        for bp in old.COST_BPS
                    }
                    for a in ARMS
                },
                "break_even": {"status": "NOT_COMPUTABLE", "reason": "REAL_EXECUTION_COST_AND_BREAK_EVEN_NOT_MODELLED"},
            },
            "daily": daily,
            "new_fits": 0,
            "new_filter_calls": 0,
            "new_predict_calls": 0,
            "database_access": False,
            "tail_accessed": False,
            "dataset_write": False,
            "runtime_action": False,
        }
    )


def execute(request_path: Path, expected_hash: str) -> dict:
    with old.zero_compute():
        request, catalog, days, signals, returns, baseline, labels = load_inputs(request_path, expected_hash)
        result = compare(catalog, days, signals, returns, baseline, labels)
        return receipt(
            {
                **{k: v for k, v in result.items() if k != "receipt_sha256"},
                "request_sha256": expected_hash,
                "source_pins": APPROVED_PINS,
                "source_paths": {k: request[k + "_path"] for k in SOURCES},
                "zero_compute_poison_active": True,
            }
        )


def validate_child(child, request_path, expected_hash):
    """Parent closes each child independently against five pinned authorities; no replay."""
    with old.zero_compute():
        request, catalog, days, _, _, baseline, _ = load_inputs(request_path, expected_hash)
    verify_receipt(child)
    require(
        child.get("schema_version") == VERSION + "_result"
        and child.get("execution_status") == "COMPLETED"
        and child.get("source_pins") == APPROVED_PINS
        and child.get("source_paths") == {k: request[k + "_path"] for k in SOURCES}
        and child.get("request_sha256") == expected_hash
        and canonical_sha256(child.get("contract")) == CONTRACT_SHA256
        and child.get("contract_sha256") == CONTRACT_SHA256
        and child.get("population_sha256") == canonical_sha256({"catalog": catalog, "decision_dates": days})
        and child.get("sector_count") == 131
        and child.get("decision_dates") == len(days)
        and child.get("action_count") == 131 * len(days)
        and old.original._sha(child.get("action_sha256"))
        and child.get("planned_return_dates") == len(days) - 1
        and [r["return_date"] for r in child.get("daily", [])] == days[1:]
        and child.get("status") == child.get("reference_status")
        and child.get("status") in {"REFERENCE_PATH_COMPLETE", "INSUFFICIENT_REFERENCE_PATH"}
        and child.get("forward_confirmed") is False
        and child.get("net_value_status") == "UNASSESSED"
        and all(
            type(child.get(k)) is int and child[k] == 0 for k in ("new_fits", "new_filter_calls", "new_predict_calls")
        )
        and all(child.get(k) is False for k in ("database_access", "tail_accessed", "dataset_write", "runtime_action"))
        and child.get("zero_compute_poison_active") is True,
        "child differs from independent five-source policy/population authority",
        "identity_mismatch",
    )
    for row, prior in zip(child["daily"], baseline["daily"], strict=True):
        require(
            all(
                row.get(k) == prior[k]
                for k in (
                    "signal_date",
                    "signal_as_of",
                    "catalog_count",
                    "eligible_count",
                    "warning_count",
                    "input_unavailable_cash_count",
                    "missing_held_return_sectors",
                )
            )
            and all(row["arms"].get(a) == prior["arms"][a] for a in old.ARMS)
            and type(row.get("confirmed_cash_count")) is int
            and 0 <= row["confirmed_cash_count"] <= prior["eligible_count"]
            and type(row.get("paired_return_available")) is bool
            and (not row["paired_return_available"] or prior["paired_return_available"])
            and set(row["arms"]) == set(ARMS),
            "child old-arm readback or causal population drifted",
            "identity_mismatch",
        )
        for a in NEW_ARMS:
            arm = row["arms"][a]
            require(
                all(old.finite(arm[k]) for k in ("risk_budget", "cash_budget"))
                and 0 <= arm["risk_budget"] <= 1 + 1e-12
                and math.isclose(arm["risk_budget"] + arm["cash_budget"], 1, abs_tol=1e-12)
                and all(arm[k] is None or old.finite(arm[k]) for k in ("gross_return", "one_sided_risk_turnover"))
                and set(arm["cost_sensitivity_return"]) == {str(v) for v in old.COST_BPS}
                and all(v is None or old.finite(v) for v in arm["cost_sensitivity_return"].values()),
                "child new-arm values invalid",
                "numeric_invalid",
            )
        require(
            math.isclose(row["arms"]["C"]["risk_budget"], row["arms"]["X_C"]["risk_budget"], abs_tol=1e-12),
            "child matched exposure differs",
            "identity_mismatch",
        )
