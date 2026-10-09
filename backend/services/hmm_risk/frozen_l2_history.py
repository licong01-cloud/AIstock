"""Approved P1/P2 frozen-model history dispatch; no training or product activation."""

from __future__ import annotations

from contextlib import ExitStack, contextmanager
from datetime import date
import math
from unittest.mock import patch

from backend.services.hmm_risk import risk_l2 as risk
from backend.services.hmm_risk import risk_l2_value_replay as risk_value
from backend.services.hmm_risk import rotation_l2 as baseline
from backend.services.hmm_risk import rotation_l2_moneyflow_price_supervised as price
from backend.services.hmm_risk import rotation_l2_moneyflow_supervised as ridge
from backend.services.hmm_risk.contracts import ALL_CORE_FEATURES, canonical_sha256
from backend.services.hmm_risk.formal_state_effect import CALENDAR_SHA as CALENDAR_SHA, verify_receipt
from backend.services.hmm_risk.formal_state_model import receipt
from scripts.hmm_risk import rotation_l2_reference_value as value

VERSION = "hmm_risk_frozen_l2_history_v1"
START, END = date(2026, 4, 1), date(2026, 8, 31)
CONTRACT = {
    "version": VERSION,
    "window": [START.isoformat(), END.isoformat()],
    "prediction_days": 104,
    "mature_days": 94,
    "horizon": 10,
    "risk_return_days": 103,
    "independence_status": "HELD_OUT_FROM_CURRENT_CANDIDATE_SELECTION",
    "independence_scope": "CURRENT_THREE_FROZEN_CANDIDATES_NOT_PROJECT_WIDE_UNTOUCHED",
    "fits": 0,
    "refilter_calls": 0,
    "product_activation": False,
}


def require(condition, message):
    if not condition:
        raise risk.fail(message, "frozen_history_invalid")


def schedule(calendar):
    require(calendar == sorted(set(calendar)), "calendar must be sorted unique")
    require(bool(calendar) and calendar[-1] == END.isoformat(), "calendar end differs from frozen history")
    try:
        first, last = calendar.index(START.isoformat()), calendar.index(END.isoformat())
    except ValueError as exc:
        raise risk.fail("approved history boundaries absent", "frozen_history_invalid") from exc
    days = calendar[first : last + 1]
    require(first >= 260 and len(days) == 104 and days[-11] == "2026-08-17", "104/94 date ledger differs")
    require(calendar[first - 25] == "2026-02-25", "rotation warmup differs")
    return {
        "days": days,
        "mature": days[:-10],
        "positions": {d: i for i, d in enumerate(calendar)},
        "as_of": {d: calendar[first + i - 1] for i, d in enumerate(days)},
    }


@contextmanager
def no_training_or_external_actions():
    def forbidden(*args, **kwargs):
        raise risk.fail("fit/filter/database/network is forbidden", "forbidden_action")

    with ExitStack() as stack:
        for target in (
            "sklearn.linear_model.Ridge.fit",
            "sklearn.linear_model.LogisticRegression.fit",
            "backend.services.hmm_risk.formal_state_model.fit_entry",
            "backend.services.hmm_risk.formal_state_model.causal_filter",
            "backend.db.pg_pool.get_conn",
            "psycopg2.connect",
            "socket.create_connection",
            "backend.services.hmm_risk.rotation_l1_input_bundle.load_active_hmm_dataset_identity",
        ):
            stack.enter_context(patch(target, forbidden))
        yield


def infer_rotation(bundle, parameters):
    verify_receipt(bundle)
    require(
        bundle["contract"] == CONTRACT and bundle["schema_version"] == VERSION + "_rotation_features",
        "rotation feature contract differs",
    )
    plan = schedule(bundle["calendar"])
    codes = bundle["catalog"]
    require(len(codes) == 131 and codes == sorted(set(codes)), "131-sector identity differs")
    calendar = [date.fromisoformat(d) for d in bundle["calendar"]]
    first = plan["positions"][plan["days"][0]]
    source_days = bundle["calendar"][first - 25 : -1]
    daily_rows = bundle["daily_aggregates"]
    require(
        len(daily_rows) == len(source_days) * 131
        and {(r["trade_date"], r["sector_code"]) for r in daily_rows} == {(d, c) for d in source_days for c in codes},
        "moneyflow feature grid is duplicated, missing or outside its causal boundary",
    )
    price_rows = bundle["price_features"]["sector_returns"]
    require(
        len(price_rows) == len(source_days) * 131
        and {(r["trade_date"], r["sector_code"]) for r in price_rows} == {(d, c) for d in source_days for c in codes},
        "price feature grid is duplicated, missing or outside its causal boundary",
    )
    require(
        all(
            type(r["quote_available"]) is bool
            and (
                (value._number(r["pct_change"]) and 1 + r["pct_change"] / 100 > 0)
                if r["quote_available"]
                else r["pct_change"] is None
            )
            for r in price_rows
        ),
        "price feature quote domain differs",
    )
    closes = bundle["price_features"]["benchmark_close"]
    close_days = bundle["calendar"][first - 26 : -1]
    require(
        len(closes) == len(close_days)
        and {r["trade_date"] for r in closes} == set(close_days)
        and all(value._number(r["close"]) and r["close"] > 0 for r in closes),
        "feature benchmark boundary/domain differs",
    )
    rows = ridge.moneyflow_rank_rows(
        calendar=calendar,
        catalog=codes,
        names={c: c for c in codes},
        daily_rows=daily_rows,
        decision_days=[date.fromisoformat(d) for d in plan["days"]],
    )
    delta = [{k: v for k, v in r.items() if k != "x"} for r in rows]
    rows = price.add_price_features(rows, calendar, bundle["price_features"])
    predictions = {"delta": delta}
    for arm, api in (("rank", price), ("return", value.return_model)):
        ridge._validate_parameter_identity(parameters[arm], variant=api)
        require(
            parameters[arm]["parameter_sha256"] == value.RANK_PINS["model_parameter_sha256"]
            if arm == "rank"
            else parameters[arm]["parameter_sha256"] == value.RETURN_PINS["model_parameter_sha256"],
            "frozen parameter pin differs",
        )
        predictions[arm] = ridge.linear_predictions_for_rows(rows, parameters[arm], term_names=price.LINEAR_TERMS)
    for arm, grid in predictions.items():
        require(len(grid) == 131 * 104, f"{arm} full prediction grid differs")
        require(
            {(r["trade_date"], r["sector_code"]) for r in grid} == {(d, c) for d in plan["days"] for c in codes},
            "prediction keys differ",
        )
        require(all(r["as_of_date"] == plan["as_of"][r["trade_date"]] for r in grid), "prediction as-of differs")
    return receipt(
        {
            "schema_version": VERSION + "_rotation_sealed",
            "contract": CONTRACT,
            "input_sha256": bundle["receipt_sha256"],
            "predictions": predictions,
            "model_pins": {"rank": value.RANK_PINS, "return": value.RETURN_PINS},
            "fits": 0,
        }
    )


def infer_risk(bundle, parameters):
    verify_receipt(bundle)
    require(
        bundle["contract"] == CONTRACT and bundle["schema_version"] == VERSION + "_risk_features",
        "risk feature contract differs",
    )
    plan, codes = schedule(bundle["calendar"]), bundle["catalog"]
    require(
        len(codes) == 131 and codes == sorted(set(codes)) and bundle["feature_names"] == list(ALL_CORE_FEATURES),
        "risk 131-sector/20D identity differs",
    )
    require(canonical_sha256(parameters) == risk_value.APPROVED_PINS["model_hash"], "risk model/scaler pin differs")
    require(set(bundle["rows"]) == set(plan["days"]), "risk dates differ")
    for day, entries in bundle["rows"].items():
        require(set(entries) == set(codes), "risk feature grid differs")
        for row in entries.values():
            require(row["as_of_date"] == plan["as_of"][day], "risk as-of differs")
            x = row["features"]
            require(
                (isinstance(x, list) and len(x) == 20 and all(value._number(v) for v in x))
                if x is not None
                else isinstance(row.get("reason_code"), str) and bool(row["reason_code"]),
                "risk observation is invalid, not legal NA",
            )
    predictions, changed = risk.predictions_from_parameters(bundle["rows"], plan["days"], codes, parameters)
    return receipt(
        {
            "schema_version": VERSION + "_risk_sealed",
            "contract": CONTRACT,
            "input_sha256": bundle["receipt_sha256"],
            "predictions": predictions,
            "model_sha256": canonical_sha256(parameters),
            "inactive_changed_rows": changed,
            "fits": 0,
        }
    )


def _paired_blocks(days, first, second):
    require([r["trade_date"] for r in first] == days == [r["trade_date"] for r in second], "paired calendar differs")
    groups, current = [], []
    for day, a, b in zip(days, first, second, strict=True):
        if a["daily_return"] is not None and b["daily_return"] is not None:
            current.append((day, a["daily_return"] - b["daily_return"]))
        elif current:
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    blocks = []
    for group in groups:
        dates = [date.fromisoformat(d) for d, _ in group]
        hac = (
            baseline._newey_west(dates, {date.fromisoformat(d): v for d, v in group}, lag=9)
            if len(group) > 9
            else {"status": "NOT_COMPUTABLE", "reason": "BLOCK_TOO_SHORT_FOR_LAG_9"}
        )
        blocks.append({"start": group[0][0], "end": group[-1][0], "dates": len(group), "hac": hac})
    return {
        "full_path_available": sum(len(g) for g in groups) == len(days),
        "paired_dates": sum(len(g) for g in groups),
        "unavailable_dates": len(days) - sum(len(g) for g in groups),
        "blocks": blocks,
        "multiplicity_adjusted": False,
        "promotion_gate": False,
    }


def evaluate_rotation(bundle, sealed, facts):
    verify_receipt(facts)
    verify_receipt(sealed)
    require(
        sealed["input_sha256"] == bundle["receipt_sha256"] and sealed["contract"] == CONTRACT,
        "sealed rotation lineage differs",
    )
    require(
        facts["feature_sha256"] == bundle["receipt_sha256"] and facts["contract"] == CONTRACT,
        "rotation outcome lineage differs",
    )
    plan = schedule(bundle["calendar"])
    predictions = sealed["predictions"]
    grids = {a: {(r["trade_date"], r["sector_code"]): r for r in rows} for a, rows in predictions.items()}
    groups = {a: {} for a in value.ARMS}
    for day in plan["mature"]:
        common = sorted(
            c for c in bundle["catalog"] if all(grids[a][day, c]["availability"] == "available" for a in grids)
        )
        groups["no_order"][day] = common
        for a in grids:
            groups[a][day] = [c for c in common if grids[a][day, c]["forecast_state"] == "trending"]
    require(len(facts["sector_returns"]) == 131 * 104, "outcome quote keys duplicated or absent")
    require(
        all(
            type(r["quote_available"]) is bool
            and (
                (value._number(r["pct_change"]) and 1 + r["pct_change"] / 100 > 0)
                if r["quote_available"]
                else r["pct_change"] is None
            )
            for r in facts["sector_returns"]
        ),
        "quote domain/availability differs",
    )
    quotes = {
        (r["trade_date"], r["sector_code"]): r["pct_change"] / 100 if r["quote_available"] else None
        for r in facts["sector_returns"]
    }
    require(set(quotes) == {(d, c) for d in plan["days"] for c in bundle["catalog"]}, "outcome quote grid differs")
    paths, summaries = {}, {}
    for a in value.ARMS:
        paths[a], summaries[a] = {}, {}
        for bp in value.COSTS:
            path = value.cohort_reference_path(plan["days"], plan["mature"], groups[a], quotes, cost_bps=bp)
            paths[a][str(bp)] = path
            summaries[a][str(bp)] = value._summary(path, initial_nav=1.0)
            summaries[a][str(bp)]["continuous_valued_blocks"] = _reference_blocks(path)
    effects = {
        a: baseline.evaluate_predictions_for_calendar(
            calendar=[date.fromisoformat(d) for d in bundle["calendar"]],
            sector_returns=facts["sector_returns"],
            benchmark_close=facts["benchmark_close"],
            predictions=rows,
            decision_start=START,
            decision_end=END,
            outcome_end=END,
            report_blocks=(("frozen_history", START, END),),
        )
        for a, rows in predictions.items()
    }
    effects = {
        a: {
            "metrics": e["metrics"],
            "original_rule_diagnostic_status": e["effect_status"],
            "original_research_qualification_rewritten": False,
            "promotion_gate": False,
        }
        for a, e in effects.items()
    }
    pairs = [(a, "no_order") for a in ("rank", "return", "delta")] + [("return", "rank"), ("return", "delta")]
    paired = {
        a + "_minus_" + b: {
            str(bp): _paired_blocks(plan["days"], paths[a][str(bp)], paths[b][str(bp)]) for bp in value.COSTS
        }
        for a, b in pairs
    }
    return receipt(
        {
            "schema_version": VERSION + "_rotation_result",
            "contract": CONTRACT,
            "status": "REFERENCE_VALUE_REPLAY_COMPLETED"
            if all(s["status"] == "AVAILABLE" for v in summaries.values() for s in v.values())
            else "INSUFFICIENT_REFERENCE_PATH",
            "summaries": summaries,
            "paired": paired,
            "native_effects": effects,
            "daily_common_population": groups["no_order"],
            "paths": paths,
            "prediction_sha256": sealed["receipt_sha256"],
            "outcome_sha256": facts["receipt_sha256"],
            "net_value_status": "UNASSESSED",
            "forward_confirmed": False,
        }
    )


def evaluate_risk(bundle, sealed, facts):
    verify_receipt(facts)
    verify_receipt(sealed)
    require(
        sealed["input_sha256"] == bundle["receipt_sha256"] and sealed["contract"] == CONTRACT,
        "sealed risk lineage differs",
    )
    require(
        facts["feature_sha256"] == bundle["receipt_sha256"] and facts["contract"] == CONTRACT,
        "risk outcome lineage differs",
    )
    plan, codes = schedule(bundle["calendar"]), bundle["catalog"]
    signals = {d: {} for d in plan["days"]}
    for row in sealed["predictions"]:
        signals[row["trade_date"]][row["sector_code"]] = row
    result = risk_value.replay(codes, plan["days"], signals, facts["returns"])
    result = receipt(
        {
            **{k: v for k, v in result.items() if k != "receipt_sha256"},
            "schema_version": VERSION + "_risk_value",
            "tail_accessed": True,
            "validation_basis": "FROZEN_MODEL_CURRENT_SELECTION_HELD_OUT_HISTORY",
        }
    )
    # Reuse the original target formula; diagnostic only, not a new promotion gate.
    labels = risk.drawdown_outcomes(bundle["calendar"], plan["days"], codes, facts["event_returns"], END.isoformat())
    mature = [
        dict(row, **labels[row["trade_date"]][row["sector_code"]])
        for row in sealed["predictions"]
        if row["trade_date"] in plan["mature"] and row["availability"] == "available"
    ]
    counts = risk._counts(mature)
    targets, eligible, warnings = risk_value._budgets(codes, signals[plan["days"][0]])
    initial = {
        "signal_date": plan["days"][0],
        "held_return": 0.0,
        "eligible_count": len(eligible),
        "warning_count": len(warnings),
        "one_sided_build_turnover": {a: math.fsum(targets[a].values()) for a in risk_value.ARMS},
        "cost_accounting": "RECORDED_AT_INITIAL_SIGNAL_INCLUDED_IN_FIRST_RETURN_ROW_NOT_DOUBLE_CHARGED",
    }
    return receipt(
        {
            "schema_version": VERSION + "_risk_result",
            "contract": CONTRACT,
            "reference_value": result,
            "event_diagnostic": counts,
            "initial_build": initial,
            "event_thresholds_diagnostic_only": {"precision_lift": 0.05, "recall": 0.25},
            "mature_days": 94,
            "right_censored_days": 10,
            "prediction_sha256": sealed["receipt_sha256"],
            "outcome_sha256": facts["receipt_sha256"],
            "new_fits": 0,
            "new_filter_calls": 0,
            "new_fixed_parameter_inference": True,
            "tail_accessed": True,
            "database_access": False,
            "runtime_action": False,
        }
    )


def _reference_blocks(path):
    groups, current = [], []
    for row in path:
        if row["nav"] is not None:
            current.append(row)
        elif current:
            groups.append(current)
            current = []
    if current:
        groups.append(current)
    return [
        {
            "start": g[0]["trade_date"],
            "end": g[-1]["trade_date"],
            "dates": len(g),
            "nav_start": g[0]["nav"],
            "nav_end": g[-1]["nav"],
            "not_stitched_or_full_period": True,
        }
        for g in groups
    ]
