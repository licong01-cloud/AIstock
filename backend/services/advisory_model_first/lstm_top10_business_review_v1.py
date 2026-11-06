"""Pure display of saved paired valuations; never a recommendation or data source."""
import math

import pandas as pd

from . import lstm_spo_shadow_v1 as shadow

TARGET_BPS = 100.0
CONTRACT = dict(schema="lstm_top10_business_review_v1", role="TOP10_AUXILIARY_OBSERVATION",
                target_increment_bps=TARGET_BPS, target_source="USER_SELECTED_20261101",
                horizon="T_OPEN_TO_T_PLUS4_CLOSE", top_k=10, production_target_count=20,
                valuation_policy_sha256=shadow.RELEASE["valuation_policy_sha256"],
                actual_fill_proven=False, automatic_activation=False,
                production_ranking_replaced=False)


def target_status(value):
    if value is None:
        return "NOT_EVALUATED"
    if not math.isfinite(value):
        raise ValueError("business estimate must be finite")
    return "POINT_ESTIMATE_AT_OR_ABOVE_TARGET" if value >= TARGET_BPS else "POINT_ESTIMATE_BELOW_TARGET"


def roster(prediction):
    rows = shadow.frame(prediction["predictions"])
    if (len(rows) != 50 or rows.duplicated(list(shadow.spo.KEY)).any()
            or any(k not in rows or not rows[k].eq(v).all() for k, v in shadow.TARGET.items())
            or any(rows[k].nunique() != 1 for k in shadow.KEY[:2])):
        raise ValueError("business view must preserve the exact fifty keys")
    for column in ("selection_effective_rank", "advisory_rank"):
        values = pd.to_numeric(rows[column], errors="raise")
        if sorted(values.to_list()) != list(range(1, 51)) or any(isinstance(v, bool) for v in rows[column]):
            raise ValueError("business view requires both original fifty-rank permutations")
    return rows


def review_day(prediction, evaluation=None):
    rows = roster(prediction)
    result = dict(status="IMMATURE_OR_PENDING", native_net_bps=None, specialist_net_bps=None, delta_bps=None,
                  native_unknown_slots=None, specialist_unknown_slots=None)
    finance = None
    if evaluation is not None:
        if evaluation["prediction_sha256"] != prediction["sha256"]:
            raise ValueError("business evaluation parent differs")
        finance = shadow.frame(evaluation["finance"])
        calculated = shadow.paired_result(rows, finance)
        if shadow.spo.sha(calculated) != shadow.spo.sha(evaluation["result"]):
            raise ValueError("business result differs from saved fixed-slot arithmetic")
        result = calculated
        rows = rows.merge(finance[list(shadow.spo.KEY) + ["valuation_status", "hypothetical_liquidation_net_bps"]],
                          on=list(shadow.spo.KEY), validate="one_to_one")
    records, counts = [], dict(ENTERED=0, EXITED=0, RETAINED=0, OUTSIDE=0)
    components = dict(added_profit_bps=None, missed_profit_bps=None, avoided_loss_bps=None, added_loss_bps=None)
    if result["status"] == "COMPLETE":
        components = dict.fromkeys(components, 0.)
    for row in rows.to_dict("records"):
        native, specialist = row["selection_effective_rank"] <= 10, row["advisory_rank"] <= 10
        change = "RETAINED" if native and specialist else "ENTERED" if specialist else "EXITED" if native else "OUTSIDE"
        counts[change] += 1
        value = row.get("hypothetical_liquidation_net_bps")
        known = row.get("valuation_status") in {"AVAILABLE", "CASH_ENTRY_NOT_EXECUTABLE"}
        value = float(value) if known else None
        if value is not None and not math.isfinite(value):
            raise ValueError("known saved valuation must be finite")
        contribution = (int(specialist) - int(native)) * value / 10 if value is not None else None
        if result["status"] == "COMPLETE" and change in {"ENTERED", "EXITED"}:
            name = (("added_profit_bps" if value >= 0 else "added_loss_bps") if specialist
                    else ("missed_profit_bps" if value >= 0 else "avoided_loss_bps"))
            components[name] += abs(value) / 10
        records.append(dict(instrument=row["instrument"], selection_effective_rank=row["selection_effective_rank"],
            advisory_rank=row["advisory_rank"], change=change, valuation_status=row.get("valuation_status", "NOT_EVALUATED"),
            net_bps=value, delta_contribution_bps=contribution))
    if result["status"] == "COMPLETE":
        total = components["added_profit_bps"] - components["missed_profit_bps"] + components["avoided_loss_bps"] - components["added_loss_bps"]
        if not math.isclose(total, result["delta_bps"], abs_tol=1e-8):
            raise ValueError("business components do not reconstruct paired delta")
    cash = {arm + "_cash_slots": None for arm in ("native", "specialist")}
    if finance is not None:
        for arm, rank in (("native", "selection_effective_rank"), ("specialist", "advisory_rank")):
            cash[arm + "_cash_slots"] = int((rows[rank].le(10) & rows.valuation_status.eq("CASH_ENTRY_NOT_EXECUTABLE")).sum())
    return dict(**result, **cash, rows=records, changes=counts, components=components,
                target_increment_bps=TARGET_BPS, target_status=target_status(result["delta_bps"]),
                evidence_scope="SINGLE_SAVED_COHORT_NOT_FORMAL_CONFIRMATION", actual_execution_proven=False)


def summarize(days):
    complete = [day["evaluation"] for day in days if day["evaluation"]["status"] == "COMPLETE"]
    means = {arm + "_mean_net_bps": (sum(r[arm + "_net_bps"] for r in complete) / len(complete) if complete else None)
             for arm in ("native", "specialist")}
    delta = sum(r["delta_bps"] for r in complete) / len(complete) if complete else None
    return dict(**means, mean_delta_bps=delta, complete_pairs=len(complete), captured_days=len(days),
                unknown_days=sum(day["evaluation"]["status"] == "UNKNOWN" for day in days),
                unevaluated_days=sum(day["evaluation"]["status"] not in {"COMPLETE", "UNKNOWN"} for day in days),
                target_increment_bps=TARGET_BPS, target_status=target_status(delta),
                evidence_scope="DESCRIPTIVE_COMPLETE_PAIRS_ONLY", independent_economic_confirmation=False)
