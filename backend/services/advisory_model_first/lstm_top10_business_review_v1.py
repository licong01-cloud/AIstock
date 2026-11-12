"""Pure display of saved paired valuations; never a recommendation or data source."""
from copy import deepcopy
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

# Frozen evidence identity; deliberately independent of a future shadow.RELEASE.
RESEARCH_IDENTITY = dict(
    schema="lstm_spo_shadow_v1", program_id="advp_ed918504a02f4b15892f0dba65da139b",
    binding_version_id="advb_9673d612b6d14668b4d7545fcee8d277",
    target=dict(package_id="pkg_cc7eccb6202e4816b9d7c7d6b748bd37",
                manifest_sha256="93a7fb7c5c0745c96d2d97376db3c669e527256666ed1e014a710c38799a7dc6",
                run_id="qe_20261007_141342_0221_L4", source_id="qe_20261007_141342_0221"),
    model_sha256="8c915844002eab8b2dde0f8bc91f176ee7768b16ba5089520f106167bdf48813",
    model_payload_sha256="50af0b697ec12e69474c4faa74561c2627df642c62c3ddf7c766ce3001dab12f",
    valuation_policy_sha256="6d54aa5f98c85083ca2de11bd52bfe79c230f9f92b32a51fb9d40ed641d44409",
    fold=3, training_cutoff="2025-07-31", first_decision_date="2026-10-27",
    mode="SHADOW_ONLY", physical_fit_count=0)
RESEARCH_STRESS = dict(
    start_date="2026-03-11", end_date="2026-08-28", original_days=118, mature_days=113,
    complete_pair_days=106, incomplete_mature_days=7, immature_days=5,
    mean_delta_bps=-136.02374797220642, ci95_bps=[-245.23549275982637, -24.895915564910403],
    native_unknown_slots=54, specialist_unknown_slots=53,
    target_status="POINT_ESTIMATE_BELOW_TARGET", historical_pit_proven=False,
    independent_economic_sample=False, actual_fill_proven=False,
    evidence_scope="PREVIOUSLY_CONSUMED_NON_VINTAGE_STRESS_ONLY")
RESEARCH_SOURCE = dict(
    report="docs/analysis/advisory_lstm_frozen_specialist_stress_20261102.md",
    source_pr="https://github.com/licong01-cloud/AIstock/pull/7016",
    producer_commit="1bc7caab4a10a2e2736bf7ce16a105357ee2f73d",
    protocol_sha256="59e4ddf73f67aa8e041d9a01a69027847ae72e52454e51fc69ba7e4ae20aae18",
    evaluated_stage_sha256="2cdac999ce3ab7f16de29cc2bc517ab05d016d4c608b04c38b62dbc6de62deb4",
    report_sha256="38edbb4e50c0d078589373c546e399db87d76dd422c1fea31b144c91ac9a7c06")


def research_assessment(release):
    """Project one already published decision, without opening research/finance files."""
    result = dict(schema="lstm_top10_research_assessment_v1", status="UNKNOWN",
                  reason_code="RESEARCH_RELEASE_IDENTITY_UNVERIFIED", matched_release_sha256=None,
                  model_sha256=None, training_cutoff=None, stress=None, source=None,
                  target_increment_bps=TARGET_BPS, independent_economic_confirmation=False,
                  production_ranking_replaced=False, observation_policy="UNCHANGED", v2_policy="UNCHANGED")
    if not isinstance(release, dict):
        return result
    identity = {key: release.get(key) for key in RESEARCH_IDENTITY}
    if (shadow.spo.sha(identity) != shadow.spo.sha(RESEARCH_IDENTITY)
            or release.get("sha256") != shadow.spo.sha({k: v for k, v in release.items() if k != "sha256"})):
        return result
    result.update(status="ACTIVE_DEVELOPMENT_STOPPED",
                  reason_code="STOP_ADDITIONAL_DEVELOPMENT_THIS_FROZEN_MODEL",
                  matched_release_sha256=release["sha256"], model_sha256=RESEARCH_IDENTITY["model_sha256"],
                  training_cutoff=RESEARCH_IDENTITY["training_cutoff"],
                  stress=deepcopy(RESEARCH_STRESS), source=deepcopy(RESEARCH_SOURCE))
    return result


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
