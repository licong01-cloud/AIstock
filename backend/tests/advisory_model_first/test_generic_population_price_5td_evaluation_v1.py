"""Synthetic accounting contracts only; these are not model-profit evidence."""
import numpy as np
import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_population_price_5td_evaluation_v1 import evaluate_population_cohorts_v1
from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import KEY, POLICY, POLICY_SHA256


@pytest.fixture
def cohort():
    calendar = [d.date() for d in pd.bdate_range("2025-06-03", "2025-07-10")]
    end = pd.Timestamp("2025-06-30").date()
    records, days = [], []
    for source in ("anchor", "held"):
        for i, d in enumerate(calendar):
            if d > end:
                break
            days.append(dict(source_id=source, decision_as_of_trade_date=d, candidate_count=6, roster_status="PRESENT"))
            for rank in range(1, 7):
                mature = calendar[i+5] <= end
                records.append(dict(zip(KEY, (d, calendar[i+1], source+str(rank)), strict=True)) | dict(
                    source_id=source, package_id="pkg_"+source, manifest_sha256="a"*64,
                    selection_effective_rank=rank, candidate_group_size=6, label_information_end=calendar[i+5],
                    observed_gap_bps=50., gross_terminal_ratio=(.98 if rank < 6 else 2.) if mature else np.nan,
                    path_min_ratio=.96 if mature else np.nan, label_status="AVAILABLE" if mature else "IMMATURE",
                    policy_sha256=POLICY_SHA256, label_contract=POLICY["label_contract"], csi300_ret_5=-.01))
    rows = pd.DataFrame(records)
    clusters = rows.drop_duplicates(list(KEY))
    predictions = {}
    for arm in ("matched_anchor", "candidate_transfer"):
        p = clusters.loc[:, KEY].copy()
        candidate = arm == "candidate_transfer"
        p["status"] = "AVOID" if candidate else "ACCEPTABLE"
        p["expected_net_bps"] = -100. if candidate else 100.
        p["downside_q90_bps"], p["profit_probability"], p["path_min_ratio_q10"] = 400., .4, .96
        p["policy_sha256"] = POLICY_SHA256
        predictions[arm] = p
    return dict(rows=rows, clusters=clusters, days=pd.DataFrame(days), predictions=predictions, calendar=calendar,
                evaluation_start=calendar[0], evaluation_end=end, anchor_source_id="anchor")


def test_five_original_slots_paired_cost_attribution_and_exploratory_boundary(cohort):
    r = evaluate_population_cohorts_v1(**cohort)
    pair = r["source_summaries"]["anchor"]["paired"]["baseline"]
    expected = -10000*(.98*(1-5.95/10000)/(1.005*(1+.95/10000))-1)
    assert pair["mean_increment_bps"] == pytest.approx(expected)
    assert pair["attribution"]["avoided_loss_bps"] == pytest.approx(expected)
    assert pair["attribution"]["known_action_contribution_bps"] == pytest.approx(expected)
    assert pair["confidence_interval_95_bps"] == pytest.approx([expected, expected])
    assert r["status"] == "EXPLORATORY_POSITIVE_NOT_CONFIRMED" and not r["activation_evidence"]
    assert not r["represents_nav"] and not r["independent_oos_evidence"]
    assert all(x["rank"] <= 5 for x in r["original_top5_episodes"])
    assert r["global_original_day_cluster"]["paired"]["baseline"]["paired_days"] == pair["paired_days"]
    assert r["source_summaries"]["anchor"]["calibration_diagnostic_only"]["candidate_transfer"]["fitting_performed"] is False
    stratified = r["lagged_regime_source_summaries"]["NEGATIVE"]["anchor"]["paired"]["baseline"]
    assert stratified["mean_increment_bps"] == pytest.approx(expected) and stratified["confidence_interval_95_bps"] is None


def test_unknown_cash_different_pair_denominators_and_no_hole_compression(cohort):
    rows = cohort["rows"]
    first = (rows.source_id.eq("anchor") & rows[KEY[0]].eq(cohort["calendar"][2]) & rows.selection_effective_rank.eq(1))
    rows.loc[first, "label_status"] = "UNKNOWN"
    rows.loc[first, ["gross_terminal_ratio", "path_min_ratio"]] = np.nan
    p = cohort["predictions"]["candidate_transfer"]
    unknown = p[KEY[0]].eq(cohort["calendar"][3]) & p.instrument.eq("anchor1")
    p.loc[unknown, "status"] = "UNKNOWN_GAP_SUPPORT"
    p.loc[unknown, ["expected_net_bps", "downside_q90_bps", "profit_probability", "path_min_ratio_q10"]] = np.nan
    r = evaluate_population_cohorts_v1(**cohort)
    summary = r["source_summaries"]["anchor"]
    pair = summary["paired"]["baseline"]
    assert pair["confidence_interval_95_bps"] is None and pair["mde_80_bps"] is None
    parts = pair["attribution"]
    assert parts["unknown_cash_contribution_bps"] > 0
    assert pair["mean_increment_bps"] == pytest.approx(parts["known_action_contribution_bps"]+parts["unknown_cash_contribution_bps"])
    assert summary["arms"]["candidate_transfer"]["complete_day_clusters"] > summary["arms"]["baseline"]["complete_day_clusters"]
    assert summary["arms"]["candidate_transfer"]["unknown_action_slots"] == 1


def test_immature_avoid_and_unknown_roster_not_zero_profit(cohort):
    days = cohort["days"]
    d = cohort["calendar"][4]
    cohort["rows"] = cohort["rows"].loc[~(cohort["rows"].source_id.eq("held") & cohort["rows"][KEY[0]].eq(d))]
    cohort["clusters"] = cohort["rows"].drop_duplicates(list(KEY))
    keys = set(cohort["clusters"][list(KEY)].itertuples(index=False, name=None))
    for arm, p in cohort["predictions"].items():
        cohort["predictions"][arm] = p.loc[[k in keys for k in p[list(KEY)].itertuples(index=False, name=None)]]
    mask = days.source_id.eq("held") & days[KEY[0]].eq(d)
    days.loc[mask, ["candidate_count", "roster_status"]] = [np.nan, "UNKNOWN_ABSENT_FROZEN_DAY"]
    r = evaluate_population_cohorts_v1(**cohort)
    missing = next(g for g in r["cohorts"] if g["source_id"] == "held" and g["decision_date"] == d.isoformat())
    assert all(v is None for v in missing["cohort_net_bps"].values()) and missing["empty_slots"] is None
    assert all(all(v is None for v in g["cohort_net_bps"].values()) for g in r["cohorts"] if not g["horizon_mature"])
    assert r["global_original_day_cluster"]["paired"]["baseline"]["confidence_interval_95_bps"] is None


@pytest.mark.parametrize("fault", ["clock", "policy", "duplicate", "prediction_key", "action"])
def test_identity_clock_or_action_inconsistency_fails_closed(cohort, fault):
    if fault == "clock":
        cohort["rows"].loc[0, "label_information_end"] = cohort["calendar"][6]
    elif fault == "policy":
        cohort["rows"].loc[0, "policy_sha256"] = "b"*64
    elif fault == "duplicate":
        cohort["rows"] = pd.concat([cohort["rows"], cohort["rows"].iloc[[0]]])
    elif fault == "prediction_key":
        cohort["predictions"]["candidate_transfer"] = cohort["predictions"]["candidate_transfer"].iloc[1:]
    else:
        cohort["predictions"]["candidate_transfer"].loc[0, "status"] = "ACCEPTABLE"
    with pytest.raises(ValueError):
        evaluate_population_cohorts_v1(**cohort)
