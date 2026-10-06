"""Prepared inputs survive an active-profile update; partial fits never silently restart."""
from datetime import date
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.generic_volume_path_price_5td_contracts_v1 import FEATURES, KEY
from backend.services.advisory_model_first.generic_volume_path_price_5td_pipeline_v1 import (
    GenericVolumePathPrice5TDPlanV1, evaluate_volume_path_price_5td_cohorts_v1, implementation_sha256,
    prepare_volume_path_price_5td_v1, preregister_volume_path_price_5td_v1, train_volume_path_price_5td_study_v1,
)
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import prepare_generic_price_5td_v1, preregister_generic_price_5td_v1
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_price_5td_pipeline_v1 import source_packet as source_packet
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_model_v1 import volume_fitted_packet as volume_fitted_packet
from backend.tests.advisory_model_first.test_generic_minute_price_5td_source_v1 import minute_provider as minute_provider


@pytest.fixture
def prepared_packet(source_packet, minute_provider):
    gp5, parent_root = source_packet
    source_plan = preregister_generic_price_5td_v1(plan=gp5, output_root=parent_root)
    parent = prepare_generic_price_5td_v1(plan=gp5, output_root=parent_root)
    rows = pd.read_parquet(parent/"rows.parquet")
    _, profile, identity, _ = minute_provider(str(rows.iloc[0][KEY[0]].date()))
    plan = GenericVolumePathPrice5TDPlanV1(configuration=gp5.configuration,
        gp5_plan_ref=evidence_reference_for_file(source_plan, role="gp5_plan"),
        gp5_prepared_ref=evidence_reference_for_file(parent/"manifest.json", role="gp5_prepared"),
        minute_identity=identity, active_profile_path=str(profile), dataset_identity=gp5.dataset_identity,
        parent_lineage=(gp5.experiment_id,), decision_dates=gp5.decision_dates, implementation_sha256=implementation_sha256())
    root = parent_root.parent/"minute_studies"
    preregister_volume_path_price_5td_v1(plan=plan, output_root=root)
    prepared = prepare_volume_path_price_5td_v1(plan=plan, output_root=root)
    return plan, root, profile, rows, prepared


def test_immutable_prepare_preserves_label_rows_and_no_later_profile_dependency(prepared_packet):
    plan, root, profile, parent, prepared = prepared_packet
    rows = pd.read_parquet(prepared/"rows.parquet")
    pd.testing.assert_frame_equal(rows.loc[:, parent.columns], parent)
    receipt = json.loads((prepared/"receipt.json").read_bytes())
    assert len(receipt["decision_dates"]) == 2 and receipt["future_price_bars_decoded"] == 0
    assert not receipt["active_profile_required_after_prepare"]
    profile.write_text("a different future profile")
    assert prepare_volume_path_price_5td_v1(plan=plan, output_root=root) == prepared
    with pytest.raises(ValueError, match="QE idle"):
        train_volume_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=False)
    assert not (prepared.parent/"fit_attempt.json").exists()


def test_partial_physical_fit_is_durable_and_cannot_implicitly_restart(prepared_packet, monkeypatch):
    plan, root, _, _, prepared = prepared_packet
    import backend.services.advisory_model_first.generic_volume_path_price_5td_pipeline_v1 as module
    def failure(**kwargs):
        kwargs["before_fit"]("candidate_mean")
        raise ValueError("interrupted")
    monkeypatch.setattr(module, "train_generic_volume_path_price_5td_v1", failure)
    with pytest.raises(ValueError, match="interrupted"):
        train_volume_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    journal = [json.loads(v) for v in (prepared.parent/"fit_journal.jsonl").read_text().splitlines()]
    assert len(journal) == 1 and journal[0]["kind"] == "PHYSICAL_FIT"
    with pytest.raises(FileExistsError):
        train_volume_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)


def test_all_dates_unknown_settlement_and_no_top6_pseudo_NAV(volume_fitted_packet):
    fitted, source, _ = volume_fitted_packet
    rows = source.iloc[:6].copy()
    rows["selection_effective_rank"] = range(1, 7)
    rows["candidate_group_size"] = 6
    rows.loc[0, "label_status"], rows.loc[0, "observed_gap_bps"] = "UNKNOWN", 0.
    result = evaluate_volume_path_price_5td_cohorts_v1(rows=rows, fitted=fitted,
        decision_dates=[rows.iloc[0].decision_as_of_trade_date.date(), date(2024, 2, 29)])
    assert len(result["cohorts"]) == 2 and result["cohorts"][0]["baseline"]["net_bps"] is None
    assert result["cohorts"][0]["baseline"]["unsettled"] == 1 and result["cohorts"][1]["baseline"]["net_bps"] == 0.
    assert result["cohorts"][0]["candidate"]["true_take"] <= 4
    assert result["paired_increments"]["baseline"]["block5_ci95_bps"] is None
    assert result["cumulative_nav"] is None and not result["economic_confirmation"] and not result["deployable"]
    rows.loc[0, list(FEATURES)] = float("nan")
    unknown = evaluate_volume_path_price_5td_cohorts_v1(rows=rows, fitted=fitted,
        decision_dates=[rows.iloc[0].decision_as_of_trade_date.date(), date(2024, 2, 29)])
    assert unknown["paired_increments"]["baseline"]["known_intervention_days"] == 0
    assert unknown["paired_increments"]["baseline"]["unsupported_only_action_difference_days"] == 1
    with pytest.raises(ValueError, match="roster"):
        evaluate_volume_path_price_5td_cohorts_v1(rows=pd.concat([rows, rows.iloc[:1]]), fitted=fitted,
                                           decision_dates=[rows.iloc[0].decision_as_of_trade_date.date()])

def test_avoided_loss_minus_missed_profit_reconciles_and_unknown_separate(volume_fitted_packet, monkeypatch):
    fitted, source, _ = volume_fitted_packet
    rows = source.iloc[:5].copy()
    rows["candidate_group_size"] = 5
    rows["observed_gap_bps"] = 0.
    rows["gross_terminal_ratio"] = [.9, 1.1, 1.2, 1.0, .95]
    import backend.services.advisory_model_first.generic_volume_path_price_5td_pipeline_v1 as module
    def decisions(**kwargs):
        return pd.DataFrame(dict(status=["AVOID", "AVOID", "UNKNOWN_INPUT_OR_SUPPORT", "ACCEPTABLE", "ACCEPTABLE"]),
                            index=kwargs["features"].index)
    monkeypatch.setattr(module, "query_volume_path_nodes_v1", decisions)
    result = evaluate_volume_path_price_5td_cohorts_v1(rows=rows, fitted=fitted,
        decision_dates=[rows.iloc[0].decision_as_of_trade_date.date()])
    item = result["cohorts"][0]
    values, attr = item["settled_returns_bps"], result["rejection_attribution"]
    assert attr["avoided_loss_bps"] == pytest.approx(-values["000001.SZ"]/5)
    assert attr["missed_profit_bps"] == pytest.approx(values["000002.SZ"]/5)
    assert attr["unknown_cash_contribution_bps"] == pytest.approx(-values["000003.SZ"]/5)
    assert item["candidate"]["net_bps"]-item["baseline"]["net_bps"] == pytest.approx(
        attr["known_rejection_contribution_bps"]+attr["unknown_cash_contribution_bps"])
    assert not attr["unknown_cash_is_model_value"]
    assert item["candidate"]["known_avoid"] == 2 and item["candidate"]["unknown"] == 1
