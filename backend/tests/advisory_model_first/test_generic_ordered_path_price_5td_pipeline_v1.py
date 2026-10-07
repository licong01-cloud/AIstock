"""Own stages/atomic retry/unchanged roster and economic attribution, shared fixtures."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _verify_reference, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_ordered_path_price_5td_pipeline_v1 import (
    GenericOrderedPathPrice5TDPlanV1, evaluate_ordered_path_price_5td_cohorts_v1, implementation_sha256,
    preregister_ordered_path_price_5td_v1, prepare_ordered_path_price_5td_v1, train_ordered_path_price_5td_study_v1,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha
from backend.tests.advisory_model_first.test_generic_ordered_path_price_5td_model_v1 import ordered_fitted_packet as ordered_fitted_packet
from backend.tests.advisory_model_first.test_generic_joint_distribution_price_5td_model_v1 import joint_fitted_packet as joint_fitted_packet
from backend.tests.advisory_model_first.test_generic_joint_distribution_price_5td_pipeline_v1 import joint_prepared_packet as joint_prepared_packet
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_model_v1 import volume_fitted_packet as volume_fitted_packet
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_pipeline_v1 import prepared_packet as prepared_packet
from backend.tests.advisory_model_first.test_generic_minute_price_5td_source_v1 import minute_provider as minute_provider
from backend.tests.advisory_model_first.test_generic_price_5td_pipeline_v1 import source_packet as source_packet


@pytest.fixture
def ordered_prepared_packet(joint_prepared_packet, joint_fitted_packet):
    joint, root, profile, _, prepared, _ = joint_prepared_packet
    control, _, _, _ = joint_fitted_packet
    manifest = json.loads((prepared/"manifest.json").read_bytes())
    publish_stage(study_root=prepared.parent, stage="trained", plan_sha256=joint.plan_sha256,
                  parent_sha256=manifest["stage_sha256"], artifacts={"model.json": _json_bytes(asdict(control))})
    volume = json.loads(_verify_reference(joint.volume_plan_ref).read_bytes())
    identity = volume["minute_identity"]
    plan = GenericOrderedPathPrice5TDPlanV1(configuration=joint.configuration,
        joint_plan_ref=evidence_reference_for_file(prepared.parent/"preregistered"/"plan.json", role="joint_plan"),
        joint_prepared_ref=evidence_reference_for_file(prepared/"manifest.json", role="joint_prepared"),
        joint_trained_ref=evidence_reference_for_file(prepared.parent/"trained"/"manifest.json", role="joint_trained"),
        dataset_identity=sha(dict(parent=joint.dataset_identity, ordered_source=identity)), minute_identity=identity,
        active_profile_path=str(profile), parent_lineage=(joint.experiment_id,), decision_dates=joint.decision_dates,
        implementation_sha256=implementation_sha256())
    out_root = root.parent/"ordered_studies"
    preregister_ordered_path_price_5td_v1(plan=plan, output_root=out_root)
    output = prepare_ordered_path_price_5td_v1(plan=plan, output_root=out_root)
    return plan, out_root, profile, prepared, output


def test_source_prepare_original_order_parent_hash_and_qe_partial_no_retry(ordered_prepared_packet, monkeypatch):
    plan, root, profile, parent, output = ordered_prepared_packet
    before, after = pd.read_parquet(parent/"rows.parquet"), pd.read_parquet(output/"rows.parquet")
    pd.testing.assert_frame_equal(after.loc[:, before.columns], before)
    receipt = json.loads((output/"receipt.json").read_bytes())
    assert not receipt["parent_refit"] and receipt["future_price_bars_decoded"] == 0 and not receipt["database_written"]
    assert receipt["frozen_control_arm"] == "candidate_joint_39D"
    profile.write_text("prepare already frozen, no later profile read")
    assert prepare_ordered_path_price_5td_v1(plan=plan, output_root=root) == output
    with pytest.raises(ValueError, match="QE idle"):
        train_ordered_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=False)
    assert not (output.parent/"fit_attempt.json").exists()
    import backend.services.advisory_model_first.generic_ordered_path_price_5td_pipeline_v1 as module
    def interruption(**kwargs):
        kwargs["before_fit"]("candidate_ordered_path_forest")
        raise ValueError("interrupted")
    monkeypatch.setattr(module, "train_ordered_path_price_5td_v1", interruption)
    with pytest.raises(ValueError, match="interrupted"):
        train_ordered_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    assert len((output.parent/"fit_journal.jsonl").read_text().splitlines()) == 1
    with pytest.raises(FileExistsError):
        train_ordered_path_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    model = parent.parent/"trained"/"model.json"
    model.write_bytes(model.read_bytes()+b" ")
    with pytest.raises(AdvisoryModelFirstError, match="hash mismatch"):
        prepare_ordered_path_price_5td_v1(plan=plan, output_root=root)


def test_four_arms_known_unknown_cash_attribution_no_top6_or_date_removal(ordered_fitted_packet, monkeypatch):
    fitted, control, source, _ = ordered_fitted_packet
    rows = source.iloc[:6].copy()
    rows["candidate_group_size"], rows["observed_gap_bps"] = 6, 0.
    rows["gross_terminal_ratio"] = [.9, 1.1, 1.2, 1., .95, 9.]
    import backend.services.advisory_model_first.generic_ordered_path_price_5td_pipeline_v1 as module
    def decisions(**kwargs):
        return pd.DataFrame(dict(status=["AVOID", "AVOID", "UNKNOWN_MODEL_DISTRIBUTION", "ACCEPTABLE", "ACCEPTABLE", "ACCEPTABLE"]), index=kwargs["features"].index)
    monkeypatch.setattr(module, "query_ordered_path_nodes_v1", decisions)
    dates = [rows.iloc[0].decision_as_of_trade_date.date(), date(2024, 2, 29)]
    result = evaluate_ordered_path_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    item = result["cohorts"][0]
    assert len(result["cohorts"]) == 2 and item["candidate"]["true_take"] == 2
    assert item["candidate"]["known_avoid"] == 2 and item["candidate"]["unknown"] == 1
    assert "000006.SZ" not in item["baseline"]["known_decisions"]
    attr = item["rejection_attribution"]
    assert attr["known_rejection_contribution_bps"]+attr["unknown_cash_contribution_bps"] == pytest.approx(item["candidate"]["net_bps"]-item["baseline"]["net_bps"])
    assert result["cumulative_nav"] is None and not result["economic_confirmation"]
    rows.loc[0, "label_status"] = "UNKNOWN"
    missing = evaluate_ordered_path_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    assert missing["cohorts"][0]["baseline"]["net_bps"] is None and missing["paired_increments"]["baseline"]["block5_ci95_bps"] is None
