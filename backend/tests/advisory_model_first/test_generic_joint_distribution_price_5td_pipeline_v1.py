"""Only own joint-stage boundaries and four-arm attribution; no indirect module suites."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_joint_distribution_price_5td_pipeline_v1 import (
    GenericJointPrice5TDPlanV1, evaluate_joint_price_5td_cohorts_v1, implementation_sha256,
    prepare_joint_price_5td_v1, preregister_joint_price_5td_v1, train_joint_price_5td_study_v1,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_joint_distribution_price_5td_model_v1 import joint_fitted_packet as joint_fitted_packet
from backend.tests.advisory_model_first.test_generic_price_5td_pipeline_v1 import source_packet as source_packet
from backend.tests.advisory_model_first.test_generic_minute_price_5td_source_v1 import minute_provider as minute_provider
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_model_v1 import volume_fitted_packet as volume_fitted_packet
from backend.tests.advisory_model_first.test_generic_volume_path_price_5td_pipeline_v1 import prepared_packet as prepared_packet


@pytest.fixture
def joint_prepared_packet(prepared_packet, volume_fitted_packet):
    volume, parent_root, profile, original, prepared = prepared_packet
    control, _, _ = volume_fitted_packet
    manifest = json.loads((prepared/"manifest.json").read_bytes())
    # Small stage-boundary fixture, not evidence of a research fit on these single-row source inputs.
    trained = publish_stage(study_root=prepared.parent, stage="trained", plan_sha256=volume.plan_sha256,
        parent_sha256=manifest["stage_sha256"], artifacts={"model.json": _json_bytes(asdict(control))})
    assert trained.exists()
    plan = GenericJointPrice5TDPlanV1(configuration=volume.configuration,
        volume_plan_ref=evidence_reference_for_file(prepared.parent/"preregistered"/"plan.json", role="volume_plan"),
        volume_prepared_ref=evidence_reference_for_file(prepared/"manifest.json", role="volume_prepared"),
        volume_trained_ref=evidence_reference_for_file(prepared.parent/"trained"/"manifest.json", role="volume_trained"),
        dataset_identity=volume.dataset_identity, decision_dates=volume.decision_dates,
        parent_lineage=(volume.experiment_id,), implementation_sha256=implementation_sha256())
    root = parent_root.parent/"joint_studies"
    preregister_joint_price_5td_v1(plan=plan, output_root=root)
    output = prepare_joint_price_5td_v1(plan=plan, output_root=root)
    return plan, root, profile, prepared, output, original


def test_readonly_parent_prepare_identity_and_qe_busy_without_fit(joint_prepared_packet):
    plan, root, profile, parent, output, _ = joint_prepared_packet
    pd.testing.assert_frame_equal(pd.read_parquet(output/"rows.parquet"), pd.read_parquet(parent/"rows.parquet"))
    receipt = json.loads((output/"receipt.json").read_bytes())
    assert not receipt["parent_refit"] and not receipt["active_profile_required"] and not receipt["database_written"]
    assert len(receipt["decision_dates"]) == 2
    profile.write_text("future profile is not read by this consumer")
    assert prepare_joint_price_5td_v1(plan=plan, output_root=root) == output
    with pytest.raises(ValueError, match="QE idle"):
        train_joint_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=False)
    assert not (output.parent/"fit_attempt.json").exists()
    model = parent.parent/"trained"/"model.json"
    model.write_bytes(model.read_bytes()+b" ")
    with pytest.raises(AdvisoryModelFirstError, match="hash mismatch"):
        prepare_joint_price_5td_v1(plan=plan, output_root=root)


def test_partial_joint_fit_journal_prevents_implicit_second_fit(joint_prepared_packet, monkeypatch):
    plan, root, _, _, output, _ = joint_prepared_packet
    import backend.services.advisory_model_first.generic_joint_distribution_price_5td_pipeline_v1 as module
    def interruption(**kwargs):
        kwargs["before_fit"]("candidate_joint_forest")
        raise ValueError("interrupted")
    monkeypatch.setattr(module, "train_joint_price_5td_v1", interruption)
    with pytest.raises(ValueError, match="interrupted"):
        train_joint_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    journal = [json.loads(v) for v in (output.parent/"fit_journal.jsonl").read_text().splitlines()]
    assert len(journal) == 1 and journal[0]["head"] == "candidate_joint_forest"
    with pytest.raises(FileExistsError):
        train_joint_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)


def test_unknown_model_mass_not_known_avoid_and_frozen_control_attribution(joint_fitted_packet, monkeypatch):
    fitted, control, source, _ = joint_fitted_packet
    rows = source.iloc[:6].copy()
    rows["candidate_group_size"] = 6
    rows["observed_gap_bps"] = 0.
    rows["gross_terminal_ratio"] = [.9, 1.1, 1.2, 1., .95, 9.]
    import backend.services.advisory_model_first.generic_joint_distribution_price_5td_pipeline_v1 as module
    def decisions(**kwargs):
        return pd.DataFrame(dict(status=["AVOID", "AVOID", "UNKNOWN_MODEL_DISTRIBUTION", "ACCEPTABLE", "ACCEPTABLE", "ACCEPTABLE"]),
                            index=kwargs["features"].index)
    monkeypatch.setattr(module, "query_joint_price_nodes_v1", decisions)
    dates = [rows.iloc[0].decision_as_of_trade_date.date(), date(2024, 2, 29)]
    result = evaluate_joint_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    item, attr = result["cohorts"][0], result["rejection_attribution"]
    assert len(result["cohorts"]) == 2 and item["candidate"]["true_take"] == 2
    assert item["candidate"]["known_avoid"] == 2 and item["candidate"]["unknown"] == 1
    assert "000006.SZ" not in item["baseline"]["known_decisions"]
    values = item["settled_returns_bps"]
    assert item["rejection_attribution"]["known_rejection_contribution_bps"] == pytest.approx(
        (-values["000001.SZ"]-values["000002.SZ"])/5)
    assert not attr["unknown_cash_is_model_value"] and result["cumulative_nav"] is None
    rows.loc[0, "label_status"] = "UNKNOWN"
    unsettled = evaluate_joint_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    assert unsettled["cohorts"][0]["baseline"]["net_bps"] is None
    assert unsettled["paired_increments"]["baseline"]["block5_ci95_bps"] is None
