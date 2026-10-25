"""Own atomic/partial/attribution contracts; shared tiny fixture, no QE/UI suite."""
from dataclasses import asdict
from datetime import date
import json

import pandas as pd
import pytest

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.services.advisory_model_first.generic_price_5td_pipeline_v1 import prepare_generic_price_5td_v1, preregister_generic_price_5td_v1
from backend.services.advisory_model_first.generic_return_volume_price_5td_pipeline_v1 import (
    GenericReturnVolumePrice5TDPlanV1, evaluate_return_volume_price_5td_cohorts_v1, implementation_sha256,
    prepare_return_volume_price_5td_v1, preregister_return_volume_price_5td_v1, train_return_volume_price_5td_study_v1,
)
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.tests.advisory_model_first.test_generic_price_5td_pipeline_v1 import source_packet as source_packet
from backend.tests.advisory_model_first.test_generic_price_5td_models_v1 import fitted_packet as fitted_packet
from backend.tests.advisory_model_first.test_generic_return_volume_price_5td_model_v1 import lag_fitted_packet as lag_fitted_packet


@pytest.fixture
def prepared_packet(source_packet, lag_fitted_packet):
    parent, parent_root = source_packet
    _, control, _, _ = lag_fitted_packet
    preregister_generic_price_5td_v1(plan=parent, output_root=parent_root)
    prepared = prepare_generic_price_5td_v1(plan=parent, output_root=parent_root)
    previous = json.loads((prepared/"manifest.json").read_bytes())
    # Stage boundary fixture, not a research fit on the single-row prepared packet.
    publish_stage(study_root=prepared.parent, stage="trained", plan_sha256=parent.plan_sha256,
        parent_sha256=previous["stage_sha256"], artifacts={"model.json": _json_bytes(asdict(control))})
    plan = GenericReturnVolumePrice5TDPlanV1(configuration=parent.configuration, dataset_identity=parent.dataset_identity,
        decision_dates=parent.decision_dates, parent_lineage=(parent.experiment_id,), implementation_sha256=implementation_sha256(),
        gp5_plan_ref=evidence_reference_for_file(prepared.parent/"preregistered/plan.json", role="gp5_plan"),
        gp5_prepared_ref=evidence_reference_for_file(prepared/"manifest.json", role="gp5_prepared"),
        gp5_trained_ref=evidence_reference_for_file(prepared.parent/"trained/manifest.json", role="gp5_trained"))
    root = parent_root.parent/"lag_studies"
    preregister_return_volume_price_5td_v1(plan=plan, output_root=root)
    output = prepare_return_volume_price_5td_v1(plan=plan, output_root=root)
    return plan, root, output, prepared


def test_readonly_full_parent_population_and_qe_busy_before_attempt(prepared_packet):
    plan, root, output, parent = prepared_packet
    rows, original = pd.read_parquet(output/"rows.parquet"), pd.read_parquet(parent/"rows.parquet")
    pd.testing.assert_frame_equal(rows.loc[:,original.columns], original)
    receipt = json.loads((output/"receipt.json").read_bytes())
    assert not receipt["parent_refit"] and not receipt["database_written"] and len(receipt["decision_dates"]) == 2
    assert prepare_return_volume_price_5td_v1(plan=plan, output_root=root) == output
    with pytest.raises(ValueError, match="QE idle"):
        train_return_volume_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=False)
    assert not (output.parent/"fit_attempt.json").exists()
    model = parent.parent/"trained/model.json"
    model.write_bytes(model.read_bytes()+b" ")
    with pytest.raises(AdvisoryModelFirstError):
        prepare_return_volume_price_5td_v1(plan=plan, output_root=root)


def test_partial_fit_preserves_journal_without_second_attempt(prepared_packet, monkeypatch):
    plan, root, output, _ = prepared_packet
    import backend.services.advisory_model_first.generic_return_volume_price_5td_pipeline_v1 as module
    def interrupted(**kwargs):
        kwargs["before_fit"]("candidate_mean")
        raise ValueError("interrupted")
    monkeypatch.setattr(module, "train_return_volume_price_5td_v1", interrupted)
    with pytest.raises(ValueError, match="interrupted"):
        train_return_volume_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)
    assert len((output.parent/"fit_journal.jsonl").read_text().splitlines()) == 1
    with pytest.raises(FileExistsError):
        train_return_volume_price_5td_study_v1(plan=plan, output_root=root, qe_training_idle=True)


def test_known_rejection_unknown_cash_full_dates_and_no_top6(lag_fitted_packet, monkeypatch):
    fitted, control, source, _ = lag_fitted_packet
    rows = source.iloc[:6].copy().assign(selection_effective_rank=range(1,7), candidate_group_size=6, observed_gap_bps=0.)
    rows["gross_terminal_ratio"] = [.9,1.1,1.2,1.,.95,9.]
    import backend.services.advisory_model_first.generic_return_volume_price_5td_pipeline_v1 as module
    def decisions(**kwargs):
        return pd.DataFrame(dict(status=["AVOID","AVOID","UNKNOWN_INPUT_OR_SUPPORT","ACCEPTABLE","ACCEPTABLE","ACCEPTABLE"]), index=kwargs["features"].index)
    monkeypatch.setattr(module, "query_return_volume_nodes_v1", decisions)
    dates = [rows.iloc[0].decision_as_of_trade_date.date(), date(2024,2,29)]
    result = evaluate_return_volume_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    item = result["cohorts"][0]
    assert len(result["cohorts"]) == 2 and item["candidate"]["true_take"] == 2 and item["candidate"]["known_avoid"] == 2
    assert item["candidate"]["unknown"] == 1 and "000006.SZ" not in item["baseline"]["known_decisions"]
    assert item["rejection_attribution"]["known_rejection_contribution_bps"] == pytest.approx((-item["settled_returns_bps"]["000001.SZ"]-item["settled_returns_bps"]["000002.SZ"])/5)
    assert not result["rejection_attribution"]["unknown_cash_is_model_value"] and result["cumulative_nav"] is None
    rows.loc[0,"label_status"] = "UNKNOWN"
    unknown = evaluate_return_volume_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
    assert unknown["cohorts"][0]["baseline"]["net_bps"] is None and unknown["paired_increments"]["baseline"]["block5_ci95_bps"] is None
    def invalid_state(**kwargs):
        return pd.DataFrame(dict(status=["FAKE_SUCCESS"]*len(kwargs["features"])), index=kwargs["features"].index)
    monkeypatch.setattr(module, "query_return_volume_nodes_v1", invalid_state)
    with pytest.raises(ValueError, match="state is invalid"):
        evaluate_return_volume_price_5td_cohorts_v1(rows=rows, fitted=fitted, frozen_control=control, decision_dates=dates)
