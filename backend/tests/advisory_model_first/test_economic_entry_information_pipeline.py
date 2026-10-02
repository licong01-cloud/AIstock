from types import SimpleNamespace
import json

import pandas as pd
import pytest

from backend.tests.advisory_model_first.test_economic_entry_information_training import arguments
from backend.services.advisory_model_first import economic_entry_information_pipeline as pipeline
from backend.services.advisory_model_first import economic_entry_information_evaluation as evaluation
from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryTrainingConfigurationV1
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError

pytest_plugins = ["backend.tests.advisory_model_first.test_economic_entry_model"]


def test_atomic_two_configuration_registration_fit_readback_and_exact_retry(tmp_path,study,monkeypatch):
    args,_=arguments(study)
    prior=tmp_path/"old_v3"
    parent_file=publish_stage(study_root=prior,stage="preregistered",plan_sha256="a"*64,parent_sha256=None,
        artifacts={"plan.json":_json_bytes({"fixture":"not real study evidence"})})/"plan.json"
    trained_dir=publish_stage(study_root=prior,stage="trained",plan_sha256="a"*64,parent_sha256="b"*64,
        artifacts={"model_metadata.json":_json_bytes({"fixture":True})})
    parent=AlignedEntryStudyPlanV3(v2_plan_ref=evidence_reference_for_file(parent_file,role="unit_v2"),
        v2_prepared_manifest_ref=evidence_reference_for_file(trained_dir/"manifest.json",role="unit_v2_prepared"),
        training_request=args["request"].parent_request,simulator_sha256="c"*64,parent_lineage=("original","risk_v2"),
        expected_train_rows=53,expected_validation_rows=30)
    original=tmp_path/"old_original"
    ranks=study["features"].loc[:,KEY].copy()
    ranks["selection_effective_rank"]=ranks.instrument.str[:6].astype(int)
    ranks["combined_score"]=-ranks.selection_effective_rank.astype(float)
    ranks["is_candidate_decision"]=True
    context=[]
    for decision,target in ranks.loc[:,KEY[:2]].drop_duplicates().itertuples(index=False,name=None):
        context.extend({KEY[0]:decision,KEY[1]:target,"instrument":f"{rank:06d}.SZ",
            "selection_effective_rank":rank,"combined_score":-float(rank),"is_candidate_decision":False} for rank in range(7,41))
    ranks=pd.concat([ranks,pd.DataFrame(context)],ignore_index=True).sort_values([KEY[0],"selection_effective_rank"])
    days=pd.bdate_range(study["request"].train_start,study["request"].label_cutoff)
    quotes=pd.MultiIndex.from_product([days,sorted(ranks.instrument.unique())],names=["trade_date","instrument"]).to_frame(index=False)
    for field in ("open","high","low","close"):
        quotes["raw_"+field+"_cny"]=10.
    quotes["policy_price_per_raw_cny"],quotes["up_limit"],quotes["down_limit"]=1.,20.,1.
    quotes["suspended"],quotes["tradability_unknown"]=False,False
    quotes["source_sha256"]=study["identity"].price_source_sha256
    quotes["price_coordinate_sha256"]=study["identity"].price_coordinate_sha256
    refs=ranks.loc[:,KEY].assign(target_reference_raw_cny=10.,reference_visible_through=ranks[KEY[0]],source_sha256=study["identity"].reference_source_sha256)
    publish_stage(study_root=original,stage="prepared",plan_sha256="d"*64,parent_sha256=None,
        artifacts={"frozen_rankings.parquet":_parquet_bytes(ranks),"identity.json":_json_bytes(study["identity"].model_dump(mode="json")),
            "prices.parquet":_parquet_bytes(quotes),"references.parquet":_parquet_bytes(refs),"calendar.json":_json_bytes([day.isoformat() for day in days])})
    original_plan=SimpleNamespace(dataset_identity="d"*64,policy_identity="e"*64)
    loaded=(None,original_plan,original,None,args["labels"],study["request"],study["features"],None)
    monkeypatch.setattr(pipeline,"_parent",lambda path:(parent,prior,{"stage_sha256":"f"*64},loaded))
    monkeypatch.setattr(pipeline,"_development_power",lambda *values:{"fixture":"exploration, not actual power"})
    info=args["information"].copy()
    info["unknown_reasons"]=[{} for _ in range(len(info))]
    receipt={"source_evidence":"CURRENT_DB_HISTORICAL_NON_VINTAGE","outcomes_read":False,
        "new_native_receipt":False,"deployable":False,"candidate_count":len(info)}
    def load(*,candidates,configuration):
        assert type(configuration) is EconomicEntryTrainingConfigurationV1
        assert configuration.model_dump()==study["request"].model_dump(include=set(EconomicEntryTrainingConfigurationV1.model_fields))
        return info,receipt
    source=SimpleNamespace(load=load)
    output=tmp_path/"new_information"
    reference=pipeline.prepare_information_inputs_v4(parent_plan_path=parent_file,output_root=output,source=source)
    assert pipeline.prepare_information_inputs_v4(parent_plan_path=parent_file,output_root=output,source=source)==reference
    plan=pipeline.build_information_plan_v4(parent_plan_path=parent_file,information_manifest_path=reference)
    plan_path=pipeline.preregister_information_study_v4(plan=plan,output_root=output)
    stage=pipeline.train_information_study_v4(plan_path=plan_path,output_root=output)
    first=pipeline.load_fitted_information_v4(plan_path=plan_path,output_root=output)
    monkeypatch.setattr(pipeline,"train_information_entry_v4",lambda **values:pytest.fail("exact retry refitted"))
    assert pipeline.train_information_study_v4(plan_path=plan_path,output_root=output)==stage
    assert len(first)==2 and first["MATCHED_NINE"].feature_names!=first["INFORMATION_THIRTEEN"].feature_names
    records=AdvisoryResearchTrialRegistryV1(output/"trial_registry.jsonl").read()
    assert len(records)==2 and {item.planned_trial_count for item in records}=={2}
    assert {item.generated_trial_count for item in records}=={0,2}
    evaluated=evaluation.evaluate_information_study_v4(plan_path=plan_path,output_root=output)
    report=json.loads((evaluated/"evaluation.json").read_text(encoding="utf-8"))
    assert set(report["metrics"])=={"baseline","rule","matched_nine","information_thirteen"}
    assert report["top20_rows"]==18 and report["test_decision_days"]==3 and not report["deployable"]
    assert report["shadow_returns_are_not_execution_proof"] and not report["daily_grid_delivery_verified"]
    assert report["information_minus_matched_nine"]["mean_bps"]==0.
    monkeypatch.setattr(evaluation,"replay_shadow_portfolio",lambda **values:pytest.fail("exact retry re-evaluated"))
    assert evaluation.evaluate_information_study_v4(plan_path=plan_path,output_root=output)==evaluated
    assert len(AdvisoryResearchTrialRegistryV1(output/"trial_registry.jsonl").read())==3
    with pytest.raises(AdvisoryModelFirstError):
        pipeline.preregister_information_study_v4(plan=plan.model_copy(update={"development_power":{"fake":True}}),output_root=output)
    with pytest.raises(AdvisoryModelFirstError):
        pipeline.prepare_information_inputs_v4(parent_plan_path=parent_file,output_root=output,
            source=SimpleNamespace(load=lambda **values:(info,{**receipt,"new_native_receipt":True})))
