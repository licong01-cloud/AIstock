"""Published-weight contracts; synthetic lineage is not business evidence."""
import json
from types import SimpleNamespace

import pytest

from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_information_bundle import load_information_research_bundle_v4, _support, _json
from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationScopeV4, EconomicEntryInformationStudyPlanV4
from backend.services.advisory_model_first.economic_entry_information_pipeline import register_information_stage
from backend.services.advisory_model_first.economic_entry_information_training import train_information_entry_v4
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, publish_stage
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2
from backend.services.advisory_model_first.research_control import evidence_reference_for_file
from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
from backend.tests.advisory_model_first.test_economic_entry_information_training import arguments

pytest_plugins=["backend.tests.advisory_model_first.test_economic_entry_model"]


@pytest.fixture(scope="module")
def weights():
    # Module-scoped fixture factory keeps one real fit per test module.
    return {}


def _published(tmp_path,study,weights,metadata_defect=None):
    args,_=arguments(study)
    if "fit" not in weights:
        weights["fit"]=train_information_entry_v4(**args)
    fitted=weights["fit"]
    seed=publish_stage(study_root=tmp_path/"unit_risk",stage="preregistered",plan_sha256="a"*64,parent_sha256=None,
        artifacts={"plan.json":_json_bytes({"synthetic_unit_parent_not_actual_training":True})})
    parent=AlignedEntryStudyPlanV3(v2_plan_ref=evidence_reference_for_file(seed/"plan.json",role="unit_v2"),
        v2_prepared_manifest_ref=evidence_reference_for_file(seed/"manifest.json",role="unit_prepared"),
        training_request=args["request"].parent_request,simulator_sha256="b"*64,parent_lineage=("unit_original","unit_risk"),
        expected_train_rows=53,expected_validation_rows=30)
    oldroot=tmp_path/parent.experiment_id
    oldregistered=publish_stage(study_root=oldroot,stage="preregistered",plan_sha256=parent.plan_sha256,parent_sha256=None,
        artifacts={"plan.json":_json_bytes(parent.model_dump(mode="json"))})
    oldtrained=publish_stage(study_root=oldroot,stage="trained",plan_sha256=parent.plan_sha256,
        parent_sha256=json.loads((oldregistered/"manifest.json").read_text())["stage_sha256"],artifacts={"unit_parent.json":_json_bytes({"fixture":True})})
    info=publish_stage(study_root=tmp_path/"information",stage="prepared",plan_sha256="c"*64,
        parent_sha256=json.loads((oldtrained/"manifest.json").read_text())["stage_sha256"],artifacts={
            "identity.json":_json_bytes({"input_identity":"c"*64,"parent_plan_sha256":parent.plan_sha256,
                "parent_trained_stage_sha256":json.loads((oldtrained/"manifest.json").read_text())["stage_sha256"],
                "information_rows_sha256":args["request"].information_rows_sha256,"implementation_sha256":args["request"].implementation_sha256,
                "models_trained":0,"sealed_accessed":False}),
            "source_receipt.json":_json_bytes({"source_evidence":"CURRENT_DB_HISTORICAL_NON_VINTAGE","outcomes_read":False,
                "new_native_receipt":False,"deployable":False})})
    request=args["request"].model_copy(update={"information_ref":evidence_reference_for_file(info/"manifest.json",role="entry_information_v4_manifest")})
    scope=EconomicEntryInformationScopeV4(parent_scope=EconomicModelScopeV2(package_id="unit",manifest_sha256="d"*64,
        selection_runtime_semantics_hash="e"*64,feature_schema_sha256="f"*64,shadow_policy_sha256="a"*64,
        cost_policy_sha256="b"*64,coordinate_algorithm_sha256="c"*64),training_information_sha256=request.information_rows_sha256)
    plan=EconomicEntryInformationStudyPlanV4(parent_plan_ref=evidence_reference_for_file(oldregistered/"plan.json",role="information_parent_v3_plan"),
        parent_trained_manifest_ref=evidence_reference_for_file(oldtrained/"manifest.json",role="information_parent_v3_trained"),
        training_request=request,model_scope=scope,simulator_sha256=parent.simulator_sha256,parent_lineage=(*parent.parent_lineage,parent.experiment_id),
        expected_train_rows=52,expected_validation_rows=30,development_power={"fixture":"not actual power evidence"})
    root=tmp_path/plan.experiment_id
    registered=publish_stage(study_root=root,stage="preregistered",plan_sha256=plan.plan_sha256,parent_sha256=None,
        artifacts={"plan.json":_json_bytes(plan.model_dump(mode="json"))})
    artifacts={"split_receipt.parquet":_parquet_bytes(fitted.split_receipt)}
    metadata={"request":request.model_dump(mode="json"),"request_sha256":request.request_sha256,"model_scope":scope.model_dump(mode="json"),
        "scope_sha256":scope.scope_sha256,"arms":{}}
    for arm,value in fitted.arms.items():
        metadata["arms"][arm]={"feature_names":value.feature_names,"feature_bounds":value.feature_bounds,
            "price_support":value.price_support,"diagnostics":value.diagnostics}
        for head,model in (("return",value.return_model),("risk",value.risk_model)):
            artifacts[arm.lower()+"_"+head+".txt"]=model.model_to_string().encode()
    if metadata_defect=="dimension":
        metadata["arms"]["INFORMATION_THIRTEEN"]["feature_names"]=metadata["arms"]["MATCHED_NINE"]["feature_names"]
    elif metadata_defect=="bounds":
        metadata["arms"]["INFORMATION_THIRTEEN"]["feature_bounds"]=dict(metadata["arms"]["INFORMATION_THIRTEEN"]["feature_bounds"])
        metadata["arms"]["INFORMATION_THIRTEEN"]["feature_bounds"]["ret_10"]=(False,1.)
    elif metadata_defect=="scope":
        metadata["scope_sha256"]="0"*64
    artifacts["model_metadata.json"]=_json_bytes(metadata)
    trained=publish_stage(study_root=root,stage="trained",plan_sha256=plan.plan_sha256,
        parent_sha256=json.loads((registered/"manifest.json").read_text())["stage_sha256"],artifacts=artifacts)
    evaluated=publish_stage(study_root=root,stage="evaluated",plan_sha256=plan.plan_sha256,
        parent_sha256=json.loads((trained/"manifest.json").read_text())["stage_sha256"],
        artifacts={"evaluation.json":_json_bytes({"fixture":"not economic evidence","plan_sha256":plan.plan_sha256,
            "decision_use":"NAVIGATION_ONLY","deployable":False,"sealed_holdout_accessed":False,"economic_effectiveness":"EXPLORATORY_NOT_CONFIRMED"})})
    owner=SimpleNamespace(dataset_identity="d"*64,policy_identity="e"*64)
    for stage,path,generated,evaluations in (("PREREGISTERED",registered,0,0),("TRAINED",trained,2,0),("EVALUATED",evaluated,2,2)):
        register_information_stage(plan,owner,root,stage,path/"manifest.json",generated=generated,evaluated=evaluations)
    return registered/"plan.json",root


def test_readonly_weights_need_no_current_training_implementation_or_DB(tmp_path,study,weights,monkeypatch):
    path,root=_published(tmp_path,study,weights)
    from backend.services.advisory_model_first import economic_entry_information_pipeline as producer
    monkeypatch.setattr(producer,"_parent",lambda *values:pytest.fail("serving entered training parent chain"))
    first=load_information_research_bundle_v4(plan_path=path)
    second=load_information_research_bundle_v4(plan_path=path)
    assert first.bundle_sha256==second.bundle_sha256 and len(first.fitted.feature_names)==13
    assert first.fitted.arm=="INFORMATION_THIRTEEN" and first.decision_use=="NAVIGATION_ONLY" and not first.deployable
    assert not (root/"fit.lock").exists()


@pytest.mark.parametrize("defect",["weight","registry","path","budget"])
def test_artifact_identity_and_read_budget_fail_closed(tmp_path,study,weights,defect):
    path,root=_published(tmp_path,study,weights)
    if defect=="weight":
        (root/"trained/information_thirteen_return.txt").write_text("corrupt")
    elif defect=="registry":
        (root.parent/"trial_registry.jsonl").write_text("")
    elif defect=="path":
        path=root/"trained/model_metadata.json"
    else:
        manifest=root/"trained/manifest.json"
        payload=json.loads(manifest.read_text())
        payload["files"]["information_thirteen_return.txt"]["size_bytes"]=16777217
        manifest.write_text(json.dumps(payload))
    with pytest.raises((AdvisoryModelFirstError,ValueError)):
        load_information_research_bundle_v4(plan_path=path)


@pytest.mark.parametrize("defect",["dimension","bounds","scope"])
def test_hash_valid_published_metadata_still_requires_strict_family_and_scope(tmp_path,study,weights,defect):
    path,_=_published(tmp_path,study,weights,metadata_defect=defect)
    with pytest.raises(AdvisoryModelFirstError):
        load_information_research_bundle_v4(plan_path=path)


@pytest.mark.parametrize("key,item",[("00",{}),("0",{"observation_count":True}),
    ("0",{"observation_count":30,"decision_day_count":5,"observed_min_gap_bps":0.,"observed_max_gap_bps":100.})])
def test_support_alias_boolean_and_cross_bin_are_rejected(study,key,item):
    with pytest.raises(AdvisoryModelFirstError):
        _support({key:item},study["request"])


@pytest.mark.parametrize("payload",['{"scope":1,"scope":2}','{"scope":NaN}'])
def test_duplicate_and_nonfinite_metadata_fail_before_model_load(tmp_path,payload):
    path=tmp_path/"invalid.json"
    path.write_text(payload)
    with pytest.raises(AdvisoryModelFirstError):
        _json(path)


def test_model_bytes_changing_after_stage_check_are_not_consumed(tmp_path,study,weights,monkeypatch):
    path,root=_published(tmp_path,study,weights)
    from backend.services.advisory_model_first import economic_entry_information_bundle as reader
    original=reader.read_stage
    def changing(target,**kwargs):
        result=original(target,**kwargs)
        if target==root/"trained":
            weight=root/"trained/information_thirteen_return.txt"
            weight.write_bytes(weight.read_bytes()+b"\n")
        return result
    monkeypatch.setattr(reader,"read_stage",changing)
    with pytest.raises(AdvisoryModelFirstError):
        reader.load_information_research_bundle_v4(plan_path=path)
