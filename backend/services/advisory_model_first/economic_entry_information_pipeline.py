"""Independent immutable information study; old models and QE stay untouched."""
import json
from pathlib import Path

import pandas as pd
import numpy as np

from backend.services.advisory_model_first.economic_entry_aligned_pipeline import _sources, load_aligned_study_v3, verify_aligned_stage_ledger
from backend.services.advisory_model_first.economic_entry_information_contracts import (
    EconomicEntryInformationScopeV4, EconomicEntryInformationStudyPlanV4, EconomicEntryInformationTrainingRequestV4,
)
from backend.services.advisory_model_first.economic_entry_contracts import (
    ECONOMIC_FEATURE_NAMES, EconomicEntryInputIdentityV1, EconomicEntryTrainingConfigurationV1,
)
from backend.services.advisory_model_first.economic_risk_alignment_contracts import EconomicModelScopeV2
from backend.services.advisory_model_first.economic_entry_information_source import EconomicEntryReadonlyInformationSourceV4
from backend.services.advisory_model_first.economic_entry_information_training import (
    EconomicInformationArmV4, assemble_information_rows_v4, information_common_fit_rows_sha256,
    information_rows_sha256, train_information_entry_v4,
)
from backend.services.advisory_model_first.economic_entry_labels import KEY, _fail
from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, _parquet_bytes, _verify_reference, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, _exclusive_file_lock, evidence_reference_for_file
from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


def information_implementation_sha256():
    paths=[Path(__file__).with_name(f"economic_entry_information_{name}.py") for name in
           ("contracts","source","training","inference","pipeline","evaluation")]
    paths.append(Path(__file__).with_name("economic_daily_information_v1.py"))
    return sha({path.name:file_sha256(path) for path in paths})


def _root(output_root):
    declared=Path(output_root)
    root=declared.resolve()
    if not declared.is_absolute() or root!=declared.absolute() or root.drive.upper()=="C:":
        _fail("information study needs an original absolute non-C durable root")
    return root


def _parent(plan_path):
    path=Path(plan_path)
    parent,root,registered=load_aligned_study_v3(plan_path=path,output_root=path.parent.parent.parent)
    trained=read_stage(root/"trained",stage="trained",plan_sha256=parent.plan_sha256,parent_sha256=registered["stage_sha256"])
    verify_aligned_stage_ledger(parent,root,"TRAINED",root/"trained/manifest.json")
    # Existing chain validates frozen dataset/window authorization before labels.
    loaded=_sources(parent.v2_plan_ref,parent.v2_prepared_manifest_ref)
    if parent.training_request.source_request!=loaded[5]:
        _fail("information parent request differs from frozen original")
    return parent,root,trained,loaded


def prepare_information_inputs_v4(*, parent_plan_path, output_root, source=None):
    parent,_,trained,loaded=_parent(parent_plan_path)
    parent_root=loaded[2]
    ranks=pd.read_parquet(parent_root/"prepared/frozen_rankings.parquet")
    configuration=parent.training_request.source_request
    roster=ranks.loc[ranks.is_candidate_decision & ranks.selection_effective_rank.le(20)
        & ranks[KEY[0]].between(pd.Timestamp(configuration.train_start),pd.Timestamp(configuration.test_end)),
        KEY+["selection_effective_rank"]].sort_values([KEY[0],"selection_effective_rank"])
    source_configuration=EconomicEntryTrainingConfigurationV1.model_validate(
        configuration.model_dump(include=set(EconomicEntryTrainingConfigurationV1.model_fields)))
    info,receipt=(source or EconomicEntryReadonlyInformationSourceV4()).load(candidates=roster,configuration=source_configuration)
    if (receipt.get("source_evidence")!="CURRENT_DB_HISTORICAL_NON_VINTAGE" or receipt.get("outcomes_read") is not False
            or receipt.get("new_native_receipt") is not False or receipt.get("deployable") is not False
            or receipt.get("candidate_count")!=len(roster)):
        _fail("information preparation cannot upgrade source evidence or change roster")
    rows_hash=information_rows_sha256(info)
    input_id=sha({"parent_plan":parent.plan_sha256,"source":receipt,"rows":rows_hash,
                  "implementation":information_implementation_sha256()})
    root=_root(output_root)/"inputs"/f"advinfoinput_{input_id[:24]}"
    stored=info.copy()
    stored["unknown_reasons"]=stored.unknown_reasons.map(lambda value:json.dumps(value,sort_keys=True))
    published=publish_stage(study_root=root,stage="prepared",plan_sha256=input_id,parent_sha256=trained["stage_sha256"],
        artifacts={"information.parquet":_parquet_bytes(stored),"source_receipt.json":_json_bytes(receipt),
            "identity.json":_json_bytes({"input_identity":input_id,"parent_plan_sha256":parent.plan_sha256,
                "parent_trained_stage_sha256":trained["stage_sha256"],"information_rows_sha256":rows_hash,
                "implementation_sha256":information_implementation_sha256(),"models_trained":0,"sealed_accessed":False})})
    return published/"manifest.json"


def _information(reference,parent,trained):
    path=_verify_reference(reference)
    if reference.role!="entry_information_v4_manifest" or path.name!="manifest.json" or path.parent.name!="prepared":
        _fail("information manifest role/path differs")
    identity=json.loads((path.parent/"identity.json").read_text(encoding="utf-8"))
    manifest=read_stage(path.parent,stage="prepared",plan_sha256=identity["input_identity"],parent_sha256=trained["stage_sha256"])
    if (identity["parent_plan_sha256"]!=parent.plan_sha256
            or identity["implementation_sha256"]!=information_implementation_sha256()
            or identity["parent_trained_stage_sha256"]!=trained["stage_sha256"]):
        _fail("information source parent/implementation differs")
    frame=pd.read_parquet(path.parent/"information.parquet")
    if information_rows_sha256(frame)!=identity["information_rows_sha256"]:
        _fail("information row content differs from its verified stage")
    return frame,identity,manifest


def build_information_plan_v4(*, parent_plan_path, information_manifest_path):
    parent,root,trained,loaded=_parent(parent_plan_path)
    info_ref=evidence_reference_for_file(information_manifest_path,role="entry_information_v4_manifest")
    information,identity,_=_information(info_ref,parent,trained)
    rows=assemble_information_rows_v4(features=loaded[6],labels=loaded[4],information=information,parent_request=parent.training_request)
    request=EconomicEntryInformationTrainingRequestV4(parent_request=parent.training_request,information_ref=info_ref,
        information_rows_sha256=identity["information_rows_sha256"],common_fit_rows_sha256=information_common_fit_rows_sha256(rows),
        implementation_sha256=information_implementation_sha256())
    return EconomicEntryInformationStudyPlanV4(parent_plan_ref=evidence_reference_for_file(root/"preregistered/plan.json",role="information_parent_v3_plan"),
        parent_trained_manifest_ref=evidence_reference_for_file(root/"trained/manifest.json",role="information_parent_v3_trained"),
        training_request=request,simulator_sha256=parent.simulator_sha256,parent_lineage=(*parent.parent_lineage,parent.experiment_id,"fixed_D_price_volume_information_v4"),
        expected_train_rows=int((rows.split.eq("train") & rows.information_training_eligible).sum()),
        expected_validation_rows=int((rows.split.eq("validation") & rows.information_training_eligible).sum()),model_scope=_scope(loaded,request),
        development_power=_development_power(parent,root,trained))


def _scope(loaded,request):
    identity=EconomicEntryInputIdentityV1.model_validate_json((loaded[2]/"prepared/identity.json").read_text(encoding="utf-8"))
    if identity.identity_sha256!=request.parent_request.source_request.input_identity_sha256:
        _fail("information model scope original input identity differs")
    base=EconomicModelScopeV2(package_id=identity.package_id,manifest_sha256=identity.manifest_sha256,
        selection_runtime_semantics_hash=identity.selection_runtime_semantics_hash,
        feature_schema_sha256=sha({"schema_version":"economic_feature_column_contract_v1","names":list(ECONOMIC_FEATURE_NAMES)}),
        shadow_policy_sha256=identity.shadow_policy_sha256,cost_policy_sha256=identity.cost_policy_sha256,
        coordinate_algorithm_sha256=sha({"raw_price_basis":"raw_cny","target_reference":"D_visible_provider_compatible_v2",
            "research_training_coordinate":"constant_per_symbol_times_db_adj_factor_all_frozen_endpoints"}),
        universe_definition_evidence="UNPROVEN")
    return EconomicEntryInformationScopeV4(parent_scope=base,training_information_sha256=request.information_rows_sha256)


def _development_power(parent,root,trained):
    stage=read_stage(root/"evaluated",stage="evaluated",plan_sha256=parent.plan_sha256,parent_sha256=trained["stage_sha256"])
    verify_aligned_stage_ledger(parent,root,"EVALUATED",root/"evaluated/manifest.json")
    frame=pd.read_parquet(root/"evaluated/matched_daily.parquet",columns=["model_minus_baseline_bps"])
    values=frame.model_minus_baseline_bps.to_numpy(dtype=float)
    if len(values)<5 or not np.isfinite(values).all():
        _fail("information planning needs finite parent development block noise")
    random=np.random.default_rng(20261002)
    means=[]
    for _ in range(2000):
        starts=random.integers(0,len(values)-5+1,size=int(np.ceil(len(values)/5)))
        means.append(float(np.concatenate([values[start:start+5] for start in starts])[:len(values)].mean()))
    noise=float(np.std(means,ddof=1))
    return {"schema_version":"information_development_power_v4","source_stage_sha256":stage["stage_sha256"],
        "source_file_sha256":file_sha256(root/"evaluated/matched_daily.parquet"),"parent_common_days":len(values),
        "block_days":5,"repetitions":2000,"seed":20261002,"bootstrap_mean_noise_bps":noise,
        "normal_approx_MDE_95pct_80power_bps":float((1.959963984540054+0.8416212335729143)*noise),
        "assumption":"parent_paired_development_noise_proxy_not_new_model_power_guarantee",
        "confirmation_effect_target_registered":False,"actual_new_intervention_support":"NOT_YET_OBSERVED",
        "classification":"EXPLORATORY_NAVIGATION_ONLY_NO_ACTIVATION"}


def _inputs(plan):
    parent,root,trained,loaded=_parent(_verify_reference(plan.parent_plan_ref))
    if (_verify_reference(plan.parent_trained_manifest_ref)!=root/"trained/manifest.json"
            or plan.training_request.parent_request!=parent.training_request
            or plan.training_request.implementation_sha256!=information_implementation_sha256()
            or plan.simulator_sha256!=parent.simulator_sha256 or parent.experiment_id not in plan.parent_lineage):
        _fail("information frozen study identity differs")
    info,identity,_=_information(plan.training_request.information_ref,parent,trained)
    if identity["information_rows_sha256"]!=plan.training_request.information_rows_sha256:
        _fail("information request source differs")
    rows=assemble_information_rows_v4(features=loaded[6],labels=loaded[4],information=info,parent_request=parent.training_request)
    if (information_common_fit_rows_sha256(rows)!=plan.training_request.common_fit_rows_sha256
            or tuple(int((rows.split.eq(split)&rows.information_training_eligible).sum()) for split in ("train","validation"))
               !=(plan.expected_train_rows,plan.expected_validation_rows)):
        _fail("information preregistered common supervision differs")
    if plan.development_power!=_development_power(parent,root,trained) or plan.model_scope!=_scope(loaded,plan.training_request):
        _fail("information preregistered development power source/algorithm differs")
    return parent,loaded,info


def register_information_stage(plan,parent,root,stage,manifest,*,generated=0,evaluated=0):
    configuration=plan.training_request.parent_request.source_request
    record=build_trial_record(experiment_id=plan.experiment_id,attempt_id="exact_attempt_v4",research_stage=stage,
        study_type=plan.study_type,hypothesis_family_id="economic_actual_open_entry_value_v1",parent_lineage=plan.parent_lineage,
        unique_variable="four_fixed_D_information_fields_with_matched_common_eligible_control",objective_contract=plan.objective_contract,
        dataset_identity=parent.dataset_identity,schema_identity=plan.schema_version,policy_identity=parent.policy_identity,
        planned_trial_count=2,generated_trial_count=generated,evaluated_trial_count=evaluated,selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="P0C_DEVELOPMENT_V1",dataset_identity=parent.dataset_identity,
            start_date=configuration.train_start,end_date=configuration.label_cutoff),),
        result_class="CONTROL_READY" if generated==0 else "EXPLORATORY",decision_use=plan.decision_use,
        evidence_refs=(evidence_reference_for_file(manifest,role=f"information_{stage.lower()}"),))
    return AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _ledger(plan,root,stage):
    matching=[value for value in AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").read()
        if value.experiment_id==plan.experiment_id and value.research_stage==stage]
    if len(matching)!=1 or len(matching[0].evidence_refs)!=1 or matching[0].evidence_refs[0].sha256!=file_sha256(root/stage.lower()/"manifest.json"):
        _fail("information stage registry identity differs")


def preregister_information_study_v4(*, plan, output_root):
    plan=EconomicEntryInformationStudyPlanV4.model_validate(plan.model_dump())
    _,loaded,_=_inputs(plan)
    root=_root(output_root)/plan.experiment_id
    target=publish_stage(study_root=root,stage="preregistered",plan_sha256=plan.plan_sha256,parent_sha256=None,
        artifacts={"plan.json":_json_bytes(plan.model_dump(mode="json"))})
    register_information_stage(plan,loaded[1],root,"PREREGISTERED",target/"manifest.json")
    return target/"plan.json"


def load_information_study_v4(*, plan_path, output_root):
    plan=EconomicEntryInformationStudyPlanV4.model_validate_json(Path(plan_path).read_text(encoding="utf-8"))
    root=_root(output_root)/plan.experiment_id
    if Path(plan_path).resolve()!=root/"preregistered/plan.json":
        _fail("information study must use its exact registered path")
    registered=read_stage(root/"preregistered",stage="preregistered",plan_sha256=plan.plan_sha256,parent_sha256=None)
    _inputs(plan)
    _ledger(plan,root,"PREREGISTERED")
    return plan,root,registered


def train_information_study_v4(*, plan_path, output_root):
    plan,root,registered=load_information_study_v4(plan_path=plan_path,output_root=output_root)
    with _exclusive_file_lock(root/"fit.lock"):
        _,loaded,info=_inputs(plan)
        if (root/"trained").exists():
            read_stage(root/"trained",stage="trained",plan_sha256=plan.plan_sha256,parent_sha256=registered["stage_sha256"])
            register_information_stage(plan,loaded[1],root,"TRAINED",root/"trained/manifest.json",generated=2)
            return root/"trained"
        fitted=train_information_entry_v4(features=loaded[6],labels=loaded[4],information=info,request=plan.training_request)
        artifacts={"split_receipt.parquet":_parquet_bytes(fitted.split_receipt)}
        metadata={"request":plan.training_request.model_dump(mode="json"),"request_sha256":plan.training_request.request_sha256,
                  "model_scope":plan.model_scope.model_dump(mode="json"),"scope_sha256":plan.model_scope.scope_sha256,
                  "diagnostics":fitted.diagnostics,"arms":{}}
        for arm,value in fitted.arms.items():
            prefix=arm.lower()
            metadata["arms"][arm]={"feature_names":list(value.feature_names),"feature_bounds":value.feature_bounds,
                "price_support":value.price_support,"diagnostics":value.diagnostics}
            artifacts[prefix+"_return.txt"]=value.return_model.model_to_string().encode()
            artifacts[prefix+"_risk.txt"]=value.risk_model.model_to_string().encode()
        artifacts["model_metadata.json"]=_json_bytes(metadata)
        target=publish_stage(study_root=root,stage="trained",plan_sha256=plan.plan_sha256,parent_sha256=registered["stage_sha256"],artifacts=artifacts)
        register_information_stage(plan,loaded[1],root,"TRAINED",target/"manifest.json",generated=2)
        return target


def load_fitted_information_v4(*, plan_path, output_root):
    import lightgbm as lgb
    plan,root,registered=load_information_study_v4(plan_path=plan_path,output_root=output_root)
    read_stage(root/"trained",stage="trained",plan_sha256=plan.plan_sha256,parent_sha256=registered["stage_sha256"])
    _ledger(plan,root,"TRAINED")
    metadata=json.loads((root/"trained/model_metadata.json").read_text(encoding="utf-8"))
    if (metadata["request"]!=plan.training_request.model_dump(mode="json") or metadata["request_sha256"]!=plan.training_request.request_sha256
            or metadata["model_scope"]!=plan.model_scope.model_dump(mode="json") or metadata["scope_sha256"]!=plan.model_scope.scope_sha256
            or lgb.__version__!=plan.training_request.parent_request.lightgbm_version):
        _fail("information fitted request/runtime differs")
    arms={}
    if set(metadata["arms"])!={"MATCHED_NINE","INFORMATION_THIRTEEN"}:
        _fail("information fit must contain both registered configurations")
    for arm,values in metadata["arms"].items():
        models=[lgb.Booster(model_file=str(root/"trained"/(arm.lower()+suffix))) for suffix in ("_return.txt","_risk.txt")]
        names=tuple(values["feature_names"])
        expected=ECONOMIC_FEATURE_NAMES if arm=="MATCHED_NINE" else plan.training_request.feature_names
        if names!=expected or set(values["feature_bounds"])!=set(expected) or any(tuple(model.feature_name())!=names for model in models):
            _fail("information model feature order differs")
        arms[arm]=EconomicInformationArmV4(plan.training_request,arm,names,*models,
            {name:tuple(value) for name,value in values["feature_bounds"].items()},
            {int(key):value for key,value in values["price_support"].items()},values["diagnostics"])
    return arms
