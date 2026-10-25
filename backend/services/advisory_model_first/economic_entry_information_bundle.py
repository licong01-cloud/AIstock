"""Published v4 weights, not a training resume or a qualification upgrade."""
from dataclasses import dataclass
import hashlib
import json
import re
from pathlib import Path
from types import MappingProxyType

import numpy as np

from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryStudyPlanV3
from backend.services.advisory_model_first.economic_entry_contracts import ECONOMIC_FEATURE_NAMES
from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationStudyPlanV4
from backend.services.advisory_model_first.economic_entry_information_training import EconomicInformationArmV4
from backend.services.advisory_model_first.economic_entry_labels import _fail
from backend.services.advisory_model_first.economic_entry_pipeline import _verify_reference, file_sha256, read_stage
from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha


@dataclass(frozen=True)
class LoadedEconomicInformationBundleV4:
    plan: EconomicEntryInformationStudyPlanV4
    fitted: EconomicInformationArmV4
    bundle_sha256: str
    evaluation: object
    decision_use: str = "NAVIGATION_ONLY"
    deployable: bool = False


def _path(value):
    path=Path(value)
    if not path.is_absolute() or path.resolve()!=path.absolute() or path.drive.upper()=="C:" or not path.is_file():
        _fail("v4 model consumption requires its original non-C nonredirected file")
    return path


def _bytes(path,*,maximum,descriptor=None):
    path=_path(path)
    if not 0<path.stat().st_size<=maximum:
        _fail("v4 model artifact exceeds its read budget")
    with path.open("rb") as source:
        data=source.read(maximum+1)
    if not 0<len(data)<=maximum:
        _fail("v4 model artifact changed or exceeds its bounded read")
    if descriptor is not None and (len(data)!=descriptor["size_bytes"] or hashlib.sha256(data).hexdigest()!=descriptor["sha256"]):
        _fail("v4 model artifact changed after stage/reference verification")
    return data


def _json(path,descriptor=None):
    data=_bytes(path,maximum=1048576,descriptor=descriptor)
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:
                _fail("v4 model metadata has contradictory duplicate fields")
            result[key]=value
        return result
    try:
        return json.loads(data.decode("utf-8"),object_pairs_hook=unique,
            parse_constant=lambda value:_fail("v4 model metadata has nonfinite JSON numbers"))
    except (ValueError,UnicodeError):
        _fail("v4 model metadata is malformed")


def _stage(root,*,stage,plan_hash,parent_hash):
    manifest=_json(root/"manifest.json")
    files=manifest.get("files")
    if not isinstance(files,dict) or not 1<=len(files)<=20:
        _fail("v4 model stage has invalid bounded artifact descriptors")
    for name,item in files.items():
        if (not re.fullmatch(r"[a-z0-9_]+\.(json|jsonl|parquet|txt)",name) or name=="manifest.json"
                or not isinstance(item,dict) or type(item.get("size_bytes")) is not int
                or not 0<item["size_bytes"]<=16777216):
            _fail("v4 model artifact exceeds its sixteen MiB budget")
        _path(root/name)
    return read_stage(root,stage=stage,plan_sha256=plan_hash,parent_sha256=parent_hash)


def _support(values,configuration):
    result={}
    if not isinstance(values,dict):
        _fail("v4 serving price support must be an explicit mapping")
    for key,item in values.items():
        if not re.fullmatch(r"-?(0|[1-9][0-9]*)",key) or str(int(key))!=key or not isinstance(item,dict):
            _fail("v4 serving price support contains aliased or malformed bins")
        count,days=item.get("observation_count"),item.get("decision_day_count")
        lower,upper=item.get("observed_min_gap_bps"),item.get("observed_max_gap_bps")
        if (type(count) is not int or type(days) is not int or count<configuration.minimum_bin_observations
                or not configuration.minimum_bin_days<=days<=count or any(type(x) not in (int,float) for x in (lower,upper))
                or not np.isfinite([lower,upper]).all() or lower>upper
                or any(int(np.floor(x/configuration.gap_bin_width_bps))!=int(key) for x in (lower,upper))):
            _fail("v4 serving price support has contradictory bounds/counts")
        result[int(key)]=dict(item)
    return result


def _ledger(plan,root,stage,records):
    matches=[row for row in records if row.experiment_id==plan.experiment_id and row.research_stage==stage]
    if len(matches)!=1:
        _fail("v4 model stage lacks its one original registration")
    row=matches[0]
    path=root/stage.lower()/"manifest.json"
    source=plan.training_request.parent_request.source_request
    if (len(row.evidence_refs)!=1 or _verify_reference(row.evidence_refs[0])!=path
            or row.evidence_refs[0].role!=f"information_{stage.lower()}"
            or row.objective_contract!=plan.objective_contract or row.decision_use!=plan.decision_use
            or row.schema_identity!=plan.schema_version or row.planned_trial_count!=2
            or row.generated_trial_count!=(0 if stage=="PREREGISTERED" else 2)
            or row.evaluated_trial_count!=(2 if stage=="EVALUATED" else 0) or row.selected_trial_count!=0
            or row.study_type!=plan.study_type or len(row.consumed_windows)!=1
            or row.consumed_windows[0].window_id!="P0C_DEVELOPMENT_V1"
            or row.consumed_windows[0].dataset_identity!=row.dataset_identity
            or row.consumed_windows[0].start_date!=source.train_start
            or row.consumed_windows[0].end_date!=source.label_cutoff):
        _fail("v4 model stage registration or trial identity differs")


def load_information_research_bundle_v4(*,plan_path):
    import lightgbm as lgb
    path=_path(plan_path)
    plan=EconomicEntryInformationStudyPlanV4.model_validate(_json(path))
    root=path.parent.parent
    if path!=root/"preregistered/plan.json" or root.name!=plan.experiment_id:
        _fail("v4 model plan is not its exact published study path")
    registered=_stage(root/"preregistered",stage="preregistered",plan_hash=plan.plan_sha256,parent_hash=None)
    trained=_stage(root/"trained",stage="trained",plan_hash=plan.plan_sha256,parent_hash=registered["stage_sha256"])
    expected_files={f"{arm}_{head}.txt" for arm in ("matched_nine","information_thirteen") for head in ("return","risk")}
    if set(trained["files"])!=expected_files|{"model_metadata.json","split_receipt.parquet"}:
        _fail("v4 serving trained stage must contain exactly four heads and original metadata/receipt")
    evaluated=_stage(root/"evaluated",stage="evaluated",plan_hash=plan.plan_sha256,parent_hash=trained["stage_sha256"])
    registry_path=_path(root.parent/"trial_registry.jsonl")
    if registry_path.stat().st_size>16777216:
        _fail("v4 model registry exceeds its read budget")
    records=AdvisoryResearchTrialRegistryV1(registry_path).read()
    for stage in ("PREREGISTERED","TRAINED","EVALUATED"):
        _ledger(plan,root,stage,records)
    parent_path=_path(_verify_reference(plan.parent_plan_ref))
    parent=AlignedEntryStudyPlanV3.model_validate(_json(parent_path,plan.parent_plan_ref.model_dump()))
    parent_root=parent_path.parent.parent
    if (plan.parent_plan_ref.role!="information_parent_v3_plan" or parent_path!=parent_root/"preregistered/plan.json"
            or parent_root.name!=parent.experiment_id or parent.training_request!=plan.training_request.parent_request
            or parent.experiment_id not in plan.parent_lineage):
        _fail("v4 serving parent plan identity differs")
    parent_registered=_stage(parent_root/"preregistered",stage="preregistered",plan_hash=parent.plan_sha256,parent_hash=None)
    parent_trained=_path(_verify_reference(plan.parent_trained_manifest_ref))
    if plan.parent_trained_manifest_ref.role!="information_parent_v3_trained" or parent_trained!=parent_root/"trained/manifest.json":
        _fail("v4 serving parent model reference differs")
    original_trained=_stage(parent_root/"trained",stage="trained",plan_hash=parent.plan_sha256,parent_hash=parent_registered["stage_sha256"])
    info_path=_path(_verify_reference(plan.training_request.information_ref))
    if info_path.name!="manifest.json" or info_path.parent.name!="prepared":
        _fail("v4 serving information stage path differs")
    identity=_json(info_path.parent/"identity.json")
    information_stage=_stage(info_path.parent,stage="prepared",plan_hash=identity["input_identity"],parent_hash=original_trained["stage_sha256"])
    identity=_json(info_path.parent/"identity.json",information_stage["files"]["identity.json"])
    if (identity["parent_plan_sha256"]!=parent.plan_sha256
            or identity["parent_trained_stage_sha256"]!=original_trained["stage_sha256"]
            or identity["information_rows_sha256"]!=plan.training_request.information_rows_sha256
            or identity["implementation_sha256"]!=plan.training_request.implementation_sha256
            or type(identity["models_trained"]) is not int or identity["models_trained"]!=0
            or identity["sealed_accessed"] is not False):
        _fail("v4 serving original input lineage differs")
    receipt=_json(info_path.parent/"source_receipt.json",information_stage["files"]["source_receipt.json"])
    if (receipt.get("source_evidence")!="CURRENT_DB_HISTORICAL_NON_VINTAGE" or receipt.get("outcomes_read") is not False
            or receipt.get("new_native_receipt") is not False or receipt.get("deployable") is not False):
        _fail("v4 serving cannot upgrade original source qualification")
    metadata=_json(root/"trained/model_metadata.json",trained["files"]["model_metadata.json"])
    if (metadata["request"]!=plan.training_request.model_dump(mode="json")
            or metadata["request_sha256"]!=plan.training_request.request_sha256
            or metadata["model_scope"]!=plan.model_scope.model_dump(mode="json")
            or metadata["scope_sha256"]!=plan.model_scope.scope_sha256
            or lgb.__version__!=plan.training_request.parent_request.lightgbm_version
            or set(metadata["arms"])!={"MATCHED_NINE","INFORMATION_THIRTEEN"}):
        _fail("v4 serving model request/scope/runtime differs")
    fitted=None
    common_support=None
    for arm,values in metadata["arms"].items():
        names=ECONOMIC_FEATURE_NAMES if arm=="MATCHED_NINE" else plan.training_request.feature_names
        if tuple(values["feature_names"])!=names or set(values["feature_bounds"])!=set(names):
            _fail("v4 serving model arm dimension/order differs")
        bounds={name:tuple(value) for name,value in values["feature_bounds"].items()}
        if any(len(value)!=2 or any(type(number) not in (int,float) for number in value)
               or not np.isfinite(value).all() or value[0]>value[1] for value in bounds.values()):
            _fail("v4 serving feature support is malformed")
        models=[]
        for suffix in ("_return.txt","_risk.txt"):
            name=arm.lower()+suffix
            data=_bytes(root/"trained"/name,maximum=16777216,descriptor=trained["files"][name])
            models.append(lgb.Booster(model_str=data.decode("utf-8")))
        if any(tuple(model.feature_name())!=names for model in models):
            _fail("v4 serving actual weights disagree with feature metadata")
        support=_support(values["price_support"],plan.training_request.parent_request.source_request)
        if common_support is not None and support!=common_support:
            _fail("v4 serving arms lost their same-cohort price support")
        common_support=support
        diagnostic=values["diagnostics"]
        if (diagnostic.get("train_rows")!=plan.expected_train_rows or diagnostic.get("validation_rows")!=plan.expected_validation_rows
                or diagnostic.get("common_fit_rows_sha256")!=plan.training_request.common_fit_rows_sha256
                or diagnostic.get("test_used_for_training_or_calibration") is not False):
            _fail("v4 serving fitted cohort differs from its registration")
        if arm=="INFORMATION_THIRTEEN":
            fitted=EconomicInformationArmV4(plan.training_request,arm,names,*models,bounds,
                support,diagnostic)
    report=_json(root/"evaluated/evaluation.json",evaluated["files"]["evaluation.json"])
    if (report["plan_sha256"]!=plan.plan_sha256 or report["decision_use"]!="NAVIGATION_ONLY" or report["deployable"] is not False
            or report["sealed_holdout_accessed"] is not False or report["economic_effectiveness"]!="EXPLORATORY_NOT_CONFIRMED"):
        _fail("v4 serving cannot reinterpret its evaluated economic evidence")
    digest=sha({"plan_sha256":plan.plan_sha256,"registered":registered["stage_sha256"],"trained":trained["stage_sha256"],
        "evaluated":evaluated["stage_sha256"],"parent_plan":file_sha256(parent_path),"parent_model":file_sha256(parent_trained),
        "information":file_sha256(info_path)})
    return LoadedEconomicInformationBundleV4(plan,fitted,digest,MappingProxyType(report))
