"""Frozen R2 information models: original JSON heads and numerical entrypoints."""
from importlib import import_module
import json
from pathlib import Path

import pandas as pd
import numpy as np

from backend.services.advisory_model_first.cross_package_validation_adapters_v1 import _checked_member
from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_bytes, publish_json
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import _parquet, checked_plan
from backend.services.advisory_model_first.economic_entry_labels import KEY
from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
from backend.services.advisory_model_first.economic_sector_price_value_v1 import SectorPriceFitV1
from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1


# (model, native module, recipe information key, native prefix, feature constant)
RECIPES = (
    ("M1","economic_sector_price_value_v1","sector_features","sector","SECTOR_FEATURES"),
    ("M5","economic_selection_state_price_v1","selection_features","selection_state","STATE_FEATURES"),
    ("M6","economic_moneyflow_price_v1","moneyflow_features","moneyflow","MONEYFLOW_FEATURES"),
    ("M7","economic_price_path_value_v1","price_path_features","price_path","PRICE_PATH_FEATURES"),
    ("M8","economic_market_risk_price_v1","market_risk_features","market_risk","MARKET_RISK_FEATURES"),
    ("M9","economic_volume_context_price_v1","volume_context_features","volume_context","VOLUME_CONTEXT_FEATURES"),
    ("M10","economic_breadth_state_price_v1","breadth_state_features","breadth_state","BREADTH_STATE_FEATURES"),
    ("M11","economic_traded_price_distribution_v1","traded_price_distribution_features","traded_price","TRADED_PRICE_FEATURES"),
    ("M12","economic_session_path_v1","session_path_features","session_path","SESSION_PATH_FEATURES"),
    ("M13","economic_free_float_turnover_v1","free_float_features","free_float_turnover","FREE_FLOAT_FEATURES"),
    ("M14","economic_valuation_context_v1","valuation_features","valuation_context","VALUATION_FEATURES"),
    ("M15","economic_limit_state_v1","limit_state_features","limit_state","LIMIT_STATE_FEATURES"),
    ("M16","economic_candidate_cohort_v1","candidate_cohort_features","candidate_cohort","CANDIDATE_COHORT_FEATURES"),
    ("M17","economic_flow_path_v1","flow_path_features","flow_path","FLOW_PATH_FEATURES"),
    ("M18","economic_asymmetric_risk_v1","asymmetric_risk_features","asymmetric_risk","ASYMMETRIC_RISK_FEATURES"),
    ("M19","economic_sector_moneyflow_v1","sector_moneyflow_features","sector_moneyflow","JOINT_FEATURES"),
    ("M20","economic_parent_raw_score_v1","parent_raw_score_features","parent_raw_score","RAW_FEATURES"),
    ("M21","economic_parent_scale_state_v1","parent_scale_state_features","parent_scale_state","SCALE_FEATURES"),
    ("M22","economic_sector_parent_raw_v1","sector_parent_raw_features","sector_parent_raw","JOINT_FEATURES"),
    ("M23","economic_parent_raw_trajectory_v1","parent_raw_trajectory_features","parent_raw_trajectory","TRAJECTORY_FEATURES"),
    ("M24","economic_parent_normalized_trajectory_v1","parent_normalized_trajectory_features","parent_normalized_trajectory","INFORMATION_FEATURES"),
    ("M25","economic_sector_path_price_v1","sector_path_price_features","sector_path_price","INFORMATION_FEATURES"),
)


def load_original_information_review_units(item):
    if item["family"] != "advisory_price_research_campaign_r2_20261004":
        raise LookupError("not an R2 information estimator")
    body,_ = _checked_member(item,"metadata.json")
    declared = [entry for entry in RECIPES if entry[2] in body["recipe"]]
    if len(declared) != 1:
        raise LookupError("R2 metadata has no unique original information recipe")
    model_id,module_name,key,prefix,constant = declared[0]
    module = import_module("backend.services.advisory_model_first."+module_name)
    features = tuple(getattr(module,constant))
    if body["recipe"][key] != list(features) or body["recipe"]["d_features"] != list(D_FEATURES):
        raise ValueError("original R2 declared information order differs")
    support = ValueAnchorGapSupportV1(tuple(tuple(p) for p in body["support"]["intervals_bps"]))
    digest = getattr(module,prefix+"_fit_identity_v1")(body["recipe"],body["models"],support)
    if digest != body["model_sha256"]:
        raise ValueError("original R2 information estimator identity differs")
    fitted = SectorPriceFitV1(body["recipe"],body["models"],support,{},digest)
    native_query = getattr(module,prefix+"_nodes_v1")
    units = []
    for arm in ("matched","candidate"):
        units.append(dict(model_id=item["model_id"],original_model_name=model_id,family=item["family"],arm=arm,
            fitted=fitted,query=lambda rows,a=arm:native_query(fitted=fitted,rows=rows,arm=a),
            policy_role="ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY",required_fields=(*D_FEATURES,*features,"actual_gap_bps"),
            manifest_sha256=item["manifest_sha256"],weight_files=[dict(file=w["file"],sha256=w["sha256"]) for w in item["weights"]],
            original_numerical_source=dict(path=str(Path(module.__file__).resolve()),sha256=file_sha(module.__file__))))
    return units


def load_original_context_review_units(item):
    from backend.services.advisory_model_first.economic_context_value_training_v1 import ContextValueFitV1, context_fit_identity_v1
    from backend.services.advisory_model_first.economic_context_value_inference_v1 import context_value_nodes_v1
    if item["family"] != "advisory_context_price_value_v1_20261004":
        raise LookupError("not the original categorical context estimator")
    body,_ = _checked_member(item,"metadata.json")
    support = {code:ValueAnchorGapSupportV1(tuple(tuple(p) for p in value["intervals_bps"])) for code,value in body["support"].items()}
    digest = context_fit_identity_v1(body["recipe"],body["models"],support)
    if digest != body["model_sha256"]:
        raise ValueError("original frozen context identity differs")
    fitted = ContextValueFitV1(body["recipe"],body["models"],support,{},digest)
    return [dict(model_id=item["model_id"],original_model_name="CONTEXT",family=item["family"],arm=arm,fitted=fitted,
        query=lambda rows,a=arm:context_value_nodes_v1(fitted=fitted,rows=rows,arm=a),
        policy_role="ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY",required_fields=(*D_FEATURES,"classification_l2_code","actual_gap_bps"),
        manifest_sha256=item["manifest_sha256"],weight_files=[dict(file=w["file"],sha256=w["sha256"]) for w in item["weights"]]) for arm in ("matched","candidate")]


def query_original_economic_nodes(*, fitted, rows):
    """Exact v1 numeric point kernel; never stamps the training roster on new data.

    The original API also constructs a price grid and its provenance envelope.
    Only its finite/support/return/risk/action kernel is applicable to transfer.
    """
    from backend.services.advisory_model_first.economic_entry_labels import _number
    names = fitted.request.feature_names
    matrix = rows.loc[:,names].map(lambda v:np.nan if (value:=_number(v)) is None else value).astype(float)
    supported = np.isfinite(matrix.to_numpy()).all(axis=1)
    if set(fitted.feature_bounds) != set(names):
        raise ValueError("original economic point support schema differs")
    for name,(lower,upper) in fitted.feature_bounds.items():
        if not np.isfinite([lower,upper]).all() or lower>upper:
            raise ValueError("original economic point bounds differ")
        supported &= matrix[name].between(lower,upper).to_numpy()
    for position,gap in enumerate(matrix.query_gap_bps):
        if not np.isfinite(gap):
            supported[position] = False
            continue
        support = fitted.price_support.get(int(np.floor(gap/fitted.request.gap_bin_width_bps)))
        if support is None:
            supported[position] = False
            continue
        count,days = int(support["observation_count"]),int(support["decision_day_count"])
        low,high = float(support["observed_min_gap_bps"]),float(support["observed_max_gap_bps"])
        if (not np.isfinite([low,high]).all() or low>high or days>count
                or count<fitted.request.minimum_bin_observations or days<fitted.request.minimum_bin_days):
            raise ValueError("original economic point price support differs")
        supported[position] &= low<=gap<=high
    result = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(rows),expected_net_bps=np.nan,downside_q90_bps=np.nan),index=rows.index)
    if supported.any():
        selected = matrix.loc[supported]
        mean,risk = (np.asarray(model.predict(selected,num_threads=2),dtype=float) for model in (fitted.return_model,fitted.risk_model))
        if (mean.shape != (len(selected),) or risk.shape != mean.shape or not np.isfinite([mean,risk]).all()
                or (risk<0).any() or (risk>10000).any()):
            raise ValueError("original economic point head output differs")
        action = (mean>fitted.request.minimum_expected_net_value_bps)&(risk<=fitted.request.downside_budget_bps)
        result.loc[supported,"status"] = np.where(action,"ACCEPTABLE","AVOID")
        result.loc[supported,"expected_net_bps"],result.loc[supported,"downside_q90_bps"] = mean,risk
    return result


def load_original_economic_review_unit(item):
    from backend.services.advisory_model_first.cross_package_validation_review_v1 import OriginalWslLightgbmPredictor
    from backend.services.advisory_model_first.economic_entry_training import EconomicEntryTrainingResult
    from backend.services.advisory_model_first.economic_entry_contracts import EconomicEntryInputIdentityV1, EconomicEntryTrainingRequestV1
    if item["family"] != "advisory_economic_entry_value_v1_20261002":
        raise LookupError("not the original economic entry estimator")
    body,_ = _checked_member(item,"model_metadata.json")
    prepared = Path(item["manifest_ref"]).parent.parent/"prepared"
    manifest = json.loads((prepared/"manifest.json").read_text(encoding="utf-8"))
    for name in ("identity.json","training_request.json"):
        if file_sha(prepared/name) != manifest["files"][name]["sha256"]:
            raise ValueError("original economic request/origin identity differs")
    request = EconomicEntryTrainingRequestV1.model_validate_json((prepared/"training_request.json").read_text(encoding="utf-8"))
    origin = EconomicEntryInputIdentityV1.model_validate_json((prepared/"identity.json").read_text(encoding="utf-8"))
    if request.request_sha256 != body["request_sha256"] or request.input_identity_sha256 != origin.identity_sha256 or body["lightgbm_version"]!="4.6.0":
        raise ValueError("original economic metadata/request/runtime differs")
    models = []
    for name in ("return_model.txt","risk_model.txt"):
        member = next(w for w in item["weights"] if w["file"]==name and w["artifact_kind"]=="PREDICTOR_WEIGHT")
        model = OriginalWslLightgbmPredictor(Path(item["manifest_ref"]).parent/name,member["sha256"])
        if tuple(model.feature_name()) != request.feature_names:
            raise ValueError("original economic frozen feature names differ")
        models.append(model)
    fitted = EconomicEntryTrainingResult(*models,request,{k:tuple(v) for k,v in body["feature_bounds"].items()},
        {int(k):v for k,v in body["price_support"].items()},body["diagnostics"],pd.DataFrame())
    return dict(model_id=item["model_id"],original_model_name="ECONOMIC_V1",family=item["family"],arm="model",fitted=fitted,
        query=lambda rows:query_original_economic_nodes(fitted=fitted,rows=rows),policy_role="ORIGINAL_20_REVIEW_STOP_RANK_POLICY",
        required_fields=request.feature_names,manifest_sha256=item["manifest_sha256"],
        weight_files=[dict(file=w["file"],sha256=w["sha256"]) for w in item["weights"]],
        original_numerical_source=dict(path=str(Path(__file__).with_name("economic_entry_inference.py")),
            sha256=file_sha(Path(__file__).with_name("economic_entry_inference.py"))),
        origin_identity_for_parity_only=origin)


def predict_original_information_review(root, *, package_id, progress=None, input_directory="information_inputs_v1",scope_models=None,
        query_spec_filename="information_query_spec_v1.json",prediction_directory="information_predictions"):
    from backend.services.advisory_model_first.cross_package_validation_review_v1 import review_price_queries, cached_review_predictions
    from backend.services.advisory_model_first.cross_package_validation_review_inputs_v1 import prepare_original_information_inputs, checked_information_checkpoint
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    if input_directory == "information_inputs_v1":
        inputs = prepare_original_information_inputs(root,package_id=package_id,progress=progress)
    elif input_directory in {"sector_information_inputs_v1","breadth_information_inputs_v1","scale_information_inputs_v1"}:
        inputs = checked_information_checkpoint(root, package_id=package_id, input_directory=input_directory)
        if inputs is None:
            raise ValueError("original information input checkpoint is not prepared")
    else:
        raise ValueError("original information input batch is not declared")
    original = review_price_queries(root,package_id=package_id)
    units,missing = [],[]
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    for item in inventory["inventory"]["items"]:
        permitted = {"advisory_price_research_campaign_r2_20261004"}
        if scope_models is not None and "CONTEXT" in scope_models:
            permitted.add("advisory_context_price_value_v1_20261004")
        if scope_models is not None and "ECONOMIC_V1" in scope_models:
            permitted.add("advisory_economic_entry_value_v1_20261002")
        if item["family"] not in permitted:
            continue
        try:
            if item["family"]=="advisory_economic_entry_value_v1_20261002":
                loaded = [load_original_economic_review_unit(item)]
            elif item["family"]=="advisory_context_price_value_v1_20261004":
                loaded = load_original_context_review_units(item)
            else:
                loaded = load_original_information_review_units(item)
        except LookupError:
            continue  # M2/M3/M4 were already evaluated in the original core.
        name = loaded[0]["original_model_name"]
        if scope_models is not None and name not in scope_models:
            continue
        if name != "ECONOMIC_V1" and name+".parquet" not in inputs["files"]:
            missing.append(dict(model_id=item["model_id"],original_model_name=name,status="INPUT_UNAVAILABLE",
                reason=next((reason for key,reason in inputs.get("not_yet_prepared",{}).items() if name in key.split("/")),"original information source absent")))
            continue
        units.extend(loaded)
    specs = [{key:value for key,value in unit.items() if key not in {"fitted","query","origin_identity_for_parity_only"}} for unit in units]
    spec_path = base/query_spec_filename
    spec = dict(parent_core_query_spec_sha256=file_sha(base/"core_query_spec_v1.json"),
        plan_sha256=file_sha(root/"plan.json"),inputs_receipt_sha256=file_sha(base/input_directory/"receipt.json"),
        T_query_sha256=file_sha(base/"T_queries.parquet"),units=specs,missing_units=missing,original_D_axis=plan["decision_dates"],
        E_outcomes_used=False,other_roles_same_window_outcomes_already_seen=True,physical_fit_count=0,decision_use="NAVIGATION_ONLY")
    publish_json(spec_path,spec)
    for unit in units:
        if unit["original_model_name"] == "ECONOMIC_V1":
            information = original.loc[:,KEY]
            original["query_gap_bps"] = original.actual_gap_bps
        else:
            path = base/input_directory/(unit["original_model_name"]+".parquet")
            if file_sha(path) != inputs["files"][path.name]:
                raise ValueError("original R2 information feature hash differs")
            information = pd.read_parquet(path)
        extras = [name for name in unit["required_fields"] if name not in original.columns]
        query = original.merge(information.loc[:,[*KEY,*extras]],on=KEY,validate="one_to_one",how="left",sort=False)
        if not query[KEY].equals(original[KEY]):
            raise ValueError("original R2 information feature join changed candidate keys")
        query = query.loc[query.selection_effective_rank.le(5)].reset_index(drop=True)
        destination = base/prediction_directory/unit["model_id"]/unit["arm"]
        if cached_review_predictions(destination, spec_path=spec_path, rows=query,
                policy_role=unit["policy_role"]) is not None:
            if progress:
                progress(dict(event="ORIGINAL_INFORMATION_CHECKPOINT_REUSED", model=unit["original_model_name"], arm=unit["arm"], physical_fit_count=0))
            continue
        output = unit["query"](query.loc[:,unit["required_fields"]])
        if not output.index.equals(query.index) or not output.status.map(lambda v:isinstance(v,str)).all():
            raise ValueError("original R2 numerical output changed its keys/status")
        predictions = pd.concat((query.loc[:,[*KEY,"selection_effective_rank"]],output),axis=1)
        publish_bytes(destination/"predictions.parquet",_parquet(predictions))
        publish_json(destination/"receipt.json",dict(spec_sha256=file_sha(spec_path),
            predictions_sha256=file_sha(destination/"predictions.parquet"),original_rows=len(query),
            physical_fit_count=0,E_outcomes_used=False,policy_role=unit["policy_role"]))
        if progress:
            progress(dict(event="ORIGINAL_INFORMATION_MODEL_PREDICTED",model=unit["original_model_name"],arm=unit["arm"],
                original_rows=len(query),status_counts={str(k):int(v) for k,v in output.status.value_counts().items()},physical_fit_count=0))
    return spec
