"""Original R2 D-only feature builders over frozen scores/read-only sources."""
from contextlib import contextmanager
from importlib import import_module
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import file_sha, publish_bytes, publish_json, readonly_connection
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import _parquet, checked_plan
from backend.services.advisory_model_first.economic_entry_labels import KEY


class ConsumerReadonlySession:
    """Inject only the public source's read session, not its business semantics."""
    @contextmanager
    def connection(self):
        with readonly_connection() as connection:
            connection.set_session(isolation_level="REPEATABLE READ",readonly=True,autocommit=False)
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout=30000")
            yield connection

    def close(self):
        pass  # The connection context already rolls back and returns the connection.


def checked_information_checkpoint(root, *, package_id, input_directory):
    from backend.services.advisory_model_first.cross_package_validation_review_v1 import checked_review_inputs
    root = Path(root)
    checked_review_inputs(root, package_id=package_id)
    base = root/"review_transfer_v1"/package_id
    if input_directory not in {"information_inputs_v1", "sector_information_inputs_v1", "breadth_information_inputs_v1", "scale_information_inputs_v1"}:
        raise ValueError("original information checkpoint directory is undeclared")
    destination = base/input_directory
    receipt_path = destination/"receipt.json"
    if not receipt_path.is_file():
        return None
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    if (receipt["inputs_receipt_sha256"] != file_sha(base/"inputs_receipt.json")
            or ("spec_sha256" in receipt and receipt["spec_sha256"] != file_sha(destination/"spec.json"))):
        raise ValueError("original information parent/spec checkpoint changed")
    for name, digest in receipt["files"].items():
        if Path(name).name != name or file_sha(destination/name) != digest:
            raise ValueError("original information input checkpoint changed")
    for name, digest in receipt.get("source_files", {}).items():
        if Path(name).name != name or file_sha(Path(__file__).parent/name) != digest:
            raise ValueError("original information numerical source changed")
    for field in ("snapshot_sha256", "source_sha256"):
        for name, digest in receipt.get(field, {}).items():
            if not Path(name).is_absolute() or file_sha(name) != digest:
                raise ValueError("original information readonly source snapshot changed")
    return receipt


def prepare_original_information_inputs(root, *, package_id, progress=None):
    root = Path(root)
    checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    destination = base/"information_inputs_v1"
    receipt_path = destination/"receipt.json"
    receipt = checked_information_checkpoint(root, package_id=package_id, input_directory=destination.name)
    if receipt is not None:
        return receipt
    original_receipt = json.loads((base/"inputs_receipt.json").read_text(encoding="utf-8"))
    for name,digest in original_receipt["files"].items():
        if file_sha(base/name) != digest:
            raise ValueError("original information D source changed")
    inputs = pd.read_parquet(base/"d_features.parquet")
    candidates = pd.read_parquet(base/"candidates.parquet")
    calendar = pd.to_datetime(json.loads((root/"calendar.json").read_text(encoding="utf-8")))
    quotes_path = root/"legacy_score_transfer_v1/shared_daily/daily.parquet"
    index_path = root/"legacy_score_transfer_v1/shared_daily/index_daily.parquet"
    daily_receipt = json.loads(quotes_path.with_name("receipt.json").read_text(encoding="utf-8"))
    # Both paths are bound by the parent's already-frozen source receipt.
    pinned = daily_receipt.get("files",{})
    for path in (quotes_path,index_path):
        expected = pinned.get(path.name)
        if isinstance(expected,dict):
            expected = expected["sha256"]
        if expected is None or file_sha(path) != expected:
            raise ValueError("original information shared snapshot changed")
    prices = pd.read_parquet(quotes_path)
    prices = prices.loc[prices.instrument.isin(candidates.instrument)&prices.trade_date.le(candidates[KEY[0]].max())].copy()
    from backend.services.advisory_model_first.economic_volume_context_price_v1 import volume_context_requests_v1
    requested_pairs = pd.MultiIndex.from_tuples(volume_context_requests_v1(candidates=candidates,calendar=calendar))
    volumes = prices.loc[pd.MultiIndex.from_frame(prices[["trade_date","instrument"]]).isin(requested_pairs)].copy()
    index = pd.read_parquet(index_path)
    index = index.loc[index.trade_date.le(candidates[KEY[0]].max())].copy()
    inputs["feature_visible_through"] = inputs[KEY[0]]  # A validated D query cutoff, not a capture timestamp.
    rows,source_files,source_receipts = {},{},{}

    def native(module_name,function,model_id,**kwargs):
        module = import_module("backend.services.advisory_model_first."+module_name)
        result = getattr(module,function)(**kwargs)
        if (result.duplicated(KEY).any() or set(map(tuple,result[KEY].to_numpy())) != set(map(tuple,candidates[KEY].to_numpy()))):
            raise ValueError("original information builder changed frozen candidate keys")
        source_files[module_name+".py"] = file_sha(module.__file__)
        rows[model_id] = result
        return result

    for module,prefix,model in (("economic_price_path_value_v1","price_path","M7"),
            ("economic_session_path_v1","session_path","M12"),("economic_asymmetric_risk_v1","asymmetric_risk","M18"),
            ("economic_limit_state_v1","limit_state","M15")):
        native(module,prefix+"_rows_v1",model,candidates=candidates,prices=prices,calendar=calendar)
    native("economic_market_risk_price_v1","market_risk_rows_v1","M8",candidates=candidates,prices=prices,benchmark=index,calendar=calendar)
    native("economic_volume_context_price_v1","volume_context_rows_v1","M9",candidates=candidates,prices=prices,volumes=volumes,calendar=calendar)
    native("economic_traded_price_distribution_v1","traded_price_distribution_rows_v1","M11",candidates=candidates,
           prices=prices,volumes=volumes,atr=inputs,calendar=calendar)
    native("economic_candidate_cohort_v1","candidate_cohort_rows_v1","M16",candidates=candidates,features=inputs,calendar=calendar)
    native("economic_selection_state_price_v1","selection_state_rows_v1","M5",candidates=candidates,
           rankings=pd.read_parquet(base/"rankings.parquet"),calendar=calendar)
    # Original complete captured score lists, not ranks/raw normalization recomputed on Top20.
    score_source = json.loads((base/"original_sources.json").read_text(encoding="utf-8"))
    original_spec = json.loads((base/"input_spec.json").read_text(encoding="utf-8"))
    records = []
    next_day = dict(zip(calendar[:-1],calendar[1:],strict=True))
    for source in score_source:
        for score in source["scores"]:
            decision = pd.Timestamp(source["decision_date"])
            key = dict(zip(KEY,(decision,next_day[decision],score["symbol"]),strict=True))
            record = {**key,"trade_date":key[KEY[0]],"package_id":package_id,"manifest_sha256":original_spec["package_manifest_sha256"]}
            for role,name in original_spec["component_roles"].items():
                component = score["component_scores"][name]
                record["raw__"+name],record["norm__"+name] = component["raw_score"],component["normalized_score"]
            records.append(record)
    full_scores = pd.DataFrame(records)
    raw_kwargs = dict(rankings=full_scores,candidates=candidates,calendar=calendar,
        component_raw_columns={role:"raw__"+name for role,name in original_spec["component_roles"].items()},
        package_id=package_id,package_manifest_sha256=original_spec["package_manifest_sha256"])
    raw = native("economic_parent_raw_score_v1","parent_raw_score_rows_v1","M20",base_rows=inputs,**raw_kwargs)
    trajectory = native("economic_parent_raw_trajectory_v1","parent_raw_trajectory_rows_v1","M23",base_rows=raw,**raw_kwargs)
    native("economic_parent_normalized_trajectory_v1","parent_normalized_trajectory_rows_v1","M24",base_rows=trajectory,**raw_kwargs)
    # Public bounded adapters retain NULLs; normalized moneyflow is consumed once.
    from backend.services.advisory_model_first.economic_moneyflow_price_source_v1 import load_moneyflow_source_v1
    flow = load_moneyflow_source_v1(candidates=candidates,session_factory=ConsumerReadonlySession)
    source_receipts["moneyflow"] = flow.receipt
    native("economic_moneyflow_price_v1","moneyflow_rows_v1","M6",candidates=candidates,amounts=flow.amounts,calendar=calendar)
    native("economic_flow_path_v1","flow_path_rows_v1","M17",candidates=candidates,amounts=flow.amounts,calendar=calendar)
    for module,loader,native_module,builder,model in (
        ("economic_free_float_turnover_pipeline_v1","load_free_float_turnover_v1","economic_free_float_turnover_v1","free_float_turnover_rows_v1","M13"),
        ("economic_valuation_context_pipeline_v1","load_valuation_context_v1","economic_valuation_context_v1","valuation_context_rows_v1","M14")):
        source_module = import_module("backend.services.advisory_model_first."+module)
        basics,receipt = getattr(source_module,loader)(candidates=candidates,calendar=calendar,session_factory=ConsumerReadonlySession)
        source_files[module+".py"] = file_sha(source_module.__file__)
        source_receipts[model] = receipt
        native(native_module,builder,model,candidates=candidates,basics=basics,calendar=calendar)
    files,summary = {},{}
    for model,frame in rows.items():
        publish_bytes(destination/(model+".parquet"),_parquet(frame))
        files[model+".parquet"] = file_sha(destination/(model+".parquet"))
        numeric = [name for name in frame if name not in {*KEY,*inputs.columns} and pd.api.types.is_numeric_dtype(frame[name])]
        available = np.isfinite(frame.loc[:,numeric].to_numpy(dtype=float)).all(axis=1) if numeric else np.zeros(len(frame),dtype=bool)
        summary[model] = dict(original_rows=len(frame),fully_known_information_rows=int(available.sum()),
            numeric_features=numeric,source_status_counts={name:{str(k):int(v) for k,v in frame[name].value_counts(dropna=False).items()}
                                                       for name in frame if name.endswith("_status")})
        if progress:
            progress(dict(event="ORIGINAL_INFORMATION_INPUT_READY",model=model,**summary[model]))
    receipt = dict(inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),plan_sha256=file_sha(root/"plan.json"),files=files,
        source_files=source_files,source_receipts=source_receipts,snapshot_sha256={str(p):file_sha(p) for p in (quotes_path,index_path)},
        summaries=summary,not_yet_prepared={"M1/M19/M22/M25/CONTEXT":"profile-bound D-visible classification and sector quote adapter pending",
            "M10":"full original market breadth 20-session series pending", "M21":"full-universe original parent center/scale statistics absent from captured score lists"},
        E_outcomes_used=False,other_roles_same_window_outcomes_already_seen=True,physical_fit_count=0,database_written=False,
        native_identity="UNPROVEN",sealed_read=False,decision_use="NAVIGATION_ONLY")
    publish_json(receipt_path,receipt)
    return receipt


def prepare_original_sector_information(root, *, package_id, profile_path, authority_root, crosswalk_ref, progress=None):
    """Pinned release classification/quotes; retain outside-pool original candidates."""
    from backend.services.advisory_model_first.economic_context_consumer_v1 import load_economic_context_source_v1, build_economic_context_rows_v1
    from backend.services.advisory_model_first.economic_sector_price_source_v1 import structural_crosswalk_v1, sector_quotes_v1, sector_dynamic_rows_v1
    from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
    root = Path(root)
    checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    destination = base/"sector_information_inputs_v1"
    profile_path,authority_root = Path(profile_path),Path(authority_root)
    reference = EvidenceReferenceV1.model_validate(crosswalk_ref)
    profile_sha = file_sha(profile_path)
    spec = dict(inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),profile_path=str(profile_path),profile_sha256=profile_sha,
        authority_root=str(authority_root),crosswalk_ref=reference.model_dump(mode="json"),original_candidates_preserved=True,
        E_outcomes_used=False,other_roles_same_window_outcomes_already_seen=True,physical_fit_count=0,decision_use="NAVIGATION_ONLY")
    publish_json(destination/"spec.json",spec)
    receipt_path = destination/"receipt.json"
    receipt = checked_information_checkpoint(root, package_id=package_id, input_directory=destination.name)
    if receipt is not None:
        return receipt
    source = load_economic_context_source_v1(profile_path=profile_path,profile_sha256=profile_sha,
        universe_selection={"mode":"stock_universe","pool_ids":[]},authority_root=authority_root)
    candidates = pd.read_parquet(base/"candidates.parquet")
    context,context_receipt = build_economic_context_rows_v1(candidates=candidates,source=source)
    for name in KEY[:2]:
        context[name] = pd.to_datetime(context[name])
    inputs = pd.read_parquet(base/"d_features.parquet").merge(context,on=KEY,validate="one_to_one",sort=False)
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    pins = profile["components"]["sector_context_pins"]
    candidate_root = Path(profile["controller_paths"]["candidate_root"])
    h5 = candidate_root/"components/factor_h5_static_candidate_v2/sector_data.h5"
    code_map_path = candidate_root/pins["component_root"]/pins["code_map_file"]
    crosswalk_path = Path(reference.artifact_uri)
    taxonomy_path = authority_root/"taxonomy_catalog.json"
    snapshot = json.loads(crosswalk_path.read_text(encoding="utf-8"))
    expected = {profile_path:profile_sha,h5:pins["sector_data_sha256"],code_map_path:pins["code_map_sha256"],
        crosswalk_path:reference.sha256,taxonomy_path:snapshot["taxonomy_catalog_sha256"]}
    for path,digest in expected.items():
        if file_sha(path) != digest:
            raise ValueError("original sector pinned source hash differs")
    crosswalk = structural_crosswalk_v1(snapshot,code_map=json.loads(code_map_path.read_text(encoding="utf-8")),
        taxonomy=json.loads(taxonomy_path.read_text(encoding="utf-8")))
    calendar = pd.to_datetime(json.loads((root/"calendar.json").read_text(encoding="utf-8")))
    with pd.HDFStore(h5,mode="r") as store:
        chunks = store.select("data",where=[f"datetime >= '{calendar.min().date()}'",f"datetime <= '{inputs[KEY[0]].max().date()}'"],
            columns=["l2_code_id","sw2_close"],chunksize=100000)
        quotes = sector_quotes_v1(chunks,namespace=set(v for v in crosswalk.values() if v is not None),
            first_day=calendar.min(),last_day=inputs[KEY[0]].max())
    sector = sector_dynamic_rows_v1(rows=inputs,quotes=quotes,calendar=calendar,crosswalk=crosswalk)
    sector_rows = inputs.merge(sector,on=KEY,validate="one_to_one",sort=False)
    additional = prepare_original_information_inputs(root,package_id=package_id)
    flow_path = base/"information_inputs_v1/M6.parquet"
    raw_path = base/"information_inputs_v1/M20.parquet"
    if (file_sha(flow_path) != additional["files"][flow_path.name] or file_sha(raw_path) != additional["files"][raw_path.name]):
        raise ValueError("original sector joint source binding differs")
    rows = {"M1":sector_rows,"CONTEXT":context}
    for module,prefix,model,kwargs in (
        ("economic_sector_moneyflow_v1","sector_moneyflow","M19",dict(sector_rows=sector_rows,moneyflow_rows=pd.read_parquet(flow_path),candidates=candidates,calendar=calendar)),
        ("economic_sector_parent_raw_v1","sector_parent_raw","M22",dict(sector_rows=sector_rows,raw_rows=pd.read_parquet(raw_path),candidates=candidates,calendar=calendar)),
        ("economic_sector_path_price_v1","sector_path_price","M25",dict(base_rows=sector_rows,quotes=quotes,candidates=candidates,calendar=calendar,crosswalk=crosswalk))):
        native_module = import_module("backend.services.advisory_model_first."+module)
        rows[model] = getattr(native_module,prefix+"_rows_v1")(**kwargs)
    source.verify_unchanged()
    for path,digest in expected.items():
        if file_sha(path) != digest:
            raise ValueError("original sector source changed during read")
    files,summary = {},{}
    for model,frame in rows.items():
        publish_bytes(destination/(model+".parquet"),_parquet(frame))
        files[model+".parquet"] = file_sha(destination/(model+".parquet"))
        summary[model] = dict(original_rows=len(frame),source_status_counts={name:{str(k):int(v) for k,v in frame[name].value_counts(dropna=False).items()}
            for name in frame if name.endswith("_status") or name=="classification_unknown_reason"})
        if progress:
            progress(dict(event="ORIGINAL_SECTOR_INPUT_READY",model=model,**summary[model]))
    receipt = dict(spec_sha256=file_sha(destination/"spec.json"),inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),
        files=files,summaries=summary,source_sha256={str(p):v for p,v in expected.items()},context=context_receipt,
        E_outcomes_used=False,physical_fit_count=0,database_written=False,native_identity="UNPROVEN",sealed_read=False,decision_use="NAVIGATION_ONLY")
    publish_json(receipt_path,receipt)
    return receipt


def prepare_original_breadth_information(root, *, package_id):
    """Native per-D two-session breadth, not a pct_change across missing days."""
    from backend.services.advisory_model_first.shared_feature_builder import _build_market_features
    from backend.services.advisory_model_first.economic_breadth_state_price_v1 import breadth_state_rows_v1
    root = Path(root)
    checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    destination = base/"breadth_information_inputs_v1"
    receipt_path = destination/"receipt.json"
    receipt = checked_information_checkpoint(root, package_id=package_id, input_directory=destination.name)
    if receipt is not None:
        return receipt
    candidates = pd.read_parquet(base/"candidates.parquet")
    calendar = pd.to_datetime(json.loads((root/"calendar.json").read_text(encoding="utf-8")))
    needed = sorted({day for decision in candidates[KEY[0]].unique() for day in calendar[max(0,calendar.get_loc(decision)-19):calendar.get_loc(decision)+1]})
    pairs = {day:(calendar[calendar.get_loc(day)-1],day) for day in needed if calendar.get_loc(day)>0}
    dates = sorted({day for pair in pairs.values() for day in pair})
    publish_json(destination/"spec.json",dict(inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),
        market_dates=[str(d.date()) for d in dates],recipe="ORIGINAL_COMMON_CORE_PER_D_TWO_SESSIONS",
        maximum_source_rows=len(dates)*10000,physical_fit_count=0,E_outcomes_used=False,
        other_roles_same_window_outcomes_already_seen=True,source_evidence="CURRENT_DATABASE_NON_VINTAGE"))
    with ConsumerReadonlySession().connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT price.trade_date,price.ts_code AS instrument,
                price.close_li/1000.0*adj.adj_factor AS close
                FROM market.kline_daily_raw price JOIN market.stock_basic stock ON stock.ts_code=price.ts_code
                JOIN market.sector_data eligible ON eligible.trade_date=price.trade_date AND eligible.ts_code=price.ts_code
                LEFT JOIN market.adj_factor adj ON adj.trade_date=price.trade_date AND adj.ts_code=price.ts_code
                  AND adj.trade_date BETWEEN %s AND %s
                WHERE price.trade_date=ANY(%s::date[]) AND price.trade_date BETWEEN %s AND %s
                  AND stock.list_date<=price.trade_date AND (stock.delist_date IS NULL OR stock.delist_date>price.trade_date)
                  AND (price.ts_code LIKE '%%.SH' OR price.ts_code LIKE '%%.SZ')
                ORDER BY price.trade_date,price.ts_code LIMIT %s""",
                (dates[0].date(),dates[-1].date(),[d.date() for d in dates],dates[0].date(),dates[-1].date(),len(dates)*10000+1))
            fetched = cursor.fetchall()
    if len(fetched)>len(dates)*10000:
        raise ValueError("original breadth source exceeded its bounded row budget")
    market = pd.DataFrame(fetched,columns=["trade_date","instrument","close"])
    market["trade_date"] = pd.to_datetime(market.trade_date)
    if market.duplicated(["trade_date","instrument"]).any() or not market.trade_date.isin(dates).all():
        raise ValueError("original breadth source has duplicate/foreign keys")
    records = []
    for decision,(prior,_) in pairs.items():
        subset = market.loc[market.trade_date.isin((prior,decision))].set_index(["trade_date","instrument"]).rename_axis(["datetime","instrument"])
        values = _build_market_features(subset.assign(limit_up=np.nan))
        ratio = values.loc[decision,"market_up_ratio"] if decision in values.index else np.nan
        position = calendar.get_loc(decision)
        # This is an aggregate-series carrier, never a selected stock or its return.
        records.append(dict(zip(KEY,(decision,calendar[position+1],"000300.SH"),strict=True))|
            dict(market_up_ratio=ratio,feature_visible_through=decision))
    breadth = pd.DataFrame(records)
    frame = breadth_state_rows_v1(candidates=candidates,breadth=breadth,calendar=calendar)
    files = {}
    for name,table in (("M10.parquet",frame),("breadth_source.parquet",breadth)):
        publish_bytes(destination/name,_parquet(table))
        files[name] = file_sha(destination/name)
    receipt = dict(inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),spec_sha256=file_sha(destination/"spec.json"),files=files,
        original_rows=len(frame),source_market_rows=len(market),raw_source_sha256=hashlib.sha256(_parquet(market)).hexdigest(),
        status_counts={str(k):int(v) for k,v in frame.breadth_state_feature_status.value_counts().items()},
        physical_fit_count=0,E_outcomes_used=False,database_written=False,sealed_read=False,decision_use="NAVIGATION_ONLY")
    publish_json(receipt_path,receipt)
    return receipt


def prepare_original_scale_information(root, *, package_id):
    """The existing M21 recipe recovers approximate moments from raw/norm pairs.

    It does not require or reconstruct full-universe members or leg ranks.
    """
    from backend.services.advisory_model_first.economic_parent_scale_state_v1 import parent_scale_state_rows_v1
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    destination = base/"scale_information_inputs_v1"
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    for item in inventory["inventory"]["items"]:
        if item["family"] != "advisory_price_research_campaign_r2_20261004":
            continue
        from backend.services.advisory_model_first.cross_package_validation_adapters_v1 import _checked_member
        body,_ = _checked_member(item,"metadata.json")
        if "parent_scale_state_features" in body["recipe"]:
            break
    else:
        raise ValueError("original M21 frozen recipe absent")
    original_plan_path = Path(item["manifest_ref"]).parent.parent/"preregistered/plan.json"
    if file_sha(original_plan_path) != item["metadata"]["preregistered/plan.json"]["sha256"]:
        raise ValueError("original M21 dtype/normalization plan changed")
    original_plan = json.loads(original_plan_path.read_text(encoding="utf-8"))
    spec = dict(inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),original_plan_sha256=file_sha(original_plan_path),
        score_dtypes=original_plan["score_dtypes"],parent_normalization=original_plan["parent_normalization"],
        state_semantics="APPROXIMATE_FROZEN_SCALE_STATE_NOT_ORIGINAL_NATIVE_METADATA",physical_fit_count=0,E_outcomes_used=False,
        original_native_recipe_reused=True,full_universe_or_leg_ranks_reconstructed=False,
        correction_of_initial_source_requirement="M21_NATIVE_RECIPE_NEEDS_CAPTURED_RAW_NORM_PAIRS_NOT_FULL_UNIVERSE_STATISTICS")
    publish_json(destination/"spec.json",spec)
    receipt_path = destination/"receipt.json"
    receipt = checked_information_checkpoint(root, package_id=package_id, input_directory=destination.name)
    if receipt is not None:
        return receipt
    original_spec = json.loads((base/"input_spec.json").read_text(encoding="utf-8"))
    original_sources = json.loads((base/"original_sources.json").read_text(encoding="utf-8"))
    inputs_receipt = json.loads((base/"inputs_receipt.json").read_text(encoding="utf-8"))
    if file_sha(base/"original_sources.json") != inputs_receipt["files"]["original_sources.json"]:
        raise ValueError("original scale score source changed")
    candidates = pd.read_parquet(base/"candidates.parquet")
    calendar = pd.to_datetime(json.loads((root/"calendar.json").read_text(encoding="utf-8")))
    following = dict(zip(calendar[:-1],calendar[1:],strict=True))
    records = []
    for source in original_sources:
        day = pd.Timestamp(source["decision_date"])
        for score in source["scores"]:
            row = {**dict(zip(KEY,(day,following[day],score["symbol"]),strict=True)),"trade_date":day,"package_id":package_id,
                   "manifest_sha256":original_spec["package_manifest_sha256"]}
            for name in original_spec["component_roles"].values():
                row["raw__"+name],row["norm__"+name] = score["component_scores"][name]["raw_score"],score["component_scores"][name]["normalized_score"]
            records.append(row)
    raw_path = base/"information_inputs_v1/M20.parquet"
    raw_receipt = prepare_original_information_inputs(root,package_id=package_id)
    if file_sha(raw_path) != raw_receipt["files"][raw_path.name]:
        raise ValueError("original scale base raw inputs changed")
    rows = parent_scale_state_rows_v1(base_rows=pd.read_parquet(raw_path),rankings=pd.DataFrame(records),candidates=candidates,
        calendar=calendar,component_raw_columns=original_plan["component_raw_columns"],package_id=package_id,
        package_manifest_sha256=original_spec["package_manifest_sha256"],score_dtypes=original_plan["score_dtypes"])
    publish_bytes(destination/"M21.parquet",_parquet(rows))
    receipt = dict(spec_sha256=file_sha(destination/"spec.json"),inputs_receipt_sha256=file_sha(base/"inputs_receipt.json"),
        files={"M21.parquet":file_sha(destination/"M21.parquet")},original_rows=len(rows),
        status_counts={str(k):int(v) for k,v in rows.parent_scale_state_feature_status.value_counts().items()},
        state_semantics=spec["state_semantics"],physical_fit_count=0,E_outcomes_used=False,database_written=False,sealed_read=False,
        decision_use="NAVIGATION_ONLY",native_identity="UNPROVEN")
    publish_json(receipt_path,receipt)
    return receipt
