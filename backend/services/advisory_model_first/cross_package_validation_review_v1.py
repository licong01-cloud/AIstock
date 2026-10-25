"""Original named-leg review-clock consumers, not a fixed-five-session proxy."""
from __future__ import annotations

from datetime import date
from dataclasses import asdict
from decimal import Decimal
import json
from pathlib import Path

import numpy as np
import pandas as pd

from backend.services.advisory_model_first.cross_package_validation_contracts_v1 import (
    canonical_sha, file_sha, publish_bytes, publish_json, readonly_connection,
)
from backend.services.advisory_model_first.cross_package_validation_inputs_v1 import _parquet, checked_plan
from backend.services.advisory_model_first.economic_entry_labels import KEY


class OriginalWslLightgbmPredictor:
    """Same frozen Booster on the existing runtime, CPU1, no fit/install/service."""
    def __init__(self, path, digest):
        self.path, self.digest = Path(path), digest
        if not self.path.is_absolute() or self.path.drive.upper() == "C:" or file_sha(self.path) != digest:
            raise ValueError("frozen WSL Booster path/hash differs")
        self.names = None
        with self.path.open(encoding="utf-8") as stream:
            for index,line in enumerate(stream):
                if index >= 100 or line.startswith("Tree="):
                    break
                if line.startswith("feature_names="):
                    self.names = line.strip().split("=",1)[1].split()
        if not self.names:
            raise ValueError("original frozen Booster has no feature-name header")

    def feature_name(self):
        return list(self.names)

    def predict(self, matrix, *, num_threads=2):
        import subprocess
        from backend.services.strategy_package.live_inference import WslStrategyPackageInferenceProvider, win_to_wsl_path
        if (not isinstance(matrix,pd.DataFrame) or list(matrix.columns) != self.names or len(matrix)>5000
                or not np.isfinite(matrix.to_numpy(dtype=float)).all() or file_sha(self.path) != self.digest):
            raise ValueError("original frozen WSL prediction schema/budget/hash differs")
        provider = WslStrategyPackageInferenceProvider()
        payload = json.dumps(dict(path=win_to_wsl_path(str(self.path)),sha256=self.digest,
            names=self.names,values=matrix.to_numpy(dtype=float).tolist()),allow_nan=False)
        script = "\n".join(("import json,sys,hashlib", "import lightgbm as lgb", "import pandas as pd",
            "p=json.load(sys.stdin)", "assert lgb.__version__=='4.6.0'",
            "assert hashlib.sha256(open(p['path'],'rb').read()).hexdigest()==p['sha256']",
            "model=lgb.Booster(model_file=p['path'])", "assert model.feature_name()==p['names']",
            "values=model.predict(pd.DataFrame(p['values'],columns=p['names']),num_threads=1)",
            "assert hashlib.sha256(open(p['path'],'rb').read()).hexdigest()==p['sha256']",
            "print(json.dumps(dict(values=values.tolist(),physical_fit_count=0,runtime=lgb.__version__),allow_nan=False))"))
        command = (f"source {provider.conda_sh} && conda activate {provider.conda_env} && "
            "TMPDIR=/mnt/x/AIstock_temp/advcp_20261009 TEMP=/mnt/x/AIstock_temp/advcp_20261009 "
            "TMP=/mnt/x/AIstock_temp/advcp_20261009 PYTHONDONTWRITEBYTECODE=1 CUDA_VISIBLE_DEVICES= "
            "OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 python -c "+provider._quote(script))
        result = subprocess.run(["wsl","-d",provider.distro,"bash","-lc",command],input=payload,
            capture_output=True,text=True,encoding="utf-8",errors="strict",timeout=120,check=False)
        if result.returncode != 0:
            raise ValueError("original frozen WSL Booster failed: "+result.stderr[-1000:])
        body = json.loads(result.stdout)
        values = np.asarray(body["values"],dtype=float)
        if (body["physical_fit_count"] != 0 or body["runtime"] != "4.6.0" or values.shape != (len(matrix),)
                or not np.isfinite(values).all() or file_sha(self.path) != self.digest):
            raise ValueError("original frozen WSL predictions violate identity/shape")
        return values


def project_original_review_scores(*, scores, decision, target, component_roles, terminal_weights):
    """Keep original Top20/40 and named inputs; never derive full-universe leg ranks."""
    if (type(decision) is not date or type(target) is not date or decision >= target
            or set(component_roles) != {"lstm", "fund"} or len(set(component_roles.values())) != 2
            or set(terminal_weights) != set(component_roles.values()) or len(scores) < 20):
        raise ValueError("original review source has incompatible clock, roles or Top20")
    weights = np.asarray(list(terminal_weights.values()), dtype=float)
    if not np.isfinite(weights).all() or (weights <= 0).any() or not np.isclose(weights.sum(), 1., atol=1e-10, rtol=0):
        raise ValueError("original review terminal weights differ")
    candidates, rankings, raw_legs = [], [], []
    symbols = set()
    for position, row in enumerate(scores, 1):
        if (type(row["rank"]) is not int or row["rank"] != position or row["symbol"] in symbols
                or isinstance(row["score"], bool) or not np.isfinite(row["score"])):
            raise ValueError("original review rank/score/uniqueness differs")
        symbols.add(row["symbol"])
        key = dict(zip(KEY, (pd.Timestamp(decision), pd.Timestamp(target), row["symbol"]), strict=True))
        if position <= 40 and len(scores) >= 40:
            rankings.append({**key, "selection_effective_rank": position, "combined_score": row["score"]})
        if position > 20:
            continue
        legs = row["component_scores"]
        if set(legs) != set(terminal_weights):
            raise ValueError("original review named components differ")
        norm, raw = {}, {}
        for role, name in component_roles.items():
            values = [legs[name][field] for field in ("weight", "raw_score", "normalized_score")]
            if any(isinstance(v, bool) for v in values) or not np.isfinite(values).all():
                raise ValueError("original review named component values differ")
            if not np.isclose(legs[name]["weight"], terminal_weights[name], atol=1e-10, rtol=0):
                raise ValueError("original review weight does not match frozen roles")
            norm["norm__"+name] = legs[name]["normalized_score"]
            raw["parent_"+role+"_raw_score_D"] = legs[name]["raw_score"]
        if not np.isclose(sum(norm["norm__"+name]*weight for name, weight in terminal_weights.items()), row["score"], atol=1e-8, rtol=0):
            raise ValueError("original review combined score differs from frozen legs")
        candidates.append({**key, "selection_effective_rank": position, "candidate_group_size": 20,
            "combined_score": row["score"], **norm})
        raw_legs.append({**key, **raw})
    return pd.DataFrame(candidates), pd.DataFrame(rankings, columns=[*KEY, "selection_effective_rank", "combined_score"]), pd.DataFrame(raw_legs)


def prepare_original_review_inputs(root, *, package_id, component_roles, terminal_weights, progress=None):
    from backend.services.advisory_model_first.economic_common_core_daily_source_v1 import EconomicCommonCoreReadonlyDailySourceV1
    from backend.services.strategy_package.selection_artifact import StrategyPackageSelectionArtifactRepository
    root = Path(root)
    plan = checked_plan(root)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    package = next(p for p in inventory["packages"] if p["package_id"] == package_id and p["disposition"] == "IN_MATRIX")
    catalog_path = root/"selection_source_catalog_v2.json"
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    if catalog["plan_sha256"] != file_sha(root/"plan.json") or catalog["inventory_sha256"] != plan["inventory_sha256"]:
        raise ValueError("review original source catalog binding differs")
    base = root/"review_transfer_v1"/package_id
    spec = dict(plan_sha256=file_sha(root/"plan.json"), source_catalog_sha256=file_sha(catalog_path),
        package_id=package_id, package_manifest_sha256=package["manifest_sha256"], component_roles=component_roles,
        terminal_weights=terminal_weights, original_D_axis=plan["decision_dates"],
        clock="D_ONLY_FEATURE_PREPARATION_NOT_AN_EPISODE_CLOCK", physical_fit_count=0,
        other_roles_same_window_outcomes_already_seen=True, native_identity="UNPROVEN", sealed_read=False)
    spec_path = base/"input_spec_v2.json"
    publish_json(spec_path, spec)
    complete = base/"inputs_receipt.json"
    if complete.is_file():
        body = json.loads(complete.read_text(encoding="utf-8"))
        if body["input_spec_sha256"] != file_sha(spec_path):
            old_path = base/"input_spec.json"
            if not old_path.is_file() or body["input_spec_sha256"] != file_sha(old_path):
                raise ValueError("review input specification changed")
            old = json.loads(old_path.read_text(encoding="utf-8"))
            if {k:v for k,v in old.items() if k != "clock"} != {k:v for k,v in spec.items() if k != "clock"}:
                raise ValueError("review older D-only input identity differs")
            # Preserve the first D-only checkpoint; it read no policy labels.
            publish_json(base/"role_clock_correction.json", dict(original_input_spec_clock_was_role_overgeneralization=True,
                prepared_role="D_ONLY_FEATURES_NO_LABELS", early_entry_policy="ORIGINAL_20_REVIEW_STOP_RANK_POLICY",
                value_anchor_context_R2="ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY", labels_read=False,
                physical_fit_count=0, original_input_files_unchanged=True))
        for name, digest in body["files"].items():
            if file_sha(base/name) != digest:
                raise ValueError("review immutable inputs changed")
        return body
    calendar = [date.fromisoformat(d) for d in json.loads((root/"calendar.json").read_text(encoding="utf-8"))]
    selected = [row for row in catalog["selected"] if row["package_id"] == package_id]
    repository = StrategyPackageSelectionArtifactRepository(conn_factory=readonly_connection)
    packets, contexts, raws, originals, unavailable = [], [], [], [], []
    by_day = {row["decision_date"]: row for row in selected}
    for day in plan["decision_dates"]:
        source = by_day.get(day)
        if source is None:
            unavailable.append(dict(decision_date=day, reason="NO_EXISTING_FROZEN_SELECTION_SCORES"))
            continue
        artifact = repository.get(package_id=package_id, manifest_sha256=package["manifest_sha256"],
            trade_date=date.fromisoformat(source["trade_date"]), data_source=source["data_source"],
            runtime_config_hash=source["runtime_config_hash"])
        scores = artifact.scores_json
        if (artifact.artifact_id != source["artifact_id"] or artifact.artifact_sha256 != source["artifact_sha256"]
                or canonical_sha(scores) != source["artifact_sha256"] or len(scores) != source["score_count"]
                or artifact.metadata.get("score_trade_date") != day or artifact.metadata.get("cutoff_date") != day):
            raise ValueError("review original Selection artifact changed")
        decision = date.fromisoformat(day)
        position = calendar.index(decision)
        target = calendar[position+1]
        if str(target) != source["trade_date"] or position < 19:
            raise ValueError("review D/T or original twenty-session history differs")
        candidates, context, raw = project_original_review_scores(scores=scores, decision=decision, target=target,
            component_roles=component_roles, terminal_weights=terminal_weights)
        packets.append(dict(candidates=candidates, calendar=calendar[position-19:position+2],
            component_roles=component_roles, terminal_weights=terminal_weights))
        contexts.append(context)
        raws.append(raw)
        originals.append(dict(artifact_id=artifact.artifact_id, artifact_sha256=artifact.artifact_sha256,
            decision_date=day, native_capture_created=False, scores=scores))
        if context.empty:
            unavailable.append(dict(decision_date=day, reason="ORIGINAL_TOP40_UNAVAILABLE", original_N=len(scores)))
    publish_json(base/"original_sources.json", originals)
    source = EconomicCommonCoreReadonlyDailySourceV1(connection_context_factory=readonly_connection)
    frames = []
    for start in range(0, len(packets), 4):
        chunk = base/("d_batch_"+str(start))
        chunk_spec = canonical_sha([str(packet["calendar"][-2]) for packet in packets[start:start+4]])
        if (chunk/"receipt.json").is_file():
            receipt = json.loads((chunk/"receipt.json").read_text(encoding="utf-8"))
            if receipt["chunk_sha256"] != chunk_spec or receipt["features_sha256"] != file_sha(chunk/"features.parquet"):
                raise ValueError("review D input checkpoint changed")
            frame = pd.read_parquet(chunk/"features.parquet")
        else:
            outputs = source.load_timing_batch(packets=packets[start:start+4])
            frame = pd.concat([pair[0] for pair in outputs], ignore_index=True)
            publish_bytes(chunk/"features.parquet", _parquet(frame))
            receipt = dict(chunk_sha256=chunk_spec, features_sha256=file_sha(chunk/"features.parquet"),
                original_receipts=[pair[1] for pair in outputs], database_written=False, outcomes_read=False)
            publish_json(chunk/"receipt.json", receipt)
        frames.append(frame)
        if progress:
            progress(dict(event="ORIGINAL_REVIEW_D_INPUT_BATCH", package_id=package_id, completed_days=min(start+4,len(packets)),
                total_days=len(packets), physical_fit_count=0))
    files = {"d_features.parquet": pd.concat(frames, ignore_index=True),
        "candidates.parquet": pd.concat([p["candidates"] for p in packets], ignore_index=True),
        "rankings.parquet": pd.concat(contexts, ignore_index=True), "raw_legs.parquet": pd.concat(raws, ignore_index=True)}
    for name, frame in files.items():
        publish_bytes(base/name, _parquet(frame))
    receipt = dict(input_spec_sha256=file_sha(spec_path),
        files={name: file_sha(base/name) for name in (*files, "original_sources.json")},
        original_days=len(plan["decision_dates"]), D_input_days=len(packets), Top40_context_days=sum(not f.empty for f in contexts),
        unavailable=unavailable, full_universe_leg_ranks_recovered=False, physical_fit_count=0,
        database_written=False, outcomes_read=False, native_identity="UNPROVEN")
    publish_json(complete, receipt)
    return receipt


def checked_review_inputs(root, *, package_id):
    """Validate every immutable parent member before using any child checkpoint."""
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    receipt = json.loads((base/"inputs_receipt.json").read_text(encoding="utf-8"))
    specs = [base/name for name in ("input_spec.json", "input_spec_v2.json")]
    bound = next((path for path in specs if path.is_file() and file_sha(path) == receipt["input_spec_sha256"]), None)
    if bound is None:
        raise ValueError("review parent input specification changed")
    spec = json.loads(bound.read_text(encoding="utf-8"))
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    package = next(p for p in inventory["packages"] if p["package_id"] == package_id and p["disposition"] == "IN_MATRIX")
    if (spec["plan_sha256"] != file_sha(root/"plan.json") or spec["package_id"] != package_id
            or spec["package_manifest_sha256"] != package["manifest_sha256"]
            or spec["original_D_axis"] != plan["decision_dates"] or spec["physical_fit_count"] != 0
            or spec["source_catalog_sha256"] != file_sha(root/"selection_source_catalog_v2.json")
            or spec["sealed_read"] is not False or receipt["outcomes_read"] is not False
            or receipt["physical_fit_count"] != 0 or receipt["database_written"] is not False):
        raise ValueError("review parent plan, package or D clock changed")
    required = {"d_features.parquet", "candidates.parquet", "rankings.parquet", "raw_legs.parquet", "original_sources.json"}
    if set(receipt["files"]) != required:
        raise ValueError("review parent members differ")
    for name, digest in receipt["files"].items():
        if file_sha(base/name) != digest:
            raise ValueError("review immutable parent inputs changed")
    return receipt


def cached_review_predictions(destination, *, spec_path, rows, policy_role):
    """Exact retry is validation/readback, not repeated native model inference."""
    destination = Path(destination)
    receipt_path = destination/"receipt.json"
    if not receipt_path.is_file():
        return None
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    path = destination/"predictions.parquet"
    if (receipt["spec_sha256"] != file_sha(spec_path) or receipt["predictions_sha256"] != file_sha(path)
            or receipt["original_rows"] != len(rows) or receipt["physical_fit_count"] != 0
            or receipt["E_outcomes_used"] is not False or receipt["policy_role"] != policy_role):
        raise ValueError("review prediction checkpoint identity changed")
    frame = pd.read_parquet(path)
    columns = [*KEY, "selection_effective_rank"]
    if (not frame[columns].equals(rows[columns].reset_index(drop=True))
            or not frame.status.map(lambda value:isinstance(value, str)).all()):
        raise ValueError("review prediction checkpoint keys or status changed")
    return frame


def review_price_queries(root, *, package_id):
    """D-visible corporate-action reference, then actual T price; never an E feature."""
    from backend.services.advisory_model_first.realtime_feature_source import _target_raw_price_multiplier
    from backend.services.advisory_model_first.economic_entry_sources import _records
    from backend.services.advisory_model_first.errors import AdvisoryModelFirstError
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    checked_review_inputs(root, package_id=package_id)
    query_path = base/"T_queries.parquet"
    quote_path = root/"legacy_score_transfer_v1/entry_coordinates/quotes.parquet"
    quote_receipt = json.loads(quote_path.with_name("receipt.json").read_text(encoding="utf-8"))
    if quote_receipt["quotes_sha256"] != file_sha(quote_path) or quote_receipt["until"] > plan["settlement_cutoff"]:
        raise ValueError("review T snapshot identity or date bound differs")
    if (base/"T_queries_receipt.json").is_file():
        bound = json.loads((base/"T_queries_receipt.json").read_text(encoding="utf-8"))
        if (bound["queries_sha256"] != file_sha(query_path) or bound["inputs_sha256"] != file_sha(base/"inputs_receipt.json")
                or bound["quote_source_sha256"] != file_sha(quote_path)
                or bound["actions_file_sha256"] != file_sha(base/"D_visible_actions.parquet")):
            raise ValueError("review T query input binding changed")
        # actions_content_sha256 binds pre-serialization DB Decimal text. Arrow
        # may normalize Decimal scale; the exact immutable file SHA is the
        # round-trip identity, not a second re-encoded scalar hash.
        return pd.read_parquet(query_path)
    roster = pd.read_parquet(base/"candidates.parquet")
    features = pd.read_parquet(base/"d_features.parquet")
    prices = pd.read_parquet(quote_path).set_index(["trade_date", "instrument"])
    with readonly_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""SELECT ts_code AS instrument,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,
                cash_div,cash_div_tax,imp_ann_date,ex_date FROM market.dividend WHERE ts_code=ANY(%s)
                AND ex_date BETWEEN %s AND %s AND div_proc='实施'
                ORDER BY ts_code,ex_date,imp_ann_date DESC NULLS LAST,end_date DESC,ann_date DESC LIMIT 5001""",
                (sorted(set(roster.instrument)), roster[KEY[1]].min().date(), roster[KEY[1]].max().date()))
            dividends = pd.DataFrame(cursor.fetchall(), columns=[col.name for col in cursor.description])
    if len(dividends) > 5000:
        raise ValueError("review D-visible action snapshot exceeds declared budget")
    by_target = {}
    for row in dividends.itertuples(index=False, name=None):
        by_target.setdefault((pd.Timestamp(row[-1]), row[0]), []).append(row[:-1])
    queries = []
    for row in roster.to_dict("records"):
        d, t, symbol = (row[name] for name in KEY)
        decision_quote = prices.loc[(d, symbol)] if (d, symbol) in prices.index else None
        target_quote = prices.loc[(t, symbol)] if (t, symbol) in prices.index else None
        item = {**{name:row[name] for name in KEY}, "target_reference_raw_cny": np.nan,
            "actual_gap_bps": np.nan, "reference_visible_through": d, "reference_status": "UNKNOWN_REFERENCE"}
        if decision_quote is not None and pd.notna(decision_quote.raw_close_cny) and decision_quote.raw_close_cny > 0:
            try:
                multiplier, _ = _target_raw_price_multiplier(symbol=symbol, decision_raw_close=float(decision_quote.raw_close_cny),
                    decision_adjustment_factor=decision_quote.adj_factor, rows=by_target.get((t,symbol),[]),
                    decision_as_of_trade_date=d.date())
            except AdvisoryModelFirstError as exc:
                item["reference_status"] = exc.reason_code
                queries.append(item)
                continue
            reference = float(decision_quote.raw_close_cny)*multiplier
            item.update(target_reference_raw_cny=reference, reference_status="D_VISIBLE_REFERENCE")
            if target_quote is not None and pd.notna(target_quote.raw_open_cny) and target_quote.raw_open_cny > 0:
                item["actual_gap_bps"] = (float(target_quote.raw_open_cny)/reference-1)*10000
        queries.append(item)
    query = features.merge(pd.DataFrame(queries), on=KEY, validate="one_to_one", sort=False)
    query = query.merge(roster.loc[:, [*KEY,"selection_effective_rank"]], on=KEY, validate="one_to_one", sort=False)
    publish_bytes(query_path, _parquet(query))
    publish_bytes(base/"D_visible_actions.parquet", _parquet(dividends))
    publish_json(base/"T_queries_receipt.json", dict(inputs_sha256=file_sha(base/"inputs_receipt.json"),
        queries_sha256=file_sha(query_path), quote_source_sha256=file_sha(quote_path),
        actions_content_sha256=canonical_sha(_records(dividends)), actions_file_sha256=file_sha(base/"D_visible_actions.parquet"),
        physical_fit_count=0, E_label_reader_ran=False, sealed_read=False, database_written=False,
        source_evidence="CURRENT_DB_D_VISIBLE_REFERENCE_AND_EXISTING_T_SNAPSHOT_NOT_NATIVE_CAPTURE"))
    return query


def load_original_core_review_units(item):
    """Only frozen weights/requests and original numerical entrypoints; zero fit."""
    from backend.services.advisory_model_first.cross_package_validation_adapters_v1 import _checked_member
    from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import ValueAnchorGapSupportV1, ValueAnchorEstimateV1
    family, directory = item["family"], Path(item["manifest_ref"]).parent
    units = []

    def booster(name):
        member = next(w for w in item["weights"] if w["file"] == name and w["artifact_kind"] == "PREDICTOR_WEIGHT")
        if file_sha(directory/name) != member["sha256"]:
            raise ValueError("original review LightGBM version/weight changed")
        try:
            import lightgbm as lgb
        except ModuleNotFoundError as exc:
            if exc.name != "lightgbm":
                raise
            return OriginalWslLightgbmPredictor(directory/name,member["sha256"])
        if lgb.__version__ != "4.6.0":
            raise ValueError("original review LightGBM runtime differs")
        return lgb.Booster(model_file=str(directory/name))

    def add(arm, fitted, query, policy_role, required):
        units.append(dict(model_id=item["model_id"], family=family, arm=arm, fitted=fitted, query=query,
            policy_role=policy_role, required_fields=tuple(required), manifest_sha256=item["manifest_sha256"],
            weight_files=[dict(file=w["file"], sha256=w["sha256"]) for w in item["weights"]]))

    if family == "advisory_aligned_entry_value_v3_20261002":
        from backend.services.advisory_model_first.economic_entry_aligned_contracts import AlignedEntryTrainingRequestV3
        from backend.services.advisory_model_first.economic_entry_aligned_training import AlignedEntryTrainingResultV3
        from backend.services.advisory_model_first.economic_entry_aligned_inference import predict_aligned_entry_nodes_v3
        body, _ = _checked_member(item, "model_metadata.json")
        request = AlignedEntryTrainingRequestV3.model_validate(body["training_request"])
        if request.request_sha256 != body["training_request_sha256"]:
            raise ValueError("original aligned frozen request changed")
        fitted = AlignedEntryTrainingResultV3(booster("return_model.txt"), booster("entry_loss_model.txt"), request,
            {k:tuple(v) for k,v in body["feature_bounds"].items()}, {int(k):v for k,v in body["price_support"].items()}, {}, pd.DataFrame())
        if any(tuple(model.feature_name()) != request.source_request.feature_names for model in (fitted.return_model,fitted.risk_model)):
            raise ValueError("original aligned feature names differ")
        add("model", fitted, lambda rows:predict_aligned_entry_nodes_v3(fitted=fitted,matrix=rows),
            "ORIGINAL_20_REVIEW_STOP_RANK_POLICY", request.source_request.feature_names)
    elif family in {"advisory_entry_information_v4_20261003", "advisory_entry_timing_v1_20261003"}:
        body, _ = _checked_member(item, "model_metadata.json")
        timing = "timing" in family
        if timing:
            from backend.services.advisory_model_first.economic_entry_timing_contracts_v1 import TimingTrainingRequestV1
            from backend.services.advisory_model_first.economic_entry_timing_training_v1 import TimingArmV1
            from backend.services.advisory_model_first.economic_entry_timing_inference_v1 import predict_timing_entry_nodes_v1
            request = TimingTrainingRequestV1.model_validate(body["request"])
            constructor, query = TimingArmV1, predict_timing_entry_nodes_v1
            required, bounds = request.candidate_names, "common_bounds"
        else:
            from backend.services.advisory_model_first.economic_entry_information_contracts import EconomicEntryInformationTrainingRequestV4
            from backend.services.advisory_model_first.economic_entry_information_training import EconomicInformationArmV4
            from backend.services.advisory_model_first.economic_entry_information_inference import predict_information_entry_nodes_v4
            request = EconomicEntryInformationTrainingRequestV4.model_validate(body["request"])
            constructor, query = EconomicInformationArmV4, predict_information_entry_nodes_v4
            required, bounds = request.feature_names, "feature_bounds"
        if request.request_sha256 != body["request_sha256"]:
            raise ValueError("original information/timing request changed")
        for arm, metadata in body["arms"].items():
            fitted = constructor(request, arm, tuple(metadata["feature_names"]), booster(arm.lower()+"_return.txt"),
                booster(arm.lower()+"_risk.txt"), {k:tuple(v) for k,v in metadata[bounds].items()},
                {int(k):v for k,v in metadata["price_support"].items()}, {})
            if any(tuple(model.feature_name()) != fitted.feature_names for model in (fitted.return_model,fitted.risk_model)):
                raise ValueError("original information/timing feature names differ")
            add(arm, fitted, lambda rows, f=fitted, q=query:q(fitted=f,matrix=rows),
                "ORIGINAL_20_REVIEW_STOP_RANK_POLICY", required)
    elif family == "advisory_value_anchor_v1_20261003":
        from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
        from backend.services.advisory_model_first.economic_value_anchor_training_v1 import ValueAnchorFitV1, value_anchor_predict_v1
        from backend.services.advisory_model_first.economic_value_anchor_inference_v1 import evaluate_value_anchor_price_v1
        body, _ = _checked_member(item, "metadata.json")
        fitted = ValueAnchorFitV1(booster("mean.txt"), booster("path.txt"), ValueAnchorEstimateV1(**body["constant"]),
            ValueAnchorGapSupportV1(tuple(tuple(p) for p in body["gap_support"]["intervals_bps"])), {})
        if body["feature_names"] != list(D_FEATURES) or any(tuple(m.feature_name()) != tuple(D_FEATURES) for m in (fitted.mean_model,fitted.path_model)):
            raise ValueError("original value-anchor feature names differ")
        for arm in ("model", "constant"):
            def query(rows, arm=arm):
                output = pd.DataFrame(dict(status=["UNKNOWN_INPUT_OR_SUPPORT"]*len(rows), expected_net_bps=np.nan,
                    downside_q90_bps=np.nan), index=rows.index)
                known = np.isfinite(rows.loc[:, [*D_FEATURES,"actual_gap_bps"]].to_numpy(dtype=float)).all(axis=1)
                if known.any():
                    estimates = value_anchor_predict_v1(fitted,rows.loc[known,D_FEATURES],arm=arm)
                    for index,estimate in zip(rows.index[known],estimates,strict=True):
                        point = evaluate_value_anchor_price_v1(estimate=estimate,price_cny=1+float(rows.loc[index,"actual_gap_bps"])/10000,
                            reference_cny=1.,support=fitted.gap_support)
                        output.loc[index] = [point.status,point.expected_net_bps,point.downside_q90_bps]
                return output
            add(arm,fitted,query,"ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY",(*D_FEATURES,"actual_gap_bps"))
    elif family == "advisory_price_research_campaign_r2_20261004":
        body, _ = _checked_member(item, "metadata.json")
        if body.get("model_id") not in {"M2","M3","M4"}:
            raise LookupError("original R2 additional information must be prepared, not filled with defaults")
        from backend.services.advisory_model_first.economic_price_campaign_models_v2 import PriceCampaignFitV2, fit_identity_v2
        from backend.services.advisory_model_first.economic_price_campaign_inference_v2 import campaign_nodes_v2
        from backend.services.advisory_model_first.economic_daily_feature_core_v1 import D_FEATURES
        support = ValueAnchorGapSupportV1(tuple(tuple(p) for p in body["support"]["intervals_bps"]))
        if fit_identity_v2(body["model_id"],body["recipe"],body["models"],support) != body["model_sha256"]:
            raise ValueError("original R2 frozen estimator identity differs")
        fitted = PriceCampaignFitV2(body["model_id"],body["recipe"],body["models"],support,{},body["model_sha256"])
        for arm in ("matched","candidate"):
            add(arm,fitted,lambda rows, arm=arm:campaign_nodes_v2(fitted=fitted,rows=rows,arm=arm),
                "ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY",(*D_FEATURES,"actual_gap_bps"))
    else:
        raise LookupError("this original review model requires its separate exact numerical consumer")
    return units


def predict_original_core_review(root, *, package_id, progress=None):
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    rows = review_price_queries(root,package_id=package_id)
    rows["query_gap_bps"] = rows.actual_gap_bps
    rows = rows.loc[rows.selection_effective_rank.le(5)].reset_index(drop=True)
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    units, unavailable = [], []
    for item in inventory["inventory"]["items"]:
        if item["role"] != "REVIEW_CLOCK_ENTRY":
            continue
        try:
            units.extend(load_original_core_review_units(item))
        except LookupError as exc:
            unavailable.append(dict(model_id=item["model_id"],family=item["family"],reason=str(exc)))
    specs = [{k:v for k,v in unit.items() if k not in {"fitted","query"}} for unit in units]
    spec = dict(plan_sha256=file_sha(root/"plan.json"),input_receipt_sha256=file_sha(base/"inputs_receipt.json"),
        T_query_sha256=file_sha(base/"T_queries.parquet"),units=specs,unavailable=unavailable,
        original_D_axis=plan["decision_dates"],physical_fit_count=0,E_outcomes_used=False,
        other_roles_same_window_outcomes_already_seen=True,decision_use="NAVIGATION_ONLY")
    publish_json(base/"core_query_spec_v1.json",spec)
    for unit in units:
        destination = base/"core_predictions"/unit["model_id"]/unit["arm"]
        if cached_review_predictions(destination, spec_path=base/"core_query_spec_v1.json", rows=rows,
                policy_role=unit["policy_role"]) is not None:
            if progress:
                progress(dict(event="ORIGINAL_REVIEW_CHECKPOINT_REUSED", family=unit["family"], arm=unit["arm"], physical_fit_count=0))
            continue
        projected = rows.loc[:, unit["required_fields"]]
        output = unit["query"](projected)
        if "model_action" in output:
            output["status"] = output.model_action.map({"TAKE":"ACCEPTABLE","SKIP":"AVOID","UNAVAILABLE":"UNKNOWN_INPUT_OR_SUPPORT"})
        if (not output.index.equals(rows.index) or output.status.isna().any()
                or not output.status.map(lambda v:isinstance(v,str)).all()):
            raise ValueError("original review nodes changed original keys or actions")
        result = pd.concat((rows.loc[:, [*KEY,"selection_effective_rank"]],output),axis=1)
        publish_bytes(destination/"predictions.parquet",_parquet(result))
        publish_json(destination/"receipt.json",dict(spec_sha256=file_sha(base/"core_query_spec_v1.json"),
            predictions_sha256=file_sha(destination/"predictions.parquet"),original_rows=len(rows),
            physical_fit_count=0,E_outcomes_used=False,policy_role=unit["policy_role"]))
        if progress:
            progress(dict(event="ORIGINAL_REVIEW_CORE_PREDICTIONS_READY",family=unit["family"],arm=unit["arm"],
                original_rows=len(rows),status_counts={str(k):int(v) for k,v in output.status.value_counts().items()},physical_fit_count=0))
    return specs


def audit_original_review_episodes(*, episodes, prices, calendar, cost_policy):
    """Execution/path audit after prediction; preserve every unavailable episode.

    The original policy builder owns exit timing. This consumer only verifies
    raw/adjusted endpoint parity, cost-once and the marks before that exit.
    """
    fields = ["trade_date", "instrument", "raw_open_cny", "raw_close_cny", "policy_price_per_raw_cny",
              "up_limit", "down_limit", "suspended", "tradability_unknown"]
    if prices.duplicated(fields[:2]).any() or episodes.duplicated(KEY).any():
        raise ValueError("original review endpoint/episode keys differ")
    for flag in ("suspended", "tradability_unknown"):
        if not prices[flag].map(lambda value:type(value) is bool).all():
            raise ValueError("original review trading flags must be explicit booleans")
    days = pd.DatetimeIndex(calendar)
    if days.has_duplicates or days.hasnans or not days.is_monotonic_increasing:
        raise ValueError("original review calendar must retain its ordered axis")
    quotes = prices.loc[:,fields].set_index(fields[:2]).to_dict("index")

    def positive(value):
        return not isinstance(value,(bool,np.bool_)) and value is not None and pd.notna(value) and np.isfinite(float(value)) and float(value)>0

    output = []
    for episode in episodes.to_dict("records"):
        row = {**episode, "settlement_status":"UNKNOWN", "settlement_reason":episode["label_status"],
               "baseline_net_bps":np.nan, "entry_executable":False}
        if episode["label_status"] != "MATURED":
            output.append(row)
            continue
        target, endpoint, symbol = pd.Timestamp(episode[KEY[1]]), pd.Timestamp(episode["effective_exit_date"]), episode[KEY[2]]
        if target not in days or endpoint not in days or endpoint <= target:
            raise ValueError("original review mature episode has an invalid clock")
        reason, marks = None, []
        for day in days[(days>=target)&(days<=endpoint)]:
            quote = quotes.get((day,symbol))
            if quote is None or quote["tradability_unknown"]:
                reason = "HOLDING_PRICE_OR_TRADABILITY_UNKNOWN"
                break
            if quote["suspended"]:
                if day in (target,endpoint):
                    reason = "ENDPOINT_SUSPENDED"
                    break
                continue  # Existing adjusted mark is carried, not a fabricated raw bar.
            if not all(positive(quote[name]) for name in ("raw_open_cny","policy_price_per_raw_cny")):
                reason = "HOLDING_OPEN_OR_COORDINATE_UNKNOWN"
                break
            raw, factor = float(quote["raw_open_cny"]), float(quote["policy_price_per_raw_cny"])
            if day in (target,endpoint):
                upper, lower = quote["up_limit"], quote["down_limit"]
                if not all(positive(value) for value in (upper,lower)):
                    reason = "ENDPOINT_LIMIT_UNKNOWN"
                    break
                if float(lower)>float(upper) or raw<float(lower)-1e-9 or raw>float(upper)+1e-9:
                    raise ValueError("original review endpoint contradicts its raw limits")
                if Decimal(str(raw)) % Decimal(".01"):
                    reason = "ENDPOINT_RAW_TICK_INVALID"
                    break
                if (day==target and raw>=float(upper)) or (day==endpoint and raw<=float(lower)):
                    reason = "ENDPOINT_OPEN_EXECUTION_UNPROVEN"
                    break
                recorded = episode["entry_price" if day==target else "exit_price"]
                if not positive(recorded) or not np.isclose(raw*factor,float(recorded),rtol=2**-22,atol=1e-8):
                    raise ValueError("original review raw-to-policy endpoint parity failed")
                if day==target:
                    row["entry_executable"] = True
            marks.append(raw*factor)
            if day != endpoint:  # An at-open exit must not consume that day's close.
                if not positive(quote["raw_close_cny"]):
                    reason = "HOLDING_CLOSE_UNKNOWN"
                    break
                marks.append(float(quote["raw_close_cny"])*factor)
        if reason is None and marks:
            net = (float(episode["exit_price"])*(1-cost_policy.sell_cost_bps/10000)
                   /(float(episode["entry_price"])*(1+cost_policy.buy_cost_bps/10000))-1)*10000
            if not np.isfinite(episode["net_return_bps"]) or not np.isclose(net,episode["net_return_bps"],rtol=1e-8,atol=1e-6):
                raise ValueError("original review cost-once return parity failed")
            row.update(settlement_status="AVAILABLE",settlement_reason=None,baseline_net_bps=net)
        else:
            row["settlement_reason"] = reason or "HOLDING_PATH_EMPTY"
        output.append(row)
    return pd.DataFrame(output)


def settle_original_core_review(root, *, package_id, progress=None):
    """Two separately frozen original policy clocks, never a five-session proxy."""
    from backend.services.advisory_model_first.policy_contracts import AdvisoryPolicyCostV1, transition_policy_from_payload
    from backend.services.advisory_model_first.policy_episode_labels import build_policy_episode_labels
    from backend.services.advisory_model_first.economic_value_anchor_contracts_v1 import COST, value_anchor_policy_v1, value_anchor_policy_sha256_v1
    from backend.services.advisory_model_first.economic_value_anchor_labels_v1 import value_anchor_shadow_inputs_v1
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    checked_review_inputs(root, package_id=package_id)
    spec = json.loads((base/"core_query_spec_v1.json").read_text(encoding="utf-8"))
    if spec["plan_sha256"] != file_sha(root/"plan.json") or spec["T_query_sha256"] != file_sha(base/"T_queries.parquet"):
        raise ValueError("original review prediction/source binding differs")
    inputs = json.loads((base/"inputs_receipt.json").read_text(encoding="utf-8"))
    for name,digest in inputs["files"].items():
        if file_sha(base/name) != digest:
            raise ValueError("original review D source changed before settlement")
    inventory = json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))
    economic = next(item for item in inventory["inventory"]["items"] if item["family"]=="advisory_economic_entry_value_v1_20261002")
    prepared = Path(economic["manifest_ref"]).parent.parent/"prepared"
    manifest = json.loads((prepared/"manifest.json").read_text(encoding="utf-8"))
    if file_sha(prepared/"identity.json") != manifest["files"]["identity.json"]["sha256"]:
        raise ValueError("original twenty-review policy identity differs")
    identity = json.loads((prepared/"identity.json").read_text(encoding="utf-8"))
    early_cost = AdvisoryPolicyCostV1.model_validate(identity["cost_policy"])
    if canonical_sha(identity["shadow_policy"]) != identity["shadow_policy_sha256"] or early_cost.policy_sha256 != identity["cost_policy_sha256"]:
        raise ValueError("original review policy/cost hash differs")
    policies = {
        "ORIGINAL_20_REVIEW_STOP_RANK_POLICY":(transition_policy_from_payload(identity["shadow_policy"]),identity["shadow_policy_sha256"],early_cost),
        "ORIGINAL_FIVE_EFFECTIVE_REVIEW_POLICY":(value_anchor_policy_v1(),value_anchor_policy_sha256_v1(),COST),
    }
    quote_path = root/"legacy_score_transfer_v1/settlement_coordinates/quotes.parquet"
    quote_receipt = json.loads(quote_path.with_name("receipt.json").read_text(encoding="utf-8"))
    if quote_receipt["until"] != plan["settlement_cutoff"] or quote_receipt["quotes_sha256"] != file_sha(quote_path):
        raise ValueError("original review settlement quote binding differs")
    prices = pd.read_parquet(quote_path).rename(columns={"adj_factor":"policy_price_per_raw_cny"})
    calendar = pd.to_datetime(json.loads((root/"calendar.json").read_text(encoding="utf-8")))
    calendar = calendar[calendar<=pd.Timestamp(plan["settlement_cutoff"])]
    market,cash,suspend,days = value_anchor_shadow_inputs_v1(prices=prices,calendar=calendar)
    candidates = pd.read_parquet(base/"candidates.parquet").loc[lambda f:f.selection_effective_rank.le(5)]
    rankings = pd.read_parquet(base/"rankings.parquet")
    destination = base/"original_policy_settlement_v1"
    benchmark_path = destination/"benchmark.parquet"
    if not benchmark_path.is_file():
        with readonly_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""SELECT c.cal_date,i.open FROM market.trading_calendar c
                    LEFT JOIN market.index_daily i ON i.trade_date=c.cal_date AND i.ts_code='000300.SH'
                    WHERE c.is_trading=TRUE AND c.cal_date BETWEEN %s AND %s ORDER BY c.cal_date""",
                    (days[0].date(),days[-1].date()))
                fetched = cursor.fetchall()
                if len(fetched)>500 or [pd.Timestamp(row[0]) for row in fetched] != list(days):
                    raise ValueError("original benchmark calendar differs")
                benchmark = pd.DataFrame(dict(datetime=days,open=[np.nan if row[1] is None else float(row[1]) for row in fetched]))
                if np.isinf(benchmark.open).any() or benchmark.open.dropna().le(0).any():
                    raise ValueError("original review benchmark price invalid")
        publish_bytes(benchmark_path,_parquet(benchmark))
    benchmark = pd.read_parquet(benchmark_path).set_index("datetime")
    freeze = dict(query_spec_sha256=file_sha(base/"core_query_spec_v1.json"),quotes_sha256=file_sha(quote_path),
        benchmark_sha256=file_sha(benchmark_path),original_policy_identity_sha256=file_sha(prepared/"identity.json"),
        policies={name:dict(payload=asdict(p),policy_sha256=digest,cost=c.model_dump(mode="json"),cost_sha256=c.policy_sha256)
                  for name,(p,digest,c) in policies.items()},original_D_axis=plan["decision_dates"],
        cutoff=plan["settlement_cutoff"],other_roles_same_window_outcomes_already_seen=True,
        physical_fit_count=0,decision_use="NAVIGATION_ONLY",source_evidence="RECOVERED_LIMITED",
        native_identity="UNPROVEN",sealed_read=False,database_written=False,
        source_files={name:file_sha(Path(__file__).parent/name) for name in (
            "policy_episode_labels.py","economic_value_anchor_labels_v1.py","economic_value_anchor_contracts_v1.py")})
    publish_json(destination/"spec.json",freeze)
    output = {}
    for name,(policy,digest,cost) in policies.items():
        known_dates = set(rankings[KEY[0]])
        valid = candidates.loc[candidates[KEY[0]].isin(known_dates)]
        episodes = build_policy_episode_labels(rankings=rankings,daily=market,benchmark_daily=benchmark if name.startswith("ORIGINAL_20") else cash,
            suspend_rows=suspend,trading_calendar=days,policy=policy,policy_sha256=digest,cost_policy=cost,
            request_identity={"request_id":plan["experiment_id"]+"_"+name},candidate_decision_dates=sorted(set(valid[KEY[0]])),candidate_depth=5).labels
        missing = candidates.loc[~candidates[KEY[0]].isin(known_dates),KEY].copy()
        missing["label_status"],missing["label_reason"] = "DATA_UNAVAILABLE","ORIGINAL_TOP40_CONTEXT_ABSENT"
        episodes = pd.concat((episodes,missing),ignore_index=True)
        labels = audit_original_review_episodes(episodes=episodes,prices=prices,calendar=days,cost_policy=cost)
        if set(map(tuple,labels[KEY].to_numpy())) != set(map(tuple,candidates[KEY].to_numpy())):
            raise ValueError("original review settlement lost candidate keys")
        labels = candidates.loc[:,KEY].merge(labels,on=KEY,validate="one_to_one",sort=False)
        publish_bytes(destination/name/"labels.parquet",_parquet(labels))
        receipt = dict(spec_sha256=file_sha(destination/"spec.json"),labels_sha256=file_sha(destination/name/"labels.parquet"),
            candidate_rows=len(labels),label_status_counts={str(k):int(v) for k,v in labels.label_status.value_counts().items()},
            settlement_status_counts={str(k):int(v) for k,v in labels.settlement_status.value_counts().items()},
            original_days=len(plan["decision_dates"]),physical_fit_count=0,activation_evidence=False)
        publish_json(destination/name/"receipt.json",receipt)
        output[name] = receipt
        if progress:
            progress(dict(event="ORIGINAL_POLICY_SETTLED",policy_role=name,**receipt))
    return output


def original_review_cohorts(*, predictions, labels, candidates, original_days):
    """Fixed original five slots: UNKNOWN is neither rejection nor cash."""
    for frame in (predictions,labels,candidates):
        if frame.duplicated(KEY).any():
            raise ValueError("original review cohort keys must be unique")
    original = candidates.loc[candidates.selection_effective_rank.le(5),[*KEY,"selection_effective_rank"]].copy()
    if (not original.groupby(KEY[0]).selection_effective_rank.apply(lambda v:list(v)==list(range(1,len(v)+1))).all()
            or set(map(tuple,predictions[KEY].to_numpy())) != set(map(tuple,original[KEY].to_numpy()))
            or set(map(tuple,labels[KEY].to_numpy())) != set(map(tuple,original[KEY].to_numpy()))):
        raise ValueError("original review prediction/settlement roster differs")
    rows = original.merge(predictions.loc[:,[*KEY,"status"]],on=KEY,validate="one_to_one",sort=False).merge(
        labels.loc[:,[*KEY,"settlement_status","baseline_net_bps"]],on=KEY,validate="one_to_one",sort=False)
    known_action = rows.status.isin(("ACCEPTABLE","AVOID"))
    if not rows.status.map(lambda v:isinstance(v,str)).all():
        raise ValueError("original review action status malformed")
    rows["paired_known"] = rows.settlement_status.eq("AVAILABLE") & known_action
    if not np.isfinite(rows.loc[rows.settlement_status.eq("AVAILABLE"),"baseline_net_bps"]).all():
        raise ValueError("available original review settlement must have finite return")
    rows["candidate_net_bps"] = np.where(rows.paired_known,np.where(rows.status.eq("AVOID"),0.,rows.baseline_net_bps),np.nan)
    rows["known_intervention"] = rows.paired_known & rows.status.eq("AVOID")
    frames = []
    for decision,group in rows.groupby(KEY[0],sort=True):
        paired = bool(group.paired_known.all())
        skip = group.loc[group.known_intervention,"baseline_net_bps"]
        frames.append(dict(decision_date=pd.Timestamp(decision).date().isoformat(),original_slots=5,
            known_settlements=int(group.settlement_status.eq("AVAILABLE").sum()),known_actions=int(group.status.isin(("ACCEPTABLE","AVOID")).sum()),
            paired_known=paired,baseline_bps=float(group.baseline_net_bps.sum()/5) if paired else np.nan,
            candidate_bps=float(group.candidate_net_bps.sum()/5) if paired else np.nan,
            increment_bps=float((group.candidate_net_bps-group.baseline_net_bps).sum()/5) if paired else np.nan,
            known_interventions=int(group.known_intervention.sum()) if paired else 0,
            avoided_loss_bps=float(-skip.clip(upper=0).sum()/5) if paired else np.nan,
            missed_profit_bps=float(skip.clip(lower=0).sum()/5) if paired else np.nan))
    daily = pd.DataFrame(frames).set_index("decision_date").reindex(original_days).reset_index()
    daily["paired_known"] = daily.paired_known.eq(True)
    if not np.allclose(daily.loc[daily.paired_known,"increment_bps"],
        daily.loc[daily.paired_known,"avoided_loss_bps"]-daily.loc[daily.paired_known,"missed_profit_bps"],rtol=1e-8,atol=1e-8):
        raise ValueError("original review avoided-loss/missed-profit attribution failed")
    return rows,daily


def evaluate_original_core_review(root, *, package_id, progress=None,
        query_spec_filename="core_query_spec_v1.json", prediction_directory="core_predictions", destination_name="core_evaluated_v1"):
    from backend.services.advisory_model_first.cross_package_validation_statistics_v1 import synchronous_block_inference
    root = Path(root)
    plan = checked_plan(root)
    base = root/"review_transfer_v1"/package_id
    checked_review_inputs(root, package_id=package_id)
    query_path = base/query_spec_filename
    spec = json.loads(query_path.read_text(encoding="utf-8"))
    if (spec["plan_sha256"] != file_sha(root/"plan.json")
            or spec["T_query_sha256"] != file_sha(base/"T_queries.parquet")
            or spec["physical_fit_count"] != 0 or spec["E_outcomes_used"] is not False):
        raise ValueError("original review evaluation clock/input contract changed")
    labels_root = base/"original_policy_settlement_v1"
    parent_spec_sha = spec.get("parent_core_query_spec_sha256",file_sha(query_path))
    if (parent_spec_sha != file_sha(base/"core_query_spec_v1.json")
            or json.loads((labels_root/"spec.json").read_text(encoding="utf-8"))["query_spec_sha256"] != parent_spec_sha):
        raise ValueError("original review settlement/prediction spec differs")
    candidates = pd.read_parquet(base/"candidates.parquet")
    regimes = {row["decision_date"]:row["regime"] for row in json.loads((root/"regimes.json").read_text(encoding="utf-8"))["rows"]}
    packages = sorted(p["package_id"] for p in json.loads((root/plan["inventory_filename"]).read_text(encoding="utf-8"))["packages"] if p["disposition"]=="IN_MATRIX")
    family = [(u["model_id"],u["arm"],p) for u in spec["units"] for p in packages]
    values = np.full((len(plan["decision_dates"]),len(family)),np.nan)
    results,frames = [],[]
    destination = base/destination_name
    for unit in spec["units"]:
        prediction_path = base/prediction_directory/unit["model_id"]/unit["arm"]/"predictions.parquet"
        prediction_receipt = json.loads(prediction_path.with_name("receipt.json").read_text(encoding="utf-8"))
        label_path = labels_root/unit["policy_role"]/"labels.parquet"
        label_receipt = json.loads(label_path.with_name("receipt.json").read_text(encoding="utf-8"))
        if (prediction_receipt["spec_sha256"] != file_sha(query_path) or prediction_receipt["predictions_sha256"] != file_sha(prediction_path)
                or label_receipt["labels_sha256"] != file_sha(label_path) or label_receipt["spec_sha256"] != file_sha(labels_root/"spec.json")):
            raise ValueError("original review immutable prediction/label binding changed")
        rows,daily = original_review_cohorts(predictions=pd.read_parquet(prediction_path),labels=pd.read_parquet(label_path),
            candidates=candidates,original_days=plan["decision_dates"])
        paired = daily.paired_known
        interventions = paired & daily.known_interventions.gt(0)
        regime_support = {regime:int(sum(flag and regimes[day]==regime for day,flag in zip(daily.decision_date,interventions,strict=True)))
                          for regime in sorted(set(regimes.values()))}
        known_days, known_interventions = int(paired.sum()),int(daily.loc[paired,"known_interventions"].sum())
        fraction = float(interventions.sum()/known_days) if known_days else 0.
        stats = plan["statistics"]
        support = (known_days>=stats["minimum_original_mature_days"] and known_interventions>=stats["minimum_known_settled_interventions"]
            and fraction>=stats["minimum_intervention_day_fraction"] and sum(c>=stats["minimum_intervention_days_per_regime"] for r,c in regime_support.items() if r!="UNKNOWN_REGIME")>=stats["minimum_regimes"])
        mean = float(daily.loc[paired,"increment_bps"].mean()) if known_days else None
        result = dict(model_id=unit["model_id"],family=unit["family"],arm=unit["arm"],package_id=package_id,policy_role=unit["policy_role"],
            original_days=len(daily),known_paired_days=known_days,known_interventions=known_interventions,intervention_day_fraction=fraction,
            regime_intervention_days=regime_support,support_met=bool(support),
            baseline_cohort_mean_bps=float(daily.loc[paired,"baseline_bps"].mean()) if known_days else None,
            candidate_cohort_mean_bps=float(daily.loc[paired,"candidate_bps"].mean()) if known_days else None,
            descriptive_increment_bps=mean,status="INSUFFICIENT_NEGATIVE" if mean is not None and mean<0 else "INSUFFICIENT_POSITIVE" if mean is not None and mean>0 else "INSUFFICIENT",
            original_row_count=len(rows),known_node_pairs=int(rows.paired_known.sum()),
            action_counts={str(k):int(v) for k,v in rows.status.value_counts().items()},physical_fit_count=0,activation_evidence=False)
        if "original_model_name" in unit:
            result["original_model_name"] = unit["original_model_name"]
        daily["model_id"],daily["arm"],daily["package_id"] = unit["model_id"],unit["arm"],package_id
        index = family.index((unit["model_id"],unit["arm"],package_id))
        values[:,index] = daily.increment_bps.to_numpy(dtype=float)
        publish_bytes(destination/unit["model_id"]/unit["arm"]/"paired_nodes.parquet",_parquet(rows))
        publish_bytes(destination/unit["model_id"]/unit["arm"]/"daily.parquet",_parquet(daily))
        results.append(result)
        frames.append(daily)
        if progress:
            progress(dict(event="ORIGINAL_REVIEW_EVALUATED",**result))
    inference = synchronous_block_inference(daily_values=values,original_days=plan["decision_dates"],expected_days=plan["decision_dates"],
        block_span=20,replicates=stats["bootstrap_count"],seed=stats["seed"])
    output = dict(query_spec_sha256=file_sha(query_path),settlement_spec_sha256=file_sha(labels_root/"spec.json"),
        comparisons=results,comparison_family=family,inference=inference,physical_fit_count=0,activation_evidence=False,
        role="REVIEW_CLOCK_ENTRY",decision_use="NAVIGATION_ONLY",not_a_portfolio_nav=True)
    publish_bytes(destination/"daily.parquet",_parquet(pd.concat(frames,ignore_index=True)))
    publish_json(destination/"results.json",output)
    return output
