"""Advisory readonly components and a bounded offline study; never deployment."""
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time

import pandas as pd
import pyarrow.dataset as ds

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, file_sha256, publish_stage, read_stage
from backend.services.advisory_model_first.economic_entry_sources import _month_batches, _suspension_states
from backend.services.advisory_model_first.generic_daily_price_input_v1 import _day, _number
from backend.services.advisory_model_first.generic_population_price_5td_contracts_v1 import (
    DAILY_FIELDS, INDEX_FIELDS, STUDY_ARMS, PopulationInputPlanV1, PopulationStudyPlanV1,
)
from backend.services.advisory_model_first.generic_population_price_5td_population_v1 import (
    _verify, build_population_inputs_v1, prepare_population_metadata_v1,
)
from backend.services.advisory_model_first.research_control_contracts import EvidenceReferenceV1
from backend.services.strategy_package.runtime_variant import canonical_json_sha256 as sha

SOURCE = "CURRENT_DATABASE_NON_VINTAGE"


def preparation_implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_population_price_5td_{n}_v1.py" for n in ("contracts", "population", "pipeline", "cli")]
    names += ["generic_daily_price_input_v1.py", "generic_price_5td_labels_v1.py", "generic_price_5td_models_v1.py", "economic_entry_sources.py"]
    return sha({n: file_sha256(directory/n) for n in names})


def _root(value):
    path = Path(value)
    if not path.is_absolute() or path.resolve() != path or path.drive.upper() == "C:":
        raise ValueError("population components require an explicit non-C real output root")
    return path


def _parquet(frame):
    stream = io.BytesIO()
    frame.to_parquet(stream, index=False)
    return stream.getvalue()


def _read_database(*, symbols, calendar, cutoff, connection_context_factory=None, progress=None):
    from backend.db.pg_pool import get_conn
    factory = connection_context_factory or (lambda: get_conn(autocommit=False, manage_transaction=False))
    days = [d for d in calendar if d <= cutoff]
    maximum = len(symbols)*len(days)
    queries, parts = [], []
    started = datetime.now(timezone.utc)
    with factory() as connection:
        cursor = connection.cursor()
        def read(name, sql, params, columns, budget):
            began = time.monotonic()
            cursor.execute(sql, params)
            values = cursor.fetchall()
            if len(values) > budget:
                raise ValueError(f"population readonly source exceeds exact row budget: {name}")
            queries.append(dict(phase=name, rows=len(values), seconds=round(time.monotonic()-began, 3)))
            if progress is not None:
                progress(queries[-1])
            return pd.DataFrame(values, columns=columns)
        try:
            connection.set_session(isolation_level="REPEATABLE READ", readonly=True, autocommit=False)
            cursor.execute("SET LOCAL statement_timeout = %s", (30000,))
            original = read("calendar", "SELECT cal_date FROM market.trading_calendar WHERE is_trading=TRUE AND cal_date BETWEEN %s AND %s ORDER BY cal_date",
                (days[0], days[-1]), ("trade_date",), len(days))
            if original.trade_date.map(_day).tolist() != days:
                raise ValueError("population DB calendar contradicts original frozen calendar")
            for start, end in _month_batches(days[0], days[-1]):
                bound = len(symbols)*sum(start <= d <= end for d in days)
                part = read("daily:"+start.isoformat(), """SELECT p.trade_date,p.ts_code,p.open_li,p.high_li,p.low_li,p.close_li,
                    a.adj_factor,p.volume_hand,l.up_limit,l.down_limit
                    FROM market.kline_daily_raw p
                    LEFT JOIN market.adj_factor a ON a.trade_date=p.trade_date AND a.ts_code=p.ts_code AND a.trade_date BETWEEN %s AND %s
                    LEFT JOIN market.stk_limit l ON l.trade_date=p.trade_date AND l.ts_code=p.ts_code AND l.trade_date BETWEEN %s AND %s
                    WHERE p.ts_code=ANY(%s::text[]) AND p.trade_date BETWEEN %s AND %s
                    ORDER BY p.trade_date,p.ts_code LIMIT %s""",
                    (start, end, start, end, symbols, start, end, bound+1),
                    (*DAILY_FIELDS[:2], "open_li", "high_li", "low_li", "close_li", "adj_factor", "volume_hand", "up_limit", "down_limit"), bound)
                parts.append(part)
            suspension = read("suspend", """SELECT trade_date,ts_code,suspend_type,suspend_timing FROM market.suspend_d
                WHERE ts_code=ANY(%s::text[]) AND trade_date BETWEEN %s AND %s
                ORDER BY trade_date,ts_code,suspend_type,suspend_timing NULLS FIRST LIMIT %s""",
                (symbols, days[0], days[-1], 2*maximum+1), ("trade_date", "instrument", "suspend_type", "suspend_timing"), 2*maximum)
            indices = read("index", "SELECT trade_date,ts_code,close FROM market.index_daily WHERE ts_code='000300.SH' AND trade_date BETWEEN %s AND %s ORDER BY trade_date LIMIT %s",
                (days[0], days[-1], len(days)+1), INDEX_FIELDS, len(days))
        finally:
            connection.rollback()
            cursor.close()
    daily = pd.concat(parts, ignore_index=True)
    for frame in (daily, suspension, indices):
        frame["trade_date"] = frame.trade_date.map(_day).map(pd.Timestamp)
        if not frame.trade_date.isin(pd.to_datetime(days)).all():
            raise ValueError("population readonly source returned foreign dates")
    if (daily.duplicated(["trade_date", "instrument"]).any() or indices.duplicated(["trade_date", "instrument"]).any()
            or not set(daily.instrument).issubset(symbols) or not set(suspension.instrument).issubset(symbols)
            or not indices.instrument.eq("000300.SH").all() or not suspension.suspend_type.isin(["S", "R"]).all()):
        raise ValueError("population readonly source contains conflicting keys/states")
    daily["price_placeholder_fields"] = ""
    for name in ("open", "high", "low", "close"):
        original = daily[name+"_li"].map(lambda v: _number(v, nonnegative=True)).astype(float)
        daily.loc[original.eq(0), "price_placeholder_fields"] += name+";"
        daily["raw_"+name+"_cny"] = (original/1000.).mask(original.eq(0))
    states = _suspension_states(suspension)
    daily = daily.merge(states, on=["trade_date", "instrument"], how="outer", validate="one_to_one")
    for name in ("suspended", "tradability_unknown"):
        daily[name] = daily[name].astype("boolean").fillna(False).astype(bool)
    daily["price_placeholder_fields"] = daily.price_placeholder_fields.fillna("NO_RAW_BAR_SUSPENSION_RECORD")
    daily = daily.loc[:, DAILY_FIELDS]
    receipt = dict(source_evidence=SOURCE, readonly=True, isolation="REPEATABLE READ", statement_timeout_ms=30000,
        read_started_at=started.isoformat(), read_completed_at=datetime.now(timezone.utc).isoformat(), queries=queries,
        price_unit_divisor=1000, volume_unit_multiplier=100, zero_placeholder_rows=int(daily.price_placeholder_fields.ne("").sum()),
        market_up_ratio="UNKNOWN_DEFINITION_NOT_REQUESTED", native_capture=False, database_written=False,
        physical_fit_count=0, research_run_created=False, source_financial_cutoff=cutoff.isoformat())
    return daily, indices, receipt


def freeze_population_source_v1(*, metadata_request, output_root, connection_context_factory=None, progress=None):
    identity = preparation_implementation_sha256()
    recipe = dict(metadata_request=metadata_request.model_dump(mode="json"), implementation_sha256=identity,
                  component="POPULATION_SOURCE_NOT_MODEL_RUN", source_evidence=SOURCE)
    key = sha(recipe)
    root = _root(output_root)/("advgp5popsource_"+key[:24])
    initial = publish_stage(study_root=root, stage="preregistered", plan_sha256=key, parent_sha256=None,
        artifacts={"recipe.json": _json_bytes(recipe)})
    parent = read_stage(initial, stage="preregistered", plan_sha256=key, parent_sha256=None)
    if (root/"prepared").exists():
        read_stage(root/"prepared", stage="prepared", plan_sha256=key, parent_sha256=parent["stage_sha256"])
        return root
    metadata = prepare_population_metadata_v1(request=metadata_request)
    calendar = [_day(d) for d in json.loads(_verify(metadata_request.calendar_ref).read_text(encoding="utf-8"))]
    daily, indices, receipt = _read_database(symbols=sorted(set(metadata.rosters.instrument)), calendar=calendar,
        cutoff=metadata_request.evaluation_end, connection_context_factory=connection_context_factory, progress=progress)
    if identity != preparation_implementation_sha256():
        raise ValueError("population source code changed during the readonly snapshot")
    for source in metadata_request.sources:
        for ref in (source.candidate_ref, source.lineage_ref, source.bundle_manifest_ref):
            if ref is not None:
                _verify(ref)
    _verify(metadata_request.calendar_ref)
    receipt.update(metadata_request_sha256=sha(metadata_request.model_dump(mode="json")), source_recipe_sha256=key)
    publish_stage(study_root=root, stage="prepared", plan_sha256=key, parent_sha256=parent["stage_sha256"],
        artifacts={"daily.parquet": _parquet(daily), "index_daily.parquet": _parquet(indices), "receipt.json": _json_bytes(receipt)})
    return root


def study_implementation_sha256():
    directory = Path(__file__).parent
    names = [f"generic_population_price_5td_{n}_v1.py" for n in ("contracts", "population", "pipeline", "cli", "model", "evaluation")]
    names += ["generic_price_5td_contracts_v1.py", "generic_price_5td_models_v1.py", "generic_daily_price_input_v1.py",
              "economic_entry_pipeline.py", "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((directory/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _study_input(plan, output_root):
    plan = PopulationStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != study_implementation_sha256():
        raise ValueError("population study source differs from its preregistered identity")
    source, root = _root(plan.input_root_uri), _root(output_root)/plan.experiment_id
    if source == root or source in root.parents:
        raise ValueError("population study output cannot be nested inside its frozen input component")
    if (file_sha256(source/"preregistered"/"plan.json") != plan.input_plan_file_sha256
            or file_sha256(source/"prepared"/"manifest.json") != plan.input_manifest_file_sha256):
        raise ValueError("population frozen input plan/manifest differs")
    parent = PopulationInputPlanV1.model_validate_json((source/"preregistered"/"plan.json").read_bytes())
    if source.name != parent.component_id or parent.metadata_request.calendar_ref.sha256 != plan.calendar_ref.sha256:
        raise ValueError("population component/calendar identity differs")
    initial = read_stage(source/"preregistered", stage="preregistered", plan_sha256=parent.plan_sha256, parent_sha256=None)
    prepared = read_stage(source/"prepared", stage="prepared", plan_sha256=parent.plan_sha256, parent_sha256=initial["stage_sha256"])
    calendar = [_day(v) for v in json.loads(_verify(plan.calendar_ref).read_bytes())]
    if not calendar or len(calendar) > 5000 or calendar != sorted(set(calendar)):
        raise ValueError("population original calendar differs")
    encoding = json.loads((source/"prepared"/"encoding.json").read_bytes())
    return plan, root, source, parent, prepared, calendar, encoding


def _study_stage(plan, root, until):
    previous = None
    for name in ("preregistered", "prepared", "trained", "evaluated"):
        body = read_stage(root/name, stage=name, plan_sha256=plan.plan_sha256, parent_sha256=previous)
        previous = body["stage_sha256"]
        if name == until:
            return body
    raise ValueError("unknown population study stage")


def _prepared_study(plan, root, source, parent, inputs, encoding):
    prepared = _study_stage(plan, root, "prepared")
    binding = json.loads((root/"prepared"/"input_binding.json").read_bytes())
    expected = dict(root_uri=plan.input_root_uri, input_plan_sha256=parent.plan_sha256,
                    input_stage_sha256=inputs["stage_sha256"], files=inputs["files"], calendar_sha256=plan.calendar_ref.sha256)
    if binding != expected or json.loads((root/"prepared"/"encoding.json").read_bytes()) != encoding:
        raise ValueError("population prepared binding/encoding contradicts its original input")
    return prepared


def _study_record(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import SCHEMA_SHA256
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY_SHA256
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id="exact_attempt_v1", research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id="generic_population_price_5td_v1", unique_variable="TRAINING_POPULATION_BREADTH",
        parent_lineage=(Path(plan.input_root_uri).name,), dataset_identity=plan.input_manifest_file_sha256,
        schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256, planned_trial_count=2,
        generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0, result_class=result_class,
        consumed_windows=(ConsumedWindowV1(window_id="CONSUMED_DEVELOPMENT_ONLY", dataset_identity=plan.input_manifest_file_sha256,
            start_date="2024-07-04", end_date="2025-09-30"),),
        evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json", role="population_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _study_publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("population study artifact budget exceeded")
    target = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent, artifacts=artifacts)
    _study_record(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return target


def preregister_population_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import POLICY
    plan, root, *_ = _study_input(plan, output_root)
    return _study_publish(plan, root, "preregistered", None,
        {"plan.json": _json_bytes(plan.model_dump(mode="json")), "policy.json": _json_bytes(POLICY)})


def prepare_population_study_v1(*, plan, output_root):
    plan, root, source, parent, inputs, calendar, encoding = _study_input(plan, output_root)
    initial = _study_stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _study_stage(plan, root, "prepared")
        _study_record(plan, root, "prepared")
        return root/"prepared"
    receipt = json.loads((source/"prepared"/"receipt.json").read_bytes())
    if (encoding.get("population_contrast_status") != "PREPARED_IDENTIFIABLE_NO_FIT"
            or encoding.get("eligible_new_training_clusters", 0) <= 0 or encoding.get("physical_fit_count") != 0
            or receipt.get("plan_sha256") != parent.plan_sha256):
        raise ValueError("population contrast is not identifiable from the original component; no fit")
    return _study_publish(plan, root, "prepared", initial["stage_sha256"],
        {"input_binding.json": _json_bytes(dict(root_uri=plan.input_root_uri, input_plan_sha256=parent.plan_sha256,
            input_stage_sha256=inputs["stage_sha256"], files=inputs["files"], calendar_sha256=plan.calendar_ref.sha256)),
         "encoding.json": _json_bytes(encoding), "receipt.json": _json_bytes(dict(
            status="POPULATION_STUDY_PREPARED_NO_FIT", original_rows=receipt["original_rows"],
            unique_clusters=receipt["unique_clusters"], anchor_source_id=parent.metadata_request.sources[0].source_id,
            physical_fit_count=0, calibration_or_threshold_fit=False, test_consumed=False,
            input_copied_or_rebuilt=False, calendar_sessions=len(calendar), deployable=False))})


def _node_observation(plan):
    if os.name == "nt" or Path(sys.executable).resolve() != Path(plan.node_python_uri).resolve():
        raise ValueError("population real research requires its explicit existing WSL/worker Python, not Windows")
    return dict(execution_node=plan.execution_node, python_uri=sys.executable, platform=sys.platform)


def _qe_observation(probe, *, idle):
    body = probe()
    if (not isinstance(body, dict) or set(body) != {"captured_at", "active_counts"}
            or not isinstance(body["active_counts"], dict) or set(body["active_counts"]) != {"single", "custom_evo", "multi_alpha"}
            or any(type(v) is not int or v < 0 for v in body["active_counts"].values())):
        raise ValueError("population fit needs fresh public running/pending observations for all three QE paths")
    captured = datetime.fromisoformat(body["captured_at"])
    if captured.tzinfo is None or not 0 <= (datetime.now(timezone.utc)-captured).total_seconds() <= 60:
        raise ValueError("population QE observation is stale")
    if idle and any(body["active_counts"].values()):
        raise ValueError("population fit waits for idle QE; no QE process is controlled")
    return body


def _exclusive_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("xb") as stream:
        stream.write(_json_bytes(value))
        stream.flush()
        os.fsync(stream.fileno())


def _journal(root, value):
    with (root/"fit_journal.jsonl").open("ab") as stream:
        stream.write(_json_bytes(value).replace(b"\n", b"")+b"\n")
        stream.flush()
        os.fsync(stream.fileno())


def train_population_study_v1(*, plan, output_root, qe_idle_probe):
    from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import (
        model_payload_v1, train_population_price_5td_v1,
    )
    plan, root, source, parent, inputs, _, encoding = _study_input(plan, output_root)
    prepared = _prepared_study(plan, root, source, parent, inputs, encoding)
    if (root/"trained").exists():
        _study_stage(plan, root, "trained")
        _study_record(plan, root, "trained", generated=2)
        return root/"trained"
    if (root/"fit_attempt.json").exists():
        raise ValueError("population partial/STARTED attempt requires reconciliation; never implicitly refit")
    node = _node_observation(plan)
    models, count, claimed = {}, 0, False
    began = time.monotonic()
    try:
        for arm in STUDY_ARMS:
            # Filter original membership before decoding supervision; held/evaluation never become training rows.
            rows = ds.dataset(source/"prepared"/"clusters.parquet", format="parquet").to_table(
                filter=ds.field(arm+"_pool").isin(["STRUCTURE", "ESTIMATION"])).to_pandas()
            after = {}
            def before_fit(name):
                nonlocal count, claimed
                if name != arm or count >= 2:
                    raise ValueError("population physical fit ordinal/arm differs")
                observation = _qe_observation(qe_idle_probe, idle=True)
                if count == 0:
                    _exclusive_json(root/"fit_attempt.json", dict(status="STARTED", plan_sha256=plan.plan_sha256,
                        node=node, physical_fit_budget=2, started_at=datetime.now(timezone.utc).isoformat()))
                    claimed = True
                count += 1
                start = root/"fits"/arm/"attempt.json"
                _exclusive_json(start, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation))
                _journal(root, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, ordinal=count, qe_observation=observation))
                _study_record(plan, root, "fit_started_"+str(count), generated=count, evidence=start)
            def after_fit(name):
                if name != arm:
                    raise ValueError("population after-fit arm differs")
                after.update(_qe_observation(qe_idle_probe, idle=False))
                _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", arm=arm, ordinal=count, qe_observation=after))
            fitted = train_population_price_5td_v1(clusters=rows, encoding=encoding, arm=arm,
                input_plan_sha256=parent.plan_sha256, before_fit=before_fit, after_fit=after_fit)
            artifact = publish_stage(study_root=root/"fits"/arm, stage="trained", plan_sha256=plan.plan_sha256,
                parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(model_payload_v1(fitted))})
            header = read_stage(artifact, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
            models[arm] = dict(stage_sha256=header["stage_sha256"], model_sha256=fitted.model_sha256, files=header["files"])
            _journal(root, dict(kind="PHYSICAL_FIT_COMPLETED", arm=arm, ordinal=count, model_sha256=fitted.model_sha256))
            if any(after["active_counts"].values()):
                raise ValueError("QE started during this fit; model retained, next fit waits for explicit reconciliation")
    except Exception as exc:
        if claimed:
            failure = root/"fit_failure.json"
            _exclusive_json(failure, dict(status="PARTIAL_NO_AUTOMATIC_RETRY", started_fit_count=count,
                error_type=type(exc).__name__, completed_arms=list(models), plan_sha256=plan.plan_sha256))
            _study_record(plan, root, "partial_fit", generated=count, evidence=failure, result_class="INCOMPLETE_NEGATIVE")
        raise
    _study_input(plan, output_root)
    if count != 2 or set(models) != set(STUDY_ARMS):
        raise ValueError("population two-arm fit ledger differs")
    return _study_publish(plan, root, "trained", prepared["stage_sha256"],
        {"models.json": _json_bytes(models), "fit_receipt.json": _json_bytes(dict(
            physical_fit_count=2, internal_tree_count_per_arm=128, model_trial_count=2,
            elapsed_seconds=time.monotonic()-began, node=node, source_commit=plan.source_commit,
            implementation_sha256=plan.implementation_sha256, fit_journal_sha256=file_sha256(root/"fit_journal.jsonl"),
            deployable=False, test_consumed=False))}, generated=2)


def evaluate_population_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.generic_price_5td_contracts_v1 import FEATURES, KEY
    from backend.services.advisory_model_first.generic_population_price_5td_evaluation_v1 import evaluate_population_cohorts_v1
    from backend.services.advisory_model_first.generic_population_price_5td_model_v1 import model_from_payload_v1, query_population_price_nodes_v1
    plan, root, source, parent, inputs, calendar, encoding = _study_input(plan, output_root)
    prepared = _prepared_study(plan, root, source, parent, inputs, encoding)
    trained = _study_stage(plan, root, "trained")
    if (root/"evaluated").exists():
        _study_stage(plan, root, "evaluated")
        cached = json.loads((root/"evaluated"/"evaluation.json").read_bytes())
        _study_record(plan, root, "evaluated", generated=2, evaluated=2,
                      result_class="NEGATIVE" if cached["status"].startswith("NEGATIVE") else "EXPLORATORY")
        return root/"evaluated"
    predicate = ((ds.field(KEY[0]) >= pd.Timestamp(parent.metadata_request.evaluation_start))
                 & (ds.field(KEY[0]) <= pd.Timestamp(parent.metadata_request.evaluation_end)))
    clusters = ds.dataset(source/"prepared"/"clusters.parquet", format="parquet").to_table(filter=predicate).to_pandas()
    rows = ds.dataset(source/"prepared"/"rows.parquet", format="parquet").to_table(filter=predicate).to_pandas()
    days = ds.dataset(source/"prepared"/"days.parquet", format="parquet").to_table(filter=predicate).to_pandas()
    predictions = {}
    descriptors = json.loads((root/"trained"/"models.json").read_bytes())
    if set(descriptors) != set(STUDY_ARMS):
        raise ValueError("population trained descriptor arms differ")
    for arm in STUDY_ARMS:
        model_root = root/"fits"/arm/"trained"
        header = read_stage(model_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
        if header["stage_sha256"] != descriptors[arm]["stage_sha256"]:
            raise ValueError("population trained arm reference differs")
        model = model_from_payload_v1(json.loads((model_root/"model.json").read_bytes()))
        if (model.recipe["arm"] != arm or model.model_sha256 != descriptors[arm]["model_sha256"]
                or model.recipe["input_plan_sha256"] != parent.plan_sha256 or model.recipe["encoding_sha256"] != sha(encoding)):
            raise ValueError("population fitted arm/model identity differs")
        # No labels, rank, scores or package identity are exposed to the predictor.
        predictions[arm] = query_population_price_nodes_v1(fitted=model, features=clusters.loc[:, [*KEY, *FEATURES]],
                                                           scenario_gap_bps=clusters.observed_gap_bps)
    result = evaluate_population_cohorts_v1(rows=rows, clusters=clusters, days=days, predictions=predictions,
        calendar=calendar, evaluation_start=parent.metadata_request.evaluation_start,
        evaluation_end=parent.metadata_request.evaluation_end, anchor_source_id=parent.metadata_request.sources[0].source_id)
    _study_input(plan, output_root)
    return _study_publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result)},
        generated=2, evaluated=2, result_class="NEGATIVE" if result["status"].startswith("NEGATIVE") else "EXPLORATORY")


def prepare_population_inputs_v1(*, metadata_request, source_root, output_root, progress=None):
    source_root = _root(source_root)
    recipe = json.loads((source_root/"preregistered"/"recipe.json").read_text(encoding="utf-8"))
    key = sha(recipe)
    if recipe["metadata_request"] != metadata_request.model_dump(mode="json") or recipe["source_evidence"] != SOURCE:
        raise ValueError("population source recipe contradicts requested original rosters")
    initial = read_stage(source_root/"preregistered", stage="preregistered", plan_sha256=key, parent_sha256=None)
    stage = read_stage(source_root/"prepared", stage="prepared", plan_sha256=key, parent_sha256=initial["stage_sha256"])
    def ref(name):
        descriptor = stage["files"][name]
        return EvidenceReferenceV1(role=name, artifact_uri=str(source_root/"prepared"/name), **descriptor)
    plan = PopulationInputPlanV1(metadata_request=metadata_request, daily_ref=ref("daily.parquet"),
        index_ref=ref("index_daily.parquet"), source_receipt_ref=ref("receipt.json"), preparation_implementation_sha256=preparation_implementation_sha256())
    source_receipt = json.loads(_verify(plan.source_receipt_ref).read_text(encoding="utf-8"))
    if (source_receipt.get("source_recipe_sha256") != key
            or source_receipt.get("metadata_request_sha256") != sha(metadata_request.model_dump(mode="json"))
            or source_receipt.get("source_financial_cutoff") != metadata_request.evaluation_end.isoformat()
            or source_receipt.get("source_evidence") != SOURCE or source_receipt.get("readonly") is not True
            or source_receipt.get("native_capture") is not False or source_receipt.get("database_written") is not False):
        raise ValueError("population source receipt contradicts readonly development scope")
    root = _root(output_root)/plan.component_id
    parent_path = publish_stage(study_root=root, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None,
        artifacts={"plan.json": _json_bytes(plan.model_dump(mode="json"))})
    parent = read_stage(parent_path, stage="preregistered", plan_sha256=plan.plan_sha256, parent_sha256=None)
    if (root/"prepared").exists():
        read_stage(root/"prepared", stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"])
        return root
    metadata = prepare_population_metadata_v1(request=metadata_request)
    calendar = [_day(d) for d in json.loads(_verify(metadata_request.calendar_ref).read_text(encoding="utf-8"))]
    cutoff = metadata_request.evaluation_end
    lower = calendar[0]
    def financial(ref, columns):
        path = _verify(ref)
        dataset = ds.dataset(path, format="parquet")
        predicate = (ds.field("trade_date") >= pd.Timestamp(lower)) & (ds.field("trade_date") <= pd.Timestamp(cutoff))
        if dataset.count_rows(filter=predicate) > max(1, len(set(metadata.rosters.instrument)))*len(calendar):
            raise ValueError("population source exceeds bounded projected finance")
        return dataset.to_table(columns=list(columns), filter=predicate).to_pandas()
    daily, indices = financial(plan.daily_ref, DAILY_FIELDS), financial(plan.index_ref, INDEX_FIELDS)
    rows, clusters, encoding = build_population_inputs_v1(metadata=metadata, request=metadata_request, calendar=calendar,
        daily=daily, index_daily=indices, source_context=dict(calendar_sha256=metadata_request.calendar_ref.sha256,
            prices_sha256=plan.daily_ref.sha256, references_sha256=plan.daily_ref.sha256,
            label_price_basis="D_REFERENCE_POLICY_RATIO", source_evidence=SOURCE), progress=progress)
    receipt = dict(metadata=metadata.receipt, encoding=encoding, source_evidence=SOURCE,
        original_rows=len(rows), unique_clusters=len(clusters), physical_fit_count=0, research_run_created=False,
        source_root=str(source_root), plan_sha256=plan.plan_sha256, implementation_sha256=plan.preparation_implementation_sha256,
        label_counts={str(k): int(v) for k, v in rows.label_status.value_counts().items()},
        maturity_counts={str(k): int(v) for k, v in rows.maturity_at_cutoff.value_counts().items()},
        feature_known_counts={name: int(rows[name].notna().sum()) for name in encoding["matrix_order"][:9]}, deployable=False)
    for evidence in (plan.daily_ref, plan.index_ref, plan.source_receipt_ref, metadata_request.calendar_ref):
        _verify(evidence)
    for source in metadata_request.sources:
        for evidence in (source.candidate_ref, source.lineage_ref, source.bundle_manifest_ref):
            if evidence is not None:
                _verify(evidence)
    if plan.preparation_implementation_sha256 != preparation_implementation_sha256():
        raise ValueError("population preparation code changed during calculation")
    publish_stage(study_root=root, stage="prepared", plan_sha256=plan.plan_sha256, parent_sha256=parent["stage_sha256"],
        artifacts={"rows.parquet": _parquet(rows), "clusters.parquet": _parquet(clusters), "days.parquet": _parquet(metadata.days),
                   "encoding.json": _json_bytes(encoding), "receipt.json": _json_bytes(receipt)})
    return root
