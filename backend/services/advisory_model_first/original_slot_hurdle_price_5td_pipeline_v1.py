"""Six bounded offline fits, immutable stages and explicit partial recovery."""
from datetime import datetime, timezone
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time

import pandas as pd
from threadpoolctl import threadpool_limits

from backend.services.advisory_model_first.economic_entry_pipeline import _json_bytes, publish_stage, read_stage
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_contracts_v1 import (
    ARMS, COMPONENTS, HYPOTHESIS, KEY, PARAMETERS, POLICY, POLICY_SHA256, ROSTER_KEY, SCHEMA_SHA256,
    VALUATION_POLICY_SHA256, OriginalSlotHurdleStudyPlanV1, node_path, sha,
)
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_inputs_v1 import (
    EVAL_FINANCE, checked_source, prepare_hurdle_inputs_v1, read_projection_v1, sessions_v1,
)
from backend.services.advisory_model_first.original_slot_hurdle_price_5td_model_v1 import (
    bundle_payload_v1, fit_component_v1, query_hurdle_nodes_v1, supervision_v1, validate_bundle_v1, validate_component_v1,
)
from backend.services.advisory_model_first.parent_score_price_5td_inputs_v1 import parquet_bytes
from backend.services.advisory_model_first.parent_score_price_5td_pipeline_v1 import _exclusive_json, _journal, _node, _qe


def implementation_sha256():
    names = [f"original_slot_hurdle_price_5td_{n}_v1.py" for n in (
        "contracts", "inputs", "model", "pipeline", "evaluation", "cli")]
    names += ["generic_daily_price_input_v1.py", "generic_price_5td_contracts_v1.py", "generic_price_5td_models_v1.py",
        "generic_price_5td_valuation_v2.py", "risk_tail_calibrated_price_5td_inputs_v1.py",
        "parent_score_price_5td_contracts_v1.py", "parent_score_price_5td_inputs_v1.py",
        "parent_score_price_5td_pipeline_v1.py", "parent_score_price_5td_evaluation_v1.py",
        "generic_population_price_5td_cli_v1.py", "economic_entry_pipeline.py", "economic_value_anchor_contracts_v1.py",
        "research_control.py", "research_control_contracts.py"]
    return sha({n: hashlib.sha256((Path(__file__).parent/n).read_bytes().replace(b"\r\n", b"\n")).hexdigest() for n in names})


def _checked(plan, output_root):
    plan = OriginalSlotHurdleStudyPlanV1.model_validate(plan)
    if plan.implementation_sha256 != implementation_sha256():
        raise ValueError("hurdle implementation changed after preregistration")
    root, source = node_path(output_root)/plan.experiment_id, node_path(plan.source_prepared.artifact_uri)
    if root == source or root.is_relative_to(source.parent) or source.is_relative_to(root):
        raise ValueError("new artifacts cannot overwrite the original input store")
    checked_source(plan)
    if plan.exact_retry_of is not None:
        prior = node_path(plan.exact_retry_of)
        if prior == root or (prior/"trained").exists() or (prior/"evaluated").exists():
            raise ValueError("technical retry cannot replace a completed/negative study")
        old = OriginalSlotHurdleStudyPlanV1.model_validate_json((prior/"preregistered"/"plan.json").read_bytes())
        _stage(old, prior, "preregistered")
        if old.attempt_id == plan.attempt_id or old.economic_contract_sha256 != plan.economic_contract_sha256:
            raise ValueError("retry requires a new identity and the unchanged economic contract")
    return plan, root


def _x_temporary_files(operation):
    @wraps(operation)
    def scoped(**kwargs):
        plan = OriginalSlotHurdleStudyPlanV1.model_validate(kwargs["plan"])
        temporary = node_path(plan.temporary_root_uri)
        temporary.mkdir(parents=True, exist_ok=True)
        keys = ("TEMP", "TMP", "TMPDIR", "JOBLIB_TEMP_FOLDER")
        previous = {n: os.environ.get(n) for n in keys}
        prior_temp, prior_bytecode = tempfile.tempdir, sys.dont_write_bytecode
        try:
            os.environ.update({n: str(temporary) for n in keys})
            tempfile.tempdir, sys.dont_write_bytecode = str(temporary), True
            return operation(**kwargs)
        finally:
            for n, v in previous.items():
                if v is None:
                    os.environ.pop(n, None)
                else:
                    os.environ[n] = v
            tempfile.tempdir, sys.dont_write_bytecode = prior_temp, prior_bytecode
    return scoped


def _stage(plan, root, until):
    parent = None
    for stage in ("preregistered", "prepared", "trained", "evaluated"):
        header = read_stage(root/stage, stage=stage, plan_sha256=plan.plan_sha256, parent_sha256=parent)
        parent = header["stage_sha256"]
        if stage == until:
            return header
    raise ValueError("unknown hurdle stage")


def _register(plan, root, stage, *, generated=0, evaluated=0, evidence=None, result_class="EXPLORATORY"):
    from backend.services.advisory_model_first.research_control import AdvisoryResearchTrialRegistryV1, evidence_reference_for_file
    from backend.services.advisory_model_first.research_control_contracts import ConsumedWindowV1, build_trial_record
    record = build_trial_record(experiment_id=plan.experiment_id, attempt_id=plan.attempt_id, research_stage=stage.upper(),
        study_type=plan.study_type, objective_contract=plan.objective_contract, decision_use=plan.decision_use,
        hypothesis_family_id=HYPOTHESIS, unique_variable="ORIGINAL_TOP5_SIGN_MAGNITUDE_TERMINAL_VALUE_CONTRACT",
        parent_lineage=tuple(s.run_id for s in plan.sources)+((plan.exact_retry_of,) if plan.exact_retry_of else ()),
        dataset_identity=plan.source_prepared.stage_sha256, schema_identity=SCHEMA_SHA256, policy_identity=POLICY_SHA256,
        planned_trial_count=2, generated_trial_count=generated, evaluated_trial_count=evaluated, selected_trial_count=0,
        consumed_windows=(ConsumedWindowV1(window_id="ALREADY_CONSUMED_DEVELOPMENT_ONLY",
            dataset_identity=plan.source_prepared.stage_sha256, start_date=plan.train_start, end_date=plan.evaluation_end),),
        result_class=result_class, evidence_refs=(evidence_reference_for_file(evidence or root/stage/"manifest.json",
            role="original_slot_hurdle_"+stage),))
    AdvisoryResearchTrialRegistryV1(root.parent/"trial_registry.jsonl").append_batch((record,))


def _publish(plan, root, stage, parent, artifacts, *, generated=0, evaluated=0, result_class="EXPLORATORY"):
    if sum(len(v) for v in artifacts.values()) > 8*1024**3:
        raise ValueError("hurdle artifact budget exceeds 8GiB")
    path = publish_stage(study_root=root, stage=stage, plan_sha256=plan.plan_sha256,
        parent_sha256=parent, artifacts=artifacts)
    _register(plan, root, stage, generated=generated, evaluated=evaluated, result_class=result_class)
    return path


@_x_temporary_files
def preregister_hurdle_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    if (root/"preregistered").exists():
        _stage(plan, root, "preregistered")
        _register(plan, root, "preregistered")
        return root/"preregistered"
    return _publish(plan, root, "preregistered", None, {
        "plan.json": _json_bytes(plan.model_dump(mode="json")),
        "policy.json": _json_bytes(dict(policy=POLICY, parameters=PARAMETERS, physical_fit_budget=6,
            optimizer_fit_budget=2, previous_physical_fit_count=136, evaluated_configurations=2, candidate_count=1,
            economic_contract_sha256=plan.economic_contract_sha256, outputs_seen_before_design=True,
            source_refs=plan.source_prepared.model_dump(mode="json"), deployable=False))})


@_x_temporary_files
def prepare_hurdle_study_v1(*, plan, output_root):
    plan, root = _checked(plan, output_root)
    first = _stage(plan, root, "preregistered")
    if (root/"prepared").exists():
        _stage(plan, root, "prepared")
        _register(plan, root, "prepared")
        return root/"prepared"
    prepared = prepare_hurdle_inputs_v1(plan=plan)
    try:
        supervision_v1(prepared["domain"], prepared["encoding"])
        status = "PREPARED_NO_RESEARCH_FIT"
    except ValueError as exc:
        if not str(exc).startswith("PREPARED_NO_FIT"):
            raise
        status = "PREPARED_NO_FIT"
    receipt = dict(status=status, original_rows=len(prepared["roster"]), original_package_days=len(prepared["days"]),
        original_train_top5_rows=len(prepared["domain"]), physical_fit_count=0, optimizer_fit_count=0,
        source_stage_sha256=prepared["source_stage_sha256"], outputs_seen_before_design=True,
        test_or_sealed_finance_consumed=False, evaluation_finance_consumed=False, parent_scores_consumed=False,
        database_written=False, deployable=False)
    _checked(plan, output_root)
    return _publish(plan, root, "prepared", first["stage_sha256"], {
        "roster.parquet": parquet_bytes(prepared["roster"]), "days.parquet": parquet_bytes(prepared["days"]),
        "domain.parquet": parquet_bytes(prepared["domain"]), "encoding.json": _json_bytes(prepared["encoding"]),
        "calendar.json": _json_bytes(prepared["calendar"]), "receipt.json": _json_bytes(receipt)})


def _component(plan, root, prepared, encoding, arm, name):
    path = root/"fits"/(arm+"_"+name)/"trained"
    header = read_stage(path, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=prepared["stage_sha256"])
    model = json.loads((path/"model.json").read_bytes())
    validate_component_v1(model, arm=arm, component=name, encoding=encoding, plan_sha256=plan.plan_sha256)
    return model, dict(stage_sha256=header["stage_sha256"], component_sha256=model["component_sha256"])


def _completed_bundle(plan, root, prepared, encoding):
    models, refs = {}, {}
    for arm in ARMS:
        models[arm] = {}
        for name in COMPONENTS:
            models[arm][name], refs[arm+"_"+name] = _component(plan, root, prepared, encoding, arm, name)
    return bundle_payload_v1(models=models, encoding=encoding, plan_sha256=plan.plan_sha256), refs


def _budget(started):
    import psutil
    if time.monotonic()-started > 1800 or psutil.Process().memory_info().rss > 8*1024**3:
        raise ValueError("RESOURCE_BUDGET_EXCEEDED: no next component will start")


def _prior_fit_attempts(plan):
    """Technical lineages retain additional actual attempts, not just six successes."""
    count, seen, current = 0, set(), plan
    while current.exact_retry_of is not None:
        previous = node_path(current.exact_retry_of)
        if previous in seen or len(seen) >= 16:
            raise ValueError("technical recovery lineage is cyclic or exceeds its bounded audit")
        seen.add(previous)
        old = OriginalSlotHurdleStudyPlanV1.model_validate_json((previous/"preregistered"/"plan.json").read_bytes())
        _stage(old, previous, "preregistered")
        if old.economic_contract_sha256 != plan.economic_contract_sha256:
            raise ValueError("technical lineage changed its economic contract")
        for arm in ARMS:
            for name in COMPONENTS:
                attempt = previous/"fits"/(arm+"_"+name)/"attempt.json"
                if not attempt.exists():
                    continue
                body = json.loads(attempt.read_bytes())
                if body.get("plan_sha256") != old.plan_sha256 or body.get("physical_fit_count") != 1:
                    raise ValueError("prior fit attempt identity/count differs")
                count += 1
        current = old
    return count


@_x_temporary_files
def train_hurdle_study_v1(*, plan, output_root, qe_idle_probe):
    plan, root = _checked(plan, output_root)
    prepared = _stage(plan, root, "prepared")
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    if (root/"trained").exists():
        _stage(plan, root, "trained")
        bundle, refs = _completed_bundle(plan, root, prepared, encoding)
        if (json.loads((root/"trained"/"bundle.json").read_bytes()) != bundle
                or json.loads((root/"trained"/"components.json").read_bytes()) != refs):
            raise ValueError("completed bundle/component chain differs")
        _register(plan, root, "trained", generated=2)
        return root/"trained"
    rows = pd.read_parquet(root/"prepared"/"domain.parquet")
    supervision_v1(rows, encoding)  # Missing classes: no attempt claimed and no fake model.
    node, started = _node(plan), time.monotonic()
    for arm in ARMS:
        for name in COMPONENTS:
            component_root = root/"fits"/(arm+"_"+name)
            if (component_root/"trained").exists():
                _component(plan, root, prepared, encoding, arm, name)
                continue
            if (component_root/"attempt.json").exists():
                raise ValueError("TECHNICAL_RECOVERY_REQUIRED: prior STARTED/FAILED component has no immutable model")
            _budget(started)
            before = _qe(qe_idle_probe, require_idle=True)
            _exclusive_json(component_root/"attempt.json", dict(status="STARTED", arm=arm, component=name,
                plan_sha256=plan.plan_sha256, physical_fit_count=1, optimizer_fit_count=int(name == "negative"),
                node=node, qe_observation=before, captured_at=datetime.now(timezone.utc).isoformat()))
            _journal(root, dict(kind="PHYSICAL_FIT_STARTED", arm=arm, component=name, qe_observation=before))
            try:
                with threadpool_limits(limits=2):
                    payload = fit_component_v1(rows=rows, encoding=encoding, arm=arm, component=name, plan_sha256=plan.plan_sha256)
                _stage(plan, root, "prepared")
                publish_stage(study_root=component_root, stage="trained", plan_sha256=plan.plan_sha256,
                    parent_sha256=prepared["stage_sha256"], artifacts={"model.json": _json_bytes(payload)})
                _journal(root, dict(kind="PHYSICAL_FIT_COMPLETE", arm=arm, component=name,
                    component_sha256=payload["component_sha256"]))
                after = _qe(qe_idle_probe, require_idle=False)
                _journal(root, dict(kind="POST_FIT_QE_OBSERVATION", arm=arm, component=name, qe_observation=after))
                if any(after["active_counts"].values()):
                    raise ValueError("RESOURCE_OVERLAP_DETECTED: completed model retained, no subsequent fit")
                _budget(started)
            except Exception as exc:
                _journal(root, dict(kind="ATTEMPT_INCOMPLETE_NO_IMPLICIT_RETRY", arm=arm, component=name,
                    model_completed=(component_root/"trained").exists(), error_type=type(exc).__name__, reason=str(exc)))
                raise
    bundle, refs = _completed_bundle(plan, root, prepared, encoding)
    _checked(plan, output_root)
    prior_attempts = _prior_fit_attempts(plan)
    receipt = dict(status="SIX_COMPONENTS_COMPLETE_NO_ACTIVATION", physical_fit_count=6, optimizer_fit_count=2,
        completed_components=6, evaluated_configurations=2, candidate_count=1,
        physical_fits_started_in_current_attempt=sum((root/"fits"/(a+"_"+c)/"attempt.json").exists() for a in ARMS for c in COMPONENTS),
        previous_physical_fit_count=136, technical_lineage_prior_fit_attempts=prior_attempts,
        technical_lineage_actual_fit_attempts=6+prior_attempts, cumulative_physical_fit_attempts=142+prior_attempts,
        node=node, no_atomic_resource_lock_claimed=True,
        test_or_sealed_finance_consumed=False, database_written=False, deployable=False)
    return _publish(plan, root, "trained", prepared["stage_sha256"], {
        "bundle.json": _json_bytes(bundle), "components.json": _json_bytes(refs), "receipt.json": _json_bytes(receipt)}, generated=2)


@_x_temporary_files
def evaluate_hurdle_study_v1(*, plan, output_root):
    from backend.services.advisory_model_first.original_slot_hurdle_price_5td_evaluation_v1 import evaluate_hurdle_cohorts_v1
    plan, root = _checked(plan, output_root)
    trained, prepared = _stage(plan, root, "trained"), _stage(plan, root, "prepared")
    encoding = json.loads((root/"prepared"/"encoding.json").read_bytes())
    bundle, refs = _completed_bundle(plan, root, prepared, encoding)
    if (json.loads((root/"trained"/"bundle.json").read_bytes()) != bundle
            or json.loads((root/"trained"/"components.json").read_bytes()) != refs):
        raise ValueError("evaluation cannot consume a mixed bundle/stage chain")
    if (root/"evaluated").exists():
        _stage(plan, root, "evaluated")
        _register(plan, root, "evaluated", generated=2, evaluated=2,
            result_class="NEGATIVE" if json.loads((root/"evaluated"/"evaluation.json").read_bytes())["status"].startswith("NEGATIVE") else "EXPLORATORY")
        return root/"evaluated"
    validate_bundle_v1(bundle)
    roster = pd.read_parquet(root/"prepared"/"roster.parquet")
    clocks, calendar = encoding["clocks"], encoding["clocks"]["calendar"]
    inputs = roster.loc[roster[KEY[0]].isin(pd.to_datetime(clocks["evaluation_dates"])) & roster.selection_effective_rank.le(5)].copy()
    gaps = read_projection_v1(plan=plan, dates=clocks["gap_evaluation_dates"], columns=("observed_gap_bps",), top5=True)
    inputs = inputs.merge(gaps[[*ROSTER_KEY, "observed_gap_bps"]], on=list(ROSTER_KEY), how="left", validate="one_to_one")
    forecasts_root = root/"forecasts"/"trained"
    if forecasts_root.exists():
        read_stage(forecasts_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
        frozen = pd.read_parquet(forecasts_root/"predictions.parquet")
        predictions = {a: frozen.loc[frozen.arm.eq(a)].reset_index(drop=True) for a in ARMS}
    else:
        predictions = {a: query_hurdle_nodes_v1(bundle=bundle, features=inputs, scenario_gap_bps=inputs.observed_gap_bps, arm=a) for a in ARMS}
        publish_stage(study_root=root/"forecasts", stage="trained", plan_sha256=plan.plan_sha256,
            parent_sha256=trained["stage_sha256"], artifacts={
                "predictions.parquet": parquet_bytes(pd.concat(list(predictions.values()), ignore_index=True)),
                "freeze.json": _json_bytes(dict(bundle_sha256=bundle["bundle_sha256"], inputs_sha256=sha(inputs.astype(str).to_dict("records")),
                    evaluation_finance_consumed=False, frozen_before_financial_projection=True))})
    freeze = read_stage(forecasts_root, stage="trained", plan_sha256=plan.plan_sha256, parent_sha256=trained["stage_sha256"])
    freeze_body = json.loads((forecasts_root/"freeze.json").read_bytes())
    if (freeze_body["inputs_sha256"] != sha(inputs.astype(str).to_dict("records"))
            or freeze_body["bundle_sha256"] != bundle["bundle_sha256"]):
        raise ValueError("frozen prediction/input identity differs")
    # First financial-outcome decoding occurs only after immutable forecasts exist.
    financial = read_projection_v1(plan=plan, dates=clocks["mature_evaluation_dates"], columns=EVAL_FINANCE,
        top5=True, forecast_stage=forecasts_root)
    rows = inputs.drop(columns="observed_gap_bps").merge(financial[[*ROSTER_KEY, *EVAL_FINANCE]],
        on=list(ROSTER_KEY), how="left", validate="one_to_one", indicator=True)
    mature = rows[KEY[0]].isin(pd.to_datetime(clocks["mature_evaluation_dates"]))
    if not rows.loc[mature, "_merge"].eq("both").all() or len(financial) != int(mature.sum()):
        raise ValueError("mature original Top5 finance was changed/removed")
    sessions = sessions_v1(calendar)
    for n in ("valuation_status", "label_status", "exit_execution_status"):
        rows.loc[~mature, n] = "IMMATURE"
    rows.loc[~mature, "valuation_policy_sha256"] = VALUATION_POLICY_SHA256
    rows.loc[~mature, "label_information_end"] = [sessions[sessions.get_loc(d)+5] for d in rows.loc[~mature, KEY[0]]]
    rows = rows.drop(columns="_merge")
    result = evaluate_hurdle_cohorts_v1(rows=rows, days=pd.read_parquet(root/"prepared"/"days.parquet"),
        predictions=predictions, original_roster=roster, plan=plan, calendar=calendar)
    result.update(bundle_sha256=bundle["bundle_sha256"], prediction_freeze_stage_sha256=freeze["stage_sha256"])
    _checked(plan, output_root)
    _stage(plan, root, "trained")
    return _publish(plan, root, "evaluated", trained["stage_sha256"], {"evaluation.json": _json_bytes(result),
        "receipt.json": _json_bytes(dict(status=result["status"], physical_fit_count=6, optimizer_fit_count=2,
            evaluated_configurations=2, independent_oos_evidence=False, activation_evidence=False,
            test_or_sealed_finance_consumed=False, database_written=False, deployable=False))},
        generated=2, evaluated=2, result_class="NEGATIVE" if result["status"].startswith("NEGATIVE") else "EXPLORATORY")
